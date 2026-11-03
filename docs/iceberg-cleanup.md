# Iceberg tables cleanup

`lakectl iceberg cleanup` is the open-source, native-Go counterpart of the lakeFS
Enterprise **Tables Cleanup** job. It does the two things an Iceberg table needs to stay
maintainable, without Spark and without the operator touching the object store:

1. **expire** old snapshots of every table on every branch, by rewriting each table's
   metadata and *staging* the rewrite on the branch that holds the table; and
2. **mark and sweep** the table files that no live table state references any more, so
   that lakeFS' own garbage collection can reclaim the bytes.

Everything it does goes through the lakeFS REST API. The tool needs the permissions of an
operator (read and write the repository) and **no blockstore credentials at all**: the
physical bytes are reclaimed by lakeFS GC after the sweep has removed the registrations
that protected them.

- Package: [`pkg/icebergcatalog/cleanup`](../pkg/icebergcatalog/cleanup/doc.go)
- Command: [`cmd/lakectl/cmd/iceberg_cleanup.go`](../cmd/lakectl/cmd/iceberg_cleanup.go)
- Background: [Iceberg REST catalog](iceberg.md), [`ICEBERG_CATALOG_PLAN.md`](../ICEBERG_CATALOG_PLAN.md),
  [`design/accepted/gc-two-steps-separation.md`](../design/accepted/gc-two-steps-separation.md)

```bash
lakectl iceberg cleanup --repo my-repo \
  --max-snapshot-age-days 5 \
  --min-snapshots-to-keep 2 \
  --safety-age 72h
```

| Flag | Default | Meaning |
|---|---|---|
| `--repo` | — (required) | The repository to clean up. The job works on a whole repository: every branch is expired, and the reports are written on the default branch. |
| `--max-snapshot-age-days` | `5` | Expire snapshots older than this. A table property `history.expire.max-snapshot-age-ms` overrides it for that table. |
| `--min-snapshots-to-keep` | `2` | Always keep this many most recent snapshots. A table property `history.expire.min-snapshots-to-keep` overrides it for that table. |
| `--safety-age` | `72h` | Never sweep a table file whose registration is younger than this. The minimum is `12h`. |
| `--dry-run` | off | Compute every decision and write nothing at all. |
| `--mark-only` | off | Run expire and mark, then stop before sweeping. |
| `--sweep-only` | off | Sweep an earlier mark. Requires `--mark-id`. |
| `--mark-id` | — | The run ID (also the mark ID) of the mark to sweep. |

A run with none of the three mode flags does **all** of expire, mark and sweep.

## What a run does

### Phase 1: expire snapshots

For every branch, the job lists the table pointers visible on it (`_lakefs_iceberg/tables/…/​.pointer`,
staged pointers included — a table created on a branch but not committed is a table that
branch's clients already read), resolves each one to its `metadata.json`, and decides which
snapshots to keep:

- the **current snapshot** is always kept;
- every snapshot an **Iceberg ref** inside the metadata (`refs`, i.e. table-level branches
  and tags — not lakeFS tags) points at is always kept;
- the newest `history.expire.min-snapshots-to-keep` (or `--min-snapshots-to-keep`)
  snapshots are kept whatever their age;
- of the rest, snapshots **older than** `history.expire.max-snapshot-age-ms` (or
  `--max-snapshot-age-days`) are expired. A snapshot exactly at the boundary, or with a
  future timestamp, is kept: the fail-safe direction.

The rewrite is done with the Iceberg model (`MetadataBuilderFromBase` + `RemoveSnapshots`,
the same model and the same operation the catalog itself applies to a `remove-snapshots`
commit), so the document has the shape a catalog commit would have produced: `last-updated-ms`
becomes the run's instant, the old metadata file is appended to `metadata-log`, and
snapshots, snapshot-log, refs, statistics and partition statistics of the removed snapshots
are pruned together.

The new document is then staged on the branch:

1. the bytes are **uploaded to the table's own metadata directory**
   (`<table-location>/metadata/<seq>-<uuid>.metadata.json`), so the pointer's
   `metadata-location` stays a fully qualified physical URI and every reader keeps working;
2. the catalog's file registration
   (`_lakefs_iceberg/files/<table-uuid>/metadata/<name>`) is staged pointing at it, so the
   new metadata file is protected by the same commit that publishes the pointer;
3. the table pointer (`_lakefs_iceberg/tables/<ns>/<table>/.pointer`) is staged with the new
   `metadata-location`, the same table UUID and the same format version;
4. the temporary entry that step 1 created under `_iceberg/…` is removed again, so the
   catalog's registration is the only one — exactly like a catalog commit.

Nothing is committed. The changes wait in the branch's staging area for the operator's
`lakectl commit`, so a cleanup is reviewable with `lakectl diff`, atomic per branch, and
undoable with `lakectl branch revert`.

**Tables that are skipped or refused:**

| Situation | Outcome |
|---|---|
| `lakefs-gc.enabled` is `false` | The table is skipped completely — no expire, and its files are treated as protected by the sweep. |
| The metadata location is outside `<storage-namespace>/_iceberg/`, or the metadata file has no catalog registration (a table registered from another catalog) | **external-metadata**: skipped. The job never rewrites metadata it does not manage. |
| A **v1 table whose snapshots embed their manifest locations** (the deprecated `manifests` field) | The whole run **aborts** with `ErrEmbeddedManifests` *before anything is written*, because a rewrite would have to guess how every reader resolves the embedded manifests. |
| A property this job reads does not parse | That table is reported as failed and skipped; the rest of the run continues. |
| The pointer moved between planning and applying (a concurrent catalog commit) | That table is skipped and reported as `changed-concurrently`, rather than overwriting the new state. |

The phase is two-pass: the first pass resolves and plans every branch and every table
without writing, the second applies. That is what makes the v1 abort a real abort — no
repository is ever left half-expired because of a table the tool refuses to rewrite.

### Phase 2: mark and sweep

**Candidates.** The sweep's unit of work is a lakeFS entry under
`_lakefs_iceberg/files/<table-uuid>/…` — the catalog's own per-file registration
(one entry per metadata file, manifest list, manifest and data file a snapshot added). There
are three reasons for that:

- those entries are the only table files enumerable through the lakeFS API; listing raw
  objects under the storage namespace would need blockstore credentials, which this tool
  deliberately does not have;
- a file with no registration is a file the catalog never staged — an in-flight write — and
  "not a candidate" is the simplest way to guarantee it is never touched;
- removing a registration is exactly what makes an object collectable by lakeFS GC.

**What is protected.** A candidate is never marked if it is referenced by:

- a **branch head**: every table pointer visible on the branch (staged included) is
  resolved, and every file its metadata references is protected — the metadata file itself,
  its `metadata-log`, statistics and partition-statistics files, every snapshot's manifest
  list, every manifest of every list, and every file of every manifest entry. `EXISTING`
  and `DELETED` manifest entries are protected too: over-protecting costs storage,
  under-protecting costs data;
- a **tag**: tags are immutable, so the commit they name is retained forever; it is resolved
  exactly like a branch head;
- a **GC-retained commit**: for each branch, the job reproduces the lakeFS GC retention walk
  (`pkg/graveler/retention/active_commits.go`) from the API's commit log — the head, then
  each first parent while the commit itself is newer than the branch's retention period,
  ending at (and including) the first commit older than the threshold. If the repository has
  **no GC rules**, lakeFS GC never removes a commit, so every reachable commit is retained;
- a **staged change**: an uncommitted table pointer is resolved like a branch head, and every
  uncommitted file registration protects its object.

In addition, a file is never marked when

- its table's live set is **unknown** — its metadata, a manifest list or a manifest could
  not be read (missing registration, foreign path, parse error). Every registration of such
  a table is protected: the job never guesses which files such a table still needs;
- some reference could not be enumerated at all — then every candidate is protected;
- its newest registration is **younger than `--safety-age`**, or has no usable timestamp. The
  safety age covers the window between a client writing files for a snapshot and the catalog
  staging that snapshot's entries — the same window the catalog's uncommitted-GC grace closes
  on the server.

**The mark.** The decision is written to
`_lakefs/retention/iceberg/sweep/<run-id>/summary.json` plus a JSON-lines list of the marked
objects in `objects.jsonl`. The run ID *is* the mark ID.

**The sweep.** A sweep can run in the same invocation or later with
`--sweep-only --mark-id <run-id>`. Either way it **re-derives every reference** and re-applies
the safety age before deleting anything: the mark is a list of candidates to *reconsider*, not
a list of things to delete. A file that became referenced between mark and sweep is skipped;
so is one that became younger than the sweep's own safety age. For each still-unreferenced
object, **every** registration of it (on every branch where it is visible) is deleted; one
leftover registration would keep the object alive.

The sweep deletes **registrations, not objects**. After the operator commits the deletions,
lakeFS committed GC — which retains exactly the commits the mark protected — finds the
objects unreachable and reclaims them with the storage credentials it already holds. Until
then the objects remain on the storage, which is the safe direction if the commit is never
made or is reverted.

## Reports

```
_lakefs/retention/iceberg/expire/<run-id>/summary.json
_lakefs/retention/iceberg/sweep/<run-id>/summary.json      # mark report: run-id == mark-id
_lakefs/retention/iceberg/sweep/<run-id>/objects.jsonl     # mark: one marked object per line
_lakefs/retention/iceberg/sweep/<run-id>-sweep/summary.json # sweep of a run's own mark
```

A `--sweep-only` run has its own run ID and writes its summary under it, naming the mark ID
it swept. Reports are staged on the repository's default branch, so the operator's commit
publishes the cleanup and its record together, and a reverted run also reverts its report.

`summary.json` carries a schema `version` and a `kind` (`iceberg-expire`, `iceberg-sweep-mark`,
`iceberg-sweep`), the effective options, the per-branch/per-table outcome of an expire run
(status `expired`/`unchanged`/`skipped`/`failed`, the reason, the expired snapshot IDs and the
pointer swap), and the mark/sweep totals: registrations, candidates, marked objects, marked
bytes, and why the rest were protected. Reasons are stable strings (`referenced-by-tag`,
`younger-than-safety-age`, `table-unreadable`, …), so a `jq` query can group them.

```bash
# What was protected, and why?
lakectl fs cat lakefs://my-repo/main/_lakefs/retention/iceberg/sweep/<mark-id>/summary.json | jq '.totals, .warnings'
# What would a sweep delete?
lakectl fs cat lakefs://my-repo/main/_lakefs/retention/iceberg/sweep/<mark-id>/objects.jsonl | jq -r .address
```

## Recommended operational flow

```bash
# 1. Rehearse: no writes at all.
lakectl iceberg cleanup --repo my-repo --dry-run

# 2. Expire and mark, but delete nothing yet.
lakectl iceberg cleanup --repo my-repo --mark-only
#    → note the mark id from the output (or the sweep/<run-id> directory)

# 3. Read the mark, then commit the expire changes if they look right.
lakectl diff lakefs://my-repo/main
lakectl commit lakefs://my-repo/main -m "iceberg: expire snapshots (<run-id>)"

# 4. Sweep the mark in a later run, and commit the removals.
lakectl iceberg cleanup --repo my-repo --sweep-only --mark-id <run-id>
lakectl commit lakefs://my-repo/main -m "iceberg: sweep unreferenced files (<run-id>)"

# 5. Let lakeFS GC reclaim the objects that are now unreferenced.
lakectl gc prepare-async lakefs://my-repo
```

Steps 2–4 are separated on purpose: the mark is the reviewable artifact, and the sweep
re-validates it, so it is safe to run a sweep hours after its mark.

## Permissions

The job uses the ordinary lakeFS API and needs, on the repository:

- `fs:ReadObject` and `fs:ReadBranch` / `fs:ReadCommit` / `fs:ListTags` for the catalog
  area and the refs it resolves;
- `fs:WriteObject` and `fs:DeleteObject` on `_lakefs_iceberg/*` for the staged pointer, the
  file registration and the sweep's deletions;
- `fs:WriteObject` on `_lakefs/retention/iceberg/*` for the reports (and on the table's
  physical `_iceberg/…` metadata path for the expire upload, which the object API writes
  through the branch);
- `fs:ReadGCRules` for the retention walk.

The installation-wide `iceberg-reserved-prefix-protect` deny policy does **not** block the
job: it denies the *object API* on `_lakefs_iceberg/*/*`, and this tool writes there through
`StageObject`/`DeleteObject` on the catalog's own paths exactly like the catalog does — the
deny is on a different arity of resource. Granting `fs:WriteObject`/`fs:DeleteObject` on the
catalog area to the cleanup principal is the intended configuration; if your installation
also restricts `_lakefs/retention/`, grant that prefix to the reports principal.

## Safety summary

- Nothing is ever committed. Every change is staged and the operator commits it.
- A file is protected if **any** branch head, tag, GC-retained commit or staged change
  references it, if its table's live set cannot be read, if any reference enumeration failed,
  or if it is younger than the safety age. All four checks run again at sweep time.
- The sweep deletes only lakeFS registrations; the objects are reclaimed by lakeFS GC, whose
  own active-commit set is the same set the mark protects.
- A concurrent catalog commit is never overwritten: the pointer is re-read before it is
  staged, and a changed pointer makes the tool skip that table.
- A v1 table with embedded manifests aborts the run before anything is written.
- External-metadata tables are never rewritten and never swept.

## Limitations

- Only files the catalog registered under `_lakefs_iceberg/files/` are sweep candidates. A
  raw object that no catalog commit ever staged is invisible to the sweep (and is the
  client's in-flight write); lakeFS uncommitted GC remains responsible for it.
- A sweep deletes all registrations of a marked object, so re-running a sweep for an old mark
  after new branches registered the same object again will delete those registrations too. The
  re-validation only guarantees the object is *unreferenced*; it is never true that a
  marked file is still needed.
- The mark and sweep phases read every metadata document and every manifest of every
  retained table state. On a large repository with a long retention period this is the
  dominant cost; `--mark-only`/`--sweep-only` let an operator run it as a separate,
  long-running step. Metadata documents and manifest lists are cached by location, so a
  thousand commits that share one table version read it once.
- Expire is per branch: a table visible on several branches is expired once per branch,
  because each branch's staged state is its own. Merge the branches afterwards if the
  branches are intended to converge.
- The report object list is a single JSON-lines object; it is written with one upload, so a
  repository marking millions of files produces a report of that size. Split runs by safety
  age (`--safety-age`) if that becomes a problem.

## Verifying a change to this tool

The unit tests run entirely in memory (`pkg/icebergcatalog/cleanup/fake_test.go` is a
small in-memory lakeFS, including real Avro manifests and manifest lists written with the
pinned `iceberg-go` writers), so they need no server and no storage:

```bash
GOTOOLCHAIN=auto go test -count=1 -race ./pkg/icebergcatalog/cleanup/
GOTOOLCHAIN=auto go vet ./pkg/icebergcatalog/cleanup/ ./cmd/lakectl/cmd/
GOTOOLCHAIN=auto go tool golangci-lint run ./pkg/icebergcatalog/cleanup/... ./cmd/lakectl/cmd/...
```

This tool adds **no new blockstore code path**: the expire upload, the pointer staging and
the sweep's deletions all go through the lakeFS object API, so the S3 behaviour they depend
on is the server's own, covered by the catalog write-path harness on a real S3-compatible
blockstore (SeaweedFS in docker, `esti/iceberg_utils.go`, commit `057bfb935`). An end-to-end
run on S3 is the same recipe with the cleanup invoked from `lakectl` against the running
server; nothing on the client side talks to S3 directly.

## License and provenance

The package is a clean-room implementation written from the Apache Iceberg table spec
(Apache-2.0, <https://iceberg.apache.org/spec/>), the Apache Iceberg REST OpenAPI spec
(Apache-2.0), the public lakeFS documentation, `ICEBERG_CATALOG_PLAN.md` and the GC design
documents under `design/accepted/`. It contains no code from lakeFS Enterprise and none from
upstream lakeFS ≥ 1.87 (BUSL 1.1). The Iceberg metadata model comes from the pinned
`github.com/apache/iceberg-go` dependency, the same model `pkg/icebergcatalog` uses.
