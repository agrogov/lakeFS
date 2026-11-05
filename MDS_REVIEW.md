# Metadata Search (MDS) review

Branch reviewed: `feature/metadata-search` (checked out detached in this worktree), HEAD
`3e47f825b3140494cdf7c4ac04408d62001988b7` — *review/mds: indexer pipeline (per-branch tick, change
sets, staging recovery)*.

This file consolidates the five review cards that were committed on top of it, oldest first: `7e33c8a`
(table and refmap), `28d41a2` (indexer fake vs lakeFS, esti coverage), `01e3c46` (indexer service, repo
setup, maintenance), `24f5e87` (server security), `3e47f82` (indexer pipeline). The five cards are
deleted; their findings are kept verbatim below, grouped by severity and deduplicated.

## Verdict

No card reported a BLOCKER, and the 34 findings raised across the five cards reduce to 31 once three
duplicate keys are merged: **0 BLOCKER, 7 MAJOR, 14 MINOR, 10 NIT**. Every card-level MAJOR (8 keys, 7
after deduplication) was re-checked against the code during this consolidation and every one is
**CONFIRMED**; there is no DISPUTED finding and none was dropped. The MAJORs sit in four places: the
indexer tick's unbounded read pattern and its two untimed HTTP clients (IX-1, IX-2, IO-1), the
compaction/rebuild interaction with position delete files (TB-1), the trust boundary around `R-metadata`
on the signing and policy sides (MDS-SEC-1, MDS-SEC-2), and one missing recovery branch when the recorded
position disappears (TS-1). Crash and concurrency behaviour is otherwise well covered — the §4.4 table,
the bounded 409 retries, staging recovery, and the real-lakeFS probes that back every claim. The
remaining debt is about bounds (memory, request count, timeouts) and about test coverage of the paths the
fakes synthesise (paged `pending`, compaction through the indexer, the esti restart window, the engine
scripts). These are review findings only: none of the five cards, and not this consolidation, changes any
product code, test, design or doc.

## Summary of findings

| Key | Severity | Area | Title | Where |
|---|---|---|---|---|
| IX-1 | MAJOR | Indexer pipeline | `pending` reads and retains the whole remaining first-parent log on every branch tick | `pkg/mds/indexer/branch.go:418-451` |
| IX-2 | MAJOR | Indexer pipeline | No timeout on either client: one wedged connection stalls the tick loop while `/_health` stays 200 | `pkg/mds/indexer/iceberg.go:94-103`; `pkg/mds/indexer/indexer.go:613` |
| IO-1 | MAJOR | Indexer service | A second initialisation races into a duplicate `mds: init` commit that is refused as a foreign repository forever | `pkg/mds/indexer/repo.go:201-235` |
| TB-1 | MAJOR | Table/refmap | Position delete files orphaned by a rebuild are never removed, so the compaction trigger can latch on forever | `pkg/mds/table/writer.go:291-338` |
| MDS-SEC-1 | MAJOR | Server security | Remote signing hands out a write signature under `R-metadata`'s table location for the virtual table | `pkg/icebergcatalog/sign.go:345-392` |
| MDS-SEC-2 | MAJOR | Server security | The `*-metadata` class of the §8 policies matches data-repository ARNs, not only companion repositories | `pkg/mds/bootstrap/bootstrap.go:102-118` |
| TS-1 | MAJOR | Tests/esti | A recorded position that no longer exists fails the branch every tick forever; §4.7's rebuild fallback is not taken | `pkg/mds/indexer/branch.go:87-97` |
| IX-3 | MINOR | Indexer pipeline | The per-commit table reload builds a new REST catalog, costing an extra config round trip and a connection pool per commit | `pkg/mds/indexer/iceberg.go:50-70` |
| IX-4 | MINOR | Indexer pipeline | The "crashed during the iceberg commit" recovery case is not tested | `pkg/mds/indexer/branch_test.go:377-388` |
| IX-5 | MINOR | Indexer pipeline | No test pages the `pending` walk, so the lag and multi-page stop-at logic is unverified | `pkg/mds/indexer/branch_test.go` (no case sets `fake.pageSize`) |
| IO-3 | MINOR | Indexer service | With `delete_branches: false` a deleted branch keeps its `/_health` entry and lag gauge, so `/_health` stays 503 | `pkg/mds/indexer/indexer.go:484-490` |
| IO-4 | MINOR | Indexer service | A branch that fails before the pending list is built reports `lag_commits: 0` and loses `last_indexed` | `pkg/mds/indexer/indexer.go:494-511` |
| TB-2 | MINOR | Table/refmap | Compaction writes rows in scan order, so the declared `path ASC` sort order no longer describes its files | `pkg/mds/table/writer.go:287-307` |
| MDS-SEC-3 | MINOR | Server security | The catalog serves any repository named `R-metadata` as `R`'s index, while the indexer refuses such a repository | `pkg/icebergcatalog/resolver.go:297-320` |
| MDS-SEC-4 | MINOR | Server security | `iceberg.metadata_search.enabled` without `iceberg.enabled` is accepted, half-applied, and answers 501 | `pkg/icebergcatalog/config.go:196-209` |
| TS-3 | MINOR | Tests/esti | The fake's write methods are more lenient than lakeFS on a missing branch, and its one modelled refusal carries the wrong status | `pkg/mds/indexer/fake_test.go:579-620` |
| TS-4 | MINOR | Tests/esti | Nothing checks that the companion repository is writable; `ReadOnly` is dead and a read-only `R-metadata` fails every tick | `pkg/mds/client/lakefs.go:119` |
| TS-5 | MINOR | Tests/esti | Startup verifies the initialisation commit by walking the entire first-parent history into memory | `pkg/mds/indexer/repo.go:152-185` |
| TS-6 | MINOR | Tests/esti | The engine scripts only assert that *some* `orders/` row came back | `esti/ops/mds-engines/run-spark.sh:72-73` |
| TS-7 | MINOR | Tests/esti | No end-to-end test of §4.9 compaction and tag retention, or of the §4.8 branch-deletion mirror | `esti/mds_test.go:864-879` |
| TS-8 | MINOR | Tests/esti | The esti "indexer restart" case is opportunistic and still passes when the kill lands between ticks | `esti/mds_test.go:476-497` |
| IX-6 | NIT | Indexer pipeline | Five error messages print `branch@repository` where every other message prints `repository@branch` | `pkg/mds/indexer/branch.go:65` |
| IX-7 | NIT | Indexer pipeline | `Table.SnapshotCount` is documented as the `every_commits` counter, which it is not | `pkg/mds/indexer/tables.go:25-27` |
| IO-5 | NIT | Indexer service | `/_health` keeps the previous error while a repo is in progress, and a cancelled tick leaves `in_progress` stuck | `pkg/mds/health.go:103-120` |
| IO-6 | NIT | Indexer service | `compaction.*` is neither validated nor disableable: an explicit `0` is silently turned into the default | `pkg/mds/config.go:268-303` |
| IO-7 | NIT | Indexer service | Dead code and one stale doc comment around the health/metrics/stage seams | `pkg/mds/metrics.go:38-46` |
| TB-4 | NIT | Table/refmap | `Compact` accepts `…Option` and discards all of them | `pkg/mds/table/writer.go:280` |
| TB-5 | NIT | Table/refmap | Nothing at compile time or in unit tests proves `*catalog.Catalog` still satisfies the ref mapper's `refResolver` | `pkg/mds/refmap/refmap.go:47-50` |
| MDS-SEC-5 | NIT | Server security | The deny does not cover `fs:UpdateRepository`, which writes a companion repository | `pkg/mds/bootstrap/bootstrap.go:140-148` |
| TS-9 | NIT | Tests/esti | The esti reads are full scans, so the filtered-scan examples of the guide are not what the suite verifies | `esti/mds_test.go:2241` |
| TS-10 | NIT | Tests/esti | The fake's merge base is "any common ancestor", not graveler's best common ancestor, plus two smaller divergences | `pkg/mds/indexer/fake_test.go:479-512` |

Duplicates merged: IO-2 → IX-1, TB-3 → IX-2, TS-2 → IX-1 (its paging-test-gap half is IX-5). The
duplicate keys are also listed under the retained finding. IO-7 overlaps IX-7 and TS-4 but is kept as
its own entry because the rest of it is distinct.

All MAJORs were verified against the code: 7 CONFIRMED, 0 DISPUTED (see the verification line in each
MAJOR entry). No BLOCKER was reported.

## Verified

Merged from the five cards; each row lists the cards that ran the command.

| Command | Cards | Result |
|---|---|---|
| `go build ./...` | all five | pass — exit 0, no output (table/refmap reports 14.9 s under Go 1.25.8 with `GOTOOLCHAIN=auto`) |
| `go vet ./pkg/mds/...` | IX, IO, TS | pass, no output |
| `go vet ./pkg/mds/table/... ./pkg/mds/refmap/...` | TB | pass (`vet exit=0`) |
| `go vet ./pkg/mds/... ./cmd/lakefs-sso/...` | IO | pass, no output |
| `go vet ./pkg/icebergcatalog/... ./pkg/mds/client/... ./pkg/mds/bootstrap/... ./pkg/api/ ./pkg/config/ ./cmd/lakefs-sso/...` | SEC | pass, no output |
| `go test -count=1 -race -timeout 10m ./pkg/mds/...` | IO, SEC, TS | all `ok` — `pkg/mds`, `bootstrap`, `client`, `indexer`, `refmap`, `table` (IO reports 3.262 s / 4.315 s / 1.466 s / 8.397 s / 5.186 s / 9.728 s) |
| `go test -count=1 -race -timeout 10m ./pkg/mds/indexer/` | IX | `ok github.com/treeverse/lakefs/pkg/mds/indexer 3.806s` |
| `go test -count=1 -race -timeout 10m ./pkg/mds/table/... ./pkg/mds/refmap/...` | TB | `ok pkg/mds/table 7.317s`, `ok pkg/mds/refmap 2.748s` |
| `go test -count=1 -race -timeout 10m ./pkg/icebergcatalog/ ./pkg/mds/client/ ./pkg/mds/bootstrap/ ./pkg/api/ ./pkg/config/` | SEC | all `ok` (icebergcatalog 28.0 s, mds/client 1.5 s, mds/bootstrap 3.8 s, api 42.3 s, config 2.3 s) |
| `go test -count=1 -race -timeout 10m ./cmd/lakefs-sso/...` | IO | `no test files` |
| `go tool golangci-lint run` on each card's target set | all five | `0 issues.` on every set (`./pkg/mds/...`, `./pkg/mds/indexer/...`, `./pkg/mds/table/... ./pkg/mds/refmap/...`, `./pkg/icebergcatalog/... ./pkg/mds/client/... ./pkg/mds/bootstrap/... ./pkg/api/ ./pkg/config/ ./cmd/lakefs-sso/...`, `./pkg/mds/... ./esti/...`) |
| Probe — indexer pipeline (5 probes: crash after step 3 on real lakeFS, foreign staged write at the recorded position, concurrent replay, `max_commits=1` with 40 pending commits, a server that never answers) | IX | all as predicted: recovery completes steps 4–5 with no re-apply and no reset and clean staging; foreign staging discarded, no foreign row survives; the second commit gets `CommitFailedException` (409) with no duplicate rows; 23 log calls / 45 commits read per tick, twice; a tick is still in progress after 3 s |
| Probe — indexer service (4 probes: duplicate `mds: init`, pending pages, deleted branch with `delete_branches: false`, failed branch lag) | IO | all as predicted; outputs quoted in IO-1…IO-4 |
| Probe — table/refmap (4 probes: orphan delete files, orphans accumulate, compaction sort order, concurrent same delta) | TB | all as predicted; outputs quoted in TB-1, TB-2 and in "Checked, no finding" |
| Probe — server security (3 probes: sign a write on the virtual table, vended-credential contrast, `*-metadata` wildcard reach) | SEC | all as predicted; outputs quoted in MDS-SEC-1 and MDS-SEC-2 |
| Probe — tests (`pkg/mds/indexer/probe_review_test.go`, `go test -count=1 -run TestProbe -v ./pkg/mds/indexer/`) | TS | `ok 1.557s`; output quoted in TS-1, TS-2, TS-3 and TS-5 |
| Esti environment check (`curl --max-time 3 http://127.0.0.1:8333/`, `docker ps`) | TS | connection refused and no containers: the esti suite was **not run** (it also needs a reachable docker daemon for the PyIceberg/DuckDB client image) |

Every probe file was temporary and deleted before its card's commit; no card left a product or test
change behind, and `git status` was clean when each card was added.

## MAJOR

Every entry here was re-checked against the code and is CONFIRMED.

### IX-1 - MAJOR: `pending` reads and retains the whole remaining first-parent log on every branch tick

**Duplicates:** IO-2 (indexer service, MAJOR — same walk, same lines, same claim; this entry is kept) and TS-2 (tests, MINOR — same defect rated lower; its paging-test-gap half is IX-5).

**Verification: CONFIRMED** — `pending` (branch.go:418-457) appends every page of the log, reverses and truncates only after the walk, and takes `total := len(commits)` after the full walk, so the tick holds the whole backlog and re-reads it every time; IO-2 cites the same lines.

**Where** `pkg/mds/indexer/branch.go:418-451` (`pending`), called from `branch.go:111`.

**What happens** `pending` walks `logCommits(R, head, stop_at=last)` page by page, appends **every** commit to
`commits`, and only after the walk reverses the slice and truncates it to `max_commits`. The walk runs to `last`
unconditionally, because `total` (the lag of §9) is `len(commits)`. So the memory of one branch tick is
`O(lag)`, and each of the `lag / max_commits` ticks needed to catch up re-reads the whole remaining log: total
IO `O(lag² / page)`. Concretely: an indexer down for a week on an auto-commit repository with 500 000 commits
behind holds ~500 000 `client.Commit` values (ID, message, `Parents`, `Metadata` map, so a few hundred MB)
per tick, and needs 500 ticks × ~500 pages = 250 000 HTTP requests to catch up. `max_commits` bounds the
applied work (§4.2 step 5) but not this.

**Evidence** probe 4 (`max_commits=1`, `pageSize=2`, 40 pending commits):

```
max_commits=1, lag=40, page=2: LogCommits calls=23, commits read=45, applied=1
second tick: LogCommits calls=23, commits read=45, applied=1, lag reported=38
```

45 commits read to apply 1, twice. The same shape appears in `backfillStart` (`branch.go:259-293`) and
`seedTag` (`branch.go:218-250`): a `since` far in the past, or `seed_lookback` at its default 1000, walks and
holds the history (plus one `GetTag` per commit in `seedTag`).

**Suggested fix** Count the pages without retaining them: keep only the newest `limit+1` commits while walking
and increment `total` per page (the log is walked newest-first, so the first `limit+1` commits are exactly
`todo`), or walk twice — once bounded for `todo`, once counting pages for `lag`. Test that would fail first:
`max_commits=1`, `pageSize=2`, 40 pending commits, assert `LogCommits` is called once per tick (2 calls total
for two ticks), not 23.

### IX-2 - MAJOR: no timeout on either client, so one wedged connection stalls the tick loop forever while `/_health` stays 200

**Duplicate:** TB-3 (table/refmap, MINOR — the same missing timeout, seen from the writer's catalog; this entry is kept).

**Verification: CONFIRMED** — `newLakeFSClient` (indexer.go:613-622) passes no `WithHTTPClient`, so apigen falls back to `http.DefaultClient` (no `Timeout`); `IcebergStore.catalog` (iceberg.go:94-103) builds `table.NewCatalog` with no transport or timeout, and the tick runs on the process context (service.go:186-197). `grep -rn Timeout pkg/mds` finds only the health server's read-header and shutdown bounds.

**Where** `pkg/mds/indexer/iceberg.go:94-103` (`IcebergStore.catalog` → `table.NewCatalog`, `pkg/mds/table/catalog.go:136`) and `pkg/mds/indexer/indexer.go:612-616` (`newLakeFSClient`).

**What happens** The lakeFS API client is built over `http.DefaultClient`
(`pkg/api/apigen/lakefs.gen.go:2805-2807`), which has no `Timeout`; iceberg-go builds
`&http.Client{Transport: session}` with no `Timeout` either
(`github.com/apache/iceberg-go@v0.7.0/catalog/rest/rest.go:1074`). The only bound is the tick's context, which
is the process context. A connection that neither answers nor resets (wedged load balancer, dropped NAT entry,
blackholed replica) therefore blocks one branch tick forever; `Service.tickUntilDone` waits for that tick before
starting the next one (`pkg/mds/service.go:186-192`), so **all** repositories stop being indexed, and
`Health.Healthy` is still true because the tick is "in progress" (`pkg/mds/health.go:65-67`, §9). The indexer
looks healthy and silently stops indexing until the process is restarted by hand.

**Evidence** probe 5: an `httptest` server that accepts the request and never answers, an `Indexer` built by
`indexer.New` (the production client) and one `Tick`:

```
after 3s against a server that never answers the tick is still in progress: neither client has a timeout
```

**Suggested fix** Give both clients an `http.Client` with a `Timeout` (say 60s) — `apigen.WithHTTPClient(...)`
for the API and a `rest.WithHTTPClient`/context deadline for the catalog — or derive a per-tick context with a
deadline from `period` inside `Indexer.Tick` and report the stall as a branch error. Test that would fail first:
the probe above, asserting the tick returns (with the repository marked failed) within the timeout.

### IO-1 - MAJOR: a second initialisation races into a duplicate `mds: init` commit that is refused as a foreign repository forever

**Verification: CONFIRMED** — `initialise` commits with `AllowEmpty: true` (repo.go:224), and `initCommitAccepted` (repo.go:184-191) accepts the marker only at `len-1`, or at `len-2` above an automatic root, so a second `mds: init` on top of the first (`[init2 init1 root]`, index 0) is refused as `ErrForeignRepository` on every later tick.

**Where** `pkg/mds/indexer/repo.go:201-235` (`initialise`, `AllowEmpty: true` at 224),
`pkg/mds/indexer/repo.go:152-191` (`verifyInitCommit`, `initCommitAccepted`), design
`METADATA_SEARCH_DESIGN.md` §4.8 ("the initialisation commit ... is the **first commit after** that
root") and §4.1 ("Correctness under accidental concurrency comes from the Iceberg CAS and the
idempotency check (4.4), not from a lease").

**What happens** Two indexer processes that meet on a freshly created companion repository both read
"R-metadata@main has only the automatic `Repository created` root", so both call `initialise`
(`repo.go:170-177`) and both commit. `AllowEmpty: true` (`repo.go:224`) guarantees the second one
succeeds: real lakeFS makes an empty commit legal under that flag
(`pkg/catalog/catalog.go:1383` → `pkg/graveler/graveler.go:2355`, `ErrNoChanges` swallowed when
`AllowEmpty` is set), so the loser commits a *second* `mds: init` commit on top of the winner's.
The next `verifyInitCommit` reads the history newest first, finds the marker at index 0 of
`[init2, init1, root]`, and `initCommitAccepted` (`repo.go:184-191`) accepts the marker only at the
root or at the first commit above the automatic root — index 0 of three is neither. The companion
repository is then refused with `ErrForeignRepository` on every tick of every future process, and
nothing in the indexer repairs it; an operator must delete `R-metadata` and lose the mirror.

The justification in the comment at `repo.go:221-223` ("keeps a retried initialisation from failing
when the creation was already committed by a previous attempt") does not hold: a previous attempt
that committed leaves the marker on the branch head, `verifyInitCommit` accepts it (`repo.go:161-169`:
the marker at index `0` of `[init1, root]` *is* `len-2`), and `initialise` is never entered again. The flag only
enables the duplicate.

A rolling restart of a single-replica Deployment (`maxSurge: 1`) is enough: two processes overlap for
a few seconds and the very first initialisation is exactly the one they race on.

**Evidence** probe `TestProbeDuplicateInitCommitBreaksTheRepository` runs one tick with a client that
lets a competing replica commit the same `mds: init` first, i.e. the interleaving of the two
processes above:

```
probe_review_test.go:76: first tick repository error: <nil>
probe_review_test.go:77: init commits of repo-metadata: [index-3 index-2]
probe_review_test.go:80: head index-4 message "mds: index repo@s1"
repo.go:175  msg="mds: completing the initialisation of a newly created metadata repository"
repo.go:233  msg="mds: initialised the metadata repository" commit=index-3
tickRepository error="the companion repository was not created by the indexer: \"repo-metadata\" carries lakefs.mds.init on a commit that is neither its root nor its first commit after \"Repository created\""
probe_review_test.go:86: second (fresh) indexer repository error: the companion repository was not created by the indexer: ...
```

The first tick reports no error at all — the damage is only visible to the *next* process. There is
no test of this window: `repo_test.go` covers `TestInterruptedInitialisationIsCompleted` and
`TestInitialiseOnTheAutomaticCreationCommit`, both single-process.

**Suggested fix** Make the commit the idempotency point: initialise with `AllowEmpty: false` (an
initialisation with nothing staged *is* an already-initialised repository), and accept a contiguous
run of `mds: init` commits above the automatic root in `initCommitAccepted`, so the loser's commit
is harmless and the next tick proceeds at the head. The test that fails before the fix: two
initialisations that both commit, then a fresh indexer must not answer `ErrForeignRepository` (add
it next to `TestInterruptedInitialisationIsCompleted`).

### TB-1 - MAJOR: position delete files orphaned by a rebuild are never removed, so the compaction trigger can latch on forever

**Verification: CONFIRMED** — `Compact` removes only the delete files carried by the *current* scan tasks (writer.go:291, 322-338), and `Rebuild` calls `txn.Overwrite` with no filter (writer.go:241), which in iceberg-go drives `performCopyOnWriteDeletion` and deletes/rewrites data files only (table/transaction.go:2466-2479); the trigger compares the snapshot summary's `total-delete-files` (maintenance.go:53-55, iceberg.go:129-139), so orphaned files keep being counted and `due` never clears.

**Where.** `pkg/mds/table/writer.go:291` (`dataFiles, deleteFiles := filesOf(tasks)`), `writer.go:301`
(`txn.ReplaceFiles(ctx, dataFiles, nil, deleteFiles, …)`), `writer.go:322-338` (`filesOf` walks
`task.DeleteFiles` / `task.EqualityDeleteFiles` of the scan tasks); the orphan source is
`pkg/mds/table/writer.go:241` (`Rebuild` → `txn.Overwrite` with no filter, which rewrites the data files and
leaves the delete files in the snapshot — the property `Compact`'s own doc comment states at
`writer.go:258-262`); the trigger that latches is `pkg/mds/indexer/maintenance.go:53-55`
(`deleteFiles := tbl.DeleteFiles(); due := … deleteFiles > x.compaction.MaxDeleteFiles`) reading
`pkg/mds/indexer/iceberg.go:129-139`, i.e. the snapshot summary `total-delete-files`.

**What happens.** Compaction removes a delete file only if some *current* scan task carries it
(`writer.go:327-336`). A position delete file whose data files were replaced by `Rebuild` is carried by no task, so
`ReplaceFiles` never names it, the file stays in the manifest list, and the snapshot summary keeps counting it.
Concretely: a branch has 2 delete files → a large commit (or a branch seeded from an `mds-<A>` tag, §4.5 step 1)
runs `Rebuild`, orphaning both → every later compaction leaves both, so the summary stays `2` no matter how often
it runs. Two consequences:

1. Leak: the orphaned files are never reclaimed. `lakectl iceberg cleanup` cannot reclaim them either — they are
   referenced by the live snapshot. Each further `Rebuild` adds the delete files attached at that moment to the
   permanent set, so the count is monotonically non-decreasing across the branch's life.
2. Latch: the indexer's delete-file trigger compares that summary number, so once the orphan count alone exceeds
   `compaction.max_delete_files` (default 200) `due` is true on every tick and never becomes false: a full table
   rewrite plus a `mds: compact <R>@<C>` lakeFS commit (`maintenance.go:70-78`) every `period` (default 60 s),
   forever. Reaching `orphans > 200` needs one rebuild while more than 200 delete files are attached — reachable
   in one tick, because a tick applies up to `max_commits` (default 1000) commits before `compactIfNeeded` runs,
   and one commit deleting objects spread over 200 data files writes 200 delete files in that commit alone
   (iceberg-go `performMergeOnReadDeletion` → `writePositionDeletesForFiles`, one file per matched data file);
   it is also reachable after one failed/skipped compaction, or by lowering `max_delete_files` under the
   accumulated orphan count.

**Evidence.**
```
$ go test -count=1 -run TestProbeOrphansAccumulate -v ./pkg/mds/table/
    zz_probe3_test.go:33: after two MOR deletes: del=2 data=1
    zz_probe3_test.go:40: after rebuild: del=2 data=1
    zz_probe3_test.go:46: after compact #1: del=2 data=1
    zz_probe3_test.go:46: after compact #2: del=2 data=1
    zz_probe3_test.go:46: after compact #3: del=2 data=1
```
and the single-orphan case (`TestProbeOrphanDeleteFiles`): `after mor delta: del=1 pos=2 data=2`, `after
rebuild: del=1 pos=2 data=1`, `after compact: del=1 pos=2 data=1`, `after second compact: del=1 pos=2 data=1`.
Rows are correct throughout, so this is waste/commit growth, not corruption. `TestCompact`
(`pkg/mds/table/writer_test.go:281-318`) asserts `total-delete-files == 0` but only on a table whose delete files
are still attached (`writer_test.go:304`), which is why the suite is green; `TestRebuild`
(`writer_test.go:228-240`) rebuilds a table that has delete files and asserts only rows and the source commit.

**Suggested fix.** Enumerate delete files to remove from the snapshot's manifest list, not from the scan tasks
(e.g. `tbl.Inspect().AllDeleteFiles(ctx)`, iceberg-go `table/inspect_files.go:72`, or
`GetAllManifests` filtered to `EntryContentDeleteFile`), and let `Rebuild` do the same. The test that fails before
the fix: extend `TestRebuild`'s delete-file case with `table.Compact` and require `total-delete-files == 0`.

### MDS-SEC-1 - MAJOR: remote signing hands out a **write** signature under `R-metadata`'s table location for the virtual table

**Verification: CONFIRMED** — `sign.go` contains no `RequireWritableNamespace`/`writableNamespace` call at all: it authorizes `OpWriteTableData` on the requested virtual address (sign.go:345-358), then scopes the signature to the resolved address's table location (sign.go:372-392), while `creds.go:452` (`if isBranch && s.writableNamespace(addr)`) is exactly the guard it lacks.

**Where**: `pkg/icebergcatalog/sign.go:345-358` (authorize `OpWriteTableData` on the requested address),
`pkg/icebergcatalog/sign.go:372-377` and `:297` (`tableLocation` resolves the *target*, i.e.
`R-metadata`'s table), `pkg/icebergcatalog/sign.go:392` (`contains` on the target's location);
compare `pkg/icebergcatalog/creds.go:452-460` and `:515-517` (`TableCredentials` /
`writableNamespace`), and `pkg/icebergcatalog/resolver.go:491-502` (`RequireWritableNamespace`).

**What happens**: a caller sends `POST /iceberg/api/v1/lakefs/namespaces/R<sep>main<sep>system/tables/object_metadata/sign`
with `{"method":"PUT","uri":"s3://<R-metadata ns>/_iceberg/<uuid>/metadata/00001.metadata.json"}` (or any
`data/` file of that table). The handler classifies PUT as `OpWriteTableData`, authorizes it **on `R`**
(`catalog:WriteTableData` on `arn:lakefs:catalog:::table/R/system/object_metadata` + `fs:ReadRepository`
on `R` + `fs:WriteObject` on `R/_lakefs_iceberg/*` — nothing on `R-metadata`), then resolves the address
through the Metadata Search resolver, which answers `R-metadata.<branch>.system.object_metadata`, and
signs the PUT with the server's own blockstore credentials, scoped to `R-metadata`'s table location. The
caller thus gets a bearer capability to overwrite the metadata-search index of `R`: `metadata.json`
(read by every user who loads `R.<ref>.system.object_metadata`), manifests and data files — the exact
files `mds-metadata-repo-protect` (§8) denies them through the fs: and catalog APIs and that §5.4 makes
read-only. `creds.go` guards precisely this case for vended sessions ("a caller who may write the data
repository must not get credentials that write the companion's files",
`pkg/icebergcatalog/creds.go:449-451`), and `sign.go` has no equivalent check.

**Evidence**:
- `sign.go:346-358` authorizes with `Op: OpWriteTableData, Address: addr` where `addr` is the *requested*
  virtual address; `sign.go:372-377` then resolves it and `sign.go:392` accepts any URI under the
  *resolved* (`R-metadata`) table location.
- `creds.go:453` `if isBranch && s.writableNamespace(addr)` is the guard the sign path lacks;
  `creds.go:516` is `s.store.RequireWritableNamespace(addr) == nil`.
- Probe (temporary test in `package icebergcatalog`, built on `newResolverFixture`, deleted before
  commit): `probe-writer` holds `catalog:*` on `arn:lakefs:catalog:::*`, `fs:ReadRepository` on
  `probe-data` and `fs:WriteObject` on `probe-data/_lakefs_iceberg/*` only — nothing on
  `probe-data-metadata`:
  ```
  RequireWritableNamespace(virtual) = namespace "system" of probe-data@main is reserved for catalog-managed metadata: it is read-only for clients
  virtual sign status=200 body={"uri":"https://signed.example/object?X-Amz-Signature=test","headers":{}}
  virtual sign presigner calls=[{identifier:s3://probe-bucket/meta/_iceberg/7777.../data/00000-...-0.parquet mode:1}]
  writableNamespace(virtual)=false
  ```
  (`mode:1` is `block.PreSignModeWrite`.) `TestMetadataSearchDataPlaneRedirect`
  (`pkg/icebergcatalog/resolver_test.go:767`) pins only the READ side of the redirection, so no existing
  test fails.

**Suggested fix**: apply the `creds.go` rule to signing: for `OpWriteTableData`, refuse with
`*ReservedNamespaceError` (403) when `s.store.RequireWritableNamespace(addr)` — or the *resolved*
target's namespace, so a redirect cannot dodge it — is non-nil, and add a test that a `PUT` sign on
`R.<branch>.system.object_metadata` is 403 while a `GET` sign on the same address still succeeds (and that
the indexer's own `PUT` on `R-metadata.system.object_metadata` still succeeds).

### MDS-SEC-2 - MAJOR: the `*-metadata` class of the §8 policies matches data-repository ARNs, not only companion repositories

**Verification: CONFIRMED** — the patterns (bootstrap.go:102-118) lean on `*` crossing `/`, which `pkg/auth/wildcard.Match` does (it matches to the end of the string, see the "Trailing * matches rest of string" arm), so `repository/*-metadata` matches `repository/analytics/object/-metadata` and `repository/*-metadata/*` matches `.../object/foo-metadata/data.yml`; the bootstrap comment states the same property.

**Where**: `pkg/mds/bootstrap/bootstrap.go:96-110` (`MetadataRepositoryPattern`,
`MetadataRepositorySubtreePattern`, `MetadataCatalogPattern`), used by `IndexerPolicy`
(`:182-217`) and `MetadataRepoProtectPolicy` (`:231-253`).

**What happens**: lakeFS evaluates a policy resource with `wildcard.Match`
(`pkg/auth/wildcard/match.go:16`, a full-string `path.Match` where `*` spans `/`) against the exact
request ARN (`pkg/permissions/permission.go:30-76`). `arn:lakefs:fs:::repository/*-metadata` is therefore
matched by `arn:lakefs:fs:::repository/analytics/object/-metadata` (star = `analytics/object/`) and
`arn:lakefs:fs:::repository/*-metadata/*` by `arn:lakefs:fs:::repository/analytics/object/foo-metadata/data.yml`,
and `arn:lakefs:catalog:::*/*-metadata/*` by `arn:lakefs:catalog:::table/analytics/foo-metadata/orders`.
Consequences, both proven against a real ACL service:
1. the **indexer** — which §8 restricts to reads on data repositories, "the indexer never writes a data
   repository, which is what keeps a bug in it from corrupting the data it is describing"
   (`pkg/mds/bootstrap/bootstrap.go:122-124`) — is granted `fs:WriteObject`, `fs:DeleteObject` and `catalog:DeleteTable` on
   data-repository objects/tables whose key or namespace level ends in `-metadata`;
2. the **deny** silently 403s a guard-group member who writes such keys in their *own* data repository
   (the deny is the only reason the same ACL probe says `false` where the operator's own `fs:*` allow
   holds), i.e. an ordinary `foo-metadata/data.yml` object in a data repository becomes undeletable by
   admins/developers.

**Evidence**: probe (temporary test in `package bootstrap`, ACL service, deleted before commit) after
`EnsurePrincipals`:
```
indexer delete object '-metadata' in a DATA repo                       allowed=true
indexer delete object '-metadata/x' in a DATA repo                     allowed=true
indexer write object 'foo-metadata/data.yml' in a DATA repo            allowed=true
indexer delete a table under a '-metadata' namespace in a DATA repo    allowed=true
operator (guard group) delete object '-metadata' in a DATA repo        allowed=false
operator (guard group) write 'foo-metadata/data.yml' in a DATA repo    allowed=false
operator (guard group) read the same object                            allowed=true
```
`TestEnsurePrincipalsAuthorization` (`pkg/mds/bootstrap/bootstrap_test.go:196`) tests only
`ObjectArn(dataRepo, "a.parquet")`, so the existing suite cannot see this.

**Suggested fix**: anchor each pattern to the ARN structure instead of relying on `*` crossing `/`:
`arn:lakefs:fs:::repository/*-metadata` for the repository itself is fine only if matching is
segment-aware, so use the shapes the other policies use — e.g. deny/allow on
`arn:lakefs:fs:::repository/*-metadata/{branch,tag,object}/*` spelled as three statements (branch, tag,
object) and `arn:lakefs:catalog:::{namespace,table,view}/*-metadata/*` spelled as three statements — and
pin each with a test asserting that `ObjectArn("analytics","-metadata")` and
`ObjectArn("analytics","foo-metadata/data.yml")` are **not** allowed for `mds-indexer` and **are**
allowed for a guard group, which fails today.

### TS-1 - MAJOR: a recorded position that no longer exists fails the branch every tick forever; §4.7's rebuild fallback is not taken

**Verification: CONFIRMED** — the `FindMergeBase` switch (branch.go:87-97) routes only `client.ErrNoMergeBase` to `rewrite` and returns every other error, and the client maps the server's 404 on the missing position to `ErrNotFound` (lakefs.go:481-483), so a collected or re-created position fails the branch on every tick with `rewrite` never reached.

**Where** `pkg/mds/indexer/branch.go:87-97` (the `FindMergeBase` switch), with the fallback it should
reach at `pkg/mds/indexer/branch.go:466-540` (`rewrite`).

**What happens** The position `last` of a branch is a commit of the *data* repository, stored in the
head commit of `R-metadata@b` and protected there only by the mirror's own `mds-<last>` tag — nothing
keeps the source commit of `R` alive. Two ordinary operations make `last` disappear from `R`:
committed GC after a hard reset/force-move of `R@b` while the indexer was down (the position is
unreachable from every branch, so after the retention period the commit is collected), and deleting
`R` and re-creating it under the same name (design §4.8: "repository deleted | log, skip. `R-metadata`
is left for the admin", and the re-creation is not covered at all). Then
`x.lakeFS.FindMergeBase(ctx, repository, last, head.ID)` dereferences `last`
(`pkg/catalog/catalog.go:1488-1497` does the same for `stop_at`), graveler answers
`ErrBranchNotFound`/`ErrCommitNotFound` (`pkg/graveler/errors.go:40`, `pkg/api/controller.go:3188-3194`
→ 404), and `branch.go:96-97` returns that error. The only fallback
(`case errors.Is(err, client.ErrNoMergeBase)`) does not match, so the branch reports
`last_error` on every tick: `/_health` is `503` forever, the lag metric never clears, and no
operator-visible instruction exists. A full rebuild at the head — exactly what `rewrite(…, "")`
already does — would be correct here and is never reached. `pending` has the same shape: it passes
`StopAt: last`, so a missing `last` would also 404 there.

**Evidence** `branch.go:87-97`; probe (fake models the 404 through `resolveLocked`,
`fake_test.go:802-818`, so this is not a fake limitation):

```
PROBE collected position: tick 1 branch err=merge base of repo@s2..fresh: resolve "s2": lakeFS resource not found
PROBE collected position: tick 2 branch err=merge base of repo@s2..fresh: resolve "s2": lakeFS resource not found
```

(Probe: index `main` at `s2`, then delete commit `s2` and move `main` to an unrelated root, i.e.
"repository re-created" / "position GC'd"; two ticks, same permanent error.)

**Suggested fix** Treat `client.ErrNotFound` from `FindMergeBase` (and from `pending`'s `stop_at`
dereference) as "the recorded position is gone" and take the existing
`rewrite(ctx, repository, branch, handle, head, "")` fallback, logging it as a rebuild; the test that
fails before the fix is the probe above asserted as `require.NoError(branch.err)` plus
`require.Contains(t, fake.hardResets, hardReset{"repo-metadata", "main", initCommit})` and a
`mds-<fresh>` tag.

## MINOR

### IX-3 - MINOR: the per-commit table reload builds a new REST catalog, costing an extra config round trip and a new connection pool per commit

**Where** `pkg/mds/indexer/iceberg.go:50-70` (`LoadTable`), called per applied commit from `branch.go:540`.

**What happens** §4.4 requires reloading the table before step 3, and `applyCommit` does that with
`x.tables.LoadTable`, which constructs a *new* `rest.Catalog` each time. `rest.NewCatalog` eagerly issues
`GET <catalog>/v1/config` (iceberg-go `catalog/rest/rest.go:1012`) and `tableHandle.swap` closes the previous
handle, whose `Close` drops the connection pool. So every indexed source commit costs three round trips
(config + load + commit) plus a fresh TCP/TLS handshake, instead of one, and `max_commits=1000` means ~2000
extra requests per tick on top of the ~5 the design expects. A 100 000-commit backfill pays this 100 000 times.

**Evidence** code path above; `LoadTable` → `s.catalog(...)` → `table.NewCatalog` on every call, and
`icebergTable.Close` (`iceberg.go:172-180`) closes the catalog. Probe 1 confirms the reload itself sees the
staged pointer (so the behaviour is correct, only the cost is avoidable).

**Suggested fix** Reuse the handle's catalog and `Table.Refresh(ctx)` for the reload (or cache one
`rest.Catalog` per branch tick in `IcebergStore`) so the config call and the transport are paid once per tick.
Test that would fail first: count requests to `/v1/config` on the in-process catalog server for a tick that
applies two commits; today it is ≥ 3, after the fix 1.

### IX-4 - MINOR: the "crashed during the iceberg commit" recovery case is not tested — the subtest injects no crash and asserts nothing new

**Where** `pkg/mds/indexer/branch_test.go:377-388` (`TestRecovery/crashed_during_the_iceberg_commit`).

**What happens** The subtest stages nothing, deletes nothing and injects no failure; it runs two ordinary ticks
and asserts what `TestBackfillHead` already asserts (`resetCalls` empty, rows `[a b]`, two indexed commits). §4.4
row 5 and the partial-write half of row 2 ("partial or foreign writes") therefore have no test: an indexer that
failed to discard a partial stage (property empty or wrong, rows half written) or that reset every tick would
still pass it. The fake can express the state — `e.fake.stage("main", &fakeState{rows: ...})` with
`sourceCommit: ""` — so the case is reachable.

**Evidence** the subtest's body is `e := indexed(t); e.tick(); require.Empty(resetCalls); require.Equal([a b])`.
Probe 1 shows real lakeFS does leave a staged pointer plus 8 staged file entries after step 3, so the state the
subtest claims to cover is a real one with staging, not the empty staging the fake leaves.

**Suggested fix** Stage a partial write (rows for `b` with `sourceCommit: ""`, i.e. the transaction that wrote
files but never committed the pointer) and assert one `resetCalls` entry and then the normal delta; that test
fails on an indexer that treats any staging as "already applied".

### IX-5 - MINOR: no test pages the `pending` walk, so the lag and multi-page stop-at logic is unverified

**Duplicate in part:** the second paragraph of TS-2 (see IX-1).

**Where** `pkg/mds/indexer/branch_test.go` (no case sets `fake.pageSize` for a branch tick; only
`changes_test.go:166` and `maintenance_test.go:150` set it, for the diff and the metadata log).

**What happens** `pending`'s cursor handling, its `metStop` detection across pages and its
"drop the inclusive `stop_at` commit" rule (`branch.go:430-451`) are exercised by every branch test on a single
page only, where `hasMore` is always false and `metStop` is decided by the same page. An off-by-one in the
cursor, or a `metStop` that only looks at the last page, would not fail any test — and it would either skip a
commit permanently or restart a branch tick endlessly. Probe 4 walks 23 pages with the current code and shows
it is correct today, so this is a test gap only.

**Suggested fix** Add a `TestPendingPagesTheLog`: `fake.pageSize = 2`, `seed_lookback`/`max_commits` small, five
pending commits, assert all five rows and `lag == 0` after the ticks, plus one case where `last` is reachable
only through a second parent with `pageSize = 1` (the §4.7 fallback across pages).

### IO-3 - MINOR: with `delete_branches: false` a deleted branch keeps its `/_health` entry and lag gauge, so `/_health` stays 503 until restart

**Where** `pkg/mds/indexer/indexer.go:485-490` (the `if x.deleteBranches` gate around
`retireDeletedBranches`), `pkg/mds/indexer/branch.go:650-676` (`ClearLag`/`RemoveBranch` only inside
the delete loop), and `pkg/mds/indexer/indexer.go:462-466` (a branch that stops matching loses its
health entry but keeps its `lag_commits` gauge). Design §4.8: "matching branch deleted | delete
`R-metadata@b` if `delete_branches` ... Its tags stay ... **Its `/_health` entry and lag gauge are
removed**, so a failed last attempt of the deleted branch does not keep `/_health` at 503."

**What happens** The `/_health`/gauge cleanup is a side effect of deleting the metadata branch, so it
only happens when `delete_branches` is true. With `delete_branches: false` the metadata branch stays
(by design) but so does the branch's last error: `Health.Report` maps any branch `LastError` to
`Healthy: false` (`pkg/mds/health.go:186-189`, status decided at `204-215`), so a single failed attempt on a branch
that has since been deleted in `R` keeps the probe at 503 for the life of the process, and the
`lakefs_mds_lag_commits{repository,branch}` series keeps its last value forever. A second, design-
silent leak of the same kind: on the "branch stops matching the config" row the health entry is
removed (`indexer.go:462-466`) but the lag gauge is not, so a branch dropped from the selector list
keeps reporting its last lag forever.

**Evidence** probe `TestProbeDeletedBranchKeepsHealthWhenDeleteBranchesIsFalse`: index `main` and
`feature`, make `feature` fail (a commit with no diff → `DiffRefs` 404), delete `feature` in `R`,
tick:

```
probe_review_test.go:159: feature error: diff repo@s1..s2: diff refs "s1..s2": lakeFS resource not found
probe_review_test.go:168: after the delete: health entry present=true error=diff repo@s1..s2: ... lag gauge=1
```

`TestBranchDelete/delete_branches: false` asserts only that the metadata branch survives; it does not
look at `/_health`, so the gap is untested.

**Suggested fix** Move `x.metrics.ClearLag` and `x.health.RemoveBranch` out of the delete loop: run
them for every `R`-metadata branch that has no branch in `R` (and for every `R` branch that stopped
matching) regardless of `delete_branches`, and delete the branch only when the flag is set. Test:
`delete_branches: false` + a failed branch + the branch deleted → `/_health` must be healthy again and
the lag series gone.

### IO-4 - MINOR: a branch that fails before the pending list is built reports `lag_commits: 0` and loses `last_indexed`, so lag alerts go blind while the indexer is stuck

**Where** `pkg/mds/indexer/indexer.go:494-511` (`indexBranch` publishes `state` unconditionally),
`pkg/mds/indexer/branch.go:70-105` (`state.last = last` at 105, after `ensureTag` at 82 and
`FindMergeBase` at 87) and `:129` (`state.lag` is set only after `pending`).

**What happens** `branchState` starts zeroed and is filled in as the tick progresses. Any failure
before `state.last = last` (a 500 from `findMergeBase`, a `GetTag`/`CreateTag` error, a failed
`LoadTable`) publishes `last_indexed: ""`, `head: <head>`, `lag_commits: 0` — the branch is behind by
an unknown but nonzero number of commits, and the report says exactly the opposite of "stuck".
`lakefs_mds_lag_commits` is set to the same 0, so an alert on `lag_commits > 0` does not fire for the
failure mode it exists for, while `/_health` is 503 for a different reason.

**Evidence** probe `TestProbeFailedBranchReportsZeroLag`: `main` indexed at `s1`, two commits pending,
`FindMergeBase` fails like a transient 500:

```
mds: branch not indexed branch=main error="merge base of repo@s1..s3: find merge base: unexpected lakeFS response" head=s3 last=
probe_review_test.go:205: health after the failure: last= head=s3 lag=0 err=merge base of repo@s1..s3: ...
```

The real lag is 2 and the real position is `s1`; both are known to the tick but not published.

**Suggested fix** Publish the last *confirmed* state on failure: remember the previous branch state in
`Health` and only overwrite the fields the tick actually recomputed (`SetBranchError` already exists
for this at `health.go:152-159` but is unused), or set `state.last` as soon as `lastIndexed` returns
and keep `lag` unset (`-1`) when it is unknown. Test: the probe above must report `last_indexed: s1`
and either the real lag or an explicit "unknown".

### TB-2 - MINOR: compaction writes rows in scan order, so the table's declared sort order (`path ASC`) no longer describes its files

**Where.** `pkg/mds/table/writer.go:287-307` (`Compact` streams the scan's batches straight into `Append`, with no
sort); `pkg/mds/table/schema.go:199-213` (`SortOrder()` = order id 1, field 2, `SortASC`, applied at creation and
kept forever); contrast `writer.go:148` where the delta path does `sort.SliceStable` by path and `rows.go:96-100`
which documents that sorting is the caller's job.

**What happens.** Two data files `[a c]` and `[b d]` (two commits, each sorted) compact into one file whose rows are
`[b d a c]`. The compacted table still advertises sort-order id 1 = `path ASC` (`SortOrder().OrderID()` = 1, one
field), while its data is not sorted. Consequences: the table metadata lies about physical ordering (iceberg spec:
`sort-order` is what writers honour), so any later tool that trusts it (sorted compaction/z-order planning, an
engine's sorted-merge or `write.distribution-mode` decision) gets an unsorted table; and because per-file
`lower_bounds`/`upper_bounds` on `path` become wide, the next `ApplyDelta`'s `IN (path …)` position-delete scan
(`writer.go:153-162`) and any path-range filter prune less and read more files. No reader returns wrong rows.

**Evidence.**
```
$ go test -count=1 -run TestProbeCompactionSortOrder -v ./pkg/mds/table/
    zz_probe3_test.go:69: before compaction data files=2 rows=[a b c d]
    zz_probe3_test.go:87: compacted file rows in written order: [b d a c]
    zz_probe3_test.go:90: sort order id after compaction: 1 (fields 1)
```
(`scanRows` sorts by path, so `TestCompact` at `writer_test.go:281-318` cannot see the order.)

**Suggested fix.** Sort the scanned rows before `Append` (re-batch through `rows.go`'s sorted builder, or sort the
scan's batches by `path` per output file), or, if compaction is meant to be order-preserving only when the input is
ordered, drop/replace the sort order in the same transaction. Test: compact two interleaved files and assert the
written order is `[a b c d]`.

### MDS-SEC-3 - MINOR: the catalog serves any repository named `R-metadata` as `R`'s index, while the indexer refuses such a repository

**Where**: `pkg/icebergcatalog/resolver.go:297-320` (`Resolve` builds the target from
`MetadataRepository(addr.Repo)` and constants only), `pkg/icebergcatalog/config.go:264`
(`MetadataSearchEnabled`), versus `pkg/mds/table/writer.go:96-104` (`repositoryOf`, the
`lakefs.mds.repository` property enforced on **write**) and `pkg/mds/indexer/repo.go:237-245`
(`ErrForeignRepository`).

**What happens**: whoever can create a repository named `R-metadata` before the indexer does (or whose
group is not in `guardGroups()`, so `mds-metadata-repo-protect` does not apply — the deny is attached to
the caller's guard list only, §8) owns the table that `R.<ref>.system.object_metadata` serves to every
user who can read `R`, because the redirected read runs with the server's identity (§5.2). Those users
then see attacker-chosen rows (paths, `commit_id`, sizes, etags) for `R`, and the attacker's
`metadata.json` `location` is what the sign/credentials paths trust (see MDS-SEC-1 for what a write
signature on that location means). The indexer refuses such a repository permanently ("any other history
and no init marker … refuse that repository", design §4.8), so the state never self-heals; the catalog
never notices, and there is no `/_health` or 404 signal for the reader. A table that the indexer itself
writes always carries `lakefs.mds.repository = R` (design §3.1) — the read side simply does not check it.

**Evidence**: `resolver.go:297-320` only checks `isObjectMetadataAddress` and the ref; `git grep -n
'lakefs.mds.repository'` finds the property only in `pkg/mds/table/schema.go:101` and
`pkg/mds/table/writer.go:98` (write path). Design §4.8's refusal is
`pkg/mds/indexer/repo.go:143-180`.

**Suggested fix**: before serving a resolved virtual table, verify the loaded table's
`lakefs.mds.repository` property equals `addr.Repo` (and, ideally, that the companion repository carries
the `lakefs.mds.init` marker at an accepted position) and answer `NoSuchTableException` otherwise; a test
with a companion table whose property names another repository (or is absent) must be 404, and it passes
today (the table is served).

### MDS-SEC-4 - MINOR: `iceberg.metadata_search.enabled` without `iceberg.enabled` is accepted, half-applied, and answers 501 at the documented endpoint

**Where**: `pkg/icebergcatalog/config.go:196-209` (`LoadConfig`) and `:359-404` (`Validate`) — no
cross-check; `pkg/icebergcatalog/http.go:122-125` (early `MountDisabled` return before the alias mount at
`:222-224`); `pkg/api/services.go:14-20` (`/mds/iceberg/api/` placeholder).

**What happens**: with `iceberg: {enabled: false, metadata_search: {enabled: true}}` startup succeeds and
`pkg/mds/bootstrap.RoutesBuilder` (`bootstrap.go:270-280`) creates the `mds-indexer` user, its group and
both policies (including the installation-wide deny), because it gates on `MetadataSearchEnabled()` only.
The catalog itself is only mounted at `/iceberg/api` and `/iceberg/relative_to` as `MountDisabled`, the
alias is not mounted at all, and `MountNotImplementedServices` (after the builders,
`cmd/lakefs/cmd/run.go:311`) then answers `/mds/iceberg/api/…` with the core placeholder — a plain-text
`501`, not the catalog's `NotImplementedException` envelope ("the Apache Iceberg catalog is disabled on
this server"). So the operator gets principals and a deny installed for a feature that cannot run, and the
endpoint design §5.5 promises for `metadata_search.enabled` does not exist. (Related: the `MountDisabled`
comment at `http.go:244-248` says the alias "stays a plain 404 while the feature is off"; in the binary it
is the placeholder's 501, and the test that asserts 404,
`pkg/icebergcatalog/http_test.go` "disabled" subtest, only sees it because that test never calls
`api.MountNotImplementedServices`.)

**Evidence**: `http.go:122-125` returns before the `r.Mount(MetadataSearchBasePath, handler)` at
`:222-224`; `bootstrap.go:275` gates only on `MetadataSearchEnabled()`; `TestRoutesBuilderDisabled`
(`http_test.go:443-478`) pins 501 only for `iceberg.enabled=false` + `metadata_search` unset.

**Suggested fix**: reject `metadata_search.enabled` unless `iceberg.enabled` in `Config.Validate` (one
error, one test), or make `MountDisabled` claim `MetadataSearchBasePath` too so the alias answers the
catalog's disabled envelope in every build.

### TS-3 - MINOR: the fake's write methods are more lenient than lakeFS on a missing branch, and its one modelled refusal carries the wrong status

**Where** `pkg/mds/indexer/fake_test.go:579-620` (`Commit`), `:454-461` (`ResetBranch`), `:463-476`
(`HardResetBranch`), `:436-451` (`DeleteBranch`); compare `pkg/api/controller.go:3188-3194` (404) and
`:3196-3200` (403 for `ErrDeleteDefaultBranch`).

**What happens** Real lakeFS resolves the branch first and answers 404 for a branch that does not exist
(`pkg/graveler/errors.go:40`, `ErrBranchNotFound` wraps `ErrNotFound`), and `Commit`/`Reset`/`ResetHard`
all go through it (`pkg/graveler/graveler.go:2239`, `:2637`, `:2581`). The fake instead invents state:
`Commit` with `f.branches[repository][branch] == ""` builds a commit with no parents and *re-creates*
the branch (`:600-616`), `HardResetBranch` assigns `f.branches[repository][branch] = commit` for a
branch that does not exist (`:470`), `ResetBranch` never looks (`:454-461`). So "the indexer wrote into a
branch that is gone" — a 404 that real lakeFS reports, and that the design recovers from on the next
tick — silently succeeds in the fake and produces a repository state lakeFS cannot have (a metadata
branch whose root commit is an indexing commit). Second: `DeleteBranch` models the default-branch
refusal as `client.ErrBadRequest` (`:443-447`) but `ErrDeleteDefaultBranch` is mapped to **403**
`ErrForbidden` (`controller.go:3196-3200`); no test can see the difference because
`retireDeletedBranches` skips the default branch by name before calling
(`branch.go:668-676`, `TestMetadataDefaultBranchIsNeverRetired`).

**Evidence** probe:

```
PROBE deleted-branch: branch error=<nil>, head=index-5 parents=[] tableRecorded=true
PROBE hard-reset missing branch: created=true head="index-2" (lakeFS answers 404 branch not found)
PROBE reset missing branch: no error (lakeFS answers 404)
PROBE default branch delete: err=delete branch "main": default branch: lakeFS request rejected bad_request=true forbidden=false
```

(Probe 1 wrapped `client.LakeFS` and deleted `R-metadata@main` immediately before the tick's commit:
the fake commits, the tick reports no error, and the branch head has no parents.)

**Suggested fix** Make `Commit`/`ResetBranch`/`HardResetBranch` return
`&client.NotFoundError{…}` when the branch (or repository) is absent, and return
`client.ErrForbidden` for the default-branch delete; the tests that fail before it are the probe
cases above asserted against those errors.

### TS-4 - MINOR: nothing checks that the companion repository is writable; `client.Repository.ReadOnly` is dead and a read-only or protected `R-metadata` fails every tick forever

**Overlap:** IO-7 lists the dead `ReadOnly` field as a NIT; this is the substantive finding.

**Where** `pkg/mds/client/lakefs.go:119` (field), `:736` (populated), `pkg/mds/indexer/repo.go:42-91`
(`ensureMetadataRepository`, which reads the repository and ignores `ReadOnly`).

**What happens** `Repository.ReadOnly` is copied out of `GET /repositories/{id}` and never read
anywhere (`grep -rn ReadOnly pkg/mds/` → only the declaration, the create-body option and the
conversion), so neither the create path nor the per-tick check refuses a companion repository that
cannot be written. A `R-metadata` created with `read_only: true` (or `R-metadata@main` covered by a
branch-protection rule that blocks `COMMIT` — `docs/metadata-search.md` presents the companion as "an
ordinary lakeFS repository") makes every `Commit`/`HardResetBranch` answer 403
(`ErrReadOnlyRepository`, `ErrProtectedBranch`, `ErrWriteToProtectedBranch` →
`pkg/api/controller.go:3196-3200`), which `finishCommit`/`rewrite` wrap and return; `runBranch` does
not retry it (not a `ErrConflict`), so the branch reports the same error on every tick, `/_health`
stays `503`, tag retention never runs, and the only diagnosis is the wrapped 403 in a log line. No
unit test can see it: the fake has no read-only or protected model (`fake_test.go:163-180`,
`addRepositoryLocked`).

**Evidence** `grep -rn "ReadOnly" pkg/mds/ | grep -v _test.go` → `lakefs.go:119,129,381,382,736` only;
`pkg/graveler/graveler.go:2246-2252` (protected COMMIT), `:2646-2649` (read-only) and
`controller.go:3196-3200` (403).

**Suggested fix** Fail the repository at startup/first tick with a clear message when
`meta.ReadOnly` is true (a new `ErrReadOnlyMetadataRepository`), and document that protection rules
must not block commits on `*-metadata`; the test that fails before it is a `fake` repository with a
`ReadOnly` flag asserted to produce one repository-level error instead of a per-tick branch error.

### TS-5 - MINOR: startup verifies the initialisation commit by walking the entire first-parent history of `R-metadata` into memory

**Where** `pkg/mds/indexer/repo.go:152-185` (`verifyInitCommit`) and `:249-275`
(`firstParentHistory`).

**What happens** `verifyInitCommit` needs the init marker and the root, so it loads *every* commit of
`R-metadata@<default>` newest-first, one page per 1000 commits, into a `[]client.Commit`. `R-metadata`
gets one indexing commit per indexed source commit, so the history is as long as everything the
repository ever indexed: with 1M indexed commits a restart issues ~1000 sequential API calls and holds
~1M commits (hundreds of MB) before it can index anything, on the tick's critical path, for every
configured repository. `docs/metadata-search.md` "Upgrades and restarts" says only that a restart is
safe and resumes at the recorded position.

**Evidence** probe: 400 synthetic indexing commits on `R-metadata@main`, `fake.pageSize = 50` (the real
cap is 1000):

```
PROBE init verification: 400 metadata commits -> 9 LogCommits pages at pageSize 50 in one tick
```

i.e. pages = ⌈history/1000⌉ and the whole slice is resident.

**Suggested fix** Stream the walk: keep only the newest commit, the count of commits older than the
marker and whether the last two commits seen are the marker/root, so memory is `O(1)` (the API-call
count stays, and can be cut by persisting the verified init commit); the test that fails before it is
a `pageSize = 1` history of a few hundred commits asserted to keep at most a fixed number of commit
values alive (or a `-benchmem` bound on `verifyInitCommit`).

### TS-6 - MINOR: the engine scripts only assert that *some* `orders/` row came back, so the documented "both endpoints return the same three rows with the same commit_id" is an unrepeatable observation

**Where** `esti/ops/mds-engines/run-spark.sh:72-73`, `esti/ops/mds-engines/run-trino.sh:107-108`;
the claim is `docs/metadata-search.md:565-575` and `esti/ops/mds-engines/README.md` § "Observed
result".

**What happens** Each variant's only gate is `grep -q 'orders/'`: a run where
`/iceberg/relative_to` served a stale snapshot, one row instead of three, a different `commit_id`, or
rows of the wrong repository still passes both scripts and prints "snippets passed". The three rows
and the `commit_id` equality that the guide reports are therefore a human reading of `.run/*.log`
(git-ignored), not a check. Related: the scripts default to floating tags
(`tabulario/spark-iceberg:latest`, `trinodb/trino:latest`, `run-spark.sh:18`, `run-trino.sh:20`)
while the README records the run with pinned digests, so a re-run does not reproduce the recorded
versions.

**Evidence** the two `grep` lines above; `run-spark.sh:18`, `run-trino.sh:20` vs the digest table in
`esti/ops/mds-engines/README.md` § "Versions used".

**Suggested fix** Capture each variant's rows, require exactly the three expected `orders/` rows, and
`diff` the two endpoints' output (and `SOURCE_COMMIT` from `state.env`) inside the scripts; default the
image variables to the pinned digests and keep `:latest` behind an explicit override.

### TS-7 - MINOR: no end-to-end test of §4.9 (compaction, tag retention) or of the §4.8 branch-deletion mirror; the indexer's `Compact` is a stub in the only test that calls it

**Where** `esti/mds_test.go:864-879` (the generated `metadata_search` section: no `compaction`,
`tag_retention`, `delete_branches`, `seed_lookback`, `rebuild_threshold`; `period` is `2s`);
`pkg/mds/indexer/maintenance_test.go:21-281` (fake-table tests); `pkg/mds/indexer/fake_test.go:910-929`
(`fakeTable.Compact` only resets `deleteFiles` and bumps `snapshots`).

**What happens** The documented behaviour of `docs/metadata-search.md` § "Automatic compaction", "Tag
retention" and "`delete_branches`" is only pinned against the fake: the compaction trigger, the
`mds: compact <R>@<C>` commit, the "the tag stays on the pre-compaction commit" rule and the fact that
a compaction on a real catalog does not break the next tick (`compactIfNeeded` → real `Compact` →
`Commit`) are never run together. `pkg/mds/table` tests `table.Compact` in isolation
(`pkg/mds/table/writer_test.go:281`), not through `indexer`/the catalog of a companion repository, so
a mismatch between "replace files, keep `lakefs.mds.source-commit`" and the indexer's commit (e.g. a
`Compact` that stages nothing, which would make the `AllowEmpty: false` commit fail) would only show
up in production. `lakefs_mds_compactions_total`, documented in § "Metrics", is read by no test.

**Evidence** `grep -n "compaction\|tag_retention\|delete_branches" esti/mds_test.go` → no hit;
`maintenance.go:49-117` vs `fake_test.go:910-929`; esti's only metric reads are
`esti/mds_test.go:722-725` (rows written, rebuilds, tick duration).

**Suggested fix** Add one esti case: `compaction: {max_delete_files: 1, every_commits: 0}`,
`tag_retention: 1s`, two deletes, then assert the `mds: compact` commit exists, the pre-compaction
`mds-*` tag did not move, the row count is unchanged, `lakefs_mds_compactions_total` is 1, and an old
`mds-*` tag is gone; a second case deletes a data branch and asserts the mirrored branch is gone while
its tags remain.

### TS-8 - MINOR: the esti "indexer restart" case is opportunistic and still passes when the kill lands between ticks, so the crash-window claim in the file header is not proven by a run

**Where** `esti/mds_test.go:476-497` (the case), `:1095-1116` (`stopIndexerWhenTicking`), and the
claim at `esti/mds_test.go:44` / `docs/metadata-search.md:562-563`.

**What happens** `stopIndexerWhenTicking` polls `/_health` every 50ms for at most 6s and kills when
`in_progress` is true; if it never observes a tick it only `t.Logf`s that the kill covered the
"between ticks" case (`:1113-1115`) and the case continues. With `period: 2s` and a tick that is a
handful of HTTP calls, the common outcome is a clean kill between ticks, which proves nothing about
§4.4's staged-pointer recovery — the rows converge either way. The header claim "a kill mid-tick and a
restart converge on the branch head (§4.4)" is therefore conditional in practice.

**Evidence** the code above; the only evidence in a run is a log line, and the assertions
(`:492-496`) are the same for both outcomes. The case's own comment (`:477-479`) already says "here
the observable outcome is convergence, not recovery"; the header row (`:44`) and the guide's
"including … an indexer restart" still read as if the crash window were covered.

**Suggested fix** Make the window deterministic — e.g. kill only after observing `in_progress` twice in
a row with a `period` of 250ms, and `t.Fatalf` when no tick was observed within a bounded number of
periods; after the restart, additionally assert that `R-metadata@main` has no leftover staging
(`GET .../branches/main/diff` is empty) and that the position advanced by exactly the missed commits.

## NIT

### IX-6 - NIT: five error messages print `branch@repository` where every other message prints `repository@branch`

**Where** `pkg/mds/indexer/branch.go:65`, `:338`, `:485`, `:508`, `:542`.

**What happens** `fmt.Errorf("load the metadata table of %s@%s: %w", branch, repository, err)` (and the same
operand swap in the two post-reset reloads, the two post-hard-reset reloads and the pre-step-3 reload) print
`load the metadata table of main@repo-metadata: ...`, while `branch.go:48` and `:56` print `repo@branch`. An
operator reading a log line or `/_health`'s `last_error` gets the two identities swapped in exactly the
messages that describe the catalog, which is the hardest part of the system to debug.

**Evidence** the five call sites above versus `fmt.Errorf("head of %s@%s: %w", repository, branch, err)` on
line 48.

**Suggested fix** Swap the operands in all five; a test that asserts `strings.Contains(err.Error(), "repo@main")`
on a forced `LoadTable` failure in `TestReloadTableBeforeApplyingCommit`-style setup fails before the fix.

### IX-7 - NIT: `Table.SnapshotCount` is documented as the `every_commits` counter, which it is not

**Duplicate in part:** the `SnapshotCount` doc-comment item of IO-7 (indexer service), kept there as part of a wider dead-code finding; this is the specific one.

**Where** `pkg/mds/indexer/tables.go:25-27` and `pkg/mds/indexer/iceberg.go:121-127`.

**What happens** The interface comment says `SnapshotCount` "returns the number of snapshots ... §4.9's
`every_commits` trigger counts them". The trigger counts *indexing commits from the first-parent log of
R-metadata@b* (`maintenance.go:56-67`, `indexedSinceCompaction`), and `SnapshotCount` is used only as a log
field (`maintenance.go:53-54,88`). Anyone changing the trigger by following the comment would swap a durable
count for a snapshot count that compaction itself increments — the exact bug commit `7f06dad27` fixed.

**Evidence** the only two call sites of `SnapshotCount` are the log field and the fake; `compactIfNeeded`'s
`due` is computed from `DeleteFiles()` and `indexedSinceCompaction`.

**Suggested fix** Correct the comment (it is the delete-file trigger that reads `DeleteFiles()`), or drop
`SnapshotCount` from the interface and log `len(tbl.metadata.Snapshots())` in the writer instead.

### IO-5 - NIT: `/_health` keeps the previous error while a repo is in progress, and a cancelled tick leaves `in_progress` stuck

**Where** `pkg/mds/health.go:103-120` (`BeginTick` sets `inProgress`, leaves `lastError`),
`pkg/mds/health.go:175-200` (`Report`), `pkg/mds/indexer/indexer.go:427-440` (`tickRepository`
returns before `FinishTick` when the tick is cancelled). Design §9: "`/_health` returns 200 when every
repository's last tick **succeeded or is in progress**, 503 otherwise".

**What happens** Two small state-machine deviations. (a) A repository whose previous tick failed is
503 for the whole of the next tick: `BeginTick` keeps the stale `lastError`, so no probe ever sees
"200 because in progress" as the design sentence describes (this may be deliberate — a retry should
not mask a failure — but then the design sentence is the thing to fix). (b) When the tick is
cancelled mid-repository (`if ctx.Err() != nil { return }`), `FinishTick` is skipped, so
`in_progress` stays true with a stale error; anything still probing the process before it exits (or a
test embedding the service) sees a repository that is "in progress" forever.

**Evidence** `grep -n "BeginTick\|FinishTick" pkg/mds/indexer/indexer.go` → the only `FinishTick` is
at `indexer.go:440`, after the early `return` at 434; `TestHealthReport` (service_test.go:31-79) calls
`BeginTick` only on a healthy repository, so neither case is covered.

**Suggested fix** Pair the calls (defer a `FinishTick` with the context error, or record
`inProgress = false` on cancellation) and decide the in-progress/error precedence explicitly, with a
test that asserts the chosen answer for "previous tick failed, current tick in progress".

### IO-6 - NIT: `compaction.*` is neither validated nor disableable — an explicit `0` is silently turned into the default

**Where** `pkg/mds/config.go:268-303` (`setDefaults` keeps an explicitly written `0`), `:309-360`
(`Validate` has no rule for `compaction.max_delete_files`, `compaction.every_commits` or
`tag_retention`), `pkg/mds/indexer/indexer.go:382-406` (`applyDefaults` maps `<= 0` to the default),
`pkg/mds/indexer/maintenance.go:55` (`due := x.compaction.MaxDeleteFiles > 0 && ...`, i.e. `0` was
meant to mean "never").

**What happens** `metadata_search.compaction.max_delete_files: 0` survives `setDefaults` and
`Validate`, and `Indexer.applyDefaults` immediately rewrites it to 200, so the delete-file trigger
cannot be turned off and a negative value (`: -1`) is accepted and silently becomes 200 as well. The
`> 0` guard in `compactIfNeeded` shows the code intends `0` to disable the trigger; the defaults
layer contradicts it. Design §7 has no validation rule for these keys either, so this is a gap in
both, but the effect is a config the operator cannot express.

**Evidence** `config.go:298-303` (`if !set("compaction.max_delete_files") { ... = Default... }`) plus
`indexer.go:395-397` (`if x.compaction.MaxDeleteFiles <= 0 { x.compaction.MaxDeleteFiles = DefaultMaxDeleteFiles }`).

**Suggested fix** Either reject non-positive `compaction.*`/negative `tag_retention` in `Validate`
(one error per problem, like the other rules) or let `0` through `applyDefaults` as "disabled" and
document it. Test: `compaction.max_delete_files: 0` must either fail validation with the documented
message or leave the trigger off (today neither).

### IO-7 - NIT: dead code and one stale doc comment around the health/metrics/stage seams

**Overlap:** its `SnapshotCount` doc-comment item duplicates IX-7 and its dead-`ReadOnly` item is TS-4; the unused stage constants and `SetBranchError` are unique to this entry.

**Where** `pkg/mds/metrics.go:38-46` (`StageRepository`, `StageBranch`), `pkg/mds/health.go:150-160`
(`SetBranchError`), `pkg/mds/indexer/tables.go:25-27` (`SnapshotCount` doc), `pkg/mds/client/lakefs.go:110-120`
(`Repository.ReadOnly`).

**What happens** `mds.StageRepository`/`mds.StageBranch` are unused: the indexer declares and uses its
own copies (`indexer.go:87-93`) because it may not import `pkg/mds`, so the two label vocabularies can
drift with nothing failing. `Health.SetBranchError`, `Health.SetBranch` and `client.Repository.ReadOnly`
are only used by tests (or not at all: `ReadOnly` is populated at `lakefs.go:736` and never read).
`tables.go:25-26` says "`SnapshotCount` returns the number of snapshots ... §4.9's `every_commits`
trigger counts them", but since `7f06dad27` the trigger counts `mds: index` commits in the commit log
(`maintenance.go:104-143`); the only caller uses it for a log line (`maintenance.go:54`, `:78`).

**Evidence** `grep -rn "StageBranch|StageRepository|SetBranchError|\.ReadOnly\b" pkg/mds cmd/lakefs-sso`
(non-test) returns only the declarations, `pkg/mds/client/lakefs.go:381,:382,:736` and the indexer's own
copies; the design §9 metric labels are therefore maintained twice.

**Suggested fix** Drop the duplicates (a `const stageRepository = indexer.StageRepository` in `pkg/mds`
would need the dependency the other way; simplest is for `mds.metrics` to declare no stage constants
and for the service to use `indexer.StageBranch` where it needs one), delete `SetBranchError` or use
it for IO-4, and fix the `SnapshotCount` comment.

### TB-4 - NIT: `Compact` accepts `…Option` and discards all of them

**Where.** `pkg/mds/table/writer.go:280` (`if _, err := resolve(tbl, opts); err != nil`) — the resolved
`writeOptions` are thrown away; the body never reads them (`writer.go:269-318`).

**What happens.** `Compact(ctx, tbl, c, table.WithDeleteChunkSize(1))` or `WithDeleteMode(copy-on-write)` is
accepted and silently ignored, while `WithDeleteMode("nonsense")` is an error, so the option set looks honoured
but is not. `NewBatchesRecordReader` also fixes the batch size at the scan's, so `WithRecordBatchSize` cannot work
here either.

**Suggested fix.** Drop the `opts` parameter from `Compact` (only `resolve`'s validation is used), or honour the
options and document which ones compaction ignores. Test: `Compact` with an invalid mode still errors (keep) and
the signature no longer promises the rest.

### TB-5 - NIT: nothing at compile time or in unit tests proves `*catalog.Catalog` still satisfies the ref mapper's `refResolver`

**Where.** `pkg/mds/refmap/refmap.go:47-50` (unexported `refResolver`), `refmap.go:73-78` (runtime type assertion;
failure returns `nil` + one warning), `cmd/lakefs-sso/main.go:93` (the only installation), tests use only the
`fakeCatalog` of `pkg/mds/refmap/refmap_test.go:26-84`.

**What happens.** If `catalog.Catalog.GetCommit`/`GetTag` are ever re-shaped (or a deployment wraps the catalog in
another `BranchLister` implementation — `ServerDeps.Catalog` is `*catalog.Catalog` today, `cmd/lakefs/cmd/hooks.go:39`),
`refmap.New` returns `nil` and every tag and commit read of `R.<ref>.system.object_metadata` silently becomes
`NoSuchTableException`; the only signal is a single `Warn` at startup and the failure surfaces only in
`esti/mds_test.go` (tag and short-commit case), not in `go test ./pkg/mds/...`.

**Suggested fix.** Add `var _ refResolver = (*catalog.Catalog)(nil)` next to the interface, plus one wiring test
that builds the resolver over a real catalog (the `pkg/api`/`pkg/catalog` test fixture already used by
`pkg/mds/table/server_test.go`) and maps a tag of `R` to `mds-<commit>`.

### MDS-SEC-5 - NIT: the deny does not cover `fs:UpdateRepository`, which writes a companion repository

**Where**: `pkg/mds/bootstrap/bootstrap.go:140-148` (`metadataRepoProtectFSDeniedActions`), used at
`:231-253`.

**What happens**: the deny's own comment promises that "no principal in a base group may write, delete,
create or revert inside a companion repository", and §8 lists `fs:Write*`, `fs:Delete*`, `fs:Create*`,
`fs:Revert*`. `fs:UpdateRepository` (`pkg/permissions/actions.go:19`) is the action of
`SetRepositoryMetadata` / `DeleteRepositoryMetadata` (`pkg/api/controller.go:2573-2612`) and matches none
of the four patterns, so a guard-group operator can still set and delete arbitrary repository metadata on
`R-metadata` through `POST/DELETE /repositories/{repo}/metadata`. Nothing in this design stores index
state in repository metadata (`lakefs.mds.*` lives on commits and table properties), so this is hardening
only, not an index-corruption path — but the deny does not cover what its comment describes.

**Evidence**: `wildcard.Match("fs:Write*", "fs:UpdateRepository") == false` (same matcher as MDS-SEC-2);
`pkg/permissions/actions.gen.go` lists `fs:UpdateRepository` among the fs actions, and the deny list names
only `Write/Delete/Create/Revert`.

**Suggested fix**: add `fs:Update*` (or `fs:UpdateRepository`) to
`metadataRepoProtectFSDeniedActions` and extend `TestMetadataRepoProtectPolicyStatements` +
`TestEnsurePrincipalsAuthorization` with the `RepositoryMetadata` case.

### TS-9 - NIT: the esti reads are full scans, so the filtered-scan examples of the guide (PyIceberg `row_filter`, DuckDB `WHERE … ORDER BY … LIMIT`) are not what "runs the snippets above" verifies

**Where** `esti/mds_test.go:2241` (`scanned = table.scan().to_arrow()`), `:2326`
(`SELECT … FROM lakefs."system"."object_metadata"`, no predicate), vs
`docs/metadata-search.md:369-373` (`row_filter=GreaterThan("size_bytes", …) & StartsWith("path", …)`)
and `:405-410` (`WHERE path LIKE 'orders/%' ORDER BY size_bytes DESC LIMIT 20`).

**What happens** The suite compares the four paths on full table scans only; the pushdown sentence
("A filtered scan is pushed down as Iceberg expressions") and the guide's `ORDER BY size_bytes` are
verified by neither the esti suite nor the Spark/Trino scripts in the same shape (they do use a
`LIKE` filter).

**Suggested fix** Add one read step with `row_filter=StartsWith("path", "data/")` (and, for DuckDB,
`WHERE size_bytes > 0 ORDER BY size_bytes DESC LIMIT 1`) to `mdsReadScript`, and assert the same
signatures from all four paths.

### TS-10 - NIT: the fake's merge base is "any common ancestor", not graveler's best common ancestor, and two smaller fake-side divergences

**Where** `pkg/mds/indexer/fake_test.go:479-512` (BFS from `sourceRef`), `:527-545` (`LogCommits`
ignores `opts.FirstParent`), `:334-345` + `:562-566` (`enterDataBranch` receives a commit ID under the
parameter name `branch`).

**What happens** (a) `pkg/graveler/ref/merge_base_finder.go:25-69` returns the *best* common ancestor
(generation-ordered heap, git's definition); the fake returns the first ancestor-of-`source` that is
also an ancestor of `destination` in BFS order. On a criss-cross history the two disagree, so a test
that pins the reset target (`hardResets` contains `ref: "mds-<base>"`) could assert a tag real lakeFS
never produces. (b) Because `LogCommits` ignores `FirstParent`, no unit test can catch a regression
that drops `FirstParent: true` from `pending`/`seedTag`/`backfillStart`/`firstParentHistory` (the
client's own test pins that the *client* sends the flag, `pkg/mds/client/lakefs_test.go:298`), and on a
merge-heavy branch that regression would apply side-branch commits' diffs to the table; only the esti
merge case could catch it. (c) The hook's second argument is a commit ID, so the doc comment and the
parameter name are wrong (harmless for the only user, `TestConcurrencyBoundsBranchesInFlight`).

**Suggested fix** Model the flag (walk all parents when `FirstParent` is false) and either implement
graveler's best-common-ancestor rule or state in the test helper that only linear/one-merge-base
histories are modelled; rename the hook parameter and assert `FirstParent` in a `TestMergeUsesTheFirst-
Parent` variant whose fake walks every parent.

## Checked, no finding

### Indexer pipeline

- **`pending`'s stop-at rule** (`branch.go:445-451`) matches the server: `pkg/catalog/catalog.go:1668-1671`
  (`foundAllCommits`) stops the walk *at* `stop_at`, so the stop commit is the last item of the last page and
  `hasMore` is false there; dropping `commits[0]` after the reversal is right, and re-applying `last` would
  indeed stage nothing and fail the `allow_empty=false` commit.
- **`DiffRefs` cursor and the two-dot direction**: `pkg/catalog/catalog.go:1997-2011` treats `after` as an
  exclusive path, matching the client's `Path` cursor; `pkg/graveler/committed/diff.go:251-273` puts the
  *right* value into an added/changed entry (and the left into a removed one), so `change.SizeBytes`,
  `Right.Checksum` and `Right.Mtime` are the right side for the entries `diffChangeSet` upserts.
- **The stat fallback** (`changes.go:144-179`): "complete" = right stat present, non-empty checksum, non-zero
  mtime, non-zero size; `content_type` and user metadata are excluded, as §4.3 step 1 says. `StatObject` is
  called against `commit.ID`, an immutable commit, never against the branch. The threshold cut-off drops the
  partial entries before any stat call (`changes.go:107-109`), so a rebuilt commit costs no stat (the audit fix
  `1cb131f13` is complete: `large` short-circuits `applyCommit` before `upsert`/`deletes` are touched).
- **`changeSet`'s root-commit path** (`changes.go:56-76`) is unreachable from `applyCommit`: a root commit can
  only be in `todo` if it is an ancestor of `head` at depth ≥ 1 below `last`, and both the `backfill` start and
  the §4.7 rewrite write a full listing instead. It is what §4.3 step 1 mandates, so it stays, but no test can
  reach it.
- **`hasUncommittedChanges`** (`branch.go:404-416`) is sound against real lakeFS: probe 1 shows the uncommitted
  diff of `R-metadata@main` reports the staged table pointer (`_lakefs_iceberg/tables/system/object_metadata/
  .pointer`, type changed) plus the staged data/metadata files, so "any staged write is at least one change"
  holds and the first page is enough. Its cost (one extra `DiffBranch` per branch per tick) is the behaviour
  §4.2 documents.
- **Concurrency**: `indexRepository` (`indexer.go:444-492`) bounds branches in flight with a semaphore and waits
  for all of them before `retireDeletedBranches`/`retireTags`; `Indexer.metadata` and `defaultBranchKept` are
  mutex-guarded; `Service.tickUntilDone` starts one tick at a time, so two ticks cannot race on the same branch;
  `-race` on the package is clean (verified above).
- **`iceberg.go`'s `SourceCommit`** (`iceberg.go:117-119`) really is the *staged* value on a branch (probe 1: a
  fresh `LoadTable` after a staged `ApplyDelta` reports the new source commit), which is what makes recovery's
  row-1 comparison work.
- **`ErrTableNotFound` for a missing metadata branch**: `pkg/icebergcatalog/store.go:315-331` maps
  `graveler.ErrNotFound` — and `ErrBranchNotFound` wraps it (`pkg/graveler/errors.go:40`) — to
  `NoSuchTableException`, which iceberg-go maps to `catalog.ErrNoSuchTable`; `iceberg.go:56-60` maps that to
  `ErrTableNotFound`, so `createBranch` is reached for a branch that does not exist yet.
- **`tableError`** (`iceberg.go:183-188`): `ErrCommitFailed` → `ErrConflict` is the only mapping needed. A
  `CommitStateUnknownError` is deliberately *not* a conflict: it fails the tick and the staged pointer is
  recovered next tick, which is §4.4's row 1. Probe 3 shows lakeFS's catalog answers
  `CommitFailedException` (409) for a stale `assert-ref-snapshot-id`, so `ErrConflict` does reach `runBranch`.
- **Tag/commit identity**: `ensureTag` (`branch.go:617-650`) leaves a tag that already records the commit alone
  (so a compaction commit does not drag `mds-<C>` forward, §4.9), deletes a mispointed tag before creating it
  (lakeFS never overwrites a tag, `pkg/graveler/graveler.go CreateTag`), and tolerates a 404 on both the tag and
  its commit. `finishCommit` uses `allow_empty=false` and always moves a table property first, so the commit is
  never empty.
- **Seed and backfill** (`branch.go:192-330`): the seed uses the *tag name* as the `CreateBranch` source, which
  `graveler.CreateBranch` resolves like any ref, and `last` comes from the new branch head's
  `lakefs.mds.source-commit`, so `onFirstParent` sees the seeded commit; `backfillStart`'s `since` comparison is
  `!Before` (i.e. `≥ since`, as §4.5 says) and returns the head when nothing is inside the window.
- **Authorization**: no gap found. Every write goes to `R-metadata` (bootstrap policy `fs:*`/`catalog:*` on
  `*-metadata`), `system` on the data repository is refused by `requireWritableNamespace`, and the only
  destructive operation on a branch a user could reach (`resetBranch`) is guarded by the deny policy.
- **Dead code**: none beyond the spec-mandated root-commit path above. `changeSet.count`, `changeSet.large`,
  `SnapshotCount`, `DeleteFiles` and `tableHandle.swap` are all read somewhere in the package.

### Indexer service, repository setup and maintenance

- **Tick loop and scheduling** (`indexer.go:414-423`, `indexer.go:444-490`): repositories are visited
  sequentially and branches within one repository run under a per-repository semaphore of
  `concurrency` (`indexer.go:468-483`), which is what §4.1 asks for; the semaphore is acquired before
  the goroutine starts and released in a `defer`, and cancellation is checked before each new branch,
  so no goroutine is started after a cancel and none outlives `wg.Wait()`. `TestConcurrencyBoundsBranchesInFlight`
  measures the real peak with a probe hook, not a mock of the scheduler.
- **Shutdown** (`service.go:118-239`, `cmd/lakefs-sso/mds.go:52`): one tick at a time (the inner
  `select` does not consume `ticker.C`, so ticks cannot pile up while one is in flight), `SIGTERM`
  reaches the service through `signal.NotifyContext`, an in-flight tick is abandoned rather than
  waited for (documented in §4.1/§4.4), and the HTTP server has a 10 s shutdown bound. The one
  stuck-`in_progress` consequence is IO-5.
- **Companion repository creation** (`repo.go:34-120`): derivation of the namespace, the bucket-root
  refusal, the `storage_namespace` override, `-metadata`-suffixed data repositories, and the
  storage-ID/default-branch inheritance all match §3.3; the overlap test runs on every tick against
  the existing repository's namespace (`repo.go:83-93`) and uses the same "/"-normalised prefix test
  as the uncommitted GC (`repo.go:302-345`, pinned in `internal_test.go`).
- **Branch matching and deletion** (`indexer.go:441-467`, `branch.go:650-692`): trailing-`*` selectors
  only, a branch that stops matching keeps its table and loses its health entry, a deleted branch's
  metadata branch is removed only when `delete_branches` is true, R-metadata's own default branch is
  skipped with a once-per-process log, and the cursor for every paging loop is the last item of the
  page with an explicit error when the server reports "more" without a cursor. `fake.DeleteBranch`
  models `ErrDeleteDefaultBranch`, and `fake.HardResetBranch` deliberately keeps staging, so the two
  lakeFS behaviours this code depends on are not faked away.
- **Crash/restart safety of the per-commit path** (`branch.go:44-156`, `:296-410`, `:466-655`): every
  row of §4.4's table is reachable and handled, the table is reloaded before step 3, a 409 restarts
  the branch at most `conflictRetries` times, `ResetBranch` precedes `HardResetBranch` (real
  `ResetHard` does not drop staging — `pkg/graveler/graveler.go:2598` TODO, verified against the
  code), and the inclusive `stop_at` is compensated by dropping the recorded position. I looked for a
  path where `applyCommit` runs with `tbl.SourceCommit() == commit.ID` and then fails on
  `AllowEmpty: false` ("commit: no changes"): recovery consumes that state before the loop, and after
  the first applied commit the reloaded table always records the previous commit, so I could not
  construct one.
- **Compaction and tag retention** (`maintenance.go:49-99`, `:104-143`, `:146-192`, `:194-257`): the delete-file trigger reads
  `total-delete-files` from the snapshot summary, which iceberg-go v0.7.0 maintains as a running total
  across the table (`table/snapshots.go:687-702`), so it is a table-wide count and not a per-snapshot
  one; the `every_commits` count is read from the log (durable, restart-safe, stops at the previous
  `mds: compact`); compaction keeps the position and writes no tag; retention keeps the tag of any
  live branch position and retires unresolvable tags, and its paging is safe against deleting
  already-seen tags. A crash between `Compact` and its lakeFS commit is discarded by recovery
  (`branch.go:376-410`) and retried when due. The per-tick full tag listing with two `GetCommit` calls
  per tag is inherent to the §3.3/§10 "one tag per indexed commit" decision and is bounded once
  retention trims the backlog, so I did not report it.
- **Config defaults and validation** (`config.go:200-364`, `config_test.go`): every rule of §7 is
  present with a table row, one error per problem, defaults applied only when the key is absent, and
  the env overrides (`LAKEFS_METADATA_SEARCH_*`) work for scalars including a bare-YAML `since`. The
  `-metadata`/54-character rules are pinned at the exact boundary. Only the `compaction.*` gap is
  reported (IO-6).
- **`/_health` body and status, metrics registration** (`health.go:20-241`, `metrics.go:24-166`): the
  JSON field names, the 200/503 rule, the method handling, `ClearLag` on delete, the `lakefs_mds_`
  prefix and all seven §9 metrics are present and served through the real handler in
  `TestServiceHandler`; the metric names are additionally pinned against the collectors.
- **Security/authorization in this area**: the indexer holds no repository secret beyond its own
  credentials, logs no config value that is secret (only repository names and namespaces), refuses
  foreign companion repositories before writing anything, refuses a companion namespace that R's
  uncommitted GC would sweep, and `/_health`/`/metrics` expose only repository/branch names and
  error strings — all as §8/§9 describe. The only authorization-adjacent behaviour I could not
  exercise is the deny policy (bootstrap, another fragment).
- **`cmd/lakefs-sso/mds.go`**: viper is pointed at `--config` with the documented `LAKEFS_` prefix and
  `.`→`_` replacer, `metadata_search` is the only section read, a missing/invalid section fails
  startup through `LoadConfig`, and `logging.ContextUnavailable()` is a real logrus logger, not a
  no-op, so the indexer logs without the server's logging configuration.

### Table writer (`pkg/mds/table`) and ref mapper (`pkg/mds/refmap`)

- **Schema and field IDs (§3.1).** `schema.go:148-192` builds the create request in iceberg-go's
  `AssignFreshSchemaIDs` pre-order (map key/value are 8/9, trailing columns 10/11/12);
  `TestCreateTableFieldIDs` (`catalog_test.go:81-133`) pins every ID against a *real* lakeFS catalog over HTTP, not
  a fake, and re-checks that the request and the created table agree; `TestCreateTableSchema` pins types,
  requiredness, `identifier-field-ids = [2]`, format v2, sort order, and the map value's optionality. Readers and the
  writer take IDs/columns from `Table.Schema()` (`rows.go:87-93`), never from the constants. Sort order's source
  ID is validated against the created schema (`catalog_test.go:70-76`).
- **Row encoding (§3.1, §3.2).** `timestamptz` maps to `arrow.FixedWidthTypes.Timestamp_us`
  (iceberg-go `table/arrow_utils.go:705-707`), so `rows.go:273`'s `UnixMicro` is the right unit — the only unit bug
  that would escape the round-trip tests, and it is not present. `user_metadata` is always `Append(true)` with
  sorted keys (`rows.go:291-303`), so an object without user metadata is an empty map, never null, and the same row
  produces the same bytes. Optional columns are `AppendNull` (`rows.go:312-319`). `TestApplyDeltaSourceRepository`
  pins `lakefs.mds.source-repository` to the table property rather than to stale row data.
- **Upserts, delete files and position deletes (§4.3 step 3, §10.2).** The spike
  (`spike_test.go:53-100`) proves against the production catalog routes that `Delete`+`Append`+`SetProperties` in one
  transaction stages position delete files and reads back exactly the expected rows; `ContentType`/metadata and
  copies of the same path are covered. Chunking is bounded at 10 000 paths with an in-test `>10 000` case
  (`writer_test.go:154-176`), and the copy-on-write fallback names the same path set (`writer.go:368-390`,
  `writer_internal_test.go:33-49`); `changedPaths` clips and sorts so a retried commit sends identical predicates.
- **Snapshot properties.** Every `Delete`, `Append`, `Overwrite`, `ReplaceFiles` call passes
  `SnapshotProperties(repository, sourceCommit)` (`writer.go:145,153-194`), each chunked delete included; the
  property-only delta (`writer.go:196-205`) still moves the indexing position while adding no snapshot
  (`writer_test.go:106-125`), which is what §4.3 step 4 relies on.
- **Optimistic concurrency and 409s.** A stale handle is refused with `CommitFailedException: the requirement
  "assert-ref-snapshot-id" is not satisfied` (probe: writer A commits, writer B on a handle loaded at the same
  position is refused; the table keeps one row). `rest.Catalog.CommitTable` maps 409 → `table.ErrCommitFailed`
  (`catalog/rest/rest.go:1626-1633`), which `tableError` turns into `indexer.ErrConflict`
  (`pkg/mds/indexer/iceberg.go:183-190`), which `runBranch` bounds at 3 restarts (`pkg/mds/indexer/branch.go:25-36`).
  I also checked that iceberg-go cannot silently replay the CAS: `doCommit`'s refresh-and-rewrite loop only runs for
  `commit.retry.num-retries > 0` and the default is `0` (`table/table.go:1048-1049`, `table/properties.go:161`), and
  the writer never sets that property; if an admin set it in the table properties the CAS would be rewritten away,
  which is worth keeping in mind but is not a defect of this code.
- **iceberg-go usage and credentials/IO (§4.6).** `CatalogConfig.URI` percent-escapes the one warehouse segment
  (`catalog.go:104-112`) and the server splits `repo.ref` at the first dot, which is unambiguous because repository
  IDs cannot contain a dot (`pkg/icebergcatalog/ident.go:152-174`) — a branch name with `.` or `/` works
  (`catalog_test.go:161-215`). Basic auth is a session default header, so it also covers the bootstrap `GET /v1/config`.
  `maps.Clone(cfg.Storage)` per catalog is required (iceberg-go writes the server config into that map) and is
  pinned by `TestNewCatalogConcurrentSharedStorage`. The static storage properties are *not* leaked into table
  metadata: `rest.Catalog.CreateTable` sends only `catalog.WithProperties` (`catalog/rest/rest.go:1507-1515`) and the
  lakeFS catalog's `/v1/config` overrides carry only `prefix` (`pkg/icebergcatalog/http.go:602`), so
  `s3.secret-access-key` stays client-side; the vended path is proven end to end by
  `vended_test.go:616-701` (fake STS + real object store, ambient credentials cannot make the write pass).
- **Compaction rewrite (§4.9) other than TB-1/TB-2.** `ReplaceFiles(data, nil, delete, …)` + `Append` is the right
  primitive (a plain `Overwrite` would keep the delete files); rows survive, the attached delete files are removed,
  the source commit does not move (`writer_test.go:281-318`), and the transaction is crash-safe in the same way as a
  delta (files are orphans, staging is discarded by §4.4 recovery).
- **Ref mapper seam (§5.1, §6).** The fake in `refmap_test.go` reports the same error *wrapping* real lakeFS uses —
  `ErrCommitNotFound`, `ErrTagNotFound`, `ErrRepositoryNotFound` all wrap `graveler.ErrNotFound`
  (`pkg/graveler/errors.go:15,36,38,42`) — so the not-found branches are exercised honestly. Short commit IDs really do
  resolve through `GetCommit` (`pkg/graveler/ref/resolve_ref.go:128-148`), ref expressions through the modifier walk
  (`resolve_ref.go:66-127`), and `$`-staging refs are rejected as `ErrInvalidRef` (returned as-is, 500, not
  mistaken for "not indexed"). No ancestor fallback: the mapper asks for exactly one tag and the tests assert the
  exact `GetTag` lookups. `noSuchTable` builds the same address shape `isObjectMetadataAddress`
  (`pkg/icebergcatalog/resolver.go:396-406`) serves, and `TestMetadataSearchResolverWiring` pins that a branch needs
  no mapper while a tag does.
- **Authorization around the seam.** The mapped ref is a tag, and writes through the virtual address never reach a
  resolution (`ReservedNamespace`, `resolver.go:369-372`, plus §5.3 authz on `R`), so the mapper cannot be used to
  write `R-metadata`; the mapper's own catalog lookups run with the server identity, which is the documented
  behaviour (§5.2 caveat) and not a new exposure.
- **Unbounded memory in my area.** `Rebuild` and `Compact` stream (`rows.go:141-231`, `writer.go:236-247,296-307`);
  a delta is bounded by `rebuild_threshold` by its caller and its Arrow batch count by `DefaultRecordBatchSize`;
  `pullRecordReader` is coroutine-based (`iter.Pull2`), so `Release` neither leaks a goroutine nor blocks on the
  producer.
- **Races.** `go test -race` clean on both packages; the writer holds no shared mutable state (`NewCatalog` copies
  the caller's `Storage`, `Schema()`/`SortOrder()` return fresh values).
- **Dead code in my area.** Only TB-4 (unused `Compact` options); `pathFilter`'s `AlwaysFalse` branch is reachable
  from tests and from the "rows only" copy-on-write case; no unused exported helper was found.

### Server security

- **Reserved `system` namespace on a data repository (§5.4)** — every catalog write reaches
  `RequireWritableNamespace` after the permission decision: create/commit (`commit_http.go:82`,
  `tables.go:663`, `tables.go:785`), register (`register.go:209`), views (`views_http.go:378, 466, 519`),
  namespaces (`namespaces.go:490, 533, 618`). The check is `Namespace[0] == "system"` so nested levels are
  covered too, and it is the *requested* address (writes never resolve, `tables.go:934-944`), so a
  redirect cannot be used to write. The `R-metadata` side stays writable because `IsMetadataRepository`
  keys on the suffix — the documented consequence for a data repo whose own name ends in `-metadata`.
  `TestMetadataSearchReservedNamespace` covers create, register, drop, commit, create view, create/update/
  drop namespace, and the companion repository's writable counterpart.
- **Read authorization of the virtual table (§5.2/§5.3)** — authorize-before-resolve on every read path
  (`tables.go:1049-1085`), `ResolveAuthorized` re-authorizing a differing `Authz`
  (`resolver.go:449-470`), and the single `fs:ReadRepository` + `catalog:ReadTable` decision on `R`
  (`authz.go:RequiredNode`). `TestMetadataSearchVirtualTableRead` pins the negative cases against a real
  ACL service: a caller with `R-metadata` access only is 403 through the virtual table; a caller with `R`
  access only cannot read `R-metadata` directly.
- **`/mds/iceberg/api` mount and the 501 when disabled (§5.5)** — the alias is the *same* handler behind
  the same auth middleware and prefix (`http.go:222-224`), with no `relative_to` form
  (`http_test.go` "there is no relative_to form under the alias"), and
  `MountNotImplementedServices`/`mountPatternClaimed` (`pkg/api/services.go:32-66`) correctly skips a
  claimed prefix and does not shadow the live catalog (both run orderings are tested,
  `pkg/api/serve_test.go:309-410`, `http_test.go:443-478,616-620`). My only objection is MDS-SEC-4.
- **Path/ref/table-name injection** — `ident.go` validates every level (no `/`, `\`, unit separator,
  `.`/`..`, control bytes, length), `store.go:690-725` re-validates before building
  `_lakefs_iceberg/…` keys (defence in depth), the resolver's target is built from constants plus a
  server-derived ref, and the generated client escapes every path parameter
  (`runtime.StyleParamWithLocation`, e.g. `pkg/api/apigen/lakefs.gen.go:12044-12070`). No path/ref
  injection found through repository, ref, table or mapped-ref names.
- **Credential vending (§4.6, §5.2)** — write sessions are refused for a reserved namespace
  (`creds.go:452-460`), the session policy is scoped to the table root with `s3:prefix` on the bucket
  action (`creds.go:sessionPolicy`), a table whose location escapes its root is not vended
  (`ErrCredentialScope`), failures are best-effort and never leak the role/STS error to the client, and
  the lazily built STS client is mutex-guarded (`creds.go:stsAssumer`). The one gap in this family is
  MDS-SEC-1 (signing, not vending).
- **`pkg/mds/bootstrap` idempotency and degradation** — user/group/membership tolerate
  `auth.ErrAlreadyExists`; a drifted policy is rewritten (`ensurePolicy`, tested by
  `TestEnsurePrincipalsRepairsDriftedPolicy`); the policy backend is probed *before* `CreateUser` so
  `BasicAuthService` neither renames the admin nor fails startup
  (`TestEnsurePrincipalsDegradesWithoutPolicyBackend`, `TestRoutesBuilder` "degrades"); a genuine backend
  error is still fatal (`TestEnsurePrincipalsPolicyErrorIsFatal`); `MetadataSearchIndexers` is dropped from
  the deny list with a warning even when an operator names it
  (`TestEnsurePrincipalsConfiguredGuardGroups`); `guardGroups()` is passed in from
  `cmd/lakefs-sso/main.go:100-135` so both installation-wide denies cover the same principals.
- **The lakeFS client (§4.2, §4.3, §4.4 mapping)** — checked against the real handlers: every paginated
  call takes the server's `next_offset` as the next cursor and refuses "more results without a cursor"
  (`lakefs.go:morePage`); the log cursor is exclusive in `pkg/catalog/catalog.go:1504-1515` and the diff
  cursor is the path (`pkg/api/controller.go:4885`); `stop_at` is inclusive exactly as design §4.3 assumes
  (`pkg/catalog/catalog.go:1668-1671`); two-dot `added` means "in right" (`pkg/graveler/committed/diff.go:271`),
  which is the upsert/delete direction the indexer uses; `include_right_stats` is sent and its absence is
  answered by `StatObject` (`pkg/api/controller.go:4865-4877`); 404/403/409/400 are typed and a transport
  error is never a not-found (`TestTransportError`); the unmapped `graveler.ErrNoMergeBase` really is a 500
  whose body contains "no merge base" (`pkg/api/controller.go:3289` reaches the `default` arm), so the
  typed fallback in `lakefs.go:477-481` matches real lakeFS. **No retries exist** in the wrapper, which
  matches the design (each tick resumes from the recorded position); nothing retries forever.
  `FindMergeBase` is called with `(last, head)` and the controller swaps the two arguments before calling
  `catalog.FindMergeBase` (`pkg/api/controller.go:5512`), so the API's `source_commit_id` /
  `destination_commit_id` labels are the reverse of the client's — harmless here because the client reads
  only `base_commit_id` (the merge base is symmetric), but worth knowing if anyone starts using those
  fields.
- **`pkg/config/oss_config.go`** — the `metadata_search` section is absorbed as `SecretSection`, so
  `viper.UnmarshalExact` accepts it and `MapLoggingFields` masks it
  (`pkg/config/struct_fields.go:67`, `pkg/config/oss_config_metadata_search_test.go:120-140`); both
  `iceberg.metadata_search.enabled` and the `LAKEFS_ICEBERG_METADATA_SEARCH_ENABLED` env override are read
  (`config.go:213-262`) and `viper.AutomaticEnv`/`SetEnvKeyReplacer` are installed by the server
  (`cmd/lakefs/cmd/root.go:136-138`).
- **`cmd/lakefs-sso/main.go` wiring** — `MetadataSearchRefMapper = refmap.New` is assigned before
  `Execute` and read only while the router is built, and `refmap.New` degrades to no mapper (logging once)
  when the lister cannot answer `GetCommit`/`GetTag`, which is the documented default. No race
  (`sync.Once`, single-threaded mount).
- **Unbounded memory/IO** — the MDS paths add none: the resolver adds one `GetBranchReference` lookup per
  virtual read, `sign`/`creds` read one pointer and one metadata document, and the client reads one page
  per call. `store.loadMetadata` (`store.go:643-668`) still `io.ReadAll`s a `metadata.json` with no size
  cap; that is a pre-existing catalog property (a `register` can adopt an externally written document),
  not something this feature introduced, so it is recorded here rather than as an MDS finding.

### The fake lakeFS of `pkg/mds/indexer`, `esti/mds_test.go` and `esti/ops/mds-engines`

- **Categories with no finding in this area:** authorization/security of the indexer's own calls (its
  whole lakeFS surface is `pkg/mds/client/lakefs.go`, and the esti suite proves the deny policy and the
  reserved `system` namespace against a real server), concurrency/races inside the indexer (`go test
  -race` over the package plus `TestConcurrencyBoundsBranchesInFlight`), and §4.4's crash-window table
  itself (`TestRecovery` pins every row, including "foreign staged write with the recorded position").
- **`ListBranches`, `ListTags`, `ListObjects`, `DiffRefs`, `DiffBranch` paging and ordering.** The fake
  paginates one sorted slice with `after` = key of the last item (`pageOf`, `fake_test.go:820-836`),
  which is the lakeFS contract (`pkg/api/controller.go:4930` `paginationFor(hasMore, serializedCommits,
  "Id")`, `:6368`, and the object/diff cursors are paths). The real caps agree with the client's
  `DefaultPageSize = 1000` (`pkg/api/controller.go:54`); I probed `pending` at `pageSize` 1, 2 and 3
  with `max_commits = 2` and it applied the oldest commits first, dropped the inclusive `stop_at`
  position exactly once and reported the full lag.
- **Error codes that do match.** Existing branch → 409 (`pkg/graveler/errors.go:45` wraps
  `ErrNotUnique`, `controller.go:3235-3239`); existing repository → 409 (same path); existing tag even
  with `force` → 409 (`errors.go:46`), which is what `fake.CreateTag` returns and what
  `ensureTag`'s delete-then-create works around; empty commit with `allow_empty=false` → 400
  (`ErrNoChanges`, `controller.go:3213`); tag/commit/branch/object 404s; the unmapped
  `ErrNoMergeBase` 500 (`pkg/graveler/graveler.go:3530`) recognised by body in
  `pkg/mds/client/lakefs.go:478-480`.
- **`HardResetBranch` keeping the staging area** (`fake_test.go:463-476` models lakeFS's
  `ResetHard` TODO at `pkg/graveler/graveler.go:2581-2605`), which is what makes
  `TestRewriteWithStagedChanges` a real test of `rewrite`'s reset-before-move order.
- **Real-lakeFS diff/stat semantics the indexer relies on:** the two-dot diff returns the *right*
  size (`pkg/api/controller.go:4857-4866`, `Size` from the right entry) so `rightStatComplete` cannot
  write a left-side size; `SizeBytes == 0` really does mean "stat it" because an empty object and an
  omitted size are indistinguishable; `statObject` does return a JSON body with user metadata
  (`controller.go:5351-5389`), so the fallback works on a real server.
- **`FirstParent`-sensitive code paths that *are* pinned elsewhere:** the merge diff is taken
  against `commit.Parents[0]` (`changes.go:94`), pinned by `TestMergeUsesTheFirstParent` and by the
  esti "merge" case, which also pins the merge commit's `commit_id`/committer/date on a real server.
- **`DiffBranch`-based recovery** (§4.4 "foreign staged write with the recorded position"): the fake's
  synthesised `_lakefs_iceberg/...` change (`fake_test.go:736-745`) stands in for the real staged
  pointer object, and `hasUncommittedChanges` only needs `len(changes) > 0`, so the modelling is
  faithful.
- **`LoadTable`/`CreateTable` ignoring their `dataRepository` argument** (`fake_test.go:772-805`) is a
  real leniency, but I found no call site that passes the wrong name: every call passes the *data*
  repository name and the production store derives `-metadata` itself
  (`pkg/mds/indexer/iceberg.go:50-70`), so it is latent only.
- **The esti suite's own strong assertions** — the four-path `requireAgrees` (path, `commit_id`, etag,
  `size_bytes`, user metadata), the seeded-branch proof from `R-metadata`'s own log, the hard reset
  through a tag, the deny-policy and reserved-`system` cases with a positive control, and the
  100k-object backfill count — do verify what the guide claims for rows, tags, short commit IDs, hard
  reset, permissions and backfill. The gaps above are the parts they do not cover.
