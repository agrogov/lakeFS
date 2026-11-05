# Indexer pipeline: per-branch tick, change sets, Iceberg commit and staging recovery

Scope: `pkg/mds/indexer/branch.go`, `changes.go`, `iceberg.go`, `tables.go` and their tests, against
`METADATA_SEARCH_DESIGN.md` §3.2, §4.1–§4.7 and §6, and against the real `pkg/graveler`, `pkg/catalog`,
`pkg/api` and `pkg/icebergcatalog` behaviour they rely on.

## Verified

| Command | Result |
|---|---|
| `go build ./...` | pass |
| `go vet ./pkg/mds/...` | pass |
| `go test -count=1 -race -timeout 10m ./pkg/mds/indexer/` | `ok github.com/treeverse/lakefs/pkg/mds/indexer 3.806s` |
| `go tool golangci-lint run ./pkg/mds/indexer/...` | `0 issues.` |
| Probe 1 (recorded below): real in-process lakeFS, crash after step 3 | recovery completes steps 4–5, no re-apply, no reset, staging clean |
| Probe 2 (recorded below): real in-process lakeFS, foreign staged write at the recorded position | staging discarded, commit applied, no foreign row survives |
| Probe 3 (recorded below): two real catalog handles, same delta | second commit gets `CommitFailedException` (409), no duplicate rows |
| Probe 4 (recorded below): fake, `max_commits=1`, 40 pending commits | 23 log calls / 45 commits read per tick, twice |
| Probe 5 (recorded below): server that never answers | a tick is still in progress after 3s |

Every probe file was deleted before this commit; the tree carries no test or product change.

### §4.4 crash table, walked point by point

Checked against the code and, where the fake could hide a difference, against a real lakeFS (probes 1–3).

| Crashed after | Code path | Verdict |
|---|---|---|
| 3 (staged, property = next commit `C`) | `recoverStaging` first branch, `branch.go:378-383` → `finishCommit` (steps 4–5) | ✔ probe 1: head `source-commit` = `C`, `mds-<C>` created, rows `[a c]`, `DiffUncommitted` 8 → 0 |
| 3, staged property is anything else | `branch.go:384-389` → `resetStaging` | ✔ `TestRecovery/foreign_staged_property` |
| 3, foreign staged changes, property = `last` | `branch.go:390-398` → `hasUncommittedChanges` (`DiffBranch`) → `resetStaging` | ✔ probe 2: 6 staged entries seen, rows after the tick `[a b]`, no `stale` |
| 4 (lakeFS commit done, no tag) | `ensureTag` (`branch.go:617`) at the top of the tick, before any write | ✔ `TestRecovery/crashed_after_the_lakefs_commit[_with_a_mispointed_tag]`; the delete-then-create order matches `pkg/graveler/graveler.go CreateTag` (`ErrTagAlreadyExists`, force only bypasses the read-only check) |
| during 3 (files written, nothing staged) | nothing staged, `hasUncommittedChanges` false, normal indexing | ✔ orphans are left to `lakectl iceberg cleanup` (§9) |
| Iceberg commit outcome unknown (`CommitStateUnknownError`, 500) | `iceberg.go:183-188` (`tableError`) leaves it unwrapped → branch fails this tick → next tick sees the staged pointer and takes the row-1 path | ✔ by construction (not covered by a test) |
| 409 from the Iceberg commit | `iceberg.go:183-188` (`tableError`) → `ErrConflict` → `runBranch` restarts, `conflictRetries = 3` (`indexer.go:112`) | ✔ `TestRecovery/a_concurrent_writer_moves_the_table`, `two_consecutive_conflicts` |
| staged inside a rewrite | `rewrite` calls `resetStaging` before `HardResetBranch` (`branch.go:472`) | ✔ `TestRewriteWithStagedChanges` (fake keeps staging on `HardResetBranch`, like `graveler.ResetHard`) |

Also checked: crash after `rewrite`'s hard reset but before the rebuild (next tick recomputes from the new
head, which records the merge base), crash after `startBranch`'s rebuild but before its commit (staged
property = `source.ID` → rebuild skipped, commit done), crash between `ensureTag`'s delete and its create
(next tick recreates the missing tag). All recover.

## Findings

### IX-1 - MAJOR: `pending` reads and retains the whole remaining first-parent log on every branch tick

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

## Probes (temporary, deleted before this commit)

All five lived in package `indexer_test` (probe 3 in `table_test`) and used the repository's own fixtures
(`newMDSServerEnv` in `pkg/mds/indexer/server_test.go`, `newTestEnv` in `pkg/mds/table/server_test.go`): a real
lakeFS catalog and block adapter over HTTP, not the fake.

**Probe 1 — `pkg/mds/indexer/probe_final_test.go`, `TestProbeCrashAfterIcebergCommit`.** Index `main` at a
first commit, add a second commit that deletes `b` and adds `c`, then stage the delta of that commit with a
real `table.ApplyDelta` (exactly what `applyCommit` does before its lakeFS commit) and run one more tick.

```
staged property on a fresh LoadTable: source-commit="58f22328f0467bb8427fb1ce8089429e266db07d7ff8c14432a626396da06bed" (next commit is "58f22328f0467bb8427fb1ce8089429e266db07d7ff8c14432a626396da06bed")
uncommitted diff of R-metadata@main: 8 entries
after the next tick: head source-commit="58f22328f0467bb8427fb1ce8089429e266db07d7ff8c14432a626396da06bed" message="mds: index server-data@58f22328f0467bb8427fb1ce8089429e266db07d7ff8c14432a626396da06bed"
after the next tick: tag mds-<second> -> commit 9fdea70b7fc7a32751a527ba567ceb09bebca55e96e204a49b0828ee2c35da94 err=<nil>
staged rows before the tick: [a/d552c06fdfb6e3b6026730b3a79116bb56d69cbe4433572c8049445ec0a8340c c/58f22328f0467bb8427fb1ce8089429e266db07d7ff8c14432a626396da06bed]
rows after the tick: [a c] (want [a c])
after the tick: uncommitted diff entries=0
```

The 8 uncommitted entries are the staged table pointer (`_lakefs_iceberg/tables/system/object_metadata/.pointer`,
type changed) plus two data files, two manifest files, one manifest list and one `metadata.json`.

**Probe 2 — same file, `TestProbeForeignStagingAtRecordedPosition`.** A foreign staged write that keeps the
recorded position (property = the *already indexed* commit, plus a bogus `stale` row), then one tick.

```
uncommitted diff with a foreign staged write at the recorded position: 6 entries
rows after the tick: [a b] (want [a b], no stale); head source-commit="6487171ccbd1bd24da4a14daad44120c87a59e564dc4b2561a32e9d50cfd5757" (want "6487171ccbd1bd24da4a14daad44120c87a59e564dc4b2561a32e9d50cfd5757")
```

**Probe 3 — `pkg/mds/table/probe_replay_test.go`, `TestProbeConcurrentReplayDuplicates`.** Two handles loaded
from the same snapshot, the same `ApplyDelta` on both (a second replica or a replay inside iceberg-go's
refresh-and-replay would show as duplicate rows).

```
A committed: snapshots=2 source=c1
B err=commit the delta at c1: CommitFailedException: the requirement "assert-ref-snapshot-id" is not satisfied by the current table
rows for path p = 1 (total rows 1)
  row path="p" commit="c1" size=1
```

**Probe 4 — `pkg/mds/indexer/probe_final_test.go`, `TestProbePendingReadsTheWholeLog`.** A counting wrapper
around the fake's `LogCommits`, `pageSize = 2`, `max_commits = 1`, 40 commits after the indexed position.

```
max_commits=1, lag=40, page=2: LogCommits calls=23, commits read=45, applied=1
second tick: LogCommits calls=23, commits read=45, applied=1, lag reported=38
```

**Probe 5 — same file, `TestProbeNoTimeout`.** `indexer.New` against an `httptest` server that accepts the
request and never answers, one `Tick` in a goroutine.

```
after 3s against a server that never answers the tick is still in progress: neither client has a timeout
```

## Checked, no finding

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
