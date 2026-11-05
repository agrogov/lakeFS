## Indexer pipeline and indexer service (`pkg/mds/indexer`, `pkg/mds/client`, `pkg/mds/{config,health,metrics,service}.go`)

Scope: every commit after `e2333be26` that touches this area — `6440e2f13` (IX-1, IO-2, TS-2, TS-1, IX-5),
`7f952279b` (IX-2, TB-3), `db8172798` (IO-1, TS-5), `98f035d34` (IO-3…IO-7), `f884a0627` (IX-3, IX-4, IX-6,
IX-7, TS-3, TS-4, TS-10), plus the `pkg/mds/table` half of IX-2 (`table/timeoutTransport`). Documentation-only
commits (`fe1e3bbf5`, `df9744488`) were read for the claims they record, not reviewed as code.

**0 BLOCKER, 1 MAJOR, 3 MINOR, 0 NIT.** No fix fails to remove its finding, and every fix test I reverted
really goes red without its fix (table below). The MAJOR is a defect *introduced* by the IO-1 fix; two of the
MINORs are consequences of the IX-1/TS-1 and TS-4 fixes.

### Verified

| Command | Result |
|---|---|
| `export PATH=$HOME/sdk/go1.25.8/bin:$PATH GOTOOLCHAIN=auto; go version` | `go version go1.26.4 darwin/arm64` (toolchain auto) |
| `go build ./...` | pass — exit 0, no output (17.4 s) |
| `go vet ./pkg/mds/...` | pass, no output |
| `go test -count=1 -race -timeout 10m ./pkg/mds/...` | all `ok` — `pkg/mds` 4.417s, `bootstrap` 4.471s, `client` 1.406s, `indexer` 13.506s, `refmap` 7.619s, `table` 15.788s |
| `go tool golangci-lint run ./pkg/mds/...` | `0 issues.` |
| `git log --oneline e2333be26..HEAD` | 16 commits: 2 docs-only (`fe1e3bbf5`, `df9744488`), 2 merges of `feature/iceberg-catalog` (`224fc82d3`, `a397ed971`), 12 code commits; all read. 5 of them are this area's fixes — `6440e2f13`, `7f952279b`, `db8172798`, `98f035d34`, `f884a0627` — and `7f952279b` also carries the `pkg/mds/table` half of IX-2 |
| Revert-and-restore: `if total >= limit+pageSize` → `if false && …` in `pending` | `TestFarBehindBranchIsRebuiltInsteadOfWalked` FAILs (`expected: 2, actual: 21` LogCommits requests) — restored |
| Revert: `case errors.Is(err, client.ErrNotFound)` in `continueFrom` → `case false` | `TestCollectedPositionIsRebuilt` FAILs — restored (`TestCollectedPositionDuringTheWalkIsRebuilt` still passes: it pins the `stop_at` half, which is a separate code path) |
| Revert: `AllowEmpty: false` → `true` in `initialise` | `TestConcurrentInitialisationCommitsOnce` FAILs (two `mds: init` commits: `index-2`, `index-3`) — restored |
| Revert: drop `retireUnfollowedBranches` + `ClearLag` from the non-matching loop | `TestDeletedBranchLeavesHealthWithoutDeletingTheMirror` and `TestBranchStopsMatchingClearsItsLagGauge` FAIL — restored |
| Revert: `if meta.ReadOnly` → `if false && meta.ReadOnly` | `TestReadOnlyMetadataRepositoryFailsTheRepository` FAILs — restored |
| Revert: remove the `lastIndexed` read-back in `indexBranch` (IO-4) | `TestFailedBranchReportsTheRecordedPosition` FAILs — restored |
| Revert: `tableHandle.refresh` → `TableStore.LoadTable` (IX-3) | `TestReloadReusesTheBranchCatalog` FAILs (`expected: 1, actual: 3` `/v1/config`) — restored |
| Revert: `http.Client{Timeout: requestTimeout}` → `Timeout: 0` (IX-2) | `TestTickAgainstAnAPIThatNeverAnswers` FAILs after 10 s (`the tick did not return within 10s`) — restored |
| Revert: re-add the 200/500 compaction defaults in `applyDefaults` (IO-6) | `TestCompactionDisabledByZero/max_delete_files:_0` and `/every_commits:_0` FAIL — restored |
| Revert: drop `TickCanceller.CancelTick` from `tickRepository` (IO-5) | `TestCancelledTickIsNotLeftInProgress` FAILs — restored |
| Probe 1 (`zz_probe_review_test.go`, deleted before committing): persistent non-`ErrNoChanges` 400 on the init commit | **26 initialisation attempts in one tick**, growing wrapped error — see R2I-1 |
| Probe 2 (same file): request counts of a caught-up tick vs the backfill tick | `FindMergeBase=1 LogCommits(R)=0 GetCommit=3 (backfill tick: mergebase=0 logs=1 gets=3)` — see R2I-2 |
| Probe 3 (same file): transient 404 on `FindMergeBase`, branch already at its head | `branch error=merge base of repo@s1..s1: find merge base "s1": lakeFS resource not found`, `lag=-1` — see R2I-2 |
| `git status --porcelain` after every revert | clean apart from the probe file; `git diff` empty when the fragment was written |

TS-5 (`initScan` streaming) and TS-3/TS-10 (fake writes, `FirstParent`, best common ancestor) were checked by
reading the code and the real implementations (`pkg/graveler/graveler.go:2239-2260`, `:2581-2605`, `:2637-2680`,
`pkg/graveler/errors.go:16-46`, `pkg/api/controller.go:3196-3240`, `pkg/graveler/ref/commit_generation_priority_queue.go`,
`pkg/graveler/ref/merge_base_finder.go`) rather than by reverting: TS-5's revert is a rewrite of the function,
and TS-3/TS-10 only change the fake, so their "test fails before the fix" claim is about the fake, not product code.

### R2I-1 - MAJOR: a 400 that is not "no changes" re-enters the whole initialisation recursively, without a bound

**Where** `pkg/mds/indexer/repo.go:327-339` (`initialise`'s `errors.Is(err, client.ErrBadRequest)` arm),
reached again from `pkg/mds/indexer/repo.go:196-203` (`verifyInitCommit` → `freshAutomaticRoot` → `initialise`).
Introduced by `db8172798` (IO-1).

**What happens** `initialise` now maps **every** HTTP 400 from the initialisation commit to "a concurrent
initialisation committed first": it calls `verifyInitCommit`, which for a companion repository that still shows
only lakeFS's automatic `Repository created` root takes the `freshAutomaticRoot()` arm and calls `initialise`
straight back. When the 400 is `graveler.ErrNoChanges` that is correct and terminates on the second pass (the
winner's marker is now on the branch). When the 400 is anything else, the two branches never exit: the same
several requests and the same commit are retried from a new stack frame, forever.

`POST /repositories/{r}/branches/{b}/commits` answers 400 for `ErrNoChanges` **and** for
`block.ErrWriteFailed` (`pkg/api/controller.go:3225`), `validator.ErrInvalidValue`/`graveler.ErrInvalidValue`
(`pkg/api/controller.go:3228-3230`), `graveler.ErrCommitMetaRangeDirtyBranch`, and so on. A local blockstore
failing to write the metarange (`pkg/block/local/adapter.go:177` returns `block.ErrWriteFailed`) is a realistic
persistent trigger; so is any HTTP intermediary or lakeFS version that answers a 400 to the commit.

Consequences, all in one goroutine and one tick:

- unbounded recursion: each pass adds a frame and, worse, re-wraps the previous error
  (`"commit the initialisation of …: %w; the branch is not initialised either: %w"`), so the string grows by one
  copy of itself per level — O(depth²) bytes held and a monstrous `/_health` `last_error`. The process
  eventually dies of stack exhaustion (`goroutine stack exceeds 1000000000-byte limit`), taking every
  repository of the tick loop with it, or of memory first.
- `/_health` never turns 503 in the meantime: the whole tick is stuck inside `ensureMetadataRepository`, i.e.
  the repository is merely "in progress" — exactly the IX-2/IO-5 shape.
- the failure is *hidden* on the first pass: a single spurious 400 is silently retried instead of failing the
  repository, which is the opposite of what the other repository-level failures do.

**Evidence** Probe 1 (a `client.LakeFS` wrapper that answers `client.ErrBadRequest` with a
`block: write failed` message to every `mds: init` commit, and gives up after 25 attempts so the test can
finish):

```
PROBE: a persistent 400 on the initialisation commit caused 26 initialisation attempts in one tick
log: mds: repository skipped error="commit the initialisation of \"repo-metadata\"@main: commit \"repo-metadata\"@main: block: write failed: lakeFS request rejected; the branch is not initialised either: commit the initialisation of \"repo-metadata\"@main: commit … (26 nested copies)"
```

Each attempt also re-ran `CreateTable`/`LoadTable` (the `ErrTableExists` arm) and one `LogCommits` page.
`logrus` printed `mds: completing the initialisation of a newly created metadata repository` 26 times.

**Suggested fix** Treat the 400 as "lost the race" only if the re-verification *accepts* the branch **and** the
commit it adopts is different from the one the failing `initialise` tried to make; if `verifyInitCommit` comes
back with `freshAutomaticRoot` (nothing to adopt) return the original 400 instead of initialising again — or,
equivalently, give `initialise` an `attempt` counter of 1 and refuse to recurse. Test that fails before it: the
probe above, asserting the repository error contains `block: write failed` and that `CreateTable` was called
once, not 26 times.

### R2I-2 - MINOR: every caught-up branch now pays one `FindMergeBase` per tick, and a transient 404 on it fails the branch

**Where** `pkg/mds/indexer/branch.go:98-104` (`runBranchOnce` calls `continueFrom` unconditionally) →
`branch.go:172-174` (`FindMergeBase`). Introduced by `6440e2f13`: the guard `if last != head.ID` around the
step-4 merge-base read was dropped when the block was extracted into `continueFrom`
(`git show e2333be26:pkg/mds/indexer/branch.go` line 86 (and 110, for the todo walk) has the guard; the new `continueFrom` has no
equivalent).

**What happens** Design §4.2 step 3 says a branch whose recorded position equals its head has "nothing to apply;
the tag repair … and the 4.9 compaction check still run", and step 4 only asks whether `last` is an ancestor of
`head` — which is trivially true when they are the same commit. The code now asks lakeFS anyway, on every tick,
for every branch that is already up to date, i.e. the steady state of a healthy indexer: +1 API request per
branch per tick (10 000 branches at `period: 60s` ⇒ 10 000 extra requests a minute) and one extra chance to
fail.

The second half is a real wrong answer, not just cost: a 404 from that unnecessary call makes
`continueFrom` ask `positionGone(last)`, which is false (the position is the head, and it exists), so the branch
is reported failed — `/_health` 503 with `lag_commits: -1` — for a branch that had nothing to do. Before the
change the same transient answer touched nothing. (`TestNotFoundThatIsNotThePositionIsAFailure` covers the
"there is work to do" case, not this one, so the suite is green.)

**Evidence** Probe 2 (counting `client.LakeFS` wrapper, two ticks, second one with no new commits):

```
PROBE caught-up tick: FindMergeBase=1 LogCommits(R)=0 GetCommit=3 (backfill tick: mergebase=0 logs=1 gets=3)
```

Probe 3 (same env, first tick indexes `s1`, then `findMergeBaseErr = &client.NotFoundError{…"s1"}` and a second
tick):

```
mds: branch not indexed … error="merge base of repo@s1..s1: find merge base \"s1\": lakeFS resource not found" head=s1 lag=-1 last=s1
PROBE idle branch, transient 404 on findMergeBase: branch error=merge base of repo@s1..s1: find merge base "s1": lakeFS resource not found
```

**Suggested fix** Restore the guard inside `continueFrom`: `if last == head.ID { return last, nil }` before the
`FindMergeBase` call (the caller already skips `pending` in that case). Test that fails before it: a second tick
with no new commits and a counting client, asserting zero `FindMergeBase` calls, plus a variant that injects
`findMergeBaseErr` on that tick and asserts the branch stays healthy.

### R2I-3 - MINOR: TS-4's fix does not see a COMMIT-protected companion branch — the review's claim that it does is wrong, and the state it was supposed to cure is unchanged

**Where** `pkg/mds/indexer/repo.go:70-74` (`ensureMetadataRepository` reads only `meta.ReadOnly`), versus
`MDS_REVIEW.md` TS-4 ("a read-only (or COMMIT-protected) `R-metadata` fails the repository once per tick, with a
message naming both causes") and `METADATA_SEARCH_DESIGN.md` §7 ("Per-repository runtime errors (… companion
repository read-only) mark that repository failed in `/_health`").

**What happens** A branch-protection rule that blocks `COMMIT` on `R-metadata@main` is not visible in the
repository record, so the check never fires: `graveler.Commit` returns `ErrCommitToProtectedBranch`
(`pkg/graveler/graveler.go:2245-2252`, wrapping `ErrProtectedBranch`, `pkg/graveler/errors.go:21`), which
`pkg/api/controller.go:3196-3200` maps to **403**. Every tick, every branch therefore fails with the same
`cannot commit to protected branch` 403, `/_health` stays 503 (and now with `lag_commits: -1`, hiding the real
lag), tag retention never runs, and the error the operator sees is the indexer's wrapper, not the message that
names the cause and the fix. That is precisely the state TS-4 described; the fix removed it only for the
`read_only: true` half. The message that "names both causes" exists only inside the `ErrReadOnlyMetadataRepository`
error, which a protected branch never produces.

No test can see it: the fake has no branch-protection model (`fake_test.go` `requireWritableBranchLocked` only
checks repository existence, `ReadOnly` and branch existence), so `TestReadOnlyMetadataRepositoryFailsTheRepository`
pins the flag and nothing else.

**Evidence** `grep -n "ReadOnly\|Protect" pkg/mds/indexer/repo.go` → only `meta.ReadOnly` at `:70`; the design
sentence quoted above; `pkg/graveler/graveler.go:2245-2252` (protection is checked *before* `ReadOnly` and before
the branch lookup) and `:2254-2257` (`ReadOnly` → `ErrReadOnlyRepository`).

**Suggested fix** Widen the repository-level refusal to the protection case: in `runBranchOnce`'s write path (or
in `ensureMetadataRepository`, over the branches the tick is going to write) detect a 403 from a write to
`R-metadata` and answer an error of the `ErrReadOnlyMetadataRepository` family that names branch protection, or
drop the "(or COMMIT-protected)" parenthetical from MDS_REVIEW and design §7. Test that fails before it: a fake
repository flagged protected whose `Commit`/`ResetBranch` answer
`ErrForbidden("cannot commit to protected branch")`, asserting one repository-level error instead of a per-branch
403 on every tick.

### R2I-4 - MINOR: the initialisation commits whatever table is already staged, so the marker is sealed on top of a table the indexer did not create

**Where** `pkg/mds/indexer/repo.go:303-306` (`ErrTableExists` → `LoadTable`, no property check),
`repo.go:325` (`AllowEmpty: false` commit of *that* staging), `pkg/mds/table/writer.go:118-125`
(`repositoryOf` is read, never compared) versus `writer.go:152-163`/`:254-265`/`:333-340` (the value is only
passed to `SnapshotProperties`).

**What happens** §4.8's refusal rule is about a repository "the indexer did not create", and MDS-SEC-3 added the
`lakefs.mds.repository` proof for the *read* side because "whoever can create a repository named `R-metadata`
before the indexer does … owns the table". The write side has no equivalent: the catalog's table creation
**stages** the pointer, so a principal that stages `system.object_metadata` on a fresh `R-metadata@main` before
the indexer's first tick supplies the table metadata (schema, `location`, properties) that `initialise` then
*commits* as the `mds: init` commit — and after that the branch is "initialised", so no later tick re-checks
anything. Two outcomes, both bad and both permanent:

- the adopted table carries no `lakefs.mds.repository`: `repositoryOf` (`writer.go:118-125`) fails on the first
  `Rebuild`/`ApplyDelta`, so the repository is initialised *and* unindexable for ever (a manual
  `delete R-metadata` is the only way out, which is the failure mode §4.8 exists to avoid);
- it carries the property (any value): the indexer writes its rows into a table whose `location` and schema come
  from that metadata document, with the server's own storage credentials, and every snapshot it adds records the
  attacker-chosen `lakefs.mds.source-repository`.

The threat model is the same as MDS-SEC-3's, one step earlier: it needs `catalog:*` on `R-metadata`, which the
§8 deny covers only for the caller's guard groups. The defect predates the fixes (the old code adopted the same
staging with `AllowEmpty: true`), but `db8172798` is the commit that made the initialisation commit "the
idempotency point" without making it the ownership point, and no test covers it — the fake cannot even express
it, because `fakeTable.Rebuild` replaces the whole `fakeState` (`fake_test.go`, `newState()`), so the foreign
table's "metadata" disappears in the fake and the test passes for the wrong reason.

**Evidence** code path above; `pkg/mds/table/schema.go:227-232` (`SnapshotProperties` is the only consumer of
`repositoryOf`'s answer); `MDS_REVIEW.md` MDS-SEC-3 for the read-side counterpart;
`pkg/mds/indexer/fake_test.go` `Rebuild`/`CreateTable` for the missing modelling (one `tableExists` bool, no
schema/location/property).

**Suggested fix** In the `ErrTableExists` arm, load the staged table and refuse to commit it unless
`tbl.Properties().Get(table.PropertyRepository) == repository` (and, if cheap, the schema's field IDs match
`table.Schema()`); answer `ErrForeignRepository` otherwise, as §4.8 does for a foreign history. Test that fails
before it: a table store whose `LoadTable` reports a table with a different/absent `lakefs.mds.repository`,
asserting `ErrForeignRepository` and no `mds: init` commit.

### Checked, no finding

- **The bounded `pending` walk and its exact lag** (`branch.go:496-575`): the window keeps the oldest
  `limit+1` commits and only its tail can be the todo; `total` counts commits per page and drops the inclusive
  `stop_at` commit exactly once; `metStop` can only be true on the last page because `pkg/catalog`'s
  `foundAllCommits`/`logCommitsResult` (`pkg/catalog/catalog.go:1665-1690`) stop the walk *at* `stop_at` and
  truncate to `Amount`, so a page containing the stop commit never reports `hasMore`; the
  `total >= limit+pageSize` give-up is the documented boundary and not off by one — the server reads
  `Amount+1` commits to set `has_more`, so a position at depth `max_commits + page` is *inside* the pages read
  and is walked, and only depth `> max_commits + page` rebuilds. The lag after a rebuild is 0 because the
  position becomes the head, and after a failed apply the per-commit decrements leave the exact remainder
  (`TestMaxCommitsBoundsATick`, `TestPendingPagesTheLog`, `TestFailedBranchReportsThePositionItAdvancedTo`).
  The fake's paging (`pageOf`) and its `stop_at` dereference (`404` on a missing stop) match
  `pkg/catalog/catalog.go:1488-1497` and `logCommitsResult`, so the paging tests are not fake-only artefacts.
- **The vanished-position rebuild** (`branch.go:172-235`): a 404 is only taken as "the position is gone" after
  `positionGone` → `GetCommit(repository, position)` also answers 404, and `GetCommit` resolves a full commit ID
  (`pkg/api/controller.go:4080-4099` → `catalog.GetCommit` → `dereferenceCommitID`), so a 404 about the head, the
  repository listing race or a flaky intermediary still fails the tick
  (`TestNotFoundThatIsNotThePositionIsAFailure`). The recursion guard for a *double* transient 404 is weak, but a
  rebuild in that case yields correct rows at the head (`§4.7`), not corruption — I could not construct a wrong
  answer from it, only cost, so it is not reported separately.
- **Concurrent initialisation** (`repo.go:298-345`, `:127-160`): the staging area is shared server-side, so the
  loser's `Commit` finds nothing to commit and gets `ErrNoChanges`; `initScan` accepts the winner's run and the
  branch continues on top of the branch head (`TestConcurrentInitialisationCommitsOnce`,
  `TestDuplicateInitCommitsAboveTheAutomaticRootAreAccepted`, `TestInitMarkerOutsideTheRunIsStillRefused`). The
  `initScan` acceptance rule (`:216-290`) matches §4.8's two shapes, refuses a marker below a gap and a marker
  above a hand-made root, and is O(1) in memory (`TestInitialisationCheckKeepsTheHistoryOutOfMemory`).
- **`request_timeout`** (`config.go:163,250-252,296-298,350-353`; `indexer.go:728-745`;
  `table/catalog.go:151-215`): 60 s default, `0`/negative refused at startup, `0` in `indexer.Options` means "the
  default", not "no bound"; the API client uses `http.Client.Timeout` (covers body reads), the catalog a
  per-request `context.WithTimeout` in a `RoundTripper` (covers the eager `GET /v1/config` and every later call).
  A timeout is not typed as 404/409 (`checkStatus` is only reached on a response), so it is never mistaken for a
  missing position or a conflict, is not retried by the client, and fails the branch or the repository.
  A timeout *after* the server committed the staging or the lakeFS commit lands exactly in §4.4's recovery
  windows (row 1 via the staged property, row 4 via `ensureTag`), which I traced through `recoverStaging` and
  `ensureTag`; data-plane writes are iceberg-go IO and are documented as out of scope. No long operation in this
  area (a 1000-entry diff page with `include_right_stats`, a 50 000-entry manifest staging commit) plausibly
  exceeds 60 s at the documented defaults.
- **Per-tick catalog reuse** (`branch.go:232-240`, `iceberg.go:110-120`): `Table.Refresh` calls the handle's own
  catalog (`iceberg-go table/table.go:225-250`), so a tick pays one `/v1/config` and keeps one connection pool;
  `TestReloadReusesTheBranchCatalog` counts exactly 1 on an in-process lakeFS for a two-commit tick and fails at 3
  when reverted. `Refresh`'s extra UUID check (`validateTableUUID`) only rejects a table that was dropped and
  recreated under the handle, where "load a new table handle" is the right advice and the next tick does exactly
  that.
- **Health/lag on deleted and unfollowed branches** (`indexer.go:496-536`, `:600-635`; `health.go:188-215`): the
  `/_health` entry and the lag series of a branch that is no longer followed are both cleared whatever
  `delete_branches` says; the cleanup is derived from the previous tick's followed set, so it also reaches a
  branch that failed before its mirror existed; a cancelled tick ends `in_progress` without inventing an outcome
  (`CancelTick`), and `BeginTick` deliberately keeps the previous error. `lag_commits: -1` is published for a
  failure before the count and read back from durable state (`TestFailedBranchReportsTheRecordedPosition`,
  `TestDeletedBranchLeavesHealthWithoutDeletingTheMirror`, `TestCancelledTickIsNotLeftInProgress`).
- **Compaction config semantics** (`config.go:109-126,366-380`, `indexer.go:418-441`): `0` disables
  `max_delete_files`/`every_commits`/`tag_retention`, a negative value is refused with one error per key, and the
  200/500/0s defaults are applied only when the key is absent (`viper.IsSet`), so an explicit `0` survives
  `LoadConfig` and reaches `compactIfNeeded`'s `> 0` guards. The trigger order (free delete-file check first),
  `indexedSinceCompaction`'s durable count and the "compaction adds no tag" rule are unchanged and still hold.
- **The stricter fake vs real lakeFS** (TS-3/TS-10): `Commit`/`ResetBranch`/`HardResetBranch` return a typed 404
  for a missing branch or repository in the same order as graveler (`pkg/graveler/graveler.go:2245-2257`,
  `:2581-2605`, `:2637-2650` — protection and `ReadOnly` before the branch lookup, `getRepository` before all of
  them), the default-branch delete is 403 (`ErrDeleteDefaultBranch`, `pkg/api/controller.go:3196-3200`), the
  `force` flag never overwrites a tag, and `FindMergeBase` reproduces `pkg/graveler/ref/merge_base_finder.go`
  including the generation/newest-first queue tie-break (`commit_generation_priority_queue.go:24-32`) and
  `ErrNoMergeBase` for an unrelated history (`pkg/graveler/graveler.go:3530-3532`). `LogCommits` with
  `FirstParent: false` is breadth-first and therefore not graveler's order, which the code states and no
  production path observes.
- **`pkg/mds/client`**: unchanged by these commits; re-read for the paths the fixes lean on — `checkStatus` never
  turns a transport error into 404/409, `morePage` refuses "more" without a cursor, the log/diff cursors are the
  last returned ID/path, and `errorMessage` is bounded. No finding.
- **`pkg/mds/service.go`**: only the `RequestTimeout` line changed in this range; `tickUntilDone` still starts one
  tick at a time, abandons (rather than waits for) an in-flight tick on stop, and the HTTP server keeps its 10 s
  read-header and shutdown bounds. The abandoned goroutine writes only through mutex-guarded sinks, so `-race`
  over the package is clean. No finding.
- **`pkg/mds/metrics.go`**: only the dead stage constants were removed; all seven §9 series remain and the
  `LagUnknown` value is documented. No finding.
- **Crash/restart safety of the new fallbacks**: `rebuildAtHead` → `rewrite` discards staging before the hard
  reset (`graveler.ResetHard` keeps it), and a crash between the hard reset and the rebuild is recovered by
  `startBranch` (head metadata has no `lakefs.mds.source-commit` ⇒ `last == ""`), which re-runs the rebuild from
  the staged position instead of double-applying; the 409 restart path is unchanged and still bounded at
  `conflictRetries = 3`. No idempotency hole found.
