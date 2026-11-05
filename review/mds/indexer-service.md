## Indexer service, repository setup and maintenance

Area: `pkg/mds/indexer/indexer.go`, `repo.go`, `maintenance.go`, `changes.go` (read only where the
tick depends on it), `pkg/mds/config.go`, `health.go`, `metrics.go`, `service.go`,
`cmd/lakefs-sso/mds.go` and their tests. Design sections 3.3, 4.1, 4.2, 4.4, 4.8, 4.9, 7, 9.
Review only: no product code, test, design or doc was changed by this card.

**Verified**

| command | result |
|---|---|
| `go build ./...` | exit 0, no output |
| `go vet ./pkg/mds/... ./cmd/lakefs-sso/...` | exit 0, no output |
| `go test -count=1 -race -timeout 10m ./pkg/mds/... ./cmd/lakefs-sso/...` | `ok pkg/mds 3.262s`, `ok pkg/mds/bootstrap 4.315s`, `ok pkg/mds/client 1.466s`, `ok pkg/mds/indexer 8.397s`, `ok pkg/mds/refmap 5.186s`, `ok pkg/mds/table 9.728s`, `cmd/lakefs-sso no test files` |
| `go tool golangci-lint run ./pkg/mds/... ./cmd/lakefs-sso/...` | `0 issues.` |
| probe `pkg/mds/indexer/probe_review_test.go` (temporary, deleted before the commit), `go test -count=1 -run TestProbe -v ./pkg/mds/indexer/` | 4 probes, all as predicted; output quoted under IO-1…IO-4 |

Everything in the table was run from the worktree root with
`export PATH=$HOME/sdk/go1.25.8/bin:$PATH GOTOOLCHAIN=auto`.

**Findings**

### IO-1 - MAJOR: a second initialisation races into a duplicate `mds: init` commit that is refused as a foreign repository forever

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

### IO-2 - MAJOR: the pending list retains the whole backlog in memory and re-reads it every tick, so cost grows with lag instead of with `max_commits`

**Where** `pkg/mds/indexer/branch.go:418-457` (`pending`; `commits = append(commits, page...)` at
436, `total := len(commits)` at 453) and `pkg/mds/indexer/repo.go:249-270` (`firstParentHistory`,
same shape, no bound). Design §4.2 step 5: "`todo` = first-parent commits of `R@b` after `last` up to
`head`, oldest first, **at most `max_commits`**".

**What happens** `pending` pages the *entire* first-parent log from `head` back to `last` (and to the
root when `last` is not on the chain) and keeps every commit in a slice before truncating to
`x.maxCommits`, so the memory a tick holds is the backlog, not the work:
`concurrency` (default 25) branches × a 1M-commit backlog × `client.Commit`
(`ID`+`Message`+`Parents`+`Metadata map`) is hundreds of MB per branch, several GB per tick, and the
same pages are read again on the next tick (only `max_commits` of them are consumed), so catching up
1M commits with `max_commits: 1000` costs ~10⁹ commit reads. The same pattern at startup:
`firstParentHistory` (`repo.go:249-270`) loads the whole history of `R-metadata`'s default branch into
memory to find a marker that is at one of two positions (`repo.go:161-169`), once per process — and
once per *tick* for a repository that is refused, because `initialised` is never set for it
(`repo.go:110-138`).

**Evidence** probe `TestProbePendingReadsTheWholeLog`: `pageSize: 1`, `max_commits: 2`, nine pending
commits. One tick reads the whole backlog (nine pages) although it applies two commits:

```
probe_review_test.go:137: LogCommits pages: map[repo:9 repo-metadata:11]
probe_review_test.go:138: health: last=s9 head=s9 lag=0
```

`repo` is the data repository: nine `LogCommits` calls = the nine commits from `s9` down to the
already-indexed stop commit `s1`, for `max_commits: 2`. `TestMaxCommitsBoundsATick`
(`branch_test.go`) asserts only how many commits are *applied*, so the bound it "proves" does not
bound the work.

**Suggested fix** Stream: keep the first `limit` commits, count the rest while discarding them (the
exact lag stays exact, memory becomes O(`max_commits`)), and make `firstParentHistory` stream too
(keep the last two commits plus "a marker was seen anywhere"). Optionally cap the lag walk and report
the lag as a lower bound above some multiple of `max_commits`. Test that fails before the fix: with
`pageSize: 1`, `max_commits: 2` and 9 pending commits, the tick must read at most
`max_commits` + the stop commit (= 3 pages here) and retain at most `max_commits` commits.

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

**Checked, no finding**

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
