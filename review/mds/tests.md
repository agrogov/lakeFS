## The fake lakeFS of `pkg/mds/indexer` and what `esti/mds_test.go` + `esti/ops/mds-engines` prove

Area: `pkg/mds/indexer/fake_test.go` (every fake method against the real lakeFS behaviour it stands
for) and `esti/mds_test.go` + `esti/ops/mds-engines` (do they prove what their names, the esti file
header and `docs/metadata-search.md` § "What was verified" claim).

### Verified

| command | result |
|---|---|
| `go build ./...` | ok |
| `go vet ./pkg/mds/...` | ok, no output |
| `go test -count=1 -race -timeout 10m ./pkg/mds/...` | ok: `pkg/mds` 2.9s, `bootstrap` 4.1s, `client` 2.4s, `indexer` 6.1s, `refmap` 7.2s, `table` 9.7s |
| `go tool golangci-lint run ./pkg/mds/... ./esti/...` | `0 issues.` |
| `curl --max-time 3 http://127.0.0.1:8333/`; `docker ps` | `000` (connection refused) and no containers: no `esti-seaweedfs` on `:8333`, so **the esti suite was not run** (it also needs a reachable docker daemon for the PyIceberg/DuckDB client image) |
| temporary probe `pkg/mds/indexer/probe_review_test.go`, `go test -count=1 -run TestProbe -v ./pkg/mds/indexer/` | `ok 1.557s`; output quoted in TS-1, TS-2, TS-3 and TS-5. Probe file deleted before this commit (`git status` is clean) |

### TS-1 - MAJOR: a recorded position that no longer exists fails the branch every tick forever; §4.7's rebuild fallback is not taken

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

### TS-2 - MINOR: `pending` re-reads and retains the whole remaining backlog on every tick, so `max_commits` bounds the writes but not the reads; the multi-page path is covered by no unit test

**Where** `pkg/mds/indexer/branch.go:418-462`.

**What happens** The loop appends every page of the first-parent log until `hasMore` is false and only
then reverses it and cuts to `limit` (`branch.go:446-461`). Because `StopAt: last` is inclusive, a
branch that is `N` commits behind makes the *server* return `N+1` commits, so one tick reads
`ceil((N+1)/1000)` pages and keeps all `N+1` `client.Commit` values resident before applying at most
`DefaultMaxCommits = 1000` of them (`indexer.go:101`). A backlog of 1M commits — e.g. a week of
60s ticks at 1000 commits/tick, or a repository added to the configuration late — costs ~1000 API
calls and a ~1M-element slice on *every* tick, and the next tick pays the same cost for `N-1000`.
`docs/metadata-search.md` "Per-tick work" reads as if `max_commits` bounds a tick; it bounds only the
applies. Nothing exercises the paged path either: every branch/repo test leaves `fake.pageSize` at 0
(`fake_test.go:31-32`, `pageOf` at `fake_test.go:820-836` returns one page), so `pending` is only ever
run with a single page — although a probe shows the paged walk is currently correct:

```
PROBE paged pending pageSize=1 err=<nil> last=s3 lag=3 paths=[a s2 s3]
PROBE paged pending pageSize=1 after 3 ticks err=<nil> last=s6 lag=0 paths=[a s2 s3 s4 s5 s6] tags=6
```

**Evidence** `branch.go:446-461`; `grep -n pageSize pkg/mds/indexer/*_test.go` → only
`changes_test.go:166` and `maintenance_test.go:150`, never a branch/repo test.

**Suggested fix** Keep a ring buffer of the last `limit` commits while walking (and count `total`
incrementally), so memory is `O(max_commits)`; add a `pageSize = 1`, `max_commits = 2`, five-pending-
commit test (a benchmark with `-benchmem` on the old code fails the allocation bound).

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

### Checked, no finding

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
