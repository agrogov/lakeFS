## pkg/mds/indexer: bounded re-initialisation, the caught-up guard, the 403 diagnosis, the foreign staged table, and the new table errors as the indexer meets them

Range reviewed: `da24d421f` (indexer fixes), `26a66b633`, `500a00a37`/`8df39cabd`, `2af2532b7`, `d993d602e`
(table, for the three new errors the indexer consumes) and the docs commit `b4c8c24ba`, on top of the
consolidated fix review `902839f67`. MDS_REVIEW_2.md's R2I-1…R2I-4 are the findings under test; nothing
already recorded there is re-reported except where a fix is incomplete or has introduced a new defect.

Product code in this area changed in **one** commit: `da24d421f` touches `pkg/mds/indexer/repo.go`
(+80/−8), `pkg/mds/indexer/branch.go` (+24/−11), `pkg/mds/indexer/fake_test.go` (+100/−19) and the two test
files (+173/−2). The rest of
`pkg/mds/indexer` (`indexer.go`, `changes.go`, `tables.go`, `iceberg.go`, `maintenance.go`) is untouched, which
is what makes the integration question of R3I-3 answerable.

### Verified

| Command | Result |
|---|---|
| `go build ./...` (Go 1.26.4 via `PATH=$HOME/sdk/go1.25.8/bin:$PATH GOTOOLCHAIN=auto`) | pass, exit 0, no output |
| `go vet ./pkg/mds/indexer/... ./pkg/mds/table/...` | pass, exit 0 |
| `go test -count=1 -race -timeout 10m ./pkg/mds/indexer/` | `ok … 10.309s` |
| `go test -count=1 -timeout 10m ./pkg/mds/table/` | `ok … 7.950s` |
| `go tool golangci-lint run ./pkg/mds/indexer/...` | `0 issues` |
| **Reversion proof R2I-1** — restore `verifyInitCommit(…, true)` in the 400 arm and delete the `!state.initialised` guard in `repo.go`, then `go test -run TestPersistentBadRequestOnTheInitialisationCommitDoesNotRecurse` | **FAIL** as claimed: `expected: 1 actual: 26`, and the tick's error is 26 nested copies of `commit the initialisation of "repo-metadata"@main: … block: write failed …; the branch is not initialised either: …` ending in `probe: gave up after 26 initialisation attempts`. Restored (`git diff` empty) |
| **Reversion proof R2I-2** — delete the `if last == head.ID { return last, nil }` guard in `continueFrom`, then `go test -run TestCaughtUpBranchDoesNotReadTheMergeBase` | **FAIL**: `Should be zero, but was 1 — a branch at its head makes no merge-base read`. Restored |
| **Reversion proof R2I-3** — make `metadataBranchWriteError` return `err` unchanged, then `go test -run TestProtectedMetadataBranchFailsTheRepository` | **FAIL** in all three subtests: the error chain is the bare `cannot commit to protected branch: lakeFS operation forbidden` / `cannot write to protected branch: …`, not `the companion repository is read-only`. Restored |
| **Reversion proof R2I-4** — make `verifyTableOwnership` return `nil`, then `go test -run TestInitialisationRefusesAForeignStagedTable` | **FAIL** both subtests: `Expected error with "the companion repository was not created by the indexer" … but got nil` — the marker is sealed on the foreign table. Restored |
| **Probe file** `pkg/mds/indexer/zz_probe_review_test.go` (temporary, **deleted before committing**): reuses `newEnv`/`applyFailureEnv`, injects `table.ErrStaleTable`, `indexer.ErrConflict`, `table.ErrUnknownSchema`, `table.ErrMergeRows`, a `client.ErrForbidden` and a `rest.ErrForbidden` answer, a `TableStore` whose `CreateTable` answers `rest.ErrForbidden`, and a two-branch repository with COMMIT rules. Outputs quoted in R3I-1…R3I-5 | ran green; all outputs below |
| **Probe: the catalog arm of the mapping is untested** — replace `errors.Is(err, rest.ErrForbidden)` with `errors.Is(err, rest.ErrBadRequest)` in `metadataBranchWriteError` (so no catalog 403 can ever match) and run the **whole** package: `go test -count=1 -timeout 5m ./pkg/mds/indexer/` | **`ok … 4.396s`** — the suite cannot see the arm being broken. Restored (`git diff` empty) |

No docker, esti or engine-harness run was needed or made: this card reads code and drives the existing
fake.

### Findings

### R3I-1 - MINOR: the new 403 → `ErrReadOnlyMetadataRepository` mapping claims branch protection for an authorization refusal, and the mds client collapses 401 into the same sentinel, so the *default* denial lands there too

**Where** `pkg/mds/indexer/repo.go:52-62` (`metadataBranchWriteError`), reached from
`pkg/mds/indexer/branch.go:164,403,712,735,752`, `repo.go:414`.

**What happens** The predicate is `errors.Is(err, client.ErrForbidden) || errors.Is(err, rest.ErrForbidden)`.
`client.ErrForbidden` is produced for **401 and 403 alike** (`pkg/mds/client/lakefs.go:63-65`,
`:844-845`), and an authorization denial is answered 401 by default by lakeFS
(`pkg/api/controller_authorization_status_test.go:36-45`: unset → 401, 403 only when
`auth.authorization_failure_status` opts in). The catalog answers 403 for a genuine permission denial
(`pkg/icebergcatalog/tables.go:1396-1397` `PermissionDeniedError`) and for a read-only ref
(`:1387-1388`), neither of which is branch protection. All of them now get the text

```
the companion repository is read-only: lakeFS refused a write to "repo-metadata"@main: a branch
protection rule blocks COMMIT or STAGING_WRITE on it, or the repository is read-only (…);
remove the rule or create the repository with read_only=false
```

That is the wrong diagnosis *and* a destructive remedy ("create the repository with read_only=false") for
the failure mode design §8 documents as expected: while the policy refresher has not yet covered a new
companion repository, "the indexer fails closed on that repository (an authorization error in `/_health`,
never a silent write)" (`METADATA_SEARCH_DESIGN.md:632-634`, `docs/metadata-search.md:801`). An operator
following the message goes looking for a protection rule instead of at `mds-indexer`'s policy. The wrapped
cause is still in the chain, so nothing is lost, but the lead-in and the instruction are both wrong.

**Evidence** Probe 5, both shapes (the second is what a catalog write actually produces):

```
PROBE forbidden api authorization denial (401) -> branch error = "the companion repository is read-only:
  lakeFS refused a write to \"repo-metadata\"@main: a branch protection rule blocks COMMIT or
  STAGING_WRITE on it, or the repository is read-only (apply repo@s2: commit \"repo-metadata\": lakeFS
  operation forbidden: permission denied: no policy grants fs:WriteObject); remove the rule or create
  the repository with read_only=false"
PROBE forbidden catalog permission refusal (403) -> branch error = "… a branch protection rule blocks
  COMMIT or STAGING_WRITE on it … (apply repo@s2: apply repo@s2: REST error: forbidden:
  ForbiddenException: permission denied for catalog:WriteTableData); remove the rule or create the
  repository with read_only=false"
```

`grep -n "ErrForbidden" pkg/mds/client/lakefs.go` → `case code == http.StatusForbidden, code ==
http.StatusUnauthorized:` at `:844`.

**Suggested fix** Claim the cause only when the answer carries it: match the wrapped message
(`cannot commit to protected branch`, `cannot write to protected branch`, `read-only repository`) for the
protection wording, and report every other forbidden answer as itself under the same family with a neutral
text ("lakeFS refused a write to R-metadata: <cause>; check branch protection, `read_only`, and the
indexer's own permissions"). Test that fails before it: inject `client.ErrForbidden` with
`permission denied: no policy grants fs:WriteObject` and assert the message does not name branch protection
(and that the same family is still returned).

### R3I-2 - MINOR: the fix does not cover the one catalog write the initialisation makes — `CreateTable` — so a `STAGING_WRITE` rule present before the first tick is still refused with a bare, unmapped 403

**Where** `pkg/mds/indexer/repo.go:359-375` (`initialise`'s `CreateTable` call and its `case err != nil:` at
`:373-374`), `pkg/mds/indexer/iceberg.go:77-98` (`IcebergStore.CreateTable`), versus
`pkg/mds/indexer/repo.go:52-62` (the mapping, applied to `Commit`, `ResetBranch`, `HardResetBranch`,
`ApplyDelta`, `Rebuild`, `Compact` only).

**What happens** The catalog's create is a **staged write**: `pkg/icebergcatalog/tables.go:749-757` →
`store.CreateTableWithMetadata` → `stage`, and `writeError` maps `graveler.ErrWriteToProtectedBranch` /
`ErrReadOnlyRepository` to **403** (`pkg/icebergcatalog/tables.go:1405-1408`); iceberg-go reports that 403 as
`rest.ErrForbidden` (`iceberg-go@v0.7.0/catalog/rest/rest.go:668-670`, `:158`). `initialise` returns it as
`create the metadata table of "repo-metadata"@main: …`, **not** through `metadataBranchWriteError`, so the
`ErrReadOnlyMetadataRepository` family, the "branch protection" wording and the "COMMIT or STAGING_WRITE"
hint never appear for this path. Reachable exactly in the state design §3.3 now promises to diagnose: an
operator-created empty `R-metadata` (its only commit is lakeFS's automatic root, which §4.8 adopts) with a
`STAGING_WRITE` rule on `main` before the indexer's first tick. The design (`METADATA_SEARCH_DESIGN.md:125`,
"A rule present before the first tick is refused during the initialisation commit itself") and the guide
(`docs/metadata-search.md:801`, "a rule that exists before the first tick fails the initialisation commit the
same way") both name the *commit*, which is where a `COMMIT` rule is met; a `STAGING_WRITE` rule is met one
step earlier, at the create, and is reported as something else.

The fake cannot see it: `pkg/mds/indexer/fake_test.go:1065-1077` (`fake.CreateTable`) checks neither
protection nor `ReadOnly`, while every other catalogue write does (`:1146`, `:1179`, `:1215`).
So the fake is *less* strict than real lakeFS precisely where the fix is missing, and it lets the whole
initialisation succeed where real lakeFS refuses it.

**Evidence** Probe 6 (a `TableStore` whose `CreateTable` answers `rest.ErrForbidden`, the production
sentinel) and Probe 7 (a `STAGING_WRITE` rule on `repo-metadata@main` before an interrupted initialisation):

```
PROBE create forbidden: repositoryError = "create the metadata table of \"repo-metadata\"@main: create
  system.object_metadata: REST error: forbidden: ForbiddenException: cannot write to protected branch"
PROBE create forbidden: is ErrReadOnlyMetadataRepository = false
PROBE fake create: createTableCalls=1 repositoryError=<nil>
```

**Suggested fix** Wrap `initialise`'s create error in `metadataBranchWriteError` (one line at `repo.go:373`)
and give the fake's `CreateTable` the same `protectionErrorLocked`/`ReadOnly` checks the other table writes
have. Test that fails before it: a `TableStore` whose `CreateTable` answers `rest.ErrForbidden` and asserts
`ErrReadOnlyMetadataRepository` (and, on the fake, a `STAGING_WRITE` rule set before the first tick, asserting
the repository-level family and no table pointer).

### R3I-3 - MINOR: `table.ErrStaleTable` is not a §4.4 conflict for the indexer, so a tick that meets it does not reload and retry as the design and the table commit both state

**Where** `pkg/mds/indexer/iceberg.go:198-203` (`tableError` maps `icebergtable.ErrCommitFailed` →
`ErrConflict`, nothing else), `pkg/mds/indexer/branch.go:41-49` (`runBranch` retries only
`errors.Is(err, ErrConflict)`), versus `pkg/mds/table/writer.go:63-72` ("the caller reloads the table and
retries the tick (§4.4)") and `METADATA_SEARCH_DESIGN.md:226-228` ("The write returns the conflict (or
`table.ErrStaleTable` …) and the branch tick reloads and retries (§4.4, commits `e3083e279` and
`d993d602e`)").

**What happens** `d993d602e` introduced a *second* "the table moved since it was read" error, returned
before any commit (`pkg/mds/table/writer.go:190`), and documented it as the retryable conflict. The indexer
was not changed to match, so the two errors are treated differently: a write that fails with
`icebergtable.ErrCommitFailed` is retried in the same tick (up to `conflictRetries`), a write that fails with
`ErrStaleTable` fails the branch for the whole tick — `/_health` reports the branch failed (`lag` unchanged,
`last_error` = "the table moved since it was read") until the next period, which for the documented
two-replica overlap or a rolling restart is the state the retry exists for. `ErrStaleTable` is also produced
by the *repair* commit of `ensureNoReplay` (`table/writer.go:197-199`), i.e. on the very write that is
supposed to make the table non-replayable, so the state is met exactly when an old table is migrated under a
concurrent writer. No wrong write results (the write is abandoned), so this is a delayed-recovery and
diagnosis defect, not a data defect.

**Evidence** Probe 1/2, same injection point, same single new commit:

```
PROBE stale: apply calls for this tick = 1, branch error = apply repo@s2: make the delta at s2
  non-replayable: the table moved since it was read: snapshot 1 is now 2
PROBE conflict: apply calls for this tick = 2, branch error = <nil>, last = s2
```

**Suggested fix** Add `errors.Is(err, table.ErrStaleTable)` to `tableError`'s mapping to `ErrConflict` (or
wrap it in `func (t *icebergTable) ApplyDelta/Rebuild/Compact` with a reload), so §4.4's restart path covers
it. Test that fails before it: a fake table whose `ApplyDelta` answers `fmt.Errorf("%w: …",
table.ErrStaleTable)` once and then succeeds, asserting the branch is healthy after **one** tick and `last`
moved — exactly the shape of Probe 2.

### R3I-4 - MINOR: the design, the guide and the fix commit all say the protection refusal is a repository-level error; the code reports it per branch

**Where** `METADATA_SEARCH_DESIGN.md:125` ("so one repository-level error replaces a bare 403 on every
branch") and `:592-593` ("a branch-protection rule the first write meets … mark that repository failed in
`/_health` and metrics"), `docs/metadata-search.md:801` ("still as a repository-level error that names branch
protection, not as a bare `403` on every branch"); the code returns the family from `runBranchOnce`
(`pkg/mds/indexer/branch.go:164,403,712,735,752`), i.e. through `indexBranch` →
`health.SetBranchState` + `metrics.Error(repository, StageBranch)`
(`pkg/mds/indexer/indexer.go:542-585`, `StageBranch` at `:93`), never through `FinishTick`/`StageRepository`.

**What happens** A `COMMIT` rule on `R-metadata` with three branches still produces three branch errors and
three `errors_total{stage="branch"}` increments per tick, and the repository's `last_error` stays empty
(`pkg/mds/health.go:126-134`, `:154-163`): /_health is 503 either way, but nothing is "refused once per tick
instead of failing every branch of it", and the metrics an operator would alert on are the branch counters.
The sentence is the rationale attached to the R2I-3 fix, so the docs state a behaviour the fix does not
implement (and the read-only half *does* implement — `repo.go:123-126` runs before any branch is touched).

**Evidence** Probe 8 (two branches, `COMMIT` rules on both):

```
PROBE protection levels: repositoryError=<nil>
PROBE protection levels: main error="the companion repository is read-only: lakeFS refused a write to
  \"repo-metadata\"@main: a branch protection rule blocks …"
PROBE protection levels: feature error="the companion repository is read-only: lakeFS refused a write to
  \"repo-metadata\"@feature: a branch protection rule blocks …"
PROBE protection levels: metric error stages=[repo/branch repo/branch]
```

**Suggested fix** Either downgrade the documentation ("each branch whose first write lakeFS refuses reports
the family, so /_health stays 503 with a per-branch error rather than a per-branch bare 403"), or implement
what the docs say: on the first `ErrReadOnlyMetadataRepository`/protection answer of a tick, cancel the rest
of the repository's branches and record it as the repository's error. Test that would fail before the
second: two protected branches, assert one `repo/repository` metric increment and a non-empty repository
`last_error` with no branch error — or, for the first, a docs change with no test.

### R3I-5 - NIT: the catalogue half of the R2I-3 mapping (`rest.ErrForbidden`) is dead in the test suite, because the fake's table writes answer the API sentinel instead of the catalog's

**Where** `pkg/mds/indexer/fake_test.go:1146`, `:1179`, `:1215` return
`client.ErrForbidden` from `ApplyDelta`/`Rebuild`/`Compact`, while production's catalog writes return
`rest.ErrForbidden` (`pkg/mds/indexer/iceberg.go:160-196`; `iceberg-go@v0.7.0/catalog/rest/rest.go:668-670`,
unwrapped at `:158`); `pkg/mds/indexer/repo.go:56` is the arm that consumes it.

**What happens** The `STAGING_WRITE`-on-a-catalog-write half of R2I-3 — the half that produces the sentinel
the indexer sees in production — has no test: every test in the package reaches the mapping through the
*lakeFS API* sentinel. The arm can be broken outright and the suite stays green.

**Evidence** Replacing `rest.ErrForbidden` with `rest.ErrBadRequest` in `metadataBranchWriteError` (so no
catalog 403 can match any more) leaves the whole package green:

```
$ go test -count=1 -timeout 5m ./pkg/mds/indexer/
ok  	github.com/treeverse/lakefs/pkg/mds/indexer	4.396s
```

(`grep -rn "rest.ErrForbidden" --include=*.go .` outside `repo.go` → only the temporary probe.)

**Suggested fix** In the fake's table writes, wrap the catalog sentinel the way `iceberg.go` does —
`fmt.Errorf("…: %w: ForbiddenException", rest.ErrForbidden)` — so the existing
`TestProtectedMetadataBranchFailsTheRepository/staging_write_protection` case exercises the arm. Test that
fails before it: the same test with `rest.ErrForbidden` removed from the predicate.

### Checked, no finding

- **R2I-1 is a real fix, and it does not lose the adoption it was built on.** `verifyInitCommit(…, false)`
  (`repo.go:398`) returns `(metadataState{}, nil)` on `freshAutomaticRoot` and the caller returns the
  original 400 (`repo.go:403-405`); a branch carrying another replica's contiguous `mds: init` run still
  takes `scan.accepted()` and is adopted, which `TestConcurrentInitialisationCommitsOnce` (`repo_test.go:449`)
  still drives through the 400 arm. The recursion is gone (reversion proof above): one `CreateTable` and one
  commit attempt per tick, and the error string no longer grows with itself.
- **R2I-2 is correct and loses nothing.** `last == head.ID` means the recorded position *is* the head, so it
  exists, is trivially its own merge base, `pending` has no work, and no §4.7 rewrite can apply
  (`branch.go:173-180`). `state.lag` stays 0, not `LagUnknown`. Reversion proof above.
- **R2I-4 cannot refuse the indexer's own table.** `lakefs.mds.repository` has been the *data* repository
  name since M4 (`git show 0f7ac16f4:pkg/mds/indexer/iceberg.go:81` →
  `pkg/mds/table/schema.go:294-305`), and both the create and the check use the same argument, so no
  pre-existing companion table is mistaken for foreign. The handle is closed on the refusal
  (`repo.go:369-372`), so no catalog is leaked. The residual threat (a staged table that *does* carry
  `lakefs.mds.repository=R` is still adopted, with its schema and properties) is the one R2I-4 already
  records as its own limit and the property is the proof §4.8 now documents; its location is constrained to
  the companion's own table area by the catalog (`pkg/icebergcatalog/tables.go:708-711` →
  `pkg/icebergcatalog/location.go:127-134`), so it is not re-reported.
- **`table.ErrUnknownSchema` as the indexer meets it** is surfaced, not swallowed: the branch fails with
  `apply repo@s2: … unknown table schema: 11 columns, this writer knows 10`, `lag` stays 1 and `last` stays
  at the previous position, and every later tick fails the same way — the documented newer-schema limit
  (`METADATA_SEARCH_DESIGN.md:75-82`), with no panic and no nulled column (that was R2T-1, and no `recover`
  is needed). Probe 3.
- **`table.ErrMergeRows` as the indexer meets it** is surfaced the same way: the commits of the tick are
  applied and committed first, the compaction then fails the branch with `compact repo@main: … cannot merge
  the rows of the compaction`, `lag` 0, and the trigger stays due (`maintenance.go:49-78`), so the failure
  repeats until it is fixed. That is the pre-existing "compaction failure fails the branch" contract, not a
  new one; the panic it replaced (R2T-1) no longer reaches iceberg-go's writer goroutine. Probe 4.
- **The stricter fake against real lakeFS** for the paths R2I-3 touches: `Commit` checks `COMMIT`
  (`fake_test.go:848` vs `pkg/graveler/graveler.go:2245-2257`), `Reset`/`ResetBranch` checks
  `STAGING_WRITE` (`:542` vs `:2637-2650`, reached from `pkg/api/controller.go:3313`), `ResetHard` checks
  both (`:554` vs `:2577-2595`), `DeleteBranch` checks neither protection rule but does refuse
  `ReadOnly` and the default branch with 403 (`:521-529` vs `:1641-1648`,
  `pkg/api/controller.go:3196-3200`), and protection is consulted before the repository/branch lookup as
  `IsBlocked` is in graveler (`pkg/graveler/branch/protection_manager.go:48-66` does not touch the branch).
  The one divergence is `CreateTable` (R3I-2).
- **Docs for the rest of my area match the code**: §4.2 step 4's "skipped when `last == head`"
  (`METADATA_SEARCH_DESIGN.md:169-172` vs `branch.go:174-179`), §4.8's "the `ErrTableExists` arm … refuses
  it with `ErrForeignRepository` unless it carries `lakefs.mds.repository = R`" (`:352-359` vs
  `repo.go:361-372`), §4.8's bounded 400 rule (`:361-367` vs `repo.go:393-411`), and §3.1's unknown-column
  rule (`:75-82` vs `pkg/mds/table/writer.go:250,370` and `Compact`, which does not call
  `checkKnownSchema`).
- **Concurrency, idempotency and resources in the new code**: `metadataBranchWriteError` is pure and
  nil-safe; `verifyTableOwnership` reads one property and allocates nothing; the added `branch` parameter of
  `rebuild` is passed the branch being indexed at all three call sites (`branch.go:381,658,698`); no new
  goroutine, lock, cache or unbounded read is introduced by any of the four fixes; the 400 arm adds at most
  one bounded first-parent walk per failing tick, and the walk allocates O(page) (TS-5's shape, unchanged).
