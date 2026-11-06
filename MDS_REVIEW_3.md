# MDS review 3 — consolidated

## Branch and HEAD reviewed

- Branch: `feature/metadata-search` (also `origin/feature/metadata-search`). This worktree is
  detached and clean at the commit below.
- HEAD reviewed: `3ee34992f8d89f049ad657f4564088eabed13e75` (`3ee34992f`), the third review card.
- The three cards consolidated here, oldest first: `45087bc2e` (indexer), `08c6a017e` (policies and
  catalog security), `3ee34992f` (table compaction and esti). Each card was written in its own
  worktree and at its own HEAD: the security and indexer cards at `45087bc2e`, the table card at
  `0dbf5db98`.
- The product code is the same at all three HEADs, which is what makes the merged citations and the
  spot-checks below apply to every card: `git diff --stat 0dbf5db98 3ee34992f` lists only the three
  review files (834 insertions, 0 deletions), so no commit in this range changes code, tests, design
  or docs after `0dbf5db98`.

## Commit range reviewed

`902839f67f1895cbdda8de08e09ff3f1b42b45e9..0dbf5db98a9880ab4410ce0f9fa9adf1ef17b038` — the seven
fix commits that follow the consolidated fix review `902839f67`, net **26 files, 4156 insertions,
561 deletions** (including the merge in the count of the tree diff, not as its own work):

| # | Commit | Subject | Card that reviewed it |
|---|---|---|---|
| 1 | `da24d421f` | fix(mds/indexer): bound init retries, restore the caught-up guard, diagnose protection, refuse a foreign staged table | indexer |
| 2 | `500a00a37` | iceberg: prove a companion table's history is the indexer's; read-vend reserved targets | security |
| 3 | `26a66b633` | mds/bootstrap: bound and harden the metadata search policy set (R2S-2..9) | security |
| 4 | `8df39cabd` | Merge branch `feature/iceberg-catalog` into `feature/metadata-search` (brings in `500a00a37`) | security |
| 5 | `2af2532b7` | esti/mds-engines: fix R2T-4, compare every Spark part file and gate the snippet's own columns | table/esti |
| 6 | `d993d602e` | mds/table: keep unknown columns through compaction, bound its memory, pin no replay (R2T-1..R2T-3) | table/esti, and the three errors the indexer card consumes |
| 7 | `b4c8c24ba` | docs: record the MDS fix-review fixes | all three (claims checked) |

The three review cards sit on top of that range and are the only commits after it:
`45087bc2e` (indexer), `08c6a017e` (security) and `3ee34992f` (table/esti). Findings under test in
this round are `MDS_REVIEW_2.md`'s R2I-1…R2I-4, R2S-1…R2S-9 and R2T-1…R2T-4. Nothing recorded in
`MDS_REVIEW.md` or `MDS_REVIEW_2.md` is re-reported here except where a fix is incomplete or has
introduced a new defect; each such case is marked as one.

## Verdict

The fixes are real work and the indexer card is the strongest of the three: it reverts four fixes,
shows each of its own tests go red without its fix (R2I-1…R2I-4), and drives the existing fake with
seven probes. But the round also shows that **three of the fixes are incomplete in ways the suite
cannot see**, and it is those, not the MINOR drift, that decide the fragment. R2S-1's two-fact
ownership proof can be forged by any principal that can commit to `R-metadata`, because it checks
neither committer nor position (R3S-1); the new base deny policy is written with zero statements on
every installation, which a spec-validating auth service rejects, and the resulting error is fatal at
startup (R3S-2); and the R2T-1 compaction rewrite hands `ReplaceFiles` the same data file once per
split scan task, so the first compaction of a table holding a file above the 128 MiB split target can
no longer be replaced (R3T-1). R2S-3's namespace deny is also narrower than the design claims: it
matches the stored spelling only (R3S-3), though the impact path through GC is unproven. Counted
across the three cards and deduplicated (there were no duplicate keys to merge): **0 BLOCKER,
4 MAJOR, 14 MINOR, 4 NIT — 22 findings**. Every MAJOR was re-checked against the code during this
consolidation and every one is **CONFIRMED**; none is DISPUTED and none was dropped. There is no
BLOCKER to check. **Verdict: fix R3S-1, R3S-2 and R3T-1 and this fragment is mergeable; R3S-3 needs
the owner's decision on the deny's shape, and the 18 MINOR/NIT can follow.** As the cards record, the
security and table reviews were cut short by a time limit: neither wrote a probe test nor ran a
revert proof, so their MINOR/NIT findings are code reasoning and are marked as such. This
consolidation changes no code, test, design or doc, and does not touch `MDS_REVIEW.md` or
`MDS_REVIEW_2.md`.

## Summary of findings

Each finding carries its own fix status below; the `Status` column here is the short form, `Docs only`
marking a finding the fix cards left to this documentation card on purpose.

| Key | Severity | Area | Status | Title | File:line |
|---|---|---|---|---|---|
| R3S-1 | MAJOR | security | Fixed `db52bd9a7`; the `fs:CreateCommit` residual closed in `2a822c719`; the bare-companion consequence fixed in `30af7b3a5` | The `mds: init` ownership proof can be forged by any caller who can commit to `R-metadata` | `pkg/icebergcatalog/resolver.go:536-540` |
| R3S-2 | MAJOR | security | Fixed `9135602ea` | The base `mds-metadata-repo-protect` policy is always written with zero statements, which a spec-validating auth service may reject | `pkg/mds/bootstrap/bootstrap.go:470-485` |
| R3S-3 | MAJOR | security | Fixed `9135602ea` | The exact-namespace `fs:AttachStorageNamespace` deny matches only the stored spelling of the namespace | `pkg/mds/bootstrap/bootstrap.go:505-506` |
| R3T-1 | MAJOR | table | Fixed `7f5fd526b` | `Compact` hands one data file to `ReplaceFiles` once per split scan task, which iceberg-go v0.7.0 rejects | `pkg/mds/table/writer.go:474-476,586-593` |
| R3S-4 | MINOR | security | Fixed `db52bd9a7` | The init-commit walk is quadratic in the history length and reruns on every new head of `R-metadata` | `pkg/icebergcatalog/resolver.go:484-500` |
| R3S-5 | MINOR | security | Fixed `9135602ea` | Bootstrap needs `ListPolicies` and `DeletePolicy`, which the in-repo ACL server answers with 501, and 501 is a fatal startup error | `pkg/mds/bootstrap/bootstrap.go:880-905` |
| R3S-6 | MINOR | security | Fixed `9135602ea` | The policy cache is not purged after a policy is attached, so a new chunk can be invisible for up to the cache TTL | `pkg/mds/bootstrap/bootstrap.go:846-875` |
| R3S-7 | MINOR | security | Fixed `9135602ea` | A companion can lose its grant and its deny between two policy writes when the sorted set shifts across chunks | `pkg/mds/bootstrap/bootstrap.go:214-262` |
| R3S-8 | MINOR | security | Fixed `db52bd9a7` | `initCache` is keyed by repository and head commit and may never evict | `pkg/icebergcatalog/resolver.go:544-552` |
| R3I-1 | MINOR | indexer | Fixed `2ae794038` | The new 403 → `ErrReadOnlyMetadataRepository` mapping claims branch protection for an authorization refusal, and the mds client collapses 401 into the same sentinel, so the default denial lands there too | `pkg/mds/indexer/repo.go:52-62` |
| R3I-2 | MINOR | indexer | Fixed `2ae794038` | The fix does not cover the one catalog write the initialisation makes — `CreateTable` — so a `STAGING_WRITE` rule present before the first tick is still refused with a bare, unmapped 403 | `pkg/mds/indexer/repo.go:359-375` |
| R3I-3 | MINOR | indexer | Fixed `2ae794038` | `table.ErrStaleTable` is not a §4.4 conflict for the indexer, so a tick that meets it does not reload and retry as the design and the table commit both state | `pkg/mds/indexer/iceberg.go:198-203` |
| R3I-4 | MINOR | indexer | Docs only | The design, the guide and the fix commit all say the protection refusal is a repository-level error; the code reports it per branch | `pkg/mds/indexer/branch.go:164,403,712,735,752` |
| R3T-2 | MINOR | esti | Fixed `0f45215bc` | The Spark self-test's two `user_metadata` cases fail on the row count, so they do not show that `user_metadata` is compared | `esti/ops/mds-engines/run-spark.sh:209-229` |
| R3T-3 | MINOR | table | Docs only | A table of legacy files is materialised all at once, not "one file's memory" | `pkg/mds/table/rows.go:428-475` |
| R3T-4 | MINOR | table | Fixed `7f5fd526b` | The memory tests do not exercise the budget, the floor or more than one file | `pkg/mds/table/writer_test.go:851-899` |
| R3T-5 | MINOR | table | Docs only | The documented memory bound counts the merge's own rows, not the scan pipeline or the writer queue | `METADATA_SEARCH_DESIGN.md:381-387` |
| R3T-6 | MINOR | table | Fixed `7f5fd526b` | `ErrMergeRows` has no test, and the recover only covers the merge's pull loop | `pkg/mds/table/rows.go:767-770` |
| R3S-9 | NIT | security | Fixed `db52bd9a7` | The R2S-5 write-vend guard is exercised only by a fake resolver, and the built-in resolver never reports its own target as reserved | `pkg/icebergcatalog/creds.go:451-458` |
| R3I-5 | NIT | indexer | Fixed `2ae794038` | The catalogue half of the R2I-3 mapping (`rest.ErrForbidden`) is dead in the test suite, because the fake's table writes answer the API sentinel instead of the catalog's | `pkg/mds/indexer/fake_test.go:1146,1179,1215` |
| R3T-7 | NIT | table | Fixed `7f5fd526b` | Streams opened by `compactReader` are released only by the merge's own `defer`, which does not run if the reader is released before its first pull; and no test checks Arrow refcounts | `pkg/mds/table/writer.go:481-496` |
| R3T-8 | NIT | table | Fixed `7f5fd526b` | The schema of the compaction reader is `nil` for a streamed run, so the "no task" branch is the common path and the declared schema differs from the yielded batches | `pkg/mds/table/rows.go:537-557` |

## Duplicates

No key is duplicated across the three cards: 22 keys, 22 findings, nothing merged and nothing
dropped. The pairs that look like duplicates were checked and are distinct, and are kept as separate
findings:

- **R3T-3 and R3T-5** both touch §4.9's memory claim. R3T-3 is the legacy-file case, where every
  file of the table is materialised at once; R3T-5 is the scan pipeline and writer queue, which hold
  memory outside the merge. Different code, different fix.
- **R3S-4 and R3S-8** are both the resolver's init-commit cache. R3S-4 is the cost of the walk
  (quadratic, repeated per head); R3S-8 is the growth of the map (unbounded, never evicted). The
  suggested fixes overlap, the defects do not.
- **R3I-3** is the only place `table.ErrStaleTable` is reported as an indexer finding. The table card
  cross-references it by key from its "Checked, no finding" section and raises nothing of its own
  there, so there is one key, in the indexer area.
- **R3I-1 and R3I-2** are the same fix seen at two write paths (the mapping over-claims the cause on
  the paths it covers; the one path it does not cover is `CreateTable`). R3I-2's fix depends on
  R3I-1's wording, but they are separate defects and a reader can fix either first.
- **R3S-9** is not R2S-5. R2S-5 was the missing write-vend guard; R3S-9 is the guard's test being
  the only thing that exercises it, on a resolver the production wiring does not use.
- The indexer card's "Checked, no finding" entry for `ErrMergeRows` (the indexer does surface it) and
  R3T-6 (nothing in `pkg/mds/table` tests the recover that produces it) are about different layers.

## Verified

The three cards ran their commands in three worktrees — security in `/tmp/mds-fork/wt-5ddb3` at
`45087bc2e` and, for the build/vet/test/lint rows, in a baseline worktree at `0dbf5db98`; indexer in
its own worktree at `45087bc2e`; table in `/Users/arogov/.cline/worktrees/7f18b/lakeFS` at
`0dbf5db98` — all with `GOTOOLCHAIN=auto`, which resolved to go1.26.4. Because no commit in
`0dbf5db98..3ee34992f` changes code or tests, the rows are valid for this HEAD as well. Rows are
merged; a command that two cards ran is listed once per distinct invocation.

| Command | Result | Card / worktree |
|---|---|---|
| `git rev-parse --short HEAD` | `45087bc2e` | security, `/tmp/mds-fork/wt-5ddb3` |
| `git diff --stat 0dbf5db98 HEAD -- pkg/mds/bootstrap pkg/icebergcatalog contrib pkg/auth` | Empty: the product code is identical to the tree the baseline ran on | security |
| `git diff 902839f67 HEAD --stat`, limited to that area | 7 files, 1294 insertions, 277 deletions: `creds.go`, `creds_test.go`, `resolver.go`, `resolver_test.go`, `sign_test.go`, `bootstrap.go`, `bootstrap_test.go`; nothing in `contrib/`, `pkg/auth` or `cmd/lakefs-sso` | security |
| `git diff --stat 0dbf5db98 HEAD` | Only `review/mds3/indexer.md`; no code or test differed from the baseline worktree | table |
| `go build ./...` | rc=0, no output | security (baseline), indexer, table |
| `go vet ./pkg/mds/bootstrap/... ./pkg/icebergcatalog/... ./cmd/lakefs-sso/...` | rc=0 | security |
| `go vet ./pkg/mds/indexer/... ./pkg/mds/table/...` | pass, rc=0 | indexer |
| `go vet ./pkg/mds/table/...` | rc=0 | table |
| `go test -count=1 -race -timeout 10m` on `pkg/mds/bootstrap`, `pkg/icebergcatalog`, `pkg/icebergcatalog/cleanup`, `pkg/icebergcatalog/conformance`, `pkg/icebergcatalog/importer` | All `ok`: bootstrap 5.9s, icebergcatalog 29.7s, cleanup 6.1s, conformance 4.4s, importer 7.4s; `cmd/lakefs-sso` has no test files | security |
| `go test -count=1 -race -timeout 10m ./pkg/mds/indexer/` | `ok … 10.309s` | indexer |
| `go test -count=1 -timeout 10m ./pkg/mds/table/` | `ok … 7.950s` (no race detector) | indexer |
| `go test -count=1 -race -timeout 10m ./pkg/mds/table/...` | `ok … 15.109s` | table |
| `go tool golangci-lint run --allow-parallel-runners` on the security packages | 0 issues | security |
| `go tool golangci-lint run ./pkg/mds/indexer/...` | 0 issues | indexer |
| `go tool golangci-lint run --timeout 10m ./pkg/mds/table/...` | 0 issues | table |
| `grep -n` of every function and call site cited by the security card | The cites not marked † match the tree | security |
| **Reversion proof R2I-1** — restore `verifyInitCommit(…, true)` in the 400 arm and delete the `!state.initialised` guard, then `go test -run TestPersistentBadRequestOnTheInitialisationCommitDoesNotRecurse` | **FAIL** as claimed: `expected: 1 actual: 26`, the tick's error being 26 nested copies ending in `probe: gave up after 26 initialisation attempts`; restored, `git diff` empty | indexer |
| **Reversion proof R2I-2** — delete the `if last == head.ID { return last, nil }` guard in `continueFrom`, then `go test -run TestCaughtUpBranchDoesNotReadTheMergeBase` | **FAIL**: `Should be zero, but was 1 — a branch at its head makes no merge-base read`; restored | indexer |
| **Reversion proof R2I-3** — make `metadataBranchWriteError` return `err` unchanged, then `go test -run TestProtectedMetadataBranchFailsTheRepository` | **FAIL** in all three subtests: the chain is the bare `cannot commit to protected branch: …`, not `the companion repository is read-only`; restored | indexer |
| **Reversion proof R2I-4** — make `verifyTableOwnership` return `nil`, then `go test -run TestInitialisationRefusesAForeignStagedTable` | **FAIL** both subtests: `Expected error with "the companion repository was not created by the indexer" … but got nil`, the marker sealed on the foreign table; restored | indexer |
| **Probe file** `pkg/mds/indexer/zz_probe_review_test.go` (temporary, deleted before committing): reuses `newEnv`/`applyFailureEnv`, injects `table.ErrStaleTable`, `indexer.ErrConflict`, `table.ErrUnknownSchema`, `table.ErrMergeRows`, a `client.ErrForbidden` and a `rest.ErrForbidden` answer, a `TableStore` whose `CreateTable` answers `rest.ErrForbidden`, and a two-branch repository with COMMIT rules | Ran green; its outputs are quoted in R3I-1…R3I-5 | indexer |
| **Probe: the catalog arm of the mapping is untested** — replace `errors.Is(err, rest.ErrForbidden)` with `errors.Is(err, rest.ErrBadRequest)` and run the whole package: `go test -count=1 -timeout 5m ./pkg/mds/indexer/` | **`ok … 4.396s`** — the suite cannot see the arm being broken; restored | indexer |
| `./esti/ops/mds-engines/run-spark.sh --self-test` | exit 0; 7 `PASS:` lines, last `self-test passed`; `git status` clean after | table |
| `grep -rn "ErrMergeRows\|compactFileBatchRows" pkg/mds --include=*_test.go` | No match in any test file | table |
| `grep -n "CheckedAllocator\|AssertSize\|WithAllocator" pkg/mds/table/*.go` | No match | table |

Not run by any card: a probe test for the security findings, a revert proof for `26a66b633`,
`500a00a37`, `d993d602e` or `2af2532b7`, `run-spark.sh` against a live environment (no docker), and
any `./esti/...` recipe. The indexer card's four revert proofs are the only fail-without-the-fix
evidence in this round. This consolidation ran no command beyond the code reads recorded under each
MAJOR below; those reads are at `3ee34992f`, where the code is byte-identical to every card's tree.

## BLOCKER (0)

None. No card in this round reported a BLOCKER, so there was none to spot-check; the
spot-check column of this consolidation covers the four MAJORs.

## MAJOR (4)

### R3S-1 - MAJOR: the `mds: init` ownership proof can be forged by any caller who can commit to `R-metadata`

**Fixed in `db52bd9a7`** (merged in `96a8834c4`): the proof now also requires the committer `mds-indexer` and the position directly above the repository's root commit. The residual that commit recorded - `CreateCommitRecord` authorizes `fs:CreateCommit` on the repository ARN and takes the committer from the client, so a principal that holds it can still mint every fact of the proof - is **closed in `2a822c719`**: `CreateCommitRecord` now answers `403` when the body's committer is `mds-indexer` and the authenticated caller is not that user, on every repository rather than only companions; the indexer still records its own committer and every other committer is still accepted. The boundary that remains is `LoadCommits`, not that endpoint: `lakectl refs-restore` restores through `RestoreSubmit` → `catalog.LoadCommits`, which copies commit records verbatim from the manifest's metaranges, so restoring an ordinary or a companion repository is unchanged, and a caller able to write forged metaranges directly into the block store is outside the API trust boundary. Consequence found while documenting this card (code read, no probe): the position rule covered only a non-bare companion, so a companion repository created **bare** - its `mds: init` commit is the branch's first commit and has no parent - was accepted by the indexer (§4.8) but not served by the catalog. **Fixed in `30af7b3a5`**: the catalog also accepts an init commit that is itself the root, with the same three facts, so a bare companion is served like a non-bare one; a non-indexer committer and an init commit with another commit between it and the root stay refused, and `TestMetadataSearchInitCommitAtTheRoot` plus the bare fixture in `TestMetadataSearchCompanionRepositoryProof` pin the shape.

Status: **CONFIRMED** (code read, no probe).

**Spot-check (this consolidation): CONFIRMED.** `isMetadataSearchInitCommit` compares only the message and `lakefs.mds.init`, never the committer and never the position, while `Commit` and `CreateCommitRecord` authorize `fs:CreateCommit` alone and take both fields from the client — so the proof establishes nothing that a writer of `R-metadata` cannot mint. The indexer's own `isInitCommit` does not check the committer either, so the ambient premise of the finding holds too.

**Where**
- `pkg/icebergcatalog/resolver.go:536-540` (`isMetadataSearchInitCommit`) and `:484-500` (`firstParentCarriesInitCommit`).
- Against `pkg/api/controller.go:3508-3531` (`Commit`) and `:3536-3552` (`CreateCommitRecord`).
- Test helper `pkg/icebergcatalog/resolver_test.go:313` (`commitResolverInit`).

**What happens**
- `isMetadataSearchInitCommit` accepts a commit when `Message == "mds: init"` and `Metadata["lakefs.mds.init"] == "true"`. It does not check the committer, and it does not check where the commit sits in history. Any commit on the first-parent chain counts.
- The `Commit` handler authorizes only `fs:CreateCommit` on the branch ARN (`:3509-3514`). It passes the client's `body.Message` and `body.Metadata` straight into `Catalog.Commit` (`:3524-3531`). The committer is the authenticated user.
- `CreateCommitRecord` authorizes `fs:CreateCommit` on the repository ARN (`:3537-3542`) and takes every field from the client, including the committer.
- So the principal MDS-SEC-3 and R2S-1 meant to exclude can satisfy both facts of the proof. That principal created `R-metadata` before the indexer did, or is outside the guard groups so the deny does not apply to it. It writes the table with `lakefs.mds.repository=R`, then runs one `lakectl commit -m "mds: init" --meta lakefs.mds.init=true`.
- The catalog then serves that table to every reader of `R`, with the server's identity.
- The proof adds no authenticity over the property check alone. Anyone who can write the table can write the commit.

**Evidence**
- The repository's own fixture builds the "genuine" init commit with a plain `cat.Commit(..., MetadataSearchInitCommitMessage, "mds", catalog.Metadata{...})` (`resolver_test.go:313-322`). That is exactly the attacker's recipe.
- The only negative test, `newResolverFixtureWithoutInitCommit` (`resolver_test.go:157`), tests the absence of the commit. It never tests a commit made by a different principal.
- The indexer's `verifyInitCommit` (`pkg/mds/indexer/repo.go:239-345`) is stricter about position: a run of init commits directly above the automatic root. The catalog's check is not.
- I did not re-read `isInitCommit` (`repo.go:~345`†), so I cannot say whether the indexer checks the committer.
- Design §5.1 presents the two facts as an ownership proof. Commit `b4c8c24ba` records R2S-1 as "fixed".

**Suggested fix**
- Make the proof depend on something the caller cannot choose:
  - Require the init commit's committer to equal the indexer principal (`mds-indexer`, set by `User.Committer()` from the authenticated user).
  - Require its position to be the directly-above-the-root run, as the indexer does.
  - State the residual in the design: `fs:CreateCommit` on the repository ARN (`CreateCommitRecord`) still lets a principal that holds it choose any committer.
- Closing it fully needs a deny the indexer's group is the only exception to. The design rejected wildcards for good reason, so this is a design decision for the owner.
- Failing test (new): as a non-indexer user, create `R-metadata`, write the table with the right property, then `Commit(..., "mds: init", {"lakefs.mds.init":"true"})`. A `GET` of the virtual table must be 404 `NoSuchTableException`. Today it is 200.


### R3S-2 - MAJOR: the base `mds-metadata-repo-protect` policy is always written with zero statements, which a spec-validating auth service may reject

**Fixed in `9135602ea`**: the deny is written as numbered `mds-metadata-repo-protect-<n>` chunks only, so no policy with zero statements is written and a repository set with no companion repository has no deny policy at all; a statement-less base left by an earlier version is deleted as a stale owned name. `TestEnsurePrincipalsOverAuthAPI` drives `acl.Serve` plus `APIAuthService` and fails before the fix with `create policy mds-metadata-repo-protect: invalid request`.

Status: **UNVERIFIED**. The schema, the validator and the policy are read in code. No HTTP round trip was run, and I did not read the kin-openapi library to confirm it answers 400.

**Spot-check (this consolidation): CONFIRMED.** `MetadataRepoProtectPolicies` puts a statement-less policy first unconditionally and `metadataSearchPolicyNames` keeps it in the written set on every installation; `api/authorization.yml` requires `minItems: 1` and `acl.Serve` validates request bodies on every route but the one carrying `x-validation-exclude-body`, `APIAuthService.WritePolicy` sends `[]`, and the resulting `auth.ErrInvalidRequest` is not `policyBackendUnavailable`, so `RoutesBuilder` returns it and `run.go:306` calls `Fatal`.

**Where**
- `pkg/mds/bootstrap/bootstrap.go:~470-485` (`MetadataRepoProtectPolicies`: the first policy has `Statement: model.Statements{}`).
- `bootstrap.go:968-1000` (`ensurePolicy` creates it).
- `api/authorization.yml:356`: `Policy.statement` has `minItems: 1`. The `statement` array is required (`:339-343`) and `PUT /policies/{policyId}` takes `Policy`.
- `contrib/auth/acl/serve.go:84-110` (`validateRequest` returns 400 on a `RequestError`).
- `pkg/auth/service.go:612-655` (`APIAuthService.WritePolicy` builds `stmts := make([]Statement, 0)` and sends it as `[]`).

**What happens**
- Before `26a66b633`, the deny had zero statements only when the server knew no repository. Now the base deny policy is empty on every installation, by design ("The first policy is empty", the doc comment on `MetadataRepoProtectPolicies`).
- Against any auth API that validates bodies against `authorization.yml`, the create call would be refused. The contrib ACL server does validate bodies.
- `APIAuthService` turns the 400 into `auth.ErrInvalidRequest`. `ensurePolicy` returns it as `create policy mds-metadata-repo-protect: …` (the `default`-less create arm, `bootstrap.go:988-996`). Only `auth.ErrNotImplemented` is treated as degraded (`policyBackendUnavailable`, `:1027`).
- `RoutesBuilder` therefore returns the error, and `cmd/lakefs/cmd/run.go:304-307` calls `Fatal("extra routes")`. The server would not start with Metadata Search enabled on an external auth service.
- The in-process `acl.AuthService` accepts empty statements (`ValidatePolicy` only loops over them, `contrib/auth/acl/service.go:695-709`), which is why every bootstrap test passes.

**Evidence**
- The tests drive `acl.AuthService` directly. No test in `contrib/auth/acl` builds an `APIAuthService` (earlier grep†), and none goes through HTTP.
- Design §8 says the base policies "carry only the statements that do not depend on a repository". It does not say the protect base has none.

**Suggested fix**
- Do not create the base deny policy at all. Chunks are enough, and the group attachment list already names chunks.
- Alternatively, give the base a harmless statement.
- The step that proves or disproves this finding: run `acl.Serve` over `httptest` with `auth.NewAPIAuthService` pointed at it, then call `EnsurePrincipals`.
- Failing test: that run must succeed. Today it fails if the validator rejects `statement: []`.


### R3S-3 - MAJOR: the exact-namespace `fs:AttachStorageNamespace` deny matches only the stored spelling of the namespace

**Fixed in `9135602ea`**: the deny emits one statement per spelling `CreateRepository` accepts for the same physical prefix (stored, trailing-slash-trimmed, that with one `/`, plus `//` for a bucket root), and `TestCompanionStorageNamespaceDenySpellings` drives the exact `CreateRepository` authorization node. Residual, stated in `storageNamespaceDenyResources`: a second or later extra trailing slash is a different physical prefix for S3 and GS and is the same prefix only in the dev-only mem and local adapters; covering it needs a `namespace/<ns>/*` pattern, which also denies every namespace below the companion's.

Status: **UNVERIFIED**. The mismatch is read in code. I did not run a `CreateRepository` with a variant namespace or a GC against the new repository.

**Spot-check (this consolidation): CONFIRMED.** The deny names `permissions.StorageNamespace(companion.StorageNamespace)` with no wildcard, `CreateRepository` authorizes the raw client string through `ArnMatch`'s exact resource comparison, and `formatPathWithNamespace` trims the trailing slash — so `s3://bucket/prefix/` is the same physical prefix under a different ARN. Only the impact tail (a GC from the variant repository deleting companion data) remains unproven, as the card says.

**Where**
- `pkg/mds/bootstrap/bootstrap.go:505-506` (deny resource `permissions.StorageNamespace(companion.StorageNamespace)`).
- `pkg/permissions/permission.go:34` (`fsArnPrefix + "namespace/" + namespace`).
- `pkg/api/controller.go:2284-2305` (`CreateRepository` authorizes the raw client string at `:2300`).
- `pkg/api/controller.go:2437` (`validateStorageNamespace`) and `:2453` (`ensureStorageNamespace`).
- `pkg/auth/arn.go:87` (`ArnMatch`) and `pkg/block/namespace.go:197` (`DefaultValidationRegex`).

**What happens**
- The deny is one ARN built from the namespace as stored in the catalog. `CreateRepository` authorizes the string the client sent, not a canonical form.
- The default validation regex lets a path suffix through (earlier read†). So a trailing `/`, an extra path segment, or a different case or scheme spelling of the same physical prefix does not match the exact deny ARN.
- Physical access ignores such differences: `formatPathWithNamespace` trims `/` (`namespace.go:117`†). `ensureStorageNamespace` refuses a non-bare repository when `<namespace>/dummy` exists. A bare repository skips that check (earlier read†).
- If that holds, a guard-group member with `fs:*` could create a bare repository on the companion's prefix and GC from there. Spark GC lists `<ns>/data/` and the root, and the uncommitted GC lists the normalized namespace. Both are what R2S-3's deny exists to prevent.
- I did not establish whether `UncommittedGCProtector` (`pkg/icebergcatalog/gc.go`†) protects companion data, which would limit the impact.

**Evidence**
- The mismatch comes from `ArnMatch` (`:87`), which uses a wildcard match on the resource ID against an exact deny string.
- Design §8 and the docs commit say the deny is on the "exact storage namespace". They do not say whether variants are covered.

**Suggested fix**
- Canonicalize on both sides. Normalize the namespace in `CreateRepository` before it authorizes, or emit the deny for the normalized and trailing-slash forms.
- Alternatively, deny `fs:AttachStorageNamespace` for the guard groups on a pattern that covers the prefix, `namespace/<ns>*`. That also covers children, and it carries the same over-match trade-off the design weighed.
- Failing test: a guard-group principal creates a repository with `storage_namespace` = companion namespace + `/` and with `--bare`. Both must be 403. Today the first is not covered by the exact ARN.


### R3T-1 - MAJOR: Compact hands one data file to `ReplaceFiles` once per split scan task, which iceberg-go v0.7.0 rejects

**Fixed in `7f5fd526b`**: `dataFilesOf` de-duplicates by `File.FilePath()` and keeps the first entry, so a split file is removed once while the read side still reads every split range. `TestCompactReplacesASplitFileOnce` (a file with several row groups, more tasks than files) is red before the fix with `delete data file paths must be unique for ReplaceDataFilesWithDataFiles` and green after it.

Status: **UNVERIFIED** end to end (not run). The mechanism is read on both sides and cited below.

**Spot-check (this consolidation): CONFIRMED.** `PlanFiles` splits a Parquet file above `read.split.target-size` into tasks that each copy the same `File` (`scan_splits.go:55-60`), `dataFilesOf` appends one entry per task, and the first argument of `ReplaceFiles` is `dataFilesToDelete`, whose `setToDelete` loop returns `delete data file paths must be unique for ReplaceFiles` — so the compaction fails and its trigger stays due.

**Where**

- `pkg/mds/table/writer.go:474-476`: `scan.PlanFiles` then `dataFilesOf(tasks)`.
- `pkg/mds/table/writer.go:586-593`: `dataFilesOf` appends `task.File` for every task. Its comment says "a scan plans one task per live data file of the snapshot".
- `pkg/mds/table/writer.go:489`: `txn.ReplaceFiles(ctx, dataFiles, nil, deleteFiles, snapshotProps)`.
- iceberg-go v0.7.0, `table/scanner.go:1792` and `:1820`: local planning calls `splitParquetScanTask(task, splitTargetSize)` for every planned file.
- iceberg-go v0.7.0, `table/scan_splits.go:32-90`: a Parquet file larger than `read.split.target-size` (128 MiB default, `table/properties.go:40-41`) with at least two split offsets is returned as several tasks. Each task copies the same `File` and has its own `Start` and `Length`.
- iceberg-go v0.7.0, `table/table.go:1350`: `Table.Scan` plans locally, so `ScanPlanningLocal` is what `tbl.Scan()` uses.
- iceberg-go v0.7.0, `table/transaction.go:1682` (`ReplaceDataFilesWithDataFiles`) returns "delete data file paths must be unique for ReplaceDataFilesWithDataFiles". `replaceFiles` (starts `:1794`) has the same check in its `setToDelete` loop ("delete data file paths must be unique for ReplaceFiles"). `ReplaceFiles` reaches one of the two whichever way `deleteFiles` falls.

**What happens**

Once one data file of the snapshot is above 128 MiB and has at least two row groups, `PlanFiles` returns two or more tasks for it.

- `dataFilesOf` then lists the file twice and `ReplaceFiles` returns the duplicate-path error.
- `Compact` returns "replace the files of the table at …".
- The compaction trigger stays due (`pkg/mds/indexer/maintenance.go:49-90`), and the indexer fails the branch on it, so the same error repeats on every tick.

A compaction output can itself be such a file. The rolling writer rolls files at 512 MiB (`write.target-file-size-bytes`) and row groups at 128 MiB or 1 048 576 rows. So the first compaction of a large table can produce a file that no later compaction can replace. `Rebuild` is not affected: `snapshotFilesOf` reads the manifests and de-duplicates by path (`writer.go:533-563`).

The `d993d602e` read path also treats each split task as its own file. `openCompactionStreams` opens one run per task and `compactFileBatchRows(len(tasks))` divides the budget by tasks (`rows.go:349`, `:428-475`), so the split ranges are read as separate sorted streams. That part is harmless. Only the removal list is wrong.

Not run: whether a table can reach this state in a reasonable time, and the exact error text at runtime.

**Evidence**

Code only, cited above. `dataFilesOf` was introduced by `e3083e279`, and no test builds a file above the split target. `TestCompact*` in `writer_test.go` use files of a few rows.

**Suggested fix**

De-duplicate by `File.FilePath()` in `dataFilesOf` (keep the first). Keep the tasks as they are for the read side; each split range is a disjoint set of row groups.

A test that fails before the fix: create the table with `read.split.target-size=1` and `write.parquet.row-group-limit=2`, apply a delta of at least 5 rows so the file has several row groups, assert `PlanFiles` returns more tasks than files, then `Compact`. Before the fix it fails with the duplicate-path error; after it, it must keep every row once.


## MINOR (14)

### R3S-4 - MINOR: the init-commit walk is quadratic in the history length and reruns on every new head of `R-metadata`

**Fixed in `db52bd9a7`**: the found init commit is cached per repository - immutable once found - and a new or repeated head is verified newest first only until it reaches that commit or a previously verified head, so a branch that gained commits pays for the new commits only. Tests assert the commit-log call count on the second read, for the same head and after one new head, over 300- and 4096-commit histories.

Status: **UNVERIFIED**. The mechanism is read in code. The cost is arithmetic from the code and was not measured.

**Where**
- `pkg/icebergcatalog/resolver.go:484-500` (`firstParentCarriesInitCommit` sets `params.FromReference = after` at `:495`).
- `pkg/catalog/catalog.go:1505-1517` (`ListCommits` skips to `FromReference` by iterating from the head).
- `resolver.go:544` and `:552` (cache keyed by repository and served head commit).
- `pkg/mds/indexer/repo.go:~236` comment (†): `R-metadata` gets one indexing commit per indexed source commit.

**What happens**
- Every page call of the walk starts a fresh `Store.Log` at the head and skips `(k-1)*256` commits to reach its page. A walk of N commits costs about N²/512 iterator steps.
- The init commit sits at the bottom of a healthy history, so every successful walk reads all of it. Examples (arithmetic, not measured):
  - N = 10,000 gives about 195,000 steps.
  - N = 100,000 gives about 19.5 million steps.
  - The default graveler commit cache holds 50,000 commits (`pkg/config/defaults.go:158-160`†), so the second case also thrashes it.
- The cache key includes the head commit, and each indexing commit moves the head. So the full walk repeats for the first read after each new indexing commit, on the request path of a read of the virtual table, `HEAD` and the credentials endpoints.
- I did not find a singleflight around it, so concurrent readers may each walk. I did not verify that.

**Evidence**
- `ListCommits` skip loop, `catalog.go:1505-1517`: `for it.Next() { if it.Value().CommitID == fromCommitID { break } }`.
- The indexer's own walk (`repo.go:239`) is also a full walk, but it runs once per process start. The catalog's runs per head.

**Suggested fix**
- Cache the positive answer by the init commit's ID, which is immutable once found. For a new head, walk only until the previous verified head is reached. `LogParams` already has `StopAt`; I did not check that `listCommitsWithoutPaths` honours it.
- Alternatively, read pages through the iterator once instead of re-skipping.
- Failing test: a repository with N commits where the call counts of the commit iterator on the second read must not grow with N.


### R3S-5 - MINOR: bootstrap needs `ListPolicies` and `DeletePolicy`, which the in-repo ACL server answers with 501, and 501 is a fatal startup error

**Fixed in `9135602ea`**: `contrib/auth/acl`'s controller implements `GET /auth/policies` (prefix, after, amount, pagination block) and `DELETE /auth/policies/{policyId}` instead of answering 501, and both are covered through `APIAuthService` in the httptest run and by `TestControllerListPolicies` / `TestControllerDeletePolicy`.

Status: **CONFIRMED** (code read, no probe).

**Where**
- `pkg/mds/bootstrap/bootstrap.go:880-905` (`removeStalePolicyChunks`: `ListPolicies` at `:883`, `DeletePolicy` at `:894`).
- `bootstrap.go:822-828`, `:1027` (`policyBackendUnavailable` matches only `auth.ErrNotImplemented`).
- `contrib/auth/acl/controller.go:221-223` (`ListPolicies`) and `:254-256` (`DeletePolicy`), both `writeError(w, http.StatusNotImplemented, …)`.
- `pkg/auth/service.go:448-466` (`validateResponse` maps 501 to `ErrUnexpectedStatusCode`).
- `cmd/lakefs/cmd/run.go:304-307`.

**What happens**
- `902839f67` called neither `ListPolicies` nor `DeletePolicy`. `26a66b633` calls both on every refresh, including the one `RoutesBuilder` runs at startup.
- Against `contrib/auth/acl`'s server, `APIAuthService` returns `ErrUnexpectedStatusCode - got 501 expected 200`. `refreshPolicies` returns it as an error, not as degraded.
- `RoutesBuilder` then returns the error and the hook loop at `run.go:306` calls `Fatal`.
- `contrib/auth/acl` is an in-repo reference server (`contrib/auth/acl/cmd/acl`). I found no document in `docs/` that names it as supported or unsupported with Metadata Search, so the severity depends on that and I rated it MINOR.
- If R3S-2 holds, the create of the empty base deny policy fails first against that server, so this 501 is reached only after R3S-2 is fixed.

**Evidence**
- Every bootstrap test uses the in-process `acl.AuthService`. The service has working `ListPolicies` (`service.go:803`†) and `DeletePolicy` (`:758`). Only the HTTP controller has the 501 stubs.

**Suggested fix**
- Implement `ListPolicies` and `DeletePolicy` in `contrib/auth/acl/controller.go`, since the service methods exist.
- Alternatively, treat `ErrUnexpectedStatusCode` with 501 from those two calls as degraded and name the consequence in the warning.
- Failing test: `acl.Serve` over `httptest` plus `NewAPIAuthService`, then `RoutesBuilder`. It must not return an error, or must degrade as `BasicAuthService` does.


### R3S-6 - MINOR: the policy cache is not purged after a policy is attached, so a new chunk can be invisible for up to the cache TTL

**Fixed in `9135602ea`**: `attachPolicies` purges the auth service's effective-policy cache once after the attachments, when at least one succeeded. `TestRefreshPurgesCacheAfterAttach` adds a 65th companion that lands in a new chunk while a service refills the cache inside the attach window, and the deny is missing on the next `Authorize` before the fix.

Status: **CONFIRMED** (code read, no probe).

**Where**
- `pkg/mds/bootstrap/bootstrap.go:808-835` (`refreshPolicies` order: `ensurePolicy` for every policy, `removeStalePolicyChunks`, then `attachPolicies`).
- `bootstrap.go:846-875` (`attachPolicies`, no purge). The only purge call sites are `:897`, `:985` and `:997`.
- `pkg/auth/service.go:978-988` (`APIAuthService.AttachPolicyToGroup`, no purge).
- `contrib/auth/acl/service.go:906` (the in-process service purges on attach).

**What happens**
- The purge after a create happens before the attach. A request between the two refills the user's effective-policy entry without the new chunk, and nothing clears it after the attach.
- With `APIAuthService`, which caches per user (`auth.cache.ttl` 20 s, jitter 3 s, `defaults.go:66-69`†), the new chunk is not effective until that entry expires.
- For an indexer chunk, the new companion repository stays unwritable for the indexer. It fails closed and loudly.
- For a deny chunk, the deny on a new companion repository stays missing, which fails open.
- R2S-9 was marked fixed as "purge after every change". Design §8 says the rewrite "is visible to authorization at once".
- The in-process ACL service purges on attach, so this affects external-auth deployments only. Replicas other than the one that ran the refresh also keep their own cache until their TTL. The design does not mention that.

**Evidence**
- The grep for `purgePolicyCache(` in `bootstrap.go` lists only the definition and the three call sites above.

**Suggested fix**
- Call `purgePolicyCache` after each successful attach in `attachPolicies`, or once at the end of `refreshPolicies`.
- Failing test: reuse `cachingPolicyService` (`bootstrap_test.go:1320†`) and assert that after `refreshPolicies` adds a chunk, the next `Authorize` sees it with a cache that was refilled between create and attach.


### R3S-7 - MINOR: a companion can lose its grant and its deny between two policy writes when the sorted set shifts across chunks

**Fixed in `9135602ea`**: stable chunk assignment replaces positional chunking (`existingCompanionChunks` reads the companions each stored chunk names, `stableCompanionChunks` keeps each in its chunk and fills spare capacity with new ones), so a refresh that only adds repositories never removes an existing companion from any chunk. `TestRefreshKeepsCompanionCoverageAcrossWrites` (130 companions, the second chunk write failed) keeps the old last member of chunk 1 denied and granted.

Status: **CONFIRMED** (code read and reasoned from the loop, no probe).

**Where**
- `pkg/mds/bootstrap/bootstrap.go:214-250` (`companionRepositories` sorts by name) and `:255-262` (`companionChunks` splits by position).
- `bootstrap.go:808-835` (`refreshPolicies` writes chunks in ascending order, one `ensurePolicy` each).
- `bootstrap.go:968-1000` (`ensurePolicy` purges after each write).

**What happens**
- Chunk membership is positional. A repository inserted early in the sort order shifts every later companion up one place.
- Chunk 1 is rewritten first. It gains the new repository and drops its former last member Y. Chunk 2, which would hold Y, is rewritten afterwards.
- Between the two writes (milliseconds over HTTP), Y is in no chunk, so the indexer's grant and the guard groups' deny on an existing companion are both absent. The purge after each write makes the gap visible at once.
- If the second write fails, `refreshPolicies` returns and the gap lasts until the next refresh. A crash leaves it until the next start.
- This needs more than 64 companions. Below that, the whole set is one chunk and one atomic `WritePolicy`.
- It is a new exposure for an existing companion because of an unrelated repository. It is distinct from the accepted window for a repository created after startup.

**Evidence**
- The loop in `refreshPolicies` has no ordering that keeps every companion covered at every step. I did not find a test that asserts coverage between writes. That is UNVERIFIED, because I did not re-read all the refresher tests.

**Suggested fix**
- Make assignment stable: keep each companion in its existing chunk, and fill gaps or append.
- Alternatively, write in two phases: first add every companion to its new chunk, then remove it from the old one.
- Failing test: 130 companions, insert a name that sorts first, fail the second `WritePolicy`, and assert the old last member of chunk 1 is still denied and granted.


### R3S-8 - MINOR: and may never evict

**Fixed in `db52bd9a7`**: the cache is keyed by repository, bounded to 1024 entries with oldest-first eviction, and each entry keeps at most 8 verified heads. Tests cover 24 heads of one repository (one entry) and 1025 repositories (bounded).

Status: **UNVERIFIED**. I did not establish whether anything deletes entries.

**Where**
- `pkg/icebergcatalog/resolver.go:~330` (`initMu`, `initCache map[string]bool`†), `:544` (`cachedInitCommit`) and `:552` (`cacheInitCommit`).

**What happens**
- Each indexing commit moves the head and so creates a new key, and negative answers appear to be cached as well. If nothing evicts or bounds the map, it grows with every indexed commit of every companion for the life of the process.
- A deleted and recreated repository keeps stale keys. They cannot be matched again, because a commit ID is unique, but they are not freed.

**Evidence**
- Only the two accessors above, in the part of the file I re-read. This finding depends on there being no eviction.

**Suggested fix**
- Bound the map with an LRU. Better, cache by repository the init commit's ID once (see R3S-4), which makes the key set one per repository.
- Failing test: N distinct heads for one repository must leave a bounded number of entries.


### R3I-1 - MINOR: the new 403 → `ErrReadOnlyMetadataRepository` mapping claims branch protection for an authorization refusal, and the mds client collapses 401 into the same sentinel, so the *default* denial lands there too

**Fixed in `2ae794038`**: the `ErrReadOnlyMetadataRepository` family is unchanged, and the protection claim is made only when the wrapped cause carries lakeFS's own wording (`cannot commit to protected branch`, `cannot write to protected branch`, `read-only repository`); every other forbidden answer gets a neutral text naming branch protection, `read_only` and the indexer principal's own permissions, and quoting the cause. Reversion proofs for both non-protection cases are recorded in the commit.

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

**Fixed in `2ae794038`**: `initialise`'s `CreateTable` error is wrapped in `metadataBranchWriteError`, and the fake's `CreateTable` now checks `STAGING_WRITE` protection and `ReadOnly` like the other table writes. `TestInitialisationCreateRefusalIsTheReadOnlyFamily` and a `STAGING_WRITE`-before-the-first-tick case fail before the fix.

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

**Fixed in `2ae794038`**: `tableError` maps `table.ErrStaleTable` to `ErrConflict` alongside `icebergtable.ErrCommitFailed`, so `runBranch` reloads and retries on it. `TestTableErrorMapsTheWriterConflict/a_stale_table` fails without the mapping; the retry itself is pinned by `TestRecovery/a_concurrent_writer_moves_the_table` and `TestRecovery/two_consecutive_conflicts`.

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

**Not fixed in code** (`2ae794038` chose the documentation option): the protection refusal stays a per-branch error, so `/_health` is 503 with a per-branch `last_error` and `errors_total{stage="branch"}` per refused branch while the repository's `last_error` stays empty. This documentation card corrects design §3.3 and §9 and the guide to say so, instead of the repository-level wording the design and the guide carried.

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


### R3T-2 - MINOR: the Spark self-test's two `user_metadata` cases fail on the row count, so they do not show that `user_metadata` is compared

**Fixed in `0f45215bc`**: the two six-row cases are replaced by three cases of exactly three rows in total (one part file, and both orders over two part files), so only the `diff` can reject them, and `expect` now also asserts the gate message each case must produce. The mutation the finding says was never run - dropping the fourth column from the comparison - now fails 3/3. Follow-up outside this card's files: `esti/ops/mds-engines/README.md` still says seven cases and describes the old layouts.

Status: **CONFIRMED** by reading and arithmetic. The mutation that would show it (drop the fourth column from the comparison) was not run.

**Where**

- `esti/ops/mds-engines/run-spark.sh:161`: `bad` is `three` with `{"team":"analytics"}`.
- `esti/ops/mds-engines/run-spark.sh:209-229`: the two cases "a user_metadata in part-00001 is compared" and "… in part-00000 is compared".
- `esti/ops/mds-engines/common.sh:98`: `assert_orders_rows` dies when the normalised row count is not exactly 3.
- `esti/ops/mds-engines/common.sh:103`: the `diff -u` against the expected rows comes after it.

**What happens**

Each of those two cases writes `one, two, three` into one part file and `bad, two, one` into the other. That is six lines. `common.sh:98` rejects it as "returned 6 orders/ rows, expected exactly 3" before `diff` runs. The case would still pass if the gate ignored the fourth column completely.

The self-test has no case with exactly three rows in total where only a `user_metadata` value is wrong. The only self-test case that exercises `{}` at all is the passing one. The commit message of `2af2532b7` presents these two cases as the proof that "a wrong user_metadata … is compared". The production gate does compare the column (`write_expected_orders_rows` with `columns=4` and `diff -u`); the test just does not prove it.

**Also**

`run-spark.sh:96-109` writes two statements. The first is the guide's `SELECT path, size_bytes, commit_id, user_metadata …`; its output goes to the container log and is never parsed. The gate asserts the second statement (`INSERT OVERWRITE DIRECTORY … concat_ws(…, to_json(user_metadata))`). That is the same table, columns and filter today. The two cannot drift apart without something noticing only if a human reads the log.

The self-test proves the gate logic on fabricated files. It does not prove Spark ever writes more than one part file. The commit message already says the live run wrote one.

**Suggested fix**

Add self-test cases of exactly three rows in total that must fail through the `diff`:

- one part file holding `one`, `two`, `bad`;
- `one`, `two` in part-00000 and `bad` in part-00001, and the mirror image.

Check that each fails with "rows differ", not "wrong number". Optionally make `expect` assert which message was produced.


### R3T-3 - MINOR: a table of legacy files is materialised all at once, not "one file's memory"

**Not fixed in code** (`7f5fd526b`, deliberately): a k-way merge cannot stream a run whose rows are not in order, so materialising legacy files one at a time would mean spilling each to a temporary sorted file - new object-store writes, new orphan files for `lakectl iceberg cleanup`, new failure modes - for a state only a table last compacted before `2daeca114` can be in and that the first compaction removes. That commit corrected the `compactReader` and `Compact` doc comments in the package, and this documentation card corrects design §4.9 and the guide to "the memory of every legacy file at once".

Status: **CONFIRMED** by reading. Not measured.

**Where**

- `pkg/mds/table/rows.go:428-475` (`openCompactionStreams`): the loop opens a run for every task before the merge starts.
- `pkg/mds/table/rows.go:603-660` (`newMaterializedRun`, `materializedRun.release`): the whole file's batches are appended to `run.batches` and released only in `release`, which `mergedBatches` calls at the very end (`:757-764`).
- `METADATA_SEARCH_DESIGN.md:387-391` and `rows.go` doc on `compactReader`: "that one file's memory, not the table's".

**What happens**

The pre-`2daeca114` compaction streamed batches into `Append`, so each file it wrote is sorted inside a batch and not across the file. After such a compaction every file of the table fails `taskRowsInPathOrder`. The next compaction then reads each of those files whole, at open time, and keeps all of them until the merge finishes, so the memory is the table's, which is R2T-2's own defect for that one compaction.

`materializedRun.held()` is `len(rows)-next` (`:655`). It shrinks as rows are consumed, but the batches are not freed, so `mergeRowsHeld` under-reports it too.

This applies only to a table last compacted by the old code. The feature is not released, so the impact is on development and test deployments. The doc statement is wrong as written.

**Suggested fix**

Correct the doc to "the memory of every legacy file at once", or materialise legacy files one at a time. For example, sort each legacy file to a temporary sorted run (re-written through the same batch path) before the merge. A test: write two legacy files (as `TestCompactSortsAFileWrittenBeforeTheFix` does) and observe the held rows with `MergeRowsHeld` before the first output batch.


### R3T-4 - MINOR: the memory tests do not exercise the budget, the floor or more than one file

**Fixed in `7f5fd526b`**: `TestCompactFileBatchRows` pins 0, 1, 2, 512, 513 and 10 000 files against the production budget 131 072 and against smaller ones, so both sides of the floor are covered, and `TestCompactDividesTheMergeBudgetAcrossFiles` compacts four real files with a budget of 3000 (750 per file, above the floor) and asserts the batch rows each stream pulls. Both fail with the old code.

Status: **CONFIRMED** by reading and arithmetic. The numbers 256 and 264 are derived, not observed.

**Where**

- `pkg/mds/table/writer_test.go:851-899` (`TestCompactHoldsABoundedNumberOfRows`): `mergeBudget = 200` (`:853`), one data file (`require.Len(t, tasks, 1, …)`), and the assertion `large <= mergeBudget+outputBatch` (`:898`).
- `pkg/mds/table/rows.go:342-354`: `compactFileBatchRows(files) = max(256, budget/files)`.
- `pkg/mds/table/rows_test.go`: `TestMergeHoldsABoundedNumberOfRows` uses two synthetic streams, `batchSize = 100`, `output = 64`.

**What happens**

- With one file and a budget of 200, the per-file batch is `max(256, 200) = 256`, not 200. The assertion is `<= 264`, and the real bound is the 256-row batch plus the 64 pending rows, so it holds with a few rows of slack, not by the budget.
- Nothing runs more than one real file, so the division `budget/files` is never exercised, and nothing runs more than 512 files, so the point where the floor overrides the budget is never reached.
- `compactFileBatchRows` has no test (grep above).
- The observer (`mergeRowsHeld`, `rows.go:784`) counts `pending` plus each stream's unread rows only (see R3T-5), so neither test can see the memory outside the merge.
- A "revert" of the fix cannot run `TestCompactHoldsABoundedNumberOfRows` against the old code, since `MergeRowsHeld` is part of the new code. The test is therefore a regression guard for the new structure, not a test that failed before the fix.

**Also**

`openCompactionStreams` opens a scan, a cursor and (for an in-order file) a worker pipeline for every task at once. The floor means the live rows are `files × 256` once there are more than 512 files, and §4.9's compaction runs after a whole tick of commits (`branch.go:164-165`), so the file count is not bounded by `every_commits`. Open handles and goroutines per file were not measured: UNVERIFIED.

**Suggested fix**

Add a unit test of `compactFileBatchRows` (1, 2, 512, 513, 10 000 files, budget 131 072) and a test with several real files and a budget that makes `budget/files` differ from the floor. Assert the batch rows seen by a stream instead of the sum.


### R3T-5 - MINOR: the documented memory bound counts the merge's own rows, not the scan pipeline or the writer queue

**Not fixed in code**: this one is the written bound, not a repair. `7f5fd526b` wrote the corrected bound out from the module sources pinned in `go.mod` - merge set (with the 131 072-row budget, the 256-row floor and the 512-file point), scan prefetch, writer queue, one row group, plus the legacy case - and this documentation card puts it into design §4.9. No memory was measured for it.

Status: **UNVERIFIED.** The mechanism is read in iceberg-go v0.7.0; no memory was measured beyond what `d993d602e`'s own commit message reports.

**Where**

- `METADATA_SEARCH_DESIGN.md:381-387` ("one read batch per file plus one batch of output").
- iceberg-go v0.7.0:
  - `table/arrow_scanner.go:2472-2478`: the scan's `records` channel has capacity `numWorkers`. `internal/utils.go:85-110` (`MakeSequencedChanWithDiscard`) adds an output channel of the same size. So a stream can have more than one batch alive beyond the one the merge holds.
  - `table/rolling_data_writer.go:51` and `:366-370`: the writer's `recordCh` holds 64 batches by default, and `Add` (`:423`) retains each one until it is written.
  - `table/arrow_utils.go:2126`: `unpartitionedWrite` pulls the merge from its own goroutine, so the merge can run ahead of the Parquet encoder by that queue.
  - `table/internal/parquet_files.go:838-845`: the writer buffers a row group (128 MiB or 1 048 576 rows by default) before it rotates.
- `pkg/mds/table/rows.go:829-830` and `:904`: an output batch of one source batch is a zero-copy slice, so a queued output batch keeps its whole source batch alive.

**What happens**

The merge's own live set is bounded as documented. What the process holds while compacting is the merge's set plus the scan pipeline's prefetch per file, up to 64 queued output batches of `compactRecordBatchSize` (64 Ki) rows and their source batches, and one buffered row group. All of that is bounded and independent of the table size, but it is a large constant, and it is not what §4.9, the tests or the comment claim.

The author's own numbers support the doubt: the commit message of `d993d602e` reports peaks of 50 MiB for 50 000 rows and 128 MiB for 200 000 rows after the fix ("2.56x for 4x rows") and says it "saturates at the 131 072-row read batch", but 200 000 rows is far below the 4.2 M rows the writer queue can hold, so that probe cannot show saturation.

**Suggested fix**

Either state the bound as "merge set + scan prefetch + writer queue + one row group" in §4.9, or bound it: pass `icebergtable.WithRecordBatchBufferSize` (iceberg-go `write_records.go:105`) if the `Append` path allows it. A measurement that would settle it: a `runtime.MemStats` probe over 0.2 M, 2 M and 8 M rows.


### R3T-6 - MINOR: `ErrMergeRows` has no test, and the recover only covers the merge's pull loop

**Fixed in `7f5fd526b`** (test only; the recover was already there and its scope is unchanged): `TestMergeRowsConvertsAPanicToErrMergeRows` runs a `rowStream` whose `advance` panics through the merge and requires `Next()` false, `errors.Is(reader.Err(), ErrMergeRows)`, the panic value in the message and a second `Next()` still false. It fails without the recover as an escaped panic. Design §4.9 now states that the setup reads are outside the recover.

Status: **CONFIRMED** that there is no test (grep above). The scope sub-claim is **UNVERIFIED**.

**Where**

- `pkg/mds/table/rows.go:767-770`: `recover()` in `mergedBatches` turns a panic into `yield(nil, fmt.Errorf("%w: %v", ErrMergeRows, recovered))`.
- `pkg/mds/table/rows.go:481-535` (`taskRowsInPathOrder`) and `:603-655` (`newMaterializedRun`): both iterate a scan from `compactReader` (`:418-426`), before `mergedBatches` runs, so they are outside that recover.

**What happens**

`d993d602e` says "a merge panic becomes `ErrMergeRows`", and §3.1 and the commit message rely on it, but no test in `pkg/mds` names `ErrMergeRows` or injects a panic into a stream. The sibling fragment exercised it only through an injected error at the indexer's `Table` fake, which does not enter `rows.go`. The recover has the right shape (the body runs inside iter.Pull2's coroutine, and the deferred `releasePending` runs before it), but nothing demonstrates it, including that `yield` can be called from a deferred function and that the next `Next` returns the error.

The setup reads (path detection, materialisation) run in the caller's goroutine. A panic there is not converted. Whether that is a problem depends on whether the caller recovers; that was not checked.

**Suggested fix**

Add a `rowStream` test double whose `advance` panics. Run `newPullRecordReader(…, mergedBatches(…))` and require `Next()` false and `errors.Is(reader.Err(), ErrMergeRows)`. It fails before the recover is added.


## NIT (4)

### R3S-9 - NIT: the R2S-5 write-vend guard is exercised only by a fake resolver, and the built-in resolver never reports its own target as reserved

**Fixed in `db52bd9a7`** (test only, no production change): a branch write credentials request for the virtual table now has a test through the real `MetadataSearchResolver`, which yields a read session scoped to the companion table with no `s3:PutObject`.

Status: **CONFIRMED** (code read).

**Where**
- `pkg/icebergcatalog/creds.go:451-458` (the `OpWriteTableData && !s.writableNamespace(scope.Target)` check) and `:533-534` (`writableNamespace`).
- `pkg/icebergcatalog/resolver.go:583-592` (`ReservedNamespace` and `reservesSystemNamespace`: `addr.Repo != "" && !IsMetadataRepository(addr.Repo)`).
- `pkg/icebergcatalog/creds_test.go:764-794` (`TestCredentialVendingReservedTarget`, `signRedirectResolver{…, reserved: target}`).

**What happens**
- For the built-in resolver, the requested address is always the data repository's `system` namespace. `TableCredentials` already refuses the write there through `writableNamespace(addr)` (`creds.go:471`) before the vend. The resolved target is in `R-metadata`, where `ReservedNamespace` returns false. So the new `scope.Target` check never fires for the only in-tree resolver.
- The guard matters for a custom resolver that redirects an ordinary namespace to a reserved one. The test passes because the fake marks its own target reserved.
- This is not a hole today. The risk is that the test gives the impression the built-in redirect is covered by this check, and a future change to the requested-address check would leave the built-in path unprotected without failing a test.
- `writableNamespace` returns true when `s.store == nil` (`:534`). `http.go:150-170`† always builds a store, so I found no production path with a nil store, and the nil-store case would panic in `newTableScopeResolver` anyway.

**Evidence**
- `creds_test.go:775` names `reserved: target`. There is no test that uses `NewMetadataSearchResolver` and asks for a write session on the virtual table through a non-reserved front.

**Suggested fix**
- Add a test through the real `MetadataSearchResolver` that a branch write `Vend` for the virtual table yields a read session. It protects the requested-address check, which is the guard that actually applies.
- Optionally make `reservesSystemNamespace` treat the companion's `system` namespace as reserved too, so both checks agree.


### R3I-5 - NIT: the catalogue half of the R2I-3 mapping (`rest.ErrForbidden`) is dead in the test suite, because the fake's table writes answer the API sentinel instead of the catalog's

**Fixed in `2ae794038`**: the fake's table writes answer `catalogWriteError` - `rest.ErrForbidden` over lakeFS's message, and in `CreateTable` too - which is what production sees, so the catalog arm of the mapping is exercised. Reverting it makes `TestProtectedMetadataBranchFailsTheRepository/staging_write_protection` fail `ErrorIs rest.ErrForbidden`.

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


### R3T-7 - NIT: streams opened by `compactReader` are released only by the merge's own `defer`, which does not run if the reader is released before its first pull; and no test checks Arrow refcounts

**Fixed in `7f5fd526b`**: `newMergeReader` installs `pullRecordReader.onRelease`, which runs after `stop()`, so `Release` without a pull frees the streams, and `mergedBatches` no longer frees them. `TestCompactionReaderReleaseFreesStreamsWithoutAPull` runs on a `memory.CheckedAllocator` and is red without the wiring; `TestCompactReleasesEveryBatchItReads` compacts three files and requires `AssertSize(t, 0)`.

Status: **CONFIRMED** by reading. Harmless under the Go allocator in production; not probed.

**Where**

- `pkg/mds/table/writer.go:481-485` (`compactReader`, then `defer reader.Release()`), `:489-496` (`ReplaceFiles` and `Append` can fail before any batch is pulled).
- `pkg/mds/table/rows.go:755-766`: the stream release is a `defer` inside the iterator body.
- go1.26.4 `src/iter/iter.go:376-445`: `Pull2`'s coroutine starts with `if pull.done { return }`, and `stop` sets `done` first. So `stop()` before the first `next()` never runs the iterator body.
- `pkg/mds/table/rows.go:904`: `array.Concatenate(parts, memory.DefaultAllocator)`; also `:109-141` and `:152`.

**What happens**

If `ReplaceFiles` or `Append` fails before the first pull, `reader.Release()` calls `stop()`, the merge body never runs, and `release()` of the opened streams (scans, cursors, and for a legacy file its batches) is never called. The doc comment on `rowStream.release` ("frees the stream: its file, its scan, its batches and its goroutine") is not true on that path. Under `memory.DefaultAllocator` the garbage collector reclaims everything, so no leak follows. A `memory.CheckedAllocator` would report it.

The code uses `memory.DefaultAllocator` directly, so no test can inject a checked allocator for `buildMergedBatch`. By reading, the Retain and Release in the merge are balanced: one `Retain` per pending row (`:829`) and one `Release` per pending row in `releasePending`; the concatenated columns are released after `NewRecordBatch`; the error path releases `columns[:c]` and `parts`. That is a reading, not a test.

**Suggested fix**

Release the streams in `compactReader`'s reader (for example from `pullRecordReader.Release`) instead of from the iterator body, or open the streams lazily inside the body. For a test, route the allocator through a package variable, use `memory.NewCheckedAllocator`, and `AssertSize(t, 0)` after (a) a full compaction and (b) a compaction whose `Append` is made to fail before the first pull.


### R3T-8 - NIT: the schema of the compaction reader is `nil` for a streamed run, so the "no task" branch is the common path and the declared schema differs from the yielded batches

**Fixed in `7f5fd526b`**: the cursor stores the schema `ReadTasks` returns at open, so the empty-plan branch is now only the empty table. `TestCompactKeepsAColumnTheWriterDoesNotKnow` asserts the reader's schema is known before the first batch and `declared.Equal(batch.Schema())` for every batch, including the multi-run ones; it is red when the cursor stores no schema.

Status: **UNVERIFIED** consequence (the suite is green). The nil schema is **CONFIRMED** by reading.

**Where**

- `pkg/mds/table/rows.go:537-557`: `newStreamedRun` does not store the schema `ReadTasks` returned.
- `pkg/mds/table/rows.go:559` and `:723`: `streamedRun.schema()` returns `r.batch.schema`, which `recordCursor.advance` sets only when it reads the first batch.
- `pkg/mds/table/rows.go:454-475`: `schema = stream.schema()` runs before any `advance`; the "No task, so no row" branch then computes `ArrowSchema(projection)`.

**What happens**

For a table whose last file is in order (the normal case), `schema` is `nil` at `:456`. The reader therefore declares `ArrowSchema(tbl.Scan().Projection())` (with field IDs, `rows.go:91`), while the single-run batches it yields are slices of the scan's batches, whose schema has no field IDs; only multi-run batches use the declared schema (`buildMergedBatch`, `:877-915`). When the last file is a legacy one, the schema is the scan's own. The comment at `:457` describes only the empty-table case. All current tests pass, so iceberg-go copes with both, but the declared schema depends on which kind of run happens to be last.

**Suggested fix**

Store `schema` in the cursor at open (as `numberedStream` does in `rows_test.go`). Add an assertion in `TestCompactKeepsAColumnTheWriterDoesNotKnow` that the reader's schema equals the schema of the batches it yields.


## Checked, no finding

Merged per area. Each bullet is the card's own text, kept in full; where a bullet is
marked † or UNVERIFIED, that marker is the card's, not this consolidation's.

### Security: the bootstrap policies and the catalog's proof and credential fallback (R3S)

- **Chunk boundaries (`companionChunks`, `bootstrap.go:255-262`).** `min(start+64, len)` has no off-by-one, and every companion lands in exactly one chunk. Chunk names are 1-based (`chunkPolicyName`). The 160 KiB cap passes in the baseline `go test -race` run. I did not recompute the byte sizes, and the figure (1000 repositories, 34 policies, largest 67,944 bytes) is `MDS_REVIEW_2.md:620`'s, not mine.
- **Operator-owned policy.** `isOwnedPolicyName` (`:276-292`) matches the base name or a purely numeric suffix. `mds-indexer-extra-storage-namespace` is never listed for deletion. An operator policy named `mds-indexer-<digits>` would be deleted, which is the stated contract.
- **`SetBranchProtectionRules` deny.** It is applied on the repository ARN only (`MetadataRepoProtectPolicies`, `repositoryActions`). The real checks in `controller.go` are on `RepoArn` (`:2636`, `:4269`, `:4299`, `:4315`†). `retention:*` stays allowed, as designed.
- **Stale-chunk deletion.** It runs after the creates and before the attaches. A crash after deletion and before attachment leaves an unattached chunk. That grants and denies nothing, so it fails closed for the indexer and open for the deny, within the accepted window. Pagination of `ListPolicies` follows `NextPageToken` correctly against the ACL service.
- **Cache purge after write and delete.** Present on rewrite, create and delete (`:897`, `:985`, `:997`). The gap is the attach (R3S-6). A service whose `Cache()` is nil is skipped (`purgePolicyCache`).
- **`RoutesBuilder` and the refresher.** The refresher starts only after `RoutesBuilder`'s own `EnsurePrincipals` returned (`:566-578`), so the two do not overlap in one process. It stops for good when `refreshPolicies` reports `inForce=false` (`:714`) and retries every minute with a `Warn` on any other error (`:696-712`). That is the documented design ("a KV hiccup must not stop a server"), not a hidden error. The degraded start does not retry, which is also documented.
- **Fail-closed resolver.** A `BranchLister` without `ListCommits` makes `firstParentCarriesInitCommit` return `false, nil`, so the table is not served (`resolver.go:485-488`). The wired catalog implements it.
- **Vended-credential fallback.** `isWriteRefusal` (`creds.go:557`) covers `ErrPermissionDenied`, `ErrRefReadOnly` and `ErrReservedNamespace`. The fallback goes to a read `Vend`, which re-authorizes. Reviewed with R3S-9.
- **`companionRepositories` criterion.** An ordinary `x-metadata` repository without an existing `x` is neither granted nor denied (`:214-250`). IDs that do not match `ReValidRepositoryID` are dropped.
- **Docs commit `b4c8c24ba`.** Design §8's description of the deny and chunking matches the code. Three claims are contradicted by findings above: "the rewrite is visible to authorization at once" (R3S-6), the two-fact proof as an ownership proof (R3S-1) and "all 17 fixed". R2S-1, R2S-5 and R2S-9 are not fully removed if R3S-1, R3S-9 and R3S-6 hold. R2S-3 is not fully removed if R3S-3 holds.

### Indexer: package `pkg/mds/indexer` (R3I)

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

### Table compaction and the Spark gate (R3T)

By reading unless a command is named. Nothing below was run beyond the Verified table.

- **k-way merge order, ties and duplicate paths:** `streamHeap.Less` (`rows.go:855-862`) orders by path and then by file position, so a tie is deterministic. `mergedBatches` rejects only a strictly smaller path (`:818-826`), so equal paths pass and every row is kept (no de-duplication, as documented in `taskRowsInPathOrder`).
- **Empty files and batches:** `recordCursor.advance` (`rows.go:694-726`) skips a nil batch and releases and skips an exhausted or empty one. With no task, `Compact` writes nothing (`writer.go:493`) and the reader still has a schema (`rows.go:456`). An empty-row file simply yields no run row.
- **Per-file batch size and `WithArrowBatchSize`:** the option is stored on the scan (`table.go:1330` in iceberg-go v0.7.0), forwarded at `scanner.go:2247`, and overrides `read.parquet.batch-size` at `arrow_scanner.go:2584-2593`. The three scans of a file (path detection, streamed run, materialised run) all pass `batchRows` (`rows.go:481`, `:537`, `:603`).
- **Unknown-column preservation vs `ErrUnknownSchema`:** `buildMergedBatch` never addresses a column by position, and `pathColumn` finds `path` by name (`rows.go:877-915`, `:925`). `checkKnownSchema` guards `ApplyDelta` only when there are upserts (`writer.go:250`), `Rebuild` (`writer.go:370`) and the two record readers (`rows.go:109`, `:152`); `Compact` does not call it. It compares count, name and ID by position (`schema.go:186-199`), so a reordered table is refused conservatively. It does not compare types; I found no legal Iceberg promotion for the ten columns' types, so no consequence was found.
- **`commit.retry.num-retries=0`:** `ensureNoReplay` (`writer.go:171-193`) reads the property with the same `GetUInt64` default that iceberg-go's `readRetryConfig` uses (`table.go:1048`), repairs with one metadata-only commit, and returns `ErrStaleTable` only when the repair moved the snapshot. `schema_test.go:184` asserts the stale rebuild returns `ErrStaleTable`. The indexer not treating `ErrStaleTable` as a conflict is **R3I-3**.
- **Arrow refcounts in the merge:** balanced by reading (see R3T-7); not run under a checked allocator.
- **Docs against code for the numbers:** §4.9's "2 × 64 Ki-row record batches, floor 256" matches `rows.go:342-354`; §3.1's `ErrUnknownSchema` rule (refused on upserts and `Rebuild`, delete-only allowed, `Compact` re-emits the scan's columns) matches `writer.go:250`, `:370`, `:453-497`. The one doc claim that does not hold is "that one file's memory" (R3T-3).
- **Spark gate, real code path:** `check_spark_rows` and `concat_part_rows` (`common.sh:117-127`) match `part-*` only, so `.crc` and `_SUCCESS` files are ignored; a directory with no part file fails; `assert_orders_rows` requires exactly three rows and then `diff -u` (`common.sh:86-108`). The self-test run (7 PASS) is what the commit message reports.

## Review limits carried over from the cards

The security and table cards were cut short by a time limit; the indexer card was
complete. Their own "Not completed" lists are merged here because they bound what the
findings above prove. Nothing in this section is a finding.

### Security

- **No revert proof for any fix.** I did not revert `26a66b633` or `500a00a37` and rerun their tests: `TestCompanionStorageNamespaceDeny`, `TestPolicySizeAtScale`, `TestRefreshPurgesPolicyCache`, `TestOperatorPolicySurvivesRefresher`, `TestCredentialVendingReservedTarget`, and the resolver ownership tests that use `newResolverFixtureWithoutInitCommit`. Whether each new test fails without its fix is unknown.
- **No probe tests.** All of the following are code reasoning only:
  - namespace variants against the deny (R3S-3)
  - forged init via `Commit` and `CreateCommitRecord` (R3S-1)
  - `EnsurePrincipals` over `acl.Serve` and `APIAuthService` (R3S-2, R3S-5)
  - cache growth and walk cost (R3S-4, R3S-8)
  - crash between policy writes (R3S-7)
- **Candidates not examined or only partly examined.**
  - Two replicas running the refresher with different `metadata_search.repositories` files, which may rewrite the policies against each other. I did not analyse it.
  - Whether `isInitCommit` in the indexer checks the committer.
  - Whether `UncommittedGCProtector` protects companion data (limits R3S-3's impact).
  - Percent-encoded namespace spellings (R3S-3).
  - A web search for any of the behaviours above. None was done.
- **Area 2 scope.** I read the resolver's cache, walk and fail-closed path and the `creds.go` fallback. I did not review `Resolve`'s ref handling for first-parent walks through merge commits.
- **The wider docs commit check** covered only the design §8 and `MDS_REVIEW_2.md` claims named above, not `docs/metadata-search.md`.

### Table compaction and the Spark gate

The brief asked for the following, which this fragment does not contain:

- **Revert proofs:** none was run. I did not check out the `d993d602e^` or `2af2532b7^` versions to see the new tests fail. In particular, whether `TestAStaleRewriteIsRefusedNotReplayed`, `TestEveryWriteRepairsTheCommitRetryProperty`, `TestWritesRefuseAColumnTheWriterDoesNotKnow`, `TestCompactSortsAFileWrittenBeforeTheFix` and the Spark self-test fail without their fixes is **not established** here. Only the commit messages claim it. For the memory tests, R3T-4 explains why a revert cannot even compile.
- **Probes:** none was written or run. Missing: a split-file Compact (R3T-1), a `memory.CheckedAllocator` leak check (R3T-7), ties and duplicate paths across files at runtime, many files against the budget with `MergeRowsHeld`, nested or reordered columns at runtime, a panic inside the merge, and a `MemStats` curve past the writer queue (R3T-5).
- **iceberg-go v0.7.0 retry behaviour** was read, not exercised; the stale-rebuild scenario was not reproduced.
- **Spark:** the live run was not made (no docker). The multi-part gate is judged on the self-test and by reading only.
- **Docs of `b4c8c24ba`:** I compared `METADATA_SEARCH_DESIGN.md` §3.1 and §4.9 and the Spark section of `docs/metadata-search.md` (`:504-570`) with the code. I did not read the esti README, the design's other changed sections, or `METADATA_SEARCH_PLAN.md`.
- **Other commits of the range:** `26a66b633` (bootstrap policies) and `500a00a37`/`8df39cabd` (iceberg catalog) were not read by me. `da24d421f` (indexer) is covered by the indexer area above (it was `review/mds3/indexer.md`, now merged into this file).
- **Areas not checked at all:** `WithRecordBatchSize` and the delete-mode paths of `ApplyDelta` beyond the unknown-schema guard, and the `Rebuild` streaming path, which `d993d602e` did not change apart from `ensureNoReplay` and `checkKnownSchema`.

### Indexer

The indexer card records no unrun step: the four reversion proofs and the seven probes
ran, and the only temporary artefact (`pkg/mds/indexer/zz_probe_review_test.go`) was
deleted before its commit. It did not run docker, esti or the engine harness, and says so:
none was needed for a card that reads code and drives the existing fake. Its scope was
`pkg/mds/indexer` plus the `pkg/mds/table` error values the indexer consumes; it did not
read the bootstrap or iceberg-catalog commits, which the security area covers.

