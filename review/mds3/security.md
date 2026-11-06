## Security: bootstrap policies (`pkg/mds/bootstrap`) and the catalog's ownership proof and credential fallback (`pkg/icebergcatalog` `resolver.go`, `creds.go`)

Range reviewed: the fixes after `902839f67`, namely `26a66b633` (bootstrap policies), `500a00a37` (merged in `8df39cabd`; resolver and creds) and the docs commit `b4c8c24ba`. The review was cut short by a time limit. Read the "Not completed" section before relying on this file. Every finding carries a status:

- **CONFIRMED** means I read the code and cite it. No probe was run for any finding in this file.
- **UNVERIFIED** means the mechanism looks real in the code but I did not prove it, and the text says what is missing.

Citations marked † come from an earlier read of the file and were not re-grepped in this session. All other file:line citations were grepped in `/tmp/mds-fork/wt-5ddb3`.

### Verified

| Command | Result |
|---|---|
| `git rev-parse --short HEAD` in `/tmp/mds-fork/wt-5ddb3` | `45087bc2e` |
| `git diff --stat 0dbf5db98 HEAD -- pkg/mds/bootstrap pkg/icebergcatalog contrib pkg/auth` | Empty. The product code of this area is identical to the tree `0dbf5db98` on which the baseline below ran. |
| `git diff 902839f67 HEAD --stat` limited to this area | 7 files, 1294 insertions, 277 deletions: `creds.go`, `creds_test.go`, `resolver.go`, `resolver_test.go`, `sign_test.go`, `bootstrap.go`, `bootstrap_test.go`. Nothing in `contrib/`, `pkg/auth` or `cmd/lakefs-sso`. |
| Baseline (in the other worktree at `0dbf5db98`, `GOTOOLCHAIN=auto`, which resolved to go1.26.4): `go build ./...` | rc=0 |
| Same worktree: `go vet ./pkg/mds/bootstrap/... ./pkg/icebergcatalog/... ./cmd/lakefs-sso/...` | rc=0 |
| Same worktree: `go test -count=1 -race -timeout 10m` on the same packages | All ok: bootstrap 5.9s, icebergcatalog 29.7s, cleanup 6.1s, conformance 4.4s, importer 7.4s. `cmd/lakefs-sso` has no test files. |
| Same worktree: `go tool golangci-lint run --allow-parallel-runners` on the same packages | 0 issues |
| `grep -n` of every function and call site cited below, in `/tmp/mds-fork/wt-5ddb3` | The cites not marked † match this tree. |

No revert-and-rerun proof was run for any fix, and no probe test was written (see "Not completed").

---

### R3S-1 - MAJOR: the `mds: init` ownership proof can be forged by any caller who can commit to `R-metadata`

Status: **CONFIRMED** (code read, no probe).

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

---

### R3S-2 - MAJOR: the base `mds-metadata-repo-protect` policy is always written with zero statements, which a spec-validating auth service may reject

Status: **UNVERIFIED**. The schema, the validator and the policy are read in code. No HTTP round trip was run, and I did not read the kin-openapi library to confirm it answers 400.

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

---

### R3S-3 - MAJOR: the exact-namespace `fs:AttachStorageNamespace` deny matches only the stored spelling of the namespace

Status: **UNVERIFIED**. The mismatch is read in code. I did not run a `CreateRepository` with a variant namespace or a GC against the new repository.

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

---

### R3S-4 - MINOR: the init-commit walk is quadratic in the history length and reruns on every new head of `R-metadata`

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

---

### R3S-5 - MINOR: bootstrap needs `ListPolicies` and `DeletePolicy`, which the in-repo ACL server answers with 501, and 501 is a fatal startup error

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

---

### R3S-6 - MINOR: the policy cache is not purged after a policy is attached, so a new chunk can be invisible for up to the cache TTL

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

---

### R3S-7 - MINOR: a companion can lose its grant and its deny between two policy writes when the sorted set shifts across chunks

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

---

### R3S-8 - MINOR: `initCache` is keyed by repository and head commit and may never evict

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

---

### R3S-9 - NIT: the R2S-5 write-vend guard is exercised only by a fake resolver, and the built-in resolver never reports its own target as reserved

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

---

### Checked, no finding

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

### Not completed

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
