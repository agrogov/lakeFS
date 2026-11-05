## Server security: `pkg/mds/bootstrap` policies and refresher, the import deny, `cmd/lakefs-sso` wiring, `pkg/icebergcatalog` sign/resolver/config

Area reviewed: the exact-name §8 policies and their background refresher (`pkg/mds/bootstrap`), the import
decision of the deny, the `lakefs-sso` wiring, and the `pkg/icebergcatalog` changes from `d2811fdcc`
(write-sign refusal, merged in `224fc82d3`), `4aaf60cb3` (companion-table ownership proof and
`metadata_search`-without-`iceberg` validation, merged in `a397ed971`) and `9cf0869c8` (import actions).
Findings are `R2S-<n>`. No product, test, design or doc file was changed by this card.

### Verified

| Command | Result |
|---|---|
| `git log --oneline e2333be26..HEAD` | 16 commits. In this area: `d2811fdcc` (MDS-SEC-1), `5b997972e` (MDS-SEC-2, MDS-SEC-5), `4aaf60cb3` (MDS-SEC-3, MDS-SEC-4), `9cf0869c8` (import half of MDS-SEC-5); the two branches are merged in `224fc82d3` and `a397ed971`. The doc commits (`fe1e3bbf5`, `df9744488`) and the other ten commits are outside this fragment. |
| `go version` (with `$HOME/sdk/go1.25.8/bin` first on `PATH`, `GOTOOLCHAIN=auto`) | `go version go1.26.4 darwin/arm64` (the `go1.25.8` directory holds a 1.26.4 toolchain) |
| `go build ./...` | pass, exit 0, 20.8 s |
| `go vet ./pkg/mds/bootstrap/... ./pkg/icebergcatalog/... ./cmd/lakefs-sso/...` | pass, no output, `vet exit=0` |
| `go test -count=1 -race -timeout 10m ./pkg/mds/bootstrap/ ./pkg/icebergcatalog/ ./cmd/lakefs-sso/` | `ok pkg/mds/bootstrap 4.070s`, `ok pkg/icebergcatalog 27.168s`, `cmd/lakefs-sso [no test files]` |
| `go tool golangci-lint run --allow-parallel-runners ./pkg/mds/bootstrap/... ./pkg/icebergcatalog/... ./cmd/lakefs-sso/...` | `0 issues.` |
| Revert `d2811fdcc`'s two guards in `pkg/icebergcatalog/sign.go` (`&& false` on both `requireWritable` calls), `go test -run TestRemoteSigningReservedNamespace` | **red**: both `a write sign on the reserved virtual address is refused` and `a redirect into a reserved namespace is refused` answer **200** instead of 403 (body `{"uri":"https://signed.example/object?X-Amz-Signature=test",...}`). Restored with `cp /tmp/sign.go.bak`; `git diff` empty. |
| Revert `4aaf60cb3`'s verifier block in `Store.Resolve` (`&& false`), `go test -run TestMetadataSearchCompanionRepositoryProof` | **red**: `a companion table that names another repository is not served` and `a companion table with no repository property is not served` answer **200** instead of 404. Restored; `git diff` empty. |
| Revert `4aaf60cb3`'s `Config.Validate` cross-check (`&& false`), `go test -run TestLoadConfigValidationErrors` | **red**: `An error is expected but got nil` for both `metadata_search without the catalog` cases. Restored; `git diff` empty. |
| Remove `fs:Import*` from `metadataRepoProtectFSDeniedActions`, `go test -run 'TestMetadataRepoProtectCoversImportActions|TestMetadataRepoProtectPolicyStatements'` | **red**: `Not equal: expected: false` (ImportCancel allowed) and `Should be true` (statement list). Restored; `git diff` empty. |
| Rebuild the pre-`5b997972e` wildcard policy statements in a probe and run the MDS-SEC-2 decisions through the real ACL service | **as the review found**: `mds-indexer fs:WriteObject …/analytics/object/foo-metadata/data.yml → allowed=true`, `fs:DeleteObject …/analytics/object/-metadata → true`, `catalog:DeleteTable …/table/analytics/foo-metadata/orders → true`, guard-group operator `fs:WriteObject …/foo-metadata/data.yml → allowed=false`. So `TestMDSPoliciesDoNotReachDataRepositories` is red on the policy the fix replaced. |
| Delete the `startPolicyRefresher(ctx, deps, policyRefreshInterval)` call from `RoutesBuilder`, `go test ./pkg/mds/bootstrap/ -run 'TestRoutesBuilder\|TestKnownDataRepositories\|TestPolicyRefresher\|TestEnsurePrincipals'` | **green**, `ok … 1.310s` — the production wiring of the refresher is untested (R2S-8). Restored; `git diff` empty. |
| Probe — `pkg/mds/bootstrap/probe_r2s_test.go` (deleted before commit): policy size, operator-statement repair, ordinary `*-metadata` repository, deny gaps, namespace bypass | outputs quoted in R2S-2, R2S-3, R2S-4, R2S-6, R2S-7 |
| Probe — `pkg/icebergcatalog/probe_r2s_test.go` (deleted before commit): ownership-proof forgery, credential redirect | outputs quoted in R2S-1 and R2S-5 |
| Probe — `wildcard.Match` on the exact ARN shapes (`pkg/mds/bootstrap`, temporary) | `namespace/comp/*` matches `namespace/comp/` (the repo-level listing ARN) and `namespace/comp/system`; `table/comp/*/*` matches `table/comp/system/object_metadata` and a deeper name, and **not** `table/comp2/system/x`; `repository/comp/*` matches `repository/comp/branch/main` and **not** `repository/comp` or `repository/comp2/object/x`; `namespace/*-metadata` matches only names ending in the suffix. |

### R2S-1 - MINOR: the MDS-SEC-3 "proof" is a table property the attacker writes, so the finding it claims to fix still works

**Where**: `pkg/icebergcatalog/resolver.go:406-419` (`VerifyResolved` compares
`tbl.Metadata.Properties()[MetadataSearchRepositoryProperty]` with `requested.Repo`),
`pkg/icebergcatalog/resolver.go:483-510` (`Store.Resolve` calls it),
`pkg/icebergcatalog/resolver.go:126` (the property literal), and the write half
`pkg/mds/table/schema.go:221` (`Properties(repository)` puts it in the create request).

**What happens**: MDS-SEC-3's scenario is "whoever creates a repository named `R-metadata` before the
indexer does owns the table served to every reader of `R`". The fix makes the catalog serve that
repository only when its `system.object_metadata` table carries `lakefs.mds.repository = R` — a value the
attacker puts in `metadata.json`, or in the `properties` of `POST /v1/…/tables` (the catalog passes the
client's `request.Properties` straight into `table.NewMetadataWithUUID`, `tables.go:724`). Nothing
distinguishes a table the indexer wrote from one the attacker wrote, and the write is not authorized by
the *server*: it is content. So the attack is unchanged except for one extra property, and the
consequence MDS-SEC-3 lists is unchanged too: the untrusted `metadata.json` `location` is what `sign.go`
(`tableLocation`/`loadLocation`) and `creds.go` (`newTableScopeResolver`) trust, so a reader of `R` is
handed **read** signatures/sessions scoped to a prefix the attacker chose.

**Evidence**: probe (`pkg/icebergcatalog/probe_r2s_test.go`, `newResolverFixtureWithTableProperty(t, "")`):
```
PROBE ownership proof: alice load of lakehouse.main.system.object_metadata before the property: 404
PROBE ownership proof: alice load after the same document was rewritten with lakefs.mds.repository=lakehouse: 200
```
The only change between the two lines is the content of the same `metadata.json` at the same location, via
`putResolverObject(t, f.catalog, resolverMetaRepo, f.metaLocation, …)` — an operation any principal that
can write that repository performs. `TestMetadataSearchCompanionRepositoryProof`
(`resolver_test.go:687`) pins the 404 cases and `TestMetadataSearchVirtualTableRead` pins the 200 for a
fixture whose companion repository *the fixture* created; no test asks who wrote the property.
The review's own suggested fix asked for "the `lakefs.mds.init` marker at an accepted position … and,
ideally" — that half was not implemented (`git grep lakefs.mds.init pkg/icebergcatalog` is empty;
`pkg/mds/indexer/repo.go` knows the marker but the catalog does not).

**Suggested fix**: prove ownership with state the attacker's `metadata.json` cannot forge: require the
`mds: <R>@<commit>` init commit (`lakefs.mds.init=true`, `pkg/mds/indexer/repo.go`) on the served ref's
first-parent history, or record the companion table's UUID in server-side state at creation and compare,
in addition to the property. Test: a companion table with `lakefs.mds.repository=R` but no init commit is
404 (it is 200 today); a genuine index is 200.

### R2S-2 - MINOR: the policies grow one statement group per repository; the deny exceeds DynamoDB's 400 KiB item limit at ~560 repositories and the server then refuses to start

**Where**: `pkg/mds/bootstrap/bootstrap.go:158-181` (`companionRepositories`), `:280-333`
(`IndexerPolicy`, six statements per companion), `:338-365` (`MetadataRepoProtectPolicy`, five per
companion), `:643-650` (`refreshPolicies`), `:713-741` (`ensurePolicy` → `WritePolicy`),
`pkg/auth/model/model.go:295` and `pkg/kv/msg.go:30-35` (a policy is one KV item, `proto.Marshal` +
`Set`, never chunked).

**What happens**: an installation with N repositories writes `6N+2` allow statements and `5N` deny
statements. The proto value is 361 031 bytes for 500 repositories and 722 031 bytes for 1000, so the deny
item crosses DynamoDB's 400 KiB item limit at about 560 repositories (CosmosDB's 2 MB at about 2 800).
At that point `WritePolicy` fails with a DynamoDB `ValidationException`; `ensurePolicy` wraps it as
`rewrite policy …`/`create policy …`, `EnsurePrincipals` returns it, `RoutesBuilder` returns it, and
`cmd/lakefs/cmd/run.go:306` turns it into `logger.WithError(err).Fatal("extra routes")` — the **server
does not start**, on a supported KV backend, purely because of the repository count. Independently of the
size limit, every authorization decision for any base-group member evaluates the whole deny
(`contrib/auth/acl/service.go:987` → `auth.CheckPermissions` over all effective policies), i.e. 5N wildcard matches
per request, and every refresh that changes the set rewrites and `PurgePolicies`s a multi-hundred-KB
value.

**Evidence** (probe, `pkg/mds/bootstrap/probe_r2s_test.go`, `IndexerPolicy`/`MetadataRepoProtectPolicy` +
`proto.Marshal(model.ProtoFromPolicy(p))`):
```
PROBE scale n=100 policy=mds-indexer statements=602 proto_bytes=49116
PROBE scale n=100 policy=mds-metadata-repo-protect statements=500 proto_bytes=72231
PROBE scale n=400 policy=mds-indexer statements=2402 proto_bytes=195816
PROBE scale n=400 policy=mds-metadata-repo-protect statements=2000 proto_bytes=288831
PROBE scale n=500 policy=mds-indexer statements=3002 proto_bytes=244716
PROBE scale n=500 policy=mds-metadata-repo-protect statements=2500 proto_bytes=361031
PROBE scale n=1000 policy=mds-indexer statements=6002 proto_bytes=489216 over_dynamodb_400KiB=true
PROBE scale n=1000 policy=mds-metadata-repo-protect statements=5000 proto_bytes=722031 over_dynamodb_400KiB=true
```
Nothing in METADATA_SEARCH_DESIGN.md §8 or `docs/metadata-search.md` names a repository-count limit.

**Suggested fix**: bound the item size, e.g. split the companion set into chunks of a few hundred and
write `mds-metadata-repo-protect-<k>` policies (the refresher already repairs by exact statement
comparison, so chunks are as idempotent as one policy), or deny the same set through one policy per
companion repository. Test: with 1000 repositories every policy value stays under a documented cap and
`RoutesBuilder` succeeds (fails today on DynamoDB and is silent about the limit everywhere).

### R2S-3 - MINOR: the deny names repository ARNs but not the companion repositories' storage namespaces, so a guard-group member can create another repository on that namespace and sweep the index

**Where**: `pkg/mds/bootstrap/bootstrap.go:233-241` and `:338-365` (the deny's resources are
`RepoArn(companion)`, `RepoArn(companion)+"/*"` and the three catalog ARNs; no
`fs:AttachStorageNamespace` statement), `pkg/api/controller.go:2290-2305` (`CreateRepository` authorizes
`fs:CreateRepository` on `repository/<name>` and `fs:AttachStorageNamespace` on the *storage namespace*),
`pkg/catalog/catalog.go:533-557` (no uniqueness or overlap check on the namespace).

**What happens**: `POST /api/v1/repositories {"name":"evil","storage_namespace":"s3://bucket/analytics-metadata"}`
by a member of `Admins`/`Developers` needs two permissions the deny deliberately does not cover — neither
ARN is a companion repository ARN — and lakeFS accepts a second repository on an existing namespace. From
then on `evil`'s uncommitted GC scans `s3://bucket/analytics-metadata`, exactly the sweep METADATA_SEARCH_DESIGN.md
§3.3 says "would delete `R-metadata`'s files" (that is why the *indexer* refuses an overlapping companion
namespace on every tick, `pkg/mds/indexer/repo.go`). The deny's promise — "no principal in a base group
may write, delete, create … inside a companion repository" — is therefore reachable through a
non-companion ARN.

**Evidence**: probe (`pkg/mds/bootstrap/probe_r2s_test.go`), real ACL service, `EnsurePrincipals` with
`dataRepositories=[analytics]` and the standard guard list:
```
PROBE namespace bypass: guard-group admin fs:CreateRepository on arn:lakefs:fs:::repository/evil -> allowed=true
PROBE namespace bypass: guard-group admin fs:AttachStorageNamespace on arn:lakefs:fs:::namespace/s3://bucket/analytics-metadata -> allowed=true
PROBE namespace bypass: guard-group admin fs:CreateRepository on arn:lakefs:fs:::repository/analytics-metadata -> allowed=false
PROBE namespace bypass: create evil on the companion's namespace mem://shared: err=<nil>
```
(The last line is the real `catalog.Catalog` of the bootstrap fixture: `analytics-metadata` and then
`evil` on `mem://shared` both succeed.)

**Suggested fix**: for every companion repository the catalog lists, add the exact
`fs:AttachStorageNamespace` deny on `permissions.StorageNamespace(repo.StorageNamespace)`, and keep
`CreateRepository`'s `fs:Create*` as it is (`knownDataRepositories` already lists the repositories; it
needs to also read each companion's namespace, skip ones the caller cannot see, and the pattern
`namespace/*-metadata` stays for not-yet-existing companions). Test: a guard-group member's
`CreateRepository`+`AttachStorageNamespace` node on a companion repository's namespace is denied (it is
allowed today) and on an ordinary namespace stays allowed.

### R2S-4 - MINOR: a data repository whose name ends in `-metadata` becomes unwritable and undeletable for every base group, while `mds-indexer` gets `fs:*` on it — and the indexer refuses to index it

**Where**: `pkg/mds/bootstrap/bootstrap.go:158-181` (`companionRepositories` keeps a name that
`icebergcatalog.IsMetadataRepository` accepts instead of mapping it to `<name>-metadata`),
`pkg/icebergcatalog/resolver.go:285-287` (`IsMetadataRepository`), `pkg/mds/config.go:407-411`
(`validateRepositories`: "a data repository name must not end in `-metadata`").

**What happens**: a pre-existing ordinary repository named `team-metadata` (a plausible name; nothing
stopped it before Metadata Search existed) is in the catalog, so `knownDataRepositories` returns it and
`companionRepositories` keeps it as a *companion*. Both policies then name it: `mds-indexer` gets `fs:*`
and `catalog:*` on it, and `mds-metadata-repo-protect` denies `fs:Write*/Delete*/Create*/Revert*/Update*/Import*`
and the catalog writes on it to every base group. The indexer, however, will never index it: as a data
repository its name is rejected by `pkg/mds/config.go` (and the indexer refuses a `*-metadata` repository
without its init marker as a foreign repository, `pkg/mds/indexer/repo.go`). Net effect: the indexer —
whose whole point is "never writes a data repository" (`bootstrap.go:191-194`) — has full write and delete
on an ordinary data repository, and `Admins`/`Developers` cannot write, commit to or delete it, with no
signal beyond their 403 and no way out except editing the catalog. The fix's own test pins the
behaviour (`bootstrap_test.go`, `TestKnownDataRepositories`: "legacy-metadata is a companion repository by
§3.3"), and §3.3 says such a repository "cannot be indexed" — it does not say the policies should treat it
as a companion.

**Evidence**: probe (`pkg/mds/bootstrap/probe_r2s_test.go`), real ACL service, `dataRepositories=["team-metadata"]`:
```
PROBE ordinary *-metadata repo "indexer write": fs:WriteObject mds-indexer -> allowed=true companions=[team-metadata]
PROBE ordinary *-metadata repo "indexer delete repo": fs:DeleteRepository mds-indexer -> allowed=true companions=[team-metadata]
PROBE ordinary *-metadata repo "operator write": fs:WriteObject operator -> allowed=false
PROBE ordinary *-metadata repo "operator commit": fs:CreateCommit operator -> allowed=false
PROBE ordinary *-metadata repo "operator delete repo": fs:DeleteRepository operator -> allowed=false
PROBE ordinary *-metadata repo "operator read": fs:ReadObject operator -> allowed=true
```

**Suggested fix**: treat a name that ends in the suffix as a companion only when it carries the indexer's
init marker (`lakefs.mds.init`, `pkg/mds/indexer/repo.go`), i.e. when it really is one; otherwise leave it
out of both policies (a repository the indexer cannot index has no companion to grant or protect). Test:
an unborn-ordinary repository named `x-metadata` stays writable for a guard group and is not granted to
`mds-indexer` (both are wrong today).

### R2S-5 - MINOR: the vended-credentials write guard checks the requested address only, while the write-sign fix it is credited with also checks the resolved target

**Where**: `pkg/icebergcatalog/creds.go:452-458` (`TableCredentials` calls `s.writableNamespace(addr)` on
the *requested* address), `:515-517` (`writableNamespace` = `RequireWritableNamespace(addr) == nil`),
`:408-441` (`Vend` resolves through `s.tableScope` and never re-checks the target), `:820-853`
(`newTableScopeResolver` takes root/location from `resolved.Target`); compare
`pkg/icebergcatalog/sign.go:522-535` (`writableTableLocation` refuses
`requireWritable(resolved.Target)`).

**What happens**: the MDS-SEC-1 fix states "the vended-credentials path already refused that write
(`creds.go` `writableNamespace`)". It refuses it only when the *caller's* address is already reserved.
A resolver that redirects an ordinary address into a reserved namespace — exactly the case
`TestRemoteSigningReservedNamespace` builds with `signRedirectResolver` — passes the requested-address
check, and `Vend` then takes the root and location of the redirected table, so the caller receives the
*write* session (`s3:PutObject`, `s3:DeleteObject`) for the reserved namespace's files. Only in-tree code
can install a resolver (`WithTableResolver`), so this is a defence-in-depth gap in the guard the fix
relies on, not an exploit of the shipped resolver.

**Evidence** (probe, `pkg/icebergcatalog/probe_r2s_test.go`, real `NewCredentialService` +
`signRedirectResolver` from `sign_test.go`, `credsTestVending()`, recording STS fake):
```
PROBE creds redirect: protected namespace of the redirect target "lakehouse-metadata@main.system" is reserved=true; requested address namespace is reserved=false
PROBE creds redirect: TableCredentials(lakehouse@main.inventory.books) err=<nil> credentials=1 elapsed=4ms
PROBE creds redirect: session prefix="s3://iceberg-sign-mds-4-ns/_iceberg/66666666-6666-6666-6666-666666666666" actions=[s3:DeleteObject s3:PutObject]
```
The same shape through the signing endpoint is 403 (`TestRemoteSigningReservedNamespace`, verified red
without the fix above).

**Suggested fix**: in `Vend` (or in `newTableScopeResolver`, which already has `resolved.Target`), refuse
`store.RequireWritableNamespace(resolved.Target)` for `OpWriteTableData` and fall back to the read
session, exactly as `sign.go`'s `writableTableLocation` does. Test: the `signRedirectResolver` +
write-credentials case returns a read session (or no write actions) — it returns `s3:PutObject` today.

### R2S-6 - MINOR: the documented "extra statement" for an explicit non-`-metadata` storage namespace is deleted again by the policy repair at the next startup

**Where**: `pkg/mds/bootstrap/bootstrap.go:713-733` (`ensurePolicy`: any statement difference triggers
`WritePolicy(…, true)` and the warning "rewrote a metadata search policy whose statements did not match"),
`:780-792` (`statementsEqual` compares the whole list), `:280-333` (`IndexerPolicy` has the single
`fs:AttachStorageNamespace` pattern `namespace/*-metadata`); docs `docs/metadata-search.md:825-828` and
METADATA_SEARCH_DESIGN.md §8 ("An explicit `metadata_search.repositories.<R>.storage_namespace` that does
not itself end in `-metadata` … needs an extra statement").

**What happens**: an operator follows the documented workaround for a bucket-root/derived-elsewhere
namespace and adds the extra `fs:AttachStorageNamespace` (and `fs:CreateRepository`) statement to the
`mds-indexer` policy. On the next `lakefs-sso` start, `EnsurePrincipals` → `refreshPolicies` →
`ensurePolicy` sees statements that differ from the built-ins and replaces the policy with the built-ins,
so the grant is gone (one warning, no error); the indexer's next `CreateRepository` of `R-metadata` is 403,
the indexer cannot start, and the same happens after every restart. Only a *second* policy attached to
`MetadataSearchIndexers` survives, and neither the design nor the guide says so.

**Evidence**: probe (`pkg/mds/bootstrap/probe_r2s_test.go`, real ACL service, extra statement appended with
`WritePolicy(…, true)`):
```
PROBE operator extra statement: before_refresh=1 after_refresh=0 statements_before=9 statements_after=8
```
`TestEnsurePrincipalsRepairsDriftedPolicy` pins the repair itself; nothing says which drift is a repair and
which is an operator grant.

**Suggested fix**: decide and document: either the extra grant must live in a separate policy attached to
the group (state it in §8 and the guide, and add a test that such a policy survives `RoutesBuilder`), or
`ensurePolicy` keeps operator-added statements (replace only the companion-repository statements it
generates). Test: add the documented statement, run `RoutesBuilder` again, assert it is still in force (it
is gone today).

### R2S-7 - NIT: the deny leaves `branches:SetBranchProtectionRules` (and `retention:*`) open, so a base-group member can stop the indexer permanently — the §3.3 promise that "user writes are blocked by a deny policy" does not cover it

**Where**: `pkg/mds/bootstrap/bootstrap.go:233-241` (deny actions are `fs:*`/`catalog:*` wildcards only),
`pkg/api/controller.go:2633-2656` (`SetBranchProtectionRules` authorizes `branches:SetBranchProtectionRules`
on `RepoArn(repo)` and blocks `STAGING_WRITE` and `COMMIT` on every matching branch),
`pkg/graveler/branch/protection_manager.go:47-70`, `pkg/auth/setup/setup.go:170-186`
(`RepoManagementFullAccess` = `ci:*`, `retention:*`, `branches:*`, `pr:*`, attached to `Admins` in the RBAC
template).

**What happens**: a member of `Admins`/`SuperUsers` can `POST /repositories/analytics-metadata/branch_protection`
with pattern `*`; the companion branch then refuses every commit, so the indexer fails every tick and
`/_health` stays 503 — the failure mode `f884a0627` added `ErrReadOnlyMetadataRepository` for. The deny's
own comment ("no principal in a base group may write … inside a companion repository") and §3.3's "user
writes are blocked by a deny policy instead" both claim this class is closed; `branches:SetBranchProtectionRules`
is a write of repository state that determines whether the indexer may write. It is loud, not silent, which
is why this is a NIT and not higher. `retention:SetGarbageCollectionRules` /
`PrepareGarbageCollectionCommits` are also allowed, but cannot delete the live index without deleting the
branch first (`fs:DeleteBranch` is denied), so I did **not** count them as a finding.

**Evidence**: probe (`pkg/mds/bootstrap/probe_r2s_test.go`), a guard-group principal with the RBAC admin
grants:
```
PROBE deny gap: guard-group admin fs:WriteObject on arn:lakefs:fs:::repository/analytics-metadata/object/forged.parquet -> allowed=false
PROBE deny gap: guard-group admin fs:DeleteRepository on arn:lakefs:fs:::repository/analytics-metadata -> allowed=false
PROBE deny gap: guard-group admin branches:SetBranchProtectionRules on arn:lakefs:fs:::repository/analytics-metadata -> allowed=true
PROBE deny gap: guard-group admin retention:SetGarbageCollectionRules on arn:lakefs:fs:::repository/analytics-metadata -> allowed=true
PROBE deny gap: guard-group admin retention:PrepareGarbageCollectionCommits on arn:lakefs:fs:::repository/analytics-metadata -> allowed=true
PROBE deny gap: guard-group admin pr:WritePullRequest on arn:lakefs:fs:::repository/analytics-metadata -> allowed=true
```
(`pr:WritePullRequest` alone cannot merge — `UpdatePullRequest`'s merge node adds `fs:CreateCommit` on the
destination branch, `controller.go:6179-6185`, which the deny covers.)

**Suggested fix**: add `branches:SetBranchProtectionRules` (and, if the owner agrees, the two
`retention:Prepare*` actions) to `metadataRepoProtectCatalogDeniedActions`, and say in §8 why
`retention:SetGarbageCollectionRules` itself stays allowed. Test: a guard-group member's
`SetBranchProtectionRules` on a companion repository is denied (allowed today).

### R2S-8 - NIT: nothing tests that `RoutesBuilder` starts the refresher, so the production half of the one-minute guarantee can be deleted unnoticed

**Where**: `pkg/mds/bootstrap/bootstrap.go:426` (`startPolicyRefresher(ctx, deps, policyRefreshInterval)`);
`pkg/mds/bootstrap/bootstrap_test.go:984-1020` (`TestPolicyRefresher` calls `startPolicyRefresher` directly).

**What happens**: every refresher test drives the goroutine itself; no test goes through
`RoutesBuilder` → `startPolicyRefresher`, so removing the call (or replacing `ctx` with
`context.Background()`, which would also outlive the server) leaves the package green — verified below.
The same tests never cover the set *shrinking* (a repository deleted: `knownDataRepositories` is only ever
observed growing) or a refresh that fails (`pkg/mds/bootstrap/bootstrap.go:513-528`, the "logged and
retried" branch), so the two behaviours the design promises are untested.

**Evidence**: delete line 426, `go test ./pkg/mds/bootstrap/ -run
'TestRoutesBuilder|TestKnownDataRepositories|TestPolicyRefresher|TestEnsurePrincipals'` → `ok
github.com/treeverse/lakefs/pkg/mds/bootstrap 1.310s`.

**Suggested test**: `RoutesBuilder` with a small `policyRefreshInterval` (needs an injection point, e.g. a
package variable the test overrides) creates a repository in the catalog and requires both policies to
name it, and requires the refresher to stop when the context is cancelled; plus a delete-then-refresh case
asserting the statements shrink, and a failing-`WritePolicy` case asserting the retry.

### R2S-9 - NIT: on an external auth service the accepted one-minute window is really one minute plus the auth cache TTL

**Where**: design §8 ("that window … is bounded by the refresher at one minute") vs
`pkg/auth/service.go:612-655` (`APIAuthService.WritePolicy` never calls `PurgePolicies`, unlike
`contrib/auth/acl/service.go:715-740`), `pkg/auth/service.go:1024-1050` (`Authorize` uses
`ListEffectivePolicies`, cached per user), `pkg/config/defaults.go:66-69` (`auth.cache.enabled` true,
`ttl` 20 s, `jitter` 3 s).

**What happens**: with `lakefs-sso` in external-auth mode (`auth.ui_config.rbac: external` →
`auth.NewAuthService` → `APIAuthService`), the refresher's rewrite is invisible to authorization until the
per-user effective-policy cache entry expires, so a newly created companion repository's deny (and the
indexer's grant) can take up to ~83 s to apply, not 60 s. The ACL service (the `lakefs-sso` SSO mode) does
purge, so this is the non-SSO/external deployment only.

**Evidence**: `git grep -n 'PurgePolicies()' -- pkg/ contrib/` lists the interface and the two
implementations (`pkg/auth/cache.go:39,105,134`) and five call sites, **all** in
`contrib/auth/acl/service.go` (564, 730, 799, 906, 929); `APIAuthService` has none, and
`pkg/auth/service.go:612-655` ends at `validateResponse`. `auth.cache.ttl` default 20 s plus 3 s jitter
(`pkg/config/defaults.go:66-69`).

**Suggested fix**: after a rewrite, call `deps.AuthService.Cache().PurgePolicies()` when the service
exposes a non-nil cache (guard the nil `Cache()` of a stub service with the `policyBackendAvailable`
style check), and state the residual cache window in §8. Test: with a cache-enabled service, a statement
rewritten by `refreshPolicies` is effective on the next `Authorize` (it is not today).

## Checked, no finding

**The exact-ARN rewrite (`5b997972e`) itself.** `catalogResources` spells namespace/table/view ARNs
(`bootstrap.go:183-188`), which are exactly the three resource shapes `pkg/icebergcatalog/authz.go:594-599`
authorizes, plus `permissions.RepoArn` for `fs:ReadRepository`; a probe of `wildcard.Match` shows the
shapes are *not* over-matching: `table/comp/*/*` does not match `table/comp2/…`, `repository/comp/*` does
not match `repository/comp2/…` nor the repository ARN itself (which has its own statement), and the
repo-level listing ARN `namespace/comp/` (namespace `""`) *is* matched by the trailing star. The `*`
matching is segment-agnostic on purpose and only the trailing part of a statement that already contains
the literal companion name, so the review's MDS-SEC-2 property (a data-repository key/branch/namespace
ending in `-metadata` being reached) is gone — confirmed both by the rebuilt pre-fix policy (Verified
table) and by `TestMDSPoliciesDoNotReachDataRepositories` passing now. The `MetadataNamespacePattern`
stays a pattern (`bootstrap.go:128-132`) and `fs:AttachStorageNamespace` is authorized only by
`CreateRepository` (`controller.go:2297-2305`), so its over-reach cannot grant object or branch access;
the indexer's *own* namespace-overlap check (`pkg/mds/indexer/repo.go`, design §3.3 `7ec4ebec7`) is what
keeps a pattern-chosen namespace from destroying a companion repository.

**The two §8 policies against real lakeFS ARNs.** `fs:CreateCommit`/`fs:WriteObject` on
`BranchArn`/`ObjectArn`, `catalog:*` on `Catalog*Arn`, and `fs:UpdateRepository` from
`controller.go:2573-2612` are all decided as the design says; `TestEnsurePrincipalsAuthorization`,
`TestMDSPoliciesDoNotReachDataRepositories` and `TestMetadataRepoProtectCoversImportActions` run through
the real `contrib/auth/acl` service (not a mock), so deny-beats-allow is a property of the service, not of
the test. The import decision (`9cf0869c8`): `ImportStart`'s node is `fs:WriteObject` +
`fs:CreateCommit` on the destination branch, `fs:ImportFromStorage` on the *source* namespace and
`fs:WriteObject` on the destination object (`controller.go:3350-3378`); `ImportCancel` is
`fs:ImportCancel` on `BranchArn(repo, branch)` (`controller.go:3489-3497`). `fs:Import*` therefore closes
cancel and was already covered for import-into-a-companion; no namespace statement is needed, and the
review's decision text matches the code.

**The refresher's lifecycle, context and concurrency.** `RoutesBuilder` runs from the builder loop in
`run.go:303-308` with `ctx` = `signal.NotifyContext` (`run.go:80`), i.e. the server's lifetime; the
goroutine closes `done` on `ctx.Done()` and on the degraded path (`bootstrap.go:501-545`), the ticker is
stopped, a failed list/refresh is logged and retried (`:513-528`), and `inForce == false` stops it
instead of warning every minute. `time.Ticker` drops ticks, so a slow catalog cannot pile up goroutines;
`knownDataRepositories` has no timeout of its own but is bound to the server context, and the only
production caller starts one refresher per process. Two `lakefs-sso` replicas derive the same sorted,
deduplicated statement list (`companionRepositories` sorts) and `ensurePolicy` writes only on a
difference, so they converge; `WritePolicy` has no compare-and-swap, so in the window where one replica's
list already has a new repository and the other's does not, the two can rewrite each other's policy and
purge the auth cache — transient, converging, and not worth a finding.

**Refresher pagination and the repository list.** `knownDataRepositories` pages with
`ListRepositories(ListRepositoriesLimitMax, "", "", after)`; the iterator is keyed by repository ID,
`SeekGE(after)` + `if record.RepositoryID == afterRepositoryID { continue }` is exclusive, and the break
conditions (`!hasMore || len(repositories)==0`) cannot loop (`pkg/catalog/catalog.go:679-742`). A
configured name that is not a valid repository ID is dropped with a warning, and `viper.GetStringMap`
lower-cases keys — harmless, because `validator.ReValidRepositoryID` is `^[a-z0-9][a-z0-9-]{2,62}$`
(`pkg/validator/validate.go:12`), the same rule `graveler.ValidateRepositoryID` enforces on creation.
Repositories longer than 54 characters are silently dropped by `companionRepositories` (their companion
cannot exist), which is correct.

**`RoutesBuilder` deps, degradation and startup.** A nil `Catalog` is `ErrMissingDeps` (the policies
would name nothing); `BasicAuthService.GetPolicy` returns `ErrNotImplemented`
(`pkg/auth/basic_service.go:428-430`), which `policyBackendAvailable` treats as the one degraded startup,
before `CreateUser` — so the administrator is not renamed (the existing test asserts exactly that); a
genuine error still propagates and stops the server. The idempotency contract holds: `ErrAlreadyExists`
is tolerated on user/group/membership/policy create and on attach, `EnsurePrincipals`' order (policies
before attachments) means an attachment never names a missing policy, and `statementsEqual`
(`bootstrap.go:780-799`) treats nil and empty conditions as equal, so a round-tripped policy is not
rewritten on every restart (`TestEnsurePrincipalsIdempotent` pins `CreatedAt`).

**`metadata_search` requires `iceberg.enabled` (`4aaf60cb3`).** `Config.Validate`
(`config.go:363-375`) is reached by every `LoadConfig` (`config.go:200-213`), which both `RoutesBuilder`s
and `pkg/icebergcatalog/bootstrap.go:85` call, so the invalid combination is fatal before any principal
or deny is installed; the only `MetadataSearchEnabled()` call sites are behind that validation
(`http.go:157,222`, `bootstrap.go:396`). The message names both keys and the reason.

**The write-sign refusal (`d2811fdcc`).** Every method `signingOperation` classifies as
`OpWriteTableData` (`PUT`, `POST`, `DELETE`, `PATCH`, `OPTIONS`, `sign.go:464-475`) goes through the
requested-address check (`:387`), and the resolved target is checked again before the redirected table is
read (`:522-535`), so the only address shape that resolves to `R-metadata` (`system.object_metadata`,
`resolver.go:455-463`) is refused twice; reads (`GET`, `HEAD`) keep signing and the location scoping is
unchanged. I tried to dodge it by naming a table of `R` whose location could sit in the companion's
prefix: `TableLocation`/`normalizeTableLocation` confine a table to `<R namespace>/_lakefs_iceberg`
(`location.go:101-151`), which is not the companion's `_iceberg/<uuid>` prefix, so a write sign for a
table of `R` cannot name a companion file. The remaining asymmetry is R2S-5.

**The companion-table proof's placement (`4aaf60cb3`).** `Store.Resolve` (`resolver.go:483-510`) loads the
target once before serving, so HEAD, metrics, vended credentials and remote signing are covered by
construction (`GetTable` does not recurse into `Resolve`, `store.go:315-348`); a resolver that omits
`Authz` gets the requested address (`:496-500`), and an incomplete target is `ErrInvalidResolution`. The
load happens before `ResolveAuthorized`'s second authorization (`:530-545`), which is a table-existence
oracle for a cross-repository `Authz` only after the caller was authorized on the requested address —
acceptable, and not reachable with the built-in resolver. The extra load is documented in design §5.1.

**`cmd/lakefs-sso` wiring.** Both builders run in registration order, the deny gets the same
`guardGroups()` as `iceberg-reserved-prefix-protect` (`main.go:76-89` and `:114-121`), the mds builder is a
no-op while `iceberg.metadata_search.enabled` is false, and `MetadataSearchRefMapper = refmap.New` is
assigned before `Execute` so the catalog sees it at mount time. `guardGroups()` loading
`datasets.LoadConfig()` twice (once per builder) is harmless.

**What I could not run**: the esti suite (`make esti` needs docker and SeaweedFS) and the DynamoDB backend
(no local DynamoDB), so R2S-2's 400 KiB failure is derived from the stored value size plus DynamoDB's
documented per-item limit, not from a live write; everything else in this fragment was run in-process.
