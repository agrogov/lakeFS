## Server security: the reserved `system` namespace, the `/mds/iceberg/api` mount, the virtual table, and the mds principals

Scope: design 5.1-5.5 and 8, as implemented in `pkg/icebergcatalog` (`config.go`, `creds.go`, `http.go`,
`namespaces.go`, `resolver.go`, `store.go`, `tables.go`, the write paths in `commit_http.go`,
`register.go`, `views_http.go`, `sign.go`), `pkg/api/services.go`, `pkg/config/oss_config.go`, the
`cmd/lakefs-sso/main.go` wiring, and `pkg/mds/client` + `pkg/mds/bootstrap` with their tests. Compared
against `METADATA_SEARCH_DESIGN.md` §5.1-5.5, §8, §9 and against the real lakeFS behaviour in
`pkg/graveler`, `pkg/catalog`, `pkg/api` and `pkg/auth` (ACL authorizer, `pkg/auth/wildcard`).

### Verified

| command | result |
|---|---|
| `export PATH=$HOME/sdk/go1.25.8/bin:$PATH; GOTOOLCHAIN=auto go build ./...` | ok (no output) |
| `GOTOOLCHAIN=auto go vet ./pkg/icebergcatalog/... ./pkg/mds/client/... ./pkg/mds/bootstrap/... ./pkg/api/ ./pkg/config/ ./cmd/lakefs-sso/...` | ok (no output) |
| `GOTOOLCHAIN=auto go test -count=1 -race -timeout 10m ./pkg/icebergcatalog/ ./pkg/mds/client/ ./pkg/mds/bootstrap/ ./pkg/api/ ./pkg/config/` | all `ok` (icebergcatalog 28.0s, mds/client 1.5s, mds/bootstrap 3.8s, api 42.3s, config 2.3s) |
| `GOTOOLCHAIN=auto go test -count=1 -race -timeout 10m ./pkg/mds/...` | all `ok` (mds, bootstrap, client, indexer, refmap, table) |
| `GOTOOLCHAIN=auto go tool golangci-lint run ./pkg/icebergcatalog/... ./pkg/mds/client/... ./pkg/mds/bootstrap/... ./pkg/api/ ./pkg/config/ ./cmd/lakefs-sso/...` | `0 issues.` |
| probe `TestProbeSignWriteOnVirtualTable` (temporary, deleted before commit) | `virtual sign status=200`, presigner called with `mode:1` (write) on `s3://probe-bucket/meta/_iceberg/<uuid>/data/...`; `RequireWritableNamespace(virtual)` returns the reserved-namespace error in the same test |
| probe `TestProbeVendedCredsContrast` (temporary, deleted before commit) | `writableNamespace(virtual)=false` (the credentials path refuses the write session for the same address) |
| probe `TestProbeMetadataWildcardReach` (temporary, deleted before commit) | `allowed=true` for indexer `fs:DeleteObject` on `repository/analytics/object/-metadata`, indexer `fs:WriteObject` on `repository/analytics/object/foo-metadata/data.yml`, indexer `catalog:DeleteTable` on `table/analytics/foo-metadata/orders`; `allowed=false` for an operator in a guard group writing the same two data-repository objects |

### MDS-SEC-1 - MAJOR: remote signing hands out a **write** signature under `R-metadata`'s table location for the virtual table

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

### Checked, no finding

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
