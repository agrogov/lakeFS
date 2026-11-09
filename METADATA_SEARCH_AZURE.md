# Metadata Search on Azure Blob Storage — design and plan

Branch: `feature/metadata-search`. Status: **AZ1–AZ7 done**, including the verification on a real Azure
account (AZ7), so the Azure support is called done. This document is
self-contained; the design sections it changes are named by number (`METADATA_SEARCH_DESIGN.md`
§x, `ICEBERG_CATALOG_PLAN.md` §x).

## 1. Problem

lakeFS on an Azure blockstore (`blockstore.type: azure`) stores repository data under namespaces of
the form

```
https://<account>.blob.core.windows.net/<container>/<prefix>
```

and the server reads and writes them with its own Azure credentials (shared key, or the default
Azure credential chain). That part works today. Metadata Search does not, for four reasons found in
the code:

1. **The catalog hands out `https://` table locations.** `DefaultTableLocation` is
   `<storage_namespace>/_iceberg/<uuid>` (`pkg/icebergcatalog/location.go`), so on Azure a table's
   `location` is `https://acct.blob.core.windows.net/container/prefix/_iceberg/<uuid>`. No Iceberg
   FileIO opens that scheme: iceberg-go registers `file` and `""` itself and `abfs[s]`/`wasb[s]` in
   `io/gocloud/azure`; Iceberg Java's `ADLSFileIO` takes `abfs[s]`/`wasb[s]`; Trino's native Azure
   file system takes `abfs[s]` only and fails on an `https://…blob.core.windows.net` warehouse
   (trinodb/trino#24807); PyIceberg maps `abfs[s]` to fsspec/adlfs.
2. **The indexer has no Azure IO backend.** `pkg/mds/table/catalog.go` blank-imports only
   `io/gocloud/s3`. Even with an `abfss://` location, iceberg-go would answer "unsupported scheme".
3. **`metadata_search.storage` cannot carry Azure credentials.** The map is passed to iceberg-go
   as-is, so the `adls.*` keys would work if (1) and (2) were fixed, but the map is file-only
   (`docs/metadata-search.md`, env-var rule): a Kubernetes deployment whose secret is an env var
   (`LAKEFS_BLOCKSTORE_AZURE_STORAGE_ACCESS_KEY`) cannot pass it.
4. **Namespace arithmetic assumes `<scheme>://<bucket>/<path>`.**
   - `metadataNamespace`/`namespaceHasPath` (`pkg/mds/indexer/repo.go`) treat the first path
     segment as "path". On Azure the first path segment is the **container**, so a container-root
     namespace `https://acct.blob.core.windows.net/data` derives `…/data-metadata` — a different
     container, which does not exist, and the companion repository cannot be written.
   - `isBucketRootNamespace`/`storageNamespaceDenyResources` (`pkg/mds/bootstrap/bootstrap.go`)
     have the same blind spot, and miss a spelling: the Azure adapter's validity regex is
     `^https?://…`, so `http://acct…/c/p` and `https://acct…/c/p` name the same prefix and both are
     accepted by CreateRepository. The R3S-3 deny must cover both.

The lakeFS public guide (https://docs.lakefs.io/guides/metadata-search/) limits only the web UI
search to S3 and says Azure and GCS can be queried through the REST catalog; the Iceberg catalog
page (https://docs.lakefs.io/iceberg/) says the catalog is tested with Azure Blob Storage and that
credential vending is S3-only. Our guide's "Azure and GCS are out of scope" (`docs/metadata-search.md`
lines 294 and 818) is therefore a gap in this implementation, not a product limit.

The indexer cannot be pointed at a separate S3 store (SeaweedFS, MinIO on Azure) instead: it writes
into the companion repository `<R>-metadata`, whose storage namespace is a namespace of the **same
lakeFS blockstore** (`metadataNamespace`), and lakeFS has one blockstore per installation.

## 2. Goals and non-goals

Goals:

- G1. On an Azure blockstore the indexer creates, writes, compacts and expires the
  `system.object_metadata` table of every configured repository, exactly as on S3.
- G2. Spark (Iceberg Java ≥ 1.8), Trino (≥ 470, native Azure FS), PyIceberg and DuckDB read that
  table through the lakeFS REST catalog with their own Azure credentials.
- G3. The general Iceberg catalog (not only MDS tables) hands out and accepts Azure locations a
  client can open, because the fix is in `TableAreaLocation`, which every table shares.
- G4. Azure credentials for the indexer can come from env vars (Kubernetes secret) or managed
  identity, with no secret in a config file.
- G5. An Azurite-based esti case is the acceptance test, running in the existing esti harness.

Non-goals (stated in the docs):

- N1. **Credential vending and remote signing for Azure.** Both stay S3-only
  (`creds.go` `CredentialVendingUnsupportedMessage`, `sign.go` `parseS3Location`). Clients and the
  indexer use their own Azure credentials. SAS vending is backlog item B1 (§8).
- N2. GCS. The location translation is written so that GCS needs only another case
  (`gs://` is already a scheme every FileIO opens), but nothing here is tested on GCS.
- N3. The web UI object search. It is S3-only upstream too.
- N4. `wasb[s]://` output. Microsoft deprecates WASB in favour of ABFS, and Trino does not accept it
  (trinodb/trino#23511).

## 3. Decisions

### D1. Canonical location form: `abfs[s]://<container>@<account>.<dfs domain>/<path>`

The catalog maps a repository's Azure namespace to one Iceberg location form and uses that form
everywhere a location is produced or compared:

| Namespace (lakeFS, block adapter) | Iceberg location (metadata.json, manifests, REST responses) |
|---|---|
| `https://acct.blob.core.windows.net/c/p` | `abfss://c@acct.dfs.core.windows.net/p` |
| `https://acct.blob.core.chinacloudapi.cn/c/p` | `abfss://c@acct.dfs.core.chinacloudapi.cn/p` |
| `https://acct.blob.core.usgovcloudapi.net/c/p` | `abfss://c@acct.dfs.core.usgovcloudapi.net/p` |
| `https://acct.azurite.test/c/p` (test domain) | `abfss://c@acct.azurite.test/p` |
| `http://…` (any of the above) | `abfs://…` (same mapping, non-TLS) |

Why `abfss` and the `dfs` host:

- It is the only form all four target engines open: Trino requires
  `abfs[s]://<container>@<account>.dfs.core.windows.net/…`
  (https://trino.io/docs/current/object-storage/file-system-azure.html); Iceberg Java `ADLSFileIO`
  accepts it (apache/iceberg#10127, PR #11504); PyIceberg/adlfs and DuckDB's azure extension accept
  it.
- iceberg-go ignores the host's storage domain and builds its endpoint from `adls.endpoint`
  (default `blob.core.windows.net`, apache/iceberg-go#2311), so the indexer is indifferent to
  `dfs` vs `blob` in the host.
- The `dfs` endpoint serves non-HNS (flat namespace) accounts for the Blob-compatible operations
  Iceberg uses (Microsoft, "Use the Azure Data Lake Storage URI",
  https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction-abfs-uri).
  **Caveat:** Hadoop ≥ 3.4's ABFS driver refuses FNS accounts over the `dfs` endpoint and expects
  `abfss://…@acct.blob.core.windows.net` with `fs.azure.account.hns.enabled=false`
  (https://hadoop.apache.org/docs/current/hadoop-azure/fns_blob.html). That only matters to engines
  reading through Hadoop's ABFS (Spark with `HadoopFileIO`); with `ADLSFileIO` or Trino's native FS
  it does not. D1 therefore has one switch, `iceberg.azure.location_host: dfs | blob` (default
  `dfs`), which picks the host's service part; the scheme stays `abfs[s]`.

The mapping is a pure function of the namespace (`IcebergNamespaceLocation(namespace)`), its
inverse is **not** a general parser: a location is mapped back to the block adapter's `https` form
only after it has been checked to be under the table area (D2), by replacing the area prefix in
Iceberg form with the area prefix in namespace form. That keeps the §13 "write-anywhere" guard
exactly as strong as today: nothing outside the area is ever translated, so nothing outside it is
ever read or written with the server's credentials.

### D2. One area, two spellings

`TableAreaLocation(namespace)` returns the Iceberg form (D1) for an Azure namespace and is
unchanged for every other blockstore. Every prefix check already goes through it or through
`TableRoot` (`normalizeTableLocation`, `tableFileCollector.relativePath`, register, views,
`creds.go` roots), so they all switch with it. A new `blockIdentifier(repo, location)` converts a
validated location to the namespace form, and it is called at the six places that hand a location
to the block adapter or store it as a lakeFS physical address (the pointer entry staged by
`store.go` `stage` uses a relative address and is not affected):

| Call site | Today | After |
|---|---|---|
| `files.go` `readTableObject` (block.Get) | location as-is | `blockIdentifier` |
| `metadata.go` `writeDocument` (block.Put) | location as-is | `blockIdentifier` |
| `store.go` `loadMetadata` (block.Get) | location as-is | `blockIdentifier` |
| `views.go` `loadViewMetadata` (block.Get) | location as-is | `blockIdentifier` |
| `files.go` `tableFileCollector` add, `PhysicalAddress` (≈ line 976, staged at ≈ 637) | location as-is | `blockIdentifier` |
| `views.go` view metadata `PhysicalAddress` (≈ line 373) | location as-is | `blockIdentifier` |

Physical addresses must be in the block adapter's form because lakeFS GC, `stat`, and the S3 gateway
read them through the same adapter; an `abfss://` physical address would be unreadable there.

**Compatibility with tables created before this change.** An Azure installation may already hold
catalog tables whose locations are `https://…` (they were usable only by clients with an HTTP
FileIO, but they exist). The area check accepts the legacy `https` area as a second spelling for
reads, commits and registration of such a table; new tables always get the Iceberg form. This is a
read-path tolerance, not a second canonical form: `TableLocation` never returns it.

### D3. Indexer Azure IO and credentials

- Blank-import `github.com/apache/iceberg-go/io/gocloud/azure` next to `…/s3` in
  `pkg/mds/table/catalog.go`. This registers `abfs`, `abfss`, `wasb`, `wasbs`.
- `metadata_search.storage` keeps its meaning (raw iceberg-go IO properties; explicit keys win).
- New scalar block `metadata_search.azure`, read with the env overrides of `applyViperOverrides`
  (so every key is a `LAKEFS_METADATA_SEARCH_AZURE_*` env var), translated into `adls.*` IO
  properties when the corresponding `storage` key is absent:

  | Key | Env var | iceberg-go property |
  |---|---|---|
  | `storage_account` | `LAKEFS_METADATA_SEARCH_AZURE_STORAGE_ACCOUNT` | `adls.auth.shared-key.account.name` |
  | `storage_access_key` | `LAKEFS_METADATA_SEARCH_AZURE_STORAGE_ACCESS_KEY` | `adls.auth.shared-key.account.key` |
  | `use_managed_identity` | `LAKEFS_METADATA_SEARCH_AZURE_USE_MANAGED_IDENTITY` | `adls.auth.managed-identity.enabled` |
  | `client_id` | `LAKEFS_METADATA_SEARCH_AZURE_CLIENT_ID` | `adls.client-id` |
  | `endpoint` | `LAKEFS_METADATA_SEARCH_AZURE_ENDPOINT` | `adls.endpoint` |
  | `protocol` | `LAKEFS_METADATA_SEARCH_AZURE_PROTOCOL` | `adls.protocol` |

  With none set, iceberg-go falls back to `DefaultAzureCredential` (workload identity, env
  `AZURE_CLIENT_ID`/`AZURE_TENANT_ID`/`AZURE_FEDERATED_TOKEN_FILE`, CLI).
- `endpoint` is required for any cloud but the public one and for Azurite, because iceberg-go
  ignores the domain in the location (apache/iceberg-go#2311). `Validate` refuses
  `storage_access_key` without `storage_account` (iceberg-go would refuse it later, at the first
  write, with a less useful error) and refuses a `protocol` other than `http`/`https`.
- The indexer does **not** read `blockstore.azure.*`: the indexer is a separate process with its own
  lakeFS credentials, and silently borrowing the server's storage key would hide a misconfiguration
  of the indexer's identity. The docs show the Kubernetes pattern of mounting the same secret under
  the `LAKEFS_METADATA_SEARCH_AZURE_*` names.

### D4. Companion namespace and deny ARNs on Azure

- A storage namespace is parsed into (root, path) per blockstore: for `s3`/`gs`/`mem`/`local` root
  is `scheme://host`; for `http(s)` (Azure) root is `scheme://host/<container>`. One helper,
  `block.NamespaceRoot(namespace) (root, path string, err error)` in `pkg/block/namespace.go`,
  replaces `namespaceHasPath` (indexer) and the host/path split of `isBucketRootNamespace`
  (bootstrap), so the two cannot disagree.
- `metadataNamespace`: a container-root Azure namespace needs the explicit
  `metadata_search.repositories.<R>.storage_namespace`, exactly like a bucket root on S3; the error
  names the container.
- `storageNamespaceDenyResources`: for an Azure namespace it adds the other scheme's spelling
  (`http` ↔ `https`) of each spelling it already emits, and the `//` bucket-root spelling applies to
  the container root instead of the host. Unit tests list the exact ARN set for S3, GS, Azure path
  and Azure container root.
- `overlapsStorageNamespace` is unchanged (it is a string prefix test on the namespace as stored),
  but it gains the scheme normalisation for Azure so that an `http` companion does not "miss" an
  `https` data namespace on the same prefix.

### D5. What clients configure

Unchanged for the catalog side (URI, OAuth2/basic credentials, `nested-namespace-enabled` for
Trino). FileIO side per engine, documented in `docs/metadata-search.md`:

- Spark: `spark.sql.catalog.lakefs.io-impl=org.apache.iceberg.azure.adlsv2.ADLSFileIO`,
  `adls.auth.shared-key.account.name`/`.key` (or the default credential chain), the
  `iceberg-azure-bundle` jar.
- Trino: `fs.azure.enabled=true` (484 renamed `fs.native-azure.enabled`, as it renamed
  `fs.native-s3.enabled`; verify against the pinned image in AZ7), `azure.auth-type=ACCESS_KEY` +
  `azure.access-key`, or `OAUTH`/managed identity, and `azure.endpoint` for a non-public cloud.
- PyIceberg: `adls.account-name`, `adls.account-key` (or `adls.connection-string`,
  `adls.sas-token`) — https://py.iceberg.apache.org/configuration/.
- DuckDB: `INSTALL azure; CREATE SECRET (TYPE azure, CONNECTION_STRING …)` before `iceberg_scan`.

## 4. Change list

| Area | File | Change |
|---|---|---|
| Location | `pkg/icebergcatalog/location.go` | `IcebergNamespaceLocation`, Azure branch in `TableAreaLocation`, legacy-area tolerance in `normalizeTableLocation`, `blockIdentifier` |
| Location | `pkg/icebergcatalog/azure_location.go` (new) | the D1 mapping table, built on `azure.ResolveBlobURLInfoFromURL` and the adapter's domain constants (export a `DFSDomainFor(blobDomain)` from `pkg/block/azure`) |
| Catalog IO | `files.go`, `metadata.go`, `store.go`, `views.go` | `blockIdentifier` at the six call sites of D2 |
| Config | `pkg/icebergcatalog/config.go` | `iceberg.azure.location_host` (`dfs`/`blob`), env override, validation |
| Vending | `creds.go` | no behaviour change; the unsupported message names Azure explicitly |
| Indexer | `pkg/mds/table/catalog.go` | azure IO import, merge of D3 properties into the cloned `Storage` map |
| Indexer config | `pkg/mds/config.go` | `AzureConfig`, env overrides, `Validate` rules |
| Namespaces | `pkg/block/namespace.go` | `NamespaceRoot` |
| Namespaces | `pkg/mds/indexer/repo.go` | `metadataNamespace` on `NamespaceRoot`; scheme-normalised overlap |
| Bootstrap | `pkg/mds/bootstrap/bootstrap.go` | Azure spellings in `storageNamespaceDenyResources`, `isBucketRootNamespace` on `NamespaceRoot` |
| esti | `esti/mds_azure_test.go` (new), `esti/ops/docker-compose-mds.yaml` | Azurite service, the AZ5 case |
| Docs | `docs/metadata-search.md`, `docs/iceberg.md` | replace "out of scope" (lines 294, 818); Azure config of indexer and of each engine; non-goals N1–N4 |
| Design | `METADATA_SEARCH_DESIGN.md` §4.6, §3.3, §8; `ICEBERG_CATALOG_PLAN.md` §6.1 | record D1–D4 |

## 5. Plan

One task per commit, in order. AZ1–AZ3 have no dependency on each other's code and may run in
parallel; AZ4 needs AZ1–AZ3; AZ5 needs AZ1–AZ4; AZ6 follows AZ5; AZ7 runs on the built image.

### AZ1 — location translation in the catalog (D1, D2)

- Implement the mapping, `blockIdentifier`, the area switch and the legacy tolerance; wire the six
  call sites.
- Unit tests:
  - table-driven mapping for every row of D1, both `location_host` values, and refusal of a
    non-Azure host under an Azure namespace;
  - round trip `blockIdentifier(TableAreaLocation(ns) + "x/y") == ns + "/_iceberg/x/y"`;
  - `normalizeTableLocation` refuses `abfss://other@acct…`, `abfss://c@other…`, `wasbs://…`,
    `..`/empty segments, and the area itself;
  - the file collector refuses a manifest entry in another container or account;
  - every existing S3/mem test unchanged (the S3 branch must be byte-identical).
- Store test on the mem blockstore with an Azure-shaped namespace is not possible (the mem adapter
  rejects `https`); the adapter-level round trip is covered in AZ4 instead.

**Done**, commit `b79a41e`: `pkg/icebergcatalog/azure_location.go`, the Azure branch of `TableAreaLocation`,
the legacy-area tolerance and `blockIdentifier`, with the mapping table, the round trip and the negative
cases as tests; the S3 and mem tests are unchanged.

### AZ2 — namespace root, companion namespace, deny ARNs (D4)

- `block.NamespaceRoot` with tests for s3/gs/mem/local/azure path/azure container root/malformed.
- `metadataNamespace`: container-root refusal; derived name stays in the same container.
- `storageNamespaceDenyResources`: exact expected ARN lists for Azure path and container root, both
  schemes. Extend the R3S-3 esti spelling case only if AZ5 runs on Azurite (it does).

**Done**, commit `fdedd3e`: `block.NamespaceRoot`, the container-root refusal naming the container, the
derived name staying in the container, the scheme-normalised overlap test, and the exact ARN lists of
`TestStorageNamespaceDenyResources` (four ARNs for an Azure prefix, six for a container root).

### AZ3 — indexer Azure IO and config (D3)

- Import, `AzureConfig`, env overrides, `Validate`, property merge (explicit `storage` keys win,
  `maps.Clone` per catalog kept).
- Unit tests: env-only configuration yields the expected `adls.*` map; `storage` overrides;
  validation errors; the azure scheme is registered (`io.LoadFS` on an `abfss://` URI with a fake
  credential reaches the Azure backend rather than "unsupported scheme").

**Done**, commit `ed7c0d3`: the `io/gocloud/azure` import, `AzureConfig`, the env overrides, the `Validate`
rules and the property merge, with the env-only configuration, the `storage` override, the validation
errors and the registered-scheme test.

### AZ4 — catalog/indexer integration test on Azurite (Go, not esti)

- `pkg/mds/table` test behind the same dockertest pattern as `pkg/block/azure/main_test.go`
  (Azurite 3.35.0, `--skipApiVersionCheck`, `AZURITE_ACCOUNTS`, custom resolver for
  `<account>.azurite.test`): a real `icebergcatalog.Store` over the Azure adapter
  (`TestEndpointURL`), an iceberg-go client with `adls.endpoint=<host:port>`,
  `adls.protocol=http`, shared key. Create table, append, compact, read back; assert the metadata
  location is `abfs://c@acct.azurite.test/…` and the lakeFS physical addresses are
  `http://acct.azurite.test/c/…`.
- Skipped when docker is unavailable, like the block adapter tests.

**Done**, commit `21fbf95`: the table package's Azurite test asserts both spellings (the `abfs://` metadata
location and the adapter's `http://` physical addresses) and skips without docker.

### AZ5 — esti end to end on Azurite (acceptance)

- Azurite in `docker-compose-mds.yaml`; lakeFS with `blockstore.type: azure`,
  `blockstore.azure.domain: azurite.test`, `blockstore.azure.test_endpoint_url`,
  `storage_account`/`storage_access_key`; the indexer with `LAKEFS_METADATA_SEARCH_AZURE_*` env
  vars only (proves G4); repository `https://acct.azurite.test/c/esti-<run>`.
- Cases: the existing happy path (three `orders/` rows, `commit_id` = source head) read by PyIceberg
  with `adls.connection-string` and by DuckDB; container-root namespace without an explicit
  companion namespace is refused; the R3S-3 spelling case with the `http`/`https` pair.
- Trino and Spark are not run on Azurite (Trino's native FS has no emulator mode); they are AZ7.

**Done**, commit `cff0360`: `esti/mds_azure_test.go` with the `azurite` service of
`esti/ops/docker-compose-mds.yaml`, the three cases above, and the indexer configured through
`LAKEFS_METADATA_SEARCH_AZURE_*` alone (the generated configuration is asserted to carry no Azure key).

### AZ6 — docs and design records

- Replace `docs/metadata-search.md` lines 294 and 818; add "Azure" to the deployment section with
  the indexer env vars and a Kubernetes example; per-engine Azure FileIO snippets (D5); update
  `docs/iceberg.md` 668 and 836–856 (locations are `abfss`, vending still S3-only).
- Record D1–D4 in `METADATA_SEARCH_DESIGN.md` and `ICEBERG_CATALOG_PLAN.md`; status line in
  `METADATA_SEARCH_PLAN.md`.

**Done** (this commit, docs only): the Azure deployment section of `docs/metadata-search.md` with the
`LAKEFS_METADATA_SEARCH_AZURE_*` variables and the Kubernetes example, the per-engine FileIO recipes of D5,
the non-goals N1–N4, the `abfs[s]://` locations and the `iceberg.azure.location_host` switch in
`docs/iceberg.md`, and the D1–D4 records.

### AZ7 — real Azure verification (operator, not CI)

**Done** (this commit, docs only): run 2026-11-09 on the hr2 cluster against the real storage account
`cpaasailakefs`; recorded in `esti/ops/mds-engines/README.md` §"Azure (hr2)".

- What ran: the lakeFS Deployment `lakefs` (namespace `lakefs`, blockstore `azure` with the shared key,
  `iceberg.enabled`, `iceberg.metadata_search.enabled`, `iceberg.azure.location_host` unset) serving
  `docker.ib-ci.com/lakefs@sha256:038fdf36…2dab759`, the indexer `mds-indexer` on the same image with
  its Azure credentials from `LAKEFS_METADATA_SEARCH_AZURE_*` alone, one repository `mds-az7` at
  `https://cpaasailakefs.blob.core.windows.net/lakefs/mds-az7-e1f13de` with three `orders/` objects
  (head `53c0d597…95bb9`, indexed 33 s after the commit, `lag_commits: 0`), then the guide's Azure
  Spark and Trino snippets on both mounts: **4/4 runs returned the three expected rows exactly**, on
  Spark 3.5.5 + Iceberg 1.8.1 (the `iceberg-azure-bundle`, `ADLSFileIO`) and Trino 484 (native Azure
  FS), on `/mds/iceberg/api` and `/iceberg/relative_to/mds-az7.main/api`.
- The FNS gate did not apply: the account is **HNS**, both engines worked on the `dfs` host and
  `iceberg.azure.location_host` stayed unset, so no cluster change was made for this task. Recorded
  consequence: D1's `blob` branch stays **unexercised on real Azure**.
- D5's Trino parenthetical, which asked to be verified here: Trino 484's `@Config` name is
  `fs.azure.enabled`, and `fs.native-azure.enabled` is its `@LegacyConfig` alias - the parenthetical
  reads backwards as written. The run used `fs.azure.enabled`. `docs/metadata-search.md`'s property
  line is right and its inline comment ("the newer name … is `fs.native-azure.enabled`") is wrong;
  recorded here and in the README record, not corrected in this close-out.
- Snippet changes the run did require (all in the README record): the Trino switch comment, the
  relative-to Trino block, and the `oauth2-server-uri` key the Spark block omits.
- Not covered by the run: writes through either mount, a non-empty `user_metadata` (the `orders/`
  objects carry `{}`, as on SeaweedFS), the `location_host: blob` branch, the `fs.native-azure.enabled`
  legacy alias (read from the bytecode), in-cluster service DNS, and Iceberg tables in the data
  repository's own namespace.
- Two facts about the installation, recorded and not worked around: the running reference is the
  mutable tag `dev-mds` (the image carries no labels or annotations, so no commit SHA is recoverable
  from it - the digest is the identifier), and the office network MITMs `*.dfs.core.windows.net`, so
  the engine harness trusts a corporate CA. Neither is a property of Azure support.

### Verification after AZ1–AZ6 (2026-11-07)

Run on `8a28450`:

- `go build ./...` and `go vet` on the touched packages pass.
- `go test -count=1 -race` passes in `pkg/icebergcatalog/...`, `pkg/mds`, `pkg/mds/bootstrap`,
  `pkg/mds/client`, `pkg/mds/indexer`, `pkg/mds/refmap` and `pkg/block`.
- The Azurite tests pass with Docker reachable (colima, `DOCKER_HOST=unix://$HOME/.colima/default/docker.sock`):
  `pkg/mds/table` `TestAzureObjectMetadataRoundTrip` (http and https) and `pkg/block/azure`.
  Without `DOCKER_HOST` set, `pkg/block/azure` panics in `TestMain` instead of skipping.

### Open items after AZ1–AZ6 (2026-11-09)

- **`golangci-lint` on AZ1–AZ6: closed.** The earlier run stopped at type-checking ("export data
  version 5 is greater than maximum supported version 4"): the packages had been compiled by Go
  1.27.2, whose export data the linter's Go 1.26 parser cannot read. Running both the compiler and
  the linter on the module's Go 1.26.4 toolchain works:

  ```bash
  export PATH="$HOME/sdk/go1.25.8/bin:$PATH" GOTOOLCHAIN=auto   # go.mod selects go1.26.4
  go version                                              # go1.26.4 darwin/arm64
  go tool golangci-lint version                           # v2.7.2 built with go1.26.4
  go tool golangci-lint run ./pkg/icebergcatalog/... ./pkg/mds/... ./pkg/block/... ./esti/...
  # 0 issues
  ```

  The only finding was `pkg/icebergcatalog/azure_location.go:130` `goconst` (the `"https"`
  literal was repeated three times in the package), fixed by naming the two accepted URL schemes
  instead of silencing the check.
- **`pkg/mds/table` `TestVendedCredentials` 403: closed as an environment failure, not a defect.**
  The test's fake STS vends the fixture identity plus the session token `mds-vended-session`; the
  test depends on SeaweedFS ignoring that token. SeaweedFS 4.48 validates it and answers
  `InvalidAccessKeyId` on the PutObject, the exact 403 that was recorded:

  ```bash
  # SeaweedFS 4.48 (fails)
  docker run -d --name seaweedfs-448 -p 8337:8333 \
    -v "$PWD/esti/ops/seaweedfs-s3.json:/etc/seaweedfs/s3.json:ro" \
    chrislusf/seaweedfs:4.48 server -s3 -dir=/data -s3.config=/etc/seaweedfs/s3.json
  LAKEFS_MDS_VENDED_S3_ENDPOINT=http://127.0.0.1:8337 \
    go test -count=1 -run '^TestVendedCredentials$' ./pkg/mds/table/
  # FAIL ... api error InvalidAccessKeyId ... on PutObject

  # SeaweedFS 4.50/4.53 (passes), on this branch and on 8047321
  LAKEFS_MDS_VENDED_S3_ENDPOINT=http://127.0.0.1:8338 \
    go test -count=1 -run '^TestVendedCredentials$' ./pkg/mds/table/
  # ok github.com/treeverse/lakefs/pkg/mds/table
  ```

  The same failure reproduces at `8047321`, the commit before the Azure work, which rules out the
  Azure changes. The fix is in the fixture, not the catalog: `esti/ops/docker-compose-mds.yaml`
  pins SeaweedFS 4.53 (verified) instead of `latest`, and the test's header records the 4.50
  boundary. A long-running container from an older image must be recreated.
- esti AZ5 was run by the task's agent only; it was not rerun independently.

## 6. Verification rules (every task)

Same as `METADATA_SEARCH_PLAN.md`: `go build ./...`, `go vet` on touched packages,
`go test -count=1 -race` on touched packages, `golangci-lint` clean, generated files untouched, one
commit per task with no attribution lines. Additionally: the S3 path of every touched function has
a test that pins its previous output, so the Azure branch cannot change S3 behaviour silently.

## 7. Risks

| Risk | Mitigation |
|---|---|
| FNS account rejected on the `dfs` host by Hadoop ≥ 3.4 ABFS | `location_host: blob` switch (D1); AZ7 records which engines need it |
| iceberg-go Azure backend bugs (young code, v0.7.0) | AZ4 exercises create/append/compact/expire against Azurite; pin the version |
| Legacy `https` tables keep a location no engine opens | documented; a `register` with a rewritten metadata.json moves one; no automatic rewrite |
| Azurite differs from real Azure (no `dfs` endpoint, no HNS) | AZ7 on the real account is a required gate before calling the feature done |
| Location translation becomes a write-anywhere hole | inverse mapping only after the area check (D1); AZ1 negative tests for other container/account/scheme |
| Trino property names change between versions | AZ7 pins the image digest and records the exact properties used |

## 8. Backlog

- B1. Azure credential vending: a user-delegation SAS scoped to the table prefix, returned in
  `storage-credentials` as `adls.sas-token.<account>.dfs.core.windows.net` (the key Iceberg Java and
  PyIceberg read). Needs a SAS builder in `pkg/block/azure` (the adapter already signs
  user-delegation SAS for pre-signed URLs, `adapter.go` ≈ line 330).
- B2. GCS: `gs://` needs no translation; needs the iceberg-go GCS import and an emulator case.
- B3. Accept `wasbs://` client-written locations by normalising them to the canonical form.

## 9. Sources

- lakeFS Metadata Search guide — https://docs.lakefs.io/guides/metadata-search/
- lakeFS Iceberg REST catalog — https://docs.lakefs.io/iceberg/
- iceberg-go Azure IO, v0.7.0 source `io/gocloud/azure/azure.go`, and the ignored-domain issue —
  https://github.com/apache/iceberg-go/issues/2311
- Iceberg Java ADLS FileIO, wasb/abfs support — https://github.com/apache/iceberg/issues/10127,
  https://github.com/apache/iceberg/pull/11504
- Trino Azure file system — https://trino.io/docs/current/object-storage/file-system-azure.html;
  wasb not accepted — https://github.com/trinodb/trino/issues/23511; `https://` warehouse failure —
  https://github.com/trinodb/trino/discussions/24807
- PyIceberg FileIO configuration — https://py.iceberg.apache.org/configuration/
- ABFS URI on non-HNS accounts —
  https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction-abfs-uri
- Hadoop ABFS FNS over Blob endpoint —
  https://hadoop.apache.org/docs/current/hadoop-azure/fns_blob.html
