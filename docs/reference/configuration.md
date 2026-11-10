# lakeFS configuration: Iceberg and Metadata Search

Configuration is a YAML file and/or environment variables. The server reads its file with
`lakefs run --config <file>`; the Metadata Search indexer runs on the same server binary and reads
the top-level `metadata_search` section of the same file with `lakefs mds run --config <file>`.
Both can share one file: the server ignores `metadata_search`, and the indexer ignores everything
else.

Every scalar key can also be set through the lakeFS environment convention: prefix `LAKEFS_`,
replace each `.` with `_`, and upper-case the result. For example `LAKEFS_ICEBERG_ENABLED` sets
`iceberg.enabled`, and `LAKEFS_METADATA_SEARCH_SECRET_KEY` sets `metadata_search.secret_key`. The
`metadata_search.repositories` and `metadata_search.storage` maps are read from the file.

The full lakeFS server reference is published with the documentation site:
<https://docs.lakefs.io/reference/configuration/>. The documentation tree lives in a separate
repository; this page records the sections the code in this repository adds, so that the feature
and its configuration stay together.

Related guides: [`../iceberg.md`](../iceberg.md) for the catalog and
[`../metadata-search.md`](../metadata-search.md) for setup, engine recipes, semantics and
maintenance.

## Iceberg catalog (server)

The catalog is off by default. `iceberg.enabled: true` mounts `/iceberg/api` and
`/iceberg/relative_to`; see [`../iceberg.md`](../iceberg.md) for every Iceberg setting
(`prefix`, `token_ttl`, `max_request_body`, `uncommitted_gc_grace`, `max_staged_files`,
`ignore_compaction_commits`).

* `iceberg.metadata_search.enabled` `(bool : false)` - serve the Metadata Search virtual table
  `<repo>.<ref>.system.object_metadata`, reserve the `system` namespace of data repositories, and
  mount the catalog at `/mds/iceberg/api`. The server reads no other Metadata Search key; the
  indexer reads no other server key. Environment: `LAKEFS_ICEBERG_METADATA_SEARCH_ENABLED`.
* `iceberg.azure.location_host` `(one of ["dfs"|"blob"] : "dfs")` - the service part of the host of
  a translated Azure table location: `abfss://<container>@<account>.dfs.core.windows.net/…` by
  default, or `…@<account>.blob.core.windows.net/…` when set to `blob`, for an engine whose Hadoop
  ABFS driver refuses a non-HNS account over the `dfs` host. The scheme stays `abfs[s]`. It changes
  what every engine sees, so it is one server-wide setting.
  Environment: `LAKEFS_ICEBERG_AZURE_LOCATION_HOST`.

## Metadata Search indexer (`metadata_search`)

Read by `lakefs mds run --config <file>` (design §7). The endpoint and the indexer credentials
are required; every other key has a default.

### Reference

* `metadata_search.lakefs_mds_endpoint` `(string : )` - **Required.** Base URL of lakeFS. The API
  is used at `/api/v1` and the Iceberg catalog at `/iceberg`; do not append either one.
* `metadata_search.access_key_id` `(string : )` - **Required.** Access key ID of the `mds-indexer`
  user.
* `metadata_search.secret_key` `(string : )` - **Required.** Secret access key of `mds-indexer`.
  Prefer `LAKEFS_METADATA_SEARCH_SECRET_KEY` over writing it in the file.
* `metadata_search.listen_address` `(string : "0.0.0.0:8080")` - Address of the indexer's own HTTP
  server. `/_health`, `/metrics` and `/_pprof/` are served there.
* `metadata_search.period` `(duration : 60s)` - Interval between ticks. Must be positive.
* `metadata_search.concurrency` `(int : 25)` - Branches one tick processes in parallel. Must be
  positive. Commits within a branch are always sequential.
* `metadata_search.max_commits` `(int : 1000)` - Maximum number of source commits one branch
  applies per tick. Must be positive. The next tick resumes at the recorded position.
* `metadata_search.since` `(RFC 3339 timestamp : )` - Optional. Oldest commit a first indexing
  starts at, by creation date. When set, first indexing walks back to the oldest first-parent commit
  created at or after it, whatever `backfill` says.
* `metadata_search.backfill` `(one of ["head"|"history"] : "head")` - What a first indexing covers
  when `since` is unset: the head commit only (`head`), or the branch's oldest first-parent commit
  (`history`).
* `metadata_search.seed_lookback` `(int : 1000)` - How many first-parent commits of a new branch to
  search for an already-indexed ancestor whose `mds-*` tag can seed the new branch. Must be
  positive.
* `metadata_search.rebuild_threshold` `(int : 100000)` - Upserts plus deletes in one commit above
  which the branch's table is rebuilt from a full listing instead of applying a delta. Must be
  positive.
* `metadata_search.delete_branches` `(bool : true)` - Delete the mirrored branch of `R-metadata` when
  a matching branch of `R` is deleted. The `mds-*` tags of that branch stay.
* `metadata_search.tag_retention` `(duration : 0s)` - Delete `mds-*` tags whose source commit is
  older than this. `0` keeps every tag forever.
* `metadata_search.compaction.max_delete_files` `(int : 200)` - Compact a branch's table when it has
  more than this many position-delete files. `0` disables the trigger.
* `metadata_search.compaction.every_commits` `(int : 500)` - Compact a branch's table after this many
  indexed commits. `0` disables the trigger. The counter is in memory, so a restart can defer a
  compaction by up to this many commits.
* `metadata_search.storage` `(map<string,string> : )` - Optional iceberg-go IO properties used when
  the catalog does not vend write credentials. Recognised keys for S3:
  `s3.endpoint`, `s3.region`, `s3.access-key-id`, `s3.secret-access-key`, `s3.path-style-access`.
  For Azure, an explicit `adls.*` key wins over the one `metadata_search.azure` translates; GCS is
  not supported (no GCS IO is registered).
* `metadata_search.azure` `(section : )` - Optional. Azure credentials and endpoint of the indexer's
  IO when the blockstore is Azure. Scalar end to end, so every key below is also an environment
  variable, which is what makes a Kubernetes secret usable; the indexer never reads
  `blockstore.azure.*`. With none of them set, iceberg-go uses `DefaultAzureCredential` (workload
  identity, managed identity, Azure CLI).
  * `metadata_search.azure.storage_account` `(string : )` - storage account name; required for the
    shared key. Environment: `LAKEFS_METADATA_SEARCH_AZURE_STORAGE_ACCOUNT`.
  * `metadata_search.azure.storage_access_key` `(string : )` - the account's shared key. Setting it
    without `storage_account` is a validation error. Environment:
    `LAKEFS_METADATA_SEARCH_AZURE_STORAGE_ACCESS_KEY`.
  * `metadata_search.azure.use_managed_identity` `(bool : false)` - use a managed identity instead of
    the credential chain. Environment: `LAKEFS_METADATA_SEARCH_AZURE_USE_MANAGED_IDENTITY`.
  * `metadata_search.azure.client_id` `(string : )` - user-assigned managed identity client ID.
    Environment: `LAKEFS_METADATA_SEARCH_AZURE_CLIENT_ID`.
  * `metadata_search.azure.endpoint` `(string : )` - storage domain iceberg-go builds its service URL
    from (for example `blob.core.windows.net`). Required for any cloud but the public one, and for
    Azurite, because iceberg-go ignores the domain in the table location. Environment:
    `LAKEFS_METADATA_SEARCH_AZURE_ENDPOINT`.
  * `metadata_search.azure.protocol` `(one of ["http"|"https"] : "https")` - the scheme of that
    service URL. Environment: `LAKEFS_METADATA_SEARCH_AZURE_PROTOCOL`.
* `metadata_search.repositories` `(map<string,RepositoryConfig> : )` - **Required.** Data
  repositories to index, by repository name. At least one entry.
* `metadata_search.repositories.<R>.branches` `(list<string> : )` - **Required.** Branch selectors:
  exact names, or a prefix followed by a single trailing `*` (for example `dev-*`). At least one
  entry. A `*` anywhere else is a validation error.
* `metadata_search.repositories.<R>.storage_namespace` `(string : derived)` - Storage namespace of
  `R-metadata`. Defaults to `<R's namespace>-metadata` when `R`'s namespace has a path component. A
  bucket-root namespace (`s3://bucket`) requires this key. The value must not overlap `R`'s
  namespace, and an explicit value that does not end in `-metadata` needs an extra
  `fs:AttachStorageNamespace` grant for `mds-indexer`.

### Example

```yaml
metadata_search:
  lakefs_mds_endpoint: http://lakefs:8000
  access_key_id: AKIA...
  secret_key: ...
  listen_address: 0.0.0.0:8080
  period: 60s
  concurrency: 25
  max_commits: 1000
  since: 2026-01-01T00:00:00Z
  backfill: head
  seed_lookback: 1000
  rebuild_threshold: 100000
  delete_branches: true
  tag_retention: 0s
  compaction:
    max_delete_files: 200
    every_commits: 500
  storage:
    s3.endpoint: http://seaweedfs:8333
    s3.region: us-east-1
    s3.access-key-id: ...
    s3.secret-access-key: ...
    s3.path-style-access: "true"
  # azure:                                   # on an Azure blockstore, instead of line-for-line adls.* keys
  #   storage_account: myaccount
  #   storage_access_key: ...                # or use_managed_identity: true
  #   endpoint: blob.core.windows.net        # required off the public cloud, and for Azurite
  repositories:
    my-repo:
      branches: [main, "dev-*"]
      storage_namespace: s3://bucket/metadata/my-repo
    other-repo:
      branches: [main]
```

### Validation

`lakefs mds run` validates the section before the first tick and fails startup with one error
per problem. Invalid: an empty endpoint, access key or secret; `period`, `concurrency`,
`max_commits`, `seed_lookback` or `rebuild_threshold` not positive; an unknown `backfill`; an empty
`repositories` map or empty `branches` list; a data repository name that ends in `-metadata` or is
longer than 54 characters; a branch selector that is empty or has a `*` that is not trailing; a
`since` that is not RFC 3339; an `azure.storage_access_key` without `azure.storage_account`; an
`azure.protocol` that is neither `http` nor `https`.

Per-repository runtime errors (repository missing, `R-metadata` namespace not derivable or
overlapping) are reported in `/_health` and the metrics, and the remaining repositories continue.

The server side of the feature is the single key `iceberg.metadata_search.enabled` above. Its
principals and credentials, health, metrics and maintenance are documented in
[`../metadata-search.md`](../metadata-search.md).
