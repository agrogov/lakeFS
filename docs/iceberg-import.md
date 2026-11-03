# Importing tables from another Iceberg catalog

`lakectl iceberg import` copies or adopts Apache Iceberg tables from a source
[Iceberg REST catalog](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)
into a branch of a lakeFS repository. The command is a client of three things and of nothing else:

- the source catalog, over the Iceberg REST protocol (`GET /v1/config`, list namespaces, list
  tables, load table);
- the lakeFS Iceberg REST catalog mounted by the `lakefs-sso` binary at
  `<lakefs-server>/iceberg/api`, over the same protocol (`CreateNamespace`, `LoadTable`,
  `DropTable` and `register`);
- the lakeFS object API, and only in `--copy` mode, to write the copied files.

It never imports `pkg/icebergcatalog` and never writes the catalog's state directly. Everything it
does to lakeFS is one of the HTTP requests a normal Iceberg client could make.

```bash
lakectl iceberg import \
  --source-uri http://nessie:19120/iceberg \
  --repo analytics --branch main \
  --namespace sales --target-namespace-prefix imported
```

```text
SOURCE TABLE    TARGET TABLE                          STATUS    DETAIL
sales.orders    analytics.main.imported.sales.orders imported  s3://warehouse/_iceberg/9c12.../metadata/...
Imported: 1  Skipped: 0  Failed: 0
```

Nothing is committed. Namespace markers, table pointers and the entries that protect a table's
files are *staged* on the branch, exactly as for a table created or appended to by a client
([`iceberg.md`](iceberg.md#what-a-commit-does)). Publish the import with an explicit commit:

```bash
lakectl commit lakefs://analytics@main -m "iceberg: import sales.orders from Nessie"
lakectl log lakefs://analytics@main
```

## Why there are two modes

The lakeFS catalog's register operation reads `metadata.json`, manifest lists and manifests **with
the lakeFS server's storage credentials**, so it only accepts a `metadata-location` inside the
repository's storage namespace, and only stages files under the table's own location inside
`<storage_namespace>/_iceberg/`. This is a security control, not a limitation that can be
configured away: without it, a register request would be a request to read (and publish) another
bucket.

That gives the command two modes:

| Mode | What it does | When to use it |
|---|---|---|
| default | Registers the table **in place**, with the `metadata-location` the source reported. Refuses, per table, a location outside the repository storage namespace or a table root outside `<storage_namespace>/_iceberg/`. | The source and lakeFS share a warehouse — for example when importing from another lakeFS catalog, or when lakeFS is configured over the same bucket and prefix the source wrote to. |
| `--copy` | Copies the table's metadata, manifest lists, manifests and data files into `<storage_namespace>/_iceberg/<new-table-uuid>/` through the lakeFS object API, rewrites the absolute locations stored in the metadata document, the manifest lists and the manifests, and registers the rewritten document. | The source is another catalog with its own bucket: Glue, Nessie, Hive, another lakeFS installation, … |

A refusal in the default mode is reported for that table and names `--copy`; it never falls back
to copying silently, because a copy duplicates every byte of the table and changes its physical
identity.

## Flags

| Flag | Meaning |
|---|---|
| `--source-uri URL` | Source catalog base URL: the URL whose `/v1/config` is `<URL>/v1/config`. A trailing `/v1` is accepted. |
| `--source-credential ID:SECRET` | OAuth2 client credentials. The command posts them to `<source>/v1/oauth/tokens` (`grant_type=client_credentials`) and sends the access token as a bearer token. |
| `--source-token TOKEN` | A bearer token, used as-is. Mutually exclusive with `--source-credential`. |
| `--source-warehouse WAREHOUSE` | Sent as the `warehouse` query parameter of `GET /v1/config`. Glue uses it for the account, several catalogs for warehouse selection. |
| `--source-prop k=v` | A client setting; repeatable. Recognised keys: `prefix` (use this prefix and skip `GET /v1/config`), `oauth2-server-uri` (a token endpoint elsewhere), `scope` (OAuth2 scope), and the FileIO keys below. |
| `--repo`, `--branch` | The lakeFS destination. The branch must exist. |
| `--namespace NS` | A source namespace to import **with its descendants**; repeatable. Levels are dot-separated (`sales.eu`). Because a lakeFS namespace level may itself contain a dot, a value containing the Iceberg unit separator (`\x1f`) is split on that instead — the unambiguous form. |
| `--all` | Every namespace of the source catalog. Mutually exclusive with `--namespace`. One of the two is required. |
| `--target-namespace-prefix P` | Levels prepended to every imported namespace, dot-separated. `sales` with prefix `imported.2024` becomes `imported.2024.sales` in lakeFS. |
| `--overwrite` | Replace a table that already exists: drop it, then import it again. |
| `--dry-run` | Discover, load the metadata and check the target, but write nothing. |
| `--copy` | Copy the table's files into the storage namespace and rewrite locations. |

The command exits non-zero if any table failed, after printing the whole summary. A failure to
*list* the source (unreachable catalog, bad credential, missing namespace) is a hard error and no
table is attempted; a failure of one table never stops the others.

## Addressing

The lakeFS catalog's namespace tree carries the repository and the ref as its first two levels
([`iceberg.md`](iceberg.md#addressing)). A source table `sales.orders` in `--repo analytics
--branch main` is therefore registered as

```text
namespace: analytics ␟ main ␟ sales
table:     orders
```

and is read by a client as `analytics.main.sales.orders`. The importer creates the namespace
markers it needs on the branch before the first register, including every multi-level ancestor, and
treats a namespace that already exists as success — that is what makes a re-run cheap.

## What `--copy` rewrites

Copying a table is not a byte-for-byte directory copy: Iceberg stores **absolute** locations in
three places, and all three must point at the new root.

| File | Fields rewritten |
|---|---|
| `metadata.json` | `location`; every snapshot's `manifest-list`; every v1 inline `manifests` entry; the location-valued write properties (`write.data.path`, `write.metadata.path`, `write.metadata.statistics.path`) when they were under the old root; a new `table-uuid`. |
| manifest list (Avro OCF) | `manifest_path`, and `manifest_length` — the re-encoded manifest has a different size. |
| manifest (Avro OCF) | every entry's `data_file.file_path`. |

Data files are copied byte for byte, so their sizes and column statistics stay true. The Avro files
are decoded and re-encoded with the file's own embedded schema and codec (null, deflate, snappy and
zstandard are supported); fields the importer does not know about — partition values, bounds,
counts, and anything a newer Iceberg version adds — are preserved because the document is decoded
generically and only the two path fields are changed.

Three things are deliberately **not** copied:

- `metadata-log` — the chain of earlier metadata documents. The catalog does not stage those files
  for the registered table, so a copied document must not claim them;
- `statistics` and `partition-statistics` — Puffin files, for the same reason. Query planning falls
  back to the manifests' column statistics;
- files referenced by **DELETED** manifest entries whose status is `DELETED`. They are historical
  and may already have been removed by the source's expire job; their paths are still rewritten for
  consistency, but a missing one is not an error.

Every file the copy *does* need must be under the source table's `location`. A table whose data
files live elsewhere (a shared `write.data.path` in another bucket, for example) is refused for
that table with the offending file named, because there is no well-defined relative path to give
it under the new root.

The copied table gets a **new table UUID**, derived deterministically from the source metadata
location and the target identifier. The derivation is what makes a resumed import land on the same
location and overwrite its own files, and a fresh UUID per target table is what keeps the
catalog's `_lakefs_iceberg/files/<uuid>/` entries from colliding when one source table is imported
into two namespaces.

The copied files are written through the lakeFS object API at `_iceberg/<uuid>/…` on the target
branch, which resolves to the physical location the rewritten metadata names. The copy is
therefore visible in `lakectl diff` and protected by the uncommitted garbage-collection grace until
the import is committed; after the commit, the `_lakefs_iceberg/files/<uuid>/` entries that register
stages take over the GC protection.

## Reading source files

`--copy` reads the source's files directly, not through the source catalog. The reader supports:

| Scheme | Configuration |
|---|---|
| `http://`, `https://` | fetched with the command's HTTP client; no extra configuration. |
| `file://`, an absolute path | read from the local filesystem. |
| `s3://`, `s3a://` | the AWS SDK. Properties (all `--source-prop`): `s3.endpoint`, `s3.region`, `s3.access-key-id`, `s3.secret-access-key`, `s3.session-token`, `s3.path-style-access=true`. Without explicit keys the SDK's ambient chain (environment, profile, instance role) is used. |

Any other scheme is refused per table, naming it. There is no implicit fallback: a `gs://` or
`abfs://` warehouse needs an importer that knows that filesystem, and silently doing something
else would be worse than a clear refusal.

```bash
# Glue catalog, source bucket in another account, static keys for the data read
lakectl iceberg import \
  --source-uri https://glue.us-east-1.amazonaws.com/iceberg \
  --source-warehouse 123456789012 \
  --source-prop s3.region=us-east-1 \
  --source-prop s3.access-key-id="$SRC_KEY" \
  --source-prop s3.secret-access-key="$SRC_SECRET" \
  --repo analytics --branch main --all --copy
```

## Bulk, resume and `--overwrite`

The command imports tables one at a time, in a stable order, and reports one row per table with
`imported`, `skipped` or `failed` (and `planned` for `--dry-run`). The file copies of one table run
with bounded concurrency; tables themselves are sequential, so a failure is always attributable to
one table and the report is deterministic.

Resuming is the default behaviour:

- a table that already exists in the target is **skipped** — no metadata read, no file copied, no
  register — unless `--overwrite` is given. This is what makes `--copy` restartable after a
  partial run;
- `--overwrite` replaces a table by dropping it and registering it again. The register operation
  itself never overwrites: its pointer write is a compare-and-swap on an absent value. The drop
  and the new register are staged on the branch and become one change on the next commit. If the
  import of a dropped table then fails, the table stays dropped (its files are left to garbage
  collection) and the failure is reported — re-run with `--overwrite` after fixing the cause;
- format version 3 tables are refused per table. This catalog serves format versions 1 and 2; the
  check runs before anything is copied or registered.

`--dry-run` still discovers namespaces and tables and loads each table's metadata, so it catches a
missing `--copy`, an unsupported format version, a target that is disabled and a table that would
be skipped — but it does not read manifests or data files, and it writes nothing.

## Permissions

The import authenticates to lakeFS exactly like the rest of `lakectl`: HTTP basic with the
configured access key pair, or the `lakectl auth login` token when no key pair is configured.

The catalog then applies its ordinary checks ([`iceberg.md`](iceberg.md#access)): the target
principal needs `fs:ReadRepository` on the repository, `catalog:CreateNamespace` on each created
namespace, `catalog:CreateTable` on every imported table (register is authorized as a create),
`catalog:DeleteTable` when `--overwrite` drops a table, and `fs:WriteObject` on
`_lakefs_iceberg/*`. In `--copy` mode it additionally needs the ordinary object-API write
permission for the branch (`fs:WriteObject` on the repository path, which `repo:WriteObject`-style
policies and the `Developers` / `SuperUsers` groups grant).

The source catalog has its own authorization; the command can only see what the source credential
can list and load.

## Limitations

- **One ref at a time.** The destination is a single branch. Importing into several branches is
  several runs; importing a source ref other than the source catalog's default is not expressible
  in the REST protocol without a ref/snapshot selector.
- **No rename mapping.** The source namespace and table names are preserved below
  `--target-namespace-prefix`. Renaming is a catalog `rename` operation, which is out of scope for
  the lakeFS catalog v1.
- **No metadata-log history.** A copied table starts with the latest metadata document; its earlier
  metadata documents are not copied (see above).
- **Copy duplicates data.** It is a physical copy, not a zero-copy clone; a second import of the
  same source table creates a second copy under a different UUID.
- **The source may answer with a page size of its own.** Listings follow `next-page-token` to the
  end; a catalog that repeats a page token is refused rather than looped.
- **A lakeFS source has the repository and ref in its namespaces.** The lakeFS catalog's first two
  namespace levels are the repository and the ref, so `--namespace myrepo.main.sales` imports that
  namespace (and, under `--all`, every repository, branch and namespace is enumerated and its macro
  levels become ordinary target namespace levels). Pointing lakeFS at lakeFS is the case the
  default mode is for: the files are already in a lakeFS storage namespace, so no copy is needed
  when the two repositories share one.
- **No esti case yet.** The end-to-end coverage lives in
  `pkg/icebergcatalog/importer/import_test.go`, which mounts the production catalog router over an
  in-memory graveler store and block adapter and imports a table served by an HTTP source — the
  same register, staging, rewriting and foreign-location code paths, without docker or an S3
  blockstore. The esti harness would add the real object-store variant.
