# Apache Iceberg REST catalog

The `lakefs-sso` binary serves an [Apache Iceberg REST catalog](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)
for lakeFS repositories. Spark, Trino, PyIceberg and DuckDB speak the Iceberg REST protocol to it, and
the catalog keeps its state — namespaces, tables and the pointers between them — as ordinary lakeFS
entries on a branch. That single design decision is what the rest of this document follows from:
branch, commit, diff, merge, revert and garbage collection work on catalog state with no special
cases, because catalog state *is* lakeFS state.

The catalog is mounted by the hook that also serves the datasets control plane
(`cmd/lakefs-sso/main.go`), at two path prefixes:

| Prefix | Meaning |
|---|---|
| `/iceberg/api` | The catalog. The repository and ref are the first two namespace levels of every identifier. |
| `/iceberg/relative_to` | The same catalog with the warehouse carried in the URL: `/iceberg/relative_to/<repo>[.<branch>]/api/…`. |

`/mds/iceberg/api` (metadata search) is not part of this catalog and answers `501` in every build.

The companion design document is [`ICEBERG_CATALOG_PLAN.md`](../ICEBERG_CATALOG_PLAN.md); the datasets
integration it unblocks is in [`DATASETS_DESIGN.md`](../DATASETS_DESIGN.md) and
[`datasets.md`](datasets.md). That integration is in place: a datasets `table` item copies a table's
pointer and its `_lakefs_iceberg/files/<table-uuid>/` entries at the source ref, a `namespace` item
copies a namespace subtree's markers, pointers and file entries, and both are accepted when
`iceberg.enabled` is true — so a published dataset repository is read as a table at
`<dataset>.<ref>.<namespace>.<table>`.

## Enabling the catalog

The catalog is off by default, and off means off: while `iceberg.enabled` is false the two prefixes
still answer, but every request gets `501 NotImplementedException` in the Iceberg error envelope. The
prefixes stay claimed on purpose — a client asking a disabled catalog must be told that the catalog is
disabled, not receive the lakeFS UI or a gateway error.

```yaml
iceberg:
  enabled: true
  prefix: lakefs                   # the only accepted value; see "Addressing"
  token_ttl: 1h                    # lifetime of a client-credentials token
  max_request_body: 8388608        # 8 MiB; larger bodies are 413
  uncommitted_gc_grace: 24h        # see "Garbage collection"
  max_staged_files: 50000          # cap on the table files one commit may stage; see "Garbage collection"
  ignore_compaction_commits: true   # auto-resolve compaction-only conflicts at merge; see "Merge"
```

Every key also has an environment variable (`LAKEFS_ICEBERG_ENABLED`, `LAKEFS_ICEBERG_TOKEN_TTL`,
`LAKEFS_ICEBERG_MAX_REQUEST_BODY`, `LAKEFS_ICEBERG_UNCOMMITTED_GC_GRACE`,
`LAKEFS_ICEBERG_MAX_STAGED_FILES`, `LAKEFS_ICEBERG_IGNORE_COMPACTION_COMMITS`). An invalid `iceberg:`
section fails startup: a prefix other than `lakefs`, a non-positive `token_ttl`, a negative
`max_request_body`, a negative `uncommitted_gc_grace` or a negative `max_staged_files` are all
configuration errors rather than settings that silently do nothing. The
section is absorbed by the core configuration (`pkg/config/oss_config.go`) so that
`viper.UnmarshalExact` accepts it, and is read by `pkg/icebergcatalog` — never by the core unmarshal.

## Addressing

An Iceberg catalog addresses a table by *namespace + table name*. lakeFS has two more levels, a
repository and a ref (branch, tag or commit), so the catalog's namespace tree carries them as its first
two levels and everything after them is a real Iceberg namespace:

```
repo␟ref␟ns1␟ns2          a namespace (␟ is the separator, %1F on the wire)
repo␟ref␟ns1␟ns2␟table   a table
```

A client writes that as a dotted identifier, `repo.ref.ns1.ns2.table`, and the catalog path parameter
carries the namespace levels joined by `%1F` — the unit separator the Iceberg spec mandates. Both forms
of addressing that the public lakeFS Iceberg documentation describes are served:

| Client calls | Catalog reads it as |
|---|---|
| `GET /iceberg/api/v1/config` | `overrides.prefix = "lakefs"`, an explicit `endpoints` list |
| `GET /iceberg/api/v1/lakefs/namespaces/{repo%1Fref%1Fns…}` | repository, ref, namespace path |
| `GET /iceberg/api/v1/lakefs/namespaces/{repo%1Fref%1Fns…}/tables/{table}` | the same, plus a table name |
| `GET /iceberg/relative_to/{repo}.{branch}/api/v1/…/namespaces/{ns…}` | repository and ref fixed by the URL; every namespace level in the parameter is a real namespace level |
| `GET /iceberg/relative_to/{repo}/api/v1/…/namespaces/{ref%1Fns…}` | repository fixed by the URL; the first level of the parameter is the ref |

Both `relative_to` shapes are mounted, and they are **identity-preserving**: the URL fixes two of the
four components, and a client addresses namespaces and tables with the names it chose and reads those
same names back. On the way in, `pkg/icebergcatalog/relative.go` prepends the levels the URL fixed to
the namespace path parameter *and* to the namespace in the JSON body of a namespace create; on the way
out it drops exactly those fixed levels from every address a response carries — the namespace of a get,
create or list, and the table identifiers of a table listing. So a client of the `{repo}.{branch}` form
creates and lists `inventory`, and reads `inventory` back, while a client of the `{repo}` form sees the
ref as its first level (`main␟inventory`). Only a namespace that starts with exactly the levels the URL
fixed is stripped, so the fixed scope cannot be escaped or partially matched, and a `relative_to` path
that does not have the `<repo>[.<branch>]/api` shape is `404 NoSuchWarehouseException` (the parser is
`pkg/icebergcatalog/ident.go`).

Rules that apply to every form:

- **The prefix is a constant.** `GET /v1/config` returns `overrides.prefix = "lakefs"` and clients
  prepend it to every path. Only `lakefs` is accepted; a different prefix in the configuration is a
  startup error, and a different prefix in a request path is `404 NoSuchWarehouseException`.
- **Namespaces are case-sensitive**, at most 254 characters in total, with no empty level and no
  special characters except dots. A dot is an ordinary character inside a level, not a nesting
  separator: levels are split on the spec's default separator, the unit separator `%1F`, and the catalog
  advertises no `namespace-separator` override (so the default applies and a dotted level name stays
  representable). This is why the Apache REST compatibility kit runs with `supports-names-with-dot=false`.
- **Reads work on any ref.** A branch, a tag and a commit id are all valid; that is what makes a table
  readable at `repo.v1.0.inventory.books` after the tag is cut, and what lets a published dataset be
  read as a table. A repository or ref that does not exist is `404 NoSuchWarehouseException`; a table or
  namespace that does not exist is `404 NoSuchTableException` / `404 NoSuchNamespaceException`.
- **Writes need a branch.** A write addressed at a tag or a commit answers `403 ForbiddenException`
  with the message `ref is read-only`.
- **The namespace listing has three shapes.** `GET /v1/lakefs/namespaces` with no parent lists the
  repositories the caller may read; with `parent=<repo>` it lists that repository's branches; with
  `parent=<repo>␟<ref>` it lists the real namespaces of that ref. Every level is filtered by the
  caller's permissions rather than returned unconditionally.

### Client recipes

The catalog root is the mount prefix; the client appends `/v1/…` itself. For the `/iceberg/api` form
an identifier is `repo.ref.namespace.table`, because the first two namespace levels are the repository
and the ref: the namespace part of a table name is `my-repo.main.inventory`, not `inventory`.

Spark (the namespace is quoted because it contains the repository and ref levels):

```properties
spark.sql.catalog.lakefs=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.lakefs.type=rest
spark.sql.catalog.lakefs.uri=http://lakefs.example.com/iceberg/api
spark.sql.catalog.lakefs.rest.auth.type=basic
spark.sql.catalog.lakefs.credential=<access_key_id>:<secret_access_key>
```

```sql
SELECT * FROM lakefs.`my-repo.main.inventory`.books;
```

Trino:

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://lakefs.example.com/iceberg/api
iceberg.rest-catalog.security=BASIC
iceberg.rest-catalog.vended-credentials-enabled=false
iceberg.rest-catalog.user=<access_key_id>
iceberg.rest-catalog.password=<secret_access_key>
```

DuckDB reads through the `relative_to` form, which is why that prefix exists: the warehouse segment and
the `/api` suffix are part of the endpoint, and DuckDB appends `/v1/…` to it. The `ATTACH` name is then
the catalog name and the schema is the client namespace:

```sql
INSTALL iceberg; LOAD iceberg;
ATTACH 'my-repo.main' AS lake (TYPE iceberg,
  ENDPOINT 'http://lakefs.example.com/iceberg/relative_to/my-repo.main/api',
  TOKEN '<lakefs-jwt>');
SELECT * FROM lake.inventory.books;
```

PyIceberg:

```python
from pyiceberg.catalog import load_catalog
catalog = load_catalog(
    "lakefs",
    type="rest",
    uri="http://lakefs.example.com/iceberg/api",
    token="<lakefs-jwt>",
)
catalog.load_table("my-repo.main.inventory.books")
```

Notes that are worth knowing before the first connection fails:

- The `ENDPOINT`/`uri` must **not** include `/v1`; the client appends it.
- `GET /v1/config` always returns both `defaults` and `overrides` objects (DuckDB hard-fails without
  them) and every value in them is a JSON string.
- `endpoints` is always sent as an explicit list, and each entry is the spec template with a literal
  `{prefix}` segment (`GET /v1/{prefix}/namespaces`). PyIceberg matches those strings byte for byte, so
  a list with the prefix expanded makes every call fail as unimplemented.
- The list is a hint, not an access-control boundary: DuckDB calls endpoints that are not advertised.
  Authorisation and error handling therefore never depend on the client having read the list.

## Staged commits

This is the part of the catalog that is lakeFS rather than Iceberg, and it is deliberate: **a catalog
write is staged on the branch and takes effect on an explicit lakeFS commit**, exactly like any other
change in lakeFS.

- A create-table, a commit (append, overwrite, schema or property change), a drop, a namespace change
  and a pointer update all become staged entries on the branch. They are visible immediately to reads
  through the catalog (staged values win over committed ones) and to `lakectl diff`, and they become
  part of the repository only when someone commits the branch.
- One lakeFS commit can publish several Iceberg operations at once, so a batch of writes does not need
  a lakeFS commit each.
- Because the writes go through the lakeFS catalog without `force`, branch protection applies: a
  protected branch rejects a catalog write, and a dataset repository rejects it through the existing
  immutability deny. That is the intended behaviour rather than a limitation.
- The Iceberg commit itself is a compare-and-swap. The request's requirements are validated against
  the current table metadata; a requirement that does not hold is `409 CommitFailedException` and the
  client refreshes and retries. The pointer is then swapped with a condition on the previous
  `metadata.json` location, so exactly one of two concurrent commits wins; the loser gets `409` too.
  Anything that fails after the new `metadata.json` is written — other than a staging refusal that is
  the client's own `400` — is `500 CommitStateUnknownException`, never `409`: the commit may well have
  landed, so the client must re-read the table instead of assuming failure.
- **The commit stages the files it added.** Each new metadata, manifest-list, manifest and data file
  becomes a lakeFS entry under `_lakefs_iceberg/files/<table-uuid>/…` (see
  [Garbage collection](#garbage-collection)). A commit that names a file outside the table's area, or
  that would stage more than `iceberg.max_staged_files` files (default 50 000), is refused with `400`
  before anything is staged — that refusal is the client's, not an unknown outcome.
- `set-location` is refused with `400`. A table's location is fixed at creation, which is what allows
  the catalog to keep its files in one place across branches.
- **Views follow the same rules.** A view replace is a compare-and-swap on the view pointer's
  `metadata-location` with the same `409` / `400` / `500 CommitStateUnknownException` classification,
  and the metadata document a replace writes is staged as a lakeFS entry before the pointer moves. A
  view has no data files, so there is no file-registration step beyond that one document. `set-location`
  *is* served for views (a view roots no data files and no GC window), but only inside the catalog's
  table area. See [Views](#views).

### What is stored, and where

Two key areas, deliberately different:

```
_lakefs_iceberg/                              on the branch: ordinary lakeFS entries
  ns/<ns1>/<ns2>/.namespace                    namespace marker; user metadata = the properties
  tables/<ns1>/<ns2>/<table>/.pointer           table pointer; user metadata:
                                                  iceberg.metadata-location = physical URI
                                                  iceberg.table-uuid
                                                  iceberg.format-version
  views/<ns1>/<ns2>/<view>/.pointer             view pointer; user metadata:
                                                  iceberg.metadata-location = physical URI
                                                  iceberg.view-uuid
                                                  iceberg.format-version
  files/<uuid>/<relative path>                 one entry per file the commit, create or replace staged:
                                                  table files (metadata, manifest-list, manifest, data)
                                                  and view metadata documents

<storage_namespace>/_iceberg/<uuid>/           in object storage: the files themselves
  metadata/…                                    metadata.json for a table or a view
  data/…                                        table data (views have none)
```

The entry area is versioned (diff, merge, revert, commit); the object area is shared by every branch and
never copied. The table location contains the table UUID and no ref, so a `metadata.json` written on one
branch stays valid after a merge, after the branch is deleted, and when a tag is read years later.
Deriving the location from the branch name — the `s3://<repo>/<branch>/…` gateway form — would embed the
branch in every absolute path and break exactly that.

A table created without an explicit location gets
`<storage_namespace>/_iceberg/<table-uuid>/`. A client that sends its own location keeps it, provided it
is inside `<storage_namespace>/_iceberg/` or below it; a location outside that area is `400`, both
because the server writes the initial metadata there with its own credentials and because a table must
not point into another repository or bucket.

## Views

An Iceberg **view** is a named, versioned SQL query: it has a schema, a set of view versions (each a
list of SQL representations plus a default namespace), and a current version — and no snapshots,
manifests or data files. The catalog serves the six view operations of the REST spec:

| Operation | Endpoint |
|---|---|
| list views | `GET /v1/{prefix}/namespaces/{namespace}/views` |
| load view | `GET /v1/{prefix}/namespaces/{namespace}/views/{view}` |
| view exists | `HEAD /v1/{prefix}/namespaces/{namespace}/views/{view}` |
| create view | `POST /v1/{prefix}/namespaces/{namespace}/views` |
| replace view | `POST /v1/{prefix}/namespaces/{namespace}/views/{view}` |
| drop view | `DELETE /v1/{prefix}/namespaces/{namespace}/views/{view}` |

They are advertised in `GET /v1/config`, they work through both addressing forms (`/iceberg/api` and
`/iceberg/relative_to/{repo}[.{branch}]/api`), and they carry the same Git semantics as tables:
a create, a replace and a drop are staged on the branch and published by an explicit lakeFS commit, so
diff, merge, revert, branch protection and GC apply to views unchanged.

**What a view is made of.** A pointer entry and a metadata document, exactly like a table:

```
_lakefs_iceberg/views/<ns>/<view>/.pointer     the one entry that makes the view exist
  iceberg.metadata-location                    the physical URI of the current view metadata document
  iceberg.view-uuid                            the view uuid the document carries
  iceberg.format-version                       1
<storage_namespace>/_iceberg/<view-uuid>/metadata/<seq>-<uuid>.metadata.json
_lakefs_iceberg/files/<view-uuid>/metadata/…   the lakeFS entry of that document (GC safety, §6.3)
```

A create mints the view uuid, places the view under `<storage_namespace>/_iceberg/<view-uuid>/` unless
the client asked for a location inside that area, writes the initial document, stages its entry, and
stages the pointer with assert-create semantics — a duplicate create is `409 AlreadyExistsException`. A
replace validates the request's requirements (`assert-view-uuid`), applies its updates, writes a fresh
document, stages it, and swaps the pointer with a compare-and-swap on the previous `metadata-location`.
A drop removes the pointer and releases the staged metadata entries in the same commit; the objects
themselves are left to the committed GC, and reverting the drop commit brings the view back.

**What a replace accepts.** The view update language, not the table one: `assign-uuid`,
`upgrade-format-version`, `add-schema`, `set-location`, `set-properties`, `remove-properties`,
`add-view-version` and `set-current-view-version`. A requirement that does not hold is
`409 CommitFailedException`, a lost compare-and-swap is `409` as well, an update the current document
cannot accept (including an unsupported format version) is `400`, and anything that fails after the new
document is written is `500 CommitStateUnknownException`. View format version 2 does not exist yet;
version 1 is the only one served.

**What is not served.** `POST /v1/{prefix}/views/rename` (`RenameView`) and
`POST …/namespaces/{namespace}/register-view` stay out of scope: neither is advertised, and a client
that calls one gets the catalog's `404`. A view *can* be created under the same name as a table in the
same namespace — they live in different directories of the reserved area and are addressed by different
endpoints — and a dropped table or view never removes the other.

**Namespaces and views.** A namespace that still holds a view is not empty: `DELETE
/v1/{prefix}/namespaces/{namespace}` answers `409 NamespaceNotEmptyException`, so a namespace drop can
never orphan a view pointer. Views of descendant namespaces count too, exactly as tables do.

**Clients.** PyIceberg 0.12 speaks the whole surface it implements — `create_view`, `load_view`,
`list_views`, `view_exists`, `drop_view` — and the system test
([`esti/iceberg_views_test.go`](../esti/iceberg_views_test.go)) runs that round trip against a real
`lakefs-sso` on S3, then replaces the view over HTTP (`assert-view-uuid` + `add-view-version` +
`set-current-view-version` + `set-properties`). Trino's `CREATE VIEW`/`DROP VIEW` use the same requests.
The **web UI is not required**: the repository Tables tab lists namespaces and tables, and views are
managed by an Iceberg client through the REST API. Reading them from the UI is not a prerequisite for
creating or replacing them, and no UI surface has to be present for a catalog client to use views.

## Access

Authorisation uses the existing `catalog:*` actions on branch-agnostic ARNs
(`arn:lakefs:catalog:::namespace/<repo>/<ns>` and `arn:lakefs:catalog:::table/<repo>/<ns>/<table>`), plus
two lakeFS actions on the repository and its reserved key area:

| Operation | Action | Resource |
|---|---|---|
| list namespaces under `repo.ref` | `catalog:ListNamespaces` | `CatalogNamespaceArn(repo, parent)` |
| get / HEAD namespace | `catalog:GetNamespace` | `CatalogNamespaceArn(repo, ns)` |
| create / update properties / drop namespace | `catalog:CreateNamespace`, `catalog:UpdateNamespace`, `catalog:DeleteNamespace` | `CatalogNamespaceArn(repo, ns)` |
| list tables (filtered per table with a `catalog:TableName` `StringLike` condition) | `catalog:ListTables` | `CatalogNamespaceArn(repo, ns)` |
| load / HEAD table | `catalog:ReadTable` | `CatalogTableArn(repo, ns, table)` |
| create / register / commit / metrics | `catalog:CreateTable`, `catalog:UpdateTable` | `CatalogTableArn(repo, ns, table)` |
| drop table | `catalog:DeleteTable` | `CatalogTableArn(repo, ns, table)` |
| data-plane read: remote signing, vended read credentials | `catalog:ReadTableData` | `CatalogTableArn(repo, ns, table)` |
| data-plane write: remote signing, vended write credentials | `catalog:WriteTableData` | `CatalogTableArn(repo, ns, table)` |
| list views (filtered per view with a `catalog:ViewName` `StringLike` condition) | `catalog:ListViews` | `CatalogNamespaceArn(repo, ns)` |
| load / HEAD view | `catalog:ReadView` | `CatalogViewArn(repo, ns, view)` |
| create / replace view | `catalog:CreateView`, `catalog:UpdateView` | `CatalogViewArn(repo, ns, view)` |
| drop view | `catalog:DeleteView` | `CatalogViewArn(repo, ns, view)` |

Additionally, every request scoped to a repository needs `fs:ReadRepository` on that repository, every
write needs `fs:WriteObject` on `ObjectArn(repo, "_lakefs_iceberg/*")` — the reserved entry area — and the
repository-level listing (no parent namespace) needs `fs:ListRepositories`. Branch protection, repository
read-only state and the datasets immutability deny therefore all apply to catalog writes without the
catalog implementing any of them itself.

`catalog:ReadTableData` and `catalog:WriteTableData` are **new actions** in this build. The built-in
policies already cover them: the `CatalogRead` policy set includes `catalog:ReadTableData`, and
`CatalogReadWrite` uses `catalog:*`. The base groups therefore need no change — `Viewers` is attached to
`CatalogReadAll` (which expands `CatalogRead`) and `Developers`, `SuperUsers` and `Admins` to
`CatalogReadWriteAll`.

> **Upgrading an existing installation.** A *custom* policy written before this upgrade does not have
> the two new actions. If such a policy is what grants a principal its catalog access, that principal
> can read and write table metadata but **not** table data: the data-plane check (`catalog:ReadTableData`
> on the table ARN) is what authorises remote signing and vended credentials, and a policy without the
> action is refused with `403 ForbiddenException` however permissive the rest of the policy is. Add
> `catalog:ReadTableData` (and, if the principal writes, `catalog:WriteTableData`) to the policy by hand,
> or attach the built-in `CatalogRead` / `CatalogReadWrite` policies instead. This is the same note the
> lakeFS Iceberg documentation carries for its own upgrade.

Authentication accepts an `Authorization: Bearer <lakefs-jwt>` header and HTTP Basic
(access key id + secret) from a catalog client, and the catalog serves `POST /v1/oauth/tokens` itself
(`client_credentials`, with the access key pair as form `client_id`/`client_secret` or as HTTP Basic) so
a client configured with an access key pair can obtain a JWT. A `401` carries no detail about *why* the
credential was rejected, and authentication runs before any request or body validation, so an
unauthenticated caller cannot learn whether its request was well-formed. Bodies larger than
`iceberg.max_request_body` are `413`.

**The web UI uses its session cookie for reads, and for four management operations.** A third scheme is
accepted for the browser rather than for a catalog client: the lakeFS login session cookie
(`internal_auth_session` — the same session, the same token key and the same `auth.UserByToken`
verification the core API's `cookie_auth` scheme uses), which is what lets a signed-in user's
browser explore and manage the catalog through the [Tables tab](#web-ui-the-tables-tab). It is
accepted for:

- `GET` and `HEAD`, unconditionally — the reads the tab makes (the namespace listing, the table
  listing, the table load) cannot change catalog state, and they are the reason the cookie scheme
  exists;
- exactly four state-changing operations, and only when the request carries an
  `X-Lakefs-Client` header whose value starts with `lakefs-webui` (the marker every web UI request
  already sends): `POST /v1/{prefix}/namespaces` (create namespace), `DELETE
  /v1/{prefix}/namespaces/{namespace}` (drop an empty namespace), `POST
  /v1/{prefix}/namespaces/{namespace}/tables` (create table) and `DELETE
  /v1/{prefix}/namespaces/{namespace}/tables/{table}` (drop table);
- nothing else. `POST`, `PUT`, `PATCH` and `DELETE` outside those four — the commit
  (`POST …/tables/{table}`), `register`, the properties update, `metrics`, remote signing,
  `POST /v1/oauth/tokens`, and every other write — are `401` with the constant message
  `authentication required` when they carry only a cookie, whatever header they send. That includes
  the view writes: `POST …/namespaces/{ns}/views` and `DELETE …/namespaces/{ns}/views/{view}` are
  not in the four, so creating, replacing or dropping a view from a browser session needs an access
  key pair or a bearer token, never the UI cookie.

The management exception is closed on purpose. A cookie is the one credential a browser attaches
by itself, so a general cookie write would be a write any page the user visits could issue on their
behalf. Four things bound it:

- `X-Lakefs-Client` is a non-simple header, so a browser attaches it to a cross-origin request only
  after a CORS preflight, and this catalog answers no preflight and grants no cross-origin access;
  the login cookie is `SameSite=Lax` as well, so a cross-site page reaches neither. The header is
  not a secret and is not the security boundary — the preflight refusal and `SameSite` are — but it
  keeps the exception scoped to the UI: a catalog client, a script, or any other same-origin caller
  that does not claim to be the web UI gets `401` for a cookie write, as before;
- the four operations are named explicitly rather than "anything the tab happens to call", so
  committing a table's data, registering a table, renaming one and minting a credential stay
  bearer/basic-only. Those are the operations that change table contents or hand out access, and
  they are the larger surface a browser-supplied credential must not reach;
- `POST /v1/oauth/tokens` is served before that decision is taken (it is the authentication
  middleware's own handler, not a route), so a cookie cannot be exchanged for a token either —
  the token endpoint only ever reads `client_id`/`client_secret` or HTTP Basic;
- a bearer token or access keys take precedence on every method: the `Authorization` header is read
  first and the cookie is consulted only when the request carries neither;
- authorisation is unchanged — a cookie-authenticated request passes the same `catalog:*` checks
  (`pkg/icebergcatalog/authz.go`), so the cookie grants nothing a bearer token of the same user
  would not.

A session whose token does not verify is a rejected credential and not an anonymous request: `401`,
with the same detail-free message (the cause goes to the log). Catalog clients never send a cookie,
so no client loses an option it had.

Every mutating operation honours `Idempotency-Key`: the final response (including a stored `409`) is
replayed for the same key while the advertised `idempotency-key-lifetime` (30 minutes) has not elapsed.

## Web UI: the Tables tab

Views are **not** managed from the UI: the tab below lists namespaces and tables, and a view is created,
replaced and dropped by an Iceberg client through the REST API (see [Views](#views)). Nothing about views
depends on the tab — no UI surface has to exist for a client to use them, and a deployment without the tab
serves them identically.

A repository's **Tables** tab is an explorer and, for the four operations below, a manager of the
Iceberg tables on the ref the page is showing — a branch, a tag or a commit id, because
[reads work on any ref](#addressing). It is part of the web UI
(`webui/src/lib/api/iceberg.ts`, `webui/src/lib/components/repository/tablesExplorer.tsx`,
`tablesManagement.tsx`), it addresses the catalog through `/iceberg/api` only (never
`relative_to`), and it calls:

- the four read operations — `GET /v1/config`, the namespace listing, the table listing and the
  table load;
- four management operations, which are exactly the ones the catalog accepts the session cookie for
  ([above](#access)): `POST …/namespaces` (create a namespace), `DELETE …/namespaces/{namespace}`
  (drop an empty namespace), `POST …/namespaces/{namespace}/tables` (create a table) and `DELETE
  …/namespaces/{namespace}/tables/{table}` (drop a table).

It never sends `HEAD`, and it never commits, registers or renames a table: committing a table's data
stays an Iceberg client's job, and the catalog does not accept the cookie for it.

`GET /iceberg/api/v1/config` decides what the tab is, because that one answer separates the
deployments from each other:

| Answer | The tab shows |
|---|---|
| `200` | the explorer described below. |
| `501` | the Enterprise upsell it has always shown: `iceberg.enabled` is false. |
| `404` | the same upsell: `/iceberg/api` is not routed at all. |
| `403 ForbiddenException` | the permission state, naming the catalog permissions the read needs. |
| `401` | the tab's own sign-in state: a sentence saying the catalog session is not valid, and a plain link the reader chooses to follow. |

The `501` is the same answer whether the catalog itself gave it (`NotImplementedException`) or the
core placeholder `pkg/api/services.go` mounts at the same prefixes did (a plain `lakefs` build): the
tab keys on the status, not the body. A disabled catalog mounts its answer *without* the
authentication middleware, so the `501` arrives before anyone is authenticated. `404` is the same
situation one step further out — a proxy in front, or a deployment that serves the UI from another
host. Neither a `401` nor a `403` navigates: the catalog's answer is about this mount's credential or
this user's permissions, not about the reader's core lakeFS session, so both are shown in place and
the tab never turns one into a login redirect. A `403` is never softened into an empty listing: "you
may not read this" and "there is nothing here" are different facts, and the tab shows the empty
state only when the catalog answered a listing.

The explorer lists the namespaces of `<repo>␟<ref>` with `parent=<repo>␟<ref>`, and every node is
loaded lazily: expanding one fetches that node's child namespaces and its tables in one go, so a
page load costs one listing plus two per top-level namespace rather than a walk of the whole
warehouse. The top level is open by default, and a ref with no namespaces below it shows the empty
state instead ("create a namespace and then a table in it, or write to it with an Iceberg client").

For the selected table the tab renders what the load result carries: the format version and the
current snapshot's operation, the `metadata-location`, the table location, the table UUID and the
last-updated time; the current schema (field id, name, type — nested types printed as
`struct<…>` / `list<…>` / `map<…>` — required or optional, and the field doc); the default
partition spec (field, transform, and the source column resolved through the schema); the current
snapshot (id, operation, commit time, sequence number, manifest list and every summary counter);
the snapshot history, built from `snapshots` and newest first, so a tagged or expired snapshot
that `snapshot-log` no longer mentions still appears, with the current one marked; and the table
properties.

The tab does not page: it sends no `pageSize`, and a catalog answers an absent `pageSize` with one
page holding the whole listing, so nothing is hidden behind a next-page token.

### Managing tables from the tab

Four controls, each of which is one of the four management operations the cookie is accepted for:

- **Create namespace** (the tab's header) opens a dialog for the levels below the ref
  (`inventory` or `inventory.books`; every parent level must exist already).
- **Create table** (on a namespace node) opens a dialog with a schema editor: a table name, columns
  with name, [type](https://iceberg.apache.org/spec/#primitive-types) (a primitive, or any type
  string the catalog's parser accepts) and required-or-optional, and an optional **identity
  partition** by one of those columns. The dialog creates the table in the namespace whose node it
  was opened from, so the address cannot drift from the tree on screen.
- **Drop table** (on a table row) and **Drop namespace** (on a namespace node) ask for confirmation
  first, the way the repositories and datasets pages confirm their destructive actions. The
  confirmations say what a drop does: the metadata pointer and the staged file entries go, the
  objects stay in the object store and are reclaimed by garbage collection, and the whole thing is a
  staged change that becomes visible to readers when the branch is committed.

Every management operation is an ordinary staged catalog write, so `409` (a taken name, a namespace
that is not empty), `404` (a missing namespace or table) and the branch-protection and read-only
refusals arrive as the catalog sends them, and the tab shows the catalog's own message:

- a **create** keeps its dialog open and renders the message in it, so the reader can correct the
  form and retry;
- a **drop** closes its confirmation and renders the message in the tab, above the tree.

A `401` becomes a sentence about the catalog session ("sign in again and retry"), in the create
dialog or in the tab; it is never a login redirect. A `403` also **disables** the management controls and says which
permissions the operations need (`catalog:CreateNamespace`, `catalog:DeleteNamespace`,
`catalog:CreateTable`, `catalog:DeleteTable`, on top of the reads' `catalog:ListNamespaces`,
`catalog:ListTables`, `catalog:ReadTable` and `fs:ReadRepository`): a user the catalog denied once
will be denied again, so the tab stops offering what cannot work.

## Data-plane access

Iceberg clients write data files themselves, so the catalog has to answer what credentials a client uses
for those reads and writes. This build serves all three modes, the last two gated by how the server is
set up:

| Mode | Status | How |
|---|---|---|
| Client's own storage credentials (**default**) | available | The client is configured with S3 credentials for the storage namespace, or writes through the lakeFS S3 gateway. The catalog returns `config` empty, so a client falls back to its own `FileIO`. |
| Remote signing | served when the blockstore can sign | `POST …/tables/{t}/sign` is mounted and advertised in `GET /v1/config` when the blockstore is S3 and its pre-signer is available (`pkg/icebergcatalog/sign.go`); the endpoint honours `X-Iceberg-Access-Delegation: remote-signing` and requires `catalog:ReadTableData` / `catalog:WriteTableData` on the table. |
| STS vended credentials | served when configured | `blockstore.s3.credentials_vending.{role_arn, session_duration, external_id, endpoint}` is parsed and validated (`session_duration` defaults to 1h and must be between 15m and 12h); `GET …/tables/{t}/credentials` (`pkg/icebergcatalog/creds.go`) is mounted and advertised when the blockstore is S3 and `role_arn` is set, and vends a read session scoped to the table prefix. |

A table load still returns an empty `config`: it carries no `storage-credentials` and does not set
`s3.remote-signing-enabled`, so a standard client is not pointed at either endpoint and keeps its own
`FileIO` unless it calls `/credentials` or `/sign` directly. **Both served modes are S3 only in this
build**: an Azure or GCS blockstore gets neither endpoint, and the client's own credentials is the mode
to use there (see [Not supported in OSS](#not-supported-in-oss)). The delegation header is always safe to
send: a client that asks for `vended-credentials` or `remote-signing` and does not receive
`storage-credentials` falls back to its own credentials, which is what makes advertising either
mechanism in the load response a non-breaking addition later.

## Garbage collection

Table files live inside the repository's storage namespace and are written by the *client*, so they are
not lakeFS entries until the catalog stages them. Two consequences and two controls:

1. **Staged entries.** The commit path registers one entry per new metadata, manifest-list, manifest
   and data file under `_lakefs_iceberg/files/<table-uuid>/…`, pointing at the physical object with a
   full address (step 5 of the commit, `pkg/icebergcatalog/files.go`). The GC therefore sees every
   file, `lakectl diff` shows file-level changes, and a merge carries them. `iceberg.max_staged_files`
   (default 50 000) caps one commit's staging: a larger commit is refused with `400` before any entry
   is written, instead of being registered partially.

   Two other writes use the same mechanism:

   - **A create stages the initial `metadata.json`.** Before the pointer is staged,
     `Store.CreateTableWithMetadata` registers the table's first metadata file as a
     `_lakefs_iceberg/files/<table-uuid>/metadata/…` entry, so a table that is created and never
     appended to still has a referenced metadata file and survives a committed-GC sweep. (An earlier
     build staged only the pointer, whose metadata file no retained commit referenced.)
   - **A drop releases the table's file entries.** The drop commit stages the removal of the pointer
     *and* of every `_lakefs_iceberg/files/<table-uuid>/…` entry the table's commits staged
     (`releaseTableFiles` in `pkg/icebergcatalog/files.go`), in one future lakeFS commit, so the
     committed GC can reclaim a dropped table's objects instead of protecting them for as long as the
     branch keeps the earlier commits. The objects themselves are not deleted — they are shared by every
     ref that still names them — only the entries that keep them GC-live are removed.
2. **An uncommitted-GC grace window**, `iceberg.uncommitted_gc_grace` (default 24h). The uncommitted GC
   lists staged entries, so a staged table file is collectable exactly like any other uncommitted
   lakeFS write. The grace keeps an entry under `_iceberg/` out of the report while it is younger than
   the window. Setting it to `0` protects every `_iceberg/` object regardless of age — the escape hatch
   for installations that keep staged branches for weeks, at the cost of never collecting an orphan.

The residual window, stated plainly, because a grace period is a mitigation and not a proof:

- **Before the catalog stages anything, a client-written file is invisible to the uncommitted GC — and
  therefore unprotected by it.** The window is `[client writes the object → the catalog stages it]`.
  During it, a *committed*-GC sweep can delete objects that no retained commit references. Nothing the
  server can do protects an object it has not registered, so the established lakeFS rule applies
  unchanged: do not run GC while writers are active.
- **The grace is bounded in time.** A branch holding staged table-file entries and left uncommitted for
  longer than the grace can lose those data files; the pointer would then reference missing objects,
  because the commit path deliberately does not HEAD every file. Commit the branch, or raise the grace
  above your commit cadence.
- **Half of the control is outside the server.** Deletion is decided by the external GC job, which has
  its own minimum age (`lakefs.debug.gc.uncommitted_min_age_seconds`). An object is deleted only if the
  prepare step lists it *and* the job considers it old enough, so the effective protection is the larger
  of the two windows: keep `iceberg.uncommitted_gc_grace` at or above that setting for the server-side
  knob to be the binding one.
- **Clock skew never deletes.** A file whose recorded modification time is in the future is protected,
  so a client with a fast clock delays collection instead of causing it.

The predicate (`icebergcatalog.UncommittedGCProtected`) is installed into `pkg/catalog`'s
uncommitted-prepare step at startup (`catalog.SetUncommittedGCProtector`, called from
`icebergcatalog.Bootstrap`), so a protected object never reaches the report the GC job deletes from.
The same bootstrap creates and attaches the installation-wide deny policy
(`iceberg-reserved-prefix-protect`) that reserves `_lakefs_iceberg/` against `fs:WriteObject` and
`fs:DeleteObject`, for the same non-service groups the datasets pin deny covers. Both controls exist
only on a `lakefs-sso` server with `iceberg.enabled`; a plain `lakefs` server installs no protector and
keeps its unchanged GC defaults.

**The deny needs an auth backend that can store policies.** The reserved-prefix deny is a stored policy,
so it only exists where the auth service can create and attach policies. In the **default** auth mode
(`lakefs-sso` and `lakefs` both run `auth.BasicAuthService`, whose policy methods return
`auth.ErrNotImplemented`) the catalog now logs a warning and **continues** instead of refusing to start
— but the deny is **not attached** in that deployment, so `_lakefs_iceberg/` is not blocked at the `fs:`
layer and any principal holding `fs:WriteObject` on the repository can write the reserved area directly.
That is a real security gap for those deployments, not a cosmetic warning: run a policy-capable auth
backend (SSO / external auth) wherever the reserved-prefix deny has to be in force. A genuine error from
a policy-capable backend — anything other than `auth.ErrNotImplemented` — still fails startup.

## Merge

The table pointer is one entry, so two branches that change the same table conflict at merge — the
table's metadata is, in lakeFS terms, just a file: when both branches appended, the conflict is
reported, and nothing guesses a three-way merge of snapshots.

The catalog also does better for one case. A **snapshot that carries the
`lakefs.compaction.operation-id` snapshot property** is a background compaction — that property, set
with `snapshotProperty("lakefs.compaction.operation-id", …)` as the public lakeFS Iceberg documentation
instructs, is the marker. It is deliberately *not* the lakeFS commit message: an earlier rule keyed on
the message and so never saw the mark a real compaction writes. A conflict caused *only* by compaction
snapshots — every snapshot one side added on top of the merge base is a compaction snapshot, and the
base snapshot is still on that side's parent chain — is auto-resolved when
`iceberg.ignore_compaction_commits` is true (the default). The resolution keeps one side's pointer
unchanged and never rewrites metadata; a side whose base snapshot was expired or rewritten is not
resolved, because that is no longer "base plus a compaction" (`Store.ResolvePointerConflict` in
`pkg/icebergcatalog/commit.go`).

The decision is **wired into a real merge**, not only unit-tested: `icebergcatalog.Bootstrap` installs
it as the `pkg/catalog` merge-conflict hook (`catalog.SetMergeConflictResolver`, consulted by
`Catalog.Merge` after the ordinary strategy resolver) when both `iceberg.enabled` and
`iceberg.ignore_compaction_commits` are true, and clears the hook otherwise — it is a process-wide
pointer, so the documented opt-out must not leave one installed. The hook may return only one of the
two conflicting entries, never a third value, so it can never manufacture a table pointer. With the
flag off, a compaction conflict is reported as an ordinary one, exactly as before.

## Endpoints

What the catalog advertises in `GET /v1/config` is what it serves; an operation is added to the list
only when it is mounted and tested.

| Endpoint | Status |
|---|---|
| `GET /v1/config` | served |
| `GET`, `POST /v1/{prefix}/namespaces` | served |
| `GET`, `HEAD`, `DELETE /v1/{prefix}/namespaces/{namespace}` | served |
| `POST /v1/{prefix}/namespaces/{namespace}/properties` | served |
| `GET`, `POST /v1/{prefix}/namespaces/{namespace}/tables` | served |
| `GET`, `HEAD`, `POST`, `DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}` | served |
| `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics` | served (accepted and discarded) |
| `POST /v1/{prefix}/namespaces/{namespace}/register` | served (adopts a client-written `metadata.json`) |
| `GET …/tables/{table}/credentials`, `POST …/tables/{table}/sign` | served and advertised only when supported: S3 blockstore for signing, S3 plus `credentials_vending.role_arn` for vending |
| `GET`, `POST /v1/{prefix}/namespaces/{namespace}/views` | served (list and create) |
| `GET`, `HEAD`, `POST`, `DELETE /v1/{prefix}/namespaces/{namespace}/views/{view}` | served (load, exists, replace, drop) |
| `tables/rename`, `views/rename`, `register-view`, `transactions/commit`, `plan`, `tasks` | out of scope for v1, never advertised |

`POST /v1/oauth/tokens` is served as well. It is the catalog's authentication endpoint rather than a
`/v1/{prefix}` operation, so it is not part of the advertised list.

Errors use the protocol's envelope, `{"error": {"message": …, "type": …, "code": …}}`, with the
`NoSuchNamespaceException`, `NoSuchTableException`, `NoSuchViewException`, `AlreadyExistsException`,
`NamespaceAlreadyExistsException`, `NamespaceNotEmptyException`, `CommitFailedException`,
`CommitStateUnknownException`, `ForbiddenException`, `NotAuthorizedException`,
`RequestTooLargeException`, `NotImplementedException` and `NoSuchWarehouseException` types. Status codes
are chosen for the exception class the client raises: `409` means "refresh and retry", `500`/`502`/`504`
mean "the outcome is unknown", and `404` on a table is surfaced by PyIceberg as `NoSuchTableError`
whatever the endpoint.

## Pagination and consistency

`pageToken` / `pageSize` are honoured on both listings and `next-page-token` is returned; the token is
opaque to clients. Table load supports the `snapshots` query parameter (`all`, the default, or `refs`,
which keeps only the snapshots a branch or tag points at). A table load sets an `ETag` derived from the
metadata location and the `snapshots` mode, and answers `304 Not Modified` with no body when
`If-None-Match` matches (the tag is set on the `304` too). A client that sends no `If-None-Match`
receives the full response; `HEAD` sets no tag.

## Not supported in OSS

A few capabilities of the lakeFS Enterprise Iceberg catalog are deliberately **not** implemented in
this open-source build. They are listed here because their absence is a decision, not a gap to work
around silently — where a request could otherwise look like it was honoured, the catalog refuses it.

### Multi-tenancy

This catalog is **single-tenant**. The tenant a request belongs to is the lakeFS installation it is
served by, and there is no per-request tenant scope. A request that carries an
`X-LakeFS-Tenant` header is therefore **not** silently treated as a request for the default tenant:
that would let a client believe it was scoped to a tenant while it was served as another one. Any
non-empty value is answered with `501 NotImplementedException`, in the Iceberg error envelope, with
the message `multi-tenancy is not supported by this catalog`:

```json
{"error": {"message": "multi-tenancy is not supported by this catalog",
           "type": "NotImplementedException", "code": 501}}
```

The check runs in the catalog middleware **before authentication**, so the answer does not depend on
— or reveal — whether the caller's credential is valid. An absent header, or one whose value is
empty, is unchanged: the request is authenticated and served exactly as before. The refusal applies
to both addressing forms and to every endpoint, including `POST /v1/oauth/tokens`; a disabled
catalog answers the tenant header the same way rather than with its "disabled" message.

### Azure and GCS credential vending

[Vended credentials](#data-plane-access) are **S3 only** in OSS, for both the feature gate and the
session policy: the catalog vends only when the blockstore is S3 and
`blockstore.s3.credentials_vending.role_arn` is set, and the session policy it writes is an AWS S3
policy scoped to the table's prefix. An Azure or GCS deployment has no credential vending — its
`GET …/tables/{table}/credentials` endpoint is not mounted or advertised, and a request that reaches
the handler is refused with `501 NotImplementedException`. Nothing is vended for a table whose own
location is not an S3 URI either.

Use one of these instead:

- **The client's own storage credentials (the default).** Configure the client's `FileIO` with
  credentials for the storage namespace (Azure or GCS), exactly as for any non-catalog table. The
  catalog returns an empty `config`, so the client keeps its own credentials anyway.
- **Remote signing**, where the deployment supports it. `POST …/tables/{t}/sign` lets the server
  sign the client's data requests with the server's own credentials, so no long-lived client
  credential is needed. In this OSS build remote signing is likewise **S3 only**: the endpoint is
  mounted only for an S3 blockstore with a working pre-signer, and an Azure or GCS blockstore gets
  no signing endpoint. An S3 deployment that is asked to sign an `abfs://` or `gs://` URI refuses
  it with `400 BadRequestException` naming the scheme, before the pre-signer is called.

### Tables Cleanup

The Enterprise **Tables Cleanup** maintenance job is not part of this build. The open-source
counterpart is the `lakectl` command
[`lakectl iceberg cleanup`](iceberg-cleanup.md): it expires snapshots and marks and sweeps orphan
files natively in Go, stages its changes as ordinary lakeFS entries, and publishes them with a
`lakectl commit` — so a cleanup is reviewable with `lakectl diff`, atomic per branch, and revertible
like any other change. See [Iceberg tables cleanup](iceberg-cleanup.md) for the full command,
including `--dry-run`, `--mark-only` and `--sweep-only`.

### Views

Views **are** served in OSS: the six operations of the REST spec — list, load, exists, create,
replace and drop — are mounted and advertised, with the same staged-commit, GC and RBAC rules as
tables (see [Views](#views)). What is not served is `POST /v1/{prefix}/views/rename` and
`POST …/namespaces/{namespace}/register-view`: neither is implemented or advertised, and a client
that calls one gets the catalog's `404`. Multi-table transaction commit (`POST
/v1/transactions/commit`) is likewise out of scope.

## Current limitations

- **Table load does not advertise the data-plane mechanisms.** `/credentials` and `/sign` are mounted
  and advertised in `GET /v1/config`, but the `LoadTableResult` still carries an empty `config`, with
  no `storage-credentials` and no `s3.remote-signing-enabled`. A standard client therefore keeps its own
  `FileIO` unless it calls an endpoint directly (see [Data-plane access](#data-plane-access)).
- **The Tables tab does not commit.** It creates and drops namespaces and tables (the four operations the
  cookie is accepted for), and everything else is still an Iceberg client's job: no commit, no register,
  no rename, no data write. It also explores one ref at a time — the one the page shows — with no
  repository or ref picker, and it uses the `/iceberg/api` form only. The permissions it needs are
  `catalog:ListNamespaces`, `catalog:ListTables`, `catalog:ReadTable` and `fs:ReadRepository` to look,
  plus `catalog:CreateNamespace`, `catalog:DeleteNamespace`, `catalog:CreateTable` and
  `catalog:DeleteTable` to manage.
- **The esti system tests run on S3, and the view suite runs with them.** `esti/iceberg_test.go`
  covers the PyIceberg/DuckDB read path, append → lakeFS commit → GC prepare → still readable, and
  branch/merge including the conflict; `esti/iceberg_views_test.go` covers the PyIceberg view round
  trip, the staged metadata entries, a replace over HTTP, the any-ref read of a tag, the
  namespace-emptiness rule and the drop. Both were run against SeaweedFS (`blockstore.type: s3`, path
  style) with the `esti-iceberg-client` image, and all six tests pass; the exact command and output are
  in the commit that added the view suite. The **iceberg-go external conformance suite is green** — 18
  of 18 subtests, 0 skipped — through the `relative_to` URI against a `lakefs-sso` server built from
  this tree; that run is recorded in `pkg/icebergcatalog/conformance/RESULTS.md`. Apache RCK was not
  run (no JDK/Gradle on the host), and S3 presigning, credential vending and remote signing remain
  unvalidated.
- **Iceberg format v3, functions, server-side scan planning, view rename and multi-table transactions**
  are out of scope for v1 and are never advertised. `add-encryption-key` (format v3 only) decodes and is
  refused with a `400` naming the reason rather than failing as a server error. Views themselves are
  served (see [Views](#views)).

### Known design divergences

- **`iceberg.prefix` is accepted but must equal `lakefs`.** It is a constant of the catalog rather than a
  configurable namespace, and any other value fails startup instead of being silently ignored.

## Configuration

```yaml
iceberg:
  enabled: false                    # true mounts /iceberg/api and /iceberg/relative_to
  prefix: lakefs                    # the only accepted value
  token_ttl: 1h                     # lifetime of a client-credentials token
  max_request_body: 8388608         # bytes; larger bodies are 413
  uncommitted_gc_grace: 24h         # 0 protects every _iceberg/ object regardless of age
  max_staged_files: 50000           # cap on the table files one commit may stage
  ignore_compaction_commits: true    # auto-resolve compaction-only conflicts at merge (see "Merge")

blockstore:
  s3:
    credentials_vending:            # parsed and validated; mounted and advertised when role_arn is set
      role_arn: arn:aws:iam::123456789012:role/lakefs-iceberg
      session_duration: 1h          # between 15m and 12h
      external_id: ""               # optional
      endpoint: ""                  # optional, for S3-compatible STS (MinIO, LocalStack)
```

Vended credentials are S3-only. A `credentials_vending` section without `role_arn` is a startup error, so
a half-written block cannot silently do nothing.

## Clean-room note

The catalog is implemented from the Apache Iceberg specifications and the public lakeFS documentation
only: the [REST OpenAPI spec](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml),
the [REST protocol notes](https://github.com/apache/iceberg/blob/main/docs/docs/rest-protocol.md), the
[table spec](https://iceberg.apache.org/spec/), the
[client catalog properties](https://github.com/apache/iceberg/blob/main/docs/docs/rest-catalog.md) and
[docs.lakefs.io/iceberg](https://docs.lakefs.io/iceberg/). No code, SQL, protobuf or UI asset was taken
from lakeFS Enterprise or from upstream lakeFS >= 1.87 (BUSL 1.1); the fork's `LICENSE` is still
Apache 2.0 and the rules are recorded in [`../DATASETS_DESIGN.md`](../DATASETS_DESIGN.md) §2 and
[`../ICEBERG_CATALOG_PLAN.md`](../ICEBERG_CATALOG_PLAN.md) §2.
