# S-I4 — PyIceberg and DuckDB against a stub `/v1/config` and stub read endpoints

Task: **I0-4** (phase I-0 Spikes). Design doc: `ICEBERG_CATALOG_PLAN.md` §2.1 (endpoint negotiation), §11 I-0, §11 I-1.
Status: **done** — request logs captured for both clients, field requirements measured, endpoint-list semantics settled.
Date: 2026-11-02. Branch: `feature/datasets`.

**License guard** (`ICEBERG_CATALOG_PLAN.md` §2, `DATASETS_DESIGN.md` §2): everything below was measured against
throwaway local code written from the Apache Iceberg REST OpenAPI spec and the clients' public behaviour. No lakeFS
Enterprise code and no lakeFS upstream ≥ 1.87 (BSL) was read or copied. Client libraries inspected are Apache-2.0
(PyIceberg, DuckDB iceberg extension).

Spec of record: <https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml>
(clients' defaults cross-checked against <https://github.com/apache/iceberg/blob/main/docs/docs/rest-protocol.md>).

---

## 1. Verdict — the three questions this spike had to answer

| Question | Answer |
|---|---|
| Which requests do the clients issue for connect / list namespaces / load table? | **§3** (PyIceberg) and **§4** (DuckDB). Both: one `GET /v1/config?warehouse=…`, then `GET /v1/{prefix}/namespaces`, `GET /v1/{prefix}/namespaces/{ns}/tables`, `GET /v1/{prefix}/namespaces/{ns}/tables/{table}`. Multi-level namespaces arrive as `ns2%1Fsub` (percent-encoded `\x1F`). |
| Which response fields are required vs optional? | **§5**. Headline: **DuckDB hard-fails if `defaults` or `overrides` is missing** (even when empty) and if any value in them is not a JSON string. PyIceberg requires `namespaces` / `identifiers` in list responses; DuckDB silently treats their absence as an empty list. `metadata-location` and `config` are optional on **load-table** for both, but the **commit response** requires `metadata-location` *and* `metadata`. |
| Does a client reject an `endpoints` list that omits rename/transactions? | **No client rejects it, and neither fails to connect.** But they behave differently (**§6**): PyIceberg *gates* the call (raises `NotImplementedError`, sends **no** HTTP request); DuckDB **ignores** the list for rename/drop/create and issues the request anyway, and gates multi-table commit on it (client-side refusal, no partial writes). |

Two findings change the design doc and must be carried into I1-3:

1. **`{prefix}` in the advertised endpoint strings must stay a literal placeholder.** Advertising
   `GET /v1/lakefs/namespaces` instead of `GET /v1/{prefix}/namespaces` makes **every PyIceberg call fail** with
   `NotImplementedError` after a successful connect (§6.2). DuckDB tolerates the expanded form.
2. **The `endpoints` list is a hint, not an access-control boundary.** DuckDB calls unadvertised endpoints regardless
   (§6.1). Unimplemented endpoints must therefore still answer with a correct Iceberg error envelope; we cannot rely on
   advertising a short list to keep clients away from them.

---

## 2. Method (throwaway; nothing here is committed)

A ~390-line Python `http.server` stub (`stub.py`) on `127.0.0.1` served `GET /v1/config` plus just enough of the read
API to complete a session. Every request was logged as JSONL (method, path, headers, body). Clients were driven by small
Python scripts. All artefacts live in `/tmp/iceberg-spike/` and are deliberately **not** part of the repository; the
recipes in §10 rebuild them.

Environment: macOS arm64, Python 3.11.18, **PyIceberg 0.12.0**, **DuckDB 2.0.0** with iceberg extension build
`4c0c0874c4`, `pyarrow` 26.0.0 for PyIceberg `FileIO`.

The stub answered `GET /v1/config` with exactly this shape (this is the shape §5 validates):

```json
{
  "defaults": {"warehouse": "/tmp/iceberg-spike/warehouse"},
  "overrides": {"prefix": "lakefs"},
  "endpoints": [
    "GET /v1/{prefix}/namespaces",
    "POST /v1/{prefix}/namespaces",
    "GET /v1/{prefix}/namespaces/{namespace}",
    "HEAD /v1/{prefix}/namespaces/{namespace}",
    "DELETE /v1/{prefix}/namespaces/{namespace}",
    "POST /v1/{prefix}/namespaces/{namespace}/properties",
    "GET /v1/{prefix}/namespaces/{namespace}/tables",
    "POST /v1/{prefix}/namespaces/{namespace}/tables",
    "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "POST /v1/{prefix}/namespaces/{namespace}/register",
    "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics",
    "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials"
  ]
}
```

Fixture: namespaces `["ns1"]` and `["ns2","sub"]`; tables `ns1.tbl`, `ns1.tbl_b`, `ns2.sub.tbl2`; each load-table
response carried inline `metadata` (format-version 2, no snapshots, `current-snapshot-id: -1`), a `metadata-location`
and an empty `config`. A real `metadata.json` was also written to disk so clients that dereference `metadata-location`
could succeed.

Client entry points:

```python
# PyIceberg
load_catalog("lakefs", type="rest", uri="http://127.0.0.1:PORT", warehouse="spike-wh",
             token="spike-token", **{"oauth2-server-uri": ".../v1/oauth/tokens"})
# or ... credential="spikekey:spikesecret" instead of token
catalog.list_namespaces(); catalog.list_tables("ns1"); catalog.load_table("ns1.tbl")
```

```sql
-- DuckDB 2.0 (NOTE: ENDPOINT/URI is the base URL; DuckDB appends /v1 itself)
INSTALL iceberg; LOAD iceberg;
ATTACH 'spike-wh' AS catt (TYPE iceberg, URI 'http://127.0.0.1:PORT', TOKEN 'spike-token');
SELECT database_name, schema_name FROM duckdb_schemas();   -- list namespaces
SHOW ALL TABLES;                                          -- list namespaces + tables
DESCRIBE catt.ns1.tbl;                                    -- load table
```

---

## 3. PyIceberg 0.12.0 — request log

### 3.1 Connect + list namespaces + list tables + load table (bearer token)

Six requests, in this order, and nothing else (`table_exists` is one of the six; the last line is the multi-level
namespace case):

```
GET    /v1/config?warehouse=spike-wh
       User-Agent: PyIceberg/0.12.0   Accept: */*   Content-type: application/json
       Authorization: Bearer spike-token
       X-Iceberg-Access-Delegation: vended-credentials
GET    /v1/lakefs/namespaces
GET    /v1/lakefs/namespaces/ns1/tables
GET    /v1/lakefs/namespaces/ns1/tables/tbl
HEAD   /v1/lakefs/namespaces/ns1/tables/tbl          <- catalog.table_exists()
GET    /v1/lakefs/namespaces/ns2%1Fsub/tables/tbl2   <- multi-level namespace
```

Observations that matter for I1:

* **`?warehouse=` is sent on `/v1/config`** (from the client's `warehouse` property). The server must ignore or accept
  the query parameter; it is not a filter we need to implement.
* **Percent-escaping is uppercase-hex**: multi-level namespaces arrive as `ns2%1Fsub` (`%1F`, not `%1f`). The server
  must percent-decode before splitting on `\x1F` (§5.6 shows the failure when it does not).
* `table_exists` uses `HEAD …/tables/{table}` **only when the HEAD endpoint is advertised**; otherwise it falls back to
  a `GET` (§6.1).
* PyIceberg 0.12 sends **`X-Iceberg-Access-Delegation: vended-credentials` on every request by default**, even though
  none was requested explicitly. Our load-table response omitting `storage-credentials` is accepted silently (relevant
  to I4-1: the header will be present whether or not we implement vending).
* It sends `Content-type: application/json` even on bodyless `GET`s. Harmless, but don't reject it.
* **No pagination parameters** (`pageToken`/`pageSize`) were sent; `next-page-token` was never needed.

### 3.2 Connect with `credential` (OAuth) — the token endpoint is called **twice**

```
POST   /v1/oauth/tokens            (no Authorization header)
       Content-type: application/x-www-form-urlencoded
       grant_type=client_credentials&client_id=spikekey&client_secret=spikesecret&scope=catalog
GET    /v1/config?warehouse=spike-wh   (Authorization: Bearer …)
POST   /v1/oauth/tokens            (again, identical body)
GET    /v1/lakefs/namespaces
…
```

* Credentials go **in the form body**, not HTTP Basic.
* `scope=catalog` (PyIceberg's `CATALOG_SCOPE`).
* The **second** token request is issued right after `/v1/config`. The endpoint must be idempotent and must not
  rate-limit or single-use-lock a client at connect time (I1-1).

### 3.3 Commit (bonus capture, feeds I2-3)

A `set_properties` transaction produced:

```
GET    /v1/lakefs/namespaces/ns1/tables/tbl      <- refresh before commit
POST   /v1/lakefs/namespaces/ns1/tables/tbl
       {"identifier": {"namespace": ["ns1"], "name": "tbl"},
        "requirements": [{"type": "assert-table-uuid", "uuid": "d4a4dd29-…"}],
        "updates": [{"action": "set-properties", "updates": {"owner": "spike2"}}]}
```

**The commit body carries an `identifier` field in addition to `requirements` and `updates`.** `ICEBERG_CATALOG_PLAN.md`
§2.1 describes the commit body as `{requirements[], updates[]}`; PyIceberg 0.12 adds `identifier`. The decoder must
tolerate the extra field (Go's `encoding/json` ignores unknown fields by default, so this is free — but a strict decoder
or a hand-written validator would break).

---

## 4. DuckDB 2.0.0 — request log

### 4.1 Connect + list namespaces + load table (bearer token)

DuckDB issues far more requests, and re-fetches rather than caching:

```
GET  /v1/config?warehouse=spike-wh
     User-Agent: duckdb/v2.0.0(osx_arm64) python/3.11 d54cbbe3df   Accept: */*
     Authorization: Bearer spike-token
GET  /v1/lakefs/namespaces                 <- duckdb_schemas()
GET  /v1/lakefs/namespaces                 <- duplicate
GET  /v1/lakefs/namespaces/ns1/tables
GET  /v1/lakefs/namespaces/ns2%1Fsub/tables
GET  /v1/lakefs/namespaces                 <- SHOW ALL TABLES re-lists
GET  /v1/lakefs/namespaces/ns1/tables
GET  /v1/lakefs/namespaces/ns2%1Fsub/tables
GET  /v1/lakefs/namespaces/ns1/tables/tbl  <- DESCRIBE / SELECT …
GET  /v1/lakefs/namespaces/ns1/tables/tbl     (one GET per statement; no cache)
GET  /v1/lakefs/namespaces/ns2%1Fsub/tables/tbl2
```

Observations:

* **`URI` (formerly `ENDPOINT`) is the base URL, without `/v1`.** Passing `…:PORT/v1` makes DuckDB request
  `/v1/v1/config` and fail at attach. When lakeFS mounts the catalog at `/iceberg/api`, the DuckDB `URI` must be
  `https://host/iceberg/api` and DuckDB will call `/iceberg/api/v1/config`. Note that DuckDB 2.0 deprecates the
  `endpoint` attach option in favour of `uri` (*"The Iceberg attach option 'endpoint' is deprecated; use 'uri'
  instead"*), so any lakeFS docs we write should say `URI`, while accepting `ENDPOINT` still works.
* Same `?warehouse=` query parameter, same `ns2%1Fsub` encoding, `Authorization: Bearer`.
* **DuckDB does send `X-Iceberg-Access-Delegation: vended-credentials` — but only on table-scoped endpoints.**
  Measured presence: `GET …/tables`, `GET …/tables/{table}`, `POST …/tables`, `POST …/tables/{table}`,
  `POST …/transactions/commit`. Absent on `/v1/config`, `/namespaces`, `/namespaces/{ns}`, `HEAD …/namespaces/{ns}`,
  `…/views`, and `POST /tables/rename`. PyIceberg sends it on *every* request. So both target clients do ask for vended
  credentials on the read path, and the loader must simply ignore the header when it does not vend (I4-1) rather than
  reject the request.
* Multi-level namespaces are surfaced as a *single* schema name with a dot: `catt."ns2.sub".tbl2`. That is a DuckDB
  naming convention, not something the catalog sends.
* DuckDB re-issues `GET …/tables/{table}` for every statement that touches the table (4 GETs for
  describe+select+snapshots+describe). Relevant to I1-9/I5-2: response caching or an ETag that clients actually use is
  worth little for DuckDB.
* Empty namespace/table list responses are treated as an empty list, not an error (§5.2, §5.3).

### 4.2 Connect with `CLIENT_ID`/`CLIENT_SECRET` (OAuth) — HTTP Basic, different scope

```
POST /v1/oauth/tokens
     Authorization: Basic c3Bpa2VrZXk6c3Bpa2VzZWNyZXQ=      (base64 "spikekey:spikesecret")
     Content-type: application/x-www-form-urlencoded
     grant_type=client_credentials&scope=PRINCIPAL_ROLE%3AALL
GET  /v1/config?warehouse=spike-wh   (Bearer)
```

* **DuckDB sends the credentials as HTTP Basic**, where PyIceberg puts them in the body. `POST /v1/oauth/tokens` must
  accept **both** forms (I1-1).
* **`scope=PRINCIPAL_ROLE:ALL`** (URL-encoded as `PRINCIPAL_ROLE%3AALL`) instead of `scope=catalog`. lakeFS must accept
  and ignore an unrecognised scope rather than 400.
* DuckDB does **not** repeat the token request after `/v1/config` the way PyIceberg does.
* `AUTHORIZATION_TYPE` defaults to `oauth2` when `CLIENT_ID`/`CLIENT_SECRET` are present; without an
  `OAUTH2_SERVER_URI` (or `URI` to fall back on) `ATTACH` fails **before any HTTP call**: *"AUTHORIZATION_TYPE is
  'oauth2', yet no 'oauth2_server_uri' was provided"*. Documentation for the DuckDB integration must mention
  `OAUTH2_SERVER_URI`.

### 4.3 Write paths DuckDB exercises (bonus capture, feeds I2-2/I2-3)

Read scopes do not include these, but they were captured because they constrain the endpoint list (see §6.1):

```
-- CREATE SCHEMA
HEAD /v1/lakefs/namespaces/fresh                 <- existence check uses HEAD when advertised, else GET
POST /v1/lakefs/namespaces          {"namespace": ["fresh"], "properties": {}}

-- CREATE TABLE: staged create, then a normal single-table commit (no transactions/commit)
GET  /v1/lakefs/namespaces/ns1/tables/newtbl
HEAD /v1/lakefs/namespaces/ns1
POST /v1/lakefs/namespaces/ns1/tables
     {"stage-create": true, "name": "newtbl", "schema": {…}, "partition-spec": {…},
      "write-order": {…}, "properties": {"format-version": "2", …}}
POST /v1/lakefs/namespaces/ns1/tables/newtbl
     {"requirements": [{"type": "assert-create"}],
      "updates": [{"action": "assign-uuid", …}, {"action": "upgrade-format-version", "format-version": 2},
                  {"action": "add-schema", …}, {"action": "set-current-schema", …}, {"action": "add-spec", …},
                  {"action": "set-default-spec", …}, {"action": "add-sort-order", …},
                  {"action": "set-default-sort-order", …}, {"action": "set-location", …},
                  {"action": "set-properties", …}],
      "identifier": {"namespace": ["ns1"], "name": "newtbl"}}

-- RENAME (note: issued even when not advertised)
POST /v1/lakefs/tables/rename
     {"source": {"namespace": ["ns1"], "name": "tbl"},
      "destination": {"namespace": ["ns1"], "name": "tbl_renamed"}}

-- DROP (note the query parameter)
DELETE /v1/lakefs/namespaces/ns1/tables/tbl?purgeRequested=false
```

* **DuckDB creates tables with `stage-create: true` and then commits the create through the ordinary
  `POST …/tables/{table}` endpoint.** Single-table writes therefore need **no** `POST /v1/{prefix}/transactions/commit`
  — which is what makes it safe for v1 to leave transactions unadvertised (§6.1).
* `DELETE …/tables/{table}` arrives with **`?purgeRequested=false`** — another query parameter to ignore.
* `assert-create` is the requirement used for a staged create; `set-location` appears even when the client supplied a
  location. Both are already in the plan's update/requirement inventory (§4.2, I0-1).

Multi-table transaction (only reachable when `transactions/commit` is advertised):

```
POST /v1/lakefs/transactions/commit
     {"table-changes": [
        {"requirements": [{"type": "assert-table-uuid", …}, {"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": null}],
         "updates": [{"action": "add-snapshot", …}, {"action": "set-snapshot-ref", …}],
         "identifier": {"namespace": ["ns1"], "name": "tbl"}},
        {… second table …}]}
```

---

## 5. Required vs optional response fields (measured, not inferred)

Legend: **required** = the client hard-fails without it; **optional** = the client proceeded; *note* columns quote the
client's own error text where relevant.

### 5.1 `GET /v1/config`

| Field | PyIceberg 0.12 | DuckDB 2.0 |
|---|---|---|
| `defaults` | optional (defaults to `{}`) | **required**, even if `{}` — *"CatalogConfig required property 'defaults' is missing"* |
| `overrides` | optional | **required**, even if `{}` — *"CatalogConfig required property 'overrides' is missing"* |
| values inside `defaults`/`overrides` | any JSON scalar tolerated | **must be JSON strings** — *"CatalogConfig property 'tmp' is not of type 'string', found JSON type 3 instead"* |
| `overrides.prefix` | optional; when absent the client omits the path segment (`/v1/namespaces`) | optional; same behaviour |
| `endpoints` | optional; when absent the client assumes its **12-endpoint** default set (§6.3) | optional; when absent DuckDB simply makes no view calls |
| `idempotency-key-lifetime` | not sent by the stub, not needed | not needed |

**Consequence for I1-2/I1-3:** always emit **both** `defaults` and `overrides` objects, even when empty, and keep every
value a JSON string (no ints, no bools, no nested objects). This is the single cheapest way to keep DuckDB working.

### 5.2 `GET /v1/{prefix}/namespaces`

| Field | PyIceberg | DuckDB |
|---|---|---|
| `namespaces` (array of string arrays) | **required** — pydantic `ValidationError: namespaces Field required` | optional; missing ⇒ treated as empty list |
| `next-page-token` | optional; never requested in this fixture | optional |

### 5.3 `GET /v1/{prefix}/namespaces/{ns}/tables`

| Field | PyIceberg | DuckDB |
|---|---|---|
| `identifiers[].namespace`, `identifiers[].name` | **required** — `ValidationError: identifiers Field required` | optional; missing ⇒ empty list |
| `next-page-token` | optional | optional |

### 5.4 `GET /v1/{prefix}/namespaces/{ns}/tables/{table}` (load table)

| Field | PyIceberg | DuckDB |
|---|---|---|
| `metadata` | **required** (inline table metadata) | **required** |
| `metadata-location` | **optional** — with `metadata` only, `load_table()` succeeds (`metadata_location` is `None`), and a subsequent commit still issues its `POST` | **optional** — `DESCRIBE`/`SELECT` worked from inline metadata alone |
| `config` | optional (defaults to `{}`) | optional |
| `storage-credentials` | optional; its absence is fine even though the client sent `X-Iceberg-Access-Delegation: vended-credentials` | optional; same — DuckDB sends the delegation header on table endpoints but works without `storage-credentials` |

### 5.4b Response to `POST /v1/{prefix}/namespaces/{ns}/tables/{table}` (commit)

The **response** to a commit is stricter than the load response: `CommitTableResponse` requires **both** `metadata`
and `metadata-location`. Omitting `metadata-location` produced
`ValidationError: 1 validation error for CommitTableResponse / metadata-location Field required` — i.e. the commit had
succeeded server-side while the client raised. I2-3 must always return both.

### 5.5 Error envelope `{"error": {"message", "type", "code"}}`

Both clients read **`message`** for the user-facing text; the exception **class is chosen from the HTTP status** (plus a
per-call override table), not from `type`. PyIceberg renders the message as `"{type}: {message}"`, and its
`ErrorResponse` model requires **all three** of `message`, `type`, `code` — a partial envelope degrades to
*"Received unexpected JSON Payload"*.

| Status | PyIceberg 0.12 raises | DuckDB 2.0 raises |
|---|---|---|
| 400 | `BadRequestError` | `HTTPException` ("BadRequest_400 … message") |
| 401 | `UnauthorizedError` — **and retries once** (observed: 2 identical requests) | `HTTPException` |
| 403 | `ForbiddenError` | `HTTPException` |
| 404 | `NoSuchTableError` | `CatalogException` ("Table … does not exist") |
| 409 | `RESTError` | `HTTPException` ("Conflict_409") |
| 413 | `RESTError` | `HTTPException` ("PayloadTooLarge_413") |
| 500 | `ServerError` | `HTTPException` |
| 501 | `NotImplementedError` | `HTTPException` ("NotImplemented_501") |

A **plain-text** error body still produces the right exception class, but PyIceberg warns
*"Received unexpected JSON Payload: … Invalid JSON"* and DuckDB shows only the raw body. So the JSON envelope is
effectively required for a usable message — implement it as designed (§7.1), and note that **404 is mapped to
`NoSuchTableError` by PyIceberg even for non-table endpoints** (e.g. a 404 on rename surfaced as `NoSuchTableError`).
Pick the status code for its client-side *class*, not only its HTTP meaning.

Statuses not exercised above, read from the Apache-2.0 client source (PyIceberg 0.12
`catalog/rest/response.py::_handle_non_200_response`): 419 → `AuthorizationExpiredError` (also retried), 422 →
`RESTError`, 429 → `TooManyRequestsError`, 503 → `ServiceUnavailableError`, anything else ≥ 500 → `ServerError`.
Per-endpoint overrides that matter to us: create table `409` → `TableAlreadyExistsError`; create namespace `409` →
`NamespaceAlreadyExistsError`, `404` → `NoSuchNamespaceError`; drop namespace `409` → `NamespaceNotEmptyError`; **commit
`409` → `CommitFailedException`, `500`/`502`/`504` → `CommitStateUnknownException`** (matching §2.1 of the plan; verified
in source, not exercised in this spike).

Because PyIceberg **retries a 401 once**, a 401 must be cheap and side-effect free (no lock, no rate-limit accounting
that would turn the retry into a 429).

### 5.6 Namespace decoding is a hard requirement

The stub initially split the raw path and every multi-level-namespace call 404'd:
`NoSuchTableException: table tbl2 not found` for `ns2.sub.tbl2`. The path segment is `ns2%1Fsub`; it must be
percent-decoded **before** splitting on `\x1F`. Both clients exercised this path (each was driven with a two-level
namespace), so I1-8/I1-9 must decode first, and the identifier test-matrix needs a percent-encoded case.

---

## 6. `endpoints` list semantics — does a client reject an incomplete list?

Short answer: **no client rejects the list, none refuses to connect, and none validates it against its own capability
set.** But the two clients honour it very differently.

### 6.1 Which operations are gated on the list (measured by removing endpoints)

Three lists were used: the 15 endpoints in §2 (no `tables/rename`, no `transactions/commit`), a
**read-only** list (only `GET`/`HEAD` entries), and a per-endpoint removal (`DELETE …/tables/{table}` dropped).

| Operation | PyIceberg 0.12 | DuckDB 2.0 |
|---|---|---|
| connect (`/v1/config`) | not gated | not gated |
| list namespaces / list tables / load table | works | works |
| `HEAD` table exists | `HEAD` when advertised, else **falls back to `GET`** (one extra request observed) | **never sends `HEAD` on a table** — always `GET …/tables/{table}` |
| `HEAD` namespace exists | `HEAD` when advertised, else falls back to `GET` | `HEAD` when advertised, else `GET` (verified on create/drop paths) |
| rename table | **gated** — `NotImplementedError: Server does not support endpoint: POST /v1/{prefix}/tables/rename`, zero HTTP | **NOT gated** — `POST /v1/{prefix}/tables/rename` sent with a read-only list |
| drop table (`DELETE`) | **gated** — `NotImplementedError: … DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}` | **NOT gated** — `DELETE …?purgeRequested=false` sent with a read-only list |
| create namespace | **gated** — `NotImplementedError: … POST /v1/{prefix}/namespaces` | **NOT gated** — `POST /v1/{prefix}/namespaces` sent with a read-only list |
| create table / table update (commit) | **gated** — `NotImplementedError: … POST /v1/{prefix}/namespaces/{namespace}/tables` and `… POST /v1/{prefix}/namespaces/{namespace}/tables/{table}` | **NOT gated** |
| list views | gated (no request) | **gated** — `GET …/namespaces/{ns}/views` only when views are advertised |
| multi-table `transactions/commit` | no multi-table transaction API at all (0.12 has no `transactions/commit` endpoint string), so it never attempts one | **gated** — see §6.4 |
| unknown/unrecognised endpoint in the list | tolerated (added to the set, ignored) | tolerated |

The contrast is stark in one measurement: against a list containing **only** `GET`/`HEAD` entries, PyIceberg
issued exactly three requests (`/v1/config`, `/v1/{prefix}/namespaces`, load table) and refused `drop_table`,
`create_namespace` and `rename_table` client-side, while DuckDB against the *same* list issued
`DELETE …/tables/tbl?purgeRequested=false` and `POST …/namespaces` and only failed because the stub was permissive.

So the plan's §2.1 rationale needs a refinement: **an explicit list stops PyIceberg from calling `rename`; it does not
stop DuckDB.** Two consequences:

* I1-3's 501/404 handlers must return a valid Iceberg error envelope for endpoints we don't implement, because DuckDB
  will reach them. The list cannot be the enforcement mechanism.
* Authorisation must not depend on "the client wouldn't call that" (matches §7.2/§12 security tests): a DuckDB user can
  reach `tables/rename`, `DELETE`, and `POST …/tables` regardless of the advertised list.

### 6.2 The `{prefix}` placeholder is a literal — this is the sharpest edge in the whole negotiation

Advertising the endpoints with the prefix **expanded** (`GET /v1/lakefs/namespaces`) instead of the template
(`GET /v1/{prefix}/namespaces`) produced:

```
load_catalog                       OK   (supported endpoint count 15)
list_namespaces                    NotImplementedError: Server does not support endpoint: GET /v1/{prefix}/namespaces
list_tables                        NotImplementedError: Server does not support endpoint: GET /v1/{prefix}/namespaces/{namespace}/tables
load_table                         NotImplementedError: Server does not support endpoint: GET /v1/{prefix}/namespaces/{namespace}/tables/{table}
```

**Zero HTTP requests after `/v1/config`.** PyIceberg 0.12 parses each list entry with `Endpoint.from_string`
(`"METHOD path"`, exactly one space) and compares it to its `Capability` templates for membership; anything that is not
byte-identical to `/v1/{prefix}/…` silently matches nothing.

DuckDB, with the same expanded list, worked completely normally (it does not use the strings for lookup).

Therefore, for I1-3:

* Emit `"<METHOD> /v1/{prefix}/<path>"` with the literal `{prefix}` token, single space, no trailing slash.
* Do **not** template them from the configured prefix — even though the prefix is the constant `lakefs` (Q-I3), the
  strings must not contain it.
* Trap this in a test: assert the emitted list contains `"{prefix}"` and that no entry contains the configured prefix
  value.

### 6.3 Absent list ⇒ PyIceberg's default set is 12 endpoints, and it is **not** the "spec default"

With no `endpoints` key at all, PyIceberg installed 12 endpoints (measured: `len(_supported_endpoints) == 12`):
list/load/create/update/delete namespace, list/load/create/update/delete table, rename table, register table — **plus
view endpoints only if the server also sets `view-endpoints-supported`**.

Concretely, the absent-list default does **not** include `transactions/commit`, metrics, credentials, or the `HEAD`
existence endpoints. So:

* `ICEBERG_CATALOG_PLAN.md` §2.1's statement that an absent list means clients assume "namespaces CRUD + properties,
  tables CRUD, register, metrics, rename, transactions/commit" is **not what PyIceberg 0.12 does** — it assumes rename,
  but not transactions/commit and not metrics. (Behaviour is client-specific; DuckDB makes no such assumption.) The
  recommendation to always send an explicit list stands, but the *reason* is the reverse of the recorded one: an absent
  list exposes **rename** while sounding permissive, whereas an explicit list lets us exclude it for PyIceberg.
* The missing `HEAD` endpoints in the default set are why `table_exists` degraded to `GET` in the absent-list run —
  harmless, but advertising the two HEAD endpoints saves a request (and the plan's endpoint inventory already includes
  them).

### 6.4 Multi-table transactions: the list decides, and the refusal is clean

DuckDB, two `INSERT`s inside one `BEGIN … COMMIT`:

| Advertised | Result |
|---|---|
| `transactions/commit` **included** | txn succeeds; `POST /v1/{prefix}/transactions/commit` with `{"table-changes": [{requirements, updates, identifier}, …]}` |
| `transactions/commit` **omitted** | client-side refusal, **zero write requests**: `Iceberg REST Catalog cannot commit this transaction atomically because it would require multiple table commit requests without atomic multi-table commit support` |

It does **not** silently degrade to two separate single-table commits, and it does not leave partial state. That is the
behaviour we want for v1 (§1.2 non-goal, Q-I9): a DuckDB user who writes two tables in one transaction gets a clear
error instead of a half-applied transaction. A separate error exists for mixing updates with rename/drop in one
transaction (*"…mixes table updates with rename/drop requests"*), which is also client-side.

**Recommendation for I1-3** — advertise exactly the endpoints we implement and pass tests for, and do not advertise:

```
tables/rename  ·  transactions/commit  ·  views/*  ·  plan/tasks  ·  register-view
```

`metrics` is not a negotiation lever for these two clients: neither called
`POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics` in any measured flow (PyIceberg 0.12 has no
capability entry for it at all). Advertise it only once the accept-and-discard handler exists (§1.2 keeps it in scope
for I-2); leaving it out meanwhile is safe for both clients.

---

## 7. What each follow-on task should take from this

| Task | Action |
|---|---|
| **I1-2** (config) | Emit `defaults` **and** `overrides` objects always, values JSON strings only. Keep `prefix` a constant `lakefs`. Nothing else in the config response is required by either client. |
| **I1-3** (router + endpoints) | Endpoint strings must be the spec templates with a literal `{prefix}` (§6.2) — test it. Include both `HEAD` endpoints. Do not rely on the list for access control (§6.1); answer unadvertised endpoints with the envelope. Ignore `?warehouse=` and `?purgeRequested=`. Percent-decode namespace segments (§5.6). 404 ⇒ PyIceberg raises `NoSuchTableError`, so use status codes for the class you want. |
| **I1-1 / OAuth** | Accept credentials **both** in the form body (`client_id`/`client_secret`, PyIceberg) **and** as HTTP Basic (DuckDB); ignore `scope=catalog` and `scope=PRINCIPAL_ROLE:ALL`; tolerate the token endpoint being called twice in a row at connect time. |
| **I1-8 / I1-9** (namespaces, tables) | `namespaces` / `identifiers` are required for PyIceberg; DuckDB silently accepts `{}`, so the unit tests must not rely on DuckDB to catch a missing field. |
| **I2-2 / I2-3** (create, commit) | DuckDB uses `stage-create: true` + a normal table commit; the commit body carries an `identifier` field; `assert-create` guards a staged create; `set-location` arrives from the client. The **commit response** must return both `metadata` and `metadata-location` (§5.4b) or the client raises after a successful commit. The `transactions/commit` body is `{"table-changes":[…]}` if we ever implement it. |
| **I4-1** (delegation) | **Both** clients ask for vended credentials on the read path: PyIceberg sends
`X-Iceberg-Access-Delegation: vended-credentials` on every request; DuckDB sends it on table-scoped endpoints
(`…/tables`, `…/tables/{table}`, `…/tables/{table}` commit, `transactions/commit`). Absence of `storage-credentials` in
our response is accepted by both, so vending can land later — but the header must never cause a 400/501. |
| **Esti / system tests (§12)** | The DuckDB harness must pass `URI` without `/v1` and must not expect caching; the PyIceberg harness must assert at least one call succeeds **after** connect, which is what catches the `{prefix}` expansion bug. |

---

## 8. Corrections to the design doc (for D1 to fold in)

1. §2.1 "Endpoint negotiation": an absent list does **not** give PyIceberg transactions/commit or metrics; it gives 12
   endpoints including **rename**. Reword and keep the "send an explicit list" recommendation.
2. §2.1 "Prefix": add that `endpoints` entries use the **literal** `{prefix}` placeholder, while `overrides.prefix`
   holds the value `lakefs`; the two must not be conflated.
3. §2.1 "Commit": the body also carries `identifier` alongside `requirements`/`updates` (PyIceberg 0.12, DuckDB 2.0),
   and the commit **response** must contain `metadata` + `metadata-location`.
4. §2.1 "Errors": 404 is surfaced by PyIceberg as `NoSuchTableError` regardless of endpoint; 401 triggers one
   automatic retry.
5. §1.2 / I1-3: the `endpoints` list is advisory for DuckDB — security and 501 handling must not assume compliance.
6. §10 / DuckDB docs: `ENDPOINT`/`URI` must exclude `/v1`; `OAUTH2_SERVER_URI` is required when using
   `CLIENT_ID`/`CLIENT_SECRET`; DuckDB does request vended credentials, but only on table-scoped endpoints (§4.1).

## 9. Not covered by this spike

* Spark and Trino (their request patterns and `X-Iceberg-Access-Delegation` behaviour are unmeasured here; §12 esti
  still needs them).
* Pagination (`pageToken`, `next-page-token`) — neither client paginated against a 2-namespace / 3-table fixture.
* PyIceberg versions other than 0.12.0 and DuckDB versions other than 2.0.0. The `_supported_endpoints` gating model is
  recent PyIceberg behaviour (0.12 has it); older PyIceberg releases were not measured, so treat "PyIceberg will not
  call rename" as scoped to 0.12.x.
* Real object storage: the fixture used `file://` locations, and credentials vending (I-4) was not exercised.
* Iceberg format v3 and the `metadata-location`-relative (v4) path modes.

## 10. Reproduction

```bash
# throwaway, outside the repo
python3 -m venv /tmp/spike-venv
/tmp/spike-venv/bin/pip install pyiceberg duckdb pyarrow          # 0.12.0 / 2.0.0 / 26.0.0
printf 'INSTALL iceberg; LOAD iceberg;\n' | /tmp/spike-venv/bin/python -c \
  'import duckdb,sys; duckdb.connect().sql(sys.stdin.read())'

# stub.py + py_driver.py + duck_driver.py + capture.sh live in /tmp/iceberg-spike/
cd /tmp/iceberg-spike && ./capture.sh        # writes canonical/<scenario>.jsonl
/tmp/spike-venv/bin/python validate.py       # re-asserts every claim above from the logs; exits non-zero on drift
```

The stub's shape (config + read endpoints + logging) is ~390 lines and took well under a day; the scenario matrix is
what took the time. If a future spike needs a stub again, reuse this file layout rather than rebuilding it.

`validate.py` is the check used for this task's validation: it re-derives each claim from the captured JSONL (request
sequences, header presence per endpoint, gating behaviour, and a live re-run of the 8-status error sweep) and fails if
the note and the logs ever disagree. It currently passes 57/57 assertions.
