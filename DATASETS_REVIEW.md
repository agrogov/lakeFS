# Datasets — Code, Security & Docs-Parity Review

Scope: every commit from `d9177c307` (foundation) to `423222b58` on `feature/datasets`: 138 files, about 48k lines.
Target behaviour:
- **https://docs.lakefs.io/datasets/**;
- the RBAC table on **https://docs.lakefs.io/security/rbac/**;
- the public Enterprise OpenAPI, **https://docs.lakefs.io/reference/api/enterprise/v1.108/swagger.yml** ("ent-swagger"
  below).

Runtime context: `LAKEFS_AUTH_UI_CONFIG_RBAC=internal`, policy RBAC through `lakefs-sso` (`pkg/sso/auth_builder.go:124`).

This document is written for a coding agent. Each finding has an ID, severity, exact `file:line` at HEAD
`423222b58`, the problem, and **what to change**, with a test to add. Line numbers drift as you edit, so re-grep the
quoted symbol before you edit.

Verification legend: **C** = code path traced and re-checked; **P** = plausible, traced but not reproduced.

## Baseline (all green, so findings are not caught by CI today)

| Check | Result |
|---|---|
| `go build ./pkg/datasets/... ./cmd/...`, `go vet ./pkg/datasets/...` | OK |
| `go test -count=1 ./pkg/datasets/...` | ok (30 s) |
| `go test -count=1 ./cmd/lakectl-datasets/...` | ok |
| `go tool golangci-lint run ./pkg/datasets/...` | 0 issues |
| `cd webui && npx vitest run src/pages/datasets src/lib/components/datasets src/lib/api` | 20 files / 165 tests pass |
| `cd webui && npx eslint src` | clean |
| `cd webui && npx tsc --noEmit` | 277 errors repo-wide (not run in CI); see UI-9 |

## Suggested fix order

1. **P0, security:** SEC-1, SEC-2, SEC-3. Data exfiltration and GC data loss are possible today under `internal` RBAC.
2. **P0, data integrity:** BUG-1 (CosmosDB), BUG-2, BUG-3, BUG-4.
3. **P1, docs parity, API-breaking:** do DOC-1 to DOC-9 as **one** API revision (spec, then `make gen-api`, then
   handlers, UI types and CLI) so the API breaks once.
4. **P1:** SEC-4, SEC-5, BUG-5 to BUG-11, UI-1, UI-2, CLI-1, CLI-2.
5. **P2:** everything else.

Regeneration after any change to `api/datasets.yml`: `make gen-api`. Makefile:154-156 runs `go generate ./pkg/datasets`
(directive at `pkg/datasets/http.go:22`); then `make validate-api` (Makefile:253).
`webui/src/lib/api/datasets.ts` is **hand-written**, so update it by hand. After any change to
`pkg/permissions/actions.go`, run `make gen-code`.

---

# Part A — Security (RBAC `internal`)

How the model works:
- The datasets service writes with `graveler.WithForce(true)` and bypasses the caller's authorizer
  (`pkg/datasets/http.go:66-70`).
- Repo safety therefore rests on three things: the per-dataset deny `dataset-<name>-immutable`, the
  `datasets-pins-protect` deny, and the attach-time source check `CheckSources`.
- In policies, `*` matches across `/` (`pkg/auth/arn.go:87` `ArnMatch` → `wildcard.Match`).

### SEC-1 — High — C — Datasets bootstrap never runs in production; pin protection and groups don't exist
- **Where:**
  - `pkg/datasets/bootstrap.go:329` `func Bootstrap` has no non-test caller (`grep -rn "Bootstrap(" pkg cmd`).
  - `pkg/datasets/http.go:50-86` `RoutesBuilder` doesn't call it.
  - `pkg/sso/rbac_bootstrap.go` calls `setup.EnsureRBACBaseGroups` (`pkg/auth/setup/setup.go:209`) without
    `WithDatasetsEnabled` (default false, `setup.go:38`).
- **Impact:**
  1. Policy `datasets-pins-protect` never exists (`pkg/datasets/authz.go:545-565`). Any member of Developers
     (`fs:*`) can `DELETE /repositories/<src>/branches/_lakefs_ds_pin_<commit>`. GC then deletes source objects
     that published dataset versions reference by physical address, which silently corrupts every consumer.
  2. `reconciler.go` `ensureInstallationPolicies` → `attachDatasetPolicies` (`bootstrap.go:419`) fails with
     `ErrNotFound` on every pass, so the reconciler never gets far enough to repair pins.
  3. Squatting: `fs:CreateBranch` is not in `pinsProtectDeniedActions` (`authz.go:561-565`). Anyone can pre-create
     `_lakefs_ds_pin_<commit>` pointing elsewhere. `pins.go:617` `verifyPinBranch` then returns
     `ErrPinBranchConflict`, so every publish pinning that commit fails (DoS).
- **Fix:**
  - In `RoutesBuilder`, after `NewService` (`http.go:70`), call
    `datasets.Bootstrap(ctx, deps.AuthService, time.Now(), WithPinsProtectGroups(ImmutabilityGuardGroups(cfg.ImmutableGuardGroups)))`.
    It must be idempotent and run on every start; a failure should be fatal.
  - In `attachDatasetPolicies` (`bootstrap.go:~419`), tolerate `auth.ErrNotFound` for groups that don't exist in
    the active template (ACL vs RBAC). Log at debug.
  - Add `permissions.CreateBranchAction` to `pinsProtectDeniedActions` (`authz.go:561`).
  - Also see DOC-3: the built-in `Datasets*` policies must be created on fresh installs.
- **Tests:**
  - Esti on fresh KV in `internal` mode: policy `datasets-pins-protect` exists and is attached to Developers and
    Admins.
  - A Developers user gets 403 on DeleteBranch and on CreateBranch for `_lakefs_ds_pin_x`.
  - `Reconcile` returns nil on a fresh install.

### SEC-2 — High — C — Immutability guard only covers a fixed list of group names
- **Where:**
  - `pkg/datasets/authz.go:596-604` `immutabilityGuardBaseGroups` (Admins, Supers, Writers, Readers,
    SuperUsers, Developers, Viewers);
  - `authz.go:609` `ImmutabilityGuardGroups`;
  - `service.go:~1490` `ensureImmutabilityGuard`, which skips missing groups.
- **Problem:** under `internal` RBAC, write power also comes from custom groups, SSO-synced groups
  (`pkg/sso/group_sync.go`) and policies attached directly to users. None of these get the deny.
- **Exploit:** a user in a custom `data-eng` group with `FSFullAccess` can, on dataset repo `foo`:
  - upload to `foo/latest/...`;
  - move tag `v3`;
  - delete `latest`;
  - `SetRepositoryMetadata` to drop `lakefs.dataset`;
  - delete the repo.

  This breaks the "published versions are immutable" guarantee.
- **Fix (preferred):** enforce immutability in the authorizer instead of through group attachments.
  - Wrap the `auth.Service` handed to the core API (in `cmd/lakefs-sso/main.go` through the auth-service builder, or
    a decorator in `pkg/datasets`).
  - When `Authorize` is called with any action in `immutabilityDeniedActions` on a resource under
    `arn:lakefs:fs:::repository/<repo>…`, and `<repo>` is a dataset repo, deny unless the caller is the datasets
    service principal. Look up dataset repos in a cached name set refreshed from the datasets KV partition.
  - Then delete the per-group attachment machinery, or keep it only as defence in depth.
- **Fix (minimum):** attach the deny to **every** group, every user with direct policies, and newly created
  groups/users (hook group creation in `group_sync.go`). The reconciler enumerates all groups each pass.
- **Also** extend `immutabilityDeniedActions` (`authz.go:481-497`) with:
  - `permissions.ImportCancelAction`;
  - `pr:WritePullRequest` (`permissions.WritePullRequestAction`);
  - `catalog:Create*`, `catalog:Update*`, `catalog:Delete*` (see ICEBERG_CATALOG_PLAN.md §7.2).
- **Test:** a custom group with `FSFullAccess`, plus a user with a directly attached `fs:*` policy. Upload,
  CreateTag, DeleteBranch and SetRepositoryMetadata on a dataset repo each return 403.

### SEC-3 — High — C — Prefix source-read check ignores Deny on sub-paths, which allows exfiltration
- **Where:** `pkg/datasets/authz.go:329-331` `SourceObjectArn` builds `ObjectArn(repo, prefix+"*")` and authorizes
  that **literal** ARN (`authz.go:256` `CheckSources`, `:300` `SourceReadNode`).
- **Problem:** `ArnMatch(policyResource, requested)` treats the policy side as the pattern, so:
  - a Deny on `…/object/salaries/*` does not match the requested literal `…/object/*`, and the deny is never seen;
  - materialisation (`materialize.go`) then copies every object under the prefix with no per-object check.
- **Exploit:**
  1. User has `fs:*` on `*`, plus Deny `fs:ReadObject` on `arn:lakefs:fs:::repository/hr/object/salaries/*`.
  2. They create a dataset with item `{repository: hr, type: prefix, prefix: {path: ""}}`.
  3. `salaries/*` is materialised.
  4. They read it through the dataset repo, or grant it to anyone.
- **Fix (pick one, A preferred):**
  - **A.** In the materialiser's prefix iterator (`materialize.go` `openItem`), authorize `fs:ReadObject` on
    `ObjectArn(repo, path)` per object for the **caller**. Cache decisions per (policy set, path prefix up to the
    deepest policy wildcard). Fail the publish with a `SourceError` naming the first denied path.
  - **B.** Before accepting a prefix item, fetch the caller's effective policies
    (`auth.Service.ListEffectivePolicies`). Reject the item if any Deny statement for `fs:ReadObject` or
    `fs:ListObjects` has a resource that could match a path under `<repo>/object/<prefix>`. Treat overlap
    conservatively: the deny resource starts with the prefix, or the prefix starts with the deny resource's literal
    part before its first `*`.
  - In both cases, reject `*` and `?` in `object.path` and `prefix.path` in `validate.go`.
- **Test:** user `u` with `allow fs:* *` and `deny fs:ReadObject …/repository/src/object/secret/*`. Creating with
  prefix `""` and prefix `sec` must both return 403 naming the item. Add a unit test that `CheckSources` respects
  sub-path denies.

### SEC-4 — Medium — C — Source refs resolved before the authz check, so existence leaks
- **Where:** Resolve runs before CheckSources at:
  - `service.go:259→264` (Create);
  - `:579→583` (Validate);
  - `:670→675` (PublishVersion);
  - `proposals.go:227→234` (CreateProposal);
  - `proposals.go:362→366` (UpdateProposal).
- **Problem:** `resolve.go` errors reveal whether a repo, branch or tag exists, its storage ID, and its dataset
  status. Validate needs only `ReadDataset`, which every consumer has through `datasets-all-read`.
- **Fix:** before Resolve, authorize `fs:ReadRepository` on `RepoArn(src)` per distinct source repo. Map any
  resolver error for an unreadable repo to the same 403 CheckSources returns.
- **Test:** a user with only `ReadDataset` validates against an existing unreadable repo and against a nonexistent
  repo. Both responses are identical 403s.

### SEC-5 — Medium — P — No body-size limit, uncapped strings, unbounded list
- **Where:**
  - `pkg/datasets/http.go:127-145` `NewRouter`: no `http.MaxBytesReader`, and the validator (`:138`) parses the
    whole body **before** auth (`:140`).
  - `api/datasets.yml` has only two `maxLength`s.
  - `service.go:1127` `effectiveAmount` treats `amount<0` as "everything".
  - `List` (`service.go:409-476`) loads all records and versions.
- **Fix:**
  1. Middleware order: auth, then `requireUser`, then `MaxBytesReader` (4 MiB, config
     `datasets.max_request_body`), then validator.
  2. Add `maxLength` (names 63, description 4096, message 4096, title 256, path 1024), `maxItems` (data 10000,
     allowed_values 256) and `maxProperties` (metadata 128).
  3. `effectiveAmount`: clamp `<=0` to the default, and `>MaxListAmount` to max. Remove the `-1` = all rule, and
     update the comment and spec.
  4. Paginate `List` in KV (see BUG-14).
- **Test:** an unauthenticated 10 MiB POST returns 401 without reading the body. `amount=-1` returns ≤ the default
  page with `has_more`.

### SEC-6 — Low — C — Pre-existing managed-name policies are trusted blindly
- **Where:** `ensureConsumerPolicy` (`service.go:~1555`), `ensureImmutabilityGuard` (`:~1490`),
  `createDatasetPolicies` (`bootstrap.go:~395`) and `reconciler.go:~624` all treat `ErrAlreadyExists` as success.
- **Exploit:** someone with `auth:CreatePolicy` (but not datasets rights) pre-creates `dataset-foo-read` as
  `fs:* on *`, or `dataset-foo-immutable` as empty. Later grants on `foo` then leak everything, or the guard is a
  no-op.
- **Fix:** on every ensure, compare statements (reuse `statementsEqual`). Overwrite a mismatch with
  `UpdatePolicy` and log a warning. The reconciler does the same.
- **Test:** pre-create `dataset-foo-read` with `fs:*`, create `foo`, grant a user. The policy is rewritten to the
  managed statements.

### SEC-7 — Low — C — 404/403 oracle
- **Where:**
  - `proposals.go:306` loads before `authorizeProposalWrite` (`:315`);
  - `service.go:~1285` `checkNameAvailable` returns 409 for any existing repo name.
- **Fix:** authorize on `DatasetArn(name)` before loading. For a name clash, return a generic 409 "name unavailable"
  and never say whether a plain repo exists.

### SEC-8 — Low — C — Info leakage in errors
- **Where:**
  - validator before auth (`http.go:138`);
  - `service.go:888` stores raw `err.Error()` in `task.Error` (KV, block and physical addresses), readable by any
    `ReadDataset` holder.
- **Fix:** move the validator after `requireUser` (see SEC-5). Store a sanitised `{code, message}` in the task and log
  the full error server-side.

### SEC-9 — Low — C — SuperUsers would get `datasets:*`
- **Where:** `pkg/auth/setup/setup.go:~245` attaches `DatasetsFullAccess` to SuperUsers when datasets is enabled.
  That includes ManageAccess and DeleteDataset.
- **Fix:** follow the docs mapping in DOC-3: SuperUsers get `DatasetsReadWriteAll`, Admins get `DatasetsFullAccess`.

Checked OK:
- KV key construction (`kv.FormatPath` and `checkSegment`);
- target path traversal (`.`, `..` and `_dataset/` rejected);
- routes behind `GenericAuthMiddleware` and `requireUser`;
- proposal merge re-checks sources as the merger (`proposals.go:399`);
- task IDs from `crypto/rand` ULIDs;
- no XSS sinks in the UI;
- the CLI has no TLS skip and doesn't print secrets.

Not covered: CSRF on cookie-authenticated datasets routes (inherited from core), regex-size DoS in
`metadata_validate.go`, metrics label cardinality.

---

# Part B — Docs parity (docs.lakefs.io/datasets + RBAC + ent-swagger v1.108)

The definition format and data-plane addressing (`lakefs://` and `s3://<ds>/<latest|vN>/…`) match. **The control-plane
API and the RBAC names do not**, so clients and policies written from the official docs fail. The design took the
plural `datasets:` service deliberately (`authz.go:28-30`, RBAC_DESIGN.md §5.4), but requirement #3 (behaviour as in
the docs) overrides that.

Recommended approach: bump the API in one change and treat ent-swagger as the compatibility target. If backwards
compatibility for existing clients of this fork matters, keep the old paths as deprecated aliases for one release.

### DOC-1 — High — C — RBAC service name `datasets` → `dataset`
- **Docs:** "Dataset operations use the `dataset:*` action namespace with resource ARN
  `arn:lakefs:dataset:::dataset/{datasetId}`."
- **Ours:** `pkg/datasets/authz.go:30` `datasetsArnPrefix = "arn:lakefs:datasets:::"`, actions `"datasets:…"` at
  `authz.go:43-61`, and `pkg/permissions/actions.go:111` registers `"datasets"`.
- **Fix:**
  - prefix → `arn:lakefs:dataset:::`, actions → `dataset:<Name>`;
  - add `"dataset": {}` to `serviceSet` (`actions.go:~111`), keeping `"datasets"` only as a deprecated alias so old
    policies still validate;
  - `make gen-code`;
  - update `pkg/auth/base.go:102-115`, `docs/datasets.md:204-207`, the spec descriptions (`api/datasets.yml:56,177,605`)
    and the webui action constants (`webui/src/lib/components/datasets/hooks.ts`);
  - add a reconciler/startup migration that rewrites managed policies (`dataset-*-read`, `-immutable`,
    `datasets-all-read`, `datasets-pins-protect`) to the new names.

### DOC-2 — High — C — Action set differs from the docs table
- **Docs (security/rbac):** List/Read/Create/Update/DeleteDataset and List/Read/Create/Update/DeleteMetadataKey.
  Metadata-key actions use resource `arn:lakefs:dataset:::dataset/*`.
- **Ours (`authz.go:42-61`):** `PublishVersion`, `CreateProposal`, `ManageAccess` and `ManageMetadataKeys`. No
  `UpdateDataset` and no per-op metadata-key actions. `MetadataKeyArn` is `…metadata-key/<key>` (`authz.go:66-70`).
- **Fix:**
  - `PublishVersion` → `UpdateDataset` (publish and PR merge).
  - `CreateProposal` → `UpdateDataset`. The docs don't mention a separate PR action. Alternatively keep it as an
    extension, but then `DatasetsReadWriteAll` must still allow PRs.
  - `ManageMetadataKeys` → the 5 doc actions, authorized on `arn:lakefs:dataset:::dataset/*` (drop
    `MetadataKeyArn`).
  - `ManageAccess` is an extension the docs don't cover. Keep it as `dataset:ManageAccess` and note it in
    `docs/datasets.md`.
  - Update every `authorize*` call in `service.go`, `proposals.go` and `handlers.go`, and the UI `useCan` constants.

### DOC-3 — High — C — Built-in policies incomplete and off by default
- **Docs:** "New installations (>= v1.89.0) include them automatically":
  - `DatasetsFullAccess` (`dataset:*`);
  - `DatasetsReadWriteAll` (List, Read, Create, Update, DeleteDataset, ListMetadataKeys, ReadMetadataKey);
  - `DatasetsReadAll` (ListDatasets, ReadDataset, ListMetadataKeys, ReadMetadataKey);
  - `DatasetsManageMetadataKeys` (all 5 key actions).

  All on resource `arn:lakefs:dataset:::dataset/*`.
- **Ours:** `pkg/auth/setup/setup.go:24-27` defines only FullAccess and ReadAll, with `datasets:List*`/`Read*` on `*`
  (`:175-189`), and `defaultDatasetsEnabled=false` (`:38`).
- **Fix:**
  - Define all 4 policies verbatim as above.
  - Create them whenever datasets is configured: plumb `WithDatasetsEnabled(cfg.Enabled)` from
    `pkg/sso/rbac_bootstrap.go`, and bump the RBAC bootstrap version so existing installs get them.
  - Group mapping: Admins → FullAccess, SuperUsers and Developers → ReadWriteAll, Viewers → ReadAll.
  - Update `docs/datasets.md:279-283`.

### DOC-4 — High — C — `PUT /datasets/{dataset}` (update = publish) is missing
- **Docs:** "Update Dataset | dataset:UpdateDataset | PUT /datasets/{dataset}"; "Every create or update produces the
  next version".
- **ent-swagger:** `put /datasets/{dataset}` takes body `DatasetUpdate{definition (req), publish{message (req)}}` and
  returns 200 `Dataset`, or 400/401/404/409.
- **Ours:** only `POST /{dataset}/versions` (`api/datasets.yml:355-425`), returning 201 `Version` or 202 `Task`.
  Handler at `handlers.go:378`.
- **Fix:** add `put` on `/{dataset}` (`operationId: updateDataset`, schema `DatasetUpdate`), and implement it in
  `handlers.go` by calling `svc.PublishVersion` synchronously and returning `Dataset`. Keep `POST /versions` and the
  async 202 as extensions.

### DOC-5 — High — C — `Dataset` schema field names and units
- **ent-swagger `Dataset`:**
  - required: `id`, `creation_date` (Unix **seconds**);
  - optional: `version` (int), `description`, `definition` (`DatasetDefinitionState`), whose items carry `status` ∈
    `ok|source_repo_unavailable|source_ref_unavailable|caller_unauthorized`.
- **Ours (`api/datasets.yml:1151-1180`):** `name`, `created_at` (**ms**), `created_by`, `latest_version`, `state`,
  `definition`, `stats`. Mapper at `handlers.go:579-598`; UI type at `webui/src/lib/api/datasets.ts:226`.
- **Fix:**
  - `name` → `id`;
  - `created_at` → `creation_date` (`.Unix()`);
  - `latest_version` → `version`;
  - add a per-item `status` computed from the resolver (degraded detection already exists in `pins.go`);
  - keep `created_by`, `state` and `stats` as optional extras;
  - update `datasets.ts`, every page reading these fields, and `cmd/lakectl-datasets/cmd/output.go`.

### DOC-6 — High — C — Version schema, plus the `GET /{dataset}/refs/{ref}` route that's missing
- **ent-swagger `DatasetVersion`:** `{id: "v<N>", message, author, creation_date (s)}`, and there is a
  `GET /datasets/{dataset}/refs/{ref}` route (`getDatasetByRef`) that returns `Dataset` at `latest` or `v<N>`.
- **Ours (`api/datasets.yml:1229-1258`):** `version` (int), `publisher`, `published_at` (ms), `commit_id`,
  `proposal_id`, `definition`, `stats`. No `/refs/{ref}` GET.
- **Fix:**
  - rename to `id` (string `v<N>`), `author`, `creation_date` (s);
  - keep the other fields as optional extras;
  - add `GET /{dataset}/refs/{ref}`;
  - update `datasets.ts:250` and `webui/src/pages/datasets/versions.tsx:~101` (`version.publisher`).

### DOC-7 — High — C — Diff response field names (path already matches)
- **ent-swagger `DatasetDiff`:** `{format_version?, description?: {left,right}, metadata[]: {type:
  added|removed|changed, key, left[], right[]}, data[]: {type, target, left: DataItem, right: DataItem}}`.
- **Ours (`api/datasets.yml:1273-1331`):** `items[]` instead of `data[]`, `change` instead of `type`, `from`/`to`
  instead of `left`/`right`. Mapper is `definitionDiffToAPI` (`handlers.go:466`).
- **Fix:** rename. Keep `?type=data` (DataDiffList) as an extension, since the default stays the definition diff.
  Update `datasets.ts:268-293`, `webui/src/pages/datasets/compare.tsx`, `definitionDiff.ts` and the CLI diff
  output.

### DOC-8 — High — C — Proposals vs. documented Pull Requests
- **ent-swagger:**
  - `GET /datasets/pulls` (global; `status` ∈ `open|closed|all`, default `all`; `dataset` filter);
  - `GET|POST /datasets/{dataset}/pulls`;
  - `GET|PATCH|DELETE /datasets/{dataset}/pulls/{pullRequest}`, where DELETE closes the PR and returns 204;
  - `GET …/pulls/{pullRequest}/diff`;
  - `POST …/merge`, with no body, returning **200** `{version, reference}` (409 when stale);
  - `DatasetPullRequest` fields: `id, dataset, title, description, author, status (open|closed|merged), base_version,
    latest_version, stale, conflicted, source_ref ("pr/<id>"), creation_date, closed_date`.
- **Docs UI:** "click **Open Pull Request** to create the dataset through a controlled review process", so PRs work
  for **new** datasets too.
- **Ours:**
  - `/{dataset}/proposals…` (`api/datasets.yml:504-671`), param `id`;
  - close is `PATCH {status: closed}` (`:1693-1695`);
  - merge returns 201 `Version` (`:652-657`, `handlers.go:573`);
  - the status enum is `open|closed|merged` (`:513-519`);
  - no global list and no PR diff;
  - create-via-PR is impossible: `proposals.go:218` requires an existing dataset, and the UI shows "Open proposal"
    only when `mode==='edit'` (`webui/src/pages/datasets/editor/index.tsx:445-450`).
- **Fix:**
  - rename the paths and schema as above;
  - add DELETE-close, `/pulls/{id}/diff` and the global `GET /pulls`;
  - merge returns 200 `{version, reference}`;
  - support PRs for a not-yet-existing dataset: store the PR under the requested name and set `conflicted=true` if the
    name is taken at merge, then create on merge;
  - compute `stale` (base_version ≠ latest) and `conflicted`;
  - `source_ref` = `pr/<id>`, and accept it as a ref (DOC-19);
  - add `pulls` to the reserved names (`pkg/datasets/definition.go:52-57`);
  - update `cmd/lakectl-datasets/cmd/proposals.go` (rename to `pulls`; status flag at `:75`) and
    `webui/src/pages/datasets/proposals/*` (labels "Pull Requests" / "Open Pull Request").

### DOC-9 — High — C — Metadata-key API: wrong paths, and all handlers return 501
- **Docs:** "Before creating datasets, an admin should define the metadata key schema." The RBAC table lists
  `GET|POST /datasets/metadata` and `GET|PUT|DELETE /datasets/metadata/{key}`.
- **Ours:**
  - paths are `/metadata-keys` (`api/datasets.yml:147-273`);
  - **all five handlers call `notImplemented`** (`pkg/datasets/handlers.go:331-349`), and so does `GetDatasetsConfig`
    (`:328`);
  - the UI settings page (`webui/src/pages/datasets/settings.tsx:~299`) and `lakectl datasets metadata-keys` hit
    those 501s.
- **Fix:**
  - rename to `/metadata` and `/metadata/{key}`, with `prefix` on list;
  - implement the handlers on the existing `metadata_keys.go` model with a KV store (add
    `PutMetadataKey/GetMetadataKey/ListMetadataKeys/DeleteMetadataKey` to `store.go` with CAS);
  - wire them into `metadata_validate.go` so create and publish enforce required keys;
  - reserved names `metadata-keys` → `metadata` (`definition.go:53-54`);
  - implement `GetDatasetsConfig` or drop it from the spec;
  - update `datasets.ts:649-671`, `cmd/lakectl-datasets/cmd/metadata_keys.go` and `docs/datasets.md:275-278`.

### DOC-10 — Med — C — Metadata-key update semantics and bounds
- **ent-swagger `DatasetMetadataKeyUpdate`:** requires `type` (full replace). `allowed_values` has minItems 1,
  maxItems 256, uniqueItems, and items of 1–256 chars.
- **Ours:** `MetadataKeyUpdateRequest` (`api/datasets.yml:1501-1515`) is partial, with no `type`, and type changes
  return 409. `allowed_values` (`:1491-1494`) is unbounded.
- **Fix:** make PUT a full replace with required `type`. If type changes stay forbidden, return 409 and document it.
  Add the bounds.

### DOC-11 — Med — C — Metadata value wire type
- **ent-swagger:** `metadata: additionalProperties: {type: array, items: string}`. The docs page example uses
  scalars, so the docs are inconsistent.
- **Ours:** `additionalProperties: {}` (`api/datasets.yml:1029-1035`), and numbers and booleans are emitted as JSON
  numbers/bools (`metadata_validate.go:200-215`).
- **Fix:** accept scalars and arrays on input, but **emit** `[]string` on output. Otherwise clients generated from
  ent-swagger fail to decode.

### DOC-12 — Med — C — `format_version` required by us, server-set in docs
- **ent-swagger:** "set by the server on write". Only `data` is required.
- **Ours:** `required: [format_version, data]`, with `data minItems: 1` (`api/datasets.yml:1042-1058`).
  `validate.go:142-146` rejects anything other than 1.
- **Fix:** drop `format_version` from `required`, and default it to 1 when it is 0 in `definitionFromAPI`
  (`handlers.go:601-603`). Drop `minItems` from the spec but keep the validator's non-empty check.

### DOC-13 — Med — C — `namespace` item is an object here, a string in ent-swagger
- **Where:** `api/datasets.yml:1110-1111,1146-1149` (`NamespaceSource` object) and `pkg/datasets/definition.go:109-112,125`
  (`NsSrc` struct).
- **Effect:** Enterprise definitions with `"namespace": "analytics"` fail with a decode 400 instead of the intended 501.
- **Fix:** make it `type: string` in the spec, and `Namespace *string` in Go.

### DOC-14 — Med — C — Create without `publish` makes no version
- **Docs:** "Every create or update produces the next version." ent-swagger `createDataset` returns 201 `Dataset`
  only.
- **Ours:** create without `publish` leaves the dataset empty, and create may return 202 `Task`
  (`api/datasets.yml:86-109`, `handlers.go:209-223`).
- **Fix:** always publish v1 on create (default message `"Create dataset <name>"`). Return 201 `Dataset`. Keep async
  only behind an explicit opt-in such as `?async=true`.

### DOC-15 — Med — C — Attach-time check stricter and on the wrong ARN
- **Docs:** "For objects and prefixes, this means fs:ListObjects + fs:ReadObject on the source path."
- **Ours (`authz.go:300-337`):**
  - also requires `fs:ReadCommit`;
  - checks `fs:ListObjects` against the **object** ARN, whereas core authorizes ListObjects on the **repository**
    ARN (`pkg/api/controller.go:3579`). A docs-style grant (`ListObjects` on `repository/src`) is therefore rejected.
- **Fix:** ListObjects on `permissions.RepoArn(src)`, ReadObject on the object ARN (and per object, SEC-3). Drop
  `ReadCommit`, or document it as an addition. Update `docs/datasets.md:208`.

### DOC-16 — Med — C — Extra `datasets.enabled` flag
- **Docs config:** only `datasets.storage_namespace_prefix`.
- **Ours:** `pkg/datasets/config.go:58` `Enabled` defaults to false, so every route returns 501 without it
  (`docs/datasets.md:16-19`).
- **Fix:** treat a non-empty `storage_namespace_prefix` as enabled when `enabled` is unset (`config.go:132-140,236`).

### DOC-17 — Low-Med — C — Authorization failure status
- **ent-swagger:** lists only 401 for denials.
- **Ours:** `handlers.go:93` always writes 403 and ignores `auth.authorization_failure_status`, which defaults to 401
  in core (`pkg/config/config.go:57`, `controller.go:6457-6459`).
- **Fix:** use the configured status, as `Controller.authorizationFailureStatus` does.

### DOC-18 — Low — C — Reserved names must track the docs' top-level routes
- `definition.go:52-57` reserves `config` and `metadata-keys`, but not `metadata` or `pulls`. Fixed by DOC-8 and
  DOC-9. Extensions such as `/validate`, `/access` and `/tasks` are fine.

### DOC-19 — Low — C — Ref patterns
- **Docs:** "Refs are `latest` or `v<N>`."
- **Ours (`api/datasets.yml:815,824,833`):** also accept `3`, `V3` and `proposal-<id>`.
- **Fix:** after DOC-8, use `pr/<id>` (ent-swagger `source_ref`) instead of `proposal-<id>`.

### DOC-20 — Low — C — Source ref types
- ent-swagger accepts only `"commit"`; we also accept `branch` and `tag` (`api/datasets.yml:1124`). This is an
  input superset, so compatible. Just document it in `docs/datasets.md:67`.

### DOC-21 — Low — C — UI labels
- **Docs:** "Create Dataset", "Open Pull Request", "Publish directly", "JSON mode", "History" tab.
- **Ours:**
  - `webui/src/pages/datasets/list.tsx:25` reads "Create dataset";
  - `editor/index.tsx:217` reads "Publish as v{N}", and `:448` reads "Open proposal";
  - `:356` reads "Raw JSON";
  - `datasetPageLayout.tsx:22` reads "Versions".
- **Fix:** rename to the doc labels.

### DOC-22 — Low — known gap — Iceberg `table` / `namespace` items return 501
- See **ICEBERG_CATALOG_PLAN.md**.

### DOC-23 — Low — C — Our docs claim no reference spec exists
- `docs/datasets.md:272-274` and `DATASETS_DESIGN.md` §1.2 ("we have no access to its spec") are wrong: ent-swagger
  v1.108 is public.
- **Fix:** name ent-swagger as the compatibility target and list the deliberate deviations (`/access`, `/validate`,
  `/tasks`, async publish, `ManageAccess`).

---

# Part C — Backend correctness

### BUG-1 — High — C — Every CAS write fails on CosmosDB
- **Where:** `pkg/datasets/store.go:574-587` `PutDatasetIf` passes re-encoded JSON as the `SetIf` predicate. The same
  pattern appears in `DeleteDataset` (`:603-626`), `AddPinReference`/`RemovePinReference`, and the lease in
  `reconciler.go`.
- **Problem:** CosmosDB treats the predicate as an ETag (`pkg/kv/cosmosdb/store.go:329`
  `azcore.ETag(valuePredicate.([]byte))`). Postgres, Dynamo, local and mem compare bytes, which is why tests pass.
- **Fix:** keep the `kv.ValueWithPredicate.Predicate` returned by `kv.Store.Get`. Change
  `GetDataset`/`GetPin`/`GetLease` to return `(record, predicate)`, and pass that predicate to `SetIf`/`Delete`.
  Remove the "re-encoding reproduces the stored value" comment (`store.go:574-576`).
- **Test:** a fake `kv.Store` whose `Get` returns an opaque predicate and whose `SetIf` compares only against it.
  `PutDatasetIf` after `GetDataset` succeeds.

### BUG-2 — High — C — A failure after the commit releases pins and wedges the dataset
- **Where:** `pkg/datasets/service.go:1354-1360`. When `PutVersion` fails after the commit and tag `vN` exist, the
  pins are released and the caller rolls the record back to `active` at N-1.
- **Impact:**
  - `latest` serves commit N with no GC pins, so data loss is possible;
  - the next publish chooses N again, which conflicts on tag `vN`, and every later publish fails;
  - the reconciler skips `active` records, so nothing repairs this.
- **Fix:**
  - After the commit exists, never release pins or roll back. Retry `PutVersion` and the record CAS with backoff. If
    that still fails, leave the record in `publishing(PendingVersion=N)` so `reconciler.completeVersion` finishes it
    from the tag.
  - In `beginPublish`, if tag `vN` already exists and points to a commit with `dataset.version=N` metadata, adopt it
    by running `completeVersion` instead of failing.
- **Test:** inject a one-shot `PutVersion` failure after the commit. Pins remain, the record reaches `LatestVersion=N`
  (after a reconcile if needed), and the next publish yields N+1.

### BUG-3 — High — P — A long publish is taken over while still running
- **Where:**
  - stale takeover in `beginPublish` (`service.go:729-738`);
  - the reconciler rollback releases N's pins (`reconciler.go:487`);
  - the owner never refreshes `UpdatedAt` (`service.go:778-804`, `845-897`).
- **Impact:** a publish running longer than `publish_timeout` (30m) gets rolled back or taken over, but the original
  goroutine still commits and tags `vN`. The result is two writers, an unpinned commit, and then BUG-2.
- **Fix:** heartbeat. While materialising, CAS-refresh `UpdatedAt` every `publish_timeout/3`. On CAS failure, cancel
  the publish context. Re-verify ownership with a CAS immediately before commit and tag.
- **Test:** fake clock plus a blocking materializer: advance past the timeout, run `Reconcile`, unblock. The original
  aborts without committing, and the surviving pins are intact.

### BUG-4 — High — C — Orphan-pin cleanup races dataset creation
- **Where:** `pkg/datasets/reconciler.go:762-785` `removeOrphanPins`. The `known` set comes from the dataset list
  taken at the start of the pass, and the branch and record are deleted with no re-check and no CAS.
- **Impact:** a dataset created and published between the list and the pin scan has its pins deleted, so GC
  protection is lost.
- **Fix:** before deleting, `GetDataset` every name in the pin's references, and skip if any exists. Delete the pin
  record with CAS on its predicate first, and delete the branch only after that succeeds. Also list pins
  **before** datasets.
- **Test:** a store hook that creates a dataset and adds a pin between `ListDatasets` and `listAllPins`. The pin
  survives.

### BUG-5 — Med-High — C — Pin release races
- **Where and problems:**
  1. `store.go:~1049` `RemovePinReference` and `pins.go:~823` delete unconditionally after a CAS to empty, so a
     concurrent `AddPinReference` is lost.
  2. `pins.go:656-665` `releaseSource` deletes the record and then the branch. A concurrent `Acquire` that got
     `ErrBranchExists` adds its reference, and the branch is then deleted underneath it.
  3. `pins.go:629-633` `verifyPinBranch` returns nil when the branch is missing.
  4. `EnsureDatasetPins` (`pins.go:302-311`) walks only existing pin records, so lost pins are never recreated
     (contrary to the comment at `store.go:1007-1010`).
- **Fix:**
  - delete with CAS on the empty-set value;
  - when the branch is missing in `verifyPinBranch`, recreate it and re-verify;
  - make reconciler ensure-pins derive the expected pins from every version record's definition and recreate
    missing records and branches.
- **Test:** concurrent `Acquire`/`Release` on the same commit with `-race` over many iterations. Afterwards, the
  record exists if and only if the branch exists. Delete a pin record, run `Reconcile`, and it is recreated.

### BUG-6 — Med — C — Reconciler: shutdown, lease renewal, unsafe release
- **Where:**
  - `reconciler.go:263,273`: `repair(context.WithoutCancel(ctx))`, so shutdown cannot stop a pass;
  - the lease (TTL 2×interval) is never renewed during a pass, so a second node starts repairing concurrently;
  - `reconciler.go:119-137` `ReleaseLease` does `Get` and then an unconditional `Delete`.
- **Fix:** pass `ctx` to `repair` (use `WithoutCancel` only for the final release). Renew the lease with CAS between
  datasets, and abort if renewal fails. Release with CAS (see BUG-1 for predicates).
- **Test:** cancel mid-pass and the pass returns promptly. A fake-clock lease expiry stops the pass.

### BUG-7 — Med — C — `finishDelete` deletes the dataset record first
- **Where:** `reconciler.go:593-609`.
- **Impact:** if a later delete of versions or proposals fails, nothing drives a retry. A re-created dataset with the
  same name then hits a stale `v1` record, which leads to BUG-2. Old proposals also leak into the new dataset.
- **Fix:** delete versions, proposals, tasks and grants first, and the dataset record **last**. In `Create`, refuse
  or clean up leftover version records for the name.
- **Test:** inject a `DeleteVersion` failure. The record stays `deleting`, and a later re-create publishes v1 cleanly.

### BUG-8 — Med — C — Proposal writes have no CAS; nothing repairs a failed merge
- **Where:** `store.go:790-803` `PutProposal` is a plain put, used at `proposals.go:346,380,463`.
- **Impact:** a merge racing a close or update ends with a final record that differs from what was published. If
  `PutProposal` fails after the merge publish, the proposal stays `open`, every retry returns 409 "rebase
  required", and the preview branch leaks.
- **Fix:** add `PutProposalIf` with a predicate and use it everywhere. In the reconciler, mark proposals merged when
  a commit's `CommitMetadataProposalKey` (already read at `reconciler.go:536`) names them, and delete their preview
  branches.
- **Test:** concurrent close and merge, exactly one wins. Inject a `PutProposal` failure after merge, and a reconcile
  pass marks the proposal merged.

### BUG-9 — Med — C — Delete during publish
- **Where:** `service.go:484-510` `Delete` has no state check.
- **Fix:** return 409 `ErrPublishInProgress` for a non-stale `publishing` record. The publish owner also checks for
  `deleting` before commit.

### BUG-10 — Med — C — A failed create stays `creating` for 30 minutes
- **Where:** `service.go:315-331`. A failure after `CreateRepository` (metadata, guard or initial publish) just
  returns.
- **Fix:** roll back inline: delete the repo, drop the guard, delete the record. This mirrors the existing
  `CreateRepository` failure path (`:305`). Leave only crash recovery to the reconciler.

### BUG-11 — Med — C — Tasks are never deleted or repaired
- **Where:** `store.go` has `PutTask`/`GetTask` only. `finishDelete` (`reconciler.go:573-619`) ignores tasks.
- **Fix:** add `ListTasks`/`DeleteTask`. Delete tasks in `finishDelete`. In `repairInterrupted`, set an interrupted
  publish's task to `failed` (or `completed` if `completeVersion` succeeded).

### BUG-12 — Low — C — Async publishes are not tied to server shutdown
- **Where:** `http.go:70` never calls `svc.SetBackgroundContext(ctx)`, so `bgCtx` is `context.Background()`
  (`service.go:185`, used at `:876`).
- **Fix:** call `svc.SetBackgroundContext(ctx)` in `RoutesBuilder`. Track in-flight publishes with a WaitGroup and
  drain it on shutdown.

### BUG-13 — Low — C — Injected clock bypassed
- **Where:** `pins.go:374,534`, `reconciler.go:82` and `service.go:779` use `time.Now()`.
- **Fix:** use `s.now()` / `r.svc.now()`. This is needed to test BUG-3 and BUG-6.

### BUG-14 — Low (perf) — C — Unbounded reads
- **Where:**
  - `List` (`service.go:409-476`) calls `GetVersion` and reads a definition for every dataset before paginating;
  - `ListVersions`, `ListProposals` and `ListGrants` load whole partitions;
  - `ListDatasetPins` (`pins.go:729-746`) costs O(datasets × pins) per pass.
- **Fix:** KV scan from `after` with a limit, enrich only the returned page, and build a pins-by-dataset index once
  per pass.

### BUG-15 — Low — P — Pin record can exceed DynamoDB's 400KB item limit
- **Where:** the pin `References` list (`store.go:964-999`) grows by one entry per version and is never compacted.
- **Fix:** store one KV record per `(commit, ds@vN)` under a prefix.

### BUG-16 — Low — C — Proposals created on datasets in any state
- **Where:** `CreateProposal` (`proposals.go:~218`) doesn't check `rec.State`.
- **Fix:** reject unless the state is `active` or `publishing`.

Design divergences to document, not necessarily fix:
- the materialiser ignores `merge_fan_in`;
- the metarange no-op check (`materialize.go:408`) can never fire;
- the reconciler doesn't detect commits on `latest` lacking `dataset.version` (§14).

---

# Part D — Web UI

The UI has no XSS sinks, every API path segment is `encodeURIComponent`-ed (`datasets.ts:157`), and the server
re-checks every permission.

### UI-1 — Med — C — `/repositories/<dataset>` never redirects to the dataset page
- **Where:** `webui/src/lib/components/repository/layout.jsx:60-67`.
- **Problem:**
  1. While `repo` or the dataset names are loading, `isDataset` is false, so the `<Outlet>` renders.
  2. The index route `<Navigate to="objects"/>` (`webui/src/pages/index.jsx:111`) fires.
  3. By the time the data arrives, the path is `/objects`, so `atRepositoryRoot` is false.
- **Fix:** render `<Loading/>` until both `repo` and `names` are loaded (expose a `loading` flag from
  `useDatasetNames`). Or move the decision into a dedicated index-route element.
- **Test:** a MemoryRouter at `/repositories/ds` with async mocks ends at `/datasets/ds/data`.

### UI-2 — Med — C — Repository list hides the paginator when a page is all datasets
- **Where:** `webui/src/pages/repositories/index.jsx:227-234` returns an alert before the `<Paginator>` (`:303`).
- **Fix:** always render the paginator. Better: keep fetching pages until there is something to show, or filter on
  the server.
- **Test:** an all-dataset page with `has_more:true` still shows "Next".

### UI-3 — Low — C — Dataset-name cache never cleared
- **Where:** `webui/src/lib/api/datasets.ts:695` `clearDatasetNamesCache` has no production caller.
- **Fix:** call it after a successful `datasets.create` and `datasets.delete`.

### UI-4 — Low — C — Partial grant failure leaves the table stale
- **Where:** `webui/src/pages/datasets/access.tsx:68-80`.
- **Fix:** use `Promise.allSettled`, always `refetch()`, and list the principals that failed.

### UI-5 — Low — C — Client permission evaluation diverges from the server
- **Where:** `webui/src/lib/components/datasets/hooks.ts:103,169`:
  - the glob is case-insensitive (`'i'`), unlike the server;
  - policy conditions are ignored;
  - only the first 100 policies are read.
- **Fix:** drop the `'i'` flag and paginate `listUserPolicies`. Longer term, ask the server for the decision.

### UI-6 — Low — P — More than 1000 versions: old `vN` silently shows the latest definition
- **Where:** `hooks.ts:195,239-249`.
- **Fix:** if `vN` is not in the list, fetch it with `getVersion`. Never fall back to the latest definition for an
  explicit `vN`.

### UI-7 — Low — C — Access tab shown to everyone
- **Where:** `webui/src/pages/datasets/datasetPageLayout.tsx:19-26`.
- **Fix:** show the tab only when `useCan(manage-access, datasetResource(id))` (design §11.2).

### UI-8 — Low — C — `navigate()` paths built unescaped
- **Where:** `editor/index.tsx:258,267,275`. Safe today because names are pattern-restricted.
- **Fix:** wrap the names in `encodeURIComponent` for consistency.

### UI-9 — Low — C — TypeScript errors in non-test dataset code
- **Where:**
  - `editor/editorModel.ts:356` (TS2322);
  - `editor/itemPickers.tsx:235-261` (`entries` typed `never`, which disables checking);
  - `editor/index.tsx:486` (TS2352);
  - `AlertError onDismiss` typing at `access.tsx:110`, `editor/index.tsx:434`, `proposals/proposal.tsx:254` and
    `settings.tsx:258,351`.
- **Fix:** type `useAPI` as `ObjectStats[]`, cast to `MetadataValue`, and fix the `AlertError` prop type. Add a
  `tsc --noEmit` scoped to `src/pages/datasets` and `src/lib/components/datasets` to `npm run lint`.

---

# Part E — `lakectl-datasets`

The docs define no `lakectl` dataset commands (checked `reference/cli/enterprise`), so §12.1 of the design is the
spec. The plugin reuses lakectl's config, credentials and TLS (`cmd/lakectl/cmd/hooks.go:30-38`).

### CLI-1 — Med — C — Answering "no" to delete prints an empty `Error:` and exits 1
- **Where:** `cmd/lakectl-datasets/cmd/datasets.go:837-846`. `lakectlcmd.Confirm` (`cmd/lakectl/cmd/common_helpers.go:405-415`)
  returns `promptui.ErrAbort` on "no" and never returns `(false, nil)`. It also prints the preamble to stdout,
  which mixes text into `-o json` output.
- **Fix:** treat `errors.Is(err, promptui.ErrAbort)` as a cancel: print "Delete cancelled." to stderr and exit
  non-zero. Print the preamble to `cmd.ErrOrStderr()` yourself.
- **Test:** an injected confirm returning `ErrAbort` sends no DELETE and prints a clear message.

### CLI-2 — Med — P — Retry client retries non-idempotent POSTs
- **Where:** `cmd/lakectl-datasets/cmd/root.go:92-100` uses `cmd/lakectl/cmd/retry_client.go:23-34,98-106`, which
  retries every method on 429, 500, 503 and transport errors.
- **Impact:** a lost publish response is retried and produces duplicate versions.
- **Fix:** retry only GET/HEAD for the datasets client. Also make `publish` send `expected_version` (fetched first)
  by default, so a retry gets 409.

### CLI-3 — Low — C — `publish --wait` has no timeout
- **Where:** `datasets.go:464-489`.
- **Fix:** add `--timeout` (default 30m) and treat unknown task statuses as errors.

### CLI-4 — Low — C — Version regex disagrees with the spec
- **Where:** `cmd/lakectl-datasets/cmd/definition.go:17,88` vs `api/datasets.yml:815`.
- **Fix:** copy the spec patterns per command, and fix the "commit id" wording. Revisit after DOC-19.

### CLI-5 — Low — C — Wrong error type
- **Where:** `metadata_keys.go:156` returns `invalidResponse` for an enum without `--allowed-values`.
- **Fix:** use `invalidArguments`.

### CLI-6 — Low — C — `--metadata k=a --metadata k=b` keeps only the last value
- **Where:** `root.go:192-200`.
- **Fix:** use `map[string][]string` with `query.Add`, or reject duplicates.

### CLI-7 — Low — P — Double escaping of `principal_id`
- **Where:** `access.go:222-227` via `pkg/datasets/apigen/datasets.gen.go:1892-1900` (oapi-codegen v1.5.6:
  `PathEscape` into `URL.Path`). `"a b"` becomes `a%2520b`.
- **Fix:** add an e2e test. If it fails, set `RawPath` or restrict `principal_id` with a pattern.

### CLI-8 — Low — C — `-o json` delete and revoke print nothing
- **Where:** `datasets.go:858-860`, `access.go:234-237`, `metadata_keys.go:298-300`.
- **Fix:** print `{"deleted":"<name>"}` (and similar).

### CLI-9 — Low — P — Endpoint path and global-flag order
- **Where:** `cmd/lakectl/cmd/hooks.go:37` `TrimSuffix("/api/v1")` breaks custom endpoint paths, and global flags
  before `datasets` aren't passed to the plugin.
- **Fix:** document the flag order in `--help`, and derive the base URL from the parsed server URL.

---

## Appendix — files most touched by the fixes

| File | Findings |
|---|---|
| `api/datasets.yml` (+ `make gen-api`) | DOC-4…14, DOC-19, SEC-5 |
| `pkg/datasets/authz.go` | SEC-1, SEC-2, SEC-3, DOC-1, DOC-2, DOC-15 |
| `pkg/datasets/http.go` | SEC-1, SEC-5, SEC-8, BUG-12 |
| `pkg/datasets/handlers.go` | DOC-4…9, DOC-12, DOC-14, DOC-17 |
| `pkg/datasets/service.go` | SEC-4, SEC-6, SEC-7, BUG-2, BUG-3, BUG-9, BUG-10, BUG-14 |
| `pkg/datasets/store.go` | BUG-1, BUG-5, BUG-8, BUG-11, BUG-15, DOC-9 |
| `pkg/datasets/reconciler.go` | BUG-4, BUG-6, BUG-7, BUG-8, BUG-11 |
| `pkg/datasets/pins.go` | BUG-5, BUG-13 |
| `pkg/datasets/proposals.go` | SEC-4, SEC-7, BUG-8, BUG-16, DOC-8 |
| `pkg/datasets/materialize.go` | SEC-3 |
| `pkg/datasets/bootstrap.go`, `pkg/auth/setup/setup.go`, `pkg/sso/rbac_bootstrap.go` | SEC-1, SEC-9, DOC-3 |
| `pkg/permissions/actions.go` (+ `make gen-code`) | DOC-1 |
| `webui/src/lib/api/datasets.ts` and `webui/src/pages/datasets/**` | DOC-5…9, DOC-21, UI-1…9 |
| `cmd/lakectl-datasets/cmd/*` | DOC-5…9, CLI-1…9 |
| `docs/datasets.md`, `DATASETS_DESIGN.md` | DOC-3, DOC-15, DOC-16, DOC-20, DOC-23 |
