# Datasets for lakeFS OSS — Design Document

Status: **Draft for implementation** · Branch base: `feature/azure-entra-sso-apache` · Date: 2026-10-28

## Executive Summary

lakeFS Enterprise (Summer 2026 release) ships **Datasets**: named, versioned, read-only collections of data
curated from one or more repositories, each item pinned to a commit, with typed metadata, version diffs,
review-by-pull-request and dataset-level access control (https://docs.lakefs.io/datasets/). The OSS build only
contains a UI gate (`webui/src/pages/datasets/index.tsx` renders `DatasetsEnterpriseInfo`). No public open-source
reimplementation exists.

This document specifies a clean-room implementation for this fork, built from public documentation only.
The core idea:

> **A dataset is materialised as a read-only lakeFS repository with the same name.** Every dataset version is a
> commit on branch `latest` whose metarange contains entries that point (zero-copy, by full physical address) at
> the source objects. Each version is tagged `vN`. Control-plane state (definitions, versions, proposals, metadata
> keys) lives in the KV store under a dedicated partition.

Because a dataset *is* a repository, every existing read path works unchanged: `lakectl fs`, the S3 gateway
(`s3://customer-360/v3/customers/...`), presigned URLs, Spark/Hadoop FS, the Python/Java SDKs and the object
browser in the UI. All new code lives in a new package (`pkg/datasets`), wired through the existing
`lakefs-sso` wrapper binary, with a core diff of ~25 lines.

Scope of the MVP: `object` and `prefix` items. Iceberg `table` / `namespace` items are out of scope because OSS has no
Iceberg REST catalog (the routes return 501 via `api.MountNotImplementedServices`).

---

## 1. Goals and Non-Goals

### 1.1 Goals

| # | Goal |
|---|---|
| G1 | Create, list, read, version and delete datasets composed of `object` and `prefix` items from any repository. |
| G2 | Immutable, linear versions `v1..vN`; `latest` always points at the newest version. |
| G3 | Zero-copy: no object data is copied; datasets reference source objects by physical address. |
| G4 | Read datasets with every existing lakeFS client using `lakefs://<dataset>/<ref>/<path>` and `s3://<dataset>/<ref>/<path>`. |
| G5 | Version diff (definition + data). |
| G6 | Typed, validated metadata keys (string, enum, number, boolean, date; required; allowed values; multi-value). |
| G7 | Publish directly or through a dataset pull request (proposal → review → merge). |
| G8 | Dataset-level access: consumers need a grant only on the dataset, not on its sources. Creators must be able to read every source they attach. |
| G9 | Protect referenced source data from garbage collection. |
| G10 | Definition JSON format compatible with Enterprise (`format_version: 1`) so definitions are portable. |
| G11 | Upstream-merge-friendly: minimal, isolated changes to upstream files. |

### 1.2 Non-Goals (MVP)

- Iceberg `table` / `namespace` items and `catalog.<ds>.<ref>.<ns>.<table>` addressing (Phase 5, needs an Iceberg REST catalog).
- Wire compatibility with the Enterprise REST API beyond path shape and definition format (we have no access to its spec).
- Datasets spanning different storage IDs (multi-blockstore) — rejected at validation time.
- Version deletion / retention policies for dataset versions.
- Copy-on-publish ("snapshot") mode that physically copies data.

---

## 2. Licensing Constraints

- Upstream lakeFS moved from Apache 2.0 to **BUSL 1.1** on 2026-09-22 (`358544dd7`, starting 1.87.0). The Additional Use
  Grant allows production use only of the *unmodified* work, for internal use.
- This fork's `LICENSE` is still Apache 2.0; `master` was last synced 2026-08-16 (`4bb11638e`).
- Rules for this work:
  1. Clean room: no code, SQL, protobufs or UI assets from lakeFS Enterprise or from upstream ≥ 1.87. Inputs are the
     public docs only. Record this in the PR description.
  2. All new code lives in new packages/directories (`pkg/datasets`, `api/datasets.yml`, `cmd/lakectl-datasets`,
     `webui/src/pages/datasets/**`).
  3. Do **not** merge upstream ≥ 1.87 into this fork without legal review — doing so would put the modified binary under
     BUSL, whose grant does not cover modified works in production.
  4. Get a legal review before production rollout (trademark: avoid "lakeFS Enterprise" wording in UI; we use "Datasets").

---

## 3. Enterprise Feature Parity

| Enterprise capability | This design | Notes |
|---|---|---|
| Global unique name `^[a-z0-9][a-z0-9-]{2,62}$` | ✅ | Same regex as repository IDs; shares the repository namespace. |
| Items: `object`, `prefix` | ✅ | |
| Items: `table`, `namespace` (Iceberg) | ❌ Phase 5 | Workaround: include a file-based table directory as a `prefix` (see §7.4). |
| Sources pinned to commit | ✅ | Branch/tag refs accepted on input, resolved to a commit and stored as `type: commit`. |
| Versions `v1..vN`, `latest` | ✅ | Tags `vN`, default branch `latest`. |
| Version diff `GET /datasets/{ds}/refs/{l}/diff/{r}` | ✅ | Definition diff + data diff. |
| Publish directly / via PR | ✅ | Proposals stored in KV, not graveler PRs. |
| Metadata keys with types / required / allowed values / multi | ✅ | Required keys enforced on write; new required key enforced on next edit. |
| `lakefs://` and `s3://` reads | ✅ | Native — dataset is a repository. |
| Consumer needs only dataset grant | ✅ | Data plane uses `fs:Read*` on the dataset repo ARN. |
| Creator must read sources | ✅ | Checked at create/publish/proposal time. |
| Built-in policies `DatasetsFullAccess`, `DatasetsReadWriteAll`, `DatasetsReadAll`, `DatasetsManageMetadataKeys` | ✅ | Created at bootstrap (§9). |
| `datasets.storage_namespace_prefix` | ✅ | Plus `datasets.storage_id`. |
| GC can break dataset references | ✅ mitigated | Hidden pin branches in source repos (§8). |

---

## 4. Architecture

### 4.1 Overview

```
                   ┌──────────────────────────── lakefs-sso binary ────────────────────────────┐
  UI / lakectl ───►│ /api/v1/datasets/...  ──►  pkg/datasets (HTTP handlers, authz)            │
  (control plane)  │                              │                                            │
                   │                              ├─► KV partition "datasets" (definitions,   │
                   │                              │    versions, proposals, metadata keys,    │
                   │                              │    pins, publish locks)                    │
                   │                              ├─► catalog.Catalog / graveler              │
                   │                              │    (resolve sources, write metarange,     │
                   │                              │     commit, tag, pin branches)             │
                   │                              └─► auth.Service (policies, Authorize)      │
                   │                                                                            │
  any client  ────►│ /api/v1/repositories/<dataset>/...  and  S3 gateway s3://<dataset>/<ref>/ │
  (data plane)     │   (unchanged upstream code — the dataset is a read-only repository)      │
                   └────────────────────────────────────────────────────────────────────────────┘
```

Two planes:

- **Control plane** (new): CRUD of dataset definitions, publishing, versions, diff, proposals, metadata keys,
  access grants. Served by `pkg/datasets` on `/api/v1/datasets/...`.
- **Data plane** (existing, unchanged): reading objects. A dataset `customer-360` is the repository
  `customer-360`; version `v3` is tag `v3`; `latest` is branch `latest`.

### 4.2 Why "dataset = materialised repository"

| Alternative | Verdict |
|---|---|
| **Materialised read-only repository per dataset** (chosen) | All read paths (REST, S3 gateway, presign, Spark, SDKs, UI browser, `lakectl fs`) work with zero changes. Metaranges are cheap (only metadata is written). Diff between versions is native graveler diff. |
| Virtual dataset resolved on each request (proxy layer in front of S3 gateway / REST) | Needs changes inside `pkg/gateway` and dozens of REST handlers; high upstream conflict surface; every client path must be re-implemented. Rejected. |
| Copy data into a dataset repository | Violates zero-copy (G3); doubles storage. Kept as a possible future "snapshot" mode. |
| Dataset as a branch/tag inside the source repo | Can't span repositories; access control would be tied to the source repo. Rejected. |

### 4.3 Package layout

```
pkg/datasets/
  config.go            # Config struct, LoadConfig (viper.UnmarshalKey("datasets"))
  service.go           # Service: Create, Publish, Delete, Get, List, Diff, Validate, ...
  definition.go        # Definition / DataItem types, JSON (de)serialisation, normalisation
  validate.go          # definition + metadata validation
  resolve.go           # resolve refs to commits, check source permissions
  materialize.go       # merge iterator, address conversion, WriteMetaRangeByIterator, commit, tag
  diff.go              # definition diff + data diff
  proposals.go         # dataset pull requests
  metadata_keys.go     # metadata key CRUD + validation
  pins.go              # GC pin branches in source repos, refcounts
  authz.go             # actions, ARNs, Authorize helpers, policy generation, access grants
  bootstrap.go         # built-in policies, immutability deny policy
  reconciler.go        # background repair loop
  store.go             # KV access (JSON records, SetIf CAS)
  http.go              # chi router, handlers (generated server interface implementation)
  apigen/              # oapi-codegen output from api/datasets.yml (go:generate)
api/datasets.yml       # OpenAPI 3 spec of the control plane
cmd/lakectl-datasets/  # lakectl plugin binary (§11)
webui/src/pages/datasets/**, webui/src/lib/api/datasets.ts   # UI (§10)
```

### 4.4 Wiring: the only upstream touches

Following `AZURE_ENTRA_SSO_PLAN.md` §0.2 (merge-friendly), the datasets service is wired from the
`lakefs-sso` wrapper binary through a new builder hook.

**`cmd/lakefs/cmd/hooks.go`** (fork-owned file) — add:

```go
// ServerDeps exposes server internals to out-of-tree route builders.
type ServerDeps struct {
	Config        config.Config
	Catalog       *catalog.Catalog
	AuthService   auth.Service
	Authenticator auth.Authenticator
	BlockAdapter  block.Adapter
	KVStore       kv.Store
	Logger        logging.Logger
}

// ExtraRoutesBuilder mounts additional HTTP routes on the API router. It may start
// background goroutines bound to ctx.
type ExtraRoutesBuilder func(ctx context.Context, deps ServerDeps, r *chi.Mux) error

var extraRoutesBuilders []ExtraRoutesBuilder

// AddExtraRoutesBuilder registers an additional route builder. Call before Execute().
func AddExtraRoutesBuilder(b ExtraRoutesBuilder) { extraRoutesBuilders = append(extraRoutesBuilders, b) }
```

**`cmd/lakefs/cmd/run.go`** — right before `api.MountNotImplementedServices(apiHandler)` (~line 303):

```go
for _, b := range extraRoutesBuilders {
	if err := b(ctx, ServerDeps{cfg, c, authService, middlewareAuthenticator, blockStore, kvStore, logger}, apiHandler); err != nil {
		logger.WithError(err).Fatal("extra routes")
	}
}
```

**`pkg/config/oss_config.go`** — absorb the YAML key, same as `sso`:

```go
Datasets map[string]interface{} `mapstructure:"datasets"` // parsed by pkg/datasets
```

**`pkg/permissions/actions.go`** — one entry in `serviceSet` so policies with `datasets:*` actions validate
(`model.ValidateActionName` → `permissions.IsValidAction` only checks the service prefix). The action constants
themselves live in `pkg/datasets/authz.go`, so `actions.gen.go` codegen is untouched:

```go
	"datasets":  {},
```

**`cmd/lakefs-sso/main.go`**:

```go
lakefsCmd.AddExtraRoutesBuilder(datasets.RoutesBuilder)
```

`datasets.RoutesBuilder` returns early (mounting a 404/disabled handler) when `datasets.enabled` is false.

### 4.5 HTTP authentication for the new routes

The upstream `AuthMiddleware` is bound to swagger routes, so the datasets router wraps itself with the exported
`api.GenericAuthMiddleware(logger, deps.Authenticator, deps.AuthService, &oidcCfg, &cookieCfg)`. It honours the same
schemes as the core API (basic, JWT, `internal_auth_session` cookie, `oidc_auth_session` cookie) and sets
`auth.WithUser` on the context. Request validation uses an `OapiRequestValidator` built from `api/datasets.yml`.

The router is mounted with `r.Mount("/api/v1/datasets", datasetsRouter)`. chi resolves the more specific pattern
before the `/api/v1/*` catch-all (`InvalidAPIEndpointHandler`). **Spike S1** confirms there's no chi pattern
conflict; the fallback prefix is `/api/v1/x/datasets`.

### 4.6 Dependency rules

- `pkg/datasets` may import `pkg/catalog`, `pkg/graveler`, `pkg/auth`, `pkg/block`, `pkg/kv`, `pkg/permissions`, `pkg/api` (for `GenericAuthMiddleware` only).
- Nothing in upstream packages imports `pkg/datasets`.
- The service calls the catalog **directly** (not through the REST API), so it isn't subject to the user's
  permissions on the dataset repo. That is what lets it write to read-only, deny-protected repositories. Every
  control-plane handler must authorise the caller explicitly (§9).

---

## 5. Data Model

### 5.1 Definition format (Enterprise-compatible, `format_version: 1`)

```json
{
  "name": "customer-360",
  "definition": {
    "format_version": 1,
    "description": "Curated customer view for analytics",
    "metadata": { "classification": "internal", "owner": "data-platform", "tier": "gold", "retention": "1y" },
    "data": [
      {
        "target": "customers",
        "repository": "lakehouse-repo",
        "ref": { "type": "commit", "id": "30f9eb1c..." },
        "type": "prefix",
        "prefix": { "path": "data/customers/" }
      },
      {
        "target": "reference/countries.csv",
        "repository": "ref-data",
        "ref": { "type": "commit", "id": "a1b2c3..." },
        "type": "object",
        "object": { "path": "geo/countries.csv" }
      }
    ]
  },
  "publish": { "message": "Initial dataset with customer objects" }
}
```

Go types (`pkg/datasets/definition.go`):

```go
type RefType string // "commit" (stored); "branch" | "tag" accepted on input only

type SourceRef struct {
	Type RefType `json:"type"`
	ID   string  `json:"id"`
}

type ItemType string // "object" | "prefix" ("table" | "namespace" rejected with 501 in MVP)

type PathSource struct {
	Path string `json:"path"`
}

type DataItem struct {
	Target     string      `json:"target"`
	Repository string      `json:"repository"`
	Ref        SourceRef   `json:"ref"`
	Type       ItemType    `json:"type"`
	Object     *PathSource `json:"object,omitempty"`
	Prefix     *PathSource `json:"prefix,omitempty"`
	Table      *TableSrc   `json:"table,omitempty"`     // reserved
	Namespace  *NsSrc      `json:"namespace,omitempty"` // reserved
}

type Definition struct {
	FormatVersion int                 `json:"format_version"`
	Description   string              `json:"description,omitempty"`
	Metadata      map[string]any      `json:"metadata,omitempty"` // string | number | bool | []string
	Data          []DataItem          `json:"data"`
}
```

### 5.2 Target path semantics

| Item type | Source | Target | Resulting keys in the dataset repo |
|---|---|---|---|
| `object` | `geo/countries.csv` | `reference/countries.csv` | exactly `reference/countries.csv` |
| `prefix` | `data/customers/` | `customers` | `customers/<rel>` for every `data/customers/<rel>` |

Normalisation rules (applied before validation and storage):

- Prefix source paths always end with `/` (append if missing). Empty prefix = whole repository.
- Targets: no leading `/`; `.`/`..` segments rejected; no trailing `/` for `object`; for `prefix` the trailing
  `/` is stripped and re-added internally. Empty target for a prefix item = repository root.
- Branch/tag refs are resolved to the commit ID at write time; the stored definition only contains `type: commit`.
- Items are stored sorted by `target`.

Validation (`validate.go`):

1. `name` matches `^[a-z0-9][a-z0-9-]{2,62}$` and isn't `reservedNames` (`_datasets`, …).
2. `format_version == 1`; `1 <= len(data) <= datasets.max_items` (default 1000).
3. Targets must not overlap: no target equal to or a path-prefix of another (`a` vs `a/b` → error). This guarantees
   each output key has exactly one source and makes materialisation a pure merge.
4. Source repository exists, isn't itself a dataset (no nesting in MVP), and has the same `StorageID` as the dataset
   (`datasets.storage_id`).
5. `object` items: the source key exists at the commit. `prefix` items: at least one object exists, unless
   `allow_empty_items: true`.
6. Metadata validates against the metadata keys (§5.5).

### 5.3 KV records

All control-plane state lives in KV partition `datasets` (separate from `auth`/graveler partitions; same store as
the rest of lakeFS, so it's covered by existing backup/HA). Values are JSON (no protobuf → no `make gen-proto`).

| Key | Value | Notes |
|---|---|---|
| `datasets/<name>` | `DatasetRecord` | head pointer, CAS target |
| `versions/<name>/<n:010d>` | `VersionRecord` | immutable after write |
| `proposals/<name>/<id>` | `ProposalRecord` | |
| `metadata_keys/<key>` | `MetadataKey` | |
| `pins/<repo>/<commit>` | `PinRecord` | set of `<dataset>@v<n>` references |
| `grants/<name>/<principal-type>/<principal>` | `GrantRecord` | mirror of attached policies, for listing |

```go
type DatasetRecord struct {
	Name           string    `json:"name"`
	Description    string    `json:"description"`
	CreatedBy      string    `json:"created_by"`
	CreatedAt      time.Time `json:"created_at"`
	LatestVersion  int       `json:"latest_version"`   // 0 = no version published yet
	State          string    `json:"state"`            // "creating" | "active" | "publishing" | "deleting"
	PendingVersion int       `json:"pending_version,omitempty"`
	StorageID      string    `json:"storage_id"`
	StorageNS      string    `json:"storage_namespace"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type VersionRecord struct {
	Dataset     string     `json:"dataset"`
	Version     int        `json:"version"`
	Definition  Definition `json:"definition"`  // fully resolved (commit refs)
	Message     string     `json:"message"`
	Publisher   string     `json:"publisher"`
	PublishedAt time.Time  `json:"published_at"`
	CommitID    string     `json:"commit_id"`   // commit in the dataset repo
	MetaRangeID string     `json:"metarange_id"`
	ProposalID  string     `json:"proposal_id,omitempty"`
	Stats       ItemStats  `json:"stats"`       // objects, bytes, per-item counts
}
```

Large definitions: a `VersionRecord` with 1000 items is ~300 KB. DynamoDB's item limit is 400 KB, so the definition is
also written as an object `_dataset/definition.json` in the version commit (§6.4). If the JSON exceeds 256 KB, the KV
record keeps only a `definition_ref` (dataset repo commit + path) instead of the inline definition.

### 5.4 Version semantics and concurrency

- Versions are dense integers starting at 1. `latest` = `LatestVersion`.
- Publish uses optimistic concurrency on `datasets/<name>`:
  1. Read the `DatasetRecord` (`predicate = returned value`).
  2. Require `State == "active"` or the `pending` state to be stale (> `publish_timeout`, default 30m).
  3. `SetIf` to `State="publishing", PendingVersion=N+1`. On `kv.ErrPredicateFailed` → `409 Conflict`.
  4. Materialise (§6). On success, write `VersionRecord` then `SetIf` `LatestVersion=N+1, State="active"`.
  5. On failure, `SetIf` back to `active` (best effort; the reconciler fixes leftovers).
- The client may pass `expected_version` (like an ETag). Mismatch → `409` with `latest_version` in the body.
- A dataset repo tag `vN` and the `VersionRecord` are the two sources of truth. On disagreement the reconciler (§12.3)
  trusts the tag and rebuilds the record from `_dataset/definition.json`.

### 5.5 Metadata keys

```go
type MetadataKey struct {
	Name          string   `json:"name"`            // ^[a-z][a-z0-9_]{0,62}$
	Type          string   `json:"type"`            // string | enum | number | boolean | date
	Required      bool     `json:"required"`
	AllowedValues []string `json:"allowed_values,omitempty"` // enum only, non-empty
	AllowMultiple bool     `json:"allow_multiple"`  // value is a JSON array
	Description   string   `json:"description,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	CreatedBy     string   `json:"created_by"`
}
```

Rules:

- `date` values are `YYYY-MM-DD` (RFC 3339 full-date). `number` is a JSON number. `boolean` is a JSON bool.
- Metadata keys not defined as metadata keys are **allowed** as free-form strings (Enterprise docs show free keys like
  `retention`). Config `datasets.metadata.strict: true` rejects unknown keys.
- Required keys are enforced whenever a definition is written (create, publish, proposal). Existing versions are
  never re-validated, so adding a required key affects only the next edit — this matches the Enterprise behaviour.
- Deleting a metadata key doesn't touch existing metadata values; they become free-form.
- Changing `type` is forbidden (delete + recreate instead); changing `allowed_values` / `required` is allowed.

### 5.6 Proposals (dataset pull requests)

```go
type ProposalRecord struct {
	ID            string     `json:"id"`              // ULID
	Dataset       string     `json:"dataset"`
	BaseVersion   int        `json:"base_version"`    // version the proposal was made against
	Definition    Definition `json:"definition"`      // resolved
	Title         string     `json:"title"`
	Description   string     `json:"description"`
	Author        string     `json:"author"`
	Status        string     `json:"status"`          // open | closed | merged
	CreatedAt     time.Time  `json:"created_at"`
	ClosedAt      *time.Time `json:"closed_at,omitempty"`
	MergedVersion int        `json:"merged_version,omitempty"`
	PreviewBranch string     `json:"preview_branch"`  // "_proposal-<id>" in the dataset repo
}
```

- Creating a proposal materialises the definition on a hidden branch `_proposal-<id>` in the dataset repo, so
  reviewers can browse the actual data (`lakefs://customer-360/_proposal-01J.../customers/`). It also runs a data diff
  against `latest`.
- Merging = publishing the proposal's definition. It requires `BaseVersion == LatestVersion`; otherwise `409`
  ("rebase required"). The UI offers "Update proposal" to re-resolve against the latest version.
- Rebase isn't automatic: definitions are small and conflicts are semantic, so the author edits and resubmits.
- Closing/merging deletes the preview branch (`Force`).
- Proposals aren't graveler PRs, because graveler merges would produce merge commits and need write permission on
  `latest`. We want single-parent linear history.

---

## 6. Materialisation

### 6.1 Dataset repository

On create, the service calls:

```go
c.CreateRepository(ctx, name, cfg.StorageID, ns, "latest", true /* readOnly */)
```

- `ns = <datasets.storage_namespace_prefix>/<name>-<ulid>`. The ULID suffix lets a deleted dataset's name be reused
  without tripping the "storage namespace already in use" dummy-object check.
- Default branch is `latest`, so `lakefs://customer-360/latest/...` and `s3://customer-360/latest/...` work as-is.
- Repository metadata (`UpdateRepositoryMetadata`) gets `lakefs.dataset=true` and `lakefs.dataset.format=1`. The UI uses
  this to tell datasets apart from repositories. `CreateRepository` in the core API doesn't block creating a repo with
  a dataset's name, but names are globally unique anyway: creating a dataset whose name is taken by a repo returns `409`.
- `ReadOnly=true` blocks all writes from the REST API and S3 gateway **unless** the caller passes `force=true`. The
  service always passes `graveler.WithForce(true)`. Users are kept from using `force` by the immutability deny policy (§9.4).
- Branch protection is **not** used on `latest`: graveler checks protection before `Force` (`graveler.go:2247`), so it
  would block the service's own commits.

### 6.2 Algorithm

```
Publish(ctx, name, def, message, publisher, expectedVersion):
  1. normalise(def); validate(def)                                   // §5.2, §5.5
  2. resolved := resolveRefs(def)                                     // branch/tag → commit id
  3. authorizeSources(publisher, resolved)                            // §9.3
  4. CAS DatasetRecord → publishing, PendingVersion = N+1             // §5.4
  5. pinSources(resolved, name, N+1)                                  // §8, BEFORE writing data
  6. it := newMergeIterator(resolved) + definition.json entry
  7. mrID := Store.WriteMetaRangeByIterator(ctx, dsRepo, it, WithForce(true))
  8. if N > 0 and mrID == version[N].MetaRangeID → abort 409 "no changes"
  9. commit := Catalog.Commit(ctx, name, "latest", message, publisher,
         metadata{dataset.version: N+1, dataset.proposal: id?}, nil, &mrID, false, WithForce(true))
 10. Catalog.CreateTag(ctx, name, "v<N+1>", commit.Reference, WithForce(true))
 11. write VersionRecord; CAS DatasetRecord → active, LatestVersion = N+1
 12. emit audit log + metrics
```

Steps 7–10 are idempotent for retries: the same input produces the same metarange ID, and the reconciler checks
whether `latest` HEAD already carries `dataset.version = N+1` before re-committing.

### 6.3 Source enumeration and address conversion

For each item, the iterator opens `Store.List(ctx, srcRepo, Ref(commitID), batchSize)` and seeks to the source prefix
(or gets exactly one key for `object`). For each `ValueRecord`:

```go
entry, _ := catalog.ValueToEntry(rec.Value)
switch entry.AddressType {
case catalog.AddressTypeFull:
	// keep as is
case catalog.AddressTypeRelative, catalog.AddressTypeByPrefixDeprecated:
	qk, err := blockAdapter.ResolveNamespace(srcRepo.StorageID, srcRepo.StorageNamespace,
		entry.PhysicalAddress, entry.AddressType.ToIdentifierType())
	entry.PhysicalAddress = qk.Format()
	entry.AddressType = catalog.AddressTypeFull
}
key := target + strings.TrimPrefix(rec.Key, sourcePrefix)
val, _ := catalog.EntryToValue(entry) // identity = size+etag+metadata+content-type, so diffs stay meaningful
```

Everything else is kept: `ETag` (checksum), `Size`, `LastModified`, `ContentType`, user `Metadata`.

Reads then work through the existing code. For example, the S3 gateway `getobject.go:113` builds
`ObjectPointer{StorageID: dsRepo.StorageID, StorageNamespace: dsRepo.StorageNamespace, Identifier: entry.PhysicalAddress,
IdentifierType: IdentifierTypeFull}`, and the adapter reads the full URI directly. This is why the dataset and the
source must share a `StorageID` (validated in §5.2).

### 6.4 Ordering: k-way merge

Targets don't overlap, but plain concatenation of per-item streams sorted by target is **not** key-sorted.
For example, target `a` (prefix → `a/x`) and target `a-b` (object): `"a-b" < "a/x"` because `-` (0x2d) < `/` (0x2f).
Each per-item stream *is* sorted (mapping is a prefix replacement), so a heap-based k-way merge over the item streams
gives a globally sorted `graveler.ValueIterator`, which `WriteMetaRangeByIterator` requires. Duplicates can't happen
(targets don't overlap); the merge asserts strictly increasing keys and fails the publish otherwise.

A synthetic entry `_dataset/definition.json` is fed into the merge. Its object is uploaded to the dataset's storage
namespace (relative address, under `_lakefs_dataset/`) before step 7. It has the resolved definition, version, publisher
and message, and it makes every version self-describing for disaster recovery (§12.3). The `_dataset/` target prefix is
reserved: validation rejects user targets under it.

### 6.5 Performance

- Metadata only: cost scales with the number of objects, not bytes. Writing a range of about 50k entries takes on the
  order of seconds. 10M objects ≈ 200 ranges; expected in minutes (**Spike S3** measures this).
- Publish is synchronous up to `datasets.sync_publish_max_objects` (default 100k, estimated from the previous version's
  stats or a quick count capped at the limit). Above that, the API returns `202` with a task ID; the publish runs in a
  goroutine and its status is polled at `GET /datasets/{ds}/tasks/{id}`. Tasks are KV records; a node restart leaves
  `State=publishing` and the reconciler resumes or rolls back.
- Batch size for `Store.List` is the graveler default; per-item iterators are opened lazily at most `merge_fan_in` at a
  time (default 64). For more items, merge in two passes, writing intermediate ranges with `WriteRange`.

### 6.6 Deleting a dataset

1. Authorise `datasets:DeleteDataset`. CAS state → `deleting`.
2. Delete the dataset repo: `Catalog.DeleteRepository(ctx, name, WithForce(true))`. Object data isn't touched;
   only `_lakefs_dataset/` files in the dataset namespace remain, and GC isn't configured there (§8.3).
3. Release pins for every version (§8.2).
4. Remove policies and grants (§9), proposals, version records and the dataset record.

Every step tolerates "not found", so a crashed delete is finished by the reconciler.

---

## 7. Read Path

### 7.1 Ref mapping

| Dataset ref | Graveler ref in repo `<dataset>` |
|---|---|
| `latest` | branch `latest` (default branch) |
| `v1` … `vN` | tag `v1` … `vN` |
| `<commit-id>` | commit (works natively) |
| `_proposal-<id>` | hidden preview branch (reviewers) |

### 7.2 Clients

```bash
lakectl fs ls   lakefs://customer-360/latest/customers/
lakectl fs cat  lakefs://customer-360/v2/reference/countries.csv
aws s3 ls       s3://customer-360/v3/customers/ --endpoint-url https://lakefs.example.com
spark.read.parquet("s3a://customer-360/latest/customers/")
duckdb: SELECT * FROM 's3://customer-360/v1/customers/*.parquet';
python: lakefs.repository("customer-360").ref("v2").object("customers/part-0.parquet").reader()
```

Presigned URLs (`presign=true`) point at the **source** physical location and work as long as the blockstore allows
presigning that bucket — the same credentials as the source repo, so no new IAM is needed.

### 7.3 Writes through the data plane

All blocked:

- `ReadOnly=true` → `ErrReadOnlyRepository` (`403`/`409` from REST, `AccessDenied` from S3) for any request without `force`.
- `force=true` requests are denied by policy (§9.4) for everyone except the service.

### 7.4 Tables without Iceberg

File-based tables (Parquet directories, Delta Lake, Hudi, Iceberg tables read through a Hadoop catalog) can be included
as `prefix` items. Delta tables work when the whole table directory (including `_delta_log/`) is the prefix; the
relative paths in `_delta_log` resolve relative to the new location because the target keeps the same layout.
Iceberg metadata contains **absolute** paths (`s3://bucket/...`) and isn't portable, so it's documented as unsupported
until Phase 5.

---

## 8. Garbage Collection Safety

### 8.1 Problem

- A dataset version references source objects by physical address. The **source repo's GC** computes reachability only
  from the source repo's own branches and commits, so once a source commit becomes unreachable or older than retention,
  GC deletes objects the dataset still serves.
- The **dataset repo's GC** isn't a risk: the GC job lists only the repo's own storage namespace
  (`GarbageCollection.scala`), and dataset entries point outside it.

### 8.2 Pin branches (chosen)

For every distinct `(source repo, commit)` referenced by any dataset version, the service keeps a hidden branch in the
source repo:

```
_lakefs_ds_pin_<commit-id>
```

- Created with `Catalog.CreateBranch(ctx, repo, pin, commitID, graveler.WithHidden(true), graveler.WithForce(true))`.
- GC treats every branch HEAD as a starting point, and `GCBranchIterator` includes hidden branches
  (`ref/manager.go:498`, `ShowHidden: true`). Branch HEADs are always retained regardless of retention rules, so the
  whole tree at that commit survives GC.
- Hidden branches don't show up in default branch listings in the UI/`lakectl`.
- Refcount: `pins/<repo>/<commit>` holds the set of `dataset@vN` that reference it. The branch is deleted when the set
  becomes empty (dataset deleted; versions aren't deleted in the MVP, so in practice only on dataset delete).
- Branch names use the commit ID only (not the dataset name), so many datasets share one pin and dataset names aren't
  leaked to readers of the source repo.
- Read-only source repos: `Force` bypasses read-only. Protected-branch rules matching `*` would block creation (protection
  only blocks staging writes and commits, not branch creation — **verify in Spike S2**); the pin is never committed to.

Risks:

- Source repo admins can delete pin branches. Mitigation: deny policy `datasets-pins-protect` with `fs:DeleteBranch` on
  `arn:lakefs:fs:::repository/*/branch/_lakefs_ds_pin_*` attached to all non-service principals (needs ARN wildcard
  support in the middle of the resource; `auth.ArnMatch` uses wildcard matching on the resource part, **verify in S2**).
  The reconciler recreates missing pins and reports them.
- Deleting a source repository deletes its pins. Dataset reads keep working until the source storage namespace's data is
  removed by someone. The reconciler marks such versions `degraded` and the UI shows a warning.

### 8.3 Alternatives considered

| Option | Why not |
|---|---|
| Tags in source repo | Tags aren't GC starting points (`starting_point_iterator.go` only iterates branches and dangling commits). |
| Long retention on source repos | Global, coarse, doesn't cover deleted branches. Still recommended as defence in depth. |
| Copy objects into the dataset namespace | Breaks zero-copy; acceptable only as a future opt-in "snapshot" mode. |
| Teach GC about datasets | Changes in Spark GC client and `PrepareGCCommits`; large upstream surface. |

### 8.4 Uncommitted GC

Not affected: dataset entries reference only committed source objects.

---

## 9. Authorization

### 9.1 Actions and resources

New service prefix `datasets` (one-line `serviceSet` change, §4.4). Constants live in `pkg/datasets/authz.go`.

| Action | Resource | Used by |
|---|---|---|
| `datasets:ListDatasets` | `*` | `GET /datasets` (results filtered by `ReadDataset`) |
| `datasets:CreateDataset` | `arn:lakefs:datasets:::dataset/<name>` | `POST /datasets` |
| `datasets:ReadDataset` | same | get definition, versions, diff, proposals |
| `datasets:PublishVersion` | same | publish directly, merge proposal |
| `datasets:CreateProposal` | same | open/update/close own proposal |
| `datasets:DeleteDataset` | same | delete |
| `datasets:ManageAccess` | same | grant/revoke consumers |
| `datasets:ManageMetadataKeys` | `arn:lakefs:datasets:::metadata-key/<key>` | metadata key CRUD |

ARN helpers:

```go
func DatasetArn(name string) string   { return "arn:lakefs:datasets:::dataset/" + name }
func MetadataKeyArn(k string) string  { return "arn:lakefs:datasets:::metadata-key/" + k }
```

Authorisation goes through `deps.AuthService.Authorize(ctx, &auth.AuthorizationRequest{Username, RequiredPermissions: node})`,
the same path `Controller.authorize` uses, so Deny precedence, wildcards and conditions behave like the core API.

### 9.2 Data-plane access (consumers)

Reading a dataset = reading repository `<name>`. Granting consumer access to a dataset writes a managed policy
`dataset-<name>-read`:

```json
{
  "statement": [
    { "effect": "allow", "action": ["fs:ReadRepository", "fs:ListObjects", "fs:ReadObject",
                                    "fs:ReadCommit", "fs:ListCommits", "fs:ReadBranch",
                                    "fs:ListBranches", "fs:ReadTag", "fs:ListTags"],
      "resource": "arn:lakefs:fs:::repository/customer-360" },
    { "effect": "allow", "action": ["fs:ReadObject", "fs:ListObjects"],
      "resource": "arn:lakefs:fs:::repository/customer-360/object/*" },
    { "effect": "allow", "action": ["datasets:ReadDataset"],
      "resource": "arn:lakefs:datasets:::dataset/customer-360" },
    { "effect": "allow", "action": ["fs:ListRepositories", "datasets:ListDatasets"], "resource": "*" }
  ]
}
```

`POST /datasets/{ds}/access` attaches it to a user or group via `AttachPolicyToUser` / `AttachPolicyToGroup`, and
records a `GrantRecord`. The REST policy API returns 501 in simplified mode, but the in-process `acl.AuthService`
methods work, so this functions with the `lakefs-sso` binary.

Consumers don't need any grant on source repos: the data plane checks only the dataset repo ARN, and the physical
read is done by the server with its own blockstore credentials. That matches the Enterprise model.

### 9.3 Attach-time source checks (creators)

On create, publish, and proposal create/update, for every item the service checks **as the caller**:

- `fs:ReadCommit` on `RepoArn(src)` and
- `fs:ReadObject` + `fs:ListObjects` on `ObjectArn(src, path)` (object) or `ObjectArn(src, prefix+"*")` (prefix).

Any failure → `403` naming the item. When a proposal is merged, the check runs for the **author** (at creation) and the
**merger** (at merge) — the merger is publishing the data under their authority.

### 9.4 Immutability guard

`ReadOnly` + `force=true` is the only bypass. Per dataset, the service writes `dataset-<name>-immutable`:

```json
{ "statement": [ { "effect": "deny",
  "action": ["fs:WriteObject", "fs:DeleteObject", "fs:CreateCommit", "fs:CreateBranch", "fs:DeleteBranch",
             "fs:RevertBranch", "fs:CreateTag", "fs:DeleteTag", "fs:UpdateRepository", "fs:DeleteRepository",
             "fs:ImportFromStorage", "fs:AttachStorageNamespace", "branches:SetBranchProtectionRules",
             "retention:SetGarbageCollectionRules", "ci:WriteAction"],
  "resource": "arn:lakefs:fs:::repository/customer-360" },
  { "effect": "deny", "action": ["...same list..."],
  "resource": "arn:lakefs:fs:::repository/customer-360/*" } ] }
```

and attaches it to the base groups `Admins`, `Supers`, `Writers`, `Readers` plus every group in
`datasets.immutable_guard_groups`. The service doesn't go through `Authorize` for its own writes, so it isn't affected.

Limits (documented, accepted):

- An admin can detach the deny policy, and then use `force`. That's equivalent to an admin deleting any repo today.
  The reconciler re-attaches it and logs a warning.
- Users who aren't members of a guarded group aren't covered. With SSO group sync every interactive user is in a base
  group; service accounts must be added to a guarded group.
- The guard uses two exact resources (`repository/<name>` and `repository/<name>/*`) rather than `repository/<name>*`,
  which would also match an unrelated repository `<name>-something`.

### 9.5 Built-in policies (created at bootstrap)

| Policy | Statements |
|---|---|
| `DatasetsFullAccess` | `datasets:*` on `*` + data-plane read on all datasets (see note) |
| `DatasetsReadWriteAll` | `datasets:{List,Create,Read,PublishVersion,CreateProposal}*` on `*` |
| `DatasetsReadAll` | `datasets:ListDatasets`, `datasets:ReadDataset` on `*` |
| `DatasetsManageMetadataKeys` | `datasets:ManageMetadataKeys` on `*` |

Note: "data-plane read on all datasets" can't be expressed as one ARN (datasets have arbitrary names). The service
keeps a managed policy `datasets-all-read` whose statements list every dataset repo and updates it on create/delete.
`DatasetsFullAccess` and `DatasetsReadAll` groups get it attached.

Bootstrap also creates groups `DatasetAdmins`, `DatasetPublishers`, `DatasetConsumers` with these policies, so
Entra app roles can map to them through the existing group sync (`sso.groups_claim`).

### 9.6 Interaction with simplified ACL mode (lakefs-sso)

The base ACL groups grant `fs:*`/`fs:Read*` on `*`. Consequences:

- Members of `Readers` and above can read **every** dataset through the data plane, regardless of dataset grants.
- Dataset grants therefore isolate only users who are in no base group (e.g. external consumers, service accounts).

Options, chosen per installation via `datasets.access_model`:

| Value | Behaviour |
|---|---|
| `open` (default) | Accept the above. Grants still control the control plane (`datasets:*`) and serve users outside base groups. |
| `strict` | Out of MVP scope. Requires replacing the global `Readers`/`Writers` ACLs with per-repository policies. Tracked as open question Q3. |

### 9.7 Repository listing

Dataset repos show up in `GET /repositories` for users with read access. The UI repositories page filters out repos
with `metadata["lakefs.dataset"] == "true"` and shows a "Dataset" badge where they're still shown (repo picker).
`lakectl repo list` keeps showing them; that's acceptable.

---

## 10. REST API (control plane)

Spec file: `api/datasets.yml` (OpenAPI 3.0), server stubs generated into `pkg/datasets/apigen` via `oapi-codegen`
(`//go:generate` in `pkg/datasets/http.go`), TS client hand-written in `webui/src/lib/api/datasets.ts`.
Base path `/api/v1/datasets`. Errors use the core `Error` schema `{"message": "..."}`.

### 10.1 Endpoints

| Method & path | Action | Description |
|---|---|---|
| `GET /datasets?prefix=&after=&amount=&search=&metadata.<k>=<v>` | `ListDatasets` (+ `ReadDataset` filter) | Paginated list with latest version summary. Metadata filters match the latest version. |
| `POST /datasets` | `CreateDataset` | Body = definition envelope (§5.1). Without `publish`, creates an empty dataset (no version; `latest` has only the initial commit). With `publish`, creates `v1`. `201` + `Dataset`; `202` + `Task` for large ones. |
| `GET /datasets/{ds}` | `ReadDataset` | `Dataset` (record + latest definition). |
| `DELETE /datasets/{ds}` | `DeleteDataset` | `204`. |
| `POST /datasets/{ds}/validate` | `ReadDataset` | Dry run: normalise, resolve, validate, authorise sources, return resolved definition + stats estimate. No writes. |
| `POST /datasets/{ds}/versions` | `PublishVersion` | Body `{definition, message, expected_version?}`. `201` + `Version` / `202` + `Task` / `409`. |
| `GET /datasets/{ds}/versions?after=&amount=` | `ReadDataset` | Newest first. |
| `GET /datasets/{ds}/versions/{version}` | `ReadDataset` | `version` = `vN` \| `N` \| `latest`. Full definition. |
| `GET /datasets/{ds}/refs/{leftRef}/diff/{rightRef}?type=definition\|data&prefix=&after=&amount=` | `ReadDataset` | §10.3. Same path shape as Enterprise. |
| `GET /datasets/{ds}/proposals?status=&after=&amount=` | `ReadDataset` | |
| `POST /datasets/{ds}/proposals` | `CreateProposal` | Body `{definition, title, description}`. |
| `GET /datasets/{ds}/proposals/{id}` | `ReadDataset` | Includes diff summary against `latest`. |
| `PATCH /datasets/{ds}/proposals/{id}` | `CreateProposal` (author) or `PublishVersion` | Update title/description/definition or `status: closed`. |
| `POST /datasets/{ds}/proposals/{id}/merge` | `PublishVersion` | Body `{message?}`. `409` when `base_version != latest`. |
| `GET /datasets/{ds}/access` | `ManageAccess` | Grants (users/groups with read access). |
| `POST /datasets/{ds}/access` | `ManageAccess` | Body `{principal_type: user\|group, principal_id}`. |
| `DELETE /datasets/{ds}/access/{principal_type}/{principal_id}` | `ManageAccess` | |
| `GET /datasets/{ds}/tasks/{task}` | `ReadDataset` | Async publish status `{status: pending\|running\|completed\|failed, version?, error?}`. |
| `GET /datasets/metadata-keys` | `ListDatasets` | All keys (needed by every editor). |
| `POST /datasets/metadata-keys` | `ManageMetadataKeys` | |
| `GET/PUT/DELETE /datasets/metadata-keys/{key}` | read: `ListDatasets`; write: `ManageMetadataKeys` | |
| `GET /datasets/config` | authenticated | `{enabled, storage_id, max_items, access_model, sync_publish_max_objects}` for the UI. |

Routing note: `metadata-keys` and `config` match the dataset name regex, so they're in `reservedNames` to avoid
colliding with `/datasets/{ds}`.

### 10.2 Core schemas

```yaml
Dataset:
  required: [name, created_at, created_by, latest_version, state]
  properties:
    name: {type: string}
    description: {type: string}
    created_at: {type: integer, format: int64}
    created_by: {type: string}
    latest_version: {type: integer}      # 0 if none
    state: {type: string, enum: [creating, active, publishing, deleting, degraded]}
    definition: {$ref: DatasetDefinition}  # of latest version
    stats: {$ref: DatasetStats}
DatasetDefinition: # §5.1
DataItem:          # §5.1, oneOf by `type`
Version:
  required: [version, commit_id, published_at, publisher]
  properties: {version, message, publisher, published_at, commit_id, proposal_id, definition, stats}
DatasetStats: {objects: int64, bytes: int64, items: [{target, objects, bytes}]}
DefinitionDiff:
  properties:
    description: {from: string, to: string}
    metadata: [{key, change: added|removed|changed, from, to}]
    items: [{target, change: added|removed|changed, from: DataItem, to: DataItem}]
DataDiffList: same shape as core DiffList (path, type: added|removed|changed, size_bytes) + pagination
Proposal: §5.6 fields
MetadataKey: §5.5 fields
Task: {id, status, version, error, created_at, updated_at}
```

### 10.3 Diff

- `leftRef`/`rightRef` accept `vN`, `latest`, or `proposal-<id>`.
- `type=definition` (default): structural diff of the two resolved definitions, matched by `target`. An item is
  `changed` when repository, commit, type or source path differs.
- `type=data`: `Catalog.Diff(ctx, ds, leftGravelerRef, rightGravelerRef, params)` on the dataset repo — native
  metarange diff, so it's fast and paginated. `_dataset/` entries are filtered out.

### 10.4 Status codes

`400` validation (body lists every problem: `{"message": "...", "errors": [{"path": "data[2].target", "message": "overlaps data[0]"}]}`),
`401` unauthenticated, `403` missing permission (dataset or source), `404`, `409` name taken / version conflict /
publish in progress / no changes, `422` storage ID mismatch, `501` `table`/`namespace` items or datasets disabled.

---

## 11. Web UI

Replaces the Enterprise gate. Keep `DatasetsNavTabs` (Datasets / Pull Requests / Settings). Delete
`datasetsEnterpriseInfo.tsx` once the real page lands. If `GET /datasets/config` returns `enabled: false` (or 404),
show a short "Datasets are disabled on this server" message instead of the Enterprise upsell.

### 11.1 Routes (`webui/src/pages/index.jsx`)

```jsx
<Route path="datasets" element={<Layout />}>
    <Route element={<DatasetsLayout />}>              {/* nav tabs: datasets | pulls | settings */}
        <Route index element={<DatasetsListPage />} />
        <Route path="pulls" element={<DatasetProposalsAllPage />} />
        <Route path="settings" element={<DatasetsSettingsPage />} />   {/* metadata keys */}
    </Route>
    <Route path="create" element={<DatasetEditorPage mode="create" />} />
    <Route path=":datasetId" element={<DatasetPageLayout />}>
        <Route index element={<Navigate to="overview" />} />
        <Route path="overview" element={<DatasetOverviewPage />} />     {/* definition, metadata, stats */}
        <Route path="data" element={<DatasetDataPage />} />             {/* object browser, ref = version */}
        <Route path="versions" element={<DatasetVersionsPage />} />
        <Route path="compare" element={<DatasetComparePage />} />       {/* ?left=v1&right=v3 */}
        <Route path="edit" element={<DatasetEditorPage mode="edit" />} />
        <Route path="proposals">
            <Route index element={<DatasetProposalsPage />} />
            <Route path=":proposalId" element={<DatasetProposalPage />} />
        </Route>
        <Route path="access" element={<DatasetAccessPage />} />
    </Route>
</Route>
```

### 11.2 Pages

| Page | Content |
|---|---|
| List | Table: name, description, latest version, objects/bytes, updated, metadata badges (e.g. `tier: gold`). Search box + metadata filters. "Create dataset" button (shown if the user has `CreateDataset`; otherwise hidden). |
| Editor | Name (create only), description, metadata form generated from metadata keys (required markers, enum dropdowns, multi-value chips, date picker). Items table: add item → repository picker → ref picker (branch/tag/commit, resolved to commit on save with a "pinned to `30f9eb1`" label) → type → path picker (reuse the object tree browser) → target. Live validation via `POST /validate`. Footer: "Publish as vN+1" or "Open proposal", message field. "Raw JSON" toggle (Monaco-free textarea, matches existing JSON editors). |
| Overview | Header with name, version selector, copy-buttons for `lakefs://`, `s3://` URIs. Items table with source links (`/repositories/<repo>/objects?ref=<commit>&path=<path>`). Metadata table. |
| Data | Reuse `RepositoryObjectsPage` components (tree + object viewer) against repo `<dataset>` with ref = selected version; upload/delete controls hidden (repo is read-only, `readOnly` already hides them). |
| Versions | Timeline: vN, message, publisher, date, items changed count, "Compare with previous". |
| Compare | Two version pickers; tabs "Definition" (item-level changes) and "Data" (reuse the compare/diff tree components with the dataset repo). |
| Proposals | List + detail: definition diff, data diff, preview link (`data?ref=_proposal-<id>`), Merge / Close buttons, conflict banner when `base_version` is behind. |
| Access | Grants table + add user/group (reuse auth pickers). Shown only with `ManageAccess`. |
| Settings | Metadata keys CRUD (only with `ManageMetadataKeys`). |

### 11.3 Repositories page

Filter out repos with `metadata["lakefs.dataset"] === "true"` from `/repositories`, and add a "Dataset" badge with a
link to `/datasets/<name>` where they appear (repo picker, breadcrumbs). Opening `/repositories/<dataset>` redirects to
`/datasets/<dataset>/data`.

### 11.4 Code layout

```
webui/src/lib/api/datasets.ts                    # typed client (fetch wrapper, same style as lib/api/index.js)
webui/src/lib/components/datasets/*.tsx          # shared components: ItemsTable, MetadataForm, VersionPicker, ...
webui/src/pages/datasets/**                      # pages
webui/src/styles/datasets.css                    # if needed; prefer bootstrap utilities and theme vars (dark mode)
```

Tests: vitest + Testing Library for `MetadataForm` validation, `ItemsTable` normalisation, client error mapping.

---

## 12. CLI, Reconciler, Observability

### 12.1 lakectl

`lakectl` supports plugins (any `lakectl-<name>` executable on `PATH`, `cmd/lakectl/cmd/plugin.go`). Ship
`cmd/lakectl-datasets` as `lakectl-datasets` (Go, reuses lakectl's config file `~/.lakectl.yaml` for endpoint and
credentials) → no change to upstream `cmd/lakectl`:

```
lakectl datasets list [--prefix P] [--metadata k=v]
lakectl datasets show <ds> [--version vN] [-o json]
lakectl datasets create <ds> -f definition.json [--publish -m "msg"]
lakectl datasets publish <ds> -f definition.json -m "msg" [--expected-version N] [--wait]
lakectl datasets validate <ds> -f definition.json
lakectl datasets versions <ds>
lakectl datasets diff <ds> <left> <right> [--data]
lakectl datasets delete <ds> [--yes]
lakectl datasets proposals list|show|create|merge|close ...
lakectl datasets access list|grant|revoke <ds> --user U | --group G
lakectl datasets metadata-keys list|create|delete ...
```

Data access needs no new commands: `lakectl fs ls lakefs://<ds>/v3/`.

### 12.2 Python

Out of MVP. A thin `lakefs_datasets` module calling the REST API with `requests` and the SDK's auth can be added
later. Reads already work through the `lakefs` SDK.

### 12.3 Reconciler

Background goroutine started by `RoutesBuilder`, every `datasets.reconcile_interval` (default 10m), with a KV lease
`datasets/_lease` (SetIf with expiry) so only one node runs it:

1. Datasets stuck in `creating`/`publishing`/`deleting` longer than `publish_timeout` → resume or roll back:
   - if tag `vN` exists → write the missing `VersionRecord` from `_dataset/definition.json`, mark `active`;
   - else if `latest` HEAD has `dataset.version = N` → create the tag, then as above;
   - else → release the pins added for N and mark `active`.
2. Pins: every `(repo, commit)` referenced by a version has its pin branch; recreate if missing (log at WARN, metric
   `datasets_pins_recreated_total`). Orphan pins (no references) are deleted.
3. Policies: immutability guard and managed read policies exist and are attached; re-attach if not.
4. Source repos deleted → mark dataset `degraded` (shown in UI/API).

### 12.4 Observability

- Prometheus: `datasets_publish_duration_seconds{result}`, `datasets_publish_objects`, `datasets_total`,
  `datasets_pins_total`, `datasets_reconcile_errors_total`.
- Audit: each mutating handler logs `logging.Fields{"dataset", "version", "user", "action"}` with the same shape as
  `Controller.LogAction`.
- Version commit metadata (`dataset.version`, `dataset.proposal`, `dataset.publisher`) makes history greppable.

---

## 13. Configuration Reference

```yaml
datasets:
  enabled: true
  # Storage for dataset repositories. Must equal the storage ID of every source repository.
  storage_id: ""                                   # "" = default single blockstore
  storage_namespace_prefix: "s3://my-bucket/lakefs-datasets"   # required when enabled
  max_items: 1000
  allow_empty_items: false
  sync_publish_max_objects: 100000                 # bigger publishes run async (202 + task)
  merge_fan_in: 64
  publish_timeout: 30m
  reconcile_interval: 10m
  access_model: open                               # open | strict (strict: future)
  immutable_guard_groups: []                       # extra groups that get the immutability deny policy
  metadata:
    strict: false                                  # reject metadata keys that aren't defined
```

Env vars follow viper's standard mapping (`LAKEFS_DATASETS_ENABLED`, `LAKEFS_DATASETS_STORAGE_NAMESPACE_PREFIX`, …).
`LoadConfig` validates: `storage_namespace_prefix` set and parseable by the blockstore, `storage_id` exists, durations
positive. Invalid config with `enabled: true` fails startup (fail fast, same as `pkg/sso`).

Also add a commented `datasets:` block to `cmd/lakefs-sso/config.example.yaml`.

---

## 14. Failure Modes

| Scenario | Behaviour |
|---|---|
| Two concurrent publishes on one dataset | Second gets `409` from the CAS in §5.4 step 3. |
| Node crashes mid-publish | Dataset stays `publishing`; the reconciler resumes or rolls back after `publish_timeout`. Pins may briefly over-retain data; never under-retain (pins come before the commit). |
| Source branch moves after create | No effect: items are pinned to commits. |
| Source object deleted on a branch | No effect: pinned commit still references it; pin branch protects it from GC. |
| Source repo GC with expired retention | Protected by pin branches (§8.2). |
| Pin branch deleted manually | Reconciler recreates it. If GC ran in between, affected objects return `404`/`NoSuchKey`; reconciler marks the dataset `degraded` after a sample HEAD check. |
| Source repo deleted | Reads continue until the underlying objects are removed; dataset marked `degraded`. |
| Storage ID mismatch | Rejected at validation (`422`). |
| Huge prefix (10M+ objects) | Async publish with task status; memory bounded (streaming iterators, range writer). |
| Name collision with an existing repo | `409` at create. |
| Somebody writes to the dataset repo with `force=true` | Denied by immutability guard; if the guard was removed, commits without `dataset.version` metadata are detected by the reconciler and reported (no auto-revert in MVP). |
| KV and repo disagree | Tag + `_dataset/definition.json` are authoritative (§5.4). |
| Upstream merge changes `run.go` near the hook | Trivial conflict on ~5 lines (§16). |

---

## 15. Testing Strategy

### 15.1 Unit (`go test ./pkg/datasets/...`)

Use the in-memory KV (`kv/mem`) and in-memory block adapter (`block/mem`) with a real `catalog.Catalog`, as in
`pkg/catalog` tests and `pkg/api` controller tests.

- Definition: JSON round trip with the Enterprise example; normalisation table tests; overlap detection (`a` vs `a/b`,
  `a` vs `a-b` must be allowed); reserved names/targets.
- Metadata validation: every type, required, enum, multi, unknown keys strict/non-strict, "new required key only on next edit".
- Merge iterator: sorted output for the `a` / `a-b` / `a/x` case, many items, empty items, `object` items; assertion on duplicates.
- Address conversion: relative → full via `ResolveNamespace`; full passthrough; checksum/size/metadata preserved.
- Publish: v1 → v2 → v3, tags created, `latest` HEAD, no-op publish → `409`, `expected_version` mismatch → `409`, CAS race (two goroutines).
- Reads: `catalog.GetEntry` + `BlockAdapter.Get` on the dataset repo returns source bytes (end-to-end zero-copy check).
- Diff: definition diff and data diff between versions.
- Proposals: create (preview branch), stale base → `409`, merge, close.
- Pins: created before commit; refcount; delete releases; reconciler recreates.
- GC: run `PrepareGCCommits` on a source repo after deleting the original branch and confirm the pinned commit is in
  the "active" set (protects the regression where tags would have been used).
- Authz: table tests with an `acl.AuthService` on mem KV — creator without source read → `403`; consumer with only dataset
  grant can read data plane; Writer blocked by immutability guard from `force` commit on the dataset repo.
- HTTP: handler tests with `httptest`, auth middleware integration (basic auth + cookie).
- Reconciler: inject crashes between each publish step (hook points via an interface) and check convergence.

### 15.2 System tests (esti)

New file `esti/datasets_test.go` behind the build tag/flag `datasets`, run against the `lakefs-sso` binary with
`datasets.enabled=true` on local blockstore + postgres (docker compose used by `make esti`):

- create from two repos → read via REST, S3 gateway (`minio-go`), presigned URL;
- publish v2, diff v1..v2, read v1 still returns old data;
- delete original source branch + run GC (local GC flow) → dataset still readable;
- proposal flow; access grant for a user outside base groups.

### 15.3 UI

vitest component tests (§11.4) and one Playwright smoke test if the repo's e2e setup supports it (`webui/test/e2e`).

---

## 16. Upstream Merge-Conflict Surface

| Area | Change | Conflict risk |
|---|---|---|
| `cmd/lakefs/cmd/hooks.go` | +`ServerDeps`, `ExtraRoutesBuilder`, `AddExtraRoutesBuilder` | None (fork-owned file) |
| `cmd/lakefs/cmd/run.go` | ~5 lines before `MountNotImplementedServices` | Low |
| `pkg/config/oss_config.go` | +1 field | Low |
| `pkg/permissions/actions.go` | +1 `serviceSet` entry | Low |
| `cmd/lakefs-sso/main.go` | +1 line | None (fork-owned) |
| `webui/src/pages/index.jsx` | datasets route tree | Medium (upstream also touches this file) |
| `webui/src/pages/repositories/index.*` | filter dataset repos | Low–medium |
| `webui/src/pages/datasets/**`, `webui/src/lib/components/datasets/**` | rewrite | Medium if upstream evolves the gate; we own these files now |
| `pkg/datasets/`, `api/datasets.yml`, `cmd/lakectl-datasets/`, `esti/datasets_test.go`, `DATASETS_DESIGN.md` | new | None |

Rename/keep upstream `design/open/design-datasets.md` untouched (it describes an unrelated table-definition idea); this
document lives at the repo root next to `AZURE_ENTRA_SSO_PLAN.md`.

---

## 17. Milestones

| Phase | Scope | Estimate |
|---|---|---|
| **0. Spikes** | S1 chi mount, S2 pin branches vs protection + deny ARN wildcards, S3 publish throughput on 1M/10M objects, S4 cross-repo full address reads on S3, Azure and GCS adapters | 2–3 d |
| **1. Core backend** | config, hook wiring, KV store, definition/validation, merge iterator, materialise, publish/create/get/list/delete, pins, `api/datasets.yml` + handlers, auth middleware, unit tests | 6–8 d |
| **2. Authz + versions** | actions, source checks, managed policies, immutability guard, built-in policies/groups, versions list/get, diff, reconciler | 4–5 d |
| **3. UI** | list, overview, data browser, versions, compare, editor, access, settings; repositories filter | 6–8 d |
| **4. Proposals, CLI, metadata keys** | proposals backend + UI, `lakectl-datasets`, metadata keys CRUD + editor form, esti tests, docs | 5–6 d |
| **5. (later)** | Iceberg tables via a REST catalog, Python helper, strict access model, snapshot mode, version retention | — |

MVP (phases 0–3) ≈ 3–4 weeks for one engineer; full (0–4) ≈ 5 weeks.

Definition of done for MVP: create/publish/read/diff/delete datasets from UI and API, data readable via S3 gateway and
`lakectl fs`, GC-safe, unit + esti green, `make lint` and `make checks-validator` clean.

---

## 18. Open Questions and Spikes

| # | Question | How to resolve |
|---|---|---|
| S1 | Does `r.Mount("/api/v1/datasets", ...)` after `api.Serve` take precedence over the `/api/v1` catch-all mount in chi? | Spike test in `pkg/datasets/http_test.go`; fallback `/api/v1/x/datasets` or route inside a wrapping mux. |
| S2 | Can a hidden branch be created in a repo whose protection rules match `*`? Does `auth.ArnMatch` match `repository/*/branch/_lakefs_ds_pin_*`? | Unit tests against graveler and `auth.ArnMatch`. |
| S3 | Publish throughput and memory for 1M / 10M objects. | Benchmark with `block/mem` + postgres KV; tune `merge_fan_in`, sync threshold. |
| S4 | Do all adapters (S3, Azure, GCS, local) read `IdentifierTypeFull` addresses outside the repo namespace? Local adapter restricts paths to `allowed_external_prefixes`. | Adapter-level tests; document required local config. |
| Q1 | Should consumers be able to see source lineage (repo + commit) if they can't read the source? | Default: show repo name + commit ID only (no links) to non-readers. Confirm with stakeholders. |
| Q2 | Version deletion / retention for datasets (e.g. keep last N)? | Out of MVP; design pin release accordingly (refcount already supports it). |
| Q3 | Strict access model under simplified ACL (no global read)? | Separate design: per-repository ACL policies instead of global `Readers`. |
| Q4 | Iceberg table items. | Phase 5: requires an OSS Iceberg REST catalog; evaluate Apache Polaris / Nessie in front of lakeFS. |
| Q5 | Legal sign-off on clean-room approach and naming. | Legal review before production rollout (§2). |
