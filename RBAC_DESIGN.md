# Policy-based RBAC for the lakeFS SSO fork — Design Document

Status: **Draft** · Branch base: `feature/azure-entra-sso-apache` (Apache 2.0, upstream base `4bb11638e`) · Date: 2026-10-28

## Executive Summary

The `lakefs-sso` build runs `contrib/auth/acl.AuthService` in-process and forces `auth.ui_config.rbac=simplified`.
Users get four fixed, global permission levels (Read / Write / Super / Admin), assigned per group. There is no way
to grant a team access to only some repositories or branches.

Upstream removed the ACL server, the remote auth client and the multi-user UI (`31663066a`, 2026-09-17), and then
relicensed to BSL 1.1 (`358544dd7`, 2026-09-22). The policy engine we need, though, is **already in this tree, under
Apache 2.0**:

- `auth.CheckPermissions`
- policy CRUD in `acl.AuthService`
- the policy REST endpoints
- the Policies UI pages
- the `lakectl auth policies` commands

This design:

- **Option 2 (main deliverable)** — turns on the existing policy-based RBAC, in-process, using only Apache 2.0 code
  already in the fork: a new `internal` mode for `lakefs-sso`, idempotent bootstrap of the RBAC base groups and
  policies, a migration from ACL groups, and the fixes the existing code needs to be safe in that mode.
- **Option 3 (gap fill)** — adds, as our own code, the parts the Apache code base never had and the Enterprise docs
  describe: `StringEquals` conditions, repository-metadata ABAC, filtered branch listing, `datasets:` actions, and
  cache invalidation. Things we deliberately leave out are listed in §7.

---

## 1. Review of the latest commits

### 1.1 `b807edb06` — sso: store email and display name from the ID token on login

What it does: `provisionUser` (`pkg/sso/oidc_provider.go:206`) creates the user at callback time, writes email and
display name from the ID token straight onto the ACL user record in KV (`kvProfileStore`, `pkg/sso/user.go:72`), and
then syncs groups. The user ID stays the Entra `oid`, and `User.Committer()` now returns the email.

Assessment: the commit is correct and the approach is reasonable for a fork that treats `acl` as a black box. Points
that matter for this design:

| # | Observation | Impact | Resolution here |
|---|---|---|---|
| 1 | The profile write bypasses `acl.AuthService` and its `auth.Cache`. `GetUser` is cached for `auth.cache.ttl` (20s, plus jitter), so the first page loads after the first login can still show the GUID. | Cosmetic | §4.6: we own `acl`, so add `UpdateUserProfile` to the service. It invalidates the cache, and `kvProfileStore` is deleted. |
| 2 | The `pkg/sso` code is coupled to the `model.UserPath` / `PartitionKey` KV layout. | Maintenance | Same as #1. |
| 3 | `preferred_username` is used as the email when it contains `@`. For Entra B2B guests it looks like `alice_contoso.com#EXT#@tenant.onmicrosoft.com`. | A guest's committer becomes an `#EXT#` UPN | Skip UPNs that contain `#EXT#`, or document the `email` optional claim. Small follow-up. |
| 4 | Committer changes from the oid to the email for new commits only. Old commits keep the oid. | Expected | Document in the SSO changelog. |
| 5 | `EnsureUser` calls `GetUserByExternalID`. On a cache miss this is a **full scan** of all users (`getUserByPredicate`, `contrib/auth/acl/service.go:172`). | O(users) per login | §4.6: SSO users have `Username == ExternalID == oid`, so call `GetUser(oid)` first and fall back to the scan. |
| 6 | The profile is refreshed even when group sync is disabled, which is a behaviour change: the user now exists after the callback. | Fine | — |

### 1.2 `f0a2f2055` — webui: group permission dropdown clipped by the table

What it does: `Dropdown.Menu popperConfig={{ strategy: 'fixed' }}` in `webui/src/pages/auth/groups/index.tsx`.

Assessment: correct, minimal, and consistent with the repository tree menus. A fixed popper does not reposition when
an inner scroll container scrolls, which is acceptable for this table. The dropdown is only rendered in `simplified`
mode; in the `internal` mode from §4 the column is hidden, so the change has no effect there.

### 1.3 What these commits tell us

Both commits work around the limits of the ACL model:

- profile fields have no service API;
- there is one global permission per group;
- the UI is tuned for four groups.

The datasets design (`DATASETS_DESIGN.md` §9.6) reaches the same wall: base ACL groups grant `fs:*` on `*`, so per-
dataset grants cannot isolate anyone. That design calls a `strict` model "out of MVP scope, requires replacing the
global ACLs with per-repository policies". This document provides exactly that.

---

## 2. Licensing rules

1. **Allowed source:** any code in this fork's history whose tree carries the Apache 2.0 `LICENSE`. In practice that
   is everything at or before upstream `4bb11638e` (our merge base), plus our own commits. Upstream commits up to
   `31663066a` were also made under Apache 2.0. We don't need any of them (it is a removal), but they may be
   consulted.
2. **Forbidden:** any upstream commit at or after `358544dd7` (BSL 1.1). That includes `a6d4f1345` (hook SSRF
   blocking), `ffaa219b4`/`245cb66af` (gateway SigV4 signed-headers fix) and `24bfa9031`. Do **not** cherry-pick
   them or copy from them. Security fixes are re-implemented from public advisories by someone who has not read the
   BSL diff, and tracked separately (§8).
3. **Enterprise behaviour:** the public RBAC docs (https://docs.lakefs.io/security/rbac/) may be used as a
   *behavioural specification*: action names, ARN formats and evaluation semantics, for client compatibility. Do
   not copy doc prose or any Enterprise/"Fluffy" code.
4. Keep `LICENSE`, `NOTICE` and `THIRD_PARTY_LICENSES` as they are. Every new file gets the repository's usual (lack
   of) header convention.
5. A guard fails if `upstream/master` past `358544dd7` becomes an ancestor of the branch
   (`git merge-base --is-ancestor 358544dd7 HEAD`). It is `scripts/check-license-base.sh`, run with
   `make check-license-base` (which first runs the script's `--self-test`, so CI proves the guard is not
   vacuous). It runs in `make checks-validator` and in the `license-base` CI job
   (`.github/workflows/test.yaml`, full clone plus a fetch of upstream master). The guard **fails closed**:
   on a shallow clone, an unborn HEAD or no repository it exits 1 with the remedy (`git fetch --unshallow`)
   rather than passing vacuously; `LICENSE_GUARD_SKIP_IF_INCONCLUSIVE=1` is the documented, local-only
   opt-out and CI never sets it. With complete history it decides the ancestry for real, and a `358544dd7`
   that is absent from a complete clone is reported as conclusively not an ancestor (the guard never fetches,
   so it also works offline).

> Not legal advice. Confirm rule 1 (Apache status of pre-relicense upstream commits) with legal before release.

---

## 3. Current state (verified in code)

### 3.1 What already exists under Apache 2.0

| Piece | Location | Status |
|---|---|---|
| Policy evaluation: deny wins, `*`/`?` wildcards, `${user}`, multi-resource, And/Or nodes | `pkg/auth/service.go:1230` `CheckPermissions` | Complete |
| ARN matching | `pkg/auth/arn.go:87` | Complete |
| Conditions: `IpAddress`, `NotIpAddress`, `StringLike`, `StringNotLike` | `pkg/auth/condition.go` | Only `SourceIp` is ever populated |
| Policy, group and user CRUD; effective policies, cached | `contrib/auth/acl/service.go` (`WritePolicy:639`, `AttachPolicyToUser:257`, `getEffectivePolicies`, …) | Complete |
| `Authorize` → `CheckPermissions` | `contrib/auth/acl/service.go:895` | Complete |
| Built-in statement templates | `pkg/auth/base.go` | Complete, with gaps (§3.3) |
| RBAC base groups and policies | `pkg/auth/setup/setup.go:42,132` | **Not idempotent** (fails on `ErrAlreadyExists`) |
| Policy REST endpoints | `pkg/api/controller.go:1405-2062` | Return **501 when `simplified`** |
| Repository list filtered by policy | `pkg/api/controller.go:2203`, `pkg/catalog/list_repository_options.go` | Works |
| UI pages: policies, policy editor, user/group policies, effective policies | `webui/src/pages/auth/policies/*`, `users/user/{policies,effectivePolicies}.jsx`, `groups/group/policies.jsx` | Hidden when `simplified` |
| CLI | `cmd/lakectl/cmd/auth_policies_*.go`, `auth_groups_policies_*.go` | Works against the API |
| Audit-style request logging | `pkg/httputil/logging.go:87-102`, gated by `IsAdvancedAuth()` | Off (ACL returns `false`) |

### 3.2 Why it is switched off

- `ensureSimplifiedRBAC` (`pkg/sso/auth_builder.go:74`) forces `simplified`.
- `acl.AuthService.IsAdvancedAuth()` returns `false` (`contrib/auth/acl/service.go:44`), so:
  - `setup.CreateBaseGroups` is a no-op;
  - the audit log fields are off.
- `auth.NewAuthService`'s `checkAuthModeSupport` (`pkg/auth/build.go:16`) rejects `internal`/`external` without
  `auth.api`. `lakefs-sso` bypasses that through the builder hook, but `lakefs setup` and `lakefs superuser`
  (`cmd/lakefs/cmd/setup.go:82`, `superuser.go:73`) call `auth.NewAuthService` directly and ignore the hook.

### 3.3 Defects in the existing ACL model

These matter in both modes:

1. **ACL `Write` cannot create or merge pull requests.** `FSReadWrite` + `RepoManagementRead` grant only
   `pr:Read*`/`pr:List*`. `CreatePullRequest` needs `pr:WritePullRequest` (`controller.go:5947`). RBAC `Developers`
   gets `PRReadWriteAll`.
2. **ACL `Admin` has no `catalog:*`.** `AllAccess` lists `fs auth ci retention branches pr` (`base.go:19`).
3. **ACL policies are snapshots.** `WriteGroupACL` stores the computed statements. Fixing templates does not update
   existing groups.
4. **`SetGroupACL` silently detaches every other policy from the group** (`contrib/auth/acl/write.go`, final loop).
   This is harmless when there are only ACLs, and destructive once custom policies exist.
5. `GetGroupACL` returns 404 "Multiple policies attached" for any group that holds more than one policy. The
   simplified UI then shows the group with no permission.

---

## 4. Option 2 — enable in-process policy RBAC

### 4.1 Modes

| `auth.ui_config.rbac` | Backend | UI | Use |
|---|---|---|---|
| `none` (default, SSO off) | `BasicAuthService` | Single admin | Unchanged |
| `simplified` (SSO default) | `acl.AuthService` | Groups with an ACL dropdown | Unchanged, plus the §3.3 fixes |
| **`internal`** (new for SSO) | `acl.AuthService` wrapped as `rbacAuthService` | Full Users / Groups / Policies UI | Policy RBAC |
| `external` | — | — | Warn, then `simplified` (§4.2): no remote auth service exists in this fork |

`internal` already exists as a constant (`pkg/config/config.go:43`) and is already treated as "advanced" by
`IsAdvancedAuth()`. The web UI already renders the policy pages for any mode other than `simplified` or `none`. **No
UI code is required to turn it on.**

### 4.2 `pkg/sso/auth_builder.go`

```go
// rbacAuthService is the ACL KV service with policy management exposed: the engine and
// CRUD are the same, only the capability flag differs.
type rbacAuthService struct{ *acl.AuthService }

func (rbacAuthService) IsAdvancedAuth() bool { return true }
```

`BuildAuthService`:

1. `resolveAuthMode(uiCfg)` replaces `ensureSimplifiedRBAC`:
   - `""`/`none` → `simplified` (unchanged default);
   - `simplified` → `simplified`;
   - `internal` → `internal`;
   - `external` → warn, then `simplified` (the same resolution runs in `setup` and `superuser`, so they create
     the local admin instead of exiting early).
2. Always: `EnsureACLBaseGroups`, then `ReconcileACLPolicies` (§4.5).
3. If `internal`:
   - `EnsureRBACBase(ctx, svc)` (§4.3);
   - wrap the service: `svc = rbacAuthService{aclService}`.
4. Break-glass admin: in `internal` mode, add it to both `Admins` (same name in both models) and nothing else.
5. `auth.NewMonitoredAuthService(svc)`.

Effects of `IsAdvancedAuth()==true`, all intended:

- request logs get `log_audit=true`, `client`, `sent_bytes` and request IDs (`pkg/httputil/logging.go`);
- the gateway does the same;
- `setup.CreateBaseGroups` becomes active (but see §4.3).

Also route `lakefs setup` and `lakefs superuser` through `authServiceBuilder`
(`cmd/lakefs/cmd/{setup,superuser}.go`). That fixes the existing bug where these commands fail under `lakefs-sso`
with any mode other than `none`.

### 4.3 Idempotent RBAC bootstrap

Refactor `pkg/auth/setup/setup.go` (Apache, in-tree):

- `createGroups`, `createPolicies` and `attachPolicies` treat `auth.ErrAlreadyExists` as success.
- New exported `EnsureRBACBaseGroups(ctx, svc, ts)` creates whatever is missing:
  - groups `Admins`, `SuperUsers`, `Developers`, `Viewers`;
  - the policies from `CreateRBACBasePolicies`;
  - the attachments at `setup.go:148-160`.
- It never modifies an existing policy, so admins can edit the built-ins.
- `CreateRBACBaseGroups` keeps its signature and delegates to it.

`pkg/sso.EnsureRBACBase` calls it, then records `rbac-bootstrap-version` in the `auth` partition. Future template
changes can then be applied once, explicitly, instead of on every boot.

### 4.4 Migration `simplified` → `internal`: `lakefs-sso rbac-migrate`

Built on the existing `sso-migrate` pattern (`pkg/sso/migrate.go`). It runs a dry run by default and needs `--yes`
to apply. It is idempotent.

| ACL group (permission) | RBAC target | Action |
|---|---|---|
| `Admins` (Admin) | `Admins` | Attach `FSFullAccess`, `CatalogReadWriteAll`, `AuthFullAccess`, `RepoManagementFullAccess`, `PRReadWriteAll`; detach `ACL(_-_)Admins` |
| `Supers` (Super) | `SuperUsers` | Copy members. `SuperUsers` carries `PRReadWriteAll` in the base template, so the copy keeps pull-request write |
| `Writers` (Write) | `Developers` | Copy members |
| `Readers` (Read) | `Viewers` | Copy members |
| Custom group with an ACL | Same group | Keep its `ACL(_-_)<g>` policy. It is a valid policy and shows in the Policies UI. |
| Custom group with no ACL | Same group | Untouched |

- `--cleanup` deletes the emptied `Supers`, `Writers` and `Readers` groups and their `ACL(_-_)*` policies. Without
  it they stay, so a rollback to `simplified` is lossless.
- The command prints the `sso.group_mapping` rewrites the operator must make (e.g. `Writers` → `Developers`).
  Otherwise the next login's group sync would re-add users to the old groups.
- **Rollback** is `rbac: simplified`. Groups that hold several policies then show "no ACL" in the simplified UI (§3.3
  #5). The command warns about this.

### 4.5 Safety fixes in the ACL layer (applied in both modes)

1. **Templates (§3.3 #1, #2):**
   - Add `catalog:*` to `AllAccess`.
   - ACL `Write` gains `PRReadWrite` and `CatalogReadWrite`.
   - ACL `Read` gains `CatalogRead`.
   - Super gains `PRReadWrite` and `CatalogReadWrite`.

   The RBAC base groups (§4.3) are the policy-set encoding of the same intent, and the parity is asserted by
   `TestACLToStatementRBACParity` (`contrib/auth/acl/permission_test.go`), which compares the effective action set of
   each ACL template with the union of the policies attached to the equivalent RBAC group:

   | ACL template | RBAC group | Difference |
   |---|---|---|
   | `Write` | `Developers` | none: `FSReadWriteAll`, `PRReadWriteAll`, `CatalogReadWriteAll`, `AuthManageOwnCredentials`, `RepoManagementReadAll` |
   | `Super` | `SuperUsers` | none: `FSFullAccess`, `PRReadWriteAll`, `CatalogReadWriteAll`, `AuthManageOwnCredentials`, `RepoManagementReadAll` |
   | `Read` | `Viewers` | **`fs:ReadConfig`**: the ACL `Read` template grants it (storage-config endpoints), `Viewers` does not. A Reader cannot call `GET /api/v1/config/storage` after migrating. Pre-existing and tracked separately (DESIGN_CONFORMANCE.md GAP-06 records the Super half, which is fixed) |

   `SuperUsers` must keep `PRReadWriteAll`: without it, `rbac-migrate` moved a `Supers` member into a group that can
   write every branch and object but cannot open, update or merge a pull request, silently downgrading every existing
   Super. Template change ⇒ bump `RBACBootstrapVersion` (§4.3), which is how existing installations pick it up once.
2. **`ReconcileACLPolicies` (§3.3 #3):** at startup, for each policy with a non-empty `ACL`, recompute
   `ACLToStatement`. If the result differs, rewrite the policy through `WritePolicy(update=true)` and log it. Run it
   once per template version, using the same version key as §4.3.
3. **`SetGroupACL` in `internal` mode (§3.3 #4):** return `409 Conflict` "group ACLs are not used in RBAC mode; attach
   policies instead". The gate lives in `controller.go:1286` and checks `IsAuthUISimplified()`, the same way as the
   policy endpoints. `lakectl auth groups acl set` gets the error as is.
4. **The `simplified` UI with multi-policy groups:** show "Custom (policies)" read-only instead of an empty dropdown.
   This is a `groups/index.tsx` change and needs `NoAcl` from the 404 body.

### 4.6 Profile and lookup (replaces the `b807edb06` workaround)

In `contrib/auth/acl/service.go`:

- Implement `UpdateUserFriendlyName`.
- Add `UpdateUserProfile(ctx, username, email, friendlyName)`, using `SetMsgIf` with a predicate as `kvProfileStore`
  does today.
- Both drop the user's cache entries. This needs a small `Cache.InvalidateUser(UserKey)` addition to `pkg/auth/cache.go`
  (`LRUCache` wraps `ccache`, which has `Delete`). `DummyCache` gets a no-op.

In `pkg/sso`:

- `profileUpdater` is satisfied by the service. `kvProfileStore` and `builtKVStore` are deleted.
- `EnsureUser` tries `GetUser(externalID)` before `GetUserByExternalID`. That avoids the full scan for every SSO user
  (§1.1 #5).

### 4.7 Cache coherence

`ListEffectivePolicies` is cached per user for `auth.cache.ttl` (`contrib/auth/acl/service.go:398`). As a result:

- group sync at login changes effective rights only after up to TTL plus jitter (default 23s);
- the same delay applies after an admin detaches a policy.

With the policy model this delay is visible and security-relevant: revoking access is not immediate. Add
`Cache.InvalidateUserPolicies(username)` and call it from:

- `AddUserToGroup` / `RemoveUserFromGroup` and `Attach/DetachPolicyToUser`, for that user;
- `Attach/DetachPolicyToGroup`, `WritePolicy(update)`, `DeletePolicy` and `DeleteGroup`, by purging the whole policy
  cache (rare admin operations).

The cache is per process. With several replicas, the other replicas still converge only within the TTL. Document
this, and recommend `auth.cache.ttl: 5s` for multi-replica deployments.

### 4.8 What Option 2 delivers

Concrete outcomes, all expressible with the existing engine:

- Repository- and branch-scoped grants, e.g. allow `fs:*` on `arn:lakefs:fs:::repository/analytics-*`, plus a deny
  for `fs:CreateCommit` on `arn:lakefs:fs:::repository/analytics-1/branch/main`. Such a deny blocks `commit`,
  `merge` into that branch, cherry-pick into it and `import`, and it hides the branch from `list branches`; it
  does **not** block uploads (see below), and blocking `revert`/`reset` or deleting the branch needs the
  matching branch actions (`fs:RevertBranch`, `fs:DeleteBranch`).
- Path-scoped object grants, e.g. a deny for `fs:WriteObject` on
  `arn:lakefs:fs:::repository/analytics-1/object/raw/*`.
- IP-restricted access (`IpAddress`/`SourceIp`).
- Per-user self-service (`${user}`).
- Repository list filtered to what the user can see (already implemented).
- Entra groups or app roles → custom lakeFS groups → custom policies, through the existing `sso.group_mapping`.
- Datasets: the `strict` access model in `DATASETS_DESIGN.md` §9.6 becomes possible by using RBAC base groups
  without global `fs:*` grants.

**An action only decides against the ARN shape it is checked on.** A statement naming a branch ARN cannot
block an object write, and a statement naming an object path cannot block a commit. The handlers in
`pkg/api/controller.go` and `pkg/gateway/operations/*` build the resource with the helpers in
`pkg/permissions/permission.go`:

| ARN | Built by | Actions checked on it | Typical example |
|---|---|---|---|
| `…/repository/<repo>/object/<path>` | `permissions.ObjectArn` | `fs:WriteObject`, `fs:ReadObject`, `fs:DeleteObject` (upload, presign, delete, copy source and destination, stat, symlink) | deny `fs:WriteObject` on `…/object/raw/*` |
| `…/repository/<repo>/branch/<branch>` | `permissions.BranchArn` | `fs:CreateCommit`, `fs:CreateBranch`, `fs:DeleteBranch`, `fs:ReadBranch`, `fs:ListBranches`, `fs:RevertBranch`, `fs:ImportCancel` | deny `fs:CreateCommit` on `…/branch/main` |
| `…/repository/<repo>` | `permissions.RepoArn` | repository-level actions: `fs:ReadRepository`, `fs:UpdateRepository`, `fs:DeleteRepository`, `fs:ListObjects`, `fs:ListTags`/`fs:ListCommits`/`fs:ListBranches` when not branch-filtered, the `pr:*` actions, GC and branch-protection actions | deny `fs:ListObjects` on `…/repository/analytics-1` |

Consequences worth stating, because the earlier version of this section got the first one wrong:

- `fs:WriteObject` on an object ARN carries no branch segment, so an object-path deny applies to every
  branch of the repository (an uncommitted write is the same object key on any branch). Branch isolation is
  not expressible at the object level; what a branch ARN does gate is publishing a change
  (`fs:CreateCommit`, `fs:CreateBranch`).
- The exceptions are pre-existing: `import` (`ImportStart`) additionally requires `fs:WriteObject` on the
  *branch* ARN, and `restore refs` / `create commit record` are checked on the repository ARN.
- `fs:ListObjects` is checked on the repository ARN, not per object, so a path-prefix grant cannot narrow a
  listing.
- A branch-ARN deny is honoured by `ListBranches` (the branch disappears from the listing) and by every
  branch handler, which is what §5.3 relies on.

### 4.9 Files touched

The **authoritative** list is the commit range, not this table:

```
git diff --name-only a25abca09..HEAD
```

(65 files at the time of writing, tests, test data and the review documents included.) The tables below
name the files that carry a design decision, so a reader can find the code for each section; anything else
in the range is test, test data or documentation for the items listed here.

Main changes:

| File | Change |
|---|---|
| `pkg/sso/auth_builder.go` | `resolveAuthMode`, `rbacAuthService`, bootstrap order |
| `pkg/sso/rbac_bootstrap.go` (new) | `EnsureRBACBase`, version key |
| `pkg/sso/rbac_migrate.go` (new) + `cmd/lakefs-sso/main.go` | `rbac-migrate` command |
| `pkg/sso/user.go`, `oidc_provider.go`, `authentication_builder.go` | Use the service profile API; delete `kvProfileStore` |
| `pkg/auth/setup/setup.go` | Idempotent helpers, `EnsureRBACBaseGroups` |
| `pkg/auth/base.go` | Template fixes |
| `pkg/auth/cache.go` | `InvalidateUser`, `InvalidateUserPolicies`, `PurgePolicies` |
| `contrib/auth/acl/service.go` | Profile updates, cache invalidation |
| `contrib/auth/acl/reconcile.go` (new) | `ReconcileACLPolicies` |
| `pkg/api/controller.go` | `SetGroupACL` gate |
| `cmd/lakefs/cmd/setup.go`, `superuser.go` | Use `authServiceBuilder` |
| `webui/src/pages/auth/groups/index.tsx` | "Custom (policies)" display in `simplified` |
| `cmd/lakefs-sso/config.example.yaml`, SSO changelog | `rbac: internal`, migration steps |

Also touched, for the cross-cutting wiring the sections above require (an earlier version of this table
omitted them):

| File | Change | Design § |
|---|---|---|
| `contrib/auth/acl/permission.go` | ACL statement templates for PRs and catalogs | §4.5 #1 |
| `pkg/auth/condition.go` | `StringEquals` / `StringNotEquals` operators | §5.1 |
| `pkg/auth/service.go` | `AuthorizationRequest.Fields` lazy provider, condition context | §5.2 |
| `pkg/cache/cache.go`, `pkg/cache/no_cache.go` | `Delete` / `Purge`, the primitives behind the policy-cache invalidation | §4.7 |
| `pkg/config/config.go`, `pkg/config/oss_config.go`, `pkg/config/testdata/*` | `auth.authorization_failure_status`, its startup validation and test fixtures | §5.5 |
| `pkg/gateway/handler.go`, `pkg/gateway/middleware.go` | Repository-metadata conditions and condition context in the S3 gateway | §5.2 phase 1 |
| `pkg/gateway/operations/listbuckets.go`, `pkg/catalog/list_repository_options.go` | Condition-aware repository filter (ListBuckets and ListRepositories) | §5.2, §5.3 |
| `pkg/permissions/actions.go` | `datasets` service set | §5.4 |
| `webui/src/lib/api/index.js` | 404 `no_acl` body → `NoAclError`, the signal §4.5 #4 depends on | §4.5 #4 |
| `scripts/check-license-base.sh` (new), `Makefile`, `.github/workflows/test.yaml` | The §2.5 licence-base guard, its make target and its CI job | §2.5 |
| `REVIEW_R1.md`, `DESIGN_CONFORMANCE.md` | The independent code review and the conformance review of this delivery | — |
| `RBAC_DESIGN.md` | This document, kept in step with what shipped (§4.8, §4.9, §6, §7, §8) | — |

### 4.10 Tests

- **Unit:**
  - `resolveAuthMode` table;
  - `EnsureRBACBaseGroups` run twice, and after a user edits `FSReadAll` (not overwritten);
  - `ReconcileACLPolicies` before and after a template change;
  - the `rbac-migrate` plan and apply on a mem KV (`kv/mem`), with re-runs as no-ops;
  - cache invalidation (revoke, then immediate `Authorize` → deny).
- **Controller:** with `internal` and `acl.AuthService`:
  - policy CRUD returns 2xx;
  - `SetGroupACL` returns 409;
  - a repo-scoped user sees only matching repositories, and reading another repository's object returns 401.
- **Existing suites:** `pkg/auth` `CheckPermissions` tests and `contrib/auth/acl/service_test.go` keep passing.
- **Manual:** Entra login → user lands in `Viewers` through the mapping → admin attaches a repo-scoped policy → the
  change applies without logging out (§4.7).

---

## 5. Option 3 — gap fill

Each item is small and independent, and lands after Option 2. Order is by value.

### 5.1 `StringEquals` / `StringNotEquals` conditions

The docs list them, and `condition.go` lacks them. Add two operators that use exact comparison. They reuse
`StringMatchOperator` with a `match func(pattern, value string) bool`, and validation works the same way.
`OperatorFactory` gets two new cases.

### 5.2 Condition context beyond `SourceIp`

`AuthorizationRequest` gets `Fields func(key string) (string, bool)`, a lazy provider, and `ConditionContext` uses it
for unknown keys. A missing key evaluates to false (doc semantics).

Phase 1:

- `lakefs:RepositoryMetadata/<k>`, resolved in `authorizeCallback` from the `repository` path parameter through
  `Catalog.GetRepositoryMetadata`. The lookup is lazy, so it runs only when a matching statement has the condition,
  and is memoised per request.
- The gateway does the same in `handler.go:255` from the bucket name.

Phase 2 (optional):

- `lakefs:ObjectMetadata/<k>`, for `fs:ReadObject` on get, head and stat only. The handler already has the entry, so
  it passes the metadata into the request.
- Listings are **not** filtered (the same documented limitation as Enterprise).

### 5.3 Branch listing filtered by policy

`ListBranches` (`controller.go:2921`) authorizes on the repository ARN and returns every branch. Mirror the
`ListRepositories` approach:

- if the user has `fs:ListBranches` on the repository ARN, return everything;
- otherwise, if they hold it on some `…/branch/*` resource, filter with `auth.CheckPermission(BranchArn(repo, b))`;
- otherwise return 401.

Explicit branch denies hide branches. This is visibility only; per-branch actions are still checked individually.

### 5.4 New action services

Add `datasets` to `permissions.serviceSet`. This is already planned in `DATASETS_DESIGN.md`; it is listed here so both
designs agree on the ARN prefix: `arn:lakefs:datasets:::dataset/<name>`, matching that design rather than the
Enterprise doc's `dataset:` singular. Also add the `Datasets*` built-in policies to `EnsureRBACBaseGroups` when the
datasets feature is enabled.

### 5.5 403 for authorization failures (optional, off by default)

Today a denial returns 401 (`ErrInsufficientPermissions`), or 500 when `Allowed=false` without an error
(`controller.go:6282`).

- Fix the 500 to 401 unconditionally.
- Add `auth.authorization_failure_status: 403` as an opt-in. The default stays 401, because the UI and SDK clients
  treat 401 without the authentication message as `AuthorizationError` (`webui/src/lib/api/index.js:84`) and the
  swagger declares no 403.

---

## 6. Rollout

1. **Phase A (both modes):**
   - §4.5 template fixes (commit `adbec67e6`);
   - §4.6 profile API and §4.7 cache invalidation (`1f8fdd760`, on the cache primitives in `d2cb4521a`);
   - the setup/superuser builder fix (`40698e6ff`);
   - the idempotent base-group helpers the others build on (`7d05c21be`).

   Shippable alone, and it fixes Writers not being able to open PRs — **on fresh installs**. It does not
   repair an installation whose KV already holds ACL policies computed from the old templates: the
   `ReconcileACLPolicies` call that rewrites them lives in the auth builder and ships with Phase B (the
   reconciler in `20881d6c4`, its startup wiring in `7d5871840`). An earlier version of this section listed
   "reconcile" under Phase A; it was delivered in Phase B, so a Phase-A-only build leaves stale policies in
   place.
2. **Phase B:**
   - the ACL template reconciler and its startup wiring, including the mode resolution that gates it
     (`20881d6c4`, `7d5871840`) — this is the part that upgrades existing installations;
   - the `internal` mode with the `rbacAuthService` wrapper (`7d5871840`);
   - RBAC bootstrap in `internal` mode, versioned once per template (`da75fd5f2`, on the A2 helpers
     `7d05c21be`);
   - the `rbac-migrate` command (`36f68c8a3`);
   - the `SetGroupACL` gate (`f22e0595c`);
   - docs (`50bc2bba9`).

   Opt-in through `auth.ui_config.rbac: internal`.
3. **Phase C:** Option 3 items as needed. §5.1 and §5.3 are first; §5.2 phase 1 comes next.
4. Default stays `simplified`. Reconsider defaulting to `internal` after one release.

Phase independence still holds: the Phase A commits build, vet and pass their package tests on top of the
base commit without any Phase B code (`DESIGN_CONFORMANCE.md` §4.3). What they cannot do on their own is
change data that was written before them.

## 7. Out of scope

- **Tenants** (`auth:*Tenant*`, tenant segment in `fs:` ARNs): OSS has no tenant model.
- **Audit log as a resource** (`audit:` actions, `arn:lakefs:audit:::log`): Option 2 enables structured audit request
  logging, which is enough for log shipping — provided the level is configured. Those lines are written at
  `logging.audit_log_level`, which defaults to `DEBUG`, and the default `logging.level` is `INFO`, so out of the box
  they are filtered and produce no output. Set `logging.audit_log_level` at or below `logging.level` (for example
  `audit_log_level: INFO`) to see them; see `CHANGELOG_SSO.md` and `cmd/lakefs-sso/config.example.yaml`.
- **External principals / AWS IAM login:** `acl.AuthService` returns `ErrNotImplemented`. It could be added later
  over the KV schema that already exists.
- **`admin:Login`:** Cloud only.
- **A remote authorization service (`external`):** removed upstream, and not needed in-process.

## 8. Related, but not part of this design

Security fixes that upstream shipped only under BSL need clean-room equivalents. They are **out of scope for
this branch and are not tracked here**: nothing in this repository's task board (`tasks.json`), ledger
(`agent-progress.txt`) or the commit range `a25abca09..HEAD` covers them, so this section is a reminder, not a
plan. Whoever picks them up should create the cards before starting, and mark each one clean-room (rule 2 in
§2):

- outbound hook requests to internal addresses (cf. `a6d4f1345`);
- SigV4 signed-header validation in the S3 gateway (cf. `ffaa219b4`).

## 9. Open questions

1. Should `rbac-migrate` merge ACL `Admins` into RBAC `Admins` in place (proposed), or create `Admins` fresh and
   rename the old one to `Admins.acl`?
2. Should break-glass admin membership also include `SuperUsers` in `internal` mode? Proposed: no, `Admins` is enough.
3. Is a 23s worst-case cross-replica revocation delay acceptable, or do we need KV-backed cache invalidation
   (generation counter per user)?
