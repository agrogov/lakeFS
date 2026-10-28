# R1 — Independent code review: policy RBAC delivery (cards A1–A6, B1–B6, C1–C5)

Review-only card (auto-review intentionally OFF — this card produces this report, no code).
Branch: `feature/azure-entra-sso-apache`, range `a25abca09424d660bc659d0b35340329877a6390..HEAD`
(17 implementation commits + kanban commits), reviewed against `RBAC_DESIGN.md`.

**Verdict: changes requested.** One blocker (introduced by C3) and two majors
(revocation staleness, remaining condition-context gaps). The rest of the delivery is
sound: the RBAC wiring, idempotent bootstrap, reconcile, migration and the 401/409/403
gates behave as designed, and every test suite listed in the card passes.

---

## 1. What was verified (not just read)

| Check | Command / method | Result |
|---|---|---|
| Build | `go build ./...` | pass |
| Go tests (as scoped by the card) | `go test -count=1 ./pkg/auth/... ./pkg/sso/... ./pkg/api/... ./contrib/auth/acl/... ./pkg/cache/... ./cmd/... ./pkg/gateway/... ./pkg/permissions/...` | all pass |
| UI tests | `cd webui && npm test` | 6 files / 72 tests pass |
| UI lint | `cd webui && npm run lint` | pass |
| gofmt | `gofmt -l` on all changed `.go` files | clean (only the deleted test file path reported) |
| Generated permissions | `cd pkg/permissions && go generate ./...` | no diff (deterministic) |
| BSL licence guard | `git merge-base --is-ancestor 358544dd7 HEAD` | not an ancestor — clean (§2 rule 2) |
| Cache-revoke behaviour | ad-hoc probe (temp test, removed) against `acl.AuthService` with cache enabled | 4/6 pass, 2 failures → findings M1 below |
| Condition evaluation on `ListBranches` | ad-hoc probe (temp test, removed) | failure → finding B1 below |

Not run: `-race`, the full `make test-go` suite, `make validate-mockgen` (no generated
mock covers `auth.Cache`/`cache.Cache`, so the interface change does not invalidate a mock).

---

## 2. Findings

### B1 — BLOCKER: `ListBranches` evaluates policies without a condition context → conditional denies are bypassed, conditional allows are lost

`pkg/api/controller.go:2979` (`ListBranches`), `pkg/api/controller.go:2929` (`hasBranchPermission`),
`pkg/api/controller.go:3034-3045`.

C3 replaced the `c.authorize(...)` call on the policy path with direct
`auth.CheckPermissions(ctx, ...)` calls, where `ctx := r.Context()` (`:2980`). The only
place that builds a `auth.ConditionContext` (client IP + `lakefs:RepositoryMetadata/*`
provider) is `authorizeCallback` (`:6373`), and it deliberately keeps that context local to
its own `Authorize` call — it is never attached to the request. Consequently every
condition in a policy is evaluated with a nil condition context on this endpoint, and a
condition then evaluates to false.

Reproduced on a scratch test (removed after the review), repo with branch `main`:

* allow `fs:ListBranches` on `repository/<repo>` **conditioned on `IpAddress`/`SourceIp`
  `203.0.113.5/32`**, request from `203.0.113.5` → `401 insufficient permissions`
  (expected: `200` + branches). The same policy evaluates `CheckAllow` when a
  `ConditionContext` is present (verified directly).
* unconditional allow + **deny conditioned on the same `SourceIp`**, request from that IP
  → `200` **with the branches listed**. The identical node returns `CheckDeny` when a
  condition context is present (verified directly). This is an authorization bypass: a
  conditional deny that used to be enforced (see below) is silently ignored.

Why this matters beyond "IP policies":

* it defeats C2 for this endpoint — a `lakefs:RepositoryMetadata/<k>` conditioned grant is
  unusable on `ListBranches`, because `repositoryMetadataFields` is only reached through
  `authorizeCallback`;
* before C3 the endpoint went through `authorizeCallback`, and `APIAuthService.Authorize`
  has always built the condition context from `req.ClientIP`
  (`pkg/auth/service.go:1027`), so for remote-auth deployments this **is a regression**
  introduced by C3, not a pre-existing limitation;
* `hasBranchPermission` (`:2929`) is a hand-rolled, allow-only, condition-blind and
  deny-blind re-implementation of policy evaluation (`:2933-2968`). It decides only the
  401-vs-200 gate, so it cannot see the conditioned deny either. Re-implementing evaluation
  next to `CheckPermissions` is the root cause and should be removed rather than patched.

Suggested fix (small, in-scope for a follow-up card):

1. Factor the condition-context construction out of `authorizeCallback` into a helper that
   takes `(r, repository)` and is used by both call sites; attach it to a derived
   `*http.Request` so downstream `CheckPermissions` calls see it.
2. Evaluate deny-first: `CheckPermissions` on `permissions.RepoArn(repo)` returning
   `CheckDeny` ⇒ fail the request (401/403) regardless of any branch-level allow; only then
   decide `repoAllowed` from `CheckAllow`, and drop `hasBranchPermission` in favour of the
   per-branch filter that already exists (`:3034`), which correctly hides deny and neutral
   results.
3. Add the missing regression tests: conditioned allow (allowed), conditioned deny
   (blocked), and a `RepositoryMetadata`-conditioned grant.

### M1 — MAJOR: `DeleteUser` does not invalidate any cache → deleted users stay authorized, and a re-created username inherits the old grants

`contrib/auth/acl/service.go:120-168` (`DeleteUser`); cache primitives available at
`pkg/auth/cache.go:87-98`.

`DeleteUser` detaches the user's policies and group memberships through the
`...NoValidation` helpers and then deletes the record, but never calls
`InvalidateUser`/`InvalidateUserPolicies`. Both are now available and used everywhere else
(`:313, :337, :538, :585, :605, :706, :775, :873, :892`).

Measured on a cache-enabled `acl.AuthService` (temp test, removed):

* after `DeleteUser("erin")`: `GetUser("erin")` still returns the user and
  `Authorize("erin", fs:ReadObject)` still returns `allowed=true` — for the whole
  `auth.cache.ttl`;
* delete `frank` (who had a policy attached), re-create `frank`: the **new** user is
  immediately allowed by the deleted user's effective policies, because `policyCache` is
  keyed by username, not user ID. That is a privilege-escalation window for the
  delete-and-recreate flow (including "reset a user by deleting and re-adding them").

Bounded by the TTL (20s + jitter by default), and pre-existing rather than introduced by
this branch, but it is squarely inside the review focus ("cache staleness on revoke",
"§4.7 every mutation path invalidates") and trivially fixable with the primitives A3 added.

Fix: capture the user returned by the existing `GetUser` at the top of `DeleteUser` and
`defer` `s.cache.InvalidateUser(auth.UserKey{Username: username}, auth.UserKey{Email: ...},
auth.UserKey{ExternalID: ...})` plus `s.cache.InvalidateUserPolicies(username)`.

### M2 — MAJOR: two authorization call sites bypass the C2 condition plumbing, so conditional denies are not enforced there

1. `pkg/gateway/operations/deleteobjects.go:77` — S3 multi-object delete authorizes each
   object with `o.Auth.Authorize(req.Context(), ...)`. The gateway's `authorize()`
   (`pkg/gateway/handler.go:283-290`) builds the condition context on a local `ctx` only and
   never stores it on `req`, so bulk delete evaluates every condition with a nil context.
2. `pkg/api/controller.go:5246` — `StatObject` with `presign=true` re-authorizes
   `fs:ReadObject` on the handler `ctx`, again without the condition context, so a
   conditional deny does not stop the pre-signed URL from being issued.

Both are fail-open for conditional denies and were left behind when C2 fixed the main
paths. In-process ACL mode had no condition context on these paths before either, so this
is a C2 completeness gap rather than a new regression; the fix is to attach the condition
context to the request (as suggested in B1) so every nested `Authorize` call inherits it.

### m1 — MINOR: no service-level "revoke is immediate" test

Design §4.10 requires "cache invalidation (revoke, then immediate `Authorize` → deny)";
B1's `files_expected` lists `contrib/auth/acl/service_test.go`, which is untouched. Only
the cache primitives are unit-tested (`pkg/auth/cache_test.go`). The 4 passing probes above
suggest the wiring is right, but the security-relevant property is currently asserted
nowhere. Recommend adding table-driven cases for detach-user-policy, remove-from-group,
detach-group-policy and delete-group (and, after M1, delete-user).

### m2 — MINOR: `auth.authorization_failure_status` is unvalidated and untested

`pkg/config/config.go:698`, `pkg/api/controller.go:6405-6414`.

Any integer is returned to `writeError` as-is, so a typo such as `200` turns an
authorization failure into a `200` response carrying an error body — which the UI and SDKs
will read as success. Restrict to `{401, 403}` at config validation time (returning a clear
error at startup). There is also no test for the opt-in path at all (C5's validation was
`go test ./pkg/api/...`, i.e. pre-existing tests only), and the new setting is not
mentioned in `cmd/lakefs-sso/config.example.yaml` or `CHANGELOG_SSO.md`.

### m3 — MINOR: reconcile/bootstrap use two different version keys, and the comment claims otherwise

`pkg/sso/rbac_bootstrap.go:26-29` states the version key "is read with the same name by the
ACL policy reconciler"; it is not — `contrib/auth/acl/reconcile.go:22` uses its own
`acl_template_version`, stored through `model.MetadataKeyPath` (`:49`), while
`rbac-bootstrap-version` is stored raw in the same partition. `RBAC_DESIGN.md` §4.5 #2
asked for the *same* version key. Functionally both are once-per-version guards, so this is
documentation/design drift plus an inconsistent key convention inside the `auth` partition
(`metadata/acl_template_version` vs `rbac-bootstrap-version`). Fix the comment and pick one
convention.

Related nit: `statementsEqual` compares `model.Statement.Resource` as a raw string
(`reconcile.go:129`); the field holds a JSON array, so a differently-ordered encoding would
trigger a needless (harmless) rewrite.

### m4 — MINOR: A6 has no test and one extra file; C5's 500→401 fix is untested

* A6 (`8d5c7c65c`) also changes `webui/src/lib/api/index.js` (beyond
  `files_expected`), which is justified — `NoAclError` needs the 404 body — but no webui
  test covers the new badge, and `npm test` exercises only the pre-existing suites.
* The C5 `!resp.Allowed` → 401 branch (`controller.go:6391`) and the 409 gate ordering have
  no test; only `TestSetGroupACL` and the pre-existing controller tests run.

### m5 — MINOR: datasets wiring is half-connected (C4)

`pkg/auth/setup/setup.go:25-46, 125-140, 200-215`.

`DatasetsFullAccess`/`DatasetsReadAll` policies are created by `CreateRBACBasePolicies`
**unconditionally** (so every installation gets two unattached datasets policies in the
Policies UI), while the attachments are behind `WithDatasetsEnabled`, which has **no
production caller** (`EnsureRBACBase` does not pass it). That is consistent with the card
note "blocked on datasets feature landing", but the immediate effects are: orphan policies
on clusters without the feature, and a bootstrap option that cannot be reached yet.
Also `pkg/permissions/actions.gen.go` gains no `datasets:*` action constants, so the action
picker (`permissions.Actions`) will not offer datasets actions; `IsValidAction` only
validates the service prefix. Fine to defer — worth recording.

### m6 — MINOR: the §2.5 licence guard script does not exist

`RBAC_DESIGN.md:89` mandates `scripts/check-license-base.sh` (fail if `358544dd7` becomes an
ancestor); the file is absent and no card covers it. The branch itself is clean (verified
with `git merge-base --is-ancestor`), and no code in the diff looks traceable to the BSL
commits called out in §2.2 (no SigV4 signed-header validation, no hook SSRF filter), but
the automated guard the design promised is still missing.

### Nits

* `contrib/auth/acl/service.go:264` — `UpdateUserProfile` invalidates the cache even when
  `changed == false` (the `defer` is registered before the check at `:266-276`). Move it
  after the `if !changed { return nil }`.
* `pkg/api/controller.go:6369-6377`, `pkg/gateway/handler.go:281-284` — comments say the
  metadata lookup happens "at most once per request"; the `sync.Once` is per
  `authorize()`/`authorizeCallback()` invocation, so a request that authorizes twice loads
  metadata twice.
* `pkg/api/controller.go:1289-1292` — the `SetGroupACL` 409 gate runs before
  `c.authorize`, so an anonymous/unauthorised caller learns the RBAC mode (401 → 409).
  Matches the design ("the same way as the policy endpoints"), so no action needed beyond
  noting it; a mode-independent 401 first would be tidier.
* `pkg/api/controller.go:2949-2951` — the `hasBranchPermission` wildcard probe
  (`strings.NewReplacer("*","x","?","x")`) is a heuristic; it is also condition-blind (see
  B1). Remove it when B1 is fixed.
* `pkg/api/controller.go:2997` — for `APIAuthService` (remote auth) `ListBranches` now
  evaluates policies locally instead of asking the remote service to authorize. Same pattern
  as the pre-existing `ListRepositories`, so acceptable, but it is a behaviour change for
  external-auth deployments.
* `pkg/catalog/list_repository_options.go:44-52` — the pre-existing `ListRepositories` filter
  also calls `CheckPermission(..., nil)`, i.e. the same condition-blindness. Out of scope
  here, but it is the reason the C3 pattern looked safe to copy; worth one card for both.
* `pkg/sso/rbac_bootstrap.go:25` — `RBACBootstrapVersion` was not bumped by C4 although the
  base template changed (new datasets policies). Harmless only because the additions are
  unattached; keep the rule "template change ⇒ bump" in mind.
* `webui/src/lib/api/index.js:276-290` — both the `no_acl` body flag and a message match are
  supported; the API always sends `no_acl` (`apigen.NotFoundOrNoACL`), so the message
  fallback in `groups/index.tsx:107-111` is redundant. Harmless.

---

## 3. Scope hygiene (commit ↔ card ↔ `files_expected`)

Card→commit map from `agent-progress.txt` is authoritative; the commit subjects are indeed
mislabeled as flagged (`7ef078aa7`=B1, `361b59e4e`=C4, `6cdc5d902`=C1, `fed01838c`=A5).
Verified per-commit file lists:

| Card | Commit | files_expected match | Notes |
|---|---|---|---|
| A1 | `9eacd521d` | exact | — |
| A2 | `7bfbe8b17` | exact | — |
| A3 | `6f18e72bc` | exact | — |
| A4 | `728493845` | exact | — |
| A5 | `fed01838c` | exact | — |
| A6 | `8d5c7c65c` | +1 file | `webui/src/lib/api/index.js` (justified, see m4) |
| B1 | `7ef078aa7` | −1 / +2 | `contrib/auth/acl/service_test.go` **not** touched (see m1); also edits `pkg/sso/auth_builder.go` (B3's file) to drop `builtKVStore`, and deletes `user_profile_internal_test.go` |
| B2 | `e99510278` | exact | — |
| B3 | `7babc8e13` | +1 file | deletes `auth_builder_internal_test.go` (obsolete `ensureSimplifiedRBAC` test) — fine |
| B4 | `50ac672ea` | exact | — |
| B5 | `135d1710f` | exact | — |
| B6 | `06c258da6` | exact | — |
| C1 | `6cdc5d902` | exact | — |
| C2 | `0009401de` | +1 file | `pkg/auth/condition.go` (C1's file) — needed for `FieldsProvider`/`Lookup`; collision note only |
| C3 | `9efa2c279` | exact | — |
| C4 | `361b59e4e` | +2 files | both tests — fine |
| C5 | `b2086304d` | exact | `api/swagger.yml` correctly untouched |

No commit touches files outside its card's blast radius without a stated reason; the
`controller.go` serialisation (A5→C2→C3→C5) and the `auth_builder.go` overlap (B1↔B3) were
handled as the ledger describes.

---

## 4. Point-by-point on the review focus

**`pkg/auth` condition/authorization logic — OK.** `StringEquals`/`StringNotEquals`
(`condition.go:239-247`) reuse `StringMatchOperator` with an explicit `match` func; `nil`
`match` still means wildcard, so `StringLike` semantics are preserved. `ConditionContext.Lookup`
(`:44-57`) also fixes a latent nil-pointer panic when no condition context is in the ctx
(missing field now denies instead of panicking). Deny-wins in `CheckPermissions` is
untouched.

**Cache invalidation (§4.7) — mostly OK, with M1.** All enumerated paths invalidate:
`AddUserToGroup`, `RemoveUserFromGroup`, `Attach/DetachPolicyToUser` (per user),
`Attach/DetachPolicyToGroup`, `WritePolicy(update)`, `DeletePolicy`, `DeleteGroup` (purge).
`WriteGroupACL` (`contrib/auth/acl/write.go`) goes through `WritePolicy` +
`Attach/DetachPolicyFromGroup`, so the `SetGroupACL` path invalidates too. `DeleteUser` is
the gap (M1). `UpdateUserProfile` invalidates the `{Username}`, `{Email}` (old and new) and
`{ExternalID}` keys, which matches exactly how `GetUser`/`GetUserByEmail`/`GetUserByExternalID`
cache — correct and better than the design asked for.

**`contrib/auth/acl` profile API + templates + reconcile — OK.** `UpdateUserProfile` keeps
the `kvProfileStore` semantics (`SetMsgIf` with predicate, tolerating a lost race) and now
invalidates; `kvProfileStore` is gone. ACL templates match §4.5 #1 (`catalog:*` in
`AllAccess`; `PRReadWrite`+`CatalogReadWrite` for Write/Super; `CatalogRead` for Read).
`ReconcileACLPolicies` only rewrites policies with a non-empty ACL, compares
semantically, is version-guarded, records the version only after success, and updates
through `WritePolicy(update=true)` (so it also purges the policy cache). Convergence and
idempotence are exercised by `contrib/auth/acl/reconcile_test.go`.

**`pkg/api` gates — A5 OK, C3 has B1, C5 OK modulo m2.** `SetGroupACL` returns 409 before
any lookup when the mode is not `simplified` (`:1289`), which protects the "silently
detaches every other policy" hazard; `lakectl auth groups acl set` surfaces it verbatim.
`authorizeCallback` now builds the lazy metadata provider and passes the same closure as
`Fields` (`:6373-6382`) — verifying both consumption styles (services that read the context
and services that read the request) is correct and well tested
(`pkg/api/controller_fields_test.go`, `pkg/gateway/authorize_fields_test.go`). Memoisation
is per-call, not per-request (nit). The `!resp.Allowed` → 401 fix is right.

**`pkg/gateway` parity — main path OK, M2 for bulk delete.** `authorize()` resolves the
bucket from `ContextKeyRepositoryID` (set by `EnrichWithParts`, `middleware.go:85`) and uses
the same lazy provider as the API; the implicit "no bucket ⇒ metadata conditions deny"
behaviour is correct.

**`pkg/sso` — OK, with m3.** `resolveAuthMode` implements the §4.1 table and normalises the
shared `uiConfig` pointer (so setup and `/setup_lakefs` see the same value); `external` and
unknown values warn and fall back. `rbacAuthService` overrides `IsAdvancedAuth`, and the
flag survives `NewMonitoredAuthService` (generated wrapper forwards it,
`service_wrapper.gen.go:18`) and `authServiceWithProfiles`. `EnsureRBACBase` is
version-gated, records the version only after a fully successful bootstrap, never
downgrades a newer stored version, and is tolerant of admin-edited built-ins. `rbac-migrate`
is a dry run by default, `--yes` applies, `--cleanup` removes the emptied legacy groups and
their ACL policies only after the members were copied, re-runs are no-ops (verified by its
tests), and it prints the `sso.group_mapping` rewrites plus the rollback warning.
`--cleanup` survives a restart because `SetupACLServer` (`contrib/auth/acl/setup.go:53-63`)
and `EnsureACLBaseGroups` both return early once initialised. `EnsureUser` now tries
`GetUser` (single KV read) before the `GetUserByExternalID` full scan, as §4.6 asked.

**Security: `IsAdvancedAuth` leak — none found.** In `simplified` mode the built service is
the plain `acl.AuthService` (false) wrapped by two forwarders, so audit logging stays off.
Only `internal` mode returns true, which is the intent. `401/403` handling: the previous
500 (`controller.go:6391`) is now 401; `ErrInsufficientPermissions` maps through
`authorizationFailureStatus()`, which defaults to 401 and is only overridable by explicit
config (m2). No path was found where a denial renders as success other than by
misconfiguration.

**Licence guard (§2) — clean.** No BSL commit is an ancestor; the diff contains no code
traceable to `a6d4f1345` (hook SSRF), `ffaa219b4`/`245cb66af` (SigV4 signed headers) or
`24bfa9031`; `LICENSE`/`NOTICE`/`THIRD_PARTY_LICENSES` untouched. Gap: the automated guard
from §2.5 was never added (m6).

---

## 5. Follow-up cards

Recorded in `tasks.json` as `R1-F1` … `R1-F6` (R1-F1 is the blocker and should gate any
release of `rbac: internal`):

| Card | Severity | Summary |
|---|---|---|
| R1-F1 | blocker | `ListBranches`: attach the condition context (shared helper) and make the 401/200 decision deny-aware; drop `hasBranchPermission`; add conditioned allow/deny + metadata tests |
| R1-F2 | major | `DeleteUser`: invalidate user + policy caches; add revoke test (delete → deny, recreate → no inheritance) |
| R1-F3 | major | Attach the condition context to the request so nested `Authorize` calls (`gateway/operations/deleteobjects.go`, `controller.go` presign stat) inherit it; parity tests |
| R1-F4 | minor | Validate `auth.authorization_failure_status ∈ {401,403}`, add a test, document the setting |
| R1-F5 | minor | B1 test gap: service-level cache-invalidation suite in `contrib/auth/acl/service_test.go` (§4.10) |
| R1-F6 | minor | Add `scripts/check-license-base.sh` + CI wiring (§2.5) |
