# Design conformance: `RBAC_DESIGN.md` vs `feature/rbac-policy-engine`

Review-only. No source, test, ledger or design file was changed. Findings about the design itself are listed as gaps.

| Item | Value |
|---|---|
| Design | `RBAC_DESIGN.md` (431 lines, Draft, dated 2026-10-28) |
| Branch under review | `feature/rbac-policy-engine`, HEAD `ba109abe7` (F4 commit); 24 commits over `a25abca09` |
| Diff size | 55 files, +5257 / -205 (includes `REVIEW_R1.md`, tests, CI, webui) |
| Review date | 2026-10-29 |
| Toolchain | `export PATH=$HOME/sdk/go1.25.8/bin:$PATH` resolves to **go1.26.4 darwin/arm64** (toolchain switch). macOS, so no `timeout`, BSD `sed`. |
| Worktree | detached HEAD at the branch tip. The tree was clean after every probe and mutation (`git status --short` empty). |

**Status legend:** IMPLEMENTED, PARTIAL, DEVIATION-JUSTIFIED, MISSING, OUT-OF-SCOPE.
**Severity legend:** BLOCKER (unsafe to ship), MAJOR (a promised behaviour is broken or its core wiring is unguarded), MINOR (works, but a test, doc or edge is missing), NIT.

## 1. Verdict

**107 matrix rows:** 84 IMPLEMENTED, 15 PARTIAL, 4 DEVIATION-JUSTIFIED, 2 MISSING, 2 OUT-OF-SCOPE. **20 gaps:** 0 BLOCKER, 2 MAJOR, 14 MINOR, 4 NIT. (R105–R107 repeat the R1 open items from earlier rows, so they are counted twice in the row totals.)

- **No BLOCKER.** The core behaviour the design promises was exercised by execution and works: mode resolution, bootstrap order, break-glass, policy CRUD, repo/path/branch/IP/ABAC grants, immediate revocation, `rbac-migrate`, the 409 gate, and the Phase A independence builds. The PARTIAL rows are the exceptions, and each one is in the gap list.
- **2 MAJOR gaps:**
  - `lakefs-sso setup` fails on a fresh KV when SSO is enabled, unless `--no-check` is passed. The design says routing `setup` through `authServiceBuilder` fixes this (GAP-01).
  - The §4.2 wiring in `BuildAuthService` and `setup.go` has no committed test. All six wiring mutants survive the full `./pkg/sso` and `./cmd/lakefs/cmd` suites (GAP-02). A test that ran `setup` through the builder would have caught GAP-01.
- R1 re-check: F1–F6 close B1, M1, M2 (see §3 for the redundant-site caveat), m1, m2 and m6. **m3, m4 and m5 are still open.**
- Several design statements are stale or wrong. They are in the gap list (GAP-04, 06, 16, 20).

## 2. Requirement matrix

Evidence conventions: `file:line` are at HEAD `ba109abe7`. "probe" means a scratch test or binary run that was deleted afterwards (§5). "mutant" means revert-the-fix testing (§4.6). Severity is shown only when the status is not IMPLEMENTED.

### §1 Review of the latest commits

| ID | Design § | Requirement | Status | Evidence | Severity |
|---|---|---|---|---|---|
| R01 | §1.1 #1 | Profile writes go through the acl service API, and the write invalidates the auth cache | IMPLEMENTED | `contrib/auth/acl/service.go:259,265` (`UpdateUserFriendlyName` delegates to `UpdateUserProfile`; deferred `InvalidateUser` on old/new email, username, external ID). Probe: friendly name "" → "New Name"; `GetUserByEmail(old)` not found, `(new)` found; `UpdateUserFriendlyName` on a cached user returns the new name and keeps the email. **No committed test** (GAP-11). | — |
| R02 | §1.1 #1, #2, #5 | `kvProfileStore` and `builtKVStore` are gone; `profileUpdater` is satisfied by the service | IMPLEMENTED | `pkg/sso/auth_builder.go:29` `authServiceWithProfiles{profileUpdater: aclService}`. `grep -rn "kvProfileStore\|builtKVStore\|hasBranchPermission\|ensureSimplifiedRBAC" --include=*.go .` gives 0 hits. | — |
| R03 | §1.1 #2 | `pkg/sso` no longer depends on the ACL user KV layout for profiles | IMPLEMENTED | Profile path removed. Remaining `model.UserPath`/`PartitionKey` uses are `pkg/sso/migrate.go` (pre-existing `sso-migrate`) and `rbac_bootstrap.go` (the version key, which §4.3 specifies). Reading only. | — |
| R04 | §1.1 #3 | Skip `#EXT#` UPNs when deriving the email | MISSING | No `EXT#` handling anywhere. The design lists it as a follow-up, so status only. | n/a (listed follow-up) |
| R05 | §1.1 #4 | Committer change (oid → email, new commits only) is documented in the SSO changelog | MISSING | `grep -n -i committer CHANGELOG_SSO.md` gives no hits. Not mentioned in the base changelog either. | MINOR (GAP-14) |
| R06 | §1.1 #5, §4.6 | `EnsureUser` tries `GetUser(externalID)` first, then `GetUserByExternalID`, then creates | IMPLEMENTED | `pkg/sso/user.go:34`. Tests: `TestEnsureUser_ExistingUserUntouched` (`user_test.go:84`), `_FallsBackToExternalIDLookup`, `_ConcurrentCreateIsNotAnError`, `_CreateFailureReported`. Mutant (remove the `GetUser` block) fails `_ExistingUserUntouched`. | — |

### §2 Licensing

| ID | Design § | Requirement | Status | Evidence | Severity |
|---|---|---|---|---|---|
| R07 | §2.2 | No BSL commit is an ancestor of HEAD | IMPLEMENTED | `git merge-base --is-ancestor X HEAD` says "no" for `358544dd7`, `a6d4f1345`, `ffaa219b4`, `245cb66af`, `24bfa9031`. `bash scripts/check-license-base.sh` prints "OK: 358544dd7 (BSL 1.1 relicense) is not an ancestor of HEAD ba109abe7.", rc 0. | — |
| R08 | §2.2 | Nothing is copied from the BSL commits | IMPLEMENTED | Content comparison: 1,846 unique added lines of ≥30 chars. Overlap with `a6d4f1345` 3 lines, `245cb66af` 2, `ffaa219b4` 0, `24bfa9031` 0. All 5 shared lines are boilerplate already present in the Apache base (an import, `t.Run(tt.name, …)`, `secret_key: "required in config"`). The `Validate()` hook in `config.go` has different content from `a6d4f1345`'s. Keyword grep (ssrf, sigv4, tenant, audit) on added lines: 0 hits. | — |
| R09 | §2.1, §2.3 | No Enterprise/"Fluffy" code or doc prose copied | IMPLEMENTED | Not provable by execution. Only signal: the word "enterprise" appears in 2 added comments (singular `dataset:` ARN note). No contrary evidence. | — |
| R10 | §2.4 | `LICENSE`, `NOTICE`, `THIRD_PARTY_LICENSES` unchanged; no new header convention | IMPLEMENTED | `git diff --name-only a25abca09..HEAD` shows none of them. No header added to new files. | — |
| R11 | §2.5 | CI guard fails when BSL becomes an ancestor | IMPLEMENTED | `scripts/check-license-base.sh` (bash 3.2-safe, `set -euo pipefail`, cwd-independent via `git -C`), target `make check-license-base` = `--self-test` + guard, wired into `checks-validator`. `--self-test` passes 6 cases: clean passes, clean passes from a subdirectory, BSL ancestor exits 1, absent commit in a complete clone passes (conclusive), shallow clone **fails closed** (exit 1), shallow clone honours the documented opt-out. **Fails closed** wherever the ancestry cannot be decided (shallow clone, unborn HEAD, no repository) with the remedy printed; the only escape hatch is `LICENSE_GUARD_SKIP_IF_INCONCLUSIVE=1`, which CI never sets. `.github/workflows/test.yaml`: `license-base` job (fetch-depth 0 + upstream master fetch) runs `make check-license-base`; the `validator` job now checks out with `fetch-depth: 0` because `checks-validator` runs the guard. Failure path proven with `BSL_COMMIT=$(git rev-parse 4bb11638e)`. | — |

### §3.3 Defects in the ACL model

| ID | Design § | Requirement | Status | Evidence | Severity |
|---|---|---|---|---|---|
| R12 | §3.3 #1, §4.5 #1 | ACL `Write` can create/merge PRs | IMPLEMENTED | Probe (simplified, cache on): Writers `pr:WritePullRequest` true. Fresh-KV dump: Writers n=22 with `pr:*`, vs n=20 at base. `TestACLToStatementActions` (`permission_test.go:17`). | — |
| R13 | §3.3 #2, §4.5 #1 | ACL `Admin` has `catalog:*` | IMPLEMENTED | `pkg/auth/base.go:19`. Probe: Admins `catalog:CreateTable` true; KV dump Admins n=7 (6 at base). | — |
| R14 | §4.5 #1 | Read gains `CatalogRead`; Super gains `PRReadWrite` and `CatalogReadWrite` | IMPLEMENTED | `contrib/auth/acl/permission.go:35,57,79`. Probe: Readers `catalog:ListNamespaces` true, `catalog:CreateTable` false, `pr:WritePullRequest` false; Supers both true. | — |
| R15 | §4.5 #1 | The fixed ACL templates "match what Viewers, Developers and SuperUsers get in RBAC" | PARTIAL | Parity probe (4 ACL/RBAC group pairs × 15 actions, internal mode): identical except `pr:WritePullRequest` for Super (ACL Super true, RBAC `SuperUsers` false). RBAC `SuperUsers` = `FSFullAccess, CatalogReadWriteAll, AuthManageOwnCredentials, RepoManagementReadAll` at base and HEAD (`pkg/auth/setup/setup.go:199`, asserted at `setup_test.go:63`). `TestACLToStatementRBACParity` (`permission_test.go:121`) allow-lists the difference through `extraPolicyType: PRReadWrite`. | MINOR (GAP-06) |
| R16 | §3.3 #3, §4.5 #2 | Stale ACL policies in an existing KV are rewritten | IMPLEMENTED | Upgrade smoke: KV created by the base binary, started by HEAD: Writers `pr:*` and `catalog:*` false → true, n=20 → 22; Supers 13, Readers 13, Admins 7. Same KV on the Phase-A-only binary stays stale (see R98). `TestReconcileACLPolicies_FixesStalePolicy` (`reconcile_test.go:46`). | — |
| R17 | §3.3 #4, §4.5 #3 | A custom policy is no longer destroyed by `SetGroupACL` once RBAC is on | IMPLEMENTED | 409 gate `pkg/api/controller.go:1288` (R51). In `simplified` mode `SetGroupACL` still detaches foreign policies (probe: Readers ends with `[ACL(_-_)Readers]` only). §3.3 #4 accepts this for ACL-only deployments. | — |
| R18 | §3.3 #5, §4.5 #4 | A multi-policy group returns 404 with `no_acl`, and the UI shows it as custom | IMPLEMENTED | `controller.go:1259,1272` (`NotFoundOrNoACL`). Probe: `GetGroupACL(Readers+extra)` 404 `{"message":"Multiple policies attached to group - no ACL","no_acl":true}`. See R52 for the UI. | — |

### §4.1 / §4.2 Modes and `BuildAuthService`

| ID | Design § | Requirement | Status | Evidence | Severity |
|---|---|---|---|---|---|
| R19 | §4.1, §4.2 #1 | `none` or empty → `simplified` | IMPLEMENTED | `TestResolveAuthMode` (`pkg/sso/auth_builder_test.go:10`, 6 cases). Probe through `BuildAuthService`: mode `none` gives `IsAdvancedAuth` false, effective rbac "simplified". Real binary: `none` runs as simplified. | — |
| R20 | §4.1, §4.2 #1 | `simplified` → `simplified` | IMPLEMENTED | `TestResolveAuthMode`. Real binary: `/auth/groups` lists Admins, Readers, Supers, Writers; `/auth/policies` 501. | — |
| R21 | §4.1, §4.2 #1 | `internal` → `internal` | IMPLEMENTED | `TestResolveAuthMode`. Real binary: `/auth/policies` 200; `/auth/groups` 7 groups incl. Developers, SuperUsers, Viewers. | — |
| R22 | §4.2 #1 (and §4.1 table) | `external` → warn, then `simplified` | IMPLEMENTED | `pkg/sso/auth_builder.go:114`. Real binary logs `sso: auth.ui_config.rbac "external" is not supported, using "simplified"`. **The design contradicts itself:** the §4.1 table says "Rejected with SSO". Code follows §4.2. See GAP-04. | — |
| R23 | §4.1 | SSO off → upstream behaviour; `internal` without an auth API is fatal as upstream | IMPLEMENTED | `BuildAuthService` falls back to `auth.NewAuthService` (`auth_builder.go:47`). Real binary, SSO off: `rbac: internal` gives the upstream fatal "Unsupported auth mode …" (`pkg/auth/build.go:34`); `rbac: none` runs `setup` OK. | — |
| R24 | §4.2 | `rbacAuthService{*acl.AuthService}` with `IsAdvancedAuth()==true` | IMPLEMENTED | `auth_builder.go:36,38`. Probe: `IsAdvancedAuth` through all wrappers is true (internal) / false (simplified, none). **No committed test; mutant (→ false) survives** (GAP-02). | — |
| R25 | §4.2 #1 | `resolveAuthMode` replaces `ensureSimplifiedRBAC` | IMPLEMENTED | `auth_builder.go:108`; `ensureSimplifiedRBAC` has 0 hits. | — |
| R26 | §4.2 #2 | Always `EnsureACLBaseGroups`, then `ReconcileACLPolicies` | IMPLEMENTED | `auth_builder.go:63`, `:69`. Upgrade smoke shows the reconcile ran (R16). **Mutant (remove the Reconcile call) survives** (GAP-02). | — |
| R27 | §4.2 #3 | In `internal`: `EnsureRBACBase`, then wrap as `rbacAuthService` | IMPLEMENTED | `auth_builder.go:77`, then the wrap. Probe: Admins in internal mode hold `ACL(_-_)Admins`, `AuthFullAccess`, `CatalogReadWriteAll`, `FSFullAccess`, `RepoManagementFullAccess` (5 effective policies). **Mutants (remove `EnsureRBACBase` call; remove the wrap) survive** (GAP-02). | — |
| R28 | §4.2 #4 | Break-glass admin goes into `Admins` and nothing else | IMPLEMENTED | `auth_builder.go:86`. Probe: `bg-admin` groups `[Admins]` in internal, simplified and none. Break-glass credential lookup works in all three. Tests `TestEnsureBreakGlassAdmin_*` (`break_glass_test.go:71-113`). | — |
| R29 | §4.2 (order) | Bootstrap ORDER: ACL groups → Reconcile → RBAC base → break-glass → Monitored wrapper | IMPLEMENTED | `auth_builder.go` L63 → L69 → L77 → L86 → L90 (`NewMonitoredAuthService`), then `authServiceWithProfiles`. Break-glass joins `Admins` after the RBAC policies exist: probe shows the break-glass user holds 5 effective policies in internal mode. | — |
| R30 | §4.2 | `IsAdvancedAuth` survives the wrappers | IMPLEMENTED | Probe (see R24). Consumers `pkg/auth/service.go:159`, `pkg/auth/build.go:73`, `pkg/auth/setup/setup.go:326`, `pkg/api/serve.go:81`, `cmd/lakefs/cmd/run.go:273`. Untested (GAP-02). | — |
| R31 | §4.2 effects | Request logs get `log_audit=true`, `client`, `sent_bytes`, request IDs | IMPLEMENTED | Executed with the real binary. Internal, `logging.level: DEBUG`: the API log line for `GET /api/v1/repositories` has `log_audit=true` and `client=curl/8.7.1`; simplified: no `log_audit`. Code `pkg/httputil/logging.go:84-102`. **Visibility caveat:** with the default `logging.audit_log_level: DEBUG` and `logging.level: INFO` there are 0 such lines; with `audit_log_level: INFO` there is 1. Not documented in the changelog or example config (GAP-15). | — |
| R32 | §4.2 effects | The gateway does the same | IMPLEMENTED | Executed: `curl --aws-sigv4 … GET /` returns 200; log line `log_audit=true client=curl/8.7.1 operation_id=list_buckets request_id=… sent_bytes=158 service_name=s3_gateway`. Simplified: no `log_audit`. | — |
| R33 | §4.2 effects | `setup.CreateBaseGroups` becomes active | IMPLEMENTED | `pkg/auth/setup/setup.go:326`. Executed: internal, `setup --no-check`, then root gets `/auth/users` 200 and `/repositories` 200 (user in `Admins`). | — |
| R34 | §4.2 | `lakefs setup` is routed through `authServiceBuilder` and works under `lakefs-sso` | PARTIAL | Routing: `cmd/lakefs/cmd/setup.go:82`. **Behaviour:** on a fresh KV with SSO on, `lakefs-sso setup --user-name root …` (no `--no-check`) prints "Setup is already complete." and exits 1 in all three modes (none, simplified, internal). Cause (code + repro): the builder runs `acl.SetupACLServer`, which writes the setup timestamp (`contrib/auth/acl/setup.go:53-65`), before `setupLakeFS` checks `IsInitialized`. `--no-check` works: rc 0, user in `Admins`, `/auth/users` 200 in all three modes. At base: `none` printed credentials that were then rejected ("credentials not found" → 401); `simplified` died on the auth health check (`pkg/auth/build.go:84`). | MAJOR (GAP-01) |
| R35 | §4.2 | `lakefs superuser` is routed through `authServiceBuilder` and works under `lakefs-sso` | PARTIAL | Routing: `superuser.go:73`. Executed on a fresh KV, then `run`, then curl with the new key: simplified and internal give `/auth/users` 200 and `/repositories` 200 (1 group). **`none` gives 401 on both**, and the user has 0 groups: `addToAdmins := !authUIConfig.IsAuthBasic()` (`superuser.go:70`) is computed from the un-normalized `none`. Not a regression: at base the credentials were unusable too. | MINOR (GAP-03) |
| R36 | §4.2 | `external` with SSO: `setup`/`superuser` behave consistently with `BuildAuthService` | PARTIAL | `setup.go:45` and `superuser.go:35` return early on a raw `external` ("nothing to do"), but `run` falls back to `simplified`. Executed: `external`, `setup --no-check` rc 0 (nothing created), then root gets 401 ("credentials not found"). The first admin can only come from `installation.*` break-glass. | MINOR (GAP-04) |

### §4.3 Idempotent RBAC bootstrap

| ID | Design § | Requirement | Status | Evidence | Severity |
|---|---|---|---|---|---|
| R37 | §4.3 | Groups, policies and attachments treat `ErrAlreadyExists` as success | IMPLEMENTED | `pkg/auth/setup/setup.go:57,67,169`. `TestEnsureRBACBaseGroups` (`setup_test.go:29`) runs it twice. | — |
| R38 | §4.3 | Exported `EnsureRBACBaseGroups(ctx, svc, ts)` creates what is missing | IMPLEMENTED | `setup.go:176`; policy lists at `setup.go:198-201`. `TestEnsureRBACBaseGroups`, `…DatasetsDisabled` (:81), `…DatasetsEnabled` (:96). | — |
| R39 | §4.3 | It never modifies an existing policy, so admin edits survive | IMPLEMENTED | `setup_test.go:38-50` (edited `FSReadAll` survives); `TestEnsureRBACBaseKeepsAdminEdits` (`pkg/sso/rbac_bootstrap_test.go:215`). | — |
| R40 | §4.3 | `CreateRBACBaseGroups` keeps its signature and delegates | IMPLEMENTED | `setup.go:224` delegates to `:176`. Reading only. | — |
| R41 | §4.3 | `EnsureRBACBase` calls it and records `rbac-bootstrap-version` in the `auth` partition; bumps apply once | IMPLEMENTED | `pkg/sso/rbac_bootstrap.go:25` (`RBACBootstrapVersion = 1`), `:54`. `TestEnsureRBACBase` (`rbac_bootstrap_test.go:86`) covers fresh install records 1, re-run is a no-op, stored current skips, and a version bump re-applies once. | — |

### §4.4 `lakefs-sso rbac-migrate`

| ID | Design § | Requirement | Status | Evidence | Severity |
|---|---|---|---|---|---|
| R42 | §4.4 | Dry run by default, `--yes` applies, idempotent | IMPLEMENTED | Real binary, fresh install: dry run prints the plan and "Dry run only. Re-run with --yes to apply."; `--yes` prints "RBAC migration applied:"; re-run prints "no authorization changes"; all rc 0. `TestRBACMigrate_DryRunMakesNoChanges` (`rbac_migrate_test.go:87`), `…_ApplyAndRerun` (:112). | — |
| R43 | §4.4 | `Admins`: attach the 5 RBAC policies, detach `ACL(_-_)Admins` | IMPLEMENTED | `pkg/sso/rbac_migrate.go:95-125`. Real-binary plan lists `FSFullAccess`, `CatalogReadWriteAll`, `AuthFullAccess`, `RepoManagementFullAccess`, `PRReadWriteAll` attached and `ACL(_-_)Admins` detached. | — |
| R44 | §4.4 | `Supers`/`Writers`/`Readers` → `SuperUsers`/`Developers`/`Viewers`, members copied | IMPLEMENTED | `…_ApplyAndRerun` asserts members copied via `requireUserInGroup`. Member copy was not exercised in the real-binary run (no members seeded). See R15/GAP-06 for the Super PR-write drop. | — |
| R45 | §4.4 | Custom group with ACL keeps its `ACL(_-_)<g>` policy; without ACL untouched | IMPLEMENTED | `rbac_migrate_test.go:149-153` (`TeamACL`, `TeamNoACL`). | — |
| R46 | §4.4 | `--cleanup` deletes emptied groups and their ACL policies; without it they stay | IMPLEMENTED | `…_CleanupDeletesLegacyGroupsAndPolicies` (:168); flag in `cmd/lakefs-sso/main.go`. | — |
| R47 | §4.4 | Prints the `sso.group_mapping` rewrites | IMPLEMENTED | `rbac_migrate.go:317`; real-binary output. | — |
| R48 | §4.4 | Warns that rollback leaves multi-policy groups showing "no ACL" | IMPLEMENTED | `rbac_migrate.go:322-325`; real-binary output prints the rollback warning. | — |

### §4.5 ACL-layer safety fixes

| ID | Design § | Requirement | Status | Evidence | Severity |
|---|---|---|---|---|---|
| R49 | §4.5 #2 | `ReconcileACLPolicies` recomputes `ACLToStatement` for ACL-bearing policies, rewrites via `WritePolicy(update=true)`, is version-guarded | IMPLEMENTED | `contrib/auth/acl/reconcile.go:41,96`. `…_FixesStalePolicy` (:46), `…_ReconcilesEditedACLPolicy` (:83), `…_RerunIsNoOp` (:112). Upgrade smoke (R16). | — |
| R50 | §4.5 #2 | Uses "the same version key as §4.3" | DEVIATION-JUSTIFIED | Two keys: `acl_template_version` (`reconcile.go:15-22`, `ACLTemplateVersion="1"`) vs `rbac-bootstrap-version` (`rbac_bootstrap.go:25`). Justified: the reconciler is in `contrib/auth/acl` and must not import `pkg/sso` (§6 independence). **But** the comment at `rbac_bootstrap.go:28-29` still claims the reconciler reads the same key. This is R1 m3, still open. | MINOR (GAP-13) |
| R51 | §4.5 #3 | `SetGroupACL` returns 409 "group ACLs are not used in RBAC mode; attach policies instead" when not simplified | IMPLEMENTED | `controller.go:1288`. `TestSetGroupACL` (`controller_test.go:5482`); mutant (remove gate) fails it. Real binary (internal): `POST /auth/groups/Readers/acl` returns 409. Real `lakectl auth groups acl set --id Readers --permission Write` prints the server message and "409 Conflict", rc 1. | — |
| R52 | §4.5 #4 | Simplified UI shows "Custom (policies)" read-only for multi-policy groups; `NoAcl` from the 404 body | IMPLEMENTED | `webui/src/pages/auth/groups/index.tsx:249`, `webui/src/lib/api/index.js:104,285`. Vitest probe (real `getACL`, real page, mocked contexts, `fetch` stubbed with the real 404 body): "Custom (policies)" for the multi-policy group and the "Write" dropdown for the single-ACL group. **No committed webui test.** | — |

### §4.6 / §4.7 Profile API and cache coherence

| ID | Design § | Requirement | Status | Evidence | Severity |
|---|---|---|---|---|---|
| R53 | §4.6 | `UpdateUserFriendlyName` implemented | IMPLEMENTED | `service.go:259`; probe (R01). Unknown user returns `nobody: not found`. | — |
| R54 | §4.6 | `UpdateUserProfile(ctx, username, email, friendlyName)` with `SetMsgIf` and a predicate | IMPLEMENTED | `service.go:265` (`kv.SetMsgIf` at :298; `ErrPredicateFailed` tolerated). Probe (R01). | — |
| R55 | §4.6 | Both drop cache entries; `Cache.InvalidateUser`; `DummyCache` no-op | IMPLEMENTED | `pkg/auth/cache.go:30-31`. `TestLRUCacheInvalidateUser` (`cache_test.go:35`), `TestDummyCacheInvalidationIsNoop` (:123). **Mutant (remove invalidation in `UpdateUserProfile`) survives `./contrib/auth/acl`** (GAP-11). | — |
| R56 | §4.7 | `Cache.InvalidateUserPolicies(username)` and `PurgePolicies()` exist | IMPLEMENTED | `cache.go:32-34`; `TestLRUCacheInvalidateUserPolicies` (`cache_test.go:80`); `pkg/cache` gained `Delete`/`Purge`. | — |
| R57 | §4.7 | `AddUserToGroup` / `RemoveUserFromGroup` invalidate that user | IMPLEMENTED | `service.go:588,618`. Probe: `AddUserToGroup` false → true with a 1h-TTL cache. `RemoveUserFromGroup` mutant is caught by `RevocationIsImmediate/remove_user_from_group`. **`AddUserToGroup` mutant survives** (GAP-11). | — |
| R58 | §4.7 | `AttachPolicyToUser` / `DetachPolicyFromUser` invalidate that user | IMPLEMENTED | `service.go:318,349`. Probe: Attach false → true. `Detach` mutant caught by `RevocationIsImmediate/detach_user_policy`. **`Attach` mutant survives** (GAP-11). | — |
| R59 | §4.7 | `AttachPolicyToGroup` / `DetachPolicyFromGroup` purge the policy cache | IMPLEMENTED | `service.go:875,906`. Probe: Attach false → true. `Detach` mutant caught by `…/detach_group_policy`. **`Attach` mutant survives** (GAP-11). | — |
| R60 | §4.7 | `WritePolicy(update)` purges the policy cache | IMPLEMENTED | `service.go:711` (create does not purge, update does). Probe: true → false. **Mutant survives** (GAP-11). | — |
| R61 | §4.7 | `DeletePolicy` and `DeleteGroup` purge | IMPLEMENTED | `service.go:513` and `DeletePolicy`. Mutants caught by `RevocationIsImmediate/delete_group`, `/delete_policy`. | — |
| R62 | §4.7 (R1 M1) | `DeleteUser` invalidates user and policy entries | IMPLEMENTED | `service.go:120`. Mutant fails `RevocationIsImmediate/delete_user`, `/delete_user_then_recreate`, `TestAuthService_DeleteUserInvalidatesCache` (`service_test.go:365`). | — |
| R63 | §4.7 | Per-process cache documented; recommend `auth.cache.ttl: 5s` for multi-replica | IMPLEMENTED | `CHANGELOG_SSO.md:28`; `cmd/lakefs-sso/config.example.yaml:28` (`ttl: 5s`). Reading only. | — |
| R64 | §4.7 (rationale) | "Revoking access is immediate", including credential revocation | PARTIAL | Not enumerated in the §4.7 list, but implied by its rationale. Probe (internal, cache on): after `DeleteCredentials(dave, key)` the API still returns **200** (expected 401). `DeleteUser` correctly returns 401. Pre-existing: at base `DeleteCredentials` has 0 cache references and `GetCredentials` reads `s.cache.GetCredential`. The F5/M1 suites do not cover credentials. | MINOR (GAP-07) |

### §4.8 What Option 2 delivers

All rows below were run in one end-to-end probe: real `sso.BuildAuthService` behind `api.Serve` on a mem KV, `rbac=internal`, auth cache on (75 `PROBE OK`, 0 FAIL).

| ID | Design § | Requirement | Status | Evidence | Severity |
|---|---|---|---|---|---|
| R65 | §4.8 | Repository-scoped grants | IMPLEMENTED | Policy CRUD returns 201, attach 201. alice (`fs:*` on `repository/analytics-*`): stat in her repo 200, in another repo 401. | — |
| R66 | §4.8 | "…a deny on `…/branch/main` for `fs:WriteObject`" | DEVIATION-JUSTIFIED | The literal example does **not** block uploads (201), because object operations authorize on `ObjectArn`. Branch-ARN denies work for branch actions: deny `fs:CreateCommit` on the branch ARN returns 401. The design example is wrong, not the code. Not repeated in `CHANGELOG_SSO.md` or the example config. | MINOR (GAP-16) |
| R67 | §4.8 | Path-scoped object grants | IMPLEMENTED | bob: 200 on `raw/x.csv`, 401 on `curated/y.csv`. | — |
| R68 | §4.8 | IP-restricted access (`IpAddress`/`SourceIp`) | IMPLEMENTED | carol: 200 from XFF 10.1.2.3, 401 from XFF 192.0.2.9, 401 with no XFF and RemoteAddr 127.0.0.1. **Caveat:** the source IP comes from `httputil.ExtractClientIP`, which takes the first client-supplied `X-Forwarded-For` entry. This is pre-existing (`pkg/httputil` untouched; used at base `controller.go:6266`), has no trusted-proxy setting, no spoof test and no doc. | — (see GAP-08) |
| R69 | §4.8 | Per-user self-service (`${user}`) | IMPLEMENTED | Credentials for self work; for another user denied. | — |
| R70 | §4.8 | Repository list filtered to what the user can see | IMPLEMENTED | alice sees `[analytics-a]`. A user with no policies gets 401. | — |
| R71 | §4.8 | Entra groups → custom groups → custom policies via `sso.group_mapping` | PARTIAL | Mechanism is pre-existing and unchanged. `TestOauthCallbackFirstLoginSyncsGroups` (`user_test.go:141`, fake store) plus the e2e probe (group membership grants policies, immediate revoke on group add/remove). Not run through a live OIDC authorization-code flow. | NIT |
| R72 | §4.8 | Datasets `strict` model becomes possible with RBAC base groups | PARTIAL | `Datasets*` policies exist in internal mode (smoke), but `pkg/datasets` does not exist in this tree and the policies are never attached in production (R94). Depends on `DATASETS_DESIGN.md`, which is outside this branch. | NIT (GAP-12) |

### §4.9 Files touched

| ID | Design § | Requirement | Status | Evidence | Severity |
|---|---|---|---|---|---|
| R73 | §4.9 | Every listed file is touched | IMPLEMENTED | All 18 listed files appear in `git diff --name-only a25abca09..HEAD`. Listed-but-untouched: none. Untouched and correctly unlisted: `contrib/auth/acl/setup.go`, `pkg/httputil`. | — |
| R74 | §4.9 | No unlisted production file is touched without reason | DEVIATION-JUSTIFIED | Touched but unlisted (non-test): `.github/workflows/test.yaml`, `REVIEW_R1.md`, `contrib/auth/acl/permission.go`, `pkg/auth/condition.go`, `pkg/auth/service.go`, `pkg/cache/{cache,no_cache}.go`, `pkg/config/{config,oss_config}.go`, `pkg/config/testdata/invalid_authorization_failure_status.yaml`, `pkg/gateway/{handler,middleware}.go`, `pkg/permissions/actions.go`, `webui/src/lib/api/index.js`, `scripts/check-license-base.sh`. All are needed by §2.5, §4.5, §4.6 (`pkg/cache` Delete/Purge; `NoAcl` from the 404 body, which §4.5 #4 itself requires) or §5, apart from `REVIEW_R1.md`, which is the review report. The design file list is incomplete. | NIT (GAP-20) |

### §4.10 Tests

| ID | Design § | Requirement | Status | Evidence | Severity |
|---|---|---|---|---|---|
| R75 | §4.10 unit | `resolveAuthMode` table | IMPLEMENTED | `TestResolveAuthMode` (`auth_builder_test.go:10`, 6 cases). | — |
| R76 | §4.10 unit | `EnsureRBACBaseGroups` twice, and after a user edits `FSReadAll` | IMPLEMENTED | `setup_test.go:29,38-50`; `rbac_bootstrap_test.go:215`. | — |
| R77 | §4.10 unit | `ReconcileACLPolicies` before and after a template change | IMPLEMENTED | `reconcile_test.go:46,83,112`. | — |
| R78 | §4.10 unit | `rbac-migrate` plan and apply on a mem KV, re-runs no-ops | IMPLEMENTED | `rbac_migrate_test.go:87,112,168`. | — |
| R79 | §4.10 unit | Cache invalidation: revoke, then immediate `Authorize` → deny | IMPLEMENTED | `TestAuthService_RevocationIsImmediate` (`service_test.go:487`): 7 cases on a real `LRUCache` with 1h TTL (`require.IsType(&auth.LRUCache{})`). 5 revoke-path mutants caught. Grant-side invalidation has no test (R57-R60). | — |
| R80 | §4.10 controller | With `internal` and `acl.AuthService`, policy CRUD returns 2xx | PARTIAL | No committed test. `TestController_Policy` (`controller_test.go:5431`) asserts 501; `pkg/api/serve_test.go:125` builds `auth.NewBasicAuthService`. Probe-verified only (201). | MINOR (GAP-10) |
| R81 | §4.10 controller | `SetGroupACL` returns 409 | IMPLEMENTED | `TestSetGroupACL` (`controller_test.go:5482`; it sets `rbac=none`, since the gate is "not simplified"). Real-binary check in internal mode (R51). | — |
| R82 | §4.10 controller | A repo-scoped user sees only matching repos; another repo's object returns 401 | PARTIAL | No committed test on the real ACL service. `TestListBranchesPolicyConditions` (:1682), `TestListBranchesPolicyFilter` (:1973) and `TestPresignPolicyConditions` (:1896) use a fake `policyAuthService`. Probe-verified only. | MINOR (GAP-10) |
| R83 | §4.10 existing | `CheckPermissions` and `acl/service_test.go` suites keep passing | IMPLEMENTED | Executed, race detector on (§4.1). | — |
| R84 | §4.10 manual | Entra login → `Viewers` via mapping → admin attaches repo-scoped policy → applies without re-login | PARTIAL | Manual item, not automatable here. The automatable halves are covered: first-login group sync (`user_test.go:141`) and immediate effect of attach (e2e probe, cache on). No live Entra login was run. | NIT |

### §5 Option 3 gap fill

| ID | Design § | Requirement | Status | Evidence | Severity |
|---|---|---|---|---|---|
| R85 | §5.1 | `StringEquals`/`StringNotEquals`, exact match, validated, in `OperatorFactory` | IMPLEMENTED | `pkg/auth/condition.go:17-18,242`; `TestStringEqualsOperators_Evaluate` (`condition_test.go:369`). e2e probe: `StringEquals` on `lakefs:RepositoryMetadata/…` enforced. | — |
| R86 | §5.2 | `AuthorizationRequest.Fields` lazy provider; missing key evaluates false | IMPLEMENTED | `pkg/auth/service.go:61,88-92`. `TestConditionContext_Lookup` (`condition_fields_test.go:12`), "lazy and memoised" (:142), `TestAPIAuthService_Authorize_Fields` (`service_fields_test.go:17`). | — |
| R87 | §5.2 phase 1 | `lakefs:RepositoryMetadata/<k>` from the `repository` path parameter; lazy; memoised | IMPLEMENTED | `controller.go:6371` (`conditionContext`), `:6389`. `TestAuthorizeCallback_RepositoryMetadataFields` (`controller_fields_test.go:48`). | — |
| R88 | §5.2 phase 1 | The gateway does the same from the bucket name | IMPLEMENTED | `pkg/gateway/handler.go:289,324-325`; `middleware.go:199`. `TestAuthorize_RepositoryMetadataFields` (`authorize_fields_test.go:63`), `TestRepositoryMetadataFields_NoRepository` (:151), `TestDeleteObjectsPolicyConditions` (`deleteobjects_condition_test.go:109`). | — |
| R89 | §5.2 phase 2 | `lakefs:ObjectMetadata/<k>` (optional) | OUT-OF-SCOPE | Optional and deferred. Not built (grep for `ObjectMetadata/` on added lines: 0). | — |
| R90 | §5.3 | `fs:ListBranches` on the repository ARN returns everything | IMPLEMENTED | `TestListBranchesPolicyFilter` (`controller_test.go:1973`); e2e probe. | — |
| R91 | §5.3 | Otherwise filter per branch when the user holds it on some `…/branch/*`; else 401 | DEVIATION-JUSTIFIED | `anyBranchAllowed` (`controller.go:2943`) pages branches (`WithShowHidden(true)`) and evaluates each, rather than scanning policy shapes. It honours conditions and denies, at O(branches) cost. | — |
| R92 | §5.3 (R1 B1) | Explicit branch denies hide branches; conditions apply on `ListBranches` | IMPLEMENTED | `controller.go:2963-2967`. Mutant (remove `withConditionContext` in `ListBranches`) fails `TestListBranchesPolicyConditions` subtests. | — |
| R93 | §5.4 | `datasets` in `serviceSet`; ARN prefix `arn:lakefs:datasets:::dataset/<name>` (plural) | IMPLEMENTED | `pkg/permissions/actions.go:104`; `TestIsValidActionDatasets` (`actions_test.go:11`); `pkg/auth/base.go:101-111` (`DatasetsRead`, `DatasetsFull`, comment cites §5.4). No datasets action constants in `actions.gen.go` (generate has no diff). | — |
| R94 | §5.4 | `Datasets*` built-ins added to `EnsureRBACBaseGroups` when datasets is enabled | PARTIAL | `setup.go:25-26,129-135` create `DatasetsFullAccess`/`DatasetsReadAll` **unconditionally**; `:204-208` attach only if `datasetsEnabled`; `WithDatasetsEnabled` (`:37-42`) has **no production caller**; the `permissions.serviceSet` comment cites a non-existent `pkg/datasets/authz.go`. Tests cover both flag values. This is R1 m5, still open. | MINOR (GAP-12) |
| R95 | §5.5 | `Allowed=false` without an error returns 401, not 500 | IMPLEMENTED | `controller.go:6422-6423`. Probe: `Allowed=false, Error=nil` gives 401 for configured 0 and for 403. **Mutant (restore 500) passes the whole `./pkg/api`:** the change is untested. R1 m4, still open. | — (see GAP-09) |
| R96 | §5.5 | Opt-in `auth.authorization_failure_status: 403`; default 401; invalid values rejected | IMPLEMENTED | `controller.go:6414-6419,6433`. Probe: `Error=ErrInsufficientPermissions` gives 401 for configured 0 and 403 for 403; e2e probe confirms the 403 opt-in. `TestConfig_AuthorizationFailureStatus` (`config_test.go:122`); mutant (drop the validation call, `oss_config.go:27`) fails `…/startup_validation`. Documented at `CHANGELOG_SSO.md:14` and `config.example.yaml:38`. The `!Allowed` branch ignores the opt-in (GAP-09). | — |

### §6 – §9 Rollout, scope, open questions

| ID | Design § | Requirement | Status | Evidence | Severity |
|---|---|---|---|---|---|
| R97 | §6 | No Phase A change depends on Phase B code | IMPLEMENTED | Executed. The 8 A-tagged commits cherry-picked onto `a25abca09` in a throwaway worktree apply cleanly; `go build ./...`, `go vet` and the tests of `pkg/sso`, `pkg/auth`, `contrib/auth/acl`, `pkg/cache`, `pkg/permissions` all pass. The 9-commit union (A plus B4, without the B3 wiring) also passes, including `./pkg/api`. Static: `go list -deps` of `contrib/auth/acl`, `pkg/auth`, `pkg/auth/setup`, `pkg/cache`, `pkg/permissions` shows no `pkg/sso`. `go build ./...` passes at each of the 24 commits (24/24). | — |
| R98 | §6 | Phase A is "shippable alone. It fixes Writers not being able to open PRs." | PARTIAL | True for fresh installs only. The `ReconcileACLPolicies` call and `EnsureACLBaseGroups` are wired in B3 (`7d5871840`), so a KV from the base binary started on the Phase-A-only build keeps the stale policies (Writers `pr:*`=false, `catalog:*`=false, n=20). The same KV on HEAD is reconciled (n=22). §6 lists "reconcile" under Phase A. | MINOR (GAP-05) |
| R99 | §6 | Phase B is opt-in via `rbac: internal`; the default stays `simplified` | IMPLEMENTED | Default `rbac: none` (`pkg/config/defaults.go:76`) resolves to simplified; internal needs the setting. `config.example.yaml:51` ships `rbac: "simplified"`. | — |
| R100 | §7 | Tenants, audit-log resource, external principals, `admin:Login`, `external` auth are not built | IMPLEMENTED | `git diff … -U0 -- '*.go'` added lines grep for `tenant`, `audit:`, `admin:Login`, `ExternalPrincipal`, `ObjectMetadata/`: 0 hits. The only `external` hits are 2 comments saying it is rejected. `acl.AuthService` still returns `ErrNotImplemented` for external principals. | — |
| R101 | §8 | SSRF and SigV4 fixes are tracked separately and clean-room | OUT-OF-SCOPE | Not built here (R08). No tracking item exists in `tasks.json` or `agent-progress.txt` (read-only grep: 0 hits). | NIT (GAP-19) |
| R102 | §9 Q1 | `rbac-migrate` merges ACL `Admins` into RBAC `Admins` in place | IMPLEMENTED | Choice: in place (proposed option). Attach 5 policies, detach `ACL(_-_)Admins`; no `Admins.acl` rename (R43). | — |
| R103 | §9 Q2 | Break-glass joins `Admins` only | IMPLEMENTED | Choice: `Admins` only (proposed). Probe (R28). | — |
| R104 | §9 Q3 | Is a 23s cross-replica revocation delay acceptable | IMPLEMENTED | Choice: accepted and documented, no KV-backed invalidation (`CHANGELOG_SSO.md:28`, `config.example.yaml:28`). | — |

### R1 open items (`REVIEW_R1.md`)

| ID | Design § | Requirement | Status | Evidence | Severity |
|---|---|---|---|---|---|
| R105 | R1 m3 | One version key, or an accurate comment | PARTIAL | Still two keys, and the stale comment at `rbac_bootstrap.go:28-29` remains (see R50). | MINOR (GAP-13) |
| R106 | R1 m5 | Datasets wiring is complete or removed | PARTIAL | Still half-wired (see R94). | MINOR (GAP-12) |
| R107 | R1 m4 | A6 (webui) and C5 (500 → 401) are tested | PARTIAL | Both halves still open. The A6 behaviour is correct (vitest probe, R52), and so is C5 (R95), but no committed test pins either. | MINOR (GAP-09, GAP-10) |

Rows resting on code reading alone (no cheap probe existed): R03, R09, R40, R63, R73, R74, R100 (grep).

## 3. R1 re-check: do F1–F6 close their findings?

Each fix was reverted in turn (§4.6) to prove that a test notices.

| R1 finding | Fix commit | Closed? | Evidence |
|---|---|---|---|
| B1 (BLOCKER): `ListBranches` has no condition context | `bd150c6b6` F1 | **Closed** | Mutant (remove `r = c.withConditionContext(...)`, `controller.go:2967`) fails `TestListBranchesPolicyConditions`. e2e probe: branch-level deny returns 401. |
| M1: `DeleteUser` does not invalidate the cache | `940900d44` (M1 commit) | **Closed** | Mutant fails 3 tests (R62). e2e probe: 401 after `DeleteUser`. |
| M2: two call sites bypass the condition-context plumbing | `d4dca3108` F3 | **Closed** (gateway, ListObjects presign). `StatObject` site is redundant. | Gateway mutant fails `TestDeleteObjectsPolicyConditions/…blocks_the_delete`. ListObjects mutant (nested per-object `Authorize` loses ctx) fails `TestPresignPolicyConditions/list_objects_presign_conditioned_deny_does_not_issue_a_URL`. Three more mutants survive: removing only `r = r.WithContext(ctx)` in ListObjects, emptying `ClientIP` in the nested `Authorize` call, and dropping `StatObject`'s `withConditionContext` (`controller.go:5301`). They are equivalent or redundant. `acl.AuthService.Authorize` reads the conditions from the context, and only `APIAuthService` uses `req.ClientIP`/`req.Fields` (`pkg/auth/service.go:1027`). In `StatObject`, `authorizeCallback` derives its own context and the presign branch does not re-authorize (GAP-17). |
| m1: no revoke-immediate test | `f565427a9` F5 | **Closed** | `TestAuthService_RevocationIsImmediate`, 7 cases; 5 revoke-path mutants caught (R57-R62). Grant-side is not in m1's scope (GAP-11). |
| m2: `authorization_failure_status` unvalidated and undocumented | `ba109abe7` F4 | **Closed** | Validation mutant caught; `CHANGELOG_SSO.md:14`; `config.example.yaml:38`. |
| m6: licence guard | `eb3267624` (labelled "R1-F7") + the `R1-F6` follow-up | **Closed** (fails closed) | R11. |
| m3: two version keys | none | **Open** | R50 / R105 |
| m4: A6 untested; C5 500 → 401 untested | none | **Open** | R95 / R107 |
| m5: datasets half-wired | none | **Open** | R94 / R106 |

The R1 report's 17 commit hashes are not ancestors of HEAD. The range was rebased. Patch-id matches found: `9eacd521d` → `adbec67e6` (A1) and `b2086304d` → `91ef0eea0` (C5).

## 4. Verified by execution

Scratch output is in `/tmp/conf/`, outside the repo. All commands ran at HEAD `ba109abe7` unless stated.

### 4.1 Required checks

| Command | Result |
|---|---|
| `go build ./...` | rc 0 |
| `go test -count=1 -race ./pkg/auth/... ./pkg/sso/... ./pkg/config/... ./contrib/auth/acl/... ./pkg/gateway/... ./pkg/cache/...` | all ok |
| `go test -count=1 ./pkg/api/...` | ok (20.1s) |
| `gofmt -l` on the 46 changed `.go` files | no output |
| `go vet` on the 12 changed package directories | rc 0 |
| `cd webui && npm run lint` | rc 0 |
| `cd webui && npm test` | 6 files, 72 tests pass |
| `go generate` in `pkg/permissions` | no diff. **`make validate-permissions-gen` itself was not run**: it depends on the heavy `gen-code`, so the per-package `go generate` equivalent was used. |
| `go build ./...` at each of the 24 commits (throwaway worktree) | 24/24 build |

### 4.2 Licence guard

- `bash scripts/check-license-base.sh` → "OK: 358544dd7 … not an ancestor of HEAD ba109abe7.", rc 0.
- `--self-test` → 3 cases pass.
- `git merge-base --is-ancestor` → "no" for all 5 BSL commits.
- **Side effect on `.git`:** the BSL commit objects had vanished from this clone. I ran `git fetch --no-tags https://github.com/treeverse/lakeFS.git master` to restore them so the guard could be conclusive. It added objects only; no refs or working-tree files changed.

### 4.3 Phase A independence (§6)

Cherry-picked the 8 A-tagged commits (`7d05c21be 40698e6ff adbec67e6 d2cb4521a fa5203bfc 1f8fdd760 940900d44 f565427a9`) onto `a25abca09` in a throwaway worktree: no conflicts, `go build ./...` rc 0, `go vet` rc 0, tests ok for `pkg/sso`, `pkg/auth`, `contrib/auth/acl`, `pkg/cache`, `pkg/permissions`. A second set (A2, routing, A1, A3 primitives, A4, B4 `20881d6c4`) and the 9-commit union both build and pass, including `./pkg/api`. All worktrees were removed and pruned.

### 4.4 Real-binary smoke runs

Binaries built from HEAD (`lakefs-sso`, `lakectl`), from `a25abca09` (`lakefs-sso-base`) and from the Phase-A union (`lakefs-sso-phaseA`). Config files in `/tmp/conf/smoke/`; a small Python OIDC discovery stub served the SSO issuer. No stray processes remained.

| Scenario | Result |
|---|---|
| `GET /auth/users` / `/auth/policies` / `/auth/groups` / `POST /auth/groups/Readers/acl`, mode `none` | 401 / 501 / 401 / 401 (server runs as simplified) |
| same, mode `simplified` | 200 / 501 / 200 (4 ACL groups) / 201 |
| same, mode `internal` | 200 / 200 / 200 (7 groups) / **409** "group ACLs are not used in RBAC mode; attach policies instead" |
| `lakectl auth groups acl set --id Readers --permission Write`, internal | prints the server message and "409 Conflict", rc 1 |
| `lakectl auth policies list`, internal | works; lists the `ACL(_-_)…` policies |
| SSO off, `rbac: internal` | upstream fatal "Unsupported auth mode …" |
| SSO off, `rbac: none`, `setup` | OK |
| `rbac-migrate --help`, dry run, `--yes`, re-run (fresh install) | text matches §4.4; plan lists the 5 attachments and `ACL(_-_)Admins` detach plus `sso.group_mapping` rewrites and the rollback warning; apply prints "RBAC migration applied:"; re-run prints "no authorization changes"; rc 0 |
| Upgrade: KV made by base binary, started by `lakefs-sso-phaseA` | Writers `pr:*`=false `catalog:*`=false, n=20 (stale) |
| Upgrade: same KV started by HEAD | Writers `pr:*`=true `catalog:*`=true, n=22; Supers 13; Readers 13; Admins 7 |
| Fresh KV, base vs phaseA vs HEAD | base: Writers 20, Supers 11, Readers 7, Admins 6; phaseA and HEAD carry the fixed templates |
| `lakefs-sso setup --user-name root …` (no `--no-check`), fresh KV, modes none/simplified/internal | **"Setup is already complete.", rc 1, in all three** (GAP-01) |
| same with `--no-check`, then `run`, root `GET /auth/users` and `/repositories` | rc 0; **200 / 200** in none, simplified and internal |
| `lakefs-sso superuser --user-name su1`, then `run`, curl with su1's key | none: **401 / 401**, 0 groups; simplified: 200 / 200; internal: 200 / 200 (GAP-03) |
| `rbac: external`, `setup --no-check`, then `run` | warning logged; setup creates nothing; root gets 401 "credentials not found"; `/auth/policies` 401 (GAP-04) |
| Base binary, `rbac: none` + SSO: `superuser` / `setup` then `run` | credentials printed, then 401 "credentials not found" (what §4.2's routing fix targets) |
| Base binary, `rbac: simplified` + SSO: `setup` | fatal "Auth API health check failed" (`pkg/auth/build.go:84`) |
| Audit log, internal, `logging.level: DEBUG` | API line has `log_audit=true client=…`; gateway line (curl `--aws-sigv4`) has `log_audit=true client request_id sent_bytes service_name=s3_gateway` |
| Audit log, simplified | no `log_audit` |
| Audit log, internal, `logging.level: INFO` with `audit_log_level: DEBUG` / `INFO` | 0 / 1 lines (GAP-15) |

### 4.5 Scratch probes (all deleted from the tree)

| Probe | What it checks | Result |
|---|---|---|
| e2e policy probe (`pkg/api`) | real `sso.BuildAuthService` behind `api.Serve`, mem KV, `rbac=internal`, cache on: policy CRUD, repo-scoped / path-scoped / IP / `${user}` / ABAC (`StringEquals`) / ListBranches filtering / branch deny / `SetGroupACL` 409 / immediate revoke (detach, group add/remove, `DeleteUser`) / 403 opt-in | 75 `PROBE OK`, 0 FAIL. `fs:WriteObject` deny on the branch ARN did **not** block uploads (R66). |
| break-glass probe (`pkg/sso`) | `IsAdvancedAuth`, effective rbac, `bg-admin` groups and policies per mode | internal: true, `[Admins]`, 5 policies; simplified and none: false, `[Admins]`, 1 policy. `none` normalizes to simplified. |
| simplified-mode probe | template effects per group; `GetGroupACL`; `SetGroupACL` | Writers PR+catalog true; Readers PR false, catalog read true, create false; Supers and Admins true; `GetGroupACL` multi-policy 404 `no_acl`; `SetGroupACL` 201 and detaches the foreign policy |
| grants probe (`contrib/auth/acl`) | grant-side invalidation and profile update with a 1h-TTL cache | `AttachPolicyToUser`, `AddUserToGroup`, `AttachPolicyToGroup` false → true; `WritePolicy(update)` true → false; profile email lookup moves from old to new |
| friendly-name probe (`contrib/auth/acl`) | `UpdateUserFriendlyName` on a cached user | "" → "Friendly U", email kept; unknown user → not found |
| credentials/Super probe | credential deletion with cache on; Super PR parity | `DeleteCredentials`: API still 200 (GAP-07); `DeleteUser`: 401 |
| parity probe (4 pairs × 15 actions) | ACL group vs RBAC group effective rights | identical except Super `pr:WritePullRequest` (ACL true, RBAC false) (GAP-06) |
| `!Allowed` probe and 403 control probe (`pkg/api`) | `authorizeCallback` status | `Allowed=false, Error=nil`: 401 for configured 0 and 403. `Error=ErrInsufficientPermissions`: 401 for 0, 403 for 403. |
| webui probe (`vitest`, `GroupsPage`) | real page and `getACL`, mocked router/config/outlet/auth contexts, `fetch` stubbed with the real 404 body | "Custom (policies)" for the multi-policy group; "Write" dropdown for the single-ACL group |
| `zzdump` (temporary `cmd/zzdump/`) | read the policy KV directly, since `GET /auth/policies` is 501 in simplified mode | used for the n=20 / 22 counts |

### 4.6 Mutation testing (revert the fix, expect a test to fail)

Each mutant was applied, the package tests were run (`go test -count=1 -run <rx> <pkg>`), and the file was restored with `git checkout`. The tree was verified clean afterwards. 27 distinct mutants (`/tmp/conf/mut/results.txt` also has lines for skipped anchors, duplicates and re-runs): **12 caught, 15 survived**. Of the survivors, 3 are equivalent or redundant (GAP-17) and 12 are real test gaps: C5 (1), grant-side and profile invalidation (5), §4.2 wiring (6).

| Mutant | Caught? | By |
|---|---|---|
| F1: remove the condition context in `ListBranches` | yes | `TestListBranchesPolicyConditions` subtests |
| F3: gateway `EnrichWithRepositoryOrFallback` loses the condition context | yes | `TestDeleteObjectsPolicyConditions/conditional_deny_from_matching_IP_blocks_the_delete` |
| F3: ListObjects presign, nested per-object `Authorize(ctx → context.Background())` | yes (re-run) | `TestPresignPolicyConditions/list_objects_presign_conditioned_deny_does_not_issue_a_URL` |
| M1: `DeleteUser` drops `InvalidateUserPolicies` | yes | `RevocationIsImmediate/delete_user`, `/delete_user_then_recreate`, `TestAuthService_DeleteUserInvalidatesCache` |
| F5: `DetachPolicyFromUser`, `DeletePolicy`, `RemoveUserFromGroup`, `DetachPolicyFromGroup`, `DeleteGroup` drop invalidation | yes (each) | `RevocationIsImmediate/<case>` |
| F4: drop `ValidateAuthorizationFailureStatus` call | yes | `TestConfig_AuthorizationFailureStatus/startup_validation` |
| §4.5 #3: remove the `SetGroupACL` 409 gate | yes | `TestSetGroupACL` |
| §1.1 #5: remove the `GetUser`-first block in `EnsureUser` | yes | `TestEnsureUser_ExistingUserUntouched` |
| C5/§5.5: restore 500 for `!Allowed` | **no** | whole `./pkg/api` passes (GAP-09) |
| `AttachPolicyToUser`, `AttachPolicyToGroup`, `AddUserToGroup`, `WritePolicy(update)` drop invalidation | **no** (each) | `./contrib/auth/acl` passes (GAP-11) |
| `UpdateUserProfile` drops invalidation | **no** | `./contrib/auth/acl` passes (GAP-11) |
| §4.2: `rbacAuthService.IsAdvancedAuth` → false | **no** | `./pkg/sso`, `./cmd/lakefs/cmd` pass (GAP-02) |
| §4.2: remove the `rbacAuthService{aclService}` wrap | **no** | same |
| §4.2: remove the `EnsureRBACBase` call | **no** | same |
| §4.2: remove the `ReconcileACLPolicies` call | **no** | same |
| §4.2: drop the `authServiceWithProfiles` wrapper | **no** | same |
| §4.2: `setup.go` back to `auth.NewAuthService` | **no** | same |
| F3: drop `withConditionContext` in `StatObject` (`controller.go:5301`) | no | redundant (GAP-17) |
| F3: remove only `r = r.WithContext(ctx)` in ListObjects | no | equivalent: the nested call uses the `ctx` variable (GAP-17) |
| F3: empty `ClientIP` in the nested ListObjects `Authorize` | no | equivalent for `acl.AuthService` (it reads conditions from the context); matters only for `APIAuthService` (GAP-17) |

`BuildAuthService` is referenced only from `cmd/lakefs-sso/main.go:34`. `TestGetAuthService` in `cmd/lakefs/cmd/run_test.go` is unchanged by the branch and uses `auth.NewAuthService`.

## 5. Scratch artifacts and side effects

- **Probe and test files created in the tree and deleted before finishing:** `pkg/api/zz_conformance_probe{,2,3,4,5}_test.go`; `pkg/sso/zz_conformance_probe_test.go`; `contrib/auth/acl/zz_conformance_probe_test.go`; `webui/src/pages/auth/groups/zz_groups_probe.test.tsx`; the temporary `cmd/zzdump/`. Source copies are under `/tmp/conf/probes/` (not committed).
- **Mutation edits** were reverted with `git checkout` after each run.
- **Throwaway git worktrees** (per-commit builds, Phase A cherry-picks) were removed and pruned. The other agents' worktrees (`…/worktrees/{2d585,eda44}/lakeFS`) and the unrelated `stash@{0}` were not touched.
- **`.git` objects:** added by one `git fetch --no-tags` from upstream `master` (see §4.2). No refs changed.
- `git status --short` was empty after every probe. `tasks.json`, `agent-progress.txt` and `RBAC_DESIGN.md` were not modified (the first two were only read).
- The only file committed is `DESIGN_CONFORMANCE.md`.

## 6. Gaps sorted by severity

| Gap | Severity | Summary | Matrix rows | Origin |
|---|---|---|---|---|
| GAP-01 | MAJOR | `lakefs-sso setup` fails on a fresh KV with SSO on (all modes), unless `--no-check` is given | R34 | new (the routing change exposed it) |
| GAP-02 | MAJOR | The §4.2 wiring in `BuildAuthService` and `setup.go` is untested: 6/6 wiring mutants survive | R24, R26, R27, R30, R34 | new |
| GAP-03 | MINOR | `lakefs superuser` under `rbac: none` + SSO creates a user with no groups | R35 | new; not a regression |
| GAP-04 | MINOR | `external` + SSO: `setup`/`superuser` exit early, so no admin exists; the design contradicts itself (§4.1 "Rejected" vs §4.2 "warn, then simplified") | R22, R36 | new + design |
| GAP-05 | MINOR | Phase A alone does not repair existing installs; reconcile wiring is in B3 but §6 lists it under Phase A | R98 | new + design |
| GAP-06 | MINOR | RBAC `SuperUsers` lacks `pr:WritePullRequest`; `rbac-migrate` silently drops PR write for Supers, and §4.5 #1 wrongly says the templates match | R15, R44 | design + pre-existing policy set |
| GAP-07 | MINOR | `DeleteCredentials` does not revoke immediately with the cache on | R64 | pre-existing |
| GAP-08 | MINOR | `IpAddress`/`SourceIp` conditions trust a client-supplied `X-Forwarded-For`; no trusted-proxy setting, no doc, no test | R68 | pre-existing |
| GAP-09 | MINOR | §5.5: the 500 → 401 fix is untested; the `!Allowed` branch ignores the 403 opt-in (R1 m4, part 1) | R95, R96, R107 | R1 m4 |
| GAP-10 | MINOR | No committed test for internal-mode controller behaviour, `GetGroupACL` `no_acl`, or the webui "Custom (policies)" badge (R1 m4, part 2) | R52, R80, R82, R107 | R1 m4 + §4.10 |
| GAP-11 | MINOR | Grant-side cache invalidation and the profile API are untested (5 mutants survive; `UpdateUserProfile` has no test at all) | R01, R55, R57–R60 | new |
| GAP-12 | MINOR | Datasets built-ins are half-wired (R1 m5) | R72, R94, R106 | R1 m5 |
| GAP-13 | MINOR | Two version keys and a stale comment (R1 m3) | R50, R105 | R1 m3 |
| GAP-14 | MINOR | The committer change is not in the SSO changelog | R05 | new |
| GAP-15 | MINOR | Audit-style logs are invisible at default log levels, and this is undocumented (§7 calls them "enough for log shipping") | R31 | new |
| GAP-16 | MINOR | Design §4.8 example (deny `fs:WriteObject` on a branch ARN) does not do what it says | R66 | design |
| GAP-17 | NIT | `StatObject`'s `withConditionContext` is redundant, and the presign tests do not discriminate it | (R1 M2) | R1 M2 |
| GAP-18 | NIT | Licence guard was fail-open locally with no Makefile target (fixed by the `R1-F6` follow-up) | R11 | R1 m6 |
| GAP-19 | NIT | §8 clean-room fixes are not tracked in the ledger | R101 | design |
| GAP-20 | NIT | The §4.9 file list is incomplete | R74 | design |

Not counted as a gap: R04 (`#EXT#`, a listed follow-up). R71, R72 and R84 are manual or external dependencies.

## 7. Follow-up cards

### GAP-01 — `lakefs-sso setup` must succeed on a fresh KV when SSO is enabled (MAJOR)
- **Files:** `cmd/lakefs/cmd/setup.go` (`setupLakeFS` init check), `pkg/sso/acl_bootstrap.go`, `contrib/auth/acl/setup.go` (`SetupACLServer` timestamp). Test: a new test under `cmd/lakefs/cmd/`.
- **Validation:**
  - For each of `none`, `simplified`, `internal`: fresh KV, `lakefs-sso setup --user-name root` without `--no-check` exits 0 and prints credentials. `run` then returns 200 on `GET /auth/users` with those keys. A second `setup` still says "already complete" with exit 1.
  - Add a unit test that installs `sso.BuildAuthService` through `SetAuthServiceBuilder` and runs `setupLakeFS` on a mem KV.

### GAP-02 — Test the `BuildAuthService` wiring (MAJOR)
- **Files:** `pkg/sso/auth_builder_test.go` (new tests on a mem KV), possibly `cmd/lakefs/cmd/setup_test.go`.
- **Validation:** each of the 6 surviving mutants must fail a test:
  1. `rbacAuthService.IsAdvancedAuth` → false;
  2. remove the `rbacAuthService` wrap;
  3. remove the `EnsureRBACBase` call;
  4. remove the `ReconcileACLPolicies` call;
  5. drop the `authServiceWithProfiles` wrapper;
  6. revert `setup.go` to `auth.NewAuthService`.

  Assert `IsAdvancedAuth` through the wrappers per mode, the Admins policy set in internal mode, a stale ACL policy reconciled, and the break-glass user in `[Admins]` only.

### GAP-03 — `superuser` under `rbac: none` + SSO must produce an admin (MINOR)
- **Files:** `cmd/lakefs/cmd/superuser.go:70` (`addToAdmins` from the un-normalized mode), `pkg/sso/auth_builder.go` (expose the resolved mode or an "ACL-backed" signal).
- **Validation:** fresh KV, `none` + SSO, `superuser --user-name su1`, `run`, `GET /auth/users` returns 200 (today 401, 0 groups). Simplified and internal remain 200.

### GAP-04 — Decide and align `external` + SSO (MINOR)
- **Files:** `RBAC_DESIGN.md` §4.1 and §4.2 (the owner decides "reject" or "warn then simplified"); `cmd/lakefs/cmd/setup.go:45`; `cmd/lakefs/cmd/superuser.go:35`.
- **Validation:** with the chosen behaviour, `external` + SSO either fails fast at startup, or `setup`/`superuser` create a working admin through the simplified service. Re-run the `external` smoke (root must not get 401).

### GAP-05 — Make Phase A repair existing installs, or amend §6 (MINOR)
- **Files:** `RBAC_DESIGN.md` §6, or `pkg/sso/auth_builder.go` (move the `EnsureACLBaseGroups`/`Reconcile` wiring ahead of the mode logic) and the commit split.
- **Validation:** re-run the upgrade smoke (base KV on the Phase-A-only build): Writers must show `pr:*`=true and n=22.

### GAP-06 — Super PR-write parity (MINOR)
- **Files:** `pkg/sso/rbac_migrate.go` (print a plan warning, or attach `PRReadWriteAll` to `SuperUsers`), `RBAC_DESIGN.md` §4.5 #1, `contrib/auth/acl/permission_test.go` (`extraPolicyType`).
- **Validation:** `rbac-migrate` dry run on a KV with a Super member warns about, or preserves, `pr:WritePullRequest`; rerun the parity probe: no `!!` row.

### GAP-07 — Revoke credentials immediately (MINOR)
- **Files:** `contrib/auth/acl/service.go` (`DeleteCredentials`), `pkg/auth/cache.go` (credential invalidation), `contrib/auth/acl/service_test.go`.
- **Validation:** with `LRUCache` TTL 1h, `GetCredentials` then `DeleteCredentials` then `Authorize` or an API call returns 401. Add a `RevocationIsImmediate` case. Note it is pre-existing.

### GAP-08 — Trusted-proxy handling and docs for IP conditions (MINOR)
- **Files:** `pkg/httputil/client.go` (`ExtractClientIP`), config, `CHANGELOG_SSO.md`, `cmd/lakefs-sso/config.example.yaml`; tests in `pkg/api` and `pkg/gateway`.
- **Validation:** a spoof test: `X-Forwarded-For: 10.1.2.3` with `RemoteAddr` 192.0.2.9 is denied by an `IpAddress` allow unless a trusted proxy is configured. Raise to MAJOR if IP conditions are meant as a security control.

### GAP-09 — Pin §5.5 behaviour (MINOR)
- **Files:** `pkg/api/controller_authorization_status_test.go`, `pkg/api/controller.go:6422` (the `!Allowed` branch), `RBAC_DESIGN.md` §5.5.
- **Validation:** a test with `Allowed=false, Error=nil` expects 401, so the "restore 500" mutant fails. Decide whether the 403 opt-in should apply to this branch, and test whichever is chosen.

### GAP-10 — Committed controller and webui tests (MINOR)
- **Files:** `pkg/api/controller_test.go` (or a new file) using `acl.AuthService` in internal mode; a `GetGroupACL` test; `webui/src/pages/auth/groups/index.test.tsx`.
- **Validation:**
  - Policy CRUD returns 2xx.
  - A repo-scoped user sees only matching repos and gets 401 on another repo's object.
  - `GetGroupACL` returns 404 with `no_acl`.
  - The vitest renders "Custom (policies)". The scratch probe in `/tmp/conf/probes/zz_groups_probe.test.tsx` is a starting point.

### GAP-11 — Cover grant-side invalidation and the profile API (MINOR)
- **Files:** `contrib/auth/acl/service_test.go`.
- **Validation:** add `RevocationIsImmediate`-style grant cases (false → true) for `AttachPolicyToUser`, `AddUserToGroup`, `AttachPolicyToGroup` and `WritePolicy(update)`. Add tests for `UpdateUserProfile` and `UpdateUserFriendlyName`. All five surviving mutants must then fail.

### GAP-12 — Finish or remove the datasets half-wiring (MINOR)
- **Files:** `pkg/auth/setup/setup.go:25-26,37-42,129-135,204-208`, `pkg/permissions/actions.go` (dangling comment).
- **Validation:** either create the policies only when `datasetsEnabled` and give `WithDatasetsEnabled` a production caller, or drop the policies and the comment. Smoke: no `Datasets*` policies in internal mode unless the feature is on.

### GAP-13 — One version key, or a correct comment (MINOR)
- **Files:** `pkg/sso/rbac_bootstrap.go:28-29`, `contrib/auth/acl/reconcile.go:15-22`, `RBAC_DESIGN.md` §4.5 #2.
- **Validation:** the comment matches the code; the design states the two keys (or a shared constant is used). Existing reconcile and bootstrap tests pass.

### GAP-14 — Document the committer change (MINOR)
- **Files:** `CHANGELOG_SSO.md`.
- **Validation:** `grep -n -i committer CHANGELOG_SSO.md` finds an entry that says the committer is the email for new commits only, and that old commits keep the oid.

### GAP-15 — Document how to see audit-style logs (MINOR)
- **Files:** `CHANGELOG_SSO.md`, `cmd/lakefs-sso/config.example.yaml`.
- **Validation:** with the documented `logging.audit_log_level`, an internal-mode request logs `log_audit=true` at the default `logging.level`. Re-run the audit smoke.

### GAP-16 — Fix the §4.8 example (MINOR)
- **Files:** `RBAC_DESIGN.md` §4.8.
- **Validation:** the example denies `fs:CreateCommit` on the branch ARN (verified: 401), or denies `fs:WriteObject` on `…/object/*` for the branch; re-run the e2e probe case.

### GAP-17 — Drop the redundant `StatObject` context call or make the test discriminate (NIT)
- **Files:** `pkg/api/controller.go:5301`, `pkg/api/controller_test.go:1896`.
- **Validation:** the mutant either no longer exists or fails a test.

### GAP-18 — Harden the licence guard locally (NIT)
- **Files:** `scripts/check-license-base.sh`, `Makefile`, `.github/workflows/test.yaml`.
- **Validation:** `make check-license-base` runs the self-test and then the check; the guard fails closed on a shallow clone with the remedy (`git fetch --unshallow`) in the message, and `LICENSE_GUARD_SKIP_IF_INCONCLUSIVE=1` is the documented local-only opt-out that CI never sets. `.github/workflows/test.yaml`: the `validator` job checks out with `fetch-depth: 0` (it runs `checks-validator`, which includes the guard).

### GAP-19 — Track the §8 clean-room fixes (NIT)
- **Files:** `tasks.json` (owner decision; not touched by this review).
- **Validation:** two ledger items exist for the hook SSRF and SigV4 signed-header work, each marked clean-room.

### GAP-20 — Refresh the §4.9 file list (NIT)
- **Files:** `RBAC_DESIGN.md` §4.9.
- **Validation:** the table lists `pkg/cache/*`, `pkg/auth/condition.go`, `pkg/auth/service.go`, `pkg/config/*`, `pkg/gateway/*`, `pkg/permissions/actions.go`, `webui/src/lib/api/index.js`, `scripts/check-license-base.sh` and the CI workflow.
