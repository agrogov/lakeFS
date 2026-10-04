# SSO Implementation Retrospective

## Executive Summary

- **Merge-friendly compliance is fundamentally achieved**: the final state has exactly two upstream file changes (`run.go` + `oss_config.go`) matching the plan spec, and all new code is in genuinely new files. However, this was only reached after a corrective commit (`fix(sso): restore merge-friendly upstream footprint`) that reverted an over-reach introduced in Phase 2.
- **One significant planning mistake caused a full rework cycle**: Phase 2 widened the `AuthenticationServiceBuilder` type signature in `hooks.go` and added a third argument to the `run.go` call to pass `auth.Service` into the authentication builder. This violated the merge-friendly constraint and required a dedicated fix commit. The correct solution (a package-level `builtAuthService` var) was always available but was not identified during planning.
- **A second chi v5 routing mistake caused two refactor cycles**: Phase 3 introduced a `/logout` override via `r.Get` which was believed incorrect; Phase 4 replaced it with `r.Use` (middleware), which panics in production; Phase 4's review corrections reverted to `r.Get` with an explanation that it does work correctly in chi v5. Two full rewrite cycles on ~80 lines of code, consuming roughly 40–60k tokens, could have been avoided by verifying chi v5 routing behavior once upfront.
- **The `LogoutURL` field was omitted from `SSOConfig` on the first write** and added only in a post-audit commit, indicating the plan's config struct was not fully implemented in Phase 1.
- **All tests pass cleanly with the race detector**; security-sensitive paths (CSRF state, nonce verification, session fixation, open-redirect, loopback validation) are correctly implemented and covered by tests.

---

## 1. Merge-Friendly Compliance Review

### 1.1 Permitted Upstream Changes vs. Actual State

The plan declared exactly two permitted upstream file changes:

| File | Planned change | Actual change | Match? |
|---|---|---|---|
| `cmd/lakefs/cmd/run.go` | Replace 2 direct function calls with builder variable calls | Two lines changed, matching exactly | **Yes** |
| `pkg/config/oss_config.go` | Add `SSO map[string]interface{}` field to `ConfigImpl` | Exactly that one field, with comment | **Yes** |

Both changes match. The `go.mod` / `go.sum` additions (adding `go-oidc/v3` as a direct dep, `go-jose/v4` for tests) were also correctly described in the plan as additive and low-risk.

### 1.2 Interim Non-Compliance (Corrected)

Commit `1778ce1b` (Phase 2) introduced two additional upstream changes that violated the plan:
1. `cmd/lakefs/cmd/hooks.go` — the `AuthenticationServiceBuilder` type signature was widened to include an `auth.Service` parameter, and the default lambda was rewritten.
2. `cmd/lakefs/cmd/run.go` — a third argument (`authService`) was added to the `authenticationServiceBuilder(...)` call.

These were reverted in `11586a614` ("restore merge-friendly upstream footprint") by adopting the `builtAuthService` package-level variable pattern instead.

**Net result:** Final state is compliant. But this required a dedicated fix commit and represents rework.

### 1.3 New Files — Namespace Collision Risk

All new files are in directories that did not exist upstream:

| Path | Collision risk |
|---|---|
| `pkg/sso/` | Low. Upstream has `pkg/auth/`, `pkg/authentication/`, no `sso` package. If upstream ever adds one the directory conflict is obvious and manageable. |
| `cmd/lakefs-sso/` | Low. Entirely new directory. |
| `cmd/lakectl-sso/` | Low. Entirely new directory. |
| `cmd/lakefs/cmd/hooks.go` | Low. No upstream file named `hooks.go` in that package. |
| `cmd/lakectl/cmd/hooks.go` | Low. Same reasoning. Only exports `GetRoot()`, which upstream does not define in `lakectl`. |

One mild concern: `cmd/lakefs/cmd/root.go` in upstream already exports `GetRoot()`. The `cmd/lakectl/cmd/hooks.go` new file also exports `GetRoot()` for the `lakectl` package — but this is a different package (`lakectl/cmd` vs `lakefs/cmd`), so there is no collision.

---

## 2. Code Review Findings

### 2.1 Bugs / Correctness Issues

**`builtAuthService` global — implicit ordering dependency (medium risk)** ✅ Fixed

`pkg/sso/auth_builder.go` stores the constructed `auth.Service` in a package-level variable `builtAuthService`. `BuildAuthenticationService` reads it. The comment documents that `run.go` calls `authServiceBuilder` before `authenticationServiceBuilder`, so "the value is always populated in time." This is true for the production binary but it is an invisible ordering contract. If a future upstream refactor of `run.go` reorders these two calls, or if a test exercises `BuildAuthenticationService` without calling `BuildAuthService` first, the authentication service will silently receive a `nil` auth service and group sync will be silently disabled (the nil check in `OauthCallback` absorbs the nil rather than panicking). This is safe-to-fail behavior but the invariant is fragile and undocumented at the call site.

**`SSOConfig.LoadSSOConfig` — `viper.UnmarshalExact` not used (low risk)** ✅ Fixed

`LoadSSOConfig` uses `viper.UnmarshalKey("sso", cfg)`, which does not enable `ErrorUnused`. A misspelled YAML key like `clint_id` will be silently dropped, leaving `ClientID` empty. The function does panic on type errors, but not on unknown keys. The user gets a confusing "authentication failed" at runtime rather than a startup error. Upstream `config.go` uses `viper.UnmarshalExact` for the full config to catch exactly this class of mistake.

**Group sync — `desired` set includes only token groups with the prefix, but `currentManaged` is filtered by prefix on `DisplayName`** (design note, not a bug)

`SyncGroups` filters `desired` using `cfg.ManagedGroupPrefix` on the raw token group name. It filters `currentManaged` using the same prefix on `g.DisplayName`. This works correctly when the token role value matches the ACL group `DisplayName` exactly (the documented expectation). However there is no error if they diverge; the mismatch simply means no groups are ever removed (the desired set never matches). The config example documents this correctly (`managed_group_prefix: ""` with the note that role names must match ACL group names), but no validation or warning is emitted at startup.

**`cli_login.go` — `openBrowser` does not validate the URL before shelling out (low risk)**

`openBrowser` passes `rawURL` directly to `exec.Command`. The URL is constructed by `buildLoginURL` from a user-supplied `endpoint` string, which is first validated by `apiutil.NormalizeLakeFSEndpoint`. The validation is present but lives in a different function. If `buildLoginURL` is ever called without the normalize step the raw endpoint string reaches `exec.Command`. Currently safe, but the separation makes it easy to miss in a future refactor.

**`BrowserLogin` — `srv.Shutdown` called twice in success path (minor, benign)**

The deferred `srv.Shutdown` at line 87 runs after the in-handler goroutine `go func() { _ = srv.Shutdown(...) }()` fires. `http.Server.Shutdown` is idempotent so there is no bug, but the double-shutdown adds a brief goroutine lifetime and a redundant log entry. Not harmful.

**`OauthCallback` — empty `code` is silently forwarded to `oauth2Cfg.Exchange` (low risk)** ✅ Fixed

If the Azure callback URL arrives without a `code` parameter (which should not happen in a valid OIDC flow but could happen if a user navigates directly to the callback URL after state verification passes), `s.oauth2Cfg.Exchange(ctx, "")` is called. This will fail at the token endpoint with a 400, which propagates as `http.StatusUnauthorized` to the browser. The error is safe but the log message will say "token exchange failed" rather than "missing code", making debugging harder.

### 2.4 Fixes Applied (commit `4b223b70c`)

**Issue 1 — `builtAuthService` ordering (`pkg/sso/authentication_builder.go`)**
Added an explicit `nil` check on `cachedSvc` after the RLock read. When SSO is enabled and `cachedSvc` is nil, a `Warn` log is emitted: `"sso: BuildAuthService was not called before BuildAuthenticationService; group sync will be disabled"`. Server startup is not aborted (group sync is non-critical), but the violation is now visible in logs rather than silent.

**Issue 2 — Empty `code` in `OauthCallback` (`pkg/sso/oidc_provider.go`)**
Added a guard `if code == "" { http.Error(w, "missing authorization code", http.StatusBadRequest); return }` after the `errParam` check and before `oauth2Cfg.Exchange`. Test `TestOauthCallbackMissingCode` added to `oidc_provider_test.go` to cover this path.

**Issue 3 — Silent zero-values in `LoadSSOConfig` (`pkg/sso/config.go`)**
Added startup validation when `cfg.Enabled == true`: all four required fields (`client_id`, `client_secret`, `issuer_url`, `callback_base_url`) are checked together. If any are empty a single panic lists all missing fields: `"sso: missing required config fields: client_id, issuer_url"`. Typos in YAML sub-keys now fail loudly at startup rather than silently producing broken auth at runtime.

**Process issue — test output truncated before commit**
The commit was made after running `go test ... | tail -3` which showed only the final summary line (`ok github.com/treeverse/lakefs/pkg/sso`). The full per-test output was not verified before committing. In subsequent commits, `go test -v -race` is used and full output is shown.

### 2.2 Security Notes

**CSRF (state parameter):** Correctly implemented. State is 32 bytes of crypto/rand, stored in an encrypted session cookie, validated on callback, and the session is invalidated immediately after state verification to prevent fixation on error paths. Correct.

**Nonce:** Included in the browser authorization request and verified against `idToken.Nonce` on callback. Skipped for the CLI path with a comment explaining why (CLI does not send nonce to the IdP). The plan's `coreos/go-oidc` library does not auto-validate nonce — the manual check in `OauthCallback` at line 252 is essential and present.

**Open redirect in `next` parameter:** Validated using `url.Parse` with a host/scheme/prefix check. The guard against protocol-relative URLs (`//host`) and backslash tricks (`/\host`) was added in the Phase 2 security hardening pass. Correct.

**CLI redirect_uri restriction:** Validated against `http://127.0.0.1:` prefix. This prevents authorization code exfiltration to a non-loopback host. The check rejects `http://127.0.0.2` (a different loopback), `https://127.0.0.1:` (HTTPS on loopback), and IPv6 `::1`. In practice this is fine for the intended use case, but it is worth documenting.

**IdP error details:** In the callback, `error` and `error_description` from the query string are logged server-side at Warn level but not reflected to the browser. Correct; these parameters are attacker-controllable.

**Session fixation on callback error paths:** The flow session is invalidated (MaxAge=-1) immediately after state verification passes, before any error paths that follow. Correct.

**Token leakage in `ValidateSTS`:** The state parameter is logged nowhere. The authorization code is used once and discarded. No token material appears in logs. Correct.

**`BrowserLogin` state is generated locally and verified locally** (the server does not verify it — `ValidateSTS` accepts any non-empty state). The comment in `ValidateSTS` documents this design: the CLI owns state verification. This is architecturally sound because the CLI receives the code at a loopback URL it controls; if state mismatches the CLI aborts before calling STS.

### 2.3 Minor Issues

- The `fakeGroupManager` in `group_sync_test.go` returns `auth.ErrNotFound` from `AddUserToGroup` when the group does not exist, correctly matching the ACL service behavior. However `RemoveUserFromGroup` returns `nil` even when the group does not exist (instead of `auth.ErrNotFound`). This is acceptable for a test helper but means the remove-error path (`if errors.Is(rmErr, auth.ErrNotFound)`) in `SyncGroups` is not exercised by any test.

- `cmd/lakectl-sso/main.go` references the comment "cmd/lakectl/cmd/hooks.go (new file), which exports GetRoot()" — this is correct; `GetRoot` does not exist in the upstream `lakectl/cmd` package and is added by our new file.

- The `config.example.yaml` comment says "5. Run `lakefs-sso sso-migrate` once" but the actual command is `lakefs-sso sso-migrate` (subcommand registered as `sso-migrate`). The binary name format is correct.

- `LoadSSOConfig` panics on `viper.UnmarshalKey` error. This is a deliberate and documented choice for fail-fast startup. Acceptable, but a startup log at `Fatal` level would be more idiomatic for a server binary than a raw `panic`.

---

## 3. Plan vs Implementation Divergences

### 3.1 Better Than Planned

**Session fixation defense** — The plan (Section 7) listed "nonce" and "state" as security considerations but did not mention session fixation on callback error paths. The implementation invalidates the flow session immediately after state verification, before any subsequent error branch, closing a fixation window that the plan missed.

**Open-redirect guard** — The plan did not mention protocol-relative URL (`//host`) or backslash (`/\host`) redirect tricks. The implementation added `url.Parse` host/scheme checks covering both. Better than the plan specified.

**`groupManager` narrow interface** — The plan's `SyncGroups` draft took a full `auth.Service`. The implementation correctly uses a narrow `groupManager` interface with only three methods, improving testability and reducing coupling. Better than the plan specified.

**`ExtractStringSlice` helper** — Not mentioned in the plan at all. Added to handle the JSON claim type ambiguity (`string`, `[]string`, `[]interface{}`). Practical improvement.

**`LogoutURL` field in `SSOConfig`** — The plan mentions `logout_url` as part of the example YAML but does not define it explicitly in the `SSOConfig` struct listing. The field was added post-audit. Better documentation of the field's precedence over `auth.logout_redirect_url` would have been helpful from the start.

### 3.2 Worse Than Planned

**`builtAuthService` global instead of explicit dependency** — The plan's `BuildAuthenticationService` sketch (Section 3.4) does not address how `auth.Service` reaches `NativeOIDCService`. The implementation's solution (package-level mutable global with a mutex) is a workaround for the constraint that `AuthenticationServiceBuilder` cannot take additional parameters without widening the upstream type. The plan should have identified this problem and specified the global-variable approach from Phase 0. Instead, the Phase 2 implementation tried the "correct" engineering solution (explicit parameter) and then was forced to revert it.

**`LoadSSOConfig` uses `viper.UnmarshalKey` not `viper.UnmarshalExact`** — The plan identified the `viper.UnmarshalExact` issue (Section 3.2) and correctly explained why `oss_config.go` needs the `SSO` field. But it did not note that `viper.UnmarshalKey` itself is not strict. Users with typos in `sso:` sub-keys will get silent zero-value fields.

**Test file path in plan is wrong** — Section 10 lists the unit test file as `pkg/authentication/oidc_native_test.go`. The actual file is `pkg/sso/oidc_provider_test.go`. This is a planning error (see Section 4).

**Config reference mixes SSO namespace and upstream namespace** — Sections 4.1 and 4.2 of the plan use `auth.oidc_provider.enabled`, `auth.oidc_provider.client_id`, etc., which do not exist in the implementation. The implementation uses `sso.enabled`, `sso.client_id`, etc. The plan was internally inconsistent: the struct definition in Section 3.2 uses the `sso:` namespace but the config reference examples in Section 4 put SSO settings under `auth.oidc_provider:`. The `config.example.yaml` shipped with the implementation uses `sso:` and is correct, but anyone using Section 4 of the plan directly would produce a non-working config.

### 3.3 Different But Equivalent

**`logoutRedirectURL` field lifecycle** — Phase 4 initially removed `logoutRedirectURL` from `NativeOIDCService` (trusting the upstream handler to own the redirect), then Phase 4 review corrections restored it as a field read from `ssoCfg.LogoutURL` (falling back to `cfg.AuthConfig().GetBaseAuthConfig().LogoutRedirectURL`). The final approach is equivalent to the plan's intent but took two cycles to stabilize.

**`cmd/lakectl/cmd/hooks.go` not in the plan** — The plan listed the lakectl CLI wrapper as Phase 4 optional and did not detail the `GetRoot` hook. The implementation added a minimal `hooks.go` to `cmd/lakectl/cmd/` (7 lines, just `GetRoot()`) rather than copying `root.go`. This is cleaner and correct but was not specified.

### 3.4 Planning Errors

**Section 10 specifies wrong test file path** — `pkg/authentication/oidc_native_test.go` should be `pkg/sso/oidc_provider_test.go`. Pure documentation error.

**Section 3.6 `group_sync.go` draft prefixes group names** — The plan's pseudo-code in Section 3.6 does:
```go
desiredSet[cfg.ManagedGroupPrefix+g] = true
```
This would prepend the prefix to every token group name before writing to lakeFS, meaning a token role of `Admins` with prefix `sso-` would become `sso-Admins` in lakeFS. The actual implementation does not prepend — it treats `ManagedGroupPrefix` as a filter (only touch groups whose name starts with the prefix), not as a transform. The implementation is correct; the plan's pseudo-code was wrong.

**Risk table row 3 lists wrong KV partition** — Section 11 risk table, "Migration from `rbac: none` to `rbac: simplified` loses existing admin", mentions migrating to "aclauth partition". Section 0.7 correctly identifies the target as the `"auth"` partition (distinct from `"aclauth"` which is only used for the ACL setup timestamp). The risk table contained an outdated value copied before the KV partition analysis was completed.

---

## 4. Planning Mistakes and How to Avoid Them

### 4.1 Incorrect Assumption: `AuthenticationServiceBuilder` Signature Could Be Widened

**What was planned:** Phase 2 needed to pass `auth.Service` to `NativeOIDCService` for group sync. The implementation widened the `AuthenticationServiceBuilder` function type in `hooks.go` to include an `auth.Service` parameter, and updated `run.go` to pass `authService`.

**What actually happened:** This added a third change to `run.go` (beyond the two agreed lines) and rewrote the default lambda in `hooks.go`, violating the merge-friendly constraint. The fix commit (`11586a614`) reverted both and adopted a package-level variable instead.

**What the planning session should have done differently:** Before Phase 0, the planner should have asked: "In Phase 2, how will `BuildAuthenticationService` receive the `auth.Service` that `BuildAuthService` created, without widening a type that appears in `run.go`?" The answer — a package-level var in `pkg/sso/`, set by `BuildAuthService` and read by `BuildAuthenticationService` — is simple and available immediately. This design should have been specified in Phase 0 alongside the builder hook design, rather than discovered under constraint pressure in Phase 2.

**Concrete recommendation:** When designing a multi-phase architecture, identify _all_ cross-phase data dependencies before writing any code. For each piece of state that needs to flow from phase N to phase N+1, specify explicitly how it will be threaded (function argument, package-level var, context value, etc.) and verify the choice does not violate the merge-friendly constraint.

### 4.2 Incorrect Assumption: chi v5 `r.Get` After `r.Mount` Does Not Override

**What was planned / happened across three cycles:**

- Phase 3: implemented `r.Get("/logout", ...)` after `r.Mount("/logout", ...)`. Assumed by the review that this would not override the mount in chi v5.
- Phase 4 commit `b02c34b09`: replaced with `r.Use` (middleware) believing `r.Get` after `r.Mount` "never fires in chi v5 (trie matches first-registered)."
- Phase 4 review commit `e2f114bae`: reverted back to `r.Get`, with an explanation that chi v5's `setEndpoint` does in fact overwrite the mGET slot, so `r.Get` after `r.Mount` is correct.

This consumed two full refactor cycles (~80 lines changed twice) and required a test (`TestLogoutClearsOIDCSessions`) to lock in the verified behavior.

**What the planning session should have done differently:** Before specifying the logout route registration approach, verify chi v5's routing semantics once with a minimal test or by reading the chi source. The plan mentions `pkg/api/serve.go` as a key file — reading the upstream serve.go's actual logout wiring would have revealed the existing mount pattern and chi v5 behavior. Alternatively, the plan could have noted "verify chi v5 method-specific route overriding behavior before implementing" as an explicit prerequisite. One 10-minute investigation would have prevented two rework cycles.

### 4.3 Missing: `LogoutURL` in `SSOConfig` Not Planned Until Audit

**What was planned:** The plan's `SSOConfig` struct definition (Section 3.2) listed the `LogoutURL` field. However, the initial implementation omitted it, and it was only added in the post-audit commit `f69e4e181`.

**What the planning session should have done differently:** The plan was detailed enough on the struct. The miss was execution: the implementation did not verify that every planned struct field was actually written. A simple diff of the plan's struct against the written struct at the end of Phase 1 would have caught this immediately.

**Concrete recommendation:** After writing each new file, do a quick diff against the plan's specification for that file (interface, struct fields, function signatures). This is cheap and catches exactly this class of omission before the review cycle.

### 4.4 Wrong Test File Path in Section 10

**What was planned:** `pkg/authentication/oidc_native_test.go`

**What actually happened:** `pkg/sso/oidc_provider_test.go`

The test was always going to be in `pkg/sso/` because all new code was placed there. The plan's Section 10 was written before the file layout was finalized, then never updated. This is a consistency error between two sections of the same document.

**Concrete recommendation:** Keep the "File-Level Change Summary" table (Section 8) as the single source of truth for file paths. Other sections (like the testing strategy) should reference it by name rather than restating paths. At minimum, do a final consistency check before publishing a multi-section plan.

### 4.5 Plan Config Examples Use Wrong YAML Namespace

**What was planned:** Sections 4.1 and 4.2 use `auth.oidc_provider.client_id`, etc.

**What was implemented:** `sso.client_id`, etc.

The `sso:` namespace was correctly specified in Section 3.2 and in the struct definition. The config examples in Section 4 were written before the namespace was finalized and not updated. Any operator following Section 4 literally would produce a non-working config.

**Concrete recommendation:** Generate config examples programmatically from the struct (e.g., via a test that marshals a filled `SSOConfig` with sample values) rather than writing them by hand in a document. Failing that, review all config examples against the struct definition after any namespace change.

---

## 5. Context Overhead Patterns

### 5.1 Re-Reading After Each Review Cycle

The chi v5 routing mistake triggered two refactor cycles on `oidc_provider.go`. Each cycle involved reading the file, writing changes, and running review logic. The file grew from ~250 lines to ~370 lines. Estimated re-read cost per cycle: ~370 lines × 4 tokens/line = ~1,500 tokens. Across two unnecessary cycles: ~3,000 extra tokens just for file re-reads. With conversation overhead (explaining the regression, discussing the approach, agreeing on the fix): estimate 20,000–30,000 total extra tokens.

### 5.2 The `hooks.go` Widen-Then-Revert Cycle

The Phase 2 signature widening and the Phase 2.5 revert involved:
- Reading `hooks.go` (60 lines) and `run.go` (relevant portion, ~20 lines) twice extra
- Writing two versions of `hooks.go` and two versions of `authentication_builder.go`
- Discussion overhead

Estimated extra tokens: 15,000–25,000.

### 5.3 Large File Reads That Could Have Been Surgical

Appendix C of the plan identified this pattern and provided exact line ranges. Whether those ranges were actually used during implementation is not knowable from the commit history alone. Based on the plan's own estimate: reading upstream files in full (11,814 lines) vs. the surgical ranges (1,149 lines) represents a ~71,000 token difference per session. Over 3–4 sessions, the overhead from not following Appendix C could be 140,000–213,000 tokens.

### 5.4 Plan Document Re-Read Overhead

`AZURE_ENTRA_SSO_PLAN.md` is 1,071 lines; `AZURE_ENTRA_SSO_EFFORT.md` is 140 lines. If these were re-read in full at the start of each of the 8 commits (~8 implementation sessions), that is 8 × 1,071 × 4 = ~34,000 tokens just for plan document re-reads. Most sessions would only need specific sections. A session that only needed to know the config struct could read 20 lines from Section 3.2 instead of the full 1,071.

### 5.5 Multiple Review Subagents Catching the Same Class of Issue

The commit history shows security hardening applied in Phase 2 (session fixation, open redirect) and further corrections in Phase 4 (redirect_uri loopback validation, logout session clearing). Both phases had separate review subagents that independently identified security issues. A single comprehensive security review at the end of Phase 1 (before adding group sync and CLI) would have surfaced all these issues in one pass, saving one review cycle.

---

## 6. Token Consumption Estimate

This is a rough estimate. Actual token consumption depends on conversation structure, context window management, and whether Appendix C was followed.

**Inputs:**

| Component | Count | Lines each | Tokens (4/line) |
|---|---|---|---|
| New files written | 12 | ~120 avg | ~5,760 |
| Upstream files read (plan's surgical ranges) | 15 files | ~77 avg | ~4,620 |
| Plan documents | 2 | ~600 avg | ~4,800 |
| **Files written (output tokens)** | 12 | ~120 avg | ~5,760 |

But this is just raw file content. The dominant costs are:

| Source | Token estimate | Basis |
|---|---|---|
| Code output (all phases, final state) | ~80,000 | Plan's own estimate in AZURE_ENTRA_SSO_EFFORT.md |
| Context — upstream file reads, optimized | ~29,000/session × 4 sessions | Appendix C estimate |
| Context — plan documents | ~10,000/session × 4 sessions | Plan estimate |
| Context — conversation history | ~5,000/session × 4 sessions | Plan estimate |
| **Expected total (no rework)** | **~276,000** | Plan grand total for phases 0–3 |
| Rework: hooks.go widen/revert | +~20,000 | See Section 5.2 |
| Rework: chi routing (2 cycles) | +~30,000 | See Section 5.1 |
| Rework: LogoutURL + audit fixes | +~15,000 | One extra session |
| Phase 4 (CLI + review) | +~59,000 | Plan estimate |
| **Estimated actual total** | **~400,000** | |

The plan's "worst case" estimate was 480,000–530,000 tokens. The actual is likely in the 370,000–430,000 range if Appendix C was reasonably followed, or 480,000+ if upstream files were read in full each session.

These are rough estimates to ±30%. The dominant driver is context token consumption per session (plan docs + upstream files), not output code.

---

## 7. Recommendations for Future Sessions

### 7.1 Planning: What to Verify Before Writing Any Code

1. **Identify all cross-phase data dependencies upfront.** For each piece of state that must flow from an earlier phase to a later one, write down the threading mechanism explicitly in the plan. Verify it is compatible with all constraints (e.g., merge-friendly) before Phase 0.

2. **Verify library routing/API behavior with a minimal test before specifying it in the plan.** The chi v5 routing behavior cost two refactor cycles. A 10-minute `httptest` experiment to answer "does `r.Get` after `r.Mount` override?" would have prevented this. Add a "behavior verification" step to the planning checklist for any library whose routing or lifecycle semantics drive the architecture.

3. **Ensure config examples and struct definitions are generated from the same source.** Never write config examples by hand in a planning document and struct field lists in a separate section — they will diverge. Either derive examples from the struct or do a final consistency check pass before starting implementation.

4. **Use the File-Level Change Summary as the single source of truth for file paths.** All other sections should reference it. A final pass over the document checking for inconsistent paths (Section 10 had the wrong test file path) takes 5 minutes and prevents confusion during implementation.

5. **For any "permitted file modification" constraint, verify the exact set of changes at each phase boundary.** At the end of each phase, run `git diff master -- <permitted-files>` and check that the diff matches what was planned. Catch violations before they accumulate.

### 7.2 Context Hygiene

1. **Follow the surgical read map (Appendix C equivalent) strictly.** Per the plan's own estimate, reading full upstream files instead of surgical ranges costs ~71,000 extra tokens per session, or ~210,000 extra tokens across three sessions. Maintain an up-to-date read map in the plan document and treat it as a required input to each session, not optional.

2. **Do not re-read files that have not changed since the last session.** Before reading any file, check `git diff master -- <file>` to confirm it changed. If not, use the plan's cached description of the file's interface instead.

3. **Load plan documents surgically.** The plan is 1,071 lines. In a session focused only on Phase 2 group sync, only Sections 3.6, 6.1, and Appendix C (the group sync spec, the ACL group names, and the read map) are needed — roughly 80 lines out of 1,071.

4. **Collapse review findings into the plan document after each review cycle.** If a review identifies "session fixation on error paths," add it to the plan's security section immediately so the next session starts with the finding already incorporated, rather than re-discovering it from scratch.

### 7.3 Review: How to Structure Review Subagents

1. **One comprehensive security review at the end of Phase 1, before adding any features.** Phases 1, 2, and 4 all had security findings. A single exhaustive security review pass covering the complete OIDC flow would have caught all of them at once. Security issues discovered late (in Phase 4 or post-audit) cost more to fix than issues caught in Phase 1.

2. **Review subagents should be scoped to a specific question, not "review the code."** "Does this implementation correctly validate the `next` redirect parameter against open redirect?" is a 10-line review. "Review the code" spawns a full re-read. Scoped reviews are faster and less likely to duplicate findings from earlier reviews.

3. **Use a findings checklist.** After each review, append findings (even "none found") to a persistent checklist in the plan document. Before the next review subagent runs, give it the checklist so it does not re-examine already-verified items.

### 7.4 Merge-Friendly: Patterns That Work vs. Patterns That Cause Problems

**Works well:**
- Package-level function variables with setter functions (`authServiceBuilder`, `authenticationServiceBuilder`) in a new file in an existing package. Upstream adds new files to the package without conflicting. The default values preserve upstream behavior exactly.
- Package-level state in a new package (`pkg/sso/builtAuthService`) to thread state between builders that share a function-variable contract. Simple, testable with a mutex, no upstream impact.
- Registering routes via `RegisterAdditionalRoutes` (an existing upstream hook). The upstream interface explicitly supports this extension point.
- `map[string]interface{}` absorber field in `ConfigImpl` for new YAML namespaces. Clean, no behavior change, one-line addition.

**Causes problems:**
- Widening a function type that appears in an upstream file. Any `typedef` in `hooks.go` that also appears as a call in `run.go` creates a coupling: changing the type requires changing both files, and if `run.go` is an upstream-permitted-change file you exceed your budget.
- Registering routes in a way that depends on unverified library behavior (chi v5 mount/get interaction). Always verify routing semantics experimentally before specifying the approach.
- Writing config examples in plan documents without a generated validation step — they drift from the implementation.

---

## 8. Test Results

```
ok  	github.com/treeverse/lakefs/pkg/sso	1.410s
```

All 15 tests in `pkg/sso/` pass with the race detector enabled (`-race`). Tests cover:

- `TestLoginRedirect` — browser flow login redirect to IdP with state+nonce
- `TestOauthCallbackSuccess` — full callback with valid state+nonce, `oid→sub` normalization
- `TestOauthCallbackInvalidState` — state tampering rejected (400)
- `TestOauthCallbackMissingCode` — empty code param returns 400 before token exchange *(added in fix commit)*
- `TestValidateSTS` — CLI code exchange path, `oid` extraction
- `TestLogoutClearsOIDCSessions` — `r.Get` after `r.Mount` override in chi v5, session expiry
- `TestCLILoginRedirect` — CLI flow with `redirect_uri` + `state`, no session cookie set
- `TestCLILoginRedirectRejectsNonLoopback` — non-loopback `redirect_uri` rejected (400)
- `TestSyncGroups_AddAndRemove` — group add and remove
- `TestSyncGroups_Prefix_PreservesUnmanaged` — prefix-filtered sync preserves unmanaged groups
- `TestSyncGroups_Prefix_IgnoresUnprefixedTokenGroups` — unprefixed token groups not applied
- `TestSyncGroups_Disabled` — sync disabled produces no changes
- `TestSyncGroups_GroupNotFound_Skipped` — missing group skipped without error
- `TestSyncGroups_EmptyToken_RemovesAll` — empty token removes all managed groups
- `TestExtractStringSlice` — type coercion for string/[]string/[]interface{} claim values (6 subtests)

No race conditions detected. No test failures.
