# SSO Implementation Retrospective

## 1. Summary

This document records the design decisions, issues found in code review, and fixes applied for the Azure Entra ID SSO implementation in `pkg/sso/`.

---

## 2. Code Review Findings

### 2.1 Bugs / Correctness Issues

| # | Description | Status |
|---|---|---|
| 1 | `builtAuthService` silent ordering dependency — if `BuildAuthenticationService` is called before `BuildAuthService`, group sync silently never runs | **Fixed** (see 2.4) |
| 2 | `OauthCallback` forwards empty `code` to token exchange — if IdP omits the `code` param, `oauth2Cfg.Exchange` is called with an empty string, making a real HTTP request to the token endpoint with a confusing result | **Fixed** (see 2.4) |
| 3 | `LoadSSOConfig` silent zero-values for typos — `viper.UnmarshalKey` ignores unrecognised sub-keys; a typo like `clien_id` causes `ClientID == ""` with no error | **Fixed** (see 2.4) |

### 2.2 Security Notes

- `redirect_uri` in the CLI login flow is restricted to loopback addresses (`http://127.0.0.1:`) to prevent open-redirect of authorization codes to external hosts.
- The `next` redirect target in `OauthCallback` is validated to prevent open-redirect via protocol-relative URLs or backslash tricks.
- The flow session is invalidated immediately after state verification (before any subsequent error path) to prevent session-fixation attacks.
- IdP error details from the callback are logged server-side but not reflected verbatim to the browser, preventing attacker-controlled values from reaching the client.

### 2.3 Design Notes

- `builtAuthService` is a package-level variable (with a RWMutex) because lakeFS passes builder functions (not interfaces) for auth construction, making it impractical to thread the value through the function signatures without forking upstream files.
- Group sync errors are non-fatal: the user is already authenticated at the point sync runs, so failures are logged and the login proceeds.
- `NativeOIDCService.authService` field uses a `groupManager` interface (not `auth.Service`) so the dependency can be nil-checked and the coupling is minimal.

---

## 2.4 Fixes Applied

All three issues from section 2.1 were fixed after the code review. The changes are minimal and confined to `pkg/sso/`.

### Fix 1 — Explicit warning for ordering violation (`pkg/sso/authentication_builder.go`)

After reading `builtAuthService` under the RLock, a nil-check was added. When SSO is enabled and `cachedSvc` is nil, a `Warn`-level log message is emitted:

> "sso: BuildAuthService was not called before BuildAuthenticationService; group sync will be disabled"

Returning an error was deliberately not done — it would abort server startup for a non-critical feature. The warning makes the violation observable in logs without breaking the server.

### Fix 2 — Reject empty `code` before token exchange (`pkg/sso/oidc_provider.go`)

In `OauthCallback`, after extracting the `code` query parameter and after the `errParam` check (which handles explicit IdP errors), a guard was added:

```go
if code == "" {
    http.Error(w, "missing authorization code", http.StatusBadRequest)
    return
}
```

This prevents a real HTTP call to the token endpoint with an empty code string, returning a clear HTTP 400 to the browser instead.

A new test `TestOauthCallbackMissingCode` was added to `pkg/sso/oidc_provider_test.go` to cover this path. The test seeds a valid flow session (valid state), sends a callback with the correct state but no `code` parameter, and asserts a 400 response.

### Fix 3 — Panic on missing required fields when SSO enabled (`pkg/sso/config.go`)

After `viper.UnmarshalKey` and default-value assignment, when `cfg.Enabled` is true, all four required fields are checked and any missing ones are collected into a slice. If the slice is non-empty, a single panic is raised listing all missing fields:

> "sso: missing required config fields: client_id, issuer_url"

This is consistent with the existing panic on parse error in the same function and ensures typos in YAML keys are caught at startup rather than causing silent fallback or confusing runtime errors.

Files changed:
- `pkg/sso/authentication_builder.go` — Issue 1 warning
- `pkg/sso/oidc_provider.go` — Issue 2 empty-code guard
- `pkg/sso/config.go` — Issue 3 required-fields validation (+ `strings` import)
- `pkg/sso/oidc_provider_test.go` — `TestOauthCallbackMissingCode` test case
