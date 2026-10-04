# Azure Entra ID SSO Implementation Plan for lakeFS OSS

## Executive Summary

This document describes how to implement multi-user SSO based on **Azure Entra ID** (formerly Azure AD) in the open-source edition of lakeFS. The OSS edition currently delegates OIDC to an external authentication API service (the `authentication_api` endpoint), with the built-in `authentication.Service` being a `DummyService` that returns "not implemented" for OIDC. The goal is to add **native** OIDC support for Azure Entra ID directly inside the OSS `lakefs` binary, removing the need for an external auth service.

> **Critical prerequisite:** The OSS edition in its default `auth.ui_config.rbac: none` mode uses `BasicAuthService` which is hard-limited to **a single admin user** (`MaxUsers = 1`). It stores exactly one user under a fixed `superAdmin` key, returns `ErrNotImplemented` for `GetUserByExternalID`, `CreateGroup`, `AddUserToGroup`, and all multi-user/RBAC operations. SSO is fundamentally a multi-user feature and **cannot work with `BasicAuthService`**. This plan addresses the auth service layer change as a mandatory prerequisite (see Section 0).

---

## 0. Prerequisite: Replace BasicAuthService with a Multi-User Auth Service

### 0.1 The Problem

The OSS `lakefs` binary selects its `auth.Service` implementation in `pkg/auth/build.go`:

```
RBAC mode "none" (default) → BasicAuthService  (1 user, no groups, no policies, no external IDs)
RBAC mode "simplified"     → requires auth.api.endpoint → external ACL service (contrib/auth/acl)
RBAC mode "internal"/"external" → requires auth.api.endpoint → external full RBAC service (lakeFS Enterprise / Cloud)
```

#### What each RBAC mode actually provides

| Mode | Auth Service | Multi-user | Groups | Per-repo policies | Authorization model |
|---|---|---|---|---|---|
| `none` | `BasicAuthService` | No (1 user) | No | No | If user exists → allow all |
| `simplified` | `acl.AuthService` (in `contrib/auth/acl`) | Yes | Yes (4 fixed tiers) | **No** — permissions are global | ACL: Read / Write / Super / Admin applied to **all repositories** |
| `internal` | Enterprise auth API | Yes | Yes (custom) | **Yes** — per-user and per-group | Full IAM-style RBAC: policies with `Action` + `Resource` statements, `arn:lakefs:repos/<repo>` scoping |
| `external` | Enterprise auth API | Yes | Yes (custom) | **Yes** — per-user and per-group | Same as `internal`, managed by external service |

The ACL model (`simplified`) defines exactly 4 permission levels in `contrib/auth/acl/permission.go`:

| ACL Level | Group | Grants |
|---|---|---|
| `Read` | `Readers` | Read all repos, read config, manage own credentials |
| `Write` | `Writers` | Read + write all repos, manage own credentials, read repo management |
| `Super` | `Supers` | Full FS access on all repos, manage own credentials, read repo management |
| `Admin` | `Admins` | Everything: all repos, all users, all credentials, all config |

**Key limitation:** In `simplified` mode there is no way to say "user X can write to repo A but only read repo B". All permissions apply globally across every repository. Per-repository scoping requires `internal`/`external` mode (Enterprise).

#### Why this matters for SSO

SSO is fundamentally a multi-user feature. `BasicAuthService` (`pkg/auth/basic_service.go`) enforces:
- `MaxUsers = 1`, `MaxCredentialsPerUser = 1`
- Single user stored under the fixed key `superAdmin`
- `GetUserByExternalID` → `ErrNotImplemented` (OIDC callback will fail here)
- `CreateUser` overwrites the single admin slot (no multi-user)
- `CreateGroup`, `AddUserToGroup`, `ListGroups` → all `ErrNotImplemented`
- `Authorize` → if user exists, allow everything (no RBAC at all)

The OIDC session handler `UserFromOIDCSession` (`pkg/auth/request_auth.go`) calls:
1. `authService.GetUserByExternalID(ctx, externalID)` — **fails with `ErrNotImplemented`**
2. `authService.CreateUser(ctx, &user)` — would overwrite the admin
3. `authService.AddUserToGroup(ctx, username, group)` — **fails with `ErrNotImplemented`**

**Conclusion:** Every step of the OIDC user provisioning flow is broken on `BasicAuthService`. This plan uses `acl.AuthService` (`simplified` mode) embedded in-process, which gives us multi-user + groups + authorization. The trade-off is that authorization is **global (not per-repo)**. See Section 6 for the mapping between Azure roles and ACL tiers, and Section 6.2 for how to extend to full RBAC if needed later.

### 0.2 Design Principle: Merge-Friendly Architecture

Upstream lakeFS is actively developed. To keep our fork easy to rebase/merge, we follow these rules:

1. **Never modify upstream files.** Don't edit `build.go`, `factory.go`, `request_auth.go`, `config.go`, `serve.go`, or any existing file that upstream will also change.
2. **Add new files only.** All our code goes in new, clearly-namespaced files that won't conflict with upstream additions.
3. **Use compile-time wiring via a separate `main.go` (or build tags).** Override the entry point to inject our services, rather than patching the factory/builder.
4. **Depend on upstream interfaces, not implementations.** `auth.Service`, `authentication.Service`, `config.Config` are stable interfaces — program against them.

### 0.3 Solution: Out-of-Tree Wrapper Binary

Instead of modifying any upstream source files, we create a **thin wrapper `main.go`** in a new directory (e.g., `cmd/lakefs-sso/`) that:
1. Reuses all upstream code via imports
2. Overrides only the auth service construction and authentication service construction
3. Produces a `lakefs-sso` binary — functionally identical to `lakefs` but with SSO support

```
cmd/
  lakefs/          ← upstream, untouched
  lakefs-sso/      ← our wrapper (new directory, ~100 lines)
    main.go
pkg/
  auth/            ← upstream, untouched
  authentication/  ← upstream, untouched
  sso/             ← all our new code lives here (new directory)
    config.go          — SSO config extension
    oidc_provider.go   — NativeOIDCService
    oidc_provider_test.go
    auth_builder.go    — builds acl.AuthService in-process + wraps OIDC config
    group_sync.go      — group sync on login
```

**How it works:**

The upstream `cmd/lakefs/cmd/run.go` calls:
- `auth.NewAuthService(ctx, cfg, ...)` — selects `BasicAuthService` or API service
- `authentication.NewAuthenticationService(ctx, cfg, ...)` — selects `DummyService` or API service

Our `cmd/lakefs-sso/main.go` replaces these two construction calls:

```go
package main

import (
    // import the upstream cmd package to reuse all CLI infrastructure
    "github.com/treeverse/lakefs/cmd/lakefs/cmd"
    "github.com/treeverse/lakefs/pkg/sso"
)

func main() {
    // Register our custom auth/authentication service builders
    // via the hooks that we expose (see 0.4)
    cmd.SetAuthServiceBuilder(sso.BuildAuthService)
    cmd.SetAuthenticationServiceBuilder(sso.BuildAuthenticationService)
    cmd.Execute()
}
```

### 0.4 The Only Upstream Touch: Service Builder Hooks in `run.go`

To make the wrapper possible without copy-pasting all of `run.go`, we need **one small, non-conflicting addition** — two package-level function variables in `cmd/lakefs/cmd/` that allow overriding the service construction:

**New file: `cmd/lakefs/cmd/hooks.go`** (will not conflict with any upstream file):

```go
package cmd

import (
    "context"
    "github.com/treeverse/lakefs/pkg/auth"
    "github.com/treeverse/lakefs/pkg/authentication"
    "github.com/treeverse/lakefs/pkg/config"
    "github.com/treeverse/lakefs/pkg/kv"
    "github.com/treeverse/lakefs/pkg/logging"
)

type AuthServiceBuilder func(ctx context.Context, cfg config.Config, logger logging.Logger, kvStore kv.Store, metadataManager *auth.KVMetadataManager) auth.Service

type AuthenticationServiceBuilder func(ctx context.Context, cfg config.Config, logger logging.Logger) (authentication.Service, error)

// Defaults: upstream behavior
var authServiceBuilder AuthServiceBuilder = func(ctx context.Context, cfg config.Config, logger logging.Logger, kvStore kv.Store, mm *auth.KVMetadataManager) auth.Service {
    return auth.NewAuthService(ctx, cfg, logger, kvStore, mm)
}

var authenticationServiceBuilder AuthenticationServiceBuilder = authentication.NewAuthenticationService

func SetAuthServiceBuilder(b AuthServiceBuilder) { authServiceBuilder = b }
func SetAuthenticationServiceBuilder(b AuthenticationServiceBuilder) { authenticationServiceBuilder = b }
```

Then `run.go` needs **two one-line changes** (the only modifications to an existing upstream file):

```diff
- authService := auth.NewAuthService(ctx, cfg, logger, kvStore, authMetadataManager)
+ authService := authServiceBuilder(ctx, cfg, logger, kvStore, authMetadataManager)

- authenticationService, err := authentication.NewAuthenticationService(ctx, cfg, logger)
+ authenticationService, err := authenticationServiceBuilder(ctx, cfg, logger)
```

**Why this is merge-safe:**
- `hooks.go` is a new file — zero conflict on merge
- The two `run.go` changes are trivial variable renames on isolated lines — even if upstream refactors `run.go`, the conflict is a trivial 1-line resolution
- Default behavior is identical to upstream (the defaults call the original functions)
- If upstream ever adds their own hook mechanism, we just delete ours and adapt

### 0.5 Alternative: Zero Upstream Changes (copy `run.go`)

If even two lines in `run.go` is too much, we can **copy `run.go` into `cmd/lakefs-sso/`** and modify the copy. The downside is that when upstream changes `run.go`, we must manually sync the copy. Given that `run.go` changes ~2-3 times per year in the auth-relevant section, this is manageable.

### 0.6 ACL Auth Service — In-Process, No Upstream Changes

Since `contrib/auth/acl/` is in the **same Go module** (`github.com/treeverse/lakefs`), we can import it freely from our new `pkg/sso/auth_builder.go`:

```go
package sso

func BuildAuthService(ctx context.Context, cfg config.Config, logger logging.Logger, kvStore kv.Store, mm *auth.KVMetadataManager) auth.Service {
    ssoCfg := LoadSSOConfig()  // reads our SSO-specific config keys
    if !ssoCfg.Enabled {
        // Fall back to upstream default
        return auth.NewAuthService(ctx, cfg, logger, kvStore, mm)
    }
    
    // Use the ACL multi-user auth service in-process
    secretStore := crypt.NewSecretStore([]byte(cfg.AuthConfig().GetBaseAuthConfig().Encrypt.SecretKey))
    aclService := acl.NewAuthService(kvStore, secretStore, authparams.ServiceCache(cfg.AuthConfig().GetBaseAuthConfig().Cache))
    
    // Bootstrap default groups/policies on first run.
    // IMPORTANT: call SetupACLServer on the concrete *acl.AuthService BEFORE wrapping
    // with NewMonitoredAuthService — SetupACLServer requires the concrete type.
    if err := acl.SetupACLServer(ctx, aclService); err != nil {
        logger.WithError(err).Warn("ACL setup (may already exist)")
    }
    
    // Wrap after bootstrap
    return auth.NewMonitoredAuthService(aclService)
}
```

This touches **zero upstream files**. The ACL service code is already there, tested, and maintained by upstream.

### 0.7 Migration Considerations

- **Existing single-user installs:** Users switching from `lakefs` to `lakefs-sso` need to migrate the admin user from `BasicAuthService` to `acl.AuthService`. Provide a `lakefs-sso migrate` CLI command.
- **KV partition mapping (verified from source):**
  - `BasicAuthService` stores the single user under KV partition `"basicAuth"` (`BasicPartitionKey`) — `pkg/auth/basic_service.go:19`
  - `acl.AuthService` stores all users/groups/policies/credentials under KV partition `"auth"` (`model.PartitionKey`) — `pkg/auth/model/model.go:23`. **Not `"aclauth"`.** (`ServerPartitionKey = "aclauth"` in `acl/service.go:21` is only used for the setup timestamp, not for user data.)
  - Migration reads the single user + credentials from `"basicAuth"` partition and writes them to the `"auth"` partition
- **Access keys:** Must be copied from `basicAuth` partition to `auth` partition.
- **Backward compatibility:** Running plain `lakefs` (upstream binary) remains unchanged. Our changes only take effect in the `lakefs-sso` binary.

### 0.8 Merge Conflict Surface Summary

| Area | Files Modified | Conflict Risk |
|---|---|---|
| `cmd/lakefs/cmd/hooks.go` | **New file** | None (new file) |
| `cmd/lakefs/cmd/run.go` | 2 lines changed | Trivial (variable rename) |
| `cmd/lakefs-sso/` | **New directory** | None (new directory) |
| `pkg/sso/` | **New directory** | None (new directory) |
| All other upstream files | **Untouched** | None |

On upstream merge/rebase: resolve the 2-line diff in `run.go` (or, with the copy approach, sync the copied `run.go`).

---

## 1. Current Auth Architecture Analysis

### 1.1 Authentication Flow (today)

```
Browser / Client
    │
    ├─ Basic Auth (access_key + secret) ──► BuiltinAuthenticator
    ├─ JWT Bearer token ──────────────────► UserByToken (verify JWT)
    ├─ cookie_auth (internal session) ────► UserByToken (from session)
    ├─ oidc_auth (oidc_auth_session) ─────► UserFromOIDCSession (claims from cookie)
    └─ saml_auth (saml_auth_session) ─────► UserFromSAMLSession (claims from cookie)
```

### 1.2 Key Files

| File | Role |
|---|---|
| `pkg/authentication/service.go` | `Service` interface: `ValidateSTS`, `OauthCallback`, `RegisterAdditionalRoutes` |
| `pkg/authentication/factory.go` | Creates `DummyService` (OSS) or `APIService` (when `authentication_api.endpoint` is set) |
| `pkg/auth/request_auth.go` | `UserFromOIDCSession` / `UserFromSAMLSession` — create-or-get user from session claims |
| `pkg/api/auth_middleware.go` | `checkSecurityRequirements` — dispatches to auth providers by security scheme name |
| `pkg/api/serve.go` | Wires OIDC config, session store, and calls `authenticationService.RegisterAdditionalRoutes` |
| `pkg/api/controller.go` | `StsLogin` handler (exchanges code for JWT), `OauthCallback` handler |
| `pkg/config/config.go` | `OIDC` struct, `CookieAuthVerification` struct, `BaseAuth` struct |
| `pkg/auth/oidc/encoding/encoding.go` | Gob-serializable `Claims` type for session storage |
| `cmd/lakefs/cmd/run.go` | Wires everything together at startup |

### 1.3 What Exists vs What's Missing

**Exists:**
- Session cookie infrastructure (`gorilla/sessions`)
- `oidc_auth` security scheme in middleware — reads claims from `oidc_auth_session` cookie
- `UserFromOIDCSession` — maps OIDC claims to a lakeFS user (auto-creates user, assigns groups)
- Config structs for OIDC claims validation, initial groups, friendly name
- `/oidc/callback` endpoint definition in swagger
- `/sts/login` endpoint for exchanging auth code for JWT token
- `oidc/encoding` package for gob-serializable claims

**Missing:**
- **OIDC provider** — no `coreos/go-oidc` or equivalent; `DummyService.OauthCallback` returns 501
- **OAuth2 authorization code flow** — no redirect to IdP, no code exchange, no token validation
- **OIDC discovery** — no fetching of `.well-known/openid-configuration`
- **ID token verification** — no JWKS fetching or signature validation
- **Configuration for OIDC provider** — no `client_id`, `client_secret`, `issuer_url`, `redirect_uri`, scopes
- **UI login redirect** — the OSS login page shows username/password only; no "Login with SSO" button
- **Group sync** — initial groups are set at user creation but never updated on subsequent logins
- **Logout** — no OIDC RP-initiated logout (end_session_endpoint)

---

## 2. Azure Entra ID Specifics

### 2.1 Endpoints (v2.0)

| Endpoint | URL |
|---|---|
| Discovery | `https://login.microsoftonline.com/{tenant-id}/v2.0/.well-known/openid-configuration` |
| Authorization | `https://login.microsoftonline.com/{tenant-id}/oauth2/v2.0/authorize` |
| Token | `https://login.microsoftonline.com/{tenant-id}/oauth2/v2.0/token` |
| JWKS | `https://login.microsoftonline.com/{tenant-id}/discovery/v2.0/keys` |
| End Session | `https://login.microsoftonline.com/{tenant-id}/oauth2/v2.0/logout` |

### 2.2 Token Claims (relevant subset)

| Claim | Use |
|---|---|
| `oid` | **User identity** — immutable Object ID, unique within a tenant. Use as `external_id`. |
| `tid` | Tenant ID — identifies the Azure tenant |
| `preferred_username` | Human-readable (e.g. `user@contoso.com`) — use for `friendly_name`. **Mutable.** |
| `email` | Optional claim — must be explicitly requested in token config |
| `name` | Display name |
| `groups` | Array of group Object ID GUIDs (subject to 200-group overage limit) |
| `roles` | Array of app role string values (no overage limit) — **recommended** |
| `sub` | Subject — unique per-application, not per-tenant. Less useful than `oid`. |

### 2.3 Critical Design Decisions

1. **Use `oid` (not `sub` or `email`) as the external user identifier.** The `oid` is immutable and unique within a tenant. The current `UserFromOIDCSession` uses `sub` — we must make the claim configurable.

2. **Prefer App Roles over raw group claims.** Azure's `groups` claim has a 200-group JWT limit (overage problem). With App Roles, you define roles in the Azure app registration (e.g., `Admin`, `Developer`, `Viewer`), assign Azure groups to those roles, and the `roles` claim in the token lists the matched role values. No overage.

3. **Single-tenant first, multi-tenant later.** Single-tenant is simpler (static issuer validation). Multi-tenant requires dynamic issuer validation because `/organizations` is not the actual issuer.

---

## 3. Implementation Plan

### Phase 1: Core OIDC Provider (Backend)

#### 3.1 Add `coreos/go-oidc/v3` dependency

```bash
go get github.com/coreos/go-oidc/v3/oidc
```

This library handles OIDC discovery, JWKS fetching, key rotation, and ID token verification. It is the de facto standard for Go OIDC relying parties. `golang.org/x/oauth2` is already an indirect dependency and will be used alongside it for the authorization code flow.

#### 3.2 SSO config: `pkg/sso/config.go` + one-field addition to `pkg/config/oss_config.go`

Define the SSO config as a **separate struct** read from the same YAML file under a new `sso:` top-level key.

> **viper.UnmarshalExact caveat:** `pkg/config/config.go:610` calls `viper.UnmarshalExact` which uses mapstructure's `ErrorUnused: true`. If `sso:` appears in the YAML but is not declared as a field in `ConfigImpl`, startup will fail with "field sso not found in struct". Fix: add one field to `pkg/config/oss_config.go`:
>
> ```go
> type ConfigImpl struct {
>     BaseConfig `mapstructure:",squash"`
>     Auth       Auth                   `mapstructure:"auth"`
>     UI         UI                     `mapstructure:"ui"`
>     SSO        map[string]interface{} `mapstructure:"sso"` // absorbed; parsed by pkg/sso/config.go
> }
> ```
>
> This is the second and final upstream file touched (1 field addition). It changes no behavior — `ConfigImpl.SSO` is never read by the upstream code, only by `pkg/sso/config.go` via `viper.UnmarshalKey("sso", &ssoCfg)`.

The full SSO config struct:

```go
// pkg/sso/config.go
package sso

type SSOConfig struct {
    Enabled             bool              `mapstructure:"enabled"`
    ClientID            string            `mapstructure:"client_id"`
    ClientSecret        string            `mapstructure:"client_secret"`
    IssuerURL           string            `mapstructure:"issuer_url"`
    CallbackBaseURL     string            `mapstructure:"callback_base_url"`
    Scopes              []string          `mapstructure:"scopes"`
    UserIDClaim         string            `mapstructure:"user_id_claim"`
    FriendlyNameClaim   string            `mapstructure:"friendly_name_claim"`
    GroupsClaim         string            `mapstructure:"groups_claim"`
    DefaultGroups       []string          `mapstructure:"default_groups"`
    SyncGroupsOnLogin   bool              `mapstructure:"sync_groups_on_login"`
    ManagedGroupPrefix  string            `mapstructure:"managed_group_prefix"`
    AuthorizeParams     map[string]string `mapstructure:"authorize_params"`
    LogoutURL           string            `mapstructure:"logout_url"`
}

func LoadSSOConfig() *SSOConfig {
    // Read from viper under "sso" prefix — completely independent of upstream config keys
    cfg := &SSOConfig{}
    viper.UnmarshalKey("sso", cfg)
    if len(cfg.Scopes) == 0 {
        cfg.Scopes = []string{"openid", "profile", "email"}
    }
    if cfg.UserIDClaim == "" {
        cfg.UserIDClaim = "oid"
    }
    return cfg
}
```

The YAML config becomes:

```yaml
# Our SSO section — completely separate from upstream auth config
sso:
  enabled: true
  client_id: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
  client_secret: "your-secret"
  issuer_url: "https://login.microsoftonline.com/{tenant-id}/v2.0"
  callback_base_url: "https://lakefs.example.com"
  user_id_claim: "oid"
  friendly_name_claim: "preferred_username"
  groups_claim: "roles"
  default_groups: ["Viewers"]
  sync_groups_on_login: true

# Upstream config — only set values, never modify schema
auth:
  ui_config:
    rbac: simplified
    login_url: "/oidc/login"
    login_url_method: "redirect"
    fallback_login_label: "Login with Access Key"
    login_cookie_names: ["oidc_auth_session", "internal_auth_session"]
    logout_url: "/logout"
  logout_redirect_url: "https://login.microsoftonline.com/{tenant-id}/oauth2/v2.0/logout?..."
```

**Why this is merge-safe:** We never touch `pkg/config/config.go`. Viper reads keys hierarchically, so our `sso.*` keys coexist with upstream's `auth.*` keys in the same YAML file without conflict.

#### 3.3 Implement native OIDC service: `pkg/sso/oidc_provider.go` (new file)

Create a new `NativeOIDCService` in `pkg/sso/` that implements the `authentication.Service` interface:

```go
type NativeOIDCService struct {
    provider     *oidc.Provider
    oauth2Config oauth2.Config
    verifier     *oidc.IDTokenVerifier
    userIDClaim  string
    logger       logging.Logger
    // state store: map[string]stateEntry with TTL (or use encrypted cookie)
}
```

**Key methods:**

1. **`RegisterAdditionalRoutes(r *chi.Mux, sessionStore sessions.Store)`**
   - Register `GET /oidc/login` — initiates the OIDC flow:
     - Generate random `state` + `nonce`, store in session
     - Redirect to Azure's `/authorize` endpoint with `response_type=code`, `scope=openid profile email`, `state`, `nonce`
   
2. **`OauthCallback(w http.ResponseWriter, r *http.Request, sessionStore sessions.Store)`**
   - Handle the callback at `/oidc/callback`:
     - Validate `state` parameter against session
     - Exchange `code` for tokens using `oauth2.Config.Exchange`
     - Extract `id_token` from the token response
     - Verify ID token signature and claims using `oidc.IDTokenVerifier`
     - Validate `nonce` from the ID token against the session
     - Extract claims into `oidcencoding.Claims`
     - Store claims in `oidc_auth_session` cookie (existing `IDTokenClaimsSessionKey`)
     - Redirect to UI (the `next` param or `/`)

3. **`ValidateSTS(ctx context.Context, code, redirectURI, state string) (string, error)`**
   - Used by the `/sts/login` endpoint for programmatic (non-browser) login
   - Exchange code for tokens, verify ID token, return external user ID
   - This enables `lakectl` and SDKs to authenticate via OIDC

4. **`IsExternalPrincipalsEnabled() bool`** — return `false` (not applicable)

5. **`ExternalPrincipalLogin`** — return `ErrNotImplemented` (not applicable)

#### 3.4 Wire via the builder hook (no factory modification)

The `cmd/lakefs-sso/main.go` registers our custom `AuthenticationServiceBuilder`:

```go
// pkg/sso/authentication_builder.go (new file)
package sso

func BuildAuthenticationService(ctx context.Context, cfg config.Config, logger logging.Logger) (authentication.Service, error) {
    ssoCfg := LoadSSOConfig()
    if !ssoCfg.Enabled {
        // Fall back to upstream default
        return authentication.NewAuthenticationService(ctx, cfg, logger)
    }
    return NewNativeOIDCService(ssoCfg, logger)
}
```

**No modification to `pkg/authentication/factory.go`** — the upstream file is untouched.

#### 3.5 Work around the hardcoded `sub` claim in `UserFromOIDCSession`

The upstream `UserFromOIDCSession` (`pkg/auth/request_auth.go:155`) hardcodes `sub` as the external user identifier:

```go
externalID, ok := idTokenClaims["sub"].(string)
```

For Azure Entra ID we need `oid` instead. **We do NOT modify this upstream file.** Instead, we solve this at the claims-injection level in our `OauthCallback`:

When `NativeOIDCService.OauthCallback` stores claims in the session cookie, it **copies the value of the configured user ID claim (`oid`) into the `sub` key**:

```go
// pkg/sso/oidc_provider.go — inside OauthCallback, after extracting claims
claims := oidcencoding.Claims{}
if err := idToken.Claims(&claims); err != nil { ... }

// Map our configured user ID claim into "sub" so upstream UserFromOIDCSession works unchanged
if s.config.UserIDClaim != "sub" {
    if val, ok := claims[s.config.UserIDClaim]; ok {
        claims["sub"] = val
    }
}

// Store in session — upstream reads claims["sub"] and it works
session.Values[auth.IDTokenClaimsSessionKey] = claims
```

**Why this works:** The upstream code reads `sub` from the claims stored in the session cookie. We control what goes into that cookie. By normalizing the claim before storage, the upstream code works unmodified.

Similarly, for group claims: the upstream reads `idTokenClaims[oidcConfig.InitialGroupsClaimName]`. Since `InitialGroupsClaimName` is already configurable via upstream's `auth.oidc.initial_groups_claim_name` config key, we just set it to `roles` in the YAML — no code change needed.

---

### Phase 2: Group Synchronization

#### 3.6 Add group sync on every login: `pkg/sso/group_sync.go` (new file)

Currently, upstream assigns groups only at user creation time (first login). On subsequent logins, group membership is not updated. This is a problem because:
- Users may be added to or removed from Entra ID groups/roles
- The lakeFS group membership drifts from the source of truth

**We do NOT modify `pkg/auth/request_auth.go`.** Instead, group sync runs as a **post-login hook** inside our `OauthCallback`, after the session is established and the user is created/found:

```go
// pkg/sso/group_sync.go (new file)
package sso

func SyncGroups(ctx context.Context, authService auth.Service, username string, tokenGroups []string, cfg *SSOConfig) error {
    if !cfg.SyncGroupsOnLogin {
        return nil
    }
    
    // Get current lakeFS groups for this user
    currentGroups, _, _ := authService.ListUserGroups(ctx, username, &model.PaginationParams{Amount: 1000})
    currentSet := map[string]bool{}
    for _, g := range currentGroups {
        // Only manage groups with our prefix to avoid touching manually-assigned groups
        if strings.HasPrefix(g.DisplayName, cfg.ManagedGroupPrefix) {
            currentSet[g.DisplayName] = true
        }
    }
    
    // Compute diff
    desiredSet := map[string]bool{}
    for _, g := range tokenGroups {
        desiredSet[cfg.ManagedGroupPrefix+g] = true
    }
    
    // Add missing
    for g := range desiredSet {
        if !currentSet[g] { authService.AddUserToGroup(ctx, username, g) }
    }
    // Remove stale
    for g := range currentSet {
        if !desiredSet[g] { authService.RemoveUserFromGroup(ctx, username, g) }
    }
    return nil
}
```

This is called from `NativeOIDCService.OauthCallback` after the session is stored and the redirect is about to happen. It reads from the `auth.Service` interface (which the ACL service implements).

#### 3.7 Handle Azure group overage (optional, Phase 2b)

When a user has >200 groups, Azure omits the `groups` claim and provides a `_claim_names`/`_claim_sources` fallback pointing to MS Graph. If you use **App Roles** (recommended), this is not needed. But for environments using raw group claims:

1. Detect overage: check for `_claim_names.groups` in token claims
2. Call the MS Graph API endpoint from `_claim_sources` to fetch full group list
3. Requires: requesting `GroupMember.Read.All` scope, storing the access token

This is complex and should only be implemented if App Roles cannot be used. **Recommendation: document App Roles as the primary approach and defer Graph-based overage handling.**

---

### Phase 3: UI Changes

#### 3.8 Add "Login with SSO" button: `webui/src/pages/auth/login.tsx`

The existing login page supports a `fallback_login_url` mechanism — a secondary login button that redirects to an external URL. This can be reused for OIDC:

- Set `auth.ui_config.fallback_login_url` to `/oidc/login` (the route registered by `NativeOIDCService`)
- Set `auth.ui_config.fallback_login_label` to `"Login with Azure SSO"`

This requires **no UI code changes** — only configuration.

However, for a better UX with SSO-first login (no access key form), support the existing `login_url` + `login_url_method` mechanism:

- `auth.ui_config.login_url: /oidc/login` — the primary login URL
- `auth.ui_config.login_url_method: redirect` — auto-redirect to OIDC (skip the form)
- `auth.ui_config.fallback_login_url: /auth/login` — allow fallback to access key login
- `auth.ui_config.fallback_login_label: "Login with Access Key"` — label for fallback

The login page already handles `login_url_method: redirect` — it will redirect to the OIDC login endpoint automatically.

#### 3.9 Add OIDC logout: `pkg/api/serve.go`

Configure `LogoutRedirectURL` to Azure's end_session_endpoint:

```
https://login.microsoftonline.com/{tenant-id}/oauth2/v2.0/logout?post_logout_redirect_uri=https://lakefs.example.com/auth/login
```

The existing `/logout` handler in `serve.go` already clears the session cookie and redirects to `LogoutRedirectURL`. Wire this through config:

```yaml
auth:
  logout_redirect_url: "https://login.microsoftonline.com/{tenant-id}/oauth2/v2.0/logout?post_logout_redirect_uri=https://lakefs.example.com/auth/login"
  ui_config:
    logout_url: "/logout"
    login_cookie_names:
      - "oidc_auth_session"
      - "internal_auth_session"
```

---

### Phase 4: CLI (`lakectl`) SSO Support

#### 3.10 OIDC device code flow or browser-based flow for `lakectl`

For `lakectl` to authenticate via SSO, implement one of:

**Option A: Browser-based flow (recommended)**
1. `lakectl` starts a local HTTP server on a random port (e.g., `http://localhost:54321/callback`)
2. Opens the browser to `/oidc/login?redirect_uri=http://localhost:54321/callback`
3. User authenticates in browser, callback returns code to local server
4. `lakectl` exchanges code via `/sts/login` endpoint to get a JWT
5. Stores JWT in `~/.lakectl.yaml` or OS keychain

**Option B: Device code flow**
- Azure Entra ID supports the device authorization grant
- `lakectl` displays a URL + code, user enters it in browser
- `lakectl` polls Azure's token endpoint until authorization is granted
- More complex but works in headless environments

**Recommendation:** Start with Option A (browser-based). The `/sts/login` endpoint already handles the code→JWT exchange. Add a `lakectl auth login --sso` command.

---

## 4. Configuration Reference

### 4.1 Minimal Azure Entra ID Configuration

```yaml
auth:
  oidc_provider:
    enabled: true
    client_id: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"     # Application (client) ID
    client_secret: "your-client-secret-value"
    callback_base_url: "https://lakefs.example.com"         # External URL of lakeFS
    scopes:
      - openid
      - profile
      - email
    user_id_claim: "oid"                                    # Use Azure Object ID
    authorize_endpoint_params:
      prompt: "select_account"                              # Optional: force account picker

  oidc:
    validate_id_token_claims:
      iss: "https://login.microsoftonline.com/{tenant-id}/v2.0"
    default_initial_groups:
      - "Viewers"
    initial_groups_claim_name: "roles"                      # Use App Roles
    friendly_name_claim_name: "preferred_username"
    persist_friendly_name: true
    user_id_claim: "oid"
    sync_groups_on_login: true

  logout_redirect_url: "https://login.microsoftonline.com/{tenant-id}/oauth2/v2.0/logout?post_logout_redirect_uri=https://lakefs.example.com/auth/login"

  ui_config:
    login_url: "/oidc/login"
    login_url_method: "redirect"
    fallback_login_url: ""
    fallback_login_label: "Login with Access Key"
    login_cookie_names:
      - "oidc_auth_session"
      - "internal_auth_session"
    logout_url: "/logout"
```

### 4.2 Dual-Mode Configuration (SSO + Access Keys)

```yaml
auth:
  oidc_provider:
    enabled: true
    client_id: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    client_secret: "your-client-secret-value"
    callback_base_url: "https://lakefs.example.com"
    scopes: [openid, profile, email]
    user_id_claim: "oid"

  oidc:
    validate_id_token_claims:
      iss: "https://login.microsoftonline.com/{tenant-id}/v2.0"
    default_initial_groups: ["Viewers"]
    initial_groups_claim_name: "roles"
    friendly_name_claim_name: "preferred_username"
    persist_friendly_name: true
    user_id_claim: "oid"

  ui_config:
    login_url: ""                                            # Shows the standard login form
    fallback_login_url: "/oidc/login"                        # "Login with SSO" button
    fallback_login_label: "Login with Azure SSO"
    login_cookie_names:
      - "oidc_auth_session"
      - "internal_auth_session"
    logout_url: "/logout"
```

---

## 5. Azure Entra ID App Registration Setup

### Step-by-step

1. **Register the application** in [Microsoft Entra admin center](https://entra.microsoft.com):
   - Identity → Applications → App registrations → New registration
   - Name: `lakeFS`
   - Supported account types: "Accounts in this organizational directory only" (single-tenant)
   - Redirect URI: Platform = **Web**, URI = `https://<lakefs-host>/oidc/callback`

2. **Note the IDs:**
   - Application (client) ID → `auth.oidc_provider.client_id`
   - Directory (tenant) ID → used in issuer URL

3. **Create client secret:**
   - Certificates & secrets → New client secret
   - Copy the Value → `auth.oidc_provider.client_secret`

4. **Configure token claims:**
   - Token configuration → Add optional claim → ID token: `email`, `preferred_username`
   - Add groups claim: select "Security groups" or "Groups assigned to the application"

5. **Configure App Roles** (recommended over raw groups):
   - App roles → Create app role for each ACL tier:
     - Display name: `Admin` / `Super` / `Writer` / `Reader`
     - Allowed member types: Users/Groups
     - Value: `Admins` / `Supers` / `Writers` / `Readers` (must match lakeFS ACL group names exactly)
   - Enterprise applications → Your App → Users and groups → Assign Azure groups to roles
   - Note: these four values correspond to the ACL groups bootstrapped by `acl.SetupACLServer()` in `contrib/auth/acl/setup.go`

6. **API permissions:**
   - Ensure `openid`, `profile`, `email` delegated permissions under Microsoft Graph
   - Grant admin consent

---

## 6. Authorization Model & Azure Role Mapping

### 6.1 ACL Mode (this plan — `simplified`)

This plan uses `acl.AuthService` which provides **simplified ACL authorization**. There are exactly 4 permission tiers, each applying **globally to all repositories**:

| Azure App Role (value) | lakeFS ACL Group | ACL Permission | What it grants |
|---|---|---|---|
| `Admins` | `Admins` | `Admin` | Everything: all repos, all users, all credentials, all config |
| `Supers` | `Supers` | `Super` | Full FS access on all repos + read repo management + own credentials |
| `Writers` | `Writers` | `Write` | Read/write on all repos + own credentials + read repo management |
| `Readers` | `Readers` | `Read` | Read-only on all repos + read config + own credentials |

The ACL groups (`Admins`, `Supers`, `Writers`, `Readers`) are **bootstrapped automatically** by `acl.SetupACLServer()` on first run (see `contrib/auth/acl/setup.go`). Each group has a hard-coded policy generated by `ACLToStatement()` in `contrib/auth/acl/permission.go`.

The Azure App Role `value` field in the app registration **must match** the lakeFS group name exactly. The `roles` claim in the Entra ID token will contain an array like `["Admins", "Writers"]`, and `initialGroupsFromClaims` will parse them and add the user to the corresponding lakeFS groups.

With `sync_groups_on_login: true`, group membership is updated on every login — if a user loses a role in Azure, they lose the corresponding lakeFS group on next login.

**What you can do:**
- Assign users to one or more ACL tiers via Azure App Roles
- The highest tier wins (a user in both `Readers` and `Writers` gets Write-level access)
- All operations are authorized through `acl.AuthService.Authorize()` which checks group membership → policy statements

**What you cannot do in ACL mode:**
- Grant user A write access to `repo-X` but only read access to `repo-Y`
- Define custom policies with specific action/resource combinations
- Scope permissions to individual repositories, branches, or paths

### 6.2 Upgrade Path to Full RBAC (future)

If per-repository authorization is needed, there are two paths:

#### Option A: External Enterprise Auth Service (sidecar)

Run the lakeFS Enterprise auth service (or a compatible implementation) as a sidecar, and configure:

```yaml
auth:
  ui_config:
    rbac: internal    # or "external"
  api:
    endpoint: "http://localhost:8001"
  authentication_api:
    endpoint: "http://localhost:8001"
```

In this mode, the enterprise auth service provides full IAM-style RBAC with:
- Custom policies: `{ "action": ["fs:WriteObject"], "resource": ["arn:lakefs:fs:::repository/repo-x/*"], "effect": "allow" }`
- Per-repository, per-branch, per-path scoping
- Policy attachment to individual users or groups
- `internal` mode: lakeFS manages users/groups/policies in the auth API
- `external` mode: an external IdP manages identity, auth API manages authorization

The SSO code from this plan (OIDC provider, group sync) works with both `simplified` and `internal`/`external` modes — the `authentication.Service` interface is the same. The only difference is which `auth.Service` implementation is behind the `authServiceBuilder` hook.

#### Option B: Implement Per-Repo ACLs In-Process (custom development)

Extend `acl.AuthService` to support repository-scoped ACLs. This would require:
1. A new ACL model: `{ group: "TeamA-Writers", permission: "Write", repositories: ["repo-x", "repo-y"] }`
2. Modifying `ACLToStatement()` to generate resource-scoped policy statements
3. A way to manage repo-scoped ACLs (API endpoint or config)
4. Mapping from Azure groups → repo-scoped ACL groups

This is significantly more work (~2-3 weeks additional) and diverges further from upstream, so Option A is recommended if per-repo authorization is needed.

### 6.3 Summary: What This Plan Delivers vs. What It Doesn't

| Capability | Covered | Notes |
|---|---|---|
| Multi-user SSO via Azure Entra ID | Yes | OIDC authorization code flow, automatic user provisioning |
| Group-based access control | Yes | 4 ACL tiers (Read/Write/Super/Admin), mapped from Azure App Roles |
| Automatic group sync on login | Yes | Add/remove lakeFS groups based on token claims |
| Per-repository authorization | **No** | All permissions apply globally; requires Enterprise auth or custom extension |
| Custom policies (action+resource) | **No** | ACL mode only supports 4 fixed permission levels |
| `lakectl` SSO login | Phase 4 (optional) | Browser-based flow with local callback |

---

## 7. Security Considerations

1. **HTTPS required.** OIDC callback must use HTTPS in production. The session cookie has `Secure: true` when TLS is enabled (already handled in `serve.go`).

2. **State parameter.** Must be cryptographically random and validated on callback to prevent CSRF. Use `crypto/rand` with at least 32 bytes.

3. **Nonce.** Must be included in the authorization request and verified in the ID token to prevent replay attacks.

4. **Token validation.** Verify signature (RS256 via JWKS), issuer, audience, expiration, nonce. The `coreos/go-oidc` library handles signature and standard claim validation.

5. **Client secret rotation.** Azure client secrets expire. Document the rotation procedure (update config, restart lakeFS).

6. **Session cookie security.** Already handled: `HttpOnly`, `Secure` (when TLS), `SameSite=Lax` (allows OAuth redirects).

7. **User ID immutability.** Using `oid` ensures that user identity is stable even if email or username changes in Azure.

---

## 8. File-Level Change Summary

### Upstream files modified (minimal, merge-safe)

| File | Change | Conflict Risk |
|---|---|---|
| `cmd/lakefs/cmd/run.go` | 2 lines: replace `auth.NewAuthService(...)` and `authentication.NewAuthenticationService(...)` with `authServiceBuilder(...)` and `authenticationServiceBuilder(...)` | Trivial — isolated single-line variable renames |
| `pkg/config/oss_config.go` | 1 field addition: `SSO map[string]interface{} \`mapstructure:"sso"\`` to `ConfigImpl` | Trivial — additive only; needed for `viper.UnmarshalExact` to accept `sso:` YAML key |
| `go.mod` / `go.sum` | Add `github.com/coreos/go-oidc/v3` dependency | Low — additive only |

### New files (zero conflict on merge)

| File | Purpose |
|---|---|
| `cmd/lakefs/cmd/hooks.go` | Builder hook variables + setters (makes `run.go` extensible) |
| `cmd/lakefs-sso/main.go` | Wrapper binary entry point — registers our SSO builders, calls upstream `Execute()` |
| `pkg/sso/config.go` | `SSOConfig` struct, `LoadSSOConfig()` — reads `sso:` YAML keys via viper |
| `pkg/sso/auth_builder.go` | `BuildAuthService()` — instantiates `acl.AuthService` in-process when SSO enabled |
| `pkg/sso/authentication_builder.go` | `BuildAuthenticationService()` — instantiates `NativeOIDCService` when SSO enabled |
| `pkg/sso/oidc_provider.go` | `NativeOIDCService` implementing `authentication.Service` (OIDC flow, callback, STS) |
| `pkg/sso/oidc_provider_test.go` | Tests for the OIDC provider (mock IdP, callback, claims) |
| `pkg/sso/group_sync.go` | Group sync logic (diff token groups vs lakeFS groups on each login) |
| `pkg/sso/group_sync_test.go` | Tests for group sync |
| `pkg/sso/migrate.go` | Migration from `BasicAuthService` single-user to ACL multi-user |

### Upstream files NOT modified

| File | Why untouched |
|---|---|
| `pkg/config/config.go` | SSO config is in separate `sso:` namespace, read by `pkg/sso/config.go` |
| `pkg/authentication/factory.go` | Builder hook in `run.go` bypasses the factory entirely |
| `pkg/authentication/service.go` | We implement the interface, not modify it |
| `pkg/auth/request_auth.go` | We normalize claims (map `oid` → `sub`) before storing in session cookie |
| `pkg/auth/build.go` | Builder hook in `run.go` bypasses this entirely |
| `pkg/auth/basic_service.go` | Not used when SSO is enabled |
| `pkg/api/serve.go` | Already calls `RegisterAdditionalRoutes` — our service uses this hook |
| `pkg/api/controller.go` | Already delegates `OauthCallback` to `authentication.Service` |
| `pkg/api/auth_middleware.go` | Already handles `oidc_auth` session — works with our claims |
| `webui/*` | Login redirect is config-driven (`login_url`, `fallback_login_url`) |

---

## 9. Implementation Order and Effort Estimates

| Phase | Task | New Files | Upstream Changes | Effort |
|---|---|---|---|---|
| **0a** | **Builder hooks in `cmd/lakefs/cmd/`** | `hooks.go` | 2 lines in `run.go` | **S** |
| **0b** | **ACL auth builder (`pkg/sso/auth_builder.go`)** | `auth_builder.go` | None | **M** |
| **0c** | **Migration tool (`pkg/sso/migrate.go`)** | `migrate.go` | None | **M** |
| **0d** | **Wrapper binary (`cmd/lakefs-sso/main.go`)** | `main.go` | None | **S** |
| 1a | SSO config (`pkg/sso/config.go`) | `config.go` | None | S |
| 1b | OIDC provider (`pkg/sso/oidc_provider.go`) | `oidc_provider.go` | None | L |
| 1c | Auth service builder (`pkg/sso/authentication_builder.go`) | `authentication_builder.go` | None | S |
| 1d | Claims normalization (`oid` → `sub` in session) | Inside `oidc_provider.go` | None | S |
| 1e | Tests: mock OIDC provider, callback, claims | `oidc_provider_test.go` | None | M |
| 2a | Group sync (`pkg/sso/group_sync.go`) | `group_sync.go`, `group_sync_test.go` | None | M |
| 2b | Group overage via MS Graph (optional) | `graph_groups.go` | None | L |
| 3a | UI configuration (YAML only) | None | None | S |
| 3b | OIDC logout (YAML only) | None | None | S |
| 4a | `lakectl-sso auth login --sso` command | `cmd/lakectl-sso/` | None | M |

**S** = Small (< 1 day), **M** = Medium (1–3 days), **L** = Large (3–5 days)

**Total estimated effort: ~3-4 weeks** for Phases 0–3 (excluding 2b and 4a).

**Upstream merge effort on each lakeFS update:** Resolve 2-line diff in `run.go`. All other code is in new files/directories with no conflicts.

---

## 10. Testing Strategy

1. **Unit tests** (`pkg/authentication/oidc_native_test.go`):
   - Mock OIDC provider (use `httptest.Server` serving a `.well-known/openid-configuration`)
   - Test authorization URL generation with correct params
   - Test callback: valid code exchange, token verification, claims extraction
   - Test state/nonce validation (reject tampered or missing values)
   - Test claims mapping to user identity

2. **Integration tests** (new test in `esti/`):
   - Spin up lakeFS with OIDC config pointing to a test IdP (e.g., Dex, mock OIDC server)
   - Full browser flow: login → callback → session → API access → logout
   - Test group sync: change roles, re-login, verify group changes

3. **Manual testing with Azure Entra ID:**
   - Register a test app in Azure
   - Configure lakeFS with the config from Section 4
   - Verify: login flow, token claims, group mapping, logout, session expiry

---

## 11. Risks and Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| **`BasicAuthService` single-user limit blocks entire SSO flow** | **Critical** | **Phase 0: embed `acl.AuthService` in-process via builder hook — no upstream file modification** |
| **Migration from `rbac: none` to `rbac: simplified` loses existing admin** | **High** | **`pkg/sso/migrate.go` copies admin user + credentials from `basicAuth` KV partition to `aclauth` partition** |
| **Upstream refactors `run.go` signature** | **Medium** | **Only 2 lines to resolve; builder hook defaults to upstream behavior, so the worst case is a clean revert** |
| Azure key rotation breaks auth | High | `coreos/go-oidc` caches JWKS with automatic refresh; document key rotation |
| Client secret expiration | Medium | Document rotation procedure; consider certificate-based auth |
| Groups claim overage (>200 groups) | Medium | Recommend App Roles; defer Graph API fallback to Phase 2b |
| Multi-tenant issuer validation | Low (Phase 1 is single-tenant) | Single-tenant only in Phase 1; extend later |
| Session cookie size limits (~4KB) | Low | Store only essential claims (oid, preferred_username, roles), not the full token |
| `gorilla/sessions` v1.4.0 `SameSite`/`Secure` defaults break OAuth over HTTP | Medium | Already handled in `serve.go` — `Secure` tied to TLS config, `SameSite=Lax` |

---

## Appendix A: Sequence Diagram — Browser SSO Login

```
Browser              lakeFS                    Azure Entra ID
  │                    │                             │
  │ GET /oidc/login    │                             │
  │───────────────────►│                             │
  │                    │ generate state, nonce       │
  │                    │ store in session cookie     │
  │ 302 Redirect       │                             │
  │◄───────────────────│                             │
  │                    │                             │
  │ GET /oauth2/v2.0/authorize?                      │
  │   client_id=...&redirect_uri=.../oidc/callback   │
  │   &response_type=code&scope=openid+profile+email │
  │   &state=...&nonce=...                           │
  │─────────────────────────────────────────────────►│
  │                    │                             │
  │  (user authenticates in Azure)                   │
  │                    │                             │
  │ 302 Redirect to /oidc/callback?code=...&state=.  │
  │◄─────────────────────────────────────────────────│
  │                    │                             │
  │ GET /oidc/callback?code=...&state=...            │
  │───────────────────►│                             │
  │                    │ validate state              │
  │                    │ POST /oauth2/v2.0/token     │
  │                    │────────────────────────────►│
  │                    │ {id_token, access_token}    │
  │                    │◄────────────────────────────│
  │                    │ verify id_token sig+claims  │
  │                    │ extract oid, roles, name    │
  │                    │ store claims in oidc_auth   │
  │                    │   session cookie            │
  │ 302 Redirect to /  │                             │
  │◄───────────────────│                             │
  │                    │                             │
  │ GET /api/v1/user   │                             │
  │ Cookie: oidc_auth  │                             │
  │───────────────────►│                             │
  │                    │ UserFromOIDCSession:        │
  │                    │  find/create user by oid    │
  │                    │  sync groups from roles     │
  │ 200 {user data}    │                             │
  │◄───────────────────│                             │
```

## Appendix B: Sequence Diagram — `lakectl` SSO Login

```
lakectl                 Browser               lakeFS           Azure Entra ID
  │                       │                     │                    │
  │ start local server    │                     │                    │
  │ on localhost:54321    │                     │                    │
  │                       │                     │                    │
  │ open browser ─────────►                     │                    │
  │                       │ GET /oidc/login     │                    │
  │                       │   ?redirect_uri=    │                    │
  │                       │   localhost:54321   │                    │
  │                       │───────────────────► │                    │
  │                       │ (OIDC flow as above)                     │
  │                       │                     │                    │
  │ callback on :54321    │                     │                    │
  │  with code + state    │                     │                    │
  │◄──────────────────────│                     │                    │
  │                       │                     │                    │
  │ POST /api/v1/sts/login                      │                    │
  │  {code, state, redirect_uri}                │                    │
  │────────────────────────────────────────────►│                    │
  │                       │                     │ exchange code      │
  │                       │                     │───────────────────►│
  │                       │                     │ verify token       │
  │ 200 {jwt_token}       │                     │◄───────────────────│
  │◄────────────────────────────────────────────│                    │
  │                       │                     │                    │
  │ store JWT in config   │                     │                    │
```

## Appendix C: Optimized Context Map — Exact Line Ranges per File

To minimize context token consumption during implementation, each file is annotated with the **exact line ranges and symbols** needed. Reading only these ranges reduces context from ~11,814 lines (full files) to **~1,149 lines** (~90% reduction, ~10× fewer context tokens).

### Full files (small, all content relevant)

| File | Lines | Key symbols |
|---|---|---|
| `pkg/auth/request_auth.go` | 1–240 (all) | `OIDCConfig` (L28), `CookieAuthConfig` (L36), `UserFromOIDCSession` (L147), `initialGroupsFromClaims` (L217) |
| `pkg/auth/build.go` | 1–88 (all) | `NewAuthService` (L29) — the factory we bypass via builder hook |
| `pkg/authentication/factory.go` | 1–46 (all) | `NewAuthenticationService` (L13), `BuildAuthenticatorChain` (L25) |
| `contrib/auth/acl/setup.go` | 1–77 (all) | `CreateACLBaseGroups` (L21), `SetupACLServer` (L53) |
| `contrib/auth/acl/permission.go` | 1–100 (all) | `ReadPermission`, `WritePermission`, `SuperPermission`, `AdminPermission`, `ACLToStatement` |

**Subtotal: 551 lines**

### Surgical reads (large files, only specific ranges needed)

| File | Line range | Symbols needed |
|---|---|---|
| `pkg/auth/service.go` | L110–167 | `type Service interface` — the core auth interface (58 lines) |
| `pkg/auth/basic_service.go` | L18–23, L109–117, L150–164, L360–414 | `MaxUsers` (L21), `Authorize` (L109), `CreateUser` (L150), `GetUserByExternalID` (L360), `CreateGroup` (L392), `AddUserToGroup` (L408) (62 lines) |
| `pkg/auth/authenticator.go` | L19–23, L30–47 | `Authenticator` interface (L19), `ChainAuthenticator` (L30) (22 lines) |
| `pkg/authentication/service.go` | L19–26, L101–127, L160–163 | `Service` interface (L19), `ValidateSTS` (L101), `OauthCallback` (L160) (38 lines) |
| `pkg/config/config.go` | L40–43, L423–445, L673–758 | `AuthRBAC*` constants (L40), `Config` interface (L423), `AuthConfig` interface (L438), `BaseAuth` struct (L673), `OIDC` struct (L732), `CookieAuthVerification` (L743), `AuthUIConfig` (L716) (112 lines) |
| `pkg/api/auth_middleware.go` | L87–160 | `checkSecurityRequirements` — handles `oidc_auth` case at L133 (74 lines) |
| `pkg/api/serve.go` | L36–129 | `Serve` function — session store (L59), oidcConfig (L69), `RegisterAdditionalRoutes` (L126) (94 lines) |
| `pkg/api/controller.go` | L855–881, L5466–5483, L6443–6444 | `StsLogin` (L855), `newLoginConfig` (L5466), `OauthCallback` (L6443) (48 lines) |
| `cmd/lakefs/cmd/run.go` | L107–109, L246–255 | `authService` creation (L107), `authenticationService` creation (L109), `oidcConfig` wiring (L247) (12 lines) |
| `contrib/auth/acl/service.go` | L22–42, L104–118, L222–226, L425–435, L521–540 | `AuthService` struct (L22), `NewAuthService` (L29), `CreateUser` (L104), `GetUserByExternalID` (L222), `CreateGroup` (L425), `AddUserToGroup` (L521) (70 lines) |
| `pkg/auth/oidc/encoding/encoding.go` | 1–25 (all) | `Claims` type, gob registration (25 lines) |
| `webui/src/pages/auth/login.tsx` | L14–30, L58–68, L166–190 | `LoginConfig` interface (L20), placeholder defaults (L68), `LoginPage` SSO redirect logic (L186) (33 lines) |

**Subtotal: 598 lines**

### Summary

| | Full files | Surgical reads | **Total** |
|---|---|---|---|
| **Lines** | 551 | 598 | **~1,149** |
| **Tokens (est.)** | ~14k | ~15k | **~29k** |

Compared to reading all 15 files in full (~11,814 lines / ~100k tokens), the optimized map saves **~71k context tokens per session** — roughly 2.5× fewer tokens for context reads.

### How to use this map

When starting a new implementation session, read only the ranges listed above. This provides all the type signatures, interfaces, factory functions, and wiring points needed without loading thousands of lines of unrelated code. If a specific implementation detail requires more context (e.g., understanding how `Authorize` dispatches), expand the read range for that file only.
