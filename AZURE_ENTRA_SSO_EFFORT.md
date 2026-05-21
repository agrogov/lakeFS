# Azure Entra ID SSO — Effort, Difficulty & Token Estimate

Companion document to `AZURE_ENTRA_SSO_PLAN.md`.

---

## Scope & Authorization Model

This plan delivers **multi-user SSO + simplified ACL authorization** (4 global tiers: Read / Write / Super / Admin). Permissions apply to **all repositories** — there is no per-repo scoping. See `AZURE_ENTRA_SSO_PLAN.md` Section 6 for details and upgrade paths to full RBAC.

All new code lives in **new files/directories only** (`pkg/sso/`, `cmd/lakefs-sso/`, `cmd/lakefs/cmd/hooks.go`). The only upstream file touched is `cmd/lakefs/cmd/run.go` (2 lines).

---

## Phase 0 — Builder Hooks + ACL Auth Service + ACL Bootstrap

| Task | File | LOC | Difficulty | Tokens |
|---|---|---|---|---|
| Builder hook variables + setters | `cmd/lakefs/cmd/hooks.go` (new) + 2 lines in `run.go` | ~60 | Easy | ~3k |
| ACL auth service wiring in-process | `pkg/sso/auth_builder.go` | ~120 | Easy-Medium | ~5k |
| ACL bootstrap (create 4 default groups + policies on first run) | Inside `auth_builder.go` (calls `acl.SetupACLServer`) | ~20 | Easy | included above |
| Migration from `basicAuth` → `aclauth` KV partition | `pkg/sso/migrate.go` | ~180 | Medium | ~8k |
| Wrapper binary entry point | `cmd/lakefs-sso/main.go` | ~40 | Easy | ~2k |
| **Phase 0 total** | | **~420** | **Medium** | **~18k** |

Phase 0 delivers: multi-user support, 4 ACL groups (Admins/Supers/Writers/Readers) with fixed global permissions, external ID lookup, group membership — all in-process via `acl.AuthService`. No OIDC yet.

---

## Phase 1 — Native OIDC Provider (hardest part)

| Task | File | LOC | Difficulty | Tokens |
|---|---|---|---|---|
| SSO config struct + Viper loader (`sso:` namespace) | `pkg/sso/config.go` | ~80 | Easy | ~3k |
| OIDC provider: discovery, state/nonce, login redirect, callback, token exchange, ID token verification, claims normalization (`oid`→`sub`), STS | `pkg/sso/oidc_provider.go` | ~500 | **Hard** | ~25k |
| Authentication service builder | `pkg/sso/authentication_builder.go` | ~50 | Easy | ~2k |
| Tests: mock `httptest` IdP, callback flow, claims, state/nonce validation | `pkg/sso/oidc_provider_test.go` | ~400 | Hard | ~20k |
| **Phase 1 total** | | **~1,030** | **Hard** | **~50k** |

> The OIDC provider is the hardest and most token-intensive piece. It is security-sensitive (state CSRF, nonce replay, JWKS key rotation, token expiry) and must be correct from the start. Testing requires a mock OIDC server and careful coverage of error paths.

---

## Phase 2 — Group Sync (Azure Roles → ACL Groups)

| Task | File | LOC | Difficulty | Tokens |
|---|---|---|---|---|
| Group diff + sync logic (maps Azure App Roles to ACL groups: Admins/Supers/Writers/Readers) | `pkg/sso/group_sync.go` | ~120 | Easy-Medium | ~5k |
| Unit tests | `pkg/sso/group_sync_test.go` | ~160 | Easy | ~6k |
| **Phase 2 total** | | **~280** | **Easy-Medium** | **~11k** |

Group sync maps the `roles` claim from the Entra ID token (e.g., `["Admins", "Writers"]`) to the 4 ACL groups. On every login it adds/removes group memberships to match the token. Only groups with a configurable managed prefix are touched — manually-assigned groups are preserved.

---

## Phase 3 — Configuration & Logout

Config YAML and documentation only — no new code files.

| Task | Difficulty | Tokens |
|---|---|---|
| YAML config examples (`sso:` section + upstream `auth:` UI settings), logout URL wiring, UI redirect config | Easy | ~1k |
| **Phase 3 total** | **Easy** | **~1k** |

---

## Phase 4 — `lakectl-sso` CLI (optional, separate scope)

| Task | File | LOC | Difficulty | Tokens |
|---|---|---|---|---|
| Browser-based auth flow + local callback server + JWT storage | `cmd/lakectl-sso/` | ~300 | Medium | ~15k |
| **Phase 4 total** | | **~300** | **Medium** | **~15k** |

---

## Token Breakdown: Output vs Context

The per-phase "Tokens" figures above count **output tokens only** (code generation + reasoning). Each session also consumes **input/context tokens** for reading upstream source files, conversation history, and plan documents.

With the **optimized context map** (see `AZURE_ENTRA_SSO_PLAN.md` Appendix C), upstream file reads drop from ~100k tokens (full files, ~12k lines) to **~29k tokens** (~1,149 lines). This reduces per-session context overhead significantly.

| Component | Tokens per session (optimized) | Tokens per session (unoptimized) |
|---|---|---|
| System prompt + conversation history | ~5k | ~5k |
| Upstream file reads (optimized line ranges) | **~29k** | ~50k |
| Plan documents (this file, AZURE_ENTRA_SSO_PLAN.md) | ~10k | ~10k |
| **Per-session context overhead** | **~44k** | **~65k** |

Expected **3–4 sessions** across phases 0–3.

---

## Summary (including context tokens)

| Phase | Delivers | Human Time | Output Tokens | Context Tokens (optimized) | Total Tokens |
|---|---|---|---|---|---|
| 0 — Auth layer | Multi-user + ACL (4 global tiers) via embedded `acl.AuthService` | 2–3 days | ~18k | ~88k (2 sessions) | **~106k** |
| 1 — OIDC Provider | Azure Entra ID login flow, automatic user provisioning | 4–6 days | ~50k | ~110k (2–3 sessions) | **~160k** |
| 2 — Group sync | Azure roles → ACL groups, synced on every login | 1 day | ~11k | ~44k (1 session) | **~55k** |
| 3 — Config/logout | YAML config, UI SSO redirect, OIDC logout | 0.5 days | ~1k | ~5k (1 session) | **~6k** |
| **Total (phases 0–3)** | **SSO + simplified ACL (global permissions, not per-repo)** | **~2.5 weeks** | **~80k** | **~247k** | **~327k** |
| 4 — `lakectl-sso` CLI | CLI SSO login via browser | +1 week | +15k | +44k | **+59k** |
| **Grand total (all phases)** | | **~3.5 weeks** | **~95k** | **~291k** | **~386k** |

**Worst case** (Azure quirks, extra debug sessions): **~480–530k tokens total**, **~4.5 weeks**.

> **Note:** Context token estimates use the optimized line ranges from `AZURE_ENTRA_SSO_PLAN.md` Appendix C (~29k tokens per session for file reads vs ~50k unoptimized). This saves ~20k tokens per session, or ~60–80k total across all phases.

---

## What Is NOT Included (requires Enterprise auth or custom extension)

| Capability | Why not covered | Upgrade path |
|---|---|---|
| Per-repository authorization | ACL mode applies permissions globally across all repos | Run Enterprise auth service as sidecar (`rbac: internal`) — our SSO code works unchanged |
| Custom policies (action + resource) | ACL mode has 4 fixed permission levels only | Same as above |
| Per-branch / per-path permissions | Not supported by any lakeFS auth mode | N/A |

See `AZURE_ENTRA_SSO_PLAN.md` Section 6.2 for the full RBAC upgrade path.

---

## Key Risk to Estimate

The main cost driver is the OIDC provider (Phase 1). It requires reading many upstream context files, writing ~900 lines of security-sensitive code and tests, and likely 1–2 debug iterations against a mock IdP.

If Azure-specific quirks surface during testing (nonce handling edge cases, multi-tenant issuer validation, group claim overage), Phase 1 token spend can reach **~170k total** (output + context) on its own.

---

## Merge-Friendliness Cost

All new code in new files. Only 2 lines modified in `run.go`.

| Scenario | Effort | Tokens |
|---|---|---|
| Typical upstream merge/rebase | 5–10 minutes, trivial 2-line conflict in `run.go` | 0 |
| Upstream refactors `run.go` auth wiring | 1–2 hours, re-align builder hook calls | ~5k output + ~40k context = ~45k |
| Upstream adds native OIDC (unlikely near-term) | Evaluate and potentially drop our `pkg/sso/` | ~10k output + ~50k context = ~60k |
