# Azure Entra ID SSO changelog

Changes carried on top of upstream lakeFS (Apache 2.0, based on upstream `4bb11638e`,
the last commit before pluggable IAM / the ACL server were removed). Kept separate from
the upstream `CHANGELOG.md` so upstream merges stay conflict-free.

## Unreleased

:bug: Bugs fixed:

- First SSO login now receives its groups. The user is provisioned in the OIDC callback before group sync runs; previously sync failed with "not found" and the user had no permissions until logging in a second time.
- `sso.friendly_name_claim` now takes effect. It fills `auth.oidc.friendly_name_claim_name` when that key is unset (an explicit value still wins), because the ACL service does not persist friendly names.
- `lakefs-sso sso-migrate` no longer fails with `Admins: not found` on an installation created with `lakefs setup`. The base ACL groups (Admins/Supers/Writers/Readers) are now also created when a setup timestamp already exists, in `sso-migrate` and at server start.

:white_check_mark: Tests:

- Regression tests for all of the above (`pkg/sso`: `user_test.go`, `acl_bootstrap_test.go`, first-login callback test).

## Phases 0–5

:new: What's new:

- **Phase 0 – ACL auth service.** New `lakefs-sso` binary that runs lakeFS with `acl.AuthService` (multi-user, groups) via builder hooks (`cmd/lakefs/cmd/hooks.go`, 2 lines in `run.go`). New `sso-migrate` command moves the single admin user and credentials from basic auth to the ACL service (idempotent).
- **Phase 1 – Native OIDC.** `NativeOIDCService` implements login against Azure Entra ID: `/oidc/login` with state and nonce, `/api/v1/oidc/callback` with ID token verification (`go-oidc/v3`), `oid` normalized into `sub`, and CLI STS validation.
- **Phase 2 – Group sync.** ACL group membership is reconciled from the token's `roles` claim on every login, scoped by `managed_group_prefix`. Hardening: flow session invalidated after state check, open-redirect protection, IdP errors logged server-side only.
- **Phase 3 – Logout and config example.** Logout clears OIDC sessions; documented `cmd/lakefs-sso/config.example.yaml`. Group-sync review fixes: concurrent-add race, truncated group list warning, absent-vs-empty groups claim.
- **Phase 4 – CLI login.** `lakectl-sso sso-login` opens the browser, receives the code on a loopback server (`http://127.0.0.1:<port>` only), and exchanges it via `/api/v1/sts/login`. Logout route fix for chi v5 and a `sso.logout_url` option.
- **Phase 5 – Correctness.** Required `sso.*` fields validated at startup, warning when the builders run out of order, HTTP 400 for an empty authorization code, build artifacts added to `.gitignore`.
