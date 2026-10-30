# Azure Entra ID SSO changelog

Changes carried on top of upstream lakeFS (Apache 2.0, based on upstream `4bb11638e`,
the last commit before pluggable IAM / the ACL server were removed). Kept separate from
the upstream `CHANGELOG.md` so upstream merges stay conflict-free.

## Unreleased

:new: What's new — policy RBAC (`auth.ui_config.rbac: internal`):

- **Opt-in internal mode.** `lakefs-sso` can now run on policy-based RBAC instead of ACL mode. Set `auth.ui_config.rbac: internal` to get the full Users / Groups / Policies UI and policy scoping by repository, branch, path and source IP, including `${user}` self-service and audit request logging. The backend is the same ACL KV service, only the capability flag differs, so no data migration of users or credentials is needed. The default stays `simplified`; an unset value and `none` also resolve to `simplified`, and `external` is rejected with a warning because SSO has no remote auth service.
- **Idempotent bootstrap.** At startup in `internal` mode the RBAC base groups (`Admins`, `SuperUsers`, `Developers`, `Viewers`), their policies and attachments are created only when missing. Existing policies are never overwritten, so edited built-ins survive restarts. The break-glass admin is added to `Admins`.
- **`SuperUsers` keeps pull-request write.** `SuperUsers` holds `PRReadWriteAll` on top of `FSFullAccess`, `CatalogReadWriteAll`, `AuthManageOwnCredentials` and `RepoManagementReadAll`, so a `Supers` member moved by `rbac-migrate` can still open, update and merge pull requests (`pr:WritePullRequest`), as it could under ACL `Super`. Without it the migration silently downgraded every existing Super. The base template version is now 3, so an installation bootstrapped by an earlier binary re-applies the template once on its first start (nothing else changes: the bootstrap never overwrites an edited built-in policy, and `rbac-migrate` attaches the missing policy itself).
- **`lakefs-sso rbac-migrate`.** New command that moves a `simplified` installation to `internal`. It is a dry run by default, needs `--yes` to apply, accepts `--cleanup`, and is idempotent (re-runs are no-ops).
- **Configurable authorization failure status.** `auth.authorization_failure_status` sets the HTTP status returned when a request lacks permissions. It defaults to `401`, because the web UI and the SDK clients treat a 401 without the authentication message as an authorization error. Set it to `403` to opt in to "forbidden" responses. Only `401` and `403` are accepted: any other value (for example a typo such as `200`, which would make a denial look like a successful response) fails startup with a clear error. See `cmd/lakefs-sso/config.example.yaml`.

Migrating `simplified` → `internal`:

1. Run `lakefs-sso rbac-migrate` on the running (still `simplified`) installation and review the printed plan. It attaches `FSFullAccess`, `CatalogReadWriteAll`, `AuthFullAccess`, `RepoManagementFullAccess` and `PRReadWriteAll` to `Admins` and detaches the old `ACL(_-_)Admins` policy; copies members `Supers` → `SuperUsers` (which already holds `PRReadWriteAll`, so pull-request write is preserved), `Writers` → `Developers`, `Readers` → `Viewers`; leaves custom groups untouched (their `ACL(_-_)<group>` policies are kept and remain valid, editable policies).
2. Run `lakefs-sso rbac-migrate --yes` to apply.
3. Update `sso.group_mapping` in the config with the rewrites the command prints (e.g. `Writers` → `Developers`, `Readers` → `Viewers`, `Supers` → `SuperUsers`). This step is required: without it the next login's group sync re-adds users to the legacy groups and the migration is undone.
4. Set `auth.ui_config.rbac: internal` and restart `lakefs-sso`.
5. Verify that the Policies pages are visible and that an SSO login lands the user in the expected RBAC group.

Rollback and notes:

- Rollback is `auth.ui_config.rbac: simplified` plus a restart. It is lossless as long as the migration was applied **without** `--cleanup`: the legacy groups and ACL policies are then still present. `--cleanup` deletes the emptied `Supers` / `Writers` / `Readers` groups and their `ACL(_-_)<group>` policies, after which a rollback needs the members re-added (for example through `sso.group_mapping`).
- After a rollback, a group that holds more than one policy shows no ACL in the simplified UI (RBAC_DESIGN.md §3.3 #5). `rbac-migrate` prints the same warning.
- The authorization cache is per process (`auth.cache.ttl`, default 20s plus jitter). Policy revocations apply immediately on the replica that handled the request, but other replicas converge only within the TTL. For multi-replica deployments use `auth.cache.ttl: 5s` (RBAC_DESIGN.md §4.7); see `cmd/lakefs-sso/config.example.yaml`.

:memo: Notes for operators:

- **Client IPs are no longer taken from an untrusted `X-Forwarded-For`.** `IpAddress` / `SourceIp`
  policy conditions (and the audit log's client field) now use the transport peer address by default.
  A client-supplied `X-Forwarded-For` or `X-Real-IP` is ignored, so it can no longer spoof the IP a
  conditional allow or deny sees. If lakeFS runs behind a proxy or load balancer, configure the
  proxy CIDRs in `security.trusted_proxies`; lakeFS then honours the forwarding headers and walks
  `X-Forwarded-For` from right to left to the first untrusted hop. Use `"*"` only when the proxy
  strips client-supplied forwarding headers, as it trusts every peer (the old behaviour). This is a
  behaviour change: a deployment behind an unconfigured proxy now sees the proxy address as the
  client IP. See `cmd/lakefs-sso/config.example.yaml`.

- **Commit identity is the email.** SSO users are keyed by the immutable Entra object ID (`oid`), which is also
  their lakeFS username. Since the ID token's `email` claim (or a UPN-shaped `preferred_username`) is now stored on
  the user record, new commits and merges are recorded with the committer = the user's **email** instead of the
  `oid`. This applies to commits created from that point on: commits written before the change keep the `oid` they
  were created with, because the committer is stored inside the commit. Users with no email claim still commit
  under their user ID. (Entra B2B guest emails can look like `alice_contoso.com#EXT#@tenant.onmicrosoft.com`;
  they are stored as-is.)
- **Audit-style request logs are level-gated.** In `internal` mode every request logs an audit line
  (`HTTP call ended`) carrying `log_audit=true`, the client, the response size and the request ID, for the REST
  API and for the S3 gateway. Those lines are written at `logging.audit_log_level`, which defaults to `DEBUG`,
  while `logging.level` defaults to `INFO` — so **with the defaults no audit line is emitted at all**. Set
  `logging.audit_log_level` at or below `logging.level` (`audit_log_level: INFO` is enough) to see them; raising
  `logging.level` to `DEBUG` also works but is noisier everywhere else. In `simplified` mode the same lines are
  emitted without `log_audit`, `client` and `sent_bytes`. See `cmd/lakefs-sso/config.example.yaml`.

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
