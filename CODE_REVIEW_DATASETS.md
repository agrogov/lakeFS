# Code review — datasets control plane (`feature/datasets`)

Scope: `pkg/datasets/*.go` (non-test), `cmd/lakectl-datasets/cmd/*.go`, `pkg/gateway/*.go` and the
datasets wiring in `pkg/api/**`, plus the branch diff of `pkg/auth/condition.go` and
`pkg/auth/service.go`. Reviewed against the diff from `4bb11638e` (merge base with `origin/master`).

Verification: `go vet ./pkg/datasets/...` is clean; `go test ./pkg/datasets/ ./cmd/lakectl-datasets/...`
passes (53s + 1.1s). The findings below are therefore not covered by the existing tests; each one says
whether I confirmed it by reasoning over the code, over its callers, or both.

## Summary

The layer is unusually disciplined: the CAS discipline of `store.go` (the `kv.Predicate` handed back by
`Get` is always passed through unmodified, with a canonical-encoding read-compare-write fallback), the
pin reference-counting of `pins.go`, and the SEC-3 per-object source-read check are careful and
internally consistent. I could not find a way to bypass the immutability guard through any of the
authorization entry points I traced (controller, S3 gateway, datasets handlers, proposal/publish paths).
The real defects are concentrated in key-space arithmetic (a pagination cursor built by appending `/` is
wrong for dataset names containing `-`), in a few places where a normal optimistic-concurrency loss is
classified as a fatal one, and in one cleanup path that leaves pin references behind. Nothing I found is
an auth bypass, an injection, or a resource leak on a hot path.

## Findings

### Major

#### M1 — the `ScanDatasets` / `ScanGrants` cursor skips every name that extends the cursor with `-`

**Where:** `pkg/datasets/store.go:693` (`ScanDatasets`) and `pkg/datasets/store.go:1824` (`ScanGrants`),
consumed through the seek at `pkg/datasets/store.go:603-606`.

**What is wrong.** Both scans continue a page by seeking to `key(cursor) + "/"`, and `kv.ScanOptions.KeyStart`
is *inclusive* — `pkg/kv/kvtest/store.go:395-425` pins that (the entry at `KeyStart` is returned), and the
mem implementation seeks with `BisectRight` at `pkg/kv/mem/store.go:216-241`. The comment at
`store.go:690-693` justifies the trick with "`/` sorts before every byte a dataset name can contain", but
`-` is `0x2d` and `/` is `0x2f`, and the dataset-name pattern (`definition.go:41`) admits `-`. So every key
in the open interval `(datasets/<cursor>, datasets/<cursor>/)` is skipped, and those are exactly the
datasets named `<cursor>-...`.

With datasets `sales`, `sales-2024` and `sales2`, page 1 ends on `sales` and returns
`next_offset: "sales"`; page 2 seeks to `datasets/sales/`, and because
`datasets/sales-2024 < datasets/sales/ < datasets/sales2`, `sales-2024` is never returned. Byte ordering
checked directly:

```
sorted keys:                             ['datasets/sales', 'datasets/sales-2024', 'datasets/sales2']
inclusive scan from 'datasets/sales/':   ['datasets/sales2']            <- sales-2024 is lost
keys that must follow cursor 'sales':    ['datasets/sales-2024', 'datasets/sales2']
```

**Why it matters.** `GET /datasets` and `GET /datasets/{ds}/access` are documented as cursor-paginated
(`List` hands the last name back as `next_offset` at `service.go:888-891`), and a client that follows
`has_more`/`next_offset` — `lakectl datasets list` and the webui both do — silently loses datasets, so the
listing looks complete while it is not. It is deterministic, not a race: no error, no warning, missing rows.
`ScanGrants` has the same defect for principal ids (`alice` vs `alice-x`), where the effect is a grant that
cannot be paged to.

`pkg/datasets/paging_test.go:128` uses the names `page-a`...`page-d`, which all differ from the cursor
*after* the shared prefix at a byte greater than `/`, so the suite cannot see this.

**Fix.** Seek to a key strictly greater than the cursor but smaller than any extension of it:
`start = append(datasetKey(after), 0x00)` — no key contains NUL, so nothing else is excluded — or seek to
the cursor key and drop the first entry inside the callback, which is what `ScanProposals` already does at
`store.go:1197-1200`. The same one-byte change fixes `ScanGrants`. Add a regression case with names `x`,
`x-2`, `x2`.

**Confidence:** certain.

### Minor

#### m1 — repeated `metadata.<key>` filters are silently reduced to the first value

**Where:** server: `pkg/datasets/handlers.go:1248-1262` (`filters[name] = values[0]`); client:
`cmd/lakectl-datasets/cmd/root.go:218-255` (`query.Add` for every value, with the comment at 218-220
promising "`--metadata tier=gold --metadata tier=silver` filters on both").

**What is wrong.** CLI-6 was fixed on the client only. The CLI now sends both values and the handler keeps
`values[0]`, so the command above filters on `gold` alone. The user gets a narrower result set than the
flag help documents, with no error and no way to notice.

**Why it matters.** Wrong results for a documented invocation, and the half-applied fix makes the flag
help and the wire behaviour disagree.

**Fix.** Pick one: implement OR semantics in `metadataMatches` (it already has the shape — match when any
requested value matches), or reject repeated keys in `parseMetadataFilters` with `invalidArguments`. Then
make `root.go`'s comment true.

**Confidence:** certain.

#### m2 — `Service.Delete` removes the head record even when an earlier step failed, leaving pin references behind

**Where:** `pkg/datasets/service.go:975-992`, contrasted with the reconciler's equivalent at
`pkg/datasets/reconciler.go:988-993` (`if len(errs) == 0 { DeleteDataset }`).

**What is wrong.** The request-path delete collects version/proposal/task/grant failures into `errs` and
then deletes the dataset record unconditionally; `finishDelete` deliberately does the opposite. The stated
invariant ("the head record is deleted last, after everything it refers to is gone", BUG-7) therefore holds
on the crash-repair path but not on the request path. `create`'s `cleanupLeftoverRecords`
(`service.go:688-720`) repairs version, proposal, task and grant leftovers of the next incarnation but
never touches pin records, so `<name>@vN` references under `pins/<repo>/<commit>` from the old incarnation
survive for the whole lifetime of the new dataset: those source commits stay pinned (over-retention —
§14-safe but unbounded) and nothing converges them until the new dataset is deleted too. `Pins.pin` also
sees the stale reference as already present (`pins.go:586-592`) and reports `fresh=false`, so a later
rollback of the new v1 does not release it either.

**Why it matters.** It is the one place where the "record last" invariant is silently broken, and the
existing repair covers four of the five record kinds. Impact is bounded (over-retention only, never
under-retention), which is why this is minor.

**Fix.** Mirror `finishDelete`: delete the head record only when `len(errs) == 0`, and have
`cleanupLeftoverRecords` call `s.pins.ReleaseDataset(ctx, dataset)` before it removes the version records
(idempotent, and it tolerates a missing source repository).

**Confidence:** likely (the ordering is certain; which step can fail first is not).

#### m3 — the publish heartbeat treats a lost CAS, or its own cancellation, as "lost the record"

**Where:** `pkg/datasets/service.go:2268-2293` (`heartbeatPublish`: any `kv.ErrPredicateFailed` becomes
`ErrDatasetConflict`, with no retry) and `service.go:2351-2370` (the ticker goroutine cancels the publish and
logs `lost the publishing record; cancelling the publish` on *any* error from it).

**What is wrong.** Two different situations are conflated:

1. A benign CAS collision. `PublishRequest.BeforeCommit` (`service.go:2220-2224`) writes the same record from
   the materialisation goroutine while the heartbeat goroutine may be between its `GetDataset` and
   `PutDatasetIf`. The loser gets `ErrPredicateFailed` — an ordinary optimistic-concurrency loss that every
   other writer in this package retries (`beginPublish:1220`, `advanceDataset:1343`,
   `rollbackPublish:1407`) — and instead the heartbeat cancels a live publish, which then rolls back and
   returns a conflict to the client for a publish that had no conflict.
2. Its own cancellation. `stopHeartbeat` calls `cancel()` before `close(stopped)` (`service.go:2371-2376`),
   so a heartbeat already inside `heartbeatPublish` fails on the cancelled context and logs the same
   "lost the record" warning. Every publish that finishes while a tick is in flight emits a false,
   `Warn`-level "lost the record" line.

**Why it matters.** A publish can fail with a conflict that is not one, and the log line an operator would
use to diagnose a genuine stale takeover by the reconciler is also emitted for the benign cases, so it
cannot be trusted.

**Fix.** In `heartbeatPublish`, retry `ErrPredicateFailed` the way `advanceDataset` does (re-read, re-check
ownership, re-CAS) and report ownership loss only when the re-read shows a different state or
`PendingVersion`. In the ticker, skip the warning when `publishCtx.Err() != nil` — that is the stop path,
not a lost record.

**Confidence:** certain for (2); likely for (1) — the code path is certain, the probability is low but not
zero.

#### m4 — `WithServiceCaller` is an exported, production-unused switch that disables the SEC-2 guard

**Where:** `pkg/datasets/immutability_guard.go:47-54`, honoured at `:150-152`.

**What is wrong.** Any context carrying `serviceCallerKey{}` skips the immutability guard entirely, and the
only caller in the tree is a test (`immutability_guard_test.go:62`). The comment's justification — "the
service ... never calls Authorize for its own repository writes" — is also how the code actually behaves,
so the escape hatch is dead weight that exists only as a bypass.

**Why it matters.** The package boundary is a convention, not a compiler check, and the ctx value is
exported. Nothing uses it today; one `WithServiceCaller` in a handler path tomorrow silently removes the
guarantee §9.4 is built on. The guard's value is precisely that a caller cannot switch it off.

**Fix.** Make it unexported and test-only, or delete it and drive the `creating`/`publishing` states from
the test instead.

**Confidence:** certain (grep for callers).

#### m5 — the guard's dataset-name cache takes a global lock around a full partition scan on the auth path

**Where:** `pkg/datasets/immutability_guard.go:90-108` (`Contains` holds `s.mu` across `s.load(ctx)`), with
`NewDatasetRepoSet` → `Store.ListDatasets` → a full scan of the `datasets/` partition (`store.go:666-673`).

**What is wrong.** Every authorization that names an immutability-denied action — every object write,
commit, branch and repository mutation in the installation — takes the same mutex, and once per
`datasetRepoCacheTTL` (2s) the holder performs a full datasets-partition scan while holding it. A burst of
concurrent writes therefore serialises behind that scan; with many datasets the scan is the slow part.

Smaller issue in the same block: `Invalidate()` (`:111-115`) clears `loaded` but keeps `names`, and the
reload failure branch (`:102-104`) returns the error whenever `!s.loaded`. So after every dataset create
(`service.go:502`) a failed reload fails closed with an error *instead of* using the names still cached in
`s.names`, and because that error propagates out of `Authorize`, unrelated non-dataset repository writes
also fail with 500 in that window.

**Why it matters.** The immutability guard should not be able to convert a KV hiccup, or a large dataset
set, into a global write stall or a global write outage. Both effects are availability, not correctness, so
this stays minor.

**Fix.** Load outside the lock and swap the map in under it (with a single-flight gate so only one reloader
runs), keep `loaded` true across `Invalidate` and add a separate `stale` flag so a failed refresh can still
serve the last known set; consider updating the set incrementally on create/delete instead of rescanning.

**Confidence:** certain for the locking; likely for the practical impact.

#### m6 — a missing grant or principal is reported as "dataset not found"

**Where:** `pkg/datasets/service.go:1891-1896` (`RevokeAccess`) and `service.go:2644-2659`
(`requirePrincipal`, whose comment admits the reuse).

**What is wrong.** `DELETE /datasets/{ds}/access/{type}/{id}` for a grant that does not exist answers 404
`dataset not found: <ds>`, and granting to an unknown user answers `dataset not found: user "bob" does not
exist`. The dataset exists in both cases.

**Why it matters.** A client cannot tell "wrong dataset" from "no such grant / no such principal", and it
makes the 404 arm of `writeServiceError` (`handlers.go:128`) look like it has one meaning when it has three.

**Fix.** Add `ErrGrantNotFound` and `ErrPrincipalNotFound`, map both to 404 in `writeServiceError`, and keep
`ErrDatasetNotFound` for the dataset itself.

**Confidence:** certain.

#### m7 — a whole-repository prefix item can collide with the synthetic definition entry

**Where:** `pkg/datasets/materialize.go:564` (the `_dataset/definition.json` stream) and the strict
key-increase assertion at `materialize.go:1083-1086`.

**What is wrong.** A prefix item with an empty source path (the whole source repository,
`definition.go:231-242`) maps every source key into the dataset key space unchanged when its target is the
root (`materialize.go:783-792`). If the source repository happens to hold a key `_dataset/definition.json`,
that data entry and the synthetic entry produce the same dataset key and the merge fails the publish with
`ErrDuplicateKey` ("duplicate or out-of-order key in materialised dataset"), which reads like an internal
bug rather than "your source has a reserved key". Validation rejects reserved *targets*
(`definition.go:211-216`) but cannot know the source keys.

**Why it matters.** It fails closed (no corrupt version), so this is minor; the error is unactionable, and
the same definition publishes fine from any other source.

**Fix.** Detect the collision in `openItems`/`itemIterator` when a mapped key equals `DefinitionPath` and
return a `*ValidationError` naming the item and the reserved path.

**Confidence:** likely (key arithmetic is clear; I did not build a source repository with that key).

## Out of scope (noted, not reviewed)

`pkg/sso/**` and the Azure Entra SSO changes, and `webui/src/pages/datasets/**`: not reviewed, as
instructed. I read `cmd/lakefs-sso/main.go` only to confirm how the SEC-2 guard is installed
(`buildAuthService`, `main.go:54-60`) and `cmd/lakefs/cmd/hooks.go` for the route-builder hook. Both look
correct: the guard wraps the very `auth.Service` later handed to the controller and the S3 gateway, so
there is no unwrapped authorization path. One observation from that wiring, outside the assigned files:
`buildAuthService` silently returns the *unwrapped* service when `datasets.LoadConfig()` returns an error,
relying on `RoutesBuilder` to fail startup afterwards. The two `LoadConfig()` calls read the same viper
state, so this is equivalent today, but the guard's installation now depends on that coincidence rather
than on a single decision point.

I ignored `pkg/api/apigen/*`, `pkg/datasets/apigen/*`, `contrib/auth/apigen/*`, `go.sum` and
`THIRD_PARTY_LICENSES` as instructed, and used `DATASETS_DESIGN.md` / `DATASETS_REVIEW.md` only to check
intent (e.g. the CLI-6 reference in m1).

## Files reviewed

Clean — reviewed in full, or in the changed regions where noted, and no findings:

- `pkg/datasets/source_cache.go` — I re-derived the soundness argument (per-operation policy snapshot;
  prefix key = longest literal policy prefix; wildcard-free resources keyed exactly, in a NUL-prefixed
  space; effects, conditions and `${user}` substitution all uniform inside a bucket) and could not build a
  case where two ARNs share a key and decide differently. `interpolateUser` matches `pkg/auth`
  (`pkg/auth/service.go:1378`), and `Amount: -1` means "all" for both `APIAuthService` and `acl.AuthService`.
- `pkg/datasets/store.go` — apart from M1: the CAS helpers (`PutDatasetIf`, `setIfUnchanged`, `deleteIf`,
  `deletePinReferences`, the lease `Acquire`/`Renew`/`Release`) pass the `kv.Predicate` through untouched as
  documented, the version-key padding makes key order equal version order, record identity is verified
  against the key on read, and no iterator is leaked (`scanPrefixFrom` defers `Close`).
- `pkg/datasets/pins.go` — branch before reference, re-verify after add (`pin`), re-create after delete
  (`deletePinBranch`), one record per reference, and the `Acquire`-rollback vs `Release`-rolls-back-
  everything asymmetry are all consistent (the latter cannot drop a needed reference: a pre-existing
  `<ds>@vN` implies no version record for vN, so a new publish of vN implies a fresh reference).
- `pkg/datasets/resolve.go`, `pkg/datasets/validate.go`, `pkg/datasets/definition.go` — no findings.
  Target-overlap detection is order-safe only because `Normalise` sorts first; `definitionProblems` always
  calls it, so the dependency holds.
- `pkg/datasets/metadata_keys.go`, `pkg/datasets/metadata_validate.go` — no findings.
- `pkg/datasets/authz.go` — no findings. `SourceReadNode` is correctly documented as the coarse gate and
  the authoritative check is the per-object one; `CheckSourceRepositories` runs before `Resolve`, so
  resolver errors cannot be used as an existence oracle (SEC-4).
- `pkg/datasets/handlers.go` — apart from m1: every handler funnels its error through one mapper, optional
  response fields are nil-checked before use, and the only endpoint without a dataset permission
  (`GetDatasetsConfig`) matches its spec ("Requires authentication but no dataset permission",
  `api/datasets.yml:138-158`).
- `pkg/datasets/http.go`, `pkg/datasets/metrics.go` — no findings. Middleware order is auth → requireUser →
  body limit → validator; `writeValidationError` maps `http.MaxBytesError` to 413 first; a context without a
  `Done` channel never starts the drain goroutine.
- `pkg/datasets/service.go` — apart from m2, m3, m6: the publish state machine (CAS to publishing, pins
  before data, `commitExistsError` short-circuit so a committed version is never rolled back, detached
  post-commit writes, sanitised task errors) is consistent with its comments.
- `pkg/datasets/materialize.go` — apart from m7: `mergeIterator.Close` closes every stream,
  `closeItemIterators` covers the partial-open path, the definition object is version-scoped, and the
  `BeforeCommit` hook runs before both the commit and the tag.

- `pkg/datasets/reconciler.go` — no findings beyond the contrast used in m2. Listing pins before datasets,
  renewing the lease between datasets, `orphanReferences` re-reading before deleting, `finishDelete`'s
  "record last, and only when clean", and the BUG-3 gated rollback are all correct.
- `pkg/datasets/proposals.go` — no findings. Base-version check, create-on-merge, conflicted reporting and
  the "a preview is not a version, so it takes no pin" decision are consistent (a lost source makes both the
  preview and the merge fail on the missing key).
- `pkg/datasets/diff.go` — no findings. The filtered-page loop advances the catalog cursor over filtered
  entries and cannot spin (the `len(page) == 0` guard), and `NextOffset` is the last returned path.
- `pkg/datasets/bootstrap.go`, `pkg/datasets/config.go` — no findings. Managed-policy comparison normalises
  nil vs empty condition maps, so a restart does not rewrite policies.
- `cmd/lakectl-datasets/cmd/retry.go` — no findings. The policy refuses to replay non-GET/HEAD requests and
  refuses to retry a transport error whose outcome is unknown; with `--expected-version`
  (`datasets.go:490-510`) a replayed publish is answered 409 rather than duplicating a version.
- `cmd/lakectl-datasets/cmd/datasets.go`, `pulls.go`, `access.go`, `metadata_keys.go`, `definition.go`,
  `output.go`, `root.go` — apart from m1: every response is status-checked before its JSON member is
  dereferenced, optional fields go through the `deref*` helpers, and the task poller is bounded by the
  `--wait` context and rejects unknown statuses.
- `pkg/gateway/handler.go`, `pkg/gateway/middleware.go`,
  `pkg/gateway/operations/{base,deleteobjects,listbuckets}.go` — no findings. `ClientIPResolver.Extract` is
  nil-receiver safe (`pkg/httputil/client.go:99-102`), so the "invalid trusted_proxies → nil resolver" path
  cannot panic; attaching the condition context before the empty-permission early return is what makes
  `DeleteObjects`' per-object re-authorization see conditions.
- `pkg/api/controller.go` (datasets and condition wiring) — no findings. The rewritten `ListBranches`
  decision is equivalent-or-stricter than the `authorize` call it replaced (a repository-level `CheckDeny`
  still fails the request; `CheckNeutral` is only shown when the repository itself is allowed), it falls
  back to the old path on `auth.ErrNotImplemented` (which is what `BasicAuthService` returns), and
  `SetGroupACL`'s new 409 in RBAC mode is correct.
- `pkg/auth/condition.go`, `pkg/auth/service.go` (branch diff only) — no findings. `StringEquals` /
  `StringNotEquals` mirror the Like variants, `ConditionContext.Lookup` preserves "missing field ⇒ condition
  false", and `NewRepositoryMetadataFields` memoises with `sync.Once` and turns a load error into a missing
  field, i.e. deny.

## Verification

- `go vet ./pkg/datasets/...` — clean.
- `go test ./pkg/datasets/ ./cmd/lakectl-datasets/...` — `ok` (53s / 1.1s). M1 is therefore invisible to the
  current suite; the cursor arithmetic was confirmed by byte-order comparison of the exact key strings the
  code builds, plus the `kv` contract pinned by `pkg/kv/kvtest/store.go` and `pkg/kv/mem/store.go`.

