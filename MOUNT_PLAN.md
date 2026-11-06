# lakeFS Mount for lakeFS OSS — implementation plan

Design: [`MOUNT_DESIGN.md`](MOUNT_DESIGN.md) (section numbers below refer to it). Option **A**.
Options B (CSI, design section 12) and C (Windows) are not scheduled.

## Branching

All tasks run as Kanban cards with base `feature/mount` (new, cut from `feature/metadata-search`, which has
`cmd/lakectl/cmd/hooks.go` with `GetRoot` and `APIClientTransport`, and the esti SeaweedFS setup of
`esti/mds_test.go` that E1 reuses) and auto-commit. The mount touches no server code, so `feature/metadata-search`
can be merged into `feature/mount` at any time without conflicts in the mount's files.

File ownership rules on top of each task's list:
- `go.mod` / `go.sum` are edited by K1 (go-fuse) and K2 (go-nfs, go-billy) only; K2 runs after K1.
- `cmd/lakectl-mount/**` is edited by C1 first and by C2 after it.
- `Makefile`, `Dockerfile` and `.github/workflows/*` are edited by C1 (build) and E1 (CI job) only, in that order.
- `pkg/mount/fs` (the core) is edited by F1, F2 and F3 only, which run in that order.
- No task edits `pkg/api`, `api/swagger.yml`, `pkg/catalog`, `pkg/graveler` or upstream `cmd/lakectl` files.

## Status

| Task | State |
|---|---|
| (none started) | |

## Tasks

Each task owns the listed files, and no other task edits them. Every task ends with the verification rules below,
and its commit message includes the commands it ran and their output summary.

### R1 — remote layer (design 4.1, 4.2, 4.5 fetch, 8)
- Files: `pkg/mount/remote/remote.go` (interface `Remote`: `ResolveRef`, `ListDir(P, dir)` paging all pages,
  `Stat`, `PresignedGet` with TTL caching, `GetRange` fallback, upload, `CopyObject`, `DeleteObjects`,
  `CreateBranch(hidden)`, `HardReset`, `Commit`, `Merge(source-wins)`, `DeleteBranch`, `LogFirstParent`, `Config`),
  `pkg/mount/remote/lakefs.go` (on `apigen.ClientWithResponses`, reusing `pkg/api/helpers` uploaders),
  `pkg/mount/remote/objectstore.go` (ranged GET of a pre-signed URL, short-read detection), tests with `httptest`.
- Pattern to follow: `pkg/mds/client/lakefs.go` (paging, typed not-found, error mapping).
- Done when: every call is unit-tested against fakes, including pagination, 404 → typed not-found, 410 → typed
  expired, presign expiry refresh (80% of TTL), 403 on a pre-signed GET → one refresh then `GetRange`, short read →
  error, dirty-branch merge (400 with the `ErrDirtyBranch` message) → typed error.

### K0 — cache (design 4.2 storage, 4.5 chunks/eviction, 3.4 lock)
- Files: `pkg/mount/cache/lock.go` (exclusive `flock` on `<dir>/lock`, "in use by PID n"), `pkg/mount/cache/meta.go`
  (sorted on-disk directory listings per commit, binary search, small decoded-page LRU), `pkg/mount/cache/data.go`
  (sparse chunk files keyed by `sha256(physical address)`, bitmap sidecar, per-chunk singleflight, LRU eviction
  to 90%, pins), tests.
- Done when: tests cover concurrent fetch of one chunk (one fetch), eviction under budget with pinned chunks kept,
  restart reuses a persistent cache dir, corrupt bitmap or truncated chunk file → refetch, second lock holder
  refused, a 1M-entry directory listing served without loading it into memory (benchmark recorded).

### F1 — read-only filesystem core (design 4)
- Files: `pkg/mount/fs/inode.go`, `pkg/mount/fs/tree.go` (lookup, readdir, getattr, names rules 4.3, attributes
  4.4, `.lakefs_mount/` virtual dir), `pkg/mount/fs/read.go` (open/read/release, read-ahead,
  `max-fetch-concurrency` semaphore with prefetch skip), `pkg/mount/fs/refresh.go` (4.6 repin and invalidation
  callbacks), `pkg/mount/fs/fake_test.go` (fake `Remote`), tests.
- Done when: unit tests (no kernel) cover object/prefix collisions, hidden names, `--include-perm`/uid/gid,
  sequential read-ahead and skip when saturated, partial listing → `EIO` and not cached, refresh with open
  handles keeps reading the old object, the semaphore bound under 1000 concurrent reads.

### K1 — FUSE adapter (design 7.1)
- Files: `pkg/mount/fuse/fuse.go` (go-fuse v2 `fs` nodes over the core), `pkg/mount/fuse/fuse_linux_test.go`
  (build tag `linux`, skips without `/dev/fuse`), `go.mod`/`go.sum` (`github.com/hanwen/go-fuse/v2`).
- Done when: on Linux with `/dev/fuse` (colima: a `golang` container with `--device /dev/fuse --cap-add SYS_ADMIN`)
  a test mounts the core over the fake remote and checks `ls`, `stat`, `cat`, `sha256sum`, `dd` with offsets, and
  that the server exits on a plain `fusermount3 -u`. Non-Linux builds compile (stub returning "unsupported").

### K2 — NFSv3 adapter (design 7.2)
- Files: `pkg/mount/nfs/nfs.go` (go-nfs handler on a go-billy view of the core, random export token, HMAC file
  handles, errno mapping), `pkg/mount/nfs/mount_darwin.go`, `pkg/mount/nfs/mount_linux.go` (root check, no `sudo`),
  `pkg/mount/nfs/watch.go` (exit when the OS mount is gone), tests, `go.mod`/`go.sum`
  (`github.com/willscott/go-nfs`, `github.com/go-git/go-billy/v5`).
- Done when: an in-process NFSv3 client test (go-nfs's client package) mounts the export, reads and lists through
  it, is refused with a wrong export path and with a forged or previous-run handle, and every core error arrives
  as an NFS status, not an RPC failure. A macOS manual run of `esti/ops/mount/smoke.sh` (written in E1) is
  recorded in the commit message if a Mac is available, else stated as not run.

### F2 — write overlay (design 5.1–5.6)
- Files: `pkg/mount/fs/overlay.go` (generations, journal, compaction, replay), `pkg/mount/fs/write.go`
  (operations of 5.2, copy-up, rename rules, `max-dir-rename`), `pkg/mount/fs/diff.go` (5.5), tests.
- Done when: unit tests cover each row of 5.2, read-after-write, rename of a committed unchanged file fetches no
  data, directory rename bound → `EXDEV`, journal replay after a crash injected after each write step, a different
  URI with uncommitted changes refused, diff output for created-then-removed and renamed paths.

### F3 — commit engine (design 6)
- Files: `pkg/mount/commit/commit.go` (seal, ephemeral branch, parallel upload, commit, merge, repin, cleanup),
  `pkg/mount/commit/commit_test.go` (fakes), `pkg/mount/commit/integration_test.go` (in-process lakeFS from the
  `pkg/api` test setup, mem blockstore), `pkg/mount/fs/generation.go` (seal/fold hooks the engine calls).
- Done when: tests cover every failure row of section 6 (upload fail, commit fail, dirty branch, branch protection,
  merge timeout with the merge actually done and not done, crash during commit), writes during a commit kept in
  the next generation, rename during a commit lands in the next commit, branch moved by another writer
  (source-wins; other paths kept), nothing to commit / `--allow-empty`, `lakefs.mount.source-commit` metadata.

### C1 — plugin binary, server and control API (design 3, 8, 9)
- Files: `cmd/lakectl-mount/main.go`, `cmd/lakectl-mount/cmd/root.go`, `mount.go` (spawn, wait for ready, log tail
  on failure), `server.go`, `umount.go`, `status.go`, `pkg/mount/control/control.go` (Unix socket, peer-UID check,
  JSON requests, authorization behind one interface for option B), `pkg/mount/control/registry.go` (runtime-dir
  records), tests, and the build lines in `Makefile` (next to `LAKECTL_DATASETS_BINARY_NAME`) and `Dockerfile`.
- Done when: tests cover flag defaults and the `mount:` config section, spawn → ready → status → umount over the
  fake remote with a no-op OS adapter, spawn failure prints the log tail and exits 2, startup timeout, a peer with a
  different UID refused, `umount` with uncommitted changes exits 4, `sudo` never invoked (root-needing path fails with
  the documented message).

### C2 — write commands (design 3.2, 5.6, 6)
- Files: `cmd/lakectl-mount/cmd/diff.go`, `commit.go`, `refresh.go`, `pkg/mount/control/write.go`, tests.
- Done when: CLI tests cover `diff`, `commit -m --meta --allow-empty`, exit 3 on conflict or refusal, `refresh`
  refused with uncommitted changes and allowed without, `umount --discard`, `status --stale`.

### E1 — end to end (design 10)
- Files: `esti/mount_test.go` (build tag `mount`), `esti/ops/mount/Dockerfile` (Linux image with fuse3 and the
  binary), `esti/ops/mount/smoke.sh` (POSIX smoke script also used for the macOS manual run), CI job in
  `.github/workflows/esti.yaml` (`--device /dev/fuse --cap-add SYS_ADMIN`).
- Setup: lakefs-sso on SeaweedFS with presign on (the MDS esti setup in `esti/mds_test.go` shows the recipe; reuse,
  do not edit), the mount running in the Linux container.
- Cases: read-only mount of a branch, a tag and a commit; content matches lakeFS checksums; a commit to the branch
  after mounting is invisible until `refresh`; write mode create/overwrite/delete/rename/`rm -r` then `diff` and
  `commit`, result visible through lakectl; concurrent writer on another path kept (source-wins); dirty branch
  refused and retried; server killed with uncommitted changes, remount resumes them; user with read-only policy
  cannot commit (403 surfaces as exit 3); read throughput with `fio` recorded (not a gate).
- Done when: all cases pass locally (`go test -tags mount ./esti -run TestMount`), and the run log is in the
  commit message.

### D1 — docs
- Files: `docs/mount.md`, a link from `docs/index.md` or the nearest docs index.
- Content: install (plugin on PATH), Linux/macOS requirements (fuse3, `/dev/fuse`, `user_allow_other`; macOS
  nothing), every flag and command, read semantics (pinning, refresh, cache), write mode and commit (generations,
  source-wins, failures), permissions table, credentials (key pair vs `sso-login` expiry), differences from Everest
  (design section 1 table), limitations, troubleshooting (stale mounts, cache dir in use).
- Done when: every flag, command, exit code and error message in the design appears, and every snippet was run in
  E1.

### W1 — web UI mount modal (official docs, https://docs.lakefs.io/mount/)
- Files: `webui/src/lib/components/repository/mountModal.jsx` (opened from `tree.jsx`), and its test if one exists.
- Change: make the modal match the official page. Command `everest mount "lakefs://<repo>/<ref>/<path>" <local-dir>`
  (the page's syntax is `everest mount <lakefs_uri> <mount_directory> [flags]`); docs link `https://docs.lakefs.io/mount/`
  (the modal still points at the old `/reference/mount/`); availability line "Available in lakeFS Team and lakeFS
  Enterprise" (the modal says Enterprise or Cloud). The command stays `everest` in the UI even though the design
  builds `lakectl mount`: the UI follows the official docs. Add a download link to
  `https://docs.lakefs.io/releases/everest/` (each version page, e.g. `.../everest/0.15.1/`, lists the macOS, Linux and
  Windows binaries on `artifacts.lakefs.io`). Link the index, not a per-version artifact URL, so it does not go stale.
  Also link the CSI driver releases page `https://docs.lakefs.io/releases/everest-csi-driver/` (Kubernetes users; image
  `treeverse/everest-lakefs-csi-driver:stable`). Link only; the clean-room note keeps us from bundling the binary.
- Done when: the modal text, command and link match the official page; `cd webui && npm test`, `npm run lint` and
  `npm run format:check` pass.

Order (Kanban links): R1 and K0 start at once. F1 waits on R1 and K0. K1 and F2 wait on F1. K2 waits on K1
(go.mod). F3 waits on F2. C1 waits on K1 and F1. C2 waits on C1 and F3. E1 waits on C2 and K2. D1 waits on E1.
At most four cards are ever in progress together.

## Verification rules (every task)

- `GOTOOLCHAIN=auto go build ./...`, `GOOS=darwin go build ./pkg/mount/... ./cmd/lakectl-mount/...` and
  `GOOS=linux` the same (both adapters must compile on both), `go vet` and `go test -count=1 -race` on touched
  packages, `go tool golangci-lint run` on touched packages.
- Fail fast: `go test -timeout 10m`; every wait bounded in seconds with the log tail dumped on timeout; mounts and
  servers torn down in `t.Cleanup` (unmount checked with `mount | grep`), containers removed on exit.
- FUSE and E1 tests need docker (colima) with `/dev/fuse`. A task that cannot run something says so in its commit
  message.
- No edits to generated files; no swagger change; no server change.

## Risks

- **FUSE inside colima/CI:** `/dev/fuse` must be passed into the container. Prove it in the first hour of K1;
  fallback: run the FUSE tests in a privileged container.
- **go-nfs maturity on macOS:** macOS's NFS client is picky (readdirplus, cookie verifiers). K2 must run the smoke
  script on a real Mac before D1 claims macOS support; otherwise D1 marks macOS as experimental.
- **Listing cost on huge flat directories:** one paged listing per directory per commit (design 11). K0's
  benchmark records the cost; the sstable path stays a decision to revisit.
- **Pre-signed URLs on SeaweedFS:** presign works there for the iceberg catalog already; the fallback through
  `getObject` with `Range` keeps reads working where it does not.
- **`sso-login` JWT expiry:** mounts fail after at most 12h; documented, key pairs recommended.
- **Merge with source-wins hides concurrent edits on the same path:** Everest's documented behaviour, kept;
  `diff` before `commit` and the docs say so.

## Option B backlog (later)

B1 CSI node plugin and mount pod, B2 token-authorized control API, B3 Helm chart and static PV docs, B4 esti on
kind. See design section 12.
