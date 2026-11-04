# lakeFS Mount for lakeFS OSS — design

Status: draft for review. Companion: [`MOUNT_PLAN.md`](MOUNT_PLAN.md).

Clean-room note: upstream lakeFS Mount (Everest) is a closed-source lakeFS Team/Enterprise product. This is an
independent OSS implementation written only from the public documentation (https://docs.lakefs.io/mount/, the
Everest and Everest CSI release notes under https://docs.lakefs.io/releases/, and the public lakeFS blog posts on
Mount), the lakeFS REST API (`api/swagger.yml`) and this repository's code. No Everest binary, code or config is
used, inspected or reverse-engineered.

## 1. Goal and scope

Mount a path of a lakeFS repository as a local directory, so that any program can read it with ordinary file I/O,
without copying the data first. In write mode, let the program change files and commit the changes back to the
branch.

Reference behaviour (public Everest docs):

- `mount <lakefs://repo/ref/path/> <dir>` mounts lazily: listing downloads no file content, and a file's content
  is fetched when it is first read;
- the mount is pinned to the commit the ref resolved to at mount time and does not follow later commits;
- data is read straight from the object store through pre-signed URLs, and only metadata goes through lakeFS;
- a local cache, persistent when `--cache-dir` is given, bounded by `--cache-size`;
- write mode (`--write-mode`, branch URIs only): writes are staged locally and are visible only inside the mount
  (read-after-write consistent); `diff` lists them; `commit` uploads them to a hidden ephemeral branch, commits it
  and merges it into the source branch with the `source-wins` strategy;
- `--include-perm` (with `--include-uid`/`--include-gid`) uses the POSIX mode stored in the
  `::lakefs::posix-permissions` user metadata, the same format `lakectl local` uses;
- macOS through a local NFSv3 server, Linux through FUSE (or NFS as root), Windows through CFAPI (read-only).

### Delivery options

| Option | Content | Status |
|---|---|---|
| **A** | `lakectl mount` CLI: read-only and write mode, Linux (FUSE) and macOS (NFSv3), persistent cache, diff/commit. | **This design, to be built.** |
| **B** | A + Kubernetes CSI driver (static PVs, mount pods). | Documented for later (section 12). |
| **C** | A + Windows (CFAPI or WinFsp). | Not planned. |

Non-goals of A: Windows, a CSI driver, sharing one cache between mounts, multi-writer coherence between two
mounts of the same branch (each mount sees its own pinned commit plus its own writes), POSIX locks across hosts,
hard links, reading metadata ranges straight from the object store (section 11).

### Deliberate differences from Everest

These are where the public docs describe a weakness or leave the behaviour undefined. Each is a small addition,
and none changes a documented Everest command's meaning.

| Everest (public docs) | Here | Why |
|---|---|---|
| "Writes to a mount directory during a commit operation may be lost." | A commit seals the current overlay generation; writes made during the commit go to the next generation and are kept (5.4). | No silent data loss. |
| Uncommitted writes are lost when the mount ends. | The overlay lives in the cache directory. A remount with the same `--cache-dir` and URI resumes it; `umount` refuses while there are uncommitted changes unless `--discard` (5.6). | Same. |
| Read-only mounts never refresh; remount to see new commits. | `lakectl mount refresh <dir>` moves a mount without local changes to the ref's current commit (4.6). Pinning stays the default. | Remount is costly for long-running jobs. |
| A file and a directory with the same name: undefined. | The directory wins; the object is hidden and logged once (4.3). | Defined behaviour. |
| A commit without changes adds an empty commit. | Prints "nothing to commit" and exits 0; `--allow-empty` restores the Everest behaviour. | Avoids empty commits from retries in scripts. |
| Rename: unsupported in the docs, added in later releases. | Supported for files and directories inside one mount (5.3). | Editors and many tools save through rename. |

## 2. What exists and what is new

| Piece | Where | State |
|---|---|---|
| lakeFS API: `statObject`/`getObject` with `presign` and `Range`, `listObjects` with `delimiter`, `getPhysicalAddress`/`linkPhysicalAddress`, presigned multipart, `createBranch` with `hidden`, `hardResetBranch`, `copyObject`, `deleteObjects`, `updateObjectUserMetadata`,
`commit`, `mergeIntoBranch` with `strategy: source-wins`, `deleteBranch`, `getConfig` (`pre_sign_support`, `pre_sign_multipart_upload`) | `api/swagger.yml`, `pkg/api/controller.go` | **Exists.** The mount needs **no server change**. |
| Client config, credentials (key pair, `sso-login` JWT, AWS IAM), retries, User-Agent | `cmd/lakectl/cmd/root.go`, exported through `cmd/lakectl/cmd/hooks.go` (`GetRoot`, `APIClientTransport`) | **Exists.** Reused as `cmd/lakectl-datasets` does. |
| Presigned and direct upload helpers, download helpers | `pkg/api/helpers/upload.go`, `download.go` | **Exists.** Reused for commit uploads. |
| POSIX permission and symlink metadata format | `pkg/api/apiutil/constant.go` (`POSIXPermissionsMetadataKey`, `SymlinkMetadataKey`), `pkg/local/posix_permissions.go` | **Exists.** Reused so mounts and `lakectl local` agree. |
| `lakectl-mount` plugin binary (`lakectl mount …`) | new `cmd/lakectl-mount/` | New (section 3). |
| Remote layer, caches, filesystem core, FUSE and NFS adapters, commit engine | new `pkg/mount/...` | New (sections 4–7). |
| Docs | new `docs/mount.md` | New. |

The mount is a **REST client** of lakeFS and a direct client of the object store through pre-signed URLs. It does
not import `pkg/catalog` or `pkg/graveler` and needs no new permission, endpoint or server configuration.

## 3. Command line and process model

### 3.1 Binary

`cmd/lakectl-mount` is a lakectl plugin, built and wired exactly like `cmd/lakectl-datasets`: it registers its
command tree on `lakectlcmd.GetRoot()`, puts `mount` back in front of the arguments and calls
`lakectlcmd.Execute()`. `lakectl mount …` (plugin discovery) and `lakectl-mount …` behave the same, and the
configuration (`~/.lakectl.yaml`, `--config`, `LAKECTL_*`), credentials providers and retry policy are lakectl's.
No upstream `cmd/lakectl` file is edited.

### 3.2 Commands

Everest command → this command: `everest mount` → `lakectl mount <uri> <dir>`, `everest umount` →
`lakectl mount umount <dir>`, `everest diff` → `lakectl mount diff`, `everest commit` → `lakectl mount commit`,
`everest mount-server` → `lakectl mount server`.

| Command | Meaning |
|---|---|
| `lakectl mount <lakefs://repo/ref[/path/]> <dir> [flags]` | Resolve `ref` to a commit, start a mount server in the background, wait until the OS mount is live (bounded, `--startup-timeout`, default 30s), exit 0. On failure print the server's log tail and exit non-zero. |
| `lakectl mount umount <dir> [--discard] [--force]` | Stop the mount. Refuses while write mode has uncommitted changes unless `--discard` (the check goes to the control socket; without a live server there is nothing to check). It then runs the system unmount (`fusermount3 -u`, `umount`) and reads nothing from the mount, so it also clears a FUSE mount whose server died. It returns once the mount is gone; the server finishes its requests and cleanup on its own. `--force` uses `umount -f` / `fusermount3 -uz`. |
| `lakectl mount status [<dir>] [--json]` | Mount URI, pinned commit, mode, protocol, cache use, overlay size, uncommitted change count, server PID. Without `<dir>`, all mounts of this user. |
| `lakectl mount diff [<dir>]` | Write mode: `+ path`, `~ path`, `- path` against the pinned commit, sorted. |
| `lakectl mount commit [<dir>] -m <msg> [--meta k=v]… [--allow-empty]` | Write mode: section 6. Prints the new commit ID. |
| `lakectl mount refresh [<dir>]` | Re-resolve the mount's ref and repin when there are no uncommitted changes (4.6). |
| `lakectl mount server <uri> <dir> [flags]` | Foreground server (what `mount` spawns; also for containers and debugging). |

`<dir>` defaults to the mount containing the current working directory, found through `.lakefs_mount/source`.

### 3.3 Flags (`mount` and `server`)

| Flag | Default | Meaning |
|---|---|---|
| `--write-mode` | `false` | Write mode. The ref must be a branch. |
| `--cache-dir` | per-mount temp dir, removed at unmount | Persistent cache and overlay. One live mount per cache dir (lock file). |
| `--cache-size` | `10GiB` | Budget for the data and metadata caches. The write overlay is not evictable and not counted (it is reported by `status`). |
| `--protocol` | `fuse` on Linux, `nfs` on macOS | `fuse` or `nfs`. |
| `--presign` | from server `getConfig` | Read and upload through pre-signed URLs; `false` reads through `getObject` with `Range` and uploads through `uploadObject`. |
| `--include-perm`, `--include-uid`, `--include-gid` | `false` | As `lakectl local` (4.4). |
| `--symlink-support` | `false` | Objects with `::lakefs::symlink-target` show as symlinks, and `symlink(2)` works in write mode. |
| `--chunk-size` | `8MiB` | Fetch and cache unit. |
| `--prefetch-chunks` | `4` | Read-ahead after two sequential chunk reads. |
| `--max-fetch-concurrency` | `100` | Per-mount cap on concurrent chunk fetches. Reads wait for a free slot; a prefetch is skipped when none is free (Everest 0.15.1). |
| `--attr-ttl` | `1h` read-only, `1s` write mode | Kernel attribute and entry cache TTL. Content of a pinned commit never changes, so read-only can cache long. |
| `--allow-other` | `false` | FUSE `allow_other` (needs `user_allow_other` in `/etc/fuse.conf`). |
| `--startup-timeout` | `30s` | `mount` only. |
| `--log-level`, `--log-file` | `info`, `<cache-dir>/server.log` | Server log. |

### 3.4 Process model

`lakectl mount` runs `lakectl-mount mount server …` as a detached child (new session, stdin closed, stdout and
stderr to the log file), then polls the control socket until the server reports `ready` or the timeout or child
exit happens. One server process per mount.

The server:

1. takes an exclusive lock on `<cache-dir>/lock`, held until the process exits. A second mount of the same cache
   dir, including one started while a previous server is still finishing after `umount`, fails with
   "cache dir is in use by PID n";
2. resolves the ref to a commit ID (section 4.1) and loads or creates the mount state (`<cache-dir>/state.json`);
3. serves the filesystem (FUSE or NFS, section 7);
4. serves the control API on `<cache-dir>/control.sock` (Unix socket, mode 0600, peer UID checked on Linux with
   `SO_PEERCRED` and on macOS with `LOCAL_PEERCRED`). JSON requests: `status`, `diff`, `commit`, `refresh`,
   `shutdown`. `diff` and `commit` stream progress lines;
5. records `<cache-dir>` and its PID in `$XDG_RUNTIME_DIR/lakectl-mount/<hash of dir>.json` (macOS:
   `~/Library/Caches/lakectl-mount/`) so the CLI finds a mount from its directory;
6. on SIGTERM or `shutdown`: unmounts, flushes state, removes a temp cache dir, exits 0. When the OS unmounts it
   (`umount <dir>`), it exits the same way.

The mount directory carries a read-only virtual directory `.lakefs_mount/` with `source`
(`lakefs://repo/<commit>/path/`, so a Git checkout can record the exact data version as Everest's
`.everest/source` does) and `status.json` (the counters of `status --json`, section 9, read live; Everest 0.15.1
serves its read-stager counters the same way at `.everest/staging.json`). Every listing hides it except an explicit lookup, and in write mode it
cannot be written or committed.

## 4. Read path

### 4.1 Pinning

At mount the ref (branch, tag, commit ID, `ref~N`) is resolved through `getCommit` to a full commit ID `P`. Every
read uses `P` as the API ref, never the branch name, so the view is a consistent snapshot. Write mode records the
branch too (needed by commit). `.lakefs_mount/source` shows `P`.

### 4.2 Metadata

lakeFS paths become a tree: `a/b/c` is file `c` in directory `a/b`. Directories are the common prefixes of
`listObjects(ref=P, prefix=<dir>/, delimiter="/")` plus `<dir>/` marker objects. Because `P` is immutable, a
directory listing never goes stale:

- a directory is listed on first `lookup` or `readdir` of it, all pages (`amount=1000`), and stored in the
  metadata cache as a sorted file `<cache>/meta/<P>/<sha256(dir)>` (name, size, mtime, checksum, physical
  address, content type, the two `::lakefs::` metadata values; prefixes as directory entries). Large directories
  stay on disk and are searched by binary search; only a small LRU of decoded pages is in memory;
- `lookup(name)` answers from the parent's listing, so no per-file `statObject`;
- `getattr` of the mount root is synthetic (mtime of `P`'s creation);
- listing is done once per directory per commit; concurrent first accesses of one directory share one listing
  (singleflight);
- user metadata other than the two `::lakefs::` keys is not fetched by listings (`user_metadata=false`). It is
  exposed as extended attributes `user.lakefs.<key>` on demand through `statObject` (read-only).

A failed or partial listing returns `EIO` and is not cached; the next access retries. A listing never returns a
partial result that looks complete (an Everest NFS bug fixed in 0.15.1).

### 4.3 Names

- Object `a` and prefix `a/` both present: the directory wins, the object is hidden and logged once per commit.
- Path elements `.` and `..`, empty elements (`a//b`) and names longer than 255 bytes cannot be represented;
  those objects are hidden and logged once per commit.
- Inode numbers are stable for `(P, path)`: a 64-bit hash with collision chaining in the inode table. A refresh
  or commit changes `P`, so inodes of unchanged paths stay stable while the kernel revalidates them.

### 4.4 Attributes

- size = object size; mtime = ctime = atime = object mtime; directories use `P`'s creation time.
- Without `--include-perm`: files 0644, directories 0755, owner the mounting user. With it: mode from
  `::lakefs::posix-permissions` (JSON `{"UID","GID","Mode"}`, as `pkg/local` writes it), UID/GID only with
  `--include-uid`/`--include-gid`; objects without the key use the defaults.
- `chmod`, `chown` and `utimes` in a read-only mount: `EROFS`. Write mode: section 5.2.

### 4.5 Data

An object's content is identified by its **physical address**. lakeFS never rewrites an object in place, so a
physical address always has the same bytes, which makes the data cache content-addressed and valid across
commits, refreshes and mounts that reuse a cache dir.

- **Chunks.** An object is read in `chunk-size` aligned chunks. Cached chunks of an object live in one sparse
  file `<cache>/data/<sha256(physical address)>` with a bitmap sidecar. Concurrent reads of one missing chunk wait
  for one fetch (singleflight per chunk).
- **Fetch.** With presign: `statObject(presign=true)` gives a pre-signed URL. It is cached until 80% of its TTL
  (from `X-Amz-Expires` or the signature date, as `presignedURLExpired` in `pkg/api/helpers/upload.go` does) and
  fetched with HTTP `Range`. Without presign, or when the pre-signed GET is refused with 403 after one refresh:
  `getObject(ref=P, path, Range)` through lakeFS.
- **Integrity.** A chunk shorter than expected (except the last) or a size mismatch fails the read with `EIO`
  and is not cached. A full-object read whose `ETag` equals the lakeFS checksum is verified when the checksum is a
  plain MD5 (single-part uploads); multipart checksums are not verified.
- **Read-ahead.** After two sequential chunk reads, the next `prefetch-chunks` chunks are fetched in the
  background within `max-fetch-concurrency`.
- **Eviction.** LRU by chunk access time over `data/` and `meta/`, down to 90% of `cache-size` when it is
  exceeded. A chunk being read, and an object open for read-write, is pinned.
- **Expired objects.** `410 Gone` from lakeFS (GC removed the data of an old commit) is `EIO`, with the path
  logged.

### 4.6 Refresh

`lakectl mount refresh` re-resolves the mount's original ref. If it moved to `P'` and the overlay is empty, the
mount switches to `P'` atomically: new lookups use the new listings; open file handles keep reading the object
they opened (by physical address); the kernel caches are invalidated (FUSE `notify_inval_entry`/`inode` for
cached paths; NFS by changing the directory change attribute and mtime). With uncommitted changes, refresh
refuses and says to commit first. Refresh of a commit-ID or tag mount is a no-op.

## 5. Write mode

### 5.1 Overlay

Changes are kept in a local overlay under `<cache-dir>/overlay/<generation>/`:

- `files/<id>`: full content of each created or changed file (`id` is a random 128-bit name);
- `index` (an append-only journal compacted on mount and after commit): for each changed path either
  `file(id, mode, uid, gid, mtime, symlink target)`, `whiteout` (removed), `dir(mode)` (created directory),
  or `copy(source physical address, source path)` (renamed, not yet changed).

A lookup checks the overlay first, then the pinned commit, so the mount reads its own writes immediately.

### 5.2 Operations

| Operation | Effect |
|---|---|
| `create`, `open(O_CREAT)` | New overlay file. |
| `open` for write of a committed object | Copy-up: the whole object is fetched into `files/<id>` (through the chunk cache) unless the open has `O_TRUNC`, in which case nothing is fetched. |
| `write`, `truncate`, `fallocate` (mode 0), `fsync` | On the overlay file. `fsync` makes the file and the journal durable. |
| `unlink` | Whiteout (or the overlay entry is dropped if the path was created in this generation). |
| `mkdir` | `dir` entry. Without `--include-perm` an empty new directory is not committed (lakeFS has no directories) and the mount says so in `diff`; with it, a `<dir>/` marker object carries its mode, as `lakectl local` does. |
| `rmdir` | `ENOTEMPTY` unless empty in the merged view; then whiteout of the marker, if any. `rm -r` works. |
| `rename(a, b)` of a file | Overlay entry moves; for a committed unchanged file a `copy` entry plus a whiteout of `a`, so no data is fetched. `RENAME_NOREPLACE` honoured; `RENAME_EXCHANGE` returns `EINVAL`. |
| `rename` of a directory | Every path below becomes a `copy`/overlay entry plus a whiteout. Bounded by `--max-dir-rename` (default 10 000 entries); beyond it `EXDEV`, so `mv` falls back to copy and delete. |
| `chmod`, `chown`, `utimes` | With `--include-perm`(`-uid`/`-gid`): recorded; a committed unchanged file gets a metadata-only entry (committed as copy plus updated metadata). Without: accepted and ignored, as Everest documents. |
| `symlink`, `readlink` | With `--symlink-support`: zero-byte object with `::lakefs::symlink-target`, as `lakectl local`. Without: `ENOTSUP`. |
| `link` (hard link) | `ENOTSUP`. |
| `flock`, `fcntl` locks | Local to this mount only (the kernel's local lock handling on FUSE, `nolocks` on NFS). Not seen by other hosts. |
| Writes under `.lakefs_mount/` | `EROFS`. |

User metadata of a changed object is kept: commit re-sends the committed object's user metadata (except the
`::lakefs::` keys it sets itself) with the new content. Content type is kept the same way; new files get none, as
`lakectl local` uploads them.

### 5.3 Consistency

- Read-after-write inside one mount: always (the overlay is consulted first and FUSE write-back caching is off).
- Other mounts and lakeFS clients see nothing until commit.
- Two writers on one mount (two processes) see each other's writes (one overlay), as on a local filesystem.
- Changes to the branch by others after the mount are not visible until commit (which repins) or refresh.

### 5.4 Generations

The overlay has a current generation `g`. `commit` seals `g` and atomically opens `g+1`: new writes, and the
first write to a file that is still open from `g`, go to `g+1` (copy-up from `g`). The commit uploads only `g`.
After success the merged view is the new commit plus `g+1`; `g` is deleted. On failure `g` and `g+1` are folded
back into one generation (newer entries win), so nothing is lost and the commit can be retried.

A rename or unlink during a commit is a `g+1` entry, so it never changes what `g` uploads: the file is committed
under its sealed-time path, and the rename lands in the next commit. Everest 0.15.1 fixed the opposite case in
`everest sync`, where a rename during an upload lost the file.

### 5.5 Diff

`diff` walks the sealed-or-current generation's index (not the whole tree): `+` for paths absent from `P`, `~` for
paths present in `P` (content or, with `--include-perm`, mode changed), `-` for whiteouts of paths present in `P`.
A created-then-removed path does not appear. A `copy` entry is `+` at the destination; its source shows as `-`.

### 5.6 Durability and unmount

The journal is fsynced on `fsync`/`close` of a changed file and at least every second. A crash loses at most the
writes of the last second that were not fsynced, like a local disk. On restart with the same cache dir and URI, the
server replays the journal and the mount has its uncommitted changes again. A restart with the same cache dir and a
different URI refuses while there are uncommitted changes. `umount` refuses with uncommitted changes, listing the
count and suggesting `commit` or `--discard`.

## 6. Commit

`lakectl mount commit -m <msg>` on a write-mode mount of branch `B` pinned at `P`:

1. **Seal** generation `g` (5.4). Nothing to commit and no `--allow-empty`: print "nothing to commit", exit 0.
2. **Ephemeral branch.** `createBranch(name=E, source=P, hidden=true)` with
   `E = lakectl-mount-<mount id>-<g>` (mount id: 8 random hex characters stored in `state.json`; matches lakeFS's
   branch-ID rule `^\w[-\w]*$`). If `E` exists (a retried commit), it is reused after a `hardReset` to `P`
   discards a partial upload.
3. **Upload** to `E`, in parallel (`--parallelism`, default 16):
   - `file` entries: `PreSignUploader` (presigned, multipart when the server supports it) or `ClientUpload`, both
     from `pkg/api/helpers`, with the kept user metadata and the `::lakefs::` keys;
   - `copy` entries: `copyObject` from `P`'s source path (server-side, no data through the client);
   - metadata-only entries: `copyObject` with the new user metadata (`updateObjectUserMetadata` when the content
     is unchanged);
   - `whiteout` entries: `deleteObjects` in batches of 1000.
4. **Commit** `E` with `<msg>`, the `--meta` pairs and `lakefs.mount.source-commit=<P>`, giving `C`.
5. **Merge** `E` into `B` with `strategy=source-wins` and message `<msg>`. If `B` did not move since `P`, the result
   has exactly the mount's changes; if it moved, changes made by others to other paths are kept, and on a path both
   changed the mount wins (Everest's documented rule). The merge commit `M` is the new `P`.
6. **Repin** to `M` (4.6 mechanics), drop generation `g`, delete `E`. Print `M`.

Failures:

| Failure | Outcome |
|---|---|
| Upload or commit of `E` fails | Generation folded back (5.4), `E` kept for the retry (step 2 resets it). Exit non-zero with the failing path and status. |
| Merge `400` with the `ErrDirtyBranch` message (`B` has uncommitted changes made through lakeFS; `pkg/api` maps it to 400 and the client matches the message) | Same, plus: "branch B has uncommitted changes; commit or reset them, then retry". |
| Merge refused by branch protection or a pre-merge hook | Same, with the server's message. |
| Merge timed out, outcome unknown | The commit reads `B`'s log for a merge commit whose second parent is `C`; found: continue at step 6; not found: retry the merge once, then fail as above. |
| Crash during commit | On restart the state shows a sealed `g`; it is folded back and `E` is reset on the next commit. |

Leftover `lakectl-mount-*` hidden branches (crash without restart) are listed by `lakectl mount status --stale`
(branches of this mount id) and removed by `lakectl mount umount --discard`. They hold no data that is not also
in the local overlay.

Permissions the user needs (all exist in `pkg/permissions`):

| Mode | Actions |
|---|---|
| Read-only | `fs:ReadRepository`, `fs:ReadCommit`, `fs:ReadBranch` (branch URIs), `fs:ReadTag` (tag URIs), `fs:ListObjects`, `fs:ReadObject` on the mounted prefix, `fs:ReadConfig` |
| Write mode adds | `fs:CreateBranch`, `fs:DeleteBranch`, `fs:RevertBranch` (reset of `E`), `fs:WriteObject`, `fs:DeleteObject`, `fs:CreateCommit` on `E` and `B` |

## 7. OS adapters

The filesystem core (`pkg/mount/fs`) is protocol-independent: inode table, lookup, readdir, getattr, open/read/
write/release, the write operations of 5.2, invalidation callbacks. Two thin adapters map it to the kernel.

### 7.1 FUSE (Linux, default)

`github.com/hanwen/go-fuse/v2` (BSD-3-Clause) through its `fs` node API. No root: `fusermount3` mounts it.
Kernel entry and attribute TTLs from `--attr-ttl`; `FOPEN_KEEP_CACHE` for read-only files (content of `P` is
immutable); write-back cache off. Invalidations through `NotifyEntry`/`NotifyContent` on refresh and commit.
Requires `/dev/fuse` and `fusermount3`; startup checks both and says what is missing.

### 7.2 NFSv3 (macOS default, Linux as root)

`github.com/willscott/go-nfs` (Apache-2.0) on a `go-billy` filesystem view of the core, listening on
`127.0.0.1:<random port>`. macOS mounts it with `mount_nfs -o vers=3,tcp,port=<p>,mountport=<p>,nolocks,
locallocks,soft,intr,timeo=10 127.0.0.1:/<export> <dir>`, which needs no root on a directory the user owns. Linux
NFS mounts need root, as Everest documents.

Root handling (as Everest 0.15.1): the tool never runs `sudo` itself. An operation that needs root fails and says
so (`sudo lakectl mount … --protocol nfs` on Linux). Under `sudo`, the lakectl config of the invoking user
(`$SUDO_USER`'s `~/.lakectl.yaml`) is read when root has none; `LAKECTL_*` variables need `sudo -E`. FUSE never needs
root. A plain `umount <dir>` (also `sudo umount` of a Linux NFS mount) stops the server: the FUSE server sees the
session end, and the NFS server watches the mount table (every 2s) and exits when its mount is gone.

Every server-side error reaches the application as a POSIX errno (`EIO` by default; `ENOENT`, `EACCES`,
`EROFS`, `ENOSPC` where they apply), never as an RPC-level failure such as macOS's "RPC struct is bad".

Local exposure: an NFSv3 server on localhost has no user authentication, so any local user could talk to the port.
Two controls keep it closed:

- the export path is a random 128-bit token (`/<export>`), and `MOUNT` of any other path is refused;
- NFS file handles are HMAC-SHA256 over `(mount id, inode)` with a per-run random key, so they cannot be guessed
  or reused across runs.

The token and key are in memory and in the server's command line only as a file descriptor, never on disk or in
`ps` output. FUSE has no such exposure (the kernel enforces the mounting user unless `--allow-other`).

NFSv3 limits: no `RENAME_NOREPLACE` (plain rename), no extended attributes (user metadata only through
`statObject`), attribute caching by the macOS client (`actimeo` follows `--attr-ttl`).

## 8. Authentication and configuration

- Credentials and endpoint come from lakectl (`APIClientTransport`): `~/.lakectl.yaml`, `--config`,
  `LAKECTL_*`. Key pairs and AWS IAM are refreshed by the existing providers.
- A JWT from `lakectl sso-login` has a fixed TTL (at most 12h) and is not refreshable by the mount. When it
  expires, reads of uncached data fail with `EACCES` and the server logs "credentials expired; run lakectl
  sso-login and lakectl mount refresh" (`refresh` reloads credentials from config, even when the ref did not
  move). The docs recommend a key pair for long-lived mounts.
- Pre-signed URLs carry the object-store access; the mount never needs object-store keys.
- The server never writes credentials to the cache dir or the log.
- Mount flags can also be set in a `mount:` section of `~/.lakectl.yaml` (same names, flags win).

## 9. Operations

- Logs: `<cache-dir>/server.log`, rotated at 10 MiB, 3 files.
- `lakectl mount status --json`: cache hits, misses, bytes fetched, fetch errors, open handles, overlay bytes,
  uncommitted changes, pinned commit, credential state.
- `--pprof-listen` (off by default) for profiling.
- User-Agent: lakectl's plus `lakectl-mount/<version> (<os>; <protocol>)`, so server logs tell mounts apart.
- Exit codes: 0 ok, 1 usage, 2 mount failed, 3 commit conflict or refused, 4 uncommitted changes block the action.

## 10. Tests

- Core, unit (no kernel): against a fake remote, every row of 5.2, generations and commit-time writes (5.4),
  journal replay after a crash at each step, name rules of 4.3, eviction and pinning.
- Remote layer, unit: against `httptest` fakes of lakeFS and the object store: paging, singleflight, presign
  expiry and refresh, `Range` fallback, short reads, `410`.
- Commit engine, integration: against an in-process lakeFS (`pkg/api` test server, mem blockstore, so the
  non-presigned paths) for every failure row of section 6, including a branch moved by another writer
  (source-wins) and a dirty branch.
- FUSE adapter: in a Linux container with `/dev/fuse` (colima locally, a CI job with `--device /dev/fuse
  --cap-add SYS_ADMIN`): POSIX behaviour subset via shell (`ls`, `cat`, `cp`, `mv`, `rm -r`, `dd`, `sha256sum`
  against the lakeFS checksum) and a Go test that drives the mounted directory.
- NFS adapter: in-process through an NFSv3 client library (no OS mount) on any platform; a manual macOS run
  of the same shell script recorded in the plan.
- esti: lakefs-sso on SeaweedFS (presign on), a Linux container that mounts read-only and write mode, end to end
  (section 6 flows, refresh, restart with uncommitted changes, permission denials). A read-throughput figure
  (`fio` sequential and random 1 MiB reads over a 10 GiB file set, cold and warm cache) is recorded, not a gate.

## 11. Decisions

- **Listings through `listObjects`, not sstables.** Everest reads lakeFS's range files directly. Here that would
  need a pre-signed metadata endpoint (`/metadata/range` returns a storage URI, not a URL the client may fetch) and
  a dependency on graveler's file format in a client. Listings are cached per immutable commit, so each directory
  costs one paged API listing per commit. Revisit if listing-heavy workloads (millions of small files) show it is
  the bottleneck: a server endpoint that pre-signs range files, plus a client reader of `pkg/graveler/sstable`.
- **Client-only.** No server change keeps the feature usable against any lakeFS OSS ≥ the fork's version and
  avoids merge conflicts with upstream.
- **Content-addressed by physical address**, not by checksum: a physical address is immutable and is what the
  object store serves, so a cache key needs no extra request.
- **FUSE on Linux, NFS on macOS.** macFUSE needs a kernel extension or FSKit setup the user must approve; the
  localhost NFS server needs nothing, which is why Everest uses it there.

## 12. Future: option B (CSI driver, not built now)

A CSI node plugin (`mount.csi.lakefs.io`) for static PVs, modelled on the public Everest CSI docs: volume
attributes `lakeFSMountUri`, `authenticationSource: secret` (a Secret with `access_key_id`, `secret_access_key`,
`server_endpoint_url`), cache `emptyDir` with size limit; `ReadOnlyMany` mounts read-only, `ReadWriteMany` in write
mode; one mount pod per (node, PV, options) running `lakectl mount server` unprivileged with the `/dev/fuse` fd
passed from the node plugin; workloads commit with `lakectl mount commit` over the control socket shared into the
pod. It needs the control API of 3.4 to accept a token (not only peer UID), which A keeps possible by putting
authorization of the control API behind one interface.
