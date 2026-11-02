# S-I2 — Per-commit file-registration cost and the uncommitted-GC window

Spike for ledger task **I0-2** (`tasks.json`), `ICEBERG_CATALOG_PLAN.md` §6.3 (staging strategy), §13
risk *"Staging thousands of entries per append"*, §11 phase I-0.

| | |
|---|---|
| Answer to (a) | A 10 k-file append staged with `catalog.CreateEntry` costs **≈2.3 s** at 32-way concurrency (**≈1.2 s** against a KV with a 1 ms round trip); a single-threaded loop costs **≈76 s** because of `graveler.max_batch_delay=3ms`, not because of the KV. The lakeFS commit that publishes the 10 k staged entries costs **40–70 ms**. |
| Answer to (b) | **No for raw client writes, yes for staged entries.** `PrepareGCUncommitted` does **not** list objects written directly under `<storage_namespace>/_iceberg/` (0 of 200 in the measurement): it lists only entries staged on branches (`DiffUncommitted`). Once the catalog stages an entry for an object, that object **is** listed. |
| Recommendation §6.3 | **Stage all files**, in a bounded worker pool, with a hard cap on files per commit. Manifests+metadata only is not viable in v1 without a GC-side prefix exclusion (§13 fallback), because nothing protects the data files. |
| Recommendation `iceberg.uncommitted_gc_grace` | **24 h default**, enforced in `PrepareGCUncommitted` as "do not list `<storage_namespace>/_iceberg/` entries younger than the grace"; document that staged table data older than the grace is collectable, exactly like any other uncommitted lakeFS data, and that the value must be ≥ the GC job's `lakefs.debug.gc.uncommitted_min_age_seconds`. |

Baseline: lakeFS `feature/datasets` @ `675c4f18e`. Spec cited (license guard, plan §2 /
`DATASETS_DESIGN.md` §2): <https://iceberg.apache.org/spec/> (table format — data files, manifests,
manifest lists) and <https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml>
(REST catalog). lakeFS behaviour was read only from this repository; no Enterprise code and nothing
from ≥ 1.87 (BSL) was consulted.

---

## 1. Method

A throwaway Go test harness (`pkg/catalog/zz_spike_i0_2_test.go`, **not committed**, deleted after the
run) built a **real** `catalog.Catalog` — graveler + committed SSTable manager + staging manager, the
same wiring the datasets spikes use — over:

* KV: `kv/mem` (in-process) and `kv/local` (badger, on-disk), driven through `kv.Open`;
* block adapter: `mem` (the `committed`/`pyramid` local cache pointed at `t.TempDir()`).

It creates a repository (`mem://<name>-ns/`), then for a table UUID `U` stages entries
`_lakefs_iceberg/files/U/data/part-NNNNN.parquet` with `AddressType=Full` pointing at the physical
address the client "wrote" (`mem://<ns>/_iceberg/U/data/part-NNNNN.parquet`) — i.e. exactly the shape
§6.3 step 3 prescribes. Two knobs were varied: `graveler.max_batch_delay` (lakeFS default 3 ms) and an
RTT added to every KV operation (a decorator over `kv.Store`) to model a network-backed KV. The
decorator also counts operations and partitions.

What this does **not** model: a real Postgres/DynamoDB (no docker daemon or DB in the spike
environment — network latency is emulated instead), and an object-store-backed block adapter (the
batched path's SSTable upload therefore costs ~0 here; on S3 add roughly 10–40 ms for a ~0.7 MB PUT).
Machine: darwin/arm64, `GOMAXPROCS=16`, Go 1.26.4 toolchain. Numbers below are the range over 3–4
runs, run-to-run spread ≈ ±10 %.

## 2. (a) Cost per staged entry — `catalog.CreateEntry` vs a batched path

### 2.1 The naive loop (one `CreateEntry` per file)

| Config | 10 k entries, sequential | per entry |
|---|---|---|
| mem KV, default batching | 1 m 16 s (**131–145 op/s**) | 6.9–7.6 ms |
| local badger, default batching | 1 m 16 s (**132–148 op/s**) | 6.8–7.6 ms |
| 50 files, default batching | 346–384 ms (**130–145 op/s**) | 6.9–7.7 ms |

The KV backend makes **no difference**, and 50 files cost the same per entry as 10 000. That is the
signature of a fixed per-call delay, not of KV work:

* `graveler.Set` → `safeBranchWrite` → `refManager.GetBranch`, which goes through
  `batch.BatchFor(..., graveler.max_batch_delay, ...)` (`pkg/graveler/ref/manager.go:408`,
  default **3 ms**, `pkg/config/defaults.go:169`). With a single producer there is nothing to batch, so
  each call pays two full batching windows ≈ 6.9 ms.
* With `graveler.max_batch_delay=0` the same loop runs at **11–13 µs/entry (mem)** and **16–18 µs/entry
  (local)** — 74–88 k entries/s. The delay, not the storage, is the floor.

**KV operations per staged entry: 3** (measured): 1 write to the staging-token partition
(`graveler.StagingTokenPartition`, `pkg/graveler/staging/manager.go:100`) and 2 reads (the repository
record and the branch). So a 10 k-file append is **30 000 KV operations** (10 k writes + 20 k reads).

### 2.2 Parallel staging (client-side batching, same API)

| Workers | mem KV | local KV |
|---|---|---|
| 8 | 9.3–9.6 s (1 046–1 175 op/s) | 9.2–9.3 s (1 079–1 192 op/s) |
| 32 | **2.2–2.4 s** (4 185–4 781 op/s) | **2.2–2.3 s** (4 332–4 782 op/s) |

Throughput scales almost linearly with workers, i.e. the 3 ms batching windows are filled rather than
paid. This is the cheapest realistic way to stage 10 k entries without changing the write path — and it
keeps the entries in each branch's staging area, so **one** lakeFS commit still publishes them (§6.2).

### 2.3 Network-backed KV model (1 ms RTT per KV operation, batching delay 0)

| Config | Result |
|---|---|
| sequential, 2 000 entries | 7.8–7.9 s → **3.9 ms/entry** (≈ 3 RTTs + overhead), 252–257 op/s |
| 32 workers, 10 000 entries | **1.18–1.20 s** (8 361–8 464 op/s) |
| lakeFS commit afterwards | 38–47 ms |

Extrapolating the measured 3 KV ops/entry: a 10 k-file append is 30 k KV round trips. At 0.5 ms RTT
(healthy Postgres in the same region) and 32-way concurrency that is ≈ 0.5 s; at 1 ms, ≈ 1.2 s
(measured). Sequentially it is 30–40 s — which is why I3-1 must not use a plain loop.

### 2.4 The batched SSTable path (`WriteRange` → `WriteMetaRange` → `Store.Import`)

| Step | mem | local |
|---|---|---|
| `WriteRange` (10 k entries, 1–2 ranges) | 13–26 ms | 11–19 ms |
| `WriteMetaRange` | 5–6 ms | 5 ms |
| `Store.Import` (commit) | 20–21 ms | 18–19 ms |
| **total** | **39–63 ms** | **35–43 ms** |

Range size ≈ **0.69 MB estimated** for 10 k entries (≈ 69 B/entry). Note `ShouldBreakAtKey`
(`pkg/graveler/sstable/writer.go:174`) splits a range stochastically after
`committed.permanent.range_raggedness_entries` (default 50 000) keys — one run produced 6 694 + 3 306
entries in two ranges, others a single 10 k range — so a caller must loop `WriteRange` until the
iterator is exhausted, exactly as `pkg/catalog/catalog.go:2548` (`importAsync`) does.

**But this path cannot be used for §6.3.** `Store.Import` creates its own commit and takes no
`graveler.WithCondition`, so it bypasses the pointer CAS of §6.2 step 6 (`WithCondition` on
`iceberg.metadata-location`) and would need a second commit to publish the pointer — breaking both the
single-commit guarantee and the 409-on-race semantics. Also, the pre-existing import **API** is
unusable here for a different reason: `verifyImportPaths`
(`pkg/catalog/catalog.go:2613`) rejects any source path inside the repository's own storage namespace,
which is where `_iceberg/` lives.

## 3. Total commit wall time at 10 k files

The lakeFS commit itself is cheap — the staged entries are merged into one SSTable by the commit path
(`pkg/graveler/committed`), it does not touch the KV per file:

| Path | stage | lakeFS commit | end-to-end |
|---|---|---|---|
| sequential, default batching (mem) | 76.5 s | 37–61 ms | **≈ 76 s** |
| sequential, default batching (local) | 75.7 s | 55–71 ms | **≈ 76 s** |
| 8 workers | 9.3 s | 41–58 ms | ≈ 9.4 s |
| **32 workers** | **2.2–2.4 s** | 41–69 ms | **≈ 2.3 s** |
| 32 workers, 1 ms KV RTT | 1.18–1.20 s | 38–47 ms | **≈ 1.2 s** |
| batched range + import | 39 ms | (import *is* the commit) 20 ms | ≈ 40–63 ms |
| manifests+metadata only, 50 files | 346–384 ms | 24–32 ms | ≈ 0.4 s |

So: **total commit wall time at 10 k files ≈ 2.3 s** (in-process KV, 32-way) / **≈ 1.2 s** when each KV
op costs 1 ms — dominated entirely by staging, not by the commit. Naive sequential staging is the only
configuration that fails a client's patience (≈ 76 s).

## 4. (b) Would `PrepareGCUncommitted` list objects the client wrote under `_iceberg/`?

**No — unless a lakeFS entry points at them.** `PrepareGCUncommitted`
(`pkg/catalog/catalog.go:2994`) never walks the storage namespace: it iterates branches
(`gcWriteUncommitted`, `pkg/catalog/gc_write_uncommitted.go:14`, branch loop at `:21`), and per
branch the *uncommitted diff* (`store.DiffUncommitted`, `processBranch`), emitting one parquet row
`{physical_address, creation_date}`
per **staged, non-tombstone** entry. Addresses inside the storage namespace are written
namespace-stripped; full addresses outside it are skipped
(`gc_write_uncommitted.go:110-118`), and tombstones are skipped at `:100`.

Measurement (`mem://<ns>/_iceberg/U/data/…`, real catalog):

| Case | Object count | Listed by `PrepareGCUncommitted` |
|---|---|---|
| raw objects written via the block adapter, **no** lakeFS entry | 200 | **0** |
| entries staged for 5 of those objects (`AddressType=Full`, in namespace) | 5 | **5** (as `_iceberg/U/data/raw-00N.parquet`) |
| staged entry with `AddressType=Relative` (`_iceberg/U/data/relative.parquet`) | 1 | **1** (as-is) |
| staged entry with a full address outside the namespace (`s3://someone-elses-bucket/…`) | 1 | **0** (skipped) |
| the same entries **after** `catalog.Commit` | 6 | **0** |
| 10 k staged file entries on one branch | 10 000 | **10 000** (prepare took **15 ms**) |

Two consequences, both load-bearing for §6.3:

1. **Staging is what puts a table file into the uncommitted-GC listing.** Nothing else does. Raw client
   writes are invisible to it, so the server cannot protect them, and it is not the uncommitted GC that
   would delete them — it is the **committed** GC sweep, which deletes namespace objects that no retained
   commit references. In this repo that sweep is performed by the external GC job
   (`io.treeverse.gc.GarbageCollection`, `esti/unified_gc_test.go:165`); the server only prepares
   `commits.csv` (`pkg/graveler/retention/garbage_collection_manager.go:176`) and the addresses location.
   `esti/unified_gc_test.go` asserts the model: an object referenced only by a **staged** entry survives
   the GC run (`file_3`), while objects whose staged entry has gone (a tombstone `file_4`, a reverted
   branch `file_7`, a deleted branch `file_10`) are deleted. That is exactly the failure mode §6.3 warns
   about, and it applies to every data file that is not registered.
2. **Once the entries are committed they leave the uncommitted listing and join the commit's metarange**,
   so a GC client that derives its tracked addresses from the metaranges sees them. Verified: after the
   commit, all 7 entries are visible on `main` under `_lakefs_iceberg/files/` with their `Full`/`Relative`
   physical addresses.

The staging itself is not free for the GC pass: each staged file entry is one parquet row in every
`prepare_gc_uncommitted` run for that branch (15 ms and 10 000 rows per branch at 10 k files, measured).
That is acceptable, but it is a second reason to commit table changes rather than leave them staged
forever.

## 5. Decision for §6.3: **stage all files**

Measured cost of the proposal as written (§6.3 steps 1–3), per 10 k-file append:

* 10 k staged entries = **30 k KV operations** = **1.2–2.4 s** of wall time at 32-way concurrency
  (≈ 0.5 s on a 0.5 ms-RTT KV, ≈ 76 s if executed sequentially — the only unacceptable configuration);
* the publishing lakeFS commit: **40–70 ms**, one SSTable of the same entries (≈ 0.7 MB estimated, as
  measured through the equivalent `WriteRange`);
* no per-object API calls, no per-object HEAD/GET: entry values are derived from the manifest Avro
  records, so the added work is KV writes only.

Arguments that decide it in favour of staging everything:

* **Data safety.** Without entries the files are unregistered namespace objects; the committed-GC sweep
  deletes them (§4, and the esti assertions). "Manifests+metadata only" therefore needs a GC-side
  exclusion of `<storage_namespace>/_iceberg/`, which does not exist in lakeFS v1 — the §13 fallback
  ("protect data files through a per-table GC rule") cannot be implemented with today's GC job, because
  retention rules protect *commits and their referenced objects*, not arbitrary prefixes.
* **Safety is testable with the existing esti GC harness** (I-3 exit criterion: append → commit → GC →
  table still readable), whereas a prefix exclusion would need GC-client changes we do not own.
* The cost is small at realistic concurrency, and it is bounded by a knob we control.

### 5.1 Guidance for I3-1 (`pkg/icebergcatalog/files.go`)

1. Stage with a **bounded worker pool of 16–32 goroutines** calling `catalog.CreateEntry` (measured
   4.2–4.8 k entries/s; 8 workers already gives 1 k/s). Do **not** use the batched
   `WriteRange`+`Store.Import` path (§2.4: no `WithCondition`, hence no pointer CAS, plus a second
   commit).
2. Order: read the manifest list and manifests, apply the **foreign-location check** (§13) for the whole
   snapshot *first* (fail before staging anything), then stage all file entries, then perform the §6.2
   step 6 pointer `CreateEntry` with `WithCondition`. All in one staging area ⇒ one lakeFS commit
   publishes the files and the pointer atomically (this is why parallel `CreateEntry` is the right
   primitive and ranges are not).
3. Set each entry's `LastModified` to the object's write time: it becomes the `creation_date` column that
   the GC job ages staged objects by (§4). Use `Size`/`ETag` from the manifest record (they are there);
   never issue HEAD requests for 10 k files.
4. Add a hard cap `iceberg.max_staged_files` (recommendation: default **50 000** ≈ 6 s at 32-way/1 ms
   RTT; reject above it with a 400-class `CommitFailedException` naming the knob). Silently skipping
   files above the cap converts a slow commit into a GC-deletable one.
5. Make retries idempotent: entry paths are deterministic (`_lakefs_iceberg/files/<table-uuid>/<relative
   path>`), so a retried commit overwrites the same keys; a commit that aborts after staging only leaves
   stage-only entries, which the uncommitted GC collects.

## 6. Recommendation for `iceberg.uncommitted_gc_grace`

**Default: 24 h. Enforced on the server, in the prepare step, not by the GC job.**

Mechanism (small, localised change, for I3-1/I3-2):

* in `processBranch` (`pkg/catalog/gc_write_uncommitted.go`), after the address is normalized, skip the
  row when the address is under `_iceberg/` **and** `entry.LastModified` is newer than the grace window;
  `0` means "never emit `_iceberg/` rows" (maximum protection, no orphan cleanup), which is the documented
  escape hatch for repositories that keep staged branches for weeks.

Why not rely on the GC job alone: deletion is decided by the external job
(`io.treeverse.gc.GarbageCollection`), which has its own
`lakefs.debug.gc.uncommitted_min_age_seconds` knob (see `esti/unified_gc_test.go:168`). An object is
deleted only if it is **both** listed by our prepare **and** old enough for the job, so the effective
protection is the **larger** of the two. Setting our grace ≥ the job's min age makes the server's knob
the binding one; document that.

Why 24 h and not "forever": staged data is not durable in lakeFS by design — the same rule already
applies to ordinary staged objects, and an unbounded grace would make `_iceberg/` staging areas
permanently uncollectable. 24 h matches the plan (§10 `iceberg.uncommitted_gc_grace`, §6.3) and
comfortably covers "Iceberg client appended, operator commits the branch the same day". Documented
consequences:

* a branch staged with table file entries and left uncommitted longer than the grace **can** lose those
  data files (the metadata pointer would then reference missing objects — there is deliberately no
  existence-check on commit: it would cost one HEAD per file);
* the same is already true, and already documented, for any lakeFS staged write; the natural fix is
  committing the branch, which is what moves files into the protected, commit-referenced set (§4,
  consequence 2);
* operators using long-lived staging branches must raise `iceberg.uncommitted_gc_grace` above their
  commit cadence.

Corollary to record in §6.3/§10: the **exposure window is [client writes the object → branch is
committed]**. Nothing the server can do protects objects before the catalog stages them (measured: they
are not listed, §4.1), so the documentation must state that a GC run while an Iceberg writer has written
files but the catalog has not yet staged and committed them may lose those files — the existing lakeFS
operational rule ("do not run GC while writers are active", which uncommitted GC relaxes only for
*listed* objects) applies unchanged to unregistered files.

## 7. Open items handed to later tasks

| # | Item | Owner |
|---|---|---|
| 1 | Confirm the GC job's deletion rule and the interaction with `uncommitted_min_age_seconds` against a **real** GC run (append 10 k files → commit → GC → table still readable; and: stage, wait, GC → files gone) | I3-3 esti |
| 2 | Decide whether `_iceberg/` (or a per-repository flag) may be excluded from the committed-GC sweep; the §13 fallback needs it | §13 / Q-I8 follow-up (new Q-I10) |
| 3 | Postgres/DynamoDB numbers: re-run the harness with `kv/postgres` to replace the 1 ms-RTT emulation | I3-1 optional |
| 4 | Whether the 3 ms `max_batch_delay` floor can be avoided on the catalog's write path (e.g. a bulk staging primitive in graveler); it would take 10 k staging writes from 2.3 s to ≈ 0.3 s | future graveler work, out of scope for v1 |

## 8. Reproduction

The harness was a single file, `pkg/catalog/zz_spike_i0_2_test.go` (package `catalog_test`), built from
the wiring in `pkg/datasets/spikes_helpers_test.go`: `kv.Open` (`mem` / `local`) →
`catalog.New(ctx, catalog.Config{Config: cfg, KVStore: store, PathProvider: upload.DefaultPathProvider})`
with `viper.Set(config.BlockstoreTypeKey, block.BlockstoreTypeMem)`, then
`cat.CreateRepository(ctx, name, "", "mem://"+name+"-ns", "main", false)`.

```go
// per-entry cost: predicate on entry shape only, so the numbers are the catalog's, not a micro-benchmark
for i := 0; i < 10000; i++ {
    rel := fmt.Sprintf("data/part-%05d.parquet", i)
    ent := catalog.DBEntry{
        Path:            "_lakefs_iceberg/files/" + U + "/" + rel,
        PhysicalAddress: ns + "/_iceberg/" + U + "/" + rel,
        CreationDate:    time.Now(), Size: 1024, Checksum: "etag",
        AddressType:     catalog.AddressTypeFull,
    }
    require.NoError(t, cat.CreateEntry(ctx, repo, "main", ent)) // or 32 goroutines over the same loop
}
start := time.Now()
_, err := cat.Commit(ctx, repo, "main", "10k", "spike", nil, nil, nil, false) // 40-70 ms
```

```go
// uncommitted-GC visibility: write objects with no entry, then stage entries for some of them
cat.BlockAdapter.Put(ctx, block.ObjectPointer{StorageNamespace: ns, Identifier: id,
    IdentifierType: block.IdentifierTypeFull}, size, reader, block.PutOpts{})
info, err := cat.PrepareGCUncommitted(ctx, repo, nil) // Location == ""  => nothing listed
// after staging: read info.Location+"/"+info.Filename as UncommittedParquetObject rows
```

Run with:

```bash
go test -count=1 -run 'TestSpikeI02' -v -timeout 60m ./pkg/catalog/   # ~4 min
```

Harness measurements were read from `SPIKE|` lines; it was deleted before the commit — this note and its
numbers are the only deliverable, per the ledger's spike rule.

## 9. References

* Apache Iceberg table spec (data files, manifests, manifest lists): <https://iceberg.apache.org/spec/>
* Apache Iceberg REST catalog OpenAPI: <https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml>
* lakeFS code anchors: `pkg/catalog/catalog.go:1201` (`CreateEntry`), `:2994`
  (`PrepareGCUncommitted`), `:2613` (`verifyImportPaths`), `:2548` (`importAsync` range loop);
  `pkg/catalog/gc_write_uncommitted.go` (`processBranch`); `pkg/graveler/ref/manager.go:408`
  (batched ref reads); `pkg/config/defaults.go:162` (`max_batch_delay`); `pkg/graveler/sstable/writer.go:174`
  (`ShouldBreakAtKey`); `pkg/graveler/staging/manager.go:105` (staging writes);
  `pkg/graveler/retention/garbage_collection_manager.go:176` (GC preparation).
* lakeFS GC behaviour asserted in-repo: `esti/unified_gc_test.go` (staged-vs-unreferenced objects).
* Plan: `ICEBERG_CATALOG_PLAN.md` §6.2, §6.3, §13, §11 I-0. Sibling spikes: S-I1, S-I3, S-I4.
