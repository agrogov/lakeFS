// Package cleanup implements the open-source counterpart of the lakeFS Enterprise
// "Tables Cleanup" job as a native Go tool - no Spark, no direct object-store access.
//
// It runs two phases over one lakeFS repository:
//
//  1. Expire - for every branch, for every Iceberg table the OSS Iceberg REST catalog
//     (pkg/icebergcatalog) knows about, expire old snapshots by rewriting table metadata
//     and staging the rewrite on that branch. Snapshots are retained according to the
//     table properties history.expire.max-snapshot-age-ms and
//     history.expire.min-snapshots-to-keep, the branch's current snapshot and every
//     snapshot an Iceberg ref points at are always kept, and a table whose property
//     lakefs-gc.enabled is false is left completely alone.
//
//  2. Mark and sweep - find the table files that no live table state references any
//     more and remove their lakeFS registrations, so that lakeFS' own garbage
//     collection can reclaim the objects. A file is never touched while it is
//     referenced by a branch head, a tag, a commit the lakeFS GC retains, or a staged
//     change, and a file younger than the configured safety age is never touched
//     either.
//
// # Working through the lakeFS API
//
// Everything this package does is a lakeFS REST call (pkg/api/apigen, wrapped in API
// below). That is a deliberate design decision, not a limitation worked around:
//
//   - the tool needs exactly the permissions an operator already has (fs:ReadObject,
//     fs:WriteObject, fs:DeleteObject on the reserved catalog area, fs:ReadBranch,
//     fs:ListTags and fs:ReadCommit), and no blockstore credentials at all;
//   - there is no second source of truth. Catalog state *is* lakeFS state
//     (ICEBERG_CATALOG_PLAN.md §6.1), so reading it through the API sees exactly what a
//     client of the Iceberg REST catalog sees, staging areas included;
//   - the safety rules of the sweep are lakeFS-level rules ("referenced by a branch
//     head, tag, GC-retained commit or staged change"), and those are the only refs the
//     API can enumerate authoritatively.
//
// The physical objects are therefore not deleted here. The sweep removes the
// _lakefs_iceberg/files/... entries that registered them, which is what makes the
// objects unreachable, and the lakeFS garbage collection (committed and uncommitted GC)
// reclaims the bytes with the storage credentials it already holds. This is the same
// two-step separation the lakeFS GC itself uses (design/accepted/gc-two-steps-separation.md):
// marking never deletes, and sweeping deletes only what a mark decided, after checking
// the references again.
//
// # Why expire rewrites metadata instead of using the catalog's commit API
//
// The Iceberg REST catalog accepts a remove-snapshots update, but a commit through the
// catalog is a client operation: it validates an Iceberg requirement against the current
// pointer and it is limited to one table at a time. A maintenance run has to work
// across every branch and every table of a repository and report what it did, so it
// writes the new metadata file through the object/workload API and stages the table
// pointer and the file registration exactly the way a catalog commit would
// (ICEBERG_CATALOG_PLAN.md §6.2 steps 4-6, §6.3):
//
//   - the metadata file is uploaded to its final location under the table's own
//     <table-location>/metadata/, so the pointer's metadata-location stays a physical,
//     fully qualified URI and every reader keeps working;
//   - the catalog's file entry _lakefs_iceberg/files/<uuid>/metadata/<name> is staged
//     pointing at that object, so the new metadata file is GC-protected by the same
//     commit that publishes the pointer (a staged entry is what the uncommitted GC
//     ages, a committed entry is what the committed GC retains);
//   - the table pointer _lakefs_iceberg/tables/<ns>/<table>/.pointer is staged with the
//     new metadata-location and the same table UUID and format version.
//
// Nothing is committed: the run stages the changes on each branch and the operator
// commits them, exactly like any other lakeFS change. A run is therefore reviewable
// (lakectl diff), revertible (lakectl branch revert) and atomic per branch, and a failed
// run leaves only unreferenced objects behind, which is the direction the catalog's own
// commit path also takes (§6.2 step 4).
//
// # Reports
//
// Every phase writes its report under the reserved _lakefs/retention/iceberg/ prefix on
// the repository's default branch, next to the lakeFS GC's own reports under
// _lakefs/retention/gc/:
//
//	_lakefs/retention/iceberg/expire/<run-id>/summary.json
//	_lakefs/retention/iceberg/sweep/<run-id>/summary.json
//	_lakefs/retention/iceberg/sweep/<run-id>/objects.jsonl    (mark only)
//
// A mark run's <run-id> is its mark ID, so a sweep can be run separately with
// --sweep-only --mark-id <run-id> and will read the objects.jsonl that mark wrote. The
// sweep report of such a run lands in its own sweep/<run-id>/ directory and names its
// mark ID, so both runs stay traceable.
//
// # Safety argument
//
// The sweep never deletes a registration whose object is still reachable:
//
//   - every branch head is enumerated (staged pointer included) and every table pointer
//     visible on it is resolved to its metadata and to every file that metadata
//     references: the metadata file itself, its metadata-log, its statistics files, the
//     manifest list of every snapshot, every manifest of every manifest list, and every
//     file of every manifest entry (ADDED, EXISTING and DELETED alike - the last is
//     over-protection by design);
//   - every tag is enumerated the same way, and so is every commit the lakeFS GC
//     retains: the first-parent chain of each branch back to (and including) the first
//     commit older than the branch's GC retention period, exactly the walk
//     pkg/graveler/retention performs (active_commits.go). When the repository has no
//     GC rules, GC never removes a commit, so every reachable commit is treated as
//     retained;
//   - every staged change is enumerated: a staged table pointer is resolved like a
//     branch head pointer, and every staged catalog file entry protects the object it
//     registers;
//   - a table whose metadata or manifests cannot be read (an unregistered "external
//     metadata" table, a missing entry, a parse error) protects *all* of its file
//     registrations, because the tool must never guess which files such a table still
//     needs;
//   - a file whose registration is younger than the safety age is skipped, which
//     covers the window between a client writing files for a snapshot and the catalog
//     staging that snapshot's entries (the same window the uncommitted-GC grace of
//     pkg/icebergcatalog/gc.go closes at the server);
//   - the sweep re-derives all of the above at sweep time and never trusts the mark
//     report: a file that became referenced between mark and sweep is skipped.
//
// v1 tables whose snapshots embed manifest locations (the deprecated "manifests" field)
// are refused outright by the expire phase: rewriting such metadata without knowing how
// every reader resolves embedded manifests would risk dropping live manifests, and the
// task is explicitly to abort instead.
//
// Licensing: this package is a clean-room implementation from the Apache Iceberg table
// spec (Apache-2.0, https://iceberg.apache.org/spec/), the Apache Iceberg REST OpenAPI
// spec (Apache-2.0), the public lakeFS documentation, ICEBERG_CATALOG_PLAN.md and the
// GC design documents in design/accepted/. No code is taken from lakeFS Enterprise or
// from upstream lakeFS >= 1.87 (BUSL 1.1).
package cleanup
