package icebergcatalog

// gc.go holds the two GC-safety controls of phase I-3 (ICEBERG_CATALOG_PLAN.md §6.1, §6.3):
//
//  1. the uncommitted-GC grace that keeps client-written table files out of the uncommitted-GC
//     report while the catalog has not staged them yet (UncommittedGCProtected), and
//  2. the installation-wide deny statement that reserves the _lakefs_iceberg/ key prefix against
//     the fs: object API (ReservedPrefixProtectPolicy).
//
// They live in one file because they exist for the same reason. A client writes data, manifest
// and metadata files straight to <storage_namespace>/_iceberg/<table-uuid>/… and the catalog then
// stages lakeFS entries that point at them (§6.3). Before the staging the objects are
// unregistered, and after it they are merely staged, never committed by the catalog (§6.2 "why
// stage instead of auto-commit"), so each of the two moments has its own GC hazard and its own
// control here.
//
// Neither control is a proof of safety, and the residual window is stated where it belongs: on
// UncommittedGCProtected below and in the D1 documentation of §6.3. The controls bound the damage
// a GC run can do to live table data; they do not make an unregistered object durable.
//
// Clean-room note (plan §2, DATASETS_DESIGN.md §2): written from the Apache Iceberg table spec
// (https://iceberg.apache.org/spec/), the Apache Iceberg REST OpenAPI spec
// (https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml) and the
// public lakeFS documentation only. No code from lakeFS Enterprise, and nothing from upstream
// lakeFS >= 1.87 (BSL), was used.

import (
	"strings"
	"time"

	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/permissions"
)

// DataPrefix is the key of the physical table-file area of a repository, relative to the
// repository's storage namespace (ICEBERG_CATALOG_PLAN.md §6.3):
//
//	<storage_namespace>/_iceberg/<table-uuid>/{metadata,data}/…
//
// It is deliberately not the same key as ReservedPrefix (store.go): ReservedPrefix
// ("_lakefs_iceberg") holds the catalog's own entries *on a branch*, which diff, merge and revert
// version, while DataPrefix holds the objects the client writes into the storage namespace, which
// are shared by every branch and are never copied. §6.3 keeps the two apart on purpose: a physical
// location that is stable across branches is what makes a merge of two branches' metadata
// pointers correct, and it is also why the uncommitted GC - which lists objects, not entries -
// needs its own protection (below).
const DataPrefix = "_iceberg"

// UncommittedGCProtected reports whether an uncommitted lakeFS object must be skipped by the
// uncommitted GC because it is a fresh client-written Iceberg table file
// (ICEBERG_CATALOG_PLAN.md §6.3, default iceberg.uncommitted_gc_grace = 24h).
//
// Arguments:
//
//   - key is the object key relative to the repository's storage namespace, which is exactly the
//     string the uncommitted-GC report stores as PhysicalAddress after truncating a full address
//     (pkg/catalog/gc_write_uncommitted.go, processBranch); a leading "/" is tolerated. A full
//     URI is not accepted: the caller already holds the truncated form, and guessing where the
//     namespace ends inside a URI would either under- or over-protect.
//   - lastModified is the staged entry's LastModified - the object's write time, which the
//     staging step copies into the entry (spike S-I2 §5.1 item 3). A zero value is treated as
//     unknown age and protected, because the report's creation_date column would carry the
//     zero time and the GC job's own age check would then delete the object immediately.
//   - now and grace are the current time and the configured window.
//
// Protection is decided as "under DataPrefix and younger than grace":
//
//   - grace > 0: a file younger than grace is protected; the boundary is exclusive, so at
//     exactly age == grace the object is collectable, like any other staged object;
//   - grace == UncommittedGCGraceProtectAll (0): every file under DataPrefix is protected,
//     whatever its age. This is the escape hatch of spike S-I2 §6 for installations that keep
//     staged branches for weeks; a negative grace (rejected by Config.Validate) is treated the
//     same way, because the fail-safe direction is "protect";
//   - a key that is not under DataPrefix is never protected: ordinary staged objects keep the
//     current behaviour, and no repository - iceberg or not - changes its GC defaults.
//
// Why this is the right hook. PrepareGCUncommitted (pkg/catalog/catalog.go) does not list objects
// written directly under <storage_namespace>/_iceberg/; it lists only *staged entries*
// (DiffUncommitted), and only afterwards does the external GC job delete what the report names.
// Measured in spike S-I2 §4: 0 of 200 raw client writes were listed, while every object with a
// staged entry was. So the window this control closes is the second half of the exposure:
// [catalog stages the entries → the lakeFS commit publishes them]. Without it, a 24h-old staged
// append on an uncommitted branch is collectable exactly like any other staged data.
//
// Residual window, stated explicitly (plan §6.3, spike S-I2 §6 "Corollary"):
//
//   - Before the catalog stages anything, a client-written file is invisible to the uncommitted
//     GC but *not* protected by it. The committed-GC sweep deletes namespace objects that no
//     retained commit references, and nothing here changes that. The exposure window is therefore
//     [client writes the object → the catalog stages it], and a committed GC run during an active
//     Iceberg write can still lose those files. The operational rule "do not run GC while writers
//     are active" applies to unregistered files unchanged.
//   - The grace is bounded: a branch staged with table-file entries and left uncommitted longer
//     than grace can lose those data files, and the metadata pointer would then reference missing
//     objects (there is deliberately no existence check on commit: it would cost one HEAD per
//     file). Commit the branch, or raise the grace, exactly as for any other staged write.
//   - Deletion is decided by the external GC job, which has its own minimum-age setting
//     (lakefs.debug.gc.uncommitted_min_age_seconds). An object is deleted only if it is both
//     listed by the prepare step and old enough for the job, so the effective protection is the
//     larger of the two; an installation must set this grace >= that setting for the server-side
//     knob to be the binding one. This function cannot enforce the job's half.
//   - Clock skew: a future lastModified (a client or storage layer with a clock ahead of the
//     server) is protected rather than collected, so skew never deletes, only delays.
//
// The predicate is exported because pkg/catalog cannot import this package (this package imports
// pkg/catalog, so the dependency back would be a cycle). PrepareGCUncommitted reaches it through
// catalog.SetUncommittedGCProtector, a narrow extension point of pkg/catalog's prepare step
// (pkg/catalog/gc_write_uncommitted.go), which Bootstrap (bootstrap.go) installs at startup with
// UncommittedGCGraceProtector. The prepare step applies it as one guarded `continue` in
// processBranch, immediately after the address has been normalized and before the row is written,
// so a protected object never reaches the parquet report the GC job deletes from.
func UncommittedGCProtected(key string, lastModified, now time.Time, grace time.Duration) bool {
	if !IsIcebergDataKey(key) {
		return false
	}
	if grace <= UncommittedGCGraceProtectAll {
		// 0 (or a rejected negative) means "protect every table file, however old".
		return true
	}
	if lastModified.IsZero() {
		// Unknown age: the report's creation_date would be the zero time, which every age check
		// considers ancient. Protect.
		return true
	}
	age := now.Sub(lastModified)
	if age < 0 {
		// lastModified in the future - clock skew, not an ancient object. Protect.
		return true
	}
	return age < grace
}

// IsIcebergDataKey reports whether key - an object key relative to the repository's storage
// namespace - is inside the physical table-file area <storage_namespace>/_iceberg/, or is the area
// marker itself.
//
// The check is a plain prefix test on the *first* path segment: "_iceberg/" or exactly "_iceberg".
// A key such as "data/_iceberg/x" is not in the area (the area is at the namespace root), and
// "_icebergx/data" is a different prefix, so neither is protected - over-protecting an unrelated
// prefix would make ordinary data uncollectable in a repository that happens to use the name.
func IsIcebergDataKey(key string) bool {
	key = strings.TrimPrefix(key, "/")
	if key == DataPrefix {
		return true
	}
	return strings.HasPrefix(key, DataPrefix+"/")
}

// UncommittedGCGraceProtector binds UncommittedGCProtected to one grace window, in the shape
// pkg/catalog's prepare step takes (catalog.UncommittedGCProtector).
//
// The function value it returns has no dependency on this package's configuration: Bootstrap reads
// iceberg.uncommitted_gc_grace once at startup and installs the result, so a prepare run never
// reads configuration. Passing the value returned here to catalog.SetUncommittedGCProtector is the
// whole hook of §6.3's grace; with no protector installed (the plain lakefs binary, or a disabled
// catalog) every staged object keeps being reported, exactly as before.
func UncommittedGCGraceProtector(grace time.Duration) func(key string, lastModified, now time.Time) bool {
	return func(key string, lastModified, now time.Time) bool {
		return UncommittedGCProtected(key, lastModified, now, grace)
	}
}

// ReservedPrefixProtectPolicyName is the display name of the installation-wide deny policy that
// reserves _lakefs_iceberg/ against the fs: object API (§6.1, §12 "security tests: _lakefs_iceberg/
// cannot be written through fs:"). It follows the naming of datasets-pins-protect
// (pkg/datasets/authz.go), the other installation-wide deny of this repository.
const ReservedPrefixProtectPolicyName = "iceberg-reserved-prefix-protect"

// The two resources of the deny, as ARNs. Both are patterns over the reserved key prefix
// _lakefs_iceberg (ReservedPrefix, store.go) in every repository.
const (
	// ReservedPrefixBranchResource is the resource §6.1 spells for the deny: the reserved prefix
	// addressed as a *branch* name. lakeFS never authorizes fs:WriteObject or fs:DeleteObject on
	// a branch ARN (see ReservedPrefixObjectResource), so this statement is inert against the
	// object API. It is kept for two reasons: §6.1 names the branch form, and it is the correct
	// resource for any authorization path that addresses the reserved area through a branch (an
	// embedded service or a future branch-scoped endpoint), where denying the write is the answer
	// such a caller must get. It is not what protects the prefix in this server.
	ReservedPrefixBranchResource = "arn:lakefs:fs:::repository/*/branch/" + ReservedPrefix + "/*"

	// ReservedPrefixObjectResource is the resource that actually protects the prefix, because the
	// object API authorizes fs:WriteObject and fs:DeleteObject on an *object* ARN:
	// permissions.ObjectArn(repo, path) == arn:lakefs:fs:::repository/<repo>/object/<path>
	// (pkg/api/controller.go UploadObject and DeleteObject, pkg/gateway/operations/putobject.go
	// and deleteobject.go). A deny on the branch form above would therefore never match a write
	// through /api/v1/repositories/…/objects or through the S3 gateway, which is the forgery
	// vector §6.1 is about: a principal holding the table actions could write
	// _lakefs_iceberg/tables/<ns>/<table>/.pointer directly and point a table at a foreign
	// location, bypassing the check-and-put of §6.2 and the location check of I3-1.
	//
	// The pattern needs *two* path segments after the prefix ("…/_iceberg/*/*"), and that arity is
	// load-bearing in both directions:
	//
	//   - one segment is not enough on the request side: §7.2 makes every catalog write require
	//     fs:WriteObject on ObjectArn(repo, "_lakefs_iceberg/*") - a *wildcard* resource, because
	//     the catalog cannot know the key of the entry it will stage. A deny on
	//     "…/object/_lakefs_iceberg/*" would match that very request string and, being a deny,
	//     would win over every grant: no principal could create, update or drop a table anymore.
	//     With two segments the pattern does not match the wildcard request (the request has no
	//     "/" after the "*"), so the catalog's own permission request is unaffected.
	//   - two segments is enough on the deny side: every key the catalog writes has at least three
	//     segments after the prefix - _lakefs_iceberg/ns/<ns>/.namespace and
	//     _lakefs_iceberg/tables/<ns>/<table>/.pointer (store.go namespaceMarkerPath /
	//     tablePointerPath) and _lakefs_iceberg/files/<uuid>/<relative path> (§6.3) - and every
	//     such key is matched. gc_test.go pins both directions against auth.ArnMatch and against
	//     the real ACL engine.
	//
	// Residual risk, stated: a principal can still write a single-segment key such as
	// "_lakefs_iceberg/foo", or a *variant* key such as "/_lakefs_iceberg/tables/…" (leading
	// slash) or "_lakefs_iceberg//tables/…", through the object API. None of them forges catalog
	// state, because object keys are opaque strings and the catalog reads the exact keys its own
	// builders produce (store.go namespaceMarkerPath / tablePointerPath), never a normalized
	// form; a variant key is a different object that nothing reads, and the GC collects it. What
	// the deny has to cover is therefore exactly "the keys the catalog writes", and gc_test.go
	// asserts that it covers every one of them.
	ReservedPrefixObjectResource = "arn:lakefs:fs:::repository/*/object/" + ReservedPrefix + "/*/*"
)

// ReservedPrefixProtectDeniedActions is the deny action list of the policy: the two actions that
// can create or remove an object through the fs: API (§6.1). Reads are deliberately allowed: the
// catalog, a diff and a datasets materialisation all read the reserved area through the catalog,
// and an operator inspecting an entry must be able to read it. Branch- and tag-level actions are
// not listed either - they are covered by the datasets-pins-protect and branch-protection denies
// of the core API, and this policy stays the narrow control §6.1 asks for.
var ReservedPrefixProtectDeniedActions = []string{
	permissions.WriteObjectAction,
	permissions.DeleteObjectAction,
}

// ReservedPrefixProtectPolicy builds the installation-wide deny policy that reserves
// _lakefs_iceberg/ against the fs: object API (§6.1, §12).
//
// It is a deny, so it wins over every allow, including the FSFullAccess allow of an
// administrator: only the catalog itself writes the reserved area, and it does that through
// catalog.CreateEntry on the server's own credentials (store.go), not through Authorize. The
// policy is installation-wide (no repository name in it) because the reserved prefix exists in
// every repository that has the catalog enabled, and because a per-repository policy would have to
// be created and repaired for repositories that predate the catalog.
//
// Bootstrap attaches it to the same base groups datasets-pins-protect uses (Admins, Developers,
// SuperUsers plus datasets.immutable_guard_groups; see pkg/datasets PinsProtectGroups), so it is
// not attached here: this file only builds the policy, like pkg/datasets/authz.go builds
// PinsProtectPolicy and leaves the attachment to bootstrap. bootstrap.go owns that attachment
// (EnsureReservedPrefixProtectPolicy), and lakefs-sso registers it with
// cmd.AddExtraRoutesBuilder next to the catalog's own builder.
func ReservedPrefixProtectPolicy(ts time.Time) *model.Policy {
	return &model.Policy{
		CreatedAt:   ts,
		DisplayName: ReservedPrefixProtectPolicyName,
		Statement: model.Statements{
			{
				Effect:   model.StatementEffectDeny,
				Action:   append([]string(nil), ReservedPrefixProtectDeniedActions...),
				Resource: ReservedPrefixBranchResource,
			},
			{
				Effect:   model.StatementEffectDeny,
				Action:   append([]string(nil), ReservedPrefixProtectDeniedActions...),
				Resource: ReservedPrefixObjectResource,
			},
		},
	}
}
