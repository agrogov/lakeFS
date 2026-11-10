package icebergcatalog

// bootstrap.go wires the GC-safety controls of gc.go and the merge-conflict hook of commit.go into
// a running lakefs-sso server (ICEBERG_CATALOG_PLAN.md §6.1, §6.3, §6.4):
//
//  1. the uncommitted-GC grace, installed into pkg/catalog's prepare step with
//     catalog.SetUncommittedGCProtector,
//  2. the installation-wide iceberg-reserved-prefix-protect deny policy, created and attached to
//     the same non-service groups datasets-pins-protect covers, and
//  3. the table-pointer conflict resolver of §6.4, installed with
//     catalog.SetMergeConflictResolver so that a merge auto-resolves a pointer conflict caused only
//     by compactions while iceberg.ignore_compaction_commits is true.
//
// and, with Metadata Search enabled, one seam that is not a GC control but a consequence of the
// companions the catalog serves:
//
//  4. the repository-deletion cascade of REPO_DELETE_STORAGE_DESIGN.md §6 (R17), installed with
//     catalog.SetDeletionCascade so that deleting a data repository's storage also deletes
//     R-metadata - but only when R-metadata passes the init-commit proof, i.e. only when it really
//     is the indexer's companion.
//
// It lives in its own file because it is not part of the HTTP surface http.go mounts: the deny has
// to exist before the first request can be served, whether or not a request ever arrives, the
// grace has to be in place before the first GC prepare run, and the merge hook has to be installed
// before the first merge - and, being a process-wide hook, it must be cleared again (not left
// installed but inert) when the setting that enables it is off. All of it is installed from one
// extra routes builder, so the lakefs-sso diff is one registration and pkg/datasets, pkg/catalog and
// pkg/icebergcatalog/http.go stay untouched.
//
// Everything here is idempotent, so it runs on every startup:
//
//   - the policy is created when absent, rewritten when its statements differ from
//     ReservedPrefixProtectPolicy (so a hand-edited protected-prefix deny is repaired instead of
//     trusted), and only then attached;
//   - an attachment that already exists is treated as success, and a group this installation's
//     group template does not have is skipped rather than fatal, exactly as pkg/datasets.Bootstrap
//     does for datasets-pins-protect (no installation runs both templates, so most of the base
//     group names are expected to be absent);
//   - installing the protector, or the merge hook, twice is a no-op.
//
// The deny policy is hardening, not a precondition: an auth service with no policy backend at all
// (auth.BasicAuthService, the default mode of both binaries, answers every policy call with
// auth.ErrNotImplemented) cannot hold a policy, and a startup that refused to run without one would
// make iceberg.enabled unusable in that mode (conformance/RESULTS.md D-3). Such a service is logged
// as a warning naming what is no longer enforced, and Bootstrap continues; every other error -
// a real failure on a backend that does store policies - still fails startup.
//
// Clean-room note (plan §2, DATASETS_DESIGN.md §2): written from the Apache Iceberg specs and the
// public lakeFS documentation only.

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"time"

	cmdpkg "github.com/treeverse/lakefs/cmd/lakefs/cmd"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/logging"
)

// ErrMissingDeps (http.go) is returned while the catalog is enabled and the builder was handed no
// auth service or logger: the deny policy cannot be attached without one, and a startup that
// silently skipped it would leave the reserved prefix writable. It also covers a missing catalog or
// block adapter, which the §6.4 merge hook reads metadata.json through.

// Bootstrap installs the GC controls of §6.1 and §6.3 and the merge hook of §6.4. It is a startup
// step of the lakefs-sso binary, registered as an extra routes builder after the catalog's own
// builder (the builder adapter, which supplies the unused *chi.Mux, is the caller's); it mounts no
// route.
//
// extraGroups are the additional groups that must carry the deny policy, i.e.
// datasets.immutable_guard_groups. They are passed in rather than read here because the datasets
// control plane is built on this package's layout, not the other way round (pkg/datasets
// materialize.go reads the keys from pkg/iceberglayout, the leaf package the two share), so this
// package does not reach into pkg/datasets for its group list; the caller adds them to the base
// groups of both group templates.
//
// While iceberg.enabled is false it installs nothing: a disabled catalog writes neither
// _lakefs_iceberg/ entries nor _iceberg/ objects, so no policy has to be created, the uncommitted GC
// keeps its defaults for every repository, and no merge hook can change how a conflict is reported.
// The one thing it does do is clear the merge hook, because that hook is a process-wide pointer and
// a disabled catalog must not leave one behind. The same holds for the documented opt-out: a catalog
// with iceberg.ignore_compaction_commits false clears the hook, so a compaction conflict is reported
// as an ordinary one. A configuration error is returned - and so made fatal by the hook loop in
// run.go - for the same reason RoutesBuilder fails on one: an installation that meant to enable the
// catalog must be told that it did not.
func Bootstrap(ctx context.Context, deps cmdpkg.ServerDeps, extraGroups []string) error {
	cfg, err := LoadConfig()
	if err != nil {
		return err
	}
	if !cfg.Enabled {
		// A process-wide hook, so a disabled catalog has to clear it rather than leave a previous
		// installation's resolver installed.
		installMergeConflictResolver(cfg, deps)
		// The deletion cascade is a process-wide hook for the same reason: Metadata Search requires
		// the catalog, so a disabled catalog has no companion any deletion could cascade into.
		installDeletionCascade(cfg, deps)
		return nil
	}
	if deps.AuthService == nil || deps.Logger == nil || deps.Catalog == nil || deps.BlockAdapter == nil {
		return ErrMissingDeps
	}

	// §6.3: keep fresh, staged client-written table files out of the uncommitted-GC report. The
	// grace is read once here, so a prepare run never reads configuration. It is installed only by
	// this binary, which is what keeps the plain lakefs server — no catalog, no _iceberg/ area —
	// reporting every staged object exactly as before.
	catalog.SetUncommittedGCProtector(UncommittedGCGraceProtector(cfg.UncommittedGCGrace))

	// §6.1: reserve _lakefs_iceberg/ against the fs: object API, for every principal that could
	// otherwise write it. An auth service that cannot manage policies degrades this to a warning
	// (see EnsureReservedPrefixProtectPolicy); anything else it reports is a startup error.
	denyAttached, err := EnsureReservedPrefixProtectPolicy(ctx, deps.AuthService, time.Now(), extraGroups)
	if err != nil {
		return err
	}

	// §6.4: let a merge auto-resolve a table-pointer conflict that only compaction snapshots
	// caused, while iceberg.ignore_compaction_commits is on (the default).
	installMergeConflictResolver(cfg, deps)

	// R17: deleting a data repository's storage also deletes its Metadata Search companion, when
	// Metadata Search is enabled and the companion passes the init-commit proof.
	installDeletionCascade(cfg, deps)

	fields := logging.Fields{
		"uncommitted_gc_grace_s":    int64(cfg.UncommittedGCGrace / time.Second),
		"ignore_compaction_commits": cfg.IgnoresCompactionCommits(),
	}
	if denyAttached {
		fields["policy"] = ReservedPrefixProtectPolicyName
		deps.Logger.WithFields(fields).
			Info("iceberg catalog: reserved-prefix deny attached, uncommitted-GC grace and merge hook installed")
		return nil
	}
	// The warning logReservedPrefixDenyNotAttached wrote says why; this line keeps the startup
	// summary from claiming the deny was attached when it was not.
	deps.Logger.WithFields(fields).
		Info("iceberg catalog: uncommitted-GC grace and merge hook installed; the reserved-prefix deny is not attached")
	return nil
}

// installMergeConflictResolver installs the §6.4 merge-conflict hook, or clears it, according to
// cfg: the hook exists only for an enabled catalog that keeps the default
// iceberg.ignore_compaction_commits (or sets it explicitly to true). The documented opt-out therefore
// does not merely leave the hook inert - the hook is a process-wide pointer, so it is cleared.
//
// It is a function of its own, rather than three lines inside Bootstrap, because the decision is
// testable without a server configuration (TestInstallMergeConflictResolver drives a real merge for
// each case); the store it builds is the one the mount serves requests with, so the hook reads
// metadata.json through the server's own block adapter.
func installMergeConflictResolver(cfg *Config, deps cmdpkg.ServerDeps) {
	if !cfg.Enabled || !cfg.IgnoresCompactionCommits() {
		catalog.SetMergeConflictResolver(nil)
		return
	}
	store := NewStore(deps.Catalog, deps.BlockAdapter, cfg.StoreOptions()...)
	catalog.SetMergeConflictResolver(store.MergeConflictResolver(cfg))
}

// installDeletionCascade installs the Metadata Search deletion cascade of
// REPO_DELETE_STORAGE_DESIGN.md §6, or clears it, according to cfg: the cascade exists only for an
// enabled catalog with iceberg.metadata_search.enabled, because that is the only configuration in
// which the indexer creates companion repositories. Like the merge hook it is a process-wide
// pointer, so it is cleared rather than left installed but inert when the setting is off.
//
// It is a function of its own, rather than three lines inside Bootstrap, because the decision is
// testable without a server configuration: TestInstallDeletionCascade requests a repository deletion
// with the cascade installed and cleared.
func installDeletionCascade(cfg *Config, deps cmdpkg.ServerDeps) {
	if !cfg.MetadataSearchEnabled() || deps.Catalog == nil {
		catalog.SetDeletionCascade(nil)
		return
	}
	deps.Catalog.SetDeletionCascade(MetadataSearchDeletionCascade(deps.Catalog))
}

// MetadataSearchDeletionCascade returns the R17 cascade for a catalog whose Metadata Search
// companions the indexer writes: for a data repository R, the companion is
// MetadataRepository(R.Name) when that repository exists and Catalog.IsMetadataCompanion proves it.
//
// The proof is the listing's proof (pkg/catalog/metadata_companion.go), not a suffix test: a user
// may create foo-metadata themselves, and that repository is the user's own - it is neither hidden
// from a listing nor deleted by a cascade into foo. A repository whose history cannot be read at all
// proves nothing and is left alone for the same reason, exactly as a listing lists it: the cascade's
// one job is to delete derived data, and it must never guess where that data is.
//
// The companion's default branch is the branch the proof walks, and it is read from the companion's
// own record: the indexer creates it with the data repository's default branch, but a record is the
// only place that fact is stored.
func MetadataSearchDeletionCascade(cat *catalog.Catalog) catalog.DeletionCascade {
	return func(ctx context.Context, repository *catalog.Repository) (string, bool, error) {
		companion := MetadataRepository(repository.Name)
		companionRepo, err := cat.GetRepository(ctx, companion)
		switch {
		case errors.Is(err, graveler.ErrNotFound):
			return "", false, nil
		case err != nil:
			// The companion cannot be read, so the request cannot decide whether it has one. Refuse
			// before anything is deleted rather than delete R and leave a proven companion behind.
			return "", false, fmt.Errorf("read repository %q: %w", companion, err)
		}
		proven, err := cat.IsMetadataCompanion(ctx, companion, companionRepo.DefaultBranch)
		if err != nil || !proven {
			// Not the indexer's companion: nothing to cascade into, and nothing to refuse.
			return "", false, nil
		}
		return companion, true, nil
	}
}

// EnsureReservedPrefixProtectPolicy makes the iceberg-reserved-prefix-protect deny policy exist
// with exactly its built-in statements and attaches it to the base groups of both group templates
// plus extraGroups (see pkg/datasets.PinsProtectGroups, the list datasets-pins-protect covers): the
// same principals need guarding against writing a reserved key directly instead of going through
// the service that owns it.
//
// A policy found with different statements is rewritten and logged as a warning: the policy name is
// predictable, so a principal that may write policies but holds no catalog permission could
// otherwise pre-create it - with no statements, or with allows - and every later server would
// inherit it. A group that does not exist in this installation is skipped with a debug log, because
// the group list spans both group templates and only one of them exists per installation.
//
// ts is the creation timestamp recorded when the policy has to be created (the caller passes
// time.Now(), as the datasets bootstrap does). A nil authService is ErrNoAuthorizer (authz.go): the
// deny policy is the whole point of the call, so there is nothing to fall back to.
//
// A service that cannot manage policies at all - auth.BasicAuthService, the default auth mode of
// both lakefs binaries, answers GetPolicy/WritePolicy/AttachPolicyToGroup with auth.ErrNotImplemented
// - is not an error: the deny is a hardening measure, so its absence degrades the deployment (the
// reserved prefix stays writable through fs:) instead of refusing to start it. That case is a
// warning naming the consequence and a nil error; every other error is returned, so a genuine
// failure on a backend that does store policies still fails startup.
//
// The bool is whether the deny is in force: true when the policy was created or found and attached
// to at least the groups this installation has, false exactly in the degraded case above. It exists
// so Bootstrap's startup summary does not report an attachment that did not happen; it is not an
// error channel and never softens the error path.
func EnsureReservedPrefixProtectPolicy(ctx context.Context, authService auth.Service, ts time.Time, extraGroups []string) (bool, error) {
	if authService == nil {
		return false, ErrNoAuthorizer
	}
	policy := ReservedPrefixProtectPolicy(ts)
	existing, err := authService.GetPolicy(ctx, policy.DisplayName)
	switch {
	case err == nil:
		if !reservedPrefixProtectStatementsEqual(existing.Statement, policy.Statement) {
			if err := authService.WritePolicy(ctx, policy, true /* update */); err != nil {
				if policyBackendUnavailable(err) {
					logReservedPrefixDenyNotAttached(ctx, err)
					return false, nil
				}
				return false, fmt.Errorf("icebergcatalog: rewrite policy %s: %w", ReservedPrefixProtectPolicyName, err)
			}
			logging.FromContext(ctx).WithField("policy", ReservedPrefixProtectPolicyName).
				Warn("icebergcatalog: rewrote the reserved-prefix deny policy, whose statements did not match")
		}
	case errors.Is(err, auth.ErrNotFound):
		if err := authService.WritePolicy(ctx, policy, false /* create */); err != nil && !errors.Is(err, auth.ErrAlreadyExists) {
			if policyBackendUnavailable(err) {
				logReservedPrefixDenyNotAttached(ctx, err)
				return false, nil
			}
			return false, fmt.Errorf("icebergcatalog: create policy %s: %w", ReservedPrefixProtectPolicyName, err)
		}
	case policyBackendUnavailable(err):
		// The service has no policy backend, so the deny can be neither created nor attached here.
		// Degrade loudly: see the comment on this function and Bootstrap.
		logReservedPrefixDenyNotAttached(ctx, err)
		return false, nil
	default:
		return false, fmt.Errorf("icebergcatalog: read policy %s: %w", ReservedPrefixProtectPolicyName, err)
	}

	for _, group := range ReservedPrefixProtectGroups(extraGroups) {
		err := authService.AttachPolicyToGroup(ctx, ReservedPrefixProtectPolicyName, group)
		switch {
		case err == nil, errors.Is(err, auth.ErrAlreadyExists):
		case errors.Is(err, auth.ErrNotFound):
			logging.FromContext(ctx).WithField("group", group).
				Debug("icebergcatalog: group is not part of this installation's template; skipping deny attachment")
		case policyBackendUnavailable(err):
			// GetPolicy answered (a stored policy, or none) but the service cannot attach it, so the
			// deny is no more in force than in the case above.
			logReservedPrefixDenyNotAttached(ctx, err)
			return false, nil
		default:
			return false, fmt.Errorf("icebergcatalog: attach policy %s to group %s: %w", ReservedPrefixProtectPolicyName, group, err)
		}
	}
	return true, nil
}

// policyBackendUnavailable reports whether err is the auth service saying it has no policy backend
// at all, rather than that an operation on one that exists failed. auth.BasicAuthService - the
// default auth mode of lakefs and lakefs-sso - is the case this exists for: it answers every policy
// call with auth.ErrNotImplemented (pkg/auth/basic_service.go). errors.Is, not ==, because a
// service is free to wrap the sentinel.
//
// The distinction is what keeps startup honest: a backend that cannot store policies degrades the
// reserved-prefix deny (a warning, D-3 in conformance/RESULTS.md), while a backend that can and
// failed - a KV error, an external auth service answering 5xx - must still stop the server.
func policyBackendUnavailable(err error) bool {
	return errors.Is(err, auth.ErrNotImplemented)
}

// logReservedPrefixDenyNotAttached reports the one degraded startup Bootstrap accepts: the auth
// service cannot manage policies, so iceberg-reserved-prefix-protect was neither created nor
// attached. The message names the consequence rather than the error alone, because an operator who
// reads only this line has to learn that the reserved prefix is not protected in this deployment.
func logReservedPrefixDenyNotAttached(ctx context.Context, err error) {
	logging.FromContext(ctx).WithError(err).WithFields(logging.Fields{
		"policy":          ReservedPrefixProtectPolicyName,
		"reserved_prefix": ReservedPrefix,
	}).Warn("icebergcatalog: the auth service cannot manage policies: the reserved-prefix deny policy is NOT attached, " +
		"so writes to the " + ReservedPrefix + "/ prefix are not blocked at the fs: layer in this deployment")
}

// ReservedPrefixProtectGroups returns the groups the reserved-prefix deny is attached to: the base
// groups of both lakeFS group templates (Admins, Supers, Writers, Readers, SuperUsers, Developers,
// Viewers) plus extra, deduplicated and in a stable order.
//
// The list is spelled out here, and not taken from pkg/datasets, because the dependency runs the
// other way: pkg/datasets materialises catalog workspaces, so pkg/icebergcatalog does not depend on
// the datasets control plane. It is the same non-service group set that
// pkg/datasets.PinsProtectGroups returns, and gc_test.go pins the two to each other as literals so
// they cannot drift apart silently.
func ReservedPrefixProtectGroups(extra []string) []string {
	seen := make(map[string]struct{}, len(reservedPrefixProtectBaseGroups)+len(extra))
	groups := make([]string, 0, len(reservedPrefixProtectBaseGroups)+len(extra))
	for _, group := range slices.Concat(reservedPrefixProtectBaseGroups, extra) {
		if group == "" {
			continue
		}
		if _, ok := seen[group]; ok {
			continue
		}
		seen[group] = struct{}{}
		groups = append(groups, group)
	}
	return groups
}

// reservedPrefixProtectBaseGroups are the groups every installation guards: the base groups of both
// lakeFS group templates, so members of either one are covered. The simplified ACL template ships
// Admins/Supers/Writers/Readers and the RBAC template ships Admins/SuperUsers/Developers/Viewers
// (pkg/auth/setup). Attaching to a group that does not exist fails with auth.ErrNotFound, which
// EnsureReservedPrefixProtectPolicy skips.
var reservedPrefixProtectBaseGroups = []string{
	"Admins",
	"Supers",
	"Writers",
	"Readers",
	"SuperUsers",
	"Developers",
	"Viewers",
}

// reservedPrefixProtectStatementsEqual compares the statements a stored policy carries with the
// built-in ones. It ignores an empty Conditions map versus nil, so a policy that merely round-tripped
// through the KV encoding is not rewritten on every startup.
func reservedPrefixProtectStatementsEqual(a, b model.Statements) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Effect != b[i].Effect ||
			!slices.Equal(a[i].Action, b[i].Action) ||
			a[i].Resource != b[i].Resource ||
			!conditionsEqual(a[i].Condition, b[i].Condition) {
			return false
		}
	}
	return true
}

// conditionsEqual compares two statement conditions, treating an empty map and a nil map as equal.
func conditionsEqual(a, b map[string]map[string][]string) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	return reflect.DeepEqual(a, b)
}
