package icebergcatalog

// bootstrap.go wires the two GC-safety controls of gc.go into a running lakefs-sso server
// (ICEBERG_CATALOG_PLAN.md §6.1, §6.3):
//
//  1. the uncommitted-GC grace, installed into pkg/catalog's prepare step with
//     catalog.SetUncommittedGCProtector, and
//  2. the installation-wide iceberg-reserved-prefix-protect deny policy, created and attached to
//     the same non-service groups datasets-pins-protect covers.
//
// It lives in its own file because it is not part of the HTTP surface http.go mounts: the deny has
// to exist before the first request can be served, whether or not a request ever arrives, and the
// grace has to be in place before the first GC prepare run. Both are installed from one extra
// routes builder, so the lakefs-sso diff is one registration and pkg/datasets, pkg/catalog and
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
//   - installing the protector twice is a no-op.
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
	"github.com/treeverse/lakefs/pkg/logging"
)

// ErrMissingDeps (http.go) is returned while the catalog is enabled and the builder was handed no
// auth service or logger: the deny policy cannot be attached without one, and a startup that
// silently skipped it would leave the reserved prefix writable.

// Bootstrap installs the GC controls of §6.1 and §6.3. It is a startup step of the lakefs-sso
// binary, registered as an extra routes builder after the catalog's own builder (the builder
// adapter, which supplies the unused *chi.Mux, is the caller's); it mounts no route.
//
// extraGroups are the additional groups that must carry the deny policy, i.e.
// datasets.immutable_guard_groups. They are passed in rather than read here because the datasets
// control plane is built on this package's layout, not the other way round (pkg/datasets
// materialize.go reads the keys from pkg/iceberglayout, the leaf package the two share), so this
// package does not reach into pkg/datasets for its group list; the caller adds them to the base
// groups of both group templates.
//
// While iceberg.enabled is false it does nothing at all: a disabled catalog writes neither
// _lakefs_iceberg/ entries nor _iceberg/ objects, so no policy has to be created and the
// uncommitted GC keeps its defaults for every repository. A configuration error is returned - and
// so made fatal by the hook loop in run.go - for the same reason RoutesBuilder fails on one: an
// installation that meant to enable the catalog must be told that it did not.
func Bootstrap(ctx context.Context, deps cmdpkg.ServerDeps, extraGroups []string) error {
	cfg, err := LoadConfig()
	if err != nil {
		return err
	}
	if !cfg.Enabled {
		return nil
	}
	if deps.AuthService == nil || deps.Logger == nil {
		return ErrMissingDeps
	}

	// §6.3: keep fresh, staged client-written table files out of the uncommitted-GC report. The
	// grace is read once here, so a prepare run never reads configuration. It is installed only by
	// this binary, which is what keeps the plain lakefs server — no catalog, no _iceberg/ area —
	// reporting every staged object exactly as before.
	catalog.SetUncommittedGCProtector(UncommittedGCGraceProtector(cfg.UncommittedGCGrace))

	// §6.1: reserve _lakefs_iceberg/ against the fs: object API, for every principal that could
	// otherwise write it.
	if err := EnsureReservedPrefixProtectPolicy(ctx, deps.AuthService, time.Now(), extraGroups); err != nil {
		return err
	}
	deps.Logger.WithFields(logging.Fields{
		"policy":                 ReservedPrefixProtectPolicyName,
		"uncommitted_gc_grace_s": int64(cfg.UncommittedGCGrace / time.Second),
	}).Info("iceberg catalog: reserved-prefix deny attached and uncommitted-GC grace installed")
	return nil
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
func EnsureReservedPrefixProtectPolicy(ctx context.Context, authService auth.Service, ts time.Time, extraGroups []string) error {
	if authService == nil {
		return ErrNoAuthorizer
	}
	policy := ReservedPrefixProtectPolicy(ts)
	existing, err := authService.GetPolicy(ctx, policy.DisplayName)
	switch {
	case err == nil:
		if !reservedPrefixProtectStatementsEqual(existing.Statement, policy.Statement) {
			if err := authService.WritePolicy(ctx, policy, true /* update */); err != nil {
				return fmt.Errorf("icebergcatalog: rewrite policy %s: %w", ReservedPrefixProtectPolicyName, err)
			}
			logging.FromContext(ctx).WithField("policy", ReservedPrefixProtectPolicyName).
				Warn("icebergcatalog: rewrote the reserved-prefix deny policy, whose statements did not match")
		}
	case errors.Is(err, auth.ErrNotFound):
		if err := authService.WritePolicy(ctx, policy, false /* create */); err != nil && !errors.Is(err, auth.ErrAlreadyExists) {
			return fmt.Errorf("icebergcatalog: create policy %s: %w", ReservedPrefixProtectPolicyName, err)
		}
	default:
		return fmt.Errorf("icebergcatalog: read policy %s: %w", ReservedPrefixProtectPolicyName, err)
	}

	for _, group := range ReservedPrefixProtectGroups(extraGroups) {
		err := authService.AttachPolicyToGroup(ctx, ReservedPrefixProtectPolicyName, group)
		switch {
		case err == nil, errors.Is(err, auth.ErrAlreadyExists):
		case errors.Is(err, auth.ErrNotFound):
			logging.FromContext(ctx).WithField("group", group).
				Debug("icebergcatalog: group is not part of this installation's template; skipping deny attachment")
		default:
			return fmt.Errorf("icebergcatalog: attach policy %s to group %s: %w", ReservedPrefixProtectPolicyName, group, err)
		}
	}
	return nil
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
