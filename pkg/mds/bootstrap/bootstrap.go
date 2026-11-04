// Package bootstrap creates and maintains the Metadata Search principals and policies of
// METADATA_SEARCH_DESIGN.md §8 on a lakeFS installation: the mds-indexer user, the
// MetadataSearchIndexers group, the mds-indexer allow policy and the mds-metadata-repo-protect
// deny policy. It runs as an extra routes builder of the lakefs-sso binary, after the catalog's
// own builders, and mounts no route: the principals have to exist before the first indexer request
// is authorized, whether or not a request ever arrives.
//
// The split of §8 is deliberate. Indexing writes, so mds-indexer gets write (fs:*) on the
// companion repositories R-metadata only, plus the read actions it needs to read a data repository
// R. Querying goes through the catalog and stays a normal lakeFS grant on R (§5.2), so the indexer
// neither queries nor is granted the data repositories' write paths. The companion repositories in
// turn are readable but not writable by ordinary principals: mds-metadata-repo-protect is a deny,
// so it wins over an administrator's fs:* allow, and only the catalog and the indexer
// (which are not in a group it can match) write there. Attaching it to
// MetadataSearchIndexers would disarm the indexer, which is why that group is not in the group
// list and is asserted to stay policy-free in the tests.
//
// Everything here is idempotent, so it runs on every startup:
//
//   - the user, the group and the group membership are created when absent, and
//     auth.ErrAlreadyExists is treated as success (so a rerun neither duplicates nor fails);
//   - a policy is created when absent and rewritten when its statements differ from the built-in
//     ones (a hand-edited or pre-created "mds-indexer" is repaired instead of trusted - the names
//     are predictable, so a principal allowed to write policies could otherwise seed a policy with
//     no statements or with allows), and a rewrite is logged as a warning;
//   - the attachments tolerate auth.ErrAlreadyExists, and a group of the deny's list that this
//     installation's group template does not have is skipped with a debug log, exactly as
//     pkg/datasets.Bootstrap treats datasets-pins-protect (no installation runs both templates).
//
// The deny policy is hardening, not a precondition. auth.BasicAuthService - the default auth mode
// of both lakefs binaries - answers every policy call with auth.ErrNotImplemented, and a startup
// that refused to run without a deny would make iceberg.metadata_search.enabled unusable in that
// mode. Such a service is logged as a warning naming what is not enforced, and Bootstrap continues;
// every other error - a real failure on a backend that does store policies - still fails startup,
// exactly like icebergcatalog.EnsureReservedPrefixProtectPolicy (pkg/icebergcatalog/bootstrap.go).
// The policy backend is probed before the user is created, because BasicAuthService *does*
// implement CreateUser but stores the single administrator user under a fixed key: creating
// mds-indexer there would rename the installation's administrator, and the startup would then fail
// at the first policy call.
//
// Clean-room note (design §2): written from the public lakeFS documentation and this repository's
// code only; no lakeFS Enterprise/Cloud code was used.
package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"time"

	"github.com/go-chi/chi/v5"
	cmdpkg "github.com/treeverse/lakefs/cmd/lakefs/cmd"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/datasets"
	"github.com/treeverse/lakefs/pkg/icebergcatalog"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/permissions"
)

// The principal names of §8. They are exported because an administrator has to name them by hand:
// credentials are created by the operator (lakectl auth users credentials create --id mds-indexer)
// and placed in the indexer configuration, and the policies are inspected and mirrored in an
// external auth service.
const (
	// UserName is the indexer principal. The design records it as "mds-indexer".
	UserName = "mds-indexer"

	// GroupName is the group the indexer user is a member of, and the only principal the mds-indexer
	// policy is attached to. The deny policy is deliberately never attached to it.
	GroupName = "MetadataSearchIndexers"

	// IndexerPolicyName is the allow policy of the indexer: read on every data repository, write on
	// the companion repositories. It is also the policy the bootstrap rewrites when it drifts.
	IndexerPolicyName = "mds-indexer"

	// MetadataRepoProtectPolicyName is the installation-wide deny policy that keeps ordinary
	// principals from writing a companion repository while the indexer owns it (§3.3, §8).
	MetadataRepoProtectPolicyName = "mds-metadata-repo-protect"
)

// The resource patterns of §8, spelled from the ARN builders of pkg/permissions so that a repo
// name, a branch name and a table address all have the shape the server authorizes on:
//
//	arn:lakefs:fs:::repository/<repo>                    fs:ReadRepository, fs:CreateRepository, …
//	arn:lakefs:fs:::repository/<repo>/branch/<branch>    fs:CreateCommit, fs:CreateBranch, …
//	arn:lakefs:fs:::repository/<repo>/object/<key>       fs:WriteObject, fs:DeleteObject, …
//	arn:lakefs:catalog:::<kind>/<repo>/<ns>/<table>      catalog:CreateTable, catalog:ReadTable, …
const (
	// MetadataRepositoryPattern matches the repository ARN of every companion repository R-metadata.
	// It needs two patterns, not one, because lakeFS ARN matching compares a statement's resource with
	// the exact request string: a statement on the repository ARN does not cover the branch, tag and
	// object ARNs below it. The trailing "*" of the old form would also have matched a data
	// repository named "-metadatax", which this pair deliberately does not.
	MetadataRepositoryPattern = "arn:lakefs:fs:::repository/*" + icebergcatalog.MetadataRepositorySuffix

	// MetadataRepositorySubtreePattern matches every ARN below a companion repository: its branches,
	// tags and objects. "*" spans "/" in the policy wildcard, so this one pattern covers all of them.
	MetadataRepositorySubtreePattern = MetadataRepositoryPattern + "/*"

	// MetadataCatalogPattern matches every catalog ARN of a companion repository, e.g.
	// arn:lakefs:catalog:::table/<R>-metadata/system/object_metadata (§8).
	MetadataCatalogPattern = "arn:lakefs:catalog:::*/*" + icebergcatalog.MetadataRepositorySuffix + "/*"
)

// indexerReadActions are the data-repository permissions of §8: what the indexer needs to list a
// data repository's branches and objects and to read one commit's diff. They are reads only: the
// indexer never writes a data repository, which is what keeps a bug in it from corrupting the data
// it is describing.
var indexerReadActions = []string{
	permissions.ReadRepositoryAction,
	permissions.ListObjectsAction,
	permissions.ReadObjectAction,
	permissions.ListBranchesAction,
	permissions.ReadBranchAction,
	permissions.ListCommitsAction,
	permissions.ReadCommitAction,
}

// metadataRepoProtectFSDeniedActions are the fs: writes §8 denies on a companion repository: the
// object and branch actions that could move an indexer's table, and the repository action that
// could remove it. They are wildcards so that a future fs: write action is covered without a
// bootstrap change; the deny is on the companion ARNs only, so ordinary data repositories are
// unaffected.
var metadataRepoProtectFSDeniedActions = []string{
	"fs:Write*",
	"fs:Delete*",
	"fs:Create*",
	"fs:Revert*",
}

// metadataRepoProtectCatalogDeniedActions are the catalog: writes §8 denies on a companion
// repository. catalog:WriteTableData is listed separately because it is the data-plane action of a
// table, not one of the catalog:Create*/Update*/Delete* metadata actions of §7.2.
var metadataRepoProtectCatalogDeniedActions = []string{
	"catalog:Create*",
	"catalog:Update*",
	"catalog:Delete*",
	permissions.CatalogWriteTableDataAction,
}

// ErrNoAuthorizer is returned by EnsurePrincipals when it is handed no auth service: the whole
// point of the call is to create and attach policies, so there is nothing to fall back to. It is a
// programming error, like icebergcatalog.ErrNoAuthorizer.
var ErrNoAuthorizer = errors.New("mds bootstrap: no auth service configured")

// ErrMissingDeps is returned by RoutesBuilder while iceberg.metadata_search.enabled is true and the
// server passed no auth service or logger: the principals cannot be created without a service, and
// a startup that silently skipped them would leave the indexer unusable. The catalog's own builder
// (pkg/icebergcatalog) fails the same way for the same reason.
var ErrMissingDeps = errors.New("mds bootstrap: metadata search is enabled and the server passed no auth service or logger")

// IndexerPolicy builds the mds-indexer allow policy of §8:
//
//	on *                    fs:ReadRepository, fs:ListObjects, fs:ReadObject, fs:ListBranches,
//	                        fs:ReadBranch, fs:ListCommits, fs:ReadCommit
//	on repository/*-metadata and below   fs:*
//	on catalog:::* / *-metadata/*        catalog:*
//	on repository/*-metadata             fs:CreateRepository
//
// The last statement is subsumed by the fs:* statement above it; it is spelled out because §8
// names fs:CreateRepository as a separate grant, and keeping it visible makes the policy a reader
// can check against the design line by line.
func IndexerPolicy(ts time.Time) *model.Policy {
	return &model.Policy{
		CreatedAt:   ts,
		DisplayName: IndexerPolicyName,
		Statement: model.Statements{
			{
				Effect:   model.StatementEffectAllow,
				Action:   append([]string(nil), indexerReadActions...),
				Resource: permissions.All,
			},
			{
				Effect:   model.StatementEffectAllow,
				Action:   []string{"fs:*"},
				Resource: MetadataRepositoryPattern,
			},
			{
				Effect:   model.StatementEffectAllow,
				Action:   []string{"fs:*"},
				Resource: MetadataRepositorySubtreePattern,
			},
			{
				Effect:   model.StatementEffectAllow,
				Action:   []string{"catalog:*"},
				Resource: MetadataCatalogPattern,
			},
			{
				Effect:   model.StatementEffectAllow,
				Action:   []string{permissions.CreateRepositoryAction},
				Resource: MetadataRepositoryPattern,
			},
		},
	}
}

// MetadataRepoProtectPolicy builds the mds-metadata-repo-protect deny policy of §8: no principal
// in a base group may write, delete, create or revert inside a companion repository, on the fs: or
// on the catalog: API.
//
// It is a deny, so it wins over every allow, including the fs:* allow of an administrator: the
// indexer and the catalog write there, not an operator, and a hand-edit of a served table would be
// silently overwritten by the next tick anyway. It is installation-wide (no repository name in it)
// because every indexed data repository has a companion repository, including the ones that predate
// Metadata Search; the *-metadata suffix is the naming convention the catalog and the indexer share
// (icebergcatalog.MetadataRepositorySuffix), so one pattern covers them all.
func MetadataRepoProtectPolicy(ts time.Time) *model.Policy {
	return &model.Policy{
		CreatedAt:   ts,
		DisplayName: MetadataRepoProtectPolicyName,
		Statement: model.Statements{
			{
				Effect:   model.StatementEffectDeny,
				Action:   append([]string(nil), metadataRepoProtectFSDeniedActions...),
				Resource: MetadataRepositoryPattern,
			},
			{
				Effect:   model.StatementEffectDeny,
				Action:   append([]string(nil), metadataRepoProtectFSDeniedActions...),
				Resource: MetadataRepositorySubtreePattern,
			},
			{
				Effect:   model.StatementEffectDeny,
				Action:   append([]string(nil), metadataRepoProtectCatalogDeniedActions...),
				Resource: MetadataCatalogPattern,
			},
		},
	}
}

// RoutesBuilder is the extra routes builder cmd/lakefs-sso registers with
// cmd.AddExtraRoutesBuilder. It mounts no route; it exists so the principals of §8 are installed
// from the same hook loop, and before the first request is served, as the catalog's own controls.
//
// While iceberg.metadata_search.enabled is false it does nothing at all: the catalog does not
// resolve the virtual table, so no indexer can read a data repository through it and no companion
// repository is written. A configuration error is returned - and so made fatal by the hook loop in
// run.go - for the same reason the catalog's builder fails on one: an installation that meant to
// enable Metadata Search must be told that it did not.
func RoutesBuilder(ctx context.Context, deps cmdpkg.ServerDeps, _ *chi.Mux) error {
	cfg, err := icebergcatalog.LoadConfig()
	if err != nil {
		return err
	}
	if !cfg.MetadataSearchEnabled() {
		return nil
	}
	if deps.AuthService == nil || deps.Logger == nil {
		return ErrMissingDeps
	}

	inForce, err := EnsurePrincipals(ctx, deps.AuthService, time.Now())
	if err != nil {
		return err
	}
	fields := logging.Fields{
		"user":     UserName,
		"group":    GroupName,
		"policies": []string{IndexerPolicyName, MetadataRepoProtectPolicyName},
	}
	if !inForce {
		// The warning EnsurePrincipals wrote says why; this line keeps the startup summary from
		// claiming the principals exist when they do not.
		deps.Logger.WithFields(fields).
			Info("metadata search: mds-indexer principals and policies are NOT in place; see the warning")
		return nil
	}
	deps.Logger.WithFields(fields).
		Info("metadata search: mds-indexer user, group and policies are in place")
	return nil
}

// EnsurePrincipals makes the principals and policies of §8 exist, with exactly their built-in
// statements, and attaches them:
//
//	user   mds-indexer             member of MetadataSearchIndexers
//	group  MetadataSearchIndexers  carries mds-indexer, never mds-metadata-repo-protect
//	policy mds-indexer             allow, attached to MetadataSearchIndexers
//	policy mds-metadata-repo-protect  deny, attached to datasets.PinsProtectGroups(nil)
//
// ts is the creation timestamp recorded on entities that have to be created (the caller passes
// time.Now(), as the datasets and catalog bootstraps do).
//
// The bool reports whether the principals are in force: true when the policies were created or
// found and attached, false exactly in the degraded case below. It is not an error channel; it
// exists so the startup summary does not report an attachment that did not happen.
//
// A service that cannot manage policies at all - auth.BasicAuthService answers every policy call
// with auth.ErrNotImplemented - is not an error: the deny is a hardening measure, so its absence
// degrades the deployment (a principal with fs:* can write a companion repository) instead of
// refusing to start it. That case is a warning naming the consequence and a nil error; every other
// error is returned, so a genuine failure on a backend that does store policies still fails
// startup. See icebergcatalog.EnsureReservedPrefixProtectPolicy for the same contract.
func EnsurePrincipals(ctx context.Context, authService auth.Service, ts time.Time) (bool, error) {
	if authService == nil {
		return false, ErrNoAuthorizer
	}

	// Probe the policy backend before writing anything: BasicAuthService implements CreateUser, but
	// it stores the installation's single administrator under a fixed key, so creating mds-indexer
	// on it would rename that administrator and then fail at the first policy call. Probing here
	// also means a degraded installation is left exactly as it was.
	available, err := policyBackendAvailable(ctx, authService)
	if err != nil || !available {
		return false, err
	}

	if _, err := authService.CreateUser(ctx, &model.User{Username: UserName, CreatedAt: ts}); err != nil &&
		!errors.Is(err, auth.ErrAlreadyExists) {
		return false, fmt.Errorf("mds bootstrap: create user %s: %w", UserName, err)
	}
	if _, err := authService.CreateGroup(ctx, &model.Group{DisplayName: GroupName, CreatedAt: ts}); err != nil &&
		!errors.Is(err, auth.ErrAlreadyExists) {
		return false, fmt.Errorf("mds bootstrap: create group %s: %w", GroupName, err)
	}
	if err := authService.AddUserToGroup(ctx, UserName, GroupName); err != nil &&
		!errors.Is(err, auth.ErrAlreadyExists) {
		return false, fmt.Errorf("mds bootstrap: add user %s to group %s: %w", UserName, GroupName, err)
	}

	// Create or repair both policies before attaching them, so an attachment never points at a
	// policy that does not exist yet.
	for _, policy := range []*model.Policy{IndexerPolicy(ts), MetadataRepoProtectPolicy(ts)} {
		inForce, err := ensurePolicy(ctx, authService, policy)
		if err != nil || !inForce {
			return false, err
		}
	}

	// The indexer policy goes to its own group only: attaching it to a base group would grant every
	// member of that group the companion repositories' write path.
	if err := authService.AttachPolicyToGroup(ctx, IndexerPolicyName, GroupName); err != nil && !errors.Is(err, auth.ErrAlreadyExists) {
		if policyBackendUnavailable(err) {
			logPrincipalsNotCreated(ctx, err)
			return false, nil
		}
		return false, fmt.Errorf("mds bootstrap: attach policy %s to group %s: %w", IndexerPolicyName, GroupName, err)
	}

	// The deny goes to the non-service groups of both group templates, the same list
	// datasets-pins-protect and iceberg-reserved-prefix-protect use (§8). MetadataSearchIndexers is
	// deliberately not in that list, and is not attached here: the indexer is the one principal that
	// must be able to write a companion repository.
	for _, group := range datasets.PinsProtectGroups(nil) {
		err := authService.AttachPolicyToGroup(ctx, MetadataRepoProtectPolicyName, group)
		switch {
		case err == nil, errors.Is(err, auth.ErrAlreadyExists):
		case errors.Is(err, auth.ErrNotFound):
			logging.FromContext(ctx).WithField("group", group).
				Debug("mds bootstrap: group is not part of this installation's template; skipping metadata-repo deny attachment")
		case policyBackendUnavailable(err):
			logPrincipalsNotCreated(ctx, err)
			return false, nil
		default:
			return false, fmt.Errorf("mds bootstrap: attach policy %s to group %s: %w",
				MetadataRepoProtectPolicyName, group, err)
		}
	}
	return true, nil
}

// policyBackendAvailable reports whether authService can manage policies at all, i.e. whether
// reading one of the policies of §8 answers with a policy or with auth.ErrNotFound rather than with
// auth.ErrNotImplemented. A service that cannot is logged as the one degraded startup
// EnsurePrincipals accepts, and the bool is false with a nil error.
func policyBackendAvailable(ctx context.Context, authService auth.Service) (bool, error) {
	_, err := authService.GetPolicy(ctx, IndexerPolicyName)
	switch {
	case err == nil, errors.Is(err, auth.ErrNotFound):
		return true, nil
	case policyBackendUnavailable(err):
		logPrincipalsNotCreated(ctx, err)
		return false, nil
	default:
		return false, fmt.Errorf("mds bootstrap: read policy %s: %w", IndexerPolicyName, err)
	}
}

// ensurePolicy makes policy exist with exactly its built-in statements: it creates it when it is
// missing and rewrites it when the statements differ, and reports whether it is in force.
//
// A rewrite is required rather than optional (§8): the names are predictable, so a principal that
// may write policies but holds no Metadata Search permission could pre-create "mds-indexer" with
// no statements or with allows on "*", and every later indexer would inherit it. A policy whose
// statements differ is logged as a warning and replaced; the reconciler runs on every startup, so a
// hand-edited policy is repaired rather than trusted.
//
// A service that lost its policy backend between the probe and this call degrades to the same
// warning as the probe (returning false with a nil error); every other error is returned.
func ensurePolicy(ctx context.Context, authService auth.Service, policy *model.Policy) (bool, error) {
	existing, err := authService.GetPolicy(ctx, policy.DisplayName)
	switch {
	case err == nil:
		if statementsEqual(existing.Statement, policy.Statement) {
			return true, nil
		}
		if err := authService.WritePolicy(ctx, policy, true /* update */); err != nil {
			if policyBackendUnavailable(err) {
				logPrincipalsNotCreated(ctx, err)
				return false, nil
			}
			return false, fmt.Errorf("mds bootstrap: rewrite policy %s: %w", policy.DisplayName, err)
		}
		logging.FromContext(ctx).WithField("policy", policy.DisplayName).
			Warn("mds bootstrap: rewrote a metadata search policy whose statements did not match")
		return true, nil
	case errors.Is(err, auth.ErrNotFound):
		if err := authService.WritePolicy(ctx, policy, false /* create */); err != nil && !errors.Is(err, auth.ErrAlreadyExists) {
			if policyBackendUnavailable(err) {
				logPrincipalsNotCreated(ctx, err)
				return false, nil
			}
			return false, fmt.Errorf("mds bootstrap: create policy %s: %w", policy.DisplayName, err)
		}
		return true, nil
	case policyBackendUnavailable(err):
		logPrincipalsNotCreated(ctx, err)
		return false, nil
	default:
		return false, fmt.Errorf("mds bootstrap: read policy %s: %w", policy.DisplayName, err)
	}
}

// policyBackendUnavailable reports whether err is the auth service saying it has no policy backend
// at all, rather than that an operation on one that exists failed. auth.BasicAuthService - the
// default auth mode of lakefs and lakefs-sso - is the case this exists for: it answers every policy
// call with auth.ErrNotImplemented (pkg/auth/basic_service.go). errors.Is, not ==, because a
// service is free to wrap the sentinel.
//
// The distinction is what keeps startup honest: a backend that cannot store policies degrades
// Metadata Search's principals (a warning), while a backend that can and failed - a KV error, an
// external auth service answering 5xx - must still stop the server.
func policyBackendUnavailable(err error) bool {
	return errors.Is(err, auth.ErrNotImplemented)
}

// logPrincipalsNotCreated reports the one degraded startup EnsurePrincipals accepts: the auth
// service cannot manage policies, so the mds-indexer user, its group and both policies were not
// created or attached. The message names the consequence rather than the error alone, because an
// operator who reads only this line has to learn that the indexer cannot be authorized and that
// companion repositories are not write-protected in this deployment.
func logPrincipalsNotCreated(ctx context.Context, err error) {
	logging.FromContext(ctx).WithError(err).WithFields(logging.Fields{
		"user":            UserName,
		"group":           GroupName,
		"policies":        []string{IndexerPolicyName, MetadataRepoProtectPolicyName},
		"metadata_suffix": icebergcatalog.MetadataRepositorySuffix,
	}).Warn("mds bootstrap: the auth service cannot manage policies: the " + UserName + " user, the " +
		GroupName + " group and the " + IndexerPolicyName + " and " + MetadataRepoProtectPolicyName +
		" policies are NOT created, so metadata search indexing is not authorized and " +
		icebergcatalog.MetadataRepositorySuffix + " repositories are not write-protected in this deployment")
}

// statementsEqual compares the statements a stored policy carries with the built-in ones. It
// ignores an empty Conditions map versus nil, so a policy that merely round-tripped through the KV
// encoding is not rewritten on every startup.
func statementsEqual(a, b model.Statements) bool {
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
