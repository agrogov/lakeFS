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
// MetadataSearchIndexers would disarm the indexer, which is why the bootstrap drops that group from
// any group list it is handed (datasets.immutable_guard_groups is operator-controlled) and the
// tests assert it stays policy-free.
//
// The deny's group list is not computed here. It is the caller's, and cmd/lakefs-sso passes the same
// guardGroups() it gives icebergcatalog.Bootstrap - the base groups of both group templates plus
// datasets.immutable_guard_groups - so both installation-wide denies cover exactly the same
// principals (§8). Taking the list from the caller is what keeps a configured extra guard group
// from getting the reserved-prefix deny but not this one.
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
// The two policies name the companion repositories explicitly instead of matching them with a
// "*-metadata" wildcard. The wildcard could not express "the repository name ends in -metadata":
// pkg/auth/wildcard.Match lets "*" span "/", so arn:lakefs:fs:::repository/*-metadata also matched
// arn:lakefs:fs:::repository/<R>/object/-metadata (the star took "<R>/object/"), and
// arn:lakefs:fs:::repository/*-metadata/object/* matched an object of <R> whose key was
// a/b-metadata/object/c. The same held for the catalog ARN pattern, whose star for the kind let
// table/<R>/<namespace ending in -metadata>/<table> match. Both consequences were real: the indexer,
// which §8 limits to reads on a data repository, was granted writes on such objects and tables, and
// the deny made an ordinary foo-metadata/data.yml object of a data repository unwritable for every
// guard-group member. An exact ARN per companion repository has neither problem, so the policies are
// built from the data repositories the server knows (knownDataRepositories) and RoutesBuilder keeps
// them in step with the repository list while the server runs (startPolicyRefresher). The cost of
// that completeness is one refresh interval (policyRefreshInterval): that is the longest a repository
// created after startup - and every repository is covered after a refresh - can be missing from the
// policies. A missing companion repository leaves the indexer without a grant (a loud authorization
// error in /_health, never a silent write) and, for the deny, is the window described in that
// function's comment.
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

	"github.com/spf13/viper"
	cmdpkg "github.com/treeverse/lakefs/cmd/lakefs/cmd"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/icebergcatalog"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/permissions"
	"github.com/treeverse/lakefs/pkg/validator"
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

// The resource ARNs of §8, spelled from the ARN builders of pkg/permissions so that a repo name, a
// branch name and a table address all have the shape the server authorizes on:
//
//	arn:lakefs:fs:::repository/<repo>                    fs:ReadRepository, fs:CreateRepository, …
//	arn:lakefs:fs:::repository/<repo>/branch/<branch>    fs:CreateCommit, fs:CreateBranch, …
//	arn:lakefs:fs:::repository/<repo>/object/<key>       fs:WriteObject, fs:DeleteObject, …
//	arn:lakefs:catalog:::<kind>/<repo>/<ns>/<table>      catalog:CreateTable, catalog:ReadTable, …
//
// They carry the name of a companion repository, never a wildcard for it (see the package comment).
// The trailing "*" of the subtree ARN is safe because the literal repository name in front of it
// already fixes the repository: arn:lakefs:fs:::repository/R-metadata/* cannot match any ARN below
// arn:lakefs:fs:::repository/R, whatever keys or branch names R holds.
const (
	// MetadataNamespacePattern matches the storage-namespace ARN of every companion repository whose
	// namespace is derived by §3.3: <R's namespace>-metadata. CreateRepository checks
	// fs:AttachStorageNamespace on arn:lakefs:fs:::namespace/<namespace>
	// (pkg/permissions.StorageNamespace), and "*" spans "/", so this is the narrowest pattern that
	// covers the derived namespaces. It is a pattern and not an exact ARN on purpose: the companion
	// repository of a repository the server knows may not exist yet, so its derived namespace is not
	// known either. An explicit metadata_search.repositories.<R>.storage_namespace that does not
	// itself end in -metadata is outside this grant and needs an extra statement.
	MetadataNamespacePattern = "arn:lakefs:fs:::namespace/*" + icebergcatalog.MetadataRepositorySuffix
)

// configuredRepositoriesKey is the indexer's repository list of §7. The server reads it in addition
// to iceberg.metadata_search.enabled (which is all §7 says it reads) because it is the only way to
// name the companion repository of a data repository that does not exist yet - the normal case of a
// repository created after this process started, and the one the indexer creates R-metadata for on
// its first tick. Section 7 lets both processes read the same file, which is how the documented
// deployment and esti run them; when the indexer reads another file, the catalog and the refresher
// below are what covers its repositories.
const configuredRepositoriesKey = "metadata_search.repositories"

// policyRefreshInterval is how often RoutesBuilder re-derives the companion repository set while the
// server runs. It bounds how long a repository created after startup can be missing from the two
// policies: the deny does not cover its companion repository and the indexer's grant does not reach
// it (so an indexer tick fails with an authorization error instead of writing anything) until the
// next refresh.
const policyRefreshInterval = time.Minute

// companionRepositories maps the data repository names the server knows to the companion repositories
// the policies have to name: R-metadata (§3.3), or the name itself when it already ends in the
// suffix, because such a repository *is* a companion repository (icebergcatalog.IsMetadataRepository)
// and not one whose companion would be R-metadata-metadata. Names whose companion is not a valid
// lakeFS repository ID are dropped: an exact ARN is only exact while the name cannot carry a "/", and
// a name such as "a/object/b" would otherwise produce an ARN of an object inside "a". The result is
// sorted and deduplicated, so the same set of repositories always builds the same statements - which
// is what lets ensurePolicy tell "unchanged" from "changed" instead of rewriting on every startup.
func companionRepositories(dataRepositories []string) []string {
	seen := make(map[string]struct{}, len(dataRepositories))
	companions := make([]string, 0, len(dataRepositories))
	for _, name := range dataRepositories {
		companion := name
		if !icebergcatalog.IsMetadataRepository(name) {
			companion = icebergcatalog.MetadataRepository(name)
		}
		if !validator.ReValidRepositoryID.MatchString(companion) {
			continue
		}
		if _, ok := seen[companion]; ok {
			continue
		}
		seen[companion] = struct{}{}
		companions = append(companions, companion)
	}
	slices.Sort(companions)
	return companions
}

// catalogResources returns the catalog ARNs of one companion repository: its namespaces, tables and
// views. Each kind is spelled through the ARN builders of pkg/permissions rather than with a "*" for
// the kind, because a wildcard there is what let catalog:::*/*-metadata/* match a table of a data
// repository whose *namespace* ends in -metadata.
func catalogResources(companion string) []string {
	return []string{
		permissions.CatalogNamespaceArn(companion, "*"),
		permissions.CatalogTableArn(companion, "*", "*"),
		permissions.CatalogViewArn(companion, "*", "*"),
	}
}

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

// metadataRepoProtectFSDeniedActions are the fs: actions §8 denies on a companion repository: the
// object and branch actions that could move an indexer's table, the repository actions that could
// remove it or change what it is, and the import pair. They are wildcards so that a future fs: action
// is covered without a bootstrap change; the deny is on the companion ARNs only, so ordinary data
// repositories are unaffected. fs:Update* is part of the list because fs:UpdateRepository is what
// SetRepositoryMetadata / DeleteRepositoryMetadata write (the review's MDS-SEC-5): without it a
// guard-group operator could still set and delete repository metadata on a companion repository,
// which the deny's own contract - "no principal in a base group may write ... inside a companion
// repository" - rules out.
//
// fs:Import* is the import pair of pkg/api/controller.go. ImportCancel authorizes fs:ImportCancel on
// arn:lakefs:fs:::repository/<companion>/branch/<branch>, which the companion subtree statement
// (repository/<companion>/*) matches, so a guard-group member can no longer cancel an import on a
// companion branch. ImportFromStorage never appears on a repository ARN: ImportStart authorizes it
// on the *source* storage namespace (arn:lakefs:fs:::namespace/<source path>), so these statements do
// not reach it, and that is deliberate rather than an omission. An import whose destination is a
// companion repository is already denied by the fs:WriteObject on the destination branch and object
// and the fs:CreateCommit on that branch, whatever its source. An import whose *source* is a
// companion repository's storage namespace writes the destination repository, not the companion one:
// it copies files out, which is no more than fs:ReadObject on the companion repository already
// allows, and §8's deny deliberately leaves reads open. A namespace statement would also not be
// exact: the narrow pattern (namespace/*-metadata) that the indexer's AttachStorageNamespace grant
// uses would deny imports from an ordinary data repository whose own namespace ends in -metadata,
// and the server does not read the explicit metadata_search.repositories.<R>.storage_namespace
// override (§7 reads only the names), so it could not name that namespace before the companion
// repository exists in the catalog. The deny protects writes inside a companion repository; copying
// its files out changes none, and the catalog and the indexer read the companion repository by name,
// not the copy.
var metadataRepoProtectFSDeniedActions = []string{
	"fs:Write*",
	"fs:Delete*",
	"fs:Create*",
	"fs:Revert*",
	"fs:Update*",
	"fs:Import*",
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
// server passed no auth service, logger or catalog: the principals cannot be created without a
// service, and the companion repositories the policies name cannot be listed without a catalog, so
// the policies would silently cover nothing. The catalog's own builder (pkg/icebergcatalog) fails
// the same way for the same reason.
var ErrMissingDeps = errors.New("mds bootstrap: metadata search is enabled and the server passed no auth service, logger or catalog")

// IndexerPolicy builds the mds-indexer allow policy of §8 for dataRepositories - the data
// repositories the server knows, whose companion repositories the statements name:
//
//	on *                    fs:ReadRepository, fs:ListObjects, fs:ReadObject, fs:ListBranches,
//	                        fs:ReadBranch, fs:ListCommits, fs:ReadCommit
//	on repository/<R>-metadata and below   fs:*, and catalog:* on table/, namespace/ and view/
//	on repository/<R>-metadata             fs:CreateRepository
//	on namespace/*-metadata               fs:AttachStorageNamespace
//
// The fs:CreateRepository statements are subsumed by the fs:* statements above them; they are
// spelled out because §8 names them as separate grants, and keeping them visible makes the policy a
// reader can check against the design line by line. CreateRepository checks both: fs:CreateRepository
// on the repository ARN and fs:AttachStorageNamespace on the storage-namespace ARN
// (pkg/api/controller.go). An empty dataRepositories list leaves the read statement and the
// namespace pattern: the indexer can read every data repository, and it can write no companion
// repository until the next refresh names one.
func IndexerPolicy(dataRepositories []string, ts time.Time) *model.Policy {
	companions := companionRepositories(dataRepositories)
	var statements model.Statements
	statements = append(statements, model.Statement{
		Effect:   model.StatementEffectAllow,
		Action:   append([]string(nil), indexerReadActions...),
		Resource: permissions.All,
	})
	for _, companion := range companions {
		statements = append(statements,
			model.Statement{
				Effect:   model.StatementEffectAllow,
				Action:   []string{"fs:*"},
				Resource: permissions.RepoArn(companion),
			},
			model.Statement{
				Effect:   model.StatementEffectAllow,
				Action:   []string{"fs:*"},
				Resource: permissions.RepoArn(companion) + "/*",
			},
			model.Statement{
				Effect:   model.StatementEffectAllow,
				Action:   []string{permissions.CreateRepositoryAction},
				Resource: permissions.RepoArn(companion),
			},
		)
		for _, resource := range catalogResources(companion) {
			statements = append(statements, model.Statement{
				Effect:   model.StatementEffectAllow,
				Action:   []string{"catalog:*"},
				Resource: resource,
			})
		}
	}
	statements = append(statements, model.Statement{
		Effect:   model.StatementEffectAllow,
		Action:   []string{permissions.AttachStorageNamespaceAction},
		Resource: MetadataNamespacePattern,
	})
	return &model.Policy{
		CreatedAt:   ts,
		DisplayName: IndexerPolicyName,
		Statement:   statements,
	}
}

// MetadataRepoProtectPolicy builds the mds-metadata-repo-protect deny policy of §8 for
// dataRepositories: no principal in a base group may write, delete, create, revert or update
// anything inside a companion repository, on the fs: or on the catalog: API.
//
// It is a deny, so it wins over every allow, including the fs:* allow of an administrator: the
// indexer and the catalog write there, not an operator, and a hand-edit of a served table would be
// silently overwritten by the next tick anyway. The statements name every companion repository of
// the repositories the server knows (see the package comment): a pattern for the "-metadata" naming
// convention (icebergcatalog.MetadataRepositorySuffix) would also match data-repository ARNs, which
// is the defect this policy's shape fixes. The policy is still installation-wide in the sense §8
// means - it is not scoped to one repository, and a repository indexed before Metadata Search was
// enabled is covered as long as the server can see it.
func MetadataRepoProtectPolicy(dataRepositories []string, ts time.Time) *model.Policy {
	companions := companionRepositories(dataRepositories)
	fsActions := append([]string(nil), metadataRepoProtectFSDeniedActions...)
	catalogActions := append([]string(nil), metadataRepoProtectCatalogDeniedActions...)
	var statements model.Statements
	for _, companion := range companions {
		statements = append(statements,
			model.Statement{
				Effect:   model.StatementEffectDeny,
				Action:   fsActions,
				Resource: permissions.RepoArn(companion),
			},
			model.Statement{
				Effect:   model.StatementEffectDeny,
				Action:   fsActions,
				Resource: permissions.RepoArn(companion) + "/*",
			},
		)
		for _, resource := range catalogResources(companion) {
			statements = append(statements, model.Statement{
				Effect:   model.StatementEffectDeny,
				Action:   catalogActions,
				Resource: resource,
			})
		}
	}
	return &model.Policy{
		CreatedAt:   ts,
		DisplayName: MetadataRepoProtectPolicyName,
		Statement:   statements,
	}
}

// RoutesBuilder is the startup hook cmd/lakefs-sso registers with cmd.AddExtraRoutesBuilder
// (wrapped in a closure that supplies guardGroups(), since a routes builder takes no arguments). It
// mounts no route; it exists so the principals of §8 are installed from the same hook loop, and
// before the first request is served, as the catalog's own controls. It also starts the refresher
// that keeps the two policies in step with the repository list while the server runs
// (startPolicyRefresher).
//
// guardGroups is the deny's group list, and it must be the caller's guardGroups(): the base groups
// of both group templates plus datasets.immutable_guard_groups, i.e. exactly the list
// icebergcatalog.Bootstrap hands to iceberg-reserved-prefix-protect (§8). MetadataSearchIndexers is
// dropped from it even when a caller lists it (see metadataRepoProtectGroups).
//
// While iceberg.metadata_search.enabled is false it does nothing at all: the catalog does not
// resolve the virtual table, so no indexer can read a data repository through it and no companion
// repository is written. A configuration error is returned - and so made fatal by the hook loop in
// run.go - for the same reason the catalog's builder fails on one: an installation that meant to
// enable Metadata Search must be told that it did not.
//
// deps.Catalog is required because it is where the companion repositories the policies name come
// from; a nil catalog is ErrMissingDeps, exactly as in icebergcatalog.Bootstrap.
func RoutesBuilder(ctx context.Context, deps cmdpkg.ServerDeps, guardGroups []string) error {
	cfg, err := icebergcatalog.LoadConfig()
	if err != nil {
		return err
	}
	if !cfg.MetadataSearchEnabled() {
		return nil
	}
	if deps.AuthService == nil || deps.Logger == nil || deps.Catalog == nil {
		return ErrMissingDeps
	}

	dataRepositories, err := knownDataRepositories(ctx, deps.Catalog)
	if err != nil {
		return err
	}
	inForce, err := EnsurePrincipals(ctx, deps.AuthService, time.Now(), guardGroups, dataRepositories)
	if err != nil {
		return err
	}
	fields := logging.Fields{
		"user":                   UserName,
		"group":                  GroupName,
		"policies":               []string{IndexerPolicyName, MetadataRepoProtectPolicyName},
		"companion_repositories": companionRepositories(dataRepositories),
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
	startPolicyRefresher(ctx, deps, policyRefreshInterval)
	return nil
}

// knownDataRepositories returns the data repositories whose companion repositories the two policies
// have to name: every repository the catalog holds, plus the ones named in this file's
// metadata_search.repositories section (configuredRepositoriesKey). Neither source is sufficient
// alone:
//
//   - the catalog cannot see a repository that does not exist yet, and the indexer creates
//     R-metadata on its first tick of R. Without the configured list, a repository created after
//     this process started would have no grant and no deny until the refresher caught up, and its
//     first indexer ticks would fail with an authorization error;
//   - metadata_search.repositories is the indexer's section, and §7 lets the two processes read
//     different files, so it may be missing here. The catalog then covers the repositories that
//     exist, including the companion repositories a previous indexer run created.
//
// A configured name that is not a valid repository ID is reported and dropped; nothing is returned
// for it, and companionRepositories applies the same filter to the catalog's names.
func knownDataRepositories(ctx context.Context, cat *catalog.Catalog) ([]string, error) {
	var names []string
	after := ""
	for {
		repositories, hasMore, err := cat.ListRepositories(ctx, catalog.ListRepositoriesLimitMax, "", "", after)
		if err != nil {
			return nil, fmt.Errorf("mds bootstrap: list repositories: %w", err)
		}
		for _, repository := range repositories {
			if !validator.ReValidRepositoryID.MatchString(repository.Name) {
				continue
			}
			names = append(names, repository.Name)
		}
		if !hasMore || len(repositories) == 0 {
			break
		}
		after = repositories[len(repositories)-1].Name
	}

	configured := viper.GetStringMap(configuredRepositoriesKey)
	for name := range configured {
		// viper lower-cases the keys of a nested map, and a lakeFS repository ID is lower case by
		// definition (validator.ReValidRepositoryID), so a name that arrives here unchanged is the
		// name the indexer will look up.
		if !validator.ReValidRepositoryID.MatchString(name) {
			logging.FromContext(ctx).WithField("repository", name).Warn(
				"mds bootstrap: ignoring a " + configuredRepositoriesKey + " entry that is not a valid repository ID: " +
					"its companion repository is left out of the metadata search policies")
			continue
		}
		names = append(names, name)
	}

	slices.Sort(names)
	return names, nil
}

// startPolicyRefresher keeps the statements of both policies in step with the repositories the
// server knows for as long as the server runs, by re-deriving the companion repository set and
// repairing a policy whose statements changed. It is a goroutine bound to ctx because the policies
// are otherwise a snapshot of what existed at startup, and a snapshot is the one thing an exact ARN
// cannot be: a repository created later would be missing from them, which costs the indexer its
// grant on R-metadata (a loud 403 in /_health, so it fails closed) and leaves the guard-group deny
// not covering R-metadata at all (which fails open, and is the reason this refresher exists rather
// than a "restart the server after creating a repository" note). After one refresh, every repository
// that exists is named; the window between a repository's creation and the next tick is
// policyRefreshInterval at worst.
//
// A failed refresh is logged and retried on the next tick: a KV hiccup must not stop a server that
// is otherwise serving. A refresh that reports the auth backend cannot store policies stops the
// refresher instead, because EnsurePrincipals has already warned about that deployment once and
// every later attempt would only repeat it.
//
// The returned channel is closed when the goroutine stops. RoutesBuilder ignores it; the tests use it
// to assert a clean stop rather than a goroutine that outlives the test's store.
func startPolicyRefresher(ctx context.Context, deps cmdpkg.ServerDeps, interval time.Duration) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			dataRepositories, err := knownDataRepositories(ctx, deps.Catalog)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				deps.Logger.WithError(err).Warn("metadata search: cannot list the repositories for the companion-repository policies; retrying on the next refresh")
				continue
			}
			inForce, err := refreshPolicies(ctx, deps.AuthService, dataRepositories, time.Now())
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				deps.Logger.WithError(err).Warn("metadata search: cannot keep the companion-repository policies in step with the repository list; retrying on the next refresh")
				continue
			}
			if !inForce {
				return
			}
		}
	}()
	return done
}

// EnsurePrincipals makes the principals and policies of §8 exist, with exactly their built-in
// statements, and attaches them:
//
//	user   mds-indexer             member of MetadataSearchIndexers
//	group  MetadataSearchIndexers  carries mds-indexer, never mds-metadata-repo-protect
//	policy mds-indexer             allow, attached to MetadataSearchIndexers
//	policy mds-metadata-repo-protect  deny, attached to guardGroups minus MetadataSearchIndexers
//
// guardGroups is the full list of groups that must carry the deny - the same list
// iceberg-reserved-prefix-protect uses, which cmd/lakefs-sso computes once in guardGroups() as
// datasets.PinsProtectGroups(datasets.immutable_guard_groups) and passes to both bootstraps (§8).
// It is an argument rather than something this package computes, because computing it here is how
// an operator's datasets.immutable_guard_groups once silently got no deny: the list must be the
// catalog's list, not a second copy of it. A nil or empty list attaches the deny to no group.
//
// dataRepositories are the data repositories the server knows; both policies name their companion
// repositories explicitly (§8, and the package comment explains why a "*-metadata" pattern is not
// an option). A nil list is allowed and builds the read-only allow and an empty deny: it is what a
// server that knows no repository yet - and is configured with none - has to offer until its
// refresher learns about one.
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
func EnsurePrincipals(ctx context.Context, authService auth.Service, ts time.Time, guardGroups, dataRepositories []string) (bool, error) {
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
	inForce, err := refreshPolicies(ctx, authService, dataRepositories, ts)
	if err != nil || !inForce {
		return false, err
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

	// The deny goes to the guard list the caller supplied - the same base groups plus
	// datasets.immutable_guard_groups that datasets-pins-protect and iceberg-reserved-prefix-protect
	// cover (§8) - minus MetadataSearchIndexers: the indexer is the one principal that must be able
	// to write a companion repository, so the deny must never reach it.
	for _, group := range metadataRepoProtectGroups(ctx, guardGroups) {
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

// refreshPolicies creates or repairs both policies of §8 for dataRepositories and reports whether
// they are in force. It is the part of EnsurePrincipals that the refresher repeats while the server
// runs (startPolicyRefresher): the user, the group and the attachments do not depend on the
// repository list, but the statements do, so a repository that appears after startup only needs the
// statements rewritten - and ensurePolicy rewrites them only when they actually differ, which is why
// an unchanged repository set costs two reads and no write.
func refreshPolicies(ctx context.Context, authService auth.Service, dataRepositories []string, ts time.Time) (bool, error) {
	for _, policy := range []*model.Policy{IndexerPolicy(dataRepositories, ts), MetadataRepoProtectPolicy(dataRepositories, ts)} {
		inForce, err := ensurePolicy(ctx, authService, policy)
		if err != nil || !inForce {
			return false, err
		}
	}
	return true, nil
}

// metadataRepoProtectGroups returns the groups the deny is attached to: guardGroups deduplicated,
// with empty names and MetadataSearchIndexers dropped. The order is kept, so the attachments (and
// the debug logs for groups an installation does not have) are deterministic.
//
// Dropping GroupName is not theoretical: guardGroups() ends with
// datasets.immutable_guard_groups, which an operator writes by hand. Listing MetadataSearchIndexers
// there would otherwise attach a deny of every fs:/catalog: write to the only principal that writes
// companion repositories, and the indexer would fail every tick. It is dropped with a warning naming
// the consequence rather than an error, because the rest of the list is still valid and a startup
// that refused to run would leave even the base groups unprotected.
func metadataRepoProtectGroups(ctx context.Context, guardGroups []string) []string {
	seen := make(map[string]struct{}, len(guardGroups))
	groups := make([]string, 0, len(guardGroups))
	for _, group := range guardGroups {
		switch group {
		case "":
			continue
		case GroupName:
			logging.FromContext(ctx).WithField("group", group).Warn(
				"mds bootstrap: ignoring " + GroupName + " in the metadata-repo deny group list: the deny is " +
					"never attached to the indexer group, whose principal must be able to write companion repositories")
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
