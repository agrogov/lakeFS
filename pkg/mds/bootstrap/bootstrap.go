// Package bootstrap creates and maintains the Metadata Search principals and policies of
// METADATA_SEARCH_DESIGN.md §8 on a lakeFS installation: the mds-indexer user, the
// MetadataSearchIndexers group, the mds-indexer allow policies and the mds-metadata-repo-protect
// deny policies. It runs as an extra routes builder of the lakefs-sso binary, after the catalog's
// own builders, and mounts no route: the principals have to exist before the first indexer request
// is authorized, whether or not a request ever arrives.
//
// The companion-repository statements are split into numbered chunk policies so that no stored
// policy can grow without bound: mds-indexer carries the statements that do not depend on a
// repository, and mds-indexer-<n> / mds-metadata-repo-protect-<n> carry at most
// companionChunkSize companion repositories each. The deny has no repository-independent statement,
// so it has no base policy at all: a policy with zero statements is not a policy (the auth API
// rejects one, api/authorization.yml minItems: 1, see R3S-2). Every chunk is attached to the same
// group as the policy it extends, and a chunk keeps the companions it already carries from one
// refresh to the next, so adding a repository never moves an unrelated companion out of the policy
// set for the duration of two writes (R3S-7). The refresher repairs the chunks and deletes a chunk
// that is no longer needed when the repository set shrinks; it deletes only names this package owns
// (the base name and numeric chunk suffixes), never an operator's policy.
//
// An explicit metadata_search.repositories.<R>.storage_namespace that does not itself end in
// -metadata needs an extra grant that this package must not guess. The operator puts that grant in
// a policy of its own (for example mds-indexer-extra-storage-namespace) and attaches it to
// MetadataSearchIndexers. The refresher does not own that name, so it survives every repair.
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
// The policies name the companion repositories explicitly instead of matching them with a
// "*-metadata" wildcard. The wildcard could not express "the repository name ends in -metadata":
// pkg/auth/wildcard.Match lets "*" span "/", so arn:lakefs:fs:::repository/*-metadata also matched
// arn:lakefs:fs:::repository/<R>/object/-metadata (the star took "<R>/object/"), and
// arn:lakefs:fs:::repository/*-metadata/object/* matched an object of <R> whose key was
// a/b-metadata/object/c. The same held for the catalog ARN pattern, whose star for the kind let
// table/<R>/<namespace ending in -metadata>/<table> match. Both consequences were real: the indexer,
// which §8 limits to reads on a data repository, was granted writes on such objects and tables, and
// the deny made an ordinary foo-metadata/data.yml object of a data repository unwritable for every
// guard-group member. An exact ARN per companion repository has neither problem, so the policies are
// built from the data repositories the server knows (knownRepositories) and RoutesBuilder keeps
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
	"strconv"
	"strings"
	"time"

	"github.com/spf13/viper"
	cmdpkg "github.com/treeverse/lakefs/cmd/lakefs/cmd"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/block"
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

// companionChunkSize is the maximum number of companion repositories whose statements one chunk
// policy stores. With six allow statements (indexer) or at most seven deny statements (protect: the
// repository pair, the three catalog kinds, and the AttachStorageNamespace deny of the companion's
// namespace in both of its accepted spellings) per companion, a chunk
// stays well under MaxPolicyStoredBytes even when every companion repository ID uses the
// 63-character maximum. A smaller chunk would only add policies; a larger one would approach
// DynamoDB's 400 KiB item limit.
const companionChunkSize = 64

// MaxPolicyStoredBytes is the documented upper bound on the encoded value of a single metadata
// search policy. DynamoDB stores a policy as one item and rejects an item above 400 KiB, so this cap
// leaves ample room for the KV key and encoding overhead. TestPolicySizeAtScale checks every policy
// generated for 1000 repositories against it.
const MaxPolicyStoredBytes = 160 << 10

// policyRefreshInterval is how often RoutesBuilder re-derives the companion repository set while the
// server runs. It bounds how long a repository created after startup can be missing from the
// policies: the deny does not cover its companion repository and the indexer's grant does not reach
// it (so an indexer tick fails with an authorization error instead of writing anything) until the
// next refresh. It is a variable only so the tests can shorten it; production leaves the default.
var policyRefreshInterval = time.Minute

// repositoryInfo is one repository the server knows: its name and, when it exists in the
// catalog, its storage namespace. A name that comes only from metadata_search.repositories has an
// empty StorageNamespace until the indexer creates the repository.
type repositoryInfo struct {
	Name             string
	StorageNamespace string
}

// companionRepository is one companion repository the policies name. StorageNamespace is the
// namespace the deny must cover for fs:AttachStorageNamespace; it is empty when the companion
// repository does not exist yet, because an exact namespace ARN cannot be derived from a name.
type companionRepository struct {
	Name             string
	StorageNamespace string
}

// companionRepositories maps the repositories the server knows to the companion repositories the
// policies have to name: R-metadata (§3.3), plus an existing repository whose own name ends in the
// suffix only when its data repository R exists too.
//
// A name that ends in -metadata is not sufficient proof that the repository is a companion: before
// Metadata Search existed an operator could create an ordinary repository named team-metadata, and
// the indexer refuses to index such a name as a data repository (pkg/mds/config.go). Treating it as
// a companion would give mds-indexer fs:* on an ordinary data repository and make that repository
// unwritable for its own users. The criterion here is the data repository's existence, not the
// lakefs.mds.init marker, because the bootstrap already has the repository list (including the
// configured names that do not exist yet) and does not otherwise read commits; requiring the marker
// would walk the companion's history on every refresh and would still race an indexer that has
// created the repository but not its init commit. A repository whose data repository is gone has
// nothing to index and is therefore an ordinary repository again.
//
// Names whose companion is not a valid lakeFS repository ID are dropped: an exact ARN is only exact
// while the name cannot carry a "/", and a name such as "a/object/b" would otherwise produce an ARN
// of an object inside "a". The result is sorted and deduplicated, so the same set of repositories
// always builds the same statements - which is what lets ensurePolicy tell "unchanged" from
// "changed" instead of rewriting on every startup.
func companionRepositories(repositories []repositoryInfo) []companionRepository {
	known := make(map[string]repositoryInfo, len(repositories))
	for _, repository := range repositories {
		known[repository.Name] = repository
	}
	seen := make(map[string]struct{}, len(repositories))
	companions := make([]companionRepository, 0, len(repositories))
	add := func(name string) {
		if !validator.ReValidRepositoryID.MatchString(name) {
			return
		}
		if _, ok := seen[name]; ok {
			return
		}
		seen[name] = struct{}{}
		companions = append(companions, companionRepository{
			Name:             name,
			StorageNamespace: known[name].StorageNamespace,
		})
	}
	for _, repository := range repositories {
		if icebergcatalog.IsMetadataRepository(repository.Name) {
			dataRepository := strings.TrimSuffix(repository.Name, icebergcatalog.MetadataRepositorySuffix)
			if _, ok := known[dataRepository]; !ok {
				continue
			}
			add(repository.Name)
			continue
		}
		add(icebergcatalog.MetadataRepository(repository.Name))
	}
	slices.SortFunc(companions, func(a, b companionRepository) int {
		return strings.Compare(a.Name, b.Name)
	})
	return companions
}

// companionChunkPlan is the chunked companion repository set: element i holds the companions of
// chunk i+1, which chunkPolicyName spells as <base>-<i+1>. Every chunk holds at most
// companionChunkSize companions, which bounds the size of every stored policy independently of the
// number of repositories: a policy item is one KV value (pkg/kv/msg.go), so an unchunked policy
// crosses DynamoDB's 400 KiB item limit at roughly 560 repositories.
type companionChunkPlan [][]companionRepository

// companionChunks splits companions into fixed-size chunks, in the (sorted) order they arrive. It
// is the plan of a fresh installation, and the plan a repository set that fits in one chunk always
// lands on.
func companionChunks(companions []companionRepository) companionChunkPlan {
	chunks := make(companionChunkPlan, 0, (len(companions)+companionChunkSize-1)/companionChunkSize)
	for start := 0; start < len(companions); start += companionChunkSize {
		end := min(start+companionChunkSize, len(companions))
		chunks = append(chunks, companions[start:end])
	}
	return chunks
}

// stableCompanionChunks is the chunk plan of a refresh (R3S-7): every companion the stored chunks
// already carry stays in the chunk that carries it, and a companion that is new to the set goes into
// a chunk's spare capacity - a new chunk at the end only when every chunk is full.
//
// Positional chunking alone loses coverage between two writes. An exact companion ARN is a
// snapshot, so the chunk set is rewritten one policy at a time; with a positional plan, inserting a
// repository early in the sort order pushes the last member of chunk n into chunk n+1. The rewrite
// of chunk n then drops that companion's statements before chunk n+1 carries them, and the purge
// after each write makes the gap visible at once. For the deny that gap fails open: a guard group
// could attach the companion's storage namespace, or write the companion repository, until the
// second write lands (or, if the second write fails, until the next refresh). For the allow the
// indexer loses its grant on an existing companion the same way.
//
// existing holds the companions each stored chunk names, in chunk order (existingCompanionChunks).
// Names a chunk carries that are no longer companions are dropped; names that are still companions
// keep their chunk, so a refresh that only adds repositories rewrites (at most) the chunks that gain
// a member and never removes an existing companion from any chunk. Empty chunks are dropped so the
// surviving plan is numbered 1..n: the stale chunk policy they leave behind is deleted only after
// every new chunk policy has been written.
func stableCompanionChunks(companions []companionRepository, existing [][]string) companionChunkPlan {
	byName := make(map[string]companionRepository, len(companions))
	for _, companion := range companions {
		byName[companion.Name] = companion
	}
	assigned := make(map[string]struct{}, len(companions))
	chunks := make(companionChunkPlan, 0, len(existing)+1)
	for _, names := range existing {
		chunk := make([]companionRepository, 0, companionChunkSize)
		for _, name := range names {
			companion, ok := byName[name]
			if !ok {
				continue // the repository was deleted: the chunk no longer names it
			}
			if _, ok := assigned[name]; ok {
				continue // a partial earlier write left it in two chunks; keep the first
			}
			if len(chunk) == companionChunkSize {
				break
			}
			assigned[name] = struct{}{}
			chunk = append(chunk, companion)
		}
		chunks = append(chunks, chunk)
	}
	fill := 0
	for _, companion := range companions {
		if _, ok := assigned[companion.Name]; ok {
			continue
		}
		for fill < len(chunks) && len(chunks[fill]) == companionChunkSize {
			fill++
		}
		if fill == len(chunks) {
			chunks = append(chunks, make([]companionRepository, 0, companionChunkSize))
		}
		chunks[fill] = append(chunks[fill], companion)
	}
	nonEmpty := make(companionChunkPlan, 0, len(chunks))
	for _, chunk := range chunks {
		if len(chunk) > 0 {
			nonEmpty = append(nonEmpty, chunk)
		}
	}
	return nonEmpty
}

// chunkPolicyName names chunk (1-based) of base. The base name itself (chunk 0) carries only the
// statements that do not depend on a companion repository, so every companion statement lives in a
// mds-indexer-<n> or mds-metadata-repo-protect-<n> policy with at most companionChunkSize
// companions. The deny has no repository-independent statement, so it has no base policy.
func chunkPolicyName(base string, chunk int) string {
	return fmt.Sprintf("%s-%d", base, chunk)
}

// IsMetadataRepoProtectChunk reports whether name is one of the numbered chunk policies of the
// installation-wide metadata-repo deny: mds-metadata-repo-protect-<n> with a positive n, no leading
// zero, and nothing else. The base name itself is not a chunk - the deny has no base policy (R3S-2)
// - and an operator policy that merely shares the prefix is not one either.
//
// It is exported for the end-to-end suite, which has to assert the policy set the server installs
// without duplicating the name format: the suite lists the policies attached to the guard groups
// and checks every metadata-repo-protect name against this predicate.
func IsMetadataRepoProtectChunk(name string) bool {
	_, ok := ownedChunkNumber(MetadataRepoProtectPolicyName, name)
	return ok
}

// isOwnedPolicyName reports whether name is a policy this package writes: the base name or one of
// its numeric chunks. It is deliberately narrower than a prefix match, so an operator policy such
// as mds-indexer-extra-storage-namespace is never deleted as a stale chunk; see the R2S-6 note in
// the package comment.
func isOwnedPolicyName(base, name string) bool {
	if name == base {
		return true
	}
	suffix, ok := strings.CutPrefix(name, base+"-")
	if !ok || suffix == "" {
		return false
	}
	for _, r := range suffix {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// ownedChunkNumber reports the 1-based chunk number of name when name is one of base's chunk
// policies (base-1, base-2, ...). The base policy itself is not a chunk, so it is not reported.
func ownedChunkNumber(base, name string) (int, bool) {
	suffix, ok := strings.CutPrefix(name, base+"-")
	if !ok || suffix == "" {
		return 0, false
	}
	chunk, err := strconv.Atoi(suffix)
	if err != nil || chunk < 1 || strconv.Itoa(chunk) != suffix {
		return 0, false
	}
	return chunk, true
}

// companionNamesInPolicy returns the companion repositories a stored chunk policy names: the exact
// repository ARNs of its statements. Both policy shapes spell every companion repository as
// arn:lakefs:fs:::repository/<name>, so this reads back what this package wrote without depending
// on the order of the statements. A hand-edited or drifted policy can name something else; that name
// is kept only when the repository set still carries it as a companion (stableCompanionChunks), so a
// bad statement cannot keep a non-companion in the policy set.
func companionNamesInPolicy(policy *model.Policy) []string {
	prefix := permissions.RepoArn("")
	names := make([]string, 0, len(policy.Statement))
	seen := make(map[string]struct{}, len(policy.Statement))
	for _, statement := range policy.Statement {
		name, ok := strings.CutPrefix(statement.Resource, prefix)
		if !ok || name == "" || strings.Contains(name, "/") {
			continue
		}
		if !validator.ReValidRepositoryID.MatchString(name) {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	return names
}

// listOwnedPolicies returns every policy of base's set this package owns: the base policy and its
// numeric chunks, and never an operator policy that merely shares the prefix (isOwnedPolicyName).
// The list is paginated, because a large installation stores one chunk per 64 companions.
func listOwnedPolicies(ctx context.Context, authService auth.Service, base string) ([]*model.Policy, error) {
	owned := make([]*model.Policy, 0)
	params := &model.PaginationParams{Prefix: base, Amount: -1}
	for {
		policies, paginator, err := authService.ListPolicies(ctx, params)
		if err != nil {
			return nil, err
		}
		for _, policy := range policies {
			if !isOwnedPolicyName(base, policy.DisplayName) {
				continue
			}
			owned = append(owned, policy)
		}
		if paginator == nil || paginator.NextPageToken == "" {
			return owned, nil
		}
		params.After = paginator.NextPageToken
	}
}

// existingCompanionChunks returns the companions every stored chunk policy of base names, in chunk
// order: element i is a chunk whose policy is named <base>-<n> with n strictly increasing. It is what
// lets stableCompanionChunks keep a companion in the chunk that already carries it, so a refresh that
// only adds a repository never rewrites a chunk whose companions did not change.
//
// The result is a dense slice of the chunks that exist, not an index by chunk number: policy names are
// predictable, so a principal allowed to write policies could create <base>-1000000, and a slice
// indexed by the number would allocate that many entries on every refresh. The numbers of the new plan
// are assigned 1..n when the policies are written, so the gaps of a stale numbering disappear.
func existingCompanionChunks(ctx context.Context, authService auth.Service, base string) ([][]string, error) {
	policies, err := listOwnedPolicies(ctx, authService, base)
	if err != nil {
		return nil, err
	}
	byChunk := make(map[int][]string, len(policies))
	numbers := make([]int, 0, len(policies))
	for _, policy := range policies {
		chunk, ok := ownedChunkNumber(base, policy.DisplayName)
		if !ok {
			continue
		}
		if _, ok := byChunk[chunk]; !ok {
			numbers = append(numbers, chunk)
		}
		byChunk[chunk] = companionNamesInPolicy(policy)
	}
	slices.Sort(numbers)
	chunks := make([][]string, 0, len(numbers))
	for _, chunk := range numbers {
		chunks = append(chunks, byChunk[chunk])
	}
	return chunks, nil
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

// IndexerPolicies builds the mds-indexer allow policies of §8 for repositories - the repositories
// the server knows, whose companion repositories the statements name. The first policy carries the
// statements that do not depend on a repository, and the rest are numbered chunks:
//
//	on *                    fs:ReadRepository, fs:ListObjects, fs:ReadObject, fs:ListBranches,
//	                        fs:ReadBranch, fs:ListCommits, fs:ReadCommit
//	on namespace/*-metadata               fs:AttachStorageNamespace
//	on repository/<R>-metadata and below   fs:*, and catalog:* on table/, namespace/ and view/
//	on repository/<R>-metadata             fs:CreateRepository
//
// The fs:CreateRepository statements are subsumed by the fs:* statements above them; they are
// spelled out because §8 names them as separate grants, and keeping them visible makes the policy a
// reader can check against the design line by line. CreateRepository checks both: fs:CreateRepository
// on the repository ARN and fs:AttachStorageNamespace on the storage-namespace ARN
// (pkg/api/controller.go). An empty repositories list leaves the read statement and the namespace
// pattern: the indexer can read every data repository, and it can write no companion repository
// until the next refresh names one.
func IndexerPolicies(repositories []repositoryInfo, ts time.Time) []*model.Policy {
	return indexerChunkPolicies(companionChunks(companionRepositories(repositories)), ts)
}

// indexerChunkPolicies builds the allow policy set for one chunk plan: the base policy followed by
// one policy per chunk. The caller decides the plan, because a refresh uses the stable plan of
// stableCompanionChunks (R3S-7) while a fresh installation starts from sorted, positional chunks.
func indexerChunkPolicies(chunks companionChunkPlan, ts time.Time) []*model.Policy {
	policies := []*model.Policy{{
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
				Action:   []string{permissions.AttachStorageNamespaceAction},
				Resource: MetadataNamespacePattern,
			},
		},
	}}
	for chunk, chunkCompanions := range chunks {
		var statements model.Statements
		for _, companion := range chunkCompanions {
			statements = append(statements,
				model.Statement{
					Effect:   model.StatementEffectAllow,
					Action:   []string{"fs:*"},
					Resource: permissions.RepoArn(companion.Name),
				},
				model.Statement{
					Effect:   model.StatementEffectAllow,
					Action:   []string{"fs:*"},
					Resource: permissions.RepoArn(companion.Name) + "/*",
				},
				model.Statement{
					Effect:   model.StatementEffectAllow,
					Action:   []string{permissions.CreateRepositoryAction},
					Resource: permissions.RepoArn(companion.Name),
				},
			)
			for _, resource := range catalogResources(companion.Name) {
				statements = append(statements, model.Statement{
					Effect:   model.StatementEffectAllow,
					Action:   []string{"catalog:*"},
					Resource: resource,
				})
			}
		}
		policies = append(policies, &model.Policy{
			CreatedAt:   ts,
			DisplayName: chunkPolicyName(IndexerPolicyName, chunk+1),
			Statement:   statements,
		})
	}
	return policies
}

// storageNamespaceDenyResources returns the storage-namespace ARNs the deny needs for one companion
// repository: the spellings CreateRepository accepts for the same physical prefix (R3S-3).
//
// CreateRepository authorizes fs:AttachStorageNamespace on arn:lakefs:fs:::namespace/<the raw string
// the client sent> (pkg/api/controller.go), before it looks at --bare, so the ARN it compares is the
// client's spelling and not a canonical one. The block layer then resolves that string:
// pkg/block.resolveRelative trims one trailing "/" from the namespace, and formatPathWithNamespace
// trims trailing slashes before it appends a key, so "s3://bucket/prefix" and "s3://bucket/prefix/"
// are the same physical prefix and both pass the storage-namespace validity regex (which matches the
// scheme only, pkg/block.DefaultValidationRegex). A stored namespace may itself end in "/", so the
// stored spelling is emitted together with the trailing-slash-trimmed spelling and that spelling with
// one "/": for every stored spelling those three cover both physical-equivalent spellings.
//
// A bucket-root namespace ("s3://bucket", only ever a companion's own namespace through an explicit
// metadata_search.repositories.<R>.storage_namespace) has one more: resolveRelative trims one "/", so
// "s3://bucket//" also resolves to the bucket root. That spelling is emitted for a bucket root only.
//
// Azure (http and https, block.StorageTypeAzure) adds two rules, both from METADATA_SEARCH_AZURE D4:
//
//   - the adapter's validity regex accepts a namespace with either scheme, and both name the same
//     container and prefix, so every spelling above is kept and its http <-> https twin is added.
//     S3, GS, mem and local keep exactly the spellings they had.
//   - a "bucket root" is an Azure container root ("https://acct.blob.core.windows.net/data"), because
//     block.NamespaceRoot treats the first path segment as the container and not as a path, so the
//     container root gets the "//" spelling and a prefix inside the container does not. S3 and GS
//     keep the bucket root they had.
//
// The enumeration stops there because a further trailing slash is a different physical prefix for S3
// and GS, whose path is used verbatim as the key prefix. The dev-only mem and local adapters clean the
// path (formatPathWithNamespace, filepath.Join), so there "…//" and "…///" happen to be the same
// prefix too; covering that would need a trailing-* pattern on namespace/<ns>/*, which also denies
// every namespace below the companion's - the over-match the exact ARNs of §8 exist to avoid.
func storageNamespaceDenyResources(namespace string) []string {
	var spellings []string
	add := func(spelling string) {
		if !slices.Contains(spellings, spelling) {
			spellings = append(spellings, spelling)
		}
	}
	add(namespace)
	trimmed := strings.TrimRight(namespace, "/")
	add(trimmed)
	add(trimmed + "/")
	if isBucketRootNamespace(trimmed) {
		add(trimmed + "//")
	}
	for _, spelling := range slices.Clone(spellings) {
		if other, ok := otherSchemeSpelling(spelling); ok {
			add(other)
		}
	}
	resources := make([]string, 0, len(spellings))
	for _, spelling := range spellings {
		resources = append(resources, permissions.StorageNamespace(spelling))
	}
	return resources
}

// otherSchemeSpelling returns the other scheme's spelling of an Azure storage namespace: the block
// adapter's validity regex accepts http and https, and both address the same container and prefix
// (METADATA_SEARCH_AZURE D4). A spelling of any other scheme has no such twin.
func otherSchemeSpelling(spelling string) (string, bool) {
	switch {
	case strings.HasPrefix(spelling, "https://"):
		return "http://" + strings.TrimPrefix(spelling, "https://"), true
	case strings.HasPrefix(spelling, "http://"):
		return "https://" + strings.TrimPrefix(spelling, "http://"), true
	default:
		return "", false
	}
}

// isBucketRootNamespace reports whether namespace addresses the root of its bucket - or, on Azure,
// of its container: block.NamespaceRoot splits the namespace into root and path per blockstore, so
// "https://acct.blob.core.windows.net/data" is a container root just like "s3://bucket" is a bucket
// root, and its first path segment is not a path to derive from (METADATA_SEARCH_AZURE D4). A
// namespace that block.NamespaceRoot refuses has no root and is not a bucket root.
func isBucketRootNamespace(namespace string) bool {
	_, path, err := block.NamespaceRoot(namespace)
	if err != nil {
		return false
	}
	return path == ""
}

// MetadataRepoProtectPolicies builds the mds-metadata-repo-protect deny policies of §8 for
// repositories: no principal in a base group may write, delete, create, revert or update anything
// inside a companion repository, on the fs: or on the catalog: API, or change the branch protection
// rules that decide whether the indexer may commit. There is no repository-independent deny, and a
// policy with zero statements is not a policy: the auth API rejects one (api/authorization.yml
// requires minItems: 1 on Policy.statement, and a validating server answers 400), so the set is only
// the numbered chunks, and a repository set with no companion repository has no deny policy at all
// (R3S-2). A stored base deny policy from an earlier version is removed by the refresher as a stale
// owned name.
//
// It is a deny, so it wins over every allow, including the fs:* allow of an administrator: the
// indexer and the catalog write there, not an operator, and a hand-edit of a served table would be
// silently overwritten by the next tick anyway. The statements name every companion repository of
// the repositories the server knows (see the package comment): a pattern for the "-metadata" naming
// convention (icebergcatalog.MetadataRepositorySuffix) would also match data-repository ARNs, which
// is the defect this policy's shape fixes. The policy is still installation-wide in the sense §8
// means - it is not scoped to one repository, and a repository indexed before Metadata Search was
// enabled is covered as long as the server can see it.
//
// The AttachStorageNamespace deny is the one statement that is not an exact ARN of one spelling: a
// companion's storage namespace is accepted by CreateRepository in more than one spelling for the
// same physical prefix (at least the namespace itself and the namespace with a trailing "/"; see
// storageNamespaceDenyResources), and CreateRepository authorizes the raw string the client sent
// (pkg/api/controller.go). One ARN of the stored spelling would leave every other spelling to a
// guard-group member with fs:*, who could create a bare repository on the companion's namespace and
// let its GC sweep the index files (R3S-3). Each normalised spelling therefore gets its own
// statement.
//
// branches:SetBranchProtectionRules is denied because setting a rule that blocks COMMIT on a
// companion branch stops the indexer permanently, which is a write of repository state that decides
// whether the indexer may write. retention:SetGarbageCollectionRules and
// retention:PrepareGarbageCollectionCommits stay allowed: they cannot remove the live index without
// first deleting the branch (fs:DeleteBranch is denied), and an operator may legitimately set a GC
// policy for a companion repository.
func MetadataRepoProtectPolicies(repositories []repositoryInfo, ts time.Time) []*model.Policy {
	return protectChunkPolicies(companionChunks(companionRepositories(repositories)), ts)
}

// protectChunkPolicies builds the deny policy set for one chunk plan. The plan is the caller's, for
// the same reason as in indexerChunkPolicies.
func protectChunkPolicies(chunks companionChunkPlan, ts time.Time) []*model.Policy {
	fsActions := append([]string(nil), metadataRepoProtectFSDeniedActions...)
	repositoryActions := append(append([]string(nil), fsActions...), permissions.SetBranchProtectionRulesAction)
	catalogActions := append([]string(nil), metadataRepoProtectCatalogDeniedActions...)
	policies := make([]*model.Policy, 0, len(chunks))
	for chunk, chunkCompanions := range chunks {
		var statements model.Statements
		for _, companion := range chunkCompanions {
			statements = append(statements,
				model.Statement{
					Effect:   model.StatementEffectDeny,
					Action:   repositoryActions,
					Resource: permissions.RepoArn(companion.Name),
				},
				model.Statement{
					Effect:   model.StatementEffectDeny,
					Action:   fsActions,
					Resource: permissions.RepoArn(companion.Name) + "/*",
				},
			)
			for _, resource := range catalogResources(companion.Name) {
				statements = append(statements, model.Statement{
					Effect:   model.StatementEffectDeny,
					Action:   catalogActions,
					Resource: resource,
				})
			}
			if companion.StorageNamespace != "" {
				for _, resource := range storageNamespaceDenyResources(companion.StorageNamespace) {
					statements = append(statements, model.Statement{
						Effect:   model.StatementEffectDeny,
						Action:   []string{permissions.AttachStorageNamespaceAction},
						Resource: resource,
					})
				}
			}
		}
		policies = append(policies, &model.Policy{
			CreatedAt:   ts,
			DisplayName: chunkPolicyName(MetadataRepoProtectPolicyName, chunk+1),
			Statement:   statements,
		})
	}
	return policies
}

// RoutesBuilder is the startup hook cmd/lakefs-sso registers with cmd.AddExtraRoutesBuilder
// (wrapped in a closure that supplies guardGroups(), since a routes builder takes no arguments). It
// mounts no route; it exists so the principals of §8 are installed from the same hook loop, and
// before the first request is served, as the catalog's own controls. It also starts the refresher
// that keeps the policy set in step with the repository list while the server runs
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

	repositories, err := knownRepositories(ctx, deps.Catalog)
	if err != nil {
		return err
	}
	ts := time.Now()
	inForce, policies, err := ensurePrincipals(ctx, deps.AuthService, ts, guardGroups, repositories)
	if err != nil {
		return err
	}
	fields := logging.Fields{
		"user":                   UserName,
		"group":                  GroupName,
		"companion_repositories": companionNames(companionRepositories(repositories)),
	}
	if policies != nil {
		fields["policies"] = policies.names()
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
	startPolicyRefresher(ctx, deps, guardGroups, policyRefreshInterval)
	return nil
}

// knownRepositories returns the repositories whose companion repositories the policies have to
// name: every repository the catalog holds (with its storage namespace) plus the names in this
// file's metadata_search.repositories section (configuredRepositoriesKey), which have no namespace
// until they exist. Neither source is sufficient alone:
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
func knownRepositories(ctx context.Context, cat *catalog.Catalog) ([]repositoryInfo, error) {
	byName := make(map[string]repositoryInfo)
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
			byName[repository.Name] = repositoryInfo{
				Name:             repository.Name,
				StorageNamespace: repository.StorageNamespace,
			}
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
		if _, ok := byName[name]; !ok {
			byName[name] = repositoryInfo{Name: name}
		}
	}

	names := make([]string, 0, len(byName))
	for name := range byName {
		names = append(names, name)
	}
	slices.Sort(names)
	repositories := make([]repositoryInfo, 0, len(names))
	for _, name := range names {
		repositories = append(repositories, byName[name])
	}
	return repositories, nil
}

// companionNames returns the names of companions in the order companionRepositories sorted them.
func companionNames(companions []companionRepository) []string {
	names := make([]string, len(companions))
	for i, companion := range companions {
		names[i] = companion.Name
	}
	return names
}

// startPolicyRefresher keeps the statements of the policy set in step with the repositories the
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
func startPolicyRefresher(ctx context.Context, deps cmdpkg.ServerDeps, guardGroups []string, interval time.Duration) <-chan struct{} {
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
			repositories, err := knownRepositories(ctx, deps.Catalog)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				deps.Logger.WithError(err).Warn("metadata search: cannot list the repositories for the companion-repository policies; retrying on the next refresh")
				continue
			}
			inForce, _, err := refreshPolicies(ctx, deps.AuthService, repositories, guardGroups, time.Now())
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
// statements, and attaches every policy of the set:
//
//	user   mds-indexer             member of MetadataSearchIndexers
//	group  MetadataSearchIndexers  carries mds-indexer and its chunks, never mds-metadata-repo-protect
//	policies mds-indexer[-<n>]              allow, attached to MetadataSearchIndexers
//	policies mds-metadata-repo-protect-<n>  deny, attached to guardGroups minus MetadataSearchIndexers
//	                                       (the deny has no base policy: it would carry no statement)
//
// guardGroups is the full list of groups that must carry the deny - the same list
// iceberg-reserved-prefix-protect uses, which cmd/lakefs-sso computes once in guardGroups() as
// datasets.PinsProtectGroups(datasets.immutable_guard_groups) and passes to both bootstraps (§8).
// It is an argument rather than something this package computes, because computing it here is how
// an operator's datasets.immutable_guard_groups once silently got no deny: the list must be the
// catalog's list, not a second copy of it. A nil or empty list attaches the deny to no group.
//
// repositories are the repositories the server knows, with their storage namespaces; every policy
// names its companion repositories explicitly (§8, and the package comment explains why a
// "*-metadata" pattern is not an option). A nil list is allowed and builds the read-only allow and
// no deny at all: it is what a server that knows no repository yet - and is configured with none -
// has to offer until its refresher learns about one.
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
func EnsurePrincipals(ctx context.Context, authService auth.Service, ts time.Time, guardGroups []string, repositories []repositoryInfo) (bool, error) {
	inForce, _, err := ensurePrincipals(ctx, authService, ts, guardGroups, repositories)
	return inForce, err
}

// ensurePrincipals is EnsurePrincipals and the policy set it wrote: the set is what the startup
// summary logs, and only the caller that built it knows its chunks after a stable plan (R3S-7) made
// chunk membership depend on what was stored before.
func ensurePrincipals(ctx context.Context, authService auth.Service, ts time.Time, guardGroups []string, repositories []repositoryInfo) (bool, *policySet, error) {
	if authService == nil {
		return false, nil, ErrNoAuthorizer
	}

	// Probe the policy backend before writing anything: BasicAuthService implements CreateUser, but
	// it stores the installation's single administrator under a fixed key, so creating mds-indexer
	// on it would rename that administrator and then fail at the first policy call. Probing here
	// also means a degraded installation is left exactly as it was.
	available, err := policyBackendAvailable(ctx, authService)
	if err != nil || !available {
		return false, nil, err
	}

	if _, err := authService.CreateUser(ctx, &model.User{Username: UserName, CreatedAt: ts}); err != nil &&
		!errors.Is(err, auth.ErrAlreadyExists) {
		return false, nil, fmt.Errorf("mds bootstrap: create user %s: %w", UserName, err)
	}
	if _, err := authService.CreateGroup(ctx, &model.Group{DisplayName: GroupName, CreatedAt: ts}); err != nil &&
		!errors.Is(err, auth.ErrAlreadyExists) {
		return false, nil, fmt.Errorf("mds bootstrap: create group %s: %w", GroupName, err)
	}
	if err := authService.AddUserToGroup(ctx, UserName, GroupName); err != nil &&
		!errors.Is(err, auth.ErrAlreadyExists) {
		return false, nil, fmt.Errorf("mds bootstrap: add user %s to group %s: %w", UserName, GroupName, err)
	}

	// Create or repair the whole policy set, attach every policy to the groups it belongs to, and
	// remove chunks that are no longer needed. Attaching inside the same repair is what lets the
	// refresher attach a new chunk when the repository set grows past a chunk boundary.
	inForce, policies, err := refreshPolicies(ctx, authService, repositories, guardGroups, ts)
	if err != nil || !inForce {
		return false, policies, err
	}
	return true, policies, nil
}

// policySet is the complete set of policies EnsurePrincipals writes for one repository list: the
// indexer allow policies (a base policy plus zero or more numbered chunks) and the metadata-repo
// deny policies (zero or more numbered chunks; the deny has no repository-independent statement, so
// it has no base policy).
type policySet struct {
	indexer []*model.Policy
	protect []*model.Policy
}

// names returns the names of every policy of the set, in the order it was built.
func (s *policySet) names() []string {
	names := make([]string, 0, len(s.indexer)+len(s.protect))
	for _, policy := range s.indexer {
		names = append(names, policy.DisplayName)
	}
	for _, policy := range s.protect {
		names = append(names, policy.DisplayName)
	}
	return names
}

// refreshPolicies creates or repairs the policy set of §8 for repositories, removes chunk policies
// that are no longer needed, and reports whether the set is in force. It is the part of
// EnsurePrincipals that the refresher repeats while the server runs (startPolicyRefresher): the
// user, the group and the attachments do not depend on the repository list, but the statements do,
// so a repository that appears after startup only needs the statements rewritten - and ensurePolicy
// rewrites them only when they actually differ, which is why an unchanged repository set costs a
// handful of reads and no write.
//
// The chunk plan it writes is the stable plan of stableCompanionChunks: it is derived from the
// chunks that are already stored, so an added repository does not move an existing companion to
// another chunk policy (R3S-7).
func refreshPolicies(ctx context.Context, authService auth.Service, repositories []repositoryInfo, guardGroups []string, ts time.Time) (bool, *policySet, error) {
	companions := companionRepositories(repositories)
	indexerChunks, err := existingCompanionChunks(ctx, authService, IndexerPolicyName)
	if err != nil {
		if policyBackendUnavailable(err) {
			logPrincipalsNotCreated(ctx, err)
			return false, nil, nil
		}
		return false, nil, fmt.Errorf("mds bootstrap: read the %s chunks: %w", IndexerPolicyName, err)
	}
	protectChunks, err := existingCompanionChunks(ctx, authService, MetadataRepoProtectPolicyName)
	if err != nil {
		if policyBackendUnavailable(err) {
			logPrincipalsNotCreated(ctx, err)
			return false, nil, nil
		}
		return false, nil, fmt.Errorf("mds bootstrap: read the %s chunks: %w", MetadataRepoProtectPolicyName, err)
	}
	policies := &policySet{
		indexer: indexerChunkPolicies(stableCompanionChunks(companions, indexerChunks), ts),
		protect: protectChunkPolicies(stableCompanionChunks(companions, protectChunks), ts),
	}
	desired := make(map[string]struct{}, len(policies.indexer)+len(policies.protect))
	for _, policy := range append(append([]*model.Policy(nil), policies.indexer...), policies.protect...) {
		inForce, err := ensurePolicy(ctx, authService, policy)
		if err != nil || !inForce {
			return false, policies, err
		}
		desired[policy.DisplayName] = struct{}{}
	}
	for _, base := range []string{IndexerPolicyName, MetadataRepoProtectPolicyName} {
		if err := removeStalePolicyChunks(ctx, authService, base, desired); err != nil {
			if policyBackendUnavailable(err) {
				logPrincipalsNotCreated(ctx, err)
				return false, policies, nil
			}
			return false, policies, err
		}
	}
	inForce, err := attachPolicies(ctx, authService, policies, guardGroups)
	if err != nil || !inForce {
		return false, policies, err
	}
	return true, policies, nil
}

// attachPolicies attaches every indexer policy to MetadataSearchIndexers and every deny policy to
// the caller's guard groups. It is idempotent (auth.ErrAlreadyExists is success), so the refresher
// can use it to attach a chunk created after a repository set grew past a chunk boundary. The
// indexer policies go to their own group only: attaching them to a base group would grant every
// member of that group the companion repositories' write path. The deny policies go to the guard
// list the caller supplied - the same base groups plus datasets.immutable_guard_groups that
// datasets-pins-protect and iceberg-reserved-prefix-protect cover (§8) - minus
// MetadataSearchIndexers: the indexer is the one principal that must be able to write a companion
// repository, so the deny must never reach it.
//
// An attach purges the auth service's effective-policy cache, exactly as a write and a delete do
// (R3S-6). pkg/auth.APIAuthService caches a user's effective policies and does not purge that cache
// in AttachPolicyToGroup, so without the purge a chunk created by this refresh (and a purge is a
// no-op for a service without a cache) would not decide anything until the cache entry expired: the
// indexer's new grant would be missing (fails closed, loudly) and, worse, a new companion
// repository's deny would be missing (fails open). Purging once after all attachments is enough and
// costs one cache scan per refresh; it is a no-op while nothing was attached.
func attachPolicies(ctx context.Context, authService auth.Service, policies *policySet, guardGroups []string) (bool, error) {
	attached := false
	for _, policy := range policies.indexer {
		err := authService.AttachPolicyToGroup(ctx, policy.DisplayName, GroupName)
		switch {
		case err == nil:
			attached = true
		case errors.Is(err, auth.ErrAlreadyExists):
		case policyBackendUnavailable(err):
			logPrincipalsNotCreated(ctx, err)
			return false, nil
		default:
			return false, fmt.Errorf("mds bootstrap: attach policy %s to group %s: %w", policy.DisplayName, GroupName, err)
		}
	}
	for _, group := range metadataRepoProtectGroups(ctx, guardGroups) {
		for _, policy := range policies.protect {
			err := authService.AttachPolicyToGroup(ctx, policy.DisplayName, group)
			switch {
			case err == nil:
				attached = true
			case errors.Is(err, auth.ErrAlreadyExists):
			case errors.Is(err, auth.ErrNotFound):
				logging.FromContext(ctx).WithField("group", group).
					Debug("mds bootstrap: group is not part of this installation's template; skipping metadata-repo deny attachment")
			case policyBackendUnavailable(err):
				logPrincipalsNotCreated(ctx, err)
				return false, nil
			default:
				return false, fmt.Errorf("mds bootstrap: attach policy %s to group %s: %w", policy.DisplayName, group, err)
			}
		}
	}
	if attached {
		purgePolicyCache(authService)
	}
	return true, nil
}

// removeStalePolicyChunks deletes the numbered chunk policies of base that desired does not name.
// The repository set can shrink (a repository is deleted, or a legacy *-metadata repository turns
// out not to be a companion), and a chunk that still named a deleted companion would keep both its
// grant and its deny alive. Only names isOwnedPolicyName accepts are considered: an operator policy
// that merely shares the base prefix is never deleted.
//
// It runs after every chunk of the new plan has been written (ensurePolicy), so a name that moved
// between chunks is covered by its new policy before its old one is deleted. A name a chunk carries
// but the plan does not - a stale base deny policy of the older, unchunked shape, for instance - is
// deleted here too, because it is not in desired.
func removeStalePolicyChunks(ctx context.Context, authService auth.Service, base string, desired map[string]struct{}) error {
	policies, err := listOwnedPolicies(ctx, authService, base)
	if err != nil {
		return fmt.Errorf("mds bootstrap: list policies with prefix %s: %w", base, err)
	}
	for _, policy := range policies {
		if _, ok := desired[policy.DisplayName]; ok {
			continue
		}
		if err := authService.DeletePolicy(ctx, policy.DisplayName); err != nil && !errors.Is(err, auth.ErrNotFound) {
			return fmt.Errorf("mds bootstrap: delete stale policy %s: %w", policy.DisplayName, err)
		}
		purgePolicyCache(authService)
		logging.FromContext(ctx).WithField("policy", policy.DisplayName).
			Warn("mds bootstrap: removed a stale metadata search policy chunk")
	}
	return nil
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
		// An external auth service (pkg/auth.APIAuthService) caches every user's effective
		// policies and does not purge that cache in WritePolicy. Without this, the one-minute
		// refresh window would really be one minute plus auth.cache.ttl (20s) and its jitter.
		purgePolicyCache(authService)
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
		purgePolicyCache(authService)
		return true, nil
	case policyBackendUnavailable(err):
		logPrincipalsNotCreated(ctx, err)
		return false, nil
	default:
		return false, fmt.Errorf("mds bootstrap: read policy %s: %w", policy.DisplayName, err)
	}
}

// purgePolicyCache clears the auth service's per-user effective-policy cache after a policy
// changes, when the service has one. A service without a cache - a stub in a test, or a
// BasicAuthService that never reaches this path - keeps working because a nil Cache is a no-op.
func purgePolicyCache(authService auth.Service) {
	policyCache := authService.Cache()
	if policyCache == nil {
		return
	}
	policyCache.PurgePolicies()
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
// service cannot manage policies, so the mds-indexer user, its group and the policy set were not
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
		" policy sets are NOT created, so metadata search indexing is not authorized and " +
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
