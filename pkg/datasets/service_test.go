package datasets

// service_test.go covers the control-plane service of this phase (DATASETS_DESIGN.md §6.1,
// §6.6, §10.1 create/get/list/delete/validate and §9.2 access grants). The fixture follows
// §15.1: a real catalog on the in-memory block adapter and KV store, and the ACL auth
// service, which is the implementation the lakefs-sso binary runs.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/contrib/auth/acl"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/auth/model"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/permissions"
)

const testServiceUser = "datasets-service-user"

// serviceFixture is the §15.1 fixture plus the service, the ACL auth service and a couple of
// users: the default caller is a dataset admin with the ACL base grants.
type serviceFixture struct {
	*datasetFixture
	svc         *Service
	cfg         *Config
	authService *acl.AuthService
	ts          time.Time
}

func newServiceFixture(t *testing.T) *serviceFixture {
	t.Helper()
	f := newDatasetFixture(t)
	ctx := t.Context()

	authService := acl.NewAuthService(
		f.kvStore,
		crypt.NewSecretStore([]byte("datasets-service-test-secret")),
		authparams.ServiceCache{Enabled: false},
	)
	ts := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	require.NoError(t, acl.CreateACLBaseGroups(ctx, authService, ts))
	require.NoError(t, Bootstrap(ctx, authService, ts))

	cfg := &Config{
		Enabled:                true,
		StorageNamespacePrefix: "mem://datasets",
	}
	cfg.setDefaults()
	svc, err := NewService(cfg, f.kvStore, f.cat, authService)
	require.NoError(t, err)

	fixture := &serviceFixture{datasetFixture: f, svc: svc, cfg: cfg, authService: authService, ts: ts}
	fixture.addUser(t, testServiceUser, acl.AdminsGroup, DatasetAdminsGroup)
	// Tests that drive the internal source walks directly (estimateSourceObjects, publish) read the
	// sources as the default admin caller, the way resolveAuthorized sets it up for a request.
	fixture.ctx = withSourceReader(fixture.ctx, svc.authorizer.SourceReader(fixture.caller()))
	return fixture
}

// caller builds the caller of the default test user.
func (f *serviceFixture) caller() Caller { return Caller{Username: testServiceUser} }

// callerFor builds the caller of a named principal.
func (f *serviceFixture) callerFor(username string) Caller { return Caller{Username: username} }

// addUser creates a user and, when groups are given, adds it to them.
func (f *serviceFixture) addUser(t *testing.T, username string, groups ...string) {
	t.Helper()
	ctx := t.Context()
	_, err := f.authService.CreateUser(ctx, &model.User{Username: username})
	if !errors.Is(err, auth.ErrAlreadyExists) {
		require.NoError(t, err)
	}
	for _, group := range groups {
		require.NoError(t, f.authService.AddUserToGroup(ctx, username, group))
	}
}

// grant attaches an inline policy to a user, for tests that need a narrowly privileged
// principal (a lister without read, a creator without source read).
func (f *serviceFixture) grant(t *testing.T, username, policyName string, statements model.Statements) {
	t.Helper()
	ctx := t.Context()
	policy := &model.Policy{CreatedAt: f.ts, DisplayName: policyName, Statement: statements}
	require.NoError(t, f.authService.WritePolicy(ctx, policy, false))
	require.NoError(t, f.authService.AttachPolicyToUser(ctx, policyName, username))
}

// sourceRepo creates a regular (non-dataset) repository with the given objects and returns
// the commit that holds them.
func (f *serviceFixture) sourceRepo(t *testing.T, name string, objects map[string]string) graveler.CommitID {
	t.Helper()
	return f.sourceRepoOnStorage(t, name, "", objects)
}

// sourceRepoOnStorage is sourceRepo with an explicit storage ID, for the 422 case.
func (f *serviceFixture) sourceRepoOnStorage(t *testing.T, name, storageID string, objects map[string]string) graveler.CommitID {
	t.Helper()
	_, err := f.cat.CreateRepository(f.ctx, name, storageID, "mem://"+name+"-ns", "main", false)
	require.NoError(t, err, "create source repository %s", name)
	items := make([]testObject, 0, len(objects))
	// Deterministic order, so the commit of a test does not depend on map iteration.
	for _, key := range sortedKeys(objects) {
		items = append(items, testObject{key: key, content: []byte(objects[key])})
	}
	return f.commitObjects(t, name, "main", items...)
}

// sortedKeys returns the keys of objects in lexicographic order.
func sortedKeys(objects map[string]string) []string {
	keys := make([]string, 0, len(objects))
	for key := range objects {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// prefixDefinition builds a one-item definition for a prefix source.
func prefixDefinition(repo, commit, target, prefix string) Definition {
	return Definition{
		FormatVersion: FormatVersion,
		Data: []DataItem{{
			Target:     target,
			Repository: repo,
			Ref:        SourceRef{Type: RefTypeCommit, ID: commit},
			Type:       ItemTypePrefix,
			Prefix:     &PathSource{Path: prefix},
		}},
	}
}

// objectDefinition builds a one-item definition for an object source.
func objectDefinition(repo, commit, target, path string) Definition {
	return Definition{
		FormatVersion: FormatVersion,
		Data: []DataItem{{
			Target:     target,
			Repository: repo,
			Ref:        SourceRef{Type: RefTypeCommit, ID: commit},
			Type:       ItemTypeObject,
			Object:     &PathSource{Path: path},
		}},
	}
}

// withMetadata returns def with a metadata map, for the metadata filters of GET /datasets.
func withMetadata(def Definition, metadata map[string]any) Definition {
	def.Metadata = metadata
	return def
}

// TestServiceCreateAlwaysPublishesV1 is DOC-14: a create has no "empty dataset" mode. It also
// pins the repository setup that used to be the whole point of a create without a publish.
func TestServiceCreateAlwaysPublishesV1(t *testing.T) {
	f := newServiceFixture(t)
	commit := f.sourceRepo(t, "src", map[string]string{"data/customers/a.parquet": "a"})
	def := prefixDefinition("src", commit.String(), "customers", "data/customers/")

	view, err := f.svc.Create(f.ctx, f.caller(), CreateRequest{Name: "v1-set", Definition: def})
	require.NoError(t, err)
	require.Equal(t, "v1-set", view.Record.Name)
	require.Equal(t, DatasetStateActive, view.Record.State)
	require.Equal(t, 1, view.Record.LatestVersion, "every create publishes v1")
	require.NotNil(t, view.Version)
	require.Equal(t, 1, view.Version.Version)
	require.Equal(t, DefaultCreateMessage("v1-set"), view.Version.Message,
		"the default message names the dataset")

	versions, err := f.svc.store.ListVersions(f.ctx, "v1-set")
	require.NoError(t, err)
	require.Len(t, versions, 1)
	require.Equal(t, 1, versions[0].Version)

	// §6.1: the backing repository is read-only, defaults to "latest" and carries the
	// dataset metadata markers.
	repo, err := f.cat.GetRepository(f.ctx, "v1-set")
	require.NoError(t, err)
	require.True(t, repo.ReadOnly)
	require.Equal(t, DefaultBranch, repo.DefaultBranch)
	require.Contains(t, repo.StorageNamespace, "mem://datasets/v1-set-")
	require.NotEqual(t, "mem://datasets/v1-set", repo.StorageNamespace, "the ULID suffix is required")

	metadata, err := f.cat.GetRepositoryMetadata(f.ctx, "v1-set")
	require.NoError(t, err)
	require.Equal(t, "true", metadata[RepositoryMetadataIsDatasetKey])
	require.Equal(t, "1", metadata[RepositoryMetadataFormatKey])

	// §9.4: the immutability guard exists and is attached to the base groups.
	policy, err := f.authService.GetPolicy(f.ctx, ImmutabilityPolicyName("v1-set"))
	require.NoError(t, err)
	require.Equal(t, ImmutabilityPolicyName("v1-set"), policy.DisplayName)

	// §9.5 note: the aggregate read policy lists the new dataset.
	allRead, err := f.authService.GetPolicy(f.ctx, DatasetsAllReadPolicyName)
	require.NoError(t, err)
	require.Contains(t, statementResources(allRead.Statement), permissions.RepoArn("v1-set"))
	require.Contains(t, statementResources(allRead.Statement), DatasetArn("v1-set"))
}

func TestServiceCreateWithPublish(t *testing.T) {
	f := newServiceFixture(t)
	commit := f.sourceRepo(t, "src", map[string]string{
		"data/customers/a.parquet": "alpha",
		"data/customers/b.parquet": "bravo!",
		"data/other/c.parquet":     "charlie",
	})
	def := withMetadata(prefixDefinition("src", commit.String(), "customers", "data/customers/"), map[string]any{"tier": "gold"})

	view, err := f.svc.Create(f.ctx, f.caller(), CreateRequest{
		Name:       "customer-360",
		Definition: def,
		Message:    "initial dataset",
	})
	require.NoError(t, err)
	require.Equal(t, DatasetStateActive, view.Record.State)
	require.Equal(t, 1, view.Record.LatestVersion, "publish produces v1")
	require.NotNil(t, view.Version)
	require.Equal(t, 1, view.Version.Version)
	require.Equal(t, "initial dataset", view.Version.Message)
	require.NotEmpty(t, view.Version.CommitID)
	require.NotEmpty(t, view.Version.MetaRangeID)
	require.Equal(t, int64(2), view.Version.Stats.Objects)
	require.Equal(t, int64(len("alpha")+len("bravo!")), view.Version.Stats.Bytes)
	require.Equal(t, []ItemStat{{Target: "customers", Objects: 2, Bytes: int64(len("alpha") + len("bravo!"))}}, view.Version.Stats.Items)

	// §6.1: version v1 is the tag v1 of the dataset repository.
	tagCommit, err := f.cat.GetTag(f.ctx, "customer-360", VersionTag(1))
	require.NoError(t, err)
	require.Equal(t, view.Version.CommitID, tagCommit)

	// Goal G3: the dataset serves the source bytes zero-copy through the source address.
	require.Equal(t, []byte("bravo!"), f.readDatasetObject(t, "customer-360", "latest", "customers/b.parquet"))

	// §8.2: the source commit is pinned by a hidden branch and a KV record.
	pinBranch, err := f.refMgr.GetBranch(f.ctx, f.repositoryRecord(t, "src"), graveler.BranchID(PinBranch(commit.String())))
	require.NoError(t, err, "pin branch exists in the source repository")
	require.True(t, pinBranch.Hidden)
	require.Equal(t, commit, pinBranch.CommitID)
	pin, _, err := f.svc.store.GetPin(f.ctx, "src", commit.String())
	require.NoError(t, err)
	require.Equal(t, []string{PinReference("customer-360", 1)}, pin.References)
}

func TestServiceCreateDuplicateName(t *testing.T) {
	f := newServiceFixture(t)
	commit := f.sourceRepo(t, "src", map[string]string{"a.txt": "a"})
	def := objectDefinition("src", commit.String(), "a.txt", "a.txt")

	_, err := f.svc.Create(f.ctx, f.caller(), CreateRequest{Name: "dup-set", Definition: def})
	require.NoError(t, err)

	t.Run("dataset", func(t *testing.T) {
		_, err := f.svc.Create(f.ctx, f.caller(), CreateRequest{Name: "dup-set", Definition: def})
		require.ErrorIs(t, err, ErrNameTaken)
	})

	t.Run("repository", func(t *testing.T) {
		// SEC-7: the answer is the same generic 409 for a dataset and for a plain repository,
		// so it never confirms that a repository the caller cannot read exists.
		_, err := f.cat.CreateRepository(f.ctx, "repo-taken", "", "mem://repo-taken-ns", "main", false)
		require.NoError(t, err)
		_, err = f.svc.Create(f.ctx, f.caller(), CreateRequest{Name: "repo-taken", Definition: def})
		require.ErrorIs(t, err, ErrNameTaken)
		require.NotContains(t, err.Error(), "repository")
		require.NotContains(t, err.Error(), "dataset")
		require.Equal(t, "name unavailable: \"repo-taken\"", err.Error())
	})
}

func TestServiceGet(t *testing.T) {
	f := newServiceFixture(t)
	commit := f.sourceRepo(t, "src", map[string]string{"a.txt": "a"})
	def := objectDefinition("src", commit.String(), "a.txt", "a.txt")
	_, err := f.svc.Create(f.ctx, f.caller(), CreateRequest{Name: "get-set", Definition: def})
	require.NoError(t, err)

	view, err := f.svc.Get(f.ctx, f.caller(), "get-set")
	require.NoError(t, err)
	require.Equal(t, "get-set", view.Record.Name)
	require.NotNil(t, view.Version)
	require.Equal(t, 1, view.Version.Version)

	t.Run("missing", func(t *testing.T) {
		_, err := f.svc.Get(f.ctx, f.caller(), "no-such-set")
		require.ErrorIs(t, err, ErrDatasetNotFound)
	})

	t.Run("forbidden", func(t *testing.T) {
		f.addUser(t, "outsider")
		_, err := f.svc.Get(f.ctx, f.callerFor("outsider"), "get-set")
		require.ErrorIs(t, err, ErrPermissionDenied)
		var denied *PermissionError
		require.ErrorAs(t, err, &denied)
		require.Equal(t, http.StatusForbidden, denied.HTTPStatusCode())
	})
}

func TestServiceList(t *testing.T) {
	f := newServiceFixture(t)
	commit := f.sourceRepo(t, "src", map[string]string{"a.txt": "a"})
	base := objectDefinition("src", commit.String(), "a.txt", "a.txt")

	// Three datasets: two under the "alpha-" prefix with different metadata, one "beta-".
	for _, ds := range []struct {
		name     string
		metadata map[string]any
	}{
		{"alpha-one", map[string]any{"tier": "gold"}},
		{"alpha-two", map[string]any{"tier": "silver", "owner": "data-platform"}},
		{"beta-three", map[string]any{"tier": "gold"}},
	} {
		def := withMetadata(base, ds.metadata)
		def.Description = "dataset " + ds.name
		_, err := f.svc.Create(f.ctx, f.caller(), CreateRequest{Name: ds.name, Definition: def})
		require.NoError(t, err)
	}

	t.Run("all, name ordered", func(t *testing.T) {
		result, err := f.svc.List(f.ctx, f.caller(), ListRequest{})
		require.NoError(t, err)
		require.Equal(t, []string{"alpha-one", "alpha-two", "beta-three"}, namesOf(result.Datasets))
		require.False(t, result.HasMore)
	})

	t.Run("prefix", func(t *testing.T) {
		result, err := f.svc.List(f.ctx, f.caller(), ListRequest{Prefix: "alpha-"})
		require.NoError(t, err)
		require.Equal(t, []string{"alpha-one", "alpha-two"}, namesOf(result.Datasets))
	})

	t.Run("search", func(t *testing.T) {
		result, err := f.svc.List(f.ctx, f.caller(), ListRequest{Search: "TWO"})
		require.NoError(t, err)
		require.Equal(t, []string{"alpha-two"}, namesOf(result.Datasets))
	})

	t.Run("search on description", func(t *testing.T) {
		result, err := f.svc.List(f.ctx, f.caller(), ListRequest{Search: "dataset beta"})
		require.NoError(t, err)
		require.Equal(t, []string{"beta-three"}, namesOf(result.Datasets))
	})

	t.Run("metadata filter", func(t *testing.T) {
		result, err := f.svc.List(f.ctx, f.caller(), ListRequest{Metadata: map[string][]string{"tier": {"gold"}}})
		require.NoError(t, err)
		require.Equal(t, []string{"alpha-one", "beta-three"}, namesOf(result.Datasets))
	})

	t.Run("metadata filter and prefix", func(t *testing.T) {
		result, err := f.svc.List(f.ctx, f.caller(), ListRequest{
			Prefix:   "alpha-",
			Metadata: map[string][]string{"tier": {"gold"}},
		})
		require.NoError(t, err)
		require.Equal(t, []string{"alpha-one"}, namesOf(result.Datasets))
	})

	t.Run("metadata filter without match", func(t *testing.T) {
		result, err := f.svc.List(f.ctx, f.caller(), ListRequest{Metadata: map[string][]string{"tier": {"bronze"}}})
		require.NoError(t, err)
		require.Empty(t, result.Datasets)
	})

	// Review m1: a repeated metadata key keeps every value, so the filter matches any of
	// them (OR) instead of silently narrowing to the first one.
	t.Run("repeated metadata values match any of them", func(t *testing.T) {
		result, err := f.svc.List(f.ctx, f.caller(), ListRequest{Metadata: map[string][]string{"tier": {"gold", "silver"}}})
		require.NoError(t, err)
		require.Equal(t, []string{"alpha-one", "alpha-two", "beta-three"}, namesOf(result.Datasets))
	})

	// Different keys are still ANDed, and a repeated key stays OR inside that AND.
	t.Run("metadata filters on different keys are ANDed", func(t *testing.T) {
		result, err := f.svc.List(f.ctx, f.caller(), ListRequest{
			Metadata: map[string][]string{"tier": {"gold", "silver"}, "owner": {"data-platform"}},
		})
		require.NoError(t, err)
		require.Equal(t, []string{"alpha-two"}, namesOf(result.Datasets))

		result, err = f.svc.List(f.ctx, f.caller(), ListRequest{
			Metadata: map[string][]string{"tier": {"gold", "silver"}, "owner": {"missing"}},
		})
		require.NoError(t, err)
		require.Empty(t, result.Datasets)
	})

	t.Run("pagination", func(t *testing.T) {
		first, err := f.svc.List(f.ctx, f.caller(), ListRequest{Amount: 2})
		require.NoError(t, err)
		require.Equal(t, []string{"alpha-one", "alpha-two"}, namesOf(first.Datasets))
		require.True(t, first.HasMore)
		require.Equal(t, "alpha-two", first.NextOffset)
		require.Equal(t, 2, first.MaxPerPage)

		second, err := f.svc.List(f.ctx, f.caller(), ListRequest{Amount: 2, After: first.NextOffset})
		require.NoError(t, err)
		require.Equal(t, []string{"beta-three"}, namesOf(second.Datasets))
		require.False(t, second.HasMore)
		require.Empty(t, second.NextOffset)
	})

	t.Run("results are filtered by ReadDataset", func(t *testing.T) {
		f.addUser(t, "lister")
		f.grant(t, "lister", "lister-list-only", model.Statements{{
			Effect:   model.StatementEffectAllow,
			Action:   []string{ActionListDatasets},
			Resource: permissions.All,
		}})
		result, err := f.svc.List(f.ctx, f.callerFor("lister"), ListRequest{})
		require.NoError(t, err)
		require.Empty(t, result.Datasets, "listing must not reveal datasets the caller cannot read")

		f.grant(t, "lister", "lister-read-one", model.Statements{{
			Effect:   model.StatementEffectAllow,
			Action:   []string{ActionReadDataset},
			Resource: DatasetArn("alpha-one"),
		}})
		result, err = f.svc.List(f.ctx, f.callerFor("lister"), ListRequest{})
		require.NoError(t, err)
		require.Equal(t, []string{"alpha-one"}, namesOf(result.Datasets))
	})
}

func namesOf(views []*DatasetView) []string {
	names := make([]string, len(views))
	for i, view := range views {
		names[i] = view.Record.Name
	}
	return names
}

func statementResources(statements model.Statements) []string {
	resources := make([]string, 0, len(statements))
	for _, statement := range statements {
		resources = append(resources, statement.Resource)
	}
	return resources
}

func TestServiceDeleteReleasesPinsAndPolicies(t *testing.T) {
	f := newServiceFixture(t)
	commit := f.sourceRepo(t, "src", map[string]string{"data/a.txt": "alpha"})
	def := prefixDefinition("src", commit.String(), "a", "data/")
	created, err := f.svc.Create(f.ctx, f.caller(), CreateRequest{Name: "gone-set", Definition: def})
	require.NoError(t, err)

	f.addUser(t, "consumer")
	_, err = f.svc.GrantAccess(f.ctx, f.caller(), "gone-set", PrincipalTypeUser, "consumer")
	require.NoError(t, err)

	// Preconditions: the pin, the repository, the policies and the version exist.
	_, _, err = f.svc.store.GetPin(f.ctx, "src", commit.String())
	require.NoError(t, err)
	_, err = f.cat.GetRepository(f.ctx, "gone-set")
	require.NoError(t, err)
	_, err = f.authService.GetPolicy(f.ctx, ConsumerReadPolicyName("gone-set"))
	require.NoError(t, err)
	_, err = f.authService.GetPolicy(f.ctx, ImmutabilityPolicyName("gone-set"))
	require.NoError(t, err)
	_, err = f.svc.store.GetVersion(f.ctx, "gone-set", 1)
	require.NoError(t, err)

	oldNamespace := created.Record.StorageNS
	require.NoError(t, f.svc.Delete(f.ctx, f.caller(), "gone-set"))

	t.Run("pins released", func(t *testing.T) {
		_, _, err := f.svc.store.GetPin(f.ctx, "src", commit.String())
		require.ErrorIs(t, err, kv.ErrNotFound)
		_, err = f.refMgr.GetBranch(f.ctx, f.repositoryRecord(t, "src"), graveler.BranchID(PinBranch(commit.String())))
		require.Error(t, err, "the hidden pin branch is gone")
	})

	t.Run("repository deleted", func(t *testing.T) {
		_, err := f.cat.GetRepository(f.ctx, "gone-set")
		require.ErrorIs(t, err, graveler.ErrNotFound)
	})

	t.Run("policies deleted", func(t *testing.T) {
		_, err := f.authService.GetPolicy(f.ctx, ConsumerReadPolicyName("gone-set"))
		require.ErrorIs(t, err, auth.ErrNotFound)
		_, err = f.authService.GetPolicy(f.ctx, ImmutabilityPolicyName("gone-set"))
		require.ErrorIs(t, err, auth.ErrNotFound)
		policies, _, err := f.authService.ListUserPolicies(f.ctx, "consumer", &model.PaginationParams{Amount: -1})
		require.NoError(t, err)
		for _, policy := range policies {
			require.NotEqual(t, ConsumerReadPolicyName("gone-set"), policy.DisplayName)
		}
	})

	t.Run("records deleted", func(t *testing.T) {
		_, _, err := f.svc.store.GetDataset(f.ctx, "gone-set")
		require.ErrorIs(t, err, kv.ErrNotFound)
		versions, err := f.svc.store.ListVersions(f.ctx, "gone-set")
		require.NoError(t, err)
		require.Empty(t, versions)
		grants, err := f.svc.store.ListGrants(f.ctx, "gone-set")
		require.NoError(t, err)
		require.Empty(t, grants)
	})

	t.Run("aggregate policy forgets the dataset", func(t *testing.T) {
		policy, err := f.authService.GetPolicy(f.ctx, DatasetsAllReadPolicyName)
		require.NoError(t, err)
		require.NotContains(t, statementResources(policy.Statement), permissions.RepoArn("gone-set"))
	})

	t.Run("name can be reused", func(t *testing.T) {
		view, err := f.svc.Create(f.ctx, f.caller(), CreateRequest{Name: "gone-set", Definition: def})
		require.NoError(t, err)
		require.Equal(t, 1, view.Record.LatestVersion)
		require.NotEqual(t, oldNamespace, view.Record.StorageNS, "a re-created dataset gets a fresh storage namespace")
		tagCommit, err := f.cat.GetTag(f.ctx, "gone-set", VersionTag(1))
		require.NoError(t, err)
		require.Equal(t, view.Version.CommitID, tagCommit)
	})

	t.Run("deleting an unknown dataset is a 404", func(t *testing.T) {
		// Deleting a name with no record is a 404; the individual steps of a delete that
		// started and crashed tolerate not-found and are finished by the reconciler (§6.6).
		err := f.svc.Delete(f.ctx, f.caller(), "never-existed")
		require.ErrorIs(t, err, ErrDatasetNotFound)
	})
}

// TestServiceDeleteRejectsLivePublish covers BUG-9: a delete racing a live publish is a 409,
// while a stale publishing record may be deleted and its resources released.
func TestServiceDeleteRejectsLivePublish(t *testing.T) {
	const dataset = "delete-during-publish"
	f := newServiceFixture(t)
	commit := f.sourceRepo(t, "src-delete-publish", map[string]string{"data/a.parquet": "a"})
	_, err := f.svc.Create(f.ctx, f.caller(), CreateRequest{
		Name:       dataset,
		Definition: prefixDefinition("src-delete-publish", commit.String(), "data", "data/"),
	})
	require.NoError(t, err)

	current := f.datasetRecord(t, dataset)
	live := *current
	live.State = DatasetStatePublishing
	live.PendingVersion = 2
	live.UpdatedAt = f.svc.now()
	require.NoError(t, f.svc.store.PutDatasetIf(f.ctx, &live, current, nil))

	err = f.svc.Delete(f.ctx, f.caller(), dataset)
	require.ErrorIs(t, err, ErrPublishInProgress)
	require.ErrorIs(t, err, ErrDatasetConflict)
	_, err = f.cat.GetRepository(f.ctx, dataset)
	require.NoError(t, err, "the repository survives a rejected delete")
	require.Equal(t, DatasetStatePublishing, f.datasetRecord(t, dataset).State)

	stale := live
	stale.UpdatedAt = f.svc.now().Add(-f.cfg.PublishTimeout - time.Minute)
	require.NoError(t, f.svc.store.PutDatasetIf(f.ctx, &stale, &live, nil))
	require.NoError(t, f.svc.Delete(f.ctx, f.caller(), dataset))
	_, err = f.cat.GetRepository(f.ctx, dataset)
	require.True(t, isNotFound(err), "a stale publishing record is deleted")
}

// TestServiceCreateFailureRollsBackInline covers BUG-10: a create that fails after the
// repository and the guard were written removes them and the record inline, so the name is
// immediately reusable instead of staying "creating" for a reconcile timeout.
func TestServiceCreateFailureRollsBackInline(t *testing.T) {
	const dataset = "create-rollback"
	f := newServiceFixture(t)
	commit := f.sourceRepo(t, "src-create-rollback", map[string]string{"data/a.parquet": "a"})
	def := prefixDefinition("src-create-rollback", commit.String(), "data", "data/")

	f.svc.publishFn = func(context.Context, *Materializer, PublishRequest) (*PublishResult, error) {
		return nil, errors.New("metarange write failed")
	}
	_, err := f.svc.Create(f.ctx, f.caller(), CreateRequest{Name: dataset, Definition: def})
	require.Error(t, err)

	_, _, err = f.svc.store.GetDataset(f.ctx, dataset)
	require.ErrorIs(t, err, kv.ErrNotFound)
	_, err = f.cat.GetRepository(f.ctx, dataset)
	require.True(t, isNotFound(err), "the repository of a failed create is removed")
	_, err = f.authService.GetPolicy(f.ctx, ImmutabilityPolicyName(dataset))
	require.ErrorIs(t, err, auth.ErrNotFound, "the guard of a failed create is removed")

	f.svc.publishFn = publishWithMaterializer
	view, err := f.svc.Create(f.ctx, f.caller(), CreateRequest{Name: dataset, Definition: def})
	require.NoError(t, err)
	require.Equal(t, 1, view.Record.LatestVersion)
}

// TestServiceInjectedClock pins BUG-13: publish timestamps, the pin manager's record timestamps
// and the reconcile lease all follow the service clock instead of time.Now, so fake-clock tests
// can drive staleness without sleeping.
func TestServiceInjectedClock(t *testing.T) {
	const (
		dataset = "clocked"
		source  = "src-clocked"
	)
	fixed := time.Date(2031, 3, 4, 5, 6, 7, 0, time.UTC)
	now := fixed
	f := newServiceFixture(t)
	f.svc.setClock(func() time.Time { return now })

	// The create publishes v1: both the dataset record and the version record must use the
	// injected clock, not the wall clock.
	commit := f.sourceRepo(t, source, map[string]string{"data/a.parquet": "a"})
	view, err := f.svc.Create(f.ctx, f.caller(), CreateRequest{
		Name:       dataset,
		Definition: prefixDefinition(source, commit.String(), "data", "data/"),
		Message:    "v1",
	})
	require.NoError(t, err)
	require.True(t, view.Record.CreatedAt.Equal(fixed), "create writes the injected clock")
	require.True(t, view.Record.UpdatedAt.Equal(fixed), "publish writes the injected clock")
	require.NotNil(t, view.Version)
	require.True(t, view.Version.PublishedAt.Equal(fixed), "the version timestamp uses the injected clock")
	require.True(t, datasetPinsInclude(t, f, dataset, 1), "v1 pins its source")

	// A later publish uses the advanced fake clock for its timestamps.
	now = fixed.Add(90 * time.Minute)
	second := f.commitObjects(t, source, "main", testObject{key: "data/a.parquet", content: []byte("b")})
	outcome, err := f.svc.PublishVersion(f.ctx, f.caller(), dataset, PublishVersionRequest{
		Definition: prefixDefinition(source, second.String(), "data", "data/"),
		Message:    "v2",
	})
	require.NoError(t, err)
	require.NotNil(t, outcome.Version)
	require.True(t, outcome.Version.PublishedAt.Equal(now), "v2 uses the advanced injected clock")
	require.True(t, f.datasetRecord(t, dataset).UpdatedAt.Equal(now), "the head uses the advanced injected clock")
	require.True(t, datasetPinsInclude(t, f, dataset, 2), "v2 pins its source")

	// The pin manager's own record writes (markDegraded and ClearDatasetDegraded) use the
	// same clock.
	now = fixed.Add(2 * time.Hour)
	marked, err := f.svc.pins.markDegraded(f.ctx, dataset)
	require.NoError(t, err)
	require.True(t, marked)
	require.True(t, f.datasetRecord(t, dataset).UpdatedAt.Equal(now), "markDegraded writes the injected clock")

	now = fixed.Add(3 * time.Hour)
	cleared, err := f.svc.pins.ClearDatasetDegraded(f.ctx, dataset)
	require.NoError(t, err)
	require.True(t, cleared)
	require.True(t, f.datasetRecord(t, dataset).UpdatedAt.Equal(now), "ClearDatasetDegraded writes the injected clock")

	// The reconcile lease is stamped and expires on the injected clock.
	now = fixed.Add(4 * time.Hour)
	acquired, err := f.svc.store.AcquireLease(f.ctx, "clocked-owner", time.Minute)
	require.NoError(t, err)
	require.True(t, acquired)
	lease, _, err := f.svc.store.GetLease(f.ctx)
	require.NoError(t, err)
	require.True(t, lease.ExpiresAt.Equal(now.Add(time.Minute)), "the lease expiry uses the injected clock")

	acquired, err = f.svc.store.AcquireLease(f.ctx, "other-owner", time.Minute)
	require.NoError(t, err)
	require.False(t, acquired, "a live lease is not taken over")

	now = now.Add(time.Minute + time.Second)
	acquired, err = f.svc.store.AcquireLease(f.ctx, "other-owner", time.Minute)
	require.NoError(t, err)
	require.True(t, acquired, "the lease expires on the injected clock")
	lease, _, err = f.svc.store.GetLease(f.ctx)
	require.NoError(t, err)
	require.Equal(t, "other-owner", lease.Owner)
	require.True(t, lease.ExpiresAt.Equal(now.Add(time.Minute)))
}

func TestServiceValidateReportsEveryProblem(t *testing.T) {
	f := newServiceFixture(t)
	def := Definition{
		FormatVersion: 2,
		Data: []DataItem{
			{
				Target: "a",
				// Missing repository, unknown ref type and missing ref id, no source
				// descriptor for an "object" item.
				Type: ItemTypeObject,
				Ref:  SourceRef{Type: "bogus"},
			},
			{
				Target:     "a/b",
				Repository: "src",
				Ref:        SourceRef{Type: RefTypeCommit, ID: "0123456789abcdef"},
				Type:       ItemTypePrefix,
				Prefix:     &PathSource{Path: "x"},
			},
			{
				Target:     ReservedTargetPrefix + "/x",
				Repository: "src",
				Ref:        SourceRef{Type: RefTypeCommit, ID: "0123456789abcdef"},
				Type:       ItemTypeObject,
				Object:     &PathSource{Path: ""},
			},
		},
	}

	_, err := f.svc.Validate(f.ctx, f.caller(), "problem-set", def)
	require.Error(t, err)
	var validation *ValidationError
	require.ErrorAs(t, err, &validation)
	require.GreaterOrEqual(t, len(validation.Problems), 5, "every problem is reported: %v", validation.Problems)

	paths := make([]string, 0, len(validation.Problems))
	messages := make([]string, 0, len(validation.Problems))
	for _, problem := range validation.Problems {
		paths = append(paths, problem.Path)
		messages = append(messages, problem.Message)
	}
	require.Contains(t, paths, "format_version")
	require.Contains(t, joinStrings(messages), "required for item type")
	require.Contains(t, joinStrings(messages), "reserved")
	require.Contains(t, joinStrings(messages), "overlaps")
}

func joinStrings(values []string) string {
	out := ""
	for _, value := range values {
		out += value + "\n"
	}
	return out
}

func TestServiceValidateSuccess(t *testing.T) {
	f := newServiceFixture(t)
	commit := f.sourceRepo(t, "src", map[string]string{
		"data/a.txt": "alpha",
		"data/b.txt": "bravo!",
	})
	def := prefixDefinition("src", "main", "out", "data/")
	def.Data[0].Ref.Type = RefTypeBranch
	def.Data[0].Ref.ID = "main"

	result, err := f.svc.Validate(f.ctx, f.caller(), "dry-run", def)
	require.NoError(t, err)
	require.Equal(t, RefTypeCommit, result.Definition.Data[0].Ref.Type, "branch refs are resolved to commits")
	require.Equal(t, commit.String(), result.Definition.Data[0].Ref.ID)
	require.Equal(t, int64(2), result.Stats.Objects)
	require.Equal(t, int64(len("alpha")+len("bravo!")), result.Stats.Bytes)
	require.Equal(t, "out", result.Stats.Items[0].Target)
}

func TestServiceValidateSourceProblems(t *testing.T) {
	f := newServiceFixture(t)
	commit := f.sourceRepo(t, "src", map[string]string{"data/a.txt": "alpha"})
	f.addUser(t, "no-source-read")
	f.grant(t, "no-source-read", "no-source-read-policy", model.Statements{{
		Effect:   model.StatementEffectAllow,
		Action:   []string{ActionReadDataset},
		Resource: permissions.All,
	}})

	t.Run("denied source read names the item", func(t *testing.T) {
		def := objectDefinition("src", commit.String(), "a.txt", "data/a.txt")
		_, err := f.svc.Validate(f.ctx, f.callerFor("no-source-read"), "dry-run", def)
		require.ErrorIs(t, err, ErrPermissionDenied)
		var denied *PermissionError
		require.ErrorAs(t, err, &denied)
		require.Contains(t, denied.Item, "data[0]")
	})

	t.Run("missing object", func(t *testing.T) {
		def := objectDefinition("src", commit.String(), "nope.txt", "no/such/key")
		_, err := f.svc.Validate(f.ctx, f.caller(), "dry-run", def)
		require.Error(t, err)
		var validation *ValidationError
		require.ErrorAs(t, err, &validation)
		require.Equal(t, "data[0].object.path", validation.Problems[0].Path)
	})

	t.Run("empty prefix", func(t *testing.T) {
		def := prefixDefinition("src", commit.String(), "empty", "nothing/here/")
		_, err := f.svc.Validate(f.ctx, f.caller(), "dry-run", def)
		require.Error(t, err)
		var validation *ValidationError
		require.ErrorAs(t, err, &validation)
		require.Equal(t, "data[0].prefix.path", validation.Problems[0].Path)

		f.cfg.AllowEmptyItems = true
		defer func() { f.cfg.AllowEmptyItems = false }()
		result, err := f.svc.Validate(f.ctx, f.caller(), "dry-run", def)
		require.NoError(t, err)
		require.Zero(t, result.Stats.Objects)
	})

	t.Run("storage ID mismatch is 422", func(t *testing.T) {
		// The source repository lives on the installation's single blockstore (""), while
		// this service is configured for a dataset namespace on another storage ID: the
		// full addresses of §6.3 could not be read back, so the definition is rejected with
		// the 422 of §5.2 rule 4.
		otherCfg := *f.cfg
		otherCfg.StorageID = "another-storage"
		otherService, err := NewService(&otherCfg, f.kvStore, f.cat, f.authService)
		require.NoError(t, err)
		def := objectDefinition("src", commit.String(), "a.txt", "data/a.txt")
		_, err = otherService.Validate(f.ctx, f.caller(), "dry-run", def)
		require.ErrorIs(t, err, ErrStorageIDMismatch)
		var sourceErr *SourceError
		require.ErrorAs(t, err, &sourceErr)
	})

	t.Run("reserved item type is 501", func(t *testing.T) {
		def := Definition{
			FormatVersion: FormatVersion,
			Data: []DataItem{{
				Target:     "t",
				Repository: "src",
				Ref:        SourceRef{Type: RefTypeCommit, ID: commit.String()},
				Type:       ItemTypeTable,
				Table:      &TableSrc{Namespace: "ns", Name: "t"},
			}},
		}
		_, err := f.svc.Validate(f.ctx, f.caller(), "dry-run", def)
		require.ErrorIs(t, err, ErrUnsupported)
	})
}

func TestServiceAccessGrantRoundTrip(t *testing.T) {
	f := newServiceFixture(t)
	commit := f.sourceRepo(t, "src", map[string]string{"a.txt": "a"})
	def := objectDefinition("src", commit.String(), "a.txt", "a.txt")
	_, err := f.svc.Create(f.ctx, f.caller(), CreateRequest{Name: "shared-set", Definition: def})
	require.NoError(t, err)

	f.addUser(t, "alice")
	_, err = f.authService.CreateGroup(f.ctx, &model.Group{DisplayName: "analysts"})
	require.NoError(t, err)

	grant, err := f.svc.GrantAccess(f.ctx, f.caller(), "shared-set", PrincipalTypeUser, "alice")
	require.NoError(t, err)
	require.Equal(t, "shared-set", grant.Dataset)
	require.Equal(t, ConsumerReadPolicyName("shared-set"), grant.PolicyName)
	require.Equal(t, testServiceUser, grant.GrantedBy)

	// The managed policy of §9.2 exists and grants both the data plane and the control
	// plane: reading the dataset is reading repository <name>.
	policy, err := f.authService.GetPolicy(f.ctx, ConsumerReadPolicyName("shared-set"))
	require.NoError(t, err)
	require.Contains(t, statementResources(policy.Statement), permissions.RepoArn("shared-set"))
	require.Contains(t, statementResources(policy.Statement), permissions.ObjectArn("shared-set", "*"))
	require.Contains(t, statementResources(policy.Statement), DatasetArn("shared-set"))

	// ... and the consumer can actually use it.
	resp, err := f.authService.Authorize(f.ctx, &auth.AuthorizationRequest{
		Username: "alice",
		RequiredPermissions: permissions.Node{Permission: permissions.Permission{
			Action:   permissions.ReadObjectAction,
			Resource: permissions.ObjectArn("shared-set", "a.txt"),
		}},
	})
	require.NoError(t, err)
	require.True(t, resp.Allowed)
	resp, err = f.authService.Authorize(f.ctx, &auth.AuthorizationRequest{
		Username:            "alice",
		RequiredPermissions: DatasetPermission(ActionReadDataset, "shared-set"),
	})
	require.NoError(t, err)
	require.True(t, resp.Allowed)

	t.Run("idempotent and listable", func(t *testing.T) {
		_, err := f.svc.GrantAccess(f.ctx, f.caller(), "shared-set", PrincipalTypeUser, "alice")
		require.NoError(t, err)
		_, err = f.svc.GrantAccess(f.ctx, f.caller(), "shared-set", PrincipalTypeGroup, "analysts")
		require.NoError(t, err)
		grants, err := f.svc.ListAccess(f.ctx, f.caller(), "shared-set", "", 0)
		require.NoError(t, err)
		require.Len(t, grants.Grants, 2)
		require.False(t, grants.HasMore)
	})

	t.Run("unknown principal is 404", func(t *testing.T) {
		_, err := f.svc.GrantAccess(f.ctx, f.caller(), "shared-set", PrincipalTypeUser, "nobody")
		require.ErrorIs(t, err, ErrDatasetNotFound)
	})

	t.Run("invalid principal type is 400", func(t *testing.T) {
		_, err := f.svc.GrantAccess(f.ctx, f.caller(), "shared-set", "service-account", "alice")
		require.Error(t, err)
		var validation *ValidationError
		require.ErrorAs(t, err, &validation)
		require.Equal(t, "principal_type", validation.Problems[0].Path)
	})

	t.Run("revoke the user grant keeps the policy for the group", func(t *testing.T) {
		require.NoError(t, f.svc.RevokeAccess(f.ctx, f.caller(), "shared-set", PrincipalTypeUser, "alice"))
		_, err := f.svc.store.GetGrant(f.ctx, "shared-set", PrincipalTypeUser, "alice")
		require.ErrorIs(t, err, kv.ErrNotFound)
		_, err = f.authService.GetPolicy(f.ctx, ConsumerReadPolicyName("shared-set"))
		require.NoError(t, err, "the group grant still needs the policy")
		resp, err := f.authService.Authorize(f.ctx, &auth.AuthorizationRequest{
			Username:            "alice",
			RequiredPermissions: DatasetPermission(ActionReadDataset, "shared-set"),
		})
		require.NoError(t, err)
		require.False(t, resp.Allowed)
	})

	t.Run("revoking the last grant deletes the policy", func(t *testing.T) {
		require.NoError(t, f.svc.RevokeAccess(f.ctx, f.caller(), "shared-set", PrincipalTypeGroup, "analysts"))
		_, err := f.authService.GetPolicy(f.ctx, ConsumerReadPolicyName("shared-set"))
		require.ErrorIs(t, err, auth.ErrNotFound)
	})

	t.Run("revoking an unknown grant is 404", func(t *testing.T) {
		err := f.svc.RevokeAccess(f.ctx, f.caller(), "shared-set", PrincipalTypeUser, "alice")
		require.ErrorIs(t, err, ErrDatasetNotFound)
	})
}

func TestServiceDatasetsAllReadMembership(t *testing.T) {
	f := newServiceFixture(t)
	commit := f.sourceRepo(t, "src", map[string]string{"a.txt": "a"})
	def := objectDefinition("src", commit.String(), "a.txt", "a.txt")

	// The aggregate policy starts with no data-plane statement for any dataset.
	policy, err := f.authService.GetPolicy(f.ctx, DatasetsAllReadPolicyName)
	require.NoError(t, err)
	require.NotContains(t, statementResources(policy.Statement), DatasetArn("first-set"))

	_, err = f.svc.Create(f.ctx, f.caller(), CreateRequest{Name: "first-set", Definition: def})
	require.NoError(t, err)
	_, err = f.svc.Create(f.ctx, f.caller(), CreateRequest{Name: "second-set", Definition: def})
	require.NoError(t, err)

	policy, err = f.authService.GetPolicy(f.ctx, DatasetsAllReadPolicyName)
	require.NoError(t, err)
	require.Contains(t, statementResources(policy.Statement), DatasetArn("first-set"))
	require.Contains(t, statementResources(policy.Statement), DatasetArn("second-set"))

	// The read-all groups keep the attachment through the rewrite.
	groups, _, err := f.authService.ListGroupPolicies(f.ctx, DatasetConsumersGroup, &model.PaginationParams{Amount: -1})
	require.NoError(t, err)
	require.Contains(t, policyNames(groups), DatasetsAllReadPolicyName)

	require.NoError(t, f.svc.Delete(f.ctx, f.caller(), "first-set"))
	policy, err = f.authService.GetPolicy(f.ctx, DatasetsAllReadPolicyName)
	require.NoError(t, err)
	require.NotContains(t, statementResources(policy.Statement), DatasetArn("first-set"))
	require.Contains(t, statementResources(policy.Statement), DatasetArn("second-set"))
}

func policyNames(policies []*model.Policy) []string {
	names := make([]string, len(policies))
	for i, policy := range policies {
		names[i] = policy.DisplayName
	}
	return names
}

// TestRollbackCreate_ModifiedRecordIsStillRemoved: the CAS predicate is the freshly read
// record, so a record changed since creation does not survive the repository's deletion.
func TestRollbackCreate_ModifiedRecordIsStillRemoved(t *testing.T) {
	const dataset = "rollback-modified"
	f := newServiceFixture(t)
	commit := f.sourceRepo(t, "src-rollback", map[string]string{"data/a.parquet": "a"})
	def := prefixDefinition("src-rollback", commit.String(), "data", "data/")
	view, err := f.svc.Create(f.ctx, f.caller(), CreateRequest{Name: dataset, Definition: def})
	require.NoError(t, err)
	stale := view.Record

	changed := *stale
	changed.UpdatedAt = stale.UpdatedAt.Add(time.Minute)
	require.NoError(t, f.svc.store.PutDatasetIf(f.ctx, &changed, stale, nil))

	f.svc.rollbackCreate(f.ctx, stale)

	_, _, err = f.svc.store.GetDataset(f.ctx, dataset)
	require.ErrorIs(t, err, kv.ErrNotFound)
}

// failingAllReadAuth fails writes of the aggregate read policy.
type failingAllReadAuth struct{ auth.Service }

func (a failingAllReadAuth) WritePolicy(ctx context.Context, p *model.Policy, update bool) error {
	if p.DisplayName == DatasetsAllReadPolicyName {
		return errors.New("injected all-read policy failure")
	}
	return a.Service.WritePolicy(ctx, p, update)
}

// TestCreate_AllReadPolicyFailureStillReturnsDataset: the dataset is active once the policy
// sync runs; the reconciler converges the policy, so Create does not fail.
func TestCreate_AllReadPolicyFailureStillReturnsDataset(t *testing.T) {
	const dataset = "policy-fails"
	f := newServiceFixture(t)
	f.svc.authService = failingAllReadAuth{Service: f.svc.authService}
	commit := f.sourceRepo(t, "src-policy", map[string]string{"data/a.parquet": "a"})
	def := prefixDefinition("src-policy", commit.String(), "data", "data/")

	view, err := f.svc.Create(f.ctx, f.caller(), CreateRequest{Name: dataset, Definition: def})
	require.NoError(t, err)
	require.Equal(t, DatasetStateActive, view.Record.State)
	require.Equal(t, DatasetStateActive, f.datasetRecord(t, dataset).State)
}

// TestEffectiveAmountClamps is SEC-5: pagination has no "everything" value. A non-positive
// amount is the default page size and an oversized one is clamped, so a client can never make
// the server build an unbounded page.
func TestEffectiveAmountClamps(t *testing.T) {
	for _, tt := range []struct {
		amount int
		want   int
	}{
		{-1, DefaultListAmount},
		{-1000, DefaultListAmount},
		{0, DefaultListAmount},
		{1, 1},
		{MaxListAmount, MaxListAmount},
		{MaxListAmount + 1, MaxListAmount},
		{1 << 20, MaxListAmount},
	} {
		require.Equal(t, tt.want, effectiveAmount(tt.amount), "effectiveAmount(%d)", tt.amount)
	}
}

// TestServiceListNegativeAmountIsDefaultPage is the SEC-5 test of the review: amount=-1 used to
// mean "everything"; it now returns a default-sized page and reports has_more.
func TestServiceListNegativeAmountIsDefaultPage(t *testing.T) {
	f := newServiceFixture(t)
	// Records without a version are enough: List only reads a version for LatestVersion > 0, so
	// seeding the store keeps this a pagination test rather than a create test.
	for i := 0; i <= DefaultListAmount; i++ {
		name := fmt.Sprintf("bulk-set-%03d", i)
		err := f.svc.store.CreateDataset(f.ctx, &DatasetRecord{
			Name:      name,
			CreatedBy: testServiceUser,
			CreatedAt: f.ts,
			UpdatedAt: f.ts,
			State:     DatasetStateActive,
			StorageID: f.cfg.StorageID,
		})
		require.NoError(t, err, "seed %s", name)
	}

	result, err := f.svc.List(f.ctx, f.caller(), ListRequest{Amount: -1})
	require.NoError(t, err)
	require.Len(t, result.Datasets, DefaultListAmount, "amount=-1 is the default page, not everything")
	require.True(t, result.HasMore)
	require.Equal(t, DefaultListAmount, result.MaxPerPage)
	require.NotEmpty(t, result.NextOffset)

	// The next page exists and is reachable through the cursor.
	next, err := f.svc.List(f.ctx, f.caller(), ListRequest{Amount: -1, After: result.NextOffset})
	require.NoError(t, err)
	require.NotEmpty(t, next.Datasets)
	require.False(t, next.HasMore)
}

// TestServiceRewritesPreCreatedManagedPolicies is SEC-6: the per-dataset managed policies have
// predictable names, so one can be pre-created by a principal that may write policies but holds
// no datasets permission. The service must compare the statements and rewrite a mismatch
// instead of trusting the existing policy.
func TestServiceRewritesPreCreatedManagedPolicies(t *testing.T) {
	f := newServiceFixture(t)
	commit := f.sourceRepo(t, "src-squat", map[string]string{"a.txt": "a"})
	def := objectDefinition("src-squat", commit.String(), "a.txt", "a.txt")

	t.Run("consumer read policy", func(t *testing.T) {
		const dataset = "squatted-read"
		// The squat: fs:* on * under the name GrantAccess manages.
		require.NoError(t, f.authService.WritePolicy(f.ctx, &model.Policy{
			CreatedAt:   f.ts,
			DisplayName: ConsumerReadPolicyName(dataset),
			Statement: model.Statements{{
				Effect:   model.StatementEffectAllow,
				Action:   []string{"fs:*"},
				Resource: permissions.All,
			}},
		}, false))

		_, err := f.svc.Create(f.ctx, f.caller(), CreateRequest{Name: dataset, Definition: def})
		require.NoError(t, err)
		f.addUser(t, "squatted-reader")
		grant, err := f.svc.GrantAccess(f.ctx, f.caller(), dataset, PrincipalTypeUser, "squatted-reader")
		require.NoError(t, err)
		require.Equal(t, ConsumerReadPolicyName(dataset), grant.PolicyName)

		policy, err := f.authService.GetPolicy(f.ctx, ConsumerReadPolicyName(dataset))
		require.NoError(t, err)
		require.True(t, statementsEqual(ConsumerReadPolicy(dataset, f.svc.now()).Statement, policy.Statement),
			"the managed read policy is rewritten to the managed statements")
		require.NotContains(t, statementActions(policy.Statement), "fs:*")

		// The grant the user actually holds is the managed one: reading the dataset control
		// plane is allowed, blanket fs:* is not part of it any more.
		resp, err := f.authService.Authorize(f.ctx, &auth.AuthorizationRequest{
			Username:            "squatted-reader",
			RequiredPermissions: DatasetPermission(ActionReadDataset, dataset),
		})
		require.NoError(t, err)
		require.True(t, resp.Allowed)
	})

	t.Run("immutability guard", func(t *testing.T) {
		const dataset = "squatted-guard"
		// The squat: an empty guard, which would make every write to the dataset repository pass.
		require.NoError(t, f.authService.WritePolicy(f.ctx, &model.Policy{
			CreatedAt:   f.ts,
			DisplayName: ImmutabilityPolicyName(dataset),
			Statement:   model.Statements{},
		}, false))

		_, err := f.svc.Create(f.ctx, f.caller(), CreateRequest{Name: dataset, Definition: def})
		require.NoError(t, err)

		policy, err := f.authService.GetPolicy(f.ctx, ImmutabilityPolicyName(dataset))
		require.NoError(t, err)
		require.True(t, statementsEqual(ImmutabilityPolicy(dataset, f.svc.now()).Statement, policy.Statement),
			"the empty guard is rewritten to the managed deny statements")
		for _, statement := range policy.Statement {
			require.Equal(t, model.StatementEffectDeny, statement.Effect, "the guard only denies")
		}
	})
}
