package datasets

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/contrib/auth/acl"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/permissions"
)

// statusOf renders err the way the HTTP layer does and returns the status.
func statusOf(t *testing.T, err error) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	writeServiceError(rec, httptest.NewRequest(http.MethodPost, "/", nil), err)
	return rec.Code, rec.Body.String()
}

// TestSourceReadDenyOnSubPath is SEC-3: a Deny on a sub-path of a prefix source must be honoured
// per object, whatever the prefix the item names.
func TestSourceReadDenyOnSubPath(t *testing.T) {
	f := newServiceFixture(t)
	commit := f.sourceRepo(t, "hrsrc", map[string]string{
		"pub/a.txt":        "a",
		"secret/b.txt":     "b",
		"sec/c.txt":        "c",
		"secretive/d.txt":  "d",
		"zzz/not-denied.t": "e",
	})
	const user = "analyst"
	f.addUser(t, user, acl.AdminsGroup, DatasetAdminsGroup)
	f.grant(t, user, "deny-secret", model.Statements{
		{Effect: model.StatementEffectDeny, Action: []string{permissions.ReadObjectAction}, Resource: permissions.ObjectArn("hrsrc", "secret/*")},
		{Effect: model.StatementEffectDeny, Action: []string{permissions.ReadObjectAction}, Resource: permissions.ObjectArn("hrsrc", "sec/*")},
	})
	caller := f.callerFor(user)

	for _, prefix := range []string{"", "secret", "sec"} {
		t.Run("create with prefix "+prefix, func(t *testing.T) {
			name := "denied-create-" + strings.NewReplacer("", "x").Replace("p") + prefix
			_, err := f.svc.Create(f.ctx, caller, CreateRequest{
				Name: name, Definition: prefixDefinition("hrsrc", commit.String(), "out", prefix), Publish: true,
			})
			require.ErrorIs(t, err, ErrPermissionDenied)
			if prefix == "" {
				require.ErrorIs(t, err, ErrSourceReadDenied, "the per-object check catches it")
			}
			status, body := statusOf(t, err)
			require.Equal(t, http.StatusForbidden, status)
			require.Contains(t, body, "data[0]", "the response names the item")
			require.Regexp(t, `sec(ret)?/`, body, "the response names the first denied path")
			_, _, getErr := f.svc.store.GetDataset(f.ctx, name)
			require.Error(t, getErr, "nothing was created")
		})
	}

	t.Run("validate", func(t *testing.T) {
		_, err := f.svc.Validate(f.ctx, caller, "validate-denied", prefixDefinition("hrsrc", commit.String(), "out", ""))
		require.ErrorIs(t, err, ErrSourceReadDenied)
		status, _ := statusOf(t, err)
		require.Equal(t, http.StatusForbidden, status)
	})

	t.Run("an object item under the denied path", func(t *testing.T) {
		_, err := f.svc.Validate(f.ctx, caller, "validate-denied", objectDefinition("hrsrc", commit.String(), "x.txt", "secret/b.txt"))
		require.ErrorIs(t, err, ErrPermissionDenied)
	})

	t.Run("publish", func(t *testing.T) {
		admin := f.caller()
		_, err := f.svc.Create(f.ctx, admin, CreateRequest{
			Name: "pub-denied", Definition: prefixDefinition("hrsrc", commit.String(), "out", "pub"), Publish: true,
		})
		require.NoError(t, err)
		_, err = f.svc.PublishVersion(f.ctx, caller, "pub-denied", PublishVersionRequest{
			Definition: prefixDefinition("hrsrc", commit.String(), "out", ""), Sync: true,
		})
		require.ErrorIs(t, err, ErrSourceReadDenied)
	})

	t.Run("a prefix that avoids the denied paths is allowed", func(t *testing.T) {
		view, err := f.svc.Create(f.ctx, caller, CreateRequest{
			Name: "allowed-pub", Definition: prefixDefinition("hrsrc", commit.String(), "out", "pub"), Publish: true,
		})
		require.NoError(t, err)
		require.NotNil(t, view.Version)
	})
}

// TestSourceReadDenyOnSecretSubPath is the SEC-3 exploit of the review: a caller with fs:* on
// everything and a Deny on fs:ReadObject for arn:lakefs:fs:::repository/src/object/secret/* must not
// be able to publish the denied objects, whatever prefix the item names.
//
// A prefix item is normalised to end with "/", so "sec" covers "sec/..." and not "secret/..."; the
// second case therefore puts the deny under the item's own prefix, which is the shape a prefix item
// cannot see at attach time (the item's literal ARN .../object/sec/* does not match a Deny on
// .../object/sec/secret/*). Both cases are caught per object, and both name the item and the first
// denied path.
func TestSourceReadDenyOnSecretSubPath(t *testing.T) {
	f := newServiceFixture(t)
	commit := f.sourceRepo(t, "src", map[string]string{
		"pub/report.csv":          "public",
		"sec/notes.txt":           "also public",
		"sec/secret/salaries.csv": "denied below the sec/ prefix",
		"secret/compensation.csv": "denied",
	})
	const user = "exfiltrator"
	f.addUser(t, user, DatasetAdminsGroup)
	f.grant(t, user, "allow-fs-all", model.Statements{{
		Effect:   model.StatementEffectAllow,
		Action:   []string{"fs:*"},
		Resource: permissions.All,
	}})
	// The review's deny, verbatim.
	f.grant(t, user, "deny-secret", model.Statements{{
		Effect:   model.StatementEffectDeny,
		Action:   []string{permissions.ReadObjectAction},
		Resource: "arn:lakefs:fs:::repository/src/object/secret/*",
	}})
	// The same caller with the deny one level deeper, to show a prefix item cannot hide a deny
	// that sits below its own literal ARN. It gets the public grant too.
	const secUser = "exfiltrator-sec"
	f.addUser(t, secUser, DatasetAdminsGroup)
	require.NoError(t, f.authService.AttachPolicyToUser(t.Context(), "allow-fs-all", secUser))
	f.grant(t, secUser, "deny-sec-secret", model.Statements{{
		Effect:   model.StatementEffectDeny,
		Action:   []string{permissions.ReadObjectAction},
		Resource: "arn:lakefs:fs:::repository/src/object/sec/secret/*",
	}})

	for _, tt := range []struct {
		name     string
		dataset  string
		prefix   string
		username string
		// wantPath is the first source object the caller may not read, in key order.
		wantPath string
	}{
		{name: "a prefix over the whole repository", dataset: "exfil-root", prefix: "", username: user, wantPath: "secret/compensation.csv"},
		{name: "a prefix over sec", dataset: "exfil-sec", prefix: "sec", username: secUser, wantPath: "sec/secret/salaries.csv"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			caller := f.callerFor(tt.username)
			_, err := f.svc.Create(f.ctx, caller, CreateRequest{
				Name:       tt.dataset,
				Definition: prefixDefinition("src", commit.String(), "out", tt.prefix),
				Publish:    true,
			})
			require.ErrorIs(t, err, ErrPermissionDenied)
			require.ErrorIs(t, err, ErrSourceReadDenied,
				"the per-object check catches the deny the attach-time check cannot see")
			var sourceErr *SourceError
			require.ErrorAs(t, err, &sourceErr)
			require.Equal(t, tt.wantPath, sourceErr.Path, "the SourceError names the denied path")
			require.Equal(t, 0, sourceErr.Item)
			require.Equal(t, "src", sourceErr.Repository)

			status, body := statusOf(t, err)
			require.Equal(t, http.StatusForbidden, status)
			require.Contains(t, body, "data[0]", "the 403 names the item")
			require.Contains(t, body, tt.wantPath, "the 403 names the denied path")
			_, _, getErr := f.svc.store.GetDataset(f.ctx, tt.dataset)
			require.Error(t, getErr, "a denied publish creates nothing")
		})
	}

	t.Run("a prefix that misses every deny", func(t *testing.T) {
		// The deny is on a sub-path, not on the item: the same caller publishes "pub" happily.
		view, err := f.svc.Create(f.ctx, f.callerFor(user), CreateRequest{
			Name:       "exfil-allowed",
			Definition: prefixDefinition("src", commit.String(), "out", "pub"),
			Publish:    true,
		})
		require.NoError(t, err)
		require.NotNil(t, view.Version)
	})
}

// TestSourceReadRepositoryUnreadableLooksLikeMissing is SEC-4: before anything is resolved the
// caller needs fs:ReadRepository, so an unreadable repository and a missing one yield the same 403.
func TestSourceReadRepositoryUnreadableLooksLikeMissing(t *testing.T) {
	f := newServiceFixture(t)
	commit := f.sourceRepo(t, "hiddenrepo", map[string]string{"a.txt": "a"})
	const user = "creator-only"
	f.addUser(t, user, DatasetAdminsGroup)
	caller := f.callerFor(user)

	existing := prefixDefinition("hiddenrepo", commit.String(), "out", "")
	missing := prefixDefinition("ghostrepo", commit.String(), "out", "")

	// A dataset the caller may read and update, plus a proposal on it, so the publish and
	// proposal paths reach their own source resolution: every one of them must answer the
	// fs:ReadRepository 403 before the resolver can say anything about the source repository.
	targetCommit := f.sourceRepo(t, "sec4-public", map[string]string{"public.txt": "public"})
	publicDef := objectDefinition("sec4-public", targetCommit.String(), "public.txt", "public.txt")
	f.createPublishedDataset(t, "sec4-target", publicDef)
	proposal := f.createProposal(t, "sec4-target", CreateProposalRequest{Definition: publicDef, Title: "public"})

	run := map[string]func(Definition) error{
		"create": func(def Definition) error {
			_, err := f.svc.Create(f.ctx, caller, CreateRequest{Name: "sec4-create", Definition: def, Publish: true})
			return err
		},
		"validate": func(def Definition) error {
			_, err := f.svc.Validate(f.ctx, caller, "sec4-validate", def)
			return err
		},
		"publish": func(def Definition) error {
			_, err := f.svc.PublishVersion(f.ctx, caller, "sec4-target", PublishVersionRequest{Definition: def, Sync: true})
			return err
		},
		"create proposal": func(def Definition) error {
			_, err := f.svc.CreateProposal(f.ctx, caller, "sec4-target", CreateProposalRequest{Definition: def, Title: "t"})
			return err
		},
		"update proposal": func(def Definition) error {
			_, err := f.svc.UpdateProposal(f.ctx, caller, "sec4-target", proposal.Record.ID, UpdateProposalRequest{Definition: &def})
			return err
		},
	}
	for name, call := range run {
		t.Run(name, func(t *testing.T) {
			errExisting, errMissing := call(existing), call(missing)
			require.ErrorIs(t, errExisting, ErrPermissionDenied)
			require.ErrorIs(t, errMissing, ErrPermissionDenied)
			s1, b1 := statusOf(t, errExisting)
			s2, b2 := statusOf(t, errMissing)
			require.Equal(t, http.StatusForbidden, s1)
			require.Equal(t, s1, s2)
			require.Equal(t, strings.ReplaceAll(b1, "hiddenrepo", "REPO"), strings.ReplaceAll(b2, "ghostrepo", "REPO"),
				"the bodies differ only in the repository name the caller supplied")
		})
	}

	t.Run("a granted repository reaches the resolver", func(t *testing.T) {
		f.grant(t, user, "read-ghost", model.Statements{{
			Effect: model.StatementEffectAllow, Action: []string{permissions.ReadRepositoryAction}, Resource: permissions.RepoArn("ghostrepo"),
		}})
		err := run["validate"](missing)
		require.ErrorIs(t, err, ErrSourceRepositoryNotFound)
	})
}

// TestSourceReadAuthzRequired: a materialiser that serves the service never reads a source without
// the caller's check on the context.
func TestSourceReadAuthzRequired(t *testing.T) {
	f := newServiceFixture(t)
	commit := f.sourceRepo(t, "src", map[string]string{"a.txt": "a"})
	def := objectDefinition("src", commit.String(), "a.txt", "a.txt")
	require.True(t, f.svc.materializer.RequireSourceAuthz)
	_, err := f.svc.materializer.openItem(t.Context(), 0, &def.Data[0])
	require.ErrorIs(t, err, ErrSourceAuthzMissing)
}

// TestSourceValidateRejectsWildcards: "*" and "?" are rejected in object and prefix paths (SEC-3).
func TestSourceValidateRejectsWildcards(t *testing.T) {
	for _, tt := range []struct {
		name string
		def  Definition
		path string
	}{
		{"object star", objectDefinition("src", "c", "t", "a*.txt"), "data[0].object.path"},
		{"object question", objectDefinition("src", "c", "t", "a?.txt"), "data[0].object.path"},
		{"prefix star", prefixDefinition("src", "c", "t", "data/*"), "data[0].prefix.path"},
		{"prefix question", prefixDefinition("src", "c", "t", "da?a/"), "data[0].prefix.path"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := Validate(&tt.def, ValidateOptions{})
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.path)
		})
	}
	ok := prefixDefinition("src", "c", "t", "data/")
	require.NoError(t, Validate(&ok, ValidateOptions{}))
}
