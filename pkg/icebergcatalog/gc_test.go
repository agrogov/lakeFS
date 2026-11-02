package icebergcatalog

// Tests for ICEBERG_CATALOG_PLAN.md §6.1 (the reserved prefix), §6.3 (the uncommitted-GC window)
// and §12 ("security tests: _lakefs_iceberg/ cannot be written through fs:"). Every test name
// starts with TestUncommittedGcGrace or TestReservedPrefixDenied, so the task's validation
// command,
//
//	go test -run 'TestUncommittedGcGrace|TestReservedPrefixDenied' ./pkg/icebergcatalog/ ./pkg/catalog/...
//
// runs exactly this file (the ./pkg/catalog/... part only proves that the predicate did not change
// the core GC).
//
// The authorization half is exercised against a real auth service - the simplified-ACL
// implementation the lakefs-sso binary uses, through the fixture of authz_test.go - and not against
// a mock, because the question is not what this package builds but what the Deny-precedence and
// wildcard machinery of lakeFS does with it. The ARNs are asserted as literals for the same reason:
// they are the strings a policy author writes by hand.

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/permissions"
)

const (
	// gcGrace is the documented default of iceberg.uncommitted_gc_grace (§10, spike S-I2 §6).
	gcGrace = 24 * time.Hour
)

// gcNow is a fixed clock, so the age cases below are exact rather than approximately exact.
var gcNow = time.Date(2025, 3, 14, 9, 0, 0, 0, time.UTC)

// TestUncommittedGcGrace covers the §6.3 predicate: an uncommitted-GC row is skipped exactly when
// the object is a client-written Iceberg table file that is younger than the grace window.
func TestUncommittedGcGrace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		key  string
		// age is how long before gcNow the object was written. A negative age is a write in the
		// future, i.e. a clock that runs ahead of the server.
		age       time.Duration
		grace     time.Duration
		unknown   bool // lastModified is the zero time
		wantSkip  bool
		rationale string
	}{
		{
			name:      "fresh data file is protected",
			key:       "_iceberg/6f1c9f2e/data/part-00000.parquet",
			age:       time.Hour,
			grace:     gcGrace,
			wantSkip:  true,
			rationale: "the client appended an hour ago and the branch is not committed yet",
		},
		{
			name:      "fresh manifest is protected",
			key:       "_iceberg/6f1c9f2e/metadata/snap-1-1-abc.avro",
			age:       10 * time.Minute,
			grace:     gcGrace,
			wantSkip:  true,
			rationale: "manifests and manifest lists are staged the same way as data files",
		},
		{
			name:      "fresh metadata json is protected",
			key:       "_iceberg/6f1c9f2e/metadata/00001-abc.metadata.json",
			age:       10 * time.Minute,
			grace:     gcGrace,
			wantSkip:  true,
			rationale: "the metadata.json a commit wrote is collectable if the commit lost, but not within the grace",
		},
		{
			name:      "one second inside the window is protected",
			key:       "_iceberg/6f1c9f2e/data/part-00000.parquet",
			age:       gcGrace - time.Second,
			grace:     gcGrace,
			wantSkip:  true,
			rationale: "the comparison is strict: younger than the grace, not younger or equal",
		},
		{
			name:      "exactly at the grace boundary is collectable",
			key:       "_iceberg/6f1c9f2e/data/part-00000.parquet",
			age:       gcGrace,
			grace:     gcGrace,
			wantSkip:  false,
			rationale: "at age == grace the object is as old as the knob allows, like any other staged object",
		},
		{
			name:      "older than the grace is collectable",
			key:       "_iceberg/6f1c9f2e/data/part-00000.parquet",
			age:       gcGrace + time.Minute,
			grace:     gcGrace,
			wantSkip:  false,
			rationale: "a staged branch left uncommitted for longer than the grace can lose its files (documented)",
		},
		{
			name:      "ordinary staged object is never protected",
			key:       "data/part-00000.parquet",
			age:       time.Second,
			grace:     gcGrace,
			wantSkip:  false,
			rationale: "no GC default changes for non-iceberg data, in any repository",
		},
		{
			name:      "the entry area of the branch is not the physical area",
			key:       ReservedPrefix + "/files/6f1c9f2e/data/part-00000.parquet",
			age:       time.Second,
			grace:     gcGrace,
			wantSkip:  false,
			rationale: "_lakefs_iceberg/ holds entries (versioned); _iceberg/ holds the objects they point at",
		},
		{
			name:      "a look-alike prefix is not the area",
			key:       "_icebergdata/part-00000.parquet",
			age:       time.Second,
			grace:     gcGrace,
			wantSkip:  false,
			rationale: "the test is on the first path segment, so a repository may use a similar name",
		},
		{
			name:      "a nested _iceberg directory is not the area",
			key:       "warehouse/_iceberg/6f1c9f2e/data/part-00000.parquet",
			age:       time.Second,
			grace:     gcGrace,
			wantSkip:  false,
			rationale: "the area is at the storage-namespace root (§6.3), not anywhere in the path",
		},
		{
			name:      "the area marker itself is protected",
			key:       DataPrefix,
			age:       time.Second,
			grace:     gcGrace,
			wantSkip:  true,
			rationale: "a zero-byte marker at the area root is harmless to keep and cannot be told apart from a file",
		},
		{
			name:      "a leading slash is tolerated",
			key:       "/_iceberg/6f1c9f2e/data/part-00000.parquet",
			age:       time.Second,
			grace:     gcGrace,
			wantSkip:  true,
			rationale: "storage keys are written both ways by clients and by the block adapters",
		},
		{
			name:      "an unknown write time is protected",
			key:       "_iceberg/6f1c9f2e/data/part-00000.parquet",
			unknown:   true,
			grace:     gcGrace,
			wantSkip:  true,
			rationale: "the report's creation_date would be the zero time, which every age check reads as ancient",
		},
		{
			name:      "a write time in the future is protected",
			key:       "_iceberg/6f1c9f2e/data/part-00000.parquet",
			age:       -time.Hour,
			grace:     gcGrace,
			wantSkip:  true,
			rationale: "clock skew must only ever delay a deletion, never cause one",
		},
		{
			name:      "protect-all grace protects an ancient file",
			key:       "_iceberg/6f1c9f2e/data/part-00000.parquet",
			age:       1000 * time.Hour,
			grace:     UncommittedGCGraceProtectAll,
			wantSkip:  true,
			rationale: "iceberg.uncommitted_gc_grace: 0 is the documented escape hatch of spike S-I2 §6",
		},
		{
			name:      "a negative grace protects rather than collects",
			key:       "_iceberg/6f1c9f2e/data/part-00000.parquet",
			age:       1000 * time.Hour,
			grace:     -time.Hour,
			wantSkip:  true,
			rationale: "Config.Validate rejects a negative value; if one arrives anyway, fail towards protection",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lastModified := gcNow.Add(-tt.age)
			if tt.unknown {
				lastModified = time.Time{}
			}
			got := UncommittedGCProtected(tt.key, lastModified, gcNow, tt.grace)
			require.Equal(t, tt.wantSkip, got, "%s: key %q, %s", tt.rationale, tt.key, tt.age)
		})
	}
}

// TestUncommittedGcGraceCallSite pins the contract of the argument the prepare step passes: the
// key relative to the storage namespace. PrepareGCUncommitted truncates a full address by the
// normalized storage namespace before it writes the row (pkg/catalog/gc_write_uncommitted.go,
// processBranch), so a row for an Iceberg data file arrives as "_iceberg/<uuid>/…" and not as a
// URI. Simulating the truncation here is what makes the predicate's documentation checkable.
func TestUncommittedGcGraceCallSite(t *testing.T) {
	t.Parallel()

	const (
		storageNamespace = "s3://lakefs-data/team-a/"
		stagedFile       = storageNamespace + "_iceberg/6f1c9f2e/data/part-00000.parquet"
		ordinaryObject   = storageNamespace + "exports/report.csv"
	)
	// processBranch's normalization, verbatim: normalizeStorageNamespace adds the trailing
	// delimiter, then the prefix is truncated for a full address.
	rowKey := strings.TrimPrefix(stagedFile, storageNamespace)
	require.Equal(t, "_iceberg/6f1c9f2e/data/part-00000.parquet", rowKey)
	require.True(t, UncommittedGCProtected(rowKey, gcNow.Add(-time.Hour), gcNow, gcGrace),
		"a staged table file must be skipped by the uncommitted GC")

	require.False(t, UncommittedGCProtected(strings.TrimPrefix(ordinaryObject, storageNamespace),
		gcNow.Add(-time.Hour), gcNow, gcGrace),
		"ordinary staged data keeps the current GC behaviour")
}

// TestUncommittedGcGraceConfig pins the configuration half of §6.3: 24h by default, an explicit 0
// meaning UncommittedGCGraceProtectAll, and a negative value rejected.
func TestUncommittedGcGraceConfig(t *testing.T) {
	t.Run("absent key is the 24h default", func(t *testing.T) {
		useConfig(t, "iceberg:\n  enabled: true\n")
		cfg, err := LoadConfig()
		require.NoError(t, err)
		require.Equal(t, DefaultUncommittedGCGrace, cfg.UncommittedGCGrace)
		require.Equal(t, 24*time.Hour, cfg.UncommittedGCGrace)

		// One day later the default has expired: a table file is then collectable, which is the
		// documented consequence of a bounded grace.
		written := gcNow
		require.True(t, UncommittedGCProtected("_iceberg/u/data/p.parquet", written, written.Add(gcGrace-time.Hour), cfg.UncommittedGCGrace))
		require.False(t, UncommittedGCProtected("_iceberg/u/data/p.parquet", written, written.Add(gcGrace+time.Hour), cfg.UncommittedGCGrace))
	})

	t.Run("explicit zero protects every table file", func(t *testing.T) {
		useConfig(t, "iceberg:\n  enabled: true\n  uncommitted_gc_grace: 0\n")
		cfg, err := LoadConfig()
		require.NoError(t, err, "0 is the documented escape hatch, not a validation error")
		require.Equal(t, UncommittedGCGraceProtectAll, cfg.UncommittedGCGrace)
		require.True(t, UncommittedGCProtected("_iceberg/u/data/p.parquet", gcNow.Add(-1000*time.Hour), gcNow, cfg.UncommittedGCGrace))
	})

	t.Run("explicit value is kept", func(t *testing.T) {
		useConfig(t, "iceberg:\n  uncommitted_gc_grace: 90m\n")
		cfg, err := LoadConfig()
		require.NoError(t, err)
		require.Equal(t, 90*time.Minute, cfg.UncommittedGCGrace)
	})

	t.Run("negative is rejected", func(t *testing.T) {
		useConfig(t, "iceberg:\n  uncommitted_gc_grace: -1h\n")
		cfg, err := LoadConfig()
		require.Error(t, err)
		require.Nil(t, cfg)
		require.ErrorIs(t, err, ErrInvalidConfig)
		require.Contains(t, err.Error(), "uncommitted_gc_grace must be positive")
	})
}

// TestReservedPrefixDenied covers §6.1 and the §12 security test: _lakefs_iceberg/ cannot be
// written through the fs: object API, the deny does not overreach into ordinary keys, and it does
// not break the catalog's own §7.2 permission request.
func TestReservedPrefixDenied(t *testing.T) {
	t.Parallel()

	t.Run("policy shape", func(t *testing.T) {
		ts := gcNow
		policy := ReservedPrefixProtectPolicy(ts)
		require.Equal(t, "iceberg-reserved-prefix-protect", policy.DisplayName)
		require.Equal(t, ts, policy.CreatedAt)
		require.Len(t, policy.Statement, 2, "one statement per resource form")

		// §6.1 names the branch form; §7.2's object permission needs the object form. Both are
		// denies on exactly the two object actions.
		require.Equal(t, []string{"arn:lakefs:fs:::repository/*/branch/_lakefs_iceberg/*",
			"arn:lakefs:fs:::repository/*/object/_lakefs_iceberg/*/*"},
			[]string{policy.Statement[0].Resource, policy.Statement[1].Resource})
		for _, stmt := range policy.Statement {
			require.Equal(t, model.StatementEffectDeny, stmt.Effect)
			require.Equal(t, []string{permissions.WriteObjectAction, permissions.DeleteObjectAction}, stmt.Action)
		}
	})

	t.Run("the object pattern selects the reserved area and only it", func(t *testing.T) {
		tests := []struct {
			name string
			key  string
			want bool
		}{
			{"table pointer", ReservedPrefix + "/tables/inventory/books/titles/.pointer", true},
			{"namespace marker", ReservedPrefix + "/ns/inventory/books/.namespace", true},
			{"staged table file", ReservedPrefix + "/files/6f1c9f2e/data/part-00000.parquet", true},
			{"catalog state in another repository", ReservedPrefix + "/tables/x/y/.pointer", true},
			{"ordinary object", "data/part-00000.parquet", false},
			{"ordinary object in a subdirectory", "exports/2025/report.csv", false},
			{"look-alike prefix", ReservedPrefix + "x/tables/inventory/books/titles/.pointer", false},
			{"reserved prefix deeper in the key", "archive/" + ReservedPrefix + "/tables/x/y/.pointer", false},
			// The single-segment hole documented on ReservedPrefixObjectResource: it cannot name
			// any key the catalog writes (all have >= 2 segments after the prefix).
			{"single-segment key", ReservedPrefix + "/foo", false},
			// The shape §7.2 requires for a catalog write: ObjectArn(repo, "_lakefs_iceberg/*").
			// Denying it would refuse every catalog write to every principal.
			{"the catalog's own wildcard request", ReservedPrefix + "/*", false},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got := auth.ArnMatch(ReservedPrefixObjectResource, permissions.ObjectArn("lakehouse", tt.key))
				require.Equal(t, tt.want, got, "key %q", tt.key)
			})
		}
	})

	t.Run("every key the catalog writes is covered", func(t *testing.T) {
		marker, err := namespaceMarkerPath(Namespace{"inventory", "books"})
		require.NoError(t, err)
		pointer, err := tablePointerPath(authzTableAddress())
		require.NoError(t, err)
		stagedFile := ReservedPrefix + "/files/6f1c9f2e/data/part-00000.parquet"

		for _, key := range []string{marker, pointer, stagedFile} {
			require.True(t, auth.ArnMatch(ReservedPrefixObjectResource, permissions.ObjectArn("lakehouse", key)),
				"the deny must match the catalog key %q", key)
			// The arity argument of the pattern: at least two segments after the prefix, so a
			// narrower pattern would still cover these keys but a single-segment one would not.
			relative := strings.TrimPrefix(key, ReservedPrefix+"/")
			require.GreaterOrEqual(t, strings.Count(relative, "/"), 1, "key %q must have >= 2 segments", key)
		}
	})

	// The end-to-end half: a principal that may do everything through fs: and catalog: still
	// cannot put the reserved prefix itself, but keeps every other object write.
	t.Run("a broad allow does not defeat the deny", func(t *testing.T) {
		t.Parallel()
		// The allow is deliberately the widest lakeFS can express - fs:* and catalog:* on "*" -
		// which is the FSFullAccess of an administrator.
		f := newAuthzUser(t, model.Statements{{
			Effect:   model.StatementEffectAllow,
			Action:   []string{"fs:*", "catalog:*"},
			Resource: permissions.All,
		}})
		f.attach(t, "alice", ReservedPrefixProtectPolicy(gcNow))

		denied := []permissions.Permission{
			{Action: permissions.WriteObjectAction, Resource: permissions.ObjectArn("lakehouse", ReservedPrefix+"/tables/inventory/books/titles/.pointer")},
			{Action: permissions.DeleteObjectAction, Resource: permissions.ObjectArn("lakehouse", ReservedPrefix+"/tables/inventory/books/titles/.pointer")},
			{Action: permissions.WriteObjectAction, Resource: permissions.ObjectArn("lakehouse", ReservedPrefix+"/ns/inventory/books/.namespace")},
			{Action: permissions.DeleteObjectAction, Resource: permissions.ObjectArn("lakehouse", ReservedPrefix+"/files/6f1c9f2e/data/part-00000.parquet")},
			{Action: permissions.WriteObjectAction, Resource: permissions.ObjectArn("other-repo", ReservedPrefix+"/tables/x/y/.pointer")},
			// The branch form §6.1 spells is denied too, for any authorization path that addresses
			// the reserved area through a branch ARN.
			{Action: permissions.WriteObjectAction, Resource: permissions.BranchArn("lakehouse", ReservedPrefix+"/some-branch")},
		}
		for _, perm := range denied {
			require.True(t, gcDenied(t, f, "alice", perm), "expected deny of %s on %s", perm.Action, perm.Resource)
		}

		allowed := []permissions.Permission{
			// Ordinary writes anywhere in any repository are untouched.
			{Action: permissions.WriteObjectAction, Resource: permissions.ObjectArn("lakehouse", "data/part-00000.parquet")},
			{Action: permissions.DeleteObjectAction, Resource: permissions.ObjectArn("lakehouse", "exports/2025/report.csv")},
			// Reads of the reserved area stay allowed: an operator must be able to read a pointer
			// the catalog wrote, and the deny is about writing and deleting.
			{Action: permissions.ReadObjectAction, Resource: permissions.ObjectArn("lakehouse", ReservedPrefix+"/tables/inventory/books/titles/.pointer")},
			// §7.2's own requirement for every catalog write: fs:WriteObject on
			// ObjectArn(repo, "_lakefs_iceberg/*"). It must survive the deny - see the arity
			// argument on ReservedPrefixObjectResource - or the catalog would refuse every write.
			{Action: permissions.WriteObjectAction, Resource: authzWriteObjectArn},
		}
		for _, perm := range allowed {
			require.False(t, gcDenied(t, f, "alice", perm), "expected allow of %s on %s", perm.Action, perm.Resource)
		}
	})

	// §7.2's catalog writes still work with the deny installed: the user holds exactly the grants
	// of the matrix (including fs:WriteObject on _lakefs_iceberg/*), and every catalog write
	// operation is authorized by the same Authorizer the HTTP layer uses.
	t.Run("catalog writes keep working", func(t *testing.T) {
		t.Parallel()
		f := newAuthzUser(t, authzWriteStatements())
		f.attach(t, "alice", ReservedPrefixProtectPolicy(gcNow))

		for _, op := range []Operation{OpCreateNamespace, OpCreateTable, OpUpdateTable, OpDeleteTable, OpWriteTableData} {
			err := f.authorize(t, "alice", Request{Op: op, Address: authzAddressFor(op), RefIsBranch: true})
			require.NoError(t, err, "catalog write %s must not be refused by iceberg-reserved-prefix-protect", op)
		}

		// The same principal, going around the catalog and writing a catalog key directly through
		// the object API, is refused: that is the forgery §6.1 is about.
		pointer, err := tablePointerPath(authzTableAddress())
		require.NoError(t, err)
		require.True(t, gcDenied(t, f, "alice", permissions.Permission{
			Action:   permissions.WriteObjectAction,
			Resource: permissions.ObjectArn(authzRepo, pointer),
		}), "a direct write to %q through fs: must be denied", pointer)
	})
}

// gcDenied evaluates one permission leaf as username against the fixture's auth service and
// reports whether it is refused. A failure to decide is a test failure, not a denial.
func gcDenied(t *testing.T, f *authzFixture, username string, perm permissions.Permission) bool {
	t.Helper()
	resp, err := f.svc.Authorize(t.Context(), &auth.AuthorizationRequest{
		Username: username,
		RequiredPermissions: permissions.Node{
			Type:       permissions.NodeTypeNode,
			Permission: perm,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	if resp.Error != nil {
		require.ErrorIs(t, resp.Error, auth.ErrInsufficientPermissions,
			"unexpected failure to decide %s on %s", perm.Action, perm.Resource)
		return true
	}
	return !resp.Allowed
}
