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
//
// The hook half (Bootstrap, EnsureReservedPrefixProtectPolicy, catalog.SetUncommittedGCProtector)
// runs against a real *catalog.Catalog from store_test.go's fixture, so "the grace removes the row
// from the parquet report the GC job reads" is a fact of the code the server runs, not a claim about
// a mock.

import (
	"io"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/permissions"
	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go/reader"
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

// TestUncommittedGcGracePrepareStep covers the hook itself, not the predicate: it runs the real
// PrepareGCUncommitted of a real catalog over a branch staged the way §6.3 stages table files, and
// reads the parquet report the GC job deletes from. The three expectations are the contract of the
// wiring:
//
//   - with no protector installed every staged object is reported, so the upstream GC behaviour of
//     the plain lakefs binary is unchanged;
//   - with the 24h grace installed a fresh table file is absent from the report while an old one
//     and an ordinary staged object are still there;
//   - with the protect-all grace (0) no table file is reported at all.
//
// The row's physical_address is the object key relative to the storage namespace - the form the
// predicate documents and the form processBranch produces - so this test is also the call-site check
// that the grace is applied to the key it is written for.
func TestUncommittedGcGracePrepareStep(t *testing.T) {
	// Not parallel: it installs a process-wide protector in pkg/catalog.
	f := newStoreFixture(t)
	namespace := "mem://" + storeTestRepo + "-ns"

	const tableUUID = "6f1c9f2e-0000-4000-8000-000000000001"
	now := time.Now()
	staged := []struct {
		name      string
		entryPath string
		objectKey string
		created   time.Time
	}{
		{
			name:      "fresh table file",
			entryPath: ReservedPrefix + "/files/" + tableUUID + "/data/part-00000.parquet",
			objectKey: DataPrefix + "/" + tableUUID + "/data/part-00000.parquet",
			created:   now.Add(-time.Hour),
		},
		{
			name:      "table file older than the grace",
			entryPath: ReservedPrefix + "/files/" + tableUUID + "/data/part-00001.parquet",
			objectKey: DataPrefix + "/" + tableUUID + "/data/part-00001.parquet",
			created:   now.Add(-gcGrace - time.Hour),
		},
		{
			name:      "ordinary staged object",
			entryPath: "exports/2025/report.csv",
			objectKey: "exports/2025/report.csv",
			created:   now.Add(-time.Hour),
		},
	}
	for _, e := range staged {
		require.NoErrorf(t, f.catalog.CreateEntry(f.ctx, storeTestRepo, "main", catalog.DBEntry{
			Path:            e.entryPath,
			PhysicalAddress: namespace + "/" + e.objectKey,
			AddressType:     catalog.AddressTypeFull,
			CreationDate:    e.created,
		}), "stage %s", e.name)
	}

	// No protector: the default behaviour of every lakeFS build without the catalog.
	catalog.SetUncommittedGCProtector(nil)
	require.ElementsMatch(t, []string{
		staged[0].objectKey, staged[1].objectKey, staged[2].objectKey,
	}, prepareUncommittedReport(t, f), "without a protector every staged object is reported")

	// The configured grace, installed exactly as Bootstrap installs it.
	catalog.SetUncommittedGCProtector(catalog.UncommittedGCProtector(UncommittedGCGraceProtector(gcGrace)))
	t.Cleanup(func() { catalog.SetUncommittedGCProtector(nil) })
	require.ElementsMatch(t, []string{
		staged[1].objectKey, staged[2].objectKey,
	}, prepareUncommittedReport(t, f),
		"the fresh table file is skipped, the old one and ordinary data are still reportable")

	// iceberg.uncommitted_gc_grace: 0 - protect every table file, however old.
	catalog.SetUncommittedGCProtector(catalog.UncommittedGCProtector(
		UncommittedGCGraceProtector(UncommittedGCGraceProtectAll)))
	require.ElementsMatch(t, []string{staged[2].objectKey}, prepareUncommittedReport(t, f),
		"protect-all keeps the whole _iceberg/ area out of the report")
}

// TestUncommittedGcGraceProtectorAdapter pins the shape of the binder Bootstrap installs: the
// closure answers exactly what the predicate answers, for the four decisions that matter.
func TestUncommittedGcGraceProtectorAdapter(t *testing.T) {
	t.Parallel()
	protect := UncommittedGCGraceProtector(gcGrace)

	fresh := gcNow.Add(-time.Hour)
	old := gcNow.Add(-gcGrace - time.Hour)
	require.True(t, protect("_iceberg/u/data/p.parquet", fresh, gcNow))
	require.False(t, protect("_iceberg/u/data/p.parquet", old, gcNow))
	require.False(t, protect("data/p.parquet", fresh, gcNow))
	require.Equal(t, UncommittedGCProtected("_iceberg/u/data/p.parquet", fresh, gcNow, gcGrace),
		protect("_iceberg/u/data/p.parquet", fresh, gcNow),
		"the installed protector and the tested predicate cannot disagree")
}

// prepareUncommittedReport runs one PrepareGCUncommitted pass over the fixture's repository and
// returns the physical_address column of the parquet report it produced, sorted.
func prepareUncommittedReport(t *testing.T, f *storeFixture) []string {
	t.Helper()
	info, err := f.catalog.PrepareGCUncommitted(f.ctx, storeTestRepo, nil)
	require.NoError(t, err, "prepare uncommitted GC")
	if info.Filename == "" {
		return nil
	}
	location, err := url.JoinPath(info.Location, info.Filename)
	require.NoError(t, err)

	readerAt, err := f.catalog.BlockAdapter.Get(f.ctx, block.ObjectPointer{
		Identifier:       location,
		IdentifierType:   block.IdentifierTypeFull,
		StorageNamespace: "mem://" + storeTestRepo + "-ns",
	})
	require.NoError(t, err, "read the uncommitted GC report")
	defer func() { _ = readerAt.Close() }()
	data, err := io.ReadAll(readerAt)
	require.NoError(t, err)

	bufferFile := buffer.NewBufferFileFromBytes(data)
	defer func() { _ = bufferFile.Close() }()
	parquetReader, err := reader.NewParquetReader(bufferFile, new(catalog.UncommittedParquetObject), 1)
	require.NoError(t, err)
	defer parquetReader.ReadStop()

	rows := make([]*catalog.UncommittedParquetObject, parquetReader.GetNumRows())
	require.NoError(t, parquetReader.Read(&rows))
	addresses := make([]string, 0, len(rows))
	for _, row := range rows {
		addresses = append(addresses, row.PhysicalAddress)
	}
	sort.Strings(addresses)
	return addresses
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

// TestReservedPrefixDeniedBootstrap covers the attachment half of §6.1: the policy built by
// ReservedPrefixProtectPolicy is created and attached to the installation's non-service groups by
// EnsureReservedPrefixProtectPolicy, idempotently, and the deny it produces is what refuses a wide
// fs:* principal. It also pins the group list to pkg/datasets' list, so the two installation-wide
// denies cannot cover different principals.
func TestReservedPrefixDeniedBootstrap(t *testing.T) {
	t.Parallel()

	t.Run("the group list covers both group templates", func(t *testing.T) {
		t.Parallel()
		// The expected list is pinned as a literal: it is the non-service group set of
		// pkg/datasets.PinsProtectGroups, which this package cannot import (pkg/datasets imports it,
		// see bootstrap.go), and lakefs-sso's main passes that list in. A change on either side that
		// made the two denies cover different principals is meant to be visible here.
		groups := ReservedPrefixProtectGroups([]string{"custom-operators", "Admins", ""})
		require.Equal(t, []string{
			"Admins", "Supers", "Writers", "Readers", "SuperUsers", "Developers", "Viewers",
			"custom-operators",
		}, groups, "base groups of both templates, then the extras, deduplicated, no empty names")
	})

	t.Run("attaches, is idempotent, repairs a tampered policy, and denies", func(t *testing.T) {
		t.Parallel()
		f := newAuthzFixture(t)
		ctx := t.Context()
		f.user(t, "alice")

		// The installation runs one of the two group templates; the fixture has one of each kind,
		// plus a custom guard group, and the missing name must not be fatal.
		for _, group := range []string{"Admins", "custom-operators"} {
			_, err := f.svc.CreateGroup(ctx, &model.Group{DisplayName: group, CreatedAt: gcNow})
			require.NoErrorf(t, err, "create group %s", group)
		}
		require.NoError(t, f.svc.AddUserToGroup(ctx, "alice", "Admins"))

		// The wide grant comes from a group policy, so the deny has to be attached to the group too,
		// and the fixture's user policies must not be what decides this.
		wide := &model.Policy{DisplayName: "grants", CreatedAt: gcNow, Statement: model.Statements{{
			Effect:   model.StatementEffectAllow,
			Action:   []string{"fs:*", "catalog:*"},
			Resource: permissions.All,
		}}}
		require.NoError(t, f.svc.WritePolicy(ctx, wide, false))
		require.NoError(t, f.svc.AttachPolicyToGroup(ctx, wide.DisplayName, "Admins"))

		// A tampered policy must be repaired rather than trusted: the name is predictable, so it can
		// be pre-created with no statements.
		require.NoError(t, f.svc.WritePolicy(ctx, &model.Policy{
			DisplayName: ReservedPrefixProtectPolicyName,
			CreatedAt:   gcNow,
			Statement:   model.Statements{},
		}, false))

		require.NoError(t, EnsureReservedPrefixProtectPolicy(ctx, f.svc, gcNow,
			[]string{"custom-operators", "missing-group"}))

		stored, err := f.svc.GetPolicy(ctx, ReservedPrefixProtectPolicyName)
		require.NoError(t, err, "bootstrap creates the deny policy")
		require.True(t, reservedPrefixProtectStatementsEqual(ReservedPrefixProtectPolicy(gcNow).Statement, stored.Statement),
			"a pre-created policy with no statements is rewritten to the built-in deny: %+v", stored.Statement)

		for _, group := range []string{"Admins", "custom-operators"} {
			policies, _, err := f.svc.ListGroupPolicies(ctx, group, &model.PaginationParams{Amount: -1})
			require.NoError(t, err)
			names := make([]string, 0, len(policies))
			for _, policy := range policies {
				names = append(names, policy.DisplayName)
			}
			require.Containsf(t, names, ReservedPrefixProtectPolicyName, "group %s must carry the deny", group)
		}

		// Idempotent: a second start changes nothing and fails nothing.
		require.NoError(t, EnsureReservedPrefixProtectPolicy(ctx, f.svc, gcNow.Add(time.Hour), []string{"custom-operators"}))
		storedAgain, err := f.svc.GetPolicy(ctx, ReservedPrefixProtectPolicyName)
		require.NoError(t, err)
		require.True(t, reservedPrefixProtectStatementsEqual(stored.Statement, storedAgain.Statement),
			"a second bootstrap leaves the policy alone")

		// The point of the attachment: an administrator's fs:* is defeated on the reserved prefix.
		pointer := ReservedPrefix + "/tables/inventory/books/titles/.pointer"
		require.True(t, gcDenied(t, f, "alice", permissions.Permission{
			Action:   permissions.WriteObjectAction,
			Resource: permissions.ObjectArn("lakehouse", pointer),
		}), "a member of Admins must not be able to write a catalog key through fs:")
		require.False(t, gcDenied(t, f, "alice", permissions.Permission{
			Action:   permissions.WriteObjectAction,
			Resource: permissions.ObjectArn("lakehouse", "data/part-00000.parquet"),
		}), "ordinary writes are untouched by the deny")
	})

	t.Run("no auth service is an error, not a silent skip", func(t *testing.T) {
		t.Parallel()
		err := EnsureReservedPrefixProtectPolicy(t.Context(), nil, gcNow, nil)
		require.ErrorIs(t, err, ErrNoAuthorizer)
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
