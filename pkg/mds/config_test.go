package mds

import (
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/icebergcatalog"
)

// minimalConfigYAML is the smallest metadata_search section that passes validation: only the
// three required settings and one repository with one branch.
const minimalConfigYAML = `
metadata_search:
  lakefs_mds_endpoint: http://lakefs:8000
  access_key_id: AKIAEXAMPLE
  secret_key: secret
  repositories:
    acme:
      branches: [main]
`

// fullConfigYAML writes every key of METADATA_SEARCH_DESIGN.md §7 with the value of the
// documented example, so a test can change one line and know that it changed only that rule.
const fullConfigYAML = `
metadata_search:
  lakefs_mds_endpoint: http://lakefs:8000
  access_key_id: AKIAEXAMPLE
  secret_key: secret
  listen_address: 0.0.0.0:8080
  period: 60s
  request_timeout: 60s
  concurrency: 25
  max_commits: 1000
  since: 2026-01-01T00:00:00Z
  backfill: head
  seed_lookback: 1000
  rebuild_threshold: 100000
  delete_branches: true
  tag_retention: 0s
  compaction:
    max_delete_files: 200
    every_commits: 500
  storage:
    s3.endpoint: http://seaweedfs:8333
    s3.region: us-east-1
    s3.path-style-access: "true"
  repositories:
    my-repo:
      branches: [main, "dev-*"]
      storage_namespace: s3://bucket/metadata/my-repo
`

// useConfig installs a viper state backed by the given YAML document with the environment
// conventions of §7, so that LoadConfig reads exactly what `lakefs-sso mds run --config` reads.
func useConfig(t *testing.T, document string) {
	t.Helper()
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.SetConfigType("yaml")
	viper.SetEnvPrefix("LAKEFS")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()
	require.NoError(t, viper.ReadConfig(strings.NewReader(document)))
}

func TestLoadConfigDefaults(t *testing.T) {
	useConfig(t, minimalConfigYAML)

	cfg, err := LoadConfig()
	require.NoError(t, err)

	// The settings the installation wrote.
	require.Equal(t, "http://lakefs:8000", cfg.LakeFSMDSEndpoint)
	require.Equal(t, "AKIAEXAMPLE", cfg.AccessKeyID)
	require.Equal(t, "secret", cfg.SecretKey)
	require.Equal(t, []string{"main"}, cfg.Repositories["acme"].Branches)

	// Every default of §7.
	require.Equal(t, DefaultListenAddress, cfg.ListenAddress)
	require.Equal(t, 60*time.Second, cfg.Period)
	require.Equal(t, DefaultRequestTimeout, cfg.RequestTimeout, "a request of either client is bounded by default")
	require.Equal(t, 60*time.Second, cfg.RequestTimeout)
	require.Equal(t, 25, cfg.Concurrency)
	require.Equal(t, 1000, cfg.MaxCommits)
	require.Equal(t, BackfillHead, cfg.Backfill)
	require.Equal(t, 1000, cfg.SeedLookback)
	require.Equal(t, 100_000, cfg.RebuildThreshold)
	require.True(t, cfg.DeleteBranches, "deleting a branch of the data repository deletes its mirror by default")
	require.Equal(t, time.Duration(0), cfg.TagRetention, "tags are kept forever by default")
	require.Equal(t, 200, cfg.Compaction.MaxDeleteFiles)
	require.Equal(t, 500, cfg.Compaction.EveryCommits)
	require.Empty(t, cfg.Since, "since is unset by default")
	require.Nil(t, cfg.Storage)

	_, ok := cfg.SinceTime()
	require.False(t, ok)
}

func TestLoadConfigDocumentedExample(t *testing.T) {
	useConfig(t, fullConfigYAML)

	cfg, err := LoadConfig()
	require.NoError(t, err)

	require.Equal(t, "http://lakefs:8000", cfg.LakeFSMDSEndpoint)
	require.Equal(t, "AKIAEXAMPLE", cfg.AccessKeyID)
	require.Equal(t, "secret", cfg.SecretKey)
	require.Equal(t, "0.0.0.0:8080", cfg.ListenAddress)
	require.Equal(t, 60*time.Second, cfg.Period)
	require.Equal(t, 60*time.Second, cfg.RequestTimeout)
	require.Equal(t, 25, cfg.Concurrency)
	require.Equal(t, 1000, cfg.MaxCommits)
	require.Equal(t, BackfillHead, cfg.Backfill)
	require.Equal(t, 1000, cfg.SeedLookback)
	require.Equal(t, 100_000, cfg.RebuildThreshold)
	require.True(t, cfg.DeleteBranches)
	require.Equal(t, time.Duration(0), cfg.TagRetention)
	require.Equal(t, 200, cfg.Compaction.MaxDeleteFiles)
	require.Equal(t, 500, cfg.Compaction.EveryCommits)

	// A bare YAML timestamp arrives as a time.Time; the setting keeps its RFC 3339 text.
	require.Equal(t, "2026-01-01T00:00:00Z", cfg.Since)
	since, ok := cfg.SinceTime()
	require.True(t, ok)
	require.Equal(t, time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC), since)

	// Dotted storage keys are the iceberg-go IO properties, taken verbatim (§4.6).
	require.Len(t, cfg.Storage, 3)
	require.Equal(t, "http://seaweedfs:8333", cfg.Storage["s3.endpoint"])
	require.Equal(t, "true", cfg.Storage["s3.path-style-access"])
	require.Equal(t, "us-east-1", cfg.Storage["s3.region"])

	repo, ok := cfg.Repositories["my-repo"]
	require.True(t, ok)
	require.Equal(t, []string{"main", "dev-*"}, repo.Branches)
	require.Equal(t, "s3://bucket/metadata/my-repo", repo.StorageNamespace)
}

// TestLoadConfigValidationRules is the table of design §7: every documented startup error, one
// table row per rule, checked against the message Validate reports for it.
func TestLoadConfigValidationRules(t *testing.T) {
	longName := strings.Repeat("a", MaxRepositoryNameLength+1)
	repositoriesBlock := "  repositories:\n    my-repo:\n      branches: [main, \"dev-*\"]\n      storage_namespace: s3://bucket/metadata/my-repo\n"

	tests := []struct {
		name     string
		document string
		wantErr  string
	}{
		{
			name: "endpoint is required",
			document: strings.Replace(fullConfigYAML,
				"  lakefs_mds_endpoint: http://lakefs:8000\n", "", 1),
			wantErr: "metadata_search.lakefs_mds_endpoint is required",
		},
		{
			name: "access key is required",
			document: strings.Replace(fullConfigYAML,
				"access_key_id: AKIAEXAMPLE", `access_key_id: ""`, 1),
			wantErr: "metadata_search.access_key_id is required",
		},
		{
			name: "secret key is required",
			document: strings.Replace(fullConfigYAML,
				"secret_key: secret", `secret_key: ""`, 1),
			wantErr: "metadata_search.secret_key is required",
		},
		{
			name: "period must be positive",
			document: strings.Replace(fullConfigYAML,
				"period: 60s", "period: 0s", 1),
			wantErr: "metadata_search.period must be positive, got 0s",
		},
		{
			name: "negative period must be positive",
			document: strings.Replace(fullConfigYAML,
				"period: 60s", "period: -1s", 1),
			wantErr: "metadata_search.period must be positive, got -1s",
		},
		{
			name: "request_timeout must be positive",
			document: strings.Replace(fullConfigYAML,
				"request_timeout: 60s", "request_timeout: 0s", 1),
			wantErr: "metadata_search.request_timeout must be positive, got 0s",
		},
		{
			name: "negative request_timeout must be positive",
			document: strings.Replace(fullConfigYAML,
				"request_timeout: 60s", "request_timeout: -1m", 1),
			wantErr: "metadata_search.request_timeout must be positive, got -1m0s",
		},
		{
			name: "concurrency must be positive",
			document: strings.Replace(fullConfigYAML,
				"concurrency: 25", "concurrency: 0", 1),
			wantErr: "metadata_search.concurrency must be positive, got 0",
		},
		{
			name: "max_commits must be positive",
			document: strings.Replace(fullConfigYAML,
				"max_commits: 1000", "max_commits: 0", 1),
			wantErr: "metadata_search.max_commits must be positive, got 0",
		},
		{
			name: "seed_lookback must be positive",
			document: strings.Replace(fullConfigYAML,
				"seed_lookback: 1000", "seed_lookback: 0", 1),
			wantErr: "metadata_search.seed_lookback must be positive, got 0",
		},
		{
			name: "rebuild_threshold must be positive",
			document: strings.Replace(fullConfigYAML,
				"rebuild_threshold: 100000", "rebuild_threshold: 0", 1),
			wantErr: "metadata_search.rebuild_threshold must be positive, got 0",
		},
		{
			name: "backfill must be known",
			document: strings.Replace(fullConfigYAML,
				"backfill: head", "backfill: sideways", 1),
			wantErr: `metadata_search.backfill must be "head" or "history", got "sideways"`,
		},
		{
			name:     "repositories must not be empty",
			document: strings.Replace(fullConfigYAML, repositoriesBlock, "  repositories: {}\n", 1),
			wantErr:  "metadata_search.repositories must list at least one repository",
		},
		{
			name: "branches must not be empty",
			document: strings.Replace(fullConfigYAML,
				"branches: [main, \"dev-*\"]", "branches: []", 1),
			wantErr: "metadata_search.repositories.my-repo.branches must list at least one branch",
		},
		{
			name:     "repository name must not end in the companion suffix",
			document: strings.Replace(fullConfigYAML, "    my-repo:", "    my-repo-metadata:", 1),
			wantErr:  `must not end in "-metadata"`,
		},
		{
			name: "repository name must fit the companion suffix",
			document: strings.Replace(fullConfigYAML,
				"    my-repo:", "    "+longName+":", 1),
			wantErr: "metadata_search.repositories." + longName +
				": a data repository name must not be longer than 54 characters",
		},
		{
			name: "glob must have a trailing star",
			document: strings.Replace(fullConfigYAML,
				"branches: [main, \"dev-*\"]", `branches: [main, "dev-*x"]`, 1),
			wantErr: `only a single trailing "*" is supported in a branch pattern, got "dev-*x"`,
		},
		{
			name: "glob must not start with a star",
			document: strings.Replace(fullConfigYAML,
				"branches: [main, \"dev-*\"]", `branches: ["*dev"]`, 1),
			wantErr: `only a single trailing "*" is supported in a branch pattern, got "*dev"`,
		},
		{
			name: "branch selector must not be empty",
			document: strings.Replace(fullConfigYAML,
				"branches: [main, \"dev-*\"]", `branches: [""]`, 1),
			wantErr: "a branch selector must not be empty",
		},
		{
			name: "since must be RFC 3339",
			document: strings.Replace(fullConfigYAML,
				"  since: 2026-01-01T00:00:00Z", "  since: \"yesterday\"", 1),
			wantErr: `metadata_search.since "yesterday" is not an RFC 3339 timestamp`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			useConfig(t, tt.document)

			cfg, err := LoadConfig()
			require.Nil(t, cfg)
			require.ErrorIs(t, err, ErrInvalidConfig)
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

// TestLoadConfigReportsEveryProblem pins "one error per problem": an operator fixes the whole
// section in one pass instead of finding the next problem on the next start.
func TestLoadConfigReportsEveryProblem(t *testing.T) {
	useConfig(t, `
metadata_search:
  access_key_id: ""
  secret_key: secret
  period: 0s
  backfill: sideways
  repositories: {}
`)

	_, err := LoadConfig()
	require.ErrorIs(t, err, ErrInvalidConfig)
	for _, want := range []string{
		"metadata_search.lakefs_mds_endpoint is required",
		"metadata_search.access_key_id is required",
		"metadata_search.period must be positive",
		`metadata_search.backfill must be "head" or "history"`,
		"metadata_search.repositories must list at least one repository",
	} {
		require.ErrorContains(t, err, want)
	}
}

func TestLoadConfigEnvironmentOverrides(t *testing.T) {
	useConfig(t, minimalConfigYAML)
	// §7: environment overrides follow lakeFS conventions.
	t.Setenv("LAKEFS_METADATA_SEARCH_PERIOD", "5s")
	t.Setenv("LAKEFS_METADATA_SEARCH_REQUEST_TIMEOUT", "7s")
	t.Setenv("LAKEFS_METADATA_SEARCH_SECRET_KEY", "from-the-environment")
	t.Setenv("LAKEFS_METADATA_SEARCH_DELETE_BRANCHES", "false")
	t.Setenv("LAKEFS_METADATA_SEARCH_SINCE", "2027-02-03T04:05:06Z")

	cfg, err := LoadConfig()
	require.NoError(t, err)
	require.Equal(t, 5*time.Second, cfg.Period)
	require.Equal(t, 7*time.Second, cfg.RequestTimeout)
	require.Equal(t, "from-the-environment", cfg.SecretKey)
	require.False(t, cfg.DeleteBranches, "an explicit env override wins over the true default")
	since, ok := cfg.SinceTime()
	require.True(t, ok)
	require.Equal(t, time.Date(2027, time.February, 3, 4, 5, 6, 0, time.UTC), since)
}

// TestLoadConfigRepositoryNameBoundary pins the boundary of §3.3: a 54-character name still fits
// a 63-character repository ID together with "-metadata", one more character does not.
func TestLoadConfigRepositoryNameBoundary(t *testing.T) {
	require.Equal(t, 54, MaxRepositoryNameLength)
	require.Equal(t, maxRepositoryIDLength, MaxRepositoryNameLength+len(icebergcatalog.MetadataRepositorySuffix))

	name := strings.Repeat("a", MaxRepositoryNameLength)
	useConfig(t, strings.Replace(fullConfigYAML, "    my-repo:", "    "+name+":", 1))

	cfg, err := LoadConfig()
	require.NoError(t, err)
	require.Contains(t, cfg.Repositories, name)
}

func TestValidateBranchGlob(t *testing.T) {
	tests := []struct {
		glob    string
		wantErr bool
	}{
		{glob: "main"},
		{glob: "release-1.2"},
		{glob: "dev-*"},
		{glob: "*"},
		{glob: "", wantErr: true},
		{glob: "*dev", wantErr: true},
		{glob: "dev-*x", wantErr: true},
		{glob: "a*b", wantErr: true},
		{glob: "**", wantErr: true},
	}
	for _, tt := range tests {
		t.Run("glob="+tt.glob, func(t *testing.T) {
			err := validateBranchGlob(tt.glob)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestRepositoryNamesSorted(t *testing.T) {
	cfg := &Config{Repositories: map[string]RepositoryConfig{
		"zeta":  {Branches: []string{"main"}},
		"alpha": {Branches: []string{"main"}},
		"mid":   {Branches: []string{"main"}},
	}}
	require.Equal(t, []string{"alpha", "mid", "zeta"}, cfg.RepositoryNames())

	var nilConfig *Config
	require.Nil(t, nilConfig.RepositoryNames())
}

// TestRequestTimeoutReachesTheIndexer pins the wiring of the bound: indexerOptions is the one place
// where the metadata_search section and the indexer meet, so the value an operator writes must be
// the value the indexer's clients carry (METADATA_SEARCH_DESIGN.md §4.1, §7).
func TestRequestTimeoutReachesTheIndexer(t *testing.T) {
	useConfig(t, strings.Replace(fullConfigYAML, "request_timeout: 60s", "request_timeout: 5s", 1))

	cfg, err := LoadConfig()
	require.NoError(t, err)
	require.Equal(t, 5*time.Second, cfg.RequestTimeout)

	opts := indexerOptions(cfg, NewHealth(cfg.RepositoryNames()), Recorder{}, nil)
	require.Equal(t, 5*time.Second, opts.RequestTimeout,
		"the request bound of the configuration is the request bound of the indexer's clients")
}
