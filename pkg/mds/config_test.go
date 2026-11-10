package mds

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/mds/indexer"
)

// minimalConfigYAML is the smallest metadata_search section that passes validation: the endpoint
// and one repository. The credential pair is optional - both or neither (G11, X7) - so the
// smallest valid section does not have it.
const minimalConfigYAML = `
metadata_search:
  lakefs_endpoint: http://lakefs:8000
  repositories:
    acme:
      branches: [main]
`

// fullConfigYAML writes every key of the guide's list (G10, G11), each with the documented
// example value, so a test can change one line and know that it changed only that rule.
const fullConfigYAML = `
metadata_search:
  lakefs_endpoint: http://lakefs:8000
  access_key_id: AKIAEXAMPLE
  secret_key: secret
  listen_address: 0.0.0.0:8080
  period: 60s
  concurrency: 25
  max_commits: 1000
  since: 2026-01-01T00:00:00Z
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

// capturePackageLog redirects the package logger - where LoadConfig reports its deprecation and
// ignored-value warnings - to a file and returns a reader for it.
func capturePackageLog(t *testing.T) func() string {
	t.Helper()
	logFile := filepath.Join(t.TempDir(), "mds-config.log")
	require.NoError(t, logging.SetOutputs([]string{logFile}, 0, 0), "redirect the package logger")
	t.Cleanup(func() {
		_ = logging.CloseWriters()
		require.NoError(t, logging.SetOutputs([]string{"-"}, 0, 0), "restore the package logger")
	})
	return func() string {
		require.NoError(t, logging.CloseWriters(), "flush the captured log")
		data, err := os.ReadFile(logFile)
		require.NoError(t, err, "read the captured log")
		return string(data)
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	useConfig(t, minimalConfigYAML)

	cfg, err := LoadConfig()
	require.NoError(t, err)

	// The settings the installation wrote.
	require.Equal(t, "http://lakefs:8000", cfg.LakeFSEndpoint)
	require.Empty(t, cfg.AccessKeyID, "the credential pair is optional")
	require.Empty(t, cfg.SecretKey)
	require.Equal(t, []string{"main"}, cfg.Repositories["acme"].Branches)

	// Every default of the guide's key list.
	require.Equal(t, DefaultListenAddress, cfg.ListenAddress)
	require.Equal(t, 60*time.Second, cfg.Period)
	require.Equal(t, 25, cfg.Concurrency)
	require.Equal(t, 1000, cfg.MaxCommits)
	require.Empty(t, cfg.Since, "since is unset by default")

	_, ok := cfg.SinceTime()
	require.False(t, ok)
}

func TestLoadConfigDocumentedExample(t *testing.T) {
	useConfig(t, fullConfigYAML)

	cfg, err := LoadConfig()
	require.NoError(t, err)

	require.Equal(t, "http://lakefs:8000", cfg.LakeFSEndpoint)
	require.Equal(t, "AKIAEXAMPLE", cfg.AccessKeyID)
	require.Equal(t, "secret", cfg.SecretKey)
	require.Equal(t, "0.0.0.0:8080", cfg.ListenAddress)
	require.Equal(t, 60*time.Second, cfg.Period)
	require.Equal(t, 25, cfg.Concurrency)
	require.Equal(t, 1000, cfg.MaxCommits)

	// A bare YAML timestamp arrives as a time.Time; the setting keeps its RFC 3339 text.
	require.Equal(t, "2026-01-01T00:00:00Z", cfg.Since)
	since, ok := cfg.SinceTime()
	require.True(t, ok)
	require.Equal(t, time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC), since)

	repo, ok := cfg.Repositories["my-repo"]
	require.True(t, ok)
	require.Equal(t, []string{"main", "dev-*"}, repo.Branches)
	require.Equal(t, "s3://bucket/metadata/my-repo", repo.StorageNamespace)
}

// TestLoadConfigValidationRules is the guide's validation list (G11): endpoint set, the credential
// pair both or neither, period > 0, concurrency >= 1 and max_commits >= 1. It is the whole list -
// nothing else fails startup - so every row here is one of those five rules.
func TestLoadConfigValidationRules(t *testing.T) {
	tests := []struct {
		name     string
		document string
		wantErr  string
	}{
		{
			name: "endpoint is required",
			document: strings.Replace(fullConfigYAML,
				"  lakefs_endpoint: http://lakefs:8000\n", "", 1),
			wantErr: "metadata_search.lakefs_endpoint is required",
		},
		{
			name: "an access key without a secret key",
			document: strings.Replace(fullConfigYAML,
				"secret_key: secret", `secret_key: ""`, 1),
			wantErr: "metadata_search.access_key_id and metadata_search.secret_key must be set together",
		},
		{
			name: "a secret key without an access key",
			document: strings.Replace(fullConfigYAML,
				"access_key_id: AKIAEXAMPLE", `access_key_id: ""`, 1),
			wantErr: "metadata_search.access_key_id and metadata_search.secret_key must be set together",
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
			name: "concurrency must be at least 1",
			document: strings.Replace(fullConfigYAML,
				"concurrency: 25", "concurrency: 0", 1),
			wantErr: "metadata_search.concurrency must be at least 1, got 0",
		},
		{
			name: "max_commits must be at least 1",
			document: strings.Replace(fullConfigYAML,
				"max_commits: 1000", "max_commits: 0", 1),
			wantErr: "metadata_search.max_commits must be at least 1, got 0",
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

// TestLoadConfigDeprecatedEndpoint pins G10's deprecation rule: lakefs_mds_endpoint is accepted
// only when it ends in /mds/iceberg/api, the suffix is stripped and a warning is logged. Our own
// configurations (hr2) use that key as a base URL, so the old form has to fail with a message that
// names the rename instead of guessing which catalog was meant.
func TestLoadConfigDeprecatedEndpoint(t *testing.T) {
	t.Run("the deprecated suffix is stripped with a warning", func(t *testing.T) {
		log := capturePackageLog(t)
		useConfig(t, `
metadata_search:
  lakefs_mds_endpoint: http://lakefs:8000/mds/iceberg/api
  repositories:
    acme:
      branches: [main]
`)

		cfg, err := LoadConfig()
		require.NoError(t, err)
		require.Equal(t, "http://lakefs:8000", cfg.LakeFSEndpoint,
			"the endpoint of a write is the lakeFS base URL, <lakefs_endpoint>/iceberg/api")

		logged := log()
		require.Contains(t, logged, "metadata_search.lakefs_mds_endpoint is deprecated")
		require.Contains(t, logged, "metadata_search.lakefs_endpoint")
		require.Contains(t, logged, "http://lakefs:8000")
	})

	t.Run("a trailing slash after the suffix is tolerated", func(t *testing.T) {
		useConfig(t, `
metadata_search:
  lakefs_mds_endpoint: http://lakefs:8000/mds/iceberg/api/
`)

		cfg, err := LoadConfig()
		require.NoError(t, err)
		require.Equal(t, "http://lakefs:8000", cfg.LakeFSEndpoint)
	})

	t.Run("a base URL fails and the message names the rename", func(t *testing.T) {
		// The hr2 form: the key carries the lakeFS base URL, which is lakefs_endpoint's value.
		useConfig(t, `
metadata_search:
  lakefs_mds_endpoint: http://lakefs:8000
  repositories:
    acme:
      branches: [main]
`)

		cfg, err := LoadConfig()
		require.Nil(t, cfg)
		require.ErrorIs(t, err, ErrInvalidConfig)
		require.ErrorContains(t, err, "metadata_search.lakefs_mds_endpoint is deprecated")
		require.ErrorContains(t, err, "/mds/iceberg/api")
		require.ErrorContains(t, err, "rename the key to metadata_search.lakefs_endpoint")
	})

	t.Run("lakefs_endpoint wins when both are set", func(t *testing.T) {
		log := capturePackageLog(t)
		useConfig(t, `
metadata_search:
  lakefs_endpoint: http://new-lakefs:8000
  lakefs_mds_endpoint: http://old-lakefs:8000/mds/iceberg/api
`)

		cfg, err := LoadConfig()
		require.NoError(t, err)
		require.Equal(t, "http://new-lakefs:8000", cfg.LakeFSEndpoint)
		require.Contains(t, log(), "deprecated spelling lakefs_mds_endpoint")
	})
}

// TestLoadConfigEmptyRepositoriesDisablesMetadataSearch pins X8: an empty repositories map and an
// empty branches list used to fail startup; the guide says metadata search activates when the map
// is non-empty, so both are accepted and the indexer has nothing to do.
func TestLoadConfigEmptyRepositoriesDisablesMetadataSearch(t *testing.T) {
	t.Run("no repositories", func(t *testing.T) {
		log := capturePackageLog(t)
		useConfig(t, `
metadata_search:
  lakefs_endpoint: http://lakefs:8000
  repositories: {}
`)

		cfg, err := LoadConfig()
		require.NoError(t, err)
		require.Empty(t, cfg.RepositoryNames())
		require.Contains(t, log(), "Metadata Search is disabled")

		opts := indexerOptions(cfg, nil, nil, "", NewHealth(cfg.RepositoryNames()), Recorder{}, nil)
		require.Empty(t, opts.Repositories, "an empty map indexes nothing")
	})

	t.Run("a repository that lists no branch", func(t *testing.T) {
		useConfig(t, `
metadata_search:
  lakefs_endpoint: http://lakefs:8000
  repositories:
    acme:
      branches: []
`)

		cfg, err := LoadConfig()
		require.NoError(t, err)
		require.Equal(t, []string{"acme"}, cfg.RepositoryNames())
	})
}

// TestLoadConfigNothingElseFailsStartup pins the second half of X6 and X9: the guide's validation
// list is the whole list. The X6 keys are gone from the section, so writing them is not an error
// and does not change anything; a repository name that ends in -metadata or is longer than 54
// characters, an unusable branch selector and a since that is not RFC 3339 are warnings at most.
func TestLoadConfigNothingElseFailsStartup(t *testing.T) {
	longName := strings.Repeat("a", 63)

	t.Run("the removed keys are ignored", func(t *testing.T) {
		useConfig(t, `
metadata_search:
  lakefs_endpoint: http://lakefs:8000
  request_timeout: 0s
  backfill: sideways
  seed_lookback: 0
  rebuild_threshold: 0
  delete_branches: false
  tag_retention: -1h
  compaction:
    max_delete_files: -1
    every_commits: -2
  storage:
    s3.endpoint: http://seaweedfs:8333
    s3.region: us-east-1
  azure:
    storage_access_key: c2VjcmV0
    protocol: ftp
  repositories:
    acme:
      branches: [main]
`)

		cfg, err := LoadConfig()
		require.NoError(t, err, "the removed keys are not part of the section any more")

		// The behaviour they used to configure is fixed at the values the keys documented.
		opts := indexerOptions(cfg, nil, nil, "", NewHealth(cfg.RepositoryNames()), Recorder{}, nil)
		require.Equal(t, indexer.DefaultRequestTimeout, opts.RequestTimeout)
		require.Equal(t, indexer.BackfillHistory, opts.Backfill, "X11: the first sync starts at branch creation")
		require.Equal(t, indexer.DefaultSeedLookback, opts.SeedLookback)
		require.Equal(t, indexer.DefaultRebuildThreshold, opts.RebuildThreshold)
		require.True(t, opts.DeleteBranches)
		require.Zero(t, opts.TagRetention)
		require.Equal(t, indexer.Compaction{
			MaxDeleteFiles: fixedCompactionMaxDeleteFiles,
			EveryCommits:   fixedCompactionEveryCommits,
		}, opts.Compaction)
		require.Nil(t, opts.Tables, "the process builds the table store, with its blockstore IO properties")
	})

	t.Run("repository names are not validated", func(t *testing.T) {
		useConfig(t, `
metadata_search:
  lakefs_endpoint: http://lakefs:8000
  repositories:
    my-repo-metadata:
      branches: [main]
    `+longName+`:
      branches: [main]
`)

		cfg, err := LoadConfig()
		require.NoError(t, err)
		require.Equal(t, []string{longName, "my-repo-metadata"}, cfg.RepositoryNames())
	})

	t.Run("a since that is not RFC 3339 is ignored with a warning", func(t *testing.T) {
		log := capturePackageLog(t)
		useConfig(t, `
metadata_search:
  lakefs_endpoint: http://lakefs:8000
  since: "yesterday"
`)

		cfg, err := LoadConfig()
		require.NoError(t, err)
		_, ok := cfg.SinceTime()
		require.False(t, ok, "an unusable since is treated as absent, not as a startup failure")
		require.Contains(t, log(), "metadata_search.since is not an RFC 3339 timestamp")
	})
}

// TestLoadConfigWarnsAboutUnmatchableSelectors pins that the branch-selector rule is a warning:
// the guide has no startup rule about it, but a selector that can never match would silently leave
// a branch unindexed.
func TestLoadConfigWarnsAboutUnmatchableSelectors(t *testing.T) {
	log := capturePackageLog(t)
	useConfig(t, `
metadata_search:
  lakefs_endpoint: http://lakefs:8000
  repositories:
    acme:
      branches: [main, "dev-*x", ""]
`)

	cfg, err := LoadConfig()
	require.NoError(t, err)
	require.Equal(t, []string{"main", "dev-*x", ""}, cfg.Repositories["acme"].Branches)

	logged := log()
	require.Contains(t, logged, "is supported in a branch pattern")
	require.Contains(t, logged, "a branch selector must not be empty")
}

// TestLoadConfigReportsEveryProblem pins "one error per problem": an operator fixes the whole
// section in one pass instead of finding the next problem on the next start.
func TestLoadConfigReportsEveryProblem(t *testing.T) {
	useConfig(t, `
metadata_search:
  access_key_id: AKIAEXAMPLE
  period: 0s
  concurrency: 0
  max_commits: 0
`)

	_, err := LoadConfig()
	require.ErrorIs(t, err, ErrInvalidConfig)
	for _, want := range []string{
		"metadata_search.lakefs_endpoint is required",
		"metadata_search.access_key_id and metadata_search.secret_key must be set together",
		"metadata_search.period must be positive",
		"metadata_search.concurrency must be at least 1",
		"metadata_search.max_commits must be at least 1",
	} {
		require.ErrorContains(t, err, want)
	}
}

func TestLoadConfigEnvironmentOverrides(t *testing.T) {
	useConfig(t, minimalConfigYAML)
	// §7: environment overrides follow lakeFS conventions.
	t.Setenv("LAKEFS_METADATA_SEARCH_PERIOD", "5s")
	t.Setenv("LAKEFS_METADATA_SEARCH_MAX_COMMITS", "17")
	t.Setenv("LAKEFS_METADATA_SEARCH_ACCESS_KEY_ID", "AKIAFROMENV")
	t.Setenv("LAKEFS_METADATA_SEARCH_SECRET_KEY", "from-the-environment")
	t.Setenv("LAKEFS_METADATA_SEARCH_SINCE", "2027-02-03T04:05:06Z")

	cfg, err := LoadConfig()
	require.NoError(t, err)
	require.Equal(t, 5*time.Second, cfg.Period)
	require.Equal(t, 17, cfg.MaxCommits)
	require.Equal(t, "AKIAFROMENV", cfg.AccessKeyID)
	require.Equal(t, "from-the-environment", cfg.SecretKey)
	since, ok := cfg.SinceTime()
	require.True(t, ok)
	require.Equal(t, time.Date(2027, time.February, 3, 4, 5, 6, 0, time.UTC), since)
}

// TestLoadConfigDeprecatedEndpointFromEnvironment pins G10 through the environment: the deprecated
// key is a scalar, so it can come from LAKEFS_METADATA_SEARCH_LAKEFS_MDS_ENDPOINT, and it is held to
// the same suffix rule there.
func TestLoadConfigDeprecatedEndpointFromEnvironment(t *testing.T) {
	t.Run("the deprecated suffix is accepted", func(t *testing.T) {
		useConfig(t, `
metadata_search:
  repositories: {}
`)
		t.Setenv("LAKEFS_METADATA_SEARCH_LAKEFS_MDS_ENDPOINT", "http://lakefs:9000/mds/iceberg/api")

		cfg, err := LoadConfig()
		require.NoError(t, err)
		require.Equal(t, "http://lakefs:9000", cfg.LakeFSEndpoint)
	})

	t.Run("the base URL form is refused", func(t *testing.T) {
		useConfig(t, `
metadata_search:
  repositories: {}
`)
		t.Setenv("LAKEFS_METADATA_SEARCH_LAKEFS_MDS_ENDPOINT", "http://lakefs:9000")

		cfg, err := LoadConfig()
		require.Nil(t, cfg)
		require.ErrorIs(t, err, ErrInvalidConfig)
		require.ErrorContains(t, err, "rename the key to metadata_search.lakefs_endpoint")
	})
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

// TestIndexerOptionsFromConfig pins the wiring of what is left in the section: the endpoint the
// indexer writes to is lakefs_endpoint (G10), the credentials are the optional pair (X7), the
// per-tick bounds are the ones the operator wrote, and since reaches the indexer as a timestamp.
func TestIndexerOptionsFromConfig(t *testing.T) {
	useConfig(t, fullConfigYAML)

	cfg, err := LoadConfig()
	require.NoError(t, err)

	opts := indexerOptions(cfg, nil, nil, "", NewHealth(cfg.RepositoryNames()), Recorder{}, nil)
	require.Equal(t, "http://lakefs:8000", opts.Endpoint)
	require.Equal(t, "AKIAEXAMPLE", opts.AccessKeyID)
	require.Equal(t, "secret", opts.SecretKey)
	require.Equal(t, 25, opts.Concurrency)
	require.Equal(t, 1000, opts.MaxCommits)
	require.NotNil(t, opts.Since)
	require.Equal(t, time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC), *opts.Since)
	require.Equal(t, []indexer.Repository{{
		Name:             "my-repo",
		Branches:         []string{"main", "dev-*"},
		StorageNamespace: "s3://bucket/metadata/my-repo",
	}}, opts.Repositories)
}

// TestLoadConfigNoCredentials pins the "both or neither" half of X7 once more from the other side:
// a section without the pair is valid, which is how an indexer runs as the server-provisioned
// service user.
func TestLoadConfigNoCredentials(t *testing.T) {
	useConfig(t, `
metadata_search:
  lakefs_endpoint: http://lakefs:8000
  access_key_id: ""
  secret_key: ""
  repositories:
    acme:
      branches: [main]
`)

	cfg, err := LoadConfig()
	require.NoError(t, err)
	require.Empty(t, cfg.AccessKeyID)
	require.Empty(t, cfg.SecretKey)
}
