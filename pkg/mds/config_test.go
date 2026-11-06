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
	require.NotContains(t, cfg.Storage, "adls.endpoint",
		"an installation without metadata_search.azure gets exactly the storage map it wrote")
	require.Zero(t, cfg.Azure, "the azure section is optional")

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
		{
			name: "tag_retention must not be negative",
			document: strings.Replace(fullConfigYAML,
				"tag_retention: 0s", "tag_retention: -1h", 1),
			wantErr: "metadata_search.tag_retention must not be negative, got -1h0m0s; 0 keeps every mds-* tag",
		},
		{
			name: "max_delete_files must not be negative",
			document: strings.Replace(fullConfigYAML,
				"max_delete_files: 200", "max_delete_files: -1", 1),
			wantErr: "metadata_search.compaction.max_delete_files must not be negative, got -1; 0 disables the trigger",
		},
		{
			name: "every_commits must not be negative",
			document: strings.Replace(fullConfigYAML,
				"every_commits: 500", "every_commits: -2", 1),
			wantErr: "metadata_search.compaction.every_commits must not be negative, got -2; 0 disables the trigger",
		},
		{
			// D3: iceberg-go needs the account name to build the container URL and would refuse the
			// key only at the first write, with an error that does not name the setting.
			name: "a shared key needs its storage account",
			document: strings.Replace(fullConfigYAML,
				"  repositories:\n", "  azure:\n    storage_access_key: c2VjcmV0\n  repositories:\n", 1),
			wantErr: "metadata_search.azure.storage_access_key requires metadata_search.azure.storage_account",
		},
		{
			name: "the azure protocol must be http or https",
			document: strings.Replace(fullConfigYAML,
				"  repositories:\n", "  azure:\n    protocol: ftp\n  repositories:\n", 1),
			wantErr: `metadata_search.azure.protocol must be "http" or "https", got "ftp"`,
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

// TestLoadConfigMaintenanceDisabledByZero pins the IO-6 rule that matches the sample config and
// docs/metadata-search.md: an explicit 0 is the documented way to turn a compaction trigger off
// (and to keep every mds-* tag forever), so it survives validation, reaches the indexer as 0, and
// is not replaced by the documented default - which applies only to an absent key.
func TestLoadConfigMaintenanceDisabledByZero(t *testing.T) {
	document := strings.Replace(fullConfigYAML, "max_delete_files: 200", "max_delete_files: 0", 1)
	document = strings.Replace(document, "every_commits: 500", "every_commits: 0", 1)
	useConfig(t, document)

	cfg, err := LoadConfig()
	require.NoError(t, err, "0 is a valid value, not a problem to report")
	require.Zero(t, cfg.Compaction.MaxDeleteFiles)
	require.Zero(t, cfg.Compaction.EveryCommits)
	require.Zero(t, cfg.TagRetention)

	opts := indexerOptions(cfg, NewHealth(cfg.RepositoryNames()), Recorder{}, nil)
	require.Zero(t, opts.Compaction.MaxDeleteFiles, `0 reaches the indexer as "disabled", not as the default`)
	require.Zero(t, opts.Compaction.EveryCommits, `0 reaches the indexer as "disabled", not as the default`)
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

// TestLoadConfigAzureFromEnvironment is the deployment story of D3: the whole Azure section can be
// a Kubernetes secret, one LAKEFS_METADATA_SEARCH_AZURE_* environment variable per key, and it
// reaches iceberg-go as the adls.* IO properties of the design's translation table.
func TestLoadConfigAzureFromEnvironment(t *testing.T) {
	useConfig(t, minimalConfigYAML)
	t.Setenv("LAKEFS_METADATA_SEARCH_AZURE_STORAGE_ACCOUNT", "acct")
	t.Setenv("LAKEFS_METADATA_SEARCH_AZURE_STORAGE_ACCESS_KEY", "c2VjcmV0")
	t.Setenv("LAKEFS_METADATA_SEARCH_AZURE_USE_MANAGED_IDENTITY", "true")
	t.Setenv("LAKEFS_METADATA_SEARCH_AZURE_CLIENT_ID", "00000000-0000-0000-0000-000000000000")
	t.Setenv("LAKEFS_METADATA_SEARCH_AZURE_ENDPOINT", "azurite.test:10000")
	t.Setenv("LAKEFS_METADATA_SEARCH_AZURE_PROTOCOL", "http")

	cfg, err := LoadConfig()
	require.NoError(t, err)

	require.Equal(t, AzureConfig{
		StorageAccount:     "acct",
		StorageAccessKey:   "c2VjcmV0",
		UseManagedIdentity: true,
		ClientID:           "00000000-0000-0000-0000-000000000000",
		Endpoint:           "azurite.test:10000",
		Protocol:           "http",
	}, cfg.Azure)
	require.Equal(t, map[string]string{
		"adls.auth.shared-key.account.name":  "acct",
		"adls.auth.shared-key.account.key":   "c2VjcmV0",
		"adls.auth.managed-identity.enabled": "true",
		"adls.client-id":                     "00000000-0000-0000-0000-000000000000",
		"adls.endpoint":                      "azurite.test:10000",
		"adls.protocol":                      "http",
	}, cfg.Storage)
}

// TestLoadConfigAzurePartialSection pins the other half of the translation: an unset key is not
// translated at all, so iceberg-go keeps its own default (https, the default credential chain, the
// public blob domain) instead of a value lakeFS would have had to invent.
func TestLoadConfigAzurePartialSection(t *testing.T) {
	useConfig(t, minimalConfigYAML+
		"  azure:\n    storage_account: acct\n    storage_access_key: c2VjcmV0\n")

	cfg, err := LoadConfig()
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"adls.auth.shared-key.account.name": "acct",
		"adls.auth.shared-key.account.key":  "c2VjcmV0",
	}, cfg.Storage)
}

// TestLoadConfigAzureStorageKeysWin pins the precedence of D3: metadata_search.storage is the raw
// iceberg-go property map, so an explicit key wins over the translated one, and a key the azure
// section has no setting for (adls.sas-token.<host>, an S3 property of the same installation)
// passes through untouched.
func TestLoadConfigAzureStorageKeysWin(t *testing.T) {
	useConfig(t, minimalConfigYAML+`  storage:
    adls.endpoint: from-the-file
    adls.protocol: https
    adls.sas-token.acct.dfs.core.windows.net: "?sig=explicit"
    s3.endpoint: http://seaweedfs:8333
  azure:
    storage_account: acct
    storage_access_key: c2VjcmV0
    endpoint: from-the-section
    protocol: http
`)

	cfg, err := LoadConfig()
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"adls.auth.shared-key.account.name":        "acct",
		"adls.auth.shared-key.account.key":         "c2VjcmV0",
		"adls.endpoint":                            "from-the-file",
		"adls.protocol":                            "https",
		"adls.sas-token.acct.dfs.core.windows.net": "?sig=explicit",
		"s3.endpoint":                              "http://seaweedfs:8333",
	}, cfg.Storage)
}

// TestLoadConfigAzureDoesNotReadBlockstore pins D3's last rule: the indexer has no access to the
// server's blockstore section, so an Azure blockstore key cannot stand in for the indexer's own
// credentials. A missing metadata_search.azure stays missing.
func TestLoadConfigAzureDoesNotReadBlockstore(t *testing.T) {
	useConfig(t, minimalConfigYAML+`  azure: {}
blockstore:
  type: azure
  azure:
    storage_account: acct
    storage_access_key: c2VjcmV0
`)

	cfg, err := LoadConfig()
	require.NoError(t, err)
	require.Zero(t, cfg.Azure)
	require.Nil(t, cfg.Storage, "blockstore.azure.* is not an IO property of the indexer")
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
