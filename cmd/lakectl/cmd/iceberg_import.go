package cmd

// lakectl iceberg import: bring tables from another Iceberg catalog into a lakeFS repository.
//
// The command is a client of the source catalog, of the lakeFS Iceberg REST catalog and of the
// lakeFS object API. All of the protocol work lives in pkg/icebergcatalog/importer; this file only
// parses flags, supplies the lakeFS credentials and an object uploader, and prints the per-table
// summary.

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/helpers"
	"github.com/treeverse/lakefs/pkg/icebergcatalog/importer"
)

const (
	icebergImportFlagSourceURI = "source-uri"
	//nolint:gosec // a flag name, not a credential
	icebergImportFlagSourceCredential        = "source-credential"
	icebergImportFlagSourceToken             = "source-token"
	icebergImportFlagSourceWarehouse         = "source-warehouse"
	icebergImportFlagSourceProp              = "source-prop"
	icebergImportFlagRepository              = "repo"
	icebergImportFlagBranch                  = "branch"
	icebergImportFlagNamespace               = "namespace"
	icebergImportFlagAll                     = "all"
	icebergImportFlagTargetNamespacePrefix   = "target-namespace-prefix"
	icebergImportFlagOverwrite               = "overwrite"
	icebergImportFlagDryRun                  = "dry-run"
	icebergImportFlagCopy                    = "copy"
	icebergImportNamespaceSeparator          = "\x1f"
	icebergImportObjectContentType           = "application/octet-stream"
	icebergImportLakeFSEndpointAPIPathSuffix = "/api/v1"
)

const icebergImportSummaryTemplate = `{{ if .DryRun }}Dry run: no changes were made.
{{ end }}Imported: {{ .Imported }}  Skipped: {{ .Skipped }}  Failed: {{ .Failed }}{{ if .DryRun }}  Planned: {{ .Planned }}{{ end }}
`

var icebergCmd = &cobra.Command{
	Use:   "iceberg",
	Short: "Work with Iceberg tables in lakeFS",
}

var icebergImportCmd = &cobra.Command{
	Use:   "import",
	Short: "Import Iceberg tables from another REST catalog",
	Long: `Import Apache Iceberg tables from a source Iceberg REST catalog into a lakeFS repository.

The source can be any catalog that speaks the Iceberg REST protocol (lakeFS, Nessie, Hive, Glue and
other implementations expose one). Tables are adopted through the lakeFS Iceberg catalog's register
operation, which requires their files to live inside the repository's storage namespace. Without
--copy the command therefore refuses a table whose locations are foreign and says so per table;
with --copy it copies the metadata and data files into the storage namespace, rewrites the absolute
locations in the metadata document, the manifest lists and the manifests, and registers the
rewritten document.

Namespaces that do not exist yet are created on the branch. Nothing is committed: the catalog
stages its writes, so publish the import with "lakectl commit".

The import is per table and resumable: a table that already exists is skipped unless --overwrite is
set, and a failure is reported for that table while the remaining tables are imported. Format
version 3 tables are refused.`,
	Example: `  lakectl iceberg import --source-uri http://nessie:19120/iceberg \
    --repo analytics --branch main --namespace sales --target-namespace-prefix imported

  lakectl iceberg import --source-uri https://glue.us-east-1.amazonaws.com/iceberg \
    --source-warehouse 123456789012 --source-prop s3.region=us-east-1 \
    --repo analytics --branch main --all --copy --dry-run`,
	Args: cobra.NoArgs,
	Run: func(cmd *cobra.Command, _ []string) {
		runIcebergImport(cmd.Context(), cmd)
	},
}

//nolint:gochecknoinits
func init() {
	flags := icebergImportCmd.Flags()
	flags.String(icebergImportFlagSourceURI, "", "Source Iceberg REST catalog base URL (its /v1/config lives at <URL>/v1/config)")
	flags.String(icebergImportFlagSourceCredential, "", "Source OAuth2 client credential, client_id:client_secret")
	flags.String(icebergImportFlagSourceToken, "", "Source bearer token; mutually exclusive with --source-credential")
	flags.String(icebergImportFlagSourceWarehouse, "", "Source warehouse, sent as the warehouse query parameter of the source /v1/config")
	flags.StringArray(icebergImportFlagSourceProp, nil, "Source client setting k=v (prefix, oauth2-server-uri, scope, s3.*); repeatable")
	flags.String(icebergImportFlagRepository, "", "lakeFS repository to import into")
	flags.String(icebergImportFlagBranch, "", "lakeFS branch to import into")
	flags.StringArray(icebergImportFlagNamespace, nil, "Source namespace to import, with its descendants; repeatable")
	flags.Bool(icebergImportFlagAll, false, "Import every namespace of the source catalog")
	flags.String(icebergImportFlagTargetNamespacePrefix, "", "Namespace levels to prepend to every imported namespace, dot-separated")
	flags.Bool(icebergImportFlagOverwrite, false, "Replace tables that already exist in the target")
	flags.Bool(icebergImportFlagDryRun, false, "Report what would be imported without writing anything")
	flags.Bool(icebergImportFlagCopy, false, "Copy table files into the repository storage namespace and rewrite locations")

	icebergCmd.AddCommand(icebergImportCmd)
	rootCmd.AddCommand(icebergCmd)
}

// runIcebergImport executes one import and prints its summary.
func runIcebergImport(ctx context.Context, cmd *cobra.Command) {
	flags := cmd.Flags()
	repository := Must(flags.GetString(icebergImportFlagRepository))
	branch := Must(flags.GetString(icebergImportFlagBranch))
	if repository == "" || branch == "" {
		Die("--repo and --branch are required", 1)
	}

	sourceProps, err := parseIcebergImportProps(Must(flags.GetStringArray(icebergImportFlagSourceProp)))
	if err != nil {
		DieErr(err)
	}
	namespaces, all, err := parseIcebergImportNamespaces(Must(flags.GetStringArray(icebergImportFlagNamespace)),
		Must(flags.GetBool(icebergImportFlagAll)))
	if err != nil {
		DieErr(err)
	}
	targetPrefix, err := parseIcebergImportNamespace(Must(flags.GetString(icebergImportFlagTargetNamespacePrefix)))
	if err != nil {
		DieErr(fmt.Errorf("--%s: %w", icebergImportFlagTargetNamespacePrefix, err))
	}

	copyMode := Must(flags.GetBool(icebergImportFlagCopy))
	dryRun := Must(flags.GetBool(icebergImportFlagDryRun))
	overwrite := Must(flags.GetBool(icebergImportFlagOverwrite))

	source, err := importer.NewSource(importer.SourceConfig{
		URI:        Must(flags.GetString(icebergImportFlagSourceURI)),
		Credential: Must(flags.GetString(icebergImportFlagSourceCredential)),
		Token:      Must(flags.GetString(icebergImportFlagSourceToken)),
		Warehouse:  Must(flags.GetString(icebergImportFlagSourceWarehouse)),
		Props:      sourceProps,
		HTTPClient: getHTTPClientWithRetryConfig(lakectlRetryPolicy, RetriesCfg{}),
	})
	if err != nil {
		DieErr(err)
	}

	client := getClient()
	storageNamespace, err := lakeFSRepositoryStorageNamespace(ctx, client, repository)
	if err != nil {
		DieErr(err)
	}

	catalogURL, err := lakeFSCatalogURL(string(cfg.Server.EndpointURL))
	if err != nil {
		DieErr(err)
	}
	target, err := importer.NewTarget(importer.TargetConfig{
		BaseURL:    catalogURL,
		HTTPClient: getHTTPClientWithRetryConfig(lakectlRetryPolicy, RetriesCfg{}),
		Authorize:  addLakeFSCatalogCredentials,
	})
	if err != nil {
		DieErr(err)
	}

	options := importer.Options{
		Source:           source,
		Target:           target,
		Repository:       repository,
		Branch:           branch,
		StorageNamespace: storageNamespace,
		Namespaces:       namespaces,
		All:              all,
		TargetPrefix:     targetPrefix,
		Overwrite:        overwrite,
		Copy:             copyMode,
		DryRun:           dryRun,
	}
	if copyMode {
		options.FileReader = importer.NewFileReader(sourceProps, getHTTPClientWithRetryConfig(lakectlRetryPolicy, RetriesCfg{}))
		options.Uploader = lakeFSObjectUploader{client: client}
	}

	summary, err := importer.Import(ctx, options)
	if err != nil {
		DieErr(err)
	}
	printIcebergImportSummary(summary)
	if summary.Failed > 0 {
		DieFmt("%d of %d table(s) failed to import", summary.Failed, len(summary.Results))
	}
}

// parseIcebergImportProps turns repeated k=v values into a map. The first '=' separates the key
// from the value, so a value may contain '=' (a credential, a URL query).
func parseIcebergImportProps(values []string) (map[string]string, error) {
	props := make(map[string]string, len(values))
	for _, value := range values {
		key, val, ok := strings.Cut(value, "=")
		key = strings.TrimSpace(key)
		if !ok || key == "" {
			return nil, fmt.Errorf("%w: --%s %q is not k=v", importer.ErrInvalidOptions, icebergImportFlagSourceProp, value)
		}
		props[key] = strings.TrimSpace(val)
	}
	return props, nil
}

// parseIcebergImportNamespaces parses the --namespace values and --all. One of them is required;
// an explicit --namespace wins only when --all is not set, and setting both is an error.
func parseIcebergImportNamespaces(values []string, all bool) ([]importer.Namespace, bool, error) {
	if all && len(values) > 0 {
		return nil, false, fmt.Errorf("%w: --%s and --%s are mutually exclusive",
			importer.ErrInvalidOptions, icebergImportFlagNamespace, icebergImportFlagAll)
	}
	if !all && len(values) == 0 {
		return nil, false, fmt.Errorf("%w: one of --%s or --%s is required",
			importer.ErrInvalidOptions, icebergImportFlagNamespace, icebergImportFlagAll)
	}
	namespaces := make([]importer.Namespace, 0, len(values))
	for _, value := range values {
		namespace, err := parseIcebergImportNamespace(value)
		if err != nil {
			return nil, false, err
		}
		namespaces = append(namespaces, namespace)
	}
	return namespaces, all, nil
}

// parseIcebergImportNamespace parses one namespace value. The value is split on the Iceberg unit
// separator when it carries one (the only unambiguous form, since a lakeFS namespace level may
// itself contain a dot), and on '.' otherwise. An empty value is an empty namespace.
func parseIcebergImportNamespace(value string) (importer.Namespace, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	separator := "."
	if strings.Contains(value, icebergImportNamespaceSeparator) {
		separator = icebergImportNamespaceSeparator
	}
	levels := strings.Split(value, separator)
	for i, level := range levels {
		level = strings.TrimSpace(level)
		if level == "" {
			return nil, fmt.Errorf("%w: namespace %q has an empty level", importer.ErrInvalidOptions, value)
		}
		if level == "." || level == ".." {
			return nil, fmt.Errorf("%w: namespace %q has the reserved level %q", importer.ErrInvalidOptions, value, level)
		}
		levels[i] = level
	}
	return importer.Namespace(levels), nil
}

// lakeFSRepositoryStorageNamespace reads the repository's storage namespace, which is what the
// importer checks a metadata location against and what a copied table is placed under.
func lakeFSRepositoryStorageNamespace(ctx context.Context, client *apigen.ClientWithResponses, repository string) (string, error) {
	resp, err := client.GetRepositoryWithResponse(ctx, repository)
	if err != nil {
		return "", err
	}
	if resp.JSON200 == nil {
		return "", helpers.ResponseAsError(resp)
	}
	storageNamespace := strings.TrimSpace(resp.JSON200.StorageNamespace)
	if storageNamespace == "" {
		return "", fmt.Errorf("%w: repository %q has no storage namespace", importer.ErrTarget, repository)
	}
	return storageNamespace, nil
}

// lakeFSCatalogURL derives the Iceberg catalog URL from the configured lakeFS endpoint. The
// endpoint may carry the API path (/api/v1), which is stripped before the catalog mount is added.
func lakeFSCatalogURL(endpoint string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil {
		return "", fmt.Errorf("%w: lakeFS endpoint: %w", importer.ErrInvalidOptions, err)
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("%w: lakeFS endpoint %q is not an absolute URL", importer.ErrInvalidOptions, endpoint)
	}
	path := strings.TrimRight(parsed.Path, "/")
	path = strings.TrimSuffix(path, icebergImportLakeFSEndpointAPIPathSuffix)
	parsed.Path = strings.TrimRight(path, "/")
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return importer.LakeFSCatalogURL(parsed.String()), nil
}

// addLakeFSCatalogCredentials authenticates a request to the lakeFS Iceberg catalog the way the
// rest of lakectl authenticates to lakeFS: HTTP basic with the configured access key pair, or the
// login token when no key pair is configured.
func addLakeFSCatalogCredentials(req *http.Request) error {
	accessKeyID := string(cfg.Credentials.AccessKeyID)
	secretAccessKey := string(cfg.Credentials.SecretAccessKey)
	if accessKeyID != "" || secretAccessKey != "" {
		req.SetBasicAuth(accessKeyID, secretAccessKey)
		return nil
	}
	if token := getTokenOnce(); token != nil && token.Token != "" {
		req.Header.Set("Authorization", "Bearer "+token.Token)
	}
	return nil
}

// lakeFSObjectUploader writes copied table files through the lakeFS object API. The key is relative
// to the branch and the object API stores it under the repository's storage namespace, which is
// exactly where the rewritten metadata locations point.
type lakeFSObjectUploader struct {
	client *apigen.ClientWithResponses
}

func (u lakeFSObjectUploader) Upload(ctx context.Context, repository, branch, key string, _ int64, body io.Reader) error {
	resp, err := u.client.UploadObjectWithBody(ctx, repository, branch,
		&apigen.UploadObjectParams{Path: key}, icebergImportObjectContentType, body)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusCreated {
		return helpers.ResponseAsError(resp)
	}
	return nil
}

// printIcebergImportSummary writes the per-table report and the totals.
func printIcebergImportSummary(summary *importer.Summary) {
	rows := make([][]any, len(summary.Results))
	for i, result := range summary.Results {
		detail := result.Detail
		if detail == "" {
			detail = result.MetadataLocation
		}
		rows[i] = []any{result.Source, result.Target, string(result.Status), detail}
	}
	PrintTable(rows, []any{"Source table", "Target table", "Status", "Detail"}, &apigen.Pagination{}, 0)
	Write(icebergImportSummaryTemplate, summary)
}
