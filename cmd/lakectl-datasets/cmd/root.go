// Package cmd implements the `lakectl datasets` command tree, shipped as the lakectl plugin
// binary lakectl-datasets (DATASETS_DESIGN.md §12.1).
//
// The commands are the client side of the datasets control plane (api/datasets.yml, §10). They
// reuse the lakeFS REST client conventions of cmd/lakectl/cmd: the configuration file and
// credentials, the retry policy, the table/JSON output formatting and the error formatting.
// Reading dataset data needs no command of its own: a dataset is a read-only repository, so
// `lakectl fs ls lakefs://<dataset>/v3/...` and the S3 gateway already serve it (§7.2).
package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/spf13/cobra"
	lakectlcmd "github.com/treeverse/lakefs/cmd/lakectl/cmd"
	"github.com/treeverse/lakefs/pkg/datasets/apigen"
)

const (
	// CommandName is the plugin command name: `lakectl datasets ...` runs `lakectl-datasets ...`
	// with the same arguments (see cmd/lakectl/cmd/plugin.go).
	CommandName = "datasets"

	// controlPlanePath is the base path of the datasets control plane (DATASETS_DESIGN.md §10).
	controlPlanePath = "/api/v1/datasets"

	// defaultAmount matches the default page size of the lakectl list commands.
	defaultAmount = 100

	// Argument counts of the commands that take more than one argument, shared by the cobra Args
	// validator and the option parsers.
	datasetAndRefsArgsCount = 3 // datasets diff <dataset> <left> <right>
	datasetAndIDArgsCount   = 2 // datasets proposals show|merge|close <dataset> <proposal>

	outputFlagName = "output"

	// Flag names of the per-command options.
	publishFlagName         = "publish"
	expectedVersionFlagName = "expected-version"
	waitFlagName            = "wait"
	dataFlagName            = "data"
	statusFlagName          = "status"
	titleFlagName           = "title"
	descriptionFlagName     = "description"
	typeFlagName            = "type"
	requiredFlagName        = "required"
	allowMultipleFlagName   = "allow-multiple"
	allowedValuesFlagName   = "allowed-values"

	metadataFlagName   = "metadata"
	prefixFlagName     = "prefix"
	afterFlagName      = "after"
	amountFlagName     = "amount"
	messageFlagName    = "message"
	fileFlagName       = "file"
	versionFlagName    = "version"
	principalUserFlag  = "user"
	principalGroupFlag = "group"
)

// outputFormat is the value of the persistent --output flag.
type outputFormat string

const (
	// outputTable is the default: the same table rendering lakectl uses for its own commands.
	outputTable outputFormat = "table"
	// outputJSON prints the control-plane response body, indented.
	outputJSON outputFormat = "json"
)

// taskStatusCompleted and taskStatusFailed are the terminal states of an asynchronous task
// (§10.2, §6.5).
const (
	taskStatusCompleted = "completed"
	taskStatusFailed    = "failed"
)

// taskPollInterval is how often `datasets publish --wait` polls an asynchronous publish task
// (§6.5). It is a variable so tests do not have to wait.
var taskPollInterval = 2 * time.Second

// newAPIClient builds the datasets control-plane client from the transport lakectl configured
// (endpoint, credentials provider, retry policy, User-Agent), so `lakectl-datasets` talks to the
// server exactly like `lakectl` does. It is a variable so tests can point the commands at an
// httptest server.
var newAPIClient = func() (*apigen.ClientWithResponses, error) {
	server, doer, editors := lakectlcmd.APIClientTransport()
	opts := make([]apigen.ClientOption, 0, len(editors)+1)
	opts = append(opts, apigen.WithHTTPClient(doer))
	for _, editor := range editors {
		opts = append(opts, apigen.WithRequestEditorFn(apigen.RequestEditorFn(editor)))
	}
	return apigen.NewClientWithResponses(strings.TrimSuffix(server, "/")+controlPlanePath, opts...)
}

// NewRoot returns the `datasets` command. It carries no PersistentPreRun of its own: when it runs
// under lakectl's root command that would replace lakectl's, which is what loads the
// configuration the client is built from.
func NewRoot() *cobra.Command {
	root := &cobra.Command{
		Use:   CommandName,
		Short: "Manage lakeFS datasets",
		Long: `Manage lakeFS datasets: named, versioned, read-only collections of data curated from one
or more repositories.

A dataset is materialised as a read-only lakeFS repository with the same name, so its data is
read with the regular lakeFS paths:

  lakectl fs ls  lakefs://<dataset>/v3/customers/
  lakectl fs cat lakefs://<dataset>/latest/reference/countries.csv

The commands below cover the control plane only: definitions, versions, diffs, proposals,
dataset access and metadata keys.`,
		SilenceUsage: true,
	}
	root.PersistentFlags().StringP(outputFlagName, "o", string(outputTable), "output format: table or json")

	root.AddCommand(
		// Datasets and versions.
		listCmd(),
		showCmd(),
		createCmd(),
		publishCmd(),
		validateCmd(),
		versionsCmd(),
		diffCmd(),
		deleteCmd(),
		// Dataset pull requests.
		proposalsCmd(),
		// Dataset-level access grants.
		accessCmd(),
		// Global metadata key definitions.
		metadataKeysCmd(),
	)
	return root
}

// outputOf returns the parsed value of the persistent --output flag.
func outputOf(cmd *cobra.Command) (outputFormat, error) {
	value, err := cmd.Flags().GetString(outputFlagName)
	if err != nil {
		return "", err
	}
	switch format := outputFormat(value); format {
	case outputTable, outputJSON:
		return format, nil
	default:
		return "", invalidArguments("unsupported output format %q: use %q or %q", value, outputTable, outputJSON)
	}
}

// paginationOptions is the parsed `--after`/`--amount` pair shared by the list commands.
type paginationOptions struct {
	After  string
	Amount int
}

func withPaginationFlags(cmd *cobra.Command) {
	cmd.Flags().Int(amountFlagName, defaultAmount, "how many results to return")
	cmd.Flags().String(afterFlagName, "", "show results after this value (used for pagination)")
}

// parsePaginationOptions parses and validates the pagination flags.
func parsePaginationOptions(cmd *cobra.Command) (paginationOptions, error) {
	amount, err := cmd.Flags().GetInt(amountFlagName)
	if err != nil {
		return paginationOptions{}, err
	}
	if amount < 1 {
		return paginationOptions{}, invalidArguments("--%s must be at least 1, got %d", amountFlagName, amount)
	}
	after, err := cmd.Flags().GetString(afterFlagName)
	if err != nil {
		return paginationOptions{}, err
	}
	return paginationOptions{After: after, Amount: amount}, nil
}

// parseMetadataFilters parses the repeatable `--metadata key=value` flag into the
// `metadata.<key>=<value>` filters of §10.1.
func parseMetadataFilters(cmd *cobra.Command) (map[string]string, error) {
	pairs, err := cmd.Flags().GetStringArray(metadataFlagName)
	if err != nil {
		return nil, err
	}
	filters := make(map[string]string, len(pairs))
	for _, pair := range pairs {
		key, value, found := strings.Cut(pair, "=")
		key = strings.TrimSpace(key)
		if !found || key == "" {
			return nil, invalidArguments("--%s expects key=value, got %q", metadataFlagName, pair)
		}
		filters[key] = value
	}
	return filters, nil
}

// metadataFilterEditor adds the `metadata.<key>=<value>` query parameters of §10.1 to a request.
// OpenAPI 3.0 cannot declare dynamic query parameter names, so the generated client leaves them
// out and the command adds them itself.
func metadataFilterEditor(filters map[string]string) apigen.RequestEditorFn {
	if len(filters) == 0 {
		return nil
	}
	return func(_ context.Context, req *http.Request) error {
		query := req.URL.Query()
		for key, value := range filters {
			query.Add(metadataFilterPrefix+key, value)
		}
		req.URL.RawQuery = query.Encode()
		return nil
	}
}

// metadataFilterPrefix is the query parameter prefix of the metadata filters (§10.1,
// pkg/datasets.MetadataFilterPrefix).
const metadataFilterPrefix = "metadata."

// Error categories of the command tree. They let callers (and tests) tell a bad command line from
// a bad server answer, while the messages themselves stay specific; err113 in this repository's
// lint configuration rejects inline dynamic errors, so the messages are carried by a type.
var (
	// ErrInvalidArguments marks command-line problems: a missing flag, a malformed value or an
	// unusable combination of flags.
	ErrInvalidArguments = errors.New("invalid arguments")
	// ErrInvalidResponse marks a control-plane answer that is missing the payload the schema
	// promises, or a validation report without the expected shape.
	ErrInvalidResponse = errors.New("invalid response")
	// ErrTaskFailed marks an asynchronous publish task that ended in failure (§6.5).
	ErrTaskFailed = errors.New("dataset task failed")
)

// commandError is a user-facing command error with a category.
type commandError struct {
	category error
	message  string
}

// Error implements error.
func (e *commandError) Error() string { return e.message }

// Unwrap exposes the category to errors.Is.
func (e *commandError) Unwrap() error { return e.category }

// invalidArguments builds a command-line error.
func invalidArguments(format string, args ...any) error {
	return &commandError{category: ErrInvalidArguments, message: fmt.Sprintf(format, args...)}
}

// invalidResponse builds the error for an answer without the promised payload.
func invalidResponse(format string, args ...any) error {
	return &commandError{category: ErrInvalidResponse, message: fmt.Sprintf(format, args...)}
}

// taskFailed builds the error for a failed publish task.
func taskFailed(format string, args ...any) error {
	return &commandError{category: ErrTaskFailed, message: fmt.Sprintf(format, args...)}
}

// errorDetail is one validation problem of §10.4.
type errorDetail struct {
	Path    string `json:"path"`
	Message string `json:"message"`
}

// apiError is a failed control-plane request: the core lakeFS error shape plus one entry per
// validation problem (§10.4).
type apiError struct {
	Status  int
	Message string
	Details []errorDetail
}

func (e *apiError) Error() string {
	if len(e.Details) == 0 {
		return e.Message
	}
	var b strings.Builder
	b.WriteString(e.Message)
	for _, detail := range e.Details {
		b.WriteString("\n  ")
		if detail.Path != "" {
			b.WriteString(detail.Path)
			b.WriteString(": ")
		}
		b.WriteString(detail.Message)
	}
	return b.String()
}

// checkResponse returns nil when the response has one of the expected status codes and an
// *apiError carrying the server message otherwise.
func checkResponse(httpResponse *http.Response, body []byte, expected ...int) error {
	if httpResponse == nil {
		return invalidResponse("no response from the datasets control plane")
	}
	for _, statusCode := range expected {
		if httpResponse.StatusCode == statusCode {
			return nil
		}
	}
	failure := &apiError{Status: httpResponse.StatusCode}
	var payload struct {
		Message string        `json:"message"`
		Errors  []errorDetail `json:"errors"`
	}
	if err := json.Unmarshal(body, &payload); err == nil && payload.Message != "" {
		failure.Message = payload.Message
		failure.Details = payload.Errors
		return failure
	}
	failure.Message = fmt.Sprintf("unexpected status %s", httpResponse.Status)
	return failure
}

// getClient builds the client or returns the construction error.
func getClient() (*apigen.ClientWithResponses, error) {
	client, err := newAPIClient()
	if err != nil {
		return nil, fmt.Errorf("could not initialize the datasets API client: %w", err)
	}
	return client, nil
}
