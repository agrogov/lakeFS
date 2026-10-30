package cmd

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/spf13/cobra"
	lakectlcmd "github.com/treeverse/lakefs/cmd/lakectl/cmd"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/datasets/apigen"
)

// ---------------------------------------------------------------------------
// datasets list
// ---------------------------------------------------------------------------

// listOptions is the parsed command line of `datasets list` (DATASETS_DESIGN.md §12.1).
type listOptions struct {
	paginationOptions
	Prefix   string
	Metadata map[string]string
	Output   outputFormat
}

// parseListOptions parses and validates the arguments of `datasets list`.
func parseListOptions(cmd *cobra.Command) (listOptions, error) {
	pagination, err := parsePaginationOptions(cmd)
	if err != nil {
		return listOptions{}, err
	}
	prefix, err := cmd.Flags().GetString(prefixFlagName)
	if err != nil {
		return listOptions{}, err
	}
	metadata, err := parseMetadataFilters(cmd)
	if err != nil {
		return listOptions{}, err
	}
	output, err := outputOf(cmd)
	if err != nil {
		return listOptions{}, err
	}
	return listOptions{paginationOptions: pagination, Prefix: prefix, Metadata: metadata, Output: output}, nil
}

func listCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Short:   "List datasets",
		Long:    "List datasets, optionally filtered by name prefix and by the metadata of the latest version.",
		Example: "lakectl datasets list --prefix customer --metadata tier=gold",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			options, err := parseListOptions(cmd)
			if err != nil {
				return err
			}
			return runList(cmd, options)
		},
	}
	withPaginationFlags(cmd)
	cmd.Flags().String(prefixFlagName, "", "only list datasets whose name starts with this prefix")
	cmd.Flags().StringArray(metadataFlagName, nil,
		"only list datasets whose latest version metadata holds key=value (repeatable)")
	return cmd
}

func runList(cmd *cobra.Command, options listOptions) error {
	client, err := getClient()
	if err != nil {
		return err
	}
	params := &apigen.ListDatasetsParams{Amount: apiutil.Ptr(apigen.Amount(options.Amount))}
	if options.Prefix != "" {
		params.Prefix = apiutil.Ptr(apigen.Prefix(options.Prefix))
	}
	if options.After != "" {
		params.After = apiutil.Ptr(apigen.After(options.After))
	}
	var editors []apigen.RequestEditorFn
	if editor := metadataFilterEditor(options.Metadata); editor != nil {
		editors = append(editors, editor)
	}
	resp, err := client.ListDatasetsWithResponse(cmd.Context(), params, editors...)
	if err != nil {
		return err
	}
	if err := checkResponse(resp.HTTPResponse, resp.Body, http.StatusOK); err != nil {
		return err
	}
	if options.Output == outputJSON {
		return printJSONBody(cmd.OutOrStdout(), resp.Body)
	}
	if resp.JSON200 == nil {
		return invalidResponse("bad response from server: missing dataset list")
	}
	datasets := resp.JSON200.Results
	rows := make([][]any, len(datasets))
	for i, dataset := range datasets {
		stats := "-"
		if dataset.Stats != nil {
			stats = formatStats(dataset.Stats)
		}
		rows[i] = []any{
			dataset.Name,
			derefString(dataset.Description),
			formatVersion(dataset.LatestVersion),
			dataset.State,
			stats,
			formatMillis(dataset.CreatedAt),
		}
	}
	printTable(cmd,
		[]any{"Dataset", "Description", "Latest Version", "State", "Data", "Creation Date"},
		rows, resp.JSON200.Pagination, options.Amount)
	return nil
}

// ---------------------------------------------------------------------------
// datasets show
// ---------------------------------------------------------------------------

// showOptions is the parsed command line of `datasets show`.
type showOptions struct {
	Dataset string
	// Version is empty when the dataset record itself is shown, `latest`/`vN`/`N`/`proposal-<id>`
	// when one version is shown.
	Version string
	Output  outputFormat
}

// parseShowOptions parses and validates the arguments of `datasets show`.
func parseShowOptions(cmd *cobra.Command, args []string) (showOptions, error) {
	if len(args) != 1 {
		return showOptions{}, invalidArguments("show expects exactly one dataset name, got %d arguments", len(args))
	}
	version, err := cmd.Flags().GetString(versionFlagName)
	if err != nil {
		return showOptions{}, err
	}
	if version != "" {
		version, err = parseVersionRef(version)
		if err != nil {
			return showOptions{}, err
		}
	}
	output, err := outputOf(cmd)
	if err != nil {
		return showOptions{}, err
	}
	return showOptions{Dataset: args[0], Version: version, Output: output}, nil
}

func showCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "show <dataset>",
		Short:   "Show a dataset and its latest definition",
		Long:    "Show a dataset record with the definition of its latest version, or one version with --version.",
		Example: "lakectl datasets show customer-360 --version v3 -o json",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			options, err := parseShowOptions(cmd, args)
			if err != nil {
				return err
			}
			return runShow(cmd, options)
		},
	}
	cmd.Flags().String(versionFlagName, "", "show this version instead of the dataset (latest, vN or N)")
	return cmd
}

func runShow(cmd *cobra.Command, options showOptions) error {
	client, err := getClient()
	if err != nil {
		return err
	}
	if options.Version != "" {
		resp, err := client.GetVersionWithResponse(cmd.Context(), apigen.DatasetName(options.Dataset), apigen.VersionRef(options.Version))
		if err != nil {
			return err
		}
		if err := checkResponse(resp.HTTPResponse, resp.Body, http.StatusOK); err != nil {
			return err
		}
		if options.Output == outputJSON {
			return printJSONBody(cmd.OutOrStdout(), resp.Body)
		}
		if resp.JSON200 == nil {
			return invalidResponse("bad response from server: missing version")
		}
		printVersion(cmd, *resp.JSON200)
		return nil
	}
	resp, err := client.GetDatasetWithResponse(cmd.Context(), apigen.DatasetName(options.Dataset))
	if err != nil {
		return err
	}
	if err := checkResponse(resp.HTTPResponse, resp.Body, http.StatusOK); err != nil {
		return err
	}
	if options.Output == outputJSON {
		return printJSONBody(cmd.OutOrStdout(), resp.Body)
	}
	if resp.JSON200 == nil {
		return invalidResponse("bad response from server: missing dataset")
	}
	printDataset(cmd, *resp.JSON200)
	return nil
}

// ---------------------------------------------------------------------------
// datasets create
// ---------------------------------------------------------------------------

// createOptions is the parsed command line of `datasets create`.
type createOptions struct {
	Dataset string
	File    string
	Publish bool
	Message string
	Output  outputFormat
}

// parseCreateOptions parses and validates the arguments of `datasets create`.
func parseCreateOptions(cmd *cobra.Command, args []string) (createOptions, error) {
	if len(args) != 1 {
		return createOptions{}, invalidArguments("create expects exactly one dataset name, got %d arguments", len(args))
	}
	file, err := cmd.Flags().GetString(fileFlagName)
	if err != nil {
		return createOptions{}, err
	}
	if file == "" {
		return createOptions{}, invalidArguments("create requires --%s with a definition file", fileFlagName)
	}
	publish, err := cmd.Flags().GetBool(publishFlagName)
	if err != nil {
		return createOptions{}, err
	}
	message, err := cmd.Flags().GetString(messageFlagName)
	if err != nil {
		return createOptions{}, err
	}
	switch {
	case publish && message == "":
		return createOptions{}, invalidArguments("--publish requires --%s", messageFlagName)
	case !publish && message != "":
		return createOptions{}, invalidArguments("--%s is only used with --publish", messageFlagName)
	}
	output, err := outputOf(cmd)
	if err != nil {
		return createOptions{}, err
	}
	return createOptions{Dataset: args[0], File: file, Publish: publish, Message: message, Output: output}, nil
}

func createCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create <dataset> --file definition.json [--publish --message msg]",
		Short: "Create a dataset",
		Long: `Create a dataset and its read-only repository.

Without --publish the dataset is created empty: it has no version, and the latest ref holds only
the initial commit. With --publish the definition is published as version v1.

--file accepts either a bare dataset definition ({"format_version": 1, "data": [...]}) or the
definition envelope ({"name": ..., "definition": {...}}); a name inside the envelope must match the
dataset name on the command line.`,
		Example: "lakectl datasets create customer-360 --file customer-360.json --publish --message \"initial dataset\"",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			options, err := parseCreateOptions(cmd, args)
			if err != nil {
				return err
			}
			return runCreate(cmd, options)
		},
	}
	definitionReadFlags(cmd)
	cmd.Flags().Bool(publishFlagName, false, "publish the definition as version v1")
	cmd.Flags().StringP(messageFlagName, "m", "", "publish message (required with --publish)")
	return cmd
}

func runCreate(cmd *cobra.Command, options createOptions) error {
	loaded, err := readDefinitionFile(options.File)
	if err != nil {
		return err
	}
	if loaded.Name != "" && loaded.Name != options.Dataset {
		return invalidArguments("definition file is for dataset %q, but the command line names %q", loaded.Name, options.Dataset)
	}
	request := apigen.DatasetCreateRequest{Name: options.Dataset, Definition: loaded.Definition}
	if options.Publish {
		request.Publish = &apigen.PublishOptions{Message: apiutil.Ptr(options.Message)}
	}
	client, err := getClient()
	if err != nil {
		return err
	}
	resp, err := client.CreateDatasetWithResponse(cmd.Context(), apigen.CreateDatasetJSONRequestBody(request))
	if err != nil {
		return err
	}
	if err := checkResponse(resp.HTTPResponse, resp.Body, http.StatusCreated, http.StatusAccepted); err != nil {
		return err
	}
	if options.Output == outputJSON {
		return printJSONBody(cmd.OutOrStdout(), resp.Body)
	}
	switch {
	case resp.JSON201 != nil:
		printDataset(cmd, *resp.JSON201)
	case resp.JSON202 != nil:
		printTask(cmd, options.Dataset, *resp.JSON202)
	default:
		return invalidResponse("bad response from server: missing dataset")
	}
	return nil
}

// ---------------------------------------------------------------------------
// datasets publish
// ---------------------------------------------------------------------------

// publishOptions is the parsed command line of `datasets publish`.
type publishOptions struct {
	Dataset         string
	File            string
	Message         string
	ExpectedVersion *int
	Wait            bool
	Output          outputFormat
}

// parsePublishOptions parses and validates the arguments of `datasets publish`.
func parsePublishOptions(cmd *cobra.Command, args []string) (publishOptions, error) {
	if len(args) != 1 {
		return publishOptions{}, invalidArguments("publish expects exactly one dataset name, got %d arguments", len(args))
	}
	file, err := cmd.Flags().GetString(fileFlagName)
	if err != nil {
		return publishOptions{}, err
	}
	if file == "" {
		return publishOptions{}, invalidArguments("publish requires --%s with a definition file", fileFlagName)
	}
	message, err := cmd.Flags().GetString(messageFlagName)
	if err != nil {
		return publishOptions{}, err
	}
	if message == "" {
		return publishOptions{}, invalidArguments("publish requires a non-empty --%s", messageFlagName)
	}
	expectedValue, err := cmd.Flags().GetString(expectedVersionFlagName)
	if err != nil {
		return publishOptions{}, err
	}
	expectedVersion, err := parseExpectedVersion(expectedValue)
	if err != nil {
		return publishOptions{}, err
	}
	wait, err := cmd.Flags().GetBool(waitFlagName)
	if err != nil {
		return publishOptions{}, err
	}
	output, err := outputOf(cmd)
	if err != nil {
		return publishOptions{}, err
	}
	return publishOptions{
		Dataset:         args[0],
		File:            file,
		Message:         message,
		ExpectedVersion: expectedVersion,
		Wait:            wait,
		Output:          output,
	}, nil
}

func publishCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "publish <dataset> --file definition.json --message msg",
		Short: "Publish a new dataset version",
		Long: `Publish a definition as the next dataset version (vN+1) and tag it in the dataset repository.

--expected-version behaves like an ETag: the publish fails with a conflict when the dataset's
latest version is not the expected one. Large publishes are accepted asynchronously; --wait polls
the task until it completes.`,
		Example: "lakectl datasets publish customer-360 --file customer-360.json --message \"add churn column\" --expected-version 2",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			options, err := parsePublishOptions(cmd, args)
			if err != nil {
				return err
			}
			return runPublish(cmd, options)
		},
	}
	definitionReadFlags(cmd)
	cmd.Flags().StringP(messageFlagName, "m", "", "publish message (required)")
	cmd.Flags().String(expectedVersionFlagName, "", "fail unless the latest version is this version (optimistic concurrency)")
	cmd.Flags().Bool(waitFlagName, false, "wait for an asynchronous publish to finish")
	return cmd
}

func runPublish(cmd *cobra.Command, options publishOptions) error {
	loaded, err := readDefinitionFile(options.File)
	if err != nil {
		return err
	}
	if loaded.Name != "" && loaded.Name != options.Dataset {
		return invalidArguments("definition file is for dataset %q, but the command line names %q", loaded.Name, options.Dataset)
	}
	request := apigen.VersionPublishRequest{
		Definition:      loaded.Definition,
		Message:         options.Message,
		ExpectedVersion: options.ExpectedVersion,
	}
	client, err := getClient()
	if err != nil {
		return err
	}
	resp, err := client.PublishVersionWithResponse(cmd.Context(), apigen.DatasetName(options.Dataset), apigen.PublishVersionJSONRequestBody(request))
	if err != nil {
		return err
	}
	if err := checkResponse(resp.HTTPResponse, resp.Body, http.StatusCreated, http.StatusAccepted); err != nil {
		return err
	}
	if resp.JSON201 != nil {
		if options.Output == outputJSON {
			return printJSONBody(cmd.OutOrStdout(), resp.Body)
		}
		printVersion(cmd, *resp.JSON201)
		return nil
	}
	if resp.JSON202 == nil {
		return invalidResponse("bad response from server: missing version")
	}
	if !options.Wait {
		if options.Output == outputJSON {
			return printJSONBody(cmd.OutOrStdout(), resp.Body)
		}
		printTask(cmd, options.Dataset, *resp.JSON202)
		return nil
	}
	task, taskBody, err := waitForTask(cmd, client, options.Dataset, resp.JSON202.Id)
	if err != nil {
		return err
	}
	if options.Output == outputJSON {
		return printJSONBody(cmd.OutOrStdout(), taskBody)
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Publish task %s completed: %s\n", task.Id, formatVersion(derefInt(task.Version)))
	return nil
}

// waitForTask polls an asynchronous publish task (§6.5) until it reaches a terminal state. It
// returns the completed task and the body of the response that completed it.
func waitForTask(cmd *cobra.Command, client *apigen.ClientWithResponses, dataset, taskID string) (*apigen.Task, []byte, error) {
	for {
		resp, err := client.GetTaskWithResponse(cmd.Context(), apigen.DatasetName(dataset), apigen.TaskID(taskID))
		if err != nil {
			return nil, nil, err
		}
		if err := checkResponse(resp.HTTPResponse, resp.Body, http.StatusOK); err != nil {
			return nil, nil, err
		}
		if resp.JSON200 == nil {
			return nil, nil, invalidResponse("bad response from server: missing task %s", taskID)
		}
		task := resp.JSON200
		switch task.Status {
		case taskStatusCompleted:
			return task, resp.Body, nil
		case taskStatusFailed:
			return nil, nil, taskFailed("publish task %s failed: %s", taskID, derefString(task.Error))
		}
		select {
		case <-cmd.Context().Done():
			return nil, nil, cmd.Context().Err()
		case <-time.After(taskPollInterval):
		}
	}
}

// ---------------------------------------------------------------------------
// datasets validate
// ---------------------------------------------------------------------------

// validateOptions is the parsed command line of `datasets validate`.
type validateOptions struct {
	Dataset string
	File    string
	Output  outputFormat
}

// parseValidateOptions parses and validates the arguments of `datasets validate`.
func parseValidateOptions(cmd *cobra.Command, args []string) (validateOptions, error) {
	if len(args) != 1 {
		return validateOptions{}, invalidArguments("validate expects exactly one dataset name, got %d arguments", len(args))
	}
	file, err := cmd.Flags().GetString(fileFlagName)
	if err != nil {
		return validateOptions{}, err
	}
	if file == "" {
		return validateOptions{}, invalidArguments("validate requires --%s with a definition file", fileFlagName)
	}
	output, err := outputOf(cmd)
	if err != nil {
		return validateOptions{}, err
	}
	return validateOptions{Dataset: args[0], File: file, Output: output}, nil
}

func validateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "validate <dataset> --file definition.json",
		Short: "Validate a dataset definition without writing it",
		Long: `Dry run: normalise the definition, resolve branch and tag refs to commits, validate it
against the metadata keys and check that you can read every source repository. Nothing is written.`,
		Example: "lakectl datasets validate customer-360 --file customer-360.json",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			options, err := parseValidateOptions(cmd, args)
			if err != nil {
				return err
			}
			return runValidate(cmd, options)
		},
	}
	definitionReadFlags(cmd)
	return cmd
}

func runValidate(cmd *cobra.Command, options validateOptions) error {
	loaded, err := readDefinitionFile(options.File)
	if err != nil {
		return err
	}
	if loaded.Name != "" && loaded.Name != options.Dataset {
		return invalidArguments("definition file is for dataset %q, but the command line names %q", loaded.Name, options.Dataset)
	}
	client, err := getClient()
	if err != nil {
		return err
	}
	resp, err := client.ValidateDatasetWithResponse(
		cmd.Context(),
		apigen.DatasetName(options.Dataset),
		apigen.ValidateDatasetJSONRequestBody(apigen.DatasetValidateRequest{Definition: loaded.Definition}),
	)
	if err != nil {
		return err
	}
	if err := checkResponse(resp.HTTPResponse, resp.Body, http.StatusOK); err != nil {
		return err
	}
	if options.Output == outputJSON {
		return printJSONBody(cmd.OutOrStdout(), resp.Body)
	}
	if resp.JSON200 == nil {
		return invalidResponse("bad response from server: missing definition")
	}
	printField(cmd.OutOrStdout(), "Dataset", options.Dataset)
	printField(cmd.OutOrStdout(), "Valid", true)
	printField(cmd.OutOrStdout(), "Data Estimate", formatStats(resp.JSON200.Stats))
	fmt.Fprintln(cmd.OutOrStdout())
	printDefinition(cmd, resp.JSON200.Definition)
	return nil
}

// ---------------------------------------------------------------------------
// datasets versions
// ---------------------------------------------------------------------------

// versionsOptions is the parsed command line of `datasets versions`.
type versionsOptions struct {
	paginationOptions
	Dataset string
	Output  outputFormat
}

// parseVersionsOptions parses and validates the arguments of `datasets versions`.
func parseVersionsOptions(cmd *cobra.Command, args []string) (versionsOptions, error) {
	if len(args) != 1 {
		return versionsOptions{}, invalidArguments("versions expects exactly one dataset name, got %d arguments", len(args))
	}
	pagination, err := parsePaginationOptions(cmd)
	if err != nil {
		return versionsOptions{}, err
	}
	output, err := outputOf(cmd)
	if err != nil {
		return versionsOptions{}, err
	}
	return versionsOptions{paginationOptions: pagination, Dataset: args[0], Output: output}, nil
}

func versionsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "versions <dataset>",
		Short:   "List dataset versions",
		Long:    "List the published versions of a dataset, newest first.",
		Example: "lakectl datasets versions customer-360",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			options, err := parseVersionsOptions(cmd, args)
			if err != nil {
				return err
			}
			return runVersions(cmd, options)
		},
	}
	withPaginationFlags(cmd)
	return cmd
}

func runVersions(cmd *cobra.Command, options versionsOptions) error {
	client, err := getClient()
	if err != nil {
		return err
	}
	params := &apigen.ListVersionsParams{Amount: apiutil.Ptr(apigen.Amount(options.Amount))}
	if options.After != "" {
		params.After = apiutil.Ptr(apigen.After(options.After))
	}
	resp, err := client.ListVersionsWithResponse(cmd.Context(), apigen.DatasetName(options.Dataset), params)
	if err != nil {
		return err
	}
	if err := checkResponse(resp.HTTPResponse, resp.Body, http.StatusOK); err != nil {
		return err
	}
	if options.Output == outputJSON {
		return printJSONBody(cmd.OutOrStdout(), resp.Body)
	}
	if resp.JSON200 == nil {
		return invalidResponse("bad response from server: missing version list")
	}
	versions := resp.JSON200.Results
	rows := make([][]any, len(versions))
	for i, version := range versions {
		rows[i] = []any{
			formatVersion(version.Version),
			derefString(version.Message),
			version.Publisher,
			formatMillis(version.PublishedAt),
			version.CommitId,
			formatStats(version.Stats),
		}
	}
	printTable(cmd,
		[]any{"Version", "Message", "Publisher", "Published At", "Commit ID", "Data"},
		rows, resp.JSON200.Pagination, options.Amount)
	return nil
}

// ---------------------------------------------------------------------------
// datasets diff
// ---------------------------------------------------------------------------

// diffOptions is the parsed command line of `datasets diff`.
type diffOptions struct {
	paginationOptions
	Dataset string
	Left    string
	Right   string
	// Data selects the data diff (`type=data`) instead of the structural definition diff.
	Data   bool
	Prefix string
	Output outputFormat
}

// parseDiffOptions parses and validates the arguments of `datasets diff`.
func parseDiffOptions(cmd *cobra.Command, args []string) (diffOptions, error) {
	if len(args) != datasetAndRefsArgsCount {
		return diffOptions{}, invalidArguments("diff expects a dataset name and two refs, got %d arguments", len(args))
	}
	pagination, err := parsePaginationOptions(cmd)
	if err != nil {
		return diffOptions{}, err
	}
	left, err := parseVersionRef(args[1])
	if err != nil {
		return diffOptions{}, err
	}
	right, err := parseVersionRef(args[2])
	if err != nil {
		return diffOptions{}, err
	}
	data, err := cmd.Flags().GetBool(dataFlagName)
	if err != nil {
		return diffOptions{}, err
	}
	prefix, err := cmd.Flags().GetString(prefixFlagName)
	if err != nil {
		return diffOptions{}, err
	}
	output, err := outputOf(cmd)
	if err != nil {
		return diffOptions{}, err
	}
	return diffOptions{
		paginationOptions: pagination,
		Dataset:           args[0],
		Left:              left,
		Right:             right,
		Data:              data,
		Prefix:            prefix,
		Output:            output,
	}, nil
}

func diffCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "diff <dataset> <left> <right>",
		Short:   "Diff two dataset refs",
		Long:    "Compare two refs of a dataset (latest, vN or proposal-<id>), by definition or, with --data, by data.",
		Example: "lakectl datasets diff customer-360 v2 v3 --data",
		Args:    cobra.ExactArgs(datasetAndRefsArgsCount),
		RunE: func(cmd *cobra.Command, args []string) error {
			options, err := parseDiffOptions(cmd, args)
			if err != nil {
				return err
			}
			return runDiff(cmd, options)
		},
	}
	withPaginationFlags(cmd)
	cmd.Flags().Bool(dataFlagName, false, "diff the data of the two refs instead of their definitions")
	cmd.Flags().String(prefixFlagName, "", "only list data changes under this path prefix")
	return cmd
}

func runDiff(cmd *cobra.Command, options diffOptions) error {
	diffType := "definition"
	if options.Data {
		diffType = "data"
	}
	params := &apigen.DiffRefsParams{
		Type:   apiutil.Ptr(diffType),
		Amount: apiutil.Ptr(apigen.Amount(options.Amount)),
	}
	if options.Prefix != "" {
		params.Prefix = apiutil.Ptr(apigen.Prefix(options.Prefix))
	}
	if options.After != "" {
		params.After = apiutil.Ptr(apigen.After(options.After))
	}
	client, err := getClient()
	if err != nil {
		return err
	}
	resp, err := client.DiffRefsWithResponse(
		cmd.Context(),
		apigen.DatasetName(options.Dataset),
		apigen.LeftRef(options.Left),
		apigen.RightRef(options.Right),
		params,
	)
	if err != nil {
		return err
	}
	if err := checkResponse(resp.HTTPResponse, resp.Body, http.StatusOK); err != nil {
		return err
	}
	if options.Output == outputJSON {
		return printJSONBody(cmd.OutOrStdout(), resp.Body)
	}
	// The response is a oneOf (§10.3), which the generated client types as interface{}. The
	// request already decided which shape the server answers with, so decode the body itself.
	if options.Data {
		var diff apigen.DataDiffList
		if err := json.Unmarshal(resp.Body, &diff); err != nil {
			return fmt.Errorf("invalid data diff response: %w", err)
		}
		printDataDiff(cmd, diff, options.Amount)
		return nil
	}
	var diff apigen.DefinitionDiff
	if err := json.Unmarshal(resp.Body, &diff); err != nil {
		return fmt.Errorf("invalid definition diff response: %w", err)
	}
	printDefinitionDiff(cmd, diff)
	return nil
}

// ---------------------------------------------------------------------------
// datasets delete
// ---------------------------------------------------------------------------

// deleteOptions is the parsed command line of `datasets delete`.
type deleteOptions struct {
	Dataset string
	Output  outputFormat
}

// parseDeleteOptions parses and validates the arguments of `datasets delete`.
func parseDeleteOptions(cmd *cobra.Command, args []string) (deleteOptions, error) {
	if len(args) != 1 {
		return deleteOptions{}, invalidArguments("delete expects exactly one dataset name, got %d arguments", len(args))
	}
	output, err := outputOf(cmd)
	if err != nil {
		return deleteOptions{}, err
	}
	return deleteOptions{Dataset: args[0], Output: output}, nil
}

func deleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete <dataset>",
		Short: "Delete a dataset",
		Long: `Delete a dataset: the read-only dataset repository, the garbage-collection pins of its
versions and its control-plane records. The data of the source repositories is not touched.`,
		Example: "lakectl datasets delete customer-360 --yes",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			options, err := parseDeleteOptions(cmd, args)
			if err != nil {
				return err
			}
			return runDelete(cmd, options)
		},
	}
	lakectlcmd.AssignAutoConfirmFlag(cmd.Flags())
	return cmd
}

func runDelete(cmd *cobra.Command, options deleteOptions) error {
	confirmed, err := lakectlcmd.Confirm(cmd.Flags(), fmt.Sprintf("Delete dataset %q?", options.Dataset),
		"This deletes the dataset repository and releases the garbage-collection pins of its versions.",
		"Object data in the source repositories is not touched.")
	if err != nil {
		return err
	}
	if !confirmed {
		fmt.Fprintln(cmd.OutOrStdout(), "Delete cancelled.")
		return nil
	}
	client, err := getClient()
	if err != nil {
		return err
	}
	resp, err := client.DeleteDatasetWithResponse(cmd.Context(), apigen.DatasetName(options.Dataset))
	if err != nil {
		return err
	}
	if err := checkResponse(resp.HTTPResponse, resp.Body, http.StatusNoContent); err != nil {
		return err
	}
	if options.Output != outputJSON {
		fmt.Fprintf(cmd.OutOrStdout(), "Dataset %q deleted.\n", options.Dataset)
	}
	return nil
}
