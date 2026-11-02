package cmd

import (
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/datasets/apigen"
)

// pullsCmd groups the dataset pull-request commands (§12.1, §5.6).
func pullsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pulls",
		Short: "Manage dataset pull requests",
		Long: `Manage dataset pull requests: a definition proposed against the current version, reviewed on a
hidden preview branch, then merged as the next dataset version.`,
	}
	cmd.AddCommand(pullsListCmd(), pullsShowCmd(), pullsCreateCmd(), pullsDiffCmd(), pullsMergeCmd(), pullsCloseCmd())
	return cmd
}

// ---------------------------------------------------------------------------
// datasets pulls list
// ---------------------------------------------------------------------------

// pullListOptions is the parsed command line of `datasets pulls list`.
type pullListOptions struct {
	paginationOptions
	// Dataset is empty for the global listing.
	Dataset string
	Status  string
	Output  outputFormat
}

// parsePullListOptions parses and validates the arguments of `datasets pulls list`.
func parsePullListOptions(cmd *cobra.Command, args []string) (pullListOptions, error) {
	if len(args) > 1 {
		return pullListOptions{}, invalidArguments("pulls list accepts at most one dataset name, got %d arguments", len(args))
	}
	pagination, err := parsePaginationOptions(cmd)
	if err != nil {
		return pullListOptions{}, err
	}
	status, err := cmd.Flags().GetString(statusFlagName)
	if err != nil {
		return pullListOptions{}, err
	}
	switch status {
	case "", "open", "closed", "merged", "all":
	default:
		return pullListOptions{}, invalidArguments("invalid --status %q: use open, closed, merged or all", status)
	}
	output, err := outputOf(cmd)
	if err != nil {
		return pullListOptions{}, err
	}
	options := pullListOptions{paginationOptions: pagination, Status: status, Output: output}
	if len(args) == 1 {
		options.Dataset = args[0]
	}
	return options, nil
}

func pullsListCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list [dataset]",
		Short:   "List dataset pull requests",
		Long:    "List the pull requests of one dataset, or of every dataset when no dataset is named.",
		Example: "lakectl datasets pulls list customer-360 --status open",
		Args:    cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			options, err := parsePullListOptions(cmd, args)
			if err != nil {
				return err
			}
			return runPullsList(cmd, options)
		},
	}
	withPaginationFlags(cmd)
	cmd.Flags().String(statusFlagName, "", "only list pull requests in this state: open, closed, merged or all")
	return cmd
}

func runPullsList(cmd *cobra.Command, options pullListOptions) error {
	client, err := getClient()
	if err != nil {
		return err
	}
	var pagination apigen.Pagination
	var pulls []apigen.DatasetPullRequest
	if options.Dataset == "" {
		params := &apigen.ListPullRequestsParams{Amount: apiutil.Ptr(apigen.Amount(options.Amount))}
		if options.Status != "" {
			params.Status = apiutil.Ptr(options.Status)
		}
		if options.After != "" {
			params.After = apiutil.Ptr(apigen.After(options.After))
		}
		resp, err := client.ListPullRequestsWithResponse(cmd.Context(), params)
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
			return invalidResponse("bad response from server: missing pull request list")
		}
		pagination, pulls = resp.JSON200.Pagination, resp.JSON200.Results
	} else {
		params := &apigen.ListDatasetPullRequestsParams{Amount: apiutil.Ptr(apigen.Amount(options.Amount))}
		if options.Status != "" {
			params.Status = apiutil.Ptr(options.Status)
		}
		if options.After != "" {
			params.After = apiutil.Ptr(apigen.After(options.After))
		}
		resp, err := client.ListDatasetPullRequestsWithResponse(cmd.Context(), apigen.DatasetName(options.Dataset), params)
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
			return invalidResponse("bad response from server: missing pull request list")
		}
		pagination, pulls = resp.JSON200.Pagination, resp.JSON200.Results
	}
	rows := make([][]any, len(pulls))
	for i, pull := range pulls {
		rows[i] = []any{
			pull.Id,
			pull.Dataset,
			pull.Title,
			pull.Status,
			pull.Author,
			formatVersion(pull.BaseVersion),
			pull.Stale,
			formatSeconds(pull.CreationDate),
		}
	}
	printTable(cmd,
		[]any{"Pull Request", "Dataset", "Title", "Status", "Author", "Base Version", "Stale", "Creation Date"},
		rows, pagination, options.Amount)
	return nil
}

// ---------------------------------------------------------------------------
// datasets pulls show
// ---------------------------------------------------------------------------

// pullShowOptions is the parsed command line of `datasets pulls show`.
type pullShowOptions struct {
	Dataset string
	ID      string
	Output  outputFormat
}

func parsePullShowOptions(cmd *cobra.Command, args []string) (pullShowOptions, error) {
	if len(args) != datasetAndIDArgsCount {
		return pullShowOptions{}, invalidArguments("pulls show expects a dataset name and a pull request id, got %d arguments", len(args))
	}
	output, err := outputOf(cmd)
	if err != nil {
		return pullShowOptions{}, err
	}
	return pullShowOptions{Dataset: args[0], ID: args[1], Output: output}, nil
}

func pullsShowCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "show <dataset> <pull-request>",
		Short:   "Show a dataset pull request",
		Long:    "Show a pull request with its definition and the diffs against the latest version.",
		Example: "lakectl datasets pulls show customer-360 01J8Z3Q4X5",
		Args:    cobra.ExactArgs(datasetAndIDArgsCount),
		RunE: func(cmd *cobra.Command, args []string) error {
			options, err := parsePullShowOptions(cmd, args)
			if err != nil {
				return err
			}
			return runPullsShow(cmd, options)
		},
	}
}

func runPullsShow(cmd *cobra.Command, options pullShowOptions) error {
	client, err := getClient()
	if err != nil {
		return err
	}
	resp, err := client.GetPullRequestWithResponse(
		cmd.Context(),
		apigen.DatasetName(options.Dataset),
		apigen.PullRequestID(options.ID),
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
		return invalidResponse("bad response from server: missing pull request")
	}
	printPullRequest(cmd, *resp.JSON200)
	return nil
}

// ---------------------------------------------------------------------------
// datasets pulls create
// ---------------------------------------------------------------------------

// pullCreateOptions is the parsed command line of `datasets pulls create`.
type pullCreateOptions struct {
	Dataset     string
	File        string
	Title       string
	Description string
	Output      outputFormat
}

func parsePullCreateOptions(cmd *cobra.Command, args []string) (pullCreateOptions, error) {
	if len(args) != 1 {
		return pullCreateOptions{}, invalidArguments("pulls create expects exactly one dataset name, got %d arguments", len(args))
	}
	file, err := cmd.Flags().GetString(fileFlagName)
	if err != nil {
		return pullCreateOptions{}, err
	}
	if file == "" {
		return pullCreateOptions{}, invalidArguments("pulls create requires --%s with a definition file", fileFlagName)
	}
	title, err := cmd.Flags().GetString(titleFlagName)
	if err != nil {
		return pullCreateOptions{}, err
	}
	if title == "" {
		return pullCreateOptions{}, invalidArguments("pulls create requires a non-empty --title")
	}
	description, err := cmd.Flags().GetString(descriptionFlagName)
	if err != nil {
		return pullCreateOptions{}, err
	}
	output, err := outputOf(cmd)
	if err != nil {
		return pullCreateOptions{}, err
	}
	return pullCreateOptions{
		Dataset:     args[0],
		File:        file,
		Title:       title,
		Description: description,
		Output:      output,
	}, nil
}

func pullsCreateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create <dataset> --file definition.json --title title",
		Short: "Open a dataset pull request",
		Long: `Open a pull request against the current dataset version. The definition is materialised on
the hidden preview branch so reviewers can browse the resulting data, and the pull request records
the definition and data diff against the latest version. Opening a pull request for a dataset
that does not exist yet is allowed: the dataset is created when the pull request is merged.`,
		Example: "lakectl datasets pulls create customer-360 --file customer-360.json --title \"add churn column\"",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			options, err := parsePullCreateOptions(cmd, args)
			if err != nil {
				return err
			}
			return runPullsCreate(cmd, options)
		},
	}
	definitionReadFlags(cmd)
	cmd.Flags().StringP(titleFlagName, "t", "", "pull request title (required)")
	cmd.Flags().StringP(descriptionFlagName, "d", "", "pull request description")
	return cmd
}

func runPullsCreate(cmd *cobra.Command, options pullCreateOptions) error {
	loaded, err := readDefinitionFile(options.File)
	if err != nil {
		return err
	}
	if loaded.Name != "" && loaded.Name != options.Dataset {
		return invalidArguments("definition file is for dataset %q, but the command line names %q", loaded.Name, options.Dataset)
	}
	request := apigen.DatasetPullRequestCreate{
		Definition: loaded.Definition,
		Title:      options.Title,
	}
	if options.Description != "" {
		request.Description = apiutil.Ptr(options.Description)
	}
	client, err := getClient()
	if err != nil {
		return err
	}
	resp, err := client.CreatePullRequestWithResponse(cmd.Context(), apigen.DatasetName(options.Dataset), apigen.CreatePullRequestJSONRequestBody(request))
	if err != nil {
		return err
	}
	if err := checkResponse(resp.HTTPResponse, resp.Body, http.StatusCreated); err != nil {
		return err
	}
	if options.Output == outputJSON {
		return printJSONBody(cmd.OutOrStdout(), resp.Body)
	}
	if resp.JSON201 == nil {
		return invalidResponse("bad response from server: missing pull request")
	}
	printPullRequest(cmd, *resp.JSON201)
	return nil
}

// ---------------------------------------------------------------------------
// datasets pulls diff
// ---------------------------------------------------------------------------

func pullsDiffCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "diff <dataset> <pull-request>",
		Short:   "Diff a dataset pull request against latest",
		Long:    "Show the structural diff between the latest version and the definition the pull request proposes.",
		Example: "lakectl datasets pulls diff customer-360 01J8Z3Q4X5",
		Args:    cobra.ExactArgs(datasetAndIDArgsCount),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := getClient()
			if err != nil {
				return err
			}
			resp, err := client.GetPullRequestDiffWithResponse(cmd.Context(), apigen.DatasetName(args[0]), apigen.PullRequestID(args[1]))
			if err != nil {
				return err
			}
			if err := checkResponse(resp.HTTPResponse, resp.Body, http.StatusOK); err != nil {
				return err
			}
			if output, err := outputOf(cmd); err != nil {
				return err
			} else if output == outputJSON {
				return printJSONBody(cmd.OutOrStdout(), resp.Body)
			}
			if resp.JSON200 == nil {
				return invalidResponse("bad response from server: missing diff")
			}
			printDefinitionDiff(cmd, *resp.JSON200)
			return nil
		},
	}
}

// ---------------------------------------------------------------------------
// datasets pulls merge
// ---------------------------------------------------------------------------

// pullActionOptions is the parsed command line of `pulls merge` and `pulls close`.
type pullActionOptions struct {
	Dataset string
	ID      string
	Output  outputFormat
}

func parsePullActionOptions(cmd *cobra.Command, args []string, action string) (pullActionOptions, error) {
	if len(args) != datasetAndIDArgsCount {
		return pullActionOptions{}, invalidArguments("pulls %s expects a dataset name and a pull request id, got %d arguments", action, len(args))
	}
	output, err := outputOf(cmd)
	if err != nil {
		return pullActionOptions{}, err
	}
	return pullActionOptions{Dataset: args[0], ID: args[1], Output: output}, nil
}

func pullsMergeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "merge <dataset> <pull-request>",
		Short: "Merge a dataset pull request",
		Long: `Publish the pull request's definition as the next dataset version and close it.

Merging needs the pull request's base version to be the latest one; otherwise it fails with a
conflict and the pull request must be updated on the current version first.`,
		Example: "lakectl datasets pulls merge customer-360 01J8Z3Q4X5",
		Args:    cobra.ExactArgs(datasetAndIDArgsCount),
		RunE: func(cmd *cobra.Command, args []string) error {
			options, err := parsePullActionOptions(cmd, args, "merge")
			if err != nil {
				return err
			}
			client, err := getClient()
			if err != nil {
				return err
			}
			resp, err := client.MergePullRequestWithResponse(
				cmd.Context(),
				apigen.DatasetName(options.Dataset),
				apigen.PullRequestID(options.ID),
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
				return invalidResponse("bad response from server: missing merge result")
			}
			printField(cmd.OutOrStdout(), "Version", versionLabel(resp.JSON200.Version))
			printField(cmd.OutOrStdout(), "Reference", resp.JSON200.Reference)
			return nil
		},
	}
}

// ---------------------------------------------------------------------------
// datasets pulls close
// ---------------------------------------------------------------------------

func pullsCloseCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "close <dataset> <pull-request>",
		Short:   "Close a dataset pull request without merging it",
		Long:    "Close a pull request and delete its preview branch. The dataset is not changed.",
		Example: "lakectl datasets pulls close customer-360 01J8Z3Q4X5",
		Args:    cobra.ExactArgs(datasetAndIDArgsCount),
		RunE: func(cmd *cobra.Command, args []string) error {
			options, err := parsePullActionOptions(cmd, args, "close")
			if err != nil {
				return err
			}
			client, err := getClient()
			if err != nil {
				return err
			}
			resp, err := client.ClosePullRequestWithResponse(
				cmd.Context(),
				apigen.DatasetName(options.Dataset),
				apigen.PullRequestID(options.ID),
			)
			if err != nil {
				return err
			}
			if err := checkResponse(resp.HTTPResponse, resp.Body, http.StatusNoContent); err != nil {
				return err
			}
			if options.Output == outputJSON {
				printField(cmd.OutOrStdout(), "closed", options.ID)
				return nil
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Closed pull request %s of dataset %s.\n", options.ID, options.Dataset)
			return nil
		},
	}
}

// versionLabel renders a merge-result version as `vN`.
func versionLabel(version int) string {
	return formatVersion(version)
}
