package cmd

import (
	"net/http"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/datasets/apigen"
)

// proposalsCmd groups the dataset pull-request commands (§12.1, §5.6).
func proposalsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "proposals",
		Short: "Manage dataset proposals (pull requests)",
		Long: `Manage dataset proposals: a definition proposed against the current version, reviewed on a
hidden preview branch, then merged as the next dataset version.`,
	}
	cmd.AddCommand(proposalsListCmd(), proposalsShowCmd(), proposalsCreateCmd(), proposalsMergeCmd(), proposalsCloseCmd())
	return cmd
}

// ---------------------------------------------------------------------------
// datasets proposals list
// ---------------------------------------------------------------------------

// proposalListOptions is the parsed command line of `datasets proposals list`.
type proposalListOptions struct {
	paginationOptions
	Dataset string
	Status  string
	Output  outputFormat
}

// parseProposalListOptions parses and validates the arguments of `datasets proposals list`.
func parseProposalListOptions(cmd *cobra.Command, args []string) (proposalListOptions, error) {
	if len(args) != 1 {
		return proposalListOptions{}, invalidArguments("proposals list expects exactly one dataset name, got %d arguments", len(args))
	}
	pagination, err := parsePaginationOptions(cmd)
	if err != nil {
		return proposalListOptions{}, err
	}
	status, err := cmd.Flags().GetString(statusFlagName)
	if err != nil {
		return proposalListOptions{}, err
	}
	switch status {
	case "", "open", "closed", "merged":
	default:
		return proposalListOptions{}, invalidArguments("invalid --status %q: use open, closed or merged", status)
	}
	output, err := outputOf(cmd)
	if err != nil {
		return proposalListOptions{}, err
	}
	return proposalListOptions{paginationOptions: pagination, Dataset: args[0], Status: status, Output: output}, nil
}

func proposalsListCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list <dataset>",
		Short:   "List dataset proposals",
		Example: "lakectl datasets proposals list customer-360 --status open",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			options, err := parseProposalListOptions(cmd, args)
			if err != nil {
				return err
			}
			return runProposalsList(cmd, options)
		},
	}
	withPaginationFlags(cmd)
	cmd.Flags().String(statusFlagName, "", "only list proposals in this state: open, closed or merged")
	return cmd
}

func runProposalsList(cmd *cobra.Command, options proposalListOptions) error {
	client, err := getClient()
	if err != nil {
		return err
	}
	params := &apigen.ListProposalsParams{Amount: apiutil.Ptr(apigen.Amount(options.Amount))}
	if options.Status != "" {
		params.Status = apiutil.Ptr(options.Status)
	}
	if options.After != "" {
		params.After = apiutil.Ptr(apigen.After(options.After))
	}
	resp, err := client.ListProposalsWithResponse(cmd.Context(), apigen.DatasetName(options.Dataset), params)
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
		return invalidResponse("bad response from server: missing proposal list")
	}
	proposals := resp.JSON200.Results
	rows := make([][]any, len(proposals))
	for i, proposal := range proposals {
		rows[i] = []any{
			proposal.Id,
			proposal.Title,
			proposal.Status,
			proposal.Author,
			formatVersion(proposal.BaseVersion),
			formatMillis(proposal.CreatedAt),
		}
	}
	printTable(cmd,
		[]any{"Proposal", "Title", "Status", "Author", "Base Version", "Creation Date"},
		rows, resp.JSON200.Pagination, options.Amount)
	return nil
}

// ---------------------------------------------------------------------------
// datasets proposals show
// ---------------------------------------------------------------------------

// proposalShowOptions is the parsed command line of `datasets proposals show`.
type proposalShowOptions struct {
	Dataset string
	ID      string
	Output  outputFormat
}

// parseProposalShowOptions parses and validates the arguments of `datasets proposals show`.
func parseProposalShowOptions(cmd *cobra.Command, args []string) (proposalShowOptions, error) {
	if len(args) != datasetAndIDArgsCount {
		return proposalShowOptions{}, invalidArguments("proposals show expects a dataset name and a proposal id, got %d arguments", len(args))
	}
	output, err := outputOf(cmd)
	if err != nil {
		return proposalShowOptions{}, err
	}
	return proposalShowOptions{Dataset: args[0], ID: args[1], Output: output}, nil
}

func proposalsShowCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "show <dataset> <proposal>",
		Short:   "Show a dataset proposal",
		Long:    "Show a proposal with its definition and the diffs against the latest version.",
		Example: "lakectl datasets proposals show customer-360 01J8Z3Q4X5",
		Args:    cobra.ExactArgs(datasetAndIDArgsCount),
		RunE: func(cmd *cobra.Command, args []string) error {
			options, err := parseProposalShowOptions(cmd, args)
			if err != nil {
				return err
			}
			return runProposalsShow(cmd, options)
		},
	}
	return cmd
}

func runProposalsShow(cmd *cobra.Command, options proposalShowOptions) error {
	client, err := getClient()
	if err != nil {
		return err
	}
	resp, err := client.GetProposalWithResponse(
		cmd.Context(),
		apigen.DatasetName(options.Dataset),
		apigen.ProposalID(options.ID),
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
		return invalidResponse("bad response from server: missing proposal")
	}
	printProposal(cmd, *resp.JSON200)
	return nil
}

// ---------------------------------------------------------------------------
// datasets proposals create
// ---------------------------------------------------------------------------

// proposalCreateOptions is the parsed command line of `datasets proposals create`.
type proposalCreateOptions struct {
	Dataset     string
	File        string
	Title       string
	Description string
	Output      outputFormat
}

// parseProposalCreateOptions parses and validates the arguments of `datasets proposals create`.
func parseProposalCreateOptions(cmd *cobra.Command, args []string) (proposalCreateOptions, error) {
	if len(args) != 1 {
		return proposalCreateOptions{}, invalidArguments("proposals create expects exactly one dataset name, got %d arguments", len(args))
	}
	file, err := cmd.Flags().GetString(fileFlagName)
	if err != nil {
		return proposalCreateOptions{}, err
	}
	if file == "" {
		return proposalCreateOptions{}, invalidArguments("proposals create requires --%s with a definition file", fileFlagName)
	}
	title, err := cmd.Flags().GetString(titleFlagName)
	if err != nil {
		return proposalCreateOptions{}, err
	}
	if title == "" {
		return proposalCreateOptions{}, invalidArguments("proposals create requires a non-empty --title")
	}
	description, err := cmd.Flags().GetString(descriptionFlagName)
	if err != nil {
		return proposalCreateOptions{}, err
	}
	output, err := outputOf(cmd)
	if err != nil {
		return proposalCreateOptions{}, err
	}
	return proposalCreateOptions{
		Dataset:     args[0],
		File:        file,
		Title:       title,
		Description: description,
		Output:      output,
	}, nil
}

func proposalsCreateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create <dataset> --file definition.json --title title",
		Short: "Open a dataset proposal",
		Long: `Open a proposal against the current dataset version. The definition is materialised on the
hidden preview branch so reviewers can browse the resulting data, and the proposal records the
definition and data diff against the latest version.`,
		Example: "lakectl datasets proposals create customer-360 --file customer-360.json --title \"add churn column\"",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			options, err := parseProposalCreateOptions(cmd, args)
			if err != nil {
				return err
			}
			return runProposalsCreate(cmd, options)
		},
	}
	definitionReadFlags(cmd)
	cmd.Flags().StringP(titleFlagName, "t", "", "proposal title (required)")
	cmd.Flags().StringP(descriptionFlagName, "d", "", "proposal description")
	return cmd
}

func runProposalsCreate(cmd *cobra.Command, options proposalCreateOptions) error {
	loaded, err := readDefinitionFile(options.File)
	if err != nil {
		return err
	}
	if loaded.Name != "" && loaded.Name != options.Dataset {
		return invalidArguments("definition file is for dataset %q, but the command line names %q", loaded.Name, options.Dataset)
	}
	request := apigen.ProposalCreateRequest{
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
	resp, err := client.CreateProposalWithResponse(cmd.Context(), apigen.DatasetName(options.Dataset), apigen.CreateProposalJSONRequestBody(request))
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
		return invalidResponse("bad response from server: missing proposal")
	}
	printProposal(cmd, *resp.JSON201)
	return nil
}

// ---------------------------------------------------------------------------
// datasets proposals merge
// ---------------------------------------------------------------------------

// proposalActionOptions is the parsed command line of `proposals merge` and `proposals close`.
type proposalActionOptions struct {
	Dataset string
	ID      string
	Message string
	Output  outputFormat
}

// parseProposalActionOptions parses and validates the arguments of `proposals merge|close`.
func parseProposalActionOptions(cmd *cobra.Command, args []string, action string) (proposalActionOptions, error) {
	if len(args) != datasetAndIDArgsCount {
		return proposalActionOptions{}, invalidArguments("proposals %s expects a dataset name and a proposal id, got %d arguments", action, len(args))
	}
	message := ""
	if cmd.Flags().Lookup(messageFlagName) != nil {
		var err error
		message, err = cmd.Flags().GetString(messageFlagName)
		if err != nil {
			return proposalActionOptions{}, err
		}
	}
	output, err := outputOf(cmd)
	if err != nil {
		return proposalActionOptions{}, err
	}
	return proposalActionOptions{Dataset: args[0], ID: args[1], Message: message, Output: output}, nil
}

func proposalsMergeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "merge <dataset> <proposal> --message msg",
		Short: "Merge a dataset proposal",
		Long: `Publish the proposal's definition as the next dataset version and close the proposal.

The merge needs the proposal's base version to be the latest one; otherwise it fails with a
conflict and the proposal must be rebased on the current version first.`,
		Example: "lakectl datasets proposals merge customer-360 01J8Z3Q4X5 --message \"add churn column\"",
		Args:    cobra.ExactArgs(datasetAndIDArgsCount),
		RunE: func(cmd *cobra.Command, args []string) error {
			options, err := parseProposalActionOptions(cmd, args, "merge")
			if err != nil {
				return err
			}
			if options.Message == "" {
				return invalidArguments("proposals merge requires a non-empty --%s", messageFlagName)
			}
			return runProposalsMerge(cmd, options)
		},
	}
	cmd.Flags().StringP(messageFlagName, "m", "", "publish message (required)")
	return cmd
}

func runProposalsMerge(cmd *cobra.Command, options proposalActionOptions) error {
	client, err := getClient()
	if err != nil {
		return err
	}
	request := apigen.ProposalMergeRequest{Message: apiutil.Ptr(options.Message)}
	resp, err := client.MergeProposalWithResponse(
		cmd.Context(),
		apigen.DatasetName(options.Dataset),
		apigen.ProposalID(options.ID),
		apigen.MergeProposalJSONRequestBody(request),
	)
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
		return invalidResponse("bad response from server: missing version")
	}
	printVersion(cmd, *resp.JSON201)
	return nil
}

// ---------------------------------------------------------------------------
// datasets proposals close
// ---------------------------------------------------------------------------

func proposalsCloseCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "close <dataset> <proposal>",
		Short:   "Close a dataset proposal without merging it",
		Long:    "Close a proposal and delete its preview branch. The dataset is not changed.",
		Example: "lakectl datasets proposals close customer-360 01J8Z3Q4X5",
		Args:    cobra.ExactArgs(datasetAndIDArgsCount),
		RunE: func(cmd *cobra.Command, args []string) error {
			options, err := parseProposalActionOptions(cmd, args, "close")
			if err != nil {
				return err
			}
			return runProposalsClose(cmd, options)
		},
	}
	return cmd
}

func runProposalsClose(cmd *cobra.Command, options proposalActionOptions) error {
	client, err := getClient()
	if err != nil {
		return err
	}
	closed := "closed"
	resp, err := client.UpdateProposalWithResponse(
		cmd.Context(),
		apigen.DatasetName(options.Dataset),
		apigen.ProposalID(options.ID),
		apigen.UpdateProposalJSONRequestBody(apigen.ProposalUpdateRequest{Status: &closed}),
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
		return invalidResponse("bad response from server: missing proposal")
	}
	printProposal(cmd, *resp.JSON200)
	return nil
}
