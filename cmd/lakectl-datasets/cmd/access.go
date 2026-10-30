package cmd

import (
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/datasets/apigen"
)

// principalTypeUser and principalTypeGroup are the principal types of the access grants
// (DATASETS_DESIGN.md §9.2).
const (
	principalTypeUser  = "user"
	principalTypeGroup = "group"
)

// accessCmd groups the dataset access grant commands (§12.1, §9.2).
func accessCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "access",
		Short: "Manage dataset access grants",
		Long: `Manage who may read a dataset. A grant attaches the dataset's managed read policy to a user
or a group, so consumers need a grant on the dataset only, never on the repositories it is built
from.`,
	}
	cmd.AddCommand(accessListCmd(), accessGrantCmd(), accessRevokeCmd())
	return cmd
}

// accessOptions is the parsed command line of `datasets access list`.
type accessOptions struct {
	Dataset string
	Output  outputFormat
}

// parseAccessOptions parses and validates the arguments of `datasets access list`.
func parseAccessOptions(cmd *cobra.Command, args []string) (accessOptions, error) {
	if len(args) != 1 {
		return accessOptions{}, invalidArguments("access list expects exactly one dataset name, got %d arguments", len(args))
	}
	output, err := outputOf(cmd)
	if err != nil {
		return accessOptions{}, err
	}
	return accessOptions{Dataset: args[0], Output: output}, nil
}

func accessListCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list <dataset>",
		Short:   "List dataset access grants",
		Example: "lakectl datasets access list customer-360",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			options, err := parseAccessOptions(cmd, args)
			if err != nil {
				return err
			}
			return runAccessList(cmd, options)
		},
	}
	return cmd
}

func runAccessList(cmd *cobra.Command, options accessOptions) error {
	client, err := getClient()
	if err != nil {
		return err
	}
	resp, err := client.ListAccessWithResponse(cmd.Context(), apigen.DatasetName(options.Dataset))
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
		return invalidResponse("bad response from server: missing access grant list")
	}
	grants := resp.JSON200.Results
	rows := make([][]any, len(grants))
	for i, grant := range grants {
		rows[i] = []any{
			grant.PrincipalType,
			grant.PrincipalId,
			derefString(grant.GrantedBy),
			formatMillis(derefInt64(grant.GrantedAt)),
		}
	}
	printTable(cmd,
		[]any{"Principal Type", "Principal", "Granted By", "Granted At"},
		rows, resp.JSON200.Pagination, 0)
	return nil
}

// accessGrantOptions is the parsed command line of `datasets access grant` and `revoke`.
type accessGrantOptions struct {
	Dataset       string
	PrincipalType string
	PrincipalID   string
	Output        outputFormat
}

// parseAccessGrantOptions parses and validates the arguments of `datasets access grant|revoke`.
// Exactly one of --user and --group must be given (§12.1).
func parseAccessGrantOptions(cmd *cobra.Command, args []string, action string) (accessGrantOptions, error) {
	if len(args) != 1 {
		return accessGrantOptions{}, invalidArguments("access %s expects exactly one dataset name, got %d arguments", action, len(args))
	}
	user, err := cmd.Flags().GetString(principalUserFlag)
	if err != nil {
		return accessGrantOptions{}, err
	}
	group, err := cmd.Flags().GetString(principalGroupFlag)
	if err != nil {
		return accessGrantOptions{}, err
	}
	var principalType, principalID string
	switch {
	case user != "" && group != "":
		return accessGrantOptions{}, invalidArguments("access %s takes exactly one of --%s and --%s, not both", action, principalUserFlag, principalGroupFlag)
	case user != "":
		principalType, principalID = principalTypeUser, user
	case group != "":
		principalType, principalID = principalTypeGroup, group
	default:
		return accessGrantOptions{}, invalidArguments("access %s requires --%s or --%s", action, principalUserFlag, principalGroupFlag)
	}
	output, err := outputOf(cmd)
	if err != nil {
		return accessGrantOptions{}, err
	}
	return accessGrantOptions{
		Dataset:       args[0],
		PrincipalType: principalType,
		PrincipalID:   principalID,
		Output:        output,
	}, nil
}

func accessGrantCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "grant <dataset> --user user | --group group",
		Short: "Grant read access to a dataset",
		Long: `Grant read access to a dataset by attaching its managed read policy to a user or a group.
Consumers can then read the dataset with lakefs:// and s3:// paths without any grant on the
source repositories.`,
		Example: "lakectl datasets access grant customer-360 --group analysts",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			options, err := parseAccessGrantOptions(cmd, args, "grant")
			if err != nil {
				return err
			}
			return runAccessGrant(cmd, options)
		},
	}
	cmd.Flags().String(principalUserFlag, "", "grant access to this user")
	cmd.Flags().String(principalGroupFlag, "", "grant access to this group")
	return cmd
}

func runAccessGrant(cmd *cobra.Command, options accessGrantOptions) error {
	client, err := getClient()
	if err != nil {
		return err
	}
	resp, err := client.GrantAccessWithResponse(
		cmd.Context(),
		apigen.DatasetName(options.Dataset),
		apigen.GrantAccessJSONRequestBody(apigen.AccessGrantRequest{
			PrincipalType: options.PrincipalType,
			PrincipalId:   options.PrincipalID,
		}),
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
		return invalidResponse("bad response from server: missing access grant")
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Granted %s %q read access to dataset %q.\n",
		resp.JSON201.PrincipalType, resp.JSON201.PrincipalId, options.Dataset)
	return nil
}

func accessRevokeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "revoke <dataset> --user user | --group group",
		Short:   "Revoke read access to a dataset",
		Long:    "Revoke a dataset access grant by detaching the dataset's managed read policy.",
		Example: "lakectl datasets access revoke customer-360 --group analysts",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			options, err := parseAccessGrantOptions(cmd, args, "revoke")
			if err != nil {
				return err
			}
			return runAccessRevoke(cmd, options)
		},
	}
	cmd.Flags().String(principalUserFlag, "", "revoke access from this user")
	cmd.Flags().String(principalGroupFlag, "", "revoke access from this group")
	return cmd
}

func runAccessRevoke(cmd *cobra.Command, options accessGrantOptions) error {
	client, err := getClient()
	if err != nil {
		return err
	}
	resp, err := client.RevokeAccessWithResponse(
		cmd.Context(),
		apigen.DatasetName(options.Dataset),
		apigen.PrincipalType(options.PrincipalType),
		apigen.PrincipalID(options.PrincipalID),
	)
	if err != nil {
		return err
	}
	if err := checkResponse(resp.HTTPResponse, resp.Body, http.StatusNoContent); err != nil {
		return err
	}
	if options.Output != outputJSON {
		fmt.Fprintf(cmd.OutOrStdout(), "Revoked %s %q access to dataset %q.\n",
			options.PrincipalType, options.PrincipalID, options.Dataset)
	}
	return nil
}

// derefInt64 returns the pointed-to integer, or 0 for a nil pointer.
func derefInt64(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}
