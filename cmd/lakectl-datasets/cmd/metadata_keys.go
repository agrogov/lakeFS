package cmd

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/datasets/apigen"
)

// metadataKeyTypes are the types a metadata key may have (DATASETS_DESIGN.md §5.5).
var metadataKeyTypes = []string{"string", "enum", "number", "boolean", "date"}

// metadataKeysCmd groups the global metadata key commands (§12.1, §5.5).
func metadataKeysCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "metadata-keys",
		Short: "Manage metadata key definitions",
		Long: `Manage the global metadata keys of the datasets control plane. A metadata key types one
metadata value: it can be required, restricted to an enum of allowed values and allow multiple
values. Metadata that is not described by a key stays free-form unless the server rejects unknown
keys.

Deleting a key does not touch the metadata of existing versions; the values become free-form.`,
	}
	cmd.AddCommand(metadataKeysListCmd(), metadataKeysCreateCmd(), metadataKeysDeleteCmd())
	return cmd
}

// ---------------------------------------------------------------------------
// datasets metadata-keys list
// ---------------------------------------------------------------------------

// metadataKeysListOptions is the parsed command line of `datasets metadata-keys list`.
type metadataKeysListOptions struct {
	paginationOptions
	Output outputFormat
}

// parseMetadataKeysListOptions parses and validates the arguments of `metadata-keys list`.
func parseMetadataKeysListOptions(cmd *cobra.Command, args []string) (metadataKeysListOptions, error) {
	if len(args) != 0 {
		return metadataKeysListOptions{}, invalidArguments("metadata-keys list takes no arguments, got %d", len(args))
	}
	pagination, err := parsePaginationOptions(cmd)
	if err != nil {
		return metadataKeysListOptions{}, err
	}
	output, err := outputOf(cmd)
	if err != nil {
		return metadataKeysListOptions{}, err
	}
	return metadataKeysListOptions{paginationOptions: pagination, Output: output}, nil
}

func metadataKeysListCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Short:   "List metadata keys",
		Example: "lakectl datasets metadata-keys list",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			options, err := parseMetadataKeysListOptions(cmd, args)
			if err != nil {
				return err
			}
			return runMetadataKeysList(cmd, options)
		},
	}
	withPaginationFlags(cmd)
	return cmd
}

func runMetadataKeysList(cmd *cobra.Command, options metadataKeysListOptions) error {
	client, err := getClient()
	if err != nil {
		return err
	}
	params := &apigen.ListMetadataKeysParams{Amount: apiutil.Ptr(apigen.Amount(options.Amount))}
	if options.After != "" {
		params.After = apiutil.Ptr(apigen.After(options.After))
	}
	resp, err := client.ListMetadataKeysWithResponse(cmd.Context(), params)
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
		return invalidResponse("bad response from server: missing metadata key list")
	}
	keys := resp.JSON200.Results
	rows := make([][]any, len(keys))
	for i, key := range keys {
		allowed := ""
		if key.AllowedValues != nil {
			allowed = strings.Join(*key.AllowedValues, ", ")
		}
		rows[i] = []any{
			key.Name,
			key.Type,
			key.Required,
			key.AllowMultiple,
			allowed,
			derefString(key.Description),
		}
	}
	printTable(cmd,
		[]any{"Key", "Type", "Required", "Multiple", "Allowed Values", "Description"},
		rows, resp.JSON200.Pagination, options.Amount)
	return nil
}

// ---------------------------------------------------------------------------
// datasets metadata-keys create
// ---------------------------------------------------------------------------

// metadataKeyCreateOptions is the parsed command line of `datasets metadata-keys create`.
type metadataKeyCreateOptions struct {
	Name          string
	Type          string
	Required      bool
	AllowMultiple bool
	AllowedValues []string
	Description   string
	Output        outputFormat
}

// parseMetadataKeyCreateOptions parses and validates the arguments of `metadata-keys create`.
func parseMetadataKeyCreateOptions(cmd *cobra.Command, args []string) (metadataKeyCreateOptions, error) {
	if len(args) != 1 {
		return metadataKeyCreateOptions{}, invalidArguments("metadata-keys create expects exactly one key name, got %d arguments", len(args))
	}
	keyType, err := cmd.Flags().GetString(typeFlagName)
	if err != nil {
		return metadataKeyCreateOptions{}, err
	}
	if keyType == "" {
		return metadataKeyCreateOptions{}, invalidArguments("metadata-keys create requires --type (one of %v)", metadataKeyTypes)
	}
	if !contains(metadataKeyTypes, keyType) {
		return metadataKeyCreateOptions{}, invalidArguments("invalid --type %q: use one of %v", keyType, metadataKeyTypes)
	}
	allowedValues, err := cmd.Flags().GetStringSlice(allowedValuesFlagName)
	if err != nil {
		return metadataKeyCreateOptions{}, err
	}
	switch {
	case keyType == "enum" && len(allowedValues) == 0:
		return metadataKeyCreateOptions{}, invalidResponse("an enum metadata key requires at least one --allowed-values entry")
	case keyType != "enum" && len(allowedValues) > 0:
		return metadataKeyCreateOptions{}, invalidArguments("--allowed-values is only allowed for %q keys, not %q", "enum", keyType)
	}
	required, err := cmd.Flags().GetBool(requiredFlagName)
	if err != nil {
		return metadataKeyCreateOptions{}, err
	}
	allowMultiple, err := cmd.Flags().GetBool(allowMultipleFlagName)
	if err != nil {
		return metadataKeyCreateOptions{}, err
	}
	description, err := cmd.Flags().GetString(descriptionFlagName)
	if err != nil {
		return metadataKeyCreateOptions{}, err
	}
	output, err := outputOf(cmd)
	if err != nil {
		return metadataKeyCreateOptions{}, err
	}
	return metadataKeyCreateOptions{
		Name:          args[0],
		Type:          keyType,
		Required:      required,
		AllowMultiple: allowMultiple,
		AllowedValues: allowedValues,
		Description:   description,
		Output:        output,
	}, nil
}

func metadataKeysCreateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create <key> --type type",
		Short: "Create a metadata key",
		Long: `Create a typed metadata key. Types are string, enum, number, boolean and date; an enum key
requires --allowed-values. Required keys are enforced on every definition write, while existing
versions are never re-validated.`,
		Example: "lakectl datasets metadata-keys create tier --type enum --allowed-values bronze,silver,gold",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			options, err := parseMetadataKeyCreateOptions(cmd, args)
			if err != nil {
				return err
			}
			return runMetadataKeysCreate(cmd, options)
		},
	}
	cmd.Flags().String(typeFlagName, "", "value type: string, enum, number, boolean or date (required)")
	cmd.Flags().Bool(requiredFlagName, false, "require this key in every new definition")
	cmd.Flags().Bool(allowMultipleFlagName, false, "the metadata value is a list of values")
	cmd.Flags().StringSlice(allowedValuesFlagName, nil, "allowed values of an enum key")
	cmd.Flags().StringP(descriptionFlagName, "d", "", "description of the key")
	return cmd
}

func runMetadataKeysCreate(cmd *cobra.Command, options metadataKeyCreateOptions) error {
	request := apigen.MetadataKeyCreateRequest{
		Name:          options.Name,
		Type:          options.Type,
		Required:      apiutil.Ptr(options.Required),
		AllowMultiple: apiutil.Ptr(options.AllowMultiple),
	}
	if len(options.AllowedValues) > 0 {
		request.AllowedValues = apiutil.Ptr(options.AllowedValues)
	}
	if options.Description != "" {
		request.Description = apiutil.Ptr(options.Description)
	}
	client, err := getClient()
	if err != nil {
		return err
	}
	resp, err := client.CreateMetadataKeyWithResponse(cmd.Context(), apigen.CreateMetadataKeyJSONRequestBody(request))
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
		return invalidResponse("bad response from server: missing metadata key")
	}
	printMetadataKey(cmd, *resp.JSON201)
	return nil
}

// ---------------------------------------------------------------------------
// datasets metadata-keys delete
// ---------------------------------------------------------------------------

// metadataKeyDeleteOptions is the parsed command line of `datasets metadata-keys delete`.
type metadataKeyDeleteOptions struct {
	Name   string
	Output outputFormat
}

// parseMetadataKeyDeleteOptions parses and validates the arguments of `metadata-keys delete`.
func parseMetadataKeyDeleteOptions(cmd *cobra.Command, args []string) (metadataKeyDeleteOptions, error) {
	if len(args) != 1 {
		return metadataKeyDeleteOptions{}, invalidArguments("metadata-keys delete expects exactly one key name, got %d arguments", len(args))
	}
	output, err := outputOf(cmd)
	if err != nil {
		return metadataKeyDeleteOptions{}, err
	}
	return metadataKeyDeleteOptions{Name: args[0], Output: output}, nil
}

func metadataKeysDeleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "delete <key>",
		Short:   "Delete a metadata key",
		Long:    "Delete a metadata key definition. Existing metadata values become free-form.",
		Example: "lakectl datasets metadata-keys delete tier",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			options, err := parseMetadataKeyDeleteOptions(cmd, args)
			if err != nil {
				return err
			}
			return runMetadataKeysDelete(cmd, options)
		},
	}
	return cmd
}

func runMetadataKeysDelete(cmd *cobra.Command, options metadataKeyDeleteOptions) error {
	client, err := getClient()
	if err != nil {
		return err
	}
	resp, err := client.DeleteMetadataKeyWithResponse(cmd.Context(), apigen.MetadataKeyName(options.Name))
	if err != nil {
		return err
	}
	if err := checkResponse(resp.HTTPResponse, resp.Body, http.StatusNoContent); err != nil {
		return err
	}
	if options.Output != outputJSON {
		fmt.Fprintf(cmd.OutOrStdout(), "Metadata key %q deleted.\n", options.Name)
	}
	return nil
}

// contains reports whether values contains value.
func contains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}
