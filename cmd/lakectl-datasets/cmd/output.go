package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"
	lakectlcmd "github.com/treeverse/lakefs/cmd/lakectl/cmd"
	"github.com/treeverse/lakefs/pkg/datasets/apigen"
)

// tableTemplate renders rows exactly like lakectl's own list commands (cmd/lakectl/cmd
// resourceListTemplate): the shared "table" template function from lakectl.
const tableTemplate = `{{.Table | table -}}`

// printTable renders rows in lakectl's table style, followed by the pagination hint lakectl
// prints when another page is available.
func printTable(cmd *cobra.Command, headers []any, rows [][]any, pagination apigen.Pagination, amount int) {
	out := cmd.OutOrStdout()
	lakectlcmd.WriteTo(tableTemplate, struct{ Table *lakectlcmd.Table }{
		Table: &lakectlcmd.Table{Headers: headers, Rows: rows},
	}, out)
	if pagination.HasMore && amount > 0 {
		fmt.Fprintf(out, "for more results run with --%s %d --%s %q\n", amountFlagName, amount, afterFlagName, pagination.NextOffset)
	}
}

// printJSONBody prints a control-plane response body, indented. Printing the body as the server
// sent it keeps every field, including ones a newer server adds that the generated client does
// not know about yet.
func printJSONBody(w io.Writer, body []byte) error {
	var indented bytes.Buffer
	if err := json.Indent(&indented, body, "", "  "); err != nil {
		return fmt.Errorf("invalid JSON response: %w", err)
	}
	indented.WriteByte('\n')
	_, err := w.Write(indented.Bytes())
	return err
}

// printField writes one "Label: value" line, the shape lakectl uses for show commands.
func printField(w io.Writer, label string, value any) {
	fmt.Fprintf(w, "%s: %v\n", label, value)
}

// derefString returns the pointed-to string, or "" for a nil pointer.
func derefString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

// derefInt returns the pointed-to integer, or 0 for a nil pointer.
func derefInt(value *int) int {
	if value == nil {
		return 0
	}
	return *value
}

// formatMillis renders a Unix epoch milliseconds timestamp the way lakectl renders times.
func formatMillis(millis int64) string {
	if millis == 0 {
		return ""
	}
	return time.UnixMilli(millis).String()
}

// formatVersion renders a version number as the ref datasets use, `vN`. Version 0 means the
// dataset has no published version yet (§10.2).
func formatVersion(version int) string {
	if version <= 0 {
		return "-"
	}
	return fmt.Sprintf("v%d", version)
}

// formatMetadata renders free-form dataset metadata sorted by key.
func formatMetadata(metadata *apigen.DatasetMetadata) string {
	if metadata == nil || len(*metadata) == 0 {
		return ""
	}
	keys := make([]string, 0, len(*metadata))
	for key := range *metadata {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	pairs := make([]string, 0, len(keys))
	for _, key := range keys {
		pairs = append(pairs, fmt.Sprintf("%s=%v", key, (*metadata)[key]))
	}
	return strings.Join(pairs, ", ")
}

// formatStats renders the objects/bytes summary of a dataset or version.
func formatStats(stats *apigen.DatasetStats) string {
	if stats == nil {
		return "-"
	}
	return fmt.Sprintf("%d objects, %d bytes", stats.Objects, stats.Bytes)
}

// itemPath returns the source path of a data item.
func itemPath(item apigen.DataItem) string {
	switch {
	case item.Object != nil:
		return item.Object.Path
	case item.Prefix != nil:
		return item.Prefix.Path
	default:
		return ""
	}
}

// formatItem renders one definition item as `repository@commit:path`.
func formatItem(item apigen.DataItem) string {
	return fmt.Sprintf("%s@%s:%s", item.Repository, item.Ref.Id, itemPath(item))
}

// formatDefinitionItem is the item cell of the `show`/`validate` tables: type, source and ref type.
func formatDataItem(item apigen.DataItem) []any {
	return []any{item.Target, item.Type, item.Repository, item.Ref.Id, itemPath(item)}
}

// printDefinitionItems prints the items of a definition.
func printDefinitionItems(cmd *cobra.Command, definition *apigen.DatasetDefinition) {
	if definition == nil || len(definition.Data) == 0 {
		return
	}
	rows := make([][]any, len(definition.Data))
	for i, item := range definition.Data {
		rows[i] = formatDataItem(item)
	}
	printTable(cmd, []any{"Target", "Type", "Repository", "Ref", "Path"}, rows, apigen.Pagination{}, 0)
}

// printDefinition prints a definition: the summary fields and the items table.
func printDefinition(cmd *cobra.Command, definition apigen.DatasetDefinition) {
	out := cmd.OutOrStdout()
	printField(out, "Format Version", definition.FormatVersion)
	if description := derefString(definition.Description); description != "" {
		printField(out, "Description", description)
	}
	if metadata := formatMetadata(definition.Metadata); metadata != "" {
		printField(out, "Metadata", metadata)
	}
	if len(definition.Data) == 0 {
		fmt.Fprintln(out, "Items: none")
		return
	}
	fmt.Fprintf(out, "Items: %d\n", len(definition.Data))
	printDefinitionItems(cmd, &definition)
}

// printDataset prints a dataset record with the definition of its latest version.
func printDataset(cmd *cobra.Command, dataset apigen.Dataset) {
	out := cmd.OutOrStdout()
	printField(out, "Dataset", dataset.Name)
	if description := derefString(dataset.Description); description != "" {
		printField(out, "Description", description)
	}
	printField(out, "Created At", formatMillis(dataset.CreatedAt))
	printField(out, "Created By", dataset.CreatedBy)
	printField(out, "Latest Version", formatVersion(dataset.LatestVersion))
	printField(out, "State", dataset.State)
	printField(out, "Data", formatStats(dataset.Stats))
	if dataset.Definition != nil {
		fmt.Fprintln(out)
		printDefinition(cmd, *dataset.Definition)
	}
}

// printVersion prints one dataset version.
func printVersion(cmd *cobra.Command, version apigen.Version) {
	out := cmd.OutOrStdout()
	printField(out, "Version", formatVersion(version.Version))
	if message := derefString(version.Message); message != "" {
		printField(out, "Message", message)
	}
	printField(out, "Publisher", version.Publisher)
	printField(out, "Published At", formatMillis(version.PublishedAt))
	printField(out, "Commit ID", version.CommitId)
	if proposal := derefString(version.ProposalId); proposal != "" {
		printField(out, "Proposal", proposal)
	}
	printField(out, "Data", formatStats(version.Stats))
	if version.Definition != nil {
		fmt.Fprintln(out)
		printDefinition(cmd, *version.Definition)
	}
}

// printTask prints an asynchronous publish task (§6.5).
func printTask(cmd *cobra.Command, dataset string, task apigen.Task) {
	out := cmd.OutOrStdout()
	printField(out, "Task ID", task.Id)
	printField(out, "Status", task.Status)
	printField(out, "Dataset", dataset)
	if task.Version != nil {
		printField(out, "Version", formatVersion(derefInt(task.Version)))
	}
	if failure := derefString(task.Error); failure != "" {
		printField(out, "Error", failure)
	}
	fmt.Fprintf(out, "\nThe publish runs in the background; check it with:\n  lakectl %s show %s\n", CommandName, dataset)
}

// printDefinitionDiff prints the structural diff of two definitions (§10.3).
func printDefinitionDiff(cmd *cobra.Command, diff apigen.DefinitionDiff) {
	out := cmd.OutOrStdout()
	if diff.Description != nil {
		printField(out, "Description", fmt.Sprintf("%q -> %q", diff.Description.From, diff.Description.To))
	}
	if diff.Metadata != nil && len(*diff.Metadata) > 0 {
		fmt.Fprintln(out, "\nMetadata:")
		rows := make([][]any, len(*diff.Metadata))
		for i, change := range *diff.Metadata {
			rows[i] = []any{change.Key, change.Change, formatMetadataValue(change.From), formatMetadataValue(change.To)}
		}
		printTable(cmd, []any{"Key", "Change", "From", "To"}, rows, apigen.Pagination{}, 0)
	}
	if len(diff.Items) == 0 {
		fmt.Fprintln(out, "\nNo definition changes.")
		return
	}
	fmt.Fprintf(out, "\nItems: %d changed\n", len(diff.Items))
	rows := make([][]any, len(diff.Items))
	for i, change := range diff.Items {
		rows[i] = []any{change.Target, change.Change, formatItemPtr(change.From), formatItemPtr(change.To)}
	}
	printTable(cmd, []any{"Target", "Change", "From", "To"}, rows, apigen.Pagination{}, 0)
}

// formatItemPtr renders an item that may be absent (an added or removed item).
func formatItemPtr(item *apigen.DataItem) string {
	if item == nil {
		return ""
	}
	return formatItem(*item)
}

// formatMetadataValue renders a metadata diff value.
func formatMetadataValue(value *interface{}) string {
	if value == nil {
		return ""
	}
	return fmt.Sprintf("%v", *value)
}

// printDataDiff prints the paginated data diff of two dataset refs (§10.3).
func printDataDiff(cmd *cobra.Command, diff apigen.DataDiffList, amount int) {
	if len(diff.Results) == 0 {
		// An empty result is a valid "the data is identical" answer, not a rendering problem.
		fmt.Fprintln(cmd.OutOrStdout(), "No data changes.")
		return
	}
	rows := make([][]any, len(diff.Results))
	for i, entry := range diff.Results {
		size := any("-")
		if entry.SizeBytes != nil {
			size = *entry.SizeBytes
		}
		rows[i] = []any{entry.Path, entry.Type, size}
	}
	printTable(cmd, []any{"Path", "Change", "Size (bytes)"}, rows, diff.Pagination, amount)
}

// printProposal prints one dataset proposal (§5.6).
func printProposal(cmd *cobra.Command, proposal apigen.Proposal) {
	out := cmd.OutOrStdout()
	printField(out, "Proposal", proposal.Id)
	printField(out, "Dataset", proposal.Dataset)
	printField(out, "Title", proposal.Title)
	if description := derefString(proposal.Description); description != "" {
		printField(out, "Description", description)
	}
	printField(out, "Status", proposal.Status)
	printField(out, "Author", proposal.Author)
	printField(out, "Base Version", formatVersion(proposal.BaseVersion))
	printField(out, "Created At", formatMillis(proposal.CreatedAt))
	if proposal.ClosedAt != nil {
		printField(out, "Closed At", formatMillis(*proposal.ClosedAt))
	}
	if proposal.MergedVersion != nil {
		printField(out, "Merged Version", formatVersion(*proposal.MergedVersion))
	}
	printField(out, "Preview Branch", proposal.PreviewBranch)
	if proposal.Definition != nil {
		fmt.Fprintln(out)
		printDefinition(cmd, *proposal.Definition)
	}
	if proposal.Diff != nil {
		fmt.Fprintln(out)
		printDefinitionDiff(cmd, *proposal.Diff)
	}
}

// printMetadataKey prints one metadata key definition (§5.5).
func printMetadataKey(cmd *cobra.Command, key apigen.MetadataKey) {
	out := cmd.OutOrStdout()
	printField(out, "Key", key.Name)
	printField(out, "Type", key.Type)
	printField(out, "Required", key.Required)
	printField(out, "Multiple", key.AllowMultiple)
	if key.AllowedValues != nil && len(*key.AllowedValues) > 0 {
		printField(out, "Allowed Values", strings.Join(*key.AllowedValues, ", "))
	}
	if description := derefString(key.Description); description != "" {
		printField(out, "Description", description)
	}
	printField(out, "Created At", formatMillis(key.CreatedAt))
	printField(out, "Created By", key.CreatedBy)
}
