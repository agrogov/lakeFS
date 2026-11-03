package importer

// The import itself: enumerate the source, then adopt one table at a time into lakeFS.
//
// The run has three phases, and keeping them apart is what makes a failed bulk import a report and
// not a mystery:
//
//  1. Discovery: list namespaces (recursively from the requested roots, or from the root for
//     --all) and the tables of each. A discovery failure is fatal for the run: without the list
//     there is nothing to report per table.
//  2. Per table: load the metadata, refuse what this catalog cannot serve (format version 3, a
//     foreign location in the default mode), skip what already exists, otherwise create the
//     namespaces, copy if asked, register, and record one result. A failure here is recorded for
//     that table and the run continues.
//  3. Reporting: the caller prints the per-table results and the totals. Nothing is committed -
//     register stages entries on the branch (ICEBERG_CATALOG_PLAN.md §6.2), so the summary
//     describes what a following `lakectl commit` will publish.

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

// Status is the outcome of importing one table.
type Status string

const (
	// StatusImported means register succeeded.
	StatusImported Status = "imported"
	// StatusSkipped means the target already had the table and --overwrite was not set.
	StatusSkipped Status = "skipped"
	// StatusFailed means this table was not imported; the run continued with the next one.
	StatusFailed Status = "failed"
	// StatusPlanned is what a --dry-run reports instead of imported/skipped.
	StatusPlanned Status = "planned"
)

// Options is one import run.
type Options struct {
	// Source is the catalog tables are read from, and Target the lakeFS catalog they are adopted
	// into. Both are required.
	Source *Source
	Target *Target

	// FileReader reads source table files. It is required by Copy.
	FileReader FileReader

	// Uploader writes the copied files through the lakeFS object API. It is required by Copy.
	Uploader Uploader

	// Repository and Branch are the lakeFS destination. StorageNamespace is the repository's
	// storage namespace, read by the command from the lakeFS API; the importer never guesses it.
	Repository       string
	Branch           string
	StorageNamespace string

	// Namespaces are the source namespaces to import, with their descendants. It is required
	// unless All is set.
	Namespaces []Namespace

	// All imports every namespace of the source catalog.
	All bool

	// TargetPrefix is prepended, level by level, to every source namespace.
	TargetPrefix []string

	// Overwrite replaces a table that already exists (drop-then-register). Without it an existing
	// table is skipped, which is what makes a resume cheap.
	Overwrite bool

	// Copy copies the table's files into the storage namespace and rewrites locations. Without it
	// a table is registered in place, and a foreign location is a per-table failure.
	Copy bool

	// DryRun discovers and validates without writing anything.
	DryRun bool
}

// TableResult is the outcome of one table.
type TableResult struct {
	// Source and Target are the dotted identifiers; Target includes the repository and the branch
	// as its first two levels, the way the catalog addresses a table.
	Source string
	Target string

	// MetadataLocation is the source metadata location the table was registered with, or the
	// rewritten one a copy registered.
	MetadataLocation string

	// Files is the number of files a copy wrote (metadata, manifests and data).
	Files int

	Status Status

	// Detail explains a skip or a failure in one sentence, and names the mode that would have
	// handled a refused location.
	Detail string
}

// Summary is the result of one run.
type Summary struct {
	Results  []TableResult
	DryRun   bool
	Imported int
	Skipped  int
	Failed   int
	Planned  int
}

// Import runs one import and returns the per-table summary. An error means the run could not
// discover the source or the options are invalid; every per-table failure is in the summary.
func Import(ctx context.Context, opts Options) (*Summary, error) {
	if err := validateOptions(opts); err != nil {
		return nil, err
	}
	runner := &runner{opts: opts}
	namespaces, err := runner.discoverNamespaces(ctx)
	if err != nil {
		return nil, err
	}
	identifiers, err := runner.discoverTables(ctx, namespaces)
	if err != nil {
		return nil, err
	}

	summary := &Summary{DryRun: opts.DryRun, Results: make([]TableResult, 0, len(identifiers))}
	for _, identifier := range identifiers {
		result := runner.importTable(ctx, identifier)
		summary.Results = append(summary.Results, result)
		switch result.Status {
		case StatusImported:
			summary.Imported++
		case StatusSkipped:
			summary.Skipped++
		case StatusFailed:
			summary.Failed++
		case StatusPlanned:
			summary.Planned++
		}
	}
	return summary, nil
}

// validateOptions refuses a run that cannot mean anything, before any network call.
func validateOptions(opts Options) error {
	switch {
	case opts.Source == nil:
		return fmt.Errorf("%w: a source catalog is required", ErrInvalidOptions)
	case opts.Target == nil:
		return fmt.Errorf("%w: a target catalog is required", ErrInvalidOptions)
	case opts.Repository == "":
		return fmt.Errorf("%w: a repository is required", ErrInvalidOptions)
	case opts.Branch == "":
		return fmt.Errorf("%w: a branch is required", ErrInvalidOptions)
	case opts.StorageNamespace == "":
		return fmt.Errorf("%w: the repository's storage namespace is required", ErrInvalidOptions)
	case opts.All && len(opts.Namespaces) > 0:
		return fmt.Errorf("%w: --all and --namespace are mutually exclusive", ErrInvalidOptions)
	case !opts.All && len(opts.Namespaces) == 0:
		return fmt.Errorf("%w: one of --namespace or --all is required", ErrInvalidOptions)
	case opts.Copy && opts.FileReader == nil:
		return fmt.Errorf("%w: copy mode requires a source file reader", ErrInvalidOptions)
	case opts.Copy && opts.Uploader == nil:
		return fmt.Errorf("%w: copy mode requires an object uploader", ErrInvalidOptions)
	}
	for _, namespace := range opts.Namespaces {
		if len(namespace) == 0 {
			return fmt.Errorf("%w: an empty --namespace is not a namespace", ErrInvalidOptions)
		}
	}
	for _, level := range opts.TargetPrefix {
		if strings.TrimSpace(level) == "" {
			return fmt.Errorf("%w: --target-namespace-prefix has an empty level", ErrInvalidOptions)
		}
	}
	return nil
}

// runner carries the options through discovery and the per-table work.
type runner struct {
	opts Options
}

// discoverNamespaces returns the namespaces to import, including their descendants, in a stable
// order. A namespace is discovered from the parent listing, so an explicitly requested namespace is
// visited even if the catalog lists no table in it.
func (r *runner) discoverNamespaces(ctx context.Context) ([]Namespace, error) {
	var queue []Namespace
	if r.opts.All {
		queue = append(queue, Namespace(nil))
	} else {
		queue = append(queue, r.opts.Namespaces...)
	}

	visited := map[string]bool{}
	var namespaces []Namespace
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		key := joinLevels(current)
		if visited[key] {
			continue
		}
		visited[key] = true
		namespaces = append(namespaces, current)

		children, err := r.opts.Source.ListNamespaces(ctx, current)
		if err != nil {
			return nil, err
		}
		for _, child := range children {
			if len(child) == 0 || visited[joinLevels(child)] {
				continue
			}
			queue = append(queue, child)
		}
	}

	sort.Slice(namespaces, func(i, j int) bool {
		return namespaces[i].Dotted() < namespaces[j].Dotted()
	})
	return namespaces, nil
}

// discoverTables lists the tables of every namespace, deduplicated and sorted. The root namespace
// is skipped for the table listing: a table lives in a namespace, and an empty namespace parameter
// is not a valid address.
func (r *runner) discoverTables(ctx context.Context, namespaces []Namespace) ([]TableIdentifier, error) {
	seen := map[string]bool{}
	var identifiers []TableIdentifier
	for _, namespace := range namespaces {
		if len(namespace) == 0 {
			continue
		}
		tables, err := r.opts.Source.ListTables(ctx, namespace)
		if err != nil {
			return nil, err
		}
		for _, table := range tables {
			if table.Name == "" {
				continue
			}
			if len(table.Namespace) == 0 {
				// Some catalogs omit the namespace in a listing because the request path already
				// named it. The identifier must be absolute from here on.
				table.Namespace = append(Namespace(nil), namespace...)
			}
			key := joinLevels(table.Namespace) + sourceNamespaceSeparator + table.Name
			if seen[key] {
				continue
			}
			seen[key] = true
			identifiers = append(identifiers, table)
		}
	}
	sort.Slice(identifiers, func(i, j int) bool {
		return identifiers[i].Dotted() < identifiers[j].Dotted()
	})
	return identifiers, nil
}

// importTable imports one table and returns its result. It never returns an error: a table's
// failure belongs in the summary.
func (r *runner) importTable(ctx context.Context, identifier TableIdentifier) TableResult {
	targetLevels := r.targetNamespace(identifier)
	result := TableResult{
		Source: identifier.Dotted(),
		Target: dottedIdentifier(append(append([]string{r.opts.Repository, r.opts.Branch}, targetLevels...), identifier.Name)),
	}

	loaded, err := r.opts.Source.LoadTable(ctx, identifier)
	if err != nil {
		return failed(result, fmt.Sprintf("load source table: %s", err))
	}
	metadata, err := ParseMetadata(loaded.Metadata)
	if err != nil {
		return failed(result, err.Error())
	}
	version, err := metadata.FormatVersion()
	if err != nil {
		return failed(result, err.Error())
	}
	if version != 1 && version != 2 {
		return failed(result, fmt.Sprintf("format version %d is not supported: this catalog serves format versions 1 and 2", version))
	}
	result.MetadataLocation = strings.TrimSpace(loaded.MetadataLocation)

	fullLevels := append([]string{r.opts.Repository, r.opts.Branch}, targetLevels...)

	if r.opts.DryRun {
		exists, err := r.opts.Target.TableExists(ctx, fullLevels, identifier.Name)
		switch {
		case err != nil:
			return failed(result, fmt.Sprintf("check the target table: %s", err))
		case exists && !r.opts.Overwrite:
			result.Status = StatusSkipped
			result.Detail = "the target table already exists"
			return result
		case exists:
			result.Status = StatusPlanned
			result.Detail = "would replace the existing table"
			return result
		default:
			result.Status = StatusPlanned
			result.Detail = "would import"
			return result
		}
	}

	// Existence first, so a resumed run stages nothing for a table it is going to skip - not even
	// the namespace markers, which would otherwise make every re-run a change on the branch.
	exists, err := r.opts.Target.TableExists(ctx, fullLevels, identifier.Name)
	if err != nil {
		return failed(result, fmt.Sprintf("check the target table: %s", err))
	}
	if exists && !r.opts.Overwrite {
		result.Status = StatusSkipped
		result.Detail = "the target table already exists (use --overwrite to replace it)"
		return result
	}
	if err := r.opts.Target.EnsureNamespace(ctx, fullLevels); err != nil {
		return failed(result, fmt.Sprintf("create the target namespace: %s", err))
	}
	if exists {
		if err := r.opts.Target.DropTable(ctx, fullLevels, identifier.Name); err != nil {
			return failed(result, fmt.Sprintf("drop the existing table for --overwrite: %s", err))
		}
	}

	if r.opts.Copy {
		return r.copyTable(ctx, identifier, fullLevels, metadata, result)
	}
	return r.registerTableInPlace(ctx, fullLevels, identifier, metadata, result)
}

// targetNamespace returns the iceberg namespace a table is imported into: the target prefix
// followed by the source namespace levels.
func (r *runner) targetNamespace(identifier TableIdentifier) []string {
	target := make([]string, 0, len(r.opts.TargetPrefix)+len(identifier.Namespace))
	target = append(target, r.opts.TargetPrefix...)
	target = append(target, identifier.Namespace...)
	return target
}

// registerTableInPlace adopts a table whose files are already inside the repository's storage
// namespace. The checks here are the catalog's own preconditions, reported per table with the mode
// that would handle the table instead.
func (r *runner) registerTableInPlace(ctx context.Context, fullLevels []string, identifier TableIdentifier, metadata *Metadata, result TableResult) TableResult {
	if result.MetadataLocation == "" {
		return failed(result, "the source catalog returned no metadata-location")
	}
	if !inside(r.opts.StorageNamespace, result.MetadataLocation) {
		return failed(result, fmt.Sprintf(
			"metadata-location %q is outside the repository storage namespace %q; use --copy to copy the table into it",
			result.MetadataLocation, r.opts.StorageNamespace))
	}
	tableArea := joinLocation(r.opts.StorageNamespace, "_iceberg")
	tableRoot := metadata.Location()
	switch {
	case tableRoot == "":
		return failed(result, "the metadata document carries no location")
	case !inside(tableArea, tableRoot):
		return failed(result, fmt.Sprintf(
			"table location %q is outside %q; use --copy to copy the table into it", tableRoot, tableArea))
	case !inside(tableRoot, result.MetadataLocation):
		return failed(result, fmt.Sprintf(
			"metadata-location %q is outside the table location %q; use --copy to copy the table", result.MetadataLocation, tableRoot))
	}

	if err := r.opts.Target.RegisterTable(ctx, fullLevels, identifier.Name, result.MetadataLocation); err != nil {
		return failed(result, fmt.Sprintf("register: %s", err))
	}
	result.Status = StatusImported
	return result
}

// copyTable copies a table's files into the storage namespace, rewrites the locations that name
// them, and registers the rewritten document.
func (r *runner) copyTable(ctx context.Context, identifier TableIdentifier, fullLevels []string, metadata *Metadata, result TableResult) TableResult {
	sourceRoot := metadata.Location()
	if sourceRoot == "" {
		return failed(result, "the metadata document carries no location to copy")
	}
	targetUUID := copyTableUUID(result.MetadataLocation, fullLevels[2:], identifier.Name)
	targetRoot := joinLocation(joinLocation(r.opts.StorageNamespace, "_iceberg"), targetUUID)

	plan, err := buildCopyPlan(ctx, r.opts.FileReader, metadata, sourceRoot, targetRoot, targetUUID)
	if err != nil {
		return failed(result, err.Error())
	}
	if err := uploadCopyPlan(ctx, r.opts.Uploader, r.opts.FileReader,
		r.opts.Repository, r.opts.Branch, r.opts.StorageNamespace, plan); err != nil {
		return failed(result, err.Error())
	}
	result.Files = len(plan.files)
	result.MetadataLocation = plan.metadataLocation

	if err := r.opts.Target.RegisterTable(ctx, fullLevels, identifier.Name, plan.metadataLocation); err != nil {
		return failed(result, fmt.Sprintf("register: %s", err))
	}
	result.Status = StatusImported
	return result
}

// failed records a per-table failure.
func failed(result TableResult, detail string) TableResult {
	result.Status = StatusFailed
	result.Detail = detail
	return result
}

// dottedIdentifier renders repository, branch, namespace levels and a table name as one dotted
// identifier.
func dottedIdentifier(levels []string) string {
	return strings.Join(levels, ".")
}
