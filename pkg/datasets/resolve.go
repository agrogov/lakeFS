// Package datasets implements the lakeFS Datasets control plane described in
// DATASETS_DESIGN.md.
//
// resolve.go implements §6.2 step 2: rewriting every branch and tag ref of a
// definition into the commit ID it points at, so that the stored definition only
// ever contains `type: commit` (§5.2). It is also where §5.2 rule 4 lives, the
// only part of definition validation that needs the catalog: the source
// repository must exist, must not itself be a dataset (no nesting in the MVP) and
// must share the dataset's storage ID.
//
// §9.3 (the source *read* check) is deliberately not implemented here: it belongs
// to the authorization layer, Authorizer.CheckSources in authz.go, which runs as
// §6.2 step 3 and returns a *PermissionError (403). Resolve only reports source
// problems as *SourceError, whose HTTPStatusCode maps the storage-ID mismatch to
// 422 and everything else to 400 (§10.4), so the API can tell the two apart.
//
// Clean-room note (§2): written from DATASETS_DESIGN.md and the public lakeFS
// documentation only.
package datasets

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/graveler"
)

// Repository metadata keys the datasets service writes on a dataset repository
// (§6.1). They make a dataset repository recognisable as a dataset — for the UI
// and the repository listing (§9.7) — and they are how Resolve refuses to nest a
// dataset inside another one (§5.2 rule 4).
const (
	// RepositoryMetadataIsDatasetKey is set to "true" on every dataset repository.
	RepositoryMetadataIsDatasetKey = "lakefs.dataset"

	// RepositoryMetadataFormatKey records the dataset format version the repository
	// was created with (§6.1).
	RepositoryMetadataFormatKey = "lakefs.dataset.format"
)

// Typed source errors (§5.2 rule 4). Resolve wraps exactly one of them in a
// *SourceError, so a caller can classify the failure with errors.Is and pick the
// status code of §10.4: ErrStorageIDMismatch is 422 ("storage ID mismatch"),
// everything else is a 400 validation problem naming the offending item.
var (
	// ErrSourceRepositoryNotFound reports a source repository that does not exist.
	ErrSourceRepositoryNotFound = errors.New("source repository not found")

	// ErrSourceIsDataset reports a source repository that is itself a dataset.
	// Nesting datasets is out of scope for the MVP, because it would make the
	// dataset graph a DAG that GC pins and versioning would have to traverse.
	ErrSourceIsDataset = errors.New("source repository is a dataset; nested datasets are not supported")

	// ErrSourceRefNotFound reports a source ref (commit, branch or tag) that does
	// not exist in the source repository, or is not a ref at all.
	ErrSourceRefNotFound = errors.New("source ref not found")

	// ErrStorageIDMismatch reports a source repository living on a different
	// blockstore than the dataset. It is a 422 rather than a 400 because it is a
	// deployment problem, not a document problem: dataset entries keep full
	// physical addresses into the source namespace (§6.3), which is only readable
	// through the dataset's own blockstore.
	ErrStorageIDMismatch = errors.New("source repository does not share the dataset storage ID")
)

// SourceError is the typed error Resolve returns for a definition whose sources
// cannot be used. Err is one of ErrSourceRepositoryNotFound, ErrSourceIsDataset,
// ErrSourceRefNotFound or ErrStorageIDMismatch, and Item is the index of the
// offending item in the (normalised, target-sorted) definition, so the API can
// report it as `{"path": "data[2]", ...}` (§10.4).
type SourceError struct {
	Item       int
	Repository string
	Ref        SourceRef
	Err        error
}

// Error implements error.
func (e *SourceError) Error() string {
	var b strings.Builder
	b.WriteString(itemPath(e.Item, ""))
	fmt.Fprintf(&b, ": repository %q", e.Repository)
	if e.Ref.ID != "" {
		fmt.Fprintf(&b, " ref %s %q", e.Ref.Type, e.Ref.ID)
	}
	fmt.Fprintf(&b, ": %v", e.Err)
	return b.String()
}

// Unwrap exposes the wrapped sentinel to errors.Is.
func (e *SourceError) Unwrap() error { return e.Err }

// HTTPStatusCode is the status the control plane answers with when it returns err
// (§10.4): 422 for a storage-ID mismatch, which is a deployment problem, and 400
// for the other source problems, which are document problems. It mirrors the
// HTTPStatusCode of authz.go's *PermissionError, so the API can map any error the
// resolve/publish path returns without type-switching on the concrete error.
func (e *SourceError) HTTPStatusCode() int {
	if errors.Is(e.Err, ErrStorageIDMismatch) {
		return http.StatusUnprocessableEntity
	}
	return http.StatusBadRequest
}

// Resolver resolves the refs of a definition and checks its sources (§5.2 rule 4,
// §6.2 step 2). It has no state beyond its configuration, so it can be reused
// across publishes.
type Resolver struct {
	// Catalog is the lakeFS catalog. Required.
	Catalog *catalog.Catalog

	// StorageID is the blockstore the dataset repository lives on
	// (datasets.storage_id, §13). Every source repository must share it (§5.2 rule
	// 4), because materialisation stores the source objects' full addresses and
	// reads them back through this blockstore (§6.3). The empty StorageID is a real
	// value, not "any": it is the ID of the single blockstore of an OSS install.
	StorageID graveler.StorageID
}

// Resolve returns a copy of def whose data items all carry `type: commit` refs
// (§5.2, §6.2 step 2), after checking the sources of §5.2 rule 4:
//
//   - the source repository exists (ErrSourceRepositoryNotFound);
//   - it is not itself a dataset (ErrSourceIsDataset);
//   - it shares StorageID (ErrStorageIDMismatch);
//   - the item's ref resolves to a commit (ErrSourceRefNotFound).
//
// The input is not modified: the caller keeps the document it was given, which is
// what makes Resolve safe to call on a definition the service also stores for a
// proposal. The returned definition has the same item order and targets as the
// input, so item indices in a *SourceError are indices into the normalised
// definition.
//
// Rule 5 of §5.2 (the source keys exist, and a prefix item matches at least one
// object unless datasets.allow_empty_items) is not checked here: it belongs to the
// service, which owns the configuration, and, for object items, materialisation
// enforces it again because it cannot produce an entry for a key that is not there.
func (r *Resolver) Resolve(ctx context.Context, def *Definition) (*Definition, error) {
	if def == nil {
		return nil, &ValidationError{Problems: []Problem{{Message: "definition is missing"}}}
	}
	resolved := cloneDefinition(*def)

	// Both caches are per call: one resolve handles at most datasets.max_items
	// items, many of which usually repeat a repository or a ref.
	checkedRepositories := make(map[string]struct{}, len(resolved.Data))
	commits := make(map[string]graveler.CommitID, len(resolved.Data))

	for i := range resolved.Data {
		item := &resolved.Data[i]

		if _, ok := checkedRepositories[item.Repository]; !ok {
			if err := r.checkSourceRepository(ctx, i, *item); err != nil {
				return nil, err
			}
			checkedRepositories[item.Repository] = struct{}{}
		}

		key := item.Repository + "\x00" + item.Ref.ID
		commitID, ok := commits[key]
		if !ok {
			var err error
			commitID, err = r.resolveRef(ctx, i, *item)
			if err != nil {
				return nil, err
			}
			commits[key] = commitID
		}

		item.Ref = SourceRef{Type: RefTypeCommit, ID: commitID.String()}
	}
	return &resolved, nil
}

// checkSourceRepository applies the three per-repository rules of §5.2 rule 4.
func (r *Resolver) checkSourceRepository(ctx context.Context, i int, item DataItem) error {
	src, err := r.Catalog.GetRepository(ctx, item.Repository)
	if err != nil {
		if errors.Is(err, graveler.ErrNotFound) {
			return &SourceError{Item: i, Repository: item.Repository, Ref: item.Ref, Err: ErrSourceRepositoryNotFound}
		}
		return fmt.Errorf("datasets: get source repository %q: %w", item.Repository, err)
	}

	metadata, err := r.Catalog.GetRepositoryMetadata(ctx, item.Repository)
	if err != nil {
		return fmt.Errorf("datasets: get metadata of source repository %q: %w", item.Repository, err)
	}
	if metadata[RepositoryMetadataIsDatasetKey] == "true" {
		return &SourceError{Item: i, Repository: item.Repository, Ref: item.Ref, Err: ErrSourceIsDataset}
	}

	if graveler.StorageID(src.StorageID) != r.StorageID {
		return &SourceError{
			Item:       i,
			Repository: item.Repository,
			Ref:        item.Ref,
			Err: fmt.Errorf("%w: source %q, dataset %q",
				ErrStorageIDMismatch, src.StorageID, r.StorageID),
		}
	}
	return nil
}

// resolveRef turns one item's ref into a commit ID. A commit ref is verified to
// exist rather than trusted, because a stale commit ID is exactly as unpublishable
// as a stale branch name and the error is clearer if it is reported at resolve
// time instead of halfway through materialisation.
func (r *Resolver) resolveRef(ctx context.Context, i int, item DataItem) (graveler.CommitID, error) {
	commit, err := r.Catalog.GetCommit(ctx, item.Repository, item.Ref.ID)
	if err != nil {
		if errors.Is(err, graveler.ErrNotFound) {
			return "", &SourceError{Item: i, Repository: item.Repository, Ref: item.Ref, Err: ErrSourceRefNotFound}
		}
		return "", fmt.Errorf("datasets: resolve %s %q in %q: %w", item.Ref.Type, item.Ref.ID, item.Repository, err)
	}
	return graveler.CommitID(commit.Reference), nil
}

// cloneDefinition copies the parts of d that Resolve or Validate may rewrite: the
// item slice and the source descriptors Normalise edits in place. Items and their
// descriptors are copied by value, so validating the clone never edits the caller's
// document (which is what makes Resolve safe to call on a definition the service also
// stores for a proposal).
func cloneDefinition(d Definition) Definition {
	out := d
	if d.Data == nil {
		return out
	}
	out.Data = make([]DataItem, len(d.Data))
	copy(out.Data, d.Data)
	for i := range out.Data {
		if src := d.Data[i].Object; src != nil {
			object := *src
			out.Data[i].Object = &object
		}
		if src := d.Data[i].Prefix; src != nil {
			prefix := *src
			out.Data[i].Prefix = &prefix
		}
		if src := d.Data[i].Table; src != nil {
			table := *src
			out.Data[i].Table = &table
		}
		if src := d.Data[i].Namespace; src != nil {
			namespace := *src
			out.Data[i].Namespace = &namespace
		}
	}
	return out
}
