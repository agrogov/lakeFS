// service.go is the business layer of the datasets control plane (see DATASETS_DESIGN.md and
// the package comment in doc.go): it owns the dataset lifecycle (create, get, list, delete),
// the validate dry run and the consumer access grants of §9.2. It sits between the HTTP
// handlers (handlers.go) and the persistence / versioning layers:
//
//	handlers.go ──► service.go ──► store.go        (KV records, §5.3)
//	                        ├──► resolve.go       (ref resolution + source rules, §6.2 step 2)
//	                        ├──► materialize.go   (the publish primitive, §6.2 steps 6-10)
//	                        ├──► pins.go          (GC pins, §8.2)
//	                        └──► authz.go         (authorization + policies, §9)
//
// Clean-room note (§2): written from DATASETS_DESIGN.md and the public lakeFS documentation
// only. No code, SQL, protobuf or UI asset from lakeFS Enterprise, and nothing from upstream
// lakeFS >= 1.87, was used.
package datasets

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/permissions"
)

// Pagination defaults of GET /datasets (§10.1). They mirror the core list endpoints: 100
// values per page unless the client asks for something else, and never more than 1000.
const (
	DefaultListAmount = 100
	MaxListAmount     = 1000
)

// Service-level errors. The HTTP layer maps them to the status codes of §10.4; see
// writeServiceError in handlers.go.
var (
	// ErrDatasetNotFound reports an unknown dataset (404).
	ErrDatasetNotFound = errors.New("dataset not found")

	// ErrNameTaken reports a name already used by a dataset or by a repository (409). A
	// dataset is a repository (§4.2), so both share one namespace.
	ErrNameTaken = errors.New("name is already taken")

	// ErrDatasetConflict reports a lost optimistic-concurrency race: the dataset record
	// changed between the read and the conditional write (409, §5.4).
	ErrDatasetConflict = errors.New("dataset changed concurrently")

	// ErrUnsupported reports a feature this build does not implement (501): a `table` or
	// `namespace` item (§1.2, §10.4).
	ErrUnsupported = errors.New("unsupported dataset feature")

	// ErrIncompleteService reports a service built without one of its required dependencies.
	// It is a wiring error, never a request error.
	ErrIncompleteService = errors.New("datasets: incomplete service")
)

// Service is the datasets control plane. One instance serves every request of a process; it
// holds no per-request state.
//
// # Authorization model (do not "fix" this)
//
// The service writes the dataset repository through the catalog directly, with
// graveler.WithForce(true), and it never routes those writes through the caller's
// permissions. That is deliberate (DATASETS_DESIGN.md §4.6): the dataset repository is
// read-only and protected by an immutability deny policy (§9.4), so a write that went
// through Authorize would be denied to everyone, including the service. Every control-plane
// method therefore authorizes the caller explicitly and first (CheckDataset / CheckSources),
// and only then performs its writes with force. A refactor that "fixes" the service by
// authorizing its own repository writes would break every publish; a refactor that removes
// the explicit checks would expose the control plane. Keep both.
type Service struct {
	cfg          *Config
	store        *Store
	catalog      *catalog.Catalog
	authService  auth.Service
	authorizer   *Authorizer
	resolver     *Resolver
	materializer *Materializer
	pins         *Pins

	// now and newULID are replaceable in tests; production uses the wall clock and
	// crypto/rand.
	now     func() time.Time
	newULID func(time.Time) (string, error)
}

// NewService builds the control-plane service. It requires the datasets configuration, the
// KV store that holds the control-plane records (§5.3), the catalog and an auth service:
// without an auth service the control plane cannot authorize anything, and it must never
// fall through to "allowed" (§4.6).
func NewService(cfg *Config, kvs kv.Store, cat *catalog.Catalog, authService auth.Service) (*Service, error) {
	switch {
	case cfg == nil:
		return nil, fmt.Errorf("%w: configuration", ErrIncompleteService)
	case kvs == nil:
		return nil, fmt.Errorf("%w: KV store", ErrIncompleteService)
	case cat == nil:
		return nil, fmt.Errorf("%w: catalog", ErrIncompleteService)
	case authService == nil:
		return nil, ErrNoAuthorizer
	}
	store := NewStore(kvs)
	return &Service{
		cfg:          cfg,
		store:        store,
		catalog:      cat,
		authService:  authService,
		authorizer:   NewAuthorizer(authService),
		resolver:     &Resolver{Catalog: cat, StorageID: graveler.StorageID(cfg.StorageID)},
		materializer: &Materializer{Catalog: cat},
		pins:         NewPins(store, cat),
		now:          time.Now,
		newULID:      newULID,
	}, nil
}

// Config returns the configuration the service was built with, for GET /datasets/config.
func (s *Service) Config() *Config { return s.cfg }

// CreateRequest is the input of Service.Create: the §5.1 envelope without its name (which
// travels separately, because it is also the URL/record key).
type CreateRequest struct {
	Name       string
	Definition Definition
	// Publish requests version v1 as part of the create (§10.1). Without it the dataset is
	// created empty: repository and record only, no version.
	Publish bool
	// Message is the v1 commit message when Publish is set.
	Message string
}

// DatasetView is one dataset as the API returns it: the head record plus the record of the
// latest version, when one was published (§10.2 Dataset).
type DatasetView struct {
	Record  *DatasetRecord
	Version *VersionRecord
}

// Create creates the dataset repository and its control-plane record, and publishes version
// v1 when req.Publish is set (§6.1, §6.2, §10.1).
//
// The dataset is materialised as a read-only repository with the dataset's name and the
// default branch "latest" (§4.2, §6.1); its storage namespace is
// <datasets.storage_namespace_prefix>/<name>-<ulid>, so deleting a dataset frees its name
// even if the old storage namespace still holds objects. Repository metadata marks the
// repository as a dataset (lakefs.dataset=true, lakefs.dataset.format=1) and the per-dataset
// immutability deny policy of §9.4 is attached before any version is published.
func (s *Service) Create(ctx context.Context, caller Caller, req CreateRequest) (*DatasetView, error) {
	// §9.1: creating a dataset needs datasets:CreateDataset on the (future) dataset ARN.
	if err := s.authorizer.CheckDataset(ctx, caller, ActionCreateDataset, req.Name); err != nil {
		return nil, err
	}
	if problems := ValidateName(req.Name); len(problems) > 0 {
		return nil, &ValidationError{Problems: problems}
	}

	// §5.2 rules 1-3 and 6 are checked for every create; the catalog-dependent rules are
	// only needed when a definition is actually attached to a version.
	def := req.Definition
	if err := s.validateDefinition(ctx, &def); err != nil {
		return nil, err
	}

	var resolved *Definition
	if req.Publish {
		var err error
		resolved, err = s.resolver.Resolve(ctx, &def)
		if err != nil {
			return nil, err
		}
		// §9.3: the creator must be able to read every source they attach.
		if err := s.authorizer.CheckSources(ctx, caller, resolved.Data); err != nil {
			return nil, err
		}
		// §5.2 rule 5, before anything is created: an object item whose key is not at
		// its commit, or an empty prefix item, must fail the create instead of leaving
		// a half-created dataset behind. The same walk estimates the version's
		// footprint, which is what the publish of §6.5 is sized against.
		if _, err := s.sourceStats(ctx, resolved); err != nil {
			return nil, err
		}
	}

	// §10.4: a name taken by a repository or by another dataset is a 409.
	if err := s.checkNameAvailable(ctx, req.Name); err != nil {
		return nil, err
	}

	now := s.now()
	ns, err := s.storageNamespace(req.Name, now)
	if err != nil {
		return nil, err
	}
	rec := &DatasetRecord{
		Name:        req.Name,
		Description: def.Description,
		CreatedBy:   caller.Username,
		CreatedAt:   now,
		State:       DatasetStateCreating,
		StorageID:   s.cfg.StorageID,
		StorageNS:   ns,
		UpdatedAt:   now,
	}
	if err := s.store.CreateDataset(ctx, rec); err != nil {
		if errors.Is(err, kv.ErrPredicateFailed) {
			return nil, fmt.Errorf("%w: dataset %q already exists", ErrNameTaken, req.Name)
		}
		return nil, err
	}

	// §6.1: the dataset repository is read-only and defaults to the "latest" branch.
	if _, err := s.catalog.CreateRepository(ctx, req.Name, s.cfg.StorageID, ns, DefaultBranch, true); err != nil {
		_ = s.store.DeleteDataset(ctx, req.Name, rec)
		if errors.Is(err, graveler.ErrNotUnique) || errors.Is(err, kv.ErrPredicateFailed) {
			return nil, fmt.Errorf("%w: repository %q already exists", ErrNameTaken, req.Name)
		}
		return nil, fmt.Errorf("datasets: create repository %q: %w", req.Name, err)
	}
	metadata := graveler.RepositoryMetadata{
		RepositoryMetadataIsDatasetKey: "true",
		RepositoryMetadataFormatKey:    strconv.Itoa(FormatVersion),
	}
	if err := s.catalog.UpdateRepositoryMetadata(ctx, req.Name, metadata); err != nil {
		return nil, fmt.Errorf("datasets: mark repository %q as a dataset: %w", req.Name, err)
	}

	// §9.4: protect the repository before the first write, so no window exists in which a
	// guarded principal could write to it without the deny policy.
	if err := s.ensureImmutabilityGuard(ctx, req.Name); err != nil {
		return nil, err
	}

	current := rec
	var version *VersionRecord
	if req.Publish {
		version, err = s.publish(ctx, current, *resolved, req.Message, caller.Username)
		if err != nil {
			return nil, err
		}
	}

	active := *current
	active.State = DatasetStateActive
	active.UpdatedAt = s.now()
	if version != nil {
		active.LatestVersion = version.Version
	}
	if err := s.store.PutDatasetIf(ctx, &active, current); err != nil {
		if errors.Is(err, kv.ErrPredicateFailed) {
			return nil, fmt.Errorf("%w: dataset %q changed concurrently", ErrDatasetConflict, req.Name)
		}
		return nil, err
	}

	// §9.5 note: the aggregate read policy lists every dataset repository and must follow
	// create/delete.
	if err := s.ensureAllReadPolicy(ctx); err != nil {
		return nil, err
	}
	return &DatasetView{Record: &active, Version: version}, nil
}

// Get returns a dataset and the record of its latest version (§10.1). A dataset without a
// published version (created without publish) is returned with a nil Version.
func (s *Service) Get(ctx context.Context, caller Caller, name string) (*DatasetView, error) {
	if err := s.authorizer.CheckDataset(ctx, caller, ActionReadDataset, name); err != nil {
		return nil, err
	}
	rec, err := s.store.GetDataset(ctx, name)
	if errors.Is(err, kv.ErrNotFound) {
		return nil, fmt.Errorf("%w: %q", ErrDatasetNotFound, name)
	}
	if err != nil {
		return nil, err
	}
	view := &DatasetView{Record: rec}
	if rec.LatestVersion > 0 {
		version, err := s.store.GetVersion(ctx, name, rec.LatestVersion)
		switch {
		case err == nil:
			view.Version = version
		case errors.Is(err, kv.ErrNotFound):
			// The record and the version records disagree. §5.4 makes the dataset
			// repository tag authoritative and the reconciler rebuilds the record; the
			// read path stays available in the meantime, without a definition.
		default:
			return nil, err
		}
	}
	return view, nil
}

// ListRequest is the input of Service.List: the filters and pagination of GET /datasets
// (§10.1). Metadata filters match the metadata of the latest version.
type ListRequest struct {
	Prefix   string
	Search   string
	Metadata map[string]string
	After    string
	Amount   int
}

// ListResult is one page of datasets. NextOffset is the cursor for the next page and
// MaxPerPage is the effective page size the service applied (§10.2 Pagination).
type ListResult struct {
	Datasets   []*DatasetView
	HasMore    bool
	NextOffset string
	MaxPerPage int
}

// List returns the datasets the caller may read (§10.1, §9.1).
//
// Every result needs datasets:ReadDataset in addition to the datasets:ListDatasets check
// that gates the endpoint: listing is not a way to discover datasets one cannot read, so
// datasets without that permission are filtered out instead of failing the request.
func (s *Service) List(ctx context.Context, caller Caller, req ListRequest) (*ListResult, error) {
	listNode := permissions.Node{
		Permission: permissions.Permission{
			Action:   ActionListDatasets,
			Resource: permissions.All,
		},
	}
	if err := s.authorizer.Check(ctx, caller, listNode, ""); err != nil {
		return nil, err
	}

	records, err := s.store.ListDatasets(ctx)
	if err != nil {
		return nil, err
	}

	query := strings.ToLower(strings.TrimSpace(req.Search))
	filtered := make([]*DatasetView, 0, len(records))
	for _, rec := range records {
		if req.Prefix != "" && !strings.HasPrefix(rec.Name, req.Prefix) {
			continue
		}
		if query != "" && !matchesSearch(rec, query) {
			continue
		}
		view := &DatasetView{Record: rec}
		if rec.LatestVersion > 0 {
			version, err := s.store.GetVersion(ctx, rec.Name, rec.LatestVersion)
			switch {
			case err == nil:
				view.Version = version
			case errors.Is(err, kv.ErrNotFound):
				// See Get: the reconciler repairs the record; the dataset is listed
				// without metadata, so metadata filters do not match it.
			default:
				return nil, err
			}
		}
		if !metadataMatches(view, req.Metadata) {
			continue
		}
		if err := s.authorizer.CheckDataset(ctx, caller, ActionReadDataset, rec.Name); err != nil {
			var denied *PermissionError
			if errors.As(err, &denied) {
				continue
			}
			return nil, err
		}
		filtered = append(filtered, view)
	}

	amount := req.Amount
	switch {
	case amount < 0: // -1 means "everything"
		amount = len(filtered)
	case amount == 0:
		amount = DefaultListAmount
	case amount > MaxListAmount:
		amount = MaxListAmount
	}

	// filtered preserves the name order of store.ListDatasets, so a cursor is a name.
	start := sort.Search(len(filtered), func(i int) bool {
		return filtered[i].Record.Name > req.After
	})
	page := filtered[start:]
	hasMore := len(page) > amount
	if hasMore {
		page = page[:amount]
	}
	next := ""
	if hasMore && len(page) > 0 {
		next = page[len(page)-1].Record.Name
	}
	return &ListResult{Datasets: page, HasMore: hasMore, NextOffset: next, MaxPerPage: amount}, nil
}

// Delete removes a dataset: the repository, the pins of every version, the policies and
// grants, the proposals, the version records and the head record (§6.6).
//
// Every step tolerates "not found", so a delete that was already in progress can simply be
// retried and the reconciler can finish a crashed one (§6.6, §12.3): the record is moved to
// state "deleting" first, which is what makes the retry safe.
func (s *Service) Delete(ctx context.Context, caller Caller, name string) error {
	if err := s.authorizer.CheckDataset(ctx, caller, ActionDeleteDataset, name); err != nil {
		return err
	}
	rec, err := s.store.GetDataset(ctx, name)
	if errors.Is(err, kv.ErrNotFound) {
		return fmt.Errorf("%w: %q", ErrDatasetNotFound, name)
	}
	if err != nil {
		return err
	}
	// Read the version records before deleting anything: the control-plane records are
	// removed one by one after the repository is gone (§6.6 step 4).
	versions, err := s.store.ListVersions(ctx, name)
	if err != nil {
		return err
	}

	deleting := *rec
	deleting.State = DatasetStateDeleting
	deleting.UpdatedAt = s.now()
	if err := s.store.PutDatasetIf(ctx, &deleting, rec); err != nil {
		if errors.Is(err, kv.ErrPredicateFailed) {
			return fmt.Errorf("%w: dataset %q changed concurrently", ErrDatasetConflict, name)
		}
		return err
	}
	current := &deleting

	// §6.6 step 2: delete the dataset repository. Object data is not touched; only the
	// service's own _lakefs_dataset/ objects remain in the (now unused) namespace.
	// A genuine failure keeps the record in "deleting" so the reconciler retries instead
	// of leaving a repository the control plane no longer knows about.
	if err := s.catalog.DeleteRepository(ctx, name, graveler.WithForce(true)); err != nil && !isNotFound(err) {
		return fmt.Errorf("datasets: delete repository %q: %w", name, err)
	}

	// §6.6 step 3: release the GC pins of every version. Releasing works from the pin
	// records, so it also covers a version whose definition is stored out of line (§5.3).
	var errs []error
	if err := s.pins.ReleaseDataset(ctx, name); err != nil {
		errs = append(errs, err)
	}
	// §6.6 step 4: remove policies and grants, then the control-plane records.
	errs = append(errs, s.dropAccessGrants(ctx, name)...)
	errs = append(errs, s.dropImmutabilityGuard(ctx, name)...)
	if proposals, err := s.store.ListProposals(ctx, name); err != nil {
		errs = append(errs, err)
	} else {
		for _, proposal := range proposals {
			if err := s.store.DeleteProposal(ctx, name, proposal.ID); err != nil && !isNotFound(err) {
				errs = append(errs, err)
			}
		}
	}
	for _, version := range versions {
		if err := s.store.DeleteVersion(ctx, name, version.Version); err != nil && !isNotFound(err) {
			errs = append(errs, err)
		}
	}
	if err := s.store.DeleteDataset(ctx, name, current); err != nil && !isNotFound(err) {
		if errors.Is(err, kv.ErrPredicateFailed) {
			errs = append(errs, fmt.Errorf("%w: dataset %q changed concurrently", ErrDatasetConflict, name))
		} else {
			errs = append(errs, err)
		}
	}

	// §9.5 note: keep the aggregate read policy in step with the datasets that exist.
	if err := s.ensureAllReadPolicy(ctx); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// ValidateResult is the answer of the validate dry run: the resolved definition and the
// stats estimate of §10.1.
type ValidateResult struct {
	Definition Definition
	Stats      ItemStats
}

// Validate is the dry run of §10.1: it normalises the definition, resolves branch/tag refs
// to commits, validates it against the metadata keys, checks that the caller may read every
// source and estimates the version's footprint. It performs no writes.
func (s *Service) Validate(ctx context.Context, caller Caller, name string, def Definition) (*ValidateResult, error) {
	if err := s.authorizer.CheckDataset(ctx, caller, ActionReadDataset, name); err != nil {
		return nil, err
	}
	if problems := ValidateName(name); len(problems) > 0 {
		return nil, &ValidationError{Problems: problems}
	}
	if err := s.validateDefinition(ctx, &def); err != nil {
		return nil, err
	}
	resolved, err := s.resolver.Resolve(ctx, &def)
	if err != nil {
		return nil, err
	}
	if err := s.authorizer.CheckSources(ctx, caller, resolved.Data); err != nil {
		return nil, err
	}
	stats, err := s.sourceStats(ctx, resolved)
	if err != nil {
		return nil, err
	}
	return &ValidateResult{Definition: *resolved, Stats: stats}, nil
}

// --- access grants (§9.2) ---

// ListAccess returns the grants of a dataset (§10.1, §10.4 404 for an unknown dataset).
func (s *Service) ListAccess(ctx context.Context, caller Caller, dataset string) ([]*GrantRecord, error) {
	if err := s.authorizer.CheckDataset(ctx, caller, ActionManageAccess, dataset); err != nil {
		return nil, err
	}
	if _, err := s.store.GetDataset(ctx, dataset); err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			return nil, fmt.Errorf("%w: %q", ErrDatasetNotFound, dataset)
		}
		return nil, err
	}
	return s.store.ListGrants(ctx, dataset)
}

// GrantAccess gives a user or a group read access to a dataset (§9.2): it creates the
// managed policy dataset-<name>-read when it does not exist yet, attaches it to the
// principal and records the grant for GET /datasets/{ds}/access.
func (s *Service) GrantAccess(ctx context.Context, caller Caller, dataset, principalType, principalID string) (*GrantRecord, error) {
	if err := s.authorizer.CheckDataset(ctx, caller, ActionManageAccess, dataset); err != nil {
		return nil, err
	}
	if err := validatePrincipal(principalType, principalID); err != nil {
		return nil, err
	}
	if err := s.requireDataset(ctx, dataset); err != nil {
		return nil, err
	}
	if err := s.requirePrincipal(ctx, principalType, principalID); err != nil {
		return nil, err
	}
	if err := s.ensureConsumerPolicy(ctx, dataset); err != nil {
		return nil, err
	}
	policyName := ConsumerReadPolicyName(dataset)
	switch principalType {
	case PrincipalTypeUser:
		if err := s.authService.AttachPolicyToUser(ctx, policyName, principalID); err != nil && !errors.Is(err, auth.ErrAlreadyExists) {
			return nil, fmt.Errorf("datasets: attach %s to user %q: %w", policyName, principalID, err)
		}
	case PrincipalTypeGroup:
		if err := s.authService.AttachPolicyToGroup(ctx, policyName, principalID); err != nil && !errors.Is(err, auth.ErrAlreadyExists) {
			return nil, fmt.Errorf("datasets: attach %s to group %q: %w", policyName, principalID, err)
		}
	}

	grant := &GrantRecord{
		Dataset:       dataset,
		PrincipalType: principalType,
		PrincipalID:   principalID,
		PolicyName:    policyName,
		GrantedBy:     caller.Username,
		GrantedAt:     s.now(),
	}
	if err := s.store.PutGrant(ctx, grant); err != nil {
		return nil, err
	}
	return grant, nil
}

// RevokeAccess removes a grant (§9.2): the policy is detached from the principal, the grant
// record is deleted and, when no grant is left, the managed policy itself is removed.
func (s *Service) RevokeAccess(ctx context.Context, caller Caller, dataset, principalType, principalID string) error {
	if err := s.authorizer.CheckDataset(ctx, caller, ActionManageAccess, dataset); err != nil {
		return err
	}
	if err := validatePrincipal(principalType, principalID); err != nil {
		return err
	}
	if _, err := s.store.GetGrant(ctx, dataset, principalType, principalID); err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			return fmt.Errorf("%w: no grant for %s %q on dataset %q", ErrDatasetNotFound, principalType, principalID, dataset)
		}
		return err
	}

	policyName := ConsumerReadPolicyName(dataset)
	var detachErr error
	switch principalType {
	case PrincipalTypeUser:
		detachErr = s.authService.DetachPolicyFromUser(ctx, policyName, principalID)
	case PrincipalTypeGroup:
		detachErr = s.authService.DetachPolicyFromGroup(ctx, policyName, principalID)
	}
	if detachErr != nil && !errors.Is(detachErr, auth.ErrNotFound) {
		return fmt.Errorf("datasets: detach %s from %s %q: %w", policyName, principalType, principalID, detachErr)
	}
	if err := s.store.DeleteGrant(ctx, dataset, principalType, principalID); err != nil && !isNotFound(err) {
		return err
	}

	remaining, err := s.store.ListGrants(ctx, dataset)
	if err != nil {
		return err
	}
	if len(remaining) == 0 {
		if err := s.authService.DeletePolicy(ctx, policyName); err != nil && !errors.Is(err, auth.ErrNotFound) {
			return fmt.Errorf("datasets: delete policy %s: %w", policyName, err)
		}
	}
	return nil
}

// --- internals ---

// validateDefinition runs the structural validation of §5.2 with the metadata key rules of
// §5.5 and turns the "not supported" problems of reserved item types into ErrUnsupported,
// which the API answers with 501 (§10.4).
func (s *Service) validateDefinition(ctx context.Context, def *Definition) error {
	keys, err := s.store.ListMetadataKeys(ctx)
	if err != nil {
		return err
	}
	return unsupportedCheck(Validate(def, s.cfg.ValidateOptions(keys)))
}

// unsupportedCheck maps a ValidationError that reports a reserved item type to
// ErrUnsupported (501); every other validation result is returned unchanged.
func unsupportedCheck(err error) error {
	var validation *ValidationError
	if !errors.As(err, &validation) {
		return err
	}
	for _, problem := range validation.Problems {
		if strings.Contains(problem.Message, "(501)") {
			return fmt.Errorf("%w: %s", ErrUnsupported, validation.Error())
		}
	}
	return err
}

// storageNamespace builds the §6.1 storage namespace of a new dataset:
// <datasets.storage_namespace_prefix>/<name>-<ulid>. The ULID makes a re-created dataset use
// a fresh namespace, so the "namespace already in use" check of the blockstore cannot
// reject a legitimate re-create after a delete.
func (s *Service) storageNamespace(name string, now time.Time) (string, error) {
	id, err := s.newULID(now)
	if err != nil {
		return "", fmt.Errorf("datasets: generate storage namespace id: %w", err)
	}
	return strings.TrimSuffix(s.cfg.StorageNamespacePrefix, "/") + "/" + name + "-" + id, nil
}

// checkNameAvailable rejects a name used by a dataset or by any repository (§10.4 409).
func (s *Service) checkNameAvailable(ctx context.Context, name string) error {
	if _, err := s.store.GetDataset(ctx, name); err == nil {
		return fmt.Errorf("%w: dataset %q already exists", ErrNameTaken, name)
	} else if !errors.Is(err, kv.ErrNotFound) {
		return err
	}
	if _, err := s.catalog.GetRepository(ctx, name); err == nil {
		return fmt.Errorf("%w: repository %q already exists", ErrNameTaken, name)
	} else if !isNotFound(err) {
		return err
	}
	return nil
}

// publish materialises one version of the dataset and writes its VersionRecord (§6.2). It
// is called only when the dataset record already exists and after resolve/validate/authorize:
// the caller passes the resolved definition.
//
// Pins are taken before any data is written (§6.2 step 5, pins.go): a crash can then only
// over-retain source data, never under-retain it (§14). A failed materialisation releases the
// reference this attempt took.
func (s *Service) publish(ctx context.Context, rec *DatasetRecord, def Definition, message, publisher string) (*VersionRecord, error) {
	version := rec.LatestVersion + 1
	// The previous version's record is what the no-op check of §6.2 step 8 compares
	// against; v1 has none. A record whose definition lives behind a DefinitionRef still
	// contributes its metarange ID, so a re-publish that changes no data is rejected.
	previous, err := s.previousVersion(ctx, rec)
	if err != nil {
		return nil, err
	}

	// §6.2 step 5: the sources are pinned before any data is written.
	ref := PinReference(rec.Name, version)
	if _, err := s.pins.Acquire(ctx, def, ref); err != nil {
		return nil, err
	}

	req := PublishRequest{
		Dataset:    rec.Name,
		Version:    version,
		Definition: def,
		Message:    message,
		Publisher:  publisher,
		Previous:   previous,
	}

	// §5.2 rule 6 is enforced on every publish, not only on create, so the materialiser
	// is built with the metadata key rules that are in force right now (§5.5).
	materializer, err := s.materializerFor(ctx)
	if err != nil {
		return nil, errors.Join(err, s.pins.Release(ctx, def, ref))
	}
	result, err := materializer.Publish(ctx, req)
	if err != nil {
		if releaseErr := s.pins.Release(ctx, def, ref); releaseErr != nil {
			err = errors.Join(err, releaseErr)
		}
		if errors.Is(err, ErrNoChanges) {
			return nil, fmt.Errorf("%w: dataset %q version %d: %s", ErrDatasetConflict, rec.Name, version, ErrNoChanges)
		}
		return nil, err
	}

	stored, err := s.store.PutVersion(ctx, *result.VersionRecord(req, s.now()))
	if err != nil {
		if releaseErr := s.pins.Release(ctx, def, ref); releaseErr != nil {
			err = errors.Join(err, releaseErr)
		}
		return nil, err
	}
	return stored, nil
}

// previousVersion loads the record of the version a publish supersedes, for the no-op check
// of §6.2 step 8. A dataset with no version, or whose latest version record is missing (the
// record and the tag disagree, §5.4), publishes without that check.
func (s *Service) previousVersion(ctx context.Context, rec *DatasetRecord) (PreviousVersion, error) {
	if rec.LatestVersion <= 0 {
		return PreviousVersion{}, nil
	}
	previous, err := s.store.GetVersion(ctx, rec.Name, rec.LatestVersion)
	if errors.Is(err, kv.ErrNotFound) {
		return PreviousVersion{}, nil
	}
	if err != nil {
		return PreviousVersion{}, err
	}
	return PreviousVersionFromRecord(previous), nil
}

// materializerFor returns the publish primitive with the current metadata key rules (§5.5).
// The service owns one Materializer, but its ValidateOptions depend on KV state, so each
// publish derives a copy instead of caching options that a metadata key change would stale.
func (s *Service) materializerFor(ctx context.Context) (*Materializer, error) {
	keys, err := s.store.ListMetadataKeys(ctx)
	if err != nil {
		return nil, err
	}
	materializer := *s.materializer
	materializer.ValidateOptions = s.cfg.ValidateOptions(keys)
	return &materializer, nil
}

// sourceStats counts the objects and bytes every item contributes, in metarange order
// (§5.2 rule 5, §10.1 "stats estimate").
//
// The returned stats feed both the validate response and the VersionRecord of a create with
// publish. Counting is also the rule-5 existence check: an object item must exist at its
// pinned commit, and a prefix item must match at least one object unless
// datasets.allow_empty_items is set.
func (s *Service) sourceStats(ctx context.Context, def *Definition) (ItemStats, error) {
	stats := ItemStats{Items: make([]ItemStat, 0, len(def.Data))}
	for i := range def.Data {
		item := &def.Data[i]
		it, err := s.materializer.openItem(ctx, i, item)
		if err != nil {
			var sourceErr *SourceError
			if errors.As(err, &sourceErr) && errors.Is(sourceErr.Err, ErrSourceKeyNotFound) {
				return stats, &ValidationError{Problems: []Problem{{
					Path:    itemPath(i, "object.path"),
					Message: fmt.Sprintf("not found in %s at commit %s", item.Repository, item.Ref.ID),
				}}}
			}
			return stats, err
		}
		var objects, bytes int64
		for it.Next() {
			record := it.Value()
			entry, err := catalog.ValueToEntry(record.Value)
			if err != nil {
				it.Close()
				return stats, fmt.Errorf("%s: %w", itemPath(i, ""), err)
			}
			objects++
			bytes += entry.Size
		}
		err = it.Err()
		it.Close()
		if err != nil {
			return stats, fmt.Errorf("%s: %w", itemPath(i, ""), err)
		}
		if objects == 0 && item.Type == ItemTypePrefix && !s.cfg.AllowEmptyItems {
			return stats, &ValidationError{Problems: []Problem{{
				Path: itemPath(i, "prefix.path"),
				Message: fmt.Sprintf("matches no objects in %s at commit %s (datasets.allow_empty_items is false)",
					item.Repository, item.Ref.ID),
			}}}
		}
		stats.Items = append(stats.Items, ItemStat{Target: item.Target, Objects: objects, Bytes: bytes})
		stats.Objects += objects
		stats.Bytes += bytes
	}
	return stats, nil
}

// ensureAllReadPolicy rewrites the managed datasets-all-read policy from the current dataset
// list (§9.5 note). It is called after a create and after a delete.
func (s *Service) ensureAllReadPolicy(ctx context.Context) error {
	records, err := s.store.ListDatasets(ctx)
	if err != nil {
		return err
	}
	names := make([]string, len(records))
	for i, rec := range records {
		names[i] = rec.Name
	}
	if _, err := EnsureDatasetsAllReadPolicy(ctx, s.authService, names, s.now()); err != nil {
		return fmt.Errorf("datasets: sync %s: %w", DatasetsAllReadPolicyName, err)
	}
	return nil
}

// ensureImmutabilityGuard writes and attaches the per-dataset deny policy of §9.4. It runs
// before the first version is published, so a guarded principal can never force a write to
// the dataset repository.
func (s *Service) ensureImmutabilityGuard(ctx context.Context, dataset string) error {
	policy := ImmutabilityPolicy(dataset, s.now())
	if err := s.authService.WritePolicy(ctx, policy, false); err != nil && !errors.Is(err, auth.ErrAlreadyExists) {
		return fmt.Errorf("datasets: write policy %s: %w", policy.DisplayName, err)
	}
	for _, group := range ImmutabilityGuardGroups(s.cfg.ImmutableGuardGroups) {
		err := s.authService.AttachPolicyToGroup(ctx, policy.DisplayName, group)
		if err != nil && !errors.Is(err, auth.ErrAlreadyExists) && !errors.Is(err, auth.ErrNotFound) {
			// A group that does not exist is fine (§9.4: "Unknown names are harmless").
			return fmt.Errorf("datasets: attach %s to group %q: %w", policy.DisplayName, group, err)
		}
	}
	return nil
}

// dropImmutabilityGuard removes the deny policy of a deleted dataset from the guard groups
// and from the auth store.
func (s *Service) dropImmutabilityGuard(ctx context.Context, dataset string) []error {
	policyName := ImmutabilityPolicyName(dataset)
	var errs []error
	for _, group := range ImmutabilityGuardGroups(s.cfg.ImmutableGuardGroups) {
		if err := s.authService.DetachPolicyFromGroup(ctx, policyName, group); err != nil && !errors.Is(err, auth.ErrNotFound) {
			errs = append(errs, fmt.Errorf("datasets: detach %s from group %q: %w", policyName, group, err))
		}
	}
	if err := s.authService.DeletePolicy(ctx, policyName); err != nil && !errors.Is(err, auth.ErrNotFound) {
		errs = append(errs, fmt.Errorf("datasets: delete policy %s: %w", policyName, err))
	}
	return errs
}

// dropAccessGrants removes every consumer grant and the managed read policy of a deleted
// dataset (§6.6 step 4).
func (s *Service) dropAccessGrants(ctx context.Context, dataset string) []error {
	grants, err := s.store.ListGrants(ctx, dataset)
	if err != nil {
		return []error{err}
	}
	policyName := ConsumerReadPolicyName(dataset)
	var errs []error
	for _, grant := range grants {
		var detachErr error
		switch grant.PrincipalType {
		case PrincipalTypeUser:
			detachErr = s.authService.DetachPolicyFromUser(ctx, policyName, grant.PrincipalID)
		case PrincipalTypeGroup:
			detachErr = s.authService.DetachPolicyFromGroup(ctx, policyName, grant.PrincipalID)
		}
		if detachErr != nil && !errors.Is(detachErr, auth.ErrNotFound) {
			errs = append(errs, fmt.Errorf("datasets: detach %s from %s %q: %w",
				policyName, grant.PrincipalType, grant.PrincipalID, detachErr))
		}
		if err := s.store.DeleteGrant(ctx, dataset, grant.PrincipalType, grant.PrincipalID); err != nil && !isNotFound(err) {
			errs = append(errs, err)
		}
	}
	if err := s.authService.DeletePolicy(ctx, policyName); err != nil && !errors.Is(err, auth.ErrNotFound) {
		errs = append(errs, fmt.Errorf("datasets: delete policy %s: %w", policyName, err))
	}
	return errs
}

// ensureConsumerPolicy creates the managed read policy of a dataset when it is missing
// (§9.2). An existing policy is left untouched: it is machine-managed, but rewriting it on
// every grant would hide an administrator's edit instead of surfacing it.
func (s *Service) ensureConsumerPolicy(ctx context.Context, dataset string) error {
	name := ConsumerReadPolicyName(dataset)
	if _, err := s.authService.GetPolicy(ctx, name); err == nil {
		return nil
	} else if !errors.Is(err, auth.ErrNotFound) {
		return fmt.Errorf("datasets: get policy %s: %w", name, err)
	}
	err := s.authService.WritePolicy(ctx, ConsumerReadPolicy(dataset, s.now()), false)
	if err != nil && !errors.Is(err, auth.ErrAlreadyExists) {
		return fmt.Errorf("datasets: write policy %s: %w", name, err)
	}
	return nil
}

// requireDataset returns 404 (ErrDatasetNotFound) for an unknown dataset.
func (s *Service) requireDataset(ctx context.Context, dataset string) error {
	if _, err := s.store.GetDataset(ctx, dataset); err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			return fmt.Errorf("%w: %q", ErrDatasetNotFound, dataset)
		}
		return err
	}
	return nil
}

// requirePrincipal returns 404 (ErrDatasetNotFound, the control plane's only not-found
// mapping for a principal) when the grant target does not exist.
func (s *Service) requirePrincipal(ctx context.Context, principalType, principalID string) error {
	var err error
	switch principalType {
	case PrincipalTypeUser:
		_, err = s.authService.GetUser(ctx, principalID)
	case PrincipalTypeGroup:
		_, err = s.authService.GetGroup(ctx, principalID)
	}
	if err == nil {
		return nil
	}
	if errors.Is(err, auth.ErrNotFound) || errors.Is(err, kv.ErrNotFound) {
		return fmt.Errorf("%w: %s %q does not exist", ErrDatasetNotFound, principalType, principalID)
	}
	return err
}

// validatePrincipal checks the principal type and ID of an access grant (§9.2).
func validatePrincipal(principalType, principalID string) error {
	if err := checkPrincipalType(principalType); err != nil {
		return &ValidationError{Problems: []Problem{{
			Path:    "principal_type",
			Message: fmt.Sprintf("must be %q or %q, got %q", PrincipalTypeUser, PrincipalTypeGroup, principalType),
		}}}
	}
	if err := checkSegment("principal id", principalID); err != nil {
		return &ValidationError{Problems: []Problem{{
			Path:    "principal_id",
			Message: "must be a non-empty id without path separators",
		}}}
	}
	return nil
}

// matchesSearch reports whether the lower-cased query appears in the dataset name or
// description (§10.1 "case-insensitive search over dataset name and description").
func matchesSearch(rec *DatasetRecord, query string) bool {
	return strings.Contains(strings.ToLower(rec.Name), query) ||
		strings.Contains(strings.ToLower(rec.Description), query)
}

// metadataMatches applies the metadata.<key>=<value> filters of §10.1 to the metadata of the
// dataset's latest version. A dataset without a version matches no metadata filter.
func metadataMatches(view *DatasetView, want map[string]string) bool {
	if len(want) == 0 {
		return true
	}
	if view.Version == nil {
		return false
	}
	for key, value := range want {
		got, ok := view.Version.Definition.Metadata[key]
		if !ok || !metadataValueMatches(got, value) {
			return false
		}
	}
	return true
}

// metadataValueMatches compares one metadata value with the filter string. Multi-value keys
// (allow_multiple, §5.5) match when any element matches, and scalars compare their textual
// form so that metadata.<k>=1 matches the JSON number 1.
func metadataValueMatches(got any, want string) bool {
	switch value := got.(type) {
	case nil:
		return false
	case string:
		return value == want
	case []string:
		for _, element := range value {
			if element == want {
				return true
			}
		}
		return false
	case []any:
		for _, element := range value {
			if fmt.Sprint(element) == want {
				return true
			}
		}
		return false
	default:
		return fmt.Sprint(value) == want
	}
}

// isNotFound reports whether err means "this record/repository/branch does not exist", the
// only failure the delete sequence of §6.6 tolerates.
func isNotFound(err error) bool {
	return errors.Is(err, kv.ErrNotFound) ||
		errors.Is(err, graveler.ErrNotFound) ||
		errors.Is(err, graveler.ErrRepositoryNotFound) ||
		errors.Is(err, graveler.ErrBranchNotFound)
}

// newULID returns a 26-character ULID (Crockford base32) for the storage namespace suffix of
// §6.1: 48 bits of millisecond timestamp followed by 80 random bits, so the value is both
// sortable and collision-free without coordination.
func newULID(ts time.Time) (string, error) {
	const (
		ulidBytes      = 16
		timestampBytes = 6
		bitsPerByte    = 8
	)
	var id [ulidBytes]byte
	// Dates before the epoch (never a real publish time) clamp to zero rather than
	// wrapping into a huge timestamp.
	var millis uint64
	if ms := ts.UnixMilli(); ms > 0 {
		millis = uint64(ms) //nolint:gosec // checked non-negative just above
	}
	for i := 0; i < timestampBytes; i++ {
		id[i] = byte(millis >> (bitsPerByte * (timestampBytes - 1 - i)))
	}
	if _, err := rand.Read(id[timestampBytes:]); err != nil {
		return "", err
	}
	encoding := base32.NewEncoding("0123456789ABCDEFGHJKMNPQRSTVWXYZ").WithPadding(base32.NoPadding)
	return encoding.EncodeToString(id[:]), nil
}
