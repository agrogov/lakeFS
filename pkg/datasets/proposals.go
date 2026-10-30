// proposals.go implements dataset proposals - the dataset pull requests of
// DATASETS_DESIGN.md §5.6 - on top of the records of store.go and the primitives of the
// publish path:
//
//	proposals.go ──► store.go        (ProposalRecord at proposals/<ds>/<id>, §5.3)
//	              ├──► resolve.go    (ref resolution + source rules, §6.2 step 2)
//	              ├──► materialize.go (the publish path, untagged, for the preview branch)
//	              ├──► service.go     (PublishVersion for a merge, §6.2, §5.4)
//	              ├──► diff.go        (definition and data diff against latest, §10.3)
//	              └──► authz.go       (CreateProposal / PublishVersion / ReadDataset, §9.1)
//
// # Why proposals are KV records and not graveler pull requests (do not "simplify" this)
//
// A proposal is a ProposalRecord in the datasets KV partition, *not* a graveler pull
// request. §5.6 is explicit about the two reasons:
//
//   - History. A dataset version is one commit on `latest`, with the previous version as its
//     only parent (§6.2 step 9). A graveler pull request merges two refs, which produces a
//     merge commit - and a merge commit would make "vN is version N of a linear history"
//     false, break the "the tag is the source of truth" recovery of §5.4, and make the
//     previous-version comparison of the no-op check ambiguous.
//   - Permission. Merging a graveler pull request is a write to the target branch performed
//     through the caller's own permissions. The dataset repository is read-only and covered
//     by the immutability deny policy (§9.4), so the write must happen in the service with
//     graveler.WithForce(true) (§4.6) - and the reviewer who approves must not need write
//     access to `latest`, only datasets:PublishVersion (§9.1). Keeping the review state in
//     KV keeps that separation: the reviewer's authority is a datasets action, not fs:Write*.
//
// The preview branch exists only so a reviewer can browse the actual data; it is not the
// proposal, and deleting it loses nothing.
//
// Clean-room note (§2): written from DATASETS_DESIGN.md and the public lakeFS documentation
// only. No code, SQL, protobuf or UI asset from lakeFS Enterprise, and nothing from upstream
// lakeFS >= 1.87, was used.
package datasets

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
)

// Proposal errors. The HTTP layer maps them in writeServiceError (§10.4): not found is a
// 404, a proposal that is no longer open is a 409, and a stale base version is the 409 with
// latest_version in the body (RebaseRequiredError).
var (
	// ErrProposalNotFound reports an unknown proposal id of a known dataset (404).
	ErrProposalNotFound = errors.New("dataset proposal not found")

	// ErrProposalNotOpen reports an update, close or merge of a proposal that is already
	// closed or merged (409): the review is over, so the answer is a conflict rather than a
	// silent no-op.
	ErrProposalNotOpen = errors.New("dataset proposal is not open")

	// ErrProposalNotSynchronous reports a merge whose publish was accepted as an asynchronous
	// task instead of running in the request. MergeProposal always asks for a synchronous
	// publish (PublishVersionRequest.Sync), because the proposal must be closed in the same
	// request that publishes it, so this is a wiring bug rather than a client error.
	ErrProposalNotSynchronous = errors.New("proposal merge was not published synchronously")
)

// RebaseRequiredError is the 409 of §5.6 when a proposal's base_version is no longer the
// dataset's latest_version: the proposal has to be rebased (updated) before it can be
// merged. It wraps ErrDatasetConflict, the way VersionConflictError does, so the generic
// 409 mapping still applies; the API additionally puts latest_version in the body, so a
// client can show "rebased needed: latest is vN" without a second read.
type RebaseRequiredError struct {
	Dataset       string
	ProposalID    string
	BaseVersion   int
	LatestVersion int
}

// Error implements error. The message says "rebase required" because that is the answer the
// design prescribes for this case (§5.6), and it is what a client keys on.
func (e *RebaseRequiredError) Error() string {
	return fmt.Sprintf("%s: rebase required: proposal %s of dataset %s is based on version %d, latest is %d",
		ErrDatasetConflict, e.ProposalID, e.Dataset, e.BaseVersion, e.LatestVersion)
}

// Unwrap exposes ErrDatasetConflict to errors.Is, so the generic 409 mapping of
// writeServiceError applies.
func (e *RebaseRequiredError) Unwrap() error { return ErrDatasetConflict }

// latestVersion reports the version the 409 body carries (writeVersionConflict in
// handlers.go), the counterpart of VersionConflictError.latestVersion.
func (e *RebaseRequiredError) latestVersion() int { return e.LatestVersion }

// CreateProposalRequest is the body of POST /datasets/{ds}/proposals (§10.1): the definition
// to review plus the review's title and description.
type CreateProposalRequest struct {
	Definition  Definition
	Title       string
	Description string
}

// UpdateProposalRequest is the body of PATCH /datasets/{ds}/proposals/{id} (§10.1): a
// partial update of the title, description and definition, or `status: closed`. A nil
// member means "leave this field alone"; Status is "" or ProposalStatusClosed.
type UpdateProposalRequest struct {
	Title       *string
	Description *string
	Definition  *Definition
	Status      string
}

// MergeProposalRequest is the body of POST /datasets/{ds}/proposals/{id}/merge (§10.1). An
// empty Message gets a generated one.
type MergeProposalRequest struct {
	Message string
}

// ProposalView is one proposal as the API returns it: the record plus the diffs the detail
// response carries (§5.6, §10.1 "includes diff summary against latest"). The diffs are nil
// when they could not be computed - see proposalView - so a read never fails because a
// preview branch is gone.
type ProposalView struct {
	Record   *ProposalRecord
	Diff     *DefinitionDiff
	DataDiff *DataDiff
}

// ProposalListResult is one page of GET /datasets/{ds}/proposals, newest first.
type ProposalListResult struct {
	Proposals  []*ProposalRecord
	HasMore    bool
	NextOffset string
	MaxPerPage int
}

// ListProposals lists the proposals of a dataset newest first, optionally filtered by status
// (§10.1). The pagination cursor is the id of the last proposal of the previous page: ULIDs
// are sortable, so an id stays a valid cursor while new proposals are opened.
func (s *Service) ListProposals(ctx context.Context, caller Caller, dataset, status, after string, amount int) (*ProposalListResult, error) {
	if err := s.authorizer.CheckDataset(ctx, caller, ActionReadDataset, dataset); err != nil {
		return nil, err
	}
	if err := s.requireDataset(ctx, dataset); err != nil {
		return nil, err
	}
	switch status {
	case "", ProposalStatusOpen, ProposalStatusClosed, ProposalStatusMerged:
	default:
		return nil, &ValidationError{Problems: []Problem{{
			Path:    "status",
			Message: fmt.Sprintf("must be %q, %q or %q", ProposalStatusOpen, ProposalStatusClosed, ProposalStatusMerged),
		}}}
	}

	stored, err := s.store.ListProposals(ctx, dataset)
	if err != nil {
		return nil, err
	}

	// The cursor is applied to the unfiltered list, so a proposal that leaves the filter
	// while the client pages (an open one that was just closed) cannot invalidate the page.
	start := 0
	if after != "" {
		start = -1
		for i, proposal := range stored {
			if proposal.ID == after {
				start = i + 1
				break
			}
		}
		if start < 0 {
			return nil, &ValidationError{Problems: []Problem{{
				Path:    "after",
				Message: fmt.Sprintf("unknown proposal %q", after),
			}}}
		}
	}

	page := make([]*ProposalRecord, 0, len(stored))
	for _, proposal := range stored[start:] {
		if status != "" && proposal.Status != status {
			continue
		}
		page = append(page, proposal)
	}

	amount = effectiveAmount(amount, len(page))
	hasMore := len(page) > amount
	if hasMore {
		page = page[:amount]
	}
	next := ""
	if hasMore && len(page) > 0 {
		next = page[len(page)-1].ID
	}
	return &ProposalListResult{Proposals: page, HasMore: hasMore, NextOffset: next, MaxPerPage: amount}, nil
}

// CreateProposal opens a proposal against the dataset's current latest version (§5.6).
//
// The order mirrors the publish path of §6.2 steps 1-3, and for the same reason - a
// proposal must never exist without the data it promises:
//
//  1. normalise, validate and resolve the definition (§5.2, §6.2 steps 1-2);
//  2. run the attach-time source check for the *author* (§9.3);
//  3. materialise the definition on the hidden preview branch `_proposal-<id>`;
//  4. write the ProposalRecord with BaseVersion = the latest version it was made against.
//
// A failure in step 3 leaves no record behind; a failure in step 4 removes the preview
// branch again, so a proposal is either fully reviewable or absent.
func (s *Service) CreateProposal(ctx context.Context, caller Caller, dataset string, req CreateProposalRequest) (*ProposalView, error) {
	if err := s.authorizer.CheckDataset(ctx, caller, ActionCreateProposal, dataset); err != nil {
		return nil, err
	}
	if problems := ValidateName(dataset); len(problems) > 0 {
		return nil, &ValidationError{Problems: problems}
	}
	rec, err := s.loadDataset(ctx, dataset)
	if err != nil {
		return nil, err
	}

	def := req.Definition
	if err := s.validateDefinition(ctx, &def); err != nil {
		return nil, err
	}
	resolved, err := s.resolver.Resolve(ctx, &def)
	if err != nil {
		return nil, err
	}
	// §9.3: the author must be able to read every source they attach. The merger's check
	// happens again at merge time (MergeProposal), because the merger publishes under their
	// own authority.
	if err := s.authorizer.CheckSources(ctx, caller, resolved.Data); err != nil {
		return nil, err
	}
	// §5.2 rule 5, before anything is written: an object item whose key is not at its
	// commit, or an empty prefix item, must fail the proposal instead of materialising a
	// preview that silently lacks data.
	if _, err := s.sourceStats(ctx, resolved); err != nil {
		return nil, err
	}

	now := s.now()
	id, err := s.newULID(now)
	if err != nil {
		return nil, fmt.Errorf("datasets: generate proposal id: %w", err)
	}
	proposal := &ProposalRecord{
		ID:            id,
		Dataset:       dataset,
		BaseVersion:   rec.LatestVersion,
		Definition:    *resolved,
		Title:         req.Title,
		Description:   req.Description,
		Author:        caller.Username,
		Status:        ProposalStatusOpen,
		CreatedAt:     now,
		PreviewBranch: ProposalPreviewBranch(id),
	}

	if err := s.materializePreview(ctx, rec, proposal, caller.Username); err != nil {
		return nil, err
	}
	if err := s.store.PutProposal(ctx, proposal); err != nil {
		// The preview branch is useless without its record; removing it keeps the
		// repository free of branches no proposal points at (§5.6).
		if removeErr := s.removePreviewBranch(ctx, dataset, proposal.PreviewBranch); removeErr != nil {
			err = errors.Join(err, removeErr)
		}
		return nil, err
	}
	return s.proposalView(ctx, rec, proposal), nil
}

// GetProposal returns one proposal with its diffs against latest (§10.1).
func (s *Service) GetProposal(ctx context.Context, caller Caller, dataset, id string) (*ProposalView, error) {
	if err := s.authorizer.CheckDataset(ctx, caller, ActionReadDataset, dataset); err != nil {
		return nil, err
	}
	rec, err := s.loadDataset(ctx, dataset)
	if err != nil {
		return nil, err
	}
	proposal, err := s.loadProposal(ctx, dataset, id)
	if err != nil {
		return nil, err
	}
	return s.proposalView(ctx, rec, proposal), nil
}

// UpdateProposal applies a partial update of §10.1, or closes the proposal.
//
// Updating the definition re-resolves it (§5.2: branch and tag refs become commits),
// re-validates it against the metadata keys (§5.5), re-runs the §9.3 source check for the
// caller and re-materialises the preview branch. The proposal's BaseVersion moves to the
// dataset's current latest version: that is the "Update proposal" rebase of §5.6, which is
// explicit precisely so that a merge never rebases on its own.
//
// `status: closed` closes the proposal and deletes the preview branch with Force, tolerating
// "not found" (§5.6).
//
// Authorization (§10.1): the author needs datasets:CreateProposal on the dataset; any other
// principal is updating on the author's behalf and needs datasets:PublishVersion, the action
// a merge needs.
func (s *Service) UpdateProposal(ctx context.Context, caller Caller, dataset, id string, req UpdateProposalRequest) (*ProposalView, error) {
	rec, err := s.loadDataset(ctx, dataset)
	if err != nil {
		return nil, err
	}
	proposal, err := s.loadProposal(ctx, dataset, id)
	if err != nil {
		return nil, err
	}
	if err := s.authorizeProposalWrite(ctx, caller, dataset, proposal.Author); err != nil {
		return nil, err
	}
	if proposal.Status != ProposalStatusOpen {
		return nil, fmt.Errorf("%w: proposal %s of dataset %s is %s", ErrProposalNotOpen, id, dataset, proposal.Status)
	}
	switch req.Status {
	case "", ProposalStatusClosed:
	default:
		return nil, &ValidationError{Problems: []Problem{{
			Path:    "status",
			Message: fmt.Sprintf("must be %q", ProposalStatusClosed),
		}}}
	}

	updated := *proposal
	if req.Title != nil {
		updated.Title = *req.Title
	}
	if req.Description != nil {
		updated.Description = *req.Description
	}

	// Closing is terminal: it does not materialise anything, and the definition of a closed
	// proposal is read-only history (§5.6). A close that also carries a definition applies
	// the field updates above and ignores the definition, because a reviewer must not be
	// shown data that no preview branch ever held.
	if req.Status == ProposalStatusClosed {
		now := s.now()
		updated.Status = ProposalStatusClosed
		updated.ClosedAt = &now
		if err := s.store.PutProposal(ctx, &updated); err != nil {
			return nil, err
		}
		if err := s.removePreviewBranch(ctx, dataset, previewBranchOf(proposal)); err != nil {
			return nil, err
		}
		return s.proposalView(ctx, rec, &updated), nil
	}

	def := proposal.Definition
	if req.Definition != nil {
		def = *req.Definition
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
	if _, err := s.sourceStats(ctx, resolved); err != nil {
		return nil, err
	}

	updated.Definition = *resolved
	// §5.6: this is the rebase. The preview is materialised against the same version, so a
	// merge right after an update never has to rebase (and never silently picks newer data).
	updated.BaseVersion = rec.LatestVersion
	if err := s.materializePreview(ctx, rec, &updated, caller.Username); err != nil {
		return nil, err
	}
	if err := s.store.PutProposal(ctx, &updated); err != nil {
		return nil, err
	}
	return s.proposalView(ctx, rec, &updated), nil
}

// MergeProposal publishes a proposal's definition as the next version (§5.6, §10.1).
//
// Merging is a publish, so it goes through Service.PublishVersion and inherits every rule of
// that path: the §9.3 source check for the *merger* (who publishes under their own
// authority), the pins of §8.2, the CAS of §5.4 and the no-op check of §6.2 step 8. What is
// specific to a merge is the base version: the proposal must have been made against the
// current latest version, otherwise the author's review no longer describes what would be
// published and the answer is 409 "rebase required" (§5.6). The publish is forced
// synchronous - the response body is the new version - so the proposal record can be closed
// in the same request that publishes it.
//
// On success the proposal is marked merged with the version it produced, and the preview
// branch is deleted with Force, tolerating "not found" (§5.6).
func (s *Service) MergeProposal(ctx context.Context, caller Caller, dataset, id string, req MergeProposalRequest) (*VersionRecord, error) {
	if err := s.authorizer.CheckDataset(ctx, caller, ActionPublishVersion, dataset); err != nil {
		return nil, err
	}
	rec, err := s.loadDataset(ctx, dataset)
	if err != nil {
		return nil, err
	}
	proposal, err := s.loadProposal(ctx, dataset, id)
	if err != nil {
		return nil, err
	}
	if proposal.Status != ProposalStatusOpen {
		return nil, fmt.Errorf("%w: proposal %s of dataset %s is %s", ErrProposalNotOpen, id, dataset, proposal.Status)
	}
	// §5.6: merging requires base_version == latest_version.
	if proposal.BaseVersion != rec.LatestVersion {
		return nil, &RebaseRequiredError{
			Dataset:       dataset,
			ProposalID:    id,
			BaseVersion:   proposal.BaseVersion,
			LatestVersion: rec.LatestVersion,
		}
	}

	message := strings.TrimSpace(req.Message)
	if message == "" {
		message = mergeMessage(proposal)
	}
	base := proposal.BaseVersion
	outcome, err := s.PublishVersion(ctx, caller, dataset, PublishVersionRequest{
		Definition:      proposal.Definition,
		Message:         message,
		ExpectedVersion: &base,
		ProposalID:      proposal.ID,
		Sync:            true,
	})
	if err != nil {
		// A publish that lost the race between the check above and the CAS is the same
		// situation as a stale base: report it as "rebase required", not as a raw conflict,
		// so the client gets one answer for one problem (§5.6).
		var conflict *VersionConflictError
		if errors.As(err, &conflict) {
			return nil, &RebaseRequiredError{
				Dataset:       dataset,
				ProposalID:    id,
				BaseVersion:   proposal.BaseVersion,
				LatestVersion: conflict.LatestVersion,
			}
		}
		return nil, err
	}
	if outcome.Accepted() || outcome.Version == nil {
		// Unreachable: Sync keeps the publish in the request. Failing loudly here beats
		// closing a proposal for a version that does not exist yet.
		return nil, fmt.Errorf("%w: proposal %s of dataset %s", ErrProposalNotSynchronous, id, dataset)
	}
	version := outcome.Version

	now := s.now()
	merged := *proposal
	merged.Status = ProposalStatusMerged
	merged.ClosedAt = &now
	merged.MergedVersion = version.Version
	if err := s.store.PutProposal(ctx, &merged); err != nil {
		return nil, err
	}
	if err := s.removePreviewBranch(ctx, dataset, previewBranchOf(proposal)); err != nil {
		return nil, err
	}
	return version, nil
}

// --- internals ---

// loadDataset reads the head record of a dataset, mapping a missing record to the 404 of
// §10.4.
func (s *Service) loadDataset(ctx context.Context, dataset string) (*DatasetRecord, error) {
	rec, err := s.store.GetDataset(ctx, dataset)
	if errors.Is(err, kv.ErrNotFound) {
		return nil, fmt.Errorf("%w: %q", ErrDatasetNotFound, dataset)
	}
	if err != nil {
		return nil, err
	}
	return rec, nil
}

// loadProposal reads one proposal, mapping a missing record to a 404 that names both the
// dataset and the proposal (§10.4).
func (s *Service) loadProposal(ctx context.Context, dataset, id string) (*ProposalRecord, error) {
	proposal, err := s.store.GetProposal(ctx, dataset, id)
	if errors.Is(err, kv.ErrNotFound) {
		return nil, fmt.Errorf("%w: proposal %q of dataset %q", ErrProposalNotFound, id, dataset)
	}
	if err != nil {
		return nil, err
	}
	if proposal.Dataset != dataset || proposal.ID != id {
		return nil, fmt.Errorf("%w: proposal record %q/%q does not match its key", ErrRecordMismatch, dataset, id)
	}
	return proposal, nil
}

// authorizeProposalWrite applies the authorization split of §10.1 for a proposal write: the
// author acts with datasets:CreateProposal, any other principal is publishing on the
// author's behalf and needs datasets:PublishVersion.
func (s *Service) authorizeProposalWrite(ctx context.Context, caller Caller, dataset, author string) error {
	if caller.Username == author {
		return s.authorizer.CheckDataset(ctx, caller, ActionCreateProposal, dataset)
	}
	return s.authorizer.CheckDataset(ctx, caller, ActionPublishVersion, dataset)
}

// previewBranchOf returns the preview branch of a record, falling back to the derived name
// for a record written before the field existed (§5.6).
func previewBranchOf(proposal *ProposalRecord) string {
	if proposal.PreviewBranch != "" {
		return proposal.PreviewBranch
	}
	return ProposalPreviewBranch(proposal.ID)
}

// ensurePreviewBranch makes sure the hidden preview branch exists, creating it from the
// current `latest` head. An existing branch is reused: materialising a complete metarange
// onto it is exactly what an update does, and reusing it keeps the previous preview readable
// when an update fails half way.
func (s *Service) ensurePreviewBranch(ctx context.Context, dataset, branch string) error {
	_, err := s.catalog.CreateBranch(ctx, dataset, branch, DefaultBranch,
		graveler.WithForce(true), graveler.WithHidden(true))
	if err == nil || errors.Is(err, graveler.ErrBranchExists) {
		return nil
	}
	return fmt.Errorf("datasets: create preview branch %q on %q: %w", branch, dataset, err)
}

// materializePreview materialises the proposal definition on its hidden preview branch
// (§5.6), so a reviewer can browse the real data at
// lakefs://<dataset>/_proposal-<id>/<target>. It uses the publish path of materialize.go
// without the version tag: a preview is a branch, not a version.
//
// The preview is pinned to no version number of its own. It carries the version the proposal
// would produce if merged now (LatestVersion+1) and its own DefinitionScope, so its
// definition object can never collide with the definition object of that version (§6.4).
//
// A preview takes no GC pin either: the pins of §8.2 track published versions, and a proposal
// is not one. If GC collects a source while a proposal is open, both the preview and the merge
// fail on the missing key (§5.2 rule 5) - the merge cannot publish a version that silently
// lacks data, and the reviewer sees exactly the loss the author would.
func (s *Service) materializePreview(ctx context.Context, rec *DatasetRecord, proposal *ProposalRecord, actor string) error {
	branch := previewBranchOf(proposal)
	if err := s.ensurePreviewBranch(ctx, rec.Name, branch); err != nil {
		return err
	}
	materializer, err := s.materializerFor(ctx)
	if err != nil {
		return err
	}
	_, err = materializer.MaterializePreview(ctx, PublishRequest{
		Dataset:         rec.Name,
		Branch:          branch,
		DefinitionScope: branch,
		Version:         rec.LatestVersion + 1,
		Definition:      proposal.Definition,
		Message:         previewMessage(proposal),
		Publisher:       actor,
	}, branch)
	return err
}

// removePreviewBranch deletes a preview branch with Force (§5.6). "Not found" is tolerated:
// closing a proposal whose preview is already gone is not an error, and neither is a close
// that races a dataset delete.
func (s *Service) removePreviewBranch(ctx context.Context, dataset, branch string) error {
	if branch == "" {
		return nil
	}
	err := s.catalog.DeleteBranch(ctx, dataset, branch, graveler.WithForce(true))
	if err != nil && !isNotFound(err) {
		return fmt.Errorf("datasets: delete preview branch %q on %q: %w", branch, dataset, err)
	}
	return nil
}

// previewMessage is the commit message of a preview commit: the review's title, so the
// branch history is readable even without the proposal record.
func previewMessage(proposal *ProposalRecord) string {
	message := "proposal " + proposal.ID
	if title := strings.TrimSpace(proposal.Title); title != "" {
		message += ": " + title
	}
	return message
}

// mergeMessage is the default commit message of a merged proposal (§10.1 makes the body's
// message optional).
func mergeMessage(proposal *ProposalRecord) string {
	message := "merge proposal " + proposal.ID
	if title := strings.TrimSpace(proposal.Title); title != "" {
		message += ": " + title
	}
	return message
}

// proposalView attaches the diffs of §5.6 to a proposal: the definition diff from latest to
// the proposal, and the data diff of the preview branch against `latest`, which is what the
// detail response of §10.1 reports.
//
// A diff that cannot be computed - the preview branch is gone, or the dataset has no version
// record yet - is logged and left out instead of failing the read: the proposal record is
// still a valid, readable review object, and the reconciler (§12.3) is what repairs a
// missing branch.
func (s *Service) proposalView(ctx context.Context, rec *DatasetRecord, proposal *ProposalRecord) *ProposalView {
	view := &ProposalView{Record: proposal}
	latest, err := s.latestVersionDefinition(ctx, rec)
	if err != nil {
		logging.FromContext(ctx).WithError(err).WithFields(logging.Fields{
			"dataset":  rec.Name,
			"proposal": proposal.ID,
		}).Warn("datasets: could not read the latest definition of a proposal")
		return view
	}
	definition := cloneDefinition(proposal.Definition)
	view.Diff = DiffDefinitions(latest, &definition)

	data, err := (&Differ{Catalog: s.catalog}).Diff(ctx, DataDiffRequest{
		Dataset: rec.Name,
		Left:    DefaultBranch,
		Right:   previewBranchOf(proposal),
	})
	if err != nil {
		logging.FromContext(ctx).WithError(err).WithFields(logging.Fields{
			"dataset":  rec.Name,
			"proposal": proposal.ID,
			"branch":   previewBranchOf(proposal),
		}).Warn("datasets: could not diff a proposal preview against latest")
		return view
	}
	view.DataDiff = data
	return view
}

// latestVersionDefinition returns the resolved definition of the dataset's latest version,
// or nil when the dataset has no published version (a proposal against base version 0 then
// diffs against the empty definition). A missing version record is treated the same way:
// §5.4 makes the tag authoritative and the reconciler repairs the record.
func (s *Service) latestVersionDefinition(ctx context.Context, rec *DatasetRecord) (*Definition, error) {
	if rec.LatestVersion <= 0 {
		return nil, nil
	}
	version, err := s.store.GetVersion(ctx, rec.Name, rec.LatestVersion)
	if errors.Is(err, kv.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return s.versionDefinition(ctx, version)
}
