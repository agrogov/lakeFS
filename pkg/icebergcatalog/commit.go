package icebergcatalog

// The commit path: requirements, updates, a new metadata.json and an atomic pointer swap
// (ICEBERG_CATALOG_PLAN.md §6.2 "Atomic commit (check-and-put)", §11 I-2).
//
// One commit is six steps:
//
//  1. resolve the table pointer on the branch (staged first, then committed) and load the
//     metadata.json it names (Store.GetTable);
//  2. validate every requirement of the request against that metadata. A failed requirement is a
//     409 CommitFailedException: the table moved under the client, and the Iceberg client's
//     response to a 409 is to refresh and retry (§2.1);
//  3. reject the set-location action with 400 (a documented Enterprise limitation, §6.2 step 3).
//     A table's location is fixed at creation, which is what lets lakeFS stage table files under
//     it and GC them (§6.3);
//  4. apply the updates with table.UpdateTableMetadata (iceberg-go), then write the result to a
//     fresh, never-reused <seq>-<uuid>.metadata.json (metadata.go);
//  5. swap the pointer with a condition on the base metadata-location, so exactly one of two
//     concurrent commits wins and the loser's metadata file is left unreferenced for GC;
//  6. return the new pointer and metadata, i.e. the body of a LoadTableResult (§6.2 step 7).
//
// Error classification, in the terms of the REST spec's ErrorModel:
//
//   - a malformed commit body, an update that cannot be applied to the current table, an
//     unsupported format version, and set-location are 400 BadRequestException. Nothing on this
//     metadata-only path may be reported as 409: a 409 tells the client to refresh and retry the
//     same doomed request forever. (I0-1 verified that only eight of the nine apply refusals even
//     wrap a sentinel, so every apply error is classified as a bad request rather than matched by
//     cause.)
//   - a requirement that does not hold, and a lost compare-and-swap
//     (graveler.ErrPreconditionFailed), are 409 CommitFailedException.
//   - any other failure of the swap happens *after* metadata.json is written and its outcome is
//     unknown to the client. It is a 500 CommitStateUnknownException and must not be mapped to
//     409: the commit may well have landed, so the client must re-read the table instead of
//     assuming failure (§2.1). The orphan metadata file is intentional and GC collects it.
//
// Everything that fails before the pointer swap is passed through unchanged: a missing table is
// NoSuchTableException (404) and a corrupt pointer is ErrInvalidPointer. A graveler refusal of the
// swap itself - a protected branch, a read-only repository - is preserved for errors.Is but is
// reported as the unknown-outcome error, because it happens after the metadata write.
//
// Staging, not committing: the swap writes the branch's staging area through
// catalog.CreateEntry without Force (I2-1), exactly like every other catalog write. The commit
// here is an Iceberg commit; it becomes a lakeFS commit only on an explicit lakeFS commit on the
// branch (§6.2 "why stage instead of auto-commit").
//
// Compaction commits and the table-pointer merge (§6.4, phase I-5) are also here, at the end of the
// file: a lakeFS commit whose message carries lakefs.compaction.operation-id is a compaction
// commit, and a three-way conflict on a table pointer that such a commit alone caused is
// auto-resolved when iceberg.ignore_compaction_commits is on (the default) instead of being
// reported (ResolvePointerConflict below).
//
// Update actions and requirements are not implemented here: I0-1 (ICEBERG_SPIKE_S-I1.md) proved
// that the pinned iceberg-go models all 23 TableUpdate actions and all 8 TableRequirement types
// from the Iceberg REST OpenAPI spec, so there is nothing to implement locally. What iceberg-go
// keeps private - the metadata file, its name and location, and the pointer swap - is what this
// file adds.
//
// Licensing: this file is a clean-room implementation from the Apache Iceberg REST OpenAPI spec
// (Apache-2.0),
// https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml, the Apache
// Iceberg table spec (Apache-2.0), https://iceberg.apache.org/spec/, and the public lakeFS Iceberg
// documentation (https://docs.lakefs.io/iceberg/) for the compaction commit message and the
// iceberg.ignore_compaction_commits setting. Tasks I2-3 and I5-1. No code is taken from lakeFS
// Enterprise or from upstream lakeFS >= 1.87 (BUSL 1.1).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/apache/iceberg-go/table"
	"github.com/treeverse/lakefs/pkg/graveler"
)

const (
	// ErrorTypeCommitStateUnknown is the Iceberg error type of a commit whose outcome the client
	// cannot know: metadata.json was written and the pointer swap then failed for a reason other
	// than a lost compare-and-swap. The spec's ErrorModel examples name it
	// CommitStateUnknownException; it is always a 5xx so that the client re-reads the table
	// instead of assuming failure (§2.1).
	ErrorTypeCommitStateUnknown = "CommitStateUnknownException"
)

var (
	// ErrCommitFailed is reported by CommitFailedError: the commit was refused before anything was
	// swapped, because a requirement does not hold or the pointer moved. 409.
	ErrCommitFailed = errors.New("iceberg commit failed")

	// ErrCommitStateUnknown is reported by CommitStateUnknownError: the metadata file was written
	// but the swap failed in a way that leaves the outcome unknown. 5xx, never 409.
	ErrCommitStateUnknown = errors.New("iceberg commit state unknown")

	// ErrCommitRejected is reported by BadRequestError: the request cannot be served as sent -
	// a malformed body, an update the current table forbids, an unsupported format version, or
	// set-location. 400.
	ErrCommitRejected = errors.New("iceberg commit rejected")

	// ErrSetLocationUnsupported is the §6.2 step 3 limitation: the set-location update is refused
	// with 400. It is the documented Enterprise limitation and it also protects §6.3: staged table
	// files and the GC window are tied to the table's location.
	ErrSetLocationUnsupported = errors.New("the set-location update is not supported")
)

// TableCommitRequest is the body of a commit (spec: components.schemas.CommitTableRequest): the
// requirements the table must still satisfy and the updates to apply. Both are decoded by
// iceberg-go, so a payload this catalog cannot model fails here rather than half-way through a
// commit.
//
// Other members of the schema are ignored: identifier repeats the address that the URL already
// carries, and stage-create is served by the create-table path (I2-4), not by this one.
type TableCommitRequest struct {
	Requirements table.Requirements `json:"requirements"`
	Updates      table.Updates      `json:"updates"`
}

// CommitResult is the outcome of a successful commit: the new pointer, the parsed metadata and
// the exact bytes written, i.e. everything a LoadTableResult needs (§6.2 step 7). MetadataJSON is
// kept so that the response does not re-marshal the model.
type CommitResult struct {
	// Address is the table the commit was addressed to, unchanged.
	Address Address

	// Pointer is the new table pointer, already staged on Address.Ref.
	Pointer TablePointer

	// Metadata is the new metadata.json, parsed.
	Metadata table.Metadata

	// MetadataJSON is the exact bytes of the new metadata.json.
	MetadataJSON []byte
}

// DecodeTableCommitRequest decodes a commit body. A body that is not a commit request - an unknown
// action, a missing action, a missing required field, a requirement type this catalog does not
// know - is a 400 BadRequestException (spec: components.schemas.CommitTableRequest).
func DecodeTableCommitRequest(body []byte) (TableCommitRequest, error) {
	var request TableCommitRequest
	if err := json.Unmarshal(body, &request); err != nil {
		return TableCommitRequest{}, &BadRequestError{
			Message: "malformed commit request",
			Err:     err,
		}
	}
	return request, nil
}

// Commit applies one Iceberg commit to the table at addr, implementing §6.2 end to end. The
// updates are staged on addr.Ref: they are visible on the branch immediately and become part of
// the repository only on an explicit lakeFS commit.
//
// The caller maps the returned error with HTTPStatusCode and ErrorType where it implements them;
// every error of this function either is a *CommitFailedError (409), a *CommitStateUnknownError
// (500), a *BadRequestError (400) or the unchanged error of resolving the base table.
func (s *Store) Commit(ctx context.Context, addr Address, request TableCommitRequest) (*CommitResult, error) {
	// Step 1: the base pointer and the metadata.json it names. A missing table, a corrupt pointer
	// and a missing metadata file are the read half's errors, reported as they are.
	base, err := s.GetTable(ctx, addr)
	if err != nil {
		return nil, err
	}

	// Step 2: every requirement of the request must hold against the base metadata.
	if err := validateRequirements(request.Requirements, base.Metadata); err != nil {
		return nil, err
	}

	// Step 3: set-location is refused before any state is touched (§6.2 step 3).
	if err := rejectUnsupportedUpdates(request.Updates); err != nil {
		return nil, err
	}

	// Step 4: apply the updates. currentFileLocation must be the file being replaced, not the one
	// about to be written: iceberg-go appends it to metadata-log with the current last-updated-ms,
	// which is exactly the metadata-log entry the Iceberg spec requires (I0-1 correction 2).
	next, err := table.UpdateTableMetadata(base.Metadata, request.Updates, base.Pointer.MetadataLocation)
	if err != nil {
		// Nothing on this metadata-only path produces table.ErrCommitFailed, so an apply failure is
		// never a conflict: it is a request this table cannot serve (§2.1, I0-1 item 4).
		return nil, &BadRequestError{
			Message: "the updates cannot be applied to the current table",
			Err:     err,
		}
	}
	// Format version 3 is a v1 non-goal (§1.2). Upgrade-format-version and add-encryption-key are
	// the two actions that can reach it; both decode and apply, so the gate belongs here, before
	// the metadata file is written, and it names the reason instead of surfacing a pointer
	// encoding failure later.
	if version := next.Version(); version != 1 && version != 2 {
		return nil, &BadRequestError{
			Message: fmt.Sprintf("format version %d is not supported: this catalog serves format versions 1 and 2", version),
		}
	}

	location, err := NewMetadataFileLocation(next.Location(), base.Pointer.MetadataLocation, next)
	if err != nil {
		return nil, err
	}
	metadataJSON, err := s.WriteMetadata(ctx, addr.Repo, location, next)
	if err != nil {
		// The metadata file is what step 4 is; failing it means nothing was swapped, so this is a
		// plain failure of the request and not an unknown outcome.
		return nil, err
	}

	// Step 5: the compare-and-swap of the pointer on the base metadata-location (§6.2 step 6).
	pointer := TablePointer{
		MetadataLocation: location,
		TableUUID:        next.TableUUID().String(),
		FormatVersion:    next.Version(),
	}
	if err := s.UpdateTablePointer(ctx, addr, base.Pointer, pointer); err != nil {
		if errors.Is(err, graveler.ErrPreconditionFailed) {
			// The pointer moved under us. The new metadata file stays unreferenced and GC
			// collects it; the client refreshes and retries.
			return nil, &CommitFailedError{Err: err}
		}
		// Anything else happened after metadata.json was written: the commit may or may not have
		// landed, so the client must re-read the table rather than assume failure (§2.1).
		return nil, &CommitStateUnknownError{MetadataLocation: location, Err: err}
	}

	// Step 6: what a LoadTableResult is made of.
	return &CommitResult{
		Address:      addr,
		Pointer:      pointer,
		Metadata:     next,
		MetadataJSON: metadataJSON,
	}, nil
}

// validateRequirements runs Requirement.Validate for every requirement, against the base table
// metadata and in request order, so the reported failure is the first one the client sent.
//
// iceberg-go's Requirement.Validate returns a plain error, not table.ErrCommitFailed (I0-1
// correction 1), so the mapping to 409 is made here rather than matched from the cause. A nil
// metadata means "the table does not exist", which only assert-create accepts; the staged-create
// path that pairs it with updates is I2-4's create table, not this function.
func validateRequirements(requirements table.Requirements, base table.Metadata) error {
	for _, requirement := range requirements {
		if err := requirement.Validate(base); err != nil {
			return &CommitFailedError{
				Requirement: requirement.GetType(),
				Err:         err,
			}
		}
	}
	return nil
}

// rejectUnsupportedUpdates refuses the update actions this catalog does not serve. It runs in
// step 3, before any state is written, so a refused update never leaves a metadata file behind.
// Only set-location is refused (§6.2 step 3); the rest of the spec's actions are applied by
// iceberg-go.
func rejectUnsupportedUpdates(updates table.Updates) error {
	for _, update := range updates {
		if update.Action() == table.UpdateSetLocation {
			return &BadRequestError{
				Message: "the set-location update is not supported: a table's location is fixed when it is created",
				Err:     ErrSetLocationUnsupported,
			}
		}
	}
	return nil
}

// CommitFailedError is a commit refused before its pointer swap: a requirement of the request does
// not hold against the current table, or the pointer moved between the read and the swap. It is a
// 409 CommitFailedException, the signal to refresh and retry (§2.1).
type CommitFailedError struct {
	// Requirement is the type of the requirement that failed, empty when the cause is the
	// compare-and-swap of the pointer rather than a requirement.
	Requirement string

	// Err is the underlying failure: iceberg-go's requirement error, or
	// graveler.ErrPreconditionFailed.
	Err error
}

// Error implements error.
func (e *CommitFailedError) Error() string {
	if e.Requirement != "" {
		return fmt.Sprintf("%s: requirement %q is not satisfied: %v", ErrCommitFailed, e.Requirement, e.Err)
	}
	return fmt.Sprintf("%s: %v", ErrCommitFailed, e.Err)
}

// Unwrap reports ErrCommitFailed and the underlying failure, so a caller can match either with
// errors.Is - in particular graveler.ErrPreconditionFailed for a lost race.
func (e *CommitFailedError) Unwrap() error { return errors.Join(ErrCommitFailed, e.Err) }

// HTTPStatusCode is the status of the refusal, 409.
func (e *CommitFailedError) HTTPStatusCode() int { return http.StatusConflict }

// ErrorType is the Iceberg error type of the refusal.
func (e *CommitFailedError) ErrorType() string { return ErrTypeCommitFailed }

// CommitStateUnknownError is a commit whose outcome the client cannot know: metadata.json was
// written, the pointer swap then failed for a reason other than a lost compare-and-swap, and the
// commit may have landed. It is a 500 CommitStateUnknownException; it must never be reported as
// 409, because a client that reads 409 assumes the commit did not happen (§2.1).
//
// MetadataLocation names the file that may now be orphaned. GC collects it once nothing references
// it, so nothing has to be cleaned up here.
type CommitStateUnknownError struct {
	// MetadataLocation is the metadata.json this commit wrote.
	MetadataLocation string

	// Err is the failure of the pointer swap.
	Err error
}

// Error implements error.
func (e *CommitStateUnknownError) Error() string {
	return fmt.Sprintf("%s: the commit to %q may or may not have been applied: %v",
		ErrCommitStateUnknown, e.MetadataLocation, e.Err)
}

// Unwrap reports ErrCommitStateUnknown and the underlying failure.
func (e *CommitStateUnknownError) Unwrap() error { return errors.Join(ErrCommitStateUnknown, e.Err) }

// HTTPStatusCode is the status of the failure, 500.
func (e *CommitStateUnknownError) HTTPStatusCode() int { return http.StatusInternalServerError }

// ErrorType is the Iceberg error type of the failure.
func (e *CommitStateUnknownError) ErrorType() string { return ErrorTypeCommitStateUnknown }

// BadRequestError is a commit this catalog refuses as sent: a body that is not a commit request,
// an update the current table forbids, an unsupported format version, or set-location. It is a 400
// BadRequestException.
type BadRequestError struct {
	// Message is the reason, in the terms of the request rather than of the implementation: it is
	// sent to the client as the envelope's message.
	Message string

	// Err is the underlying failure, for errors.Is and for the log.
	Err error
}

// Error implements error.
func (e *BadRequestError) Error() string {
	if e.Err == nil {
		return fmt.Sprintf("%s: %s", ErrCommitRejected, e.Message)
	}
	return fmt.Sprintf("%s: %s: %v", ErrCommitRejected, e.Message, e.Err)
}

// Unwrap reports ErrCommitRejected and the underlying failure.
func (e *BadRequestError) Unwrap() error { return errors.Join(ErrCommitRejected, e.Err) }

// HTTPStatusCode is the status of the refusal, 400.
func (e *BadRequestError) HTTPStatusCode() int { return http.StatusBadRequest }

// ErrorType is the Iceberg error type of the refusal.
func (e *BadRequestError) ErrorType() string { return ErrTypeBadRequest }

// ---------------------------------------------------------------------------------------------
// Compaction commits and the table-pointer merge (§6.4, phase I-5)
// ---------------------------------------------------------------------------------------------
//
// The table pointer is one lakeFS entry, so two branches that both moved it conflict at merge,
// exactly as the docs describe: "metadata files are treated as ordinary files"
// (https://docs.lakefs.io/iceberg/). That is the right default - two logical commits to the same
// table produce two metadata.json files, and there is no way to reconcile them without rewriting
// one of them - but a *compaction* commit is not a logical change: it rewrites data files and
// moves the pointer, while the table's contents stay the same. Reporting a conflict against it
// would make every concurrent append lose to a background rewrite of the table, so lakeFS
// auto-resolves such a conflict when iceberg.ignore_compaction_commits is on, which is the
// documented default and DefaultIgnoreCompactionCommits.
//
// What "auto-resolve" means here, precisely: the compaction side is ignored, and the merge keeps
// the pointer of the side that changed the table logically. That is safe only because compaction
// leaves the logical contents alone: the data files the ignored commit would have introduced are
// referenced by no kept metadata.json, so GC collects them, and the kept side's metadata still
// names every file the kept snapshot needs. Nothing is merged at the snapshot level: "ignore the
// compaction commit" is not "merge two snapshots", and the note of §6.4 is taken literally - this
// code never guesses a three-way merge of snapshots.
//
// Two guards keep that from resolving a conflict it cannot prove:
//
//   - both sides must still descend from the merge-base metadata file, i.e. base's
//     metadata-location must appear in the metadata-log chain of the side's metadata.json. A side
//     that rewrote its history (dropped and recreated the table, hard-reset, a third history) is
//     not "base plus a compaction", so the conflict is reported;
//   - the compaction side is identified by the commit message alone: a message that carries
//     lakefs.compaction.operation-id. Two logical changes, even if one of them also touched data
//     files, are always a conflict.
//
// metadata-log is trimmed by write.metadata.previous-versions-max (100 entries by default), so a
// very old base turns the ancestry guard into "cannot prove it" and the conflict is reported. That
// is the fail-safe direction: a reported conflict costs a client a refresh, a wrong resolution
// costs data.

const (
	// CompactionCommitMarker is the marker that a lakeFS compaction commit - the Spark metadata
	// client rewriting a table's data files - carries in its commit message. It is what makes a
	// commit recognisable as a compaction of a table rather than a logical change to it (§6.4).
	CompactionCommitMarker = "lakefs.compaction.operation-id"
)

// IsCompactionCommit reports whether a lakeFS commit message marks a compaction commit: the
// message carries CompactionCommitMarker, wherever it appears and whatever operation id follows.
func IsCompactionCommit(message string) bool {
	return strings.Contains(message, CompactionCommitMarker)
}

// PointerRevision is one side of a three-way conflict on a table pointer: the pointer the side's
// ref holds, and the message of the lakeFS commit that last changed it. The message is what
// separates a compaction commit from a logical one; it is the message of the commit that produced
// Pointer, not of the ref's head, which may be a later commit that touched something else.
type PointerRevision struct {
	// Pointer is the pointer this side has at its head.
	Pointer TablePointer

	// CommitMessage is the message of the lakeFS commit that last moved this side's pointer.
	CommitMessage string
}

// PointerConflict is the three-way conflict a merge of two branches sees on a table pointer: the
// merge-base pointer, and the pointer each side has at its head (§6.4). Base is the zero
// TablePointer when neither side had a table at the merge base, which is a genuine conflict
// ("both branches created it") and is never auto-resolved.
type PointerConflict struct {
	// Base is the pointer at the merge base, i.e. the pointer the two sides forked from.
	Base TablePointer

	// Destination is the side being merged into.
	Destination PointerRevision

	// Source is the side being merged from.
	Source PointerRevision
}

// PointerMergeResolution is the outcome of a resolved conflict: the pointer the merged branch
// keeps, and the compaction pointer that was dropped to get there.
type PointerMergeResolution struct {
	// Pointer is the pointer the merge keeps. It is one of the two sides' pointers, unchanged: a
	// resolution never rewrites metadata, it only chooses which side's commit wins.
	Pointer TablePointer

	// IgnoredCompaction is the pointer of the compaction commit that was ignored, zero when no
	// compaction commit had to be dropped (i.e. when only one side had moved the pointer). It is
	// informational: the metadata.json it names is unreferenced after the merge and GC collects it.
	IgnoredCompaction TablePointer
}

// ResolvePointerConflict decides a three-way conflict on a table pointer (§6.4). It returns the
// resolution, or nil for "this is a real conflict, report it" - the same convention as graveler's
// ConflictResolver, whose ResolveConflict returns a nil value for a conflict it cannot resolve.
//
// The decision, in order:
//
//  1. both sides hold the same pointer: resolved, that pointer wins;
//  2. one side still holds the base pointer: resolved, the other side's pointer wins - that side
//     only moved forward and there is nothing to reconcile;
//  3. both sides moved it and cfg says compaction commits are not ignored: conflict;
//  4. both sides changed the table logically (neither commit is a compaction): conflict;
//  5. otherwise exactly one side is a compaction commit (or both are, in which case both are
//     logically no-ops and the destination is kept), and the compaction side is ignored - provided
//     the base metadata file is still an ancestor of both sides (see the file comment above).
//
// cfg may be nil: it then answers with the documented default, DefaultIgnoreCompactionCommits.
//
// A metadata file that cannot be read is returned as an error rather than as a conflict: the guard
// cannot be evaluated, and a merge must not resolve what it cannot prove. Nothing is written here;
// applying the resolution (writing the kept pointer on the destination branch) is the merge's job.
func (s *Store) ResolvePointerConflict(ctx context.Context, repositoryID string, conflict PointerConflict, cfg *Config) (*PointerMergeResolution, error) {
	base := conflict.Base
	destination := conflict.Destination.Pointer
	source := conflict.Source.Pointer

	switch {
	case destination == source:
		return &PointerMergeResolution{Pointer: destination}, nil
	case destination == base:
		return &PointerMergeResolution{Pointer: source}, nil
	case source == base:
		return &PointerMergeResolution{Pointer: destination}, nil
	}

	// Both sides moved the pointer. Nothing but a compaction commit may be ignored.
	if !cfg.IgnoresCompactionCommits() {
		return nil, nil
	}
	destinationCompaction := IsCompactionCommit(conflict.Destination.CommitMessage)
	sourceCompaction := IsCompactionCommit(conflict.Source.CommitMessage)
	if !destinationCompaction && !sourceCompaction {
		return nil, nil
	}

	// The kept side is the one that changed the table logically. When both sides are compaction
	// commits the destination is kept: both are logically no-ops, so either is correct and the
	// destination is the one the merge is already writing.
	kept, ignored := conflict.Destination.Pointer, conflict.Source.Pointer
	if destinationCompaction && !sourceCompaction {
		kept, ignored = source, destination
	}

	// Both sides must still be base plus their own commits. A side whose metadata-log no longer
	// contains the base metadata file is not a compaction of the base table, and ignoring it would
	// silently drop whatever else that side did.
	for _, location := range []string{kept.MetadataLocation, ignored.MetadataLocation} {
		descends, err := s.metadataDescendsFrom(ctx, repositoryID, location, base.MetadataLocation)
		if err != nil {
			return nil, err
		}
		if !descends {
			return nil, nil
		}
	}

	return &PointerMergeResolution{Pointer: kept, IgnoredCompaction: ignored}, nil
}

// metadataDescendsFrom reports whether ancestor is metadata-location (location itself, or an entry
// of the metadata-log chain of the metadata.json at location). It is the "base is still an
// ancestor" guard of ResolvePointerConflict.
//
// Only the chain recorded in the metadata file at location is consulted: an ancestor further back
// than write.metadata.previous-versions-max was trimmed out of metadata-log and answers false, so
// the conflict is reported. The zero location or ancestor is never an ancestor of anything - a
// table that did not exist at the merge base has no history to descend from.
func (s *Store) metadataDescendsFrom(ctx context.Context, repositoryID, location, ancestor string) (bool, error) {
	if location == "" || ancestor == "" {
		return false, nil
	}
	if location == ancestor {
		return true, nil
	}
	metadata, _, err := s.loadMetadata(ctx, repositoryID, location)
	if err != nil {
		return false, err
	}
	for entry := range metadata.PreviousFiles() {
		if entry.MetadataFile == ancestor {
			return true, nil
		}
	}
	return false, nil
}
