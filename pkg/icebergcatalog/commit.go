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
// Update actions and requirements are not implemented here: I0-1 (ICEBERG_SPIKE_S-I1.md) proved
// that the pinned iceberg-go models all 23 TableUpdate actions and all 8 TableRequirement types
// from the Iceberg REST OpenAPI spec, so there is nothing to implement locally. What iceberg-go
// keeps private - the metadata file, its name and location, and the pointer swap - is what this
// file adds.
//
// Licensing: this file is a clean-room implementation from the Apache Iceberg REST OpenAPI spec
// (Apache-2.0),
// https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml, and the Apache
// Iceberg table spec (Apache-2.0), https://iceberg.apache.org/spec/. Task I2-3. No code is taken
// from lakeFS Enterprise or from upstream lakeFS >= 1.87 (BUSL 1.1).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

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
