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
//  5. stage the files the commit added as lakeFS entries (§6.3, files.go StageTableFiles): the new
//     metadata file, the snapshots' manifest lists, the manifests they add and every file those
//     manifests add. A path outside the table's root, or a commit above iceberg.max_staged_files,
//     is refused with 400 here, before the pointer moves;
//  6. swap the pointer with a condition on the base metadata-location, so exactly one of two
//     concurrent commits wins and the loser's metadata file is left unreferenced for GC;
//  7. return the new pointer and metadata, i.e. the body of a LoadTableResult (§6.2 step 7).
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
//   - a snapshot that references a file outside the table's root (§13), or a commit that would
//     stage more files than iceberg.max_staged_files allows (files.go DefaultMaxStagedFiles), is
//     a 400 BadRequestException: the request cannot be served as sent, and retrying it unchanged
//     cannot help. Both are detected while planning, so such a commit stages nothing.
//   - any other failure of the swap happens *after* metadata.json is written and its outcome is
//     unknown to the client. It is a 500 CommitStateUnknownException and must not be mapped to
//     409: the commit may well have landed, so the client must re-read the table instead of
//     assuming failure (§2.1). The orphan metadata file is intentional and GC collects it. The
//     same holds for a failure while staging a file: some entries may already be staged (they are
//     deterministic, so a retry overwrites them) and the pointer has not moved.
//
// Everything that fails before metadata.json is written is passed through unchanged: a missing
// table is NoSuchTableException (404) and a corrupt pointer is ErrInvalidPointer. A graveler refusal
// of the swap itself - a protected branch, a read-only repository - is preserved for errors.Is but
// is reported as the unknown-outcome error, because it happens after the metadata write.
//
// Staging, not committing: the swap writes the branch's staging area through
// catalog.CreateEntry without Force (I2-1), exactly like every other catalog write. The commit
// here is an Iceberg commit; it becomes a lakeFS commit only on an explicit lakeFS commit on the
// branch (§6.2 "why stage instead of auto-commit").
//
// Compaction commits and the table-pointer merge (§6.4, phase I-5) are also here, at the end of the
// file: a snapshot the compaction operation marked with the lakefs.compaction.operation-id
// *snapshot property* is a compaction, and a three-way conflict on a table pointer that only
// compaction snapshots caused is auto-resolved when iceberg.ignore_compaction_commits is on (the
// default) instead of being reported (ResolvePointerConflict below, installed into merges by
// MergeConflictResolver).
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
// documentation (https://docs.lakefs.io/iceberg/) for the lakefs.compaction.operation-id snapshot
// property and the iceberg.ignore_compaction_commits setting. Tasks I2-3 and I5-1. No code is taken
// from lakeFS Enterprise or from upstream lakeFS >= 1.87 (BUSL 1.1).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/apache/iceberg-go/table"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/logging"
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

	// Step 5: stage the files of the snapshots this commit added, and the metadata file it wrote
	// (§6.2 step 5, §6.3). Planning happens first and inside StageTableFiles, so a foreign location
	// or an oversized commit is refused before a single entry is written; both are the client's
	// 400. Anything else fails after metadata.json was written and before the pointer swap, so the
	// outcome is unknown to the client (§6.2 step 6).
	if _, err := s.StageTableFiles(ctx, addr, StageTableFilesRequest{
		Base:          base.Metadata,
		Next:          next,
		MetadataFiles: []TableMetadataFile{{Location: location, Size: int64(len(metadataJSON))}},
		MaxFiles:      s.maxStagedFiles,
	}); err != nil {
		if isRefusedTableFiles(err) {
			return nil, &BadRequestError{
				Message: "the commit's table files cannot be registered",
				Err:     err,
			}
		}
		// Some of the files may already be staged - the writes are deterministic, so a retry overwrites
		// the same keys - and the pointer has not moved, so the client cannot know whether the commit
		// landed. That is exactly what CommitStateUnknownException tells it (§2.1).
		return nil, &CommitStateUnknownError{MetadataLocation: location, Err: err}
	}

	// Step 6: the compare-and-swap of the pointer on the base metadata-location (§6.2 step 6).
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

	// Step 7: what a LoadTableResult is made of.
	return &CommitResult{
		Address:      addr,
		Pointer:      pointer,
		Metadata:     next,
		MetadataJSON: metadataJSON,
	}, nil
}

// isRefusedTableFiles reports whether an error of file registration is the client's fault: a path
// outside the table's root (§13) or a commit above the staged-file cap
// (iceberg.max_staged_files). Both are raised while planning, i.e. before anything is staged, and
// both are a 400. Every other failure of that step is a server-side or storage failure and is
// reported as an unknown outcome by the caller.
func isRefusedTableFiles(err error) bool {
	return errors.Is(err, ErrForeignTableFile) || errors.Is(err, ErrTooManyTableFiles)
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
// exactly as the docs describe: "table metadata files are treated as regular files, and no special
// merge logic is applied to conflicting table changes"
// (https://docs.lakefs.io/iceberg/, "Merging conflicting table changes"). That is the right
// default: two logical commits to the same table produce two metadata.json files, and there is no
// way to reconcile them without rewriting one of them, so nothing here guesses a three-way merge
// of snapshots.
//
// A *compaction* is the exception the same documentation promises, in "Table Maintenance →
// Conflict Resolution":
//
//	When merging branches, lakeFS automatically resolves conflicts if both branches have
//	compaction commits for the same table and at most one branch has non-compaction data changes.
//
// A compaction is recognised exactly as those docs tell the operator to mark it -
// `snapshotProperty("lakefs.compaction.operation-id", "rewrite-data-files")` on the compaction
// action, i.e. CompactionCommitMarker as a property of the *snapshot* the compaction wrote, not as
// a commit message. That is why this code reads metadata.json and never reads lakeFS commit
// messages: the message is whatever the client's commit said, while the property is the mark the
// docs instruct the client to set.
//
// What "auto-resolve" means here, precisely: the compaction side is ignored, and the merge keeps
// the pointer of the side that changed the table logically. That is safe only because a compaction
// leaves the logical contents alone: the data files the ignored commit would have introduced are
// referenced by no kept metadata.json, so GC collects them, and the kept side's metadata still
// names every file the kept snapshot needs.
//
// Two guards keep that from resolving a conflict it cannot prove:
//
//   - the snapshots a side added since the merge base must be *only* compaction snapshots, and the
//     base's current snapshot must still be on that side's parent chain. A side whose history was
//     rewritten (dropped and recreated the table, hard reset to a third history, snapshots expired
//     or a trimmed metadata.json) is not "base plus a compaction", so the conflict is reported -
//     which is what the same docs warn about: "Expired or deleted snapshots will very likely lead
//     to merge conflicts, as lakeFS cannot determine which snapshots to keep";
//   - the side that is kept must also be shown to descend from the base. It is never assumed.
//
// The docs describe the common case, in which the compaction job ran on both branches. The rule
// implemented here is the same rule read one step further: the *ignored* side must be compaction-
// only, which is what makes ignoring it free. Requiring the kept side to carry a compaction snapshot
// too would refuse the equally safe - and more common - case where only one branch happened to be
// compacted, and the conflict would then block a merge for no reason.
//
// The decision logic is a pure function of the three pointers and of the metadata files they name
// (ResolvePointerConflict). Wiring it into a merge is the second half of this section:
// MergeConflictResolver is the pkg/catalog merge-conflict hook that Bootstrap installs on a
// lakefs-sso server with iceberg.enabled and iceberg.ignore_compaction_commits.

const (
	// CompactionCommitMarker is the Iceberg *snapshot property* that marks a snapshot written by a
	// compaction operation. The docs instruct clients to set it with
	// snapshotProperty("lakefs.compaction.operation-id", "rewrite-data-files"), so its presence on
	// a snapshot is what makes that snapshot recognisable as a compaction rather than a logical
	// change to the table (§6.4): it is the only mark the public documentation defines.
	CompactionCommitMarker = "lakefs.compaction.operation-id"
)

// IsCompactionSnapshot reports whether snapshot was written by a compaction: it carries the
// lakefs.compaction.operation-id snapshot property. A nil snapshot - a table that has no current
// snapshot at all - is not a compaction.
func IsCompactionSnapshot(snapshot *table.Snapshot) bool {
	if snapshot == nil || snapshot.Summary == nil {
		return false
	}
	return snapshot.Summary.Properties[CompactionCommitMarker] != ""
}

// IsTablePointerKey reports whether key is a table pointer entry of the on-branch layout (§6.1):
//
//	_lakefs_iceberg/tables/<ns1>/<ns2>/…/<table>/.pointer
//
// It is the key shape a merge conflict must have for this package to resolve it: every other key,
// including the _lakefs_iceberg/files/… entries a commit stages and the .namespace markers, is a
// logical change and stays a conflict.
func IsTablePointerKey(key graveler.Key) bool {
	path := string(key)
	prefix := ReservedPrefix + "/" + TablesDirectory + "/"
	suffix := "/" + TablePointerName
	if !strings.HasPrefix(path, prefix) || !strings.HasSuffix(path, suffix) {
		return false
	}
	// Between the prefix and the file name there must be at least one namespace level and the
	// table name: /tables/<ns>/<table>/.pointer.
	remainder := strings.TrimSuffix(strings.TrimPrefix(path, prefix), suffix)
	return strings.Contains(remainder, "/")
}

// PointerConflict is the three-way conflict a merge of two branches sees on a table pointer: the
// pointer at the merge base, and the pointer each side has. Base is the zero TablePointer when
// neither side had a table at the merge base, which is a genuine conflict ("both branches created
// it") and is never auto-resolved.
type PointerConflict struct {
	// Base is the pointer at the merge base, i.e. the pointer the two sides forked from.
	Base TablePointer

	// Destination is the side being merged into.
	Destination TablePointer

	// Source is the side being merged from.
	Source TablePointer
}

// PointerMergeResolution is the outcome of a resolved conflict: the pointer the merged branch
// keeps, and the compaction pointer that was dropped to get there.
type PointerMergeResolution struct {
	// Pointer is the pointer the merge keeps. It is one of the two sides' pointers, unchanged: a
	// resolution never rewrites metadata, it only chooses which side's commit wins.
	Pointer TablePointer

	// IgnoredCompaction is the pointer of the compaction that was ignored, zero when no compaction
	// had to be dropped. It is informational: the metadata.json it names is unreferenced after the
	// merge and GC collects it, together with the data files only that snapshot named.
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
//  4. both sides changed the table logically (neither side's added snapshots are all compaction
//     snapshots): conflict - the documented "at most one branch has non-compaction data changes";
//  5. otherwise the compaction-only side is ignored and the logical side wins, provided both sides
//     still descend from the base snapshot (see the file comment above). When both sides are
//     compaction-only nothing logical is being dropped, and the destination is kept: either side is
//     correct, and the destination is the one the merge is already writing.
//
// cfg may be nil: it then answers with the documented default, DefaultIgnoreCompactionCommits.
//
// A metadata file that cannot be read is returned as an error rather than as a conflict: the guard
// cannot be evaluated, and a merge must not resolve what it cannot prove. Nothing is written here;
// applying the resolution is the merge's job (see MergeConflictResolver).
func (s *Store) ResolvePointerConflict(ctx context.Context, repositoryID string, conflict PointerConflict, cfg *Config) (*PointerMergeResolution, error) {
	base := conflict.Base
	destination := conflict.Destination
	source := conflict.Source

	switch {
	case destination == source:
		return &PointerMergeResolution{Pointer: destination}, nil
	case destination == base:
		return &PointerMergeResolution{Pointer: source}, nil
	case source == base:
		return &PointerMergeResolution{Pointer: destination}, nil
	}

	// Both sides moved the pointer, and there is no base to reconcile them against.
	if base == (TablePointer{}) {
		return nil, nil
	}
	// Nothing but a compaction may be ignored, and the flag can turn that off.
	if !cfg.IgnoresCompactionCommits() {
		return nil, nil
	}

	baseMetadata, err := s.metadataAt(ctx, repositoryID, base)
	if err != nil {
		return nil, err
	}
	destinationMetadata, err := s.metadataAt(ctx, repositoryID, destination)
	if err != nil {
		return nil, err
	}
	sourceMetadata, err := s.metadataAt(ctx, repositoryID, source)
	if err != nil {
		return nil, err
	}

	destinationCompaction := isCompactionOnly(destinationMetadata, baseMetadata)
	sourceCompaction := isCompactionOnly(sourceMetadata, baseMetadata)

	switch {
	case destinationCompaction && sourceCompaction:
		// Both sides are physically rewritten and logically unchanged: nothing is lost either way.
		return &PointerMergeResolution{Pointer: destination, IgnoredCompaction: source}, nil
	case destinationCompaction:
		if !descendsFrom(sourceMetadata, baseMetadata) {
			return nil, nil
		}
		return &PointerMergeResolution{Pointer: source, IgnoredCompaction: destination}, nil
	case sourceCompaction:
		if !descendsFrom(destinationMetadata, baseMetadata) {
			return nil, nil
		}
		return &PointerMergeResolution{Pointer: destination, IgnoredCompaction: source}, nil
	}
	return nil, nil
}

// MergeConflictResolver returns the pkg/catalog merge-conflict hook of §6.4: the resolver that lets
// a merge auto-resolve a conflict on a table pointer instead of reporting it. Bootstrap installs it
// with catalog.SetMergeConflictResolver on a lakefs-sso server whose iceberg.enabled is true, and
// only while iceberg.ignore_compaction_commits is true; with no hook installed every merge reports
// its conflicts exactly as upstream lakeFS does.
//
// The hook is asked about every unresolved conflict of every merge, so it answers "not resolved"
// immediately for any key that is not a table pointer. A conflict on a pointer is handed to
// ResolvePointerConflict, and the winning side's own entry is returned unchanged: a resolution
// picks a side, it never rewrites one, and the merge writes the winner's value verbatim.
//
// A pointer entry this catalog cannot decode is reported as a conflict rather than as an error:
// there is nothing to prove about it, and reporting the conflict is both the safe answer and the
// upstream one. A metadata.json that cannot be read is an error, because there the resolver was
// asked to prove something and could not: the merge then fails instead of guessing.
func (s *Store) MergeConflictResolver(cfg *Config) catalog.MergeConflictResolver {
	return func(ctx context.Context, conflict catalog.MergeConflict) (*graveler.ValueRecord, error) {
		if !IsTablePointerKey(conflict.Key) {
			return nil, nil
		}
		// A table dropped on one side is a logical change: the pointer is a tombstone there and
		// there is nothing to compare.
		if isAbsentValue(conflict.Source) || isAbsentValue(conflict.Destination) {
			return nil, nil
		}
		base, destination, source, err := pointerConflictOf(conflict)
		if err != nil {
			logging.FromContext(ctx).WithError(err).WithField("key", string(conflict.Key)).
				Debug("iceberg catalog: a table pointer in a merge conflict cannot be decoded; reporting the conflict")
			return nil, nil
		}
		resolution, err := s.ResolvePointerConflict(ctx, conflict.RepositoryID, PointerConflict{
			Base:        base,
			Destination: destination,
			Source:      source,
		}, cfg)
		if err != nil {
			return nil, err
		}
		if resolution == nil {
			return nil, nil
		}
		switch resolution.Pointer {
		case destination:
			return conflict.Destination, nil
		case source:
			return conflict.Source, nil
		}
		return nil, fmt.Errorf("%w: the resolved pointer %q is neither side of the conflict",
			ErrInvalidPointer, resolution.Pointer.MetadataLocation)
	}
}

// metadataAt loads the metadata.json a pointer names. It is ResolvePointerConflict's only I/O.
func (s *Store) metadataAt(ctx context.Context, repositoryID string, pointer TablePointer) (table.Metadata, error) {
	metadata, _, err := s.loadMetadata(ctx, repositoryID, pointer.MetadataLocation)
	return metadata, err
}

// isCompactionOnly reports whether the only snapshots side added on top of base are compaction
// snapshots: the side rewrote the table's physical layout and changed nothing else. That is the
// documented "at most one branch has non-compaction data changes", from the side of the side that
// has none.
func isCompactionOnly(side, base table.Metadata) bool {
	added, ok := snapshotsAddedOnTop(side, base)
	if !ok || len(added) == 0 {
		return false
	}
	for _, snapshot := range added {
		if !IsCompactionSnapshot(snapshot) {
			return false
		}
	}
	return true
}

// descendsFrom reports whether side's snapshot chain still reaches base's current snapshot, i.e.
// whether side is base plus its own work rather than a rewritten history.
func descendsFrom(side, base table.Metadata) bool {
	_, ok := snapshotsAddedOnTop(side, base)
	return ok
}

// snapshotsAddedOnTop returns the snapshots side added on top of base: the parent chain from side's
// current snapshot down to base's current snapshot, which is excluded. ok is false when that chain
// never reaches the base - a base without a snapshot, a snapshot expired out of the metadata, a
// trimmed metadata.json, a table dropped and recreated under the same name, or a hard reset to a
// third history.
//
// Walking the parent chain, and not metadata-log, is what makes the guard mean "the base snapshot
// is still in this history": metadata-log is trimmed by write.metadata.previous-versions-max, while
// an expired snapshot is exactly the case the docs say must not be resolved.
func snapshotsAddedOnTop(side, base table.Metadata) ([]*table.Snapshot, bool) {
	baseSnapshot := base.CurrentSnapshot()
	if baseSnapshot == nil {
		return nil, false
	}
	var added []*table.Snapshot
	for current := side.CurrentSnapshot(); current != nil; {
		if current.SnapshotID == baseSnapshot.SnapshotID {
			return added, true
		}
		added = append(added, current)
		if current.ParentSnapshotID == nil {
			return nil, false
		}
		parent := side.SnapshotByID(*current.ParentSnapshotID)
		if parent == nil {
			return nil, false
		}
		current = parent
	}
	return nil, false
}

// pointerConflictOf decodes the three pointers of a merge conflict on a table pointer. A side that
// is absent decodes as the zero TablePointer.
func pointerConflictOf(conflict catalog.MergeConflict) (base, destination, source TablePointer, err error) {
	if base, err = tablePointerFromValue(conflict.Base); err != nil {
		return base, destination, source, err
	}
	if destination, err = tablePointerFromValue(conflict.Destination); err != nil {
		return base, destination, source, err
	}
	source, err = tablePointerFromValue(conflict.Source)
	return base, destination, source, err
}

// tablePointerFromValue decodes a graveler value as the catalog entry a table pointer is stored in
// (§6.1) and then as a TablePointer. A nil record - the key is absent on that side - is the zero
// pointer, not an error.
func tablePointerFromValue(record *graveler.ValueRecord) (TablePointer, error) {
	if isAbsentValue(record) {
		return TablePointer{}, nil
	}
	entry, err := catalog.ValueToEntry(record.Value)
	if err != nil {
		return TablePointer{}, fmt.Errorf("%w: decode entry: %w", ErrInvalidPointer, err)
	}
	pointer, err := DecodeTablePointer(entry.Metadata)
	if err != nil {
		return TablePointer{}, err
	}
	return pointer, nil
}

// isAbsentValue reports whether a side of a conflict holds no entry at all: a nil record, or a
// tombstone (a nil Value), which is how graveler records a deletion.
func isAbsentValue(record *graveler.ValueRecord) bool {
	return record == nil || record.Value == nil
}
