package cleanup

// expire.go is phase 1: expire old snapshots and stage the rewritten table metadata.
//
// # What "expire a snapshot" means here
//
// The Iceberg spec keeps a table's history in two places: the list of snapshots the table
// can still read (snapshots, current-snapshot-id), and the snapshot-log that records which
// snapshot was current when. Expiring removes snapshots from both, leaving the data files
// they referenced unreferenced by the table (which is what phase 2 then reclaims). The
// rewrite is done with iceberg-go's MetadataBuilderFromBase + RemoveSnapshots, the same
// model and the same operation the catalog serves for a remove-snapshots commit
// (pkg/icebergcatalog/commit.go, doc.go "Dependency rule"), so the document this phase
// writes is byte-for-byte the shape a catalog commit would have written:
//
//   - last-updated-ms becomes the run's instant;
//   - the old metadata file is appended to metadata-log (MetadataBuilderFromBase does that
//     when it is given the current file's location);
//   - snapshots, snapshot-log, refs, statistics and partition-statistics of removed
//     snapshots are pruned together (MetadataBuilder.RemoveSnapshots);
//   - current-snapshot-id and every Iceberg ref's snapshot are kept, so the table a
//     branch head or a tag reads stays readable.
//
// # Retention
//
// The retention window comes from the table, not only from the command line: a table that
// sets history.expire.max-snapshot-age-ms or history.expire.min-snapshots-to-keep overrides
// the run's defaults for that table, which is what the Iceberg property names mean
// (https://iceberg.apache.org/docs/latest/configuration/#table-behavior-properties). The
// command line values are the defaults for tables that do not set them. A table with
// lakefs-gc.enabled=false is skipped entirely, so an operator can opt a table out of the
// job without touching the repository.
//
// # Planning happens before anything is written
//
// The phase is two-pass. The first pass resolves every branch and every table and builds a
// plan - the new metadata bytes, the new location, the new pointer - without writing
// anything. The second pass applies the plan. That is what makes the abort on a v1 table
// with embedded manifests a real abort: the first pass sees it before any other table has
// been touched (ErrEmbeddedManifests), so a repository is never half-expired because of a
// table the tool refuses to rewrite.
//
// # Staging, not committing
//
// The apply step uploads the new metadata file to its final physical location, stages the
// catalog's registration of it, and stages the table pointer - in that order, so the
// metadata object is protected by an entry before the pointer can reference it. Everything
// lands in the branch's staging area and the operator commits it; nothing in this file
// calls a commit API. See doc.go "Why expire rewrites metadata instead of using the
// catalog's commit API" for the full argument and the key layout.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/treeverse/lakefs/pkg/icebergcatalog"
)

// Table properties this phase honors. They are the Iceberg table-behavior properties and
// the lakeFS catalog's opt-out, quoted verbatim from the public lakeFS Iceberg
// documentation and the Iceberg configuration table.
const (
	// PropertyMaxSnapshotAgeMs overrides the run's snapshot retention for one table.
	PropertyMaxSnapshotAgeMs = "history.expire.max-snapshot-age-ms"

	// PropertyMinSnapshotsToKeep overrides the number of most recent snapshots one table
	// keeps.
	PropertyMinSnapshotsToKeep = "history.expire.min-snapshots-to-keep"

	// PropertyLakeFSGCEnabled is the per-table opt-out: the string "false" makes this job
	// skip the table completely (no expire, no sweep of its files).
	PropertyLakeFSGCEnabled = "lakefs-gc.enabled"
)

// RetainedSnapshot is a snapshot the retention decision keeps, with the reason. The reason
// is part of the report because "why was this snapshot not expired" is the first question
// an operator asks after a run that removed less than they expected.
type RetainedSnapshot struct {
	SnapshotID  int64
	TimestampMs int64
	Reason      string
}

// ExpiredSnapshot is a snapshot the retention decision removes.
type ExpiredSnapshot struct {
	SnapshotID  int64
	TimestampMs int64
}

// SnapshotRetention is one table's retention decision.
type SnapshotRetention struct {
	Keep   []RetainedSnapshot
	Expire []ExpiredSnapshot
}

// Retention reasons.
const (
	SnapshotReasonCurrent            = "current"
	SnapshotReasonRef                = "ref"
	SnapshotReasonMinSnapshotsToKeep = "min-snapshots-to-keep"
	SnapshotReasonNewerThanMaxAge    = "newer-than-max-snapshot-age"
)

// SnapshotRetentionFor computes which snapshots to keep and which to expire
// (https://iceberg.apache.org/docs/latest/maintenance/#expire-snapshots).
//
// The rules, applied in this order:
//
//  1. the current snapshot is always kept;
//  2. every snapshot an Iceberg ref (branch or tag inside the table's metadata, not a
//     lakeFS tag) points at is always kept;
//  3. the minSnapshotsToKeep most recent snapshots (by timestamp-ms, newest first) are
//     kept whatever their age, so a reader of the previous snapshot keeps working;
//  4. of the remaining snapshots, every snapshot older than maxAge is expired. A snapshot
//     exactly at the boundary and a snapshot whose timestamp is in the future are kept:
//     both are the fail-safe direction.
//
// The returned slices are sorted by snapshot ID, so a report and a test are stable.
func SnapshotRetentionFor(metadata table.Metadata, now time.Time, maxAge time.Duration, minSnapshotsToKeep int) SnapshotRetention {
	var retention SnapshotRetention
	if metadata == nil {
		return retention
	}
	snapshots := metadata.Snapshots()
	if len(snapshots) == 0 {
		return retention
	}
	keep := make(map[int64]string, len(snapshots))
	if current := metadata.CurrentSnapshot(); current != nil {
		keep[current.SnapshotID] = SnapshotReasonCurrent
	}
	for name, ref := range metadata.Refs() {
		// A lakeFS tag or branch does not appear here; these are Iceberg snapshot refs
		// inside metadata.json (branch and tag refs of the table itself). They are
		// protected for the same reason the current snapshot is: a reader can resolve
		// them without consulting lakeFS.
		if _, already := keep[ref.SnapshotID]; !already {
			keep[ref.SnapshotID] = SnapshotReasonRef + ":" + name
		}
	}

	newest := make([]table.Snapshot, len(snapshots))
	copy(newest, snapshots)
	sort.SliceStable(newest, func(i, j int) bool {
		if newest[i].TimestampMs != newest[j].TimestampMs {
			return newest[i].TimestampMs > newest[j].TimestampMs
		}
		return newest[i].SnapshotID > newest[j].SnapshotID
	})
	for index, snapshot := range newest {
		if index >= minSnapshotsToKeep {
			break
		}
		if _, already := keep[snapshot.SnapshotID]; !already {
			keep[snapshot.SnapshotID] = SnapshotReasonMinSnapshotsToKeep
		}
	}

	expireBefore := now.Add(-maxAge)
	ordered := make([]table.Snapshot, len(snapshots))
	copy(ordered, snapshots)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].SnapshotID < ordered[j].SnapshotID })
	for _, snapshot := range ordered {
		if reason, kept := keep[snapshot.SnapshotID]; kept {
			retention.Keep = append(retention.Keep, RetainedSnapshot{
				SnapshotID:  snapshot.SnapshotID,
				TimestampMs: snapshot.TimestampMs,
				Reason:      reason,
			})
			continue
		}
		timestamp := time.UnixMilli(snapshot.TimestampMs).UTC()
		if !timestamp.Before(expireBefore) {
			retention.Keep = append(retention.Keep, RetainedSnapshot{
				SnapshotID:  snapshot.SnapshotID,
				TimestampMs: snapshot.TimestampMs,
				Reason:      SnapshotReasonNewerThanMaxAge,
			})
			continue
		}
		retention.Expire = append(retention.Expire, ExpiredSnapshot{
			SnapshotID:  snapshot.SnapshotID,
			TimestampMs: snapshot.TimestampMs,
		})
	}
	return retention
}

// expireSettings is one table's effective configuration.
type expireSettings struct {
	maxSnapshotAge     time.Duration
	minSnapshotsToKeep int
	gcEnabled          bool
}

// settingsFor resolves a table's effective settings: the run's defaults overridden by the
// table's own properties. A property that does not parse is an error rather than something
// to ignore: silently falling back to a smaller retention could expire snapshots the
// table's owner asked to keep.
func settingsFor(metadata table.Metadata, defaults Config) (expireSettings, error) {
	settings := expireSettings{
		maxSnapshotAge:     defaults.MaxSnapshotAge,
		minSnapshotsToKeep: defaults.MinSnapshotsToKeep,
		gcEnabled:          true,
	}
	properties := metadata.Properties()
	if raw := strings.TrimSpace(properties[PropertyLakeFSGCEnabled]); strings.EqualFold(raw, "false") {
		settings.gcEnabled = false
	}
	if raw := strings.TrimSpace(properties[PropertyMaxSnapshotAgeMs]); raw != "" {
		milliseconds, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || milliseconds <= 0 {
			return settings, fmt.Errorf("%w: %s is %q, not a positive number of milliseconds",
				ErrInvalidProperty, PropertyMaxSnapshotAgeMs, raw)
		}
		settings.maxSnapshotAge = time.Duration(milliseconds) * time.Millisecond
	}
	if raw := strings.TrimSpace(properties[PropertyMinSnapshotsToKeep]); raw != "" {
		keep, err := strconv.Atoi(raw)
		if err != nil || keep < 0 {
			return settings, fmt.Errorf("%w: %s is %q, not a non-negative integer",
				ErrInvalidProperty, PropertyMinSnapshotsToKeep, raw)
		}
		settings.minSnapshotsToKeep = keep
	}
	return settings, nil
}

// expireAction is one table's planned rewrite. Applying it is a pure sequence of lakeFS
// calls, which is what keeps the apply step (and its tests) readable.
type expireAction struct {
	branch      string
	pointerPath string
	tableUUID   uuid.UUID
	root        string

	previous    icebergcatalog.TablePointer
	next        table.Metadata
	nextJSON    []byte
	newLocation string
}

// expire runs phase 1 and writes its report.
func (r *Runner) expire(ctx context.Context) (*ExpireReport, error) {
	report := &ExpireReport{
		Version:       ReportVersion,
		Kind:          ReportKindExpire,
		Repository:    r.repo.Name,
		RunID:         r.runID,
		DefaultBranch: r.repo.DefaultBranch,
		DryRun:        r.dryRun,
		StartedAt:     r.now,
		Options:       r.optionsReport(),
	}
	branches, err := r.api.ListBranches(ctx, r.repo.Name)
	if err != nil {
		return report, err
	}
	sort.Strings(branches)
	report.Totals.Branches = len(branches)

	// Pass 1: plan every branch and every table. Nothing is written here, so the
	// ErrEmbeddedManifests abort below happens before the first staged change.
	// A plan refers to its table report by (branch, index) rather than by pointer: an
	// append to a branch's Tables slice may reallocate it, and a pointer taken before the
	// reallocation would update a copy that never reaches the report.
	type plannedTable struct {
		action *expireAction
		branch *BranchExpireReport
		index  int
	}
	planned := make([]plannedTable, 0, len(branches))
	branchReports := make(map[string]*BranchExpireReport, len(branches))
	for _, branch := range branches {
		branchReport := &BranchExpireReport{Branch: branch}
		branchReports[branch] = branchReport
		pointers, err := r.listTablePointers(ctx, branch)
		if err != nil {
			report.Warnings = append(report.Warnings, fmt.Sprintf("branch %s: %v", branch, err))
			continue
		}
		for _, pointer := range pointers {
			action, tableReport, err := r.planExpire(ctx, branch, pointer)
			if err != nil {
				report.FinishedAt = r.now
				return report, err
			}
			branchReport.Tables = append(branchReport.Tables, *tableReport)
			planned = append(planned, plannedTable{
				action: action,
				branch: branchReport,
				index:  len(branchReport.Tables) - 1,
			})
		}
	}

	// Pass 2: apply. A failure of one table is reported and the run continues, because a
	// single unreadable table should not block the cleanup of every other table.
	for _, table := range planned {
		if table.action == nil {
			continue
		}
		if err := r.applyExpire(ctx, table.action); err != nil {
			tableReport := &table.branch.Tables[table.index]
			if errors.Is(err, errChangedConcurrently) {
				tableReport.Status = StatusSkipped
				tableReport.Reason = ReasonChangedConcurrent
				continue
			}
			tableReport.Status = StatusFailed
			tableReport.Error = err.Error()
		}
	}

	for _, branch := range branches {
		branchReport, ok := branchReports[branch]
		if !ok {
			continue
		}
		totals := ExpireTotals{Tables: len(branchReport.Tables)}
		for _, table := range branchReport.Tables {
			switch table.Status {
			case StatusExpired:
				totals.Changed++
				totals.SnapshotsExpired += len(table.ExpiredIDs)
			case StatusUnchanged:
				totals.Unchanged++
			case StatusSkipped:
				totals.Skipped++
			case StatusFailed:
				totals.Failed++
			}
		}
		branchReport.Totals = totals
		report.Branches = append(report.Branches, *branchReport)
		report.Totals.Tables += totals.Tables
		report.Totals.Changed += totals.Changed
		report.Totals.Unchanged += totals.Unchanged
		report.Totals.Skipped += totals.Skipped
		report.Totals.Failed += totals.Failed
		report.Totals.SnapshotsExpired += totals.SnapshotsExpired
	}

	report.FinishedAt = r.now
	data, err := marshalJSON(report)
	if err != nil {
		return report, fmt.Errorf("marshal the expire report: %w", err)
	}
	if err := r.writeReport(ctx, r.repo.DefaultBranch, reportPath(ExpireReportsPrefix, r.runID, SummaryFileName), data); err != nil {
		return report, err
	}
	return report, nil
}

// errChangedConcurrently says a table pointer moved between planning and applying. It is
// the tool's optimistic-concurrency check: the lakeFS object API stages a pointer without a
// compare-and-swap (StageObject has no If-Match), so the only way not to overwrite a
// concurrent catalog commit is to read the pointer again immediately before writing it and
// to leave the table alone when it moved.
var errChangedConcurrently = errors.New("the table pointer changed while the run was planning")

// planExpire resolves one table pointer into an action, or explains why it does not need
// one. A non-nil error is fatal to the whole run (ErrEmbeddedManifests); every other
// outcome is a table report.
func (r *Runner) planExpire(ctx context.Context, branch string, entry ObjectInfo) (*expireAction, *TableExpireReport, error) {
	report := &TableExpireReport{PointerPath: entry.Path, Status: StatusSkipped}

	pointer, err := r.pointerFor(ctx, branch, entry)
	if err != nil {
		report.Status = StatusFailed
		report.Error = err.Error()
		return nil, report, nil
	}
	report.TableUUID = pointer.TableUUID

	if !inTableArea(r.repo.StorageNamespace, pointer.MetadataLocation) {
		report.Reason = ReasonExternalMetadata
		return nil, report, nil
	}
	tableUUID, err := uuid.Parse(pointer.TableUUID)
	if err != nil {
		report.Status = StatusFailed
		report.Error = fmt.Sprintf("table UUID %q is not a UUID: %v", pointer.TableUUID, err)
		return nil, report, nil
	}

	metadata, root, err := r.readTableMetadata(ctx, branch, tableUUID, pointer.MetadataLocation)
	if err != nil {
		if isNotFound(err) {
			// The catalog has a pointer but no registration of the metadata file: a table
			// whose metadata is managed outside this catalog (registered, or written by a
			// client that never staged its files). This job does not rewrite those.
			report.Reason = ReasonExternalMetadata
			return nil, report, nil
		}
		report.Status = StatusFailed
		report.Error = err.Error()
		return nil, report, nil
	}
	if hasEmbeddedManifests(metadata) {
		return nil, report, fmt.Errorf("%w: table %s on branch %s (metadata %s)",
			ErrEmbeddedManifests, entry.Path, branch, pointer.MetadataLocation)
	}

	settings, err := settingsFor(metadata, r.cfg)
	if err != nil {
		report.Status = StatusFailed
		report.Error = err.Error()
		return nil, report, nil
	}
	if !settings.gcEnabled {
		report.Reason = ReasonGCDisabled
		return nil, report, nil
	}

	retention := SnapshotRetentionFor(metadata, r.now, settings.maxSnapshotAge, settings.minSnapshotsToKeep)
	report.KeptSnapshots = len(retention.Keep)
	report.MetadataLocation = pointer.MetadataLocation
	if len(retention.Expire) == 0 {
		report.Status = StatusUnchanged
		report.Reason = ReasonNoSnapshots
		return nil, report, nil
	}

	expireIDs := make([]int64, 0, len(retention.Expire))
	for _, snapshot := range retention.Expire {
		expireIDs = append(expireIDs, snapshot.SnapshotID)
	}
	next, data, err := rewriteMetadata(metadata, pointer.MetadataLocation, expireIDs)
	if err != nil {
		report.Status = StatusFailed
		report.Error = err.Error()
		return nil, report, nil
	}
	newLocation, err := icebergcatalog.NewMetadataFileLocation(metadata.Location(), pointer.MetadataLocation, next)
	if err != nil {
		report.Status = StatusFailed
		report.Error = err.Error()
		return nil, report, nil
	}
	if !inTableArea(r.repo.StorageNamespace, newLocation) {
		report.Status = StatusFailed
		report.Error = fmt.Sprintf("the new metadata location %q is outside the table area", newLocation)
		return nil, report, nil
	}

	report.Status = StatusExpired
	report.ExpiredIDs = expireIDs
	report.PreviousMetadataLocation = pointer.MetadataLocation
	report.MetadataLocation = newLocation
	return &expireAction{
		branch:      branch,
		pointerPath: entry.Path,
		tableUUID:   tableUUID,
		root:        root,
		previous:    pointer,
		next:        next,
		nextJSON:    data,
		newLocation: newLocation,
	}, report, nil
}

// rewriteMetadata applies the expire and returns the new document and its bytes. The bytes
// are parsed back before they are returned: a document this tool wrote but cannot read
// would be an unrecoverable table for every reader, so it must never be staged.
func rewriteMetadata(metadata table.Metadata, currentLocation string, expireIDs []int64) (table.Metadata, []byte, error) {
	builder, err := table.MetadataBuilderFromBase(metadata, currentLocation)
	if err != nil {
		return nil, nil, fmt.Errorf("build table metadata: %w", err)
	}
	if err := builder.RemoveSnapshots(expireIDs, false); err != nil {
		return nil, nil, fmt.Errorf("remove snapshots %v: %w", expireIDs, err)
	}
	next, err := builder.Build()
	if err != nil {
		return nil, nil, fmt.Errorf("build the expired table metadata: %w", err)
	}
	data, err := json.Marshal(next)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal the expired table metadata: %w", err)
	}
	if _, err := table.ParseMetadataBytes(data); err != nil {
		return nil, nil, fmt.Errorf("the rewritten table metadata does not parse back: %w", err)
	}
	return next, data, nil
}

// applyExpire stages one planned rewrite on its branch.
func (r *Runner) applyExpire(ctx context.Context, action *expireAction) error {
	if err := r.verifyPointerUnchanged(ctx, action); err != nil {
		return err
	}
	if r.dryRun {
		return nil
	}

	uploadKey, err := storageKey(r.repo.StorageNamespace, action.newLocation)
	if err != nil {
		return err
	}
	uploaded, err := r.api.UploadObject(ctx, r.repo.Name, action.branch, uploadKey, action.nextJSON)
	if err != nil {
		return fmt.Errorf("upload the new metadata file: %w", err)
	}
	if normalizeAddress(uploaded.PhysicalAddress) != normalizeAddress(action.newLocation) {
		return fmt.Errorf("%w: the new metadata file was written to %q instead of %q",
			ErrInvalidCatalogState, uploaded.PhysicalAddress, action.newLocation)
	}

	size := uploaded.SizeBytes
	if size <= 0 {
		size = int64(len(action.nextJSON))
	}
	modified := time.UnixMilli(action.next.LastUpdatedMillis()).UTC()
	entryPath, err := fileEntryPath(action.tableUUID, action.root, action.newLocation)
	if err != nil {
		return fmt.Errorf("register the new metadata file: %w", err)
	}
	// Register the metadata file under the catalog's own file key first: from this point
	// on the object is protected the same way every other table file is, so the pointer
	// swap below can never expose an unprotected metadata file.
	if err := r.api.StageObject(ctx, r.repo.Name, action.branch, entryPath, StagedObject{
		PhysicalAddress: uploaded.PhysicalAddress,
		Checksum:        uploaded.Checksum,
		SizeBytes:       size,
		Mtime:           modified,
	}); err != nil {
		return fmt.Errorf("register the new metadata file %q: %w", entryPath, err)
	}

	// The pointer is the last write: it is what a reader resolves, and it now names a file
	// that is already registered and already present.
	if err := r.verifyPointerUnchanged(ctx, action); err != nil {
		return err
	}
	pointerMetadata, err := icebergcatalog.EncodeTablePointer(icebergcatalog.TablePointer{
		MetadataLocation: action.newLocation,
		TableUUID:        action.tableUUID.String(),
		FormatVersion:    action.next.Version(),
	})
	if err != nil {
		return fmt.Errorf("encode the new table pointer: %w", err)
	}
	if err := r.api.StageObject(ctx, r.repo.Name, action.branch, action.pointerPath, StagedObject{
		PhysicalAddress: uploaded.PhysicalAddress,
		Checksum:        uploaded.Checksum,
		SizeBytes:       size,
		Mtime:           modified,
		Metadata:        pointerMetadata,
	}); err != nil {
		return fmt.Errorf("stage the table pointer %q: %w", action.pointerPath, err)
	}

	// The upload also staged an ordinary entry at the physical key (_iceberg/...), which is
	// not where a table file lives in lakeFS. Removing it leaves the catalog's registration
	// as the only one, exactly like a catalog commit. A failure here is harmless - the
	// duplicate entry protects the same object - so it does not fail the table.
	_ = r.api.DeleteObject(ctx, r.repo.Name, action.branch, uploadKey)
	return nil
}

// verifyPointerUnchanged re-reads the pointer and refuses to act if it no longer names the
// metadata the plan was computed from.
func (r *Runner) verifyPointerUnchanged(ctx context.Context, action *expireAction) error {
	entry, err := r.api.StatObject(ctx, r.repo.Name, action.branch, action.pointerPath)
	if err != nil {
		return fmt.Errorf("re-read the table pointer %q: %w", action.pointerPath, err)
	}
	pointer, err := icebergcatalog.DecodeTablePointer(entry.Metadata)
	if err != nil {
		return fmt.Errorf("decode the table pointer %q: %w", action.pointerPath, err)
	}
	if pointer.MetadataLocation != action.previous.MetadataLocation {
		return fmt.Errorf("%w: %s is %q, planned from %q", errChangedConcurrently,
			action.pointerPath, pointer.MetadataLocation, action.previous.MetadataLocation)
	}
	return nil
}

// pointerFor returns the pointer of a listed table entry, falling back to StatObject when
// the listing did not carry user metadata (a server or proxy that stripped it).
func (r *Runner) pointerFor(ctx context.Context, ref string, entry ObjectInfo) (icebergcatalog.TablePointer, error) {
	if len(entry.Metadata) > 0 {
		if pointer, err := icebergcatalog.DecodeTablePointer(entry.Metadata); err == nil {
			return pointer, nil
		}
	}
	stat, err := r.api.StatObject(ctx, r.repo.Name, ref, entry.Path)
	if err != nil {
		return icebergcatalog.TablePointer{}, fmt.Errorf("stat table pointer %q: %w", entry.Path, err)
	}
	pointer, err := icebergcatalog.DecodeTablePointer(stat.Metadata)
	if err != nil {
		return icebergcatalog.TablePointer{}, fmt.Errorf("decode table pointer %q: %w", entry.Path, err)
	}
	return pointer, nil
}

// listTablePointers lists every table pointer visible on ref, staged ones included: a table
// created on a branch but not yet committed is a table that branch's clients can read, so
// it is a table this job must expire.
func (r *Runner) listTablePointers(ctx context.Context, ref string) ([]ObjectInfo, error) {
	var pointers []ObjectInfo
	after := ""
	for {
		objects, hasMore, err := r.api.ListObjects(ctx, r.repo.Name, ref, tablesPrefix, after)
		if err != nil {
			return nil, err
		}
		for _, object := range objects {
			if isPointerPath(object.Path) {
				pointers = append(pointers, object)
			}
		}
		if !hasMore {
			break
		}
		if len(objects) == 0 {
			return nil, fmt.Errorf("%w: list table pointers at %q: more results without a page", ErrBadPagination, ref)
		}
		after = objects[len(objects)-1].Path
	}
	sort.Slice(pointers, func(i, j int) bool { return pointers[i].Path < pointers[j].Path })
	return pointers, nil
}

// readTableMetadata resolves metadata.json through its catalog registration. The table root
// is not known before the document is read (the document declares it), so the two shapes
// the catalog and every Iceberg writer use are tried:
//
//	<root>/metadata/<name>   -> the catalog's own layout and the Iceberg convention
//	<root>/<name>            -> a document that sits in the table root itself
//
// The first root whose entry exists and parses wins; the document's own location is then
// authoritative for every file below it.
func (r *Runner) readTableMetadata(ctx context.Context, ref string, tableUUID uuid.UUID, location string) (table.Metadata, string, error) {
	var lastErr error
	for _, root := range metadataRootCandidates(location) {
		metadata, err := r.index.readMetadata(ctx, ref, tableUUID, root, location)
		if err == nil {
			resolved := strings.TrimRight(metadata.Location(), "/")
			if resolved == "" {
				resolved = root
			}
			return metadata, resolved, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("%w: cannot derive a table root from %q", ErrInvalidLayout, location)
	}
	return nil, "", lastErr
}

// maxMetadataRootCandidates is the number of roots metadataRootCandidates can return: the
// table root and the metadata directory below it.
const maxMetadataRootCandidates = 2

// metadataRootCandidates returns the roots to try for a metadata location, most likely
// first and without duplicates.
func metadataRootCandidates(location string) []string {
	location = strings.TrimRight(location, "/")
	if location == "" {
		return nil
	}
	parent := location
	if index := strings.LastIndex(location, "/"); index >= 0 {
		parent = location[:index]
	}
	roots := make([]string, 0, maxMetadataRootCandidates)
	if index := strings.LastIndex(parent, "/"); index >= 0 {
		grandparent := parent[:index]
		if parent == grandparent+"/"+icebergcatalog.MetadataDirectory {
			roots = append(roots, grandparent)
		}
	}
	if len(roots) == 0 || roots[len(roots)-1] != parent {
		roots = append(roots, parent)
	}
	return roots
}
