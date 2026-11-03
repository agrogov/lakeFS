package cleanup

// sweep.go is phase 2: mark the table files no live reference protects, then sweep the
// registrations of the marked files.
//
// # What a candidate is
//
// A candidate is an address that a lakeFS entry under _lakefs_iceberg/files/ registers: the
// catalog's own per-file registration (pkg/icebergcatalog/files.go). Those entries are the
// sweep's unit of work, and there are three reasons for that:
//
//   - they are the only table files the tool can enumerate through the lakeFS API. The
//     objects themselves live in the storage namespace, and listing them would need
//     blockstore credentials, which this tool deliberately does not have (doc.go "Working
//     through the lakeFS API");
//   - a file with no registration is a file the catalog never staged, i.e. a file of an
//     in-flight write. It must not be touched, and not being a candidate is the simplest
//     way to guarantee that;
//   - removing a registration is exactly what makes an object collectable by lakeFS GC.
//     If a file is still referenced by anything live, the sweep keeps its registration and
//     the object stays protected. If it is not, removing the registration and committing
//     is what lets the GC reclaim it (doc.go "Working through the lakeFS API").
//
// # Why mark and sweep are separate
//
// Mark writes an object list under _lakefs/retention/iceberg/sweep/<run-id>/, and a sweep
// can be run later with --sweep-only --mark-id <run-id>. That separation is the lakeFS GC's
// own (design/accepted/gc-two-steps-separation.md) and it exists for the same two reasons:
// an operator can inspect and even hand-edit the list before anything is deleted, and a
// sweep that runs hours after the mark sees the references as they are *now*, which is why
// it re-derives them instead of trusting the list. The mark report is a list of
// *candidates to reconsider*, never a list of things to delete.
//
// # The decision
//
// For each candidate, in this order:
//
//  1. a reference could not be enumerated at all -> protect everything (unknownAll);
//  2. the address is referenced by a branch head, tag, GC-retained commit or staged change
//     -> skip;
//  3. the table's live set is unknown (unreadable metadata or manifests) -> skip;
//  4. the newest registration is younger than the safety age, or has no usable time ->
//     skip;
//  5. otherwise -> mark.
//
// The order is part of the safety argument: every "cannot tell" outcome is checked before
// the age rule, and the age rule is checked before the mark, so no failure path can fall
// through to "mark".

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// registration is one lakeFS entry that registers a physical table file.
type registration struct {
	branch       string
	path         string
	address      string
	tableUUID    uuid.UUID
	sizeBytes    int64
	lastModified time.Time
}

// candidate is the set of registrations of one physical address plus what the decision
// needs to know about it.
type candidate struct {
	address       string
	tableUUID     uuid.UUID
	sizeBytes     int64
	lastModified  time.Time
	registrations []registration
}

// candidateIndex groups registrations by normalized physical address.
type candidateIndex struct {
	byAddress map[string]*candidate
	order     []string
	total     int
}

// newCandidateIndex groups a repository's registrations.
func newCandidateIndex(registrations []registration) *candidateIndex {
	index := &candidateIndex{byAddress: make(map[string]*candidate, len(registrations))}
	for _, entry := range registrations {
		key := normalizeAddress(entry.address)
		if key == "" {
			continue
		}
		index.total++
		existing, ok := index.byAddress[key]
		if !ok {
			existing = &candidate{address: entry.address, tableUUID: entry.tableUUID}
			index.byAddress[key] = existing
			index.order = append(index.order, key)
		}
		existing.registrations = append(existing.registrations, entry)
		if entry.sizeBytes > existing.sizeBytes {
			existing.sizeBytes = entry.sizeBytes
		}
		// The age of a file is the age of its *newest* registration: one fresh
		// registration is enough to protect the object, which is the conservative choice
		// when a table is registered on an old branch and a new one at the same time.
		if entry.lastModified.After(existing.lastModified) {
			existing.lastModified = entry.lastModified
		}
	}
	return index
}

// mark runs the mark phase and writes its report. It returns the marked objects so a
// combined run can hand them to the sweep without reading its own report back.
func (r *Runner) mark(ctx context.Context) (*MarkReport, []MarkObject, error) {
	report := &MarkReport{
		Version:       ReportVersion,
		Kind:          ReportKindSweepMark,
		Repository:    r.repo.Name,
		RunID:         r.runID,
		MarkID:        r.runID,
		DefaultBranch: r.repo.DefaultBranch,
		DryRun:        r.dryRun,
		StartedAt:     r.now,
		Options:       r.optionsReport(),
	}

	view, refs, err := r.computeProtected(ctx)
	if err != nil {
		return report, nil, err
	}
	registrations, err := r.listRegistrations(ctx)
	if err != nil {
		return report, nil, err
	}
	index := newCandidateIndex(registrations)
	report.Refs = RefSummary{
		Branches:        len(refs.branches),
		Tags:            len(refs.tags),
		RetainedCommits: len(refs.retained),
		StagedBranches:  view.stagedBranches,
		AllHistory:      refs.allHistory,
		Tables:          view.tableStates,
		UnknownTables:   len(view.unknownTables),
	}
	report.Totals.Registrations = index.total
	report.Totals.Candidates = len(index.order)

	marked := make([]MarkObject, 0, len(index.order))
	for _, key := range index.order {
		candidate := index.byAddress[key]
		switch {
		case view.unknownAll != "":
			report.Warnings = appendOnce(report.Warnings, view.unknownAll)
			report.Totals.Referenced++
			continue
		case isReferenced(view, candidate):
			report.Totals.Referenced++
			continue
		case isUnknownTable(view, candidate):
			report.Totals.UnknownTable++
			continue
		case !olderThanSafetyAge(candidate.lastModified, r.now, r.cfg.SafetyAge):
			report.Totals.YoungerThanSafety++
			continue
		}
		report.Totals.Marked++
		report.Totals.MarkedBytes += candidate.sizeBytes
		marked = append(marked, MarkObject{
			Address:       candidate.address,
			TableUUID:     candidate.tableUUID.String(),
			SizeBytes:     candidate.sizeBytes,
			LastModified:  candidate.lastModified,
			Registrations: registrationsOf(candidate),
		})
	}
	sort.Slice(marked, func(i, j int) bool { return marked[i].Address < marked[j].Address })

	report.FinishedAt = r.now
	summary, err := marshalJSON(report)
	if err != nil {
		return report, marked, fmt.Errorf("marshal the mark report: %w", err)
	}
	objects, err := marshalJSONLines(marked)
	if err != nil {
		return report, marked, fmt.Errorf("marshal the marked objects: %w", err)
	}
	if err := r.writeReport(ctx, r.repo.DefaultBranch, reportPath(SweepReportsPrefix, r.runID, SummaryFileName), summary); err != nil {
		return report, marked, err
	}
	if err := r.writeReport(ctx, r.repo.DefaultBranch, reportPath(SweepReportsPrefix, r.runID, ObjectsFileName), objects); err != nil {
		return report, marked, err
	}
	return report, marked, nil
}

// sweep runs the sweep phase. When marked is nil it resumes the mark named by markID from
// its report; either way it re-derives every reference before deleting anything.
//
// A sweep that belongs to a mark of the same invocation writes its report under
// <mark-id>-sweep, so it does not overwrite the mark's own summary.json and objects.jsonl -
// the very files a later --sweep-only run (or an operator) reads. A --sweep-only run has no
// mark of its own and uses the run ID it was given.
func (r *Runner) sweep(ctx context.Context, markID string, marked []MarkObject) (*SweepReport, error) {
	sweepRunID := r.runID
	if marked != nil {
		sweepRunID = markID + "-sweep"
	}
	report := &SweepReport{
		Version:       ReportVersion,
		Kind:          ReportKindSweepSweep,
		Repository:    r.repo.Name,
		RunID:         sweepRunID,
		MarkID:        markID,
		DefaultBranch: r.repo.DefaultBranch,
		DryRun:        r.dryRun,
		StartedAt:     r.now,
		Options:       r.optionsReport(),
	}
	if marked == nil {
		resumed, err := r.readMark(ctx, markID)
		if err != nil {
			return report, err
		}
		marked = resumed
	}

	view, _, err := r.computeProtected(ctx)
	if err != nil {
		return report, err
	}
	registrations, err := r.listRegistrations(ctx)
	if err != nil {
		return report, err
	}
	index := newCandidateIndex(registrations)
	report.Totals.Marked = len(marked)

	var failures, failedObjects int
	for _, object := range marked {
		key := normalizeAddress(object.Address)
		current := index.byAddress[key]
		switch {
		case view.unknownAll != "":
			report.Totals.Referenced++
			report.Warnings = appendOnce(report.Warnings, view.unknownAll)
			continue
		case current == nil:
			report.Totals.Gone++
			report.appendSkip(SweepSkip{Address: object.Address, Reason: ReasonEntryGone})
			continue
		case isReferenced(view, current):
			report.Totals.Referenced++
			report.appendSkip(SweepSkip{Address: object.Address, Reason: referencedReason(view, current)})
			continue
		case isUnknownTable(view, current):
			report.Totals.UnknownTable++
			report.appendSkip(SweepSkip{Address: object.Address, Reason: ReasonUnknownTable})
			continue
		case !olderThanSafetyAge(current.lastModified, r.now, r.cfg.SafetyAge):
			report.Totals.YoungerThanSafety++
			report.appendSkip(SweepSkip{Address: object.Address, Reason: ReasonYoungerThanSafety})
			continue
		}
		deletedAny := false
		objectFailed := false
		for _, entry := range current.registrations {
			if r.dryRun {
				deletedAny = true
				report.Totals.DeletedEntries++
				continue
			}
			if err := r.api.DeleteObject(ctx, r.repo.Name, entry.branch, entry.path); err != nil {
				failures++
				objectFailed = true
				report.Totals.Failed++
				report.appendSkip(SweepSkip{Address: object.Address, Error: fmt.Sprintf("%s on %s: %v", entry.path, entry.branch, err)})
				continue
			}
			deletedAny = true
			report.Totals.DeletedEntries++
		}
		if objectFailed {
			failedObjects++
		}
		if deletedAny {
			report.Totals.DeletedObjects++
			report.Totals.DeletedBytes += current.sizeBytes
		}
	}

	report.FinishedAt = r.now
	summary, err := marshalJSON(report)
	if err != nil {
		return report, fmt.Errorf("marshal the sweep report: %w", err)
	}
	if err := r.writeReport(ctx, r.repo.DefaultBranch, reportPath(SweepReportsPrefix, sweepRunID, SummaryFileName), summary); err != nil {
		return report, err
	}
	if failures > 0 {
		return report, fmt.Errorf("%w: %d of %d marked objects could not be swept (%d registration deletions failed); see %s",
			ErrSweepIncomplete, failedObjects, len(marked), failures,
			reportPath(SweepReportsPrefix, sweepRunID, SummaryFileName))
	}
	return report, nil
}

// appendSkip records a skip, capped at maxReportedSkips. The totals remain exact.
func (report *SweepReport) appendSkip(skip SweepSkip) {
	if len(report.Skipped) >= maxReportedSkips {
		return
	}
	report.Skipped = append(report.Skipped, skip)
	if len(report.Skipped) == maxReportedSkips {
		report.Warnings = appendOnce(report.Warnings,
			fmt.Sprintf("the skip list is capped at %d entries; the totals are exact", maxReportedSkips))
	}
}

// readMark reads the mark report and object list of an earlier run from the repository.
// Both files are validated: the summary must be a mark report of this schema version with
// the same mark ID, and every line of the object list must parse. A mark that cannot be
// read completely is refused, because acting on part of it would be acting on a decision
// nobody made.
func (r *Runner) readMark(ctx context.Context, markID string) ([]MarkObject, error) {
	summaryPath := reportPath(SweepReportsPrefix, markID, SummaryFileName)
	summaryData, err := r.api.GetObject(ctx, r.repo.Name, r.repo.DefaultBranch, summaryPath)
	if err != nil {
		return nil, fmt.Errorf("read the mark report %q: %w", summaryPath, err)
	}
	var summary MarkReport
	if err := json.Unmarshal(summaryData, &summary); err != nil {
		return nil, fmt.Errorf("parse the mark report %q: %w", summaryPath, err)
	}
	if summary.Version != ReportVersion || summary.Kind != ReportKindSweepMark {
		return nil, fmt.Errorf("%w: %q is not a version %d mark report (version %d, kind %q)",
			ErrInvalidMark, summaryPath, ReportVersion, summary.Version, summary.Kind)
	}
	if summary.MarkID != markID {
		return nil, fmt.Errorf("%w: %q belongs to mark %q, not %q", ErrInvalidMark, summaryPath, summary.MarkID, markID)
	}

	objectsPath := reportPath(SweepReportsPrefix, markID, ObjectsFileName)
	objectsData, err := r.api.GetObject(ctx, r.repo.Name, r.repo.DefaultBranch, objectsPath)
	if err != nil {
		return nil, fmt.Errorf("read the marked objects %q: %w", objectsPath, err)
	}
	objects, err := unmarshalMarkObjects(objectsData)
	if err != nil {
		return nil, fmt.Errorf("parse the marked objects %q: %w", objectsPath, err)
	}
	return objects, nil
}

// listRegistrations returns every table-file registration visible on every branch. It is
// the candidate universe of the sweep: a file with no registration is never touched, and a
// file registered on several branches yields one registration per branch, because the
// sweep has to remove all of them for lakeFS GC to reclaim the object.
func (r *Runner) listRegistrations(ctx context.Context) ([]registration, error) {
	branches, err := r.api.ListBranches(ctx, r.repo.Name)
	if err != nil {
		return nil, err
	}
	sort.Strings(branches)
	var registrations []registration
	for _, branch := range branches {
		after := ""
		for {
			objects, hasMore, err := r.api.ListObjects(ctx, r.repo.Name, branch, filesPrefix, after)
			if err != nil {
				return nil, fmt.Errorf("list the table-file registrations of %q: %w", branch, err)
			}
			for _, object := range objects {
				tableUUID, ok := tableFileEntryUUID(object.Path)
				if !ok {
					// Not one of the catalog's file registrations (a directory marker, or
					// something else under the reserved prefix). It is not a candidate and
					// deleting it is not this tool's business.
					continue
				}
				registrations = append(registrations, registration{
					branch:       branch,
					path:         object.Path,
					address:      object.PhysicalAddress,
					tableUUID:    tableUUID,
					sizeBytes:    object.SizeBytes,
					lastModified: object.Mtime,
				})
			}
			if !hasMore {
				break
			}
			if len(objects) == 0 {
				return nil, fmt.Errorf("%w: list the table-file registrations of %q: more results without a page", ErrBadPagination, branch)
			}
			after = objects[len(objects)-1].Path
		}
	}
	return registrations, nil
}

// isReferenced reports whether the candidate address is protected by a live reference.
func isReferenced(view *protectedView, candidate *candidate) bool {
	_, ok := view.protect(candidate.address)
	return ok
}

// referencedReason returns the reason a candidate is protected, for the report.
func referencedReason(view *protectedView, candidate *candidate) string {
	reason, _ := view.protect(candidate.address)
	if reason == "" {
		return ReasonReferencedByBranch
	}
	return reason
}

// isUnknownTable reports whether every file of the candidate's table is protected because
// the table's live set could not be read.
func isUnknownTable(view *protectedView, candidate *candidate) bool {
	_, ok := view.protectTable(candidate.tableUUID)
	return ok
}

// olderThanSafetyAge reports whether a registration is old enough for its object to be
// swept. A zero or future timestamp is never old enough: both mean the age is not known to
// be safe, and protecting an object costs storage while deleting one costs data.
func olderThanSafetyAge(lastModified, now time.Time, safetyAge time.Duration) bool {
	if lastModified.IsZero() {
		return false
	}
	return now.Sub(lastModified) >= safetyAge
}

// registrationsOf converts a candidate's registrations into the report shape, sorted so a
// report is deterministic.
func registrationsOf(candidate *candidate) []MarkRegistration {
	registrations := make([]MarkRegistration, 0, len(candidate.registrations))
	for _, entry := range candidate.registrations {
		registrations = append(registrations, MarkRegistration{Branch: entry.branch, Path: entry.path})
	}
	sort.Slice(registrations, func(i, j int) bool {
		if registrations[i].Branch != registrations[j].Branch {
			return registrations[i].Branch < registrations[j].Branch
		}
		return registrations[i].Path < registrations[j].Path
	})
	return registrations
}

// appendOnce appends a warning if it is not already present. A per-candidate warning would
// otherwise repeat the same server error once per file.
func appendOnce(warnings []string, warning string) []string {
	if warning == "" {
		return warnings
	}
	for _, existing := range warnings {
		if existing == warning {
			return warnings
		}
	}
	return append(warnings, warning)
}
