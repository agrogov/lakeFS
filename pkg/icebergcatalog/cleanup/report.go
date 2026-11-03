package cleanup

// report.go defines the reports the job writes under _lakefs/retention/iceberg/ and the
// writer that stages them on the default branch.
//
// The report format follows the lakeFS GC reports' conventions
// (design/accepted/gc-two-steps-separation.md, _lakefs/retention/gc/...): a run has an ID,
// its intermediate results live under that ID so a later run can resume from them, and the
// summary names the run it belongs to and what it decided. Two differences are deliberate:
//
//   - the summary is JSON, not parquet. The job is native Go and has no Spark, and its
//     readers are an operator and a shell (`jq`), not a Spark job. One JSON document per
//     run is also self-describing: every string a report needs to quote (a table, a
//     reason, a server error) is in it without a schema.
//   - the marked objects are a JSON-lines file next to the summary rather than part of it,
//     so that a repository with hundreds of thousands of marked files still has a small
//     summary and a streamable object list (a sweep reads it page by page, it never needs
//     the whole list in memory).
//
// Nothing in this file decides anything: a report is emitted by a phase, and a report that
// is wrong is a bug in the phase, not a second source of truth for the next phase. That is
// why the sweep recomputes every reference at sweep time instead of trusting the mark
// report (doc.go "Safety argument").

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// ReportVersion is the schema version of every report this job writes. A reader must
// refuse a report it does not know rather than interpret unknown fields, because the
// object list is used to delete entries.
const ReportVersion = 1

// Report kinds. They let one reader (or a `jq` one-liner) tell the reports apart without
// relying on the directory name.
const (
	ReportKindExpire     = "iceberg-expire"
	ReportKindSweepMark  = "iceberg-sweep-mark"
	ReportKindSweepSweep = "iceberg-sweep"
)

// Table result statuses.
const (
	StatusExpired   = "expired"   // metadata rewritten and staged
	StatusUnchanged = "unchanged" // nothing to expire
	StatusSkipped   = "skipped"   // deliberately not touched
	StatusFailed    = "failed"    // could not be planned or applied
)

// Reasons a table is skipped or a file is protected. They are report strings: an operator
// must be able to see why nothing happened, and a test pins the important ones.
const (
	ReasonExternalMetadata   = "external-metadata"
	ReasonGCDisabled         = "lakefs-gc-disabled"
	ReasonNoSnapshots        = "no-snapshots-to-expire"
	ReasonChangedConcurrent  = "changed-concurrently"
	ReasonReferencedByBranch = "referenced-by-branch"
	ReasonReferencedByTag    = "referenced-by-tag"
	ReasonReferencedByCommit = "referenced-by-commit"
	ReasonReferencedByStaged = "referenced-by-staged-change"
	ReasonUnknownTable       = "table-unreadable"
	ReasonYoungerThanSafety  = "younger-than-safety-age"
	ReasonEntryGone          = "registration-gone"
)

// OptionsReport is the effective configuration of a run, quoted in every report so a
// report explains its own decisions without the command line that produced it.
type OptionsReport struct {
	MaxSnapshotAge     string `json:"max_snapshot_age"`
	MaxSnapshotAgeDays int    `json:"max_snapshot_age_days"`
	MinSnapshotsToKeep int    `json:"min_snapshots_to_keep"`
	SafetyAge          string `json:"safety_age"`
	DryRun             bool   `json:"dry_run"`
	MarkOnly           bool   `json:"mark_only"`
	SweepOnly          bool   `json:"sweep_only"`
	MarkID             string `json:"mark_id,omitempty"`
}

// ExpireReport is the _lakefs/retention/iceberg/expire/<run-id>/summary.json document.
type ExpireReport struct {
	Version       int                  `json:"version"`
	Kind          string               `json:"kind"`
	Repository    string               `json:"repository"`
	RunID         string               `json:"run_id"`
	DefaultBranch string               `json:"default_branch"`
	DryRun        bool                 `json:"dry_run"`
	StartedAt     time.Time            `json:"started_at"`
	FinishedAt    time.Time            `json:"finished_at"`
	Options       OptionsReport        `json:"options"`
	Branches      []BranchExpireReport `json:"branches"`
	Totals        ExpireTotals         `json:"totals"`
	Warnings      []string             `json:"warnings,omitempty"`
}

// ExpireTotals aggregates the per-branch counters.
type ExpireTotals struct {
	Branches         int `json:"branches"`
	Tables           int `json:"tables"`
	Changed          int `json:"changed"`
	Unchanged        int `json:"unchanged"`
	Skipped          int `json:"skipped"`
	Failed           int `json:"failed"`
	SnapshotsExpired int `json:"snapshots_expired"`
}

// BranchExpireReport is one branch's section of an expire report.
type BranchExpireReport struct {
	Branch string              `json:"branch"`
	Tables []TableExpireReport `json:"tables"`
	Totals ExpireTotals        `json:"totals"`
}

// TableExpireReport is one table's outcome.
type TableExpireReport struct {
	PointerPath   string  `json:"pointer_path"`
	TableUUID     string  `json:"table_uuid,omitempty"`
	Status        string  `json:"status"`
	Reason        string  `json:"reason,omitempty"`
	Error         string  `json:"error,omitempty"`
	KeptSnapshots int     `json:"kept_snapshots"`
	ExpiredIDs    []int64 `json:"expired_snapshot_ids,omitempty"`
	// PreviousMetadataLocation and MetadataLocation show the pointer swap a run staged,
	// which is what makes the report readable next to `lakectl diff`.
	PreviousMetadataLocation string `json:"previous_metadata_location,omitempty"`
	MetadataLocation         string `json:"metadata_location,omitempty"`
}

// MarkReport is the _lakefs/retention/iceberg/sweep/<run-id>/summary.json document of a
// mark run. The run ID is the mark ID a later --sweep-only run passes.
type MarkReport struct {
	Version       int           `json:"version"`
	Kind          string        `json:"kind"`
	Repository    string        `json:"repository"`
	RunID         string        `json:"run_id"`
	MarkID        string        `json:"mark_id"`
	DefaultBranch string        `json:"default_branch"`
	DryRun        bool          `json:"dry_run"`
	StartedAt     time.Time     `json:"started_at"`
	FinishedAt    time.Time     `json:"finished_at"`
	Options       OptionsReport `json:"options"`
	Refs          RefSummary    `json:"refs"`
	Totals        MarkTotals    `json:"totals"`
	Warnings      []string      `json:"warnings,omitempty"`
}

// RefSummary records which references the mark resolved, so an operator can check that
// the run saw the branches, tags and commits they expected.
type RefSummary struct {
	Branches        int  `json:"branches"`
	Tags            int  `json:"tags"`
	RetainedCommits int  `json:"retained_commits"`
	StagedBranches  int  `json:"branches_with_staged_changes"`
	AllHistory      bool `json:"all_commits_retained"`
	Tables          int  `json:"table_states"`
	UnknownTables   int  `json:"unreadable_tables"`
}

// MarkTotals aggregates the candidate decisions of a mark run.
type MarkTotals struct {
	Registrations     int   `json:"registrations"`
	Candidates        int   `json:"candidates"`
	Marked            int   `json:"marked"`
	MarkedBytes       int64 `json:"marked_bytes"`
	Referenced        int   `json:"referenced"`
	YoungerThanSafety int   `json:"younger_than_safety_age"`
	UnknownTable      int   `json:"unreadable_table"`
}

// MarkObject is one line of objects.jsonl: a physical file whose every registration is
// marked for removal, with the registrations a sweep will delete.
type MarkObject struct {
	Address       string             `json:"address"`
	TableUUID     string             `json:"table_uuid"`
	SizeBytes     int64              `json:"size_bytes"`
	LastModified  time.Time          `json:"last_modified"`
	Registrations []MarkRegistration `json:"registrations"`
}

// MarkRegistration is one lakeFS entry that registers a marked object.
type MarkRegistration struct {
	Branch string `json:"branch"`
	Path   string `json:"path"`
}

// SweepReport is the _lakefs/retention/iceberg/sweep/<run-id>/summary.json document of a
// sweep run.
type SweepReport struct {
	Version       int           `json:"version"`
	Kind          string        `json:"kind"`
	Repository    string        `json:"repository"`
	RunID         string        `json:"run_id"`
	MarkID        string        `json:"mark_id"`
	DefaultBranch string        `json:"default_branch"`
	DryRun        bool          `json:"dry_run"`
	StartedAt     time.Time     `json:"started_at"`
	FinishedAt    time.Time     `json:"finished_at"`
	Options       OptionsReport `json:"options"`
	Totals        SweepTotals   `json:"totals"`
	Skipped       []SweepSkip   `json:"skipped,omitempty"`
	Warnings      []string      `json:"warnings,omitempty"`
}

// SweepTotals aggregates a sweep run.
type SweepTotals struct {
	Marked            int   `json:"marked"`
	DeletedObjects    int   `json:"deleted_objects"`
	DeletedEntries    int   `json:"deleted_entries"`
	DeletedBytes      int64 `json:"deleted_bytes"`
	Referenced        int   `json:"skipped_referenced"`
	YoungerThanSafety int   `json:"skipped_younger_than_safety_age"`
	UnknownTable      int   `json:"skipped_unreadable_table"`
	Gone              int   `json:"skipped_registration_gone"`
	Failed            int   `json:"failed"`
}

// SweepSkip is one marked object a sweep refused to delete, with the reason. The list is
// capped (see appendSkip) because a sweep that protects a large unreadable table could
// otherwise write a report bigger than the repository.
type SweepSkip struct {
	Address string `json:"address"`
	Reason  string `json:"reason,omitempty"`
	Error   string `json:"error,omitempty"`
}

// maxReportedSkips caps the per-reason skip list in a sweep report. The totals stay exact;
// only the detail list is capped, and a warning says how many were elided.
const maxReportedSkips = 1000

// marshalJSON marshals one report with a trailing newline, so a report object is a
// well-formed text file (and `cat`/`jq` behave).
func marshalJSON(document any) ([]byte, error) {
	data, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

// marshalJSONLines marshals objects as JSON lines, one object per line.
func marshalJSONLines(objects []MarkObject) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	for _, object := range objects {
		if err := encoder.Encode(object); err != nil {
			return nil, err
		}
	}
	return buffer.Bytes(), nil
}

// unmarshalMarkObjects parses an objects.jsonl document, refusing a line it cannot read
// rather than skipping it: a line that is skipped is a file the sweep silently does not
// delete, and a report that cannot be parsed completely is not a mark this tool may act
// on.
func unmarshalMarkObjects(data []byte) ([]MarkObject, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	var objects []MarkObject
	for decoder.More() {
		var object MarkObject
		if err := decoder.Decode(&object); err != nil {
			return nil, fmt.Errorf("parse marked object %d: %w", len(objects)+1, err)
		}
		if object.Address == "" {
			return nil, fmt.Errorf("%w: marked object %d has no address", ErrInvalidMark, len(objects)+1)
		}
		objects = append(objects, object)
	}
	return objects, nil
}

// reportPath returns the report object path of one file of one run:
// <prefix>/<run-id>/<name>.
func reportPath(prefix, runID, name string) string {
	return prefix + "/" + runID + "/" + name
}

// writeReport uploads one report file on branch.
//
// A report is an ordinary lakeFS object: it is staged, not committed, exactly like the
// changes the run staged, and the operator's commit publishes both. That keeps one commit
// as the unit of "this cleanup happened", and it means a run that is reverted also
// reverts its report.
func (r *Runner) writeReport(ctx context.Context, branch, path string, data []byte) error {
	if r.dryRun {
		return nil
	}
	if _, err := r.api.UploadObject(ctx, r.repo.Name, branch, path, data); err != nil {
		return fmt.Errorf("write report %q: %w", path, err)
	}
	return nil
}
