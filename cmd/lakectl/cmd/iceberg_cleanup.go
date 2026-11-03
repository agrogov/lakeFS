package cmd

// iceberg_cleanup.go is the command-line surface of the native OSS "Tables Cleanup" job
// (pkg/icebergcatalog/cleanup): it wires the flags of the Enterprise job to the package's
// two phases and prints one human-readable summary per phase.
//
// The command deliberately has no storage flags. The cleanup job works through the lakeFS
// API only, so the only connection it needs is the one every other lakectl command uses
// (--server-endpoint, the credentials in the lakectl config file, or a token), and it needs
// no blockstore credentials at all. The physical bytes are reclaimed by lakeFS' own GC after
// the sweep has removed the registrations - see the package documentation for why that is
// the safe direction and not a shortcut.

import (
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/icebergcatalog/cleanup"
)

const (
	// defaultMaxSnapshotAgeDays is the default retention of the command: five days, the
	// Enterprise default and the Iceberg default of
	// history.expire.max-snapshot-age-ms (a table can override it).
	defaultMaxSnapshotAgeDays = 5

	// defaultMinSnapshotsToKeep is the default number of most recent snapshots kept
	// regardless of their age. Two keeps a reader of the previous snapshot working while a
	// new one is written.
	defaultMinSnapshotsToKeep = 2

	// defaultSafetyAge is the default minimum age of a table file whose registration the
	// sweep may delete. It covers an in-flight write that the catalog has not staged yet;
	// the package refuses anything shorter than 12h.
	defaultSafetyAge = 72 * time.Hour
)

var (
	// cleanupRepository is the repository the run cleans up. It is a flag rather than a
	// positional URI because the job works on a whole repository, not on a branch or an
	// object: every branch's tables are expired, and the reports go to the default branch.
	cleanupRepository string

	// cleanupMaxSnapshotAgeDays and cleanupMinSnapshotsToKeep are the run's defaults; a
	// table's history.expire.* properties override them.
	cleanupMaxSnapshotAgeDays int
	cleanupMinSnapshotsToKeep int

	// cleanupSafetyAge is the minimum age a table file must have before its registration
	// may be swept.
	cleanupSafetyAge time.Duration

	// cleanupDryRun plans everything and writes nothing.
	cleanupDryRun bool

	// cleanupMarkOnly stops after the mark phase; cleanupSweepOnly runs only the sweep of
	// an earlier mark named by cleanupMarkID.
	cleanupMarkOnly  bool
	cleanupSweepOnly bool
	cleanupMarkID    string
)

var icebergCleanupCmd = &cobra.Command{
	Use:   "cleanup",
	Short: "Expire old Iceberg snapshots and sweep unreferenced table files",
	Long: `Expire old snapshots of every Iceberg table on every branch, then mark and sweep the
table files no live reference protects.

Phase 1 (expire) rewrites each table's metadata through the lakeFS API and stages the result
on the branch that holds the table: the branch's current snapshot and every snapshot an
Iceberg ref or tag points at are kept, the newest history.expire.min-snapshots-to-keep
snapshots are kept, and older snapshots than history.expire.max-snapshot-age-ms (or
--max-snapshot-age-days) are removed. A table with lakefs-gc.enabled=false is skipped, a table
whose metadata is managed outside the catalog is skipped, and a v1 table whose snapshots embed
their manifest locations aborts the run before anything is written.

Phase 2 (mark and sweep) removes the lakeFS registrations of table files that no branch head,
tag, GC-retained commit or staged change references, and that are older than --safety-age.
LakeFS' own garbage collection then reclaims the objects. Nothing is committed: every change
is staged, so the run is reviewable with 'lakectl diff' and undoable with 'lakectl branch revert'.

Reports are written under _lakefs/retention/iceberg/expire/<run-id>/summary.json and
_lakefs/retention/iceberg/sweep/<run-id>/{summary.json,objects.jsonl}.`,
	Example: `  # Full run: expire, mark, sweep - staging everything on the branches
  lakectl iceberg cleanup --repo my-repo

  # Rehearsal: compute and print the decisions, write nothing
  lakectl iceberg cleanup --repo my-repo --dry-run

  # Mark only (expire + mark), then inspect the marked objects before deleting anything
  lakectl iceberg cleanup --repo my-repo --mark-only
  lakectl fs cat lakefs://my-repo/main/_lakefs/retention/iceberg/sweep/<run-id>/objects.jsonl

  # Sweep a previous mark in a later, separate run
  lakectl iceberg cleanup --repo my-repo --sweep-only --mark-id <run-id>`,
	Args: cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		config := cleanup.Config{
			Repository:         cleanupRepository,
			MaxSnapshotAge:     time.Duration(cleanupMaxSnapshotAgeDays) * 24 * time.Hour,
			MinSnapshotsToKeep: cleanupMinSnapshotsToKeep,
			SafetyAge:          cleanupSafetyAge,
			DryRun:             cleanupDryRun,
			MarkOnly:           cleanupMarkOnly,
			SweepOnly:          cleanupSweepOnly,
			MarkID:             cleanupMarkID,
		}
		runner, err := cleanup.New(cleanup.NewClient(getClient(), 0), config)
		if err != nil {
			DieErr(err)
		}
		result, err := runner.Run(cmd.Context())
		writeCleanupResult(cmd.OutOrStdout(), cleanupRepository, config, result)
		if err != nil {
			DieErr(err)
		}
	},
}

// writeCleanupResult prints one line per phase and the report paths. It prints whatever
// phases completed even when the run failed, so an operator sees how far it got before the
// error message.
func writeCleanupResult(w io.Writer, repository string, config cleanup.Config, result *cleanup.Result) {
	mode := "run"
	if config.DryRun {
		mode = "dry run (nothing was written)"
	}
	fmt.Fprintf(w, "Iceberg cleanup of %s: %s\n", repository, mode)
	if result == nil {
		return
	}
	if expire := result.Expire; expire != nil {
		fmt.Fprintf(w, "expire: %d branches, %d tables: %d expired (%d snapshots), %d unchanged, %d skipped, %d failed\n",
			expire.Totals.Branches, expire.Totals.Tables, expire.Totals.Changed,
			expire.Totals.SnapshotsExpired, expire.Totals.Unchanged, expire.Totals.Skipped, expire.Totals.Failed)
		if !config.DryRun {
			fmt.Fprintf(w, "  report: %s/%s\n", cleanup.ExpireReportsPrefix+"/"+expire.RunID, cleanup.SummaryFileName)
		}
		for _, warning := range expire.Warnings {
			fmt.Fprintf(w, "  warning: %s\n", warning)
		}
	}
	if mark := result.Mark; mark != nil {
		fmt.Fprintf(w, "mark: %d registrations, %d candidates: %d marked (%d bytes), %d referenced, %d younger than safety age, %d unreadable table\n",
			mark.Totals.Registrations, mark.Totals.Candidates, mark.Totals.Marked,
			mark.Totals.MarkedBytes, mark.Totals.Referenced, mark.Totals.YoungerThanSafety, mark.Totals.UnknownTable)
		if !config.DryRun {
			fmt.Fprintf(w, "  mark id: %s\n  report: %s/%s\n", mark.MarkID,
				cleanup.SweepReportsPrefix+"/"+mark.RunID, cleanup.SummaryFileName)
		}
		for _, warning := range mark.Warnings {
			fmt.Fprintf(w, "  warning: %s\n", warning)
		}
	}
	if sweep := result.Sweep; sweep != nil {
		fmt.Fprintf(w, "sweep: %d marked: %d objects (%d registrations, %d bytes) removed, %d skipped, %d failed\n",
			sweep.Totals.Marked, sweep.Totals.DeletedObjects, sweep.Totals.DeletedEntries,
			sweep.Totals.DeletedBytes,
			sweep.Totals.Referenced+sweep.Totals.YoungerThanSafety+sweep.Totals.UnknownTable+sweep.Totals.Gone,
			sweep.Totals.Failed)
		if !config.DryRun {
			fmt.Fprintf(w, "  report: %s/%s\n", cleanup.SweepReportsPrefix+"/"+sweep.RunID, cleanup.SummaryFileName)
		}
		for _, warning := range sweep.Warnings {
			fmt.Fprintf(w, "  warning: %s\n", warning)
		}
	}
	if !config.DryRun {
		fmt.Fprintln(w, "All changes are staged; review them with 'lakectl diff' and commit them with 'lakectl commit'.")
	}
}

//nolint:gochecknoinits
func init() {
	icebergCmd := &cobra.Command{
		Use:   "iceberg",
		Short: "Manage the Iceberg tables in a repository",
	}
	icebergCleanupCmd.Flags().StringVar(&cleanupRepository, "repo", "", "repository to clean up")
	icebergCleanupCmd.Flags().IntVar(&cleanupMaxSnapshotAgeDays, "max-snapshot-age-days", defaultMaxSnapshotAgeDays,
		"expire snapshots older than this many days (a table's history.expire.max-snapshot-age-ms overrides it)")
	icebergCleanupCmd.Flags().IntVar(&cleanupMinSnapshotsToKeep, "min-snapshots-to-keep", defaultMinSnapshotsToKeep,
		"always keep this many most recent snapshots (a table's history.expire.min-snapshots-to-keep overrides it)")
	icebergCleanupCmd.Flags().DurationVar(&cleanupSafetyAge, "safety-age", defaultSafetyAge,
		"never sweep a table file younger than this (minimum 12h)")
	icebergCleanupCmd.Flags().BoolVar(&cleanupDryRun, "dry-run", false, "compute every decision and write nothing")
	icebergCleanupCmd.Flags().BoolVar(&cleanupMarkOnly, "mark-only", false, "expire and mark, but do not sweep")
	icebergCleanupCmd.Flags().BoolVar(&cleanupSweepOnly, "sweep-only", false, "sweep an earlier mark; requires --mark-id")
	icebergCleanupCmd.Flags().StringVar(&cleanupMarkID, "mark-id", "", "the run ID of the mark to sweep")
	_ = icebergCleanupCmd.MarkFlagRequired("repo")

	icebergCmd.AddCommand(icebergCleanupCmd)
	rootCmd.AddCommand(icebergCmd)
}
