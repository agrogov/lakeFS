package indexer

// maintenance.go is the table maintenance of design §4.9. The indexer does not expire snapshots or
// sweep files - lakectl iceberg cleanup does that on a schedule, honouring the mds-* tags - so what
// is left here are the two things only the indexer knows when to do:
//
//   - compaction: merge-on-read deltas accumulate position delete files, and a table that reads too
//     many of them is rewritten from its own rows under the same lakefs.mds.source-commit;
//   - tag retention: retire mds-* tags older than tag_retention, which is what lets lakeFS GC
//     reclaim the data of source commits nobody queries any more.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/mds/client"
)

// compactMessagePrefix and indexMessagePrefix tell the two kinds of commit of R-metadata apart
// when the every_commits count is read back from its log: a compaction commit is "mds: compact
// <R>@<C>" (§4.9) and an indexing commit is "mds: index <R>@<C>" (§4.3 step 4), which are
// CompactCommitMessage and IndexCommitMessage without their operands.
const (
	compactMessagePrefix = "mds: compact "
	indexMessagePrefix   = "mds: index "
)

// compactIfNeeded rewrites the branch's table when §4.9's thresholds are crossed: more than
// compaction.max_delete_files delete files, or compaction.every_commits indexed commits since the
// last compaction.
//
// Neither trigger is process state, so a restart neither defers nor advances a compaction: the
// delete files are on the summary of the current snapshot, and the commits are counted from the
// commit log of R-metadata@b, the durable record of both the indexed commits and the compactions
// (indexedSinceCompaction). Compaction itself is not part of the count: a rewrite adds a snapshot
// but no indexed commit, which is what makes the compaction commit the place the count restarts.
//
// The delete-file trigger is checked first because it is free - it reads a number the loaded table
// already carries - so a branch that is compacting for deletes does not pay a log read for the
// commit count as well (the count is then reported as 0 in the log line).
//
// The compaction keeps the position: the rows are the same and the lakefs.mds.source-commit stays
// the commit the table already records, so the lakeFS commit it produces carries no new tag - the
// tag stays on the pre-compaction commit, which has the same rows.
func (x *Indexer) compactIfNeeded(ctx context.Context, repository, branch string, tbl Table, last string) error {
	if last == "" || tbl == nil {
		return nil
	}
	deleteFiles := tbl.DeleteFiles()
	snapshots := tbl.SnapshotCount()
	due := x.compaction.MaxDeleteFiles > 0 && deleteFiles > x.compaction.MaxDeleteFiles
	indexed := 0
	if !due && x.compaction.EveryCommits > 0 {
		var err error
		indexed, err = x.indexedSinceCompaction(ctx, repository, branch)
		if err != nil {
			return err
		}
		due = indexed >= x.compaction.EveryCommits
	}
	if !due {
		return nil
	}

	if err := tbl.Compact(ctx, last); err != nil {
		return fmt.Errorf("compact %s@%s: %w", repository, branch, err)
	}
	metadata := metadataRepository(repository)
	if _, err := x.lakeFS.Commit(ctx, metadata, branch, client.CommitOptions{
		Message:    fmt.Sprintf(CompactCommitMessage, repository, last),
		Metadata:   sourceMetadata(repository, last),
		AllowEmpty: false,
	}); err != nil {
		return fmt.Errorf("commit the compaction of %q@%s: %w", metadata, branch, err)
	}
	x.metrics.Compaction()
	x.log.WithFields(logging.Fields{
		"repository":    repository,
		"branch":        branch,
		"source_commit": last,
		"delete_files":  deleteFiles,
		"snapshots":     snapshots,
		"indexed":       indexed,
	}).Info("mds: compacted the metadata table")
	return nil
}

// indexedSinceCompaction returns how many commits of R the branch has indexed since its last
// compaction (§4.9). The count is derived from durable state - the first-parent log of
// R-metadata@b - and not from a tally this process keeps: the log is what a restarted indexer
// reads, so a restart, or a second replica, counts the same commits and reaches every_commits at
// the same commit of R. A compaction commit repeats the source commit of the index commit it
// follows, and is what the walk stops at; an interrupted tick leaves no log entry, so it cannot
// lose or gain a count.
//
// The walk stops at the first compaction commit, after every_commits indexing commits, or at the
// start of the history - whichever comes first - because the caller only compares the result with
// every_commits. One page of the log (client.DefaultPageSize) therefore covers the default
// configuration in a single request.
func (x *Indexer) indexedSinceCompaction(ctx context.Context, repository, branch string) (int, error) {
	metadata := metadataRepository(repository)
	limit := x.compaction.EveryCommits
	indexed := 0
	after := ""
	for indexed < limit {
		commits, hasMore, err := x.lakeFS.LogCommits(ctx, metadata, branch, client.LogCommitsOptions{
			FirstParent: true,
			After:       after,
		})
		if err != nil {
			return 0, fmt.Errorf("log commits of %q@%s for the compaction count: %w", metadata, branch, err)
		}
		for _, commit := range commits {
			switch {
			case strings.HasPrefix(commit.Message, compactMessagePrefix):
				return indexed, nil
			case strings.HasPrefix(commit.Message, indexMessagePrefix):
				indexed++
			}
			if indexed >= limit {
				return indexed, nil
			}
		}
		if !hasMore || len(commits) == 0 {
			return indexed, nil
		}
		after = lastNameOf(len(commits), func(i int) string { return commits[i].ID })
		if after == "" {
			return 0, fmt.Errorf("%w: log commits of %q@%s: more results without a cursor",
				client.ErrBadPagination, metadata, branch)
		}
	}
	return indexed, nil
}

// addMaintenanceCommit is the branch tick's report that one commit of R was just indexed (§4.3
// step 4). §4.9's every_commits trigger no longer counts these in memory: it reads them back from
// the commit log (indexedSinceCompaction), which is what makes the count survive a restart. The
// tally is therefore write-only and the call site in the branch tick is what removes it.
func (x *Indexer) addMaintenanceCommit(repository, branch string) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.maintenance[repository+"@"+branch]++
}

// retireTags deletes the mds-* tags of R-metadata whose source commit is older than tag_retention
// (§4.9). A tag whose source commit is gone is retired too: it cannot be resolved through the
// catalog any more (§6). tag_retention 0 keeps every tag.
func (x *Indexer) retireTags(ctx context.Context, repository string) error {
	if x.tagRetention <= 0 {
		return nil
	}
	metadata := metadataRepository(repository)
	cutoff := time.Now().Add(-x.tagRetention)

	after := ""
	for {
		tags, hasMore, err := x.lakeFS.ListTags(ctx, metadata, after)
		if err != nil {
			return fmt.Errorf("list tags of %q: %w", metadata, err)
		}
		for _, tag := range tags {
			if !strings.HasPrefix(tag.Name, TagPrefix) {
				continue
			}
			retire, err := x.tagExpired(ctx, repository, tag, cutoff)
			if err != nil {
				return err
			}
			if !retire {
				continue
			}
			if err := x.lakeFS.DeleteTag(ctx, metadata, tag.Name); err != nil && !errors.Is(err, client.ErrNotFound) {
				return fmt.Errorf("delete tag %q of %q: %w", tag.Name, metadata, err)
			}
			x.log.WithFields(logging.Fields{
				"repository": repository,
				"tag":        tag.Name,
			}).Info("mds: retired a metadata tag")
		}
		if !hasMore {
			return nil
		}
		after = lastNameOf(len(tags), func(i int) string { return tags[i].Name })
		if after == "" {
			return fmt.Errorf("%w: list tags of %q: more results without a cursor", client.ErrBadPagination, metadata)
		}
	}
}

// tagExpired reports whether one mds-* tag is older than cutoff, by the creation date of the source
// commit it records. A tag whose commit or source commit cannot be read any more is expired: it is
// not resolvable, which is the reason retention exists.
func (x *Indexer) tagExpired(ctx context.Context, repository string, tag client.Tag, cutoff time.Time) (bool, error) {
	metadata := metadataRepository(repository)
	indexed, err := x.lakeFS.GetCommit(ctx, metadata, tag.CommitID)
	switch {
	case errors.Is(err, client.ErrNotFound):
		return true, nil
	case err != nil:
		return false, fmt.Errorf("commit %q of %q: %w", tag.CommitID, metadata, err)
	}
	source := indexed.Metadata[MetadataKeySourceCommit]
	if source == "" {
		return false, nil
	}
	commit, err := x.lakeFS.GetCommit(ctx, repository, source)
	switch {
	case errors.Is(err, client.ErrNotFound):
		return true, nil
	case err != nil:
		return false, fmt.Errorf("source commit %q of %q: %w", source, repository, err)
	}
	return commit.CreationDate.Before(cutoff), nil
}
