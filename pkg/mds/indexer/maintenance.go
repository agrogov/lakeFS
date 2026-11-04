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

// compactIfNeeded rewrites the branch's table when §4.9's thresholds are crossed: more than
// compaction.max_delete_files delete files, or compaction.every_commits indexed commits since the
// last compaction.
//
// The commit count is kept in memory, so a restart can defer a compaction by up to every_commits
// commits; the delete-file trigger is read from the table and is unaffected. Compaction itself is
// not part of the count: a rewrite adds a snapshot but no indexed commit.
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
	indexed := x.maintenanceCommits(repository, branch)
	switch {
	case x.compaction.MaxDeleteFiles > 0 && deleteFiles > x.compaction.MaxDeleteFiles:
	case x.compaction.EveryCommits > 0 && indexed >= x.compaction.EveryCommits:
	default:
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
	x.setMaintenanceCommits(repository, branch, 0)
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

// maintenanceCommits returns how many indexed commits a branch has received since its last
// compaction.
func (x *Indexer) maintenanceCommits(repository, branch string) int {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.maintenance[repository+"@"+branch]
}

// addMaintenanceCommit counts one indexed commit towards the every_commits trigger of §4.9.
func (x *Indexer) addMaintenanceCommit(repository, branch string) {
	x.mu.Lock()
	defer x.mu.Unlock()
	key := repository + "@" + branch
	x.maintenance[key]++
}

// setMaintenanceCommits resets the commit counter of a branch after a compaction.
func (x *Indexer) setMaintenanceCommits(repository, branch string, value int) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.maintenance[repository+"@"+branch] = value
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
