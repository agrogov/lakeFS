package indexer

// changes.go turns one commit of the data repository into the rows a table write applies
// (design §4.3 steps 1 and 2):
//
//   - a commit with a parent is diffed against its first parent (two-dot, right-side stats). The
//     first parent is what §3.2 requires: after a merge, the objects the merge brought in get the
//     merge commit's ID, committer and date;
//   - a root commit is a full listing of that commit, because it has nothing to diff against;
//   - the committer and creation_date of every row come from the commit itself, never from the
//     object;
//   - a change set larger than rebuild_threshold is not applied as a delta at all: the caller
//     writes a full listing instead (§4.3 step 2), which bounds both memory and the delete
//     predicate.

import (
	"context"
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/pkg/mds/client"
	"github.com/treeverse/lakefs/pkg/mds/table"
)

// changeSet is the delta of one commit of the data repository: the rows to write and the paths to
// remove.
type changeSet struct {
	upserts []table.Row
	deletes []string
}

// count is the size of the change set: the number §4.3 step 2 compares with rebuild_threshold.
func (c changeSet) count() int { return len(c.upserts) + len(c.deletes) }

// changeSet reads the delta of one commit of R (§4.3 step 1).
func (x *Indexer) changeSet(ctx context.Context, repository string, commit client.Commit) (changeSet, error) {
	if len(commit.Parents) == 0 {
		// A root commit: every object of it is an upsert. Commits reach here only on a branch that
		// is indexed commit by commit, so the listing is a delta of one commit and not a backfill.
		var changes changeSet
		if err := x.eachObject(ctx, repository, commit, func(stat client.ObjectStat) error {
			changes.upserts = append(changes.upserts, rowOf(repository, commit, stat))
			return nil
		}); err != nil {
			return changeSet{}, err
		}
		return changes, nil
	}

	var changes changeSet
	after := ""
	for {
		page, hasMore, err := x.lakeFS.DiffRefs(ctx, repository, commit.Parents[0], commit.ID, after)
		if err != nil {
			return changeSet{}, fmt.Errorf("diff %s@%s..%s: %w", repository, commit.Parents[0], commit.ID, err)
		}
		for _, change := range page {
			switch change.Type {
			case client.ChangeAdded, client.ChangeChanged:
				stat, err := x.objectStatOf(ctx, repository, commit.ID, change)
				if err != nil {
					return changeSet{}, err
				}
				changes.upserts = append(changes.upserts, rowOf(repository, commit, stat))
			case client.ChangeRemoved:
				changes.deletes = append(changes.deletes, change.Path)
			default:
				return changeSet{}, fmt.Errorf("%w: %q at %q in %s@%s",
					ErrUnknownChangeType, change.Type, change.Path, repository, commit.ID)
			}
		}
		if !hasMore {
			return changes, nil
		}
		after = lastNameOf(len(page), func(i int) string { return page[i].Path })
		if after == "" {
			return changeSet{}, fmt.Errorf("%w: diff %s@%s..%s: more results without a cursor",
				client.ErrBadPagination, repository, commit.Parents[0], commit.ID)
		}
	}
}

// objectStatOf returns the object stats of an added or changed entry (§4.3 step 1).
//
// include_right_stats is experimental (the swagger marks it so, and the plan's risk list says the
// indexer falls back to statObject), so a right-side stat that carries neither a checksum nor a
// modification time is treated as absent rather than as an object with an empty etag.
func (x *Indexer) objectStatOf(ctx context.Context, repository, ref string, change client.Change) (client.ObjectStat, error) {
	if right := change.Right; right != nil && right.ETag != "" && !right.Mtime.IsZero() {
		stat := *right
		stat.Path = change.Path
		return stat, nil
	}
	stat, err := x.lakeFS.StatObject(ctx, repository, ref, change.Path)
	if err != nil {
		return client.ObjectStat{}, fmt.Errorf("stat %q in %s@%s: %w", change.Path, repository, ref, err)
	}
	return stat, nil
}

// eachObject walks the full listing of one commit of R, one page at a time (§4.3 step 2).
func (x *Indexer) eachObject(ctx context.Context, repository string, commit client.Commit, visit func(client.ObjectStat) error) error {
	after := ""
	for {
		page, hasMore, err := x.lakeFS.ListObjects(ctx, repository, commit.ID, after)
		if err != nil {
			return fmt.Errorf("list objects of %s@%s: %w", repository, commit.ID, err)
		}
		for _, object := range page {
			if err := visit(object); err != nil {
				return err
			}
		}
		if !hasMore {
			return nil
		}
		after = lastNameOf(len(page), func(i int) string { return page[i].Path })
		if after == "" {
			return fmt.Errorf("%w: list objects of %s@%s: more results without a cursor",
				client.ErrBadPagination, repository, commit.ID)
		}
	}
}

// listingRows streams the full listing of one commit of R as table rows (§4.3 step 2). The rows are
// pulled by the writer, so a rebuild of a large branch is bounded by the writer's batch size and not
// by the number of objects; count receives how many rows were produced.
func (x *Indexer) listingRows(ctx context.Context, repository string, commit client.Commit, count *int) table.Rows {
	return func(yield func(table.Row, error) bool) {
		err := x.eachObject(ctx, repository, commit, func(stat client.ObjectStat) error {
			*count++
			if !yield(rowOf(repository, commit, stat), nil) {
				return errStopListing
			}
			return nil
		})
		if err == nil || errors.Is(err, errStopListing) {
			return
		}
		yield(table.Row{}, err)
	}
}

// errStopListing ends a listing when the consumer stopped reading it.
var errStopListing = errors.New("listing stopped by the consumer")

// rowOf builds one row of system.object_metadata (§3.1): the object's stats and the commit that
// produced it. committer and creation_date are the commit's, the same for every row of the commit
// and never the object's mtime (§3.1: creation_date is "creation time of the commit commit_id").
func rowOf(repository string, commit client.Commit, stat client.ObjectStat) table.Row {
	row := table.Row{
		Repository:   repository,
		Path:         stat.Path,
		CommitID:     commit.ID,
		SizeBytes:    stat.SizeBytes,
		LastModified: stat.Mtime,
		ETag:         stat.ETag,
		UserMetadata: stat.UserMetadata,
	}
	if commit.Committer != "" {
		committer := commit.Committer
		row.Committer = &committer
	}
	if !commit.CreationDate.IsZero() {
		creationDate := commit.CreationDate
		row.CreationDate = &creationDate
	}
	if stat.ContentType != "" {
		contentType := stat.ContentType
		row.ContentType = &contentType
	}
	return row
}
