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
//   - a right-side stat that does not carry every field the row needs is filled in with one
//     statObject call. content_type and metadata are the exception: an object may legitimately have
//     neither, so an empty value there is not a missing one;
//   - a change set larger than rebuild_threshold is not applied as a delta at all: the caller
//     writes a full listing instead (§4.3 step 2), which bounds both memory and the delete
//     predicate. The walk of the diff stops at the first entry above the threshold, so a commit
//     that is going to be rebuilt is never read past it and never costs a statObject call.

import (
	"context"
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/pkg/mds/client"
	"github.com/treeverse/lakefs/pkg/mds/table"
)

// changeSet is the delta of one commit of the data repository: the rows to write and the paths to
// remove.
//
// A change set that grows past rebuild_threshold is abandoned: large is set, the entries read so far
// are dropped, and the caller writes a full listing of the commit instead (§4.3 step 2). That is
// what bounds the memory of one commit by the threshold instead of by the size of its diff.
type changeSet struct {
	upserts []table.Row
	deletes []string

	// large reports that the change set is larger than rebuild_threshold, so it is incomplete: the
	// caller must rebuild the table at the commit and must never apply this change set as a delta.
	large bool

	// count is how many entries the walk read: the size of the change set when large is false, and
	// rebuild_threshold+1 - where the walk stopped - when it is true. upserts and deletes are empty
	// in that case; count is what the caller logs.
	count int
}

// errChangeSetFull ends the walk of a change set that already grew past rebuild_threshold: §4.3
// step 2 rebuilds the branch's table in that case, so the remaining entries of the diff do not have
// to be read at all.
var errChangeSetFull = errors.New("change set above rebuild_threshold")

// changeSet reads the delta of one commit of R (§4.3 step 1), or stops as soon as it knows that the
// delta is too large to apply (§4.3 step 2).
func (x *Indexer) changeSet(ctx context.Context, repository string, commit client.Commit) (changeSet, error) {
	if len(commit.Parents) == 0 {
		// A root commit: every object of it is an upsert. Commits reach here only on a branch that
		// is indexed commit by commit, so the listing is a delta of one commit and not a backfill.
		var changes changeSet
		err := x.eachObject(ctx, repository, commit, func(stat client.ObjectStat) error {
			changes.upserts = append(changes.upserts, rowOf(repository, commit, stat))
			changes.count++
			if changes.count > x.rebuildThreshold {
				changes = changeSet{large: true, count: changes.count}
				return errChangeSetFull
			}
			return nil
		})
		switch {
		case errors.Is(err, errChangeSetFull):
		case err != nil:
			return changeSet{}, err
		}
		return changes, nil
	}
	return x.diffChangeSet(ctx, repository, commit)
}

// diffChangeSet diffs a commit against its first parent and turns the result into a change set
// (§4.3 step 1), unless the change set exceeds rebuild_threshold (§4.3 step 2).
//
// The entries are collected first and statted afterwards. That is what makes the threshold a real
// bound on the work as well as on the memory: a commit that ends up being rebuilt costs neither the
// rest of its diff nor a single statObject call, and the entries of a commit that stays a delta are
// all read before the first of them is described.
func (x *Indexer) diffChangeSet(ctx context.Context, repository string, commit client.Commit) (changeSet, error) {
	var (
		upserted []client.Change
		deletes  []string
	)
	after := ""
	for {
		page, hasMore, err := x.lakeFS.DiffRefs(ctx, repository, commit.Parents[0], commit.ID, after)
		if err != nil {
			return changeSet{}, fmt.Errorf("diff %s@%s..%s: %w", repository, commit.Parents[0], commit.ID, err)
		}
		for _, change := range page {
			switch change.Type {
			case client.ChangeAdded, client.ChangeChanged:
				upserted = append(upserted, change)
			case client.ChangeRemoved:
				deletes = append(deletes, change.Path)
			default:
				return changeSet{}, fmt.Errorf("%w: %q at %q in %s@%s",
					ErrUnknownChangeType, change.Type, change.Path, repository, commit.ID)
			}
			if count := len(upserted) + len(deletes); count > x.rebuildThreshold {
				return changeSet{large: true, count: count}, nil
			}
		}
		if !hasMore {
			break
		}
		after = lastNameOf(len(page), func(i int) string { return page[i].Path })
		if after == "" {
			return changeSet{}, fmt.Errorf("%w: diff %s@%s..%s: more results without a cursor",
				client.ErrBadPagination, repository, commit.Parents[0], commit.ID)
		}
	}

	changes := changeSet{
		upserts: make([]table.Row, 0, len(upserted)),
		deletes: deletes,
		count:   len(upserted) + len(deletes),
	}
	for _, change := range upserted {
		stat, err := x.objectStatOf(ctx, repository, commit.ID, change)
		if err != nil {
			return changeSet{}, err
		}
		changes.upserts = append(changes.upserts, rowOf(repository, commit, stat))
	}
	return changes, nil
}

// objectStatOf returns the object stats of an added or changed entry (§4.3 step 1).
//
// include_right_stats is experimental (the swagger marks it so, and the plan's risk list says the
// indexer falls back to statObject), so a right-side stat that does not carry every field a row
// needs is treated as absent rather than as an object with an empty checksum, no modification time
// or no size. content_type and metadata are deliberately not part of that: an object may have
// neither, so an empty value there is a fact about the object and not a gap in the answer.
func (x *Indexer) objectStatOf(ctx context.Context, repository, ref string, change client.Change) (client.ObjectStat, error) {
	if rightStatComplete(change) {
		stat := *change.Right
		stat.Path = change.Path
		stat.SizeBytes = change.SizeBytes
		return stat, nil
	}
	stat, err := x.lakeFS.StatObject(ctx, repository, ref, change.Path)
	if err != nil {
		return client.ObjectStat{}, fmt.Errorf("stat %q in %s@%s: %w", change.Path, repository, ref, err)
	}
	return stat, nil
}

// rightStatComplete reports whether the right side of a diff entry carries every field a row of
// §3.1 needs: the checksum, the modification time and the size.
//
// The size of an entry is on the diff, not inside the right-side stat (the swagger's
// DiffObjectStat has no size), and the narrowed client reports a size the server omitted as zero.
// A zero-byte object has the same size, so the indexer cannot tell the two apart and calls
// statObject for a zero-size entry; one extra call per empty object is the price of never writing a
// size the server did not confirm.
func rightStatComplete(change client.Change) bool {
	right := change.Right
	return right != nil && right.ETag != "" && !right.Mtime.IsZero() && change.SizeBytes != 0
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
