package indexer

// branch.go is the per-branch state machine of design §4.2, and the recovery, backfill, rewrite and
// lifecycle rules of §4.4, §4.5, §4.7 and §4.8.
//
// The invariant everything else follows from is that the position of branch b is the
// lakefs.mds.source-commit metadata of the head commit of R-metadata@b (§3.3). A tick therefore
// starts by reading that position, and any crash leaves one of the four states of §4.4 - and only
// those - because the Iceberg commit (step 3) and the lakeFS commit (step 4) are the two halves of
// the move.

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/mds/client"
)

// runBranch indexes one branch of one repository (§4.2), restarting the branch when another writer
// moved the table during the tick (§4.4). A restart re-reads the position, so it is the same code
// path a crash recovery takes.
func (x *Indexer) runBranch(ctx context.Context, repository, branch string, state *branchState, retries int) error {
	for attempt := 0; ; attempt++ {
		err := x.runBranchOnce(ctx, repository, branch, state, true)
		if !errors.Is(err, ErrConflict) || attempt >= retries {
			return err
		}
		x.log.WithError(err).WithFields(logging.Fields{
			"repository": repository,
			"branch":     branch,
		}).Warn("mds: the metadata table moved during the tick, restarting the branch")
	}
}

// runBranchOnce is one pass of §4.2 over one branch. mayCreate reports whether the branch of
// R-metadata may still be created by this pass: it is true on the first pass, so a repository whose
// mirror cannot be created fails instead of looping.
func (x *Indexer) runBranchOnce(ctx context.Context, repository, branch string, state *branchState, mayCreate bool) error {
	head, err := x.lakeFS.GetCommit(ctx, repository, branch)
	if errors.Is(err, client.ErrNotFound) {
		// §4.8: the branch was deleted between the listing and this tick.
		return nil
	}
	if err != nil {
		return fmt.Errorf("head of %s@%s: %w", repository, branch, err)
	}
	state.head = head.ID

	tbl, err := x.tables.LoadTable(ctx, repository, branch)
	switch {
	case errors.Is(err, ErrTableNotFound):
		if !mayCreate {
			return fmt.Errorf("the metadata branch of %s@%s has no table: %w", repository, branch, err)
		}
		// §4.5: R-metadata@b does not exist yet. Create it - seeded from an indexed ancestor or
		// from the initialisation commit - and index it in the same tick.
		if err := x.createBranch(ctx, repository, branch, head); err != nil {
			return err
		}
		return x.runBranchOnce(ctx, repository, branch, state, false)
	case err != nil:
		return fmt.Errorf("load the metadata table of %s@%s: %w", branch, repository, err)
	}
	handle := &tableHandle{x: x, repository: repository, branch: branch, table: tbl}
	defer handle.close()

	metaHead, last, err := x.lastIndexed(ctx, repository, branch)
	if err != nil {
		return err
	}
	if last == "" {
		// The branch exists but no commit records a position: a first indexing interrupted
		// between its table write and its commit (§4.5 step 2, §4.4).
		if last, err = x.startBranch(ctx, repository, branch, handle, head); err != nil {
			return err
		}
	} else {
		// §4.4 "crashed after 4": the lakeFS commit is there, its tag may not be.
		if err := x.ensureTag(ctx, repository, branch, last, metaHead.ID); err != nil {
			return err
		}
		// §4.2 step 4, §4.7: is the recorded position still on the branch?
		if last != head.ID {
			base, err := x.lakeFS.FindMergeBase(ctx, repository, last, head.ID)
			if err != nil {
				return fmt.Errorf("merge base of %s@%s..%s: %w", repository, last, head.ID, err)
			}
			if base != last {
				if last, err = x.rewrite(ctx, repository, branch, handle, head, base); err != nil {
					return err
				}
			}
		}
	}
	state.last = last

	// §4.2 step 5: the first-parent commits to apply, oldest first, at most max_commits.
	var todo []client.Commit
	var total int
	if last != head.ID {
		todo, total, err = x.pending(ctx, repository, head, last, x.maxCommits)
		if err != nil {
			return err
		}
	}
	state.lag = int64(total)

	// §4.4: recover what the previous tick staged before it crashed.
	recovered, err := x.recoverStaging(ctx, repository, branch, handle, last, todo)
	if err != nil {
		return err
	}
	last, todo = recovered.last, recovered.todo
	state.last = last
	if recovered.finished {
		// The staged Iceberg commit was completed (steps 4-5), so it counts as indexed.
		x.metrics.IndexedCommit(repository)
		state.lag--
	}

	// §4.2 step 6: apply one commit at a time; a failure stops this branch for this tick.
	for _, commit := range todo {
		if err := x.applyCommit(ctx, repository, branch, handle.table, commit); err != nil {
			return err
		}
		last = commit.ID
		state.last = last
		state.lag--
	}

	// §4.9: compaction is a property of the branch's table, so it runs after the commits.
	return x.compactIfNeeded(ctx, repository, branch, handle.table, last)
}

// lastIndexed returns the head commit of R-metadata@b and the source commit it records (§3.3).
// The source commit is empty on a branch that was created but never indexed.
func (x *Indexer) lastIndexed(ctx context.Context, repository, branch string) (client.Commit, string, error) {
	metadata := metadataRepository(repository)
	head, err := x.lakeFS.GetCommit(ctx, metadata, branch)
	if err != nil {
		return client.Commit{}, "", fmt.Errorf("head of %q@%s: %w", metadata, branch, err)
	}
	return head, head.Metadata[MetadataKeySourceCommit], nil
}

// tableHandle owns the table of one branch tick and lets a recovery step replace it: a discarded
// staging must be re-read, and the handle that described it must be released.
type tableHandle struct {
	x          *Indexer
	repository string
	branch     string
	table      Table
}

// swap closes the current handle and adopts table.
func (h *tableHandle) swap(table Table) {
	if h.table != nil && h.table != table {
		h.x.closeTable(h.table, h.repository, h.branch)
	}
	h.table = table
}

// close releases the handle.
func (h *tableHandle) close() { h.swap(nil) }

// createBranch creates R-metadata@b for a branch that has no mirror yet (§4.5). It seeds from the
// tag of an indexed ancestor when there is one, and otherwise from the initialisation commit so
// that startBranch can rebuild from a full listing.
func (x *Indexer) createBranch(ctx context.Context, repository, branch string, head client.Commit) error {
	metadata := metadataRepository(repository)
	source, seeded, err := x.seedTag(ctx, repository, head)
	if err != nil {
		return err
	}
	if !seeded {
		if source, err = x.initCommit(repository); err != nil {
			return err
		}
	}
	if err := x.lakeFS.CreateBranch(ctx, metadata, branch, source); err != nil && !errors.Is(err, client.ErrConflict) {
		return fmt.Errorf("create %q@%s from %q: %w", metadata, branch, source, err)
	}
	x.log.WithFields(logging.Fields{
		"repository": repository,
		"branch":     branch,
		"seeded":     seeded,
		"source":     source,
	}).Info("mds: created the metadata branch")
	return nil
}

// seedTag looks for an indexed ancestor of head (§4.5 step 1): the first commit of the first
// seed_lookback commits of the branch whose tag exists in R-metadata. Seeding a new branch from it
// makes a feature branch cheap, because the table it starts from is already written.
func (x *Indexer) seedTag(ctx context.Context, repository string, head client.Commit) (string, bool, error) {
	metadata := metadataRepository(repository)
	var (
		after = ""
		seen  int
	)
	for seen < x.seedLookback {
		commits, hasMore, err := x.lakeFS.LogCommits(ctx, repository, head.ID, client.LogCommitsOptions{
			FirstParent: true,
			After:       after,
		})
		if err != nil {
			return "", false, fmt.Errorf("log commits of %s@%s: %w", repository, head.ID, err)
		}
		for _, commit := range commits {
			tag := tagName(commit.ID)
			if _, err := x.lakeFS.GetTag(ctx, metadata, tag); err == nil {
				return tag, true, nil
			} else if !errors.Is(err, client.ErrNotFound) {
				return "", false, fmt.Errorf("tag %q of %q: %w", tag, metadata, err)
			}
			seen++
			if seen >= x.seedLookback {
				break
			}
		}
		if !hasMore {
			return "", false, nil
		}
		after = lastNameOf(len(commits), func(i int) string { return commits[i].ID })
		if after == "" {
			return "", false, fmt.Errorf("%w: log commits of %s@%s: more results without a cursor",
				client.ErrBadPagination, repository, head.ID)
		}
	}
	return "", false, nil
}

// backfillStart returns the commit a first indexing of a branch starts at (§4.5 step 2): the head
// (backfill: head, the default), the oldest first-parent commit (backfill: history), or the oldest
// first-parent commit created at or after metadata_search.since.
func (x *Indexer) backfillStart(ctx context.Context, repository string, head client.Commit) (client.Commit, error) {
	if x.since == nil && x.backfill != BackfillHistory {
		return head, nil
	}
	var (
		after  = ""
		oldest = head
	)
	for {
		commits, hasMore, err := x.lakeFS.LogCommits(ctx, repository, head.ID, client.LogCommitsOptions{
			FirstParent: true,
			After:       after,
		})
		if err != nil {
			return client.Commit{}, fmt.Errorf("log commits of %s@%s: %w", repository, head.ID, err)
		}
		for _, commit := range commits {
			if x.since != nil && commit.CreationDate.Before(*x.since) {
				// The chain is walked newest first: the first commit older than since ends the
				// window, and the last commit that was inside it is the start.
				return oldest, nil
			}
			oldest = commit
		}
		if !hasMore {
			return oldest, nil
		}
		after = lastNameOf(len(commits), func(i int) string { return commits[i].ID })
		if after == "" {
			return client.Commit{}, fmt.Errorf("%w: log commits of %s@%s: more results without a cursor",
				client.ErrBadPagination, repository, head.ID)
		}
	}
}

// startBranch indexes a branch that has a table but no recorded position: §4.5 step 2 writes a full
// listing of the backfill start commit, commits it and tags it. It returns the position it recorded.
func (x *Indexer) startBranch(ctx context.Context, repository, branch string, handle *tableHandle, head client.Commit) (string, error) {
	source, err := x.backfillStart(ctx, repository, head)
	if err != nil {
		return "", err
	}
	// §4.4: a staged rebuild at the same commit only misses its lakeFS commit; anything else
	// staged by a crashed attempt is discarded before the rebuild.
	switch staged := handle.table.SourceCommit(); {
	case staged == source.ID:
	case staged != "":
		if err := x.resetStaging(ctx, repository, branch, handle); err != nil {
			return "", err
		}
	}

	var written int64
	if err := x.rebuild(ctx, repository, handle.table, source, &written); err != nil {
		return "", err
	}
	x.metrics.RowsWritten(written)
	x.metrics.Rebuild()
	if err := x.finishCommit(ctx, repository, branch, source); err != nil {
		return "", err
	}
	x.metrics.IndexedCommit(repository)
	x.log.WithFields(logging.Fields{
		"repository":    repository,
		"branch":        branch,
		"source_commit": source.ID,
		"rows_written":  written,
	}).Info("mds: indexed the branch for the first time")
	return source.ID, nil
}

// resetStaging discards the uncommitted changes of R-metadata@b (§4.4) and replaces the tick's
// table handle with one that describes the committed state.
func (x *Indexer) resetStaging(ctx context.Context, repository, branch string, handle *tableHandle) error {
	if err := x.lakeFS.ResetBranch(ctx, metadataRepository(repository), branch); err != nil {
		return fmt.Errorf("discard the staging of %q@%s: %w", metadataRepository(repository), branch, err)
	}
	reloaded, err := x.tables.LoadTable(ctx, repository, branch)
	if err != nil {
		return fmt.Errorf("reload the metadata table of %s@%s after a reset: %w", branch, repository, err)
	}
	handle.swap(reloaded)
	x.log.WithFields(logging.Fields{
		"repository": repository,
		"branch":     branch,
	}).Warn("mds: discarded uncommitted changes of the metadata branch")
	return nil
}

// recovery is what §4.4's recovery concluded about a branch.
type recovery struct {
	// last is the position the branch is at after the recovery.
	last string

	// todo is the remaining commits to apply.
	todo []client.Commit

	// finished reports that a staged Iceberg commit was completed (steps 4-5 of §4.3), so it
	// counts as one indexed commit.
	finished bool
}

// recoverStaging is step 1 of §4.2, the recovery table of §4.4:
//
//   - nothing staged (the staged property equals the recorded position): nothing to do. This is
//     also the "crashed during step 3" row: data files without a staged pointer are unreferenced
//     files that lakectl iceberg cleanup removes (§9);
//   - the staged property is the next commit C after the recorded position: the Iceberg commit of
//     C happened and its lakeFS commit did not, so steps 4-5 are done for C;
//   - the staged property is anything else (a partial or foreign write): the staging is
//     discarded, and the branch is indexed normally.
//
// §4.4's "crashed after 4" row is handled before this, by ensureTag.
func (x *Indexer) recoverStaging(ctx context.Context, repository, branch string, handle *tableHandle, last string, todo []client.Commit) (recovery, error) {
	staged := handle.table.SourceCommit()
	if staged == last {
		return recovery{last: last, todo: todo}, nil
	}
	if len(todo) > 0 && todo[0].ID == staged {
		if err := x.finishCommit(ctx, repository, branch, todo[0]); err != nil {
			return recovery{}, err
		}
		return recovery{last: staged, todo: todo[1:], finished: true}, nil
	}
	if err := x.resetStaging(ctx, repository, branch, handle); err != nil {
		return recovery{}, err
	}
	return recovery{last: last, todo: todo}, nil
}

// pending returns the first-parent commits of R@b after last, oldest first, at most limit of them,
// and how many there are in total (§4.2 step 5). The total is the lag /_health and the lag_commits
// metric report (§9), which max_commits must not hide.
func (x *Indexer) pending(ctx context.Context, repository string, head client.Commit, last string, limit int) ([]client.Commit, int, error) {
	var commits []client.Commit
	after := ""
	for {
		page, hasMore, err := x.lakeFS.LogCommits(ctx, repository, head.ID, client.LogCommitsOptions{
			FirstParent: true,
			StopAt:      last,
			After:       after,
		})
		if err != nil {
			return nil, 0, fmt.Errorf("log commits of %s@%s after %s: %w", repository, head.ID, last, err)
		}
		commits = append(commits, page...)
		if !hasMore {
			break
		}
		after = lastNameOf(len(page), func(i int) string { return page[i].ID })
		if after == "" {
			return nil, 0, fmt.Errorf("%w: log commits of %s@%s: more results without a cursor",
				client.ErrBadPagination, repository, head.ID)
		}
	}
	slices.Reverse(commits)
	total := len(commits)
	if len(commits) > limit {
		commits = commits[:limit]
	}
	return commits, total, nil
}

// rewrite handles a recorded position that is not an ancestor of the branch head (§4.7): a hard
// reset, a revert by reset, or a force-moved branch. When the merge base has a tag the branch is
// moved back to it and the commits after it are indexed normally; otherwise the branch is reset to
// the initialisation commit and rebuilt from a full listing at the head.
func (x *Indexer) rewrite(ctx context.Context, repository, branch string, handle *tableHandle, head client.Commit, base string) (string, error) {
	metadata := metadataRepository(repository)
	tag := tagName(base)
	_, err := x.lakeFS.GetTag(ctx, metadata, tag)
	switch {
	case err == nil:
		if err := x.lakeFS.HardResetBranch(ctx, metadata, branch, tag); err != nil {
			return "", fmt.Errorf("hard reset %q@%s to %q: %w", metadata, branch, tag, err)
		}
		reloaded, err := x.tables.LoadTable(ctx, repository, branch)
		if err != nil {
			return "", fmt.Errorf("reload the metadata table of %s@%s after a hard reset: %w", branch, repository, err)
		}
		handle.swap(reloaded)
		x.log.WithFields(logging.Fields{
			"repository": repository,
			"branch":     branch,
			"merge_base": base,
		}).Info("mds: history rewrite, moved the metadata branch back to the merge base")
		return base, nil
	case !errors.Is(err, client.ErrNotFound):
		return "", fmt.Errorf("tag %q of %q: %w", tag, metadata, err)
	}

	init, err := x.initCommit(repository)
	if err != nil {
		return "", err
	}
	if err := x.lakeFS.HardResetBranch(ctx, metadata, branch, init); err != nil {
		return "", fmt.Errorf("hard reset %q@%s to the initialisation commit: %w", metadata, branch, err)
	}
	reloaded, err := x.tables.LoadTable(ctx, repository, branch)
	if err != nil {
		return "", fmt.Errorf("reload the metadata table of %s@%s after a hard reset: %w", branch, repository, err)
	}
	handle.swap(reloaded)

	var written int64
	if err := x.rebuild(ctx, repository, handle.table, head, &written); err != nil {
		return "", err
	}
	x.metrics.RowsWritten(written)
	x.metrics.Rebuild()
	if err := x.finishCommit(ctx, repository, branch, head); err != nil {
		return "", err
	}
	x.metrics.IndexedCommit(repository)
	x.log.WithFields(logging.Fields{
		"repository":    repository,
		"branch":        branch,
		"source_commit": head.ID,
		"rows_written":  written,
	}).Warn("mds: history rewrite without an indexed merge base, rebuilt the branch at its head")
	return head.ID, nil
}

// applyCommit applies one commit of R to the branch's table and records it (§4.3).
func (x *Indexer) applyCommit(ctx context.Context, repository, branch string, tbl Table, commit client.Commit) error {
	changes, err := x.changeSet(ctx, repository, commit)
	if err != nil {
		return err
	}

	var written int64
	if changes.count() > x.rebuildThreshold {
		// §4.3 step 2: a change set this large is cheaper written as a full listing, and it
		// bounds both the memory of the delta and the size of its delete predicate.
		if err := x.rebuild(ctx, repository, tbl, commit, &written); err != nil {
			return err
		}
		x.metrics.Rebuild()
		x.log.WithFields(logging.Fields{
			"repository":        repository,
			"commit":            commit.ID,
			"changes":           changes.count(),
			"rebuild_threshold": x.rebuildThreshold,
		}).Info("mds: change set above rebuild_threshold, rebuilt the branch table")
	} else if tbl.SourceCommit() != commit.ID {
		// §4.4: a staged table that already records this commit only misses its lakeFS commit.
		if err := tbl.ApplyDelta(ctx, changes.upserts, changes.deletes, commit.ID); err != nil {
			return fmt.Errorf("apply %s@%s: %w", repository, commit.ID, err)
		}
		written = int64(len(changes.upserts))
	}
	x.metrics.RowsWritten(written)

	if err := x.finishCommit(ctx, repository, branch, commit); err != nil {
		return err
	}
	x.metrics.IndexedCommit(repository)
	return nil
}

// rebuild writes the full listing of R@commit into the branch's table (§4.3 step 2). A table that
// already records the commit is left alone (§4.4): the write happened, only its lakeFS commit did
// not.
func (x *Indexer) rebuild(ctx context.Context, repository string, tbl Table, commit client.Commit, written *int64) error {
	if tbl.SourceCommit() == commit.ID {
		return nil
	}
	var rows int
	if err := tbl.Rebuild(ctx, x.listingRows(ctx, repository, commit, &rows), commit.ID); err != nil {
		return fmt.Errorf("rebuild %s@%s: %w", repository, commit.ID, err)
	}
	*written = int64(rows)
	return nil
}

// finishCommit does steps 4 and 5 of §4.3: the lakeFS commit of R-metadata@b that records the
// position, and the mds-<commit> tag that makes the commit addressable through the catalog (§6).
func (x *Indexer) finishCommit(ctx context.Context, repository, branch string, commit client.Commit) error {
	metadata := metadataRepository(repository)
	indexed, err := x.lakeFS.Commit(ctx, metadata, branch, client.CommitOptions{
		Message:    fmt.Sprintf(IndexCommitMessage, repository, commit.ID),
		Metadata:   sourceMetadata(repository, commit.ID),
		AllowEmpty: false,
	})
	if err != nil {
		return fmt.Errorf("commit %q@%s for %s: %w", metadata, branch, commit.ID, err)
	}
	if err := x.ensureTag(ctx, repository, branch, commit.ID, indexed.ID); err != nil {
		return err
	}
	// §4.9's every_commits trigger counts indexed commits: this is the one place every path
	// (delta, rebuild, rewrite, a completed crash recovery) records exactly one.
	x.addMaintenanceCommit(repository, branch)
	return nil
}

// ensureTag makes sure tag mds-<commit> of R-metadata points at ref (§4.3 step 5). created with
// force, so it also repairs the tag of a tick that crashed after its lakeFS commit (§4.4).
func (x *Indexer) ensureTag(ctx context.Context, repository, branch, commit, ref string) error {
	metadata := metadataRepository(repository)
	tag := tagName(commit)
	existing, err := x.lakeFS.GetTag(ctx, metadata, tag)
	switch {
	case err == nil && existing.CommitID == ref:
		return nil
	case err != nil && !errors.Is(err, client.ErrNotFound):
		return fmt.Errorf("tag %q of %q: %w", tag, metadata, err)
	}
	if _, err := x.lakeFS.CreateTag(ctx, metadata, tag, ref, true); err != nil {
		return fmt.Errorf("tag %q of %q at %q on %s@%s: %w", tag, metadata, ref, repository, branch, err)
	}
	return nil
}

// retireDeletedBranches mirrors the deletion of a branch of R to R-metadata (§4.8). Its tags stay,
// so a commit or tag read of the deleted branch's history still works (§6).
func (x *Indexer) retireDeletedBranches(ctx context.Context, repository, metadata string, branches []string) error {
	live := make(map[string]struct{}, len(branches))
	for _, branch := range branches {
		live[branch] = struct{}{}
	}
	after := ""
	for {
		indexed, hasMore, err := x.lakeFS.ListBranches(ctx, metadata, after)
		if err != nil {
			return fmt.Errorf("list branches of %q: %w", metadata, err)
		}
		for _, branch := range indexed {
			if _, ok := live[branch.Name]; ok {
				continue
			}
			if err := x.lakeFS.DeleteBranch(ctx, metadata, branch.Name); err != nil && !errors.Is(err, client.ErrNotFound) {
				return fmt.Errorf("delete %q@%s: %w", metadata, branch.Name, err)
			}
			x.metrics.ClearLag(repository, branch.Name)
			x.log.WithFields(logging.Fields{
				"repository": repository,
				"branch":     branch.Name,
			}).Info("mds: deleted the metadata branch of a deleted branch")
		}
		if !hasMore {
			return nil
		}
		after = lastNameOf(len(indexed), func(i int) string { return indexed[i].Name })
		if after == "" {
			return fmt.Errorf("%w: list branches of %q: more results without a cursor", client.ErrBadPagination, metadata)
		}
	}
}
