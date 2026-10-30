import React, { FC } from 'react';
import Table from 'react-bootstrap/Table';
import Badge from 'react-bootstrap/Badge';
import { LinkExternalIcon } from '@primer/octicons-react';

import { DataItem, DatasetStats } from '../../api/datasets';
import { Link } from '../nav';
import { Na } from '../controls';
import { humanSize } from '../repository/tree';
import { itemSourcePath } from './definitionDiff';

/** `/repositories/<repo>/objects?ref=<commit>&path=<path>` — where an item's source lives (§11.2). */
export const sourceObjectUrl = (repository: string, commit: string, path: string) => ({
    pathname: '/repositories/:repoId/objects',
    params: { repoId: repository },
    query: { ref: commit, path },
});

/** The stats of one item, matched to its row by target (§10.2 `DatasetStats.items`). */
const itemStats = (stats: DatasetStats | undefined, target: string) =>
    stats?.items?.find((item) => item.target === target);

const shortCommit = (commit: string): string => (commit.length > 12 ? commit.substring(0, 12) : commit);

export interface DefinitionItemsTableProps {
    items: DataItem[];
    /** Per-item object/byte counts, when the version/validation carries them. */
    stats?: DatasetStats;
    /** Shows the per-item objects/bytes columns; off when there are no stats to show. */
    showStats?: boolean;
    caption?: string;
    emptyText?: string;
}

/**
 * Read-only view of a dataset definition's items (DATASETS_DESIGN.md §11.2).
 *
 * Every item is a source repository, a commit it is pinned to and a path in that commit, so each
 * row links to the source object in the repository browser. This is the same information the
 * editor's items table edits; the difference is that nothing here is a control.
 */
export const DefinitionItemsTable: FC<DefinitionItemsTableProps> = ({
    items,
    stats,
    showStats = true,
    caption,
    emptyText = 'This version has no items',
}) => {
    if (items.length === 0) {
        return <div className="text-muted dataset-items-empty">{emptyText}</div>;
    }

    return (
        <Table hover responsive size="sm" className="dataset-items-table">
            {caption && <caption className="text-muted small">{caption}</caption>}
            <thead>
                <tr>
                    <th>Target</th>
                    <th>Type</th>
                    <th>Source</th>
                    {showStats && <th className="text-end">Objects</th>}
                    {showStats && <th className="text-end">Bytes</th>}
                </tr>
            </thead>
            <tbody>
                {items.map((item) => {
                    const commit = item.ref?.id ?? '';
                    const path = itemSourcePath(item);
                    const sourceStats = itemStats(stats, item.target);
                    return (
                        <tr key={item.target} className="dataset-item-row">
                            <td>
                                <code className="dataset-item-target">{item.target || '/'}</code>
                            </td>
                            <td>
                                <Badge bg="light" text="dark" className="border dataset-item-type">
                                    {item.type}
                                </Badge>
                            </td>
                            <td className="dataset-item-source">
                                <Link
                                    href={sourceObjectUrl(item.repository, commit, path)}
                                    title={`${item.repository}@${commit}:${path}`}
                                >
                                    <span className="dataset-item-repository">{item.repository}</span>
                                    <span className="text-muted">@{shortCommit(commit)}</span>
                                    <code className="dataset-item-path ms-1">/{path}</code>
                                    <LinkExternalIcon className="ms-1" />
                                </Link>
                            </td>
                            {showStats && (
                                <td className="text-end">
                                    {sourceStats ? sourceStats.objects.toLocaleString() : <Na />}
                                </td>
                            )}
                            {showStats && (
                                <td className="text-end">{sourceStats ? humanSize(sourceStats.bytes) : <Na />}</td>
                            )}
                        </tr>
                    );
                })}
            </tbody>
        </Table>
    );
};

export default DefinitionItemsTable;
