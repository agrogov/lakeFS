import React, { FC } from 'react';
import Table from 'react-bootstrap/Table';

import { DataDiffEntry } from '../../api/datasets';
import { ChangeBadge } from './definitionDiffTable';

/**
 * The data (object-level) diff of a dataset (DATASETS_DESIGN.md §10.3, §11.2).
 *
 * The compare page renders this diff as a repository tree; a proposal's stored diff summary
 * is a flat page of entries, so it is rendered as a table instead. Both start from the same
 * entries, and both hide the synthetic `_dataset/` documents (the definition file every
 * version carries, §6.4) because they are not data.
 */

/** The synthetic `_dataset/` entries every version carries: the definition, not data. */
export const INTERNAL_DATASET_PREFIX = '_dataset/';

export const dataDiffEntries = <T extends { path: string }>(entries: T[] | undefined): T[] =>
    (entries ?? []).filter((entry) => !entry.path.startsWith(INTERNAL_DATASET_PREFIX));

const humanBytes = (bytes: number | undefined): string => {
    if (bytes === undefined || bytes === null) return '—';
    if (bytes < 1024) return `${bytes} B`;
    const units = ['KB', 'MB', 'GB', 'TB'];
    let value = bytes / 1024;
    let unit = 0;
    while (value >= 1024 && unit < units.length - 1) {
        value /= 1024;
        unit += 1;
    }
    return `${value.toFixed(value >= 10 ? 0 : 1)} ${units[unit]}`;
};

export interface DataDiffTableProps {
    entries?: DataDiffEntry[];
    emptyText?: string;
}

/** The object changes of a proposal, path by path. */
export const DataDiffTable: FC<DataDiffTableProps> = ({ entries, emptyText = 'No data changes in this proposal' }) => {
    const visible = dataDiffEntries(entries);
    if (visible.length === 0) {
        return <div className="text-muted dataset-data-diff-empty">{emptyText}</div>;
    }

    return (
        <Table hover responsive size="sm" className="dataset-data-diff-table">
            <thead>
                <tr>
                    <th>Path</th>
                    <th>Change</th>
                    <th>Size</th>
                </tr>
            </thead>
            <tbody>
                {visible.map((entry) => (
                    <tr key={`${entry.type}-${entry.path}`} className={`dataset-data-diff-${entry.type}`}>
                        <td>
                            <code className="dataset-data-diff-path">{entry.path}</code>
                        </td>
                        <td>
                            <ChangeBadge change={entry.type} />
                        </td>
                        <td className="text-nowrap text-muted">{humanBytes(entry.size_bytes)}</td>
                    </tr>
                ))}
            </tbody>
        </Table>
    );
};

export default DataDiffTable;
