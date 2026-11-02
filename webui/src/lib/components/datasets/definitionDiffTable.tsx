import React, { FC } from 'react';
import Table from 'react-bootstrap/Table';
import Badge from 'react-bootstrap/Badge';
import { ArrowRightIcon, PlusIcon, TrashIcon, PencilIcon } from '@primer/octicons-react';

import { ChangeType, DataItem, DatasetDiff, MetadataChange } from '../../api/datasets';
import { Link } from '../nav';
import { itemSourcePath } from './definitionDiff';
import { sourceObjectUrl } from './definitionItemsTable';

/** Badge/icon of a `added | removed | changed` change type (§10.2). */
export const ChangeBadge: FC<{ change: ChangeType }> = ({ change }) => {
    const icon = change === 'added' ? <PlusIcon /> : change === 'removed' ? <TrashIcon /> : <PencilIcon />;
    const variant = change === 'added' ? 'success' : change === 'removed' ? 'danger' : 'warning';
    return (
        <Badge bg={variant} className={`dataset-change-badge dataset-change-${change}`}>
            {icon} {change}
        </Badge>
    );
};

const itemSummary = (item: DataItem | undefined) => {
    if (!item) return null;
    const commit = item.ref?.id ?? '';
    const path = itemSourcePath(item);
    return (
        <Link href={sourceObjectUrl(item.repository, commit, path)} title={`${item.repository}@${commit}:${path}`}>
            <span className="dataset-item-repository">{item.repository}</span>
            <span className="text-muted">@{commit.length > 12 ? commit.substring(0, 12) : commit}</span>
            <code className="dataset-item-path ms-1">/{path}</code>
        </Link>
    );
};

const metadataSummary = (values: string[] | undefined) =>
    values === undefined || values.length === 0 ? (
        <span className="text-muted">&mdash;</span>
    ) : (
        <code>{values.join(', ')}</code>
    );

const MetadataChangeRow: FC<{ change: MetadataChange }> = ({ change }) => (
    <tr>
        <td>
            <code>{change.key}</code>
        </td>
        <td>
            <ChangeBadge change={change.type} />
        </td>
        <td>{metadataSummary(change.left)}</td>
        <td>
            <ArrowRightIcon className="text-muted" />
        </td>
        <td>{metadataSummary(change.right)}</td>
    </tr>
);

export interface DefinitionDiffTableProps {
    diff: DatasetDiff;
}

/**
 * The structural diff between two dataset definitions (DATASETS_DESIGN.md §10.3, §11.2
 * "Definition" tab): description, metadata keys and items, each matched by target/key.
 *
 * Items carry their source, so a changed or added item links straight to the commit and path it
 * now points at.
 */
export const DefinitionDiffTable: FC<DefinitionDiffTableProps> = ({ diff }) => {
    const items = diff.data ?? [];
    const metadata = diff.metadata ?? [];
    const nothing = items.length === 0 && metadata.length === 0 && diff.description === undefined;

    if (nothing) {
        return <div className="text-muted dataset-diff-empty">The two definitions are identical</div>;
    }

    return (
        <div className="dataset-definition-diff">
            {diff.description && (
                <Table size="sm" className="dataset-diff-description">
                    <tbody>
                        <tr>
                            <th scope="row">Description</th>
                            <td>
                                <ChangeBadge change="changed" />
                            </td>
                            <td className="text-muted">{diff.description.left || <em>empty</em>}</td>
                            <td>
                                <ArrowRightIcon className="text-muted" />
                            </td>
                            <td>{diff.description.right || <em>empty</em>}</td>
                        </tr>
                    </tbody>
                </Table>
            )}

            {metadata.length > 0 && (
                <>
                    <h6 className="mt-3">Metadata</h6>
                    <Table size="sm" responsive className="dataset-diff-metadata">
                        <thead>
                            <tr>
                                <th>Key</th>
                                <th>Change</th>
                                <th>From</th>
                                <th />
                                <th>To</th>
                            </tr>
                        </thead>
                        <tbody>
                            {metadata.map((change) => (
                                <MetadataChangeRow key={change.key} change={change} />
                            ))}
                        </tbody>
                    </Table>
                </>
            )}

            <h6 className="mt-3">
                Items <span className="text-muted small">({items.length} changed)</span>
            </h6>
            {items.length === 0 ? (
                <div className="text-muted">No item changes</div>
            ) : (
                <Table size="sm" responsive className="dataset-diff-items">
                    <thead>
                        <tr>
                            <th>Target</th>
                            <th>Change</th>
                            <th>From</th>
                            <th>To</th>
                        </tr>
                    </thead>
                    <tbody>
                        {items.map((change) => (
                            <tr key={change.target}>
                                <td>
                                    <code>{change.target}</code>
                                </td>
                                <td>
                                    <ChangeBadge change={change.type} />
                                </td>
                                <td>{itemSummary(change.left) ?? <span className="text-muted">&mdash;</span>}</td>
                                <td>{itemSummary(change.right) ?? <span className="text-muted">&mdash;</span>}</td>
                            </tr>
                        ))}
                    </tbody>
                </Table>
            )}
        </div>
    );
};

export default DefinitionDiffTable;
