import React, { FC, useMemo, useState } from 'react';
import Button from 'react-bootstrap/Button';
import Col from 'react-bootstrap/Col';
import Form from 'react-bootstrap/Form';
import Row from 'react-bootstrap/Row';
import Table from 'react-bootstrap/Table';
import { FileDirectoryIcon, PlusIcon, TrashIcon } from '@primer/octicons-react';

import { RefPicker, PathPicker, RepositoryPicker } from './itemPickers';
import {
    EditorItem,
    EditorItemType,
    MappedItemError,
    localItemProblems,
    normaliseSourcePath,
    normaliseTarget,
    shortCommit,
} from './editorModel';

/**
 * The items table of the editor (§11.2): add item → repository → ref → type → source path →
 * target, with the normalisation feedback and every problem shown inline under the offending
 * row.
 *
 * Local problems come from `localItemProblems` (the `pkg/datasets` rules) and are available
 * immediately; `serverErrors` are the per-row findings of `POST /datasets/{dataset}/validate`,
 * already mapped from the server's `data[<index>]` paths by `mapValidationErrors`.
 */

export interface ItemsTableProps {
    items: EditorItem[];
    repositories: string[];
    serverErrors: Record<string, MappedItemError[]>;
    onChange: (item: EditorItem) => void;
    onRemove: (key: string) => void;
    onAdd: () => void;
    disabled?: boolean;
}

const ITEM_TYPES: EditorItemType[] = ['object', 'prefix'];

/** Renders the "stored as ..." hint when normalisation changes what the user typed. */
const NormalisationHint: FC<{ raw: string; normalised: string; what: string }> = ({ raw, normalised, what }) =>
    raw === normalised ? null : (
        <Form.Text className="text-muted">
            {what} <code>{normalised === '' ? '/' : normalised}</code>
        </Form.Text>
    );

const ItemRow: FC<{
    item: EditorItem;
    repositories: string[];
    local: string[];
    server: MappedItemError[];
    onChange: (item: EditorItem) => void;
    onRemove: (key: string) => void;
    onPickPath: (key: string) => void;
    disabled?: boolean;
}> = ({ item, repositories, local, server, onChange, onRemove, onPickPath, disabled }) => {
    const normalisedTarget = normaliseTarget(item.target);
    const normalisedSource = normaliseSourcePath(item.type, item.path);
    const problems = [
        ...local,
        ...server.map((error) => (error.field ? `${error.field}: ${error.message}` : error.message)),
    ];

    return (
        <tr className={problems.length > 0 ? 'table-danger' : undefined}>
            <td style={{ minWidth: 160 }}>
                <RepositoryPicker
                    repositories={repositories}
                    value={item.repository}
                    disabled={disabled}
                    onChange={(repository) => onChange({ ...item, repository, pinnedCommit: undefined })}
                />
            </td>
            <td style={{ minWidth: 220 }}>
                <RefPicker
                    repo={item.repository}
                    refType={item.refType}
                    refId={item.refId}
                    pinnedCommit={item.pinnedCommit}
                    disabled={disabled}
                    onChange={(next) => onChange({ ...item, ...next, pinnedCommit: undefined })}
                />
            </td>
            <td style={{ maxWidth: 130 }}>
                <Form.Select
                    size="sm"
                    aria-label="Item type"
                    value={item.type}
                    disabled={disabled}
                    onChange={(e) => onChange({ ...item, type: e.target.value as EditorItemType })}
                >
                    {ITEM_TYPES.map((type) => (
                        <option key={type} value={type}>
                            {type}
                        </option>
                    ))}
                </Form.Select>
            </td>
            <td style={{ minWidth: 240 }}>
                <div className="d-flex gap-1">
                    <Form.Control
                        size="sm"
                        aria-label="Source path"
                        placeholder={item.type === 'prefix' ? 'data/customers/' : 'geo/countries.csv'}
                        value={item.path}
                        disabled={disabled}
                        onChange={(e) => onChange({ ...item, path: e.target.value, pinnedCommit: undefined })}
                    />
                    <Button
                        variant="outline-secondary"
                        size="sm"
                        aria-label="Browse paths"
                        disabled={disabled || item.repository === '' || item.refId === ''}
                        onClick={() => onPickPath(item.key)}
                    >
                        <FileDirectoryIcon />
                    </Button>
                </div>
                <NormalisationHint raw={item.path} normalised={normalisedSource} what="Stored as" />
            </td>
            <td style={{ minWidth: 200 }}>
                <Form.Control
                    size="sm"
                    aria-label="Target path"
                    placeholder="customers"
                    value={item.target}
                    disabled={disabled}
                    onChange={(e) => onChange({ ...item, target: e.target.value, pinnedCommit: undefined })}
                />
                <NormalisationHint raw={item.target} normalised={normalisedTarget} what="Stored as" />
            </td>
            <td className="text-end">
                <Button
                    variant="link"
                    size="sm"
                    className="text-danger"
                    aria-label={`Remove item ${item.target || item.repository || item.key}`}
                    disabled={disabled}
                    onClick={() => onRemove(item.key)}
                >
                    <TrashIcon />
                </Button>
            </td>
            {problems.length > 0 && (
                <td colSpan={6} className="p-0 border-0">
                    <div className="dataset-item-problems px-2 pb-2" role="alert">
                        {problems.map((problem) => (
                            <div key={problem} className="text-danger small">
                                {problem}
                            </div>
                        ))}
                    </div>
                </td>
            )}
        </tr>
    );
};

export const ItemsTable: FC<ItemsTableProps> = ({
    items,
    repositories,
    serverErrors,
    onChange,
    onRemove,
    onAdd,
    disabled,
}) => {
    const localProblems = useMemo(() => localItemProblems(items), [items]);
    const [pickerFor, setPickerFor] = useState<string | null>(null);
    const pickerItem = items.find((item) => item.key === pickerFor);

    if (items.length === 0) {
        return (
            <div className="dataset-items-empty border rounded p-4 text-center">
                <p className="text-muted mb-3">
                    A dataset is built from one or more repository items. Add the first one to get started.
                </p>
                <Button variant="success" onClick={onAdd} disabled={disabled}>
                    <PlusIcon className="me-1" />
                    Add item
                </Button>
            </div>
        );
    }

    return (
        <>
            <Table responsive size="sm" className="dataset-items-table align-middle">
                <thead>
                    <tr>
                        <th>Repository</th>
                        <th>Ref</th>
                        <th>Type</th>
                        <th>Source path</th>
                        <th>Target</th>
                        <th />
                    </tr>
                </thead>
                <tbody>
                    {items.map((item) => (
                        <ItemRow
                            key={item.key}
                            item={item}
                            repositories={repositories}
                            local={localProblems[item.key] ?? []}
                            server={serverErrors[item.key] ?? []}
                            disabled={disabled}
                            onChange={onChange}
                            onRemove={onRemove}
                            onPickPath={setPickerFor}
                        />
                    ))}
                </tbody>
            </Table>
            <Row className="align-items-center">
                <Col>
                    <Button variant="outline-success" size="sm" onClick={onAdd} disabled={disabled}>
                        <PlusIcon className="me-1" />
                        Add item
                    </Button>
                </Col>
                <Col className="text-end text-muted small">
                    {items.length} item{items.length === 1 ? '' : 's'}
                </Col>
            </Row>
            {pickerItem && (
                <PathPicker
                    show={pickerFor !== null}
                    repo={pickerItem.repository}
                    refId={pickerItem.refId}
                    itemType={pickerItem.type}
                    initialPath={pickerItem.path}
                    onHide={() => setPickerFor(null)}
                    onSelect={(path) => onChange({ ...pickerItem, path })}
                />
            )}
        </>
    );
};

/** Short "pinned to abc1234" text, exported for reuse in the editor summary. */
export const pinnedLabel = (commit: string): string => `pinned to ${shortCommit(commit)}`;
