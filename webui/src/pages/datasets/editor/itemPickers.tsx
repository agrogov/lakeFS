import React, { FC, useEffect, useState } from 'react';
import Button from 'react-bootstrap/Button';
import Breadcrumb from 'react-bootstrap/Breadcrumb';
import Form from 'react-bootstrap/Form';
import InputGroup from 'react-bootstrap/InputGroup';
import Modal from 'react-bootstrap/Modal';
import Table from 'react-bootstrap/Table';
import { FileDirectoryIcon, FileIcon } from '@primer/octicons-react';

import { MAX_LISTING_AMOUNT, branches, objects, tags } from '../../../lib/api';
import { AlertError, Loading } from '../../../lib/components/controls';
import { useAPI } from '../../../lib/hooks/api';
import { SourceRefType } from '../../../lib/api/datasets';
import { EditorItemType, shortCommit } from './editorModel';

/**
 * The three pickers of the items table (§11.2): source repository, source ref and source path.
 *
 * The path picker browses a repository through the same object listing the repository tree uses
 * (`GET /repositories/{repo}/refs/{ref}/objects/ls`, i.e. `objects.list`) and renders its
 * `common_prefix`/`object` entries with the same iconography, so choosing a dataset source looks
 * like browsing the repository it comes from.
 */

// ---------------------------------------------------------------------------
// Repository picker
// ---------------------------------------------------------------------------

export const RepositoryPicker: FC<{
    repositories: string[];
    value: string;
    onChange: (repository: string) => void;
    disabled?: boolean;
    id?: string;
}> = ({ repositories, value, onChange, disabled, id }) => (
    <Form.Select
        id={id}
        size="sm"
        aria-label="Source repository"
        value={value}
        disabled={disabled}
        onChange={(e) => onChange(e.target.value)}
    >
        <option value="">Select a repository…</option>
        {repositories.map((repository) => (
            <option key={repository} value={repository}>
                {repository}
            </option>
        ))}
    </Form.Select>
);

// ---------------------------------------------------------------------------
// Ref picker
// ---------------------------------------------------------------------------

const REF_TYPES: SourceRefType[] = ['branch', 'tag', 'commit'];

const listRefIds = async (repo: string, refType: SourceRefType): Promise<string[]> => {
    if (repo === '' || (refType !== 'branch' && refType !== 'tag')) return [];
    const response =
        refType === 'branch'
            ? ((await branches.list(repo, false, '', '', MAX_LISTING_AMOUNT)) as { results?: { id: string }[] })
            : ((await tags.list(repo, '', '', MAX_LISTING_AMOUNT)) as { results?: { id: string }[] });
    return (response.results ?? []).map((entry) => entry.id);
};

/**
 * Branch / tag / commit selector. Stored definitions only ever hold commits (§5.1), so a
 * branch or tag is resolved by the server on save; until then the row shows the ref it was
 * chosen from, and "pinned to <commit>" once a validation returned the resolution.
 */
export const RefPicker: FC<{
    repo: string;
    refType: SourceRefType;
    refId: string;
    pinnedCommit?: string;
    onChange: (next: { refType: SourceRefType; refId: string }) => void;
    disabled?: boolean;
}> = ({ repo, refType, refId, pinnedCommit, onChange, disabled }) => {
    const { response, loading, error } = useAPI(() => listRefIds(repo, refType), [repo, refType]);
    const refIds = response ?? [];

    return (
        <div className="dataset-ref-picker">
            <InputGroup size="sm">
                <Form.Select
                    aria-label="Ref type"
                    value={refType}
                    disabled={disabled}
                    onChange={(e) => onChange({ refType: e.target.value as SourceRefType, refId: '' })}
                    style={{ maxWidth: 110 }}
                >
                    {REF_TYPES.map((type) => (
                        <option key={type} value={type}>
                            {type}
                        </option>
                    ))}
                </Form.Select>
                {refType === 'commit' ? (
                    <Form.Control
                        aria-label="Commit id"
                        placeholder="commit id"
                        value={refId}
                        disabled={disabled}
                        onChange={(e) => onChange({ refType, refId: e.target.value })}
                    />
                ) : (
                    <Form.Select
                        aria-label={`${refType} name`}
                        value={refId}
                        disabled={disabled || loading || repo === ''}
                        onChange={(e) => onChange({ refType, refId: e.target.value })}
                    >
                        <option value="">{repo === '' ? 'Pick a repository first' : `Select a ${refType}…`}</option>
                        {refIds.map((id) => (
                            <option key={id} value={id}>
                                {id}
                            </option>
                        ))}
                    </Form.Select>
                )}
            </InputGroup>
            {error && <div className="text-danger small">Could not list {refType}s</div>}
            {pinnedCommit && (
                <div className="text-muted small" title={pinnedCommit}>
                    pinned to <code>{shortCommit(pinnedCommit)}</code>
                </div>
            )}
        </div>
    );
};

// ---------------------------------------------------------------------------
// Path picker
// ---------------------------------------------------------------------------

interface TreeEntry {
    path: string;
    path_type: 'object' | 'common_prefix';
    size_bytes?: number;
}

const listEntries = async (repo: string, refId: string, prefix: string): Promise<TreeEntry[]> => {
    if (repo === '' || refId === '') return [];
    const response = (await objects.list(repo, refId, prefix, '', false, MAX_LISTING_AMOUNT)) as {
        results?: TreeEntry[];
    };
    return (response.results ?? []).filter((entry) => entry.path !== '');
};

const entryName = (entry: TreeEntry, prefix: string): string => {
    const name = entry.path.startsWith(prefix) ? entry.path.slice(prefix.length) : entry.path;
    return name.endsWith('/') ? name.slice(0, -1) : name;
};

const parentPrefix = (prefix: string): string => {
    const trimmed = prefix.endsWith('/') ? prefix.slice(0, -1) : prefix;
    const index = trimmed.lastIndexOf('/');
    return index < 0 ? '' : trimmed.slice(0, index + 1);
};

/**
 * Modal that browses one repository ref and returns the chosen path. Prefixes end with `/`
 * (the normalised prefix form); an empty path means the whole repository.
 */
export const PathPicker: FC<{
    show: boolean;
    repo: string;
    refId: string;
    itemType: EditorItemType;
    initialPath?: string;
    onSelect: (path: string) => void;
    onHide: () => void;
}> = ({ show, repo, refId, itemType, initialPath = '', onSelect, onHide }) => {
    const [prefix, setPrefix] = useState(initialPath);

    // Start each open at the directory of the current value and reset when it closes, so
    // reopening the picker for another row never shows the previous row's location.
    useEffect(() => {
        if (!show) return;
        const path = initialPath;
        const isDirectory = itemType === 'prefix' || path.endsWith('/');
        setPrefix(isDirectory ? path : parentPrefix(path));
    }, [show, initialPath, itemType]);

    const { response, loading, error } = useAPI(
        () => (show ? listEntries(repo, refId, prefix) : []),
        [show, repo, refId, prefix],
    );
    const entries = response ?? [];

    const segments = prefix
        .split('/')
        .filter((segment) => segment !== '')
        .map((segment, index, all) => ({
            name: segment,
            path: `${all.slice(0, index + 1).join('/')}/`,
        }));

    const selectPrefix = () => {
        onSelect(prefix);
        onHide();
    };

    return (
        <Modal show={show} onHide={onHide} size="lg" scrollable>
            <Modal.Header closeButton>
                <Modal.Title className="h6">
                    Choose a {itemType === 'object' ? 'object' : 'prefix'} in <code>{repo}</code>
                    {refId === '' ? '' : <span className="text-muted"> @ {refId}</span>}
                </Modal.Title>
            </Modal.Header>
            <Modal.Body>
                <Breadcrumb className="dataset-path-picker-breadcrumb">
                    <Breadcrumb.Item onClick={() => setPrefix('')} active={prefix === ''}>
                        {repo || 'repository'}
                    </Breadcrumb.Item>
                    {segments.map((segment, index) => (
                        <Breadcrumb.Item
                            key={segment.path}
                            active={index === segments.length - 1}
                            onClick={() => setPrefix(segment.path)}
                        >
                            {segment.name}
                        </Breadcrumb.Item>
                    ))}
                </Breadcrumb>
                {loading && <Loading />}
                {error && <AlertError error={error as Error} />}
                {!loading && !error && (
                    <Table borderless size="sm" className="tree-table dataset-path-picker-table">
                        <tbody>
                            {entries.map((entry) => (
                                <tr key={entry.path}>
                                    <td>
                                        {entry.path_type === 'common_prefix' ? <FileDirectoryIcon /> : <FileIcon />}{' '}
                                        {entry.path_type === 'common_prefix' ? (
                                            <Button
                                                variant="link"
                                                size="sm"
                                                className="p-0 align-baseline"
                                                onClick={() => setPrefix(entry.path)}
                                            >
                                                {entryName(entry, prefix)}/
                                            </Button>
                                        ) : (
                                            entryName(entry, prefix)
                                        )}
                                    </td>
                                    <td className="text-end">
                                        {(
                                            itemType === 'object'
                                                ? entry.path_type === 'object'
                                                : entry.path_type === 'common_prefix'
                                        ) ? (
                                            <Button
                                                variant="outline-secondary"
                                                size="sm"
                                                onClick={() => {
                                                    onSelect(entry.path);
                                                    onHide();
                                                }}
                                            >
                                                Select
                                            </Button>
                                        ) : null}
                                    </td>
                                </tr>
                            ))}
                            {entries.length === 0 && (
                                <tr>
                                    <td className="text-muted">This directory is empty.</td>
                                </tr>
                            )}
                        </tbody>
                    </Table>
                )}
            </Modal.Body>
            <Modal.Footer>
                <span className="me-auto text-muted small">
                    Selected: <code>{prefix === '' ? '/' : prefix}</code>
                </span>
                <Button variant="secondary" onClick={onHide}>
                    Cancel
                </Button>
                <Button
                    variant="primary"
                    onClick={selectPrefix}
                    disabled={repo === '' || refId === '' || (itemType === 'object' && prefix === '')}
                >
                    Select {prefix === '' ? 'whole repository' : `"${prefix}"`}
                </Button>
            </Modal.Footer>
        </Modal>
    );
};
