import React, { FC, useState } from 'react';
import Badge from 'react-bootstrap/Badge';
import Button from 'react-bootstrap/Button';
import Card from 'react-bootstrap/Card';
import { ChevronDownIcon, ChevronRightIcon, FileDirectoryIcon, TableIcon } from '@primer/octicons-react';

import { AlertError, Loading } from '../controls';
import { useAPI } from '../../hooks/api';
import {
    iceberg,
    isIcebergDisabledError,
    isIcebergForbiddenError,
    namespaceLevelsBelowRef,
    namespaceOf,
} from '../../api/iceberg';
import { TablesEnterpriseInfo } from './tablesEnterpriseInfo';
import { IcebergTableDetails } from './tablesDetails';
import { NoTables, TablesPermissionRequired } from './tablesStates';

/**
 * The repository Tables tab (DATA_VIEW_TABLES in
 * `pages/repositories/repository/objects.jsx`).
 *
 * When the server runs with `iceberg.enabled: false` the catalog answers `GET /v1/config` with
 * `501` and the tab keeps the Enterprise upsell (`TablesEnterpriseInfo`). When the catalog is on,
 * the tab becomes a read-only explorer of the Iceberg tables on the current ref: the namespace
 * tree, the tables in each namespace, and, for a selected table, its schema, partition spec,
 * current snapshot, snapshot history, properties and metadata location.
 *
 * Reads only, deliberately: creating, dropping and committing tables stays an Iceberg client's
 * job, and the catalog accepts the UI's session cookie for GET and HEAD alone
 * (`pkg/icebergcatalog/auth.go`). Authorization is the catalog's `catalog:*` checks; this file only
 * turns their `403` into a state that says so.
 */
export const TablesTab: FC<{ repository: string; refId: string }> = ({ repository, refId }) => {
    const { loading, disabled, error } = useIcebergConfig();

    if (loading) return <Loading />;
    if (disabled) return <TablesEnterpriseInfo />;
    if (error) {
        if (isIcebergForbiddenError(error)) return <TablesPermissionRequired />;
        return <AlertError error={error} />;
    }
    return <IcebergTablesExplorer repository={repository} refId={refId} />;
};

interface IcebergConfigState {
    loading: boolean;
    /** The catalog is switched off (or not mounted), so the Enterprise upsell stays. */
    disabled: boolean;
    error: Error | null;
}

/** `GET /v1/config`, the catalog's own answer to "is there a catalog here, and is it on". */
const useIcebergConfig = (): IcebergConfigState => {
    const { loading, error } = useAPI(() => iceberg.getConfig(), []);
    if (loading) return { loading: true, disabled: false, error: null };
    if (error) return { loading: false, disabled: isIcebergDisabledError(error), error: error as Error };
    return { loading: false, disabled: false, error: null };
};

/** The ref's namespace tree and the table view; rendered once the catalog is known to be on. */
const IcebergTablesExplorer: FC<{ repository: string; refId: string }> = ({ repository, refId }) => {
    const [selected, setSelected] = useState<{ levels: string[]; name: string } | null>(null);
    const { response, loading, error, refetch } = useAPI(
        () => iceberg.listNamespaces({ parent: namespaceOf(repository, refId) }),
        [repository, refId],
    );

    const refLabel = `${repository}/${refId}`;
    const namespaces = ((response?.namespaces ?? []) as string[][]).map((namespace) =>
        namespaceLevelsBelowRef(repository, refId, namespace),
    );

    return (
        <div className="tree-container tree-container-wide">
            <Card>
                <Card.Header className="d-flex flex-wrap align-items-center justify-content-between gap-2">
                    <span>
                        Iceberg tables on <code>{refLabel}</code>
                    </span>
                    <Button size="sm" variant="secondary" onClick={refetch}>
                        Refresh
                    </Button>
                </Card.Header>
                <Card.Body>
                    {loading ? (
                        <Loading />
                    ) : error ? (
                        isIcebergForbiddenError(error) ? (
                            <TablesPermissionRequired />
                        ) : (
                            <AlertError error={error} />
                        )
                    ) : namespaces.length === 0 ? (
                        <NoTables refLabel={refLabel} />
                    ) : (
                        <div className="row g-3">
                            <div className="col-lg-4">
                                <div className="border rounded p-2" data-testid="iceberg-namespace-tree">
                                    {namespaces.map((levels) => (
                                        <NamespaceBranch
                                            key={levels.join('\u001f')}
                                            repository={repository}
                                            refId={refId}
                                            levels={levels}
                                            depth={0}
                                            selected={selected}
                                            onSelectTable={setSelected}
                                        />
                                    ))}
                                </div>
                            </div>
                            <div className="col-lg-8">
                                {selected ? (
                                    <IcebergTableDetails
                                        namespace={namespaceOf(repository, refId, selected.levels)}
                                        table={selected.name}
                                    />
                                ) : (
                                    <div className="text-muted p-3">
                                        Select a table to see its schema, partition spec, snapshots and properties.
                                    </div>
                                )}
                            </div>
                        </div>
                    )}
                </Card.Body>
            </Card>
        </div>
    );
};

interface NamespaceBranchProps {
    repository: string;
    refId: string;
    /** The namespace's own levels, below the repository and the ref. */
    levels: string[];
    depth: number;
    selected: { levels: string[]; name: string } | null;
    onSelectTable: (table: { levels: string[]; name: string }) => void;
}

/**
 * One node of the namespace tree: the namespace's name, its immediate child namespaces and its
 * tables. Children and tables are read only when the node is expanded, so a large namespace tree
 * does not turn one page load into a walk over all of it.
 */
const NamespaceBranch: FC<NamespaceBranchProps> = ({ repository, refId, levels, depth, selected, onSelectTable }) => {
    // The top level is open, so the tables of a flat warehouse are visible at once.
    const [expanded, setExpanded] = useState(depth === 0);
    // The namespace's own levels as one string: the hook's dependencies stay simple values, and
    // the address is rebuilt from it when the listing runs.
    const path = levels.join('\u001f');
    const { response, loading, error } = useAPI(async () => {
        if (!expanded) return null;
        const parent = namespaceOf(repository, refId, path === '' ? [] : path.split('\u001f'));
        const [children, tables] = await Promise.all([iceberg.listNamespaces({ parent }), iceberg.listTables(parent)]);
        return {
            namespaces: children.namespaces.map((namespace) => namespaceLevelsBelowRef(repository, refId, namespace)),
            tables: tables.identifiers.map((identifier) => identifier.name),
        };
    }, [expanded, repository, refId, path]);

    const childNamespaces = (response?.namespaces ?? []) as string[][];
    const tables = (response?.tables ?? []) as string[];
    const label = levels[levels.length - 1] ?? refId;

    return (
        <div className="mb-1">
            <Button
                variant="link"
                className="p-1 text-start text-decoration-none d-flex align-items-center gap-1 w-100"
                style={{ paddingLeft: `${depth * 1.25}rem` }}
                onClick={() => setExpanded((open) => !open)}
                aria-expanded={expanded}
            >
                {expanded ? <ChevronDownIcon size={14} /> : <ChevronRightIcon size={14} />}
                <FileDirectoryIcon size={14} />
                <span className="fw-semibold text-break">{label}</span>
                {expanded && response && tables.length > 0 && (
                    <Badge bg="secondary" pill className="ms-1">
                        {tables.length}
                    </Badge>
                )}
            </Button>

            {expanded && (
                <div>
                    {loading && <div className="ps-4 py-1 small text-muted">Loading namespaces and tables…</div>}
                    {error &&
                        (isIcebergForbiddenError(error) ? (
                            <div className="ps-4 py-1 small text-muted">
                                You are not allowed to read this namespace.
                            </div>
                        ) : (
                            <AlertError error={error} />
                        ))}
                    {tables.map((table) => {
                        const isSelected =
                            selected !== null && selected.name === table && selected.levels.join('\u001f') === path;
                        return (
                            <Button
                                key={table}
                                variant="link"
                                className={`p-1 text-start text-decoration-none d-flex align-items-center gap-1 w-100${
                                    isSelected ? ' fw-bold text-primary' : ''
                                }`}
                                style={{ paddingLeft: `${(depth + 1) * 1.25 + 1}rem` }}
                                onClick={() => onSelectTable({ levels, name: table })}
                            >
                                <TableIcon size={14} />
                                <span className="text-break">{table}</span>
                            </Button>
                        );
                    })}
                    {childNamespaces.map((child) => (
                        <NamespaceBranch
                            key={child.join('\u001f')}
                            repository={repository}
                            refId={refId}
                            levels={child}
                            depth={depth + 1}
                            selected={selected}
                            onSelectTable={onSelectTable}
                        />
                    ))}
                    {response && tables.length === 0 && childNamespaces.length === 0 && (
                        <div className="py-1 small text-muted" style={{ paddingLeft: `${(depth + 1) * 1.25 + 1}rem` }}>
                            No tables or nested namespaces.
                        </div>
                    )}
                </div>
            )}
        </div>
    );
};
