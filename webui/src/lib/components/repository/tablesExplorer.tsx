import React, { FC, useState } from 'react';
import Alert from 'react-bootstrap/Alert';
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
    isIcebergUnauthenticatedError,
    namespaceLevelsBelowRef,
    namespaceOf,
} from '../../api/iceberg';
import { TablesEnterpriseInfo } from './tablesEnterpriseInfo';
import { IcebergTableDetails } from './tablesDetails';
import { NoTables, TablesError } from './tablesStates';
import {
    CreateNamespaceButton,
    CreateTableButton,
    DropNamespaceButton,
    DropTableButton,
    ManagementCallbacks,
} from './tablesManagement';

/**
 * The repository Tables tab (DATA_VIEW_TABLES in
 * `pages/repositories/repository/objects.jsx`).
 *
 * When the server runs with `iceberg.enabled: false` the catalog answers `GET /v1/config` with
 * `501` and the tab keeps the Enterprise upsell (`TablesEnterpriseInfo`). When the catalog is on,
 * the tab becomes an explorer of the Iceberg tables on the current ref: the namespace tree, the
 * tables in each namespace, and, for a selected table, its schema, partition spec, current
 * snapshot, snapshot history, properties and metadata location.
 *
 * The tab also *manages* tables, with the four operations the catalog accepts the UI's session
 * cookie for (`pkg/icebergcatalog/auth.go`): create a namespace, drop an empty namespace, create a
 * table from a schema editor, and drop a table. Committing a table's data stays an Iceberg client's
 * job, and so does registering one: the tab never sends a request whose write the catalog would not
 * authenticate with a cookie. The two destructive operations are behind a confirmation dialog, the
 * way the repositories and datasets pages confirm theirs.
 *
 * Refusals are rendered in place rather than escalated: a `401` (the catalog does not accept the
 * browser's session) becomes a sign-in state with a plain link, a `403` (the catalog's `catalog:*`
 * checks denied the operation) becomes the permission state and disables the management controls,
 * and anything else keeps the catalog's own message. Nothing here navigates - in particular the tab
 * never sends the reader to the login page, because the catalog's answer says nothing about the
 * core session (`TablesError`).
 */
export const TablesTab: FC<{ repository: string; refId: string }> = ({ repository, refId }) => {
    const { loading, disabled, error } = useIcebergConfig();

    if (loading) return <Loading />;
    if (disabled) return <TablesEnterpriseInfo />;
    if (error) return <TablesError error={error} />;
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
    // Every node loads its own listings, so a write reloads them all by bumping one dependency
    // (`version`) that each of their hooks carries.
    const [version, setVersion] = useState(0);
    // A management operation the catalog denied disables the management controls: the permission is
    // a property of the caller, so the next operation would be denied too.
    const [managementForbidden, setManagementForbidden] = useState(false);
    const [managementError, setManagementError] = useState<string | null>(null);
    const { response, loading, error, refetch } = useAPI(() => {
        // `version` is this listing's reload token, not one of its inputs: a write bumps it so
        // the catalog is asked again rather than the tree being guessed at.
        void version;
        return iceberg.listNamespaces({ parent: namespaceOf(repository, refId) });
    }, [repository, refId, version]);

    const callbacks: ManagementCallbacks = {
        onChanged: () => {
            // The selected table may be the one that was just dropped, and a create changes what
            // the tree holds: reload from the catalog rather than guess.
            setSelected(null);
            setManagementError(null);
            setVersion((current) => current + 1);
        },
        onError: setManagementError,
        onForbidden: () => setManagementForbidden(true),
    };

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
                    <div className="d-flex flex-wrap align-items-center gap-2">
                        <CreateNamespaceButton
                            repository={repository}
                            refId={refId}
                            defaultPath=""
                            disabled={managementForbidden}
                            callbacks={callbacks}
                        />
                        <Button size="sm" variant="secondary" onClick={refetch}>
                            Refresh
                        </Button>
                    </div>
                </Card.Header>
                <Card.Body>
                    {managementError !== null && (
                        <Alert
                            variant="danger"
                            dismissible
                            onClose={() => setManagementError(null)}
                            className="text-wrap text-break"
                            data-testid="iceberg-management-error"
                        >
                            {managementError}
                        </Alert>
                    )}
                    {managementForbidden && (
                        <div className="small text-muted mb-2" data-testid="iceberg-management-forbidden">
                            Managing tables is disabled: the catalog denied a management operation for this user. Ask an
                            administrator for the <code>catalog:CreateNamespace</code>,{' '}
                            <code>catalog:DeleteNamespace</code>, <code>catalog:CreateTable</code> and{' '}
                            <code>catalog:DeleteTable</code> permissions on this repository.
                        </div>
                    )}
                    {loading ? (
                        <Loading />
                    ) : error ? (
                        <TablesError error={error} />
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
                                            version={version}
                                            managementForbidden={managementForbidden}
                                            callbacks={callbacks}
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
    /** The reload token of the tab: changing it re-reads this node's listings. */
    version: number;
    managementForbidden: boolean;
    callbacks: ManagementCallbacks;
}

/**
 * One node of the namespace tree: the namespace's name, its immediate child namespaces and its
 * tables. Children and tables are read only when the node is expanded, so a large namespace tree
 * does not turn one page load into a walk over all of it.
 *
 * The node also carries the two operations that belong to a namespace: create a table in it, and
 * drop it (the catalog drops only an empty one). A table row carries the drop of that table.
 */
const NamespaceBranch: FC<NamespaceBranchProps> = ({
    repository,
    refId,
    levels,
    depth,
    selected,
    onSelectTable,
    version,
    managementForbidden,
    callbacks,
}) => {
    // The top level is open, so the tables of a flat warehouse are visible at once.
    const [expanded, setExpanded] = useState(depth === 0);
    // The namespace's own levels as one string: the hook's dependencies stay simple values, and
    // the address is rebuilt from it when the listing runs.
    const path = levels.join('\u001f');
    const { response, loading, error } = useAPI(async () => {
        // The tab's reload token: a write anywhere in the tree re-reads every expanded node.
        void version;
        if (!expanded) return null;
        const parent = namespaceOf(repository, refId, path === '' ? [] : path.split('\u001f'));
        const [children, tables] = await Promise.all([iceberg.listNamespaces({ parent }), iceberg.listTables(parent)]);
        return {
            namespaces: children.namespaces.map((namespace) => namespaceLevelsBelowRef(repository, refId, namespace)),
            tables: tables.identifiers.map((identifier) => identifier.name),
        };
    }, [expanded, repository, refId, path, version]);

    const childNamespaces = (response?.namespaces ?? []) as string[][];
    const tables = (response?.tables ?? []) as string[];
    const label = levels[levels.length - 1] ?? refId;
    const namespace = namespaceOf(repository, refId, levels);
    // A node whose own read was refused cannot be managed either - a `403` there is the catalog's
    // `catalog:*` decision, and a `401` is a session the catalog does not accept - so its controls
    // are disabled rather than offered and then refused.
    const nodeManagementDisabled =
        managementForbidden || isIcebergForbiddenError(error) || isIcebergUnauthenticatedError(error);

    return (
        <div className="mb-1" data-testid={`iceberg-namespace-${path}`}>
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
                    {error && <NamespaceError error={error} />}
                    {tables.map((table) => {
                        const isSelected =
                            selected !== null && selected.name === table && selected.levels.join('\u001f') === path;
                        return (
                            <div
                                key={table}
                                className="d-flex align-items-center"
                                style={{ paddingLeft: `${(depth + 1) * 1.25 + 1}rem` }}
                            >
                                <Button
                                    variant="link"
                                    className={`p-1 text-start text-decoration-none d-flex align-items-center gap-1 flex-grow-1${
                                        isSelected ? ' fw-bold text-primary' : ''
                                    }`}
                                    onClick={() => onSelectTable({ levels, name: table })}
                                >
                                    <TableIcon size={14} />
                                    <span className="text-break">{table}</span>
                                </Button>
                                <DropTableButton
                                    namespace={namespace}
                                    table={table}
                                    disabled={managementForbidden}
                                    callbacks={callbacks}
                                />
                            </div>
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
                            version={version}
                            managementForbidden={managementForbidden}
                            callbacks={callbacks}
                        />
                    ))}
                    {response && tables.length === 0 && childNamespaces.length === 0 && (
                        <div className="py-1 small text-muted" style={{ paddingLeft: `${(depth + 1) * 1.25 + 1}rem` }}>
                            No tables or nested namespaces.
                        </div>
                    )}
                    <div
                        className="d-flex align-items-center gap-2 py-1"
                        style={{ paddingLeft: `${(depth + 1) * 1.25 + 1}rem` }}
                    >
                        <CreateTableButton
                            namespace={namespace}
                            disabled={nodeManagementDisabled}
                            callbacks={callbacks}
                        />
                        <DropNamespaceButton
                            namespace={namespace}
                            disabled={nodeManagementDisabled}
                            callbacks={callbacks}
                        />
                    </div>
                </div>
            )}
        </div>
    );
};

/**
 * A failed read of one node, rendered inside the tree where the node is.
 *
 * A node is too small for the full cards of `TablesError`, so the two refusals become one muted
 * line each: a `401` is the catalog not accepting the session, a `403` is the missing permission.
 * Anything else is a real failure and keeps the alert, which carries the message and the status.
 */
const NamespaceError: FC<{ error: unknown }> = ({ error }) => {
    let refusal: string | null = null;
    if (isIcebergUnauthenticatedError(error)) {
        refusal = 'Your session is not valid for the Iceberg catalog. Sign in again to read this namespace.';
    } else if (isIcebergForbiddenError(error)) {
        refusal = 'You are not allowed to read this namespace.';
    }

    if (refusal !== null) return <div className="ps-4 py-1 small text-muted">{refusal}</div>;
    return (
        <div className="ps-4 py-1">
            <AlertError error={error as Error} />
        </div>
    );
};
