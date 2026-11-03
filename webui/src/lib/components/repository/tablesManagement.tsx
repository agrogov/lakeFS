import React, { FC, useState } from 'react';
import Alert from 'react-bootstrap/Alert';
import Button from 'react-bootstrap/Button';
import Form from 'react-bootstrap/Form';
import Modal from 'react-bootstrap/Modal';
import Spinner from 'react-bootstrap/Spinner';
import { PlusIcon, TrashIcon } from '@primer/octicons-react';

import { ConfirmationButton } from '../modals';
import {
    ICEBERG_PRIMITIVE_TYPES,
    IcebergCreateTableForm,
    IcebergNewColumn,
    iceberg,
    isIcebergForbiddenError,
    isIcebergUnauthenticatedError,
    namespaceLabel,
    namespaceOf,
    namespacePathLevels,
    validateCreateTableForm,
    validateNamespacePath,
} from '../../api/iceberg';

/**
 * The Tables tab's management controls: create a namespace, create a table, drop an empty
 * namespace and drop a table.
 *
 * Every control here is one of the four operations the catalog accepts the UI's session cookie for
 * (`pkg/icebergcatalog/auth.go`): they carry the `X-Lakefs-Client: lakefs-webui/...` header the
 * shared API headers set, and they address exactly `POST`/`DELETE` on the namespace and table
 * collections. Nothing here commits, registers or renames a table, because the catalog does not
 * accept the cookie for those and the tab never asks for what it cannot do.
 *
 * Failures are shown in place, never escalated:
 *
 *   - a create keeps its dialog open and renders the catalog's own message in it (the pattern of
 *     the repositories page's create dialog), so the reader can correct the form and retry;
 *   - a drop renders the message in the tab, next to the tree it belongs to;
 *   - a `401` becomes a plain sentence about the catalog session, never a login redirect: the
 *     catalog's `401` says nothing about the reader's core lakeFS session
 *     (`webui/src/lib/api/iceberg.ts`, "Error handling");
 *   - a `403` is reported to the tab as well, which disables these controls: a catalogue that has
 *     denied one management operation will deny the next, and offering a button that cannot work is
 *     worse than saying why.
 */

/** What a management control reports back to the Tables tab it lives in. */
export interface ManagementCallbacks {
    /** The catalog accepted the operation: reload the listings. */
    onChanged: () => void;
    /** The operation failed: the tab reports this message in place. */
    onError: (message: string) => void;
    /** The catalog denied the operation (403): the tab disables management. */
    onForbidden: () => void;
}

/**
 * The message a failed management operation is reported with. A `401` and a `403` get a sentence of
 * their own - the two are different facts and neither is a lost core session - and everything else
 * is the catalog's own message, which is what an Iceberg client would see.
 */
export const managementErrorMessage = (error: unknown, action: string): string => {
    if (isIcebergUnauthenticatedError(error)) {
        return `Your session is not valid for the Iceberg catalog, so it could not ${action}. Sign in again and retry.`;
    }
    if (isIcebergForbiddenError(error)) {
        return `You are not allowed to ${action} here: the catalog's catalog:* permissions decide that, and this user does not hold them.`;
    }
    return error instanceof Error && error.message !== '' ? error.message : `The catalog could not ${action}.`;
};

/** A blank column of the create-table form: a name to type, a type to keep or change, optional. */
const emptyColumn = (): IcebergNewColumn => ({ name: '', type: 'long', required: false });

/** The schema editor: one row per column, with add and remove. */
const ColumnEditor: FC<{
    columns: IcebergNewColumn[];
    onChange: (columns: IcebergNewColumn[]) => void;
    disabled: boolean;
}> = ({ columns, onChange, disabled }) => {
    const update = (index: number, patch: Partial<IcebergNewColumn>) => {
        onChange(columns.map((column, i) => (i === index ? { ...column, ...patch } : column)));
    };

    return (
        <div data-testid="iceberg-column-editor">
            <div className="d-flex align-items-center justify-content-between mb-2">
                <span className="fw-semibold">Columns</span>
                <Button
                    size="sm"
                    variant="outline-secondary"
                    disabled={disabled}
                    onClick={() => onChange([...columns, emptyColumn()])}
                >
                    <PlusIcon size={14} className="me-1" /> Add column
                </Button>
            </div>
            {columns.length === 0 && <div className="small text-muted mb-2">A table needs at least one column.</div>}
            {columns.map((column, index) => (
                <div className="row g-2 align-items-center mb-2" key={index}>
                    <div className="col-5">
                        <Form.Control
                            size="sm"
                            aria-label={`Column ${index + 1} name`}
                            placeholder="column name"
                            value={column.name}
                            disabled={disabled}
                            onChange={(event) => update(index, { name: event.target.value })}
                        />
                    </div>
                    <div className="col-4">
                        <Form.Select
                            size="sm"
                            aria-label={`Column ${index + 1} type`}
                            value={column.type}
                            disabled={disabled}
                            onChange={(event) => update(index, { type: event.target.value })}
                        >
                            {/* A type the caller typed by hand - a nested one, say - is kept in the
                                list, so editing a column never silently rewrites it. */}
                            {!(ICEBERG_PRIMITIVE_TYPES as readonly string[]).includes(column.type) && (
                                <option value={column.type}>{column.type}</option>
                            )}
                            {ICEBERG_PRIMITIVE_TYPES.map((type) => (
                                <option key={type} value={type}>
                                    {type}
                                </option>
                            ))}
                        </Form.Select>
                    </div>
                    <div className="col-2">
                        <Form.Check
                            type="checkbox"
                            id={`iceberg-column-required-${index}`}
                            label="Required"
                            checked={column.required}
                            disabled={disabled}
                            onChange={(event) => update(index, { required: event.target.checked })}
                        />
                    </div>
                    <div className="col-1 text-end">
                        <Button
                            size="sm"
                            variant="link"
                            className="text-danger p-0"
                            aria-label={`Remove column ${index + 1}`}
                            disabled={disabled}
                            onClick={() => onChange(columns.filter((_, i) => i !== index))}
                        >
                            <TrashIcon size={14} />
                        </Button>
                    </div>
                </div>
            ))}
        </div>
    );
};

/** The "creating…" body of a submit button. */
const BusyLabel: FC<{ label: string }> = ({ label }) => (
    <>
        <Spinner as="span" size="sm" animation="border" role="status" className="me-2" />
        {label}
    </>
);

/**
 * `POST /v1/{prefix}/namespaces`: the create-namespace dialog.
 *
 * The dialog asks for the levels below the ref (`inventory` or `inventory.books`), because the
 * repository and the ref are fixed by the repository page the tab lives in. Every parent level has
 * to exist already - the catalog's `404 NoSuchNamespaceException` says so if not - and it is
 * answered in the same place the form is.
 */
export const CreateNamespaceButton: FC<{
    repository: string;
    refId: string;
    /** The levels below the ref the input starts with; empty for a fresh namespace. */
    defaultPath: string;
    disabled: boolean;
    callbacks: ManagementCallbacks;
}> = ({ repository, refId, defaultPath, disabled, callbacks }) => {
    const [show, setShow] = useState(false);
    const [path, setPath] = useState(defaultPath);
    const [error, setError] = useState<string | null>(null);
    const [busy, setBusy] = useState(false);

    const open = () => {
        setPath(defaultPath);
        setError(null);
        setShow(true);
    };
    const submit = async () => {
        const problem = validateNamespacePath(path);
        if (problem !== null) {
            setError(problem);
            return;
        }
        setBusy(true);
        setError(null);
        try {
            await iceberg.createNamespace(namespaceOf(repository, refId, namespacePathLevels(path)));
            setShow(false);
            callbacks.onChanged();
        } catch (submitError) {
            if (isIcebergForbiddenError(submitError)) callbacks.onForbidden();
            setError(managementErrorMessage(submitError, 'create the namespace'));
        } finally {
            setBusy(false);
        }
    };

    return (
        <>
            <Button size="sm" variant="primary" disabled={disabled} onClick={open}>
                <PlusIcon size={14} className="me-1" /> Create namespace
            </Button>
            <Modal show={show} onHide={() => setShow(false)}>
                <Modal.Header closeButton>
                    <Modal.Title>Create namespace</Modal.Title>
                </Modal.Header>
                <Modal.Body>
                    {error !== null && (
                        <Alert variant="danger" className="text-wrap text-break">
                            {error}
                        </Alert>
                    )}
                    <Form
                        onSubmit={(event) => {
                            event.preventDefault();
                            void submit();
                        }}
                    >
                        <Form.Group controlId="iceberg-create-namespace-path">
                            <Form.Label>Namespace</Form.Label>
                            <Form.Control
                                autoFocus
                                value={path}
                                disabled={busy}
                                placeholder="inventory.books"
                                onChange={(event) => setPath(event.target.value)}
                            />
                            <Form.Text className="text-muted">
                                Levels below <code>{`${repository}/${refId}`}</code>, separated by dots. Every parent
                                level must exist already.
                            </Form.Text>
                        </Form.Group>
                    </Form>
                </Modal.Body>
                <Modal.Footer>
                    <Button variant="outline-secondary" disabled={busy} onClick={() => setShow(false)}>
                        Cancel
                    </Button>
                    <Button variant="success" disabled={busy} onClick={() => void submit()}>
                        {busy ? <BusyLabel label="Creating…" /> : 'Create namespace'}
                    </Button>
                </Modal.Footer>
            </Modal>
        </>
    );
};

/**
 * `POST /v1/{prefix}/namespaces/{namespace}/tables`: the create-table dialog with its schema editor.
 *
 * The table is created in the namespace of the button that opened it, so the address cannot drift
 * from the tree the reader is looking at. The editor covers what a first table needs - a name, its
 * columns (name, type, required) and an optional identity partition column - which are the members
 * of a CreateTableRequest that shape the table; location, sort order and properties keep the
 * catalog's defaults.
 */
export const CreateTableButton: FC<{
    /** The full catalog address of the namespace: `[repository, ref, …levels]`. */
    namespace: string[];
    disabled: boolean;
    callbacks: ManagementCallbacks;
}> = ({ namespace, disabled, callbacks }) => {
    const [show, setShow] = useState(false);
    const [form, setForm] = useState<IcebergCreateTableForm>({
        name: '',
        columns: [emptyColumn()],
        partitionBy: '',
    });
    const [error, setError] = useState<string | null>(null);
    const [busy, setBusy] = useState(false);
    const label = namespaceLabel(namespace);

    const open = () => {
        setForm({ name: '', columns: [emptyColumn()], partitionBy: '' });
        setError(null);
        setShow(true);
    };
    const submit = async () => {
        const problem = validateCreateTableForm(form);
        if (problem !== null) {
            setError(problem);
            return;
        }
        setBusy(true);
        setError(null);
        try {
            await iceberg.createTable(namespace, form);
            setShow(false);
            callbacks.onChanged();
        } catch (submitError) {
            if (isIcebergForbiddenError(submitError)) callbacks.onForbidden();
            setError(managementErrorMessage(submitError, 'create the table'));
        } finally {
            setBusy(false);
        }
    };

    // The partition column is offered only when it can be chosen: a column with a name.
    const partitionColumns = form.columns.filter((column) => column.name.trim() !== '');

    return (
        <>
            <Button size="sm" variant="outline-primary" disabled={disabled} onClick={open}>
                <PlusIcon size={14} className="me-1" />
                <span className="visually-hidden">Create table in {label}</span>
                <span aria-hidden="true">New table</span>
            </Button>
            <Modal show={show} onHide={() => setShow(false)} size="lg">
                <Modal.Header closeButton>
                    <Modal.Title>
                        Create table in <code>{label}</code>
                    </Modal.Title>
                </Modal.Header>
                <Modal.Body>
                    {error !== null && (
                        <Alert variant="danger" className="text-wrap text-break">
                            {error}
                        </Alert>
                    )}
                    <Form
                        onSubmit={(event) => {
                            event.preventDefault();
                            void submit();
                        }}
                    >
                        <Form.Group controlId="iceberg-create-table-name" className="mb-3">
                            <Form.Label>Table name</Form.Label>
                            <Form.Control
                                autoFocus
                                value={form.name}
                                disabled={busy}
                                placeholder="books"
                                onChange={(event) => setForm({ ...form, name: event.target.value })}
                            />
                        </Form.Group>
                        <ColumnEditor
                            columns={form.columns}
                            disabled={busy}
                            onChange={(columns) => setForm({ ...form, columns })}
                        />
                        <Form.Group controlId="iceberg-create-table-partition" className="mt-3">
                            <Form.Label>Partition by</Form.Label>
                            <Form.Select
                                value={form.partitionBy ?? ''}
                                disabled={busy}
                                onChange={(event) => setForm({ ...form, partitionBy: event.target.value })}
                            >
                                <option value="">No partitioning</option>
                                {partitionColumns.map((column) => (
                                    <option key={column.name} value={column.name}>
                                        {column.name} (identity)
                                    </option>
                                ))}
                            </Form.Select>
                            <Form.Text className="text-muted">
                                An identity partition keeps one directory per value of the column.
                            </Form.Text>
                        </Form.Group>
                    </Form>
                </Modal.Body>
                <Modal.Footer>
                    <Button variant="outline-secondary" disabled={busy} onClick={() => setShow(false)}>
                        Cancel
                    </Button>
                    <Button variant="success" disabled={busy} onClick={() => void submit()}>
                        {busy ? <BusyLabel label="Creating…" /> : 'Create table'}
                    </Button>
                </Modal.Footer>
            </Modal>
        </>
    );
};

/**
 * `DELETE /v1/{prefix}/namespaces/{namespace}`: drop an (empty) namespace, behind a confirmation.
 *
 * The catalog is the judge of "empty": a namespace that still holds a table or a child namespace is
 * `409 NamespaceNotEmptyException`, and that answer is shown in the tab rather than pre-empted here.
 */
export const DropNamespaceButton: FC<{
    namespace: string[];
    disabled: boolean;
    callbacks: ManagementCallbacks;
}> = ({ namespace, disabled, callbacks }) => {
    const [busy, setBusy] = useState(false);
    const label = namespaceLabel(namespace);

    const drop = async (hide: () => void) => {
        setBusy(true);
        try {
            await iceberg.dropNamespace(namespace);
            hide();
            callbacks.onChanged();
        } catch (error) {
            hide();
            if (isIcebergForbiddenError(error)) callbacks.onForbidden();
            callbacks.onError(managementErrorMessage(error, `drop the namespace ${label}`));
        } finally {
            setBusy(false);
        }
    };

    return (
        <ConfirmationButton
            variant="link"
            modalVariant="danger"
            size="sm"
            disabled={disabled || busy}
            tooltip="Drop this namespace"
            msg={
                <>
                    Drop the namespace <code>{label}</code>? The catalog drops only an empty namespace: one that still
                    holds a table or a child namespace is refused. The drop is staged on this ref and becomes visible to
                    readers when the branch is committed.
                </>
            }
            onConfirm={(hide) => void drop(hide)}
        >
            <TrashIcon size={14} className="text-danger" />
            <span className="visually-hidden">Drop namespace {label}</span>
        </ConfirmationButton>
    );
};

/**
 * `DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}`: drop a table, behind a confirmation.
 *
 * The confirmation says what a drop does and does not do: the table's pointer and its staged file
 * entries go, the objects themselves stay in the object store and are reclaimed by garbage
 * collection, and the drop is staged on the ref like every other catalog write.
 */
export const DropTableButton: FC<{
    namespace: string[];
    table: string;
    disabled: boolean;
    callbacks: ManagementCallbacks;
}> = ({ namespace, table, disabled, callbacks }) => {
    const [busy, setBusy] = useState(false);

    const drop = async (hide: () => void) => {
        setBusy(true);
        try {
            await iceberg.dropTable(namespace, table);
            hide();
            callbacks.onChanged();
        } catch (error) {
            hide();
            if (isIcebergForbiddenError(error)) callbacks.onForbidden();
            callbacks.onError(managementErrorMessage(error, `drop the table ${table}`));
        } finally {
            setBusy(false);
        }
    };

    return (
        <ConfirmationButton
            variant="link"
            modalVariant="danger"
            size="sm"
            disabled={disabled || busy}
            tooltip="Drop this table"
            msg={
                <>
                    Drop the table <code>{table}</code>? Its metadata pointer and the file entries staged for it are
                    removed; the objects themselves stay in the object store and are reclaimed by garbage collection.
                    The drop is staged on this ref and becomes visible to readers when the branch is committed.
                </>
            }
            onConfirm={(hide) => void drop(hide)}
        >
            <TrashIcon size={14} className="text-danger" />
            <span className="visually-hidden">Drop table {table}</span>
        </ConfirmationButton>
    );
};
