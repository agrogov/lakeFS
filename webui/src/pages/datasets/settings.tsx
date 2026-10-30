import React, { FC, useEffect, useMemo, useState } from 'react';
import Alert from 'react-bootstrap/Alert';
import Badge from 'react-bootstrap/Badge';
import Button from 'react-bootstrap/Button';
import Form from 'react-bootstrap/Form';
import Modal from 'react-bootstrap/Modal';
import Table from 'react-bootstrap/Table';
import { LockIcon, PlusIcon } from '@primer/octicons-react';

import { MAX_LISTING_AMOUNT } from '../../lib/api';
import {
    MetadataKey,
    MetadataKeyCreateRequest,
    MetadataKeyList,
    MetadataKeyType,
    MetadataKeyUpdateRequest,
    datasets,
    isForbiddenError,
} from '../../lib/api/datasets';
import { AlertError, Loading } from '../../lib/components/controls';
import { ConfirmationButton } from '../../lib/components/modals';
import { DatasetPermissionRequired } from '../../lib/components/datasets/gate';
import {
    DATASETS_MANAGE_METADATA_KEYS_ACTION,
    metadataKeyResource,
    usePermission,
} from '../../lib/components/datasets/hooks';
import { useAPI } from '../../lib/hooks/api';

/** The metadata key types of §5.5, in the order the form offers them. */
export const METADATA_KEY_TYPES: readonly MetadataKeyType[] = ['string', 'enum', 'number', 'boolean', 'date'];

/** The name rule of §5.5: `^[a-z][a-z0-9_]{0,62}$`. */
export const METADATA_KEY_NAME_PATTERN = /^[a-z][a-z0-9_]{0,62}$/;

/** The explanation shown where the type would be editable: it is not (§5.5). */
export const METADATA_KEY_TYPE_IMMUTABLE_NOTE =
    'The type cannot be changed after a metadata key is created, because existing values keep their type. ' +
    'Delete the key and recreate it instead.';

/**
 * What the page shows while the permission is not yet known: no keys. It never reaches the
 * screen, because the page renders a loading state until the permission decision arrives.
 */
const EMPTY_METADATA_KEY_LIST: MetadataKeyList = {
    results: [],
    pagination: { has_more: false, next_offset: '', results: 0, max_per_page: 0 },
};

export interface MetadataKeyFormState {
    name: string;
    type: MetadataKeyType;
    required: boolean;
    allowMultiple: boolean;
    allowedValues: string;
    description: string;
}

const emptyForm = (): MetadataKeyFormState => ({
    name: '',
    type: 'string',
    required: false,
    allowMultiple: false,
    allowedValues: '',
    description: '',
});

const formFromKey = (key: MetadataKey): MetadataKeyFormState => ({
    name: key.name,
    type: key.type,
    required: key.required,
    allowMultiple: key.allow_multiple,
    allowedValues: (key.allowed_values ?? []).join(', '),
    description: key.description ?? '',
});

const splitValues = (value: string): string[] =>
    value
        .split(',')
        .map((entry) => entry.trim())
        .filter((entry) => entry !== '');

/** The client-side problems of a form, one per field. The server validates again (§5.5). */
export const metadataKeyFormProblems = (
    form: MetadataKeyFormState,
    mode: 'create' | 'edit',
): Record<string, string> => {
    const problems: Record<string, string> = {};
    if (mode === 'create' && !METADATA_KEY_NAME_PATTERN.test(form.name)) {
        problems.name = 'Use lowercase letters, digits and underscores, starting with a letter (max 63).';
    }
    if (form.type === 'enum' && splitValues(form.allowedValues).length === 0) {
        problems.allowedValues = 'An enum key needs at least one allowed value.';
    }
    return problems;
};

/** The create request of the form: `type` is only ever sent when the key is created. */
export const metadataKeyCreateRequest = (form: MetadataKeyFormState): MetadataKeyCreateRequest => {
    const request: MetadataKeyCreateRequest = {
        name: form.name,
        type: form.type,
        required: form.required,
        allow_multiple: form.allowMultiple,
    };
    if (form.type === 'enum') request.allowed_values = splitValues(form.allowedValues);
    if (form.description !== '') request.description = form.description;
    return request;
};

/** The update request of the form: it never carries `type`, which §5.5 forbids changing. */
export const metadataKeyUpdateRequest = (form: MetadataKeyFormState): MetadataKeyUpdateRequest => {
    const request: MetadataKeyUpdateRequest = {
        required: form.required,
        allow_multiple: form.allowMultiple,
    };
    if (form.type === 'enum') request.allowed_values = splitValues(form.allowedValues);
    if (form.description !== '') request.description = form.description;
    return request;
};

const MetadataKeyFormModal: FC<{
    show: boolean;
    mode: 'create' | 'edit';
    initial?: MetadataKey;
    onHide: () => void;
    onSubmit: (form: MetadataKeyFormState) => Promise<void>;
}> = ({ show, mode, initial, onHide, onSubmit }) => {
    const [form, setForm] = useState<MetadataKeyFormState>(() => (initial ? formFromKey(initial) : emptyForm()));
    const [error, setError] = useState<Error | null>(null);
    const [submitting, setSubmitting] = useState(false);

    // Re-seed the form whenever a different key (or a fresh create) opens the modal.
    useEffect(() => {
        if (!show) return;
        setForm(initial ? formFromKey(initial) : emptyForm());
        setError(null);
    }, [show, initial]);

    const problems = useMemo(() => metadataKeyFormProblems(form, mode), [form, mode]);
    const editing = mode === 'edit';

    const submit = async () => {
        if (Object.keys(problems).length > 0) return;
        setSubmitting(true);
        setError(null);
        try {
            await onSubmit(form);
        } catch (submitError) {
            setError(submitError as Error);
        } finally {
            setSubmitting(false);
        }
    };

    return (
        <Modal show={show} onHide={onHide}>
            <Modal.Header closeButton>
                <Modal.Title>
                    {editing ? `Edit metadata key "${initial?.name ?? ''}"` : 'Create metadata key'}
                </Modal.Title>
            </Modal.Header>
            <Modal.Body>
                <Form
                    onSubmit={(event) => {
                        event.preventDefault();
                        submit();
                    }}
                >
                    <Form.Group className="mb-3" controlId="metadata-key-name">
                        <Form.Label>Name</Form.Label>
                        <Form.Control
                            value={form.name}
                            onChange={(event) => setForm({ ...form, name: event.target.value })}
                            placeholder="classification"
                            disabled={editing}
                            isInvalid={!!problems.name}
                            autoFocus={!editing}
                        />
                        <Form.Text className="text-muted">
                            Lowercase letters, digits and underscores, starting with a letter.
                        </Form.Text>
                        {problems.name && <div className="text-danger small">{problems.name}</div>}
                    </Form.Group>

                    <Form.Group className="mb-3" controlId="metadata-key-type">
                        <Form.Label>Type</Form.Label>
                        <Form.Select
                            value={form.type}
                            onChange={(event) => setForm({ ...form, type: event.target.value as MetadataKeyType })}
                            disabled={editing}
                            aria-describedby="metadata-key-type-note"
                        >
                            {METADATA_KEY_TYPES.map((type) => (
                                <option key={type} value={type}>
                                    {type}
                                </option>
                            ))}
                        </Form.Select>
                        {editing && (
                            <Form.Text
                                id="metadata-key-type-note"
                                className="text-muted dataset-metadata-key-type-note"
                            >
                                <LockIcon className="me-1" />
                                {METADATA_KEY_TYPE_IMMUTABLE_NOTE}
                            </Form.Text>
                        )}
                    </Form.Group>

                    {form.type === 'enum' && (
                        <Form.Group className="mb-3" controlId="metadata-key-allowed-values">
                            <Form.Label>Allowed values</Form.Label>
                            <Form.Control
                                value={form.allowedValues}
                                onChange={(event) => setForm({ ...form, allowedValues: event.target.value })}
                                placeholder="public, internal, confidential"
                                isInvalid={!!problems.allowedValues}
                            />
                            <Form.Text className="text-muted">Comma-separated values.</Form.Text>
                            {problems.allowedValues && (
                                <div className="text-danger small">{problems.allowedValues}</div>
                            )}
                        </Form.Group>
                    )}

                    <Form.Group className="mb-3" controlId="metadata-key-required">
                        <Form.Check
                            type="checkbox"
                            label="Required"
                            checked={form.required}
                            onChange={(event) => setForm({ ...form, required: event.target.checked })}
                        />
                        <Form.Text className="text-muted">
                            Required keys are enforced on the next definition write, not on existing versions.
                        </Form.Text>
                    </Form.Group>

                    <Form.Group className="mb-3" controlId="metadata-key-multiple">
                        <Form.Check
                            type="checkbox"
                            label="Allow multiple values"
                            checked={form.allowMultiple}
                            onChange={(event) => setForm({ ...form, allowMultiple: event.target.checked })}
                        />
                    </Form.Group>

                    <Form.Group controlId="metadata-key-description">
                        <Form.Label>Description</Form.Label>
                        <Form.Control
                            as="textarea"
                            rows={2}
                            value={form.description}
                            onChange={(event) => setForm({ ...form, description: event.target.value })}
                        />
                    </Form.Group>
                </Form>
                {error && <AlertError error={error} onDismiss={() => setError(null)} />}
            </Modal.Body>
            <Modal.Footer>
                <Button variant="secondary" onClick={onHide}>
                    Cancel
                </Button>
                <Button
                    variant="success"
                    onClick={submit}
                    disabled={submitting || Object.keys(problems).length > 0}
                    className="dataset-metadata-key-save"
                >
                    {editing ? 'Save' : 'Create'}
                </Button>
            </Modal.Footer>
        </Modal>
    );
};

/**
 * Metadata-key management (DATASETS_DESIGN.md §11.2, §5.5).
 *
 * Metadata keys are global: they type the metadata of every dataset. The table offers create,
 * edit and delete. The type is not editable - it is rendered read-only with a short explanation,
 * because §5.5 forbids changing it: existing values keep their type, so a changed type would
 * silently reinterpret them. The intended path is delete and recreate.
 *
 * The page is rendered only for a caller holding `datasets:ManageMetadataKeys`; a `403` from the
 * list is handled the same way as the policy check saying no.
 */
const DatasetsSettingsPage: FC = () => {
    // ``datasets:ManageMetadataKeys`` is granted on `metadata-key/*` (§9.1), so any representative
    // key name resolves the wildcard for the question "may this user manage the keys".
    const permission = usePermission(DATASETS_MANAGE_METADATA_KEYS_ACTION, metadataKeyResource('classification'));
    const [creating, setCreating] = useState(false);
    const [editing, setEditing] = useState<MetadataKey | null>(null);
    const [actionError, setActionError] = useState<Error | null>(null);

    const { response, loading, error, refetch } = useAPI(
        () =>
            permission.allowed
                ? datasets.listMetadataKeys({ amount: MAX_LISTING_AMOUNT })
                : Promise.resolve(EMPTY_METADATA_KEY_LIST),
        [permission.allowed],
    );

    if (permission.loading) return <Loading />;
    if (!permission.allowed || isForbiddenError(error)) {
        return (
            <DatasetPermissionRequired action={DATASETS_MANAGE_METADATA_KEYS_ACTION} what="Managing metadata keys" />
        );
    }
    if (loading) return <Loading />;
    if (error) return <AlertError error={error as Error} />;

    const keys = ((response as MetadataKeyList | null)?.results ?? []) as MetadataKey[];

    const create = async (form: MetadataKeyFormState) => {
        await datasets.createMetadataKey(metadataKeyCreateRequest(form));
        setCreating(false);
        refetch();
    };

    const update = async (form: MetadataKeyFormState) => {
        if (!editing) return;
        await datasets.updateMetadataKey(editing.name, metadataKeyUpdateRequest(form));
        setEditing(null);
        refetch();
    };

    const remove = async (key: MetadataKey) => {
        setActionError(null);
        try {
            await datasets.deleteMetadataKey(key.name);
            refetch();
        } catch (deleteError) {
            setActionError(deleteError as Error);
        }
    };

    return (
        <div className="dataset-settings-page">
            <div className="d-flex flex-wrap align-items-center mb-2">
                <h5 className="mb-0">Metadata keys</h5>
                <Button variant="success" size="sm" className="ms-auto" onClick={() => setCreating(true)}>
                    <PlusIcon className="me-1" /> Create metadata key
                </Button>
            </div>
            <p className="text-muted small">
                Metadata keys type the metadata of every dataset. Deleting a key leaves the existing values as free-form
                metadata.
            </p>

            {actionError && <AlertError error={actionError} onDismiss={() => setActionError(null)} />}

            {keys.length === 0 ? (
                <Alert variant="secondary" className="dataset-metadata-keys-empty">
                    No metadata keys are defined yet. Metadata is free-form until a key is created.
                </Alert>
            ) : (
                <Table hover responsive className="dataset-metadata-keys-table">
                    <thead>
                        <tr>
                            <th>Name</th>
                            <th>Type</th>
                            <th>Required</th>
                            <th>Allowed values</th>
                            <th>Created by</th>
                            <th />
                        </tr>
                    </thead>
                    <tbody>
                        {keys.map((key) => (
                            <tr key={key.name} data-testid={`metadata-key-row-${key.name}`}>
                                <td>
                                    <code>{key.name}</code>
                                    {key.allow_multiple && (
                                        <Badge bg="light" text="dark" className="border ms-2">
                                            multiple
                                        </Badge>
                                    )}
                                    {key.description && (
                                        <div className="text-muted small dataset-metadata-key-description">
                                            {key.description}
                                        </div>
                                    )}
                                </td>
                                <td>
                                    <Badge bg="secondary" className="dataset-metadata-key-type">
                                        {key.type}
                                    </Badge>
                                </td>
                                <td>{key.required ? 'Yes' : 'No'}</td>
                                <td className="dataset-metadata-key-values">
                                    {key.type === 'enum' && (key.allowed_values ?? []).length > 0 ? (
                                        <span className="d-flex flex-wrap gap-1">
                                            {(key.allowed_values ?? []).map((value) => (
                                                <Badge key={value} bg="light" text="dark" className="border">
                                                    {value}
                                                </Badge>
                                            ))}
                                        </span>
                                    ) : (
                                        <span className="text-muted">—</span>
                                    )}
                                </td>
                                <td className="text-muted small">{key.created_by}</td>
                                <td>
                                    <span className="d-flex gap-1">
                                        <Button
                                            variant="outline-secondary"
                                            size="sm"
                                            className="dataset-metadata-key-edit"
                                            onClick={() => setEditing(key)}
                                        >
                                            Edit
                                        </Button>
                                        <ConfirmationButton
                                            variant="outline-danger"
                                            size="sm"
                                            msg={
                                                <span>
                                                    Delete metadata key <strong>{key.name}</strong>? Existing values
                                                    stay as free-form metadata.
                                                </span>
                                            }
                                            onConfirm={() => remove(key)}
                                        >
                                            Delete
                                        </ConfirmationButton>
                                    </span>
                                </td>
                            </tr>
                        ))}
                    </tbody>
                </Table>
            )}

            <MetadataKeyFormModal show={creating} mode="create" onHide={() => setCreating(false)} onSubmit={create} />
            <MetadataKeyFormModal
                show={editing !== null}
                mode="edit"
                initial={editing ?? undefined}
                onHide={() => setEditing(null)}
                onSubmit={update}
            />
        </div>
    );
};

export default DatasetsSettingsPage;
