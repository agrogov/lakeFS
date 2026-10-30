import React, { FC, useEffect, useMemo, useState } from 'react';
import Alert from 'react-bootstrap/Alert';
import Breadcrumb from 'react-bootstrap/Breadcrumb';
import Button from 'react-bootstrap/Button';
import Card from 'react-bootstrap/Card';
import Col from 'react-bootstrap/Col';
import Container from 'react-bootstrap/Container';
import Form from 'react-bootstrap/Form';
import Row from 'react-bootstrap/Row';
import Spinner from 'react-bootstrap/Spinner';
import { useNavigate, useParams } from 'react-router-dom';
import { GitPullRequestIcon, UploadIcon } from '@primer/octicons-react';

import { MAX_LISTING_AMOUNT, repositories } from '../../../lib/api';
import {
    Dataset,
    DatasetDefinition,
    DatasetsError,
    MetadataKey,
    MetadataValue,
    datasets,
} from '../../../lib/api/datasets';
import { AlertError, Loading } from '../../../lib/components/controls';
import { DatasetsGate } from '../../../lib/components/datasets/gate';
import { useDatasetNames } from '../../../lib/components/datasets/hooks';
import { partitionDatasetRepos } from '../../../lib/components/datasets/metadata';
import { Link } from '../../../lib/components/nav';
import { useAPI } from '../../../lib/hooks/api';
import { ItemsTable } from './itemsTable';
import { MetadataForm } from './metadataForm';
import {
    EditorItem,
    MetadataDraft,
    buildDefinition,
    buildMetadata,
    hasProblems,
    itemsFromDefinition,
    localItemProblems,
    mapValidationErrors,
    metadataDraftFromValues,
    pinnedCommits,
    validateMetadataDraft,
} from './editorModel';
import { useDatasetValidation } from './useDatasetValidation';

/**
 * Create / edit a dataset (DATASETS_DESIGN.md §11.2).
 *
 * `mode` is the only difference between the two routes: creating also picks the name and
 * publishes `v1`; editing starts from the latest definition and publishes as `latest + 1`.
 * Both share the same form: description, the metadata form generated from the metadata keys,
 * the items table with its repository/ref/path pickers, live validation through
 * `POST /datasets/{dataset}/validate` and the publish / open-proposal footer.
 */

export type DatasetEditorMode = 'create' | 'edit';

/** §5.2 rule 1, the repository name pattern a dataset name must match. */
export const DATASET_NAME_PATTERN = /^[a-z0-9][a-z0-9-]{2,62}$/;

let newItemCounter = 0;

/** A blank items-table row. */
export const newEditorItem = (overrides: Partial<EditorItem> = {}): EditorItem => ({
    key: `row-${++newItemCounter}`,
    repository: '',
    refType: 'branch',
    refId: '',
    type: 'prefix',
    path: '',
    target: '',
    ...overrides,
});

const EditorBreadcrumb: FC<{ mode: DatasetEditorMode; datasetId: string }> = ({ mode, datasetId }) => (
    <Breadcrumb>
        <Link href={{ pathname: '/datasets' }} component={Breadcrumb.Item}>
            Datasets
        </Link>
        {mode === 'edit' && (
            <Link
                href={{ pathname: '/datasets/:datasetId/overview', params: { datasetId } }}
                component={Breadcrumb.Item}
            >
                {datasetId}
            </Link>
        )}
        <Breadcrumb.Item active>{mode === 'edit' ? 'Edit' : 'Create dataset'}</Breadcrumb.Item>
    </Breadcrumb>
);

const fetchMetadataKeys = async (): Promise<MetadataKey[]> => {
    const response = (await datasets.listMetadataKeys({ amount: MAX_LISTING_AMOUNT })) as { results?: MetadataKey[] };
    return response.results ?? [];
};

const fetchRepositoryIds = async (): Promise<string[]> => {
    const response = (await repositories.list('', '', MAX_LISTING_AMOUNT)) as { results?: { id: string }[] };
    return (response.results ?? []).map((repo) => repo.id);
};

/** Splits stored metadata into known keys (form) and free-form keys (kept verbatim). */
const splitMetadata = (keys: MetadataKey[], metadata: Record<string, MetadataValue> | undefined) => {
    const known = new Set(keys.map((key) => key.name));
    const extra: Record<string, MetadataValue> = {};
    for (const [name, value] of Object.entries(metadata ?? {})) {
        if (!known.has(name)) extra[name] = value;
    }
    return extra;
};

const parseRawDefinition = (text: string): { definition?: DatasetDefinition; error?: string } => {
    let parsed: unknown;
    try {
        parsed = JSON.parse(text);
    } catch (error) {
        return { error: `Invalid JSON: ${(error as Error).message}` };
    }
    const definition = parsed as DatasetDefinition;
    if (!definition || typeof definition !== 'object' || definition.format_version !== 1) {
        return { error: 'A definition needs "format_version": 1' };
    }
    if (!Array.isArray(definition.data)) {
        return { error: 'A definition needs a "data" array' };
    }
    return { definition };
};

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

interface EditorFormProps {
    mode: DatasetEditorMode;
    datasetId: string;
    dataset: Dataset | null;
    metadataKeys: MetadataKey[];
    repositoryIds: string[];
}

const EditorForm: FC<EditorFormProps> = ({ mode, datasetId, dataset, metadataKeys, repositoryIds }) => {
    const navigate = useNavigate();

    const [name, setName] = useState('');
    const [description, setDescription] = useState('');
    const [draft, setDraft] = useState<MetadataDraft>({});
    const [extraMetadata, setExtraMetadata] = useState<Record<string, MetadataValue>>({});
    const [items, setItems] = useState<EditorItem[]>([]);
    const [message, setMessage] = useState('');
    const [rawMode, setRawMode] = useState(false);
    const [rawText, setRawText] = useState('');
    const [submitting, setSubmitting] = useState(false);
    const [submitError, setSubmitError] = useState<Error | null>(null);
    const [initialised, setInitialised] = useState(false);

    // Seed the form once both the metadata keys and (in edit mode) the dataset are loaded.
    useEffect(() => {
        if (initialised) return;
        const definition = dataset?.definition;
        setDescription(definition?.description ?? dataset?.description ?? '');
        setDraft(metadataDraftFromValues(metadataKeys, definition?.metadata));
        setExtraMetadata(splitMetadata(metadataKeys, definition?.metadata));
        setItems(definition ? itemsFromDefinition(definition) : mode === 'create' ? [newEditorItem()] : []);
        setInitialised(true);
    }, [initialised, dataset, metadataKeys, mode]);

    const structuredDefinition = useMemo(
        () => buildDefinition(items, description, { ...extraMetadata, ...buildMetadata(metadataKeys, draft) }),
        [items, description, extraMetadata, metadataKeys, draft],
    );

    const rawParsed = useMemo(() => (rawMode ? parseRawDefinition(rawText) : {}), [rawMode, rawText]);
    const activeDefinition = rawMode ? rawParsed.definition : structuredDefinition;

    const metadataProblems = useMemo(() => validateMetadataDraft(metadataKeys, draft), [metadataKeys, draft]);
    const itemProblems = useMemo(() => localItemProblems(items), [items]);
    const structuredValid = items.length > 0 && !hasProblems(itemProblems) && !hasProblems(metadataProblems);

    const effectiveName = mode === 'edit' ? datasetId : name.trim();
    const nameProblem =
        mode !== 'create'
            ? null
            : name.trim() === ''
              ? 'Name is required'
              : DATASET_NAME_PATTERN.test(name.trim())
                ? null
                : 'Must match ^[a-z0-9][a-z0-9-]{2,62}$ (lower-case letters, digits and hyphens)';

    const canAskServer = DATASET_NAME_PATTERN.test(effectiveName);
    const localValid = rawMode ? rawParsed.definition !== undefined : structuredValid;
    const validation = useDatasetValidation(
        canAskServer ? effectiveName : '',
        activeDefinition ?? structuredDefinition,
        localValid,
    );
    const mappedErrors = useMemo(() => mapValidationErrors(validation.errors, items), [validation.errors, items]);

    // Carry the commit refs the server resolved across the rows ("pinned to <commit>").
    const definitionSignature = JSON.stringify(activeDefinition ?? structuredDefinition);
    const currentResolution = validation.resolvedFor === definitionSignature ? validation.resolved : undefined;
    useEffect(() => {
        if (!currentResolution || rawMode) return;
        const pinned = pinnedCommits(currentResolution, items);
        setItems((current) => {
            let changed = false;
            const next = current.map((item) => {
                const commit = pinned[item.key];
                if (!commit || commit === item.pinnedCommit) return item;
                changed = true;
                return { ...item, pinnedCommit: commit };
            });
            return changed ? next : current;
        });
    }, [currentResolution, rawMode, items]);

    const latestVersion = dataset?.latest_version ?? 0;
    const publishLabel = `Publish as v${latestVersion + 1}`;

    const serverBlocked = validation.errors.length > 0;
    const canSubmit =
        !submitting &&
        !validation.validating &&
        localValid &&
        nameProblem === null &&
        effectiveName !== '' &&
        message.trim() !== '' &&
        (!serverBlocked || validation.unavailable);

    const definitionForSubmit = activeDefinition;

    const applyRawText = (checked: boolean) => {
        if (!checked) {
            // Leaving raw mode: only accept a definition that parses, otherwise stay in the
            // textarea so nothing is lost.
            if (!rawParsed.definition) return;
            setDescription(rawParsed.definition.description ?? '');
            setDraft(metadataDraftFromValues(metadataKeys, rawParsed.definition.metadata));
            setExtraMetadata(splitMetadata(metadataKeys, rawParsed.definition.metadata));
            setItems(rawParsed.definition.data.length > 0 ? itemsFromDefinition(rawParsed.definition) : []);
            setRawMode(false);
            return;
        }
        setRawText(JSON.stringify(structuredDefinition, null, 4));
        setRawMode(true);
    };

    const submit = async (action: 'publish' | 'proposal') => {
        if (!definitionForSubmit || !canSubmit) return;
        setSubmitting(true);
        setSubmitError(null);
        try {
            if (action === 'proposal') {
                await datasets.createProposal(datasetId, {
                    definition: definitionForSubmit,
                    title: message.trim(),
                    description: description.trim() === '' ? undefined : description.trim(),
                });
                navigate(`/datasets/${datasetId}/proposals`);
                return;
            }
            if (mode === 'create') {
                await datasets.create({
                    name: effectiveName,
                    definition: definitionForSubmit,
                    publish: { message: message.trim() },
                });
                navigate(`/datasets/${effectiveName}/overview`);
                return;
            }
            await datasets.publishVersion(datasetId, {
                definition: definitionForSubmit,
                message: message.trim(),
                expected_version: latestVersion,
            });
            navigate(`/datasets/${datasetId}/overview`);
        } catch (error) {
            setSubmitError(error instanceof DatasetsError ? new Error(error.message) : (error as Error));
        } finally {
            setSubmitting(false);
        }
    };

    const updateItem = (item: EditorItem) =>
        setItems((current) => current.map((existing) => (existing.key === item.key ? item : existing)));
    const removeItem = (key: string) => setItems((current) => current.filter((item) => item.key !== key));
    const addItem = () => setItems((current) => [...current, newEditorItem()]);

    const status = validation.validating ? (
        <span className="text-muted">
            <Spinner animation="border" size="sm" className="me-2" />
            Validating…
        </span>
    ) : validation.unavailable ? (
        <span className="text-muted">Validation is not available on this server</span>
    ) : serverBlocked ? (
        <span className="text-danger">
            {validation.message ?? `${validation.errors.length} problem${validation.errors.length === 1 ? '' : 's'}`}
        </span>
    ) : validation.resolved ? (
        <span className="text-success">Definition is valid</span>
    ) : null;

    return (
        <Form
            onSubmit={(e) => {
                e.preventDefault();
                void submit('publish');
            }}
        >
            <Card className="mb-4 dataset-editor-details">
                <Card.Body>
                    <Row>
                        {mode === 'create' && (
                            <Col md={6}>
                                <Form.Group className="mb-3" controlId="dataset-name">
                                    <Form.Label>Name</Form.Label>
                                    <Form.Control
                                        value={name}
                                        autoFocus
                                        placeholder="customer-360"
                                        isInvalid={nameProblem !== null && name !== ''}
                                        onChange={(e) => setName(e.target.value)}
                                    />
                                    <Form.Text className="text-muted">
                                        The dataset name is also the name of its read-only repository.
                                    </Form.Text>
                                    {nameProblem && name !== '' && (
                                        <Form.Control.Feedback type="invalid">{nameProblem}</Form.Control.Feedback>
                                    )}
                                </Form.Group>
                            </Col>
                        )}
                        <Col md={mode === 'create' ? 6 : 12}>
                            <Form.Group className="mb-3" controlId="dataset-description">
                                <Form.Label>Description</Form.Label>
                                <Form.Control
                                    as="textarea"
                                    rows={2}
                                    value={description}
                                    placeholder="What is this dataset for?"
                                    onChange={(e) => setDescription(e.target.value)}
                                />
                            </Form.Group>
                        </Col>
                    </Row>
                </Card.Body>
            </Card>

            <Card className="mb-4 dataset-editor-definition">
                <Card.Header className="d-flex align-items-center">
                    <span className="fw-semibold">Definition</span>
                    <Form.Check
                        type="switch"
                        id="dataset-raw-json"
                        className="ms-auto"
                        label="Raw JSON"
                        checked={rawMode}
                        onChange={(e) => applyRawText(e.target.checked)}
                    />
                </Card.Header>
                <Card.Body>
                    {rawMode ? (
                        <>
                            <Form.Control
                                as="textarea"
                                rows={20}
                                className="policy-document font-monospace"
                                aria-label="Raw definition JSON"
                                value={rawText}
                                onChange={(e) => setRawText(e.target.value)}
                            />
                            {rawParsed.error && (
                                <Alert variant="danger" className="mt-2 mb-0">
                                    {rawParsed.error}
                                </Alert>
                            )}
                        </>
                    ) : (
                        <>
                            <h6 className="mb-3">Metadata</h6>
                            <MetadataForm
                                keys={metadataKeys}
                                draft={draft}
                                problems={metadataProblems}
                                disabled={submitting}
                                onChange={(key, value) => setDraft((current) => ({ ...current, [key]: value }))}
                            />
                            <hr />
                            <div className="d-flex align-items-center mb-3">
                                <h6 className="mb-0">Items</h6>
                                <span className="text-muted small ms-2">
                                    A source repository item, pinned to a commit and materialised at a target.
                                </span>
                            </div>
                            <ItemsTable
                                items={items}
                                repositories={repositoryIds}
                                serverErrors={mappedErrors.items}
                                onChange={updateItem}
                                onRemove={removeItem}
                                onAdd={addItem}
                                disabled={submitting}
                            />
                            {mappedErrors.general.length > 0 && (
                                <Alert variant="danger" className="mt-3 mb-0" role="alert">
                                    {mappedErrors.general.map((problem) => (
                                        <div key={problem}>{problem}</div>
                                    ))}
                                </Alert>
                            )}
                        </>
                    )}
                </Card.Body>
            </Card>

            <Card className="mb-4 dataset-editor-footer">
                <Card.Body>
                    <Form.Group controlId="dataset-message">
                        <Form.Label>Message</Form.Label>
                        <Form.Control
                            as="textarea"
                            rows={2}
                            value={message}
                            placeholder="Describe the change this version makes"
                            onChange={(e) => setMessage(e.target.value)}
                        />
                    </Form.Group>
                    {validation.stats && (
                        <div className="text-muted small mt-2">
                            Estimate: {validation.stats.objects.toLocaleString()} objects,{' '}
                            {validation.stats.bytes.toLocaleString()} bytes
                        </div>
                    )}
                    {submitError && <AlertError error={submitError} onDismiss={() => setSubmitError(null)} />}
                </Card.Body>
                <Card.Footer className="d-flex align-items-center gap-2">
                    <Button type="submit" variant="success" disabled={!canSubmit}>
                        {submitting ? (
                            <Spinner animation="border" size="sm" className="me-2" />
                        ) : (
                            <UploadIcon className="me-2" />
                        )}
                        {publishLabel}
                    </Button>
                    {mode === 'edit' && (
                        <Button variant="outline-primary" disabled={!canSubmit} onClick={() => void submit('proposal')}>
                            <GitPullRequestIcon className="me-2" />
                            Open proposal
                        </Button>
                    )}
                    <span className="ms-auto small">{status}</span>
                </Card.Footer>
            </Card>
        </Form>
    );
};

const DatasetEditor: FC<{ mode: DatasetEditorMode; datasetId: string }> = ({ mode, datasetId }) => {
    const {
        response: datasetResponse,
        loading: datasetLoading,
        error: datasetError,
    } = useAPI(() => (mode === 'edit' ? datasets.get(datasetId) : Promise.resolve(null)), [mode, datasetId]);
    const { response: metadataKeys, loading: keysLoading, error: keysError } = useAPI(fetchMetadataKeys, []);
    const { response: repositoryIds } = useAPI(fetchRepositoryIds, []);
    const { names: datasetNames } = useDatasetNames();

    const repositories = useMemo(
        () =>
            partitionDatasetRepos(
                (repositoryIds ?? []).map((id) => ({ id })),
                datasetNames,
            ).repositories.map((r) => r.id),
        [repositoryIds, datasetNames],
    );

    if (datasetLoading || keysLoading) return <Loading />;
    if (datasetError) return <AlertError error={datasetError as Error} />;
    if (keysError) return <AlertError error={keysError as Error} />;

    return (
        <EditorForm
            mode={mode}
            datasetId={datasetId}
            dataset={(datasetResponse as Dataset | null) ?? null}
            metadataKeys={(metadataKeys as MetadataKey[] | undefined) ?? []}
            repositoryIds={repositories}
        />
    );
};

const DatasetEditorPage: FC<{ mode: DatasetEditorMode }> = ({ mode }) => {
    const { datasetId = '' } = useParams();
    return (
        <DatasetsGate>
            <Container fluid="xl">
                <EditorBreadcrumb mode={mode} datasetId={datasetId} />
                <div className="mt-4">
                    <h4 className="mb-4">{mode === 'edit' ? `Edit ${datasetId}` : 'Create dataset'}</h4>
                    <DatasetEditor mode={mode} datasetId={datasetId} />
                </div>
            </Container>
        </DatasetsGate>
    );
};

export default DatasetEditorPage;
