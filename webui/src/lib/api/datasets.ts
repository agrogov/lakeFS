import {
    API_ENDPOINT,
    AuthenticationError,
    DEFAULT_LISTING_AMOUNT,
    MAX_LISTING_AMOUNT,
    defaultAPIHeaders,
} from './index';

/**
 * Typed client for the datasets control plane (`api/datasets.yml`, DATASETS_DESIGN.md §10).
 *
 * The transport mirrors `lib/api/index.js`: `fetch` with the lakeFS default headers, a
 * status switch per call and errors surfaced as exceptions. The only difference is the error
 * shape the control plane guarantees (§10.4): every failure carries the core lakeFS
 * `{"message": "..."}` body and validation failures add one entry per problem in `errors`.
 * `DatasetsError` keeps both.
 *
 * Reading dataset *data* does not go through this client: a dataset is a read-only
 * repository, so the regular repository/object endpoints and the S3 gateway serve it.
 */

const DATASETS_PATH = '/datasets';

/** Base URL of the control plane. The collection root is served at the bare base path. */
export const DATASETS_ENDPOINT = `${API_ENDPOINT}${DATASETS_PATH}`;

/** The disabled message the UI shows when the server has the feature switched off (§11). */
export const DATASETS_DISABLED_MESSAGE = 'Datasets are disabled on this server';

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

/** One validation problem, e.g. `{"path": "data[2].target", "message": "..."}` (§10.4). */
export interface ErrorDetail {
    path?: string;
    message: string;
}

/**
 * A failed control-plane request. `message` is the server's human-readable message and
 * `errors` lists every validation problem (empty for non-validation failures).
 */
export class DatasetsError extends Error {
    readonly status: number;
    readonly errors: ErrorDetail[];

    constructor(message: string, status: number, errors: ErrorDetail[] = []) {
        super(message);
        this.name = 'DatasetsError';
        this.status = status;
        this.errors = errors;
    }
}

/**
 * True when the control plane is switched off on this server, so clients should say so
 * instead of reporting a failure. A disabled control plane answers `501` (§10.4); a server
 * without the routes at all answers `404`.
 */
export const isDatasetsDisabledError = (error: unknown): boolean =>
    error instanceof DatasetsError && (error.status === 404 || error.status === 501);

const parseErrorBody = async (response: Response): Promise<{ message: string; errors: ErrorDetail[] }> => {
    try {
        const body = await response.json();
        const errors: ErrorDetail[] = Array.isArray(body?.errors)
            ? body.errors.filter((detail: ErrorDetail) => detail && typeof detail.message === 'string')
            : [];
        const message = typeof body?.message === 'string' && body.message !== '' ? body.message : response.statusText;
        return { message: message || 'request failed', errors };
    } catch {
        // Not a JSON body (a proxy error page, for example): fall back to the status line.
        return { message: response.statusText || 'request failed', errors: [] };
    }
};

const datasetsRequest = async <T>(path: string, requestData: RequestInit = {}): Promise<T> => {
    const headers = new Headers({
        ...defaultAPIHeaders,
        ...((requestData.headers as Record<string, string> | undefined) ?? {}),
    });
    const response = await fetch(`${DATASETS_ENDPOINT}${path}`, { headers, ...requestData });

    if (response.status === 401) {
        // Keep the shared auth flow working: useAPI() redirects to the login page on this class.
        const { message } = await parseErrorBody(response);
        throw new AuthenticationError(message, response.status);
    }
    if (!response.ok) {
        const { message, errors } = await parseErrorBody(response);
        throw new DatasetsError(message, response.status, errors);
    }
    if (response.status === 204) return undefined as T;
    return (await response.json()) as T;
};

// ---------------------------------------------------------------------------
// Query building
// ---------------------------------------------------------------------------

/**
 * Builds a query string, dropping unset values. Unlike `qs` in `lib/api/index.js` it accepts
 * repeated and dynamic keys (the `metadata.<key>=<value>` filters of `GET /datasets`, §10.1)
 * and values that are `0` or `false`.
 *
 * `verbatim` entries are appended even when empty: a metadata filter with an empty value
 * means "this key is empty", which is a different question from "no filter".
 */
const buildQuery = (
    params: Record<string, string | number | boolean | undefined | null>,
    verbatim: Record<string, string> = {},
): string => {
    const search = new URLSearchParams();
    for (const [key, value] of Object.entries(params)) {
        if (value === undefined || value === null || value === '') continue;
        search.append(key, String(value));
    }
    for (const [key, value] of Object.entries(verbatim)) {
        search.append(key, value);
    }
    const query = search.toString();
    return query === '' ? '' : `?${query}`;
};

const buildMetadataQuery = (metadata?: Record<string, string>): Record<string, string> => {
    const params: Record<string, string> = {};
    for (const [key, value] of Object.entries(metadata ?? {})) {
        if (value === undefined || value === null) continue;
        params[`metadata.${key}`] = value;
    }
    return params;
};

const segment = (value: string): string => encodeURIComponent(value);

// ---------------------------------------------------------------------------
// Schema types (api/datasets.yml §10.2)
// ---------------------------------------------------------------------------

export interface Pagination {
    has_more: boolean;
    next_offset: string;
    results: number;
    max_per_page: number;
}

/** `commit` for stored definitions; `branch`/`tag` are accepted on input only (§5.1). */
export type SourceRefType = 'commit' | 'branch' | 'tag';

export interface SourceRef {
    type: SourceRefType;
    id: string;
}

export type DataItemType = 'object' | 'prefix' | 'table' | 'namespace';

export interface PathSource {
    path: string;
}

export interface DataItem {
    target: string;
    repository: string;
    ref: SourceRef;
    type: DataItemType;
    object?: PathSource;
    prefix?: PathSource;
    /** Reserved for Iceberg `table` items; answered with 501 in this build (§1.2). */
    table?: Record<string, unknown>;
    /** Reserved for Iceberg `namespace` items; answered with 501 in this build (§1.2). */
    namespace?: Record<string, unknown>;
}

/**
 * A metadata value is a JSON scalar, or an array of scalars for an `allow_multiple` key (§5.1,
 * §5.5). The array element type follows the key type: strings/dates/enums are strings, numbers
 * are numbers and booleans are booleans (see `pkg/datasets/metadata_validate.go`).
 */
export type MetadataScalarValue = string | number | boolean;
export type MetadataValue = MetadataScalarValue | MetadataScalarValue[];

export interface DatasetDefinition {
    format_version: 1;
    description?: string;
    metadata?: Record<string, MetadataValue>;
    data: DataItem[];
}

export type DatasetState = 'creating' | 'active' | 'publishing' | 'deleting' | 'degraded';

export interface ItemStats {
    target: string;
    objects: number;
    bytes: number;
}

export interface DatasetStats {
    objects: number;
    bytes: number;
    items: ItemStats[];
}

export interface Dataset {
    name: string;
    description?: string;
    /** Unix epoch milliseconds (§10.2). */
    created_at: number;
    created_by: string;
    /** 0 when no version was published yet. */
    latest_version: number;
    state: DatasetState;
    /** Definition of the latest version, when there is one. */
    definition?: DatasetDefinition;
    stats?: DatasetStats;
    /**
     * Unix epoch milliseconds of the last change. The record tracks it (§5.3) and the list
     * view shows it; the spec only requires `created_at`, so callers must fall back to it.
     */
    updated_at?: number;
}

export interface DatasetList {
    results: Dataset[];
    pagination: Pagination;
}

export interface Version {
    version: number;
    message?: string;
    publisher: string;
    /** Unix epoch milliseconds. */
    published_at: number;
    commit_id: string;
    proposal_id?: string;
    definition?: DatasetDefinition;
    stats?: DatasetStats;
}

export interface VersionList {
    results: Version[];
    pagination: Pagination;
}

/** `added` | `removed` | `changed` for definitions and data diffs (§10.3). */
export type ChangeType = 'added' | 'removed' | 'changed';

export interface StringChange {
    from: string;
    to: string;
}

export interface MetadataChange {
    key: string;
    change: ChangeType;
    from?: MetadataValue;
    to?: MetadataValue;
}

export interface ItemChange {
    target: string;
    change: ChangeType;
    from?: DataItem;
    to?: DataItem;
}

export interface DefinitionDiff {
    description?: StringChange;
    metadata?: MetadataChange[];
    items: ItemChange[];
}

export interface DataDiffEntry {
    path: string;
    type: ChangeType;
    size_bytes?: number;
}

export interface DataDiffList {
    results: DataDiffEntry[];
    pagination: Pagination;
}

export type ProposalStatus = 'open' | 'closed' | 'merged';

export interface Proposal {
    id: string;
    dataset: string;
    /** Version the proposal was made against; merging requires it to be `latest` (§5.6). */
    base_version: number;
    definition?: DatasetDefinition;
    title: string;
    description?: string;
    author: string;
    status: ProposalStatus;
    /** Unix epoch milliseconds. */
    created_at: number;
    /** Unix epoch milliseconds; set when closed or merged. */
    closed_at?: number;
    merged_version?: number;
    /** Hidden branch `_proposal-<id>` holding the materialised definition. */
    preview_branch: string;
    diff?: DefinitionDiff;
    data_diff?: DataDiffList;
}

export interface ProposalList {
    results: Proposal[];
    pagination: Pagination;
}

export type MetadataKeyType = 'string' | 'enum' | 'number' | 'boolean' | 'date';

export interface MetadataKey {
    name: string;
    type: MetadataKeyType;
    required: boolean;
    allowed_values?: string[];
    allow_multiple: boolean;
    description?: string;
    /** Unix epoch milliseconds. */
    created_at: number;
    created_by: string;
}

export interface MetadataKeyList {
    results: MetadataKey[];
    pagination: Pagination;
}

export type TaskStatus = 'pending' | 'running' | 'completed' | 'failed';

/** Status of an asynchronous publish (§6.5, §10.1). */
export interface Task {
    id: string;
    status: TaskStatus;
    version?: number;
    error?: string;
    /** Unix epoch milliseconds. */
    created_at: number;
    /** Unix epoch milliseconds. */
    updated_at: number;
}

export type PrincipalType = 'user' | 'group';

export interface AccessGrant {
    principal_type: PrincipalType;
    principal_id: string;
    granted_by?: string;
    /** Unix epoch milliseconds. */
    granted_at?: number;
}

export interface AccessGrantList {
    results: AccessGrant[];
    pagination: Pagination;
}

/** `GET /datasets/config`: what the UI needs to gate itself (§10.1, §11). */
export interface DatasetsConfig {
    enabled: boolean;
    storage_id?: string;
    max_items?: number;
    access_model?: 'open' | 'strict';
    sync_publish_max_objects?: number;
}

// ---------------------------------------------------------------------------
// Request bodies
// ---------------------------------------------------------------------------

export interface PublishOptions {
    message?: string;
}

export interface DatasetCreateRequest {
    name: string;
    definition: DatasetDefinition;
    /** Without it the dataset is created empty; with it version `v1` is published (§10.1). */
    publish?: PublishOptions;
}

export interface VersionPublishRequest {
    definition: DatasetDefinition;
    message: string;
    /** Optimistic concurrency check, like an ETag (§5.4). */
    expected_version?: number;
}

export interface DatasetValidateResponse {
    definition: DatasetDefinition;
    stats?: DatasetStats;
}

export interface ProposalCreateRequest {
    definition: DatasetDefinition;
    title: string;
    description?: string;
}

export interface ProposalUpdateRequest {
    title?: string;
    description?: string;
    definition?: DatasetDefinition;
    status?: 'closed';
}

export interface ProposalMergeRequest {
    message?: string;
}

export interface MetadataKeyCreateRequest {
    name: string;
    type: MetadataKeyType;
    required?: boolean;
    allowed_values?: string[];
    allow_multiple?: boolean;
    description?: string;
}

export interface MetadataKeyUpdateRequest {
    required?: boolean;
    allowed_values?: string[];
    allow_multiple?: boolean;
    description?: string;
}

export interface AccessGrantRequest {
    principal_type: PrincipalType;
    principal_id: string;
}

export type DiffType = 'definition' | 'data';

// ---------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------

class Datasets {
    // --- config -------------------------------------------------------------

    /** Feature configuration; `enabled: false` (or a 404/501) means the UI shows the gate. */
    async getConfig(): Promise<DatasetsConfig> {
        return datasetsRequest<DatasetsConfig>('/config', { method: 'GET' });
    }

    // --- datasets -----------------------------------------------------------

    /**
     * Paginated list, newest name order. `metadata` becomes the `metadata.<key>=<value>`
     * filters of §10.1 and matches against the latest version's metadata.
     */
    async list(
        params: {
            prefix?: string;
            after?: string;
            amount?: number;
            search?: string;
            metadata?: Record<string, string>;
        } = {},
    ): Promise<DatasetList> {
        const query = buildQuery(
            {
                prefix: params.prefix,
                after: params.after,
                amount: params.amount ?? DEFAULT_LISTING_AMOUNT,
                search: params.search,
            },
            buildMetadataQuery(params.metadata),
        );
        return datasetsRequest<DatasetList>(`${query}`, { method: 'GET' });
    }

    /** Creates the dataset (and `v1` when `publish` is set). Large ones answer a `Task`. */
    async create(request: DatasetCreateRequest): Promise<Dataset | Task> {
        return datasetsRequest<Dataset | Task>('', {
            method: 'POST',
            body: JSON.stringify(request),
        });
    }

    async get(name: string): Promise<Dataset> {
        return datasetsRequest<Dataset>(`/${segment(name)}`, { method: 'GET' });
    }

    async delete(name: string): Promise<void> {
        return datasetsRequest<void>(`/${segment(name)}`, { method: 'DELETE' });
    }

    /** Dry run: normalise, resolve, validate and authorise sources without writing (§10.1). */
    async validate(name: string, definition: DatasetDefinition): Promise<DatasetValidateResponse> {
        return datasetsRequest<DatasetValidateResponse>(`/${segment(name)}/validate`, {
            method: 'POST',
            body: JSON.stringify({ definition }),
        });
    }

    // --- versions -----------------------------------------------------------

    /** Newest first. */
    async listVersions(name: string, params: { after?: string; amount?: number } = {}): Promise<VersionList> {
        const query = buildQuery({ after: params.after, amount: params.amount ?? DEFAULT_LISTING_AMOUNT });
        return datasetsRequest<VersionList>(`/${segment(name)}/versions${query}`, { method: 'GET' });
    }

    /** `version` is `vN`, `N` or `latest`. */
    async getVersion(name: string, version: string | number): Promise<Version> {
        return datasetsRequest<Version>(`/${segment(name)}/versions/${segment(String(version))}`, { method: 'GET' });
    }

    /** `201` + `Version`, `202` + `Task` for large publishes, `409` on a version conflict. */
    async publishVersion(name: string, request: VersionPublishRequest): Promise<Version | Task> {
        return datasetsRequest<Version | Task>(`/${segment(name)}/versions`, {
            method: 'POST',
            body: JSON.stringify(request),
        });
    }

    // --- diff ---------------------------------------------------------------

    /** `leftRef`/`rightRef` accept `vN`, `latest` or `proposal-<id>` (§10.3). */
    async diff(
        name: string,
        leftRef: string,
        rightRef: string,
        params: { type?: DiffType; prefix?: string; after?: string; amount?: number } = {},
    ): Promise<DefinitionDiff | DataDiffList> {
        const query = buildQuery({
            type: params.type,
            prefix: params.prefix,
            after: params.after,
            amount: params.amount,
        });
        return datasetsRequest<DefinitionDiff | DataDiffList>(
            `/${segment(name)}/refs/${segment(leftRef)}/diff/${segment(rightRef)}${query}`,
            { method: 'GET' },
        );
    }

    /** Typed wrapper around `diff` for the structural diff, the default `type`. */
    async diffDefinitions(name: string, leftRef: string, rightRef: string): Promise<DefinitionDiff> {
        return (await this.diff(name, leftRef, rightRef, { type: 'definition' })) as DefinitionDiff;
    }

    /** Typed wrapper around `diff` for the paginated data diff. */
    async diffData(
        name: string,
        leftRef: string,
        rightRef: string,
        params: { prefix?: string; after?: string; amount?: number } = {},
    ): Promise<DataDiffList> {
        return (await this.diff(name, leftRef, rightRef, {
            ...params,
            type: 'data',
        })) as DataDiffList;
    }

    // --- proposals ----------------------------------------------------------

    async listProposals(
        name: string,
        params: { status?: ProposalStatus; after?: string; amount?: number } = {},
    ): Promise<ProposalList> {
        const query = buildQuery({ status: params.status, after: params.after, amount: params.amount });
        return datasetsRequest<ProposalList>(`/${segment(name)}/proposals${query}`, { method: 'GET' });
    }

    async createProposal(name: string, request: ProposalCreateRequest): Promise<Proposal> {
        return datasetsRequest<Proposal>(`/${segment(name)}/proposals`, {
            method: 'POST',
            body: JSON.stringify(request),
        });
    }

    /** Includes the diff summary against `latest`. */
    async getProposal(name: string, proposalId: string): Promise<Proposal> {
        return datasetsRequest<Proposal>(`/${segment(name)}/proposals/${segment(proposalId)}`, { method: 'GET' });
    }

    /** Partial update; `status: 'closed'` closes the proposal. */
    async updateProposal(name: string, proposalId: string, request: ProposalUpdateRequest): Promise<Proposal> {
        return datasetsRequest<Proposal>(`/${segment(name)}/proposals/${segment(proposalId)}`, {
            method: 'PATCH',
            body: JSON.stringify(request),
        });
    }

    /** `409` when the proposal's `base_version` is behind `latest` (§5.6). */
    async mergeProposal(name: string, proposalId: string, request: ProposalMergeRequest = {}): Promise<Version> {
        return datasetsRequest<Version>(`/${segment(name)}/proposals/${segment(proposalId)}/merge`, {
            method: 'POST',
            body: JSON.stringify(request),
        });
    }

    // --- access -------------------------------------------------------------

    async listAccess(name: string): Promise<AccessGrantList> {
        return datasetsRequest<AccessGrantList>(`/${segment(name)}/access`, { method: 'GET' });
    }

    async grantAccess(name: string, request: AccessGrantRequest): Promise<void> {
        return datasetsRequest<void>(`/${segment(name)}/access`, {
            method: 'POST',
            body: JSON.stringify(request),
        });
    }

    async revokeAccess(name: string, principalType: PrincipalType, principalId: string): Promise<void> {
        return datasetsRequest<void>(`/${segment(name)}/access/${segment(principalType)}/${segment(principalId)}`, {
            method: 'DELETE',
        });
    }

    // --- tasks --------------------------------------------------------------

    /** Status of an asynchronous publish (§6.5). */
    async getTask(name: string, taskId: string): Promise<Task> {
        return datasetsRequest<Task>(`/${segment(name)}/tasks/${segment(taskId)}`, { method: 'GET' });
    }

    // --- metadata keys ------------------------------------------------------

    async listMetadataKeys(params: { after?: string; amount?: number } = {}): Promise<MetadataKeyList> {
        const query = buildQuery({ after: params.after, amount: params.amount ?? DEFAULT_LISTING_AMOUNT });
        return datasetsRequest<MetadataKeyList>(`/metadata-keys${query}`, { method: 'GET' });
    }

    async createMetadataKey(request: MetadataKeyCreateRequest): Promise<MetadataKey> {
        return datasetsRequest<MetadataKey>('/metadata-keys', {
            method: 'POST',
            body: JSON.stringify(request),
        });
    }

    async getMetadataKey(key: string): Promise<MetadataKey> {
        return datasetsRequest<MetadataKey>(`/metadata-keys/${segment(key)}`, { method: 'GET' });
    }

    async updateMetadataKey(key: string, request: MetadataKeyUpdateRequest): Promise<MetadataKey> {
        return datasetsRequest<MetadataKey>(`/metadata-keys/${segment(key)}`, {
            method: 'PUT',
            body: JSON.stringify(request),
        });
    }

    async deleteMetadataKey(key: string): Promise<void> {
        return datasetsRequest<void>(`/metadata-keys/${segment(key)}`, { method: 'DELETE' });
    }
}

export const datasets = new Datasets();

// ---------------------------------------------------------------------------
// Dataset repositories
// ---------------------------------------------------------------------------

/**
 * Names of all datasets, for the repository views (DATASETS_DESIGN.md §11.3).
 *
 * A dataset is a repository with `metadata["lakefs.dataset"] === "true"`, but `GET
 * /repositories` does not return metadata, so the names come from the control plane instead.
 * Failures are not fatal here: when datasets are disabled (or unreachable) the caller simply
 * filters nothing out and the repositories page behaves as it did before the feature.
 */
const DATASET_NAMES_CACHE_MS = 30 * 1000;
/** Hard cap so a huge installation can't turn one page load into an endless paging loop. */
const DATASET_NAMES_MAX_PAGES = 10;

let datasetNamesCache: { names: Set<string>; at: number } | null = null;

export const clearDatasetNamesCache = (): void => {
    datasetNamesCache = null;
};

export const listDatasetNames = async (): Promise<Set<string>> => {
    const names = new Set<string>();
    let after = '';
    for (let page = 0; page < DATASET_NAMES_MAX_PAGES; page++) {
        const response = await datasets.list({ after, amount: MAX_LISTING_AMOUNT });
        for (const dataset of response.results) names.add(dataset.name);
        const next = response.pagination?.has_more ? response.pagination.next_offset : '';
        if (!next) break;
        after = next;
    }
    return names;
};

/** Cached `listDatasetNames`; resolves to an empty set when the control plane is unavailable. */
export const getDatasetNames = async (): Promise<Set<string>> => {
    const now = Date.now();
    if (datasetNamesCache && now - datasetNamesCache.at < DATASET_NAMES_CACHE_MS) {
        return datasetNamesCache.names;
    }
    try {
        const names = await listDatasetNames();
        datasetNamesCache = { names, at: now };
        return names;
    } catch {
        // Datasets disabled, unreachable, or the caller may not list them: filter nothing.
        return new Set<string>();
    }
};
