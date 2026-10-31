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
 *
 * `latestVersion` carries the extra member of the two stale-version `409`s of §5.4/§5.6, so
 * a caller never has to re-read the dataset to find out what the current version is.
 */
export class DatasetsError extends Error {
    readonly status: number;
    readonly errors: ErrorDetail[];
    readonly latestVersion?: number;

    constructor(message: string, status: number, errors: ErrorDetail[] = [], latestVersion?: number) {
        super(message);
        this.name = 'DatasetsError';
        this.status = status;
        this.errors = errors;
        this.latestVersion = latestVersion;
    }
}

/**
 * True for the stale-base `409` of §5.6: merging a pull request whose `base_version` is
 * behind the dataset's latest version requires an update first.
 * The status is decisive; the message is a fallback for servers that omit `latest_version`.
 */
export const isRebaseRequiredError = (error: unknown): error is DatasetsError =>
    error instanceof DatasetsError &&
    error.status === 409 &&
    (typeof error.latestVersion === 'number' || /rebase required/i.test(error.message));

/**
 * True when the control plane is switched off on this server, so clients should say so
 * instead of reporting a failure. A disabled control plane answers `501` (§10.4); a server
 * without the routes at all answers `404`.
 */
export const isDatasetsDisabledError = (error: unknown): boolean =>
    error instanceof DatasetsError && (error.status === 404 || error.status === 501);

/** True for a `403`: the caller is authenticated but does not hold the required permission. */
export const isForbiddenError = (error: unknown): error is DatasetsError =>
    error instanceof DatasetsError && error.status === 403;

const parseErrorBody = async (
    response: Response,
): Promise<{ message: string; errors: ErrorDetail[]; latestVersion?: number }> => {
    try {
        const body = await response.json();
        const errors: ErrorDetail[] = Array.isArray(body?.errors)
            ? body.errors.filter((detail: ErrorDetail) => detail && typeof detail.message === 'string')
            : [];
        const message = typeof body?.message === 'string' && body.message !== '' ? body.message : response.statusText;
        const latestVersion = typeof body?.latest_version === 'number' ? body.latest_version : undefined;
        return { message: message || 'request failed', errors, latestVersion };
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
        const { message, errors, latestVersion } = await parseErrorBody(response);
        throw new DatasetsError(message, response.status, errors, latestVersion);
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

/** Per-item source-resolution status of a published definition (§10.2 DataItemState). */
export type ItemStatus = 'ok' | 'source_repo_unavailable' | 'source_ref_unavailable' | 'caller_unauthorized';

export interface DataItemState extends DataItem {
    status?: ItemStatus;
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

/** The definition of a published version, with the status of every item (§10.2). */
export interface DatasetDefinitionState {
    format_version?: number;
    description?: string;
    metadata?: Record<string, MetadataValue>;
    data?: DataItemState[];
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
    id: string;
    /** Unix epoch seconds (§10.2). */
    creation_date: number;
    /** Latest published version; absent (or 0) when nothing was published yet. */
    version?: number;
    description?: string;
    definition?: DatasetDefinitionState;
    stats?: DatasetStats;
    /** Extension: the principal that created the dataset. */
    created_by?: string;
    /** Extension: the lifecycle state of the dataset record. */
    state?: DatasetState;
    /**
     * Extension: Unix epoch milliseconds of the last change. The record tracks it (§5.3) and
     * the list view shows it; the spec only requires `creation_date`.
     */
    updated_at?: number;
}

export interface DatasetList {
    results: Dataset[];
    pagination: Pagination;
}

export interface DatasetVersion {
    /** Version id, `v<N>`. */
    id: string;
    message?: string;
    author: string;
    /** Unix epoch seconds. */
    creation_date: number;
    /** Extension: the commit in the dataset repository. */
    commit_id?: string;
    /** Extension: set when the version was published by merging a pull request. */
    proposal_id?: string;
    definition?: DatasetDefinition;
    stats?: DatasetStats;
}

export interface DatasetVersionList {
    results: DatasetVersion[];
    pagination: Pagination;
}

/** `added` | `removed` | `changed` for definitions and data diffs (§10.3). */
export type ChangeType = 'added' | 'removed' | 'changed';

export interface StringChange {
    left: string;
    right: string;
}

export interface MetadataChange {
    type: ChangeType;
    key: string;
    left?: string[];
    right?: string[];
}

export interface ItemChange {
    type: ChangeType;
    target: string;
    left?: DataItem;
    right?: DataItem;
}

export interface DatasetDiff {
    format_version?: number;
    description?: StringChange;
    metadata?: MetadataChange[];
    data: ItemChange[];
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

export type PullRequestStatus = 'open' | 'closed' | 'merged';

export interface DatasetPullRequest {
    id: string;
    dataset: string;
    title: string;
    description?: string;
    author: string;
    status: PullRequestStatus;
    /** Version the pull request was made against; merging requires it to be latest (§5.6). */
    base_version: number;
    /** Latest version of the dataset at read time (0 when it does not exist). */
    latest_version: number;
    /** `base_version` differs from `latest_version`, so the pull request must be updated first. */
    stale: boolean;
    /** The dataset's name is unavailable (it was taken after the pull request was opened). */
    conflicted: boolean;
    /** The ref the pull request is materialised on, `pr/<id>`. */
    source_ref: string;
    /** Unix epoch seconds. */
    creation_date: number;
    /** Unix epoch seconds; set when closed or merged. */
    closed_date?: number;
    /** Extension: version produced by the merge. */
    merged_version?: number;
    /** Extension: hidden branch `_proposal-<id>` holding the materialised definition. */
    preview_branch?: string;
    definition?: DatasetDefinition;
    diff?: DatasetDiff;
    data_diff?: DataDiffList;
}

export interface DatasetPullRequestList {
    results: DatasetPullRequest[];
    pagination: Pagination;
}

/** The result of merging a pull request (§5.6). */
export interface DatasetPullRequestMerge {
    version: number;
    reference: string;
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

export type TaskErrorCode = 'publish_failed' | 'permission_denied' | 'source_unavailable';

/**
 * Sanitised failure of a failed task (SEC-8): a stable code and a fixed message. The raw error
 * stays in the server log, so it is never rendered from here.
 */
export interface TaskError {
    code: TaskErrorCode;
    message: string;
}

/** Status of an asynchronous publish (§6.5, §10.1). */
export interface Task {
    id: string;
    status: TaskStatus;
    version?: number;
    error?: TaskError;
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

/** Body of `PUT /datasets/{dataset}`: the new definition and its publish message (§10.1). */
export interface DatasetUpdate {
    definition: DatasetDefinition;
    publish: { message: string };
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

export interface DatasetPullRequestCreate {
    definition: DatasetDefinition;
    title: string;
    description?: string;
}

export interface DatasetPullRequestUpdate {
    title?: string;
    description?: string;
    definition?: DatasetDefinition;
    /** `closed` closes the pull request; the documented way to close is `DELETE`. */
    status?: 'closed';
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

    /** `ref` is `latest` or `v<N>`. */
    async getByRef(name: string, ref: string): Promise<Dataset> {
        return datasetsRequest<Dataset>(`/${segment(name)}/refs/${segment(ref)}`, { method: 'GET' });
    }

    /** The documented update: publishes the definition synchronously and returns the dataset. */
    async update(name: string, request: DatasetUpdate): Promise<Dataset> {
        return datasetsRequest<Dataset>(`/${segment(name)}`, {
            method: 'PUT',
            body: JSON.stringify(request),
        });
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
    async listVersions(name: string, params: { after?: string; amount?: number } = {}): Promise<DatasetVersionList> {
        const query = buildQuery({ after: params.after, amount: params.amount ?? DEFAULT_LISTING_AMOUNT });
        return datasetsRequest<DatasetVersionList>(`/${segment(name)}/versions${query}`, { method: 'GET' });
    }

    /** `version` is `vN`, `N` or `latest`. */
    async getVersion(name: string, version: string | number): Promise<DatasetVersion> {
        return datasetsRequest<DatasetVersion>(`/${segment(name)}/versions/${segment(String(version))}`, {
            method: 'GET',
        });
    }

    /** `201` + `DatasetVersion`, `202` + `Task` for large publishes, `409` on a version conflict. */
    async publishVersion(name: string, request: VersionPublishRequest): Promise<DatasetVersion | Task> {
        return datasetsRequest<DatasetVersion | Task>(`/${segment(name)}/versions`, {
            method: 'POST',
            body: JSON.stringify(request),
        });
    }

    // --- diff ---------------------------------------------------------------

    /** `leftRef`/`rightRef` accept `vN`, `latest` or `pr/<id>` (§10.3). */
    async diff(
        name: string,
        leftRef: string,
        rightRef: string,
        params: { type?: DiffType; prefix?: string; after?: string; amount?: number } = {},
    ): Promise<DatasetDiff | DataDiffList> {
        const query = buildQuery({
            type: params.type,
            prefix: params.prefix,
            after: params.after,
            amount: params.amount,
        });
        return datasetsRequest<DatasetDiff | DataDiffList>(
            `/${segment(name)}/refs/${segment(leftRef)}/diff/${segment(rightRef)}${query}`,
            { method: 'GET' },
        );
    }

    /** Typed wrapper around `diff` for the structural diff, the default `type`. */
    async diffDefinitions(name: string, leftRef: string, rightRef: string): Promise<DatasetDiff> {
        return (await this.diff(name, leftRef, rightRef, { type: 'definition' })) as DatasetDiff;
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

    // --- pull requests ------------------------------------------------------

    /** The global listing (§10.1 `GET /datasets/pulls`). */
    async listPullRequests(
        params: {
            status?: PullRequestStatus | 'all';
            dataset?: string;
            after?: string;
            amount?: number;
        } = {},
    ): Promise<DatasetPullRequestList> {
        const query = buildQuery({
            status: params.status,
            dataset: params.dataset,
            after: params.after,
            amount: params.amount ?? DEFAULT_LISTING_AMOUNT,
        });
        return datasetsRequest<DatasetPullRequestList>(`/pulls${query}`, { method: 'GET' });
    }

    async listDatasetPullRequests(
        name: string,
        params: { status?: PullRequestStatus; after?: string; amount?: number } = {},
    ): Promise<DatasetPullRequestList> {
        const query = buildQuery({ status: params.status, after: params.after, amount: params.amount });
        return datasetsRequest<DatasetPullRequestList>(`/${segment(name)}/pulls${query}`, { method: 'GET' });
    }

    async createPullRequest(name: string, request: DatasetPullRequestCreate): Promise<DatasetPullRequest> {
        return datasetsRequest<DatasetPullRequest>(`/${segment(name)}/pulls`, {
            method: 'POST',
            body: JSON.stringify(request),
        });
    }

    /** Includes the diff summary against `latest`. */
    async getPullRequest(name: string, pullRequestId: string): Promise<DatasetPullRequest> {
        return datasetsRequest<DatasetPullRequest>(`/${segment(name)}/pulls/${segment(pullRequestId)}`, {
            method: 'GET',
        });
    }

    /** Partial update; the documented way to close a pull request is `closePullRequest`. */
    async updatePullRequest(
        name: string,
        pullRequestId: string,
        request: DatasetPullRequestUpdate,
    ): Promise<DatasetPullRequest> {
        return datasetsRequest<DatasetPullRequest>(`/${segment(name)}/pulls/${segment(pullRequestId)}`, {
            method: 'PATCH',
            body: JSON.stringify(request),
        });
    }

    /** Closes the pull request without publishing anything (`DELETE`, 204). */
    async closePullRequest(name: string, pullRequestId: string): Promise<void> {
        return datasetsRequest<void>(`/${segment(name)}/pulls/${segment(pullRequestId)}`, { method: 'DELETE' });
    }

    /** The structural diff between `latest` and what the pull request proposes. */
    async getPullRequestDiff(name: string, pullRequestId: string): Promise<DatasetDiff> {
        return datasetsRequest<DatasetDiff>(`/${segment(name)}/pulls/${segment(pullRequestId)}/diff`, {
            method: 'GET',
        });
    }

    /** `409` when the pull request's `base_version` is behind `latest` (§5.6). */
    async mergePullRequest(name: string, pullRequestId: string): Promise<DatasetPullRequestMerge> {
        return datasetsRequest<DatasetPullRequestMerge>(`/${segment(name)}/pulls/${segment(pullRequestId)}/merge`, {
            method: 'POST',
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

    async listMetadataKeys(params: { prefix?: string; after?: string; amount?: number } = {}): Promise<MetadataKeyList> {
        const query = buildQuery({
            prefix: params.prefix,
            after: params.after,
            amount: params.amount ?? DEFAULT_LISTING_AMOUNT,
        });
        return datasetsRequest<MetadataKeyList>(`/metadata${query}`, { method: 'GET' });
    }

    async createMetadataKey(request: MetadataKeyCreateRequest): Promise<MetadataKey> {
        return datasetsRequest<MetadataKey>('/metadata', {
            method: 'POST',
            body: JSON.stringify(request),
        });
    }

    async getMetadataKey(key: string): Promise<MetadataKey> {
        return datasetsRequest<MetadataKey>(`/metadata/${segment(key)}`, { method: 'GET' });
    }

    async updateMetadataKey(key: string, request: MetadataKeyUpdateRequest): Promise<MetadataKey> {
        return datasetsRequest<MetadataKey>(`/metadata/${segment(key)}`, {
            method: 'PUT',
            body: JSON.stringify(request),
        });
    }

    async deleteMetadataKey(key: string): Promise<void> {
        return datasetsRequest<void>(`/metadata/${segment(key)}`, { method: 'DELETE' });
    }
}

export const datasets = new Datasets();

/** Kept for callers that used the pre-pull-request name of the async publish response. */
export type Version = DatasetVersion;
/** Kept for callers that used the pre-pull-request name of the version list. */
export type VersionList = DatasetVersionList;
/** `DatasetPullRequest` under its pre-pull-request name. */
export type Proposal = DatasetPullRequest;
/** `DatasetPullRequestList` under its pre-pull-request name. */
export type ProposalList = DatasetPullRequestList;
/** `PullRequestStatus` under its pre-pull-request name. */
export type ProposalStatus = PullRequestStatus;
/** `DatasetDiff` under its pre-pull-request name. */
export type DefinitionDiff = DatasetDiff;

/** The numeric part of a version id, `v<N>`; 0 for a missing version. */
export const versionNumber = (version: Pick<DatasetVersion, 'id'> | null | undefined): number => {
    if (!version) return 0;
    const parsed = Number.parseInt(version.id.replace(/^v/i, ''), 10);
    return Number.isFinite(parsed) ? parsed : 0;
};

/** The published definition of a dataset record, in the input definition shape. */
export const definitionOf = (state: DatasetDefinitionState | undefined): DatasetDefinition | undefined => {
    if (!state) return undefined;
    return {
        format_version: 1,
        description: state.description,
        metadata: state.metadata,
        data: state.data ?? [],
    };
};

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
        for (const dataset of response.results) names.add(dataset.id);
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
