import { defaultAPIHeaders } from './index';

/**
 * Typed, read-only client for the Apache Iceberg REST catalog (`pkg/icebergcatalog`, see
 * `docs/iceberg.md`).
 *
 * The transport mirrors `lib/api/index.js` and `lib/api/datasets.ts`: `fetch` with the lakeFS
 * default headers, a status switch per call and errors surfaced as exceptions. It differs from
 * them in one deliberate way: no answer becomes an `AuthenticationError`, so `useAPI()` cannot
 * redirect a reader of the Tables tab to the login page (see "Error handling" below). The
 * catalog is mounted at `/iceberg/api`, next to `/api/v1` rather than below it, and it addresses a
 * table as `repo␟ref␟namespace…␟table` - the repository and the ref are the first two namespace
 * levels (docs/iceberg.md, "Addressing"). The `␟` separator is `%1F` on the wire.
 *
 * Only the catalog's own read endpoints are used here: `GET /v1/config`, the namespace listing,
 * the table listing and the table load. The repository Tables tab is a viewer; creating, dropping
 * or committing a table stays a catalog-client job.
 */

/** Base URL of the catalog. It sits next to the core API, not below it (`API_ENDPOINT`). */
export const ICEBERG_ENDPOINT = '/iceberg/api';

/** `GET /v1/config`, the one catalog operation that carries no `{prefix}` segment. */
const CONFIG_PATH = '/v1/config';

/** The separator of the Iceberg namespace levels; `%1F` on the wire (docs/iceberg.md). */
export const NAMESPACE_SEPARATOR = '\u001f';

/** The prefix the catalog advertises in `GET /v1/config` when the server names no other. */
const DEFAULT_CATALOG_PREFIX = 'lakefs';

/**
 * The message the UI shows when the catalog answers 501, i.e. when `iceberg.enabled` is false.
 * The plain build keeps its Enterprise upsell for that answer.
 */
export const ICEBERG_DISABLED_MESSAGE = 'The Iceberg catalog is disabled on this server';

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

/**
 * A failed catalog request. The catalog answers every failure in the Iceberg envelope,
 * `{"error": {"message": …, "type": …, "code": …}}`, where `type` is the exception class the
 * Iceberg clients map to an error (`ForbiddenException`, `NoSuchTableException`, …).
 *
 * Every status, a `401` included, is this error and nothing else: the catalog's `401` means the
 * request carried no credential the catalog could use (`pkg/icebergcatalog/auth.go`), which says
 * nothing about the reader's core lakeFS session, so the tab shows the answer in place instead of
 * handing the reader to the login page.
 */
export class IcebergError extends Error {
    readonly status: number;
    readonly type?: string;

    constructor(message: string, status: number, type?: string) {
        super(message);
        this.name = 'IcebergError';
        this.status = status;
        this.type = type;
    }
}

/**
 * True when the catalog is switched off on this server, so the repository Tables tab shows the
 * Enterprise upsell instead of an error. A disabled catalog answers `501` (the catalog's own
 * `NotImplementedException`); `404` is included for a server that does not mount the catalog at
 * all, where the core placeholder answers instead.
 */
export const isIcebergDisabledError = (error: unknown): boolean =>
    error instanceof IcebergError && (error.status === 501 || error.status === 404);

/**
 * True when the catalog could not authenticate the caller: the session cookie the browser sent is
 * missing, expired or not valid for the catalog, and the catalog answers the constant `401` of
 * `pkg/icebergcatalog/auth.go`. The Tables tab says so in place and offers a plain sign-in link;
 * it never turns it into a login redirect (see the error-handling comment above).
 */
export const isIcebergUnauthenticatedError = (error: unknown): boolean =>
    error instanceof IcebergError && error.status === 401;

/**
 * True when the caller is authenticated but not allowed: the catalog answers a denied
 * `catalog:*` check with `403 ForbiddenException` (`pkg/icebergcatalog/authz.go`). Authorization
 * stays the catalog's decision; this only picks the UI state for it. A `401` is deliberately not
 * merged into this: it is about the session, not about the permission.
 */
export const isIcebergForbiddenError = (error: unknown): boolean =>
    error instanceof IcebergError && error.status === 403;

const parseErrorBody = async (response: Response): Promise<{ message: string; type?: string }> => {
    try {
        const body = await response.json();
        const error = body?.error;
        if (error && typeof error === 'object') {
            const type = typeof error.type === 'string' && error.type !== '' ? error.type : undefined;
            const message =
                typeof error.message === 'string' && error.message !== '' ? error.message : response.statusText;
            return { message: message || 'request failed', type };
        }
        // Some answers (the core placeholder, a proxy) use the core {"message": …} body.
        const message = typeof body?.message === 'string' && body.message !== '' ? body.message : response.statusText;
        return { message: message || 'request failed' };
    } catch {
        // Not a JSON body (a proxy error page, for example): fall back to the status line.
        return { message: response.statusText || 'request failed' };
    }
};

const icebergRequest = async <T>(path: string, requestData: RequestInit = {}): Promise<T> => {
    const headers = new Headers({
        ...defaultAPIHeaders,
        ...((requestData.headers as Record<string, string> | undefined) ?? {}),
    });
    // The UI authenticates with the lakeFS login session cookie. The catalog accepts it for reads
    // only (pkg/icebergcatalog/auth.go), and `fetch` sends a same-origin cookie by default; the
    // option is spelled out so the dependency on it is visible.
    const response = await fetch(`${ICEBERG_ENDPOINT}${path}`, {
        credentials: 'same-origin',
        ...requestData,
        headers,
    });

    if (!response.ok) {
        // Every failure, a 401 included, stays an IcebergError. This client never throws the shared
        // AuthenticationError of `lib/api/index.js`, so `useAPI()` never treats a catalog answer as
        // a lost core session and never redirects to the login page: the repository Tables tab
        // reports the failure in place, the way the Datasets and Repositories pages do.
        const { message, type } = await parseErrorBody(response);
        throw new IcebergError(message, response.status, type);
    }
    if (response.status === 204) return undefined as T;
    return (await response.json()) as T;
};

// ---------------------------------------------------------------------------
// Addressing (docs/iceberg.md, "Addressing")
// ---------------------------------------------------------------------------

/**
 * The namespace levels of a repository ref: the catalog carries the repository and the ref as the
 * first two namespace levels, so everything the UI looks at starts with `[repo, ref]`.
 */
export const refNamespace = (repository: string, ref: string): string[] => [repository, ref];

/** The levels of a real namespace: `[repo, ref, …levels]`. */
export const namespaceOf = (repository: string, ref: string, levels: string[] = []): string[] =>
    refNamespace(repository, ref).concat(levels);

/** The wire form of one path parameter: levels joined by `%1F` (each level encoded). */
export const namespaceParam = (levels: string[]): string => levels.map(encodeURIComponent).join('%1F');

/** The namespace as a client writes it: `repo.ref.ns1.ns2` (dots are ordinary characters). */
export const namespaceLabel = (levels: string[]): string => levels.join('.');

/**
 * The levels below a ref that a listing returned, i.e. what the UI shows as the namespace name.
 * The catalog returns full addresses (`[repo, ref, ns…]`), so the two macro levels are dropped;
 * this is the same "the URL fixed them" idea as the catalog's `relative_to` form, done here
 * because the UI always uses the `/iceberg/api` form.
 */
export const namespaceLevelsBelowRef = (repository: string, ref: string, namespace: string[]): string[] =>
    namespace.length >= 2 && namespace[0] === repository && namespace[1] === ref ? namespace.slice(2) : namespace;

// ---------------------------------------------------------------------------
// Schema types (the catalog's JSON models)
// ---------------------------------------------------------------------------

/** `GET /v1/config` (spec: CatalogConfig). */
export interface IcebergCatalogConfig {
    defaults?: Record<string, string>;
    overrides?: Record<string, string>;
    endpoints?: string[];
    'idempotency-key-lifetime'?: string;
}

/** `GET /v1/{prefix}/namespaces` (spec: ListNamespacesResponse). */
export interface IcebergNamespaceList {
    namespaces: string[][];
    'next-page-token'?: string;
}

/** The spec's TableIdentifier: the namespace levels and the table name. */
export interface IcebergTableIdentifier {
    namespace: string[];
    name: string;
}

/** `GET /v1/{prefix}/namespaces/{namespace}/tables` (spec: ListTablesResponse). */
export interface IcebergTableList {
    identifiers: IcebergTableIdentifier[];
    'next-page-token'?: string;
}

/**
 * A field's type: a primitive name (`"long"`, `"string"`, …), or a nested type
 * (`{"type": "struct", "fields": […]}` / `{"type": "list", "element": …}`).
 */
export type IcebergFieldType = string | { [key: string]: unknown };

export interface IcebergSchemaField {
    id: number;
    name: string;
    required?: boolean;
    type: IcebergFieldType;
    doc?: string;
}

/** A schema (spec: Schema): `struct` with its fields, plus the ids the metadata refers to. */
export interface IcebergSchema {
    type?: string;
    'schema-id'?: number;
    'identifier-field-ids'?: number[];
    fields?: IcebergSchemaField[];
}

export interface IcebergPartitionField {
    name: string;
    transform: string;
    'source-id': number;
    'field-id': number;
}

/** A partition spec (spec: PartitionSpec). */
export interface IcebergPartitionSpec {
    'spec-id': number;
    fields?: IcebergPartitionField[];
}

/** A snapshot (spec: Snapshot). `summary` carries the operation and its counters. */
export interface IcebergSnapshot {
    'snapshot-id': number;
    'parent-snapshot-id'?: number;
    'sequence-number'?: number;
    'timestamp-ms': number;
    'manifest-list'?: string;
    'schema-id'?: number;
    summary?: Record<string, string>;
}

/** A snapshot log entry (spec: SnapshotLog). */
export interface IcebergSnapshotLogEntry {
    'snapshot-id': number;
    'timestamp-ms': number;
}

/**
 * A table's metadata document (spec: TableMetadata). Version 1 carries `schema` and
 * `partition-spec` instead of `schemas`/`partition-specs`, so both shapes are modelled and read
 * through the helpers below.
 */
export interface IcebergTableMetadata {
    'format-version'?: number;
    'table-uuid'?: string;
    location?: string;
    'last-updated-ms'?: number;
    'last-sequence-number'?: number;
    'last-column-id'?: number;
    'current-schema-id'?: number;
    schemas?: IcebergSchema[];
    schema?: IcebergSchema;
    'default-spec-id'?: number;
    'partition-specs'?: IcebergPartitionSpec[];
    'partition-spec'?: IcebergPartitionField[];
    'last-partition-id'?: number;
    'default-sort-order-id'?: number;
    properties?: Record<string, string>;
    'current-snapshot-id'?: number;
    snapshots?: IcebergSnapshot[];
    'snapshot-log'?: IcebergSnapshotLogEntry[];
    refs?: Record<string, { 'snapshot-id': number; type: string }>;
    [key: string]: unknown;
}

/** `GET /v1/{prefix}/namespaces/{namespace}/tables/{table}` (spec: LoadTableResult). */
export interface IcebergLoadTableResult {
    'metadata-location': string;
    metadata: IcebergTableMetadata;
    config?: Record<string, string>;
}

// ---------------------------------------------------------------------------
// Reading the metadata document
// ---------------------------------------------------------------------------

/**
 * The table's current schema: the one `current-schema-id` names, falling back to the last schema
 * in the list (the newest) and to the version 1 `schema` member. `undefined` for a metadata
 * document that carries no schema at all.
 */
export const currentSchema = (metadata: IcebergTableMetadata | undefined): IcebergSchema | undefined => {
    if (!metadata) return undefined;
    const schemas = metadata.schemas ?? [];
    const byID = schemas.find((schema) => schema['schema-id'] === metadata['current-schema-id']);
    return byID ?? schemas[schemas.length - 1] ?? metadata.schema;
};

/**
 * The table's default partition spec: the one `default-spec-id` names, falling back to the first
 * spec and to a version 1 `partition-spec` list (an unpartitioned table has no fields).
 */
export const currentPartitionSpec = (
    metadata: IcebergTableMetadata | undefined,
): { id?: number; fields: IcebergPartitionField[] } => {
    if (!metadata) return { fields: [] };
    const specs = metadata['partition-specs'] ?? [];
    const byID = specs.find((spec) => spec['spec-id'] === metadata['default-spec-id']);
    const spec = byID ?? specs[0];
    if (spec) return { id: spec['spec-id'], fields: spec.fields ?? [] };
    return { fields: metadata['partition-spec'] ?? [] };
};

/** The schema field a partition field's `source-id` names, for the partition spec table. */
export const sourceFieldName = (schema: IcebergSchema | undefined, sourceID: number): string | undefined =>
    schema?.fields?.find((field) => field.id === sourceID)?.name;

/**
 * The snapshot `current-snapshot-id` names: the table's current state. A table that has never
 * been written to has no current snapshot.
 */
export const currentSnapshot = (metadata: IcebergTableMetadata | undefined): IcebergSnapshot | undefined => {
    const id = metadata?.['current-snapshot-id'];
    if (!metadata || id === undefined || id === null) return undefined;
    return (metadata.snapshots ?? []).find((snapshot) => snapshot['snapshot-id'] === id);
};

/**
 * The snapshot history, newest first. The metadata's `snapshot-log` records when each snapshot
 * became current, but a table may carry snapshots outside the log (expired or tagged ones), so the
 * history is built from `snapshots`, ordered by timestamp.
 */
export const snapshotHistory = (metadata: IcebergTableMetadata | undefined): IcebergSnapshot[] =>
    [...(metadata?.snapshots ?? [])].sort((a, b) => b['timestamp-ms'] - a['timestamp-ms']);

/** The operation of a snapshot's summary (`append`, `overwrite`, `delete`, `replace`, …). */
export const snapshotOperation = (snapshot: IcebergSnapshot | undefined): string | undefined =>
    snapshot?.summary?.operation;

/**
 * A human-readable rendering of an Iceberg type: primitives as written in the metadata, and the
 * nested types in the JSON notation Iceberg clients print (`struct<…>`, `list<…>`, `map<…>`).
 */
export const formatIcebergType = (type: IcebergFieldType | undefined): string => {
    if (type === undefined || type === null) return 'unknown';
    if (typeof type === 'string') return type;
    const nested = type as Record<string, unknown>;
    switch (nested.type) {
        case 'struct': {
            const fields = (nested.fields as IcebergSchemaField[] | undefined) ?? [];
            return `struct<${fields
                .map(
                    (field) =>
                        `${field.id}: ${field.name}: ${field.required ? 'required ' : 'optional '}${formatIcebergType(field.type)}`,
                )
                .join(', ')}>`;
        }
        case 'list': {
            const element = nested.element as IcebergFieldType | undefined;
            const elementID = nested['element-id'];
            return `list<${elementID !== undefined ? `${elementID}: ` : ''}${formatIcebergType(element)}${
                nested['element-required'] === false ? ' optional' : ''
            }>`;
        }
        case 'map': {
            const key = nested.key as IcebergFieldType | undefined;
            const value = nested.value as IcebergFieldType | undefined;
            return `map<${formatIcebergType(key)}, ${formatIcebergType(value)}>`;
        }
        default:
            return typeof nested.type === 'string' ? nested.type : JSON.stringify(type);
    }
};

// ---------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------

class Iceberg {
    /**
     * The prefix `GET /v1/config` advertised (`overrides.prefix`, i.e. `lakefs`). It is fetched
     * once per page load and reused: every namespace, table and load request carries it.
     */
    private prefixPromise: Promise<string> | null = null;

    /** `GET /v1/config`: the catalog's negotiation endpoint. A 501 means the catalog is off. */
    async getConfig(): Promise<IcebergCatalogConfig> {
        const config = await icebergRequest<IcebergCatalogConfig>(CONFIG_PATH, { method: 'GET' });
        const prefix = config?.overrides?.prefix ?? config?.defaults?.prefix;
        if (prefix) this.prefixPromise = Promise.resolve(prefix);
        return config;
    }

    /** Drops the cached prefix, so the next request reads the configuration again. */
    clearCache(): void {
        this.prefixPromise = null;
    }

    private async prefix(): Promise<string> {
        if (!this.prefixPromise) {
            this.prefixPromise = this.getConfig()
                .then((config) => config?.overrides?.prefix ?? config?.defaults?.prefix ?? DEFAULT_CATALOG_PREFIX)
                .catch((error) => {
                    // A failed read must not be cached: the next call retries it.
                    this.prefixPromise = null;
                    throw error;
                });
        }
        return this.prefixPromise;
    }

    /**
     * `GET /v1/{prefix}/namespaces`: the immediate child namespaces of `parent`.
     *
     * With no parent the catalog lists the repositories the caller may read, with
     * `[repository]` the refs of a repository and with `[repository, ref, …ns]` the namespaces
     * below that one. The answer always carries full addresses, so each entry starts with the
     * parent's levels (docs/iceberg.md, "the namespace listing has three shapes").
     */
    async listNamespaces({
        parent,
        pageToken,
        pageSize,
    }: { parent?: string[]; pageToken?: string; pageSize?: number } = {}): Promise<IcebergNamespaceList> {
        const prefix = await this.prefix();
        const query = new URLSearchParams();
        if (parent && parent.length > 0) query.set('parent', parent.join(NAMESPACE_SEPARATOR));
        if (pageToken) query.set('pageToken', pageToken);
        if (pageSize !== undefined) query.set('pageSize', String(pageSize));
        const search = query.toString();
        return icebergRequest<IcebergNamespaceList>(
            `/v1/${encodeURIComponent(prefix)}/namespaces${search ? `?${search}` : ''}`,
            {
                method: 'GET',
            },
        );
    }

    /** `GET /v1/{prefix}/namespaces/{namespace}`: the namespace and its properties. */
    async getNamespace(namespace: string[]): Promise<{ namespace: string[]; properties?: Record<string, string> }> {
        const prefix = await this.prefix();
        return icebergRequest(`/v1/${encodeURIComponent(prefix)}/namespaces/${namespaceParam(namespace)}`, {
            method: 'GET',
        });
    }

    /** `GET /v1/{prefix}/namespaces/{namespace}/tables`: the tables directly in `namespace`. */
    async listTables(
        namespace: string[],
        { pageToken, pageSize }: { pageToken?: string; pageSize?: number } = {},
    ): Promise<IcebergTableList> {
        const prefix = await this.prefix();
        const query = new URLSearchParams();
        if (pageToken) query.set('pageToken', pageToken);
        if (pageSize !== undefined) query.set('pageSize', String(pageSize));
        const search = query.toString();
        return icebergRequest<IcebergTableList>(
            `/v1/${encodeURIComponent(prefix)}/namespaces/${namespaceParam(namespace)}/tables${search ? `?${search}` : ''}`,
            { method: 'GET' },
        );
    }

    /**
     * `GET /v1/{prefix}/namespaces/{namespace}/tables/{table}`: the load result - the physical
     * metadata.json location, the metadata document and the table's client configuration.
     */
    async loadTable(namespace: string[], table: string): Promise<IcebergLoadTableResult> {
        const prefix = await this.prefix();
        return icebergRequest<IcebergLoadTableResult>(
            `/v1/${encodeURIComponent(prefix)}/namespaces/${namespaceParam(namespace)}/tables/${encodeURIComponent(table)}`,
            { method: 'GET' },
        );
    }
}

export const iceberg = new Iceberg();
