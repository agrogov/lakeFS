import { defaultAPIHeaders } from './index';

/**
 * Typed client for the Apache Iceberg REST catalog (`pkg/icebergcatalog`, see `docs/iceberg.md`).
 *
 * The transport mirrors `lib/api/index.js` and `lib/api/datasets.ts`: `fetch` with the lakeFS
 * default headers, a status switch per call and errors surfaced as exceptions. It differs from
 * them in one deliberate way: no answer becomes an `AuthenticationError`, so `useAPI()` cannot
 * redirect a reader of the Tables tab to the login page (see "Error handling" below). The
 * catalog is mounted at `/iceberg/api`, next to `/api/v1` rather than below it, and it addresses a
 * table as `repo␟ref␟namespace…␟table` - the repository and the ref are the first two namespace
 * levels (docs/iceberg.md, "Addressing"). The `␟` separator is `%1F` on the wire.
 *
 * Two kinds of operation are used here:
 *
 *   - the read endpoints: `GET /v1/config`, the namespace listing, the table listing and the
 *     table load, which the Tables tab renders;
 *   - the four management operations the Tables tab performs: create namespace, drop namespace,
 *     create table and drop table. They are state-changing, and the catalog accepts the UI's
 *     session cookie for them only because every request from this client carries the
 *     `X-Lakefs-Client: lakefs-webui/...` header `defaultAPIHeaders` sets, and only for exactly
 *     those four operations (`pkg/icebergcatalog/auth.go`). A cookie write anywhere else - the
 *     commit, register, properties, metrics, remote signing, the token endpoint - is `401`, which
 *     is why this client never makes one: committing and registering a table's data stays an
 *     Iceberg client's job.
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
    // The UI authenticates with the lakeFS login session cookie. The catalog accepts it for reads,
    // and for the four management operations of this client - create/drop namespace and
    // create/drop table - because every request carries the `X-Lakefs-Client: lakefs-webui/...`
    // header of `defaultAPIHeaders` (pkg/icebergcatalog/auth.go). `fetch` sends a same-origin cookie
    // by default; the option is spelled out so the dependency on it is visible.
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

/**
 * One row of the create-table form: a column name, an Iceberg type and whether the column is
 * required. The type is one of `ICEBERG_PRIMITIVE_TYPES`, or any type string the catalog's own
 * parser accepts (the form keeps it a string so a client's `struct<…>` can be typed too).
 */
export interface IcebergNewColumn {
    name: string;
    type: string;
    required: boolean;
}

/**
 * The create-table form: the table's name, its initial columns, and the optional column to
 * partition by with the identity transform. Everything else a CreateTableRequest can carry
 * (location, sort order, table properties, format version) is left to the catalog's defaults.
 */
export interface IcebergCreateTableForm {
    name: string;
    columns: IcebergNewColumn[];
    /** The column the table is identity-partitioned by; absent or empty means unpartitioned. */
    partitionBy?: string;
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
// Writing: the create forms and the request they become
// ---------------------------------------------------------------------------

/**
 * The primitive types the create-table form offers. They are the names of the Iceberg type spec, so
 * the catalog's own parser (`iceberg-go`) accepts them unchanged; the form lets a caller type any
 * other type string it accepts as well.
 */
export const ICEBERG_PRIMITIVE_TYPES = [
    'boolean',
    'int',
    'long',
    'float',
    'double',
    'date',
    'time',
    'timestamp',
    'timestamptz',
    'string',
    'uuid',
    'binary',
] as const;

// partitionFieldID is the field id of the create form's identity partition field. A create request
// carries the client's own placeholders and the catalog resolves them (§6.2), so any id the client
// picks is fine; 1000 is the first id the Iceberg spec reserves for partition fields.
const partitionFieldID = 1000;

/**
 * The schema of a create request (spec: Schema), built from the form's columns.
 *
 * Field ids are the client's to assign on a create - the table does not exist yet - so they are
 * numbered from 1 in form order. The catalog resolves the partition spec's `source-id` against
 * them and assigns the table its own ids, which is what makes a `required` flag, a column order and
 * an identity partition by name all survive the create.
 */
export const createTableSchema = (columns: IcebergNewColumn[]): IcebergSchema => ({
    type: 'struct',
    'schema-id': 0,
    fields: columns.map((column, index) => ({
        id: index + 1,
        name: column.name.trim(),
        required: column.required,
        type: column.type.trim(),
    })),
});

/**
 * The identity partition spec of a create request for `partitionBy`, or `undefined` for an
 * unpartitioned table. The source id is the column's id in `createTableSchema`, which is its
 * position in the form plus one.
 */
export const createTablePartitionSpec = (
    columns: IcebergNewColumn[],
    partitionBy: string | undefined,
): IcebergPartitionSpec | undefined => {
    const column = partitionBy ? columns.find((candidate) => candidate.name.trim() === partitionBy.trim()) : undefined;
    if (!column) return undefined;
    return {
        'spec-id': 0,
        fields: [
            {
                name: column.name.trim(),
                transform: 'identity',
                'source-id': columns.indexOf(column) + 1,
                'field-id': partitionFieldID,
            },
        ],
    };
};

/**
 * The CreateTableRequest body of a form (spec: components.schemas.CreateTableRequest): the name, the
 * schema, and the identity partition spec when the form named a partition column. Members the form
 * does not model are absent rather than empty, so the catalog applies its own defaults (the managed
 * table location, an unsorted order, format version 2).
 */
export const createTableRequestBody = (form: IcebergCreateTableForm): Record<string, unknown> => {
    const body: Record<string, unknown> = {
        name: form.name.trim(),
        schema: createTableSchema(form.columns),
    };
    const partitionSpec = createTablePartitionSpec(form.columns, form.partitionBy);
    if (partitionSpec) body['partition-spec'] = partitionSpec;
    return body;
};

/**
 * The problem with a create-table form, or `null` when it can be submitted. It checks only what the
 * form can get wrong on its own - the catalog remains the authority on identifiers, types and
 * locations, and its answer is shown in place. `partitionBy` is matched against the columns by name,
 * because that is how the form offers it.
 */
export const validateCreateTableForm = (form: IcebergCreateTableForm): string | null => {
    if (form.name.trim() === '') return 'The table needs a name.';
    if (form.columns.length === 0) return 'The table needs at least one column.';
    const names = form.columns.map((column) => column.name.trim());
    if (names.some((name) => name === '')) return 'Every column needs a name.';
    const duplicate = names.find((name, index) => names.indexOf(name) !== index);
    if (duplicate !== undefined) return `The column ${duplicate} is defined twice.`;
    if (form.columns.some((column) => column.type.trim() === '')) return 'Every column needs a type.';
    if (form.partitionBy && form.partitionBy.trim() !== '' && !names.includes(form.partitionBy.trim())) {
        return 'The partition column must be one of the table columns.';
    }
    return null;
};

/**
 * The namespace levels of a path as the create forms type it: `inventory.books` becomes
 * `['inventory', 'books']`. Dots are the separator the UI already displays namespace levels with
 * (`namespaceLabel`), so a level whose own name contains a dot cannot be typed this way; the tab's
 * tree is the unambiguous view of it.
 */
export const namespacePathLevels = (path: string): string[] =>
    path
        .split('.')
        .map((level) => level.trim())
        .filter((level) => level !== '');

/**
 * The problem with the namespace path of a create-namespace or create-table form, or `null` when it
 * can be submitted. The catalog validates the identifiers themselves; what it cannot answer for the
 * UI is that there is nothing to send at all.
 */
export const validateNamespacePath = (path: string): string | null =>
    namespacePathLevels(path).length === 0 ? 'The namespace needs at least one level below the ref.' : null;

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

    /**
     * `POST /v1/{prefix}/namespaces`: create a namespace, with `namespace` the full levels
     * `[repository, ref, …levels]` (docs/iceberg.md, "Addressing").
     *
     * The catalog stages a marker entry for it on the ref, so the create is not visible in the
     * branch's data until the branch is committed - the same as every other catalog write. A name
     * that is taken is `409 NamespaceAlreadyExistsException`.
     */
    async createNamespace(
        namespace: string[],
        properties?: Record<string, string>,
    ): Promise<{ namespace: string[]; properties: Record<string, string> }> {
        const prefix = await this.prefix();
        return icebergRequest<{ namespace: string[]; properties: Record<string, string> }>(
            `/v1/${encodeURIComponent(prefix)}/namespaces`,
            { method: 'POST', body: JSON.stringify({ namespace, properties: properties ?? {} }) },
        );
    }

    /**
     * `DELETE /v1/{prefix}/namespaces/{namespace}`: drop a namespace.
     *
     * The catalog refuses a namespace that still holds a table or a child namespace with
     * `409 NamespaceNotEmptyException`, which is the answer the tab shows in place; the reader drops
     * what is inside it first.
     */
    async dropNamespace(namespace: string[]): Promise<void> {
        const prefix = await this.prefix();
        await icebergRequest<void>(`/v1/${encodeURIComponent(prefix)}/namespaces/${namespaceParam(namespace)}`, {
            method: 'DELETE',
        });
    }

    /**
     * `POST /v1/{prefix}/namespaces/{namespace}/tables`: create a table from the form, and return the
     * load result of the table the catalog created.
     *
     * The schema and the optional identity partition spec are built by `createTableRequestBody`; the
     * namespace must already exist (`404 NoSuchNamespaceException`), and a taken name is
     * `409 AlreadyExistsException`.
     */
    async createTable(namespace: string[], form: IcebergCreateTableForm): Promise<IcebergLoadTableResult> {
        const prefix = await this.prefix();
        return icebergRequest<IcebergLoadTableResult>(
            `/v1/${encodeURIComponent(prefix)}/namespaces/${namespaceParam(namespace)}/tables`,
            { method: 'POST', body: JSON.stringify(createTableRequestBody(form)) },
        );
    }

    /**
     * `DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}`: drop a table.
     *
     * The drop removes the table's pointer and the file entries staged for it on the ref; the objects
     * themselves stay in the object store and are reclaimed by GC, so the drop is staged and
     * reversible like every other catalog write (docs/iceberg.md, "Staged commits").
     */
    async dropTable(namespace: string[], table: string): Promise<void> {
        const prefix = await this.prefix();
        await icebergRequest<void>(
            `/v1/${encodeURIComponent(prefix)}/namespaces/${namespaceParam(namespace)}/tables/${encodeURIComponent(table)}`,
            { method: 'DELETE' },
        );
    }
}

export const iceberg = new Iceberg();
