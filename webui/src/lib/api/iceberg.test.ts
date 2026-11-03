import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { AuthenticationError } from './index';
import {
    createTablePartitionSpec,
    createTableRequestBody,
    createTableSchema,
    currentPartitionSpec,
    currentSchema,
    currentSnapshot,
    formatIcebergType,
    iceberg,
    IcebergError,
    isIcebergDisabledError,
    isIcebergForbiddenError,
    isIcebergUnauthenticatedError,
    namespaceLevelsBelowRef,
    namespaceOf,
    namespaceParam,
    namespacePathLevels,
    refNamespace,
    snapshotHistory,
    validateCreateTableForm,
    validateNamespacePath,
} from './iceberg';

const jsonResponse = (status: number, body: unknown) => ({
    status,
    ok: status < 400,
    statusText: '',
    headers: {
        get: (name: string) => (name.toLowerCase() === 'content-type' ? 'application/json' : null),
    },
    json: async () => body,
    text: async () => JSON.stringify(body),
});

const textResponse = (status: number) => ({
    status,
    ok: status < 400,
    statusText: 'Bad Gateway',
    headers: { get: () => 'text/html' },
    json: async () => {
        throw new Error('not json');
    },
    text: async () => '<html>bad gateway</html>',
});

const catalogConfig = (prefix = 'lakefs') =>
    jsonResponse(200, { defaults: {}, overrides: { prefix }, endpoints: [`GET /v1/{prefix}/namespaces`] });

const loadTableResult = (metadata = {}) =>
    jsonResponse(200, {
        'metadata-location': 'lakefs://repo/main/_lakefs_iceberg/meta/v1.json',
        metadata: {
            'format-version': 2,
            'table-uuid': 'a-uuid',
            location: 'lakefs://repo/main/_lakefs_iceberg/',
            ...metadata,
        },
        config: {},
    });

const urlOf = (call: unknown[]): string => call[0] as string;
const queryOf = (url: string): URLSearchParams => new URLSearchParams(url.split('?')[1] ?? '');

describe('iceberg client error mapping', () => {
    let fetchMock: ReturnType<typeof vi.fn>;

    beforeEach(() => {
        fetchMock = vi.fn();
        vi.stubGlobal('fetch', fetchMock);
        iceberg.clearCache();
    });

    afterEach(() => {
        vi.unstubAllGlobals();
        iceberg.clearCache();
    });

    it('maps the catalog envelope to an IcebergError with its exception type', async () => {
        fetchMock.mockResolvedValue(
            jsonResponse(403, {
                error: { message: 'user u insufficient permissions', type: 'ForbiddenException', code: 403 },
            }),
        );

        const error = await iceberg.getConfig().catch((e) => e);

        expect(error).toBeInstanceOf(IcebergError);
        expect(error.status).toBe(403);
        expect(error.type).toBe('ForbiddenException');
        expect(error.message).toBe('user u insufficient permissions');
        expect(isIcebergForbiddenError(error)).toBe(true);
        // A denial is about the permission, not about the session.
        expect(isIcebergUnauthenticatedError(error)).toBe(false);
        expect(isIcebergDisabledError(error)).toBe(false);
    });

    it('treats a 501 as "the catalog is disabled", which keeps the Enterprise upsell', async () => {
        fetchMock.mockResolvedValue(
            jsonResponse(501, {
                error: {
                    message: 'the Apache Iceberg catalog is disabled on this server',
                    type: 'NotImplementedException',
                },
            }),
        );

        const error = await iceberg.getConfig().catch((e) => e);

        expect(isIcebergDisabledError(error)).toBe(true);
        expect(isIcebergForbiddenError(error)).toBe(false);
    });

    it('does not treat a 500 as disabled', async () => {
        fetchMock.mockResolvedValue(jsonResponse(500, { error: { message: 'boom', type: 'InternalServerError' } }));

        const error = await iceberg.getConfig().catch((e) => e);

        expect(isIcebergDisabledError(error)).toBe(false);
        expect(isIcebergForbiddenError(error)).toBe(false);
    });

    it('keeps a 401 as an IcebergError, so useAPI() never redirects to the login page', async () => {
        fetchMock.mockResolvedValue(
            jsonResponse(401, { error: { message: 'authentication required', type: 'NotAuthorizedException' } }),
        );

        const error = await iceberg.loadTable(['repo', 'main'], 'books').catch((e) => e);

        // The catalog's 401 says nothing about the core lakeFS session (pkg/icebergcatalog/auth.go),
        // so it stays the catalog's own error: the Tables tab shows it in place.
        expect(error).toBeInstanceOf(IcebergError);
        expect(error).not.toBeInstanceOf(AuthenticationError);
        expect(error.status).toBe(401);
        expect(error.message).toBe('authentication required');
        expect(isIcebergUnauthenticatedError(error)).toBe(true);
        expect(isIcebergForbiddenError(error)).toBe(false);
        expect(isIcebergDisabledError(error)).toBe(false);
    });

    it('falls back to the status line when the body is not the catalog envelope', async () => {
        fetchMock.mockResolvedValue(textResponse(502));

        const error = await iceberg.getConfig().catch((e) => e);

        expect(error).toBeInstanceOf(IcebergError);
        expect(error.status).toBe(502);
        expect(error.message).toBe('Bad Gateway');
    });

    it('reads the core {"message"} body of the placeholder a plain build serves', async () => {
        fetchMock.mockResolvedValue(jsonResponse(501, { message: 'not implemented' }));

        const error = await iceberg.getConfig().catch((e) => e);

        expect(error).toBeInstanceOf(IcebergError);
        expect(error.message).toBe('not implemented');
        expect(isIcebergDisabledError(error)).toBe(true);
    });
});

describe('iceberg client requests', () => {
    let fetchMock: ReturnType<typeof vi.fn>;

    beforeEach(() => {
        fetchMock = vi.fn();
        vi.stubGlobal('fetch', fetchMock);
        iceberg.clearCache();
    });

    afterEach(() => {
        vi.unstubAllGlobals();
        iceberg.clearCache();
    });

    it('reads the catalog config at its own mount, not under /api/v1', async () => {
        fetchMock.mockResolvedValue(catalogConfig());

        const config = await iceberg.getConfig();

        expect(urlOf(fetchMock.mock.calls[0])).toBe('/iceberg/api/v1/config');
        expect(config.overrides).toEqual({ prefix: 'lakefs' });
        // The UI authenticates by cookie; the catalog accepts it for reads.
        expect(fetchMock.mock.calls[0][1].credentials).toBe('same-origin');
    });

    it('lists the child namespaces of a parent with the %1F separator in the query', async () => {
        fetchMock
            .mockResolvedValueOnce(catalogConfig())
            .mockResolvedValueOnce(jsonResponse(200, { namespaces: [['repo', 'main', 'inventory']] }));

        const response = await iceberg.listNamespaces({ parent: refNamespace('repo', 'main') });

        expect(response.namespaces).toEqual([['repo', 'main', 'inventory']]);
        const url = urlOf(fetchMock.mock.calls[1]);
        expect(url.startsWith('/iceberg/api/v1/lakefs/namespaces?')).toBe(true);
        expect(queryOf(url).get('parent')).toBe('repo\u001fmain');
    });

    it('keeps the namespace levels of a nested parent and pages with pageToken/pageSize', async () => {
        fetchMock.mockResolvedValueOnce(catalogConfig()).mockResolvedValueOnce(jsonResponse(200, { namespaces: [] }));

        await iceberg.listNamespaces({
            parent: ['repo', 'main', 'inventory', 'books'],
            pageToken: 'next',
            pageSize: 50,
        });

        const query = queryOf(urlOf(fetchMock.mock.calls[1]));
        expect(query.get('parent')).toBe('repo\u001fmain\u001finventory\u001fbooks');
        expect(query.get('pageToken')).toBe('next');
        expect(query.get('pageSize')).toBe('50');
    });

    it('lists tables of a namespace addressed as repo%1Fref%1Fns', async () => {
        fetchMock
            .mockResolvedValueOnce(catalogConfig())
            .mockResolvedValueOnce(
                jsonResponse(200, { identifiers: [{ namespace: ['repo', 'main', 'inv'], name: 'books' }] }),
            );

        const response = await iceberg.listTables(['repo', 'main', 'inv']);

        expect(response.identifiers).toEqual([{ namespace: ['repo', 'main', 'inv'], name: 'books' }]);
        expect(urlOf(fetchMock.mock.calls[1])).toBe('/iceberg/api/v1/lakefs/namespaces/repo%1Fmain%1Finv/tables');
    });

    it('loads one table, encoding the table name in the path', async () => {
        fetchMock.mockResolvedValueOnce(catalogConfig()).mockResolvedValueOnce(loadTableResult());

        const result = await iceberg.loadTable(['repo', 'main', 'inv'], 'daily sales');

        expect(result['metadata-location']).toBe('lakefs://repo/main/_lakefs_iceberg/meta/v1.json');
        expect(urlOf(fetchMock.mock.calls[1])).toBe(
            '/iceberg/api/v1/lakefs/namespaces/repo%1Fmain%1Finv/tables/daily%20sales',
        );
    });

    it('advertises the prefix only once, and re-reads it after the cache is cleared', async () => {
        fetchMock.mockResolvedValue(catalogConfig()).mockResolvedValue(jsonResponse(200, { namespaces: [] }));

        await iceberg.listNamespaces({ parent: ['repo', 'main'] });
        await iceberg.listNamespaces({ parent: ['repo', 'main', 'inventory'] });
        expect(fetchMock).toHaveBeenCalledTimes(3); // config once, then the two listings

        iceberg.clearCache();
        await iceberg.listNamespaces({ parent: ['repo', 'main'] });
        expect(fetchMock).toHaveBeenCalledTimes(5); // config again, then the listing
    });

    it('does not cache a failed config read', async () => {
        fetchMock
            .mockResolvedValueOnce(
                jsonResponse(501, { error: { message: 'disabled', type: 'NotImplementedException' } }),
            )
            .mockResolvedValueOnce(catalogConfig())
            .mockResolvedValueOnce(jsonResponse(200, { namespaces: [] }));

        await expect(iceberg.listNamespaces({ parent: ['repo', 'main'] })).rejects.toBeInstanceOf(IcebergError);
        await expect(iceberg.listNamespaces({ parent: ['repo', 'main'] })).resolves.toEqual({ namespaces: [] });
    });
});

describe('iceberg addressing', () => {
    it('builds the ref namespace and appends the real namespace levels', () => {
        expect(refNamespace('repo', 'main')).toEqual(['repo', 'main']);
        expect(namespaceOf('repo', 'main')).toEqual(['repo', 'main']);
        expect(namespaceOf('repo', 'main', ['inventory', 'books'])).toEqual(['repo', 'main', 'inventory', 'books']);
    });

    it('joins levels with %1F and encodes each one', () => {
        expect(namespaceParam(['repo', 'main'])).toBe('repo%1Fmain');
        expect(namespaceParam(['repo', 'my branch', 'ns.with.dots'])).toBe('repo%1Fmy%20branch%1Fns.with.dots');
    });

    it('drops exactly the repository and ref a listing echoes back', () => {
        expect(namespaceLevelsBelowRef('repo', 'main', ['repo', 'main', 'inventory'])).toEqual(['inventory']);
        expect(namespaceLevelsBelowRef('repo', 'main', ['repo', 'main'])).toEqual([]);
        // A namespace that merely starts with the same word is not the ref, and is kept whole.
        expect(namespaceLevelsBelowRef('repo', 'main', ['repo', 'mainline', 'inventory'])).toEqual([
            'repo',
            'mainline',
            'inventory',
        ]);
    });
});

describe('iceberg metadata helpers', () => {
    const schemas = [
        { type: 'struct', 'schema-id': 0, fields: [{ id: 1, name: 'x', required: true, type: 'long' }] },
        {
            type: 'struct',
            'schema-id': 1,
            fields: [
                { id: 1, name: 'id', required: true, type: 'long' },
                { id: 2, name: 'name', required: false, type: 'string' },
            ],
        },
    ];

    it('reads the schema current-schema-id names, falling back to the newest one', () => {
        expect(currentSchema({ 'current-schema-id': 0, schemas })?.fields?.[0].name).toBe('x');
        expect(currentSchema({ 'current-schema-id': 7, schemas })?.fields?.[0].name).toBe('id');
        // Format v1 carries a single `schema` member instead of `schemas`.
        expect(currentSchema({ schema: schemas[0] })?.fields?.[0].name).toBe('x');
        expect(currentSchema(undefined)).toBeUndefined();
    });

    it('reads the v2 default partition spec and the v1 partition-spec list', () => {
        const specs = [
            { 'spec-id': 0, fields: [] },
            { 'spec-id': 1, fields: [{ name: 'day', transform: 'day', 'source-id': 1, 'field-id': 1000 }] },
        ];

        expect(currentPartitionSpec({ 'default-spec-id': 1, 'partition-specs': specs })).toEqual({
            id: 1,
            fields: [{ name: 'day', transform: 'day', 'source-id': 1, 'field-id': 1000 }],
        });
        expect(currentPartitionSpec({ 'partition-specs': specs }).id).toBe(0);
        expect(
            currentPartitionSpec({
                'partition-spec': [{ name: 'id_bucket', transform: 'bucket[4]', 'source-id': 1, 'field-id': 1000 }],
            }),
        ).toEqual({ fields: [{ name: 'id_bucket', transform: 'bucket[4]', 'source-id': 1, 'field-id': 1000 }] });
        expect(currentPartitionSpec({ 'partition-specs': [] })).toEqual({ fields: [] });
    });

    it('finds the current snapshot and orders the history newest first', () => {
        const snapshots = [
            { 'snapshot-id': 1, 'timestamp-ms': 1000, summary: { operation: 'append' } },
            { 'snapshot-id': 3, 'timestamp-ms': 3000, summary: { operation: 'overwrite' } },
            { 'snapshot-id': 2, 'timestamp-ms': 2000, summary: { operation: 'delete' } },
        ];

        const metadata = { 'current-snapshot-id': 2, snapshots };
        expect(currentSnapshot(metadata)?.['snapshot-id']).toBe(2);
        expect(snapshotHistory(metadata).map((snapshot) => snapshot['snapshot-id'])).toEqual([3, 2, 1]);
        // A table that was never written to has no current snapshot and no history.
        expect(currentSnapshot({ snapshots: [] })).toBeUndefined();
        expect(snapshotHistory(undefined)).toEqual([]);
    });

    it('renders primitive and nested types the way Iceberg clients print them', () => {
        expect(formatIcebergType('long')).toBe('long');
        expect(formatIcebergType(undefined)).toBe('unknown');
        expect(
            formatIcebergType({
                type: 'struct',
                fields: [{ id: 1, name: 'id', required: true, type: 'long' }],
            }),
        ).toBe('struct<1: id: required long>');
        expect(formatIcebergType({ type: 'list', 'element-id': 2, element: 'string' })).toBe('list<2: string>');
        expect(formatIcebergType({ type: 'map', key: 'string', value: 'long' })).toBe('map<string, long>');
    });
});

describe('iceberg client management requests', () => {
    let fetchMock: ReturnType<typeof vi.fn>;

    beforeEach(() => {
        fetchMock = vi.fn();
        vi.stubGlobal('fetch', fetchMock);
        iceberg.clearCache();
    });

    afterEach(() => {
        vi.unstubAllGlobals();
        iceberg.clearCache();
    });

    const initOf = (call: unknown[]): RequestInit => call[1] as RequestInit;
    const bodyOf = (call: unknown[]): Record<string, unknown> => JSON.parse(String(initOf(call).body));

    it('creates a namespace with a POST to the collection, carrying the UI client header', async () => {
        fetchMock
            .mockResolvedValueOnce(catalogConfig())
            .mockResolvedValueOnce(jsonResponse(200, { namespace: ['repo', 'main', 'inventory'], properties: {} }));

        const response = await iceberg.createNamespace(['repo', 'main', 'inventory']);

        expect(response.namespace).toEqual(['repo', 'main', 'inventory']);
        const call = fetchMock.mock.calls[1];
        expect(urlOf(call)).toBe('/iceberg/api/v1/lakefs/namespaces');
        expect(initOf(call).method).toBe('POST');
        expect(bodyOf(call)).toEqual({ namespace: ['repo', 'main', 'inventory'], properties: {} });
        // The catalog accepts the session cookie for this write only when the request says it is
        // the web UI (pkg/icebergcatalog/auth.go).
        expect((initOf(call).headers as Headers).get('X-Lakefs-Client')).toMatch(/^lakefs-webui/);
        expect(initOf(call).credentials).toBe('same-origin');
    });

    it('keeps the namespace properties a caller sets', async () => {
        fetchMock
            .mockResolvedValueOnce(catalogConfig())
            .mockResolvedValueOnce(
                jsonResponse(200, { namespace: ['repo', 'main', 'inv'], properties: { owner: 'a' } }),
            );

        await iceberg.createNamespace(['repo', 'main', 'inv'], { owner: 'a' });

        expect(bodyOf(fetchMock.mock.calls[1]).properties).toEqual({ owner: 'a' });
    });

    it('drops a namespace with a DELETE on its %1F address', async () => {
        fetchMock.mockResolvedValueOnce(catalogConfig()).mockResolvedValueOnce({ status: 204, ok: true });

        await iceberg.dropNamespace(['repo', 'main', 'inventory', 'books']);

        const call = fetchMock.mock.calls[1];
        expect(urlOf(call)).toBe('/iceberg/api/v1/lakefs/namespaces/repo%1Fmain%1Finventory%1Fbooks');
        expect(initOf(call).method).toBe('DELETE');
        expect((initOf(call).headers as Headers).get('X-Lakefs-Client')).toMatch(/^lakefs-webui/);
    });

    it('creates a table from the form: a schema with client-assigned ids and an identity partition', async () => {
        fetchMock.mockResolvedValueOnce(catalogConfig()).mockResolvedValueOnce(loadTableResult());

        await iceberg.createTable(['repo', 'main', 'inventory'], {
            name: 'books',
            columns: [
                { name: 'id', type: 'long', required: true },
                { name: 'title', type: 'string', required: false },
            ],
            partitionBy: 'title',
        });

        const call = fetchMock.mock.calls[1];
        expect(urlOf(call)).toBe('/iceberg/api/v1/lakefs/namespaces/repo%1Fmain%1Finventory/tables');
        expect(initOf(call).method).toBe('POST');
        expect(bodyOf(call)).toEqual({
            name: 'books',
            schema: {
                type: 'struct',
                'schema-id': 0,
                fields: [
                    { id: 1, name: 'id', required: true, type: 'long' },
                    { id: 2, name: 'title', required: false, type: 'string' },
                ],
            },
            'partition-spec': {
                'spec-id': 0,
                fields: [{ name: 'title', transform: 'identity', 'source-id': 2, 'field-id': 1000 }],
            },
        });
        expect((initOf(call).headers as Headers).get('X-Lakefs-Client')).toMatch(/^lakefs-webui/);
    });

    it('creates an unpartitioned table when the form names no partition column', async () => {
        fetchMock.mockResolvedValueOnce(catalogConfig()).mockResolvedValueOnce(loadTableResult());

        await iceberg.createTable(['repo', 'main', 'inv'], {
            name: 'books',
            columns: [{ name: 'id', type: 'long', required: true }],
            partitionBy: '',
        });

        const body = bodyOf(fetchMock.mock.calls[1]);
        expect(body).not.toHaveProperty('partition-spec');
    });

    it('drops a table with a DELETE on its address, encoding the name', async () => {
        fetchMock.mockResolvedValueOnce(catalogConfig()).mockResolvedValueOnce({ status: 204, ok: true });

        await iceberg.dropTable(['repo', 'main', 'inventory'], 'daily sales');

        const call = fetchMock.mock.calls[1];
        expect(urlOf(call)).toBe('/iceberg/api/v1/lakefs/namespaces/repo%1Fmain%1Finventory/tables/daily%20sales');
        expect(initOf(call).method).toBe('DELETE');
        expect((initOf(call).headers as Headers).get('X-Lakefs-Client')).toMatch(/^lakefs-webui/);
    });

    it('reports a refused management operation as an IcebergError, not an AuthenticationError', async () => {
        fetchMock.mockResolvedValueOnce(catalogConfig()).mockResolvedValueOnce(
            jsonResponse(403, {
                error: { message: 'insufficient permissions', type: 'ForbiddenException', code: 403 },
            }),
        );

        const error = await iceberg.dropTable(['repo', 'main', 'inventory'], 'books').catch((e) => e);

        expect(error).toBeInstanceOf(IcebergError);
        expect(error).not.toBeInstanceOf(AuthenticationError);
        expect(isIcebergForbiddenError(error)).toBe(true);
        expect(error.message).toBe('insufficient permissions');
    });
});

describe('iceberg create-table form', () => {
    it('numbers the schema fields from one in form order', () => {
        expect(
            createTableSchema([
                { name: 'id', type: 'long', required: true },
                { name: 'name', type: 'string', required: false },
            ]),
        ).toEqual({
            type: 'struct',
            'schema-id': 0,
            fields: [
                { id: 1, name: 'id', required: true, type: 'long' },
                { id: 2, name: 'name', required: false, type: 'string' },
            ],
        });
    });

    it('builds the identity partition spec of the column the form chose', () => {
        const columns = [
            { name: 'id', type: 'long', required: true },
            { name: 'title', type: 'string', required: false },
        ];

        expect(createTablePartitionSpec(columns, 'title')).toEqual({
            'spec-id': 0,
            fields: [{ name: 'title', transform: 'identity', 'source-id': 2, 'field-id': 1000 }],
        });
        expect(createTablePartitionSpec(columns, '')).toBeUndefined();
        expect(createTablePartitionSpec(columns, undefined)).toBeUndefined();
        // A partition column that is not a column of the table cannot become a spec.
        expect(createTablePartitionSpec(columns, 'missing')).toBeUndefined();
    });

    it('omits the partition spec of an unpartitioned table and keeps the rest of the request', () => {
        expect(
            createTableRequestBody({
                name: ' books ',
                columns: [{ name: ' id ', type: ' long ', required: true }],
            }),
        ).toEqual({
            name: 'books',
            schema: { type: 'struct', 'schema-id': 0, fields: [{ id: 1, name: 'id', required: true, type: 'long' }] },
        });
    });

    it('accepts a complete form and names what is missing otherwise', () => {
        const valid = {
            name: 'books',
            columns: [{ name: 'id', type: 'long', required: true }],
            partitionBy: 'id',
        };

        expect(validateCreateTableForm(valid)).toBeNull();
        expect(validateCreateTableForm({ ...valid, name: '  ' })).toMatch(/needs a name/);
        expect(validateCreateTableForm({ ...valid, columns: [] })).toMatch(/at least one column/);
        expect(validateCreateTableForm({ ...valid, columns: [{ name: ' ', type: 'long', required: true }] })).toMatch(
            /Every column needs a name/,
        );
        expect(
            validateCreateTableForm({
                ...valid,
                columns: [
                    { name: 'id', type: 'long', required: true },
                    { name: 'id', type: 'string', required: false },
                ],
            }),
        ).toMatch(/defined twice/);
        expect(validateCreateTableForm({ ...valid, columns: [{ name: 'id', type: '  ', required: true }] })).toMatch(
            /Every column needs a type/,
        );
        expect(validateCreateTableForm({ ...valid, partitionBy: 'other' })).toMatch(/partition column/);
    });

    it('splits a typed namespace path into its levels', () => {
        expect(namespacePathLevels('inventory.books')).toEqual(['inventory', 'books']);
        expect(namespacePathLevels(' inventory . books ')).toEqual(['inventory', 'books']);
        expect(namespacePathLevels('inventory..books')).toEqual(['inventory', 'books']);
        expect(namespacePathLevels('')).toEqual([]);
        expect(validateNamespacePath('')).toMatch(/at least one level/);
        expect(validateNamespacePath('inventory')).toBeNull();
    });
});
