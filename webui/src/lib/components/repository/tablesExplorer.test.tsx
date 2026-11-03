import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import { iceberg } from '../../api/iceberg';
import { TablesTab } from './tablesExplorer';

// useAPI() reports authentication failures through the auth context; these tests never take that
// path, but the hook reads the context on every render.
vi.mock('../../auth/authContext', () => ({
    useAuth: () => ({ onUnauthenticated: () => {} }),
}));

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

const catalogEnvelope = (status: number, type: string, message: string) =>
    jsonResponse(status, { error: { message, type, code: status } });

const config = () => jsonResponse(200, { defaults: {}, overrides: { prefix: 'lakefs' }, endpoints: [] });

const namespacesOf = (namespaces: string[][]) => jsonResponse(200, { namespaces });

/**
 * The namespaces the catalog reports below the `parent` the client asked for. The listing is keyed
 * by that query (`repo%1Fmain`, i.e. the levels joined by the separator), because a catalog answers
 * with the *children* of the parent and never with the parent itself: `inventory` is a leaf, so the
 * listing below it is empty. Answering every parent with the ref's child namespace instead would
 * echo `inventory` back as its own child and make the namespace tree render itself recursively,
 * one level per request round trip - which is uncontrolled async work inside the test.
 */
const namespacesBelow = (url: string) => {
    const parent = new URL(url, 'http://localhost').searchParams.get('parent') ?? '';
    const childrenByParent: Record<string, string[][]> = {
        'repo\u001fmain': [['repo', 'main', 'inventory']],
        'repo\u001fmain\u001finventory': [],
    };
    return namespacesOf(childrenByParent[parent] ?? []);
};

const tablesOf = (names: string[]) =>
    jsonResponse(200, { identifiers: names.map((name) => ({ namespace: ['repo', 'main', 'inventory'], name })) });

const loadedTable = () =>
    jsonResponse(200, {
        'metadata-location': 'lakefs://repo/main/_lakefs_iceberg/inventory/books/metadata/v1.json',
        metadata: {
            'format-version': 2,
            'table-uuid': 'table-uuid-1',
            location: 'lakefs://repo/main/_lakefs_iceberg/inventory/books/',
            'current-schema-id': 0,
            schemas: [
                {
                    type: 'struct',
                    'schema-id': 0,
                    fields: [
                        { id: 1, name: 'book_id', required: true, type: 'long' },
                        { id: 2, name: 'title', required: false, type: 'string' },
                    ],
                },
            ],
            'default-spec-id': 0,
            'partition-specs': [
                {
                    'spec-id': 0,
                    fields: [{ name: 'title_bucket', transform: 'bucket[4]', 'source-id': 2, 'field-id': 1000 }],
                },
            ],
            properties: { 'write.format.default': 'parquet' },
            'current-snapshot-id': 10,
            snapshots: [
                {
                    'snapshot-id': 10,
                    'timestamp-ms': 1700000000000,
                    'sequence-number': 1,
                    summary: { operation: 'append', 'added-records': '2' },
                    'manifest-list': 'lakefs://repo/main/_lakefs_iceberg/inventory/books/metadata/snap-10.avro',
                },
            ],
        },
        config: {},
    });

/** Serves the catalog by URL, so the order the components load things in does not matter. */
const serveCatalog = (fetchMock: ReturnType<typeof vi.fn>, overrides: Record<string, () => unknown> = {}) => {
    fetchMock.mockImplementation(async (url: string) => {
        for (const [fragment, respond] of Object.entries(overrides)) {
            if (url.includes(fragment)) return respond();
        }
        if (url.endsWith('/v1/config')) return config();
        if (url.includes('/tables/')) return loadedTable();
        if (url.includes('/tables')) return tablesOf(['books']);
        if (url.includes('/namespaces?')) return namespacesBelow(url);
        return jsonResponse(404, { error: { message: `no mock for ${url}`, type: 'NotFoundException' } });
    });
};

describe('TablesTab', () => {
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

    it('keeps the Enterprise upsell when the catalog answers 501 (iceberg.enabled=false)', async () => {
        fetchMock.mockResolvedValue(
            catalogEnvelope(501, 'NotImplementedException', 'the Apache Iceberg catalog is disabled on this server'),
        );

        render(<TablesTab repository="repo" refId="main" />);

        expect(await screen.findByText('Tables is an Enterprise feature')).toBeInTheDocument();
        expect(screen.queryByTestId('iceberg-namespace-tree')).not.toBeInTheDocument();
    });

    it('says no session on a 401 authentication answer instead of an upsell', async () => {
        // The API client turns this 401 into AuthenticationError, which useAPI answers with the
        // login redirect; the tab itself must not claim Enterprise.
        fetchMock.mockResolvedValue(catalogEnvelope(401, 'NotAuthorizedException', 'authentication required'));

        render(<TablesTab repository="repo" refId="main" />);

        expect(await screen.findByText('authentication required')).toBeInTheDocument();
        expect(screen.queryByText('Tables is an Enterprise feature')).not.toBeInTheDocument();
    });

    it('shows the permission state on a 403 from the catalog', async () => {
        fetchMock.mockResolvedValue(
            catalogEnvelope(403, 'ForbiddenException', 'the credentials do not permit this operation'),
        );

        render(<TablesTab repository="repo" refId="main" />);

        expect(await screen.findByText('Permission required')).toBeInTheDocument();
        expect(screen.getByText('catalog:ListTables')).toBeInTheDocument();
    });

    it('shows the empty state when the ref has no namespaces and therefore no tables', async () => {
        serveCatalog(fetchMock, {
            '/namespaces?': () => namespacesOf([]),
        });

        render(<TablesTab repository="repo" refId="main" />);

        expect(await screen.findByText('No Iceberg tables')).toBeInTheDocument();
        expect(screen.getByText(/has no Iceberg tables yet/)).toBeInTheDocument();
    });

    it('lists namespaces and tables, and shows the details of the selected table', async () => {
        const user = userEvent.setup();
        serveCatalog(fetchMock);

        render(<TablesTab repository="repo" refId="main" />);

        // The top-level namespace is open, so its tables are visible without a click.
        const tree = await screen.findByTestId('iceberg-namespace-tree');
        expect(await within(tree).findByText('inventory')).toBeInTheDocument();
        // One namespace node: `inventory` is a leaf, so it must not list itself as a child.
        expect(within(tree).getAllByText('inventory')).toHaveLength(1);
        const table = await screen.findByRole('button', { name: /books/ });

        await user.click(table);

        expect(await screen.findByTestId('iceberg-table-details')).toBeInTheDocument();
        // Metadata location, schema, partition spec, snapshot and properties are all shown.
        expect(
            screen.getByText('lakefs://repo/main/_lakefs_iceberg/inventory/books/metadata/v1.json'),
        ).toBeInTheDocument();
        expect(screen.getByText('book_id')).toBeInTheDocument();
        expect(screen.getByText('bucket[4]')).toBeInTheDocument();
        expect(screen.getByText('parquet')).toBeInTheDocument();
        expect(screen.getByText('current')).toBeInTheDocument();
        expect(screen.getByText('format v2')).toBeInTheDocument();
        // The table is loaded through the catalog, addressed as repo%1Fref%1Fns.
        expect(
            fetchMock.mock.calls.some(([url]) =>
                String(url).includes('/namespaces/repo%1Fmain%1Finventory/tables/books'),
            ),
        ).toBe(true);
    });

    it('shows the permission state when the namespace listing is denied', async () => {
        serveCatalog(fetchMock, {
            '/namespaces?': () => catalogEnvelope(403, 'ForbiddenException', 'not allowed'),
        });

        render(<TablesTab repository="repo" refId="main" />);

        expect(await screen.findByText('Permission required')).toBeInTheDocument();
    });
});
