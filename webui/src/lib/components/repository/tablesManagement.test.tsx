import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import { iceberg } from '../../api/iceberg';
import { TablesTab } from './tablesExplorer';

// useAPI() reports authentication failures through the auth context; a catalog 401 must never
// reach it (that is the point of the client keeping its own error class), so the spy stays
// uncalled and the tests below assert exactly that - no management failure redirects to the login
// page.
const { onUnauthenticated } = vi.hoisted(() => ({ onUnauthenticated: vi.fn() }));
vi.mock('../../auth/authContext', () => ({
    useAuth: () => ({ onUnauthenticated }),
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

const emptyResponse = (status: number) => ({
    status,
    ok: status < 400,
    statusText: '',
    headers: { get: () => null },
    json: async () => ({}),
    text: async () => '',
});

const catalogEnvelope = (status: number, type: string, message: string) =>
    jsonResponse(status, { error: { message, type, code: status } });

const config = () => jsonResponse(200, { defaults: {}, overrides: { prefix: 'lakefs' }, endpoints: [] });

const namespacesBelow = (url: string) => {
    const parent = new URL(url, 'http://localhost').searchParams.get('parent') ?? '';
    const childrenByParent: Record<string, string[][]> = {
        'repo\u001fmain': [['repo', 'main', 'inventory']],
        'repo\u001fmain\u001finventory': [],
    };
    return jsonResponse(200, { namespaces: childrenByParent[parent] ?? [] });
};

const tablesOf = (names: string[]) =>
    jsonResponse(200, { identifiers: names.map((name) => ({ namespace: ['repo', 'main', 'inventory'], name })) });

const loadedTable = () =>
    jsonResponse(200, {
        'metadata-location': 'lakefs://repo/main/_lakefs_iceberg/inventory/books/metadata/v1.json',
        metadata: {
            'format-version': 2,
            'table-uuid': 'table-uuid-1',
            'current-schema-id': 0,
            schemas: [
                {
                    type: 'struct',
                    'schema-id': 0,
                    fields: [{ id: 1, name: 'book_id', required: true, type: 'long' }],
                },
            ],
            'partition-specs': [{ 'spec-id': 0, fields: [] }],
        },
        config: {},
    });

type Override = (url: string, init?: RequestInit) => unknown;

/**
 * Serves the catalog by URL, so the order the components load things in does not matter. An
 * override that answers `undefined` falls through, which is how a test can answer one URL for a
 * write and leave the reads on that same URL alone.
 */
const serveCatalog = (fetchMock: ReturnType<typeof vi.fn>, overrides: Override[] = []) => {
    fetchMock.mockImplementation(async (url: string, init?: RequestInit) => {
        const method = init?.method ?? 'GET';
        for (const override of overrides) {
            const answer = override(url, init);
            if (answer !== undefined) return answer;
        }
        if (method === 'POST' || method === 'DELETE') return emptyResponse(204);
        if (url.endsWith('/v1/config')) return config();
        if (url.includes('/tables/')) return loadedTable();
        if (url.includes('/tables')) return tablesOf(['books']);
        if (url.includes('/namespaces?')) return namespacesBelow(url);
        return jsonResponse(404, { error: { message: `no mock for ${url}`, type: 'NotFoundException' } });
    });
};

/** An override that answers one write whose URL contains fragment, and nothing else. */
const onWrite =
    (fragment: string, respond: () => unknown): Override =>
    (url, init) =>
        (init?.method ?? 'GET') !== 'GET' && url.includes(fragment) ? respond() : undefined;

/** The write requests the tab made, as [url, init] pairs. */
const writeCalls = (fetchMock: ReturnType<typeof vi.fn>): [string, RequestInit][] =>
    fetchMock.mock.calls.filter(([, init]) => {
        const method = (init as RequestInit | undefined)?.method ?? 'GET';
        return method !== 'GET';
    }) as [string, RequestInit][];

describe('Tables tab management', () => {
    let fetchMock: ReturnType<typeof vi.fn>;

    beforeEach(() => {
        fetchMock = vi.fn();
        vi.stubGlobal('fetch', fetchMock);
        iceberg.clearCache();
        onUnauthenticated.mockClear();
    });

    afterEach(() => {
        vi.unstubAllGlobals();
        iceberg.clearCache();
    });

    it('creates a namespace with the UI client header and reloads the tree', async () => {
        const user = userEvent.setup();
        serveCatalog(fetchMock);

        render(<TablesTab repository="repo" refId="main" />);
        await screen.findByTestId('iceberg-namespace-tree');

        await user.click(screen.getByRole('button', { name: 'Create namespace' }));
        const dialog = screen.getByRole('dialog');
        await user.type(within(dialog).getByLabelText('Namespace'), 'analytics');
        await user.click(within(dialog).getByRole('button', { name: 'Create namespace' }));

        await waitFor(() => expect(writeCalls(fetchMock)).toHaveLength(1));
        const [url, init] = writeCalls(fetchMock)[0];
        expect(url).toBe('/iceberg/api/v1/lakefs/namespaces');
        expect(init.method).toBe('POST');
        expect(JSON.parse(String(init.body))).toEqual({
            namespace: ['repo', 'main', 'analytics'],
            properties: {},
        });
        // The catalog authenticates this write by cookie only for a request that says it is the UI.
        expect((init.headers as Headers).get('X-Lakefs-Client')).toMatch(/^lakefs-webui/);
        // The dialog closes and the tab re-reads the namespaces.
        await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument());
        await waitFor(() =>
            expect(
                fetchMock.mock.calls.some(
                    ([calledUrl, calledInit]) =>
                        (calledInit as RequestInit | undefined)?.method !== 'POST' &&
                        String(calledUrl).includes('/namespaces?parent=repo%1Fmain'),
                ),
            ).toBe(true),
        );
    });

    it('keeps the create-namespace dialog open with the local problem when the form is empty', async () => {
        const user = userEvent.setup();
        serveCatalog(fetchMock);

        render(<TablesTab repository="repo" refId="main" />);
        await screen.findByTestId('iceberg-namespace-tree');

        await user.click(screen.getByRole('button', { name: 'Create namespace' }));
        const dialog = screen.getByRole('dialog');
        await user.click(within(dialog).getByRole('button', { name: 'Create namespace' }));

        expect(within(dialog).getByText(/needs at least one level/)).toBeInTheDocument();
        expect(writeCalls(fetchMock)).toHaveLength(0);
    });

    it('creates a table from the schema editor, with the chosen identity partition', async () => {
        const user = userEvent.setup();
        serveCatalog(fetchMock);

        render(<TablesTab repository="repo" refId="main" />);
        await screen.findByTestId('iceberg-namespace-tree');

        await user.click(screen.getByRole('button', { name: 'Create table in repo.main.inventory' }));
        const dialog = screen.getByRole('dialog');
        await user.type(within(dialog).getByLabelText('Table name'), 'orders');
        await user.type(within(dialog).getByLabelText('Column 1 name'), 'id');
        await user.click(within(dialog).getByLabelText('Required'));
        await user.selectOptions(within(dialog).getByLabelText('Partition by'), 'id');
        await user.click(within(dialog).getByRole('button', { name: 'Create table' }));

        await waitFor(() => expect(writeCalls(fetchMock)).toHaveLength(1));
        const [url, init] = writeCalls(fetchMock)[0];
        expect(url).toBe('/iceberg/api/v1/lakefs/namespaces/repo%1Fmain%1Finventory/tables');
        expect(JSON.parse(String(init.body))).toEqual({
            name: 'orders',
            schema: {
                type: 'struct',
                'schema-id': 0,
                fields: [{ id: 1, name: 'id', required: true, type: 'long' }],
            },
            'partition-spec': {
                'spec-id': 0,
                fields: [{ name: 'id', transform: 'identity', 'source-id': 1, 'field-id': 1000 }],
            },
        });
        await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument());
    });

    it('reports a refused create in the dialog and never redirects to the login page', async () => {
        const user = userEvent.setup();
        serveCatalog(fetchMock, [
            onWrite('/namespaces', () => catalogEnvelope(401, 'NotAuthorizedException', 'authentication required')),
        ]);

        render(<TablesTab repository="repo" refId="main" />);
        await screen.findByTestId('iceberg-namespace-tree');

        await user.click(screen.getByRole('button', { name: 'Create namespace' }));
        const dialog = screen.getByRole('dialog');
        await user.type(within(dialog).getByLabelText('Namespace'), 'analytics');
        await user.click(within(dialog).getByRole('button', { name: 'Create namespace' }));

        expect(await within(dialog).findByText(/not valid for the Iceberg catalog/)).toBeInTheDocument();
        expect(screen.getByRole('dialog')).toBeInTheDocument();
        expect(onUnauthenticated).not.toHaveBeenCalled();
    });

    it('disables management once the catalog denies one operation', async () => {
        const user = userEvent.setup();
        serveCatalog(fetchMock, [
            onWrite('/namespaces', () => catalogEnvelope(403, 'ForbiddenException', 'user u insufficient permissions')),
        ]);

        render(<TablesTab repository="repo" refId="main" />);
        await screen.findByTestId('iceberg-namespace-tree');

        await user.click(screen.getByRole('button', { name: 'Create namespace' }));
        const dialog = screen.getByRole('dialog');
        await user.type(within(dialog).getByLabelText('Namespace'), 'analytics');
        await user.click(within(dialog).getByRole('button', { name: 'Create namespace' }));

        expect(await within(dialog).findByText(/not allowed to create the namespace/)).toBeInTheDocument();
        expect(onUnauthenticated).not.toHaveBeenCalled();

        // Closing the dialog leaves the tab with its management controls disabled, and says why.
        await user.click(within(dialog).getByRole('button', { name: 'Cancel' }));
        expect(await screen.findByTestId('iceberg-management-forbidden')).toBeInTheDocument();
        expect(screen.getByRole('button', { name: 'Create namespace' })).toBeDisabled();
        expect(screen.getByRole('button', { name: 'Create table in repo.main.inventory' })).toBeDisabled();
        expect(screen.getByRole('button', { name: 'Drop table books' })).toBeDisabled();
        expect(screen.getByRole('button', { name: 'Drop namespace repo.main.inventory' })).toBeDisabled();
    });

    it('drops a table after a confirmation, addressing it with the UI client header', async () => {
        const user = userEvent.setup();
        serveCatalog(fetchMock);

        render(<TablesTab repository="repo" refId="main" />);
        await screen.findByTestId('iceberg-namespace-tree');

        await user.click(await screen.findByRole('button', { name: 'Drop table books' }));
        const dialog = screen.getByRole('dialog');
        expect(within(dialog).getByText(/Drop the table/)).toBeInTheDocument();
        await user.click(within(dialog).getByRole('button', { name: 'Yes' }));

        await waitFor(() => expect(writeCalls(fetchMock)).toHaveLength(1));
        const [url, init] = writeCalls(fetchMock)[0];
        expect(url).toBe('/iceberg/api/v1/lakefs/namespaces/repo%1Fmain%1Finventory/tables/books');
        expect(init.method).toBe('DELETE');
        expect((init.headers as Headers).get('X-Lakefs-Client')).toMatch(/^lakefs-webui/);
    });

    it('shows a failed drop as the catalog message in the tab', async () => {
        const user = userEvent.setup();
        serveCatalog(fetchMock, [
            onWrite('/tables/books', () =>
                catalogEnvelope(404, 'NoSuchTableException', 'table inventory.books not found'),
            ),
        ]);

        render(<TablesTab repository="repo" refId="main" />);
        await screen.findByTestId('iceberg-namespace-tree');

        await user.click(await screen.findByRole('button', { name: 'Drop table books' }));
        await user.click(within(screen.getByRole('dialog')).getByRole('button', { name: 'Yes' }));

        const alert = await screen.findByTestId('iceberg-management-error');
        expect(alert).toHaveTextContent('table inventory.books not found');
        expect(onUnauthenticated).not.toHaveBeenCalled();
    });

    it('shows the catalog message when a namespace is not empty', async () => {
        const user = userEvent.setup();
        serveCatalog(fetchMock, [
            onWrite('/namespaces/repo%1Fmain%1Finventory', () =>
                catalogEnvelope(409, 'NamespaceNotEmptyException', 'namespace repo.main.inventory is not empty'),
            ),
        ]);

        render(<TablesTab repository="repo" refId="main" />);
        await screen.findByTestId('iceberg-namespace-tree');

        await user.click(await screen.findByRole('button', { name: 'Drop namespace repo.main.inventory' }));
        const dialog = screen.getByRole('dialog');
        expect(within(dialog).getByText(/Drop the namespace/)).toBeInTheDocument();
        await user.click(within(dialog).getByRole('button', { name: 'Yes' }));

        const alert = await screen.findByTestId('iceberg-management-error');
        expect(alert).toHaveTextContent('namespace repo.main.inventory is not empty');
    });
});
