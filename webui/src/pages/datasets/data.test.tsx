import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Route, Routes } from 'react-router-dom';

import { DatasetDefinition, Version } from '../../lib/api/datasets';
import DatasetDataPage from './data';

// useAPI() reports authentication failures through the auth context.
vi.mock('../../lib/auth/authContext', () => ({
    useAuth: () => ({ onUnauthenticated: () => {} }),
}));

// The object viewer pulls in DuckDB's wasm build, which the test transformer cannot load. The
// data page only needs it for single-object paths, which these tests do not render.
vi.mock('../repositories/repository/objectViewer', () => ({
    FileContents: () => <div data-testid="file-contents" />,
    getFileExtension: (path: string) => path.split('.').pop() ?? '',
    getContentType: () => undefined,
}));

// The pages read the storage configuration from a provider that lives in the app shell.
vi.mock('../../lib/hooks/configProvider', () => ({
    useConfigContext: () => ({
        config: {
            storages: [
                {
                    blockstore_type: 'local',
                    blockstore_id: 'local',
                    import_support: true,
                    pre_sign_support: true,
                    pre_sign_support_ui: false,
                },
            ],
        },
        loading: false,
        error: null,
    }),
}));

const jsonResponse = (body: unknown, status = 200) => ({
    status,
    ok: status < 400,
    statusText: '',
    headers: {
        get: (name: string) => (name.toLowerCase() === 'content-type' ? 'application/json' : null),
    },
    json: async () => body,
    text: async () => JSON.stringify(body),
});

const definition = (targets: string[]): DatasetDefinition => ({
    format_version: 1,
    data: targets.map((target) => ({
        target,
        repository: 'lakehouse-repo',
        ref: { type: 'commit' as const, id: 'a'.repeat(40) },
        type: 'prefix' as const,
        prefix: { path: `${target}/` },
    })),
});

const version = (number: number, targets: string[]): Version => ({
    version: number,
    message: `message for v${number}`,
    publisher: 'publisher',
    published_at: Date.UTC(2026, 0, number),
    commit_id: `${number}`.repeat(40),
    definition: definition(targets),
    stats: { objects: 10, bytes: 1024, items: [] },
});

const versions = [version(2, ['customers']), version(1, ['customers'])];

const dataset = {
    name: 'customer-360',
    created_at: Date.UTC(2026, 0, 1),
    created_by: 'creator',
    latest_version: 2,
    state: 'active',
    definition: versions[0].definition,
    stats: versions[0].stats,
};

const repository = {
    id: 'customer-360',
    default_branch: 'latest',
    read_only: true,
    storage_id: 'local',
    storage_namespace: 'local://customer-360',
    creation_date: 0,
};

/** One page of the object listing of the dataset repository. */
const entries = {
    results: [
        { path: 'customers/', path_type: 'common_prefix' },
        { path: 'README.md', path_type: 'object', size_bytes: 512, mtime: 1_700_000_000 },
    ],
    pagination: { has_more: false, next_offset: '', results: 2, max_per_page: 1000 },
};

describe('DatasetDataPage', () => {
    let fetchMock: ReturnType<typeof vi.fn>;
    const fetchedUrls = () => fetchMock.mock.calls.map((call) => String(call[0]));

    const renderPage = (url = '/datasets/customer-360/data') =>
        render(
            <MemoryRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }} initialEntries={[url]}>
                <Routes>
                    <Route path="/datasets/:datasetId/data" element={<DatasetDataPage />} />
                </Routes>
            </MemoryRouter>,
        );

    beforeEach(() => {
        fetchMock = vi.fn(async (url: unknown) => {
            const path = String(url);
            if (path.includes('/objects/ls')) {
                // Inside a prefix the listing returns that prefix's children.
                if (path.includes('prefix=customers')) {
                    return jsonResponse({
                        results: [
                            {
                                path: 'customers/part-0.parquet',
                                path_type: 'object',
                                size_bytes: 2048,
                                mtime: 1_700_000_000,
                            },
                        ],
                        pagination: { has_more: false, next_offset: '', results: 1, max_per_page: 1000 },
                    });
                }
                return jsonResponse(entries);
            }
            if (path.includes('/datasets/customer-360/versions')) {
                return jsonResponse({ results: versions, pagination: {} });
            }
            if (path.includes('/datasets/customer-360')) return jsonResponse(dataset);
            if (path.includes('/repositories/customer-360')) return jsonResponse(repository);
            return jsonResponse({ message: 'not found' }, 404);
        });
        vi.stubGlobal('fetch', fetchMock);
    });

    afterEach(() => {
        vi.unstubAllGlobals();
    });

    it('browses the dataset repository at the newest version tag', async () => {
        renderPage();

        expect(await screen.findByText('customers/')).toBeInTheDocument();
        await waitFor(() => expect(fetchedUrls().some((url) => url.includes('/refs/v2/objects/ls'))).toBe(true));
    });

    it('has no upload, import, commit or delete control anywhere on the page', async () => {
        renderPage();
        await screen.findByText('customers/');

        for (const name of [/upload/i, /import/i, /commit/i, /revert/i, /delete/i]) {
            expect(screen.queryByRole('button', { name })).not.toBeInTheDocument();
        }
        // The row actions of the tree are read-only too (no "Delete" entry in the gear menu).
        expect(screen.queryByText(/Delete/)).not.toBeInTheDocument();
        expect(screen.queryByText(/Upload Files/)).not.toBeInTheDocument();
    });

    it('links a folder into the dataset data browser, not the repository pages', async () => {
        renderPage();

        const folder = await screen.findByRole('link', { name: 'customers/' });
        expect(folder).toHaveAttribute('href', '/datasets/customer-360/data?path=customers%2F');
    });

    it('follows the version picked in the header', async () => {
        const user = userEvent.setup();
        renderPage();
        await screen.findByText('customers/');

        await user.selectOptions(screen.getByLabelText('Version'), 'v1');

        await waitFor(() => expect(fetchedUrls().some((url) => url.includes('/refs/v1/objects/ls'))).toBe(true));
        expect(screen.getByLabelText('Version')).toHaveValue('v1');
    });

    it('starts on the version named in the URL', async () => {
        renderPage('/datasets/customer-360/data?version=v1');

        await waitFor(() => expect(fetchedUrls().some((url) => url.includes('/refs/v1/objects/ls'))).toBe(true));
        expect(screen.getByLabelText('Version')).toHaveValue('v1');
    });

    it('lists the prefix named in the URL', async () => {
        renderPage('/datasets/customer-360/data?path=customers%2F');

        await screen.findByText('part-0.parquet');
        await waitFor(() =>
            expect(
                fetchedUrls().some((url) => url.includes('/refs/v2/objects/ls') && url.includes('prefix=customers')),
            ).toBe(true),
        );
    });

    it('offers only the read-only actions of a row', async () => {
        const user = userEvent.setup();
        renderPage();
        await screen.findByText('customers/');

        const row = screen.getByText('README.md').closest('tr') as HTMLElement;
        await user.click(within(row).getByRole('button'));

        expect(await screen.findByText('Download')).toBeInTheDocument();
        expect(screen.getByText('Object Info')).toBeInTheDocument();
        expect(screen.getByText('Copy URI')).toBeInTheDocument();
        expect(screen.queryByText('Delete')).not.toBeInTheDocument();
    });

    it('says so when the dataset has no versions', async () => {
        fetchMock.mockResolvedValueOnce(jsonResponse({ ...dataset, latest_version: 0, definition: undefined }));
        fetchMock.mockResolvedValueOnce(jsonResponse({ results: [], pagination: {} }));
        renderPage();

        expect(await screen.findByText(/no data to browse/i)).toBeInTheDocument();
        expect(fetchedUrls().some((url) => url.includes('/objects/ls'))).toBe(false);
    });

    it('browses a proposal preview branch through ?ref=', async () => {
        // A proposal's preview branch is `_proposal-<id>` (§5.6); `?ref=` browses it directly.
        renderPage('/datasets/customer-360/data?ref=_proposal-p1');

        expect(await screen.findByText('customers/')).toBeInTheDocument();
        await waitFor(() =>
            expect(fetchedUrls().some((url) => url.includes('/refs/_proposal-p1/objects/ls'))).toBe(true),
        );
        expect(screen.getAllByText(/_proposal-p1/).length).toBeGreaterThan(0);
        // A branch would offer the tree's delete action; the preview keeps every write control off.
        expect(screen.queryByText('Delete')).not.toBeInTheDocument();
        // The preview is left by a dedicated control, since `latest` is already selected.
        expect(screen.getByRole('button', { name: /Back to published data/i })).toBeInTheDocument();
    });
});
