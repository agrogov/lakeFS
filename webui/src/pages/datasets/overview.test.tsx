import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router-dom';

import { DatasetDefinition, Version } from '../../lib/api/datasets';
import DatasetOverviewPage from './overview';

// useAPI() reports authentication failures through the auth context.
vi.mock('../../lib/auth/authContext', () => ({
    useAuth: () => ({ onUnauthenticated: () => {} }),
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

const COMMIT = 'a'.repeat(40);

const definition = (targets: string[]): DatasetDefinition => ({
    format_version: 1,
    description: 'Curated customer view for analytics',
    metadata: { tier: 'gold', owner: 'data-platform', regions: ['emea', 'us'] },
    data: targets.map((target) => ({
        target,
        repository: 'lakehouse-repo',
        ref: { type: 'commit' as const, id: COMMIT },
        type: 'prefix' as const,
        prefix: { path: `${target}/` },
    })),
});

const version = (number: number, targets: string[]): Version => ({
    version: number,
    message: `message for v${number}`,
    publisher: `publisher-${number}`,
    published_at: Date.UTC(2026, 0, number),
    commit_id: `${number}`.repeat(40),
    definition: definition(targets),
    stats: {
        objects: 1200 * number,
        bytes: 5 * 1024 * 1024,
        items: targets.map((target) => ({ target, objects: 100 * number, bytes: 1024 * number })),
    },
});

const versions = [version(2, ['customers']), version(1, ['customers', 'orders'])];

const dataset = {
    name: 'customer-360',
    description: 'Curated customer view for analytics',
    created_at: Date.UTC(2026, 0, 1),
    created_by: 'creator',
    latest_version: 2,
    state: 'active',
    definition: versions[0].definition,
    stats: versions[0].stats,
};

describe('DatasetOverviewPage', () => {
    let fetchMock: ReturnType<typeof vi.fn>;
    const writeText = vi.fn(async () => {});

    const renderPage = (url = '/datasets/customer-360/overview') =>
        render(
            <MemoryRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }} initialEntries={[url]}>
                <Routes>
                    <Route path="/datasets/:datasetId/overview" element={<DatasetOverviewPage />} />
                </Routes>
            </MemoryRouter>,
        );

    beforeEach(() => {
        writeText.mockClear();
        Object.defineProperty(navigator, 'clipboard', {
            value: { writeText },
            configurable: true,
            writable: true,
        });
        fetchMock = vi.fn(async (url: unknown) => {
            const path = String(url);
            if (path.includes('/datasets/customer-360/versions')) {
                return jsonResponse({ results: versions, pagination: {} });
            }
            if (path.includes('/datasets/customer-360')) return jsonResponse(dataset);
            return jsonResponse({ message: 'not found' }, 404);
        });
        vi.stubGlobal('fetch', fetchMock);
    });

    afterEach(() => {
        vi.unstubAllGlobals();
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        delete (navigator as any).clipboard;
    });

    it('shows the name, the state, the description and the version selector', async () => {
        renderPage();

        expect(await screen.findByRole('heading', { name: 'customer-360' })).toBeInTheDocument();
        expect(screen.getByText('Curated customer view for analytics')).toBeInTheDocument();
        const picker = screen.getByLabelText('Version');
        expect(picker).toHaveValue('latest');
        expect(within(picker).getByRole('option', { name: 'latest (v2)' })).toBeInTheDocument();
        expect(within(picker).getByRole('option', { name: 'v2' })).toBeInTheDocument();
        expect(screen.getByText(/published by/)).toBeInTheDocument();
    });

    it('shows the statistics of the selected version', async () => {
        renderPage();

        await screen.findByRole('heading', { name: 'customer-360' });
        expect(screen.getByText('2,400')).toBeInTheDocument();
        expect(screen.getByText('5.0 MB')).toBeInTheDocument();
    });

    it('lists the items with a link to each source object', async () => {
        renderPage();

        const target = await screen.findByText('customers');
        const row = target.closest('tr') as HTMLElement;
        const source = within(row).getByRole('link');
        // §11.2: /repositories/<repo>/objects?ref=<commit>&path=<path>
        expect(source).toHaveAttribute('href', `/repositories/lakehouse-repo/objects?ref=${COMMIT}&path=customers%2F`);
        // The per-item stats of v2: 100 objects and 1 KB per version number.
        expect(within(row).getByText('200')).toBeInTheDocument();
        expect(within(row).getByText('2.0 KB')).toBeInTheDocument();
    });

    it('shows the metadata of the version as a table', async () => {
        renderPage();

        await screen.findByText('Metadata');
        expect(screen.getByRole('rowheader', { name: 'tier' })).toBeInTheDocument();
        expect(screen.getByText('gold')).toBeInTheDocument();
        expect(screen.getByRole('rowheader', { name: 'owner' })).toBeInTheDocument();
        expect(screen.getByText('data-platform')).toBeInTheDocument();
        // Multi-value metadata renders one badge per value.
        expect(screen.getByText('emea')).toBeInTheDocument();
        expect(screen.getByText('us')).toBeInTheDocument();
    });

    it('copies the lakefs:// and s3:// URIs of the selected version', async () => {
        renderPage();
        await screen.findByRole('heading', { name: 'customer-360' });

        fireEvent.click(screen.getByRole('button', { name: 'Copy lakefs URI' }));
        await waitFor(() => expect(writeText).toHaveBeenCalledWith('lakefs://customer-360/latest'));

        fireEvent.click(screen.getByRole('button', { name: 'Copy s3 URI' }));
        await waitFor(() => expect(writeText).toHaveBeenCalledWith('s3://customer-360/latest'));
    });

    it('shows an older version picked from the URL', async () => {
        renderPage('/datasets/customer-360/overview?version=v1');

        expect(await screen.findByLabelText('Version')).toHaveValue('v1');
        // v1 has two items and the stats of v1.
        expect(screen.getByText('orders')).toBeInTheDocument();
        expect(screen.getByText('1,200')).toBeInTheDocument();
        expect(screen.getByText('publisher-1')).toBeInTheDocument();
        fireEvent.click(screen.getByRole('button', { name: 'Copy lakefs URI' }));
        await waitFor(() => expect(writeText).toHaveBeenCalledWith('lakefs://customer-360/v1'));
    });

    it('falls back to latest when the URL names a version that does not exist', async () => {
        renderPage('/datasets/customer-360/overview?version=v7');

        expect(await screen.findByLabelText('Version')).toHaveValue('latest');
        expect(screen.getByText('publisher-2')).toBeInTheDocument();
    });

    it('offers no items message when nothing is published', async () => {
        fetchMock.mockResolvedValueOnce(jsonResponse({ ...dataset, latest_version: 0, definition: undefined }));
        fetchMock.mockResolvedValueOnce(jsonResponse({ results: [], pagination: {} }));
        renderPage();

        expect(await screen.findByText(/No version has been published yet/i)).toBeInTheDocument();
    });

    it('only reads: the overview issues no mutating request', async () => {
        renderPage();
        await screen.findByRole('heading', { name: 'customer-360' });

        const methods = fetchMock.mock.calls.map((call) => (call[1]?.method ?? 'GET') as string);
        expect(methods.every((method) => method === 'GET')).toBe(true);
        for (const name of [/publish/i, /upload/i, /delete/i]) {
            expect(screen.queryByRole('button', { name })).not.toBeInTheDocument();
        }
    });
});
