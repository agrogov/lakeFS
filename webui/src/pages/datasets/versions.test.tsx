import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router-dom';

import { DatasetDefinition, Version } from '../../lib/api/datasets';
import { versionTimeline } from './versions';
import DatasetVersionsPage from './versions';

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

const definition = (targets: string[], overrides: Partial<DatasetDefinition> = {}): DatasetDefinition => ({
    format_version: 1,
    description: 'Curated customer view',
    metadata: { tier: 'gold' },
    data: targets.map((target) => ({
        target,
        repository: 'lakehouse-repo',
        ref: { type: 'commit' as const, id: 'a'.repeat(40) },
        type: 'prefix' as const,
        prefix: { path: `${target}/` },
    })),
    ...overrides,
});

const version = (number: number, targets: string[], overrides: Partial<Version> = {}): Version => ({
    version: number,
    message: `message for v${number}`,
    publisher: `publisher-${number}`,
    published_at: Date.UTC(2026, 0, number),
    commit_id: `${number}`.repeat(40),
    definition: definition(targets),
    stats: { objects: 10 * number, bytes: 1024 * number, items: [] },
    ...overrides,
});

const versions = [version(3, ['customers', 'orders']), version(2, ['customers']), version(1, ['customers'])];

const dataset = {
    name: 'customer-360',
    description: 'Curated customer view',
    created_at: Date.UTC(2026, 0, 1),
    created_by: 'creator',
    latest_version: 3,
    state: 'active',
    definition: versions[0].definition,
    stats: versions[0].stats,
};

describe('versionTimeline', () => {
    it('counts what each version changed against the version before it', () => {
        const entries = versionTimeline(versions);

        expect(entries.map((entry) => entry.version.version)).toEqual([3, 2, 1]);
        // v3 adds "orders" to the item v2 already had.
        expect(entries[0].counts).toEqual({ added: 1, removed: 0, changed: 0, total: 1 });
        // v2 and v3 declare the same single item, so nothing changed.
        expect(entries[1].counts).toEqual({ added: 0, removed: 0, changed: 0, total: 0 });
        // The first version adds every item it declares.
        expect(entries[2].counts).toEqual({ added: 1, removed: 0, changed: 0, total: 1 });
    });

    it('sorts newest first whatever order it is given', () => {
        const entries = versionTimeline([...versions].reverse());
        expect(entries.map((entry) => entry.version.version)).toEqual([3, 2, 1]);
    });
});

describe('DatasetVersionsPage', () => {
    let fetchMock: ReturnType<typeof vi.fn>;

    const renderPage = () =>
        render(
            <MemoryRouter
                future={{ v7_startTransition: true, v7_relativeSplatPath: true }}
                initialEntries={['/datasets/customer-360/versions']}
            >
                <Routes>
                    <Route path="/datasets/:datasetId/versions" element={<DatasetVersionsPage />} />
                </Routes>
            </MemoryRouter>,
        );

    beforeEach(() => {
        fetchMock = vi.fn(async (url: unknown) => {
            const path = String(url);
            if (path.includes('/versions')) return jsonResponse({ results: versions, pagination: {} });
            if (path.startsWith('/api/v1/datasets/customer-360')) return jsonResponse(dataset);
            return jsonResponse({ message: 'not found' }, 404);
        });
        vi.stubGlobal('fetch', fetchMock);
    });

    afterEach(() => {
        vi.unstubAllGlobals();
    });

    it('renders one timeline entry per version, newest first', async () => {
        renderPage();

        expect(await screen.findByTestId('dataset-version-v3')).toBeInTheDocument();
        expect(screen.getByTestId('dataset-version-v2')).toBeInTheDocument();
        expect(screen.getByTestId('dataset-version-v1')).toBeInTheDocument();

        // Newest first: v3 appears before v1 in the document.
        const rendered = screen.getAllByTestId(/^dataset-version-v\d$/).map((node) => node.dataset.testid);
        expect(rendered).toEqual(['dataset-version-v3', 'dataset-version-v2', 'dataset-version-v1']);
    });

    it('shows the message, publisher and items changed count of every version', async () => {
        renderPage();

        const first = await screen.findByTestId('dataset-version-v3');
        expect(within(first).getByText('message for v3')).toBeInTheDocument();
        expect(within(first).getByText('publisher-3')).toBeInTheDocument();
        expect(within(first).getByText('1 item changed')).toBeInTheDocument();

        const second = screen.getByTestId('dataset-version-v2');
        expect(within(second).getByText('message for v2')).toBeInTheDocument();
        expect(within(second).getByText('no item changes')).toBeInTheDocument();
    });

    it('links each version to a comparison with the one before it', async () => {
        renderPage();

        const v3 = await screen.findByTestId('dataset-version-v3');
        expect(within(v3).getByRole('link', { name: /Compare with previous/i })).toHaveAttribute(
            'href',
            '/datasets/customer-360/compare?left=v2&right=v3',
        );

        const v2 = screen.getByTestId('dataset-version-v2');
        expect(within(v2).getByRole('link', { name: /Compare with previous/i })).toHaveAttribute(
            'href',
            '/datasets/customer-360/compare?left=v1&right=v2',
        );

        // v1 has nothing before it, so it offers no comparison.
        const v1 = screen.getByTestId('dataset-version-v1');
        expect(within(v1).queryByRole('link', { name: /Compare with previous/i })).not.toBeInTheDocument();
    });

    it('links every version to its own data browser', async () => {
        renderPage();

        const v3 = await screen.findByTestId('dataset-version-v3');
        expect(within(v3).getByRole('link', { name: /Browse v3/i })).toHaveAttribute(
            'href',
            '/datasets/customer-360/data?version=v3',
        );
    });

    it('says so when the dataset has no versions', async () => {
        fetchMock.mockResolvedValueOnce(jsonResponse({ ...dataset, latest_version: 0, definition: undefined }));
        fetchMock.mockResolvedValueOnce(jsonResponse({ results: [], pagination: {} }));
        renderPage();

        expect(await screen.findByText('No versions yet')).toBeInTheDocument();
    });
});
