import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Route, Routes } from 'react-router-dom';

import { DatasetDefinition, DefinitionDiff, Version } from '../../lib/api/datasets';
import DatasetComparePage, { defaultCompareRefs, isCompareTab } from './compare';

// The data tab pulls in the repository diff components, and `lib/hooks/appContext` reads
// `window.localStorage` while it is being imported. This happy-dom setup has no
// `localStorage`, so give it a minimal one before the imports above are evaluated.
vi.hoisted(() => {
    if (typeof window === 'undefined' || window.localStorage) return;
    const store = new Map<string, string>();
    Object.defineProperty(window, 'localStorage', {
        configurable: true,
        value: {
            getItem: (key: string) => store.get(key) ?? null,
            setItem: (key: string, value: string) => void store.set(key, value),
            removeItem: (key: string) => void store.delete(key),
            clear: () => store.clear(),
        },
    });
});

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

const item = (target: string) => ({
    target,
    repository: 'lakehouse-repo',
    ref: { type: 'commit' as const, id: 'a'.repeat(40) },
    type: 'prefix' as const,
    prefix: { path: `${target}/` },
});

const definition = (targets: string[]): DatasetDefinition => ({
    format_version: 1,
    description: 'Curated customer view',
    metadata: { tier: 'gold' },
    data: targets.map(item),
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

const versions = [version(3, ['customers', 'orders']), version(2, ['customers']), version(1, ['customers'])];

const dataset = {
    name: 'customer-360',
    created_at: Date.UTC(2026, 0, 1),
    created_by: 'creator',
    latest_version: 3,
    state: 'active',
    definition: versions[0].definition,
    stats: versions[0].stats,
};

const repository = {
    id: 'customer-360',
    default_branch: 'latest',
    read_only: true,
    storage_namespace: 'local://customer-360',
    creation_date: 0,
};

const definitionDiff: DefinitionDiff = {
    description: { from: 'Curated customer view', to: 'Curated customer view, with orders' },
    metadata: [{ key: 'tier', change: 'changed', from: 'gold', to: 'platinum' }],
    items: [
        { target: 'customers', change: 'changed', from: item('customers'), to: item('customers') },
        { target: 'orders', change: 'added', to: item('orders') },
    ],
};

const dataDiff = {
    results: [
        {
            path: 'orders/part-0.parquet',
            path_type: 'object',
            type: 'added',
            size_bytes: 2048,
            mtime: 1_700_000_000,
        },
        {
            path: '_dataset/definition.json',
            path_type: 'object',
            type: 'changed',
            size_bytes: 512,
            mtime: 1_700_000_000,
        },
    ],
    pagination: { has_more: false, next_offset: '', results: 2, max_per_page: 1000 },
};

describe('compare helpers', () => {
    it('defaults to comparing the newest version with the one before it', () => {
        expect(defaultCompareRefs(3)).toEqual({ left: 'v2', right: 'v3' });
        // With a single version there is nothing before it.
        expect(defaultCompareRefs(1)).toEqual({ left: 'v1', right: 'v1' });
    });

    it('recognises the two tabs and nothing else', () => {
        expect(isCompareTab('definition')).toBe(true);
        expect(isCompareTab('data')).toBe(true);
        expect(isCompareTab('metadata')).toBe(false);
        expect(isCompareTab(undefined)).toBe(false);
    });
});

describe('DatasetComparePage', () => {
    let fetchMock: ReturnType<typeof vi.fn>;
    const fetchedUrls = () => fetchMock.mock.calls.map((call) => String(call[0]));

    const renderPage = (url = '/datasets/customer-360/compare') =>
        render(
            <MemoryRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }} initialEntries={[url]}>
                <Routes>
                    <Route path="/datasets/:datasetId/compare" element={<DatasetComparePage />} />
                </Routes>
            </MemoryRouter>,
        );

    beforeEach(() => {
        fetchMock = vi.fn(async (url: unknown) => {
            const path = String(url);
            if (path.includes('/refs/') && path.includes('/diff/')) {
                // The control plane answers the definition diff; the repository diff is the
                // data diff of the dataset repository itself.
                if (path.startsWith('/api/v1/repositories/')) return jsonResponse(dataDiff);
                return jsonResponse(definitionDiff);
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

    it('opens on the definition tab, comparing the newest two versions by default', async () => {
        renderPage();

        expect(await screen.findByLabelText('Left version')).toHaveValue('v2');
        expect(screen.getByLabelText('Right version')).toHaveValue('v3');
        // Item and metadata changes from the control plane's definition diff.
        expect(await screen.findByText('orders')).toBeInTheDocument();
        expect(screen.getByText('platinum')).toBeInTheDocument();
        expect(screen.getByText(/Curated customer view, with orders/)).toBeInTheDocument();
    });

    it('asks the control plane for the definition diff of the two refs', async () => {
        renderPage();

        await screen.findByText('orders');
        await waitFor(() =>
            expect(
                fetchedUrls().some((url) => url.includes('/refs/v2/diff/v3') && url.includes('type=definition')),
            ).toBe(true),
        );
    });

    it('switches to the data tab and shows the repository diff of the dataset', async () => {
        const user = userEvent.setup();
        renderPage();
        await screen.findByText('orders');

        // react-bootstrap renders nav links as `role="button"` anchors, like every tab bar here.
        await user.click(screen.getByRole('button', { name: 'Data' }));

        // The data diff comes from the dataset repository itself and drops `_dataset/`.
        expect(await screen.findByText('orders/part-0.parquet')).toBeInTheDocument();
        expect(screen.queryByText('_dataset/definition.json')).not.toBeInTheDocument();
        await waitFor(() =>
            expect(
                fetchedUrls().some(
                    (url) => url.includes('/repositories/customer-360/refs/v2/diff/v3') && !url.includes('type='),
                ),
            ).toBe(true),
        );
        // The structural diff is gone: the tabs really swap the content.
        expect(screen.queryByText('platinum')).not.toBeInTheDocument();
    });

    it('opens the data tab directly when the URL asks for it', async () => {
        renderPage('/datasets/customer-360/compare?left=v1&right=v3&tab=data');

        expect(await screen.findByText('orders/part-0.parquet')).toBeInTheDocument();
        expect(screen.getByLabelText('Left version')).toHaveValue('v1');
        expect(screen.getByRole('button', { name: 'Data' })).toHaveClass('active');
    });

    it('compares the versions picked in the header', async () => {
        const user = userEvent.setup();
        renderPage();
        await screen.findByText('orders');

        await user.selectOptions(screen.getByLabelText('Left version'), 'v1');

        await waitFor(() =>
            expect(
                fetchedUrls().some((url) => url.includes('/refs/v1/diff/v3') && url.includes('type=definition')),
            ).toBe(true),
        );
        expect(screen.getByLabelText('Left version')).toHaveValue('v1');
    });

    it('says so when the dataset has no versions to compare', async () => {
        fetchMock.mockResolvedValueOnce(jsonResponse({ ...dataset, latest_version: 0, definition: undefined }));
        fetchMock.mockResolvedValueOnce(jsonResponse({ results: [], pagination: {} }));
        renderPage();

        expect(await screen.findByText(/nothing to compare/i)).toBeInTheDocument();
    });
});
