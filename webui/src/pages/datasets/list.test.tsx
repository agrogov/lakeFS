import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router-dom';

import DatasetsListPage from './list';

const { canCreate } = vi.hoisted(() => ({ canCreate: { current: false } }));

vi.mock('../../lib/components/datasets/hooks', () => ({
    useCanCreateDataset: () => canCreate.current,
}));

// useAPI() reports authentication failures through the auth context.
vi.mock('../../lib/auth/authContext', () => ({
    useAuth: () => ({ onUnauthenticated: () => {} }),
}));

const jsonResponse = (status, body) => ({
    status,
    ok: status < 400,
    statusText: '',
    headers: {
        get: (name) => (name.toLowerCase() === 'content-type' ? 'application/json' : null),
    },
    json: async () => body,
    text: async () => JSON.stringify(body),
});

const datasetList = (results) => ({
    results,
    pagination: { has_more: false, next_offset: '', results: results.length, max_per_page: 100 },
});

const dataset = (overrides = {}) => ({
    name: 'customer-360',
    description: 'Curated customer view for analytics',
    created_at: 1710000000000,
    created_by: 'tester',
    latest_version: 3,
    state: 'active',
    stats: { objects: 1200, bytes: 5 * 1024 * 1024, items: [] },
    definition: {
        format_version: 1,
        metadata: { tier: 'gold', owner: 'data-platform' },
        data: [],
    },
    ...overrides,
});

describe('DatasetsListPage', () => {
    let fetchMock;

    const renderPage = (url = '/datasets') =>
        render(
            <MemoryRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }} initialEntries={[url]}>
                <Routes>
                    <Route path="/datasets" element={<DatasetsListPage />} />
                </Routes>
            </MemoryRouter>,
        );

    beforeEach(() => {
        canCreate.current = false;
        fetchMock = vi.fn(async (url) => {
            if (String(url).startsWith('/api/v1/datasets')) return jsonResponse(200, datasetList([]));
            return jsonResponse(404, { message: 'not found' });
        });
        vi.stubGlobal('fetch', fetchMock);
    });

    afterEach(() => {
        vi.unstubAllGlobals();
    });

    it('renders the empty state and hides "Create dataset" without the permission', async () => {
        renderPage();

        expect(await screen.findByText('No datasets yet')).toBeInTheDocument();
        expect(screen.queryByRole('button', { name: /Create dataset/i })).not.toBeInTheDocument();
        expect(screen.queryByRole('link', { name: /Create dataset/i })).not.toBeInTheDocument();
    });

    it('offers "Create dataset" when the user may create datasets', async () => {
        canCreate.current = true;
        renderPage();

        expect(await screen.findByText('No datasets yet')).toBeInTheDocument();
        // Offered both in the toolbar and in the empty state.
        expect(screen.getAllByText('Create dataset').length).toBeGreaterThan(0);
    });

    it('renders a dataset row with version, objects, size and metadata badges', async () => {
        fetchMock.mockResolvedValue(jsonResponse(200, datasetList([dataset()])));
        renderPage();

        expect(await screen.findByText('customer-360')).toBeInTheDocument();
        expect(screen.getByText('Curated customer view for analytics')).toBeInTheDocument();
        expect(screen.getByText('v3')).toBeInTheDocument();
        expect(screen.getByText('1,200 / 5.0 MB')).toBeInTheDocument();
        expect(screen.getByLabelText('tier: gold')).toBeInTheDocument();
        expect(screen.getByLabelText('owner: data-platform')).toBeInTheDocument();
        expect(screen.queryByText('No datasets yet')).not.toBeInTheDocument();
    });

    it('shows the state of a dataset that is not active', async () => {
        fetchMock.mockResolvedValue(jsonResponse(200, datasetList([dataset({ state: 'degraded' })])));
        renderPage();

        expect(await screen.findByText('degraded')).toBeInTheDocument();
    });

    it('sends the search term as a query parameter', async () => {
        renderPage('/datasets?search=customer');

        await screen.findByText('No datasets match your search');
        const listUrl = String(fetchMock.mock.calls[0][0]);
        expect(listUrl).toContain('search=customer');
    });

    it('sends metadata filters taken from the URL', async () => {
        renderPage('/datasets?metadata.tier=gold');

        await screen.findByText('No datasets match your search');
        const listUrl = String(fetchMock.mock.calls[0][0]);
        expect(listUrl).toContain('metadata.tier=gold');
        // The active filter is visible and removable.
        expect(screen.getByText('tier: gold')).toBeInTheDocument();
    });
});
