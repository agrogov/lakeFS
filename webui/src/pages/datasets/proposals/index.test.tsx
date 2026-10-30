import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Route, Routes } from 'react-router-dom';

import { Proposal } from '../../../lib/api/datasets';
import DatasetProposalsPage from './index';

// useAPI() reports authentication failures through the auth context.
vi.mock('../../../lib/auth/authContext', () => ({
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

const proposal = (id: string, status: Proposal['status'], title: string): Proposal => ({
    id,
    dataset: 'customer-360',
    base_version: 1,
    title,
    author: 'alice',
    status,
    created_at: Date.UTC(2026, 0, 1),
    preview_branch: `_proposal-${id}`,
});

const proposalsByStatus: Record<string, Proposal[]> = {
    open: [proposal('p-open', 'open', 'Open review')],
    merged: [proposal('p-merged', 'merged', 'Merged review')],
    closed: [proposal('p-closed', 'closed', 'Closed review')],
};

describe('DatasetProposalsPage status filters', () => {
    let fetchMock: ReturnType<typeof vi.fn>;

    const renderPage = (url = '/datasets/customer-360/proposals') =>
        render(
            <MemoryRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }} initialEntries={[url]}>
                <Routes>
                    <Route path="/datasets/:datasetId/proposals" element={<DatasetProposalsPage />} />
                </Routes>
            </MemoryRouter>,
        );

    const proposalListCalls = () => fetchMock.mock.calls.filter(([url]) => String(url).includes('/proposals?'));

    beforeEach(() => {
        fetchMock = vi.fn(async (url: unknown) => {
            const path = String(url);
            if (path.includes('/proposals?')) {
                const status = new URLSearchParams(path.split('?')[1] ?? '').get('status') ?? 'open';
                return jsonResponse({
                    results: proposalsByStatus[status] ?? [],
                    pagination: { has_more: false, next_offset: '', results: 1, max_per_page: 1000 },
                });
            }
            return jsonResponse({ message: 'not found' }, 404);
        });
        vi.stubGlobal('fetch', fetchMock);
    });

    afterEach(() => {
        vi.unstubAllGlobals();
    });

    it('defaults to the open proposals', async () => {
        renderPage();

        expect(await screen.findByTestId('dataset-proposal-row-p-open')).toBeInTheDocument();
        expect(screen.queryByTestId('dataset-proposal-row-p-merged')).not.toBeInTheDocument();

        const [url] = proposalListCalls()[0];
        expect(new URLSearchParams(String(url).split('?')[1]).get('status')).toBe('open');
    });

    it('shows the merged proposals when the Merged filter is selected', async () => {
        renderPage();
        await screen.findByTestId('dataset-proposal-row-p-open');

        await userEvent.click(screen.getByRole('button', { name: 'Merged' }));

        expect(await screen.findByTestId('dataset-proposal-row-p-merged')).toBeInTheDocument();
        await waitFor(() => expect(screen.queryByTestId('dataset-proposal-row-p-open')).not.toBeInTheDocument());
        const statuses = proposalListCalls().map(([url]) =>
            new URLSearchParams(String(url).split('?')[1]).get('status'),
        );
        expect(statuses).toContain('merged');
    });

    it('shows the closed proposals when the Closed filter is selected', async () => {
        renderPage();
        await screen.findByTestId('dataset-proposal-row-p-open');

        await userEvent.click(screen.getByRole('button', { name: 'Closed' }));

        expect(await screen.findByTestId('dataset-proposal-row-p-closed')).toBeInTheDocument();
    });

    it('reads the filter from the URL so a filtered list can be linked to', async () => {
        renderPage('/datasets/customer-360/proposals?status=merged');

        expect(await screen.findByTestId('dataset-proposal-row-p-merged')).toBeInTheDocument();
        const [url] = proposalListCalls()[0];
        expect(new URLSearchParams(String(url).split('?')[1]).get('status')).toBe('merged');
    });

    it('falls back to open for an unknown status value', async () => {
        renderPage('/datasets/customer-360/proposals?status=bogus');

        expect(await screen.findByTestId('dataset-proposal-row-p-open')).toBeInTheDocument();
    });

    it('links each row to the proposal detail page', async () => {
        renderPage();

        const row = await screen.findByTestId('dataset-proposal-row-p-open');
        expect(screen.getByRole('link', { name: 'Open review' })).toHaveAttribute(
            'href',
            '/datasets/customer-360/proposals/p-open',
        );
        expect(row).toBeInTheDocument();
    });
});
