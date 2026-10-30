import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router-dom';

import { clearDatasetNamesCache } from '../../../lib/api/datasets';
import DatasetProposalsAllPage from './all';

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

const proposal = (id: string, dataset: string, status: 'open' | 'merged', createdAt: number) => ({
    id,
    dataset,
    base_version: 1,
    title: `${dataset} ${status}`,
    author: 'alice',
    status,
    created_at: createdAt,
    preview_branch: `_proposal-${id}`,
});

const page = (results: unknown[]) => ({
    results,
    pagination: { has_more: false, next_offset: '', results: results.length, max_per_page: 1000 },
});

describe('DatasetProposalsAllPage', () => {
    let fetchMock: ReturnType<typeof vi.fn>;

    const renderPage = (url = '/datasets/pulls') =>
        render(
            <MemoryRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }} initialEntries={[url]}>
                <DatasetProposalsAllPage />
            </MemoryRouter>,
        );

    beforeEach(() => {
        clearDatasetNamesCache();
        fetchMock = vi.fn(async (url: unknown) => {
            const path = String(url);
            if (path.includes('/proposals?')) {
                const status = new URLSearchParams(path.split('?')[1] ?? '').get('status') ?? 'open';
                if (path.includes('/datasets/alpha/')) {
                    return jsonResponse(
                        status === 'merged'
                            ? page([proposal('a-merged', 'alpha', 'merged', 1000)])
                            : page([proposal('a-open', 'alpha', 'open', 3000)]),
                    );
                }
                if (path.includes('/datasets/beta/')) {
                    return jsonResponse(page([proposal('b-open', 'beta', 'open', 2000)]));
                }
            }
            if (path.endsWith('/api/v1/datasets?amount=1000')) {
                return jsonResponse(page([{ name: 'alpha' }, { name: 'beta' }]));
            }
            return jsonResponse({ message: 'not found' }, 404);
        });
        vi.stubGlobal('fetch', fetchMock);
    });

    afterEach(() => {
        vi.unstubAllGlobals();
        clearDatasetNamesCache();
    });

    it('lists the open proposals of every dataset, newest first', async () => {
        renderPage();

        expect(await screen.findByTestId('dataset-proposal-row-a-open')).toBeInTheDocument();
        expect(screen.getByTestId('dataset-proposal-row-b-open')).toBeInTheDocument();

        const rows = screen.getAllByTestId(/^dataset-proposal-row-/).map((node) => node.dataset.testid);
        expect(rows).toEqual(['dataset-proposal-row-a-open', 'dataset-proposal-row-b-open']);
    });

    it('links every row to the proposal of its own dataset', async () => {
        renderPage();

        await screen.findByTestId('dataset-proposal-row-b-open');
        expect(screen.getByRole('link', { name: 'beta open' })).toHaveAttribute(
            'href',
            '/datasets/beta/proposals/b-open',
        );
    });

    it('refilters across all datasets when a status is selected', async () => {
        renderPage();
        await screen.findByTestId('dataset-proposal-row-a-open');

        await userEvent.click(screen.getByRole('button', { name: 'Merged' }));

        expect(await screen.findByTestId('dataset-proposal-row-a-merged')).toBeInTheDocument();
        expect(screen.queryByTestId('dataset-proposal-row-a-open')).not.toBeInTheDocument();
    });
});
