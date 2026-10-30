import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Route, Routes } from 'react-router-dom';

import { Proposal, Version } from '../../../lib/api/datasets';
import DatasetProposalPage from './proposal';

// `lib/api` caches the current user in `localStorage`, which this happy-dom setup does not
// provide. Give it a minimal one before the imports above are evaluated.
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

const dataset = (latest: number) => ({
    name: 'customer-360',
    created_at: Date.UTC(2026, 0, 1),
    created_by: 'creator',
    latest_version: latest,
    state: 'active' as const,
});

const proposal = (overrides: Partial<Proposal> = {}): Proposal => ({
    id: 'p1',
    dataset: 'customer-360',
    base_version: 1,
    title: 'Add customers',
    description: 'Adds the customers prefix',
    author: 'alice',
    status: 'open',
    created_at: Date.UTC(2026, 0, 2),
    preview_branch: '_proposal-p1',
    diff: { description: { from: 'old', to: 'new' }, items: [] },
    data_diff: {
        results: [
            { path: 'customers/a.parquet', type: 'added', size_bytes: 2048 },
            // The synthetic definition document is not data (§6.4) and must not be listed.
            { path: '_dataset/definition.json', type: 'changed', size_bytes: 12 },
        ],
        pagination: { has_more: false, next_offset: '', results: 2, max_per_page: 1000 },
    },
    ...overrides,
});

const version = (number: number): Version => ({
    version: number,
    publisher: 'reviewer',
    published_at: Date.UTC(2026, 0, number),
    commit_id: `${number}`.repeat(40),
});

/** An allow statement for the given actions on `*`, the shape `listUserPolicies` returns. */
const policies = (actions: string[]) => ({
    results: [{ statement: [{ effect: 'allow', action: actions, resource: '*' }] }],
});

const MANAGE_READ = policies(['datasets:ReadDataset']);
const MANAGE_PUBLISH = policies(['datasets:ReadDataset', 'datasets:PublishVersion', 'datasets:CreateProposal']);

describe('DatasetProposalPage', () => {
    let fetchMock: ReturnType<typeof vi.fn>;
    let currentProposal: Proposal;
    let mergeResponse: () => ReturnType<typeof jsonResponse>;
    let currentPolicies: ReturnType<typeof policies>;

    const renderPage = () =>
        render(
            <MemoryRouter
                future={{ v7_startTransition: true, v7_relativeSplatPath: true }}
                initialEntries={['/datasets/customer-360/proposals/p1']}
            >
                <Routes>
                    <Route path="/datasets/:datasetId/proposals/:proposalId" element={<DatasetProposalPage />} />
                </Routes>
            </MemoryRouter>,
        );

    const mergeCalls = () => fetchMock.mock.calls.filter(([url]) => String(url).endsWith('/proposals/p1/merge'));

    beforeEach(() => {
        currentProposal = proposal();
        currentPolicies = MANAGE_PUBLISH;
        mergeResponse = () => jsonResponse(version(2), 201);

        fetchMock = vi.fn(async (url: unknown, init: RequestInit = {}) => {
            const path = String(url);
            const method = (init.method ?? 'GET').toUpperCase();

            if (path.endsWith('/api/v1/user')) return jsonResponse({ user: { id: 'reviewer' } });
            if (path.includes('/auth/users/reviewer/policies')) return jsonResponse(currentPolicies);
            if (path.endsWith('/proposals/p1/merge')) return mergeResponse();
            if (path.endsWith('/proposals/p1')) {
                if (method === 'PATCH') {
                    // "Update proposal" is the explicit rebase: the base moves to latest (§5.6).
                    currentProposal = { ...currentProposal, base_version: 2 };
                    return jsonResponse(currentProposal);
                }
                return jsonResponse(currentProposal);
            }
            if (path.endsWith('/datasets/customer-360')) return jsonResponse(dataset(2));
            return jsonResponse({ message: 'not found' }, 404);
        });
        vi.stubGlobal('fetch', fetchMock);
    });

    afterEach(() => {
        vi.unstubAllGlobals();
    });

    it('shows the rebase banner when the proposal base is behind the latest version', async () => {
        // base_version 1, latest 2: the review no longer describes what a merge would publish.
        renderPage();

        const banner = await screen.findByTestId('dataset-proposal-conflict');
        expect(banner).toHaveTextContent(/Rebase required/i);
        expect(banner).toHaveTextContent(/made against v1/i);
        expect(banner).toHaveTextContent(/now at v2/i);
        expect(screen.getByRole('button', { name: /Update proposal/i })).toBeInTheDocument();
        // A stale base cannot be merged: the button would only produce the same 409.
        expect(screen.getByRole('button', { name: /Merge/i })).toBeDisabled();
    });

    it('surfaces the 409 of a refused merge as the rebase banner, not a generic failure', async () => {
        // The record looks current (base 1 == latest 1); only the merge reveals the conflict.
        currentProposal = proposal({ base_version: 1 });
        fetchMock.mockImplementation(async (url: unknown) => {
            const path = String(url);
            if (path.endsWith('/api/v1/user')) return jsonResponse({ user: { id: 'reviewer' } });
            if (path.includes('/auth/users/reviewer/policies')) return jsonResponse(MANAGE_PUBLISH);
            if (path.endsWith('/proposals/p1/merge')) {
                return jsonResponse(
                    {
                        message:
                            'conflict: rebase required: proposal p1 of dataset customer-360 is based on version 1, latest is 2',
                        latest_version: 2,
                    },
                    409,
                );
            }
            if (path.endsWith('/proposals/p1')) return jsonResponse(currentProposal);
            if (path.endsWith('/datasets/customer-360')) return jsonResponse(dataset(1));
            return jsonResponse({ message: 'not found' }, 404);
        });

        renderPage();

        const merge = await screen.findByRole('button', { name: /Merge/i });
        await waitFor(() => expect(merge).toBeEnabled());
        expect(screen.queryByTestId('dataset-proposal-conflict')).not.toBeInTheDocument();

        await userEvent.click(merge);

        const banner = await screen.findByTestId('dataset-proposal-conflict');
        expect(banner).toHaveTextContent(/Rebase required/i);
        // The latest version comes from the 409 body, not from a second read of the dataset.
        expect(banner).toHaveTextContent(/now at v2/i);
        expect(mergeCalls()).toHaveLength(1);
    });

    it('offers Update proposal as the explicit rebase of the conflict', async () => {
        renderPage();

        await screen.findByTestId('dataset-proposal-conflict');
        await userEvent.click(screen.getByRole('button', { name: /Update proposal/i }));

        await waitFor(() => expect(screen.queryByTestId('dataset-proposal-conflict')).not.toBeInTheDocument());
        const patch = fetchMock.mock.calls.filter(
            ([url, init]) =>
                String(url).endsWith('/proposals/p1') && (init as RequestInit | undefined)?.method === 'PATCH',
        );
        expect(patch).toHaveLength(1);
    });

    it('gates Merge on datasets:PublishVersion', async () => {
        currentProposal = proposal({ base_version: 2 });
        currentPolicies = MANAGE_READ;

        fetchMock.mockImplementation(async (url: unknown) => {
            const path = String(url);
            if (path.endsWith('/api/v1/user')) return jsonResponse({ user: { id: 'reviewer' } });
            if (path.includes('/auth/users/reviewer/policies')) return jsonResponse(MANAGE_READ);
            if (path.endsWith('/proposals/p1')) return jsonResponse(currentProposal);
            if (path.endsWith('/datasets/customer-360')) return jsonResponse(dataset(2));
            return jsonResponse({ message: 'not found' }, 404);
        });

        renderPage();

        const merge = await screen.findByRole('button', { name: /Merge/i });
        expect(merge).toBeDisabled();
        expect(merge.getAttribute('title') ?? '').toContain('datasets:PublishVersion');
        // Close is still offered: an author may close their own review.
        expect(screen.getByRole('button', { name: /Close/i })).toBeInTheDocument();
    });

    it('enables Merge for a caller who may publish when the base is current', async () => {
        currentProposal = proposal({ base_version: 2 });
        fetchMock.mockImplementation(async (url: unknown) => {
            const path = String(url);
            if (path.endsWith('/api/v1/user')) return jsonResponse({ user: { id: 'reviewer' } });
            if (path.includes('/auth/users/reviewer/policies')) return jsonResponse(MANAGE_PUBLISH);
            if (path.endsWith('/proposals/p1')) return jsonResponse(currentProposal);
            if (path.endsWith('/datasets/customer-360')) return jsonResponse(dataset(2));
            return jsonResponse({ message: 'not found' }, 404);
        });

        renderPage();

        const merge = await screen.findByRole('button', { name: /Merge/i });
        await waitFor(() => expect(merge).toBeEnabled());
        expect(screen.queryByTestId('dataset-proposal-conflict')).not.toBeInTheDocument();
    });

    it('links to the preview branch as data?ref=_proposal-<id>', async () => {
        renderPage();

        const link = await screen.findByRole('link', { name: /Preview data/i });
        expect(link).toHaveAttribute('href', '/datasets/customer-360/data?ref=_proposal-p1');
    });

    it('renders the definition diff and hides the internal _dataset entries from the data diff', async () => {
        renderPage();

        // Definition is the default tab.
        expect(await screen.findByText('old')).toBeInTheDocument();
        expect(screen.getByText('new')).toBeInTheDocument();

        await userEvent.click(screen.getByRole('button', { name: /^Data/ }));

        expect(await screen.findByText('customers/a.parquet')).toBeInTheDocument();
        expect(screen.queryByText('_dataset/definition.json')).not.toBeInTheDocument();
    });
});
