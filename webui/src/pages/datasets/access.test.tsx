import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Route, Routes } from 'react-router-dom';

import DatasetAccessPage from './access';

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

const policies = (actions: string[]) => ({
    results: [{ statement: [{ effect: 'allow', action: actions, resource: '*' }] }],
});

const grants = {
    results: [
        { principal_type: 'user', principal_id: 'alice', granted_by: 'admin', granted_at: Date.UTC(2026, 0, 2) },
        { principal_type: 'group', principal_id: 'analysts', granted_by: 'admin', granted_at: Date.UTC(2026, 0, 3) },
    ],
    pagination: { has_more: false, next_offset: '', results: 2, max_per_page: 1000 },
};

describe('DatasetAccessPage', () => {
    let fetchMock: ReturnType<typeof vi.fn>;
    let accessResponse: () => ReturnType<typeof jsonResponse>;

    const renderPage = () =>
        render(
            <MemoryRouter
                future={{ v7_startTransition: true, v7_relativeSplatPath: true }}
                initialEntries={['/datasets/customer-360/access']}
            >
                <Routes>
                    <Route path="/datasets/:datasetId/access" element={<DatasetAccessPage />} />
                </Routes>
            </MemoryRouter>,
        );

    const accessCalls = () => fetchMock.mock.calls.filter(([url]) => String(url).endsWith('/access'));

    const mockWith = (actions: string[]) => {
        fetchMock.mockImplementation(async (url: unknown) => {
            const path = String(url);
            if (path.endsWith('/api/v1/user')) return jsonResponse({ user: { id: 'reviewer' } });
            if (path.includes('/auth/users/reviewer/policies')) return jsonResponse(policies(actions));
            if (path.endsWith('/datasets/customer-360/access')) return accessResponse();
            return jsonResponse({ message: 'not found' }, 404);
        });
    };

    beforeEach(() => {
        accessResponse = () => jsonResponse(grants);
        fetchMock = vi.fn();
        vi.stubGlobal('fetch', fetchMock);
    });

    afterEach(() => {
        vi.unstubAllGlobals();
    });

    it('renders the grants when the caller holds datasets:ManageAccess', async () => {
        mockWith(['dataset:ReadDataset', 'dataset:ManageAccess']);
        renderPage();

        expect(await screen.findByTestId('dataset-grant-user-alice')).toBeInTheDocument();
        expect(screen.getByTestId('dataset-grant-group-analysts')).toBeInTheDocument();
        expect(accessCalls()).toHaveLength(1);
    });

    it('does not render the grants without datasets:ManageAccess', async () => {
        mockWith(['dataset:ReadDataset']);
        renderPage();

        expect(await screen.findByText('Permission required')).toBeInTheDocument();
        expect(screen.getAllByText(/dataset:ManageAccess/).length).toBeGreaterThan(0);
        expect(screen.queryByTestId('dataset-grant-user-alice')).not.toBeInTheDocument();
        // The page does not even ask for the grants it may not see.
        expect(accessCalls()).toHaveLength(0);
    });

    it('reports only the principals whose grant failed, and refreshes the list (UI-4)', async () => {
        fetchMock.mockImplementation(async (url: unknown, init?: RequestInit) => {
            const path = String(url);
            const method = (init?.method ?? 'GET').toUpperCase();
            if (path.endsWith('/api/v1/user')) return jsonResponse({ user: { id: 'reviewer' } });
            if (path.includes('/auth/users/reviewer/policies')) {
                return jsonResponse(policies(['dataset:ReadDataset', 'dataset:ManageAccess']));
            }
            if (path.startsWith('/api/v1/auth/users?')) {
                return jsonResponse({ results: [{ id: 'bob' }, { id: 'carol' }], pagination: { has_more: false } });
            }
            if (path.endsWith('/datasets/customer-360/access')) {
                if (method === 'POST') {
                    const body = JSON.parse(String(init?.body));
                    return body.principal_id === 'carol'
                        ? jsonResponse({ message: 'forbidden: user carol is not allowed' }, 403)
                        : jsonResponse({}, 201);
                }
                return accessResponse();
            }
            return jsonResponse({ message: 'not found' }, 404);
        });
        renderPage();

        await screen.findByTestId('dataset-grant-user-alice');
        await userEvent.click(screen.getByRole('button', { name: 'Add user' }));
        for (const checkbox of await screen.findAllByRole('checkbox')) {
            await userEvent.click(checkbox);
        }
        await userEvent.click(screen.getByRole('button', { name: 'Add users' }));

        // Only the principal that failed is named: bob's grant went through.
        expect(await screen.findByText(/Could not grant read access to carol/)).toBeInTheDocument();
        expect(screen.queryByText(/Could not grant read access to bob/)).not.toBeInTheDocument();
        // The list is re-read even though the batch failed: bob's grant is already in force.
        const reads = fetchMock.mock.calls.filter(
            ([url, init]) =>
                String(url).endsWith('/datasets/customer-360/access') &&
                ((init as RequestInit | undefined)?.method ?? 'GET').toUpperCase() === 'GET',
        );
        expect(reads).toHaveLength(2);
    });

    it('handles a 403 from the grants endpoint the same way as a denied policy check', async () => {
        accessResponse = () => jsonResponse({ message: 'forbidden: user reviewer is not allowed' }, 403);
        mockWith(['dataset:ReadDataset', 'dataset:ManageAccess']);
        renderPage();

        expect(await screen.findByText('Permission required')).toBeInTheDocument();
        expect(screen.queryByText('forbidden: user reviewer is not allowed')).not.toBeInTheDocument();
    });
});
