import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router-dom';

import { MetadataKey } from '../../lib/api/datasets';
import DatasetsSettingsPage from './settings';

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

const classificationKey: MetadataKey = {
    name: 'classification',
    type: 'enum',
    required: true,
    allowed_values: ['public', 'internal', 'confidential'],
    allow_multiple: false,
    description: 'Sensitivity of the data',
    created_at: Date.UTC(2026, 0, 1),
    created_by: 'admin',
};

describe('DatasetsSettingsPage', () => {
    let fetchMock: ReturnType<typeof vi.fn>;
    let keys: MetadataKey[];

    const renderPage = () =>
        render(
            <MemoryRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
                <DatasetsSettingsPage />
            </MemoryRouter>,
        );

    const metadataCalls = () => fetchMock.mock.calls.filter(([url]) => String(url).includes('/metadata-keys'));

    const mockWith = (actions: string[]) => {
        fetchMock.mockImplementation(async (url: unknown, init: RequestInit = {}) => {
            const path = String(url);
            const method = (init.method ?? 'GET').toUpperCase();
            if (path.endsWith('/api/v1/user')) return jsonResponse({ user: { id: 'reviewer' } });
            if (path.includes('/auth/users/reviewer/policies')) return jsonResponse(policies(actions));
            if (path.includes('/metadata-keys/') && method === 'PUT') return jsonResponse(classificationKey);
            if (path.includes('/metadata-keys')) {
                return jsonResponse({
                    results: keys,
                    pagination: { has_more: false, next_offset: '', results: keys.length, max_per_page: 1000 },
                });
            }
            return jsonResponse({ message: 'not found' }, 404);
        });
    };

    beforeEach(() => {
        keys = [classificationKey];
        fetchMock = vi.fn();
        vi.stubGlobal('fetch', fetchMock);
    });

    afterEach(() => {
        vi.unstubAllGlobals();
    });

    it('renders the metadata keys when the caller holds datasets:ManageMetadataKeys', async () => {
        mockWith(['datasets:ReadDataset', 'datasets:ManageMetadataKeys']);
        renderPage();

        expect(await screen.findByTestId('metadata-key-row-classification')).toBeInTheDocument();
        expect(screen.getByText('public')).toBeInTheDocument();
        expect(metadataCalls().length).toBeGreaterThan(0);
    });

    it('does not render the metadata keys without datasets:ManageMetadataKeys', async () => {
        mockWith(['datasets:ReadDataset']);
        renderPage();

        expect(await screen.findByText('Permission required')).toBeInTheDocument();
        expect(screen.getAllByText(/datasets:ManageMetadataKeys/).length).toBeGreaterThan(0);
        expect(screen.queryByTestId('metadata-key-row-classification')).not.toBeInTheDocument();
        expect(metadataCalls()).toHaveLength(0);
    });

    it('treats a 403 from the metadata-key endpoint like a denied policy check', async () => {
        fetchMock.mockImplementation(async (url: unknown) => {
            const path = String(url);
            if (path.endsWith('/api/v1/user')) return jsonResponse({ user: { id: 'reviewer' } });
            if (path.includes('/auth/users/reviewer/policies')) {
                return jsonResponse(policies(['datasets:ManageMetadataKeys']));
            }
            if (path.includes('/metadata-keys')) return jsonResponse({ message: 'forbidden' }, 403);
            return jsonResponse({ message: 'not found' }, 404);
        });
        renderPage();

        expect(await screen.findByText('Permission required')).toBeInTheDocument();
    });

    it('renders the type read-only with an explanation when editing a key', async () => {
        mockWith(['datasets:ManageMetadataKeys']);
        renderPage();

        await screen.findByTestId('metadata-key-row-classification');
        await userEvent.click(screen.getByRole('button', { name: 'Edit' }));

        const type = await screen.findByLabelText('Type');
        expect(type).toBeDisabled();
        expect(screen.getByLabelText('Name')).toBeDisabled();
        expect(screen.getByText(/cannot be changed after a metadata key is created/i)).toBeInTheDocument();
    });

    it('never sends the type in an update, because §5.5 forbids changing it', async () => {
        mockWith(['datasets:ManageMetadataKeys']);
        renderPage();

        await screen.findByTestId('metadata-key-row-classification');
        await userEvent.click(screen.getByRole('button', { name: 'Edit' }));

        // Make an allowed change: flip "required".
        const required = await screen.findByLabelText('Required');
        await userEvent.click(required);
        await userEvent.click(screen.getByRole('button', { name: 'Save' }));

        await waitFor(() =>
            expect(
                fetchMock.mock.calls.some(
                    ([url, init]) =>
                        String(url).includes('/metadata-keys/classification') &&
                        (init as RequestInit | undefined)?.method === 'PUT',
                ),
            ).toBe(true),
        );

        const call = fetchMock.mock.calls.find(
            ([url, init]) =>
                String(url).includes('/metadata-keys/classification') &&
                (init as RequestInit | undefined)?.method === 'PUT',
        );
        const body = JSON.parse(String((call?.[1] as RequestInit).body));
        expect(body).not.toHaveProperty('type');
        expect(body).toMatchObject({
            required: false,
            allow_multiple: false,
            allowed_values: ['public', 'internal', 'confidential'],
        });
    });
});
