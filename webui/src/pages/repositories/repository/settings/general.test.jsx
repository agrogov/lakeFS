import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Outlet, Route, Routes } from 'react-router-dom';

import RepositoryGeneralSettingsPage from './general';

// `lib/api` caches the current user in `localStorage`, which this happy-dom setup does not
// provide. Give it a minimal one before the imports above are evaluated.
vi.hoisted(() => {
    if (typeof window === 'undefined' || window.localStorage) return;
    const store = new Map();
    Object.defineProperty(window, 'localStorage', {
        configurable: true,
        value: {
            getItem: (key) => store.get(key) ?? null,
            setItem: (key, value) => void store.set(key, value),
            removeItem: (key) => void store.delete(key),
            clear: () => store.clear(),
        },
    });
});

const { repoState, configState } = vi.hoisted(() => ({
    repoState: { current: null },
    configState: { current: null },
}));

vi.mock('../../../../lib/hooks/repo', () => ({
    useRefs: () => repoState.current,
}));

vi.mock('../../../../lib/hooks/configProvider', () => ({
    useConfigContext: () => configState.current,
}));

const jsonResponse = (status, body) => ({
    status,
    ok: status < 400,
    headers: {
        get: (name) => (name.toLowerCase() === 'content-type' ? 'application/json' : null),
    },
    json: async () => body,
    text: async () => JSON.stringify(body),
});

const repo = {
    id: 'my-repo',
    storage_id: 's3',
    storage_namespace: 's3://bucket/my-repo',
    default_branch: 'main',
    read_only: false,
};

const storageConfig = (deleteNamespaceSupport) => ({
    blockstore_type: 's3',
    blockstore_id: 's3',
    delete_namespace_support: deleteNamespaceSupport,
});

const setConfig = (config) => {
    configState.current = { config, loading: false, error: null };
};

const renderPage = () =>
    render(
        <MemoryRouter initialEntries={['/repositories/my-repo/settings/general']}>
            <Routes>
                <Route path="/repositories/:repoId/settings" element={<Outlet context={[vi.fn()]} />}>
                    <Route path="general" element={<RepositoryGeneralSettingsPage />} />
                </Route>
                <Route path="/repositories" element={<div>Repositories list</div>} />
            </Routes>
        </MemoryRouter>,
    );

const openDeleteModal = async () => {
    await userEvent.click(screen.getByRole('button', { name: /delete repository/i }));
    await screen.findByRole('dialog');
};

const confirmDelete = async () => {
    await userEvent.type(screen.getByPlaceholderText('Enter repository name to confirm'), repo.id);
    await userEvent.click(screen.getByRole('button', { name: /delete this repository/i }));
};

const deleteCalls = (fetchMock) => fetchMock.mock.calls.filter(([, init]) => init?.method === 'DELETE');

describe('repository general settings deletion', () => {
    let fetchMock;

    beforeEach(() => {
        repoState.current = { repo, reference: null, loading: false, error: null };
        setConfig({ storages: [storageConfig(true)] });
        fetchMock = vi.fn(async () => jsonResponse(204, null));
        vi.stubGlobal('fetch', fetchMock);
    });

    afterEach(() => {
        vi.unstubAllGlobals();
    });

    it('shows an unchecked delete-storage checkbox when the blockstore supports it', async () => {
        renderPage();
        await openDeleteModal();

        const checkbox = screen.getByLabelText(/Also delete all objects in/i);
        expect(checkbox).not.toBeChecked();
        expect(screen.getByText(repo.storage_namespace)).toBeInTheDocument();
        expect(screen.getByText(/will not be deleted by this action/i)).toBeInTheDocument();
    });

    it('hides the delete-storage checkbox when the blockstore does not support it', async () => {
        setConfig({ storages: [storageConfig(false)] });
        renderPage();
        await openDeleteModal();

        expect(screen.queryByLabelText(/Also delete all objects in/i)).not.toBeInTheDocument();
        expect(screen.getByText(/will not be deleted by this action/i)).toBeInTheDocument();
    });

    it('explains that the objects are deleted in the background when the checkbox is checked', async () => {
        renderPage();
        await openDeleteModal();

        await userEvent.click(screen.getByLabelText(/Also delete all objects in/i));

        expect(screen.getByText(/will be deleted in the background/i)).toBeInTheDocument();
        expect(screen.queryByText(/will not be deleted by this action/i)).not.toBeInTheDocument();
    });

    it('sends delete_storage=true only when the checkbox is checked', async () => {
        renderPage();
        await openDeleteModal();
        await confirmDelete();

        await waitFor(() => expect(deleteCalls(fetchMock)).toHaveLength(1));
        expect(String(deleteCalls(fetchMock)[0][0])).toBe('/api/v1/repositories/my-repo');
    });

    it('requests the repository deletion with the storage when the checkbox is checked', async () => {
        renderPage();
        await openDeleteModal();
        await userEvent.click(screen.getByLabelText(/Also delete all objects in/i));
        await confirmDelete();

        await waitFor(() => expect(deleteCalls(fetchMock)).toHaveLength(1));
        expect(String(deleteCalls(fetchMock)[0][0])).toBe('/api/v1/repositories/my-repo?delete_storage=true');
    });

    it('shows that the objects are deleted in the background and returns to the repository list on 202', async () => {
        fetchMock.mockResolvedValue(
            jsonResponse(202, {
                repository: repo.id,
                storage_namespace: repo.storage_namespace,
                state: 'pending',
            }),
        );
        renderPage();
        await openDeleteModal();
        await userEvent.click(screen.getByLabelText(/Also delete all objects in/i));
        await confirmDelete();

        expect(await screen.findByText('Deleting objects in the background')).toBeInTheDocument();
        expect(await screen.findByText('Repositories list', {}, { timeout: 3000 })).toBeInTheDocument();
    });
});
