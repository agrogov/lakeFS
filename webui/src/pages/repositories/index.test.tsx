import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';

import { RepositoryList } from './index';

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

const { datasetNames } = vi.hoisted(() => ({ datasetNames: { current: new Set<string>() } }));

vi.mock('../../lib/components/datasets/hooks', () => ({
    useDatasetNames: () => ({ names: datasetNames.current, loading: false }),
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

const repo = (id: string) => ({
    id,
    creation_date: Date.UTC(2026, 0, 2) / 1000,
    default_branch: 'main',
    storage_namespace: `s3://bucket/${id}`,
    read_only: false,
});

const renderList = (onPaginate = vi.fn(), after = '') =>
    render(
        <MemoryRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
            <RepositoryList
                search=""
                after={after}
                refresh={0}
                onPaginate={onPaginate}
                allowSampleRepoCreation={true}
                onCreateSampleRepo={() => {}}
                onCreateEmptyRepo={() => {}}
                toggleShowActionsBar={() => {}}
                creatingRepo={false}
                createRepoError={null}
            />
        </MemoryRouter>,
    );

describe('RepositoryList', () => {
    let fetchMock: ReturnType<typeof vi.fn>;

    beforeEach(() => {
        datasetNames.current = new Set(['sales-dataset', 'customer-360']);
        fetchMock = vi.fn();
        vi.stubGlobal('fetch', fetchMock);
    });

    afterEach(() => {
        vi.unstubAllGlobals();
    });

    it('keeps the paginator when the whole page is datasets (UI-2)', async () => {
        fetchMock.mockResolvedValue(
            jsonResponse({
                results: [repo('sales-dataset'), repo('customer-360')],
                pagination: { has_more: true, next_offset: 'customer-360', results: 2, max_per_page: 100 },
            }),
        );

        const onPaginate = vi.fn();
        renderList(onPaginate);

        expect(await screen.findByText(/These repositories are datasets/)).toBeInTheDocument();
        const next = screen.getByRole('button', { name: 'Next Page' });
        expect(next).toBeEnabled();
        next.click();
        expect(onPaginate).toHaveBeenCalledWith('customer-360');
    });

    it('lists the ordinary repositories of a mixed page and still paginates', async () => {
        fetchMock.mockResolvedValue(
            jsonResponse({
                results: [repo('sales-dataset'), repo('warehouse')],
                pagination: { has_more: true, next_offset: 'warehouse', results: 2, max_per_page: 100 },
            }),
        );

        renderList();

        expect(await screen.findByText('warehouse')).toBeInTheDocument();
        expect(screen.queryByText('sales-dataset')).not.toBeInTheDocument();
        expect(screen.getByRole('button', { name: 'Next Page' })).toBeEnabled();
    });

    it('disables the next page on the last page', async () => {
        fetchMock.mockResolvedValue(
            jsonResponse({
                results: [repo('sales-dataset')],
                pagination: { has_more: false, next_offset: '', results: 1, max_per_page: 100 },
            }),
        );

        renderList(vi.fn(), 'customer-360');

        expect(await screen.findByText(/This repository is a dataset/)).toBeInTheDocument();
        expect(screen.getByRole('button', { name: 'Next Page' })).toBeDisabled();
        expect(screen.getByRole('button', { name: 'Back to Start' })).toBeEnabled();
    });
});
