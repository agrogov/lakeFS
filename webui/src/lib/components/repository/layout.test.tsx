import React, { ReactNode } from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MemoryRouter, Navigate, Route, Routes, useLocation } from 'react-router-dom';

import { RepositoryPageLayout } from './layout';

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

// The dataset names the (mocked) control-plane hook resolves to, once its "request" lands.
const { datasetNames } = vi.hoisted(() => ({ datasetNames: { current: new Set<string>() } }));

// Both hooks answer asynchronously, as the real ones do, so the layout starts out loading.
vi.mock('../../hooks/repo', async () => {
    const ReactModule = await import('react');
    const RouterModule = await import('react-router-dom');
    const useRefs = () => {
        const { pathname } = RouterModule.useLocation();
        const repoId = pathname.split('/')[2] ?? '';
        const [state, setState] = ReactModule.useState<{
            repo: { id: string; read_only: boolean; default_branch: string } | null;
            reference: { id: string; type: string } | null;
            loading: boolean;
        }>({ repo: null, reference: null, loading: true });
        ReactModule.useEffect(() => {
            const timer = setTimeout(
                () =>
                    setState({
                        repo: { id: repoId, read_only: true, default_branch: 'main' },
                        reference: { id: 'main', type: 'branch' },
                        loading: false,
                    }),
                5,
            );
            return () => clearTimeout(timer);
        }, []);
        return state;
    };
    const RefContextProvider = ({ children }: { children: ReactNode }) =>
        ReactModule.createElement(ReactModule.Fragment, null, children);
    return { useRefs, RefContextProvider };
});

vi.mock('../datasets/hooks', async () => {
    const ReactModule = await import('react');
    const useDatasetNames = () => {
        const [state, setState] = ReactModule.useState<{ names: Set<string>; loading: boolean }>({
            names: new Set<string>(),
            loading: true,
        });
        ReactModule.useEffect(() => {
            const timer = setTimeout(() => setState({ names: datasetNames.current, loading: false }), 5);
            return () => clearTimeout(timer);
        }, []);
        return state;
    };
    return { useDatasetNames };
});

const PathProbe = () => <div data-testid="path">{useLocation().pathname}</div>;

const renderRepository = (path: string) =>
    render(
        <MemoryRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }} initialEntries={[path]}>
            <Routes>
                <Route path="/repositories/:repoId" element={<RepositoryPageLayout />}>
                    <Route index element={<Navigate to="objects" />} />
                    <Route path="objects" element={<PathProbe />} />
                </Route>
                <Route
                    path="/datasets/:datasetId/data"
                    element={
                        <>
                            <div>dataset data page</div>
                            <PathProbe />
                        </>
                    }
                />
            </Routes>
        </MemoryRouter>,
    );

describe('RepositoryPageLayout', () => {
    afterEach(() => {
        datasetNames.current = new Set<string>();
        vi.unstubAllGlobals();
    });

    it('waits for the dataset names before the index route can navigate away (UI-1)', async () => {
        datasetNames.current = new Set(['ds']);
        renderRepository('/repositories/ds');

        // The dataset control-plane page is where a dataset repository belongs.
        expect(await screen.findByText('dataset data page')).toBeInTheDocument();
        expect(screen.getByTestId('path')).toHaveTextContent('/datasets/ds/data');
    });

    it('leaves an ordinary repository to the index route', async () => {
        renderRepository('/repositories/plain');

        expect(await screen.findByTestId('path')).toHaveTextContent('/repositories/plain/objects');
    });
});
