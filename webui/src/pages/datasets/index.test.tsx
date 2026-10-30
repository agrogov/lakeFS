import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Route, Routes } from 'react-router-dom';

import DatasetsLayout, { datasetsTabForPath } from './index';

const { configState } = vi.hoisted(() => ({
    configState: { current: { loading: false, disabled: false, error: null, config: { enabled: true } } },
}));

vi.mock('../../lib/components/datasets/hooks', () => ({
    useDatasetsConfig: () => configState.current,
}));

const renderLayout = (path = '/datasets') =>
    render(
        <MemoryRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }} initialEntries={[path]}>
            <Routes>
                <Route path="/datasets" element={<DatasetsLayout />}>
                    <Route index element={<div>datasets list</div>} />
                    <Route path="pulls" element={<div>all pull requests</div>} />
                    <Route path="settings" element={<div>dataset settings</div>} />
                </Route>
            </Routes>
        </MemoryRouter>,
    );

describe('DatasetsLayout', () => {
    beforeEach(() => {
        configState.current = { loading: false, disabled: false, error: null, config: { enabled: true } };
    });

    afterEach(() => {
        vi.unstubAllGlobals();
    });

    it('renders the dataset tabs and the active tab page when datasets are enabled', () => {
        renderLayout();

        expect(screen.getByText('Datasets')).toBeInTheDocument();
        expect(screen.getByText('Pull Requests')).toBeInTheDocument();
        expect(screen.getByText('Settings')).toBeInTheDocument();
        expect(screen.getByText('datasets list')).toBeInTheDocument();
    });

    it('says datasets are disabled when the config reports enabled: false', () => {
        configState.current = { loading: false, disabled: true, error: null, config: { enabled: false } };
        renderLayout();

        expect(screen.getByText('Datasets are disabled on this server')).toBeInTheDocument();
        expect(screen.queryByText('Pull Requests')).not.toBeInTheDocument();
        expect(screen.queryByText('datasets list')).not.toBeInTheDocument();
    });

    it('treats a server without the control plane as disabled', () => {
        configState.current = {
            loading: false,
            disabled: true,
            error: new Error('not found'),
            config: null,
        };
        renderLayout();

        expect(screen.getByText('Datasets are disabled on this server')).toBeInTheDocument();
    });

    it('shows a loading state while the config is fetched', () => {
        configState.current = { loading: true, disabled: false, error: null, config: null };
        renderLayout();

        // The spinner and its caption.
        expect(screen.getAllByText('Loading...').length).toBeGreaterThan(0);
        expect(screen.queryByText('Datasets are disabled on this server')).not.toBeInTheDocument();
    });

    it('navigates between the tabs', async () => {
        renderLayout();

        await userEvent.click(screen.getByText('Pull Requests'));
        expect(await screen.findByText('all pull requests')).toBeInTheDocument();
    });
});

describe('datasetsTabForPath', () => {
    it('selects the tab that owns a path', () => {
        expect(datasetsTabForPath('/datasets')).toBe('datasets');
        expect(datasetsTabForPath('/datasets/pulls')).toBe('pulls');
        expect(datasetsTabForPath('/datasets/settings')).toBe('settings');
    });
});
