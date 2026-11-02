import React from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router-dom';

import DatasetPageLayout from './datasetPageLayout';

const { canManageAccess } = vi.hoisted(() => ({ canManageAccess: { current: false } }));

// The layout asks the permission hook and the feature gate; both live in the datasets hooks module.
vi.mock('../../lib/components/datasets/hooks', () => ({
    DATASETS_MANAGE_ACCESS_ACTION: 'dataset:ManageAccess',
    datasetResource: (name: string) => `arn:lakefs:dataset:::dataset/${name}`,
    useCan: () => canManageAccess.current,
    useDatasetsConfig: () => ({ loading: false, disabled: false, error: null, config: { enabled: true } }),
}));

const renderLayout = () =>
    render(
        <MemoryRouter
            future={{ v7_startTransition: true, v7_relativeSplatPath: true }}
            initialEntries={['/datasets/customer-360/overview']}
        >
            <Routes>
                <Route path="/datasets/:datasetId" element={<DatasetPageLayout />}>
                    <Route path="overview" element={<div>overview page</div>} />
                </Route>
            </Routes>
        </MemoryRouter>,
    );

describe('DatasetPageLayout', () => {
    afterEach(() => {
        canManageAccess.current = false;
    });

    it('offers the Access tab only to a caller who may manage access (UI-7)', () => {
        renderLayout();

        expect(screen.getByText('overview page')).toBeInTheDocument();
        expect(screen.queryByText('Access')).not.toBeInTheDocument();
    });

    it('shows the Access tab for a caller holding dataset:ManageAccess', () => {
        canManageAccess.current = true;
        renderLayout();

        expect(screen.getByText('Access')).toBeInTheDocument();
    });
});
