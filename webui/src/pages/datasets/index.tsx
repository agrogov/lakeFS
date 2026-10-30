import React, { FC } from 'react';
import Container from 'react-bootstrap/Container';
import { Outlet, useLocation, useNavigate } from 'react-router-dom';

import { AlertError, Loading } from '../../lib/components/controls';
import { DatasetsNavTabs } from '../../lib/components/datasets/tabs';
import { DatasetsDisabled } from '../../lib/components/datasets/gate';
import { useDatasetsConfig } from '../../lib/components/datasets/hooks';

/** The three top-level dataset views, as the tab bar presents them (DATASETS_DESIGN.md §11). */
export const DATASETS_TABS = [
    { key: 'datasets', path: '/datasets' },
    { key: 'pulls', path: '/datasets/pulls' },
    { key: 'settings', path: '/datasets/settings' },
] as const;

export type DatasetsTabKey = (typeof DATASETS_TABS)[number]['key'];

/** Which tab a location belongs to; anything below `/datasets/pulls` is the pull-requests tab. */
export const datasetsTabForPath = (pathname: string): DatasetsTabKey => {
    if (pathname.startsWith('/datasets/pulls')) return 'pulls';
    if (pathname.startsWith('/datasets/settings')) return 'settings';
    return 'datasets';
};

/**
 * The datasets layout: gate, nav tabs and the outlet for the tab pages (§11.1).
 *
 * It replaces the old Enterprise upsell. When `GET /datasets/config` reports the feature as
 * disabled - or the server does not serve the control plane (404/501) - the layout shows
 * "Datasets are disabled on this server" and no tabs: there is nothing to navigate to.
 */
const DatasetsLayout: FC = () => {
    const location = useLocation();
    const navigate = useNavigate();
    const { loading, disabled, error } = useDatasetsConfig();

    if (loading) {
        return (
            <Container fluid="xl">
                <div className="mt-4">
                    <Loading />
                </div>
            </Container>
        );
    }
    if (error && !disabled) {
        return (
            <Container fluid="xl">
                <div className="mt-4">
                    <AlertError error={error} />
                </div>
            </Container>
        );
    }
    if (disabled) {
        return (
            <Container fluid="xl">
                <div className="mt-4">
                    <DatasetsDisabled />
                </div>
            </Container>
        );
    }

    const activeTab = datasetsTabForPath(location.pathname);
    return (
        <div>
            <div className="full-width-tabs-border">
                <Container fluid="xl">
                    <DatasetsNavTabs
                        active={activeTab}
                        onSelect={(eventKey) => {
                            const tab = DATASETS_TABS.find((candidate) => candidate.key === eventKey);
                            if (tab) navigate(tab.path);
                        }}
                    />
                </Container>
            </div>
            <Container fluid="xl">
                <div className="mt-4">
                    <Outlet />
                </div>
            </Container>
        </div>
    );
};

export default DatasetsLayout;
