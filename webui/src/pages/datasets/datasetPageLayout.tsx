import React, { FC } from 'react';
import Container from 'react-bootstrap/Container';
import Breadcrumb from 'react-bootstrap/Breadcrumb';
import Nav from 'react-bootstrap/Nav';
import { Outlet, useLocation, useParams } from 'react-router-dom';
import {
    BookIcon,
    DiffIcon,
    FileDirectoryIcon,
    GitPullRequestIcon,
    HistoryIcon,
    ShieldIcon,
} from '@primer/octicons-react';

import { Link } from '../../lib/components/nav';
import { DatasetsGate } from '../../lib/components/datasets/gate';

/** The pages of one dataset, as the dataset nav presents them (§11.1, §11.2). */
export const DATASET_PAGES = [
    { key: 'overview', label: 'Overview', icon: BookIcon },
    { key: 'data', label: 'Data', icon: FileDirectoryIcon },
    { key: 'versions', label: 'Versions', icon: HistoryIcon },
    { key: 'compare', label: 'Compare', icon: DiffIcon },
    { key: 'proposals', label: 'Pull Requests', icon: GitPullRequestIcon },
    { key: 'access', label: 'Access', icon: ShieldIcon },
] as const;

/** The dataset page a pathname points at; nested proposal routes belong to `proposals`. */
export const datasetPageForPath = (pathname: string): string => {
    const match = /^\/datasets\/[^/]+\/([^/]+)/.exec(pathname);
    return match ? match[1] : 'overview';
};

const DatasetNav: FC<{ datasetId: string; active: string }> = ({ datasetId, active }) => (
    <Nav variant="tabs" className="datasets-nav-tabs" activeKey={active}>
        {DATASET_PAGES.map((page) => (
            <Nav.Item key={page.key}>
                <Link
                    component={Nav.Link}
                    href={{ pathname: `/datasets/:datasetId/${page.key}`, params: { datasetId } }}
                    eventKey={page.key}
                >
                    <page.icon /> {page.label}
                </Link>
            </Nav.Item>
        ))}
    </Nav>
);

/**
 * Layout of one dataset (§11.1): breadcrumb, page tabs and the outlet for the page.
 *
 * Reads of the dataset itself are left to the pages so each one fetches only what it shows.
 */
const DatasetPageLayout: FC = () => {
    const { datasetId = '' } = useParams();
    const location = useLocation();
    const active = datasetPageForPath(location.pathname);

    return (
        <DatasetsGate>
            <Breadcrumb>
                <Link href={{ pathname: '/datasets' }} component={Breadcrumb.Item}>
                    Datasets
                </Link>
                <Link
                    href={{ pathname: '/datasets/:datasetId/overview', params: { datasetId } }}
                    component={Breadcrumb.Item}
                >
                    {datasetId}
                </Link>
            </Breadcrumb>
            <div className="full-width-tabs-border">
                <Container fluid="xl">
                    <DatasetNav datasetId={datasetId} active={active} />
                </Container>
            </div>
            <Container fluid="xl">
                <div className="mt-4">
                    <Outlet />
                </div>
            </Container>
        </DatasetsGate>
    );
};

export default DatasetPageLayout;
