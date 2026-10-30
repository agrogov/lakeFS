import React, { useEffect, useState } from 'react';

import Container from 'react-bootstrap/Container';
import Breadcrumb from 'react-bootstrap/Breadcrumb';
import Stack from 'react-bootstrap/Stack';

import { useRefs } from '../../hooks/repo';
import { Navigate, Outlet, useLocation } from 'react-router-dom';
import { RepositoryNavTabs } from './tabs';
import { Link } from '../nav';
import { RefContextProvider } from '../../hooks/repo';
import { ReadOnlyBadge } from '../badges';
import { DatasetBadge } from '../datasets/datasetBadge';
import { useDatasetNames } from '../datasets/hooks';

const RepoNav = ({ isDataset = false }) => {
    const { repo, reference } = useRefs();
    const [repoId, setRepoId] = useState('');
    useEffect(() => {
        if (repo) {
            setRepoId(repo.id);
        }
    }, [repo]);

    const repoLink = {
        pathname: '/repositories/:repoId/objects',
        params: { repoId },
    };
    if (reference?.id) {
        repoLink.query = { ref: reference.id };
    }

    return (
        <Stack direction="horizontal" gap={2}>
            <Breadcrumb>
                <Link href={{ pathname: '/repositories' }} component={Breadcrumb.Item}>
                    Repositories
                </Link>
                <Link href={repoLink} component={Breadcrumb.Item}>
                    {repoId}
                </Link>
            </Breadcrumb>
            {isDataset && <DatasetBadge name={repoId} style={{ marginBottom: 16 }} />}
            <ReadOnlyBadge readOnly={repo?.read_only} style={{ marginBottom: 16 }} />
        </Stack>
    );
};

/**
 * A dataset is a read-only repository with the same name, but its pages live under
 * `/datasets/<name>/**` (DATASETS_DESIGN.md §11.3). Opening a dataset through a repository URL
 * sends the user to the dataset control-plane page instead of the read-only repository view.
 *
 * Repository listings carry no metadata, so the dataset names come from the control plane
 * (`useDatasetNames`); until they arrive the repository renders normally.
 */
const RepositoryOutlet = ({ fluid, activePage, setActivePage }) => {
    const { repo } = useRefs();
    const location = useLocation();
    const { names } = useDatasetNames();
    const isDataset = !!repo && names.has(repo.id);
    // Only the repository entry point redirects: the sub-pages stay reachable (and carry the
    // Dataset badge) so links into the read-only repository view keep working.
    const atRepositoryRoot = !!repo && location.pathname === `/repositories/${repo.id}`;
    if (isDataset && atRepositoryRoot) {
        return <Navigate to={`/datasets/${encodeURIComponent(repo.id)}/data`} replace />;
    }
    return (
        <div>
            <RepoNav isDataset={isDataset} />

            <div className="full-width-tabs-border">
                <RepositoryNavTabs active={activePage} />
            </div>

            <Container fluid={fluid}>
                <div className="mt-4">
                    <Outlet context={[setActivePage]} />
                </div>
            </Container>
        </div>
    );
};

export const RepositoryPageLayout = ({ fluid = 'sm' }) => {
    const [activePage, setActivePage] = useState('objects');
    return (
        <RefContextProvider>
            <RepositoryOutlet fluid={fluid} activePage={activePage} setActivePage={setActivePage} />
        </RefContextProvider>
    );
};
