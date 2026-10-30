import React, { FC } from 'react';
import CSS from 'csstype';
import Badge from 'react-bootstrap/Badge';
import { Link as RouterLink } from 'react-router-dom';

import { isDatasetRepoMetadata } from './metadata';

/**
 * The "Dataset" badge (DATASETS_DESIGN.md §11.3).
 *
 * Dataset repositories are filtered out of the repositories page, but they are still visible
 * in places that list repositories without a dataset-aware layout (the repository breadcrumb
 * while a redirect is in flight, or any future repo picker). The badge links to the dataset
 * control-plane page so those places stay navigable.
 */
export const DatasetBadge: FC<{ name: string; className?: string; style?: CSS.Properties }> = ({
    name,
    className = '',
    style = {},
}) => (
    <Badge
        bg="info"
        text="dark"
        as={RouterLink}
        to={`/datasets/${encodeURIComponent(name)}`}
        className={`dataset-badge ${className}`.trim()}
        style={style}
        title={`${name} is a dataset`}
    >
        Dataset
    </Badge>
);

/** Renders the badge for a repository object, or nothing when it is an ordinary repository. */
export const DatasetBadgeForRepo: FC<{ repo: { id: string; metadata?: Record<string, string> } | null }> = ({
    repo,
}) => {
    if (!repo || !isDatasetRepoMetadata(repo.metadata)) return null;
    return <DatasetBadge name={repo.id} />;
};
