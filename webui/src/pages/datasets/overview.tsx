import React, { FC } from 'react';
import Badge from 'react-bootstrap/Badge';
import Card from 'react-bootstrap/Card';
import Col from 'react-bootstrap/Col';
import Row from 'react-bootstrap/Row';
import { useParams } from 'react-router-dom';
import dayjs from 'dayjs';
import relativeTime from 'dayjs/plugin/relativeTime';

import { Dataset, DatasetStats } from '../../lib/api/datasets';
import { AlertError, Loading } from '../../lib/components/controls';
import { useDatasetVersion, versionNumbers } from '../../lib/components/datasets/hooks';
import { DefinitionItemsTable } from '../../lib/components/datasets/definitionItemsTable';
import { MetadataTable } from '../../lib/components/datasets/metadataTable';
import { DatasetUriCopyButtons } from '../../lib/components/datasets/uriCopy';
import { DatasetVersionPicker } from '../../lib/components/datasets/versionPicker';
import { VersionSelection, selectionToRef } from '../../lib/components/datasets/versions';
import { useRouter } from '../../lib/hooks/router';
import { Link } from '../../lib/components/nav';
import { humanSize } from '../../lib/components/repository/tree';

dayjs.extend(relativeTime);

/** The query parameter the page keeps the selected version in. */
export const VERSION_QUERY_KEY = 'version';

const StateBadge: FC<{ state: Dataset['state'] }> = ({ state }) => {
    if (state === 'active') return null;
    return (
        <Badge bg="warning" text="dark" className="ms-2 dataset-state-badge">
            {state}
        </Badge>
    );
};

const Stat: FC<{ label: string; value: string; title?: string }> = ({ label, value, title }) => (
    <Col xs={6} md={3} className="dataset-stat">
        <div className="dataset-stat-label text-muted small">{label}</div>
        <div className="dataset-stat-value" title={title}>
            {value}
        </div>
    </Col>
);

const StatsRow: FC<{ stats?: DatasetStats; items: number; sources: number }> = ({ stats, items, sources }) => (
    <Row className="dataset-stats g-3">
        <Stat label="Objects" value={stats ? stats.objects.toLocaleString() : '—'} />
        <Stat
            label="Bytes"
            value={stats ? humanSize(stats.bytes) : '—'}
            title={stats ? `${stats.bytes} bytes` : undefined}
        />
        <Stat label="Items" value={items.toLocaleString()} />
        <Stat label="Source repositories" value={sources.toLocaleString()} />
    </Row>
);

/**
 * Overview of one dataset (DATASETS_DESIGN.md §11.2): the header with the name, the version
 * selector and the two URIs the data is reachable through, then the items table with links to
 * each source, the metadata table and the statistics of the selected version.
 *
 * The version lives in the URL (`?version=v2`), so a specific version can be linked to and
 * survives a reload, exactly like a ref on the repository pages.
 */
const DatasetOverviewPage: FC = () => {
    const { datasetId = '' } = useParams();
    const router = useRouter();
    const state = useDatasetVersion(datasetId, router.query[VERSION_QUERY_KEY] ?? 'latest');
    const { dataset, version, definition, stats, selection } = state;

    const onSelectVersion = (value: VersionSelection) => {
        const query: Record<string, string> = {};
        if (value !== 'latest') query[VERSION_QUERY_KEY] = value;
        router.push({ pathname: '/datasets/:datasetId/overview', params: { datasetId }, query });
    };

    if (state.loading) return <Loading />;
    if (state.error) return <AlertError error={state.error} />;
    if (!dataset) return <AlertError error={new Error(`Dataset "${datasetId}" was not found`)} />;

    const items = definition?.data ?? [];
    const sources = new Set(items.map((item) => item.repository)).size;
    const ref = selectionToRef(selection);
    const hasVersions = (dataset.latest_version ?? 0) > 0;

    return (
        <div className="dataset-overview-page">
            <Card className="mb-4 dataset-overview-header">
                <Card.Body>
                    <div className="d-flex flex-wrap align-items-center gap-2">
                        <h4 className="mb-0 dataset-overview-name">{dataset.name}</h4>
                        <StateBadge state={dataset.state} />
                        <div className="ms-auto d-flex align-items-center gap-2 dataset-overview-actions">
                            <DatasetVersionPicker
                                versions={versionNumbers(state.versions)}
                                latest={dataset.latest_version}
                                value={selection}
                                onChange={onSelectVersion}
                                label="Version"
                                id="dataset-overview-version"
                            />
                            <DatasetUriCopyButtons dataset={dataset.name} versionRef={ref} />
                        </div>
                    </div>

                    {dataset.description && <p className="text-muted mt-3 mb-0">{dataset.description}</p>}

                    <div className="text-muted small mt-2 dataset-overview-origin">
                        Created by <strong>{dataset.created_by}</strong>{' '}
                        <span title={dayjs(dataset.created_at).toISOString()}>
                            {dayjs(dataset.created_at).fromNow()}
                        </span>
                        {version && (
                            <>
                                <span className="mx-2">&middot;</span>
                                <span className="dataset-version-ref">{`v${version.version}`}</span> published by{' '}
                                <strong>{version.publisher}</strong>{' '}
                                <span title={dayjs(version.published_at).toISOString()}>
                                    {dayjs(version.published_at).fromNow()}
                                </span>
                            </>
                        )}
                        {version?.message && <div className="dataset-version-message mt-1">{version.message}</div>}
                    </div>
                </Card.Body>
            </Card>

            <Card className="mb-4 dataset-overview-stats">
                <Card.Body>
                    <StatsRow stats={stats} items={items.length} sources={sources} />
                </Card.Body>
            </Card>

            <Card className="mb-4 dataset-overview-items">
                <Card.Header className="d-flex align-items-center">
                    <span className="fw-semibold">Items</span>
                    {hasVersions && (
                        <Link
                            href={{ pathname: '/datasets/:datasetId/data', params: { datasetId } }}
                            className="ms-auto small"
                        >
                            Browse data
                        </Link>
                    )}
                </Card.Header>
                <Card.Body>
                    <DefinitionItemsTable
                        items={items}
                        stats={stats}
                        emptyText={
                            hasVersions
                                ? 'This version has no items'
                                : 'No version has been published yet, so the dataset has no data'
                        }
                    />
                </Card.Body>
            </Card>

            <Card className="dataset-overview-metadata">
                <Card.Header className="fw-semibold">Metadata</Card.Header>
                <Card.Body>
                    <MetadataTable metadata={definition?.metadata} />
                </Card.Body>
            </Card>
        </div>
    );
};

export default DatasetOverviewPage;
