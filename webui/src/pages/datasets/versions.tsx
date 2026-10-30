import React, { FC, useMemo } from 'react';
import Badge from 'react-bootstrap/Badge';
import Card from 'react-bootstrap/Card';
import { useParams } from 'react-router-dom';
import dayjs from 'dayjs';
import relativeTime from 'dayjs/plugin/relativeTime';
import { DiffIcon, HistoryIcon } from '@primer/octicons-react';

import { DatasetDefinition, Version } from '../../lib/api/datasets';
import { AlertError, Loading } from '../../lib/components/controls';
import { useDatasetVersion } from '../../lib/components/datasets/hooks';
import { DefinitionDiffCounts, versionChangeCounts } from '../../lib/components/datasets/definitionDiff';
import { previousVersionRef, versionTag } from '../../lib/components/datasets/versions';
import { Link } from '../../lib/components/nav';

dayjs.extend(relativeTime);

/** One timeline entry: a version plus what it changed against the version before it. */
export interface VersionTimelineEntry {
    version: Version;
    /** The definition of the version before it, or `undefined` for `v1`. */
    previous?: DatasetDefinition;
    counts: DefinitionDiffCounts;
}

/**
 * The versions of a dataset as a timeline, newest first (DATASETS_DESIGN.md §11.2).
 *
 * "Items changed" is computed from the definitions the version list already carries
 * (`versionChangeCounts`), so the timeline costs one request no matter how many versions exist.
 * The first version counts every item it declares as added.
 */
export const versionTimeline = (versions: Version[]): VersionTimelineEntry[] => {
    const byNumber = new Map<number, Version>();
    for (const version of versions) byNumber.set(version.version, version);
    return [...versions]
        .sort((left, right) => right.version - left.version)
        .map((version) => {
            const previous = byNumber.get(version.version - 1);
            return {
                version,
                previous: previous?.definition,
                counts: versionChangeCounts(previous?.definition, version.definition),
            };
        });
};

const CountsBadges: FC<{ counts: DefinitionDiffCounts }> = ({ counts }) => {
    if (counts.total === 0) return <span className="text-muted small">no item changes</span>;
    return (
        <span className="dataset-version-counts d-inline-flex align-items-center gap-1">
            <span className="dataset-version-count">
                {counts.total} item{counts.total === 1 ? '' : 's'} changed
            </span>
            {counts.added > 0 && (
                <Badge bg="success" className="dataset-count-added">
                    +{counts.added}
                </Badge>
            )}
            {counts.removed > 0 && (
                <Badge bg="danger" className="dataset-count-removed">
                    -{counts.removed}
                </Badge>
            )}
            {counts.changed > 0 && (
                <Badge bg="warning" text="dark" className="dataset-count-changed">
                    ~{counts.changed}
                </Badge>
            )}
        </span>
    );
};

const VersionEntry: FC<{ entry: VersionTimelineEntry; datasetId: string; latest: number }> = ({
    entry,
    datasetId,
    latest,
}) => {
    const { version, counts } = entry;
    const ref = versionTag(version.version);
    const previousRef = previousVersionRef(version.version);
    return (
        <div className="dataset-version-entry" data-testid={`dataset-version-${ref}`}>
            <div className="dataset-version-marker">
                <Badge bg={version.version === latest ? 'primary' : 'secondary'} className="dataset-version-badge">
                    {ref}
                </Badge>
            </div>
            <Card className="dataset-version-card mb-3">
                <Card.Body>
                    <div className="d-flex flex-wrap align-items-center gap-2">
                        <span className="dataset-version-message-title fw-semibold">
                            {version.message || <em className="text-muted">No message</em>}
                        </span>
                        {version.version === latest && (
                            <Badge bg="light" text="dark" className="border">
                                latest
                            </Badge>
                        )}
                        <span className="text-muted small ms-auto">
                            <strong>{version.publisher}</strong>{' '}
                            <span title={dayjs(version.published_at).toISOString()}>
                                {dayjs(version.published_at).fromNow()}
                            </span>
                        </span>
                    </div>
                    <div className="d-flex flex-wrap align-items-center gap-2 mt-2">
                        <CountsBadges counts={counts} />
                        <span className="text-muted small">
                            {version.commit_id.substring(0, 12)} · {version.stats?.objects.toLocaleString() ?? 0}{' '}
                            objects
                        </span>
                        <div className="ms-auto d-flex gap-2">
                            <Link
                                className="btn btn-outline-secondary btn-sm"
                                href={{
                                    pathname: '/datasets/:datasetId/data',
                                    params: { datasetId },
                                    query: { version: ref },
                                }}
                            >
                                <HistoryIcon className="me-1" /> Browse {ref}
                            </Link>
                            {previousRef && (
                                <Link
                                    className="dataset-compare-previous btn btn-outline-primary btn-sm"
                                    href={{
                                        pathname: '/datasets/:datasetId/compare',
                                        params: { datasetId },
                                        query: { left: previousRef, right: ref },
                                    }}
                                >
                                    <DiffIcon className="me-1" /> Compare with previous
                                </Link>
                            )}
                        </div>
                    </div>
                </Card.Body>
            </Card>
        </div>
    );
};

/** Timeline of the immutable versions `v1..vN` of one dataset (§11.2). */
const DatasetVersionsPage: FC = () => {
    const { datasetId = '' } = useParams();
    // The timeline always shows every version, so it never needs a selection.
    const state = useDatasetVersion(datasetId, 'latest');
    const entries = useMemo(() => versionTimeline(state.versions), [state.versions]);

    if (state.loading) return <Loading />;
    if (state.error) return <AlertError error={state.error} />;
    if (!state.dataset) return <AlertError error={new Error(`Dataset "${datasetId}" was not found`)} />;

    if (entries.length === 0) {
        return (
            <Card className="dataset-versions-empty">
                <Card.Body>
                    <h5>No versions yet</h5>
                    <p className="text-muted mb-0">
                        Every publish creates an immutable version <code>v1</code>, <code>v2</code>, … of this dataset.
                    </p>
                </Card.Body>
            </Card>
        );
    }

    const latest = state.dataset.latest_version ?? 0;
    return (
        <div className="dataset-versions-page">
            <div className="d-flex align-items-center mb-3">
                <h5 className="mb-0">Versions</h5>
                <span className="text-muted small ms-2">
                    {entries.length} version{entries.length === 1 ? '' : 's'}, newest first
                </span>
            </div>
            <div className="dataset-versions-timeline">
                {entries.map((entry) => (
                    <VersionEntry key={entry.version.version} entry={entry} datasetId={datasetId} latest={latest} />
                ))}
            </div>
        </div>
    );
};

export default DatasetVersionsPage;
