import React, { FC, useCallback, useMemo, useState } from 'react';
import Alert from 'react-bootstrap/Alert';
import Card from 'react-bootstrap/Card';
import Nav from 'react-bootstrap/Nav';
import { useParams } from 'react-router-dom';

import { refs, repositories } from '../../lib/api';
import { DefinitionDiff, datasets } from '../../lib/api/datasets';
import { AlertError, Loading } from '../../lib/components/controls';
import { useDatasetVersion, versionNumbers } from '../../lib/components/datasets/hooks';
import { DefinitionDiffTable } from '../../lib/components/datasets/definitionDiffTable';
// The proposal detail renders the same object diff as a table, so the `_dataset/` filtering
// lives beside both of them. Re-exported here for the compare page's own callers.
import { dataDiffEntries, INTERNAL_DATASET_PREFIX } from '../../lib/components/datasets/dataDiffTable';
import { DatasetVersionPicker } from '../../lib/components/datasets/versionPicker';
import { VersionSelection, normaliseSelection, versionTag } from '../../lib/components/datasets/versions';
import { ChangesTreeContainer } from '../../lib/components/repository/changes';
import { RefContext, refContextValue } from '../../lib/hooks/repo';
import { useAPI, useAPIWithPagination } from '../../lib/hooks/api';
import { useRouter } from '../../lib/hooks/router';
import { RefTypeTag } from '../../constants';

/** Query parameters: the two refs and the active tab. */
export const COMPARE_QUERY_KEYS = { left: 'left', right: 'right', tab: 'tab' } as const;

/** The tabs of the compare page (§11.2). */
export const COMPARE_TABS = [
    { key: 'definition', label: 'Definition' },
    { key: 'data', label: 'Data' },
] as const;

export type CompareTabKey = (typeof COMPARE_TABS)[number]['key'];

export const isCompareTab = (value: string | undefined): value is CompareTabKey =>
    value === 'definition' || value === 'data';

/**
 * The synthetic `_dataset/` entries every version carries: the definition document, not data
 * (§6.4, §10.3). The control plane's own data diff filters them out, and so does this page.
 */
export { dataDiffEntries, INTERNAL_DATASET_PREFIX };

/** The default comparison: the newest version against the one before it. */
export const defaultCompareRefs = (latest: number): { left: string; right: string } => {
    const right = latest > 0 ? versionTag(latest) : 'latest';
    const left = latest > 1 ? versionTag(latest - 1) : right;
    return { left, right };
};

const CompareHeader: FC<{
    versions: number[];
    latest: number;
    left: string;
    right: string;
    onChange: (refs: { left?: string; right?: string }) => void;
}> = ({ versions, latest, left, right, onChange }) => (
    <Card className="mb-3 dataset-compare-header">
        <Card.Body className="d-flex flex-wrap align-items-end gap-3">
            <div>
                <div className="text-muted small mb-1">Compare</div>
                <DatasetVersionPicker
                    versions={versions}
                    latest={latest}
                    value={left}
                    onChange={(value: VersionSelection) => onChange({ left: value })}
                    label="Left version"
                    id="dataset-compare-left"
                    size="sm"
                />
            </div>
            <div className="text-muted pb-2">with</div>
            <div>
                <div className="text-muted small mb-1">To</div>
                <DatasetVersionPicker
                    versions={versions}
                    latest={latest}
                    value={right}
                    onChange={(value: VersionSelection) => onChange({ right: value })}
                    label="Right version"
                    id="dataset-compare-right"
                    size="sm"
                />
            </div>
        </Card.Body>
    </Card>
);

const DefinitionTab: FC<{ datasetId: string; left: string; right: string }> = ({ datasetId, left, right }) => {
    const { response, loading, error } = useAPI(
        () => datasets.diffDefinitions(datasetId, left, right),
        [datasetId, left, right],
    );

    if (loading) return <Loading />;
    if (error) return <AlertError error={error as Error} />;
    return <DefinitionDiffTable diff={(response as DefinitionDiff | null) ?? { items: [] }} />;
};

/** What `ObjectsDiff` reads from the ref context to resolve the storage configuration. */
interface DiffRepo {
    id: string;
    storage_id?: string;
    storage_namespace?: string;
}

const DataTab: FC<{
    datasetId: string;
    left: string;
    right: string;
    repo: DiffRepo;
    reference: { id: string; type: string };
}> = ({ datasetId, left, right, repo, reference }) => {
    const [, setAfterUpdated] = useState('');

    const getMore = useCallback(
        async (after: string, path: string, useDelimiter = true, amount = -1) => {
            const page = await refs.diff(
                datasetId,
                left,
                right,
                after,
                path,
                useDelimiter ? '/' : '',
                amount > 0 ? amount : undefined,
            );
            return { ...page, results: dataDiffEntries(page.results) };
        },
        [datasetId, left, right],
    );

    const { results, loading, error, nextPage } = useAPIWithPagination(async () => {
        const page = await refs.diff(datasetId, left, right, '', '', '/');
        return { ...page, results: dataDiffEntries(page.results) };
    }, [datasetId, left, right]);

    if (loading) return <Loading />;
    if (error) return <AlertError error={error as Error} />;

    return (
        <RefContext.Provider value={refContextValue({ repo, reference }) as never}>
            <ChangesTreeContainer
                results={results}
                delimiter="/"
                uriNavigator={
                    <span className="text-muted small">
                        {left} &rarr; {right}
                    </span>
                }
                leftDiffRefID={left}
                rightDiffRefID={right}
                repo={repo}
                reference={reference}
                internalRefresh={false}
                prefix=""
                getMore={getMore}
                loading={loading}
                nextPage={nextPage}
                setAfterUpdated={setAfterUpdated}
                // A changed prefix opens the data browser at that folder on the right version;
                // there is nothing to revert or to paginate outside the tree itself.
                onNavigate={(entry: { path: string }) => ({
                    pathname: '/datasets/:datasetId/data',
                    params: { datasetId },
                    query: { version: right, path: entry.path },
                })}
                onRevert={undefined}
                emptyStateComponent={undefined}
                changesTreeMessage={
                    <p className="mt-2 ps-3 text-muted small">
                        Data changes between <code>{left}</code> and <code>{right}</code>
                    </p>
                }
                noChangesText="No data changes between these versions"
            />
        </RefContext.Provider>
    );
};

/**
 * Definition and data diff between two versions of a dataset (DATASETS_DESIGN.md §11.2, §10.3).
 *
 * Two version pickers drive both tabs. "Definition" asks the control plane for the structural
 * diff of the two definitions; "Data" reuses the repository compare tree (`ChangesTreeContainer`
 * and its inline `ObjectsDiff`) against the dataset repository itself, with the two versions as
 * the refs - a dataset is a repository, so its metarange diff is the data diff.
 */
const DatasetComparePage: FC = () => {
    const { datasetId = '' } = useParams();
    const router = useRouter();
    const state = useDatasetVersion(datasetId, 'latest');
    const latest = state.dataset?.latest_version ?? 0;
    const defaults = defaultCompareRefs(latest);
    const leftParam = router.query[COMPARE_QUERY_KEYS.left];
    const rightParam = router.query[COMPARE_QUERY_KEYS.right];
    const left = leftParam ? normaliseSelection(leftParam, latest) : defaults.left;
    const right = rightParam ? normaliseSelection(rightParam, latest) : defaults.right;
    const tabParam = router.query[COMPARE_QUERY_KEYS.tab];
    const tab: CompareTabKey = isCompareTab(tabParam) ? tabParam : 'definition';

    const { response: repo } = useAPI(() => repositories.get(datasetId), [datasetId]);
    const reference = useMemo(() => ({ id: right, type: RefTypeTag }), [right]);
    // Before the repository record arrives only its id is known; every consumer of the ref
    // context works with that alone on a single-blockstore server.
    const diffRepo: DiffRepo = (repo as DiffRepo | null) ?? { id: datasetId };
    const versions = useMemo(() => versionNumbers(state.versions), [state.versions]);

    const push = (next: { left?: string; right?: string; tab?: CompareTabKey }) => {
        const query: Record<string, string> = {};
        const nextLeft = next.left ?? left;
        const nextRight = next.right ?? right;
        const nextTab = next.tab ?? tab;
        if (nextLeft) query[COMPARE_QUERY_KEYS.left] = nextLeft;
        if (nextRight) query[COMPARE_QUERY_KEYS.right] = nextRight;
        if (nextTab !== 'definition') query[COMPARE_QUERY_KEYS.tab] = nextTab;
        router.push({ pathname: '/datasets/:datasetId/compare', params: { datasetId }, query });
    };

    if (state.loading) return <Loading />;
    if (state.error) return <AlertError error={state.error} />;
    if (!state.dataset) return <AlertError error={new Error(`Dataset "${datasetId}" was not found`)} />;

    if (latest < 1) {
        return (
            <Alert variant="secondary" className="dataset-compare-empty">
                This dataset has no published version yet, so there is nothing to compare.
            </Alert>
        );
    }

    return (
        <div className="dataset-compare-page">
            <CompareHeader versions={versions} latest={latest} left={left} right={right} onChange={push} />

            <Nav
                variant="tabs"
                activeKey={tab}
                onSelect={(eventKey) => push({ tab: (eventKey as CompareTabKey) ?? 'definition' })}
                className="datasets-nav-tabs dataset-compare-tabs mb-3"
            >
                {COMPARE_TABS.map((entry) => (
                    <Nav.Item key={entry.key}>
                        <Nav.Link eventKey={entry.key}>{entry.label}</Nav.Link>
                    </Nav.Item>
                ))}
            </Nav>

            {tab === 'definition' ? (
                <DefinitionTab datasetId={datasetId} left={left} right={right} />
            ) : (
                <DataTab datasetId={datasetId} left={left} right={right} repo={diffRepo} reference={reference} />
            )}
        </div>
    );
};

export default DatasetComparePage;
