import React, { FC, useMemo } from 'react';
import Alert from 'react-bootstrap/Alert';
import Button from 'react-bootstrap/Button';
import Card from 'react-bootstrap/Card';
import { TagIcon } from '@primer/octicons-react';
import { useParams } from 'react-router-dom';
import dayjs from 'dayjs';
import relativeTime from 'dayjs/plugin/relativeTime';

import { objects, repositories } from '../../lib/api';
import { AlertError, Loading } from '../../lib/components/controls';
import { useDatasetVersion, versionNumbers } from '../../lib/components/datasets/hooks';
import { DatasetUriCopyButtons } from '../../lib/components/datasets/uriCopy';
import { DatasetVersionPicker } from '../../lib/components/datasets/versionPicker';
import { VersionSelection, selectionToRef, versionTag } from '../../lib/components/datasets/versions';
import { Tree } from '../../lib/components/repository/tree';
import { useAPI, useAPIWithPagination } from '../../lib/hooks/api';
import { useConfigContext } from '../../lib/hooks/configProvider';
import { useRouter } from '../../lib/hooks/router';
import { RefTypeBranch, RefTypeTag } from '../../constants';
import { getRepoStorageConfig } from '../repositories/repository/utils';
import { FileContents, getContentType, getFileExtension } from '../repositories/repository/objectViewer';

/**
 * Query parameters of the page: the version, an explicit ref, the prefix to list and the
 * pagination cursor.
 *
 * `version` selects a published version and is the normal way in. `ref` browses an arbitrary
 * ref of the dataset repository, which is how a proposal's preview branch is reached
 * (`data?ref=_proposal-<id>`, DATASETS_DESIGN.md §5.6, §11.2). It wins over `version` when
 * both are present, and selecting a version clears it.
 */
export const DATA_QUERY_KEYS = { version: 'version', ref: 'ref', path: 'path', after: 'after' } as const;

// The repository tree renders "modified 3 days ago" with the relative-time plugin, which the
// repository pages register on import. This page imports the tree directly, so it registers it.
dayjs.extend(relativeTime);

/** A dataset repository as the object browser needs it: read-only, with the real storage. */
export interface DatasetRepo {
    id: string;
    read_only: boolean;
    storage_id?: string;
    storage_namespace?: string;
}

/**
 * The object browser of one dataset (DATASETS_DESIGN.md §11.2).
 *
 * A dataset *is* a read-only repository with the same name (§4.2), so this page reuses the
 * repository tree and object viewer against it with `ref` = the selected version. The tree is
 * always pinned to the version **tag** (`v3`), never to the `latest` branch: a tag reference is
 * immutable and the repository components key their write controls off the reference type
 * (`EntryRowActions` only offers "Delete" for a branch, `UploadButton` only for a branch), so the
 * read-only invariant holds for every version without a special-case flag. The page itself adds
 * no upload, import, commit or revert control.
 */
const DatasetDataPage: FC = () => {
    const { datasetId = '' } = useParams();
    const router = useRouter();
    const state = useDatasetVersion(datasetId, router.query[DATA_QUERY_KEYS.version] ?? 'latest');
    const { selection, versionNumber, dataset } = state;

    const path = router.query[DATA_QUERY_KEYS.path] ?? '';
    const after = router.query[DATA_QUERY_KEYS.after] ?? '';
    const isObjectView = path !== '' && !path.endsWith('/');

    // A proposal preview branch (`_proposal-<id>`, §5.6) is not a version: browse it directly.
    const previewRef = router.query[DATA_QUERY_KEYS.ref] ?? '';

    const {
        response: repo,
        loading: repoLoading,
        error: repoError,
    } = useAPI(() => repositories.get(datasetId), [datasetId]);
    const { config, loading: configLoading, error: configError } = useConfigContext();
    const { storageConfig, error: storageConfigError } = getRepoStorageConfig(config?.storages, repo as never);

    // Browse through the immutable tag of the version, so the listing can never move and no
    // write control can appear (see the component comment). A proposal preview branch is
    // browsed through its own branch ref instead: it is a hidden, throw-away ref that the
    // control plane deletes on merge/close (§5.6), and the page hides every write control.
    const browseRef = previewRef !== '' ? previewRef : versionNumber > 0 ? versionTag(versionNumber) : '';
    const reference = useMemo(
        () => ({ id: browseRef, type: previewRef !== '' ? RefTypeBranch : RefTypeTag }),
        [browseRef, previewRef],
    );
    const isPreview = previewRef !== '';

    const {
        results,
        loading: listLoading,
        error: listError,
        nextPage,
    } = useAPIWithPagination(
        () =>
            browseRef === ''
                ? Promise.resolve({ results: [], pagination: {} })
                : objects.list(datasetId, browseRef, path, after, storageConfig?.pre_sign_support_ui ?? false),
        [datasetId, browseRef, path, after, storageConfig?.pre_sign_support_ui],
    );

    const {
        response: head,
        loading: headLoading,
        error: headError,
    } = useAPI(
        () => (isObjectView && browseRef !== '' ? objects.head(datasetId, browseRef, path) : Promise.resolve(null)),
        [datasetId, browseRef, path, isObjectView],
    );

    const pushPath = (nextPath: string, nextAfter = '') => {
        const query: Record<string, string> = {};
        if (isPreview) query[DATA_QUERY_KEYS.ref] = previewRef;
        else if (selection !== 'latest') query[DATA_QUERY_KEYS.version] = selection;
        if (nextPath !== '') query[DATA_QUERY_KEYS.path] = nextPath;
        if (nextAfter !== '') query[DATA_QUERY_KEYS.after] = nextAfter;
        router.push({ pathname: '/datasets/:datasetId/data', params: { datasetId }, query });
    };

    // Leaving the preview keeps the browsed path but drops `ref`, so the page goes back to the
    // published data. Picking `latest` in the version picker cannot do this: it is already the
    // selected value while previewing, so no change event would fire.
    const exitPreview = () => {
        const query: Record<string, string> = {};
        if (path !== '') query[DATA_QUERY_KEYS.path] = path;
        router.push({ pathname: '/datasets/:datasetId/data', params: { datasetId }, query });
    };

    // Picking a version leaves the preview branch: the version picker is the way back to the
    // published data, so it drops `ref` instead of combining the two.
    const onSelectVersion = (value: VersionSelection) => {
        const query: Record<string, string> = {};
        if (value !== 'latest') query[DATA_QUERY_KEYS.version] = value;
        if (path !== '') query[DATA_QUERY_KEYS.path] = path;
        router.push({ pathname: '/datasets/:datasetId/data', params: { datasetId }, query });
    };

    if (state.loading || repoLoading || configLoading) return <Loading />;
    const error = state.error ?? repoError ?? configError ?? storageConfigError;
    if (error) return <AlertError error={error as Error} />;
    if (!dataset) return <AlertError error={new Error(`Dataset "${datasetId}" was not found`)} />;

    const header = (
        <Card className="mb-3 dataset-data-header">
            <Card.Body className="d-flex flex-wrap align-items-center gap-2">
                <div className="d-flex align-items-center gap-2">
                    <span className="text-muted small">Version</span>
                    <DatasetVersionPicker
                        versions={versionNumbers(state.versions)}
                        latest={dataset.latest_version}
                        value={selection}
                        onChange={onSelectVersion}
                        label="Version"
                        id="dataset-data-version"
                        size="sm"
                    />
                </div>
                <DatasetUriCopyButtons
                    className="ms-auto"
                    size="sm"
                    dataset={datasetId}
                    versionRef={isPreview ? previewRef : selectionToRef(selection)}
                    path={path}
                />
            </Card.Body>
        </Card>
    );

    const previewNotice = isPreview ? (
        <Alert variant="info" className="dataset-data-preview d-flex flex-wrap align-items-center gap-2">
            <span>
                Previewing the proposal branch <code>{previewRef}</code>.
            </span>
            <Button variant="link" size="sm" className="dataset-data-preview-exit p-0" onClick={exitPreview}>
                Back to published data
            </Button>
        </Alert>
    ) : null;

    if (!isPreview && versionNumber < 1) {
        return (
            <>
                {header}
                <Alert variant="secondary" className="dataset-data-empty">
                    This dataset has no published version yet, so there is no data to browse.
                </Alert>
            </>
        );
    }

    const repoForTree = {
        id: datasetId,
        // The repository components disable their write paths through these two flags; a dataset
        // repository is read-only by construction (§7.3).
        readOnly: true,
        read_only: true,
        storage_id: (repo as DatasetRepo | null)?.storage_id,
        storage_namespace: (repo as DatasetRepo | null)?.storage_namespace,
    };

    if (isObjectView) {
        const parentPath = path.slice(0, path.lastIndexOf('/') + 1);
        const loading = headLoading;
        return (
            <>
                {header}
                {previewNotice}
                <div className="d-flex align-items-center mb-2">
                    <Button variant="link" className="px-0" onClick={() => pushPath(parentPath)}>
                        &larr; Back to /{parentPath}
                    </Button>
                </div>
                <FileContents
                    repoId={datasetId}
                    reference={reference}
                    path={path}
                    fileExtension={getFileExtension(path)}
                    // eslint-disable-next-line @typescript-eslint/no-explicit-any
                    contentType={getContentType((head as any)?.headers)}
                    // eslint-disable-next-line @typescript-eslint/no-explicit-any
                    sizeBytes={Number((head as any)?.headers?.get?.('Content-Length') ?? -1)}
                    error={headError as Error | null}
                    loading={loading}
                    showFullNavigator={false}
                    presign={!!storageConfig?.pre_sign_support_ui}
                />
            </>
        );
    }

    if (listLoading) return <Loading />;
    if (listError) return <AlertError error={listError as Error} />;

    return (
        <>
            {header}
            {previewNotice}
            {!isPreview && browseRef !== selectionToRef(selection) && (
                <div className="text-muted small mb-2 dataset-data-viewing">
                    <TagIcon className="me-1" />
                    Viewing the immutable tag <code>{browseRef}</code>
                </div>
            )}
            <Tree
                config={{ config: storageConfig }}
                repo={repoForTree}
                reference={reference}
                path={path}
                results={results}
                after={after}
                nextPage={nextPage}
                // Read-only repository: no upload, import or delete entry points (§7.3). A
                // proposal preview branch is a branch, which the tree would otherwise offer
                // "Delete" for, so its row actions are off entirely.
                showActions={!isPreview}
                onPaginate={(nextAfter: string) => pushPath(path, nextAfter)}
                onUpload={() => undefined}
                onImport={() => undefined}
                onDelete={() => undefined}
                pathURLBuilder={(_params: unknown, query: Record<string, string>) => {
                    const next: Record<string, string> = {};
                    if (isPreview) next[DATA_QUERY_KEYS.ref] = previewRef;
                    else if (selection !== 'latest') next[DATA_QUERY_KEYS.version] = selection;
                    if (query.path) next[DATA_QUERY_KEYS.path] = query.path;
                    return { pathname: '/datasets/:datasetId/data', params: { datasetId }, query: next };
                }}
                entryURLBuilder={(entry: { path: string }) => {
                    const next: Record<string, string> = {};
                    if (isPreview) next[DATA_QUERY_KEYS.ref] = previewRef;
                    else if (selection !== 'latest') next[DATA_QUERY_KEYS.version] = selection;
                    if (entry.path) next[DATA_QUERY_KEYS.path] = entry.path;
                    return { pathname: '/datasets/:datasetId/data', params: { datasetId }, query: next };
                }}
                hasMountButton={false}
            />
        </>
    );
};

export default DatasetDataPage;
