import React, { FC, useCallback, useMemo, useState } from 'react';
import Button from 'react-bootstrap/Button';
import ButtonToolbar from 'react-bootstrap/ButtonToolbar';
import Col from 'react-bootstrap/Col';
import Form from 'react-bootstrap/Form';
import InputGroup from 'react-bootstrap/InputGroup';
import Table from 'react-bootstrap/Table';
import Badge from 'react-bootstrap/Badge';
import { PlusIcon, SearchIcon } from '@primer/octicons-react';
import dayjs from 'dayjs';
import relativeTime from 'dayjs/plugin/relativeTime';

import { ActionsBar, AlertError, Loading, Na, useDebouncedState } from '../../lib/components/controls';
import { Paginator } from '../../lib/components/pagination';
import { Link } from '../../lib/components/nav';
import { useRouter } from '../../lib/hooks/router';
import { useAPIWithPagination } from '../../lib/hooks/api';
import { Dataset, datasets } from '../../lib/api/datasets';
import { MetadataBadges } from '../../lib/components/datasets/metadataBadges';
import { useCanCreateDataset } from '../../lib/components/datasets/hooks';
import { humanSize } from '../../lib/components/repository/tree';

dayjs.extend(relativeTime);

export const CREATE_DATASET_BUTTON_TEXT = 'Create dataset';
export const EMPTY_DATASETS_TEXT = 'No datasets yet';

/** Query-parameter prefix of the metadata filters (`metadata.<key>=<value>`, §10.1). */
const METADATA_FILTER_PREFIX = 'metadata.';

export interface MetadataFilter {
    key: string;
    value: string;
}

/**
 * The metadata filters a location carries, in query order.
 *
 * They live in the URL (rather than in component state) so a filtered list can be linked to
 * and survives a reload, exactly like the repository search box.
 */
export const metadataFiltersFromQuery = (query: Record<string, string | undefined>): MetadataFilter[] =>
    Object.entries(query)
        .filter(([key]) => key.startsWith(METADATA_FILTER_PREFIX))
        .map(([key, value]) => ({ key: key.slice(METADATA_FILTER_PREFIX.length), value: value ?? '' }));

/** The `metadata.<key>=<value>` query object for a set of filters. */
export const metadataQueryFromFilters = (filters: MetadataFilter[]): Record<string, string> => {
    const query: Record<string, string> = {};
    for (const filter of filters) {
        if (filter.key === '') continue;
        query[`${METADATA_FILTER_PREFIX}${filter.key}`] = filter.value;
    }
    return query;
};

const StateBadge: FC<{ state: Dataset['state'] }> = ({ state }) => {
    if (state === 'active') return null;
    return (
        <Badge bg="warning" text="dark" className="ms-2">
            {state}
        </Badge>
    );
};

const DatasetRow: FC<{ dataset: Dataset }> = ({ dataset }) => {
    const updatedAt = dataset.updated_at ?? dataset.created_at;
    const stats = dataset.stats;
    return (
        <tr>
            <td className="dataset-name-cell">
                <Link
                    href={{
                        pathname: '/datasets/:datasetId/overview',
                        params: { datasetId: dataset.name },
                    }}
                >
                    {dataset.name}
                </Link>
                <StateBadge state={dataset.state} />
            </td>
            <td className="text-truncate dataset-description-cell" title={dataset.description}>
                {dataset.description || <Na />}
            </td>
            <td>{dataset.latest_version > 0 ? `v${dataset.latest_version}` : <Na />}</td>
            <td title={stats ? `${stats.bytes.toLocaleString()} bytes` : undefined}>
                {stats ? `${stats.objects.toLocaleString()} / ${humanSize(stats.bytes)}` : <Na />}
            </td>
            <td title={dayjs(updatedAt).toISOString()}>{dayjs(updatedAt).fromNow()}</td>
            <td>
                <MetadataBadges metadata={dataset.definition?.metadata} />
            </td>
        </tr>
    );
};

const EmptyState: FC<{ canCreate: boolean; filtered: boolean }> = ({ canCreate, filtered }) => (
    <div className="datasets-empty-state text-center py-5">
        <h5>{filtered ? 'No datasets match your search' : EMPTY_DATASETS_TEXT}</h5>
        <p className="text-muted">
            {filtered
                ? 'Try a different search term or remove the metadata filters.'
                : 'A dataset is a versioned, read-only collection of data curated from one or more repositories.'}
        </p>
        {!filtered && canCreate && (
            <Link component={Button} variant="success" href="/datasets/create">
                <PlusIcon className="me-2" />
                {CREATE_DATASET_BUTTON_TEXT}
            </Link>
        )}
    </div>
);

/**
 * The datasets list (§11.2): name, description, latest version, objects/bytes, updated and
 * metadata badges, with a search box and metadata filters.
 *
 * "Create dataset" is only offered to users who may create one: the answer comes from the
 * user's effective policies (`useCanCreateDataset`), and the server enforces it regardless.
 */
const DatasetsListPage: FC = () => {
    const router = useRouter();
    const search = router.query.search ?? '';
    const after = router.query.after ?? '';
    const filters = metadataFiltersFromQuery(router.query);
    const filtersSignature = JSON.stringify(filters);
    const canCreate = useCanCreateDataset();

    const [metadataKey, setMetadataKey] = useState('');
    const [metadataValue, setMetadataValue] = useState('');

    const pushQuery = useCallback(
        (next: { search?: string; after?: string; filters?: MetadataFilter[] }) => {
            const query: Record<string, string> = {
                ...metadataQueryFromFilters(next.filters ?? filters),
            };
            const nextSearch = next.search ?? search;
            if (nextSearch !== '') query.search = nextSearch;
            const nextAfter = next.after ?? '';
            if (nextAfter !== '') query.after = nextAfter;
            router.push({ pathname: '/datasets', params: {}, query });
        },
        // eslint-disable-next-line react-hooks/exhaustive-deps
        [router, search, filtersSignature],
    );

    const [searchInput, setSearchInput] = useDebouncedState(search, (value: string) => {
        pushQuery({ search: value, after: '' });
    });

    // Identity of the filter object is stable per filter set, so the API call below does not
    // re-run on every render.
    const metadataQuery = useMemo(() => metadataQueryFromFilters(filters), [filtersSignature]); // eslint-disable-line react-hooks/exhaustive-deps

    const { results, loading, error, nextPage } = useAPIWithPagination(() => {
        return datasets.list({
            search: search === '' ? undefined : search,
            after: after === '' ? undefined : after,
            metadata: metadataQuery,
        });
    }, [search, after, metadataQuery]);

    const onAddFilter = (event: React.FormEvent<HTMLFormElement>) => {
        event.preventDefault();
        const key = metadataKey.trim();
        if (key === '') return;
        const next = filters.filter((filter) => filter.key !== key);
        next.push({ key, value: metadataValue.trim() });
        setMetadataKey('');
        setMetadataValue('');
        pushQuery({ filters: next, after: '' });
    };

    const onRemoveFilter = (key: string) => {
        pushQuery({ filters: filters.filter((filter) => filter.key !== key), after: '' });
    };

    let content;
    if (loading) {
        content = <Loading />;
    } else if (error) {
        content = <AlertError error={error} />;
    } else if (results.length === 0) {
        content = <EmptyState canCreate={canCreate} filtered={search !== '' || filters.length > 0} />;
    } else {
        content = (
            <>
                <Table hover responsive className="datasets-table">
                    <thead>
                        <tr>
                            <th>Dataset</th>
                            <th>Description</th>
                            <th>Latest version</th>
                            <th>Objects / Bytes</th>
                            <th>Updated</th>
                            <th>Metadata</th>
                        </tr>
                    </thead>
                    <tbody>
                        {results.map((dataset: Dataset) => (
                            <DatasetRow key={dataset.name} dataset={dataset} />
                        ))}
                    </tbody>
                </Table>
                <Paginator
                    after={after}
                    nextPage={nextPage}
                    onPaginate={(nextAfter: string) => pushQuery({ after: nextAfter })}
                />
            </>
        );
    }

    return (
        <div className="datasets-list-page">
            <ActionsBar>
                <Form
                    className="datasets-search-form"
                    onSubmit={(event) => {
                        event.preventDefault();
                    }}
                >
                    <Form.Group as={Col}>
                        <InputGroup className="search-input-group">
                            <InputGroup.Text className="border-end-0">
                                <SearchIcon />
                            </InputGroup.Text>
                            <Form.Control
                                className="border-start-0"
                                placeholder="Search datasets..."
                                aria-label="Search datasets"
                                value={searchInput}
                                onChange={(event) => setSearchInput(event.target.value)}
                            />
                        </InputGroup>
                    </Form.Group>
                </Form>

                <Form className="datasets-metadata-filter ms-2" onSubmit={onAddFilter}>
                    <InputGroup>
                        <Form.Control
                            placeholder="Metadata key"
                            aria-label="Metadata key"
                            value={metadataKey}
                            onChange={(event) => setMetadataKey(event.target.value)}
                            size="sm"
                        />
                        <Form.Control
                            placeholder="Value"
                            aria-label="Metadata value"
                            value={metadataValue}
                            onChange={(event) => setMetadataValue(event.target.value)}
                            size="sm"
                        />
                        <Button
                            variant="outline-secondary"
                            size="sm"
                            type="submit"
                            disabled={metadataKey.trim() === ''}
                        >
                            Add filter
                        </Button>
                    </InputGroup>
                </Form>

                {canCreate && (
                    <ButtonToolbar className="ms-auto">
                        <Link
                            component={Button}
                            variant="success"
                            href="/datasets/create"
                            className="d-flex align-items-center"
                        >
                            <PlusIcon className="me-2" />
                            {CREATE_DATASET_BUTTON_TEXT}
                        </Link>
                    </ButtonToolbar>
                )}
            </ActionsBar>

            {filters.length > 0 && (
                <div className="datasets-active-filters mb-3 d-flex flex-wrap gap-2 align-items-center">
                    <span className="text-muted">Filters:</span>
                    {filters.map((filter) => (
                        <Badge key={filter.key} bg="light" text="dark" className="border dataset-active-filter">
                            {`${filter.key}: ${filter.value}`}
                            <Button
                                variant="link"
                                size="sm"
                                className="p-0 ms-1 align-baseline"
                                aria-label={`Remove ${filter.key} filter`}
                                onClick={() => onRemoveFilter(filter.key)}
                            >
                                &times;
                            </Button>
                        </Badge>
                    ))}
                </div>
            )}

            {content}
        </div>
    );
};

export default DatasetsListPage;
