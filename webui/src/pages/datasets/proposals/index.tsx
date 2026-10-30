import React, { FC } from 'react';
import Card from 'react-bootstrap/Card';
import { useParams } from 'react-router-dom';
import { GitPullRequestIcon } from '@primer/octicons-react';

import { MAX_LISTING_AMOUNT } from '../../../lib/api';
import { Proposal, ProposalList, ProposalStatus, datasets } from '../../../lib/api/datasets';
import { AlertError, Loading } from '../../../lib/components/controls';
import {
    ProposalStatusFilter,
    ProposalTable,
    PROPOSAL_STATUS_LABELS,
    proposalStatusFromQuery,
    proposalStatusQuery,
} from '../../../lib/components/datasets/proposals';
import { useAPI } from '../../../lib/hooks/api';
import { useRouter } from '../../../lib/hooks/router';

/** The query parameter the list keeps the status filter in. */
export const PROPOSALS_QUERY_KEYS = { status: 'status' } as const;

/**
 * The pull requests of one dataset (DATASETS_DESIGN.md §11.2, §5.6).
 *
 * The status filter is kept in the URL (`?status=merged`), so a filtered list can be linked to
 * and survives a reload; `open` is the default and is left out of the URL. The rows link to the
 * proposal detail page, which holds the diff, the preview link and the merge / close actions.
 */
const DatasetProposalsPage: FC = () => {
    const { datasetId = '' } = useParams();
    const router = useRouter();
    const status: ProposalStatus = proposalStatusFromQuery(router.query[PROPOSALS_QUERY_KEYS.status]);

    const { response, loading, error } = useAPI(
        () => datasets.listProposals(datasetId, { status, amount: MAX_LISTING_AMOUNT }),
        [datasetId, status],
    );
    const proposals = ((response as ProposalList | null)?.results ?? []) as Proposal[];

    const onSelectStatus = (next: ProposalStatus) =>
        router.push({
            pathname: '/datasets/:datasetId/proposals',
            params: { datasetId },
            query: proposalStatusQuery(next),
        });

    return (
        <div className="dataset-proposals-page">
            <div className="d-flex flex-wrap align-items-center mb-2">
                <h5 className="mb-0">
                    <GitPullRequestIcon className="me-1" /> Pull requests
                </h5>
                <span className="text-muted small ms-2">{PROPOSAL_STATUS_LABELS[status]}</span>
            </div>

            <ProposalStatusFilter value={status} onSelect={onSelectStatus} idPrefix="dataset-proposals" />

            {loading && <Loading />}
            {!loading && error && <AlertError error={error as Error} />}
            {!loading && !error && (
                <ProposalTable
                    proposals={proposals}
                    datasetId={datasetId}
                    emptyText={
                        status === 'open'
                            ? 'This dataset has no open pull requests'
                            : `This dataset has no ${status} pull requests`
                    }
                />
            )}

            {!loading && !error && status === 'open' && proposals.length === 0 && (
                <Card className="dataset-proposals-hint mt-3">
                    <Card.Body className="text-muted small">
                        Open a pull request from the dataset editor to propose a new definition for review.
                    </Card.Body>
                </Card>
            )}
        </div>
    );
};

export default DatasetProposalsPage;
