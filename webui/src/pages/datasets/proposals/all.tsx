import React, { FC } from 'react';
import { GitPullRequestIcon } from '@primer/octicons-react';

import { MAX_LISTING_AMOUNT } from '../../../lib/api';
import { Proposal, ProposalStatus, datasets, getDatasetNames } from '../../../lib/api/datasets';
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

/** The query parameter the cross-dataset list keeps the status filter in. */
export const ALL_PROPOSALS_QUERY_KEYS = { status: 'status' } as const;

/** A proposal of the cross-dataset list: the record plus the dataset it belongs to. */
export interface DatasetProposal extends Proposal {
    dataset: string;
}

/**
 * Every pull request of every dataset (DATASETS_DESIGN.md §11.2), newest first.
 *
 * There is no cross-dataset proposals endpoint (§10.1), so this composes one: it lists the
 * dataset names, then asks each dataset for its proposals of the selected status and merges the
 * results. A dataset whose proposals cannot be read (deleted between the two requests, or not
 * readable by this caller) is skipped rather than failing the whole page, so one unavailable
 * dataset never hides everyone else's reviews.
 */
export const listAllProposals = async (status: ProposalStatus): Promise<DatasetProposal[]> => {
    const names = [...(await getDatasetNames())];
    if (names.length === 0) return [];

    const perDataset = await Promise.all(
        names.map(async (name): Promise<DatasetProposal[]> => {
            try {
                const response = await datasets.listProposals(name, { status, amount: MAX_LISTING_AMOUNT });
                return response.results.map((proposal) => ({ ...proposal, dataset: proposal.dataset || name }));
            } catch {
                return [];
            }
        }),
    );

    return perDataset.flat().sort((left, right) => right.created_at - left.created_at);
};

/**
 * The second top-level datasets tab (§11.1): the open (or closed, or merged) pull requests of
 * every dataset, so a reviewer does not have to visit each dataset in turn.
 */
const DatasetProposalsAllPage: FC = () => {
    const router = useRouter();
    const status: ProposalStatus = proposalStatusFromQuery(router.query[ALL_PROPOSALS_QUERY_KEYS.status]);
    const { response, loading, error } = useAPI(() => listAllProposals(status), [status]);
    const proposals = (response ?? []) as DatasetProposal[];

    const onSelectStatus = (next: ProposalStatus) =>
        router.push({ pathname: '/datasets/pulls', params: {}, query: proposalStatusQuery(next) });

    return (
        <div className="dataset-proposals-all-page">
            <div className="d-flex flex-wrap align-items-center mb-2">
                <h5 className="mb-0">
                    <GitPullRequestIcon className="me-1" /> Pull requests
                </h5>
                <span className="text-muted small ms-2">
                    Across all datasets &middot; {PROPOSAL_STATUS_LABELS[status]}
                </span>
            </div>

            <ProposalStatusFilter value={status} onSelect={onSelectStatus} idPrefix="datasets-pulls" />

            {loading && <Loading />}
            {!loading && error && <AlertError error={error as Error} />}
            {!loading && !error && (
                <ProposalTable
                    proposals={proposals}
                    emptyText={
                        status === 'open'
                            ? 'No open pull requests in any dataset'
                            : `No ${status} pull requests in any dataset`
                    }
                />
            )}
        </div>
    );
};

export default DatasetProposalsAllPage;
