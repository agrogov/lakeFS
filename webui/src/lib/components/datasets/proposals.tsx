import React, { FC } from 'react';
import Badge from 'react-bootstrap/Badge';
import Nav from 'react-bootstrap/Nav';
import Table from 'react-bootstrap/Table';
import dayjs from 'dayjs';
import relativeTime from 'dayjs/plugin/relativeTime';

import { Proposal, ProposalStatus } from '../../api/datasets';
import { Link } from '../nav';

dayjs.extend(relativeTime);

/**
 * Shared pieces of the dataset proposal (pull request) views (DATASETS_DESIGN.md §5.6, §11.2).
 *
 * The per-dataset list, the cross-dataset list and the detail page all show the same rows and
 * the same `open | closed | merged` filter, so they are defined once here instead of three
 * times. The filter value lives in the URL (`?status=merged`) on both list pages.
 */

/** The three review states of a proposal (§5.6), in the order the filter presents them. */
export const PROPOSAL_STATUSES: readonly ProposalStatus[] = ['open', 'closed', 'merged'];

export const PROPOSAL_STATUS_LABELS: Record<ProposalStatus, string> = {
    open: 'Open',
    closed: 'Closed',
    merged: 'Merged',
};

const STATUS_BADGE_VARIANTS: Record<ProposalStatus, string> = {
    open: 'success',
    closed: 'secondary',
    merged: 'primary',
};

/** The default filter: what a reviewer wants to see, the reviews that are still running. */
export const DEFAULT_PROPOSAL_STATUS: ProposalStatus = 'open';

/**
 * The status a `?status=` value selects. Anything unknown (a hand-typed URL, a deleted
 * state) falls back to `open` rather than rendering an unfiltered, misleading list.
 */
export const proposalStatusFromQuery = (value: string | undefined): ProposalStatus =>
    PROPOSAL_STATUSES.includes(value as ProposalStatus) ? (value as ProposalStatus) : DEFAULT_PROPOSAL_STATUS;

/** `?status=` for a status, omitting the default so the common URL stays short. */
export const proposalStatusQuery = (status: ProposalStatus): Record<string, string> =>
    status === DEFAULT_PROPOSAL_STATUS ? {} : { status };

export const ProposalStatusBadge: FC<{ status: ProposalStatus }> = ({ status }) => (
    <Badge bg={STATUS_BADGE_VARIANTS[status] ?? 'secondary'} className={`dataset-proposal-status-${status}`}>
        {PROPOSAL_STATUS_LABELS[status] ?? status}
    </Badge>
);

/**
 * The status filter of a proposal list: one tab per review state.
 *
 * Rendered as tabs (`Nav`) because the pages are already tabbed, so the control reads as part
 * of the page rather than as a form. Each tab navigates with `onSelect`, which lets the caller
 * keep the value in the URL.
 */
export const ProposalStatusFilter: FC<{
    value: ProposalStatus;
    onSelect: (status: ProposalStatus) => void;
    idPrefix?: string;
}> = ({ value, onSelect, idPrefix = 'dataset-proposals' }) => (
    <Nav
        variant="tabs"
        activeKey={value}
        onSelect={(eventKey) => onSelect(proposalStatusFromQuery(eventKey ?? undefined))}
        className="datasets-nav-tabs dataset-proposal-status-filter mb-3"
    >
        {PROPOSAL_STATUSES.map((status) => (
            <Nav.Item key={status}>
                <Nav.Link eventKey={status} id={`${idPrefix}-status-${status}`}>
                    {PROPOSAL_STATUS_LABELS[status]}
                </Nav.Link>
            </Nav.Item>
        ))}
    </Nav>
);

export interface ProposalTableProps {
    proposals: Proposal[];
    /**
     * The dataset the rows belong to. Omitted by the cross-dataset list, whose rows carry
     * their own `dataset` and therefore show a dataset column instead.
     */
    datasetId?: string;
    emptyText?: string;
}

/** One review row: title, author, the version it was based on and its state (§11.2). */
export const ProposalTable: FC<ProposalTableProps> = ({ proposals, datasetId, emptyText = 'No pull requests' }) => {
    if (proposals.length === 0) {
        return <div className="text-muted dataset-proposals-empty">{emptyText}</div>;
    }

    const showDataset = datasetId === undefined;

    return (
        <Table hover responsive className="dataset-proposals-table">
            <thead>
                <tr>
                    <th>Title</th>
                    {showDataset && <th>Dataset</th>}
                    <th>Status</th>
                    <th>Author</th>
                    <th>Base</th>
                    <th>Created</th>
                </tr>
            </thead>
            <tbody>
                {proposals.map((proposal) => {
                    const dataset = proposal.dataset || datasetId || '';
                    return (
                        <tr key={`${dataset}/${proposal.id}`} data-testid={`dataset-proposal-row-${proposal.id}`}>
                            <td className="dataset-proposal-title">
                                <Link
                                    href={{
                                        pathname: '/datasets/:datasetId/proposals/:proposalId',
                                        params: { datasetId: dataset, proposalId: proposal.id },
                                    }}
                                >
                                    {proposal.title || proposal.id}
                                </Link>
                            </td>
                            {showDataset && (
                                <td>
                                    <code>{dataset}</code>
                                </td>
                            )}
                            <td>
                                <ProposalStatusBadge status={proposal.status} />
                            </td>
                            <td>{proposal.author}</td>
                            <td className="text-nowrap">
                                <code>{proposal.base_version > 0 ? `v${proposal.base_version}` : '—'}</code>
                            </td>
                            <td
                                className="text-nowrap text-muted small"
                                title={dayjs(proposal.created_at).toISOString()}
                            >
                                {dayjs(proposal.created_at).fromNow()}
                            </td>
                        </tr>
                    );
                })}
            </tbody>
        </Table>
    );
};

export default ProposalTable;
