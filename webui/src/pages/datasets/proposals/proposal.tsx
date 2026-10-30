import React, { FC, useState } from 'react';
import Alert from 'react-bootstrap/Alert';
import Badge from 'react-bootstrap/Badge';
import Button from 'react-bootstrap/Button';
import Card from 'react-bootstrap/Card';
import Nav from 'react-bootstrap/Nav';
import { useParams } from 'react-router-dom';
import dayjs from 'dayjs';
import relativeTime from 'dayjs/plugin/relativeTime';
import { AlertIcon, GitMergeIcon, SyncIcon, XCircleIcon } from '@primer/octicons-react';

import { Dataset, DefinitionDiff, Proposal, Version, datasets, isRebaseRequiredError } from '../../../lib/api/datasets';
import { AlertError, Loading } from '../../../lib/components/controls';
import { DefinitionDiffTable } from '../../../lib/components/datasets/definitionDiffTable';
import { DataDiffTable } from '../../../lib/components/datasets/dataDiffTable';
import { DATASETS_PUBLISH_ACTION, datasetResource, useCan } from '../../../lib/components/datasets/hooks';
import { ProposalStatusBadge } from '../../../lib/components/datasets/proposals';
import { Link } from '../../../lib/components/nav';
import { useAPI } from '../../../lib/hooks/api';

dayjs.extend(relativeTime);

/** The tabs of the proposal detail: the structural diff and the object diff (§11.2). */
export const PROPOSAL_DIFF_TABS = [
    { key: 'definition', label: 'Definition' },
    { key: 'data', label: 'Data' },
] as const;

export type ProposalDiffTabKey = (typeof PROPOSAL_DIFF_TABS)[number]['key'];

/** The hidden preview branch of a proposal, used when the record does not name one (§5.6). */
export const previewBranchOf = (proposal: Proposal): string => proposal.preview_branch || `_proposal-${proposal.id}`;

/** The data browser URL of a proposal's preview branch: `data?ref=_proposal-<id>` (§11.2). */
export const previewHref = (datasetId: string, proposal: Proposal) => ({
    pathname: '/datasets/:datasetId/data',
    params: { datasetId },
    query: { ref: previewBranchOf(proposal) },
});

export interface ProposalConflict {
    baseVersion: number;
    latestVersion: number;
}

/**
 * The stale-base conflict banner of §5.6.
 *
 * A proposal is made against a version, and merging publishes its definition on top of
 * `latest`. Once a version is published in between, the review no longer describes what a merge
 * would produce, so the control plane answers `409` and the reviewer has to rebase: "Update
 * proposal" re-resolves the definition against the latest version and moves the base forward.
 * This banner is the same message whether the conflict is noticed from the record
 * (`base_version < latest_version`) or from a refused merge (the `409`), so the user sees one
 * explanation for one problem.
 */
export const ProposalConflictBanner: FC<{
    conflict: ProposalConflict;
    onUpdate: () => void;
    updating: boolean;
}> = ({ conflict, onUpdate, updating }) => (
    <Alert variant="warning" className="dataset-proposal-conflict" data-testid="dataset-proposal-conflict">
        <div className="d-flex align-items-start gap-2">
            <AlertIcon className="mt-1" />
            <div>
                <strong>Rebase required</strong>
                <div>
                    This pull request was made against <code>v{conflict.baseVersion}</code>, but the dataset is now at{' '}
                    <code>v{conflict.latestVersion}</code>. Update the pull request to re-resolve its definition against
                    the latest version before merging it.
                </div>
                <Button
                    variant="warning"
                    size="sm"
                    className="mt-2 dataset-proposal-update"
                    onClick={onUpdate}
                    disabled={updating}
                >
                    <SyncIcon className="me-1" />
                    {updating ? 'Updating…' : 'Update proposal'}
                </Button>
            </div>
        </div>
    </Alert>
);

const ProposalHeader: FC<{ datasetId: string; proposal: Proposal }> = ({ datasetId, proposal }) => (
    <Card className="mb-3 dataset-proposal-header">
        <Card.Body>
            <div className="d-flex flex-wrap align-items-center gap-2">
                <h5 className="mb-0 dataset-proposal-title">{proposal.title || proposal.id}</h5>
                <ProposalStatusBadge status={proposal.status} />
                {/* Closing or merging deletes the preview branch (§5.6), so only an open
                    proposal still has data to preview. */}
                {proposal.status === 'open' && (
                    <Link className="btn btn-outline-secondary btn-sm ms-auto" href={previewHref(datasetId, proposal)}>
                        Preview data
                    </Link>
                )}
            </div>
            {proposal.description && <p className="text-muted mt-2 mb-0">{proposal.description}</p>}
            <div className="text-muted small mt-2 dataset-proposal-origin">
                Opened by <strong>{proposal.author}</strong>{' '}
                <span title={dayjs(proposal.created_at).toISOString()}>{dayjs(proposal.created_at).fromNow()}</span>
                <span className="mx-2">&middot;</span>
                Based on <code>v{proposal.base_version}</code>
                {proposal.status === 'merged' && proposal.merged_version !== undefined && (
                    <>
                        <span className="mx-2">&middot;</span>
                        Merged as <code>v{proposal.merged_version}</code>
                    </>
                )}
                {proposal.closed_at !== undefined && (
                    <>
                        <span className="mx-2">&middot;</span>
                        Closed{' '}
                        <span title={dayjs(proposal.closed_at).toISOString()}>
                            {dayjs(proposal.closed_at).fromNow()}
                        </span>
                    </>
                )}
            </div>
        </Card.Body>
    </Card>
);

/**
 * One pull request of a dataset (DATASETS_DESIGN.md §11.2, §5.6): the definition diff, the data
 * diff, a link to the preview branch (`data?ref=_proposal-<id>`) and the merge / close actions.
 *
 * Merge is offered only to a caller holding `datasets:PublishVersion`, because merging is a
 * publish (§5.6). The refusal case is surfaced as the rebase banner rather than as a generic
 * failure, and the banner is also shown up front when `base_version` is already behind
 * `latest_version` - the button would only produce that same `409`.
 */
const DatasetProposalPage: FC = () => {
    const { datasetId = '', proposalId = '' } = useParams();
    const [tab, setTab] = useState<ProposalDiffTabKey>('definition');
    const [busy, setBusy] = useState<'merge' | 'close' | 'update' | null>(null);
    const [actionError, setActionError] = useState<Error | null>(null);
    const [conflict, setConflict] = useState<ProposalConflict | null>(null);
    const [notice, setNotice] = useState<string | null>(null);

    const {
        response: datasetResponse,
        loading: datasetLoading,
        error: datasetError,
        refetch: refetchDataset,
    } = useAPI(() => datasets.get(datasetId), [datasetId]);

    const {
        response: proposalResponse,
        loading: proposalLoading,
        error: proposalError,
        refetch: refetchProposal,
    } = useAPI(() => datasets.getProposal(datasetId, proposalId), [datasetId, proposalId]);

    const dataset = datasetResponse as Dataset | null;
    const proposal = proposalResponse as Proposal | null;
    const canPublish = useCan(DATASETS_PUBLISH_ACTION, datasetResource(datasetId));

    if (datasetLoading || proposalLoading) return <Loading />;
    if (datasetError || proposalError) {
        return <AlertError error={(datasetError ?? proposalError) as unknown as Error} />;
    }
    if (!proposal) return <AlertError error={new Error(`Pull request "${proposalId}" was not found`)} />;

    const latestVersion = dataset?.latest_version ?? proposal.base_version;
    // The record's own staleness, or the latest_version the refused merge reported (which may be
    // newer than what this page loaded, if a publish happened meanwhile).
    const recordConflict: ProposalConflict | null =
        proposal.status === 'open' && proposal.base_version < latestVersion
            ? { baseVersion: proposal.base_version, latestVersion }
            : null;
    const open = proposal.status === 'open';
    const shownConflict = open ? (conflict ?? recordConflict) : null;

    const runMerge = async () => {
        setBusy('merge');
        setActionError(null);
        setNotice(null);
        try {
            const version = (await datasets.mergeProposal(datasetId, proposalId)) as Version;
            setConflict(null);
            setNotice(`Merged as v${version.version}`);
            refetchProposal();
            refetchDataset();
        } catch (error) {
            if (isRebaseRequiredError(error)) {
                setConflict({
                    baseVersion: proposal.base_version,
                    latestVersion: error.latestVersion ?? latestVersion,
                });
            } else {
                setActionError(error as Error);
            }
        } finally {
            setBusy(null);
        }
    };

    const runClose = async () => {
        setBusy('close');
        setActionError(null);
        setNotice(null);
        try {
            await datasets.updateProposal(datasetId, proposalId, { status: 'closed' });
            setConflict(null);
            refetchProposal();
            refetchDataset();
        } catch (error) {
            setActionError(error as Error);
        } finally {
            setBusy(null);
        }
    };

    // "Update proposal" is the explicit rebase of §5.6: an empty update re-resolves the stored
    // definition against the current latest version and moves the base forward.
    const runUpdate = async () => {
        setBusy('update');
        setActionError(null);
        try {
            await datasets.updateProposal(datasetId, proposalId, {});
            setConflict(null);
            setNotice('Pull request updated against the latest version');
            refetchProposal();
            refetchDataset();
        } catch (error) {
            setActionError(error as Error);
        } finally {
            setBusy(null);
        }
    };

    return (
        <div className="dataset-proposal-page">
            <ProposalHeader datasetId={datasetId} proposal={proposal} />

            {shownConflict && (
                <ProposalConflictBanner conflict={shownConflict} onUpdate={runUpdate} updating={busy === 'update'} />
            )}

            {notice && (
                <Alert
                    variant="success"
                    dismissible
                    onClose={() => setNotice(null)}
                    className="dataset-proposal-notice"
                >
                    {notice}
                </Alert>
            )}
            {actionError && <AlertError error={actionError} onDismiss={() => setActionError(null)} />}

            {open && (
                <Card className="mb-3 dataset-proposal-actions">
                    <Card.Body className="d-flex flex-wrap align-items-center gap-2">
                        <span className="text-muted small">Review this pull request</span>
                        <div className="ms-auto d-flex gap-2">
                            <Button
                                variant="outline-danger"
                                size="sm"
                                className="dataset-proposal-close"
                                onClick={runClose}
                                disabled={busy !== null}
                            >
                                <XCircleIcon className="me-1" />
                                Close
                            </Button>
                            <Button
                                variant="success"
                                size="sm"
                                className="dataset-proposal-merge"
                                onClick={runMerge}
                                disabled={busy !== null || !canPublish || shownConflict !== null}
                                title={
                                    !canPublish
                                        ? 'Merging a pull request publishes a version (datasets:PublishVersion)'
                                        : shownConflict
                                          ? 'Rebase the pull request before merging'
                                          : undefined
                                }
                            >
                                <GitMergeIcon className="me-1" />
                                {busy === 'merge' ? 'Merging…' : 'Merge'}
                            </Button>
                        </div>
                    </Card.Body>
                </Card>
            )}

            {!open && (
                <Alert variant="secondary" className="dataset-proposal-finished">
                    This pull request is <strong>{proposal.status}</strong>, so there is nothing left to review.
                </Alert>
            )}

            <Nav
                variant="tabs"
                activeKey={tab}
                onSelect={(eventKey) => setTab((eventKey as ProposalDiffTabKey) ?? 'definition')}
                className="datasets-nav-tabs dataset-proposal-tabs mb-3"
            >
                {PROPOSAL_DIFF_TABS.map((entry) => (
                    <Nav.Item key={entry.key}>
                        <Nav.Link eventKey={entry.key}>
                            {entry.label}
                            {entry.key === 'data' && (
                                <Badge bg="light" text="dark" className="border ms-2">
                                    {proposal.data_diff?.results?.length ?? 0}
                                </Badge>
                            )}
                        </Nav.Link>
                    </Nav.Item>
                ))}
            </Nav>

            {tab === 'definition' ? (
                proposal.diff ? (
                    <DefinitionDiffTable diff={proposal.diff as DefinitionDiff} />
                ) : (
                    <div className="text-muted">The definition diff of this pull request is not available.</div>
                )
            ) : (
                <DataDiffTable entries={proposal.data_diff?.results} />
            )}
        </div>
    );
};

export default DatasetProposalPage;
