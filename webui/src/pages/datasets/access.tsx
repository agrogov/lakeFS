import React, { FC, useState } from 'react';
import Button from 'react-bootstrap/Button';
import Table from 'react-bootstrap/Table';
import { PersonAddIcon, PeopleIcon } from '@primer/octicons-react';
import { useParams } from 'react-router-dom';

import { auth } from '../../lib/api';
import { AccessGrant, AccessGrantList, PrincipalType, datasets, isForbiddenError } from '../../lib/api/datasets';
import { AlertError, FormattedDate, Loading } from '../../lib/components/controls';
import { ConfirmationButton } from '../../lib/components/modals';
import { AttachModal } from '../../lib/components/auth/forms';
import { DatasetPermissionRequired } from '../../lib/components/datasets/gate';
import { DATASETS_MANAGE_ACCESS_ACTION, datasetResource, usePermission } from '../../lib/components/datasets/hooks';
import { useAPI } from '../../lib/hooks/api';
import { PageSize } from '../../constants';

/**
 * What the page shows while the permission is not yet known: no grants. It never reaches the
 * screen, because the page renders a loading state until the permission decision arrives.
 */
const EMPTY_GRANT_LIST: AccessGrantList = {
    results: [],
    pagination: { has_more: false, next_offset: '', results: 0, max_per_page: 0 },
};

const PRINCIPAL_LABELS: Record<PrincipalType, string> = {
    user: 'User',
    group: 'Group',
};

const resolveUserName = (user: { id: string; friendly_name?: string }) => user.friendly_name || user.id;
const resolveGroupName = (group: { id: string; name?: string }) => group.name || group.id;

/**
 * Access of one dataset (DATASETS_DESIGN.md §11.2, §9.2): the users and groups that were granted
 * consumer read access, and the controls to grant and revoke it.
 *
 * The page is rendered only for a caller holding `datasets:ManageAccess`; a `403` from the list
 * is treated exactly like the policy check saying no, so both paths show the same explanation
 * rather than an error.
 */
const DatasetAccessPage: FC = () => {
    const { datasetId = '' } = useParams();
    const permission = usePermission(DATASETS_MANAGE_ACCESS_ACTION, datasetResource(datasetId));
    const [addType, setAddType] = useState<PrincipalType | null>(null);
    const [actionError, setActionError] = useState<Error | null>(null);

    const { response, loading, error, refetch } = useAPI(
        () => (permission.allowed ? datasets.listAccess(datasetId) : Promise.resolve(EMPTY_GRANT_LIST)),
        [datasetId, permission.allowed],
    );

    if (permission.loading) return <Loading />;
    if (!permission.allowed) {
        return <DatasetPermissionRequired action={DATASETS_MANAGE_ACCESS_ACTION} what="Managing dataset access" />;
    }

    // A 403 means the same as a denied policy check: show the missing permission, not an error.
    if (isForbiddenError(error)) {
        return <DatasetPermissionRequired action={DATASETS_MANAGE_ACCESS_ACTION} what="Managing dataset access" />;
    }
    if (loading) return <Loading />;
    if (error) return <AlertError error={error as Error} />;

    const grants = ((response as AccessGrantList | null)?.results ?? []) as AccessGrant[];

    const grant = async (type: PrincipalType, entities: { id: string }[]) => {
        setActionError(null);
        try {
            await Promise.all(
                entities.map((entity) =>
                    datasets.grantAccess(datasetId, { principal_type: type, principal_id: entity.id }),
                ),
            );
            refetch();
        } catch (grantError) {
            setActionError(grantError as Error);
        } finally {
            setAddType(null);
        }
    };

    const revoke = async (granted: AccessGrant) => {
        setActionError(null);
        try {
            await datasets.revokeAccess(datasetId, granted.principal_type, granted.principal_id);
            refetch();
        } catch (revokeError) {
            setActionError(revokeError as Error);
        }
    };

    return (
        <div className="dataset-access-page">
            <div className="d-flex flex-wrap align-items-center mb-2">
                <h5 className="mb-0">Dataset access</h5>
                <div className="ms-auto d-flex gap-2">
                    <Button variant="success" size="sm" onClick={() => setAddType('user')}>
                        <PersonAddIcon className="me-1" /> Add user
                    </Button>
                    <Button variant="outline-primary" size="sm" onClick={() => setAddType('group')}>
                        <PeopleIcon className="me-1" /> Add group
                    </Button>
                </div>
            </div>
            <p className="text-muted small">
                Consumers listed here can read <code>{datasetId}</code> and its data through the API and the S3 gateway.
            </p>

            {actionError && <AlertError error={actionError} onDismiss={() => setActionError(null)} />}

            {grants.length === 0 ? (
                <div className="text-muted dataset-access-empty">
                    No users or groups have read access to this dataset yet.
                </div>
            ) : (
                <Table hover responsive className="dataset-access-table">
                    <thead>
                        <tr>
                            <th>Type</th>
                            <th>Principal</th>
                            <th>Granted by</th>
                            <th>Granted at</th>
                            <th />
                        </tr>
                    </thead>
                    <tbody>
                        {grants.map((granted) => (
                            <tr
                                key={`${granted.principal_type}/${granted.principal_id}`}
                                data-testid={`dataset-grant-${granted.principal_type}-${granted.principal_id}`}
                            >
                                <td>{PRINCIPAL_LABELS[granted.principal_type] ?? granted.principal_type}</td>
                                <td>
                                    <code>{granted.principal_id}</code>
                                </td>
                                <td className="text-muted small">{granted.granted_by ?? '—'}</td>
                                <td className="text-muted small">
                                    {granted.granted_at !== undefined ? (
                                        <FormattedDate dateValue={granted.granted_at} />
                                    ) : (
                                        '—'
                                    )}
                                </td>
                                <td>
                                    <ConfirmationButton
                                        variant="outline-danger"
                                        size="sm"
                                        msg={
                                            <span>
                                                Revoke read access of <strong>{granted.principal_id}</strong> to{' '}
                                                <strong>{datasetId}</strong>?
                                            </span>
                                        }
                                        onConfirm={() => revoke(granted)}
                                    >
                                        Revoke
                                    </ConfirmationButton>
                                </td>
                            </tr>
                        ))}
                    </tbody>
                </Table>
            )}

            {addType !== null && (
                <AttachModal
                    show={true}
                    emptyState={addType === 'user' ? 'No users found' : 'No groups found'}
                    filterPlaceholder={addType === 'user' ? 'Find user…' : 'Find group…'}
                    modalTitle={addType === 'user' ? 'Add users' : 'Add groups'}
                    addText={addType === 'user' ? 'Add users' : 'Add groups'}
                    headers={['', addType === 'user' ? 'User' : 'Group']}
                    searchFn={(prefix, after) =>
                        addType === 'user'
                            ? auth.listUsers(prefix, after, PageSize)
                            : auth.listGroups(prefix, after, PageSize)
                    }
                    resolveEntityFn={addType === 'user' ? resolveUserName : resolveGroupName}
                    onHide={() => setAddType(null)}
                    onAttach={(selected: { id: string }[]) => grant(addType, selected)}
                />
            )}
        </div>
    );
};

export default DatasetAccessPage;
