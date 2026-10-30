import React, { FC, ReactElement } from 'react';
import Card from 'react-bootstrap/Card';
import { DatabaseIcon, ShieldIcon } from '@primer/octicons-react';

import { AlertError, Loading } from '../controls';
import { DATASETS_DISABLED_MESSAGE } from '../../api/datasets';
import { useDatasetsConfig } from './hooks';

/**
 * The datasets gate (DATASETS_DESIGN.md §11).
 *
 * Replaces the Enterprise upsell: when `GET /datasets/config` reports `enabled: false` - or the
 * server does not serve the control plane at all (404/501) - the pages say that datasets are
 * disabled on this server instead of pretending the feature exists.
 */
export const DatasetsDisabled: FC = () => (
    <Card className="datasets-disabled-card text-center">
        <Card.Body className="datasets-disabled-content">
            <div className="datasets-disabled-icon">
                <DatabaseIcon size={24} />
            </div>
            <h4>{DATASETS_DISABLED_MESSAGE}</h4>
            <p className="text-muted mb-0">
                Ask the lakeFS administrator to enable datasets if you expected them to be available here.
            </p>
        </Card.Body>
    </Card>
);

/**
 * Renders `children` only when the control plane is enabled. `loading` and unexpected errors
 * are handled here so every dataset page can assume the feature is live.
 */
export const DatasetsGate: FC<{ children: ReactElement | ReactElement[] }> = ({ children }) => {
    const { loading, disabled, error } = useDatasetsConfig();
    if (loading) return <Loading />;
    if (disabled) return <DatasetsDisabled />;
    if (error) return <AlertError error={error} />;
    return <>{children}</>;
};

/**
 * A dataset page the caller may not use (DATASETS_DESIGN.md §9.1, §11.2).
 *
 * Access management and metadata-key management are each gated on one action, and a `403` from
 * the control plane means exactly the same thing as the policy check saying no. Saying which
 * permission is missing beats a bare "forbidden": the reader knows who to ask.
 */
export const DatasetPermissionRequired: FC<{ action: string; what: string }> = ({ action, what }) => (
    <Card className="dataset-permission-required text-center">
        <Card.Body>
            <div className="datasets-disabled-icon">
                <ShieldIcon size={24} />
            </div>
            <h4>Permission required</h4>
            <p className="text-muted mb-0">
                {what} requires the <code>{action}</code> permission. Ask an administrator to grant it on this server.
            </p>
        </Card.Body>
    </Card>
);
