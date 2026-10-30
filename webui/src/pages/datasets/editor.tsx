import React, { FC } from 'react';
import Container from 'react-bootstrap/Container';
import Breadcrumb from 'react-bootstrap/Breadcrumb';
import { useParams } from 'react-router-dom';

import { DatasetsGate } from '../../lib/components/datasets/gate';
import { DatasetPlaceholder } from '../../lib/components/datasets/placeholder';
import { Link } from '../../lib/components/nav';

export type DatasetEditorMode = 'create' | 'edit';

const EditorBreadcrumb: FC<{ mode: DatasetEditorMode; datasetId: string }> = ({ mode, datasetId }) => (
    <Breadcrumb>
        <Link href={{ pathname: '/datasets' }} component={Breadcrumb.Item}>
            Datasets
        </Link>
        {mode === 'edit' && (
            <Link
                href={{ pathname: '/datasets/:datasetId/overview', params: { datasetId } }}
                component={Breadcrumb.Item}
            >
                {datasetId}
            </Link>
        )}
        <Breadcrumb.Item active>{mode === 'edit' ? 'Edit' : 'Create dataset'}</Breadcrumb.Item>
    </Breadcrumb>
);

/**
 * Create / edit a dataset (§11.2): name, description, metadata form, items table, live
 * validation and the publish or open-proposal footer. Filled in by the editor card.
 *
 * `mode` is the only difference between the two routes: creating also picks the name, editing
 * starts from the latest definition and publishes as `latest_version + 1`.
 */
const DatasetEditorPage: FC<{ mode: DatasetEditorMode }> = ({ mode }) => {
    const { datasetId = '' } = useParams();
    return (
        <DatasetsGate>
            <Container fluid="xl">
                <EditorBreadcrumb mode={mode} datasetId={datasetId} />
                <div className="mt-4">
                    <DatasetPlaceholder
                        title={mode === 'edit' ? `Edit ${datasetId}` : 'Create dataset'}
                        note="The definition editor, metadata form and item picker will be shown here."
                    />
                </div>
            </Container>
        </DatasetsGate>
    );
};

export default DatasetEditorPage;
