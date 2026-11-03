import React, { FC } from 'react';
import Badge from 'react-bootstrap/Badge';
import Card from 'react-bootstrap/Card';
import Table from 'react-bootstrap/Table';
import dayjs from 'dayjs';

import { AlertError, Loading } from '../controls';
import { useAPI } from '../../hooks/api';
import {
    IcebergLoadTableResult,
    IcebergSnapshot,
    currentPartitionSpec,
    currentSchema,
    currentSnapshot,
    formatIcebergType,
    iceberg,
    isIcebergForbiddenError,
    namespaceLabel,
    snapshotHistory,
    snapshotOperation,
    sourceFieldName,
} from '../../api/iceberg';
import { TablesPermissionRequired } from './tablesStates';

/** A timestamp of the metadata document, which is in epoch milliseconds. */
const formatTimestamp = (millis: number | undefined): string =>
    millis === undefined ? '-' : dayjs(millis).format('YYYY-MM-DD HH:mm:ss');

/**
 * The read-only view of one Iceberg table: its schema, its partition spec, its current snapshot
 * and snapshot history, its properties, and the physical locations behind it.
 *
 * Everything shown comes from the catalog's own load endpoint
 * (`GET /v1/{prefix}/namespaces/{namespace}/tables/{table}`), so the view cannot disagree with what
 * an Iceberg client reads. Nothing here writes: the metadata is rendered, never edited.
 */
export const IcebergTableDetails: FC<{
    /** The full catalog address of the table: `[repository, ref, …namespace levels]`. */
    namespace: string[];
    table: string;
}> = ({ namespace, table }) => {
    // The namespace is the levels array the catalog addresses the table with; `namespaceKey` is the
    // same address as one string, so the hook's dependencies stay simple values.
    const namespaceKey = namespace.join('\u001f');
    const { response, loading, error } = useAPI(
        () => iceberg.loadTable([...namespaceKey.split('\u001f')], table),
        [namespaceKey, table],
    );

    if (loading) return <Loading />;
    if (error) {
        if (isIcebergForbiddenError(error)) {
            return <TablesPermissionRequired what={`Reading the table ${table}`} />;
        }
        return <AlertError error={error} />;
    }
    if (!response) return <></>;

    const result = response as IcebergLoadTableResult;
    const metadata = result.metadata;
    const schema = currentSchema(metadata);
    const partitionSpec = currentPartitionSpec(metadata);
    const snapshot = currentSnapshot(metadata);
    const history = snapshotHistory(metadata);
    const properties = Object.entries(metadata.properties ?? {}).sort(([left], [right]) => left.localeCompare(right));

    return (
        <div className="d-flex flex-column gap-3" data-testid="iceberg-table-details">
            <Card>
                <Card.Body>
                    <div className="d-flex flex-wrap align-items-center gap-2 mb-2">
                        <h5 className="mb-0">{table}</h5>
                        {metadata['format-version'] !== undefined && (
                            <Badge bg="secondary">format v{metadata['format-version']}</Badge>
                        )}
                        {snapshot && <Badge bg="info">{snapshotOperation(snapshot) ?? 'snapshot'}</Badge>}
                    </div>
                    <div className="text-muted small mb-3">
                        in namespace <code>{namespaceLabel(namespace)}</code>
                    </div>
                    <dl className="row mb-0 small">
                        <dt className="col-sm-3">Metadata location</dt>
                        <dd className="col-sm-9 text-break">
                            <code>{result['metadata-location'] || '-'}</code>
                        </dd>
                        <dt className="col-sm-3">Table location</dt>
                        <dd className="col-sm-9 text-break">
                            <code>{metadata.location || '-'}</code>
                        </dd>
                        <dt className="col-sm-3">Table UUID</dt>
                        <dd className="col-sm-9 text-break">
                            <code>{metadata['table-uuid'] || '-'}</code>
                        </dd>
                        <dt className="col-sm-3">Last updated</dt>
                        <dd className="col-sm-9">{formatTimestamp(metadata['last-updated-ms'])}</dd>
                    </dl>
                </Card.Body>
            </Card>

            <Card>
                <Card.Header>
                    <h6 className="mb-0">
                        Schema
                        {schema?.['schema-id'] !== undefined && (
                            <span className="text-muted fw-normal"> (schema {schema['schema-id']})</span>
                        )}
                    </h6>
                </Card.Header>
                <Card.Body className="p-0">
                    {schema?.fields && schema.fields.length > 0 ? (
                        <Table responsive hover className="mb-0">
                            <thead>
                                <tr>
                                    <th>ID</th>
                                    <th>Name</th>
                                    <th>Type</th>
                                    <th>Required</th>
                                    <th>Description</th>
                                </tr>
                            </thead>
                            <tbody>
                                {schema.fields.map((field) => (
                                    <tr key={field.id}>
                                        <td>{field.id}</td>
                                        <td>
                                            <code>{field.name}</code>
                                        </td>
                                        <td>
                                            <code>{formatIcebergType(field.type)}</code>
                                        </td>
                                        <td>{field.required ? 'required' : 'optional'}</td>
                                        <td className="text-muted">{field.doc || '-'}</td>
                                    </tr>
                                ))}
                            </tbody>
                        </Table>
                    ) : (
                        <div className="p-3 text-muted">This table has no schema.</div>
                    )}
                </Card.Body>
            </Card>

            <Card>
                <Card.Header>
                    <h6 className="mb-0">
                        Partition spec
                        {partitionSpec.id !== undefined && (
                            <span className="text-muted fw-normal"> (spec {partitionSpec.id})</span>
                        )}
                    </h6>
                </Card.Header>
                <Card.Body className="p-0">
                    {partitionSpec.fields.length > 0 ? (
                        <Table responsive hover className="mb-0">
                            <thead>
                                <tr>
                                    <th>Field</th>
                                    <th>Transform</th>
                                    <th>Source column</th>
                                    <th>Field ID</th>
                                </tr>
                            </thead>
                            <tbody>
                                {partitionSpec.fields.map((field) => (
                                    <tr key={field['field-id']}>
                                        <td>
                                            <code>{field.name}</code>
                                        </td>
                                        <td>
                                            <code>{field.transform}</code>
                                        </td>
                                        <td>{sourceFieldName(schema, field['source-id']) ?? field['source-id']}</td>
                                        <td>{field['field-id']}</td>
                                    </tr>
                                ))}
                            </tbody>
                        </Table>
                    ) : (
                        <div className="p-3 text-muted">Unpartitioned table.</div>
                    )}
                </Card.Body>
            </Card>

            <CurrentSnapshot snapshot={snapshot} />
            <SnapshotHistory snapshots={history} currentId={metadata['current-snapshot-id']} />

            <Card>
                <Card.Header>
                    <h6 className="mb-0">Properties</h6>
                </Card.Header>
                <Card.Body className="p-0">
                    {properties.length > 0 ? (
                        <Table responsive hover className="mb-0">
                            <thead>
                                <tr>
                                    <th>Key</th>
                                    <th>Value</th>
                                </tr>
                            </thead>
                            <tbody>
                                {properties.map(([key, value]) => (
                                    <tr key={key}>
                                        <td>
                                            <code>{key}</code>
                                        </td>
                                        <td className="text-break">{value}</td>
                                    </tr>
                                ))}
                            </tbody>
                        </Table>
                    ) : (
                        <div className="p-3 text-muted">This table has no properties.</div>
                    )}
                </Card.Body>
            </Card>
        </div>
    );
};

/** The snapshot the table's readers currently see, with the counters its summary carries. */
const CurrentSnapshot: FC<{ snapshot: IcebergSnapshot | undefined }> = ({ snapshot }) => (
    <Card>
        <Card.Header>
            <h6 className="mb-0">Current snapshot</h6>
        </Card.Header>
        <Card.Body>
            {snapshot ? (
                <>
                    <dl className="row mb-0 small">
                        <dt className="col-sm-3">Snapshot ID</dt>
                        <dd className="col-sm-9">
                            <code>{snapshot['snapshot-id']}</code>
                        </dd>
                        <dt className="col-sm-3">Operation</dt>
                        <dd className="col-sm-9">{snapshotOperation(snapshot) ?? '-'}</dd>
                        <dt className="col-sm-3">Committed at</dt>
                        <dd className="col-sm-9">{formatTimestamp(snapshot['timestamp-ms'])}</dd>
                        <dt className="col-sm-3">Sequence number</dt>
                        <dd className="col-sm-9">{snapshot['sequence-number'] ?? '-'}</dd>
                        <dt className="col-sm-3">Manifest list</dt>
                        <dd className="col-sm-9 text-break">
                            <code>{snapshot['manifest-list'] || '-'}</code>
                        </dd>
                        {summaryEntries(snapshot).map(([key, value]) => (
                            <React.Fragment key={key}>
                                <dt className="col-sm-3">{key}</dt>
                                <dd className="col-sm-9">{value}</dd>
                            </React.Fragment>
                        ))}
                    </dl>
                </>
            ) : (
                <div className="text-muted">This table has no current snapshot: nothing was written to it yet.</div>
            )}
        </Card.Body>
    </Card>
);

/** Every summary counter except the operation, which is shown on its own row above. */
const summaryEntries = (snapshot: IcebergSnapshot): [string, string][] =>
    Object.entries(snapshot.summary ?? {})
        .filter(([key]) => key !== 'operation')
        .sort(([left], [right]) => left.localeCompare(right));

/** Every snapshot the metadata still carries, newest first, with the current one marked. */
const SnapshotHistory: FC<{ snapshots: IcebergSnapshot[]; currentId?: number }> = ({ snapshots, currentId }) => (
    <Card>
        <Card.Header>
            <h6 className="mb-0">Snapshot history</h6>
        </Card.Header>
        <Card.Body className="p-0">
            {snapshots.length > 0 ? (
                <Table responsive hover className="mb-0">
                    <thead>
                        <tr>
                            <th>Snapshot ID</th>
                            <th>Operation</th>
                            <th>Committed at</th>
                            <th>Sequence</th>
                            <th>Parent</th>
                        </tr>
                    </thead>
                    <tbody>
                        {snapshots.map((snapshot) => (
                            <tr key={snapshot['snapshot-id']}>
                                <td>
                                    <code>{snapshot['snapshot-id']}</code>
                                    {snapshot['snapshot-id'] === currentId && (
                                        <Badge bg="info" className="ms-2">
                                            current
                                        </Badge>
                                    )}
                                </td>
                                <td>{snapshotOperation(snapshot) ?? '-'}</td>
                                <td>{formatTimestamp(snapshot['timestamp-ms'])}</td>
                                <td>{snapshot['sequence-number'] ?? '-'}</td>
                                <td>
                                    <code>{snapshot['parent-snapshot-id'] ?? '-'}</code>
                                </td>
                            </tr>
                        ))}
                    </tbody>
                </Table>
            ) : (
                <div className="p-3 text-muted">This table has no snapshots: nothing was written to it yet.</div>
            )}
        </Card.Body>
    </Card>
);
