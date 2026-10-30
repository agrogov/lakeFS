import React, { FC } from 'react';
import Table from 'react-bootstrap/Table';
import Badge from 'react-bootstrap/Badge';

import { MetadataValue } from '../../api/datasets';
import { formatMetadataValue } from './metadataBadges';

export interface MetadataTableProps {
    metadata?: Record<string, MetadataValue>;
    emptyText?: string;
}

/**
 * The metadata of a dataset version as a key/value table (DATASETS_DESIGN.md §11.2).
 *
 * Keys are sorted so the same metadata always reads the same way; the values keep their type in
 * the rendering (`true` stays `true`, arrays join with commas) because the type is part of the
 * metadata key definition (§5.5).
 */
export const MetadataTable: FC<MetadataTableProps> = ({ metadata, emptyText = 'No metadata' }) => {
    const entries = Object.entries(metadata ?? {}).sort(([left], [right]) => left.localeCompare(right));
    if (entries.length === 0) {
        return <div className="text-muted dataset-metadata-empty">{emptyText}</div>;
    }

    return (
        <Table hover responsive size="sm" className="dataset-metadata-table">
            <tbody>
                {entries.map(([key, value]) => (
                    <tr key={key}>
                        <th scope="row" className="dataset-metadata-table-key">
                            {key}
                        </th>
                        <td>
                            {Array.isArray(value) ? (
                                <span className="d-flex flex-wrap gap-1">
                                    {value.map((entry) => (
                                        <Badge key={String(entry)} bg="light" text="dark" className="border">
                                            {String(entry)}
                                        </Badge>
                                    ))}
                                </span>
                            ) : (
                                <code className="dataset-metadata-value">{formatMetadataValue(value)}</code>
                            )}
                        </td>
                    </tr>
                ))}
            </tbody>
        </Table>
    );
};

export default MetadataTable;
