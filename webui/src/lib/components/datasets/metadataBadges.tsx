import React, { FC } from 'react';
import Badge from 'react-bootstrap/Badge';
import OverlayTrigger from 'react-bootstrap/OverlayTrigger';
import Tooltip from 'react-bootstrap/Tooltip';

import { MetadataValue } from '../../api/datasets';

/** Renders a metadata value of any supported type (`string | number | boolean | string[]`, §5.1). */
export const formatMetadataValue = (value: MetadataValue | undefined): string => {
    if (value === undefined || value === null) return '';
    if (Array.isArray(value)) return value.join(', ');
    return String(value);
};

/**
 * Metadata badges of a dataset (`tier: gold`, `owner: data-platform`, ...), as the list view
 * shows them (§11.2). Keys are sorted so the same metadata always renders in the same order,
 * and long lists collapse into a `+N` badge that reveals the rest on hover.
 */
export const MetadataBadges: FC<{ metadata?: Record<string, MetadataValue>; limit?: number }> = ({
    metadata,
    limit = 4,
}) => {
    const entries = Object.entries(metadata ?? {}).sort(([left], [right]) => left.localeCompare(right));
    if (entries.length === 0) return null;

    const shown = entries.slice(0, limit);
    const hidden = entries.slice(limit);
    return (
        <div className="dataset-metadata-badges d-flex flex-wrap gap-1">
            {shown.map(([key, value]) => {
                const text = formatMetadataValue(value);
                return (
                    <OverlayTrigger key={key} placement="bottom" overlay={<Tooltip>{`${key}: ${text}`}</Tooltip>}>
                        <Badge
                            bg="light"
                            text="dark"
                            className="border dataset-metadata-badge text-truncate"
                            aria-label={`${key}: ${text}`}
                        >
                            <span className="dataset-metadata-key">{key}:</span> {text}
                        </Badge>
                    </OverlayTrigger>
                );
            })}
            {hidden.length > 0 && (
                <OverlayTrigger
                    placement="bottom"
                    overlay={
                        <Tooltip>
                            {hidden.map(([key, value]) => `${key}: ${formatMetadataValue(value)}`).join(', ')}
                        </Tooltip>
                    }
                >
                    <Badge bg="secondary" className="dataset-metadata-badge">
                        +{hidden.length}
                    </Badge>
                </OverlayTrigger>
            )}
        </div>
    );
};
