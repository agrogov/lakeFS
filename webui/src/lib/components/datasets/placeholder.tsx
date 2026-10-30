import React, { FC } from 'react';
import Alert from 'react-bootstrap/Alert';

/**
 * Placeholder for a dataset page that a later card fills in (DATASETS_DESIGN.md §11.1).
 *
 * The route tree has to be navigable now, so every not-yet-built page renders one of these
 * instead of a broken route. Keeping them in one component means the real page replaces just
 * one file per route.
 */
export const DatasetPlaceholder: FC<{ title: string; note?: string }> = ({ title, note }) => (
    <Alert variant="secondary" className="dataset-placeholder">
        <strong>{title}</strong>
        <div className="text-muted">{note ?? 'This page is not available in this build yet.'}</div>
    </Alert>
);
