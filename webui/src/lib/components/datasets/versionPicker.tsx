import React, { FC } from 'react';
import Form from 'react-bootstrap/Form';

import { VersionSelection, versionOptions } from './versions';

export interface DatasetVersionPickerProps {
    /** Published version numbers, newest first. Empty until the version list loads. */
    versions: number[];
    /** The dataset's `latest_version`; `0` means "no version published yet". */
    latest: number;
    /** Current selection: `latest` or `vN`. */
    value: VersionSelection;
    onChange: (value: VersionSelection) => void;
    disabled?: boolean;
    /** Accessible name; the header and the compare page name the picker differently. */
    label?: string;
    id?: string;
    size?: 'sm' | 'lg';
    className?: string;
}

/**
 * Version selector of a dataset (DATASETS_DESIGN.md §11.2).
 *
 * A plain `select` rather than a dropdown: it is a single-value choice among a dense list, and
 * the native control gives keyboard and screen-reader support for free. `latest` is offered
 * first and labelled with the version it points at, since "latest" alone says nothing about
 * what the data currently is.
 */
export const DatasetVersionPicker: FC<DatasetVersionPickerProps> = ({
    versions,
    latest,
    value,
    onChange,
    disabled = false,
    label = 'Version',
    id,
    size,
    className = '',
}) => {
    const options = versionOptions(versions, latest);
    const hasVersions = options.length > 0;

    return (
        <Form.Select
            id={id}
            size={size}
            className={`dataset-version-picker ${className}`.trim()}
            aria-label={label}
            title={label}
            value={value}
            disabled={disabled || !hasVersions}
            onChange={(event) => onChange(event.target.value)}
        >
            {!hasVersions && <option value={value}>no versions yet</option>}
            {options.map((option) => (
                <option key={option.value} value={option.value}>
                    {option.label}
                </option>
            ))}
        </Form.Select>
    );
};

export default DatasetVersionPicker;
