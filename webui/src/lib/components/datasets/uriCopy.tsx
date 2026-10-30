import React, { FC, useState } from 'react';
import Button from 'react-bootstrap/Button';
import ButtonGroup from 'react-bootstrap/ButtonGroup';
import { CheckIcon, CopyIcon } from '@primer/octicons-react';

import { copyTextToClipboard } from '../controls';
import { lakefsUri, s3Uri } from './versions';

/** The two URIs a dataset is reachable through (§7.2): the lakeFS one and the S3 gateway one. */
export type DatasetUriScheme = 'lakefs' | 's3';

export interface DatasetUri {
    scheme: DatasetUriScheme;
    uri: string;
}

/**
 * The URIs of a dataset at a ref: `lakefs://<dataset>/<ref>[/<path>]` and the S3 equivalent.
 * Both address the same data, so they are rendered side by side.
 */
export const datasetUris = (dataset: string, ref: string, path = ''): DatasetUri[] => [
    { scheme: 'lakefs', uri: lakefsUri(dataset, ref, path) },
    { scheme: 's3', uri: s3Uri(dataset, ref, path) },
];

/**
 * One "copy" button per URI (§11.2). The confirmation is local to the button ("Copied") and
 * resets on the next click, so copying twice looks the same as copying once.
 */
export const DatasetUriCopyButtons: FC<{
    dataset: string;
    /** The version ref the buttons copy: `latest` or `vN`. Not named `ref`: React owns that. */
    versionRef: string;
    path?: string;
    className?: string;
    size?: 'sm' | 'lg';
}> = ({ dataset, versionRef, path = '', className = '', size }) => {
    const [copied, setCopied] = useState<DatasetUriScheme | null>(null);

    return (
        <ButtonGroup size={size} className={`dataset-uri-buttons ${className}`.trim()}>
            {datasetUris(dataset, versionRef, path).map(({ scheme, uri }) => {
                const isCopied = copied === scheme;
                return (
                    <Button
                        key={scheme}
                        variant="outline-secondary"
                        title={uri}
                        aria-label={`Copy ${scheme} URI`}
                        onClick={() => {
                            setCopied(scheme);
                            copyTextToClipboard(uri);
                        }}
                    >
                        {isCopied ? <CheckIcon className="me-1" /> : <CopyIcon className="me-1" />}
                        <code className="dataset-uri-scheme">{`${scheme}://`}</code>
                        {isCopied && <span className="dataset-uri-copied ms-1">Copied</span>}
                    </Button>
                );
            })}
        </ButtonGroup>
    );
};

export default DatasetUriCopyButtons;
