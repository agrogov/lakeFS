import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';

import { DatasetUriCopyButtons, datasetUris } from './uriCopy';
import { lakefsUri, s3Uri } from './versions';

describe('dataset URIs', () => {
    it('builds the lakeFS and S3 URIs of a dataset version', () => {
        expect(lakefsUri('customer-360', 'v3')).toBe('lakefs://customer-360/v3');
        expect(s3Uri('customer-360', 'v3')).toBe('s3://customer-360/v3');
    });

    it('appends a path with exactly one separating slash', () => {
        expect(lakefsUri('customer-360', 'v3', 'customers/')).toBe('lakefs://customer-360/v3/customers/');
        expect(s3Uri('customer-360', 'v3', '/reference/countries.csv')).toBe(
            's3://customer-360/v3/reference/countries.csv',
        );
        expect(s3Uri('customer-360', 'latest')).toBe('s3://customer-360/latest');
    });

    it('lists both schemes for a dataset ref', () => {
        expect(datasetUris('customer-360', 'v2', 'a/b')).toEqual([
            { scheme: 'lakefs', uri: 'lakefs://customer-360/v2/a/b' },
            { scheme: 's3', uri: 's3://customer-360/v2/a/b' },
        ]);
    });
});

describe('DatasetUriCopyButtons', () => {
    const writeText = vi.fn(async () => {});

    beforeEach(() => {
        writeText.mockClear();
        // `copyTextToClipboard` prefers the async clipboard API; happy-dom does not ship one.
        // Clicks are dispatched with `fireEvent` rather than `userEvent`: `userEvent.setup()`
        // installs its own `navigator.clipboard` stub and would hide this one.
        Object.defineProperty(navigator, 'clipboard', {
            value: { writeText },
            configurable: true,
            writable: true,
        });
    });

    afterEach(() => {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        delete (navigator as any).clipboard;
    });

    it('shows one copy button per URI scheme', () => {
        render(<DatasetUriCopyButtons dataset="customer-360" versionRef="v3" />);

        expect(screen.getByRole('button', { name: 'Copy lakefs URI' })).toBeInTheDocument();
        expect(screen.getByRole('button', { name: 'Copy s3 URI' })).toBeInTheDocument();
    });

    it('copies the lakefs:// URI of the selected version', async () => {
        render(<DatasetUriCopyButtons dataset="customer-360" versionRef="v3" />);

        fireEvent.click(screen.getByRole('button', { name: 'Copy lakefs URI' }));

        await waitFor(() => expect(writeText).toHaveBeenCalledWith('lakefs://customer-360/v3'));
        expect(screen.getByRole('button', { name: 'Copy lakefs URI' })).toHaveTextContent('Copied');
        expect(screen.getByRole('button', { name: 'Copy s3 URI' })).not.toHaveTextContent('Copied');
    });

    it('copies the s3:// URI including the current path', async () => {
        render(<DatasetUriCopyButtons dataset="customer-360" versionRef="v1" path="customers/" />);

        fireEvent.click(screen.getByRole('button', { name: 'Copy s3 URI' }));

        await waitFor(() => expect(writeText).toHaveBeenCalledWith('s3://customer-360/v1/customers/'));
    });
});
