import React, { useState } from 'react';
import { describe, expect, it, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import { DatasetVersionPicker } from './versionPicker';

/** Mimics a page: the selection is owned by the parent, as the URL owns it in the real pages. */
const Harness = ({
    versions,
    latest,
    initial = 'latest',
}: {
    versions: number[];
    latest: number;
    initial?: string;
}) => {
    const [value, setValue] = useState(initial);
    return (
        <>
            <DatasetVersionPicker
                versions={versions}
                latest={latest}
                value={value}
                onChange={setValue}
                id="version"
                label="Version"
            />
            <output data-testid="selection">{value}</output>
        </>
    );
};

describe('DatasetVersionPicker', () => {
    it('offers latest first, labelled with the version it points at, then every version newest first', () => {
        render(<Harness versions={[3, 2, 1]} latest={3} />);

        const options = screen.getAllByRole('option').map((option) => option.textContent);
        expect(options).toEqual(['latest (v3)', 'v3', 'v2', 'v1']);
    });

    it('reports the selected version to the page', async () => {
        const user = userEvent.setup();
        render(<Harness versions={[3, 2, 1]} latest={3} />);

        await user.selectOptions(screen.getByLabelText('Version'), 'v2');

        expect(screen.getByTestId('selection')).toHaveTextContent('v2');
        expect((screen.getByLabelText('Version') as HTMLSelectElement).value).toBe('v2');
    });

    it('falls back to the version numbers when the version list has not loaded', () => {
        render(<Harness versions={[]} latest={2} />);

        expect(screen.getAllByRole('option').map((option) => option.textContent)).toEqual(['latest (v2)', 'v2', 'v1']);
    });

    it('is disabled and says so when the dataset has no versions', () => {
        render(<Harness versions={[]} latest={0} />);

        const select = screen.getByLabelText('Version');
        expect(select).toBeDisabled();
        expect(screen.getByRole('option', { name: 'no versions yet' })).toBeInTheDocument();
    });

    it('can be disabled from the outside', () => {
        render(
            <DatasetVersionPicker
                versions={[1]}
                latest={1}
                value="latest"
                onChange={vi.fn()}
                disabled
                label="Version"
            />,
        );

        expect(screen.getByLabelText('Version')).toBeDisabled();
    });
});
