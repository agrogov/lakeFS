import React, { useState } from 'react';
import { describe, expect, it } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import { MetadataKey } from '../../../lib/api/datasets';
import { MetadataDraft, MetadataProblems, validateMetadataDraft } from './editorModel';
import { MetadataForm } from './metadataForm';

const key = (overrides: Partial<MetadataKey> = {}): MetadataKey => ({
    name: 'owner',
    type: 'string',
    required: false,
    allow_multiple: false,
    created_at: 0,
    created_by: 'tester',
    ...overrides,
});

/** Renders the form with the same wiring the editor uses: draft in, problems derived. */
const Harness = ({ keys, initial = {} }: { keys: MetadataKey[]; initial?: MetadataDraft }) => {
    const [draft, setDraft] = useState<MetadataDraft>(initial);
    const problems: MetadataProblems = validateMetadataDraft(keys, draft);
    return (
        <>
            <MetadataForm
                keys={keys}
                draft={draft}
                problems={problems}
                onChange={(name, value) => setDraft((current) => ({ ...current, [name]: value }))}
            />
            <output data-testid="draft">{JSON.stringify(draft)}</output>
        </>
    );
};

const draftOf = () => JSON.parse(screen.getByTestId('draft').textContent ?? '{}');

describe('MetadataForm', () => {
    it('marks required keys and enforces them', async () => {
        const user = userEvent.setup();
        render(<Harness keys={[key({ required: true })]} />);

        expect(screen.getByText('*')).toBeInTheDocument();
        expect(screen.getByText('Required')).toBeInTheDocument();

        await user.type(screen.getByLabelText(/owner/), 'data-platform');

        expect(screen.queryByText('Required')).not.toBeInTheDocument();
        expect(draftOf()).toEqual({ owner: 'data-platform' });
    });

    it('renders an enum key as a dropdown of the allowed values', async () => {
        const user = userEvent.setup();
        render(<Harness keys={[key({ name: 'tier', type: 'enum', allowed_values: ['gold', 'silver'] })]} />);

        const select = screen.getByLabelText(/tier/);
        expect(select.tagName).toBe('SELECT');
        expect(screen.getByRole('option', { name: 'gold' })).toBeInTheDocument();
        expect(screen.getByRole('option', { name: 'silver' })).toBeInTheDocument();

        await user.selectOptions(select, 'gold');
        expect(draftOf()).toEqual({ tier: 'gold' });
    });

    it('shows the enum error for a value outside allowed_values', () => {
        render(
            <Harness
                keys={[key({ name: 'tier', type: 'enum', allowed_values: ['gold'] })]}
                initial={{ tier: 'bronze' }}
            />,
        );
        expect(screen.getByText('Must be one of: gold')).toBeInTheDocument();
    });

    it('renders a date key as a date picker and accepts a real calendar day', () => {
        render(<Harness keys={[key({ name: 'release', type: 'date' })]} />);

        const input = screen.getByLabelText(/release/);
        expect(input).toHaveAttribute('type', 'date');

        fireEvent.change(input, { target: { value: '2024-02-29' } });
        expect(draftOf()).toEqual({ release: '2024-02-29' });
        expect(screen.queryByText('Must be a date (YYYY-MM-DD)')).not.toBeInTheDocument();
    });

    it('reports an impossible date', () => {
        render(<Harness keys={[key({ name: 'release', type: 'date' })]} initial={{ release: '2023-02-29' }} />);
        expect(screen.getByText('Must be a date (YYYY-MM-DD)')).toBeInTheDocument();
    });

    it('renders a number key as a number input and rejects a non-numeric value', () => {
        render(<Harness keys={[key({ name: 'priority', type: 'number' })]} initial={{ priority: 'high' }} />);

        expect(screen.getByLabelText(/priority/)).toHaveAttribute('type', 'number');
        expect(screen.getByText('Must be a number')).toBeInTheDocument();

        fireEvent.change(screen.getByLabelText(/priority/), { target: { value: '2' } });
        expect(draftOf()).toEqual({ priority: '2' });
        expect(screen.queryByText('Must be a number')).not.toBeInTheDocument();
    });

    it('renders a boolean key as a toggle', async () => {
        const user = userEvent.setup();
        render(<Harness keys={[key({ name: 'public', type: 'boolean' })]} />);

        const toggle = screen.getByRole('checkbox', { name: /public/i });
        expect(toggle).not.toBeChecked();

        await user.click(toggle);
        expect(draftOf()).toEqual({ public: 'true' });
    });

    it('adds and removes multi-value chips', async () => {
        const user = userEvent.setup();
        render(<Harness keys={[key({ name: 'tags', allow_multiple: true })]} />);

        const input = screen.getByLabelText('Add tags value');
        await user.type(input, 'analytics');
        await user.click(screen.getByRole('button', { name: /add/i }));

        expect(screen.getByText('analytics')).toBeInTheDocument();
        expect(draftOf()).toEqual({ tags: ['analytics'] });

        await user.type(input, 'finance');
        await user.click(screen.getByRole('button', { name: /add/i }));
        expect(draftOf()).toEqual({ tags: ['analytics', 'finance'] });

        await user.click(screen.getByRole('button', { name: 'Remove analytics' }));
        expect(screen.queryByText('analytics')).not.toBeInTheDocument();
        expect(draftOf()).toEqual({ tags: ['finance'] });
    });

    it('adds an enum chip from the allowed values', async () => {
        const user = userEvent.setup();
        render(
            <Harness
                keys={[key({ name: 'tiers', type: 'enum', allow_multiple: true, allowed_values: ['gold', 'silver'] })]}
            />,
        );

        await user.selectOptions(screen.getByLabelText('Add tiers value'), 'silver');
        await user.click(screen.getByRole('button', { name: /add/i }));

        expect(draftOf()).toEqual({ tiers: ['silver'] });
    });

    it('renders the problems it is given against the key', () => {
        render(
            <MetadataForm
                keys={[key({ name: 'tier', type: 'enum', allowed_values: ['gold', 'silver'] })]}
                draft={{}}
                problems={{ tier: ['Must be one of: gold, silver'] }}
                onChange={() => {}}
            />,
        );

        expect(screen.getByRole('alert')).toHaveTextContent('Must be one of: gold, silver');
    });
});
