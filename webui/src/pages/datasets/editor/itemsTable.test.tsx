import React from 'react';
import { describe, expect, it, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import { ItemsTable, ItemsTableProps } from './itemsTable';
import { EditorItem } from './editorModel';

// The pickers hit the repository APIs; the table logic under test does not need them.
vi.mock('./itemPickers', () => ({
    RepositoryPicker: ({ value, onChange }: { value: string; onChange: (value: string) => void }) => (
        <select aria-label="Source repository" value={value} onChange={(e) => onChange(e.target.value)}>
            <option value="">Select…</option>
            <option value="lakehouse-repo">lakehouse-repo</option>
        </select>
    ),
    RefPicker: ({ refId, pinnedCommit }: { refId: string; pinnedCommit?: string }) => (
        <span data-testid="ref-picker">{pinnedCommit ? `pinned to ${pinnedCommit}` : refId}</span>
    ),
    PathPicker: () => null,
}));

const item = (overrides: Partial<EditorItem> = {}): EditorItem => ({
    key: 'row-1',
    repository: 'lakehouse-repo',
    refType: 'branch',
    refId: 'main',
    type: 'prefix',
    path: 'data/customers',
    target: 'customers',
    ...overrides,
});

const renderTable = (items: EditorItem[], serverErrors: ItemsTableProps['serverErrors'] = {}, onAdd = vi.fn()) => {
    render(
        <ItemsTable
            items={items}
            repositories={['lakehouse-repo']}
            serverErrors={serverErrors}
            onChange={vi.fn()}
            onRemove={vi.fn()}
            onAdd={onAdd}
        />,
    );
    return { onAdd };
};

describe('ItemsTable', () => {
    it('offers an add button when there are no items', async () => {
        const user = userEvent.setup();
        const { onAdd } = renderTable([]);

        await user.click(screen.getByRole('button', { name: /add item/i }));
        expect(onAdd).toHaveBeenCalled();
    });

    it('shows how a target and a prefix path will be stored', () => {
        renderTable([item({ target: '/customers/', path: '/data/customers' })]);

        // The "/" a user typed around the target is stripped before storage, and the source
        // prefix gains exactly one trailing slash (§5.2).
        expect(screen.getAllByText(/Stored as/)).toHaveLength(2);
        expect(screen.getByText('customers', { selector: 'code' })).toBeInTheDocument();
        expect(screen.getByText('data/customers/', { selector: 'code' })).toBeInTheDocument();
    });

    it('displays duplicate-target overlap errors inline', () => {
        renderTable([item({ key: 'row-1', target: 'customers' }), item({ key: 'row-2', target: '/customers/' })]);

        expect(screen.getByText('Target duplicates "customers"')).toBeInTheDocument();
    });

    it('displays ancestor overlap errors inline', () => {
        renderTable([item({ key: 'row-1', target: 'customers' }), item({ key: 'row-2', target: 'customers/orders' })]);

        expect(screen.getByText('Target overlaps "customers"')).toBeInTheDocument();
    });

    it('does not report two distinct sibling targets', () => {
        renderTable([item({ key: 'row-1', target: 'customers' }), item({ key: 'row-2', target: 'orders' })]);

        expect(screen.queryByText(/overlaps/)).not.toBeInTheDocument();
        expect(screen.queryByText(/duplicates/)).not.toBeInTheDocument();
    });

    it('renders the server problems against the offending row', () => {
        renderTable([item({ key: 'row-1' })], {
            'row-1': [{ field: 'repository', message: 'cannot read source repository' }],
        });

        expect(screen.getByRole('alert')).toHaveTextContent('repository: cannot read source repository');
    });

    it('shows the pinned commit the validation resolved', () => {
        renderTable([item({ refType: 'branch', refId: 'main', pinnedCommit: '30f9eb1c9a3d' })]);

        expect(screen.getByTestId('ref-picker')).toHaveTextContent('pinned to 30f9eb1c9a3d');
    });
});
