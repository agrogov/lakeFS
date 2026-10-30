import { describe, expect, it } from 'vitest';

import { DataItem, DatasetDefinition } from '../../api/datasets';
import {
    countDefinitionDiff,
    definitionDiff,
    definitionsEqual,
    diffDefinitionItems,
    diffDefinitionMetadata,
    itemSourcePath,
} from './definitionDiff';

const item = (overrides: Partial<DataItem> = {}): DataItem => ({
    target: 'customers',
    repository: 'lakehouse-repo',
    ref: { type: 'commit', id: 'commit-a' },
    type: 'prefix',
    prefix: { path: 'data/customers/' },
    ...overrides,
});

const definition = (overrides: Partial<DatasetDefinition> = {}): DatasetDefinition => ({
    format_version: 1,
    description: 'Curated customer view',
    metadata: { tier: 'gold' },
    data: [item()],
    ...overrides,
});

describe('itemSourcePath', () => {
    it('reads the path of either item type', () => {
        expect(itemSourcePath(item())).toBe('data/customers/');
        expect(
            itemSourcePath(
                item({ target: 'ref.csv', type: 'object', prefix: undefined, object: { path: 'geo/ref.csv' } }),
            ),
        ).toBe('geo/ref.csv');
        expect(itemSourcePath(undefined)).toBe('');
    });
});

describe('diffDefinitionItems', () => {
    it('matches items by target and reports added, removed and unchanged items', () => {
        const left = definition({ data: [item(), item({ target: 'orders', prefix: { path: 'data/orders/' } })] });
        const right = definition({ data: [item(), item({ target: 'refunds', prefix: { path: 'data/refunds/' } })] });

        expect(diffDefinitionItems(left, right)).toEqual([
            {
                target: 'orders',
                change: 'removed',
                from: item({ target: 'orders', prefix: { path: 'data/orders/' } }),
            },
            {
                target: 'refunds',
                change: 'added',
                to: item({ target: 'refunds', prefix: { path: 'data/refunds/' } }),
            },
        ]);
    });

    it.each([
        ['repository', item({ repository: 'other-repo' })],
        ['commit', item({ ref: { type: 'commit', id: 'commit-b' } })],
        ['type', item({ type: 'object', prefix: undefined, object: { path: 'data/customers/' } })],
        ['source path', item({ prefix: { path: 'data/customers-v2/' } })],
    ])('marks an item changed when its %s differs', (_label, changed) => {
        const changes = diffDefinitionItems(definition(), definition({ data: [changed] }));

        expect(changes).toHaveLength(1);
        expect(changes[0].change).toBe('changed');
        expect(changes[0].to).toEqual(changed);
    });

    it('reports nothing for identical definitions', () => {
        expect(diffDefinitionItems(definition(), definition())).toEqual([]);
    });

    it('treats a missing definition as empty', () => {
        expect(diffDefinitionItems(undefined, definition())).toEqual([
            { target: 'customers', change: 'added', to: item() },
        ]);
        expect(diffDefinitionItems(definition(), undefined)).toEqual([
            { target: 'customers', change: 'removed', from: item() },
        ]);
    });
});

describe('diffDefinitionMetadata', () => {
    it('reports added, removed and changed keys', () => {
        const left = definition({ metadata: { tier: 'gold', owner: 'data-platform', drop: 'me' } });
        const right = definition({ metadata: { tier: 'platinum', owner: 'data-platform', added: 'yes' } });

        expect(diffDefinitionMetadata(left, right)).toEqual([
            { key: 'added', change: 'added', to: 'yes' },
            { key: 'drop', change: 'removed', from: 'me' },
            { key: 'tier', change: 'changed', from: 'gold', to: 'platinum' },
        ]);
    });

    it('compares multi-value metadata as a whole', () => {
        const left = definition({ metadata: { regions: ['emea'] } });
        const right = definition({ metadata: { regions: ['emea', 'us'] } });

        expect(diffDefinitionMetadata(left, right)).toEqual([
            { key: 'regions', change: 'changed', from: ['emea'], to: ['emea', 'us'] },
        ]);
    });
});

describe('definitionDiff', () => {
    it('omits the sections that did not change', () => {
        const diff = definitionDiff(definition(), definition());

        expect(diff).toEqual({ items: [] });
        expect(diff.description).toBeUndefined();
        expect(diff.metadata).toBeUndefined();
    });

    it('reports a description-only change', () => {
        const diff = definitionDiff(definition(), definition({ description: 'Something else' }));

        expect(diff).toEqual({
            items: [],
            description: { from: 'Curated customer view', to: 'Something else' },
        });
    });

    it('counts the item changes for the timeline', () => {
        const left = definition({ data: [item(), item({ target: 'orders' })] });
        const right = definition({
            data: [item({ prefix: { path: 'data/customers-v2/' } }), item({ target: 'refunds' })],
        });

        expect(countDefinitionDiff(definitionDiff(left, right))).toEqual({
            added: 1,
            removed: 1,
            changed: 1,
            total: 3,
        });
    });

    it('knows when two definitions are equal', () => {
        expect(definitionsEqual(definition(), definition())).toBe(true);
        expect(definitionsEqual(definition(), definition({ metadata: { tier: 'silver' } }))).toBe(false);
        expect(definitionsEqual(definition(), definition({ description: 'Other' }))).toBe(false);
        expect(definitionsEqual(undefined, undefined)).toBe(true);
    });
});
