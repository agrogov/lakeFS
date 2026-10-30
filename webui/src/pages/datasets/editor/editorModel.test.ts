import { describe, expect, it } from 'vitest';

import { DatasetDefinition, MetadataKey } from '../../../lib/api/datasets';
import {
    EditorItem,
    buildDefinition,
    buildMetadata,
    hasProblems,
    itemsFromDefinition,
    localItemProblems,
    mapValidationErrors,
    metadataDraftFromValues,
    normaliseSourcePath,
    normaliseTarget,
    overlapProblems,
    pinnedCommits,
    targetProblem,
    validateMetadataDraft,
} from './editorModel';

const item = (overrides: Partial<EditorItem> = {}): EditorItem => ({
    key: overrides.key ?? 'row-1',
    repository: 'lakehouse-repo',
    refType: 'branch',
    refId: 'main',
    type: 'prefix',
    path: 'data/customers',
    target: 'customers',
    ...overrides,
});

const key = (overrides: Partial<MetadataKey> = {}): MetadataKey => ({
    name: 'owner',
    type: 'string',
    required: false,
    allow_multiple: false,
    created_at: 0,
    created_by: 'tester',
    ...overrides,
});

describe('normaliseTarget', () => {
    it('drops leading, trailing and doubled slashes', () => {
        expect(normaliseTarget('/customers/')).toBe('customers');
        expect(normaliseTarget('//a//b//')).toBe('a/b');
        expect(normaliseTarget('/')).toBe('');
        expect(normaliseTarget('customers')).toBe('customers');
    });
});

describe('normaliseSourcePath', () => {
    it('keeps exactly one trailing slash on a prefix path', () => {
        expect(normaliseSourcePath('prefix', '/data/customers')).toBe('data/customers/');
        expect(normaliseSourcePath('prefix', 'data/customers/')).toBe('data/customers/');
    });

    it('keeps an empty prefix path as the whole repository', () => {
        expect(normaliseSourcePath('prefix', '')).toBe('');
        expect(normaliseSourcePath('prefix', '/')).toBe('');
    });

    it('strips the leading slash of an object path', () => {
        expect(normaliseSourcePath('object', '/geo/countries.csv')).toBe('geo/countries.csv');
        expect(normaliseSourcePath('object', 'geo/countries.csv')).toBe('geo/countries.csv');
    });
});

describe('targetProblem', () => {
    it('accepts normalisable targets', () => {
        expect(targetProblem('/customers/')).toBeNull();
        expect(targetProblem('vehicles/cars')).toBeNull();
    });

    it('rejects dot segments and the reserved directory', () => {
        expect(targetProblem('a/../b')).toMatch(/must not contain/);
        expect(targetProblem('_dataset/definition.json')).toMatch(/reserved/);
    });
});

describe('localItemProblems', () => {
    it('reports the required members of a row', () => {
        const problems = localItemProblems([
            item({ key: 'row-1', repository: '', refId: '', type: 'object', path: '', target: '' }),
        ]);
        const messages = problems['row-1'] ?? [];
        expect(messages).toContain('Repository is required');
        expect(messages).toContain('Ref is required');
        expect(messages).toContain('Target is required for object items');
        expect(messages).toContain('Source path is required for object items');
    });

    it('accepts a complete prefix row', () => {
        expect(hasProblems(localItemProblems([item()]))).toBe(false);
    });
});

describe('overlapProblems', () => {
    it('reports duplicate targets', () => {
        const problems = overlapProblems([
            item({ key: 'a', target: 'customers' }),
            item({ key: 'b', target: '/customers/' }),
        ]);
        expect(problems).toEqual({ b: ['Target duplicates "customers"'] });
    });

    it('reports duplicate root items with a readable message', () => {
        const problems = overlapProblems([item({ key: 'a', target: '' }), item({ key: 'b', target: '/' })]);
        expect(problems).toEqual({ b: ['Target duplicates the repository root item'] });
    });

    it('reports a target that contains another on a "/" boundary', () => {
        const problems = overlapProblems([
            item({ key: 'a', target: 'customers' }),
            item({ key: 'b', target: 'customers/orders' }),
        ]);
        expect(problems).toEqual({ b: ['Target overlaps "customers"'] });
    });

    it('reports an item at the repository root as overlapping everything', () => {
        const problems = overlapProblems([item({ key: 'root', target: '' }), item({ key: 'b', target: 'customers' })]);
        expect(problems).toEqual({ b: ['Target overlaps the repository root item'] });
    });

    it('does not treat a shared string prefix as an overlap', () => {
        expect(overlapProblems([item({ key: 'a', target: 'a' }), item({ key: 'b', target: 'a-b' })])).toEqual({});
        expect(overlapProblems([item({ key: 'a', target: 'a/b' }), item({ key: 'b', target: 'a/c' })])).toEqual({});
    });
});

describe('buildDefinition', () => {
    it('normalises and stores items sorted by target', () => {
        const definition = buildDefinition(
            [
                item({ key: 'b', repository: ' ref-data ', refType: 'commit', refId: ' a1b2 ', target: '/z/' }),
                item({ key: 'a', type: 'object', path: '/geo/countries.csv', target: '/a/' }),
            ],
            '  Curated  ',
            { tier: 'gold' },
        );

        expect(definition.format_version).toBe(1);
        expect(definition.description).toBe('Curated');
        expect(definition.metadata).toEqual({ tier: 'gold' });
        expect(definition.data.map((entry) => entry.target)).toEqual(['a', 'z']);
        expect(definition.data[0]).toMatchObject({
            target: 'a',
            repository: 'lakehouse-repo',
            ref: { type: 'branch', id: 'main' },
            type: 'object',
            object: { path: 'geo/countries.csv' },
        });
        expect(definition.data[1]).toMatchObject({
            target: 'z',
            repository: 'ref-data',
            ref: { type: 'commit', id: 'a1b2' },
            type: 'prefix',
            prefix: { path: 'data/customers/' },
        });
    });

    it('omits description and metadata when they are empty', () => {
        const definition = buildDefinition([item()], '   ', {});
        expect(definition.description).toBeUndefined();
        expect(definition.metadata).toBeUndefined();
    });
});

describe('itemsFromDefinition', () => {
    it('restores rows from a stored definition', () => {
        const definition: DatasetDefinition = {
            format_version: 1,
            data: [
                {
                    target: 'customers',
                    repository: 'lakehouse-repo',
                    ref: { type: 'commit', id: 'abc' },
                    type: 'prefix',
                    prefix: { path: 'data/customers/' },
                },
                {
                    target: 'countries',
                    repository: 'ref-data',
                    ref: { type: 'commit', id: 'def' },
                    type: 'object',
                    object: { path: 'geo/countries.csv' },
                },
            ],
        };
        const rows = itemsFromDefinition(definition);
        expect(rows).toHaveLength(2);
        expect(rows[0]).toMatchObject({ type: 'prefix', path: 'data/customers/', pinnedCommit: 'abc' });
        expect(rows[1]).toMatchObject({ type: 'object', path: 'geo/countries.csv', pinnedCommit: 'def' });
    });
});

describe('mapValidationErrors', () => {
    it('maps data[<index>] paths onto rows by target order', () => {
        const items = [item({ key: 'z', target: 'z' }), item({ key: 'a', target: 'a' })];
        const mapped = mapValidationErrors(
            [
                { path: 'data[0].target', message: 'target overlaps data[1]' },
                { path: 'data[1].repository', message: 'repository not found' },
                { path: 'metadata.tier', message: 'required_missing' },
                { message: 'definition is missing' },
            ],
            items,
        );

        // data[0] is "a" because the server validates the target-sorted definition.
        expect(mapped.items).toEqual({
            a: [{ field: 'target', message: 'target overlaps data[1]' }],
            z: [{ field: 'repository', message: 'repository not found' }],
        });
        expect(mapped.general).toEqual(['metadata.tier: required_missing', 'definition is missing']);
    });
});

describe('pinnedCommits', () => {
    it('reads the resolved commit of each row', () => {
        const items = [item({ key: 'a', target: 'a', refType: 'branch', refId: 'main' })];
        const resolved: DatasetDefinition = {
            format_version: 1,
            data: [
                {
                    target: 'a',
                    repository: 'lakehouse-repo',
                    ref: { type: 'commit', id: '30f9eb1c9a3d' },
                    type: 'prefix',
                    prefix: { path: 'data/customers/' },
                },
            ],
        };
        expect(pinnedCommits(resolved, items)).toEqual({ a: '30f9eb1c9a3d' });
    });
});

describe('validateMetadataDraft', () => {
    it('accepts a defined string value', () => {
        expect(validateMetadataDraft([key()], { owner: 'data-platform' })).toEqual({});
    });

    it('enforces required keys', () => {
        const problems = validateMetadataDraft([key({ required: true })], {});
        expect(problems).toEqual({ owner: ['Required'] });
    });

    it('reports a number that is not a number', () => {
        const problems = validateMetadataDraft([key({ name: 'priority', type: 'number' })], { priority: 'high' });
        expect(problems).toEqual({ priority: ['Must be a number'] });
        expect(validateMetadataDraft([key({ name: 'priority', type: 'number' })], { priority: '2' })).toEqual({});
    });

    it('accepts only real RFC 3339 full dates', () => {
        const release = key({ name: 'release', type: 'date' });
        expect(validateMetadataDraft([release], { release: '2024-02-29' })).toEqual({});
        expect(validateMetadataDraft([release], { release: '2023-02-29' })).toEqual({
            release: ['Must be a date (YYYY-MM-DD)'],
        });
        expect(validateMetadataDraft([release], { release: '01/02/2024' })).toEqual({
            release: ['Must be a date (YYYY-MM-DD)'],
        });
    });

    it('accepts a boolean value', () => {
        const isPublic = key({ name: 'public', type: 'boolean' });
        expect(validateMetadataDraft([isPublic], { public: 'true' })).toEqual({});
        expect(validateMetadataDraft([isPublic], { public: 'false' })).toEqual({});
    });

    it('accepts only allowed enum values', () => {
        const tier = key({ name: 'tier', type: 'enum', allowed_values: ['gold', 'silver'] });
        expect(validateMetadataDraft([tier], { tier: 'gold' })).toEqual({});
        expect(validateMetadataDraft([tier], { tier: 'bronze' })).toEqual({
            tier: ['Must be one of: gold, silver'],
        });
    });

    it('requires at least one value on a required allow_multiple key', () => {
        const tags = key({ name: 'tags', required: true, allow_multiple: true });
        expect(validateMetadataDraft([tags], { tags: [] })).toEqual({ tags: ['Required'] });
        expect(validateMetadataDraft([tags], { tags: ['analytics', 'finance'] })).toEqual({});
    });

    it('validates every element of an allow_multiple key', () => {
        const scores = key({ name: 'scores', type: 'number', allow_multiple: true });
        expect(validateMetadataDraft([scores], { scores: ['1', '2'] })).toEqual({});
        expect(validateMetadataDraft([scores], { scores: ['1', 'x'] })).toEqual({ scores: ['Must be a number'] });
    });
});

describe('buildMetadata', () => {
    it('converts draft strings to the typed JSON values', () => {
        const keys = [
            key({ name: 'owner' }),
            key({ name: 'priority', type: 'number' }),
            key({ name: 'public', type: 'boolean' }),
            key({ name: 'scores', type: 'number', allow_multiple: true }),
        ];
        expect(
            buildMetadata(keys, {
                owner: 'data-platform',
                priority: '2',
                public: 'false',
                scores: ['1', '3'],
            }),
        ).toEqual({ owner: 'data-platform', priority: 2, public: false, scores: [1, 3] });
    });

    it('drops untouched keys', () => {
        expect(buildMetadata([key()], {})).toEqual({});
    });
});

describe('metadataDraftFromValues', () => {
    it('round-trips metadata into form values', () => {
        const keys = [key(), key({ name: 'scores', type: 'number', allow_multiple: true })];
        expect(metadataDraftFromValues(keys, { owner: 'data-platform', scores: [1, 3] })).toEqual({
            owner: 'data-platform',
            scores: ['1', '3'],
        });
    });
});
