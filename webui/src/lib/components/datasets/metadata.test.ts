import { describe, expect, it } from 'vitest';

import { isDatasetRepoMetadata, isDatasetName, partitionDatasetRepos } from './metadata';
import { globMatch, policiesAllow } from './hooks';

describe('isDatasetRepoMetadata', () => {
    it('recognises the flag the datasets service stamps on the repository', () => {
        expect(isDatasetRepoMetadata({ 'lakefs.dataset': 'true' })).toBe(true);
        expect(isDatasetRepoMetadata({ 'lakefs.dataset': 'true', 'lakefs.dataset.format': '1' })).toBe(true);
    });

    it('ignores ordinary repositories', () => {
        expect(isDatasetRepoMetadata({ 'lakefs.dataset': 'false' })).toBe(false);
        expect(isDatasetRepoMetadata({ owner: 'data-platform' })).toBe(false);
        expect(isDatasetRepoMetadata({})).toBe(false);
        expect(isDatasetRepoMetadata(null)).toBe(false);
        expect(isDatasetRepoMetadata(undefined)).toBe(false);
    });
});

describe('partitionDatasetRepos', () => {
    const repos = [{ id: 'lakehouse-repo' }, { id: 'customer-360' }, { id: 'sales' }];

    it('moves dataset repositories out of the repository list', () => {
        const { repositories: visible, datasets } = partitionDatasetRepos(repos, new Set(['customer-360', 'sales']));

        expect(visible.map((repo) => repo.id)).toEqual(['lakehouse-repo']);
        expect(datasets.map((repo) => repo.id)).toEqual(['customer-360', 'sales']);
    });

    it('filters nothing out when the dataset names are unknown', () => {
        const { repositories: visible, datasets } = partitionDatasetRepos(repos, undefined);

        expect(visible).toEqual(repos);
        expect(datasets).toEqual([]);
    });
});

describe('isDatasetName', () => {
    it('answers true only for a known dataset name', () => {
        const names = new Set(['customer-360']);
        expect(isDatasetName('customer-360', names)).toBe(true);
        expect(isDatasetName('lakehouse-repo', names)).toBe(false);
        expect(isDatasetName('customer-360', undefined)).toBe(false);
    });
});

describe('globMatch', () => {
    it('matches actions and ARNs with wildcards', () => {
        expect(globMatch('datasets:CreateDataset', 'datasets:CreateDataset')).toBe(true);
        expect(globMatch('datasets:*', 'datasets:CreateDataset')).toBe(true);
        expect(globMatch('*', 'arn:lakefs:datasets:::dataset/x')).toBe(true);
        expect(globMatch('arn:lakefs:datasets:::dataset/*', 'arn:lakefs:datasets:::dataset/customer-360')).toBe(true);
        expect(globMatch('datasets:ReadDataset', 'datasets:CreateDataset')).toBe(false);
        expect(globMatch('arn:lakefs:fs:::repository/*', 'arn:lakefs:datasets:::dataset/x')).toBe(false);
    });
});

describe('policiesAllow', () => {
    const CREATE = 'datasets:CreateDataset';
    const RESOURCE = 'arn:lakefs:datasets:::dataset/sample';

    it('allows through a wildcard statement', () => {
        expect(
            policiesAllow(
                [{ statement: [{ effect: 'allow', action: ['datasets:*'], resource: ['*'] }] }],
                CREATE,
                RESOURCE,
            ),
        ).toBe(true);
    });

    it('allows through a statement naming the action', () => {
        expect(
            policiesAllow(
                [{ statement: [{ effect: 'allow', action: CREATE, resource: 'arn:lakefs:datasets:::dataset/*' }] }],
                CREATE,
                RESOURCE,
            ),
        ).toBe(true);
    });

    it('denies when nothing matches', () => {
        expect(policiesAllow([], CREATE, RESOURCE)).toBe(false);
        expect(
            policiesAllow(
                [{ statement: [{ effect: 'allow', action: ['datasets:ReadDataset'], resource: ['*'] }] }],
                CREATE,
                RESOURCE,
            ),
        ).toBe(false);
        expect(
            policiesAllow(
                [
                    {
                        statement: [
                            { effect: 'allow', action: ['datasets:*'], resource: ['arn:lakefs:fs:::repository/x'] },
                        ],
                    },
                ],
                CREATE,
                RESOURCE,
            ),
        ).toBe(false);
    });

    it('gives deny precedence over allow', () => {
        expect(
            policiesAllow(
                [
                    { statement: [{ effect: 'allow', action: ['datasets:*'], resource: ['*'] }] },
                    { statement: [{ effect: 'deny', action: [CREATE], resource: [RESOURCE] }] },
                ],
                CREATE,
                RESOURCE,
            ),
        ).toBe(false);
    });
});
