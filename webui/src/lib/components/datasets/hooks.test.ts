import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { renderHook, waitFor } from '@testing-library/react';

import { globMatch, policiesAllow, useCan, useDatasetVersion } from './hooks';

// `lib/api` caches the current user in `localStorage`, which this happy-dom setup does not
// provide. Give it a minimal one before the imports above are evaluated.
vi.hoisted(() => {
    if (typeof window === 'undefined' || window.localStorage) return;
    const store = new Map<string, string>();
    Object.defineProperty(window, 'localStorage', {
        configurable: true,
        value: {
            getItem: (key: string) => store.get(key) ?? null,
            setItem: (key: string, value: string) => void store.set(key, value),
            removeItem: (key: string) => void store.delete(key),
            clear: () => store.clear(),
        },
    });
});

// useAPI() reports authentication failures through the auth context.
vi.mock('../../auth/authContext', () => ({
    useAuth: () => ({ onUnauthenticated: () => {} }),
}));

const jsonResponse = (body: unknown, status = 200) => ({
    status,
    ok: status < 400,
    statusText: '',
    headers: {
        get: (name: string) => (name.toLowerCase() === 'content-type' ? 'application/json' : null),
    },
    json: async () => body,
    text: async () => JSON.stringify(body),
});

const allow = (action: string | string[], resource: string | string[]) => ({
    effect: 'allow',
    action,
    resource,
});

describe('globMatch', () => {
    it('matches a whole pattern only, with * spanning any run of characters', () => {
        expect(globMatch('dataset:ReadDataset', 'dataset:ReadDataset')).toBe(true);
        expect(globMatch('dataset:*', 'dataset:ReadDataset')).toBe(true);
        expect(globMatch('*', 'anything at all')).toBe(true);
        expect(globMatch('dataset:ReadDataset', 'dataset:Read')).toBe(false);
        expect(globMatch('arn:lakefs:dataset:::dataset/*', 'arn:lakefs:dataset:::dataset/foo')).toBe(true);
        // `.` and `+` are literal, not regular-expression operators.
        expect(globMatch('a+b', 'aab')).toBe(false);
    });

    it('is case-sensitive, like the server (UI-5)', () => {
        expect(globMatch('dataset:readdataset', 'dataset:ReadDataset')).toBe(false);
        expect(globMatch('arn:lakefs:dataset:::dataset/foo', 'arn:lakefs:Dataset:::dataset/foo')).toBe(false);
    });
});

describe('policiesAllow', () => {
    const resource = 'arn:lakefs:dataset:::dataset/customer-360';

    it('lets deny win over allow', () => {
        const policies = [
            { statement: [allow('dataset:*', '*')] },
            { statement: [{ effect: 'deny', action: 'dataset:ManageAccess', resource: '*' }] },
        ];

        expect(policiesAllow(policies, 'dataset:ReadDataset', resource)).toBe(true);
        expect(policiesAllow(policies, 'dataset:ManageAccess', resource)).toBe(false);
    });

    it('does not count a conditioned allow, which only the server can evaluate (UI-5)', () => {
        const policies = [
            {
                statement: [
                    {
                        ...allow('dataset:CreateDataset', '*'),
                        condition: { StringEquals: { 'aws:SourceIp': ['10.0.0.1'] } },
                    },
                ],
            },
        ];

        expect(policiesAllow(policies, 'dataset:CreateDataset', resource)).toBe(false);
    });

    it('does not count a conditioned deny either, so the unconditional allow stands (UI-5)', () => {
        const policies = [
            { statement: [allow('dataset:CreateDataset', '*')] },
            {
                statement: [
                    {
                        effect: 'deny',
                        action: 'dataset:CreateDataset',
                        resource: '*',
                        condition: { StringEquals: { 'aws:SourceIp': ['10.0.0.1'] } },
                    },
                ],
            },
        ];

        expect(policiesAllow(policies, 'dataset:CreateDataset', resource)).toBe(true);
    });
});

describe('useCan', () => {
    let fetchMock: ReturnType<typeof vi.fn>;

    beforeEach(() => {
        fetchMock = vi.fn();
        vi.stubGlobal('fetch', fetchMock);
    });

    afterEach(() => {
        vi.unstubAllGlobals();
    });

    it('reads every page of the effective policies (UI-5)', async () => {
        fetchMock.mockImplementation(async (url: unknown) => {
            const path = String(url);
            if (path.endsWith('/api/v1/user')) return jsonResponse({ user: { id: 'reviewer' } });
            if (path.includes('/auth/users/reviewer/policies')) {
                const after = new URLSearchParams(path.split('?')[1] ?? '').get('after');
                if (after === 'page-2') {
                    return jsonResponse({
                        results: [{ statement: [allow('dataset:CreateDataset', '*')] }],
                        pagination: { has_more: false, next_offset: '', results: 1, max_per_page: 1000 },
                    });
                }
                return jsonResponse({
                    results: [{ statement: [allow('fs:ReadObject', '*')] }],
                    pagination: { has_more: true, next_offset: 'page-2', results: 1, max_per_page: 1000 },
                });
            }
            return jsonResponse({ message: 'not found' }, 404);
        });

        const { result } = renderHook(() => useCan('dataset:CreateDataset', 'arn:lakefs:dataset:::dataset/sample'));

        await waitFor(() => expect(result.current).toBe(true));
        const policyCalls = fetchMock.mock.calls.filter(([url]) => String(url).includes('/policies'));
        expect(policyCalls).toHaveLength(2);
        const secondQuery = new URLSearchParams(String(policyCalls[1][0]).split('?')[1] ?? '');
        expect(secondQuery.get('after')).toBe('page-2');
        expect(secondQuery.get('effective')).toBe('true');
        expect(secondQuery.get('amount')).toBe('1000');
    });
});

describe('useDatasetVersion', () => {
    let fetchMock: ReturnType<typeof vi.fn>;

    const latestItem = {
        target: 'latest-target',
        repository: 'lakehouse',
        ref: { type: 'commit', id: 'a' },
        type: 'prefix',
    };
    const oldItem = { target: 'old-target', repository: 'lakehouse', ref: { type: 'commit', id: 'b' }, type: 'prefix' };

    beforeEach(() => {
        fetchMock = vi.fn(async (url: unknown) => {
            const path = String(url);
            if (path.endsWith('/api/v1/datasets/ds')) {
                return jsonResponse({
                    id: 'ds',
                    creation_date: 1_700_000_000,
                    version: 5,
                    definition: { format_version: 1, data: [latestItem] },
                });
            }
            if (path.includes('/api/v1/datasets/ds/versions?')) {
                return jsonResponse({
                    results: [
                        { id: 'v5', author: 'alice', creation_date: 1_700_000_500 },
                        { id: 'v4', author: 'alice', creation_date: 1_700_000_400 },
                    ],
                    pagination: { has_more: false, next_offset: '', results: 2, max_per_page: 1000 },
                });
            }
            if (path.endsWith('/api/v1/datasets/ds/versions/v2')) {
                return jsonResponse({
                    id: 'v2',
                    author: 'bob',
                    creation_date: 1_700_000_200,
                    definition: { format_version: 1, data: [oldItem] },
                });
            }
            return jsonResponse({ message: 'not found' }, 404);
        });
        vi.stubGlobal('fetch', fetchMock);
    });

    afterEach(() => {
        vi.unstubAllGlobals();
    });

    it('fetches an explicit vN that the capped version list does not carry (UI-6)', async () => {
        const { result } = renderHook(() => useDatasetVersion('ds', 'v2'));

        await waitFor(() => expect(result.current.version?.id).toBe('v2'));
        expect(result.current.versionNumber).toBe(2);
        expect(result.current.definition?.data?.[0]?.target).toBe('old-target');
        expect(fetchMock.mock.calls.some(([url]) => String(url).endsWith('/datasets/ds/versions/v2'))).toBe(true);
    });

    it('never substitutes the latest definition for an explicit vN (UI-6)', async () => {
        // The version cannot be read: an explicit `v2` must fail rather than show `latest`'s data.
        fetchMock.mockImplementation(async (url: unknown) => {
            const path = String(url);
            if (path.endsWith('/api/v1/datasets/ds')) {
                return jsonResponse({
                    id: 'ds',
                    creation_date: 1_700_000_000,
                    version: 5,
                    definition: { format_version: 1, data: [latestItem] },
                });
            }
            if (path.includes('/api/v1/datasets/ds/versions?')) {
                return jsonResponse({
                    results: [{ id: 'v5', author: 'alice', creation_date: 1_700_000_500 }],
                    pagination: { has_more: false, next_offset: '', results: 1, max_per_page: 1000 },
                });
            }
            return jsonResponse({ message: 'version not found' }, 404);
        });

        const { result } = renderHook(() => useDatasetVersion('ds', 'v2'));

        await waitFor(() => expect(result.current.loading).toBe(false));
        expect(result.current.version).toBeNull();
        expect(result.current.definition).toBeUndefined();
        expect(result.current.versionNumber).toBe(2);
        expect(result.current.error).not.toBeNull();
    });
});
