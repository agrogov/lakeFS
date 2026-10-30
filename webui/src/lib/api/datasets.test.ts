import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { AuthenticationError } from './index';
import {
    clearDatasetNamesCache,
    DatasetsError,
    datasets,
    isDatasetsDisabledError,
    isRebaseRequiredError,
    listDatasetNames,
} from './datasets';

const jsonResponse = (status, body) => ({
    status,
    ok: status < 400,
    statusText: '',
    headers: {
        get: (name) => (name.toLowerCase() === 'content-type' ? 'application/json' : null),
    },
    json: async () => body,
    text: async () => JSON.stringify(body),
});

const textResponse = (status) => ({
    status,
    ok: status < 400,
    statusText: 'Bad Gateway',
    headers: { get: () => 'text/html' },
    json: async () => {
        throw new Error('not json');
    },
    text: async () => '<html>bad gateway</html>',
});

const queryOf = (url: string): URLSearchParams => new URLSearchParams(url.split('?')[1] ?? '');

describe('datasets client error mapping', () => {
    let fetchMock: ReturnType<typeof vi.fn>;

    beforeEach(() => {
        fetchMock = vi.fn();
        vi.stubGlobal('fetch', fetchMock);
    });

    afterEach(() => {
        vi.unstubAllGlobals();
        clearDatasetNamesCache();
    });

    it('keeps the server message and every validation problem of a 400', async () => {
        fetchMock.mockResolvedValue(
            jsonResponse(400, {
                message: 'definition is invalid',
                errors: [
                    { path: 'data[2].target', message: 'target overlaps data[0]' },
                    { message: 'metadata key "owner" is required' },
                ],
            }),
        );

        const error = await datasets
            .publishVersion('customer-360', {
                definition: { format_version: 1, data: [] },
                message: 'v2',
            })
            .catch((e) => e);

        expect(error).toBeInstanceOf(DatasetsError);
        expect(error.message).toBe('definition is invalid');
        expect(error.status).toBe(400);
        expect(error.errors).toEqual([
            { path: 'data[2].target', message: 'target overlaps data[0]' },
            { message: 'metadata key "owner" is required' },
        ]);
    });

    it('maps a 409 conflict to a DatasetsError without details', async () => {
        fetchMock.mockResolvedValue(jsonResponse(409, { message: 'publish already in progress' }));

        const error = await datasets.listVersions('customer-360').catch((e) => e);

        expect(error).toBeInstanceOf(DatasetsError);
        expect(error.status).toBe(409);
        expect(error.errors).toEqual([]);
        expect(error.latestVersion).toBeUndefined();
    });

    it('carries the latest_version of a stale-base 409 and recognises it as a rebase', async () => {
        fetchMock.mockResolvedValue(
            jsonResponse(409, {
                message: 'conflict: rebase required: proposal p1 of dataset d is based on version 1, latest is 4',
                latest_version: 4,
            }),
        );

        const error = await datasets.mergeProposal('d', 'p1').catch((e) => e);

        expect(error).toBeInstanceOf(DatasetsError);
        expect(error.status).toBe(409);
        expect(error.latestVersion).toBe(4);
        expect(isRebaseRequiredError(error)).toBe(true);
    });

    it('does not treat a 409 without a rebase indication as a rebase', async () => {
        fetchMock.mockResolvedValue(jsonResponse(409, { message: 'proposal is not open' }));

        const error = await datasets.mergeProposal('d', 'p1').catch((e) => e);

        expect(error.status).toBe(409);
        expect(isRebaseRequiredError(error)).toBe(false);
        expect(isRebaseRequiredError(new Error('rebase required'))).toBe(false);
    });

    it('falls back to the status line when the body is not JSON', async () => {
        fetchMock.mockResolvedValue(textResponse(502));

        const error = await datasets.get('customer-360').catch((e) => e);

        expect(error).toBeInstanceOf(DatasetsError);
        expect(error.status).toBe(502);
        expect(error.message).toBe('Bad Gateway');
    });

    it('throws AuthenticationError on 401 so the shared auth redirect still runs', async () => {
        fetchMock.mockResolvedValue(jsonResponse(401, { message: 'user not found' }));

        const error = await datasets.getConfig().catch((e) => e);

        expect(error).toBeInstanceOf(AuthenticationError);
        expect(error.status).toBe(401);
        expect(error.message).toBe('user not found');
    });

    it('resolves to undefined for a 204 response', async () => {
        fetchMock.mockResolvedValue(jsonResponse(204, null));

        await expect(datasets.delete('customer-360')).resolves.toBeUndefined();
        expect(fetchMock.mock.calls[0][0]).toBe('/api/v1/datasets/customer-360');
        expect(fetchMock.mock.calls[0][1].method).toBe('DELETE');
    });
});

describe('isDatasetsDisabledError', () => {
    it('treats 404 and 501 as "datasets are disabled"', () => {
        expect(isDatasetsDisabledError(new DatasetsError('not implemented', 501))).toBe(true);
        expect(isDatasetsDisabledError(new DatasetsError('not found', 404))).toBe(true);
    });

    it('does not treat other failures as disabled', () => {
        expect(isDatasetsDisabledError(new DatasetsError('conflict', 409))).toBe(false);
        expect(isDatasetsDisabledError(new Error('boom'))).toBe(false);
        expect(isDatasetsDisabledError(undefined)).toBe(false);
    });
});

describe('datasets client requests', () => {
    let fetchMock: ReturnType<typeof vi.fn>;

    beforeEach(() => {
        fetchMock = vi.fn(async () => jsonResponse(200, { results: [], pagination: { has_more: false } }));
        vi.stubGlobal('fetch', fetchMock);
    });

    afterEach(() => {
        vi.unstubAllGlobals();
        clearDatasetNamesCache();
    });

    it('lists through the collection root and sends metadata filters as metadata.<key>', async () => {
        await datasets.list({ search: 'customer', metadata: { tier: 'gold', owner: '' }, amount: 25 });

        const [url] = fetchMock.mock.calls[0];
        expect(url.startsWith('/api/v1/datasets?')).toBe(true);
        const query = queryOf(url);
        expect(query.get('search')).toBe('customer');
        expect(query.get('amount')).toBe('25');
        expect(query.get('metadata.tier')).toBe('gold');
        // An empty metadata value is still a filter ("owner is empty"), not an omission.
        expect(query.get('metadata.owner')).toBe('');
        // Unset parameters are dropped instead of being sent empty.
        expect(query.has('after')).toBe(false);
        expect(query.has('prefix')).toBe(false);
    });

    it('encodes dataset names and version refs in the path', async () => {
        await datasets.getVersion('customer 360', 'v1');

        expect(fetchMock.mock.calls[0][0]).toBe('/api/v1/datasets/customer%20360/versions/v1');
    });

    it('pages through the dataset names', async () => {
        fetchMock
            .mockResolvedValueOnce(
                jsonResponse(200, {
                    results: [{ name: 'customer-360' }],
                    pagination: { has_more: true, next_offset: 'customer-360', results: 1, max_per_page: 1000 },
                }),
            )
            .mockResolvedValueOnce(
                jsonResponse(200, {
                    results: [{ name: 'sales' }],
                    pagination: { has_more: false, next_offset: '', results: 1, max_per_page: 1000 },
                }),
            );

        const names = await listDatasetNames();

        expect(names).toEqual(new Set(['customer-360', 'sales']));
        expect(fetchMock).toHaveBeenCalledTimes(2);
        expect(queryOf(fetchMock.mock.calls[1][0]).get('after')).toBe('customer-360');
    });
});
