import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { auth, NoAclError, NotFoundError } from './index';

const jsonResponse = (status, body) => ({
    status,
    ok: status < 400,
    headers: {
        get: (name) => (name.toLowerCase() === 'content-type' ? 'application/json' : null),
    },
    json: async () => body,
    text: async () => JSON.stringify(body),
});

describe('auth.getACL', () => {
    let fetchMock;

    beforeEach(() => {
        fetchMock = vi.fn();
        vi.stubGlobal('fetch', fetchMock);
    });

    afterEach(() => {
        vi.unstubAllGlobals();
    });

    it('maps a 404 carrying no_acl to a NoAclError', async () => {
        fetchMock.mockResolvedValue(
            jsonResponse(404, { message: 'Multiple policies attached to group - no ACL', no_acl: true }),
        );

        const error = await auth.getACL('devs').catch((e) => e);

        expect(fetchMock).toHaveBeenCalledWith('/api/v1/auth/groups/devs/acl', expect.anything());
        expect(error).toBeInstanceOf(NoAclError);
        expect(error).toBeInstanceOf(NotFoundError);
        expect(error.noAcl).toBe(true);
        expect(error.message).toBe('Multiple policies attached to group - no ACL');
    });

    it('keeps a 404 without no_acl as a plain NotFoundError', async () => {
        fetchMock.mockResolvedValue(jsonResponse(404, { message: 'group not found' }));

        const error = await auth.getACL('devs').catch((e) => e);

        expect(error).toBeInstanceOf(NotFoundError);
        expect(error).not.toBeInstanceOf(NoAclError);
        expect(error.message).toBe('group not found');
    });

    it('returns the ACL of a group that has a single policy', async () => {
        fetchMock.mockResolvedValue(jsonResponse(200, { permission: 'Read', repositories: null }));

        await expect(auth.getACL('devs')).resolves.toEqual({ permission: 'Read', repositories: [] });
    });
});
