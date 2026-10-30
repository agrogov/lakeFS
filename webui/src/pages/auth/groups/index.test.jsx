import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';

import GroupsPage from './index';

const { loginConfig, setActiveTab, onUnauthenticated } = vi.hoisted(() => ({
    loginConfig: { current: { RBAC: 'simplified' } },
    setActiveTab: vi.fn(),
    onUnauthenticated: vi.fn(),
}));

vi.mock('../../../lib/hooks/conf', () => ({
    useLoginConfigContext: () => loginConfig.current,
}));

vi.mock('../../../lib/components/auth/layout', () => ({
    useAuthOutletContext: () => [setActiveTab],
}));

vi.mock('../../../lib/auth/authContext', () => ({
    useAuth: () => ({ onUnauthenticated }),
}));

const jsonResponse = (status, body) => ({
    status,
    ok: status < 400,
    headers: {
        get: (name) => (name.toLowerCase() === 'content-type' ? 'application/json' : null),
    },
    json: async () => body,
    text: async () => JSON.stringify(body),
});

const group = (id) => ({ id, name: id, creation_date: '2024-01-01T00:00:00Z' });

const multiplePoliciesGroup = group('multiple-policies-group');
const singlePolicyGroup = group('single-policy-group');
const unknownAclGroup = group('unknown-acl-group');

const aclResponses = {
    [multiplePoliciesGroup.id]: {
        status: 404,
        body: { message: 'Multiple policies attached to group - no ACL', no_acl: true },
    },
    [singlePolicyGroup.id]: {
        status: 200,
        body: { permission: 'Read', repositories: [] },
    },
    [unknownAclGroup.id]: {
        status: 404,
        body: { message: 'group not found' },
    },
};

describe('GroupsPage permission column in simplified mode', () => {
    let fetchMock;
    let groups;

    const aclRequests = () => fetchMock.mock.calls.map(([url]) => String(url)).filter((url) => url.endsWith('/acl'));

    const renderPage = () =>
        render(
            <MemoryRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
                <GroupsPage />
            </MemoryRouter>,
        );

    beforeEach(() => {
        loginConfig.current = { RBAC: 'simplified' };
        groups = [multiplePoliciesGroup];
        fetchMock = vi.fn(async (url) => {
            const aclMatch = /\/auth\/groups\/([^/]+)\/acl$/.exec(String(url));
            if (aclMatch) {
                const response = aclResponses[aclMatch[1]];
                return jsonResponse(response.status, response.body);
            }
            return jsonResponse(200, { results: groups, pagination: { has_more: false } });
        });
        vi.stubGlobal('fetch', fetchMock);
    });

    afterEach(() => {
        vi.unstubAllGlobals();
    });

    it('renders the custom (policies) badge for a group with multiple policies', async () => {
        renderPage();

        expect(await screen.findByText('Custom (policies)')).toBeInTheDocument();
        expect(aclRequests()).toEqual(['/api/v1/auth/groups/multiple-policies-group/acl']);
    });

    it('renders the ACL permission selector, and no badge, for a group with a single policy', async () => {
        groups = [singlePolicyGroup];
        renderPage();

        await screen.findByText(singlePolicyGroup.name);
        expect(await screen.findByText('Read')).toBeInTheDocument();
        expect(screen.queryByText('Custom (policies)')).not.toBeInTheDocument();
    });

    it('surfaces a 404 without no_acl as an error instead of a badge', async () => {
        groups = [unknownAclGroup];
        renderPage();

        expect(await screen.findByText('group not found')).toBeInTheDocument();
        expect(screen.queryByText('Custom (policies)')).not.toBeInTheDocument();
    });

    it('does not show the badge outside simplified mode', async () => {
        loginConfig.current = { RBAC: 'acl' };
        groups = [multiplePoliciesGroup, singlePolicyGroup];
        renderPage();

        await screen.findByText(multiplePoliciesGroup.name);
        await screen.findByText(singlePolicyGroup.name);

        expect(screen.queryByText('Custom (policies)')).not.toBeInTheDocument();
        expect(screen.queryByText('Permission')).not.toBeInTheDocument();
        expect(aclRequests()).toEqual([]);
    });
});
