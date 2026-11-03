import React, { FC } from 'react';
import Card from 'react-bootstrap/Card';
import { SignInIcon, ShieldIcon, TableIcon } from '@primer/octicons-react';

import { AlertError } from '../controls';
import { isIcebergForbiddenError, isIcebergUnauthenticatedError } from '../../api/iceberg';
import { ROUTES } from '../../utils';

/**
 * The states of the repository Tables tab that are not a table and not an error: the catalog does
 * not accept the caller's session, the caller may not read the catalog, and the ref has no tables.
 *
 * They live in one small module so the explorer and the table view render the same thing - a
 * `401` and a `403` from the catalog mean the same thing wherever they are answered, and the empty
 * state is shown where the listing turned out to be empty.
 */

/**
 * The catalog did not accept the browser's session (`401`).
 *
 * The catalog authenticates a browser by the lakeFS login session cookie
 * (`pkg/icebergcatalog/auth.go`), for reads only. A `401` therefore means the cookie was missing,
 * expired, or not usable for the catalog - not that the rest of the UI is signed out. The tab says
 * exactly that and offers a plain sign-in link, which the reader chooses to follow: nothing here
 * navigates, so a stale catalog session never yanks the reader off the page the way a core `401`
 * does.
 */
export const TablesSessionRequired: FC = () => (
    <Card className="text-center" data-testid="iceberg-session-required">
        <Card.Body className="py-5">
            <div className="mb-3">
                <SignInIcon size={24} />
            </div>
            <h4>Sign in to read Iceberg tables</h4>
            <p className="text-muted mb-2">
                Your session is not valid for the Iceberg catalog on this server: it is missing, or it expired. Sign in
                again to keep reading tables here.
            </p>
            <p className="text-muted mb-0">
                <a href={ROUTES.LOGIN}>Sign in</a>
            </p>
        </Card.Body>
    </Card>
);

/**
 * A catalog read the caller is not allowed to make (`403`).
 *
 * The catalog's own authorization (`catalog:*` policies, `pkg/icebergcatalog/authz.go`) answers
 * `403 ForbiddenException`; the UI only explains it, and names the permissions so the reader knows
 * who to ask. It never softens the answer into an empty listing: "you may not read this" and
 * "there is nothing here" are different facts.
 */
export const TablesPermissionRequired: FC<{ what?: string }> = ({ what = 'Reading Iceberg tables' }) => (
    <Card className="text-center">
        <Card.Body className="py-5">
            <div className="mb-3">
                <ShieldIcon size={24} />
            </div>
            <h4>Permission required</h4>
            <p className="text-muted mb-2">
                {what} requires the <code>catalog:ListNamespaces</code>, <code>catalog:ListTables</code> and{' '}
                <code>catalog:LoadTable</code> permissions on this repository, together with{' '}
                <code>fs:ReadRepository</code>.
            </p>
            <p className="text-muted mb-0">Ask an administrator to grant them on this server.</p>
        </Card.Body>
    </Card>
);

/**
 * The ref has no Iceberg tables.
 *
 * Tables are created by an Iceberg client (Spark, Trino, PyIceberg, DuckDB) pointed at the
 * catalog, so the empty state says that instead of offering a create button this tab does not
 * have.
 */
export const NoTables: FC<{ refLabel: string }> = ({ refLabel }) => (
    <Card className="text-center">
        <Card.Body className="py-5">
            <div className="mb-3">
                <TableIcon size={24} />
            </div>
            <h4>No Iceberg tables</h4>
            <p className="text-muted mb-0">
                <code>{refLabel}</code> has no Iceberg tables yet. Write to it with an Iceberg client pointed at this
                server&apos;s catalog, and the tables appear here.
            </p>
        </Card.Body>
    </Card>
);

/**
 * A failed catalog read, rendered as the state it means: a session the catalog does not accept, a
 * permission the caller does not hold, or - for everything else - the error itself.
 *
 * Every read in this tab goes through here, so a `401` can never be mistaken for a `403` or fall
 * through to the login page, and the two refusals read the same wherever they are answered.
 */
export const TablesError: FC<{ error: unknown; what?: string }> = ({ error, what }) => {
    if (isIcebergUnauthenticatedError(error)) return <TablesSessionRequired />;
    if (isIcebergForbiddenError(error)) return <TablesPermissionRequired what={what} />;
    return <AlertError error={error as Error} />;
};
