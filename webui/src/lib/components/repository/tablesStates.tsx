import React, { FC } from 'react';
import Card from 'react-bootstrap/Card';
import { ShieldIcon, TableIcon } from '@primer/octicons-react';

/**
 * The two states of the repository Tables tab that are not a table and not an error: the caller
 * may not read the catalog, and the ref has no tables.
 *
 * They live in one small module so the explorer and the table view render the same thing - a
 * `403` from the catalog on the namespace listing and on a table load means the same thing, and
 * the empty state is shown where the listing turned out to be empty.
 */

/**
 * A catalog read the caller is not allowed to make.
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
