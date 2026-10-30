import React, { FC } from 'react';

import { DatasetPlaceholder } from '../../lib/components/datasets/placeholder';

/** Grants of a dataset (§11.2); only reachable with `datasets:ManageAccess`. */
const DatasetAccessPage: FC = () => (
    <DatasetPlaceholder
        title="Dataset access"
        note="The users and groups with read access to this dataset will be shown here."
    />
);

export default DatasetAccessPage;
