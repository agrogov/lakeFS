import React, { FC } from 'react';

import { DatasetPlaceholder } from '../../../lib/components/datasets/placeholder';

/** Pull requests across all datasets, the second top-level datasets tab (§11.1). */
const DatasetProposalsAllPage: FC = () => (
    <DatasetPlaceholder
        title="Pull requests"
        note="Open dataset pull requests across all datasets will be listed here."
    />
);

export default DatasetProposalsAllPage;
