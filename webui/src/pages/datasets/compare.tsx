import React, { FC } from 'react';

import { DatasetPlaceholder } from '../../lib/components/datasets/placeholder';

/** Definition and data diff between two versions of a dataset (§11.2, §10.3). */
const DatasetComparePage: FC = () => (
    <DatasetPlaceholder
        title="Compare versions"
        note="The definition and data diff between two versions will be shown here."
    />
);

export default DatasetComparePage;
