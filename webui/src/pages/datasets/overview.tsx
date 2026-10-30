import React, { FC } from 'react';

import { DatasetPlaceholder } from '../../lib/components/datasets/placeholder';

/**
 * Overview of one dataset (§11.2): definition, metadata and stats, plus the `lakefs://` and
 * `s3://` URIs to copy. Filled in by the dataset detail cards.
 */
const DatasetOverviewPage: FC = () => (
    <DatasetPlaceholder
        title="Dataset overview"
        note="The definition, metadata and statistics of this dataset will be shown here."
    />
);

export default DatasetOverviewPage;
