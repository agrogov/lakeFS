import React, { FC } from 'react';

import { DatasetPlaceholder } from '../../lib/components/datasets/placeholder';

/**
 * The object browser of one dataset (§11.2).
 *
 * A dataset is a read-only repository with the same name, so this page reuses the repository
 * object-browser components with `ref` set to the selected version.
 */
const DatasetDataPage: FC = () => (
    <DatasetPlaceholder title="Dataset data" note="The read-only object browser of this dataset will be shown here." />
);

export default DatasetDataPage;
