import React, { FC } from 'react';

import { DatasetPlaceholder } from '../../lib/components/datasets/placeholder';

/** Timeline of the immutable versions `v1..vN` of one dataset (§11.2). */
const DatasetVersionsPage: FC = () => (
    <DatasetPlaceholder title="Dataset versions" note="The version timeline of this dataset will be shown here." />
);

export default DatasetVersionsPage;
