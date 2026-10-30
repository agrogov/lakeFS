import React, { FC } from 'react';

import { DatasetPlaceholder } from '../../../lib/components/datasets/placeholder';

/** Pull requests (proposals) of one dataset (§11.2, §5.6). */
const DatasetProposalsPage: FC = () => (
    <DatasetPlaceholder title="Dataset pull requests" note="The proposals of this dataset will be listed here." />
);

export default DatasetProposalsPage;
