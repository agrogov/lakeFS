import React, { FC } from 'react';

import { DatasetPlaceholder } from '../../../lib/components/datasets/placeholder';

/** One proposal: description, definition diff, data diff and the merge / close actions (§11.2). */
const DatasetProposalPage: FC = () => (
    <DatasetPlaceholder
        title="Pull request"
        note="The proposal diff and its merge and close actions will be shown here."
    />
);

export default DatasetProposalPage;
