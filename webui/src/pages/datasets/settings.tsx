import React, { FC } from 'react';

import { DatasetPlaceholder } from '../../lib/components/datasets/placeholder';

/** Metadata key CRUD for the whole installation (§11.2); needs `datasets:ManageMetadataKeys`. */
const DatasetsSettingsPage: FC = () => (
    <DatasetPlaceholder
        title="Dataset settings"
        note="The metadata keys that type dataset metadata will be managed here."
    />
);

export default DatasetsSettingsPage;
