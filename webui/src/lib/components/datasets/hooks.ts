import { MAX_LISTING_AMOUNT, auth } from '../../api';
import {
    Dataset,
    DatasetDefinition,
    DatasetStats,
    DatasetsConfig,
    Version,
    VersionList,
    datasets,
    getDatasetNames,
    isDatasetsDisabledError,
} from '../../api/datasets';
import { useAPI } from '../../hooks/api';
import { VersionSelection, normaliseSelection, selectionToVersion } from './versions';

/**
 * Hooks shared by the dataset pages (DATASETS_DESIGN.md §11).
 *
 * They stay on the client side of the control plane: the server always re-checks every
 * permission, so anything here only decides what the UI offers.
 */

// ---------------------------------------------------------------------------
// Feature configuration
// ---------------------------------------------------------------------------

export interface DatasetsConfigState {
    config: DatasetsConfig | null;
    loading: boolean;
    /** True when the server has the feature switched off (or does not serve it at all, §11). */
    disabled: boolean;
    error: Error | null;
}

/**
 * `GET /datasets/config`, gating the UI (§11). A `404` or `501` counts as disabled: the
 * design wants "Datasets are disabled on this server" instead of a failure.
 */
export const useDatasetsConfig = (): DatasetsConfigState => {
    const { response, loading, error } = useAPI(fetchDatasetsConfig, []);
    const config = (response ?? null) as DatasetsConfig | null;
    if (loading) return { config: null, loading: true, disabled: false, error: null };
    if (error) return { config: null, loading: false, disabled: isDatasetsDisabledError(error), error };
    return { config, loading: false, disabled: config ? config.enabled === false : false, error: null };
};

const fetchDatasetsConfig = async (): Promise<DatasetsConfig> => datasets.getConfig();

// ---------------------------------------------------------------------------
// Dataset repositories (§11.3)
// ---------------------------------------------------------------------------

/**
 * Names of all datasets. The repository views use it to tell dataset repositories apart:
 * `GET /repositories` carries no metadata, so the control plane is the source of truth.
 * Resolves to an empty set when datasets are disabled or unreachable.
 */
export const useDatasetNames = (): { names: Set<string>; loading: boolean } => {
    const { response, loading } = useAPI(fetchDatasetNames, []);
    return { names: response ?? EMPTY_NAMES, loading };
};

const EMPTY_NAMES = new Set<string>();

const fetchDatasetNames = async (): Promise<Set<string>> => getDatasetNames();

// ---------------------------------------------------------------------------
// Permissions
// ---------------------------------------------------------------------------

export const DATASETS_CREATE_ACTION = 'datasets:CreateDataset';
export const DATASETS_PUBLISH_ACTION = 'datasets:PublishVersion';
export const DATASETS_MANAGE_ACCESS_ACTION = 'datasets:ManageAccess';
export const DATASETS_MANAGE_METADATA_KEYS_ACTION = 'datasets:ManageMetadataKeys';

/** The `§9.1` ARN of one dataset: `arn:lakefs:datasets:::dataset/<name>`. */
export const datasetResource = (name: string): string => `arn:lakefs:datasets:::dataset/${name}`;

/** The `§9.1` ARN of one metadata key: `arn:lakefs:datasets:::metadata-key/<key>`. */
export const metadataKeyResource = (key: string): string => `arn:lakefs:datasets:::metadata-key/${key}`;

/** Policy shape returned by `GET /auth/users/{id}/policies?effective=true`. */
interface PolicyStatementLike {
    effect?: string;
    action?: string | string[];
    resource?: string | string[];
}

interface PolicyLike {
    statement?: PolicyStatementLike[];
}

/**
 * A concrete dataset ARN used to resolve wildcard resources such as `*` or
 * `arn:lakefs:datasets:::dataset/*` when the question is "may this user create *a* dataset".
 * Names are validated by the server; the placeholder is only ever matched, never created.
 */
const SAMPLE_DATASET_ARN = 'arn:lakefs:datasets:::dataset/sample';

/** Glob matching for ARNs and actions (`*` matches any run of characters), as the server does. */
export const globMatch = (pattern: string, value: string): boolean => {
    const escaped = pattern.replace(/[.+?^${}()|[\]\\]/g, '\\$&').replace(/\*/g, '.*');
    return new RegExp(`^${escaped}$`, 'i').test(value);
};

const asArray = (value: string | string[] | undefined): string[] => {
    if (value === undefined) return [];
    return Array.isArray(value) ? value : [value];
};

/**
 * Whether a set of effective policies allows `action` on `resource`. Deny wins over allow,
 * and both action and resource are glob-matched, mirroring the server's permission model.
 */
export const policiesAllow = (policies: PolicyLike[], action: string, resource: string): boolean => {
    let allowed = false;
    for (const policy of policies) {
        for (const statement of policy.statement ?? []) {
            if (!asArray(statement.action).some((pattern) => globMatch(pattern, action))) continue;
            if (!asArray(statement.resource).some((pattern) => globMatch(pattern, resource))) continue;
            if (statement.effect === 'deny') return false;
            if (statement.effect === 'allow') allowed = true;
        }
    }
    return allowed;
};

/**
 * Whether the current user may create datasets, deciding if the "Create dataset" button is
 * shown (§11.2).
 *
 * The answer is computed from the user's effective policies. Simplified ACL mode answers
 * `501` for policy listing (the base ACLs grant everything there), so an unavailable answer
 * falls back to "yes": the server enforces the permission anyway, and hiding a button from a
 * user who is allowed is worse than showing one that fails with `403`.
 */
export const useCanCreateDataset = (): boolean => useCan(DATASETS_CREATE_ACTION, SAMPLE_DATASET_ARN);

export interface PermissionState {
    /** The decision; `false` while it is still unknown, so nothing appears before it arrives. */
    allowed: boolean;
    loading: boolean;
    /** Set when the policies could not be read (e.g. `501` in simplified ACL mode). */
    error: Error | null;
}

/**
 * `datasets:<action>` on one resource, from the user's effective policies.
 *
 * This only decides what the UI *offers*; the control plane re-checks every permission and
 * answers `403` regardless. A failure to read the policies counts as "allowed" for the same
 * reason as `useCanCreateDataset`: the server is the authority, and a permitted user must not
 * lose a control because the policy listing is unavailable (simplified ACL mode answers
 * `501` there).
 */
export const usePermission = (action: string, resource: string): PermissionState => {
    const { response, loading, error } = useAPI(() => fetchCan(action, resource), [action, resource]);
    if (loading) return { allowed: false, loading: true, error: null };
    if (error) return { allowed: true, loading: false, error: error as Error };
    return { allowed: response ?? true, loading: false, error: null };
};

/** `usePermission` when only the yes/no answer is needed. */
export const useCan = (action: string, resource: string): boolean => usePermission(action, resource).allowed;

const fetchCan = async (action: string, resource: string): Promise<boolean> => {
    const user = await auth.getCurrentUserWithCache();
    if (!user?.id) return false;
    const policies = await auth.listUserPolicies(user.id, true);
    const results: PolicyLike[] = policies?.results ?? [];
    return policiesAllow(results, action, resource);
};

// ---------------------------------------------------------------------------
// Reads (§11.2 pages)
// ---------------------------------------------------------------------------

/**
 * `GET /datasets/{dataset}`: the record plus the latest definition and stats. Every detail page
 * needs it - the overview for the header, the data page for the version count, the compare page
 * for the default left/right versions.
 */
export const useDataset = (name: string) => {
    const { response, loading, error } = useAPI(() => datasets.get(name), [name]);
    return { dataset: (response ?? null) as Dataset | null, loading, error: (error ?? null) as Error | null };
};

/**
 * Every published version, newest first. The timeline shows them all and the pickers choose
 * among them, and each version carries its definition (§10.1), so one request serves both -
 * including the "items changed" count that `versionChangeCounts` derives locally.
 */
export const useDatasetVersions = (name: string) => {
    const { response, loading, error } = useAPI(
        () => datasets.listVersions(name, { amount: MAX_LISTING_AMOUNT }),
        [name],
    );
    const versions = ((response as VersionList | null)?.results ?? []) as Version[];
    return { versions, loading, error: (error ?? null) as Error | null };
};

/** The version numbers of `useDatasetVersions`, newest first, for the version pickers. */
export const versionNumbers = (versions: Version[]): number[] => versions.map((version) => version.version);

export interface DatasetVersionState {
    dataset: Dataset | null;
    /** Every published version, newest first. */
    versions: Version[];
    /** The record of the selected version; `null` when nothing is published yet. */
    version: Version | null;
    /** The selection actually in force: `latest`, or the requested `vN` when it exists. */
    selection: VersionSelection;
    /** The selected version number; `0` when the dataset has no versions. */
    versionNumber: number;
    definition?: DatasetDefinition;
    stats?: DatasetStats;
    loading: boolean;
    error: Error | null;
}

/**
 * The record, definition and stats of one dataset version (DATASETS_DESIGN.md §11.2).
 *
 * The version list already carries a definition and stats per version (§10.1), so a page that
 * shows "latest or vN" needs exactly two requests - the dataset record and the version list -
 * and no `GET .../versions/{version}` round trip when the selection changes.
 *
 * A selection that names a version the dataset does not have (a stale link to `v7` of a `v3`
 * dataset) falls back to `latest` instead of rendering an empty page.
 */
export const useDatasetVersion = (name: string, requested: VersionSelection): DatasetVersionState => {
    const { dataset, loading: datasetLoading, error: datasetError } = useDataset(name);
    const { versions, loading: versionsLoading, error: versionsError } = useDatasetVersions(name);

    const latest = dataset?.latest_version ?? 0;
    const selection = normaliseSelection(requested, latest);
    const wanted = selectionToVersion(selection);
    const version =
        wanted === null ? (versions[0] ?? null) : (versions.find((candidate) => candidate.version === wanted) ?? null);

    return {
        dataset,
        versions,
        version,
        selection,
        versionNumber: version?.version ?? latest,
        definition: version?.definition ?? dataset?.definition,
        stats: version?.stats ?? dataset?.stats,
        loading: datasetLoading || versionsLoading,
        error: datasetError ?? versionsError,
    };
};
