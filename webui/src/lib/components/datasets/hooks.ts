import { MAX_LISTING_AMOUNT, auth } from '../../api';
import {
    Dataset,
    DatasetDefinition,
    DatasetStats,
    DatasetsConfig,
    Version,
    VersionList,
    datasets,
    definitionOf,
    getDatasetNames,
    isDatasetsDisabledError,
    versionNumber,
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

// The `dataset:*` actions of the public RBAC table (DOC-1, DOC-2). Metadata-key operations are
// authorized on the datasets namespace, `arn:lakefs:dataset:::dataset/*`, not on a key ARN.
export const DATASETS_CREATE_ACTION = 'dataset:CreateDataset';
export const DATASETS_UPDATE_ACTION = 'dataset:UpdateDataset';
export const DATASETS_MANAGE_ACCESS_ACTION = 'dataset:ManageAccess';
export const DATASETS_LIST_METADATA_KEYS_ACTION = 'dataset:ListMetadataKeys';
export const DATASETS_READ_METADATA_KEY_ACTION = 'dataset:ReadMetadataKey';
export const DATASETS_CREATE_METADATA_KEY_ACTION = 'dataset:CreateMetadataKey';
export const DATASETS_UPDATE_METADATA_KEY_ACTION = 'dataset:UpdateMetadataKey';
export const DATASETS_DELETE_METADATA_KEY_ACTION = 'dataset:DeleteMetadataKey';
/** Kept for callers that gate the whole metadata-key screen. */
export const DATASETS_MANAGE_METADATA_KEYS_ACTION = DATASETS_CREATE_METADATA_KEY_ACTION;

/** The `§9.1` ARN of one dataset: `arn:lakefs:dataset:::dataset/<name>`. */
export const datasetResource = (name: string): string => `arn:lakefs:dataset:::dataset/${name}`;

/** The `§9.1` ARN metadata-key operations are authorized on: `arn:lakefs:dataset:::dataset/*`. */
export const METADATA_KEY_RESOURCE = datasetResource('*');

/** The ARN of one metadata key, kept for display; authorization uses `METADATA_KEY_RESOURCE`. */
export const metadataKeyResource = (_key: string): string => METADATA_KEY_RESOURCE;

/** Policy condition block, e.g. `{"StringLike": {"lakefs:metadata:owner": ["*"]}}`. */
type PolicyConditionLike = Record<string, Record<string, string[]>>;

/** Policy shape returned by `GET /auth/users/{id}/policies?effective=true`. */
interface PolicyStatementLike {
    effect?: string;
    action?: string | string[];
    resource?: string | string[];
    condition?: PolicyConditionLike;
}

interface PolicyLike {
    statement?: PolicyStatementLike[];
}

/** One page of `GET /auth/users/{id}/policies`. */
interface PolicyListLike {
    results?: PolicyLike[];
    pagination?: { has_more?: boolean; next_offset?: string };
}

/**
 * A concrete dataset ARN used to resolve wildcard resources such as `*` or
 * `arn:lakefs:dataset:::dataset/*` when the question is "may this user create *a* dataset".
 * Names are validated by the server; the placeholder is only ever matched, never created.
 */
const SAMPLE_DATASET_ARN = 'arn:lakefs:dataset:::dataset/sample';

/**
 * Glob matching for ARNs and actions (`*` matches any run of characters), as the server does.
 * Case-sensitive, like `pkg/auth/arn.go`: a policy action never matches with different case.
 */
export const globMatch = (pattern: string, value: string): boolean => {
    const escaped = pattern.replace(/[.+?^${}()|[\]\\]/g, '\\$&').replace(/\*/g, '.*');
    return new RegExp(`^${escaped}$`).test(value);
};

const asArray = (value: string | string[] | undefined): string[] => {
    if (value === undefined) return [];
    return Array.isArray(value) ? value : [value];
};

/**
 * True when a statement carries a condition block. The server evaluates conditions from the
 * request context (the caller's address, the object's metadata, ...), which the browser does
 * not have, so such a statement is never counted as a decision here (UI-5).
 */
const hasConditions = (statement: PolicyStatementLike): boolean => {
    const condition = statement.condition;
    return !!condition && typeof condition === 'object' && Object.keys(condition).length > 0;
};

/**
 * Whether a set of effective policies allows `action` on `resource`. Deny wins over allow,
 * and both action and resource are glob-matched, mirroring the server's permission model.
 *
 * A statement with conditions is skipped: the UI cannot evaluate them, so it never claims their
 * allow (no control for a user who may be denied) nor their deny (no control withheld from a
 * user who may be allowed). The control plane answers `403` regardless.
 */
export const policiesAllow = (policies: PolicyLike[], action: string, resource: string): boolean => {
    let allowed = false;
    for (const policy of policies) {
        for (const statement of policy.statement ?? []) {
            if (hasConditions(statement)) continue;
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
 * `dataset:<action>` on one resource, from the user's effective policies.
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

/** Hard cap so a huge policy set cannot turn one page render into an endless paging loop. */
const MAX_POLICY_PAGES = 10;

/**
 * Every effective policy of a user. `GET /auth/users/{id}/policies` is paginated and defaults
 * to 100 results, so reading only the first page would silently drop the grants of a user with
 * many policies (UI-5).
 */
const listEffectivePolicies = async (userId: string): Promise<PolicyLike[]> => {
    const policies: PolicyLike[] = [];
    let after = '';
    for (let page = 0; page < MAX_POLICY_PAGES; page++) {
        const response = (await auth.listUserPolicies(userId, true, after, MAX_LISTING_AMOUNT)) as PolicyListLike;
        policies.push(...(response?.results ?? []));
        const next = response?.pagination?.has_more ? response.pagination.next_offset : '';
        if (!next) break;
        after = next;
    }
    return policies;
};

const fetchCan = async (action: string, resource: string): Promise<boolean> => {
    const user = await auth.getCurrentUserWithCache();
    if (!user?.id) return false;
    return policiesAllow(await listEffectivePolicies(user.id), action, resource);
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
export const versionNumbers = (versions: Version[]): number[] => versions.map((version) => versionNumber(version));

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
 *
 * The version list itself is capped, so an explicit `vN` of a long history can be missing from
 * it: that version is fetched with `GET .../versions/{version}` instead, and an explicit `vN`
 * never falls back to the latest definition (UI-6).
 */
export const useDatasetVersion = (name: string, requested: VersionSelection): DatasetVersionState => {
    const { dataset, loading: datasetLoading, error: datasetError } = useDataset(name);
    const { versions, loading: versionsLoading, error: versionsError } = useDatasetVersions(name);

    const latest = dataset?.version ?? 0;
    const selection = normaliseSelection(requested, latest);
    const wanted = selectionToVersion(selection);
    const wantedVersion = wanted ?? 0;
    const explicit = wanted !== null;
    const listed =
        wanted === null
            ? (versions[0] ?? null)
            : (versions.find((candidate) => versionNumber(candidate) === wanted) ?? null);

    const fetchMissing = wanted !== null && !versionsLoading && listed === null;
    const {
        response: fetchedResponse,
        loading: fetchedLoading,
        error: fetchedError,
    } = useAPI(
        // `selection` is the normalised `vN`, the documented version ref (DOC-19).
        () => (fetchMissing ? datasets.getVersion(name, selection) : Promise.resolve(null)),
        [name, fetchMissing, selection],
    );
    const fetched = (fetchedResponse ?? null) as Version | null;
    const version = listed ?? fetched;

    return {
        dataset,
        versions,
        version,
        selection,
        versionNumber: version ? versionNumber(version) : explicit ? wantedVersion : latest,
        definition: explicit ? version?.definition : (version?.definition ?? definitionOf(dataset?.definition)),
        stats: explicit ? version?.stats : (version?.stats ?? dataset?.stats),
        loading: datasetLoading || versionsLoading || (fetchMissing && fetchedLoading),
        error: datasetError ?? versionsError ?? (fetchMissing ? fetchedError : null),
    };
};
