import { auth } from '../../api';
import { DatasetsConfig, datasets, getDatasetNames, isDatasetsDisabledError } from '../../api/datasets';
import { useAPI } from '../../hooks/api';

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
export const useCanCreateDataset = (): boolean => {
    const { response, loading, error } = useAPI(fetchCanCreateDataset, []);
    if (loading) return false;
    if (error) return true;
    return response ?? true;
};

const fetchCanCreateDataset = async (): Promise<boolean> => {
    const user = await auth.getCurrentUserWithCache();
    if (!user?.id) return false;
    const policies = await auth.listUserPolicies(user.id, true);
    const results: PolicyLike[] = policies?.results ?? [];
    return policiesAllow(results, DATASETS_CREATE_ACTION, SAMPLE_DATASET_ARN);
};
