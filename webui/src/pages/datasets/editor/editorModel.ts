import {
    DataItem,
    DatasetDefinition,
    ErrorDetail,
    MetadataKey,
    MetadataValue,
    SourceRefType,
} from '../../../lib/api/datasets';

/**
 * Pure model behind the dataset editor (DATASETS_DESIGN.md §11.2).
 *
 * The editor keeps its own draft (`EditorItem`, metadata draft strings) and derives the
 * `format_version: 1` definition from it. The rules below mirror `pkg/datasets/definition.go`
 * (`Normalise`, `validate.go`) so the normalisation feedback is shown before a round trip and a
 * `POST /datasets/{dataset}/validate` answer lands on the row that caused it.
 *
 * Nothing here touches the network or React: it is unit tested directly.
 */

export const FORMAT_VERSION = 1 as const;

/** Targets under this directory belong to the service (§6.4). */
export const RESERVED_TARGET_PREFIX = '_dataset';

/** The two item types this build supports (`table`/`namespace` answer 501, §1.2). */
export type EditorItemType = 'object' | 'prefix';

/**
 * One row of the items table. `key` is a stable local identifier (the array index changes as
 * rows are added and removed); the definition never contains it.
 */
export interface EditorItem {
    key: string;
    repository: string;
    refType: SourceRefType;
    refId: string;
    type: EditorItemType;
    path: string;
    target: string;
    /**
     * Commit id the server resolved the ref to, when the last successful validation returned it
     * (§5.2: branch/tag refs are resolved on write). Rendered as "pinned to <commit>".
     */
    pinnedCommit?: string;
}

/** Problems attached to one item, keyed by the item's `key`. */
export type ItemProblems = Record<string, string[]>;

// ---------------------------------------------------------------------------
// Target and source-path normalisation (§5.2)
// ---------------------------------------------------------------------------

/** Drops empty segments, so leading, trailing and doubled slashes disappear. */
export const normaliseTarget = (raw: string): string =>
    raw
        .split('/')
        .filter((segment) => segment !== '')
        .join('/');

const hasDotSegment = (raw: string): boolean => raw.split('/').some((segment) => segment === '.' || segment === '..');

const isReservedTarget = (target: string): boolean =>
    target === RESERVED_TARGET_PREFIX || target.startsWith(`${RESERVED_TARGET_PREFIX}/`);

/** The problem a submitted target cannot be normalised out of, or null when it is fine. */
export const targetProblem = (raw: string): string | null => {
    if (hasDotSegment(raw)) return 'Target must not contain "." or ".." segments';
    if (isReservedTarget(normaliseTarget(raw)))
        return `Target must not be inside the reserved "${RESERVED_TARGET_PREFIX}" directory`;
    return null;
};

/**
 * Canonical source path for an item: a prefix keeps a trailing `/` (an empty path means the
 * whole repository), an object path loses its leading `/`.
 */
export const normaliseSourcePath = (type: EditorItemType, raw: string): string => {
    if (type === 'object') return raw.replace(/^\/+/, '');
    const trimmed = raw.replace(/^\/+|\/+$/g, '');
    return trimmed === '' ? '' : `${trimmed}/`;
};

/** Items sorted by normalised target, the order in which the definition is stored. */
export const sortedByTarget = (items: EditorItem[]): EditorItem[] =>
    [...items].sort((left, right) => {
        const a = normaliseTarget(left.target);
        const b = normaliseTarget(right.target);
        return a < b ? -1 : a > b ? 1 : 0;
    });

// ---------------------------------------------------------------------------
// Local item validation (before the server is asked)
// ---------------------------------------------------------------------------

/** Every "/" boundary prefix of a target, shallowest first, including the root. */
const ancestorTargets = (target: string): string[] => {
    if (target === '') return [];
    const ancestors = [''];
    for (let i = 1; i < target.length; i++) {
        if (target[i] === '/') ancestors.push(target.slice(0, i));
    }
    return ancestors;
};

/**
 * Target collisions of §5.2 rule 3: equal targets, targets on a `/` boundary ("a" vs "a/b"),
 * and the root target ("") which covers everything. "a" vs "a-b" is fine.
 *
 * Mirrors `targetProblems` in `pkg/datasets/validate.go`: items are walked in normalised target
 * order and the deepest ancestor is reported. Problems are keyed by the item's local `key`,
 * because the editor knows its rows by key and the server knows them by sorted index.
 */
export const overlapProblems = (items: EditorItem[]): ItemProblems => {
    const problems: ItemProblems = {};
    const sorted = sortedByTarget(items);
    const first = new Map<string, EditorItem>();
    for (const item of sorted) {
        const target = normaliseTarget(item.target);
        if (targetProblem(item.target) !== null) continue;

        const duplicate = first.get(target);
        if (duplicate) {
            problems[item.key] = [
                target === '' ? 'Target duplicates the repository root item' : `Target duplicates "${target}"`,
            ];
            continue;
        }
        let overlap: string | null = null;
        const ancestors = ancestorTargets(target);
        for (let i = ancestors.length - 1; i >= 0; i--) {
            if (first.has(ancestors[i])) {
                overlap = ancestors[i];
                break;
            }
        }
        if (overlap !== null) {
            problems[item.key] = [
                overlap === '' ? 'Target overlaps the repository root item' : `Target overlaps "${overlap}"`,
            ];
            continue;
        }
        first.set(target, item);
    }
    return problems;
};

/**
 * Every local problem of a row: the required members and the target/source normalisation rules
 * of §5.2. Server-side findings (repository existence, storage id, authorisation) are separate -
 * they can only be known after `POST /validate`.
 */
export const localItemProblems = (items: EditorItem[]): ItemProblems => {
    const problems: ItemProblems = {};
    const add = (key: string, message: string) => {
        (problems[key] ??= []).push(message);
    };

    for (const item of items) {
        if (item.repository === '') add(item.key, 'Repository is required');
        if (item.refId.trim() === '') add(item.key, 'Ref is required');
        const targetIssue = targetProblem(item.target);
        if (targetIssue) add(item.key, targetIssue);
        if (item.type === 'object') {
            if (normaliseTarget(item.target) === '') add(item.key, 'Target is required for object items');
            if (normaliseSourcePath(item.type, item.path) === '')
                add(item.key, 'Source path is required for object items');
        }
    }

    for (const [key, messages] of Object.entries(overlapProblems(items))) {
        for (const message of messages) add(key, message);
    }
    return problems;
};

// ---------------------------------------------------------------------------
// Definition building
// ---------------------------------------------------------------------------

/** Builds the §5.1 definition from the editor rows, normalised the way the server stores it. */
export const buildDefinition = (
    items: EditorItem[],
    description: string,
    metadata: Record<string, MetadataValue>,
): DatasetDefinition => ({
    format_version: FORMAT_VERSION,
    ...(description.trim() === '' ? {} : { description: description.trim() }),
    ...(Object.keys(metadata).length === 0 ? {} : { metadata }),
    data: sortedByTarget(items).map((item): DataItem => {
        const base: DataItem = {
            target: normaliseTarget(item.target),
            repository: item.repository.trim(),
            ref: { type: item.refType, id: item.refId.trim() },
            type: item.type,
        };
        if (item.type === 'object') {
            return { ...base, object: { path: normaliseSourcePath(item.type, item.path) } };
        }
        return { ...base, prefix: { path: normaliseSourcePath(item.type, item.path) } };
    }),
});

/** Parses a stored definition back into editor rows (edit mode). */
export const itemsFromDefinition = (definition: DatasetDefinition | undefined): EditorItem[] => {
    const data = definition?.data ?? [];
    return data.map((item, index) => ({
        key: `item-${index}`,
        repository: item.repository,
        refType: item.ref.type,
        refId: item.ref.id,
        type: item.type === 'prefix' ? 'prefix' : 'object',
        path: item.type === 'prefix' ? (item.prefix?.path ?? '') : (item.object?.path ?? ''),
        target: item.target,
        pinnedCommit: item.ref.type === 'commit' ? item.ref.id : undefined,
    }));
};

// ---------------------------------------------------------------------------
// Server validation errors (§10.4)
// ---------------------------------------------------------------------------

/** A per-item error after the server's `data[<index>]` paths were mapped onto editor rows. */
export interface MappedItemError {
    field: string;
    message: string;
}

export interface MappedValidationErrors {
    /** Problems keyed by the editor row they belong to. */
    items: Record<string, MappedItemError[]>;
    /** Problems that belong to the document rather than a row (name, format_version, data, ...). */
    general: string[];
}

const ITEM_PATH = /^data\[(\d+)\](?:\.(.+))?$/;

/**
 * Maps the `errors` array of a `400` onto editor rows.
 *
 * The server validates the *normalised* definition, so `data[2]` is the third item in target
 * order, not the third row on screen: `sortedByTarget` restores that mapping. An index the editor
 * cannot resolve (the server reported more items than the editor holds) becomes a general problem
 * rather than being dropped silently.
 */
export const mapValidationErrors = (errors: ErrorDetail[], items: EditorItem[]): MappedValidationErrors => {
    const mapped: MappedValidationErrors = { items: {}, general: [] };
    const sorted = sortedByTarget(items);
    for (const error of errors) {
        const match = ITEM_PATH.exec(error.path ?? '');
        if (!match) {
            mapped.general.push(error.path ? `${error.path}: ${error.message}` : error.message);
            continue;
        }
        const item = sorted[Number(match[1])];
        if (!item) {
            mapped.general.push(error.message);
            continue;
        }
        const field = match[2] ?? '';
        (mapped.items[item.key] ??= []).push({ field, message: error.message });
    }
    return mapped;
};

/**
 * The commit ids a successful validation resolved each row to, keyed by row. Used to show
 * "pinned to <commit>" (§11.2) and to carry the resolution across a save.
 */
export const pinnedCommits = (resolved: DatasetDefinition | undefined, items: EditorItem[]): Record<string, string> => {
    const pinned: Record<string, string> = {};
    const sorted = sortedByTarget(items);
    (resolved?.data ?? []).forEach((dataItem, index) => {
        const item = sorted[index];
        if (item && dataItem.ref.type === 'commit' && dataItem.ref.id !== '') {
            pinned[item.key] = dataItem.ref.id;
        }
    });
    return pinned;
};

/** `30f9eb1c9a3d4b8e...` → `30f9eb1`, the short form the rest of the UI shows. */
export const shortCommit = (commit: string): string => commit.slice(0, 7);

// ---------------------------------------------------------------------------
// Metadata form (§5.5)
// ---------------------------------------------------------------------------

/** What the form holds per key: a scalar input value or the chips of an allow_multiple key. */
export type MetadataDraft = Record<string, string | string[]>;

export type MetadataProblems = Record<string, string[]>;

const isFullDate = (value: string): boolean => {
    if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
    const parsed = new Date(`${value}T00:00:00Z`);
    return !Number.isNaN(parsed.getTime()) && parsed.toISOString().slice(0, 10) === value;
};

/** A date input value in local time can be off by one; the form works with plain YYYY-MM-DD. */
const isBlank = (value: string | string[] | undefined): boolean =>
    value === undefined || (Array.isArray(value) ? value.length === 0 : value.trim() === '');

/** Converts one scalar draft value to the JSON value the definition carries. */
const scalarValue = (key: MetadataKey, raw: string): MetadataValue => {
    if (key.type === 'number') return Number(raw);
    if (key.type === 'boolean') return raw === 'true';
    return raw;
};

/** Validates one scalar draft value; returns a human-readable problem or null. */
export const metadataValueProblem = (key: MetadataKey, raw: string): string | null => {
    if (key.type === 'number' && !Number.isFinite(Number(raw))) return 'Must be a number';
    if (key.type === 'date' && !isFullDate(raw)) return 'Must be a date (YYYY-MM-DD)';
    if (key.type === 'enum' && !(key.allowed_values ?? []).includes(raw)) {
        const allowed = (key.allowed_values ?? []).join(', ');
        return `Must be one of: ${allowed}`;
    }
    return null;
};

/**
 * Client-side validation of the metadata form, mirroring `pkg/datasets/metadata_validate.go`:
 * required keys must be present, values must match the key type, enum values must be allowed,
 * a date must be a real RFC 3339 full-date and an allow_multiple key must have at least one chip.
 */
export const validateMetadataDraft = (keys: MetadataKey[], draft: MetadataDraft): MetadataProblems => {
    const problems: MetadataProblems = {};
    const add = (name: string, message: string) => {
        (problems[name] ??= []).push(message);
    };

    for (const key of keys) {
        const value = draft[key.name];
        if (isBlank(value)) {
            if (key.required) add(key.name, 'Required');
            continue;
        }
        const values = Array.isArray(value) ? value : [value];
        for (const raw of values) {
            const message = metadataValueProblem(key, raw);
            if (message) add(key.name, message);
        }
    }
    return problems;
};

/** Builds the definition's metadata map from the form draft, dropping untouched keys. */
export const buildMetadata = (keys: MetadataKey[], draft: MetadataDraft): Record<string, MetadataValue> => {
    const metadata: Record<string, MetadataValue> = {};
    for (const key of keys) {
        const value = draft[key.name];
        if (isBlank(value)) continue;
        if (key.allow_multiple) {
            metadata[key.name] = (value as string[]).map((raw) => scalarValue(key, raw));
        } else {
            metadata[key.name] = scalarValue(key, value as string);
        }
    }
    return metadata;
};

/** Turns stored metadata into form values (edit mode), ignoring keys without a definition. */
export const metadataDraftFromValues = (
    keys: MetadataKey[],
    metadata: Record<string, MetadataValue> | undefined,
): MetadataDraft => {
    const draft: MetadataDraft = {};
    for (const key of keys) {
        const value = metadata?.[key.name];
        if (value === undefined || value === null) continue;
        if (Array.isArray(value)) {
            draft[key.name] = value.map(String);
        } else {
            draft[key.name] = String(value);
        }
    }
    return draft;
};

/** True when the form holds at least one problem; used to gate the footer buttons. */
export const hasProblems = (problems: Record<string, string[] | undefined>): boolean =>
    Object.values(problems).some((messages) => (messages ?? []).length > 0);
