/**
 * Version reference helpers (DATASETS_DESIGN.md §5.4, §7.1, §11.2).
 *
 * A dataset version `N` is the tag `vN` in the dataset repository and `latest` is the branch
 * that always points at the newest one. The UI keeps the *version number* in the URL and turns
 * it into a ref at the edges: `v3` for a specific version (immutable, so browsing it can never
 * show write controls) and `latest` where the moving pointer is wanted.
 */

/** The ref a dataset version number maps to in the dataset repository. */
export const versionTag = (version: number): string => `v${version}`;

/** The ref shown for "whatever is newest" (§11.2: the default of every version selector). */
export const LATEST_VERSION_REF = 'latest';

/** What a version picker holds: `latest` or a version tag. */
export type VersionSelection = string;

/** The default selection of a page: the newest published version. */
export const defaultVersionSelection = (): VersionSelection => LATEST_VERSION_REF;

/**
 * A version selection as a repository ref. `latest` stays `latest`; `3`, `v3` and
 * `latest` all round-trip to something the repository API accepts.
 */
export const selectionToRef = (selection: VersionSelection): string => {
    if (!selection || selection === LATEST_VERSION_REF) return LATEST_VERSION_REF;
    const match = /^v?(\d+)$/.exec(selection.trim());
    return match ? versionTag(Number(match[1])) : selection;
};

/** The version number a selection points at, or `null` for `latest`/an unparsable value. */
export const selectionToVersion = (selection: VersionSelection): number | null => {
    const match = /^v?(\d+)$/.exec((selection ?? '').trim());
    return match ? Number(match[1]) : null;
};

/** A selection normalised to the value a picker renders (`latest` or `vN`). */
export const normaliseSelection = (selection: VersionSelection | undefined, latest: number): VersionSelection => {
    if (!selection || selection === LATEST_VERSION_REF) return LATEST_VERSION_REF;
    const version = selectionToVersion(selection);
    if (version === null || version < 1 || version > latest) return LATEST_VERSION_REF;
    return versionTag(version);
};

export interface VersionOption {
    value: VersionSelection;
    label: string;
}

/**
 * The options of a version selector: `latest` first (labelled with the version it currently
 * points at), then every version newest first (§11.2).
 */
export const versionOptions = (versions: number[], latest: number): VersionOption[] => {
    const options: VersionOption[] = [];
    if (latest > 0) options.push({ value: LATEST_VERSION_REF, label: `latest (v${latest})` });
    const numbers = versions.length > 0 ? versions : rangeDescending(latest);
    for (const version of numbers) {
        if (version < 1) continue;
        options.push({ value: versionTag(version), label: versionTag(version) });
    }
    return options;
};

/** `latest = 3` → `[3, 2, 1]`; used before the version list has loaded. */
const rangeDescending = (latest: number): number[] => {
    const numbers: number[] = [];
    for (let version = latest; version >= 1; version--) numbers.push(version);
    return numbers;
};

/** The ref to compare a version with: the version before it, or `null` for `v1` (§11.2). */
export const previousVersionRef = (version: number): string | null =>
    Number.isInteger(version) && version > 1 ? versionTag(version - 1) : null;

/** `lakefs://<dataset>/<ref>` — the dataset root in the selected version (§7.2). */
export const lakefsUri = (dataset: string, ref: string, path = ''): string =>
    `lakefs://${dataset}/${ref}${normalisePath(path)}`;

/** `s3://<dataset>/<ref>` — the same data through the S3 gateway (§7.2). */
export const s3Uri = (dataset: string, ref: string, path = ''): string =>
    `s3://${dataset}/${ref}${normalisePath(path)}`;

/** Appends a path with exactly one separating slash, keeping a trailing slash if given. */
const normalisePath = (path: string): string => {
    if (!path) return '';
    return path.startsWith('/') ? path : `/${path}`;
};
