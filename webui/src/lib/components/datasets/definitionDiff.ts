/**
 * Client-side definition diff (DATASETS_DESIGN.md §10.3).
 *
 * The control plane answers the definition diff itself (`GET .../refs/{l}/diff/{r}?type=definition`),
 * and the Compare page uses it. The versions timeline needs the *number of items a version
 * changed* for every row, and every version already carries its definition (§10.1), so this
 * module computes the diff between two definitions the page already has instead of making one
 * request per row.
 *
 * The rules are the ones the server documents: items are matched by `target`, and an item is
 * `changed` when its repository, commit, type or source path differs.
 */

import {
    DataItem,
    DatasetDefinition,
    DatasetDiff,
    ItemChange,
    MetadataChange,
    MetadataValue,
} from '../../api/datasets';

/** The source path of an item, whichever of the two source shapes it carries (§5.1). */
export const itemSourcePath = (item: DataItem | undefined): string => {
    if (!item) return '';
    if (item.type === 'object') return item.object?.path ?? '';
    if (item.type === 'prefix') return item.prefix?.path ?? '';
    return item.object?.path ?? item.prefix?.path ?? '';
};

/** The fields that make two items with the same target different (§10.3). */
const itemSignature = (item: DataItem): string =>
    [item.repository, item.ref?.type ?? '', item.ref?.id ?? '', item.type, itemSourcePath(item)].join('\u0000');

const sameMetadataValue = (left: MetadataValue | undefined, right: MetadataValue | undefined): boolean =>
    JSON.stringify(left ?? null) === JSON.stringify(right ?? null);

/** Item changes between two definitions, sorted by target. */
export const diffDefinitionItems = (
    left: DatasetDefinition | undefined,
    right: DatasetDefinition | undefined,
): ItemChange[] => {
    const leftItems = new Map<string, DataItem>();
    const rightItems = new Map<string, DataItem>();
    for (const item of left?.data ?? []) leftItems.set(item.target, item);
    for (const item of right?.data ?? []) rightItems.set(item.target, item);

    const targets = Array.from(new Set([...leftItems.keys(), ...rightItems.keys()])).sort((a, b) => a.localeCompare(b));

    const changes: ItemChange[] = [];
    for (const target of targets) {
        const from = leftItems.get(target);
        const to = rightItems.get(target);
        if (from && to) {
            if (itemSignature(from) !== itemSignature(to)) {
                changes.push({ target, type: 'changed', left: from, right: to });
            }
            continue;
        }
        if (to) changes.push({ target, type: 'added', right: to });
        else if (from) changes.push({ target, type: 'removed', left: from });
    }
    return changes;
};

/** One metadata value as the string list the diff schema reports (§10.2 MetadataChange). */
const metadataValues = (value: MetadataValue): string[] =>
    Array.isArray(value) ? value.map(String) : [String(value)];

/** Metadata changes between two definitions, sorted by key. */
export const diffDefinitionMetadata = (left: DatasetDefinition, right: DatasetDefinition): MetadataChange[] => {
    const leftMetadata = left.metadata ?? {};
    const rightMetadata = right.metadata ?? {};
    const keys = Array.from(new Set([...Object.keys(leftMetadata), ...Object.keys(rightMetadata)])).sort((a, b) =>
        a.localeCompare(b),
    );

    const changes: MetadataChange[] = [];
    for (const key of keys) {
        const from = leftMetadata[key];
        const to = rightMetadata[key];
        if (sameMetadataValue(from, to)) continue;
        if (from === undefined) changes.push({ key, type: 'added', right: metadataValues(to) });
        else if (to === undefined) changes.push({ key, type: 'removed', left: metadataValues(from) });
        else changes.push({ key, type: 'changed', left: metadataValues(from), right: metadataValues(to) });
    }
    return changes;
};

/**
 * The structural diff of two definitions, in the `DatasetDiff` shape of §10.2. Sections with
 * no changes are omitted, so a `definitionIsEqual` check is a cheap property test on the result.
 */
export const definitionDiff = (
    left: DatasetDefinition | undefined,
    right: DatasetDefinition | undefined,
): DatasetDiff => {
    const diff: DatasetDiff = { data: diffDefinitionItems(left, right) };
    const description = { left: left?.description ?? '', right: right?.description ?? '' };
    if (description.left !== description.right) diff.description = description;
    const metadata = left && right ? diffDefinitionMetadata(left, right) : [];
    if (metadata.length > 0) diff.metadata = metadata;
    return diff;
};

export interface DefinitionDiffCounts {
    added: number;
    removed: number;
    changed: number;
    /** Items that differ in any way — the "items changed" column of the timeline (§11.2). */
    total: number;
}

export const countDefinitionDiff = (diff: DatasetDiff | undefined): DefinitionDiffCounts => {
    const counts: DefinitionDiffCounts = { added: 0, removed: 0, changed: 0, total: 0 };
    for (const item of diff?.data ?? []) {
        counts[item.type] += 1;
        counts.total += 1;
    }
    return counts;
};

/** True when the two definitions have no item, metadata or description difference. */
export const definitionsEqual = (
    left: DatasetDefinition | undefined,
    right: DatasetDefinition | undefined,
): boolean => {
    const diff = definitionDiff(left, right);
    return diff.data.length === 0 && (diff.metadata ?? []).length === 0 && diff.description === undefined;
};

/**
 * The change summary of one version against the definition before it (§11.2 "items changed").
 * The first version adds every item it declares, so it counts as all-added against nothing.
 */
export const versionChangeCounts = (
    previous: DatasetDefinition | undefined,
    current: DatasetDefinition | undefined,
): DefinitionDiffCounts => countDefinitionDiff(definitionDiff(previous, current));
