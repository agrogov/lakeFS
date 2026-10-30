/**
 * Helpers for telling a dataset repository apart from an ordinary repository.
 *
 * The service stamps `lakefs.dataset=true` (plus the definition format) on the metadata of
 * every dataset repository it creates (DATASETS_DESIGN.md §6.1, §11.3), so any repository
 * object that carries metadata can be classified locally.
 */

/** Repository metadata key set by the datasets service on the dataset repository. */
export const DATASET_METADATA_KEY = 'lakefs.dataset';

/** Repository metadata key holding the dataset definition format version. */
export const DATASET_FORMAT_METADATA_KEY = 'lakefs.dataset.format';

/**
 * True when the repository metadata marks the repository as a dataset. Metadata values are
 * strings on the wire, so the flag is the string "true".
 */
export const isDatasetRepoMetadata = (metadata?: Record<string, string> | null): boolean =>
    metadata?.[DATASET_METADATA_KEY] === 'true';

/**
 * Repository names that are datasets, for callers that only have a name (repository listings
 * do not carry metadata; see `useDatasetNames` in `hooks.ts`).
 */
export const isDatasetName = (name: string, datasetNames?: Set<string>): boolean =>
    !!datasetNames && datasetNames.has(name);

/** Splits a repository list into ordinary repositories and dataset repositories. */
export const partitionDatasetRepos = <T extends { id: string }>(
    repos: T[],
    datasetNames?: Set<string>,
): { repositories: T[]; datasets: T[] } => {
    const repositories: T[] = [];
    const datasets: T[] = [];
    for (const repo of repos) {
        (isDatasetName(repo.id, datasetNames) ? datasets : repositories).push(repo);
    }
    return { repositories, datasets };
};
