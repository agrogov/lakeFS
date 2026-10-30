import React, { useContext, createContext } from 'react';

import { repositories, branches, commits, NotFoundError, tags, BadRequestError, BareRepositoryError } from '../api';
import { useRouter } from './router';
import { useAPI } from './api';
import { RefTypeBranch, RefTypeCommit, RefTypeTag } from '../../constants';

export const resolveRef = async (repoId, refId) => {
    // try branch
    try {
        const branch = await branches.get(repoId, refId);
        return { id: branch.id, type: RefTypeBranch };
    } catch (error) {
        if (error instanceof NotFoundError) {
            // check if the repository is bare (has no branches)
            const branchList = await branches.list(repoId, true, '', '', 1);
            const isBareRepo = branchList?.results?.length === 0;
            if (isBareRepo) {
                throw new BareRepositoryError('Repository has no branches');
            }
        } else if (!(error instanceof BadRequestError)) {
            throw error;
        }
    }
    // try tag
    try {
        const tag = await tags.get(repoId, refId);
        return { id: tag.id, type: RefTypeTag };
    } catch (error) {
        if (!(error instanceof NotFoundError) && !(error instanceof BadRequestError)) {
            throw error;
        }
    }
    // try commit
    try {
        const commit = await commits.get(repoId, refId);
        return { id: commit.id, type: RefTypeCommit };
    } catch (error) {
        if (!(error instanceof NotFoundError)) {
            throw error;
        }
    }

    throw new NotFoundError('ref not found');
};

// Exported so a fork-owned page that browses a repository without the repository router
// (the dataset data/compare pages, DATASETS_DESIGN.md §11.2) can provide the repo/ref pair to
// the repository components it reuses. The provider below stays the only writer for the
// repository pages themselves.
export const RefContext = createContext(null);

export const useRefs = () => {
    const [refsState] = useContext(RefContext);
    return refsState;
};

/** A `RefContext` value that no repository component can tell apart from the real one. */
export const refContextValue = ({ repo, reference, compare = null, loading = false, error = null }) => [
    { repo, reference, compare, loading, error },
];

export const RefContextProvider = ({ children }) => {
    const router = useRouter();
    const { repoId } = router.params;
    const { ref, compare } = router.query;

    const { response, error, loading } = useAPI(async () => {
        if (!repoId) return null;
        const repo = await repositories.get(repoId);
        const reference = await resolveRef(repoId, ref || repo.default_branch);
        const comparedRef = await resolveRef(repoId, compare || repo.default_branch);
        return { repo, reference, compare: comparedRef };
    }, [repoId, ref, compare]);

    const refsState = {
        loading,
        error,
        repo: response?.repo || null,
        reference: response?.reference || null,
        compare: response?.compare || null,
    };

    return <RefContext.Provider value={[refsState]}>{children}</RefContext.Provider>;
};
