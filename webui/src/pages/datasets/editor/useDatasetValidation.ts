import { useEffect, useState } from 'react';

import { DatasetDefinition, DatasetStats, DatasetsError, ErrorDetail, datasets } from '../../../lib/api/datasets';

/**
 * Live validation against `POST /datasets/{dataset}/validate` (§10.1, §11.2).
 *
 * The request is debounced so typing a target path does not fire one call per keystroke, and a
 * stale answer is dropped when the definition changed while it was in flight. A disabled
 * control plane (`404`/`501`) is reported as `unavailable` rather than as a failure: the editor
 * still works, the server just cannot dry-run the definition.
 */

export const VALIDATION_DEBOUNCE_MS = 500;

export interface ValidationState {
    /** A request is scheduled or in flight. */
    validating: boolean;
    /** Definition the server resolved (commit refs, normalised paths) on the last success. */
    resolved?: DatasetDefinition;
    /** JSON signature of the definition `resolved` belongs to, so a stale answer is not shown. */
    resolvedFor?: string;
    stats?: DatasetStats;
    /** Per-problem list of the last `400` (§10.4); empty when the definition is valid. */
    errors: ErrorDetail[];
    /** Human-readable message of the last `400`. */
    message?: string;
    /** The server does not serve the validation route: the editor must not block on it. */
    unavailable: boolean;
    /** Anything that is not a validation answer (a network failure, for instance). */
    error?: Error;
}

const EMPTY: ValidationState = { validating: false, errors: [], unavailable: false };

const isUnavailable = (error: DatasetsError): boolean => error.status === 404 || error.status === 501;

/**
 * Runs `datasets.validate` for `definition` whenever it changes, debounced. `enabled` is false
 * while the local rules already reject the draft (a missing name, an overlap, ...): asking the
 * server about a document the editor knows is invalid only produces noise.
 */
export const useDatasetValidation = (
    datasetName: string,
    definition: DatasetDefinition,
    enabled: boolean,
): ValidationState => {
    const [state, setState] = useState<ValidationState>(EMPTY);

    useEffect(() => {
        if (datasetName === '' || !enabled) {
            setState(EMPTY);
            return;
        }

        let cancelled = false;
        const signature = JSON.stringify(definition);
        setState((previous) => ({ ...previous, validating: true }));
        const timer = setTimeout(() => {
            datasets
                .validate(datasetName, definition)
                .then((response) => {
                    if (cancelled) return;
                    setState({
                        validating: false,
                        resolved: response.definition,
                        resolvedFor: signature,
                        stats: response.stats,
                        errors: [],
                        unavailable: false,
                    });
                })
                .catch((error: unknown) => {
                    if (cancelled) return;
                    if (error instanceof DatasetsError) {
                        if (isUnavailable(error)) {
                            setState({ validating: false, errors: [], unavailable: true });
                            return;
                        }
                        setState({
                            validating: false,
                            errors: error.errors,
                            message: error.message,
                            unavailable: false,
                        });
                        return;
                    }
                    setState({ validating: false, errors: [], unavailable: false, error: error as Error });
                });
        }, VALIDATION_DEBOUNCE_MS);

        return () => {
            cancelled = true;
            clearTimeout(timer);
        };
    }, [datasetName, definition, enabled]);

    return state;
};
