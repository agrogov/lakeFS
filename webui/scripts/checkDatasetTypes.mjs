#!/usr/bin/env node
/**
 * Type-checks the dataset UI: `src/pages/datasets` and `src/lib/components/datasets`.
 *
 * `tsc --noEmit` over the whole webui reports hundreds of pre-existing errors (the repo-wide
 * check is not part of CI), which would drown the ones this project cares about. The scoped
 * project (`tsconfig.datasets.json`) compiles exactly those two directories; because they
 * import shared components, the program also contains files outside the scope, so only the
 * diagnostics reported *in* the two directories fail this check.
 */
import { spawnSync } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const TSC = path.join(root, 'node_modules', 'typescript', 'bin', 'tsc');
const PROJECT = path.join(root, 'tsconfig.datasets.json');
const SCOPED_DIRS = ['src/pages/datasets/', 'src/lib/components/datasets/'];

const run = spawnSync(process.execPath, [TSC, '--noEmit', '--pretty', 'false', '--project', PROJECT], {
    cwd: root,
    encoding: 'utf8',
});

if (run.error) {
    console.error(`could not run tsc: ${run.error.message}`);
    process.exit(1);
}

const output = `${run.stdout ?? ''}${run.stderr ?? ''}`;
const lines = output
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line !== '');
const scoped = lines.filter((line) => SCOPED_DIRS.some((dir) => line.startsWith(dir)));

if (scoped.length > 0) {
    console.error(`TypeScript errors in the dataset UI (${scoped.length}):`);
    for (const line of scoped) console.error(`  ${line}`);
    process.exit(1);
}

// A crash (a bad option, a missing project file) reports no `error TS....` line at all.
if (run.status !== 0 && !lines.some((line) => /error TS\d+/.test(line))) {
    console.error('tsc did not run:');
    console.error(output);
    process.exit(1);
}

console.log('no TypeScript errors in src/pages/datasets or src/lib/components/datasets');
