#!/bin/bash
#
# Fail if an upstream commit licensed under BSL 1.1 has become part of this branch.
#
# RBAC_DESIGN.md §2 (rules 2 and 5): upstream relicensed lakeFS from Apache 2.0 to
# BSL 1.1 in 358544dd7. Nothing at or after that commit may end up in our history, so
# this guard fails when 358544dd7 is an ancestor of HEAD.
#
# Inconclusive-history policy: FAIL CLOSED.
# The guard never passes vacuously. It either has enough history to answer the
# ancestry question conclusively, or it fails (exit 1) with the remedy:
#
#   * complete history (no .git/shallow marker): every ancestor of HEAD is present
#     locally, so a real `git merge-base --is-ancestor 358544dd7 HEAD` decides it;
#     if 358544dd7 is not in the object database at all, it cannot be an ancestor
#     of HEAD, which is itself a conclusive (and reported) result. The guard does
#     not fetch anything, so it works offline and cannot be fooled by a failed or
#     partial fetch;
#   * shallow clone (`.git/shallow` present), unborn HEAD, or no git repository:
#     ancestry cannot be decided, so the guard fails and tells you to deepen the
#     clone (`git fetch --unshallow`) or to run it where the history is complete.
#     CI does that: `.github/workflows/test.yaml` checks out with `fetch-depth: 0`
#     in the `license-base` and `validator` jobs (the latter runs the guard via
#     `make checks-validator`).
#
# LICENSE_GUARD_SKIP_IF_INCONCLUSIVE=1 is the documented, explicit opt-out for
# local work on a shallow clone: it downgrades the fail-closed cases to a warning
# and exit 0. CI never sets it, so a CI run can never pass inconclusively.
#
# Usage:
#   bash scripts/check-license-base.sh              # the guard
#   bash scripts/check-license-base.sh --self-test  # prove the guard can detect a violation
#   bash scripts/check-license-base.sh --help
#
# The guard is cwd-independent: it inspects the git work tree that contains the
# current directory, falling back to the one that contains this script (all git
# calls use `git -C`, so no relative path is resolved against the caller's cwd).
#
# Environment overrides:
#   BSL_COMMIT  the commit that re-licensed the tree to BSL 1.1 (default 358544dd7…)
#               — mainly for the self-test and for the failure-path demo:
#                 BSL_COMMIT=$(git rev-parse 4bb11638e) bash scripts/check-license-base.sh
#   LICENSE_GUARD_SKIP_IF_INCONCLUSIVE=1  local-only opt-out, see above
#
# Exit codes:
#   0  clean — no BSL 1.1 commit is an ancestor of HEAD (or the opt-out was used)
#   1  a BSL 1.1 upstream commit is an ancestor of HEAD, or the check is inconclusive
#   2  bad usage (or a failing self-test)

set -euo pipefail

readonly DEFAULT_BSL_COMMIT=358544dd7184fa972b0aa7862372366960511422
readonly BSL_COMMIT="${BSL_COMMIT:-${DEFAULT_BSL_COMMIT}}"
readonly SELF="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"

usage() {
	cat <<'EOF'
Fail if an upstream commit licensed under BSL 1.1 has become part of this branch
(RBAC_DESIGN.md §2 rules 2 and 5).

Usage:
  bash scripts/check-license-base.sh              # the guard, from any cwd
  bash scripts/check-license-base.sh --self-test  # prove the guard can detect a violation
  bash scripts/check-license-base.sh --help

Environment overrides:
  BSL_COMMIT  the commit that re-licensed the tree to BSL 1.1
              (default: 358544dd7184fa972b0aa7862372366960511422)
  LICENSE_GUARD_SKIP_IF_INCONCLUSIVE=1
              local-only opt-out: turn the fail-closed cases (shallow clone, unborn
              HEAD, no git repository) into a warning + exit 0. CI never sets it.

Exit codes:
  0  clean — no BSL 1.1 commit is an ancestor of HEAD (or the opt-out was used)
  1  a BSL 1.1 upstream commit is an ancestor of HEAD, or the check is inconclusive
  2  bad usage (or a failing self-test)
EOF
}

skip_requested() {
	[ "${LICENSE_GUARD_SKIP_IF_INCONCLUSIVE:-0}" = "1" ]
}

# inconclusive <reason...>: the guard cannot decide the ancestry question.
# Fails closed (exit 1) unless the caller explicitly opted out.
inconclusive() {
	if skip_requested; then
		echo "WARNING: licence-base check is INCONCLUSIVE: $*" >&2
		echo "WARNING: LICENSE_GUARD_SKIP_IF_INCONCLUSIVE=1 is set, so this local run does not fail." >&2
		echo "WARNING: CI never sets it — do not rely on this result in CI." >&2
		exit 0
	fi
	cat >&2 <<EOF
ERROR: licence-base check is INCONCLUSIVE: $*

The guard fails closed: it will not report "clean" when it cannot see the history
it is asked about (RBAC_DESIGN.md §2 rule 5).

To make it conclusive, run it in a clone with complete history:
  git fetch --unshallow          # deepen a shallow clone
  # in CI: actions/checkout with fetch-depth: 0 (see .github/workflows/test.yaml)

For local-only work on a shallow clone you may opt out explicitly:
  LICENSE_GUARD_SKIP_IF_INCONCLUSIVE=1 bash scripts/check-license-base.sh
EOF
	exit 1
}

# Repository the guard inspects: the git work tree containing the current directory,
# else the one containing this script. Using `git -C` everywhere makes the guard
# independent of the caller's cwd (no relative paths, no cd juggling).
REPO_DIR=""
gitq() { git -C "${REPO_DIR}" "$@"; }

violation() {
	cat >&2 <<EOF
ERROR: an upstream commit licensed under BSL 1.1 is part of this branch's history.

  BSL 1.1 commit: ${BSL_COMMIT}
                  $(gitq log -1 --format='%s' "${BSL_COMMIT}" 2>/dev/null || echo '<not readable>')
  HEAD:           $(gitq rev-parse HEAD)
                  $(gitq log -1 --format='%s' HEAD)

RBAC_DESIGN.md §2 rule 2 forbids merging, cherry-picking or rebasing onto upstream
commits at or after ${BSL_COMMIT:0:9} (the Apache 2.0 -> BSL 1.1 relicense). Rebase
this branch onto the Apache 2.0 history (upstream 4bb11638e) instead of onto later
upstream commits, which removes the BSL commit from the branch history.
EOF
	exit 1
}

resolve_repo() {
	local candidate
	local cwd="${PWD:-.}"
	for candidate in "${cwd}" "$(dirname "${SELF}")"; do
		if [ -n "${candidate}" ] && [ "$(git -C "${candidate}" rev-parse --is-inside-work-tree 2>/dev/null || echo false)" = "true" ]; then
			REPO_DIR="${candidate}"
			if [ "${candidate}" != "${cwd}" ]; then
				echo "note: no git work tree at ${cwd}; checking the one containing this script (${candidate})."
			fi
			return 0
		fi
	done
	inconclusive "not inside a git work tree, and neither is the directory holding this script"
}

run_check() {
	if ! command -v git >/dev/null 2>&1; then
		inconclusive "git is not installed"
	fi

	resolve_repo

	if ! gitq rev-parse --verify --quiet HEAD >/dev/null; then
		inconclusive "HEAD is unborn in ${REPO_DIR} (the repository has no commits yet)"
	fi

	local is_shallow=false
	if [ "$(gitq rev-parse --is-shallow-repository 2>/dev/null || echo false)" = "true" ]; then
		is_shallow=true
	fi

	local has_commit=false
	if gitq cat-file -e "${BSL_COMMIT}^{commit}" 2>/dev/null; then
		has_commit=true
	fi

	if [ "${has_commit}" = "true" ]; then
		local status=0
		gitq merge-base --is-ancestor "${BSL_COMMIT}" HEAD || status=$?
		case "${status}" in
		0)
			violation
			;;
		1)
			if [ "${is_shallow}" = "true" ]; then
				# The commit is here, but HEAD's ancestry is truncated: "not an
				# ancestor" may be an artefact of the shallow boundary.
				inconclusive "shallow clone: ${BSL_COMMIT:0:9} is present but HEAD's history is truncated"
			fi
			echo "OK: ${BSL_COMMIT:0:9} (BSL 1.1 relicense) is not an ancestor of HEAD $(gitq rev-parse --short HEAD)."
			exit 0
			;;
		*)
			inconclusive "'git merge-base --is-ancestor' exited with status ${status}"
			;;
		esac
	fi

	# The commit object is absent.
	if [ "${is_shallow}" = "true" ]; then
		inconclusive "shallow clone and commit ${BSL_COMMIT:0:9} is absent, so ancestry cannot be decided"
	fi

	# Complete history: every ancestor of HEAD is in the object database, so an
	# absent commit cannot be an ancestor of HEAD.
	echo "OK: ${BSL_COMMIT:0:9} (BSL 1.1 relicense) is absent from this clone, so it is not an ancestor of HEAD $(gitq rev-parse --short HEAD)."
	echo "    (complete history: no .git/shallow marker; nothing was fetched)"
	exit 0
}

# --- self-test -------------------------------------------------------------
# Builds throwaway repositories and asserts that the guard (a) passes clean
# history, (b) fails when the BSL commit is an ancestor, (c) passes when the
# commit is absent from a complete clone, (d) fails closed on a shallow clone,
# and (e) honours the documented local opt-out.
selftest_repo_commit() { # <dir> <file> <message>
	local dir="$1" file="$2" message="$3"
	echo "${message}" >"${dir}/${file}"
	git -C "${dir}" add "${file}"
	git -C "${dir}" -c user.name=license-guard-selftest \
		-c user.email=license-guard-selftest@example.com \
		-c commit.gpgsign=false commit -qm "${message}"
	git -C "${dir}" rev-parse HEAD
}

run_self_test() {
	local tmp failures=0
	tmp="$(mktemp -d)"
	# shellcheck disable=SC2064 # expand $tmp now, it is local to this function
	trap "rm -rf '${tmp}'" EXIT

	git init -q "${tmp}/repo"
	local apache_commit bsl_commit ours_commit
	apache_commit="$(selftest_repo_commit "${tmp}/repo" apache.txt "apache base")"
	git -C "${tmp}/repo" checkout -q -b bsl
	bsl_commit="$(selftest_repo_commit "${tmp}/repo" bsl.txt "BSL 1.1 relicense")"
	git -C "${tmp}/repo" checkout -q "${apache_commit}"
	git -C "${tmp}/repo" checkout -q -b ours
	ours_commit="$(selftest_repo_commit "${tmp}/repo" ours.txt "our work")"

	# A genuinely shallow clone of the clean "ours" branch.
	git clone -q --depth=1 --branch ours "file://${tmp}/repo" "${tmp}/shallow"
	mkdir -p "${tmp}/repo/sub/deeper"

	expect() { # <expected-exit> <commit> <dir> <description> [env assignment...]
		local expected="$1" commit="$2" dir="$3" description="$4" status=0
		shift 4
		(
			cd "${dir}"
			env BSL_COMMIT="${commit}" "$@" bash "${SELF}"
		) >/dev/null 2>&1 || status=$?
		if [ "${status}" -eq "${expected}" ]; then
			echo "PASS: ${description} (exit ${status})"
		else
			echo "FAIL: ${description} (expected exit ${expected}, got ${status})" >&2
			failures=$((failures + 1))
		fi
	}

	expect 0 "${bsl_commit}" "${tmp}/repo" "clean history passes"
	expect 0 "${bsl_commit}" "${tmp}/repo/sub/deeper" "clean history passes from a subdirectory"
	git -C "${tmp}/repo" checkout -q "${bsl_commit}"
	expect 1 "${bsl_commit}" "${tmp}/repo" "BSL 1.1 commit as ancestor fails"
	git -C "${tmp}/repo" checkout -q "${ours_commit}"
	expect 0 "0000000000000000000000000000000000000000" "${tmp}/repo" \
		"absent commit in a complete clone passes (conclusive)"
	expect 1 "${bsl_commit}" "${tmp}/shallow" "shallow clone fails closed"
	expect 0 "${bsl_commit}" "${tmp}/shallow" "shallow clone honours the opt-out" \
		LICENSE_GUARD_SKIP_IF_INCONCLUSIVE=1

	if [ "${failures}" -ne 0 ]; then
		echo "self-test FAILED (${failures} case(s))" >&2
		exit 2
	fi
	echo "self-test passed"
	exit 0
}

case "${1:-}" in
"") run_check ;;
--self-test) run_self_test ;;
-h | --help)
	usage
	exit 0
	;;
*)
	echo "FATAL: unknown argument: $1" >&2
	usage >&2
	exit 2
	;;
esac
