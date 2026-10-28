#!/bin/bash
#
# Fail if an upstream commit licensed under BSL 1.1 has become part of this branch.
#
# RBAC_DESIGN.md §2 (rules 2 and 5): upstream relicensed lakeFS from Apache 2.0 to
# BSL 1.1 in 358544dd7. Nothing at or after that commit may end up in our history,
# so this guard fails if it has become an ancestor of HEAD. Missing history is not
# a failure: if the commit cannot be fetched (upstream remote not configured, no
# network, shallow clone) the check is skipped with a warning instead.
#
# Usage:
#   bash scripts/check-license-base.sh              # the actual guard
#   bash scripts/check-license-base.sh --self-test  # verify the guard can detect a violation
#
# Environment overrides (mainly for self-testing):
#   BSL_COMMIT       the commit that re-licensed the tree to BSL 1.1
#   UPSTREAM_REMOTE  remote the commit is fetched from   (default: upstream)
#   UPSTREAM_REF     upstream branch to fetch from       (default: master)
#
# Exit codes:
#   0  clean — no BSL 1.1 commit is an ancestor of HEAD (or the check was skipped)
#   1  a BSL 1.1 upstream commit is an ancestor of HEAD
#   2  bad usage (or a failing self-test)

set -euo pipefail

readonly BSL_COMMIT="${BSL_COMMIT:-358544dd7184fa972b0aa7862372366960511422}"
readonly UPSTREAM_REMOTE="${UPSTREAM_REMOTE:-upstream}"
readonly UPSTREAM_REF="${UPSTREAM_REF:-master}"
readonly SELF="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"

usage() {
	cat <<'EOF'
Fail if an upstream commit licensed under BSL 1.1 has become part of this branch
(RBAC_DESIGN.md §2 rules 2 and 5).

Usage:
  bash scripts/check-license-base.sh              # the actual guard
  bash scripts/check-license-base.sh --self-test  # verify the guard can detect a violation
  bash scripts/check-license-base.sh --help

Environment overrides (mainly for self-testing):
  BSL_COMMIT       the commit that re-licensed the tree to BSL 1.1
  UPSTREAM_REMOTE  remote the commit is fetched from   (default: upstream)
  UPSTREAM_REF     upstream branch to fetch from       (default: master)

Exit codes:
  0  clean — no BSL 1.1 commit is an ancestor of HEAD (or the check was skipped)
  1  a BSL 1.1 upstream commit is an ancestor of HEAD
  2  bad usage (or a failing self-test)
EOF
}

skip() {
	echo "WARNING: skipping licence-base check: $*" >&2
	echo "WARNING: the check is inconclusive; not failing." >&2
	exit 0
}

have_commit() {
	git cat-file -e "${BSL_COMMIT}^{commit}" 2>/dev/null
}

has_remote() {
	git remote get-url "${UPSTREAM_REMOTE}" >/dev/null 2>&1
}

fetch_quiet() {
	git fetch --no-tags --quiet "$@" >/dev/null 2>&1
}

run_check() {
	if ! git rev-parse --git-dir >/dev/null 2>&1; then
		skip "not inside a git repository"
	fi

	if ! git rev-parse --verify --quiet HEAD >/dev/null; then
		skip "HEAD is unborn (no commits yet)"
	fi

	# The commit object is only needed to answer the ancestry question, so fetch it
	# best-effort rather than requiring the caller to have a full clone.
	if ! have_commit; then
		echo "Commit ${BSL_COMMIT:0:9} is not in the local object database; trying to fetch it."
		if has_remote; then
			fetch_quiet "${UPSTREAM_REMOTE}" "${UPSTREAM_REF}" ||
				echo "Could not fetch ${UPSTREAM_REMOTE}/${UPSTREAM_REF}." >&2
			if ! have_commit; then
				# Some servers allow fetching a commit by its SHA; harmless if they don't.
				fetch_quiet --depth=1 "${UPSTREAM_REMOTE}" "${BSL_COMMIT}" ||
					echo "Could not fetch ${BSL_COMMIT:0:9} directly." >&2
			fi
		else
			echo "No '${UPSTREAM_REMOTE}' remote configured." >&2
		fi
	fi

	if ! have_commit; then
		skip "commit ${BSL_COMMIT:0:9} is unavailable (upstream remote '${UPSTREAM_REMOTE}' not fetched?)"
	fi

	local is_shallow=false
	if [ "$(git rev-parse --is-shallow-repository 2>/dev/null || echo false)" = "true" ]; then
		is_shallow=true
	fi

	local status=0
	git merge-base --is-ancestor "${BSL_COMMIT}" HEAD || status=$?

	case "${status}" in
	0)
		cat >&2 <<EOF
ERROR: an upstream commit licensed under BSL 1.1 is part of this branch's history.

  BSL 1.1 commit: ${BSL_COMMIT}
                  $(git log -1 --format='%s' "${BSL_COMMIT}" || true)
  HEAD:           $(git rev-parse HEAD)
                  $(git log -1 --format='%s' HEAD || true)

RBAC_DESIGN.md §2 rule 2 forbids merging, cherry-picking or rebasing onto upstream
commits at or after ${BSL_COMMIT:0:9} (the Apache 2.0 -> BSL 1.1 relicense). Rebase
this branch onto the Apache 2.0 history (upstream 4bb11638e) instead of onto later
upstream commits.
EOF
		exit 1
		;;
	1)
		if [ "${is_shallow}" = "true" ]; then
			skip "the repository is a shallow clone, so ancestry cannot be decided (run 'git fetch --unshallow')"
		fi
		echo "OK: ${BSL_COMMIT:0:9} (BSL 1.1 relicense) is not an ancestor of HEAD $(git rev-parse --short HEAD)."
		exit 0
		;;
	*)
		skip "'git merge-base --is-ancestor' exited with status ${status}"
		;;
	esac
}

# --- self-test -------------------------------------------------------------
# Builds throwaway repositories and checks that the guard (a) passes clean
# history, (b) fails when the BSL commit is an ancestor, and (c) skips, rather
# than failing, when the commit is simply unavailable.
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

	expect() { # <expected-exit> <commit> <dir> <description>
		local expected="$1" commit="$2" dir="$3" description="$4" status=0
		(
			cd "${dir}"
			env BSL_COMMIT="${commit}" \
				UPSTREAM_REMOTE="license-guard-selftest-no-such-remote" \
				bash "${SELF}"
		) >/dev/null 2>&1 || status=$?
		if [ "${status}" -eq "${expected}" ]; then
			echo "PASS: ${description} (exit ${status})"
		else
			echo "FAIL: ${description} (expected exit ${expected}, got ${status})" >&2
			failures=$((failures + 1))
		fi
	}

	expect 0 "${bsl_commit}" "${tmp}/repo" "clean history passes"
	git -C "${tmp}/repo" checkout -q "${bsl_commit}"
	expect 1 "${bsl_commit}" "${tmp}/repo" "BSL 1.1 commit as ancestor fails"
	git -C "${tmp}/repo" checkout -q "${ours_commit}"
	expect 0 "0000000000000000000000000000000000000000" "${tmp}/repo" \
		"unavailable commit skips instead of failing"

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
