#!/usr/bin/env bash
# Audit uv.lock against OSV and report what it finds. Never fail (#86).
#
# This is the update mechanism patching-hosts' record cites (run.md section 7),
# run by .github/workflows/audit.yml and by hand. It is report-only on purpose.
# A gate that failed on a finding would turn main red on someone else's
# disclosure date, and every later check-in would read that red (#79). So every
# exit is 0:
#   uv exit 1 (findings)        -> warning "uv.lock advisories"
#   any other non-zero exit     -> warning "uv audit did not run", never clean
#   checkout not entered        -> warning "uv audit did not run", uv never called
# The audit text goes to the job summary. In Actions the stdout copy sits
# between stop-commands and a random resume token: it is OSV's text, and a line
# of it starting `::` would otherwise be read as a workflow command.
#
# --frozen reads uv.lock as committed. A relock would need the private
# wheelhouse (CLAUDE.md, Environment & Tooling), which this job has no
# credential to fetch. `uv audit` is a preview command, hence the flag. uv
# before 0.11.19 (astral-sh/uv#19645) has the command but not the name
# `audit-command`, and rejects it with exit 2 and "Unknown feature flag", so
# that one rejection retries without the flag (#103). It needs no uv floor.
# From 0.11.20 (astral-sh/uv#19669) an unknown name only warns, so the retry
# fires on uv <= 0.11.18 alone: a later uv that drops the name still audits.
# tests/deploy/test_lock_audit.py pins all of it.
set -u

# The lock is the one beside this script. A checkout it cannot enter is not
# audited: the caller's directory may hold some other lock (#112, SC2164).
if ! cd "$(dirname "$0")/.."; then
	echo "::warning title=uv audit did not run::cannot enter the checkout at $(dirname "$0")/..; the lock was not audited."
	exit 0
fi

out=$(uv audit --frozen --preview-features audit-command 2>&1)
rc=$?
if [ "$rc" -eq 2 ] && [[ $out == *"Unknown feature flag"* ]]; then
	out=$(uv audit --frozen 2>&1)
	rc=$?
fi

if [ -n "${GITHUB_ACTIONS:-}" ]; then
	token=$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')
	echo "::stop-commands::$token"
	printf '%s\n' "$out"
	echo "::$token::"
	fence='```'
	printf '### uv audit --frozen (exit %s)\n\n%s\n%s\n%s\n' "$rc" "$fence" "$out" "$fence" >>"${GITHUB_STEP_SUMMARY:-/dev/null}"
else
	printf '%s\n' "$out"
fi

case $rc in
0) ;;
1) echo "::warning title=uv.lock advisories::uv audit found advisories in uv.lock; the job summary lists them. Sync the wheelhouse, then uv lock --upgrade-package <name> (CLAUDE.md, Environment & Tooling)." ;;
*) echo "::warning title=uv audit did not run::uv audit exited $rc; the lock was not audited. The job summary has its output." ;;
esac
exit 0
