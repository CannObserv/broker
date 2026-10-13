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
# The audit text goes to the job summary. In Actions the stdout copy sits
# between stop-commands and a random resume token: it is OSV's text, and a line
# of it starting `::` would otherwise be read as a workflow command.
#
# --frozen reads uv.lock as committed. A relock would need the private
# wheelhouse (CLAUDE.md, Environment & Tooling), which this job has no
# credential to fetch. `uv audit` is a preview command, hence the flag.
# tests/deploy/test_lock_audit.py pins all of it.
set -u

cd "$(dirname "$0")/.."

out=$(uv audit --frozen --preview-features audit-command 2>&1)
rc=$?

if [ -n "${GITHUB_ACTIONS:-}" ]; then
	token=$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')
	echo "::stop-commands::$token"
	printf '%s\n' "$out"
	echo "::$token::"
	printf '### uv audit --frozen (exit %s)\n\n```\n%s\n```\n' "$rc" "$out" >>"${GITHUB_STEP_SUMMARY:-/dev/null}"
else
	printf '%s\n' "$out"
fi

case $rc in
0) ;;
1) echo "::warning title=uv.lock advisories::uv audit found advisories in uv.lock; the job summary lists them. Sync the wheelhouse, then uv lock --upgrade-package <name> (CLAUDE.md, Environment & Tooling)." ;;
*) echo "::warning title=uv audit did not run::uv audit exited $rc; the lock was not audited. The job summary has its output." ;;
esac
exit 0
