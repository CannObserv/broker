#!/bin/bash
# Render deploy/redis-acl.conf into a file redis-server will actually load.
#
# TWO transforms, and the first is not optional: **an aclfile permits no
# comments and no blank lines.** Redis refuses to start on one -
# "should start with user keyword followed by the username" - and it refuses the
# whole file, not the offending line. The tracked copy carries the reasoning for
# every grant, which is the repo's whole value, so the reasoning is stripped here
# rather than never written.
#
# Both the install in deploy/README.md and tests/deploy/test_redis_acl.py call
# this script, so what is tested is what is installed.
#
# Usage: render-acl.sh <passwords-file>  > /etc/redis/users.acl
# The passwords file is `PLACEHOLDER=value` lines, e.g. __ARCHIVER_PW__=hunter2.
set -euo pipefail

PASSWORDS="${1:?usage: render-acl.sh <passwords-file>}"
SOURCE="$(dirname "$(readlink -f "$0")")/redis-acl.conf"

rendered=$(grep -v '^\s*#' "$SOURCE" | grep -v '^\s*$')

while IFS='=' read -r placeholder value; do
    [[ -z "${placeholder// }" || "$placeholder" == \#* ]] && continue
    rendered=${rendered//"$placeholder"/"$value"}
done < "$PASSWORDS"

if grep -q '__[A-Z]*_PW__' <<<"$rendered"; then
    echo "FATAL: unsubstituted placeholders remain:" >&2
    grep -o '__[A-Z]*_PW__' <<<"$rendered" | sort -u >&2
    exit 1
fi

printf '%s\n' "$rendered"
