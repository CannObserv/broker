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
# The second: every `>__X_PW__` becomes `#<sha256>`, **for every user**. The
# passwords file may hold a user either way -
#
#   __X_PW__=<plaintext>          hashed here
#   __X_PW_SHA256__=<64-hex>      passed through; the node holds no plaintext
#
# - and exactly one of the two per placeholder. The digest spelling is what a
# hash-only handoff leaves (CannObserv/archiver#251), and digests for everyone
# is what keeps the output plaintext-free, in the `#<sha256>` form `ACL SAVE`
# writes anyway (CannObserv/broker#49).
#
# Both the install in deploy/README.md and tests/deploy/ call this script, so
# what is tested is what is installed. Nothing it prints to stderr carries a
# value from the passwords file - placeholders and line numbers only.
#
# Usage: render-acl.sh <passwords-file>  > /etc/redis/users.acl
set -euo pipefail

PASSWORDS="${1:?usage: render-acl.sh <passwords-file>}"
SOURCE="$(dirname "$(readlink -f "$0")")/redis-acl.conf"

# What an empty value hashes to - a valid digest redis accepts, for a password
# that is nothing. docs/ACL-CUTOVER.md, "Rotating __DEFAULT_PW__".
EMPTY_SHA256=e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855

rendered=$(grep -v '^\s*#' "$SOURCE" | grep -v '^\s*$')

declare -A plain=() digest=()
errors=()
n=0
# `|| [[ -n $key ]]`: `read` is false on a final line with no newline, and
# without this that line was silently dropped.
while IFS='=' read -r key value || [[ -n $key ]]; do
    n=$((n + 1))
    [[ -z "${key// }" || "$key" == \#* ]] && continue
    if [[ $key =~ ^(__[A-Z]+_PW)_SHA256__$ ]]; then
        p="${BASH_REMATCH[1]}__"
        [[ -n ${digest[$p]+set} ]] && errors+=("$key: given twice")
        digest[$p]=$value
    elif [[ $key =~ ^__[A-Z]+_PW__$ ]]; then
        [[ -n ${plain[$key]+set} ]] && errors+=("$key: given twice")
        plain[$key]=$value
    else
        # Not echoed: a malformed line is as likely to be a bare password.
        errors+=("line $n: not __X_PW__=<plaintext> or __X_PW_SHA256__=<64-hex>")
    fi
done < "$PASSWORDS"

mapfile -t placeholders < <(grep -o '>__[A-Z]*_PW__' <<<"$rendered" | cut -c2- | sort -u)

for p in "${placeholders[@]}"; do
    dkey="${p%__}_SHA256__"
    if [[ -n ${plain[$p]+set} && -n ${digest[$p]+set} ]]; then
        errors+=("$p: both $p and $dkey - keep exactly one")
        continue
    elif [[ -n ${plain[$p]+set} ]]; then
        if [[ -z ${plain[$p]} ]]; then
            errors+=("$p: empty")
            continue
        fi
        # printf is a builtin: the value travels on a pipe, never on an argv.
        hash=$(printf %s "${plain[$p]}" | sha256sum | cut -d' ' -f1)
    elif [[ -n ${digest[$p]+set} ]]; then
        hash=${digest[$p]}
        if [[ ! $hash =~ ^[0-9a-f]{64}$ ]]; then
            errors+=("$dkey: not 64 lowercase hex, which is all redis accepts")
            continue
        elif [[ $hash == "$EMPTY_SHA256" ]]; then
            errors+=("$dkey: the digest of the empty string")
            continue
        fi
    else
        errors+=("$p: no $p or $dkey line")
        continue
    fi
    rendered=${rendered//">$p"/"#$hash"}
done

if ((${#errors[@]})); then
    echo "FATAL: $PASSWORDS cannot be rendered:" >&2
    printf '  %s\n' "${errors[@]}" >&2
    exit 1
fi

# Belt and braces over the loop above: nothing may leave here as a plaintext
# rule or a placeholder, whatever the tracked file grows to contain.
if grep -qE '(^| )>|__[A-Z0-9_]*_PW[A-Z0-9_]*__' <<<"$rendered"; then
    echo "FATAL: a plaintext rule or placeholder survived the render" >&2
    exit 1
fi

printf '%s\n' "$rendered"
