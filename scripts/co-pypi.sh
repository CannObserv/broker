#!/usr/bin/env bash
# Run one uv command with a co-pypi access token in its environment (#144).
#
#   bash scripts/co-pypi.sh uv sync --locked --group dev   # a dev tree
#   bash scripts/co-pypi.sh uv lock                        # a pin change
#
# The token is minted by scripts/co_pypi_token.py from the co-pypi-reader key
# (/etc/broker/co-pypi-reader.json, or BROKER_CO_PYPI_KEY) and given to the
# wrapped uv alone; it is never printed. The outer `uv run` starts from an
# allowlisted environment, so a sourced ~/broker/.env's PATs and a shell-wide
# GOOGLE_APPLICATION_CREDENTIALS reach neither uv. The two lists match
# co_pypi_token.py's ALLOWED and ALLOWED_PREFIXES, plus the key variable
# (tests/deploy/test_co_pypi_token.py). That module's own dependency is uv.lock's
# co-pypi group, run --frozen in this checkout (--project), never resolved at
# run time (#113). The wrapped uv runs where this was called from:
# scripts/deploy.sh calls it in a release. Details: docs/CI.md, The co-pypi index.
set -euo pipefail

ALLOWED=(PATH HOME USER LOGNAME LANG TERM TMPDIR http_proxy https_proxy no_proxy all_proxy HTTP_PROXY HTTPS_PROXY NO_PROXY ALL_PROXY BROKER_CO_PYPI_KEY)
PREFIXES=(LC_ XDG_ SSL_CERT_ UV_)

keep=()
while IFS= read -r name; do
  allowed=0
  for a in "${ALLOWED[@]}"; do [[ $name == "$a" ]] && allowed=1; done
  for p in "${PREFIXES[@]}"; do [[ $name == "$p"* ]] && allowed=1; done
  [[ $allowed == 1 ]] && keep+=("$name=${!name}")
done < <(compgen -e)

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec env -i "${keep[@]}" uv run --project "$(dirname "$HERE")" --frozen --only-group co-pypi \
  python "$HERE/co_pypi_token.py" "$@"
