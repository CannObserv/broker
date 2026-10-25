#!/usr/bin/env bash
# Prune VS Code server builds and CLI binaries nothing runs (#126). By hand only:
# a dry run unless --apply, and never from a unit or a timer, because the editor
# is the operator's working surface. Run as the editor's user, never root.
#
# This is the interim step for the monthly patching run until patching-hosts
# prunes editor builds itself (#126 option 1, an ask on gregoryfoster/skills).
# Delete it when that ships. docs/HOST-DISK.md has why it is needed: the
# editor's own LRU loses track of builds, and `code tunnel prune` is a no-op on
# 1.140.0 and would take a staged newer build on 1.143.1.
#
# Rules (tests/deploy/test_prune_editor_builds.py pins each):
#   - a build whose commit hash is in `ps` is kept, and `ps` is read again just
#     before each removal, so one a client started since the plan is left;
#   - a server build newer, by version, than the newest running one is kept,
#     and one with no readable version; with nothing running, the newest stays;
#   - a CLI binary (code-<hash>) newer, by mtime, than the newest running one is
#     kept; with nothing running, the newest stays;
#   - extension builds, the VSIX cache, lru.json, .locks and staging dirs are
#     not touched: the editor prunes the first two itself on a restart;
#   - --apply records each removal, its bytes, the total and `df /` before and
#     after, under ~/.local/state/editor-prune/.
set -euo pipefail

apply=0
case ${1:-} in
'') ;;
--apply) apply=1 ;;
*)
	echo "usage: $0 [--apply]" >&2
	exit 2
	;;
esac

if [ "$(id -u)" -eq 0 ]; then
	echo "run as the editor's user, not root" >&2
	exit 2
fi

vsc=$HOME/.vscode-server
if [ ! -d "$vsc" ]; then
	echo "no $vsc: nothing to prune"
	exit 0
fi

# Every commit hash a process names: the server's paths and the CLI's argv[0].
# pgrep matches a pattern but cannot hand back the part that matched.
# shellcheck disable=SC2009
running() { ps -eo args= | grep -oE '[0-9a-f]{40}' | sort -u || true; }
live=$(running)
is_live() { grep -qx "$1" <<<"$live"; }
version() { grep -m1 -oE '"version": *"[^"]+"' "$1/server/package.json" 2>/dev/null | cut -d'"' -f4 || true; }
# Is $1 at or above $2, as versions?
at_least() { [ "$(printf '%s\n%s\n' "$1" "$2" | sort -V | tail -n1)" = "$1" ]; }

plan=() # "<path> <hash>" to remove
notes=()

# --- server builds: cli/servers/<Quality>-<hash> ---
servers=()
for d in "$vsc"/cli/servers/*; do
	[[ -d $d && ${d##*/} =~ ^[A-Za-z]+-[0-9a-f]{40}$ ]] && servers+=("$d")
done
top=
for d in "${servers[@]}"; do
	v=$(version "$d")
	if is_live "${d##*-}" && [ -n "$v" ] && { [ -z "$top" ] || at_least "$v" "$top"; }; then top=$v; fi
done
if [ -z "$top" ]; then # nothing runs: the newest stays
	for d in "${servers[@]}"; do
		v=$(version "$d")
		if [ -n "$v" ] && { [ -z "$top" ] || at_least "$v" "$top"; }; then top=$v; fi
	done
fi
for d in "${servers[@]}"; do
	h=${d##*-} v=$(version "$d")
	if is_live "$h"; then
		notes+=("kept $d: running ($v)")
	elif [ -z "$v" ]; then
		notes+=("kept $d: no readable version")
	elif at_least "$v" "$top"; then
		notes+=("kept $d: $v, not older than $top")
	else
		plan+=("$d $h")
	fi
done

# --- CLI binaries: code-<hash>, each with its .cli.<hash>.log ---
clis=()
for f in "$vsc"/code-*; do
	[[ -f $f && ${f##*/} =~ ^code-[0-9a-f]{40}$ ]] && clis+=("$f")
done
ref=
for f in "${clis[@]}"; do
	m=$(stat -c %Y "$f")
	if is_live "${f##*-}" && [[ -z $ref || $m -gt $ref ]]; then ref=$m; fi
done
if [ -z "$ref" ]; then
	for f in "${clis[@]}"; do
		m=$(stat -c %Y "$f")
		if [[ -z $ref || $m -gt $ref ]]; then ref=$m; fi
	done
fi
for f in "${clis[@]}"; do
	h=${f##*-}
	if is_live "$h"; then
		notes+=("kept $f: running")
	elif [ "$(stat -c %Y "$f")" -ge "$ref" ]; then
		notes+=("kept $f: not older than the newest running")
	else
		plan+=("$f $h")
	fi
done

if [ "$apply" -eq 0 ]; then
	[ "${#notes[@]}" -eq 0 ] || printf '%s\n' "${notes[@]}"
	for p in "${plan[@]}"; do echo "would remove ${p% *} ($(du -sh "${p% *}" | cut -f1))"; done
	[ "${#plan[@]}" -gt 0 ] || echo "nothing to prune"
	echo "dry run: nothing removed. Rerun with --apply to remove the above."
	exit 0
fi

state=$HOME/.local/state/editor-prune
mkdir -p "$state"
record=$state/$(date -u +%Y-%m-%dT%H%M%SZ).log
log() { echo "$*" | tee -a "$record"; }

log "editor-build prune (broker#126) $(date -u +%Y-%m-%dT%H:%M:%SZ) on $(hostname)"
log "df /: $(df -B1 --output=used,avail,pcent / | tail -n1)"
for n in "${notes[@]}"; do log "$n"; done
freed=0
for p in "${plan[@]}"; do
	path=${p% *} h=${p##* }
	if grep -qx "$h" <<<"$(running)"; then
		log "left $path: running now"
		continue
	fi
	bytes=$(du -sb "$path" | cut -f1)
	rm -rf -- "$path"
	if [[ $path == "$vsc/code-$h" ]]; then rm -f -- "$vsc/.cli.$h.log"; fi
	freed=$((freed + bytes))
	log "removed $path $bytes bytes"
done
log "freed $freed bytes"
log "df /: $(df -B1 --output=used,avail,pcent / | tail -n1)"
log "record: $record"
