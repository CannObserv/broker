#!/usr/bin/env bash
# Deploy broker: build a root-owned release from a pushed commit, switch, verify (#106).
#
#   scripts/deploy.sh [<ref>]            <ref> on origin/main, CI green; default origin/main
#   scripts/deploy.sh --skip-ci [<ref>]  without asking CI: an emergency, logged
#   scripts/deploy.sh --help
#
# A rollback is a deploy of the previous build: scripts/deploy.sh <old sha12>,
# gated the same way. Exits 0 when deployed and verified, 1 when refused or
# switched back to a build that answers, 4 when nothing answers.
#
# Epic #22's design (docs/plans/2026-10-17-22-release-deploy.md, D1, D4, D7-D9),
# adapted from CannObserv/processor's deploy.sh (no HTTP surface, a CI gate, a
# unit install) with CannObserv/status's root-owned releases (status#14). The
# units run /srv/broker/live, a symlink into releases/<sha12>: a read-only
# `git archive` of one commit on origin/main, its locked wheels and its own
# venv, owned by root. Nothing done in a checkout reaches a unit that runs live
# until this script puts it there.
#
# In this order:
#   0. ask GitHub whether the commit's CI passed (unless --skip-ci)
#   1. build releases/<sha12> at its final path (or reuse it): archive, the
#      wheelhouse, uv sync, read-only, root's, REVISION last
#   2. switch live (rename(2), atomic), then install each deploy/broker-* unit
#      from the release that differs from its installed copy
#   3. verify each unit this deploy changed: one whose unit it installed, or one
#      that runs live when live moved. The probe by its next natural tick, never
#      forced (the two-tick pending rule); the backup by one forced run; the
#      drift check by nothing, on purpose (#110)
#   4. on failure, switch back, units too, and prove the old build the same way
#   5. verified: enable the drift timer if this deploy added it (#110)
#   6. compare the host configs (never installed), prune to KEEP releases plus live
#
# It restarts nothing, and never redis-server. The one thing it starts is the
# drift timer, the first time it installs it. Runs as exedev; sudo -n for every
# write under the deploy root, for the unit files and for systemctl: a password
# prompt would hang the deploy holding its lock, so it is a refusal instead. exedev can
# sudo anything, so this is no boundary against it: what root ownership buys is
# that changing what a unit runs takes sudo, which journals the command.
set -euo pipefail

ROOT="${BROKER_DEPLOY_ROOT:-/srv/broker}"
# Prefixed to every installed path: a test's tree, nothing on the node.
HOST="${BROKER_DEPLOY_HOST:-}"
# GOOGLE_APPLICATION_CREDENTIALS for the wheelhouse sync: the read-only
# co-pypi-reader key's path, which exedev reads (B4).
ENV_FILE="${BROKER_DEPLOY_ENV_FILE:-/etc/broker/.env}"
BACKUP_STATE="${BROKER_DEPLOY_BACKUP_STATE:-/var/lib/broker-backup/state.json}"
KEEP="${BROKER_DEPLOY_KEEP:-5}"
# The system interpreter: the backup cannot see /home (ProtectHome=yes, #108),
# so a uv-managed Python under ~/.local/share/uv would not start for it.
PYTHON="${BROKER_DEPLOY_PYTHON:-/usr/bin/python3.12}"
# CI takes about 2 minutes; a run still going after this is wedged or queued behind one.
CI_WAIT_SECONDS="${BROKER_DEPLOY_CI_WAIT_SECONDS:-600}"
CI_POLL_SECONDS="${BROKER_DEPLOY_CI_POLL_SECONDS:-30}"
# The probe's next natural tick: OnUnitActiveSec=10min plus AccuracySec=1min,
# then up to its TimeoutStartSec=60.
PROBE_WAIT_SECONDS="${BROKER_DEPLOY_PROBE_WAIT_SECONDS:-900}"
# A backup run already going: its TimeoutStartSec=600.
BACKUP_WAIT_SECONDS="${BROKER_DEPLOY_BACKUP_WAIT_SECONDS:-660}"
POLL_SECONDS="${BROKER_DEPLOY_POLL_SECONDS:-10}"
# The jobs a run must have, which tests/deploy/test_deploy.py holds ci.yml to;
# every job a run lists must pass too, named here or not. Each quoted: the name
# has spaces.
CI_JOBS=("lint + test")
GITHUB_API="https://api.github.com/repos/CannObserv/broker"
UNIT_DIR="$HOST/etc/systemd/system"
PROBE_UNIT=broker-bus-health.service
BACKUP_UNIT=broker-backup.service
DRIFT_UNIT=broker-drift.service
# The timers a deploy enables, once, when it adds one and verifies (#110): a
# decision made per timer, never "any new timer". The backup's waits for its key
# (deploy/README.md, Install), so a rebuilt node's deploy must not enable it.
ENABLE_NEW=(broker-drift.timer)
# deploy/<file>=<where Install in deploy/README.md puts it>: installed verbatim,
# so compared byte for byte after a deploy, and never installed by one (D9).
# tests/deploy/test_deploy.py accounts for every file under deploy/.
HOST_CONFIGS=(
  "redis-server.service.d/broker.conf=/etc/systemd/system/redis-server.service.d/broker.conf"
  "redis-server.service.d/memory.conf=/etc/systemd/system/redis-server.service.d/memory.conf"
  "system.slice.d/broker-memory.conf=/etc/systemd/system/system.slice.d/broker-memory.conf"
  "tailscaled.service.d/memory.conf=/etc/systemd/system/tailscaled.service.d/memory.conf"
  "sysctl.d/60-broker-memory.conf=/etc/sysctl.d/60-broker-memory.conf"
  "earlyoom.default=/etc/default/earlyoom"
  "needrestart.conf.d/broker.conf=/etc/needrestart/conf.d/broker.conf"
  "apt/preferences.d/tailscale.pref=/etc/apt/preferences.d/tailscale.pref"
  "wait-for-tailnet-addr.sh=/usr/local/sbin/wait-for-tailnet-addr.sh"
)
# Installed derived, not verbatim: redis.conf.broker appended with a requirepass
# minted at install, redis-acl.conf rendered with digests, both under /etc/redis,
# which exedev cannot read. Compared with the release live ran instead; the
# parity tests in tests/deploy compare them with the node.
DERIVED_CONFIGS=(redis.conf.broker redis-acl.conf)
# What a release must import - the probe's, the backup's, restore's and the drift
# check's entry points (docs/RECOVERY.md runs restore as root from live) and co-core - and the
# build id it reports (src/broker/revision.py, which reads REVISION beside
# pyproject.toml: the editable install keeps src/ the release's own, #105 CR 2).
IMPORT_PROBE='import src.broker.bus_health, src.broker.backup, src.broker.restore, src.broker.drift, co_core; from src.broker.revision import read_revision; print(read_revision())'
SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

note() { echo "deploy: $*" >&2; }
die() {
  note "$*"
  exit 1
}
dead() {
  note "$*"
  exit 4
}

usage() { sed -n '4,6p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; }

# Whole numbers, or refused before anything runs: bash arithmetic on "5x" is an
# error that would otherwise end a verified deploy at the prune.
for knob in KEEP CI_WAIT_SECONDS CI_POLL_SECONDS PROBE_WAIT_SECONDS BACKUP_WAIT_SECONDS POLL_SECONDS; do
  [[ "${!knob}" =~ ^[0-9]+$ ]] || die "BROKER_DEPLOY_$knob=${!knob} is not a whole number"
done

skip_ci=0
ref=""
while (($#)); do
  case "$1" in
    --skip-ci) skip_ci=1 ;;
    -h | --help)
      usage
      exit 0
      ;;
    -*) die "unknown flag $1 (see --help)" ;;
    *)
      [[ -z "$ref" ]] || die "one ref at a time"
      ref="$1"
      ;;
  esac
  shift
done
ref="${ref:-origin/main}"

# exedev builds, and root only takes the finished release. Built by root, uv
# would use root's cache, and no write under the root would need sudo.
[[ "$(id -u)" -eq 0 ]] && die "run as exedev, not root; the script sudoes where it needs to"

# A release has a copy of this script and no .git to build from.
git -C "$SRC" rev-parse --git-dir >/dev/null 2>&1 ||
  die "run from a checkout (~/broker/scripts/deploy.sh); $SRC is not one"

[[ -d "$ROOT" ]] || die "$ROOT does not exist. Once: sudo install -d -o root -g root -m 755 $ROOT"

# Root's, and writable by root alone: a link is only as safe as its directory,
# and exedev could otherwise repoint live with ln (status#14).
roots_alone() { # <dir>
  # A link's own mode is 777, so find would call it writable.
  [[ ! -L "$1" ]] || die "$1 is a link; it must be the directory itself"
  [[ "$(stat -c %u:%g -- "$1")" == 0:0 ]] ||
    die "$1 is not root:root, so exedev can change what the units run." \
      "Once: sudo chown root:root $1 (deploy/README.md, Deploying)"
  [[ -z "$(find "$1" -maxdepth 0 -perm /022)" ]] ||
    die "$1 is writable by more than root. Once: sudo chmod 755 $1"
}
roots_alone "$ROOT"
# -L too: -e is false for a link to nowhere, which install -d cannot make.
[[ ! -e "$ROOT/releases" && ! -L "$ROOT/releases" ]] || roots_alone "$ROOT/releases"

# The root directory itself: exedev cannot create a lock file in it.
exec 9<"$ROOT"
flock -n 9 || die "another deploy is running (it holds the lock on $ROOT)"

# The path seams are the tests'; left set in a shell, one retargets a real
# deploy, so each that is not the node's default is said, and journaled (CR 19).
seams=""
for seam in ROOT=/srv/broker HOST= ENV_FILE=/etc/broker/.env \
  BACKUP_STATE=/var/lib/broker-backup/state.json; do
  name="${seam%%=*}"
  [[ "${!name}" == "${seam#*=}" ]] || seams+=" BROKER_DEPLOY_$name=${!name}"
done
if [[ -n "$seams" ]]; then
  note "paths that are not the node's:$seams"
  logger -t broker-deploy "paths that are not the node's:$seams" || true
fi

# The installed units this deploy replaces, kept for a switch back. They are
# the only copies left, a hand fix included: an exit that leaves them neither
# installed back nor needed (a signal, a switch back that could not rename)
# keeps them and says where (CR 18).
backup="$(mktemp -d "${TMPDIR:-/tmp}/broker-deploy.XXXXXX")"
keep_units=0
cleanup() {
  if ((keep_units)) && [[ -n "$(units_touched 2>/dev/null)" ]]; then
    note "the units this deploy replaced are kept in $backup/units, and the ones it added are" \
      "named in $backup/units.added: put them back by hand (sudo install -m 644, daemon-reload)"
  else
    # `|| :`: under set -e a failing command in the trap would replace the exit status.
    rm -rf "$backup" || :
  fi
}
trap cleanup EXIT

# Ended by a signal (Ctrl-C, or an agent's tool call timing out: a deploy can
# wait 600 s for CI, then 900 s for a probe tick, twice on a switch back), say
# where it stopped and what live names, in the journal too. A signal to the
# process group ends the step in progress; this runs when it has.
stage="before the switch"
interrupted() {
  trap - INT TERM HUP
  local now
  case "$stage" in
    "before the switch") die "interrupted before the switch; nothing switched" ;;
    "while pruning") note "interrupted while pruning; live is on ${build:-?}, verified" && exit 0 ;;
  esac
  now="$(readlink "$ROOT/live" 2>/dev/null)" || now="nothing"
  logger -t broker-deploy "interrupted on ${build:-?} $stage: live -> $now, unverified" || true
  dead "interrupted on ${build:-?} $stage: live -> $now, unverified. Deploy again, or deploy" \
    "the build it should run: scripts/deploy.sh <sha12> (journalctl -t broker-deploy -n 20)"
}
trap interrupted INT TERM HUP

# --- which commit ----------------------------------------------------------

git -C "$SRC" fetch --quiet --prune origin
sha="$(git -C "$SRC" rev-parse --verify --quiet "${ref}^{commit}")" || die "cannot resolve $ref"
git -C "$SRC" merge-base --is-ancestor "$sha" origin/main ||
  die "$ref ($sha) is not on origin/main; only a pushed main commit is deployed"
build="$(git -C "$SRC" rev-parse --short=12 "$sha")"
release="$ROOT/releases/$build"

# --- CI (D7) -----------------------------------------------------------------
#
# CannObserv/status's gate (status#11), as processor ported it. Asked before
# anything is built, so a refusal changes nothing. Rollbacks too: a kept release
# proves it was built, not that its CI passed; --skip-ci is the escape when
# GitHub cannot answer. audit.yml is report-only and never asked.

# Unauthenticated: the repo is public, and 60 requests an hour per address
# covers a deploy's 2 (22 waiting the full 600 s). No token, so none to store.
# A refusal (rate limit, outage, no answer) never passes.
github() { # <path>: GitHub's JSON answer, or a refusal naming GitHub's message
  local out
  if out="$(curl -sS --fail-with-body --max-time 10 \
    -H 'Accept: application/vnd.github+json' "$GITHUB_API/$1")"; then
    # A JSON object, or refused: jq reads an empty body as no input at all,
    # prints nothing and exits 0, so an empty jobs answer would list no failed job.
    jq -e 'type == "object"' <<<"$out" >/dev/null 2>&1 ||
      die "GitHub's answer about $build's CI is not the JSON expected; nothing was built." \
        "Deploy again later, or pass --skip-ci."
    printf '%s\n' "$out"
    return
  fi
  out="$(jq -r '.message // empty' <<<"$out" 2>/dev/null)" || out=""
  die "GitHub did not answer about $build's CI.${out:+ GitHub says: $out}" \
    "Nothing was built; deploy again later, or pass --skip-ci."
}

# The run that decides: the newest push run of ci.yml on main for exactly this
# commit. A dispatch adds a workflow_dispatch run on the same SHA, and check
# runs cannot tell them apart. A re-run counts, since a run reports its latest attempt.
push_run() { # the run as JSON, or nothing
  jq -c --arg sha "$sha" '[.workflow_runs[]
    | select(.head_sha == $sha and .event == "push" and .head_branch == "main")]
    | max_by(.created_at) // empty' ||
    die "GitHub's answer about $build's CI runs is not the JSON expected; nothing was built." \
      "Deploy again later, or pass --skip-ci."
}

# Waits for the run, bounded. Two commits will never get one, so they are
# refused at once: one behind origin/main's tip (GitHub runs CI on the newest
# commit of each push only), and one a workflow pushed - GitHub never runs CI
# for a push made with GITHUB_TOKEN, and the weekly context measurement is one
# (c5a0e06, 2026-10-15, has no run). The skills hook pushes with a PAT, and its
# commits do get runs.
finished_run() {
  local deadline=$((SECONDS + CI_WAIT_SECONDS)) tip run state url left committer
  tip="$(git -C "$SRC" rev-parse origin/main)"
  committer="$(git -C "$SRC" log -1 --format=%cn "$sha")"
  while :; do
    run="$(github "actions/workflows/ci.yml/runs?head_sha=$sha&event=push&branch=main&per_page=100" | push_run)" ||
      exit 1
    left=$((deadline - SECONDS))
    if [[ -z "$run" && "$committer" == "github-actions[bot]" ]]; then
      die "no CI run for $build, pushed by a workflow ($committer): GitHub never runs CI for a" \
        "push made with GITHUB_TOKEN, such as the weekly context measurement. Deploy the commit" \
        "under it, scripts/deploy.sh $(git -C "$SRC" rev-parse --short=12 "$sha^"), or pass" \
        "--skip-ci. Nothing was built."
    elif [[ -z "$run" && "$sha" != "$tip" ]]; then
      die "no CI run for $build as a push to main. GitHub runs CI on the newest commit of each push" \
        "only: deploy that one, or pass --skip-ci. Pushed in the last minute, with another push" \
        "after it? Its run may not be listed yet: deploy again shortly. Nothing was built."
    elif [[ -z "$run" ]]; then
      state="not queued yet" url=""
      ((left > 0)) ||
        die "no CI run for $build after ${CI_WAIT_SECONDS}s ([skip ci]?). Pass --skip-ci to deploy it" \
          "anyway. Nothing was built."
    else
      state="$(jq -r .status <<<"$run")"
      url="$(jq -r .html_url <<<"$run")"
      [[ "$state" == completed ]] && {
        printf '%s\n' "$run"
        return
      }
      ((left > 0)) ||
        die "CI for $build is still $state after ${CI_WAIT_SECONDS}s. Nothing was built; deploy again" \
          "when it finishes. Run: $url"
    fi
    note "waiting for CI on $build ($state)${url:+: $url}"
    sleep $((left < CI_POLL_SECONDS ? left : CI_POLL_SECONDS))
  done
}

# Success only, from the run itself, every job it lists, and at least CI_JOBS.
# A skipped job leaves the run "success", so the jobs are read too. CI_JOBS is
# only the floor: it comes from this checkout, which may be older than ci.yml.
# It goes to jq as a JSON array: "lint + test" would not survive a split on spaces.
ci_gate() {
  local run url conclusion jobs problems required
  run="$(finished_run)" || exit 1
  url="$(jq -r .html_url <<<"$run")"
  conclusion="$(jq -r '.conclusion // "nothing"' <<<"$run")"
  jobs="$(github "actions/runs/$(jq -r .id <<<"$run")/jobs?per_page=100")" || exit 1
  required="$(jq -cn '$ARGS.positional' --args "${CI_JOBS[@]}")"
  problems="$(jq -r --argjson required "$required" '[
      (.jobs[] | select(.conclusion != "success") | "\(.name) (\(.conclusion // .status))"),
      ($required[] as $name | select(any(.jobs[]; .name == $name) | not)
        | "\($name) (not in the run)")
    ] | join(", ")' <<<"$jobs")" ||
    die "GitHub's answer about $build's CI jobs is not the JSON expected; nothing was built." \
      "Deploy again later, or pass --skip-ci."
  [[ "$conclusion" == success ]] || problems="run concluded $conclusion${problems:+; $problems}"
  # Cancelled is not a verdict, and has nothing to fix. ci.yml cancels no push
  # run in progress, but a pending one is replaced when a newer push queues
  # behind a running one, and the skills hook pushes unprompted.
  local remedy="fix it on main"
  [[ "$conclusion" == cancelled ]] && remedy="re-run it from its page (a re-run counts), or deploy a newer commit"
  [[ -z "$problems" ]] || die "CI did not pass for $build: $problems. Nothing was built; $remedy. Run: $url"
  note "CI passed for $build: $url"
  logger -t broker-deploy "CI passed for $build: $url" || true
}

if ((skip_ci)); then
  note "not asking CI about $build (--skip-ci)"
  logger -t broker-deploy "CI not checked for $build (--skip-ci)" || true
else
  ci_gate
fi

# --- the release -----------------------------------------------------------

# What the release's venv says it is, run from the release as exedev: it
# imports both modules and co-core, and prints read_revision(). Its stderr is
# kept for the message. -I: no PYTHONPATH, PYTHONHOME or user site from the
# caller, and no cwd on sys.path, so src comes through the release's own
# editable .pth and co_core from its own venv. -B: bytecode is the build's to
# write, never a probe's (-I ignores PYTHONDONTWRITEBYTECODE).
reports() {
  (cd "$release" && .venv/bin/python -I -B -c "$IMPORT_PROBE" 2>"$backup/probe.err")
}
probe_error() { tr '\n' ' ' <"$backup/probe.err" 2>/dev/null | sed 's/ *$//'; }

# The wheelhouse files uv.lock names: the sdist and wheel paths of each package
# whose source is registry = ".wheelhouse" (co-core), relative to that directory.
# "./.wheelhouse" too: pyproject.toml spells it so, and uv normalises it today.
locked_wheels() {
  awk '
    /^\[/ { wheelhouse = 0 }
    /^source = \{ registry = "(\.\/)?\.wheelhouse" \}$/ { wheelhouse = 1 }
    wheelhouse && match($0, /path = "[^"]*"/) { print substr($0, RSTART + 8, RLENGTH - 9) }
  ' "$release/uv.lock"
}

# The digest the release's pyproject.toml pins for each wheelhouse file, as
# "<file> <sha256>" lines (#125): its [tool.broker.wheelhouse-sha256] table, one
# "file" = "hex" line each, the form tests/deploy/test_sync_wheelhouse.py holds.
pinned_digests() {
  awk '
    /^\[/ { table = ($0 == "[tool.broker.wheelhouse-sha256]"); next }
    table && /^"[^"]+" = "[^"]*"$/ { split($0, field, "\""); print field[2], field[4] }
  ' "$release/pyproject.toml"
}

# D4/B3: the checkout's sync, then only what the lock names into the release.
# The checkout's copy of sync_wheelhouse.py writes its own ../.wheelhouse, the
# whole bucket (652 files, 89 MB in 2026-10); the release's would take the
# bucket into the release. Copied with cp, never linked: a link shares an inode
# with the checkout, and the chown below would reach it. The sync's own deps are
# uv.lock's wheelhouse group, in the checkout's venv, never the project (#113).
# Each copy is held to the release's own pinned digest (#125): uv.lock records
# none, so `uv sync --locked` would install any file of that name. The check is
# on the copy, the bytes uv sync reads, and comes before it.
wheelhouse() {
  local key name digest got missing=()
  local -A pinned=()
  [[ -r "$ENV_FILE" ]] ||
    die "$ENV_FILE is missing or unreadable; the wheelhouse sync's key path is there. Nothing switched."
  # The file's value or nothing: the unit reads only the file, and so does this.
  key="$(
    unset GOOGLE_APPLICATION_CREDENTIALS
    set -a
    # shellcheck disable=SC1090
    . "$ENV_FILE" || exit 1 # set -e does not reach into a command substitution
    printf '%s' "${GOOGLE_APPLICATION_CREDENTIALS:-}"
  )" || die "cannot read $ENV_FILE as shell (set -a; . $ENV_FILE); nothing switched"
  [[ -n "$key" ]] || die "no GOOGLE_APPLICATION_CREDENTIALS in $ENV_FILE for the wheelhouse sync; nothing switched"
  (cd "$SRC" && GOOGLE_APPLICATION_CREDENTIALS="$key" uv run --frozen \
    --only-group wheelhouse python scripts/sync_wheelhouse.py >&2) ||
    die "syncing the wheelhouse into $SRC/.wheelhouse failed for $build; nothing switched"
  while read -r name digest; do
    pinned[$name]="$digest"
  done < <(pinned_digests)
  mkdir -p "$release/.wheelhouse"
  while read -r name; do
    [[ -n "$name" && "$name" != */* ]] ||
      die "uv.lock names the wheelhouse file '$name', not a file name; nothing switched"
    digest="${pinned[$name]:-}"
    [[ "$digest" =~ ^[0-9a-f]{64}$ ]] ||
      die "uv.lock names $name, which $build's pyproject.toml [tool.broker.wheelhouse-sha256] pins no sha256 for; nothing switched"
    if [[ -f "$SRC/.wheelhouse/$name" ]]; then
      cp -- "$SRC/.wheelhouse/$name" "$release/.wheelhouse/$name" ||
        die "copying $name into $build's wheelhouse failed; nothing switched"
      got="$(sha256sum -- "$release/.wheelhouse/$name")" ||
        die "cannot hash $name in $build's wheelhouse; nothing switched"
      got="${got%% *}"
      if [[ "$got" != "$digest" ]]; then
        logger -t broker-deploy "refused $build: $name has sha256 $got, not the $digest it pins" || true
        die "$name in $SRC/.wheelhouse has sha256 $got, not the $digest $build's pyproject.toml pins; nothing switched. Remove it and deploy again: the sync fetches a file that is not there, and this checks it again"
      fi
    else
      missing+=("$name")
    fi
  done < <(locked_wheels)
  ((${#missing[@]} == 0)) ||
    die "uv.lock names ${missing[*]}, which the sync left out of $SRC/.wheelhouse; nothing switched"
}

# Each step says why it stopped: under set -e alone the deploy would exit
# silently. The release, left without REVISION, is rebuilt next time.
build_release() {
  local home out
  if [[ -e "$release" ]]; then
    sudo -n rm -rf "$release" || die "cannot remove the unfinished releases/$build; nothing switched"
  fi
  note "building $build"
  [[ -d "$ROOT/releases" ]] || sudo -n install -d -o root -g root -m 755 "$ROOT/releases" ||
    die "cannot make $ROOT/releases; nothing switched"
  # Built where it will run, by exedev: a uv venv embeds its absolute path.
  sudo -n install -d -m 755 -o "$(id -un)" -g "$(id -gn)" "$release" ||
    die "cannot make releases/$build for $(id -un) to build in; nothing switched"
  git -C "$SRC" archive "$sha" | tar -x -C "$release" ||
    die "git archive failed for $build; nothing switched"
  wheelhouse
  # Copied, never hardlinked from ~/.cache/uv: a hardlink is the cache's inode
  # and every dev venv's, so the chown below would reach them (status#14).
  # Editable, as the design chose (Q5): its .pth names this release itself.
  # The venv is the release's own .venv, whatever the caller's shell names.
  (cd "$release" && env -u UV_PROJECT_ENVIRONMENT -u VIRTUAL_ENV UV_PYTHON_DOWNLOADS=never \
    uv sync --locked --no-dev --compile-bytecode --link-mode copy --python "$PYTHON" --quiet) ||
    die "uv sync failed for $build; nothing switched"
  home="$(sed -n 's/^home *= *//p' "$release/.venv/pyvenv.cfg" 2>/dev/null)" || home=""
  [[ -n "$home" && "$home" != /home/* ]] ||
    die "$build's venv runs ${home:-an unknown interpreter}, not one outside /home; nothing switched"
  # --compile-bytecode reaches site-packages alone; src/ is the project's.
  (cd "$release" && .venv/bin/python -m compileall -q src >/dev/null) ||
    die "compileall failed for $build's src/; nothing switched"
  reports >/dev/null || die "$build's venv does not import ($(probe_error)); nothing switched"
  # Readable by every user under any umask, writable by none: the probe runs as
  # exedev, and the synced wheels are 0600.
  chmod -R a-w,a+rX "$release" || die "cannot make $build read-only; nothing switched"
  sudo -n chown -R root:root "$release" || die "cannot hand $build to root; nothing switched"
  # REVISION last, by root: a release without one is an interrupted build.
  { echo "$build" | sudo -n tee "$release/REVISION" >/dev/null && sudo -n chmod 444 "$release/REVISION"; } ||
    die "cannot write $build's REVISION; nothing switched"
  # From the release, as exedev, after root wrote it: the path read_revision()
  # reads and the 444 read, in one line. A miss takes REVISION back off.
  out="$(reports)" || out=""
  if [[ "$out" != "$build" ]]; then
    sudo -n rm -f "$release/REVISION" || :
    die "$build reports ${out:-nothing} from the release, not $build ($(probe_error)); nothing switched"
  fi
}

# The build a link names, by its last component: a link made by hand during a
# recovery may be absolute.
release_of() {
  local link
  link="$(readlink "$ROOT/live" 2>/dev/null)" || return 0
  basename "$link"
}

# Why this release cannot be reused as it stands; nothing when it can.
#   - No REVISION: an interrupted build, or half-pruned.
#   - Not root's: handed back by hand, so exedev may have changed it.
#   - The probe fails, or reports another build: its venv no longer runs.
unusable() {
  local out
  if [[ ! -e "$release" ]]; then
    echo "not built"
  elif [[ ! -f "$release/REVISION" ]]; then
    echo "an interrupted build"
  elif [[ "$(stat -c %u -- "$release")" != 0 ]]; then
    echo "not root's"
  elif ! out="$(reports)"; then
    echo "a venv that no longer runs ($(probe_error))"
  elif [[ "$out" != "$build" ]]; then
    echo "a release that reports $out"
  fi
}

# A release live runs is never rebuilt in place, whatever is wrong with it: that
# pulls the code out from under its units, and a failed sync leaves live with no
# release at all. Unless it is gone altogether: building it is the repair.
why="$(unusable)"
if [[ -z "$why" ]]; then
  note "reusing release $build"
else
  [[ "$why" == "not built" || "$(release_of)" != "$build" ]] ||
    die "release $build is $why, and live runs it. Deploy another build first; this one is then rebuilt."
  [[ "$why" == "not built" ]] || note "release $build is $why; rebuilding"
  build_release
fi
# Prune by last deploy, not first build. A failure only reorders the prune.
sudo -n touch "$release" || note "cannot touch $release; the prune may take it sooner"

# --- switch, verify, or switch back ----------------------------------------

# What a directory reports as its build, as src/broker/revision.py reads it:
# REVISION's contents, dev when there is none, unknown when it is empty. A
# relative link resolves from the deploy root.
served_build() { # <dir or link target>
  local dir rev
  dir="$(cd "$ROOT" 2>/dev/null && cd -P "$1" 2>/dev/null && pwd)" || {
    echo dev
    return 0
  }
  [[ -e "$dir/REVISION" ]] || {
    echo dev
    return 0
  }
  rev="$(tr -d '[:space:]' <"$dir/REVISION" 2>/dev/null)" || rev=""
  echo "${rev:-unknown}"
}

swap() { # <link> <target>: rename(2) over the old link, so there is never no link
  sudo -n ln -sfn "$2" "$1.new" && sudo -n mv -Tf "$1.new" "$1"
}

# --- units (D9) --------------------------------------------------------------

units_of() { # the release's units, by name: deploy/broker-*.{service,timer} only
  local path
  for path in "$release"/deploy/broker-*.service "$release"/deploy/broker-*.timer; do
    [[ -f "$path" ]] && basename "$path"
  done
  return 0
}

# Installs the release's units that differ from their installed copies, after
# keeping each copy aside for restore_units. One daemon-reload, and a changed
# timer is try-restarted so it re-arms on its new schedule. A new unit is
# installed, never enabled: enabling is a decision, made in ENABLE_NEW for the
# timers named there (enable_new_timers). Units the release lacks stay.
# Called as `install_units || ...`, so set -e is off in here: every step that
# can fail says so itself.
install_units() {
  local name unit changed=() added=() timers=()
  { mkdir -p "$backup/units" && : >"$backup/units.added"; } ||
    {
      note "cannot keep the installed units aside in $backup"
      return 1
    }
  while read -r name; do
    unit="$UNIT_DIR/$name"
    cmp -s "$release/deploy/$name" "$unit" && continue
    if [[ -e "$unit" ]]; then
      cp "$unit" "$backup/units/$name" ||
        {
          note "cannot keep $unit aside; not installing it"
          return 1
        }
      changed+=("$name")
      [[ "$name" != *.timer ]] || timers+=("$name")
    else
      echo "$name" >>"$backup/units.added"
      added+=("$name")
    fi
    sudo -n install -m 644 "$release/deploy/$name" "$unit" ||
      {
        note "installing $name in $UNIT_DIR failed"
        return 1
      }
  done < <(units_of)
  ((${#changed[@]} + ${#added[@]})) || return 0
  sudo -n systemctl daemon-reload || {
    note "systemctl daemon-reload failed"
    return 1
  }
  # A timer that will not re-arm is a schedule that no longer runs, and the
  # probe's is the node's only always-on broker-side signal: fail, never verify
  # past it.
  for name in "${timers[@]}"; do
    sudo -n systemctl try-restart "$name" ||
      {
        note "systemctl try-restart $name failed: systemctl status $name"
        return 1
      }
  done
  local list="${changed[*]}"
  for name in "${added[@]}"; do list+=" $name (new)"; done
  note "units installed from $build: ${list# }"
  logger -t broker-deploy "units from $build: ${list# }" || true
  for name in "${added[@]}"; do
    if enabled_when_verified "$name"; then
      note "$name is new here: enabled once this deploy verifies"
    elif ! enabled_when_verified "${name%.service}.timer"; then
      note "$name is new here and not enabled. If it should run: sudo systemctl enable --now $name"
    fi
  done
}

enabled_when_verified() { # <unit>: one of ENABLE_NEW
  local name
  for name in "${ENABLE_NEW[@]}"; do [[ "$name" == "$1" ]] && return 0; done
  return 1
}

# Each ENABLE_NEW timer this deploy added is enabled once the deploy verifies, so
# a deploy switched back never ran it. One installed before is never enabled
# again: an operator's `systemctl disable --now` sticks. Processor's
# enable_new_timers (processor#35), for named timers only. A failure is a note:
# the drift timer watches the deploy, it never decides one.
enable_new_timers() {
  local name
  [[ -f "$backup/units.added" ]] || return 0
  while read -r name; do
    enabled_when_verified "$name" || continue
    if sudo -n systemctl enable --now "$name"; then
      # --now starts it, and a timer whose OnBootSec has passed fires at once:
      # that first run is the one to read.
      note "$name enabled; its first run: journalctl -u ${name%.timer} -n 5"
      logger -t broker-deploy "$name enabled" || true
    else
      note "systemctl enable --now $name failed; enable it by hand (deploy/README.md, The drift check)"
    fi
  done <"$backup/units.added"
}

# The unit files install_units replaced or added, or tried to.
units_touched() {
  local path
  for path in "$backup/units"/*; do
    [[ -f "$path" ]] && basename "$path"
  done
  [[ ! -f "$backup/units.added" ]] || cat "$backup/units.added"
}

units_replaced() { [[ -n "$(units_touched)" ]]; }

# Switch back, for units: the copies install_units replaced go back, and what it
# added is removed. A failure is a note: the rollback still proves the old
# build, and says whether it answers.
restore_units() {
  local path name timers=() any=0
  for path in "$backup/units"/*; do
    [[ -f "$path" ]] || continue
    name="$(basename "$path")"
    sudo -n install -m 644 "$path" "$UNIT_DIR/$name" || note "restoring $name failed: $path"
    [[ "$name" != *.timer ]] || timers+=("$name")
    any=1
  done
  # Missing when install_units could not even start; set -e is on here, and a
  # failed redirect would end the rollback before it proves the old build.
  if [[ -f "$backup/units.added" ]]; then
    while read -r name; do
      sudo -n rm -f "$UNIT_DIR/$name" || note "removing $UNIT_DIR/$name failed"
      any=1
    done <"$backup/units.added"
  fi
  ((any)) || return 0
  sudo -n systemctl daemon-reload || note "systemctl daemon-reload failed"
  for name in "${timers[@]}"; do
    sudo -n systemctl try-restart "$name" || note "systemctl try-restart $name failed"
  done
  logger -t broker-deploy "units restored" || true
}

# --- verification (D8) -------------------------------------------------------

# The services this deploy changed, by name: each whose unit or timer it
# installed, and, when live moved, each that runs live. A code-only deploy moves
# live and installs nothing, and is most deploys once C3 and C4 have landed.
changed_services() {
  local name path
  {
    while read -r name; do
      [[ -n "$name" ]] && echo "${name%.*}.service"
    done < <(units_touched)
    if ((moved)); then
      for path in "$UNIT_DIR"/broker-*.service; do
        [[ -f "$path" ]] || continue
        [[ "$(sed -n 's/^WorkingDirectory=//p' "$path" | tail -n 1)" == "$ROOT/live" ]] &&
          basename "$path"
      done
    fi
  } | sort -u
}

# The build a service must report: what its installed unit's WorkingDirectory=
# holds. A release reports its sha12; a unit still on ~/broker reports dev.
expected_of() { # <service>
  served_build "$(sed -n 's/^WorkingDirectory=//p' "$UNIT_DIR/$1" 2>/dev/null | tail -n 1)"
}

# `systemctl start` on a oneshot mid-run joins that run, which started on the
# old release (status CR 2): wait it out first. reset-failed, so a start limit a
# failing build reached cannot refuse the run that proves the fix.
verify_backup() { # <build it must report>
  local want="$1" deadline=$((SECONDS + BACKUP_WAIT_SECONDS)) outcome revision
  sudo -n systemctl reset-failed "$BACKUP_UNIT" || true
  while [[ "$(systemctl show -p ActiveState --value "$BACKUP_UNIT" 2>/dev/null)" == activating ]]; do
    ((SECONDS < deadline)) || {
      note "$BACKUP_UNIT has been running for ${BACKUP_WAIT_SECONDS}s"
      return 1
    }
    sleep "$POLL_SECONDS"
  done
  sudo -n systemctl start "$BACKUP_UNIT" || {
    note "the $BACKUP_UNIT run failed on $want: journalctl -u ${BACKUP_UNIT%.service} -n 50"
    return 1
  }
  read -r outcome revision < <(jq -r '"\(.outcome // "none") \(.revision // "none")"' \
    "$BACKUP_STATE" 2>/dev/null || echo "unreadable none")
  if [[ ("$outcome" == uploaded || "$outcome" == unchanged) && "$revision" == "$want" ]]; then
    note "the backup run on $want: $outcome"
    return 0
  fi
  note "the $BACKUP_UNIT run on $want wrote outcome $outcome, revision $revision to $BACKUP_STATE"
  return 1
}

# The next natural tick, never a forced one: two ticks seconds apart would read
# an in-flight entry as stuck (the two-tick pending rule, docs/BUS-HEALTH.md).
# The invocation systemd lists at the first poll is the baseline, whether it is
# running or done: it started before the switch. The next one started after it,
# on whatever live names now. It must exit 0 and log its summary with the build.
verify_probe() { # <build it must report>
  local want="$1" deadline=$((SECONDS + PROBE_WAIT_SECONDS)) base="" first=1 key value
  local id state result status rev
  while :; do
    id="" state="" result="" status=""
    while IFS='=' read -r key value; do
      case "$key" in
        InvocationID) id="$value" ;;
        ActiveState) state="$value" ;;
        Result) result="$value" ;;
        ExecMainStatus) status="$value" ;;
      esac
    done < <(systemctl show -p InvocationID -p ActiveState -p Result -p ExecMainStatus \
      "$PROBE_UNIT" 2>/dev/null)
    if ((first)); then
      base="$id" first=0
    elif [[ -n "$id" && "$id" != "$base" && "$state" != activating ]]; then
      break
    fi
    ((SECONDS < deadline)) || {
      note "no tick of $PROBE_UNIT finished within ${PROBE_WAIT_SECONDS}s, and the deploy never" \
        "forces one: systemctl list-timers broker-bus-health.timer"
      return 1
    }
    sleep "$POLL_SECONDS"
  done
  [[ "$result" == success && "$status" == 0 ]] || {
    note "the probe's tick $id failed on $want (Result=$result, status $status):" \
      "journalctl _SYSTEMD_INVOCATION_ID=$id"
    return 1
  }
  # journald can trail the unit's exit by a moment.
  for _ in 1 2 3; do
    rev="$(journalctl "_SYSTEMD_INVOCATION_ID=$id" -o cat --no-pager 2>/dev/null |
      jq -rR 'fromjson? | select(.message == "Bus health summary") | .revision' | tail -n 1)" || rev=""
    [[ -z "$rev" ]] || break
    sleep "$POLL_SECONDS"
  done
  [[ "$rev" == "$want" ]] || {
    note "the probe's tick $id reported revision ${rev:-none}, not $want"
    return 1
  }
  note "the probe's tick $id on $want: exited 0"
}

verify_all() { # each service in $verify, against what its own unit runs
  local svc
  ((${#verify[@]})) || {
    note "nothing to verify: no unit changed, and none runs live from a link that moved"
    return 0
  }
  for svc in "${verify[@]}"; do
    case "$svc" in
      "$BACKUP_UNIT") verify_backup "$(expected_of "$svc")" || return 1 ;;
      "$PROBE_UNIT") verify_probe "$(expected_of "$svc")" || return 1 ;;
      # On purpose (#110): its hourly run and co-broker-drift are its check. A
      # forced run would spend GitHub's unauthenticated budget, and on a switch
      # back the honest verdict may be a lag, which pages.
      "$DRIFT_UNIT") note "$svc: not verified by a deploy; its hourly run and co-broker-drift are its check" ;;
      *) note "$svc changed but is not verified: this deploy has no check for it" ;;
    esac
  done
}

# Broker's installed units that this release no longer has. A deploy never
# removes one; retiring a unit is the operator's.
note_units_not_in_release() {
  local path name
  for path in "$UNIT_DIR"/broker-*.service "$UNIT_DIR"/broker-*.timer; do
    [[ -f "$path" ]] || continue
    name="$(basename "$path")"
    [[ -f "$release/deploy/$name" ]] && continue
    note "$name is installed but not in $build's deploy/; it keeps running. If it should not:" \
      "sudo systemctl disable --now $name"
  done
  return 0
}

# Host configs change rarely, and installing one is a reload, a sysctl or a
# redis-server restart: by hand, in a window when it is the last. A difference is
# a note, never an install, never a failure (D9).
compare_host_configs() {
  local entry rel dest previous_dir="" told=0
  for entry in "${HOST_CONFIGS[@]}"; do
    rel="${entry%%=*}" dest="$HOST${entry#*=}"
    [[ -f "$release/deploy/$rel" ]] || continue
    if [[ ! -e "$dest" ]]; then
      note "host config deploy/$rel is not installed at $dest; install it by hand (deploy/README.md, Install)"
    elif ! cmp -s "$release/deploy/$rel" "$dest"; then
      note "host config deploy/$rel differs from $dest; install it by hand (deploy/README.md, Install)"
    fi
  done
  [[ -z "$previous_link" ]] || previous_dir="$(cd "$ROOT" 2>/dev/null && cd -P "$previous_link" 2>/dev/null && pwd)" || previous_dir=""
  for rel in "${DERIVED_CONFIGS[@]}"; do
    [[ -f "$release/deploy/$rel" ]] || continue
    if [[ -z "$previous_dir" || ! -f "$previous_dir/deploy/$rel" ]]; then
      ((told)) || note "deploy/${DERIVED_CONFIGS[0]} and deploy/${DERIVED_CONFIGS[1]} are installed" \
        "derived, so a first deploy compares them with nothing; tests/deploy compares them with the node"
      told=1
    elif ! cmp -s "$previous_dir/deploy/$rel" "$release/deploy/$rel"; then
      note "deploy/$rel changed since $(basename "$previous_dir"). A deploy never installs it:" \
        "deploy/README.md says how, redis-server rereads it only in a restart window" \
        "(docs/RESTART-WINDOW.md), and tests/deploy compares it with the node"
    fi
  done
}

link="$ROOT/live"
# The link exactly as it was, to put back on failure: one made by hand may point
# anywhere. Its last component is for comparing and for messages.
previous_link="$(readlink "$link" 2>/dev/null || true)"
previous="$(release_of)"
moved=1
[[ -n "$previous_link" && "$previous" == "$build" ]] && moved=0

stage="while switching"
swap "$link" "releases/$build" || die "cannot switch live to $build (sudo ln, mv); nothing switched"
stage="after the switch, installing units and verifying"
logger -t broker-deploy "live -> $build (was ${previous_link:-nothing})" || true
units_ok=1
keep_units=1
install_units || units_ok=0
verify=()
while read -r svc; do
  [[ -n "$svc" ]] && verify+=("$svc")
done < <(changed_services)

if ((units_ok)) && verify_all; then
  keep_units=0
  note "live is on $build"
  enable_new_timers
else
  stage="while switching back"
  if [[ -z "$previous_link" ]]; then
    # A first deploy: there was no link, so restoring it exactly is removing it.
    sudo -n rm -f "$link" || note "removing $link failed"
    back="there was no previous release, so removed live and put back the units it replaced"
  elif [[ "$previous" == "$build" ]]; then
    # The link never moved, so the units are all that changed. A hand-fixed
    # unit is the likeliest way here, and its copy kept aside the only one left.
    # None replaced (the first could not be kept aside) is nothing changed at all.
    units_replaced ||
      die "live failed on $build, which it was already running, before any unit was replaced;" \
        "nothing changed"
    back="put back the units it replaced, on $build"
  else
    back="switched back to $(served_build "$previous_link")"
    if ! swap "$link" "$previous_link"; then
      logger -t broker-deploy "live failed on $build; switching it back to $previous_link failed" || true
      dead "live failed on $build, and switching it back to $previous_link failed (sudo ln, mv):" \
        "live still runs $build. sudo ln -sfn $previous_link $link.new && sudo mv -Tf $link.new $link"
    fi
  fi
  restore_units
  keep_units=0
  stage="after switching back, verifying the old build"
  if verify_all; then
    logger -t broker-deploy "live rolled back after $build failed: $back" || true
    die "live failed on $build; $back, which is answering"
  fi
  logger -t broker-deploy "live failed on $build; $back, which is NOT answering" || true
  dead "live failed on $build; $back, which is NOT answering: journalctl -t broker-deploy -n 20"
fi
stage="while pruning"
note_units_not_in_release
compare_host_configs

# --- prune -----------------------------------------------------------------

# KEEP releases besides the one live names (the design's Q2): live is left out
# of the count, never only out of the deletion.
live_build="$(release_of)"
# shellcheck disable=SC2012 # names are 12 hex characters
ls -1t "$ROOT/releases" | { grep -vxF -- "$live_build" || :; } | tail -n +$((KEEP + 1)) | while read -r old; do
  note "pruning release $old"
  # REVISION first: a prune cut short leaves an interrupted build, never a
  # "complete" one with half its files. Verified by now: a failed prune is a note.
  { sudo -n rm -f "$ROOT/releases/$old/REVISION" && sudo -n rm -rf "${ROOT:?}/releases/$old"; } ||
    note "prune failed for $old; remove it by hand: sudo rm -rf $ROOT/releases/$old"
done
