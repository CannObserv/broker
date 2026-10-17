"""End-to-end tests for scripts/deploy.sh against a throwaway root (#106, epic #22).

Ported from CannObserv/status's harness (``tests/deploy/test_deploy.py`` at
``16102ea``, its root ledger from status#14) and CannObserv/processor's
(``tests/test_deploy.py`` at ``4b3a500``, no HTTP and a CI gate). Real git, a
temporary origin and clone; stubs for ``uv``, ``sudo``, ``systemctl``,
``journalctl``, ``stat``, ``curl`` and ``logger`` on ``PATH`` that record every
call. ``FAKE_*`` variables make them play a failing sync, a venv that no longer
runs, a backup run that fails, or a probe tick on the wrong build.

What the script must hold (the design's Q1-5, Q8, Q12, D1, D4, D7-D9):

- **Only pushed commits, and only green ones.** The commit must be on
  ``origin/main``, and its push run of ``ci.yml`` on ``main`` must be
  ``success`` with every job in it, asked of GitHub before anything is built.
  ``--skip-ci`` skips the gate, logged.
- **Root owns what the units run.** The deploy root, ``releases/``, each
  finished release, and so ``live``. The stubs play root: a ledger of what it
  owns, kept unwritable except through ``sudo``, so a write under the root
  without ``sudo`` fails here as it would on the node.
- **One immutable release per commit,** built at its final path, read-only and
  readable, ``REVISION`` written last by root. A directory without one is an
  interrupted build. A release ``live`` links is never rebuilt.
- **The wheelhouse in a release is what ``uv.lock`` names,** copied, never
  linked, from the checkout's synced one.
- **Units:** ``deploy/broker-*.{service,timer}`` installed from the release when
  they differ, one ``daemon-reload``; host configs compared, never installed;
  ``redis-server`` never touched.
- **Verify what changed** (D8): a unit whose file this deploy installed, or one
  that runs ``live`` when ``live`` moved. The probe by its next natural tick,
  never forced; the backup by one forced run. A failure switches back, units
  too, and proves the old build: exit 1 when it answers, 4 when nothing does.
"""

import fcntl
import grp
import json
import os
import pwd
import re
import shutil
import stat
import subprocess
import time
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
DEPLOY = REPO_ROOT / "scripts" / "deploy.sh"
CI_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yml"
README = REPO_ROOT / "deploy" / "README.md"

PROBE = "broker-bus-health.service"
BACKUP = "broker-backup.service"
CO_CORE_WHEEL = "co_core-0.36.0-py3-none-any.whl"
CO_CORE_SDIST = "co_core-0.36.0.tar.gz"

# A uv.lock with one wheelhouse package, as broker's has (co-core), between two
# that are not.
UV_LOCK = f"""version = 1
requires-python = ">=3.12"

[[package]]
name = "broker"
version = "0.1.0"
source = {{ editable = "." }}
dependencies = [
    {{ name = "co-core" }},
]

[[package]]
name = "co-core"
version = "0.36.0"
source = {{ registry = ".wheelhouse" }}
dependencies = [
    {{ name = "pydantic" }},
]
sdist = {{ path = "{CO_CORE_SDIST}" }}
wheels = [
    {{ path = "{CO_CORE_WHEEL}" }},
]

[[package]]
name = "pydantic"
version = "2.12.0"
source = {{ registry = "https://pypi.org/simple" }}
sdist = {{ url = "https://files.example/pydantic-2.12.0.tar.gz", hash = "sha256:00" }}
wheels = [
    {{ url = "https://files.example/pydantic-2.12.0-py3-none-any.whl", hash = "sha256:00" }},
]
"""

STUB_UV = r"""#!/usr/bin/env bash
echo "uv $PWD GAC=${GOOGLE_APPLICATION_CREDENTIALS:-} PYDL=${UV_PYTHON_DOWNLOADS:-} $*" \
  >> "$FAKE_LOG"
case "$1" in
  sync)
    [[ -n "${FAKE_SYNC_FAIL:-}" ]] && exit 1
    mkdir -p .venv/bin
    printf 'home = %s\n' "${FAKE_PYTHON_HOME:-/usr/bin}" > .venv/pyvenv.cfg
    cp "$FAKE_PYTHON" .venv/bin/python ;;
  run)
    [[ "$*" == *sync_wheelhouse.py* && -n "${FAKE_WHEELHOUSE_FAIL:-}" ]] && exit 1 ;;
esac
exit 0
"""

# The release venv's interpreter. `-c` is the import probe, which prints what
# src/broker/revision.py's read_revision() would: REVISION beside it, else dev.
STUB_PYTHON = r"""#!/usr/bin/env bash
echo "python $PWD $*" >> "$FAKE_LOG"
case "$1" in
  -m) [[ "$2" == compileall && -n "${FAKE_COMPILE_FAIL:-}" ]] && exit 1 ;;
  -c)
    [[ -n "${FAKE_BROKEN_VENV:-}" ]] && { echo "probe: no interpreter" >&2; exit 2; }
    if [[ -f REVISION && -n "${FAKE_REVISION_SAYS:-}" ]]; then
      echo "$FAKE_REVISION_SAYS"
      exit 0
    fi
    cat REVISION 2>/dev/null || echo dev ;;
esac
exit 0
"""

# Plays root (status#14). FAKE_ROOT_OWNED is the ledger of what root owns, one
# "D <dir>" (that directory) or "R <path>" (that tree) per line; the stat stub
# answers from it. What root owns is kept unwritable, and only a command run
# through here opens it, so a write under the deploy root without sudo fails.
STUB_SUDO = r"""#!/usr/bin/env bash
echo "sudo $*" >> "$FAKE_LOG"
[[ "$1" == chown && -n "${FAKE_CHOWN_FAIL:-}" ]] && exit 1
as_root() {
  local kind path opened=() rc=0
  while read -r kind path; do
    [[ -d "$path" && ! -L "$path" ]] && /bin/chmod u+w "$path" && opened+=("$path")
  done <"$FAKE_ROOT_OWNED"
  # Root needs no permission to delete a read-only tree.
  if [[ "$1" == rm && "$2" == -rf ]]; then
    for path in "${@:3}"; do [[ -e "$path" ]] && /bin/chmod -R u+w "$path"; done
  fi
  "$@" || rc=$?
  for path in "${opened[@]}"; do [[ -d "$path" ]] && /bin/chmod a-w "$path"; done
  return $rc
}
ledger() { echo "$1 $2" >>"$FAKE_ROOT_OWNED"; }
case "$1" in
  install)
    [[ -n "${FAKE_INSTALL_FAIL:-}" && "${@: -1}" == */"$FAKE_INSTALL_FAIL" ]] && exit 1
    # The ledger, not the file, says root owns it: drop `-o root` and `-g root`.
    args=()
    while (($#)); do
      if [[ ("$1" == -o || "$1" == -g) && "$2" == root ]]; then shift 2; continue; fi
      args+=("$1")
      shift
    done
    set -- "${args[@]}"
    as_root "$@" || exit
    # A directory install -d makes is root's unless it names another owner,
    # and unwritable at once, like the rest of what root owns (status CR 8).
    if [[ "$2" == -d* && " $* " != *" -o "* ]]; then
      ledger D "${@: -1}"
      /bin/chmod a-w "${@: -1}"
    fi
    exit 0 ;;
  chown)
    [[ "$2" == -R && "$3" == root:root ]] || { echo "stub sudo: chown $*?" >&2; exit 1; }
    # Read-only first, so a root-owned tree is never a writable one (#106).
    [[ -z "$(find "$4" ! -type l -perm /222 -print -quit)" ]] ||
      echo "chown-of-writable $4" >> "$FAKE_LOG"
    ledger R "$4"
    exit 0 ;;
  rm)
    as_root "$@" || exit
    for path in "${@:2}"; do
      [[ -e "$path" || -L "$path" ]] || sed -i "\#^[DR] $path\(/.*\)\{0,1\}\$#d" "$FAKE_ROOT_OWNED"
    done
    exit 0 ;;
  ln | mv | chmod | touch | tee) as_root "$@"; exit ;;
  systemctl) shift; FAKE_VIA_SUDO=1 exec systemctl "$@" ;;
esac
echo "stub sudo: $*?" >&2
exit 1
"""

# `stat -c %u -- <path>` (or %u:%g): root for what the ledger says root owns (status#14).
STUB_STAT = r"""#!/usr/bin/env bash
if [[ "$1" == -c && ( "$2" == %u || "$2" == %u:%g ) ]]; then
  path="${@: -1}"
  while read -r kind owned; do
    if [[ "$path" == "$owned" || ("$kind" == R && "$path" == "$owned"/*) ]]; then
      [[ "$2" == %u ]] && echo 0 || echo 0:0
      exit 0
    fi
  done <"$FAKE_ROOT_OWNED"
fi
exec @STAT@ "$@"
"""

# The two units, as systemd would run them: each reports the build its
# installed unit's WorkingDirectory= holds, its REVISION or else dev, the rule
# src/broker/revision.py applies. The backup's forced run writes its state
# file; the probe ticks on its own, one new invocation every FAKE_TICK_EVERY
# polls, and journalctl reads the tick the last poll saw.
STUB_SYSTEMCTL = r"""#!/usr/bin/env bash
# Through sudo, the sudo stub has logged it already.
[[ -n "${FAKE_VIA_SUDO:-}" ]] || echo "systemctl $*" >> "$FAKE_LOG"
dir="$(dirname "$FAKE_LOG")"
served() {
  local wd rev
  wd="$(sed -n 's/^WorkingDirectory=//p' "$FAKE_UNIT_DIR/$1" 2>/dev/null)"
  rev="$(cd -P "$wd" 2>/dev/null && cat REVISION 2>/dev/null)" || rev=""
  echo "${rev:-dev}"
}
case "$1" in
  show)
    unit="${@: -1}"
    if [[ "$unit" == broker-bus-health.service ]]; then
      n=$(( $(cat "$dir/probe-polls") + 1 )); echo "$n" > "$dir/probe-polls"
      tick=$(( n / ${FAKE_TICK_EVERY:-2} ))
      build="$(served broker-bus-health.service)"
      rc=0; [[ "$build" == "${FAKE_PROBE_FAIL_BUILD:-none}" ]] && rc=1
      echo "tick-$tick $build" > "$dir/probe-tick"
      result=success; ((rc == 0)) || result=exit-code
      printf 'Result=%s\nExecMainStatus=%s\nActiveState=%s\nInvocationID=tick-%s\n' \
        "$result" "$rc" "${FAKE_PROBE_STATE:-inactive}" "$tick"
    elif [[ "$unit" == broker-backup.service ]]; then
      left="$(cat "$dir/backup-busy")"
      if ((left > 0)); then
        echo $((left - 1)) > "$dir/backup-busy"
        echo activating
      else
        echo inactive
      fi
    fi ;;
  start)
    [[ "$2" == broker-backup.service ]] || exit 0
    build="$(served broker-backup.service)"
    [[ "$build" == "${FAKE_BACKUP_FAIL_BUILD:-none}" ]] && exit 1
    # A run that succeeds only while its unit holds this text: a hand fix the repo lacks.
    [[ -z "${FAKE_BACKUP_NEEDS:-}" ]] ||
      grep -qF -- "$FAKE_BACKUP_NEEDS" "$FAKE_UNIT_DIR/broker-backup.service" || exit 1
    printf '{"outcome": "%s", "revision": "%s"}\n' "${FAKE_BACKUP_OUTCOME:-uploaded}" \
      "${FAKE_BACKUP_SAYS:-$build}" > "$FAKE_BACKUP_STATE" ;;
  try-restart) [[ "$2" == "${FAKE_TRY_RESTART_FAIL:-none}" ]] && exit 1 ;;
esac
exit 0
"""

# One tick's journal, as `-o cat` prints it: a line that is not JSON, then the
# summary. FAKE_PROBE_STALE_BUILD plays a tick that reports another build.
STUB_JOURNALCTL = r"""#!/usr/bin/env bash
echo "journalctl $*" >> "$FAKE_LOG"
read -r tick build < "$(dirname "$FAKE_LOG")/probe-tick"
[[ "$1" == "_SYSTEMD_INVOCATION_ID=$tick" ]] || exit 0
[[ "$build" == "${FAKE_PROBE_STALE_BUILD:-none}" ]] && build=stale
echo "Starting broker-bus-health.service..."
printf '{"level": "INFO", "message": "Bus health summary", "finding_count": 0, '
printf '"revision": "%s"}\n' "$build"
"""

# GitHub's Actions API answers from <tmp>/ci: runs-<n>.json in turn, the last
# repeating (a run that finishes between polls), and jobs-<id>.json. With none
# written, one green push run on main for whatever commit was asked about.
# FAKE_CI_ERROR is GitHub refusing; as real curl does, only --fail-with-body
# passes its message on.
STUB_CURL = r"""#!/usr/bin/env bash
url="${@: -1}"
echo "curl $url" >> "$FAKE_LOG"
ci="$(dirname "$FAKE_LOG")/ci"
if [[ -n "${FAKE_CI_ERROR:-}" ]]; then
  [[ " $* " == *" --fail-with-body "* ]] && echo "{\"message\":\"$FAKE_CI_ERROR\"}"
  exit 22
fi
case "$url" in
  */jobs*)
    id="${url#*/actions/runs/}"
    id="${id%%/*}"
    cat "$ci/jobs-$id.json" 2>/dev/null ||
      echo '{"jobs":[{"name":"lint + test","status":"completed","conclusion":"success"}]}' ;;
  *)
    sha="${url#*head_sha=}"
    sha="${sha%%&*}"
    n="$(cat "$ci/polls")"
    echo $((n + 1)) > "$ci/polls"
    answers=("$ci"/runs-*.json)
    if [[ -e "${answers[0]}" ]]; then
      last=$((${#answers[@]} - 1))
      cat "$ci/runs-$((n < last ? n : last)).json"
    else
      echo "{\"workflow_runs\":[{\"id\":1,\"head_sha\":\"$sha\",\"event\":\"push\",
        \"head_branch\":\"main\",\"status\":\"completed\",\"conclusion\":\"success\",
        \"created_at\":\"2026-10-01T00:00:00Z\",\"html_url\":\"https://github.test/runs/1\"}]}"
    fi ;;
esac
"""

# FAKE_RM_FAIL fails the recursive delete, as a real prune failure would.
STUB_RM = r"""#!/usr/bin/env bash
[[ -n "${FAKE_RM_FAIL:-}" && "$1" == -rf ]] && exit 1
exec /bin/rm "$@"
"""

STUB_LOGGER = r"""#!/usr/bin/env bash
echo "logger $*" >> "$FAKE_LOG"
"""

BOT = {
    "GIT_AUTHOR_NAME": "github-actions[bot]",
    "GIT_AUTHOR_EMAIL": "41898282+github-actions[bot]@users.noreply.github.com",
    "GIT_COMMITTER_NAME": "github-actions[bot]",
    "GIT_COMMITTER_EMAIL": "41898282+github-actions[bot]@users.noreply.github.com",
}


def ci_run(
    run_id: int,
    sha: str,
    *,
    event: str = "push",
    branch: str = "main",
    status: str = "completed",
    conclusion: str = "success",
    created: str = "2026-10-01T00:00:00Z",
) -> dict:
    """One workflow run as GitHub's Actions API lists it."""
    return {
        "id": run_id,
        "head_sha": sha,
        "event": event,
        "head_branch": branch,
        "status": status,
        "conclusion": conclusion if status == "completed" else None,
        "created_at": created,
        "html_url": f"https://github.test/runs/{run_id}",
    }


def git(cwd: Path, *args: str, env: dict | None = None) -> str:
    return subprocess.run(
        ["git", *args],
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
        env={**os.environ, **(env or {})},
    ).stdout.strip()


def unit(name: str, workdir: Path | str, version: str = "v1") -> str:
    kind = "Timer" if name.endswith(".timer") else "Service"
    lines = [f"[{kind}]", f"# {name} {version}"]
    if kind == "Service":
        lines.append(f"WorkingDirectory={workdir}")
    return "\n".join(lines) + "\n"


class World:
    """A temp origin, clone, deploy root, host tree and stubbed PATH.

    By default it has C2's shape: both units installed, each running the dev
    checkout, and every release's units equal to the installed ones.
    """

    def __init__(self, tmp: Path) -> None:
        self.tmp = tmp
        self.origin = tmp / "origin.git"
        self.checkout = tmp / "checkout"
        self.root = tmp / "srv"
        self.host = tmp / "host"  # / for units and host configs
        self.units = self.host / "etc" / "systemd" / "system"
        self.env_file = tmp / "broker.env"
        self.backup_state = tmp / "backup-state.json"
        self.log = tmp / "calls.log"
        self.stubs = tmp / "stubs"
        self.ci = tmp / "ci"
        self.owned = tmp / "root-owned"  # the sudo and stat stubs' ledger
        for d in (self.root, self.stubs, self.ci):
            d.mkdir()
        self.units.mkdir(parents=True)
        self.log.touch()
        # As the runbook makes it: root's, so only sudo writes in it.
        self.owned.write_text(f"D {self.root}\n")
        self.root.chmod(0o555)
        self.env_file.write_text(
            "BROKER_REDIS_URL=redis://brokeradmin@localhost:6379/0\n"
            "GOOGLE_APPLICATION_CREDENTIALS=/etc/broker/co-pypi-reader.json\n"
        )

        git(tmp, "init", "-q", "--bare", "-b", "main", str(self.origin))
        git(tmp, "clone", "-q", str(self.origin), str(self.checkout))
        git(self.checkout, "config", "user.email", "t@example.com")
        git(self.checkout, "config", "user.name", "t")
        self.dev_units = self.units_on(self.checkout)
        self.live_units = self.units_on(self.root / "live")
        self.write_tree(self.checkout, self.dev_units)
        (self.checkout / "uv.lock").write_text(UV_LOCK)
        (self.checkout / ".wheelhouse").mkdir()
        (self.checkout / ".wheelhouse" / ".gitkeep").touch()
        git(self.checkout, "add", "-A")
        self.main = [self.commit(f"main-{i}") for i in range(3)]
        git(self.checkout, "push", "-q", "origin", "main")
        git(self.checkout, "switch", "-q", "-c", "feature")
        self.feature = self.commit("feature")
        git(self.checkout, "push", "-q", "origin", "feature")
        git(self.checkout, "switch", "-q", "main")
        self.unpushed = self.commit("unpushed")
        self.install(self.dev_units)

        # The synced wheelhouse: what uv.lock names, and one wheel it does not.
        # The wheel is a hardlink, as `uv sync` and the dev venvs leave them.
        wheelhouse = self.checkout / ".wheelhouse"
        (self.tmp / "cache-wheel").write_bytes(b"wheel")
        os.link(self.tmp / "cache-wheel", wheelhouse / CO_CORE_WHEEL)
        (wheelhouse / CO_CORE_SDIST).write_bytes(b"sdist")
        (wheelhouse / CO_CORE_SDIST).chmod(0o600)  # as the sync leaves them
        (wheelhouse / "other-9.9.9-py3-none-any.whl").write_bytes(b"other")

        scripts = self.checkout / "scripts"
        scripts.mkdir()
        shutil.copy(DEPLOY, scripts / "deploy.sh")

        for name, body in (
            ("uv", STUB_UV),
            ("sudo", STUB_SUDO),
            ("curl", STUB_CURL),
            ("logger", STUB_LOGGER),
            ("systemctl", STUB_SYSTEMCTL),
            ("journalctl", STUB_JOURNALCTL),
            ("rm", STUB_RM),
            ("stat", STUB_STAT.replace("@STAT@", shutil.which("stat") or "/usr/bin/stat")),
            ("fake-python", STUB_PYTHON),
        ):
            stub = self.stubs / name
            stub.write_text(body)
            stub.chmod(0o755)

    @staticmethod
    def units_on(workdir: Path) -> dict[str, str]:
        return {
            name: unit(name, workdir)
            for name in (
                "broker-bus-health.service",
                "broker-bus-health.timer",
                "broker-backup.service",
                "broker-backup.timer",
            )
        }

    @staticmethod
    def write_tree(checkout: Path, units: dict[str, str | None]) -> None:
        for name, body in units.items():
            path = checkout / "deploy" / name
            if body is None:
                path.unlink()
                continue
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(body)

    def commit(self, label: str, env: dict | None = None) -> str:
        (self.checkout / "version.txt").write_text(label)
        git(self.checkout, "add", "-A")
        git(self.checkout, "commit", "-q", "-m", label, env=env)
        return git(self.checkout, "rev-parse", "HEAD")

    def install(self, units: dict[str, str]) -> None:
        """Units as an operator installed them before this deploy."""
        for name, body in units.items():
            (self.units / name).write_text(body)

    def run(self, *args: str, umask: int | None = None, **fake: str) -> subprocess.CompletedProcess:
        env = {
            "PATH": f"{self.stubs}:{os.environ['PATH']}",
            "HOME": str(self.tmp),
            "BROKER_DEPLOY_ROOT": str(self.root),
            "BROKER_DEPLOY_HOST": str(self.host),
            "BROKER_DEPLOY_ENV_FILE": str(self.env_file),
            "BROKER_DEPLOY_BACKUP_STATE": str(self.backup_state),
            "BROKER_DEPLOY_PROBE_WAIT_SECONDS": "10",
            "BROKER_DEPLOY_BACKUP_WAIT_SECONDS": "10",
            "BROKER_DEPLOY_POLL_SECONDS": "0",
            "FAKE_LOG": str(self.log),
            "FAKE_ROOT_OWNED": str(self.owned),
            "FAKE_UNIT_DIR": str(self.units),
            "FAKE_BACKUP_STATE": str(self.backup_state),
            "FAKE_PYTHON": str(self.stubs / "fake-python"),
            **fake,
        }
        # Per run, not per world (status CR 19).
        (self.tmp / "backup-busy").write_text(fake.get("FAKE_BACKUP_BUSY", "0"))
        (self.tmp / "probe-polls").write_text("0")
        (self.tmp / "probe-tick").write_text("none none\n")
        (self.ci / "polls").write_text("0")
        argv = [str(self.checkout / "scripts" / "deploy.sh"), *args]
        if umask is not None:
            argv = ["bash", "-c", f'umask {umask:03o}; exec "$0" "$@"', *argv]
        return subprocess.run(
            argv, cwd=self.tmp, env=env, capture_output=True, text=True, timeout=120
        )

    def build(self, sha: str) -> str:
        return git(self.checkout, "rev-parse", "--short=12", sha)

    def release(self, sha: str) -> Path:
        return self.root / "releases" / self.build(sha)

    def push(self, units: dict[str, str | None] | None = None, env: dict | None = None) -> str:
        """Commit on top of origin's main and push it. The checkout's main holds
        an unpushed commit, so this happens in a clone of its own."""
        pusher = self.tmp / "pusher"
        if not pusher.exists():
            git(self.tmp, "clone", "-q", str(self.origin), str(pusher))
            git(pusher, "config", "user.email", "t@example.com")
            git(pusher, "config", "user.name", "t")
        git(pusher, "fetch", "-q", "origin")
        git(pusher, "switch", "-q", "-C", "main", "origin/main")
        self.write_tree(pusher, units or {})
        git(pusher, "add", "-A")
        git(pusher, "commit", "-q", "--allow-empty", "-m", "pushed", env=env)
        git(pusher, "push", "-q", "origin", "main")
        git(self.checkout, "fetch", "-q", "origin")
        sha = git(pusher, "rev-parse", "HEAD")
        self.main.append(sha)
        return sha

    def go_live(self) -> str:
        """C3 and C4 done: both units run the release live names. Returns the
        commit that put them there."""
        sha = self.push(self.live_units)
        self.install(self.live_units)
        return sha

    def installed(self, name: str) -> str | None:
        path = self.units / name
        return path.read_text() if path.exists() else None

    def target(self) -> str | None:
        link = self.root / "live"
        return os.readlink(link) if link.is_symlink() else None

    def ci_answers(self, *answers: list[dict] | str) -> None:
        """What GitHub lists as the commit's runs, poll by poll; the last repeats."""
        for n, runs in enumerate(answers):
            body = runs if isinstance(runs, str) else json.dumps({"workflow_runs": runs})
            (self.ci / f"runs-{n}.json").write_text(body)

    def ci_jobs(self, run_id: int, **conclusions: str) -> None:
        """A run's jobs by conclusion; ``lint + test`` left out is green."""
        jobs = {
            "lint + test": "success",
            **{k.replace("_", " "): v for k, v in conclusions.items()},
        }
        if "lint" in conclusions:
            jobs["lint + test"] = jobs.pop("lint")
        body = [
            {"name": name, "status": "completed", "conclusion": conclusion}
            for name, conclusion in jobs.items()
            if conclusion != "absent"
        ]
        (self.ci / f"jobs-{run_id}.json").write_text(json.dumps({"jobs": body}))

    def calls(self) -> list[str]:
        return self.log.read_text().splitlines()

    def root_owned(self, path: Path) -> bool:
        """Whether the ledger says root owns ``path``, as the stat stub answers."""
        for line in self.owned.read_text().splitlines():
            kind, owned = line.split(" ", 1)
            if str(path) == owned or (kind == "R" and str(path).startswith(owned + "/")):
                return True
        return False

    def disown(self, path: Path) -> None:
        """``path`` is exedev's again: handed back by hand."""
        lines = self.owned.read_text().splitlines()
        self.owned.write_text("".join(f"{x}\n" for x in lines if x.split(" ", 1)[1] != str(path)))

    def as_root(self):
        """A hand edit under the deploy root, done with sudo as an operator would."""
        return _AsRoot(self)

    def github_calls(self) -> list[str]:
        return [c for c in self.calls() if c.startswith("curl https://api.github.com/")]

    def reset_log(self) -> None:
        self.log.write_text("")


class _AsRoot:
    def __init__(self, world: World) -> None:
        self.world = world
        self.dirs: list[Path] = []

    def __enter__(self) -> None:
        paths = [Path(x.split(" ", 1)[1]) for x in self.world.owned.read_text().splitlines()]
        self.dirs = [d for d in paths if d.is_dir() and not d.is_symlink()]
        for d in self.dirs:
            d.chmod(d.stat().st_mode | stat.S_IWUSR)

    def __exit__(self, *exc) -> None:
        for d in self.dirs:
            if d.is_dir():
                d.chmod(d.stat().st_mode & ~0o222)


@pytest.fixture
def world(tmp_path):
    w = World(tmp_path)
    yield w
    # The root and releases are read-only; make them deletable for tmp_path's cleanup.
    if w.root.exists():
        for path in [w.root, *w.root.rglob("*")]:
            if not path.is_symlink():
                path.chmod(path.stat().st_mode | stat.S_IWUSR)


def assert_ok(result: subprocess.CompletedProcess) -> None:
    assert result.returncode == 0, result.stdout + result.stderr


def index_of(calls: list[str], needle: str) -> int:
    return next(i for i, call in enumerate(calls) if needle in call)


def syncs(world: World) -> list[str]:
    return [c for c in world.calls() if c.startswith("uv ") and " sync " in c]


def installs(calls: list[str]) -> list[str]:
    """The unit names `sudo install` wrote, in order; not the release directories."""
    return [Path(c.split()[-1]).name for c in calls if c.startswith("sudo install -m 644 ")]


def forced_backups(world: World) -> list[str]:
    return [c for c in world.calls() if c == f"sudo systemctl start {BACKUP}"]


def journal_reads(world: World) -> list[str]:
    return [c for c in world.calls() if c.startswith("journalctl ")]


def test_script_exists_and_is_executable():
    assert DEPLOY.is_file()
    assert DEPLOY.stat().st_mode & stat.S_IXUSR


class TestADeploy:
    def test_puts_origin_main_on_live(self, world):
        assert_ok(world.run())
        build = world.build(world.main[-1])
        assert world.target() == f"releases/{build}"
        assert (world.release(world.main[-1]) / "REVISION").read_text().strip() == build

    def test_the_release_is_the_commit_and_nothing_else(self, world):
        """No .git, no untracked files: nothing in a release can drift."""
        (world.checkout / "untracked.txt").write_text("x")
        assert_ok(world.run())
        release = world.release(world.main[-1])
        assert (release / "version.txt").read_text() == "main-2"
        assert not (release / ".git").exists()
        assert not (release / "untracked.txt").exists()
        assert not (release / "scripts" / "deploy.sh").exists(), "untracked in this world"

    def test_the_release_is_read_only(self, world):
        assert_ok(world.run())
        release = world.release(world.main[-1])
        for path in [release, *release.rglob("*")]:
            if not path.is_symlink():
                assert not path.stat().st_mode & 0o222, path

    def test_the_release_is_readable_by_every_user_under_any_umask(self, world):
        """The probe runs as exedev, not root: under umask 077 the archive and
        the venv come out 0600/0700, and the synced wheels are 0600 anyway."""
        assert_ok(world.run(umask=0o077))
        release = world.release(world.main[-1])
        for path in [release, *release.rglob("*")]:
            if path.is_symlink():
                continue
            mode = path.stat().st_mode
            assert mode & 0o444 == 0o444, path
            if path.is_dir() or mode & stat.S_IXUSR:
                assert mode & 0o111 == 0o111, path

    def test_the_build_is_synced_locked_no_dev_copied_on_the_system_python(self, world):
        assert_ok(world.run())
        (sync,) = syncs(world)
        for flag in (
            "--locked",
            "--no-dev",
            "--compile-bytecode",
            "--link-mode copy",
            "--python /usr/bin/python3.12",
        ):
            assert flag in sync, flag
        assert "PYDL=never" in sync
        assert "--no-editable" not in sync, "REVISION is read beside src/ (#105 CR 2)"
        assert sync.startswith(f"uv {world.release(world.main[-1])} ")

    def test_the_build_runs_in_this_order(self, world):
        assert_ok(world.run())
        release = world.release(world.main[-1])
        me, group = pwd.getpwuid(os.getuid()).pw_name, grp.getgrgid(os.getgid()).gr_name
        calls = world.calls()
        steps = [
            calls.index(f"sudo install -d -m 755 -o {me} -g {group} {release}"),
            index_of(calls, "sync_wheelhouse.py"),
            index_of(calls, " sync --locked "),
            index_of(calls, " -m compileall -q src"),
            index_of(calls, f"python {release} -c "),
            calls.index(f"sudo chown -R root:root {release}"),
            calls.index(f"sudo tee {release}/REVISION"),
            calls.index(f"sudo chmod 444 {release}/REVISION"),
            max(i for i, c in enumerate(calls) if c.startswith(f"python {release} -c ")),
            index_of(calls, "logger -t broker-deploy live -> "),
        ]
        assert steps == sorted(steps), steps
        assert not [c for c in calls if c.startswith("chown-of-writable")], "chmod before chown"

    def test_bytecode_is_compiled_for_src_too(self, world):
        """--compile-bytecode reaches site-packages alone; the editable src/ is not there."""
        assert_ok(world.run())
        (compiled,) = [c for c in world.calls() if "-m compileall" in c]
        assert compiled.endswith("-m compileall -q src")

    def test_the_import_probe_names_both_modules_and_co_core(self, world):
        assert_ok(world.run())
        probe = next(c for c in world.calls() if c.startswith("python ") and " -c " in c)
        for module in ("src.broker.bus_health", "src.broker.backup", "co_core"):
            assert module in probe, module
        assert "read_revision" in probe

    def test_the_deploy_is_recorded_in_the_journal(self, world):
        assert_ok(world.run())
        build = world.build(world.main[-1])
        logged = [c for c in world.calls() if c.startswith("logger ")]
        assert f"logger -t broker-deploy live -> {build} (was nothing)" in logged

    def test_a_second_deploy_journals_what_live_was(self, world):
        assert_ok(world.run(world.main[0]))
        assert_ok(world.run())
        old, new = world.build(world.main[0]), world.build(world.main[-1])
        assert f"logger -t broker-deploy live -> {new} (was releases/{old})" in world.calls()


class TestTheC2Shape:
    """The first release: both units still run ~/broker, and the release's units
    equal the installed ones. Nothing is installed, nothing verified."""

    def test_no_unit_is_installed_and_nothing_reloads(self, world):
        result = world.run()
        assert_ok(result)
        assert not installs(world.calls())
        assert not [c for c in world.calls() if "daemon-reload" in c]
        for name, body in world.dev_units.items():
            assert world.installed(name) == body

    def test_nothing_is_verified_or_started(self, world):
        result = world.run()
        assert_ok(result)
        assert not forced_backups(world)
        assert not journal_reads(world)
        assert not [c for c in world.calls() if "systemctl" in c and PROBE in c]
        assert "nothing to verify" in result.stderr

    def test_redis_is_never_touched(self, world):
        world.go_live()
        assert_ok(world.run())
        assert not [c for c in world.calls() if "redis-server" in c]


class TestWheelhouse:
    """D4/B3: synced at deploy time in the checkout, and only what uv.lock
    names enters a release, as copies."""

    def test_the_checkouts_sync_runs_with_the_env_files_key(self, world):
        result = world.run(GOOGLE_APPLICATION_CREDENTIALS="/from/the/shell.json")
        assert_ok(result)
        (sync,) = [c for c in world.calls() if "sync_wheelhouse.py" in c]
        assert sync.startswith(f"uv {world.checkout} ")
        assert "GAC=/etc/broker/co-pypi-reader.json " in sync, "the file's, never the shell's"
        assert (
            "run --no-project --with google-cloud-storage>=2,<4 python scripts/sync_wheelhouse.py"
            in sync
        )

    def test_a_key_missing_from_the_env_file_is_refused(self, world):
        world.env_file.write_text("BROKER_REDIS_URL=redis://brokeradmin@localhost:6379/0\n")
        result = world.run(GOOGLE_APPLICATION_CREDENTIALS="/from/the/shell.json")
        assert result.returncode == 1
        assert "GOOGLE_APPLICATION_CREDENTIALS" in result.stderr
        assert str(world.env_file) in result.stderr
        assert world.target() is None
        assert not syncs(world)

    def test_an_unreadable_env_file_is_refused(self, world):
        world.env_file.unlink()
        result = world.run()
        assert result.returncode == 1
        assert str(world.env_file) in result.stderr
        assert world.target() is None

    def test_an_env_file_that_will_not_source_is_refused_with_why(self, world):
        """CR 2: set -e does not reach into a command substitution, so the lines
        before the error were taken as the file, in silence."""
        world.env_file.write_text("GOOGLE_APPLICATION_CREDENTIALS=/k.json\nif then\n")
        result = world.run()
        assert result.returncode == 1
        assert f"cannot read {world.env_file}" in result.stderr
        assert world.target() is None

    def test_only_the_files_the_lock_names_are_copied(self, world):
        assert_ok(world.run())
        wheelhouse = world.release(world.main[-1]) / ".wheelhouse"
        assert sorted(p.name for p in wheelhouse.iterdir()) == sorted(
            [".gitkeep", CO_CORE_SDIST, CO_CORE_WHEEL]
        )

    def test_the_copies_are_files_never_links(self, world):
        assert (world.checkout / ".wheelhouse" / CO_CORE_WHEEL).stat().st_nlink == 2
        assert_ok(world.run())
        wheelhouse = world.release(world.main[-1]) / ".wheelhouse"
        for name in (CO_CORE_WHEEL, CO_CORE_SDIST):
            path = wheelhouse / name
            assert not path.is_symlink(), name
            assert path.stat().st_nlink == 1, name
            assert path.read_bytes() == (world.checkout / ".wheelhouse" / name).read_bytes()

    def test_the_copies_are_made_before_the_sync_needs_them(self, world):
        assert_ok(world.run())
        calls = world.calls()
        assert index_of(calls, "sync_wheelhouse.py") < index_of(calls, " sync --locked ")

    def test_a_locked_file_the_sync_did_not_bring_fails_the_build(self, world):
        (world.checkout / ".wheelhouse" / CO_CORE_SDIST).unlink()
        result = world.run()
        assert result.returncode == 1
        assert CO_CORE_SDIST in result.stderr
        assert "nothing switched" in result.stderr
        assert world.target() is None
        assert not syncs(world)
        assert not (world.release(world.main[-1]) / "REVISION").exists()

    def test_a_failed_sync_fails_the_build(self, world):
        result = world.run(FAKE_WHEELHOUSE_FAIL="1")
        assert result.returncode == 1
        assert "wheelhouse" in result.stderr
        assert world.target() is None
        assert not syncs(world)

    def test_a_reused_release_syncs_nothing(self, world):
        assert_ok(world.run())
        world.reset_log()
        assert_ok(world.run())
        assert not [c for c in world.calls() if c.startswith("uv ")]


class TestWhatMayBeDeployed:
    def test_an_unpushed_commit_is_refused(self, world):
        result = world.run(world.unpushed)
        assert result.returncode == 1
        assert "origin/main" in result.stderr
        assert not (world.root / "releases").exists()

    def test_a_branch_commit_is_refused(self, world):
        result = world.run(world.feature)
        assert result.returncode == 1
        assert world.target() is None

    def test_an_older_main_commit_may_be_deployed(self, world):
        """How a rollback is spelled: deploy the previous build again."""
        assert_ok(world.run(world.build(world.main[0])))
        assert world.target() == f"releases/{world.build(world.main[0])}"

    def test_an_unknown_ref_is_refused(self, world):
        result = world.run("no-such-ref")
        assert result.returncode == 1
        assert "no-such-ref" in result.stderr


# A wait no stubbed poll can outlast, polled at once: SECONDS ticks on the
# wall clock's second, so a 1 s wait can be spent before its first answer.
PROMPT_POLLS = {"BROKER_DEPLOY_CI_WAIT_SECONDS": "60", "BROKER_DEPLOY_CI_POLL_SECONDS": "0"}


def assert_nothing_happened(world: World) -> None:
    """Refused before the build: no release, no unit touched, nothing sudoed."""
    assert not (world.root / "releases").exists()
    assert world.target() is None
    assert not [c for c in world.calls() if c.startswith(("uv ", "sudo "))]


class TestTheCIGate:
    """D7: the commit's push run of ci.yml on main, every job success."""

    @staticmethod
    def other_and_push(world: World, event: str = "workflow_dispatch", branch: str = "feature"):
        sha = world.main[-1]
        later, earlier = "2026-10-01T02:00:00Z", "2026-10-01T01:00:00Z"
        other = ci_run(8, sha, event=event, branch=branch, created=later)
        world.ci_answers([other, ci_run(7, sha, created=earlier)])

    OTHER_RUNS = pytest.mark.parametrize(
        ("event", "branch"),
        [("workflow_dispatch", "main"), ("pull_request", "main"), ("push", "feature")],
        ids=["dispatch-on-main", "pr-from-main", "push-elsewhere"],
    )

    def test_asks_about_this_commit_as_pushed_to_main(self, world):
        assert_ok(world.run())
        runs, jobs = world.github_calls()
        assert "/repos/CannObserv/broker/actions/workflows/ci.yml/runs?" in runs
        for param in (f"head_sha={world.main[-1]}", "event=push", "branch=main"):
            assert param in runs
        assert jobs.endswith("/repos/CannObserv/broker/actions/runs/1/jobs?per_page=100")

    def test_a_pass_is_journaled_with_its_run_before_anything_is_built(self, world):
        assert_ok(world.run())
        calls = world.calls()
        passed = index_of(calls, "CI passed")
        build = world.build(world.main[-1])
        assert (
            calls[passed]
            == f"logger -t broker-deploy CI passed for {build}: https://github.test/runs/1"
        )
        assert passed < index_of(calls, "sudo ")

    def test_the_lint_and_test_job_failing_is_refused_by_name(self, world):
        world.ci_answers([ci_run(7, world.main[-1])])
        world.ci_jobs(7, lint="failure")
        result = world.run()
        assert result.returncode == 1
        assert "lint + test (failure)" in result.stderr
        assert "https://github.test/runs/7" in result.stderr
        assert_nothing_happened(world)

    @pytest.mark.parametrize("conclusion", ["skipped", "cancelled", "absent"])
    def test_a_job_that_did_not_succeed_is_not_a_pass(self, world, conclusion):
        """A skipped job leaves the run's own conclusion 'success'."""
        world.ci_answers([ci_run(7, world.main[-1])])
        world.ci_jobs(7, lint=conclusion)
        result = world.run()
        assert result.returncode == 1
        assert "lint + test (" in result.stderr
        assert_nothing_happened(world)

    def test_a_failed_job_the_checkout_does_not_know_is_refused(self, world):
        """CI_JOBS comes from the checkout, which may predate a job; every job counts."""
        world.ci_answers([ci_run(7, world.main[-1], conclusion="failure")])
        world.ci_jobs(7, e2e="failure")
        result = world.run()
        assert result.returncode == 1
        assert "e2e (failure)" in result.stderr
        assert_nothing_happened(world)

    def test_a_cancelled_run_says_re_run_it(self, world):
        """A pending push run on main is replaced when a newer push queues
        behind a running one; it lists no jobs, and has nothing to fix."""
        world.ci_answers([ci_run(7, world.main[-1], conclusion="cancelled")])
        world.ci_jobs(7, lint="absent")
        result = world.run()
        assert result.returncode == 1
        assert "run concluded cancelled" in result.stderr
        assert "lint + test (not in the run)" in result.stderr
        assert "re-run it" in result.stderr
        assert "fix it on main" not in result.stderr
        assert_nothing_happened(world)

    def test_a_run_that_did_not_conclude_success_is_refused_whatever_its_jobs(self, world):
        world.ci_answers([ci_run(7, world.main[-1], conclusion="failure")])
        world.ci_jobs(7)
        result = world.run()
        assert result.returncode == 1
        assert "run concluded failure" in result.stderr
        assert_nothing_happened(world)

    def test_a_run_still_going_when_the_wait_ends_is_refused(self, world):
        world.ci_answers([ci_run(7, world.main[-1], status="in_progress")])
        result = world.run(BROKER_DEPLOY_CI_WAIT_SECONDS="0")
        assert result.returncode == 1
        assert "in_progress" in result.stderr
        assert "https://github.test/runs/7" in result.stderr
        assert not [c for c in world.github_calls() if "/jobs" in c]
        assert_nothing_happened(world)

    def test_a_run_that_finishes_within_the_wait_is_deployed(self, world):
        sha = world.main[-1]
        world.ci_answers([ci_run(7, sha, status="in_progress")], [ci_run(7, sha)])
        started = time.monotonic()
        result = world.run(**PROMPT_POLLS)
        assert_ok(result)
        assert time.monotonic() - started < 30, "polled every 30 s, not as told"
        waiting = next(line for line in result.stderr.splitlines() if "waiting for CI" in line)
        assert "https://github.test/runs/7" in waiting
        assert len([c for c in world.github_calls() if "/runs?" in c]) == 2

    def test_the_tip_just_pushed_waits_for_its_run_to_appear(self, world):
        world.ci_answers([], [ci_run(7, world.main[-1])])
        assert_ok(world.run(**PROMPT_POLLS))

    def test_the_tip_with_no_run_when_the_wait_ends_is_refused(self, world):
        world.ci_answers([])
        result = world.run(BROKER_DEPLOY_CI_WAIT_SECONDS="0")
        assert result.returncode == 1
        assert "no CI run" in result.stderr
        assert "--skip-ci" in result.stderr
        assert_nothing_happened(world)

    def test_a_commit_behind_the_tip_with_no_run_is_refused_at_once(self, world):
        """GitHub runs CI on the newest commit of a push only: it never will."""
        world.ci_answers([])
        result = world.run(world.main[0])  # the default wait, 600 s
        assert result.returncode == 1
        assert "newest commit of each push" in result.stderr
        assert "not be listed yet" in result.stderr
        assert len(world.github_calls()) == 1
        assert_nothing_happened(world)

    def test_a_commit_a_workflow_pushed_is_refused_at_once_naming_its_parent(self, world):
        """The weekly context measurement is pushed with GITHUB_TOKEN, which
        never triggers a workflow: c5a0e06 (2026-10-15) has no run, and never will."""
        parent = world.main[-1]
        world.push(env=BOT)
        world.ci_answers([])
        result = world.run()  # the default wait, 600 s
        assert result.returncode == 1
        assert "pushed by a workflow" in result.stderr
        assert f"scripts/deploy.sh {world.build(parent)}" in result.stderr
        assert "--skip-ci" in result.stderr
        assert len(world.github_calls()) == 1
        assert_nothing_happened(world)

    def test_a_workflow_pushed_commit_with_a_run_is_gated_as_any(self, world):
        world.push(env=BOT)
        assert_ok(world.run())

    def test_a_run_for_another_commit_is_not_this_ones(self, world):
        world.ci_answers([ci_run(7, world.main[0])])
        result = world.run(BROKER_DEPLOY_CI_WAIT_SECONDS="0")
        assert result.returncode == 1
        assert "no CI run" in result.stderr

    @OTHER_RUNS
    def test_only_the_push_run_on_main_decides(self, world, event, branch):
        self.other_and_push(world, event, branch)
        world.ci_jobs(8, lint="failure")
        world.ci_jobs(7)
        assert_ok(world.run())
        assert world.github_calls()[-1].endswith("/actions/runs/7/jobs?per_page=100")

    @OTHER_RUNS
    def test_a_green_run_of_another_kind_does_not_pass_a_failed_push_run(
        self, world, event, branch
    ):
        self.other_and_push(world, event, branch)
        world.ci_jobs(8)
        world.ci_jobs(7, lint="failure")
        result = world.run()
        assert result.returncode == 1
        assert "lint + test (failure)" in result.stderr

    def test_of_two_push_runs_on_main_the_newest_decides(self, world):
        sha = world.main[-1]
        world.ci_answers(
            [
                ci_run(7, sha, created="2026-10-01T01:00:00Z"),
                ci_run(9, sha, created="2026-10-01T03:00:00Z"),
                ci_run(8, sha, created="2026-10-01T02:00:00Z"),
            ]
        )
        world.ci_jobs(7, lint="failure")
        world.ci_jobs(8, lint="failure")
        world.ci_jobs(9)
        assert_ok(world.run())
        assert world.github_calls()[-1].endswith("/actions/runs/9/jobs?per_page=100")

    def test_github_refusing_refuses_the_deploy_with_its_message(self, world):
        """Unauthenticated: 60 requests an hour per address."""
        result = world.run(FAKE_CI_ERROR="API rate limit exceeded for 192.0.2.1.")
        assert result.returncode == 1
        assert "API rate limit exceeded for 192.0.2.1." in result.stderr
        assert "--skip-ci" in result.stderr
        assert_nothing_happened(world)

    @pytest.mark.parametrize("body", ["<html>unicorn</html>", '{"total_count":0}', "", " \n"])
    def test_a_runs_answer_that_is_not_what_was_expected_refuses(self, world, body):
        world.ci_answers(body)
        result = world.run(BROKER_DEPLOY_CI_WAIT_SECONDS="0")
        assert result.returncode == 1
        assert "not the JSON expected" in result.stderr
        assert_nothing_happened(world)

    @pytest.mark.parametrize("body", ["<html>unicorn</html>", '{"total_count":0}', "", " \n"])
    def test_a_jobs_answer_that_is_not_what_was_expected_refuses(self, world, body):
        world.ci_answers([ci_run(7, world.main[-1])])
        (world.ci / "jobs-7.json").write_text(body)
        result = world.run()
        assert result.returncode == 1
        assert "not the JSON expected" in result.stderr
        assert_nothing_happened(world)

    def test_skip_ci_deploys_without_asking_and_journals_it_first(self, world):
        world.ci_answers([ci_run(7, world.main[-1])])
        world.ci_jobs(7, lint="failure")
        assert_ok(world.run("--skip-ci"))
        assert not world.github_calls()
        calls = world.calls()
        skipped = index_of(calls, "--skip-ci")
        build = world.build(world.main[-1])
        assert calls[skipped] == f"logger -t broker-deploy CI not checked for {build} (--skip-ci)"
        assert skipped < index_of(calls, "sudo ")

    def test_help_names_skip_ci_and_touches_nothing(self, world):
        result = world.run("--help")
        assert_ok(result)
        assert "--skip-ci" in result.stdout
        assert not world.calls()

    def test_the_jobs_the_gate_requires_are_ones_ci_yml_runs_on_a_push_to_main(self):
        """A job renamed in ci.yml must change the gate too. Its name has
        spaces, which the templates' split(" ") would have broken."""
        required = re.search(r"^CI_JOBS=\((.*)\)$", DEPLOY.read_text(), re.M)
        assert required, "deploy.sh names its jobs in CI_JOBS=(...)"
        names = re.findall(r'"([^"]+)"', required.group(1))
        assert names, "each job quoted"
        workflow = CI_WORKFLOW.read_text()
        jobs = workflow.split("\njobs:\n", 1)[1]
        declared = re.findall(r"^  [\w-]+:\n(?:    .*\n)*?    name: (.+)$", jobs, re.M)
        assert set(names) <= set(declared), (names, declared)
        assert "strategy:" not in jobs and "uses: ./" not in jobs, "a matrix renames the job"
        assert re.search(r"^on:\n  push:\n    branches: \[main\]$", workflow, re.M)


class TestReleases:
    def test_a_built_release_is_reused(self, world):
        assert_ok(world.run(world.main[0]))
        assert_ok(world.run(world.main[-1]))
        world.reset_log()
        result = world.run(world.main[0])
        assert_ok(result)
        assert not syncs(world)
        assert "reusing release" in result.stderr

    def test_the_reuse_probe_imports_and_reads_its_revision(self, world):
        assert_ok(world.run(world.main[0]))
        world.reset_log()
        assert_ok(world.run(world.main[0]))
        (probe,) = [c for c in world.calls() if " -c " in c]
        assert probe.startswith(f"python {world.release(world.main[0])} ")
        assert "import src.broker.bus_health, src.broker.backup, co_core" in probe

    def test_an_unlinked_release_whose_venv_no_longer_runs_is_rebuilt(self, world):
        assert_ok(world.run(world.main[0]))
        assert_ok(world.run(world.main[1]))
        world.reset_log()
        result = world.run(world.main[0], FAKE_BROKEN_VENV="1")
        assert result.returncode == 1, "the rebuilt venv is as broken: the stub stays broken"
        assert syncs(world)
        assert "probe: no interpreter" in result.stderr, "why it rebuilt"

    def test_an_unlinked_release_that_misreads_its_revision_is_rebuilt(self, world):
        assert_ok(world.run(world.main[0]))
        assert_ok(world.run(world.main[1]))
        world.reset_log()
        result = world.run(world.main[0], FAKE_REVISION_SAYS="unknown")
        assert syncs(world)
        assert "reports unknown" in result.stderr

    def test_a_linked_release_whose_venv_no_longer_runs_is_never_rebuilt(self, world):
        """It is what live runs: rebuilding it in place pulls the code out from
        under the units, and a failed sync leaves live with no release at all."""
        assert_ok(world.run(world.main[0]))
        release = world.release(world.main[0])
        world.reset_log()
        result = world.run(world.main[0], FAKE_BROKEN_VENV="1")
        assert result.returncode == 1
        assert (release / "REVISION").exists()
        assert (release / ".venv").exists()
        assert not syncs(world)
        assert "live runs it" in result.stderr

    def test_an_interrupted_build_is_rebuilt(self, world):
        """No REVISION means the build never finished: start it again."""
        partial = world.release(world.main[-1])
        with world.as_root():
            partial.parent.mkdir()
        world.owned.write_text(world.owned.read_text() + f"D {partial.parent}\n")
        with world.as_root():
            partial.mkdir()
        (partial / "half-written").write_text("x")
        assert_ok(world.run())
        assert not (partial / "half-written").exists()
        assert (partial / "REVISION").exists()

    def test_a_failed_uv_sync_switches_nothing(self, world):
        result = world.run(FAKE_SYNC_FAIL="1")
        assert result.returncode == 1
        assert "uv sync failed" in result.stderr
        assert world.target() is None

    def test_an_interpreter_under_home_is_refused(self, world):
        """The probe's user can read /home/exedev; the backup's (C5) cannot."""
        result = world.run(FAKE_PYTHON_HOME="/home/exedev/.local/share/uv/python/bin")
        assert result.returncode == 1
        assert "/home/exedev/.local/share/uv/python/bin" in result.stderr
        assert "nothing switched" in result.stderr
        assert world.target() is None
        assert not (world.release(world.main[-1]) / "REVISION").exists()

    def test_a_venv_that_does_not_import_fails_the_build(self, world):
        result = world.run(FAKE_BROKEN_VENV="1")
        assert result.returncode == 1
        assert "probe: no interpreter" in result.stderr
        assert world.target() is None

    def test_a_failed_compile_fails_the_build(self, world):
        result = world.run(FAKE_COMPILE_FAIL="1")
        assert result.returncode == 1
        assert "compileall" in result.stderr
        assert world.target() is None

    def test_a_release_that_cannot_read_its_own_revision_never_switches(self, world):
        """The self-check (#105 CR 2): from the release, as exedev, after root
        wrote REVISION. It proves the path read_revision() looks at and the 444
        read in one line, and a failure leaves an interrupted build, not a
        finished one."""
        result = world.run(FAKE_REVISION_SAYS="unknown")
        assert result.returncode == 1
        assert "reports unknown" in result.stderr
        assert world.target() is None
        assert not (world.release(world.main[-1]) / "REVISION").exists()

    def test_old_releases_are_pruned_but_never_the_linked_one(self, world):
        for sha in world.main:
            assert_ok(world.run(sha, BROKER_DEPLOY_KEEP="1"))
        kept = {p.name for p in (world.root / "releases").iterdir()}
        assert kept == {world.build(world.main[-1])}

    def test_five_releases_are_kept_by_default(self, world):
        for _ in range(4):
            world.push()
        for sha in world.main:  # 7 builds
            assert_ok(world.run(sha))
        kept = {p.name for p in (world.root / "releases").iterdir()}
        assert kept == {world.build(sha) for sha in world.main[-5:]}

    def test_a_failed_prune_does_not_fail_a_deploy_that_succeeded(self, world):
        assert_ok(world.run(world.main[0]))
        assert_ok(world.run(world.main[1]))
        result = world.run(world.main[2], BROKER_DEPLOY_KEEP="1", FAKE_RM_FAIL="1")
        assert_ok(result)
        assert "prune failed" in result.stderr
        # A half-pruned release reads as interrupted, never as complete.
        half = world.release(world.main[0])
        assert half.exists() and not (half / "REVISION").exists()

    def test_a_linked_release_without_revision_is_never_deleted(self, world):
        assert_ok(world.run(world.main[0]))
        release = world.release(world.main[0])
        with world.as_root():
            (release / "REVISION").unlink()
        world.reset_log()
        result = world.run(world.main[0])
        assert result.returncode == 1
        assert (release / "version.txt").exists()
        assert not syncs(world)

    def test_a_linked_release_deleted_by_hand_is_built_again(self, world):
        """Nothing runs from a directory that is gone; building it is the fix."""
        assert_ok(world.run(world.main[0]))
        release = world.release(world.main[0])
        for path in [release, *release.rglob("*")]:
            if not path.is_symlink():
                path.chmod(path.stat().st_mode | 0o200)
        with world.as_root():
            shutil.rmtree(release)
        world.disown(release)
        assert_ok(world.run(world.main[0]))
        assert (release / "REVISION").exists()

    def test_a_release_root_does_not_own_is_rebuilt(self, world):
        assert_ok(world.run(world.main[0]))
        assert_ok(world.run(world.main[1]))
        release = world.release(world.main[0])
        world.disown(release)
        world.reset_log()
        result = world.run(world.main[0])
        assert_ok(result)
        assert syncs(world)
        assert "not root's" in result.stderr, "why it rebuilt"
        assert world.root_owned(release)

    def test_a_linked_release_root_does_not_own_is_never_rebuilt(self, world):
        assert_ok(world.run(world.main[0]))
        release = world.release(world.main[0])
        world.disown(release)
        world.reset_log()
        result = world.run(world.main[0])
        assert result.returncode == 1
        assert (release / "REVISION").exists()
        assert not syncs(world)
        assert "not root's" in result.stderr and "live runs it" in result.stderr

    def test_an_absolute_link_made_by_hand_still_protects_its_release(self, world):
        assert_ok(world.run(world.main[0]))
        assert_ok(world.run(world.main[1]))
        live = world.root / "live"
        target = world.release(world.main[0])
        with world.as_root():
            live.unlink()
            live.symlink_to(target)
        world.reset_log()
        result = world.run(world.main[0], FAKE_BROKEN_VENV="1")
        assert result.returncode == 1
        assert "live runs it" in result.stderr
        assert (target / "REVISION").exists()


class TestOwnership:
    """status#14: root owns the deploy root, releases/ and every finished release.

    exedev has passwordless sudo, so this is no boundary against exedev. What it
    buys: changing what a unit runs takes sudo, which journals the command.
    """

    def test_a_built_release_is_roots_throughout(self, world):
        assert_ok(world.run())
        release = world.release(world.main[-1])
        assert world.root_owned(world.root / "releases")
        assert world.root_owned(release)
        assert world.root_owned(release / "REVISION")
        assert world.root_owned(release / ".venv")
        assert world.root_owned(release / ".wheelhouse" / CO_CORE_WHEEL)

    def test_revision_is_written_by_root_after_the_chown_and_is_444(self, world):
        assert_ok(world.run())
        release = world.release(world.main[-1])
        calls = world.calls()
        assert calls.index(f"sudo chown -R root:root {release}") < calls.index(
            f"sudo tee {release}/REVISION"
        )
        assert f"sudo chmod 444 {release}/REVISION" in calls
        assert (release / "REVISION").stat().st_mode & 0o777 == 0o444
        assert (release / "REVISION").read_text() == world.build(world.main[-1]) + "\n"

    def test_the_release_is_555(self, world):
        assert_ok(world.run())
        assert world.release(world.main[-1]).stat().st_mode & 0o777 == 0o555

    def test_a_release_directory_root_will_not_make_says_so(self, world):
        build = world.build(world.main[-1])
        result = world.run(FAKE_INSTALL_FAIL=build)
        assert result.returncode == 1
        assert f"cannot make releases/{build}" in result.stderr
        assert "nothing switched" in result.stderr
        assert world.target() is None

    def test_a_failed_chown_switches_nothing_and_leaves_no_revision(self, world):
        result = world.run(FAKE_CHOWN_FAIL="1")
        assert result.returncode == 1
        assert world.target() is None
        assert not (world.release(world.main[-1]) / "REVISION").exists()
        assert f"cannot hand {world.build(world.main[-1])} to root" in result.stderr

    def test_a_deploy_root_root_does_not_own_is_refused_with_the_fix(self, world):
        world.disown(world.root)
        result = world.run()
        assert result.returncode == 1
        assert f"sudo chown root:root {world.root}" in result.stderr
        assert not [c for c in world.calls() if c.startswith(("uv ", "sudo ", "curl "))]

    def test_a_releases_directory_root_does_not_own_is_refused_with_the_fix(self, world):
        assert_ok(world.run(world.main[0]))
        world.disown(world.root / "releases")
        world.reset_log()
        result = world.run()
        assert result.returncode == 1
        assert f"sudo chown root:root {world.root}/releases" in result.stderr
        assert not [c for c in world.calls() if c.startswith(("uv ", "sudo "))]

    @pytest.mark.parametrize("mode", [0o575, 0o557])
    def test_a_group_or_world_writable_deploy_root_is_refused(self, world, mode):
        world.root.chmod(mode)
        result = world.run()
        assert result.returncode == 1
        assert f"sudo chmod 755 {world.root}" in result.stderr
        assert not [c for c in world.calls() if c.startswith("sudo ")]

    def test_a_deploy_root_that_is_a_link_is_refused_as_one(self, world):
        real = world.tmp / "real-srv"
        world.root.rename(real)
        world.root.symlink_to(real)
        result = world.run()
        assert result.returncode == 1
        assert "a link" in result.stderr and "writable" not in result.stderr
        assert not [c for c in world.calls() if c.startswith("sudo ")]

    def test_a_releases_link_to_nowhere_is_refused_as_a_link(self, world):
        with world.as_root():
            (world.root / "releases").symlink_to(world.tmp / "nowhere")
        result = world.run()
        assert result.returncode == 1
        assert f"{world.root}/releases is a link" in result.stderr
        assert not [c for c in world.calls() if c.startswith("sudo ")]

    def test_a_directory_the_sudo_stub_makes_as_root_is_unwritable_at_once(self, world):
        made = world.root / "releases"
        env = {**os.environ, "FAKE_LOG": str(world.log), "FAKE_ROOT_OWNED": str(world.owned)}
        subprocess.run(
            [str(world.stubs / "sudo"), "install", "-d", "-o", "root", "-m", "755", str(made)],
            env=env,
            check=True,
        )
        assert world.root_owned(made)
        assert not made.stat().st_mode & 0o222

    def test_a_write_under_the_root_without_sudo_fails(self, world):
        """The harness's premise: the ledger keeps root's directories unwritable."""
        assert_ok(world.run())
        with pytest.raises(PermissionError):
            (world.release(world.main[-1]) / "x").write_text("x")
        with pytest.raises(PermissionError):
            (world.root / "live.new").symlink_to("/tmp")

    def test_a_missing_releases_directory_is_made_by_root(self, world):
        assert_ok(world.run())
        assert f"sudo install -d -o root -g root -m 755 {world.root}/releases" in world.calls()

    def test_live_is_replaced_by_root_with_a_rename(self, world):
        assert_ok(world.run())
        calls = world.calls()
        build = world.build(world.main[-1])
        new = calls.index(f"sudo ln -sfn releases/{build} {world.root}/live.new")
        assert calls[new + 1] == f"sudo mv -Tf {world.root}/live.new {world.root}/live"

    def test_a_reused_release_is_touched_by_root(self, world):
        """Prune order is last deploy, not first build."""
        assert_ok(world.run(world.main[0]))
        world.reset_log()
        assert_ok(world.run(world.main[0]))
        assert f"sudo touch {world.release(world.main[0])}" in world.calls()

    def test_the_prune_is_done_by_root_revision_first(self, world):
        assert_ok(world.run(world.main[0]))
        assert_ok(world.run(world.main[1]))
        world.reset_log()
        assert_ok(world.run(world.main[2], BROKER_DEPLOY_KEEP="1"))
        old = world.release(world.main[0])
        assert not old.exists()
        calls = world.calls()
        assert calls.index(f"sudo rm -f {old}/REVISION") < calls.index(f"sudo rm -rf {old}")
        assert not world.root_owned(old), "the ledger forgets what was removed"


class TestVerification:
    """D8, once C3 and C4 have put both units on live: a code-only deploy moves
    live under both, so both are verified - the probe by its next natural tick,
    the backup by one forced run. Each test starts from ``old``, the release
    that carries the live units, already deployed."""

    @staticmethod
    def on_live(world: World) -> str:
        old = world.go_live()
        assert_ok(world.run(old))
        world.push()
        world.reset_log()
        return world.build(old)

    def test_a_code_deploy_verifies_both_units_that_run_live(self, world):
        self.on_live(world)
        result = world.run()
        assert_ok(result)
        assert not installs(world.calls()), "no unit changed: live moved under them"
        assert len(forced_backups(world)) == 1
        assert len(journal_reads(world)) == 1
        assert f"live is on {world.build(world.main[-1])}" in result.stderr

    def test_the_probe_is_never_started_by_the_deploy(self, world):
        """The two-tick pending rule: a forced tick seconds after the last one
        reads an in-flight entry as stuck (docs/BUS-HEALTH.md)."""
        self.on_live(world)
        assert_ok(world.run())
        assert not [c for c in world.calls() if "start" in c and PROBE in c]

    def test_the_tick_that_verifies_is_one_that_started_after_the_switch(self, world):
        self.on_live(world)
        assert_ok(world.run(FAKE_TICK_EVERY="3"))
        calls = world.calls()
        switched = index_of(calls, "logger -t broker-deploy live -> ")
        polls = [i for i, c in enumerate(calls) if c.startswith("systemctl show") and PROBE in c]
        assert polls[0] > switched
        assert len(polls) == 3, "the first poll's invocation is the baseline, never counted"
        (read,) = journal_reads(world)
        assert read.startswith("journalctl _SYSTEMD_INVOCATION_ID=tick-1 ")

    def test_a_tick_still_running_is_waited_out(self, world):
        self.on_live(world)
        result = world.run(FAKE_PROBE_STATE="activating", BROKER_DEPLOY_PROBE_WAIT_SECONDS="0")
        assert result.returncode == 4, result.stderr
        assert not journal_reads(world)

    def test_the_backup_run_is_one_that_started_on_this_build(self, world):
        """`systemctl start` on a oneshot mid-run joins that run, which started
        on the old release (status CR 2); and a start limit is cleared first."""
        self.on_live(world)
        assert_ok(world.run(FAKE_BACKUP_BUSY="2"))
        calls = world.calls()
        start = calls.index(f"sudo systemctl start {BACKUP}")
        polls = [i for i, c in enumerate(calls) if c.startswith("systemctl show") and BACKUP in c]
        assert len(polls) == 3, "two busy answers, then the idle one"
        assert max(polls) < start
        assert calls.index(f"sudo systemctl reset-failed {BACKUP}") < start

    def test_a_backup_run_that_never_ends_fails_the_verify(self, world):
        self.on_live(world)
        result = world.run(FAKE_BACKUP_BUSY="99", BROKER_DEPLOY_BACKUP_WAIT_SECONDS="0")
        assert result.returncode == 4, result.stderr
        assert result.stderr.count("has been running") == 2
        assert not forced_backups(world)

    @pytest.mark.parametrize("outcome", ["uploaded", "unchanged"])
    def test_a_backup_that_shipped_or_found_its_snapshot_there_verifies(self, world, outcome):
        self.on_live(world)
        assert_ok(world.run(FAKE_BACKUP_OUTCOME=outcome))

    def test_a_backup_run_that_fails_is_switched_back(self, world):
        old = self.on_live(world)
        result = world.run(FAKE_BACKUP_FAIL_BUILD=world.build(world.main[-1]))
        assert result.returncode == 1, result.stderr
        assert world.target() == f"releases/{old}"
        assert f"switched back to {old}, which is answering" in result.stderr
        assert len(forced_backups(world)) == 2, "on the new build, then on the old"
        assert json.loads(world.backup_state.read_text())["revision"] == old

    @pytest.mark.parametrize("outcome", ["failed", "skipped"])
    def test_a_backup_outcome_other_than_shipped_fails_the_verify(self, world, outcome):
        self.on_live(world)
        result = world.run(FAKE_BACKUP_OUTCOME=outcome)
        assert result.returncode == 4, "the old build reports the same outcome"
        assert f"outcome {outcome}" in result.stderr

    def test_a_backup_on_another_revision_fails_the_verify(self, world):
        self.on_live(world)
        result = world.run(FAKE_BACKUP_SAYS="dev")
        assert result.returncode == 4
        assert "revision dev" in result.stderr

    def test_a_probe_tick_on_another_build_is_switched_back(self, world):
        old = self.on_live(world)
        result = world.run(FAKE_PROBE_STALE_BUILD=world.build(world.main[-1]))
        assert result.returncode == 1, result.stderr
        assert world.target() == f"releases/{old}"
        assert "reported revision stale" in result.stderr
        assert len(journal_reads(world)) == 2, "the new build's tick, then the old's"

    def test_a_probe_tick_that_fails_is_switched_back(self, world):
        self.on_live(world)
        result = world.run(FAKE_PROBE_FAIL_BUILD=world.build(world.main[-1]))
        assert result.returncode == 1, result.stderr
        assert "Result=exit-code" in result.stderr
        assert "which is answering" in result.stderr

    def test_no_tick_within_the_wait_on_either_build_is_exit_4(self, world):
        self.on_live(world)
        result = world.run(FAKE_TICK_EVERY="1000000", BROKER_DEPLOY_PROBE_WAIT_SECONDS="0")
        assert result.returncode == 4, result.stderr
        assert "NOT answering" in result.stderr
        logged = [c for c in world.calls() if c.startswith("logger ")]
        assert not [c for c in logged if "rolled back" in c], "never claim it"
        assert [c for c in logged if "NOT answering" in c]

    def test_an_old_build_that_fails_too_is_exit_4(self, world):
        old = self.on_live(world)
        result = world.run(
            FAKE_BACKUP_FAIL_BUILD=world.build(world.main[-1]), FAKE_PROBE_FAIL_BUILD=old
        )
        assert result.returncode == 4
        assert f"switched back to {old}, which is NOT answering" in result.stderr

    def test_redeploying_the_running_build_verifies_nothing_it_did_not_change(self, world):
        self.on_live(world)
        assert_ok(world.run())
        world.reset_log()
        result = world.run()
        assert_ok(result)
        assert not forced_backups(world) and not journal_reads(world)

    def test_redeploying_the_running_build_puts_back_a_hand_fixed_unit(self, world):
        """Only the units changed, so they are what switches back; the hand fix
        is the known-good state, and the copy kept aside its only copy."""
        self.on_live(world)
        assert_ok(world.run())
        hand_fixed = world.live_units[BACKUP] + "# hand fix\n"
        (world.units / BACKUP).write_text(hand_fixed)
        world.reset_log()
        result = world.run(FAKE_BACKUP_NEEDS="hand fix")
        assert result.returncode == 1, result.stderr
        assert world.installed(BACKUP) == hand_fixed
        assert "put back the units it replaced" in result.stderr
        assert "which is answering" in result.stderr
        assert "switched back" not in result.stderr, "the link never moved"
        assert world.target() == f"releases/{world.build(world.main[-1])}"

    def test_redeploying_the_running_build_on_units_that_fail_too_is_exit_4(self, world):
        self.on_live(world)
        assert_ok(world.run())
        (world.units / BACKUP).write_text(world.live_units[BACKUP] + "# hand edit\n")
        result = world.run(FAKE_BACKUP_NEEDS="never there")
        assert result.returncode == 4, result.stderr
        assert world.installed(BACKUP) == world.live_units[BACKUP] + "# hand edit\n"
        assert "NOT answering" in result.stderr

    def test_a_rollback_restores_a_hand_made_link_exactly(self, world):
        self.on_live(world)
        elsewhere = world.tmp / "elsewhere"
        elsewhere.mkdir()
        live = world.root / "live"
        with world.as_root():
            live.unlink()
            live.symlink_to(elsewhere)
        result = world.run(FAKE_BACKUP_FAIL_BUILD=world.build(world.main[-1]))
        assert result.returncode == 1, "a tree without REVISION answers dev, and it did"
        assert "switched back to dev" in result.stderr
        assert os.readlink(live) == str(elsewhere)
        assert any(str(elsewhere) in c for c in world.calls() if c.startswith("logger ")), (
            "the journal names the real old link"
        )

    def test_a_unit_edit_on_the_dev_tree_is_verified_as_dev(self, world):
        """What a unit must report is read from its own WorkingDirectory=, so a
        changed unit that still runs ~/broker proves dev, not the release."""
        world.push({BACKUP: world.dev_units[BACKUP] + "# edited\n"})
        result = world.run()
        assert_ok(result)
        assert len(forced_backups(world)) == 1
        assert not journal_reads(world), "the probe's unit did not change"
        assert json.loads(world.backup_state.read_text())["revision"] == "dev"

    def test_a_first_deploy_that_fails_removes_live_and_puts_the_units_back(self, world):
        """C3's own shape: the units move from ~/broker onto live in the deploy
        that makes live. There was no link before, so restoring it exactly is
        removing it; the units that ran ~/broker go back, and must answer dev."""
        world.push(world.live_units)
        result = world.run(FAKE_BACKUP_FAIL_BUILD=world.build(world.main[-1]))
        assert result.returncode == 1, result.stderr
        assert not (world.root / "live").is_symlink()
        for name, body in world.dev_units.items():
            assert world.installed(name) == body, name
        assert "no previous release" in result.stderr
        assert "which is answering" in result.stderr

    def test_a_first_deploy_whose_old_units_fail_too_is_exit_4(self, world):
        world.push(world.live_units)
        result = world.run(FAKE_BACKUP_OUTCOME="failed")
        assert result.returncode == 4, result.stderr
        assert "no previous release" in result.stderr


V2 = "v2"


class TestUnits:
    """D9: deploy/broker-*.{service,timer} reach /etc/systemd/system with the
    release they run; nothing else under deploy/ does."""

    def test_only_the_units_that_differ_are_installed_with_one_reload(self, world):
        world.push({BACKUP: unit(BACKUP, world.checkout, V2)})
        world.reset_log()
        assert_ok(world.run())
        assert installs(world.calls()) == [BACKUP]
        assert len([c for c in world.calls() if "daemon-reload" in c]) == 1
        assert world.installed(BACKUP) == unit(BACKUP, world.checkout, V2)
        assert (world.units / BACKUP).stat().st_mode & 0o777 == 0o644

    def test_units_install_after_the_switch_and_before_the_verify(self, world):
        world.push({BACKUP: unit(BACKUP, world.checkout, V2)})
        assert_ok(world.run())
        calls = world.calls()
        switched = index_of(calls, "logger -t broker-deploy live -> ")
        installed = index_of(calls, "sudo install -m 644 ")
        reload = index_of(calls, "daemon-reload")
        assert switched < installed < reload < calls.index(f"sudo systemctl start {BACKUP}")

    def test_only_broker_units_are_installed(self, world):
        world.push({"other.service": "[Service]\n", "broker-x.conf": "x\n"})
        assert_ok(world.run())
        assert not installs(world.calls())
        assert world.installed("other.service") is None

    def test_a_changed_timer_is_restarted_so_it_rearms(self, world):
        world.push({"broker-backup.timer": unit("broker-backup.timer", "", V2)})
        world.reset_log()
        assert_ok(world.run())
        try_restarts = [c for c in world.calls() if "try-restart" in c]
        assert try_restarts == ["sudo systemctl try-restart broker-backup.timer"]

    def test_a_changed_timer_verifies_its_service(self, world):
        world.push({"broker-backup.timer": unit("broker-backup.timer", "", V2)})
        assert_ok(world.run())
        assert len(forced_backups(world)) == 1

    def test_a_timer_that_will_not_restart_fails_and_switches_back(self, world):
        """The probe's timer is the node's only always-on broker-side signal."""
        assert_ok(world.run(world.main[0]))
        previous = world.target()
        world.push({"broker-bus-health.timer": unit("broker-bus-health.timer", "", V2)})
        result = world.run(
            FAKE_TRY_RESTART_FAIL="broker-bus-health.timer", BROKER_DEPLOY_PROBE_WAIT_SECONDS="5"
        )
        assert result.returncode == 1, result.stderr
        assert "try-restart broker-bus-health.timer" in result.stderr
        assert world.target() == previous
        assert (
            world.installed("broker-bus-health.timer") == world.dev_units["broker-bus-health.timer"]
        )

    def test_a_new_unit_is_installed_not_enabled_and_named_with_how(self, world):
        world.push({"broker-drift.timer": "[Timer]\n"})
        world.reset_log()
        result = world.run()
        assert_ok(result)
        assert world.installed("broker-drift.timer") == "[Timer]\n"
        assert "sudo systemctl enable --now broker-drift.timer" in result.stderr
        assert not [c for c in world.calls() if " enable " in c]
        logged = [c for c in world.calls() if c.startswith("logger ") and " units from " in c]
        assert any("broker-drift.timer (new)" in c for c in logged)

    def test_a_changed_unit_with_no_verifier_is_named(self, world):
        """C6's drift unit has none yet: say so, never call it verified."""
        world.push({"broker-drift.service": unit("broker-drift.service", world.root / "live")})
        result = world.run()
        assert_ok(result)
        assert "broker-drift.service" in result.stderr
        assert "not verified" in result.stderr

    def test_a_hand_edited_unit_is_replaced_and_named(self, world):
        (world.units / BACKUP).write_text("edited by hand\n")
        result = world.run()
        assert_ok(result)
        assert world.installed(BACKUP) == world.dev_units[BACKUP]
        assert BACKUP in result.stderr

    def test_a_unit_the_release_lacks_is_left_alone_and_named(self, world):
        (world.units / "broker-drift.service").write_text("installed\n")
        result = world.run()
        assert_ok(result)
        assert world.installed("broker-drift.service") == "installed\n"
        assert "broker-drift.service is installed but not in" in result.stderr

    def test_a_failed_verify_puts_back_exactly_the_units_it_replaced(self, world):
        """Switch back means the units too: what was installed, hand edits
        included, and nothing the failed build added."""
        assert_ok(world.run(world.go_live()))
        (world.units / BACKUP).write_text(world.live_units[BACKUP] + "# by hand\n")
        world.push({PROBE: unit(PROBE, world.root / "live", V2), "broker-drift.timer": "[Timer]\n"})
        world.reset_log()
        result = world.run(FAKE_BACKUP_FAIL_BUILD=world.build(world.main[-1]))
        assert result.returncode == 1, result.stderr
        assert world.installed(BACKUP) == world.live_units[BACKUP] + "# by hand\n"
        assert world.installed(PROBE) == world.live_units[PROBE]
        assert world.installed("broker-drift.timer") is None
        calls = world.calls()
        restored = calls.index("logger -t broker-deploy units restored")
        reloads = [i for i, c in enumerate(calls) if "daemon-reload" in c]
        assert len(reloads) == 2 and reloads[-1] < restored

    def test_a_unit_that_cannot_be_kept_aside_is_not_replaced(self, world):
        assert_ok(world.run(world.main[0]))
        previous = world.target()
        world.push({BACKUP: unit(BACKUP, world.checkout, V2)})
        (world.units / BACKUP).chmod(0o000)
        try:
            result = world.run()
        finally:
            (world.units / BACKUP).chmod(0o644)
        assert result.returncode == 1, result.stderr
        assert "aside" in result.stderr
        assert world.target() == previous
        assert world.installed(BACKUP) == world.dev_units[BACKUP]

    def test_a_redeploy_that_changed_nothing_before_failing_is_exit_1(self, world):
        """CR 1: the link never moved and no unit was replaced, so the node is
        as it was. Exit 4 would say nothing answers."""
        assert_ok(world.run())
        (world.units / BACKUP).write_text("edited by hand\n")
        (world.units / BACKUP).chmod(0o000)
        try:
            result = world.run()
        finally:
            (world.units / BACKUP).chmod(0o644)
        assert result.returncode == 1, result.stderr
        assert "nothing changed" in result.stderr
        assert world.installed(BACKUP) == "edited by hand\n"

    def test_a_failed_install_switches_back(self, world):
        assert_ok(world.run(world.main[0]))
        previous = world.target()
        world.push(
            {
                PROBE: unit(PROBE, world.checkout, V2),
                BACKUP: unit(BACKUP, world.checkout, V2),
            }
        )
        result = world.run(FAKE_INSTALL_FAIL=PROBE)
        assert result.returncode == 1, result.stderr
        assert PROBE in result.stderr
        assert world.target() == previous
        assert world.installed(BACKUP) == world.dev_units[BACKUP]
        assert world.installed(PROBE) == world.dev_units[PROBE]

    def test_the_copies_kept_for_a_switch_back_are_removed(self, world):
        tmp = world.tmp / "tmpdir"
        tmp.mkdir()
        (world.units / BACKUP).write_text("edited by hand\n")
        assert_ok(world.run(TMPDIR=str(tmp)))
        assert not list(tmp.iterdir())

    def test_a_rollback_deploy_installs_the_older_units(self, world):
        old = world.main[-1]
        world.push({BACKUP: unit(BACKUP, world.checkout, V2), "broker-drift.timer": "[Timer]\n"})
        assert_ok(world.run())
        assert_ok(world.run(world.build(old)))
        assert world.installed(BACKUP) == world.dev_units[BACKUP]
        assert world.installed("broker-drift.timer") == "[Timer]\n", "the release lacks it"


def host_configs() -> dict[str, str]:
    """deploy.sh's HOST_CONFIGS: each deploy/ file and where it is installed."""
    body = re.search(r"^HOST_CONFIGS=\((.*?)^\)", DEPLOY.read_text(), re.M | re.S)
    assert body, "HOST_CONFIGS=( ... ) in deploy.sh"
    return dict(re.findall(r'"([^"=]+)=([^"]+)"', body.group(1)))


def derived_configs() -> list[str]:
    body = re.search(r"^DERIVED_CONFIGS=\((.*?)\)", DEPLOY.read_text(), re.M | re.S)
    assert body, "DERIVED_CONFIGS=( ... ) in deploy.sh"
    return body.group(1).split()


class TestHostConfigs:
    """D9: compared, never installed, never a failure; redis-server never restarted."""

    def install_host(self, world: World, dest: str, body: str) -> Path:
        path = world.host / dest.lstrip("/")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body)
        return path

    def test_a_differing_host_config_is_a_note_naming_both_paths(self, world):
        earlyoom = self.install_host(world, "/etc/default/earlyoom", "older\n")
        self.install_host(world, "/etc/sysctl.d/60-broker-memory.conf", "sysctl v1\n")
        world.push(
            {"earlyoom.default": "earlyoom v1\n", "sysctl.d/60-broker-memory.conf": "sysctl v1\n"}
        )
        result = world.run()
        assert_ok(result)
        assert "deploy/earlyoom.default" in result.stderr
        assert str(earlyoom) in result.stderr
        assert "60-broker-memory.conf" not in result.stderr, "that one is the same"
        assert earlyoom.read_text() == "older\n", "never installed"

    def test_a_missing_host_config_is_a_note(self, world):
        world.push({"earlyoom.default": "earlyoom v1\n"})
        result = world.run()
        assert_ok(result)
        assert "deploy/earlyoom.default is not installed" in result.stderr

    def test_a_derived_config_changed_since_the_previous_release_is_a_note(self, world):
        """redis.conf.broker is appended with a minted requirepass, redis-acl.conf
        rendered with digests, under /etc/redis that exedev cannot read: compared
        with the release live ran, never with the installed file."""
        world.push({"redis.conf.broker": "maxmemory 512mb\n", "redis-acl.conf": "user a on\n"})
        assert_ok(world.run())
        world.push({"redis.conf.broker": "maxmemory 1gb\n"})
        result = world.run()
        assert_ok(result)
        assert "deploy/redis.conf.broker changed since" in result.stderr
        assert "restart window" in result.stderr
        assert "redis-acl.conf changed" not in result.stderr

    def test_a_derived_config_on_a_first_deploy_points_at_the_parity_tests(self, world):
        world.push({"redis.conf.broker": "maxmemory 512mb\n"})
        result = world.run()
        assert_ok(result)
        assert "tests/deploy" in result.stderr

    def test_a_host_config_is_never_installed_as_a_unit(self, world):
        world.push({"earlyoom.default": "earlyoom v1\n"})
        assert_ok(world.run())
        assert not installs(world.calls())

    def test_nothing_compared_lives_under_etc_redis(self):
        for dest in host_configs().values():
            assert not dest.startswith("/etc/redis/"), dest

    def test_every_file_under_deploy_is_a_unit_a_config_the_render_tool_or_the_readme(self):
        """A new file there is installed, compared or named, never forgotten."""
        configs, derived = host_configs(), derived_configs()
        repo_deploy = REPO_ROOT / "deploy"
        for path in sorted(p for p in repo_deploy.rglob("*") if p.is_file()):
            rel = path.relative_to(repo_deploy).as_posix()
            kinds = [
                "/" not in rel
                and rel.startswith("broker-")
                and path.suffix in (".service", ".timer"),
                rel in configs,
                rel in derived,
                rel in ("render-acl.sh", "README.md"),
            ]
            assert kinds.count(True) == 1, (rel, kinds)
        for rel in [*configs, *derived]:
            assert (repo_deploy / rel).is_file(), f"deploy.sh names {rel}, not in deploy/"

    def test_the_install_runbook_puts_each_host_config_where_deploy_compares_it(self):
        install = README.read_text().split("\n## Install\n", 1)[1].split("\n## ", 1)[0]
        install = install.replace("\\\n", " ")
        for rel, dest in host_configs().items():
            assert rel in install, rel
            assert (
                dest in install
                or f"{Path(dest).parent}/" in install
                or ('"/etc/systemd/system/$d"' in install and dest == f"/etc/systemd/system/{rel}")
            ), dest


class TestOperation:
    def test_a_concurrent_deploy_is_refused_before_the_ci_gate(self, world):
        """The lock is the deploy root itself: exedev cannot create a file in it."""
        held = os.open(world.root, os.O_RDONLY)
        try:
            fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
            result = world.run()
        finally:
            os.close(held)
        assert result.returncode == 1
        assert "another deploy" in result.stderr
        assert not world.github_calls()

    def test_the_lock_leaves_nothing_behind(self, world):
        assert_ok(world.run())
        assert sorted(p.name for p in world.root.iterdir()) == ["live", "releases"]

    def test_a_missing_root_says_how_to_create_it(self, world):
        world.root.rmdir()
        result = world.run()
        assert result.returncode == 1
        assert f"sudo install -d -o root -g root -m 755 {world.root}" in result.stderr

    def test_run_from_outside_a_checkout_says_where_to_run_it(self, world):
        """/srv/broker/live/scripts/deploy.sh exists too, with no .git."""
        release_scripts = world.tmp / "release" / "scripts"
        release_scripts.mkdir(parents=True)
        shutil.copy(DEPLOY, release_scripts / "deploy.sh")
        result = subprocess.run(
            [str(release_scripts / "deploy.sh")],
            env={
                "PATH": f"{world.stubs}:{os.environ['PATH']}",
                "BROKER_DEPLOY_ROOT": str(world.root),
                "FAKE_LOG": str(world.log),
            },
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 1
        assert "from a checkout" in result.stderr

    def test_an_unknown_flag_is_refused(self, world):
        assert world.run("--live-only").returncode == 1

    def test_two_refs_are_refused(self, world):
        assert world.run(world.main[0], world.main[1]).returncode == 1

    def test_root_is_refused(self):
        """exedev builds: as root, uv would build with root's cache, and every
        write under the root would be unjournaled."""
        assert '"$(id -u)" -eq 0' in DEPLOY.read_text()

    def test_the_script_holds_no_redis_credential(self):
        """It needs none: tests/deploy/test_runbook_credentials.py scans it too."""
        text = DEPLOY.read_text()
        assert "redis-cli" not in text
        assert "redis://" not in text
