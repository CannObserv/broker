"""The node's engine is Valkey since #85's window: what runs, and what an operator
copies, names Valkey's unit, paths and CLI (CannObserv/broker#85).

Ubuntu's ``valkey-server`` co-installs with ``redis-server`` and renames
everything: ``valkey-server.service``, ``/etc/valkey/valkey.conf``,
``/var/lib/valkey``, ``User=valkey``, ``valkey-cli``. The compat package that
would keep some ``redis-*`` names is never installed (it removes the 7.0 server
and moves ``/var/lib/redis``). Through the soak the 7.0 packages stay installed
and stopped, so a stale ``/etc/redis`` path still resolves - to a file the
running engine never reads. That is the drift this module exists to catch: a
command that succeeds against the wrong engine.

Scanned: every non-comment line of ``deploy/`` (the README aside, whose commands
sit in code blocks), of ``scripts/deploy.sh`` and of the patching knob; and the
code blocks of every runbook. Not scanned: the record of what was run before the
window - the 2026-09-10 incident, ``docs/archive/``, ``docs/plans/``, and the
one-time reap in ``docs/CONSUMER-REGISTRATIONS.md`` - whose commands are history,
not instructions. A rollback to 7.0 is #85's own plan, on the issue, not a
runbook here: it ends with the soak.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

#: The 7.0 engine's unit, paths, CLI, checker and owner, as a command names them.
RETIRED = re.compile(
    r"\bredis-server\b|/etc/redis\b|/var/lib/redis\b|/var/log/redis\b|\bredis-cli\b"
    r"|\bredis-check-(?:rdb|aof)\b|\bredis:redis\b|-g redis\b|\b(?:User|Group)=redis\b"
)

#: History: what was run, kept as it was run.
HISTORY = (
    "docs/INCIDENT-2026-09-10.md",
    "docs/CONSUMER-REGISTRATIONS.md",
)
HISTORY_DIRS = ("docs/archive/", "docs/plans/")

#: Lines that name the 7.0 packages on purpose, exactly as written: the knob's
#: held step keeps them out of the bulk lane while the soak leaves them installed.
ALLOWED = {
    ".skills/patching-hosts": {"hold redis-server redis-tools valkey-server valkey-tools redis"},
}


def _rel(path: Path) -> str:
    return str(path.relative_to(REPO_ROOT))


def _config_files() -> list[Path]:
    deploy = sorted(
        p for p in (REPO_ROOT / "deploy").rglob("*") if p.is_file() and p.name != "README.md"
    )
    return [*deploy, REPO_ROOT / "scripts" / "deploy.sh", REPO_ROOT / ".skills" / "patching-hosts"]


def _runbooks() -> list[Path]:
    docs = sorted((REPO_ROOT / "docs").rglob("*.md"))
    kept = [p for p in docs if _rel(p) not in HISTORY and not _rel(p).startswith(HISTORY_DIRS)]
    return [REPO_ROOT / "deploy" / "README.md", REPO_ROOT / "README.md", *kept]


def _code_lines(path: Path):
    """Each line inside a fenced code block, with its number."""
    fenced = False
    for number, line in enumerate(path.read_text().splitlines(), start=1):
        if line.lstrip().startswith("```"):
            fenced = not fenced
            continue
        if fenced:
            yield number, line


def test_no_config_line_names_the_retired_engine() -> None:
    hits = [
        f"{_rel(path)}:{number}: {line.strip()}"
        for path in _config_files()
        for number, line in enumerate(path.read_text().splitlines(), start=1)
        if line.strip()
        and not line.lstrip().startswith("#")
        and line.strip() not in ALLOWED.get(_rel(path), ())
        and RETIRED.search(line)
    ]
    assert not hits, "a config line names the 7.0 engine:\n" + "\n".join(hits)


def test_no_runbook_command_names_the_retired_engine() -> None:
    hits = [
        f"{_rel(path)}:{number}: {line.strip()}"
        for path in _runbooks()
        for number, line in _code_lines(path)
        if RETIRED.search(line)
    ]
    assert not hits, "a runbook command names the 7.0 engine:\n" + "\n".join(hits)


def test_the_scan_reads_what_it_claims() -> None:
    """A guard that matches nothing passes by reading nothing."""
    assert RETIRED.search("sudo systemctl restart redis-server")
    assert RETIRED.search("sudo ls /var/lib/redis/appendonlydir/")
    assert RETIRED.search('REDISCLI_AUTH="$p" redis-cli --user acladmin PING')
    assert not RETIRED.search("sudo systemctl restart valkey-server")
    assert not RETIRED.search("deploy/redis.conf.broker deploy/redis-acl.conf")
    runbooks = {_rel(p) for p in _runbooks()}
    assert {"docs/RESTART-WINDOW.md", "docs/RECOVERY.md", "deploy/README.md"} <= runbooks
    assert not runbooks & set(HISTORY)
    assert sum(1 for p in _runbooks() for _ in _code_lines(p)) > 100
    assert {"deploy.sh", "patching-hosts", "broker-backup.service"} <= {
        p.name for p in _config_files()
    }
