"""The patching-hosts knob reads clean, and keeps the decisions it records (#67).

``.skills/patching-hosts`` is what the vendored ``patching-hosts`` skill reads to
learn what only this repo's owner knows: the window, the backup that stands in
for a dump, who is told before a restart. **A malformed line makes the host
report-only** until it is fixed, which reads as "nothing to do" at the next
monthly run rather than as an error, so the skill's own reader is run here.

The reader ships in the submodule, which CI's checkout does not initialise: off
the node that is a skip, on it a failure (``require_broker_node``).

The rest are pure parses of the file, run everywhere:

- **No knob command escalates.** ``health`` and ``inflight`` run as the invoking
  user by design; a ``sudo`` in one would hand root to whoever commits the knob.
  Pending counts and ``ACL LOG`` need ``acladmin``, i.e. root, so they stay out.
- **The backup covers the data store**, so the recovery point is the hourly
  snapshot already off the node rather than a second copy of ``dump.rdb``.
- **The callers are the public participants.** Adding a participant means a
  line here; the private one is told by hand, and its name stays out of this
  public repo (gregoryfoster/skills#350).
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from tests.deploy.conftest import on_broker_node

REPO_ROOT = Path(__file__).resolve().parents[2]
KNOB = REPO_ROOT / ".skills" / "patching-hosts"
READ_KNOB = REPO_ROOT / "skills" / "patching-hosts" / "scripts" / "read-knob.sh"

PUBLIC_CALLERS = {
    "CannObserv/archiver",
    "CannObserv/watcher",
    "CannObserv/replicator",
    "CannObserv/processor",
}


def _directives() -> list[list[str]]:
    """Each directive line's words. ``#`` opens a comment at a line's start or
    after whitespace (the skill's knob.md), so a command keeps a bare ``#``."""
    lines = []
    for raw in KNOB.read_text().splitlines():
        words = []
        for word in raw.split():
            if word.startswith("#"):
                break
            words.append(word)
        if words:
            lines.append(words)
    return lines


def _named(directive: str) -> list[list[str]]:
    return [words[1:] for words in _directives() if words[0] == directive]


def test_the_knob_reads_clean() -> None:
    if not READ_KNOB.is_file():
        if on_broker_node():
            pytest.fail(f"{READ_KNOB} missing on the node: run bash .skills/doctor.sh")
        pytest.skip("patching-hosts submodule not initialised")
    out = subprocess.run(
        ["bash", str(READ_KNOB), "--config", str(KNOB), "--host", "co-broker"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    knob = json.loads(out)
    assert knob["findings"] == []
    assert knob["report_only"] is False, knob["report_only_reasons"]
    assert knob["class"]["value"] == "production"
    assert knob["posture"]["value"] == "scheduled"
    assert knob["window"], "no window: apply.sh refuses every step"


def test_no_knob_command_escalates() -> None:
    commands = [" ".join(words) for words in _named("health") + _named("inflight")]
    assert commands, "the knob declares no health line"
    for command in commands:
        for word in ("sudo", "systemd-creds", "rcli", "REDISCLI_AUTH", "runuser"):
            assert word not in command, f"{word!r} in a knob command: {command}"


def test_the_backup_covers_the_redis_datastore() -> None:
    assert _named("datastore") == [["redis", "redis-server"]]
    assert _named("backup") == [["broker-backup.service", "redis-server.service"]]


def test_the_callers_are_the_public_participants() -> None:
    assert {words[0] for words in _named("caller")} == PUBLIC_CALLERS
