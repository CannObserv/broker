"""No runbook here puts a credential on a command line (CannObserv/broker#47).

Generalised from CannObserv/archiver#251, where a broker credential reached
archiver's journald from two sources: the application's own start log, and one
``sudo`` command line - the ``sed -i`` that wrote the value into an env file.
Their fix was application-side redaction, which cannot reach the second one.
**Broker's exposure is entirely that second half**, because nothing under
``src/broker/`` ever emits a credential and every ACL runbook here used to build
a ``redis://user:<plaintext>@host`` URL and hand it to ``redis-cli -u``.

The plaintext lands in the process ``argv`` - readable from ``ps`` by any local
user for the life of the call - and in root's shell history; under ``sudo`` it
also reaches journald, which is archiver's finding exactly. This node is
single-tenant and tailnet-bound, so "any local user" is a small set today. That
is a mitigating circumstance and not a reason: the same runbook is what a
rebuild (CannObserv/broker#4) or a new cluster repeats.

The replacement is ``REDISCLI_AUTH`` plus ``--user``, verified on a scratch
7.0.15 (the same binary as prod). It also emits no
"Using a password ... may not be safe" warning, so ``--no-auth-warning`` comes
off with the URL - its presence in a diff is itself the tell that the old form
is back.

**What is scanned, and what is not.** ``docs/`` and ``deploy/`` plus the two
root Markdown files: everything an operator copies from. ``tests/`` is
deliberately out of reach, because a pattern that scanned its own directory
would match the pattern strings in this module and the escape would have to be
uglier than the rule.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

#: Every file an operator copies a command out of.
SCANNED = (
    sorted((REPO_ROOT / "docs").rglob("*.md"))
    + sorted(p for p in (REPO_ROOT / "deploy").rglob("*") if p.is_file())
    + [REPO_ROOT / "AGENTS.md", REPO_ROOT / "README.md"]
)

#: A bus URL whose password segment is a shell expansion:
#: ``redis://acladmin:$(pw ACLADMIN)@127.0.0.1:6379/0``. The documentary forms -
#: ``redis://<service>:<its password>@broker:6379/0`` - carry no ``$`` and stay;
#: they illustrate a shape rather than producing one. The ``@`` exclusion keeps
#: an unrelated later ``$`` on the same line from reading as a secret.
SECRET_IN_URL = re.compile(r"""redis://[^\s"'/]*:[^\s"'@]*\$""")

#: The laundered form, which the pattern above cannot see: the URL is built once
#: into a variable and the variable is handed over, so no literal ``redis://``
#: sits beside the expansion. ``-u "$BROKER_REDIS_URL"`` is the same defect
#: wearing an env file. ``-a`` and ``--pass`` are the direct spellings of it.
CREDENTIAL_FLAG = re.compile(r"redis-cli\b[^\n]*?\s(?:-u|-a|--pass)\b")


def _hits(pattern: re.Pattern[str]) -> list[str]:
    findings = []
    for path in SCANNED:
        for number, line in enumerate(path.read_text().splitlines(), start=1):
            if pattern.search(line):
                findings.append(f"{path.relative_to(REPO_ROOT)}:{number}")
    return findings


def test_no_runbook_builds_a_bus_url_out_of_a_secret() -> None:
    """The direct form. ``docs/ACL-CUTOVER.md``'s step 4 loop was the worst of
    them: one loop that put all six service credentials in ``argv``, run at the
    most security-sensitive moment this cluster has."""
    assert not _hits(SECRET_IN_URL), "a credential is expanded into a URL: use REDISCLI_AUTH"


def test_no_runbook_hands_redis_cli_a_credential_bearing_flag() -> None:
    """``-u`` is banned outright rather than only when a secret is visible on
    the line. Every URL that reaches this node's broker carries a password, so a
    ``-u`` that does not is either a rehearsal server or a line that is about to
    grow one."""
    assert not _hits(CREDENTIAL_FLAG), "redis-cli is given a credential in argv: use REDISCLI_AUTH"
