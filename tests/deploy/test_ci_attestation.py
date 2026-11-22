"""ci.yml's half of the local-gate attestation (#147, gregoryfoster/skills#688).

With ``.skills/attest-local-gate`` committed ``on``, shipping-work's ``push.sh``
runs the local gate on the exact tree, pushes, and posts the commit status
``gate/local`` = ``success``. ci.yml reads it and skips its heavy tier, the
``pytest`` job, for that commit, and only for a push to a branch or a person's
pull request from this repository. It never skips for a bot's PR, a fork's, a
schedule, a dispatch or a tag.

What the shape here buys, against the reference's step-level skip:

- **A job, not a step.** A skipped step leaves its job ``success``;
  scripts/deploy.sh's gate refuses a skipped job and requires the ``pytest``
  step to have run (``CI_STEPS``), so a release never rests on tests that ran
  only locally. A full run (dispatch, schedule) is what an attested commit
  deploys from.
- **The read is last in lint + test.** ``push.sh`` posts the status seconds
  after the push; a read that beats it runs the full tier. Last, it reads after
  the setup and the cheap tiers.
- **``statuses: read`` on the job.** Job-level ``permissions`` replace the
  workflow's, so the reference's workflow-level grant would not reach it.
- **``!cancelled()``.** A lint failure still runs the tests, attested or not:
  the attest step never ran, so its output is empty.

No YAML parser here (none is a dependency, and uv.lock is a runtime file), so
the workflow is read by its fixed indentation, as test_workflow_pins.py does.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
CI = ROOT / ".github" / "workflows" / "ci.yml"
HEAVY = "pytest"
CHEAP = "lint + test"


def _jobs() -> dict[str, str]:
    """Each job's block under ``jobs:``, by its key."""
    body = CI.read_text().split("\njobs:\n", 1)[1]
    parts = re.split(r"^  ([\w-]+):\n", body, flags=re.M)
    return dict(zip(parts[1::2], parts[2::2], strict=True))


def _job(name: str) -> str:
    (block,) = [
        b for b in _jobs().values() if re.search(rf"^    name: {re.escape(name)}$", b, re.M)
    ]
    return block


def _key(name: str) -> str:
    (key,) = [
        k for k, b in _jobs().items() if re.search(rf"^    name: {re.escape(name)}$", b, re.M)
    ]
    return key


def _steps(block: str) -> list[str]:
    """A job's steps, each from just after its ``- ``."""
    return re.split(r"^      - ", block.split("\n    steps:\n", 1)[1], flags=re.M)[1:]


def _step_names(block: str) -> list[str]:
    return [m.group(1) for s in _steps(block) if (m := re.match(r"name: (.+)$", s, re.M))]


def _permissions(block: str) -> dict[str, str]:
    found = re.search(r"^    permissions:\n((?:      .*\n|\s*#.*\n)+)", block, re.M)
    assert found, "the job declares its own permissions"
    return dict(re.findall(r"^      ([\w-]+): (\w+)", found.group(1), re.M))


def _condition() -> str:
    """The pytest job's ``if:``, a folded block, as one line."""
    found = re.search(r"^    if: >-\n((?:      .*\n)+)", _job(HEAVY), re.M)
    assert found, "the pytest job's if: is a folded block"
    return " ".join(line.strip() for line in found.group(1).splitlines())


# --- a GitHub expression, evaluated -----------------------------------------
#
# Just enough of the expression language for the one condition: property
# paths, string literals, ==, !=, &&, ||, !, parentheses, startsWith() and
# cancelled(). A missing property is null, which equals no string, as on Actions.
# Strings compare ignoring case there, startsWith() too, so both sides are
# case-folded here (CR 3).


class _Null:
    def __getattr__(self, _: str) -> _Null:
        return self

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _Null)

    def __hash__(self) -> int:
        return 0


class _Ctx:
    def __init__(self, data: dict) -> None:
        self._data = data

    def __getattr__(self, name: str):
        value = self._data.get(name, _Null())
        if isinstance(value, dict):
            return _Ctx(value)
        return value.casefold() if isinstance(value, str) else value


def _evaluate(expr: str, context: dict, *, cancelled: bool = False) -> bool:
    py = expr.replace("&&", " and ").replace("||", " or ")
    py = re.sub(r"!(?!=)", " not ", py)
    py = re.sub(r"'([^']*)'", lambda m: repr(m.group(1).casefold()), py)
    assert re.fullmatch(r"[\w.'\s()=!,/-]+", py), py
    names = {k: _Ctx(v) for k, v in context.items()}
    names["startsWith"] = lambda s, p: isinstance(s, str) and s.startswith(p)
    names["cancelled"] = lambda: cancelled
    return bool(eval(py, {"__builtins__": {}}, names))  # noqa: S307 - our own workflow


REPO = "CannObserv/broker"


def _context(event: str, *, attested: str = "yes", ref: str = "refs/heads/main", **pr) -> dict:
    github = {"event_name": event, "ref": ref, "repository": REPO}
    if event == "pull_request":
        github["event"] = {
            "pull_request": {
                "user": {"type": pr.get("user_type", "User")},
                "head": {"repo": {"full_name": pr.get("head_repo", REPO)}},
            }
        }
    return {"github": github, "needs": {"test": {"outputs": {"attested": attested}}}}


SKIPS = [
    pytest.param(_context("push"), id="attested-push-to-main"),
    pytest.param(_context("push", ref="refs/heads/topic"), id="attested-push-to-a-branch"),
    pytest.param(_context("pull_request"), id="attested-pr-from-a-person-here"),
]

RUNS = [
    pytest.param(_context("push", attested="no"), id="push-unattested"),
    pytest.param(_context("push", attested=""), id="push-lint-failed-before-attest"),
    pytest.param(_context("pull_request", attested="no"), id="pr-unattested"),
    pytest.param(_context("pull_request", user_type="Bot"), id="bot-pr"),
    pytest.param(_context("pull_request", head_repo="someone/broker"), id="fork-pr"),
    pytest.param(_context("schedule"), id="schedule"),
    pytest.param(_context("workflow_dispatch"), id="dispatch"),
    pytest.param(_context("push", ref="refs/tags/v1"), id="tag"),
    pytest.param(_context("pull_request_target"), id="pull-request-target"),
]


@pytest.mark.parametrize("context", SKIPS)
def test_the_heavy_tier_is_skipped_only_for_an_attested_push_or_a_persons_pr(context):
    assert not _evaluate(_condition(), context)


@pytest.mark.parametrize("context", RUNS)
def test_the_heavy_tier_runs_for_everything_else(context):
    assert _evaluate(_condition(), context)


def test_a_cancelled_run_starts_no_heavy_tier():
    assert not _evaluate(_condition(), _context("push", attested="no"), cancelled=True)


def test_the_evaluator_compares_strings_ignoring_case_as_actions_does():
    assert _evaluate(
        "github.event.pull_request.user.type == 'bot'", _context("pull_request", user_type="Bot")
    )
    assert _evaluate("startsWith(github.ref, 'REFS/tags/')", _context("push", ref="refs/tags/v1"))


def test_the_evaluator_reads_null_as_no_string():
    """The fork clause compares a property a push event lacks."""
    assert _evaluate("github.event.pull_request.user.type != 'Bot'", _context("push"))
    assert not _evaluate("github.event.pull_request.user.type == 'Bot'", _context("push"))


# --- the shape ---------------------------------------------------------------


def test_the_heavy_tier_is_its_own_job_after_lint_and_test():
    block = _job(HEAVY)
    assert re.search(rf"^    needs: {_key(CHEAP)}$", block, re.M)
    assert "pytest" not in _step_names(_job(CHEAP))
    (step,) = [s for s in _steps(block) if s.startswith(f"name: {HEAVY}\n")]
    assert re.search(r"^        run: uv run pytest$", step, re.M)


def test_the_heavy_job_syncs_its_own_venv():
    names = _step_names(_job(HEAVY))
    for name in ("Install uv", "Install Python", "Authenticate to Google Cloud (WIF, read-only)"):
        assert name in names
    assert names.index("uv sync") < names.index(HEAVY)


def test_attest_is_the_last_step_of_lint_and_test_and_its_output():
    block = _job(CHEAP)
    last = _steps(block)[-1]
    assert last.startswith("name: attest\n")
    assert re.search(r"^        id: attest$", last, re.M)
    assert _step_names(block).index("shellcheck") < _step_names(block).index("attest")
    assert re.search(
        r"^    outputs:\n      attested: \$\{\{ steps\.attest\.outputs\.attested \}\}$", block, re.M
    )


def test_attest_reads_gate_local_success_from_the_combined_status_of_the_head():
    step = _steps(_job(CHEAP))[-1]
    assert "GH_TOKEN: ${{ github.token }}" in step
    assert "HEAD: ${{ github.event.pull_request.head.sha || github.sha }}" in step
    assert "/commits/$HEAD/status?per_page=100" in step
    assert 'select(.context == "gate/local" and .state == "success")' in step
    assert 'echo "attested=' in step and '>>"$GITHUB_OUTPUT"' in step


def test_statuses_read_is_granted_on_the_job_that_reads_them():
    """Job-level permissions replace the workflow's: a workflow-level grant
    would not reach a job that declares its own."""
    assert _permissions(_job(CHEAP)) == {
        "contents": "read",
        "id-token": "write",
        "statuses": "read",
    }
    assert _permissions(_job(HEAVY)) == {"contents": "read", "id-token": "write"}
    assert re.search(r"^permissions:\n  contents: read\n\n", CI.read_text(), re.M)


def test_a_schedule_and_a_dispatch_run_the_heavy_tier_on_main():
    """The only runner runs a directly shipped, attested commit gets, and what
    scripts/deploy.sh deploys one from."""
    text = CI.read_text()
    on = text.split("\non:\n", 1)[1].split("\n\n", 1)[0]
    assert re.search(r"^  workflow_dispatch:$", on, re.M)
    assert re.search(
        r"^  schedule:\n(?:    #.*\n)*    - cron: \"[\d*]+ [\d*]+ \* \* \*\"$", on, re.M
    )
