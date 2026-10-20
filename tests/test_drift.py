"""``src.broker.drift``: does live lag origin/main in code that runs (#110)?

Ported from CannObserv/processor's ``tests/test_drift.py`` (at ``540975e``),
itself from CannObserv/status's (status#12), their CR numbers kept. GitHub's
answers are built here in the shapes its REST API returns: the compare
(``status``, ``total_commits``, ``commits``, ``files``) and the CI workflow's
runs. The verdicts are tested on those through a fake ``get``; the getter that
asks GitHub, and the command end to end, against local HTTP stubs
(``tests/http_stub.py``), never the network.

Where broker differs from processor: what runs is ``deploy/broker-*`` rather
than all of ``deploy/``; an unstamped release is ``dev`` or ``unknown``; an
``alert`` carries ``metadata.fault`` (CannObserv/status#51); and co-status's
base is a constant, which the tests repoint at a stub.
"""

import json
import logging
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.broker import checkin, drift, revision
from src.broker.drift import (
    COMPARE_FILES_LIMIT,
    GRACE,
    RUNS_PATH,
    GitHubNotFound,
    GitHubSilent,
    Push,
    Verdict,
    assess,
    counts,
    diff_counts,
    first_look,
    lagging,
    main,
    pushes,
    tip_ci,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 10, 20, 22, 0, tzinfo=UTC)
LIVE = "7fe8f445cc31"


def sha(n: int) -> str:
    """A distinct full-length commit id."""
    return f"{n:040x}"


def commit(n: int, committed: datetime = NOW) -> dict:
    return {"sha": sha(n), "commit": {"committer": {"date": committed.isoformat()}}}


def compare(*shas: int, files=("src/broker/bus_health.py",), status="ahead", total=None) -> dict:
    """GitHub's ``compare/<live>...main``: *shas* oldest first, the last one main's."""
    answer = {
        "status": status,
        "total_commits": len(shas) if total is None else total,
        "commits": [commit(n) for n in shas],
    }
    if files is not None:
        answer["files"] = [{"filename": f} for f in files]
    return answer


def run(
    n: int,
    created: datetime,
    *,
    event="push",
    branch="main",
    status="completed",
    conclusion="success",
) -> dict:
    return {
        "head_sha": sha(n),
        "event": event,
        "head_branch": branch,
        "created_at": created.isoformat().replace("+00:00", "Z"),
        "status": status,
        "conclusion": conclusion if status == "completed" else None,
    }


def runs(*items: dict) -> dict:
    return {"workflow_runs": list(items)}


class TestCounts:
    """What runs is an allowlist: the design's D6 runtime paths, defined once here."""

    @pytest.mark.parametrize(
        "path",
        [
            "src/broker/bus_health.py",
            "src/broker/drift.py",
            "deploy/broker-bus-health.service",
            "deploy/broker-drift.timer",
            "scripts/deploy.sh",
            "pyproject.toml",
            "uv.lock",
        ],
    )
    def test_what_runs_counts(self, path):
        assert counts(path)

    @pytest.mark.parametrize(
        "path",
        [
            # deploy/ but not a unit: installed by hand, never from live (D9).
            "deploy/redis-acl.conf",
            "deploy/redis.conf.broker",
            "deploy/render-acl.sh",
            "deploy/README.md",
            "deploy/redis-server.service.d/broker.conf",
            "deploy/wait-for-tailnet-addr.sh",
            "docs/plans/2026-10-17-22-release-deploy.md",
            "AGENTS.md",
            "CLAUDE.md",
            "tests/test_drift.py",
            ".github/workflows/ci.yml",
            ".claude/settings.json",
            "skills-vendor/gregoryfoster-skills",
            ".skills/context-loss-ok",
            # deploy.sh runs the checkout's copy, never the release's.
            "scripts/sync_wheelhouse.py",
            "scripts/audit_lock.sh",
            ".gitmodules",
            "srcery/x.py",
            "deployment.md",
            "deploy/brokerage.service",
            "",
        ],
    )
    def test_what_never_runs_does_not(self, path):
        assert not counts(path)


class TestDiffCounts:
    def test_docs_alone_do_not_count(self):
        assert not diff_counts(compare(1, files=["docs/a.md", "tests/test_a.py"]))

    def test_one_path_that_runs_is_enough(self):
        assert diff_counts(compare(1, files=["docs/a.md", "src/broker/drift.py"]))

    def test_a_file_moved_out_of_a_runtime_path_counts(self):
        """Its old path left the release: ``previous_filename`` says where it was (status#26)."""
        answer = compare(1, files=[])
        answer["files"] = [{"filename": "docs/old.py", "previous_filename": "src/broker/x.py"}]
        assert diff_counts(answer)

    def test_a_null_previous_filename_is_no_rename(self):
        answer = compare(1, files=[])
        answer["files"] = [{"filename": "docs/a.md", "previous_filename": None}]
        assert not diff_counts(answer)

    def test_no_file_list_counts(self):
        assert diff_counts(compare(1, files=None))

    def test_a_file_list_at_githubs_limit_counts(self):
        """GitHub cuts the list off there; what follows could be anything."""
        assert diff_counts(compare(1, files=["docs/a.md"] * COMPARE_FILES_LIMIT))

    def test_a_commit_list_cut_short_counts(self):
        assert diff_counts(compare(1, files=["docs/a.md"], total=300))


class TestVerdict:
    @pytest.mark.parametrize(
        ("kind", "status"),
        [("ok", "ok"), ("lag", "alert"), ("off_main", "alert"), ("unstamped", "alert"),
         ("test", "alert"), ("silent", None)],
    )  # fmt: skip
    def test_the_check_in_each_kind_sends(self, kind, status):
        assert Verdict(kind, "b", LIVE).status == status

    def test_every_check_in_carries_every_variable(self):
        """The monitor's templates render under StrictUndefined (CannObserv/status#51)."""
        assert Verdict("off_main", "b", LIVE).variables() == {
            "kind": "off_main",
            "live": LIVE,
            "main": "unknown",
            "body": "b",
        }

    @pytest.mark.parametrize("kind", ["lag", "off_main", "unstamped", "test"])
    def test_every_alert_s_fault_is_its_kind(self, kind):
        """The one value co-status compares: a change of fault reports at once (status#28)."""
        assert Verdict(kind, "b", LIVE).metadata() == {"fault": kind}

    def test_an_ok_carries_no_metadata(self):
        assert Verdict("ok", "b", LIVE).metadata() is None


class TestFirstLook:
    @pytest.mark.parametrize(
        ("revision", "says"),
        [
            ("dev", "dev: its release has no REVISION"),
            ("unknown", "unknown: its REVISION is unreadable or empty"),
            ("not-a-sha", "'not-a-sha': its REVISION is not a commit id"),
        ],
    )
    def test_unstamped_alerts_and_says_which(self, revision, says):
        verdict = first_look(revision, compare())
        assert (verdict.kind, verdict.status) == ("unstamped", "alert")
        assert verdict.body == f"live is unstamped ({says})"
        assert (verdict.live, verdict.main) == ("unknown", "unknown"), "status#51's contract"

    def test_identical_is_ok(self):
        verdict = first_look(LIVE, compare(status="identical", files=[]))
        assert verdict == Verdict("ok", f"live {LIVE} is main", LIVE, LIVE)

    @pytest.mark.parametrize("status", ["diverged", "behind"])
    def test_live_not_on_main_alerts(self, status):
        verdict = first_look(LIVE, compare(1, status=status))
        assert verdict.kind == "off_main"
        assert f"live {LIVE} is not on main" in verdict.body
        assert status in verdict.body
        assert verdict.main == sha(1)[:12]

    def test_ahead_in_docs_alone_is_ok(self):
        verdict = first_look(LIVE, compare(1, 2, files=["docs/plans/x.md"]))
        assert verdict.kind == "ok"
        assert "2 commits ahead, none that runs" in verdict.body
        assert verdict.main == sha(2)[:12]

    def test_ahead_in_code_needs_the_clock(self):
        assert first_look(LIVE, compare(1)) is None


class TestPushes:
    def test_each_push_is_its_runs_creation_oldest_first(self):
        found = pushes(
            compare(1, 2, 3),
            runs(run(3, NOW - timedelta(hours=1)), run(1, NOW - timedelta(hours=5))),
        )
        assert found == [
            Push(sha(1), NOW - timedelta(hours=5)),
            Push(sha(3), NOW - timedelta(hours=1)),
        ]

    def test_runs_of_deployed_commits_are_not_pushes_since(self):
        found = pushes(compare(2), runs(run(1, NOW - timedelta(days=2)), run(2, NOW)))
        assert found == [Push(sha(2), NOW)]

    def test_only_push_runs_on_main(self):
        found = pushes(
            compare(1, 2),
            runs(
                run(1, NOW - timedelta(hours=9), event="workflow_dispatch"),
                run(1, NOW - timedelta(hours=8), event="pull_request", branch="110-drift"),
                run(2, NOW),
            ),
        )
        assert found == [Push(sha(2), NOW)]

    def test_a_commit_with_two_push_runs_was_pushed_at_the_first(self):
        found = pushes(compare(1), runs(run(1, NOW), run(1, NOW - timedelta(hours=2))))
        assert found == [Push(sha(1), NOW - timedelta(hours=2))]

    def test_a_tip_with_no_run_falls_back_to_its_commit_date(self):
        """A workflow's push (GITHUB_TOKEN) or ``[skip ci]``: the commit is the only clock."""
        answer = compare(1, 2)
        answer["commits"][1] = commit(2, NOW - timedelta(hours=3))
        found = pushes(answer, runs(run(1, NOW - timedelta(hours=4))))
        assert found == [
            Push(sha(1), NOW - timedelta(hours=4)),
            Push(sha(2), NOW - timedelta(hours=3)),
        ]

    def test_no_runs_at_all_is_the_tips_commit_date(self):
        answer = compare(1)
        answer["commits"][0] = commit(1, NOW - timedelta(hours=3))
        assert pushes(answer, runs()) == [Push(sha(1), NOW - timedelta(hours=3))]

    def test_a_tip_with_no_run_landed_no_earlier_than_the_pushes_under_it(self):
        """An old commit pushed on top with no run: never sorted before main's pushes (CR 10)."""
        answer = compare(1, 2)
        answer["commits"][1] = commit(2, NOW - timedelta(days=3))
        found = pushes(answer, runs(run(1, NOW - timedelta(hours=2))))
        assert found[-1] == Push(sha(2), NOW - timedelta(hours=2)), "the walk relies on it"


class TestTipCi:
    def test_the_newest_push_runs_conclusion(self):
        answer = runs(
            run(2, NOW - timedelta(hours=2), conclusion="failure"),
            run(2, NOW - timedelta(hours=1), conclusion="success"),
            run(1, NOW, conclusion="cancelled"),
        )
        assert tip_ci(answer, sha(2)) == "success"

    def test_unfinished_is_its_status(self):
        assert tip_ci(runs(run(2, NOW, status="in_progress")), sha(2)) == "in_progress"

    def test_no_run(self):
        assert tip_ci(runs(run(1, NOW)), sha(2)) == "no run"


class TestLagging:
    def test_within_grace_is_ok(self):
        """The clock's push, never "in code since": unwalked, it may be docs only (CR 4)."""
        since = Push(sha(1), NOW - GRACE + timedelta(minutes=1))
        verdict = lagging(LIVE, compare(1, 2), since, ci="success", now=NOW)
        assert verdict == Verdict(
            "ok",
            f"live {LIVE}, main {sha(2)[:12]}: 2 commits ahead, the clock started at the push "
            "of 2026-10-20T14:01:00Z (8.0 h ago; grace 8 h)",
            LIVE,
            sha(2)[:12],
        )

    def test_past_grace_is_a_lag_naming_mains_ci(self):
        since = Push(sha(1), NOW - GRACE - timedelta(minutes=30))
        verdict = lagging(LIVE, compare(1), since, ci="failure", now=NOW)
        assert (verdict.kind, verdict.status) == ("lag", "alert")
        assert verdict.body.endswith(
            "(8.5 h ago; grace 8 h). main's CI: failure - deploy.sh refuses it until CI passes"
        )

    def test_past_grace_with_green_ci_says_deploy(self):
        since = Push(sha(1), NOW - timedelta(days=2))
        verdict = lagging(LIVE, compare(1), since, ci="success", now=NOW)
        assert verdict.body.endswith("main's CI: success - scripts/deploy.sh")

    def test_past_grace_with_no_run_says_deploy_the_newest_green_commit(self):
        """A tip a workflow pushed never gets a run, and deploy.sh refuses it at once
        (#106): waiting for CI is the wrong remedy."""
        since = Push(sha(1), NOW - timedelta(days=2))
        verdict = lagging(LIVE, compare(1), since, ci="no run", now=NOW)
        assert verdict.body.endswith(
            "main's CI: no run - scripts/deploy.sh <sha12>, the newest commit with a green run"
        )

    def test_one_commit_is_singular(self):
        since = Push(sha(1), NOW)
        assert "1 commit ahead," in lagging(LIVE, compare(1), since, ci="success", now=NOW).body


class Unrouted(Exception):
    """A path the test did not expect asked; assess must not swallow it."""


class FakeGitHub:
    """``get`` for :func:`assess`: answers by path, records each call."""

    def __init__(self) -> None:
        self.answers: dict[str, object] = {}
        self.calls: list[str] = []

    def route(self, path: str, answer: object) -> None:
        self.answers[path] = answer

    def __call__(self, path: str):
        self.calls.append(path)
        if path not in self.answers:
            raise Unrouted(path)
        answer = self.answers[path]
        if isinstance(answer, Exception):
            raise answer
        return answer


@pytest.fixture
def github() -> FakeGitHub:
    return FakeGitHub()


def _route_compare(github: FakeGitHub, head: str, answer: dict) -> None:
    github.route(f"compare/{LIVE}...{head}", answer)


class TestAssess:
    @pytest.mark.parametrize("revision", ["dev", "unknown", "../../x"])
    def test_unstamped_asks_github_nothing(self, github, revision):
        assert assess(revision, now=NOW, get=github).kind == "unstamped"
        assert not github.calls

    def test_in_sync_is_one_call(self, github):
        _route_compare(github, "main", compare(status="identical", files=[]))
        assert assess(LIVE, now=NOW, get=github).kind == "ok"
        assert len(github.calls) == 1

    def test_docs_alone_never_ask_for_runs(self, github):
        _route_compare(github, "main", compare(1, files=["docs/a.md"]))
        assert assess(LIVE, now=NOW, get=github).kind == "ok"
        assert len(github.calls) == 1

    def test_a_workflow_pushed_docs_commit_is_ok_at_any_age(self, github):
        """#106's note: the weekly context measurement gets no CI run, and touches no
        runtime path, so its lag is ``ok`` without the grace."""
        answer = compare(1, files=[".skills/context-loss-ok", "docs/CONTEXT.md"])
        answer["commits"][0] = commit(1, NOW - timedelta(days=6))
        _route_compare(github, "main", answer)
        assert assess(LIVE, now=NOW, get=github).kind == "ok"

    def test_code_within_grace_is_two_calls(self, github):
        _route_compare(github, "main", compare(1))
        github.route(RUNS_PATH, runs(run(1, NOW - timedelta(hours=1))))
        verdict = assess(LIVE, now=NOW, get=github)
        assert verdict.kind == "ok"
        assert "1.0 h ago" in verdict.body
        assert len(github.calls) == 2

    def test_code_past_grace_is_a_lag(self, github):
        _route_compare(github, "main", compare(1))
        github.route(RUNS_PATH, runs(run(1, NOW - timedelta(hours=9))))
        verdict = assess(LIVE, now=NOW, get=github)
        assert verdict.kind == "lag"
        assert "main's CI: success" in verdict.body
        assert len(github.calls) == 2

    def test_old_docs_then_new_code_starts_the_clock_at_the_code(self, github):
        """Live sat behind a docs push for two days; code pushed an hour ago is not late."""
        _route_compare(github, "main", compare(1, 2))
        github.route(
            RUNS_PATH, runs(run(1, NOW - timedelta(days=2)), run(2, NOW - timedelta(hours=1)))
        )
        _route_compare(github, sha(1), compare(1, files=["docs/a.md"]))
        verdict = assess(LIVE, now=NOW, get=github)
        assert verdict.kind == "ok"
        assert "1.0 h ago" in verdict.body

    def test_old_code_then_new_docs_is_late(self, github):
        _route_compare(github, "main", compare(1, 2))
        github.route(
            RUNS_PATH, runs(run(1, NOW - timedelta(days=2)), run(2, NOW - timedelta(hours=1)))
        )
        _route_compare(github, sha(1), compare(1))
        verdict = assess(LIVE, now=NOW, get=github)
        assert verdict.kind == "lag"
        assert "48.0 h ago" in verdict.body

    @staticmethod
    def _docs_pushes_then_code(github: FakeGitHub, third: timedelta) -> None:
        """Pushes 4 and 3 days ago, docs only; a third *third* ago; main now."""
        _route_compare(github, "main", compare(1, 2, 3, 4))
        github.route(
            RUNS_PATH,
            runs(
                run(1, NOW - timedelta(days=4)),
                run(2, NOW - timedelta(days=3)),
                run(3, NOW - third),
                run(4, NOW),
            ),
        )
        for n in (1, 2, 3):
            _route_compare(github, sha(n), compare(n, files=["docs/a.md"]))

    def test_an_old_tip_with_no_run_on_new_code_is_not_late(self, github):
        """Code pushed 2 h ago under a no-run commit written 3 days ago (CR 10)."""
        answer = compare(1, 2)
        answer["commits"][1] = commit(2, NOW - timedelta(days=3))
        _route_compare(github, "main", answer)
        github.route(RUNS_PATH, runs(run(1, NOW - timedelta(hours=2))))
        verdict = assess(LIVE, now=NOW, get=github)
        assert verdict.kind == "ok"
        assert "2.0 h ago" in verdict.body

    def test_the_walk_stops_at_the_first_push_inside_the_grace(self, github):
        """It, or a newer push, brought the code: ok either way, so ask no further (CR 14)."""
        _route_compare(github, "main", compare(1, 2, 3))
        github.route(
            RUNS_PATH,
            runs(
                run(1, NOW - timedelta(days=2)),
                run(2, NOW - timedelta(hours=1)),
                run(3, NOW),
            ),
        )
        _route_compare(github, sha(1), compare(1, files=["docs/a.md"]))
        verdict = assess(LIVE, now=NOW, get=github)
        assert verdict.kind == "ok"
        assert "1.0 h ago" in verdict.body
        assert f"compare/{LIVE}...{sha(2)}" not in github.calls

    def test_the_walk_stops_at_its_limit_at_the_first_push_unchecked(self, github, monkeypatch):
        """Pushes 1 and 2 are known not to count: the code came with 3 at the earliest (CR 1)."""
        monkeypatch.setattr(drift, "WALK_LIMIT", 2)
        self._docs_pushes_then_code(github, timedelta(days=2))
        verdict = assess(LIVE, now=NOW, get=github)
        assert verdict.kind == "lag", "the code may be as old as push 3"
        assert "48.0 h ago" in verdict.body
        assert len(github.calls) == 2 + 2

    def test_past_the_limit_recent_code_is_not_late(self, github, monkeypatch):
        monkeypatch.setattr(drift, "WALK_LIMIT", 2)
        self._docs_pushes_then_code(github, timedelta(hours=1))
        assert assess(LIVE, now=NOW, get=github).kind == "ok"

    def test_the_most_a_run_asks_is_two_plus_the_walk(self, github):
        """The budget D6 sized: 60 unauthenticated calls an hour, shared with deploy.sh."""
        assert drift.WALK_LIMIT == 8
        _route_compare(github, "main", compare(*range(1, 12)))
        github.route(RUNS_PATH, runs(*(run(n, NOW - timedelta(days=12 - n)) for n in range(1, 12))))
        for n in range(1, 11):
            _route_compare(github, sha(n), compare(n, files=["docs/a.md"]))
        assess(LIVE, now=NOW, get=github)
        assert len(github.calls) == 2 + drift.WALK_LIMIT


class TestAssessWhenGitHubCannotSay:
    """Silent: no check-in at all, so a long outage ends in co-status's ``missing``."""

    def test_a_refusal_is_silent_with_githubs_message(self, github):
        _route_compare(github, "main", GitHubSilent("403 API rate limit exceeded for 1.2.3.4."))
        verdict = assess(LIVE, now=NOW, get=github)
        assert verdict == Verdict(
            "silent", "GitHub did not answer: 403 API rate limit exceeded for 1.2.3.4.", LIVE
        )
        assert verdict.status is None

    def test_live_unknown_to_github_is_off_main(self, github):
        """GitHub answers 404 for a base it does not know: not on main, said so (CR 2)."""
        _route_compare(github, "main", GitHubNotFound("404 Not Found"))
        verdict = assess(LIVE, now=NOW, get=github)
        assert verdict.kind == "off_main"
        assert verdict.body.startswith(f"GitHub does not know live {LIVE} (404 Not Found)")

    def test_a_404_on_anything_else_is_silent(self, github):
        _route_compare(github, "main", compare(1))
        github.route(RUNS_PATH, GitHubNotFound("404 Not Found"))
        verdict = assess(LIVE, now=NOW, get=github)
        assert verdict == Verdict("silent", "GitHub did not answer: 404 Not Found", LIVE)

    @pytest.mark.parametrize("answer", [{"status": "ahead"}, {"status": "ahead", "commits": 3}])
    def test_unexpected_json_is_silent(self, github, answer):
        _route_compare(github, "main", answer)
        verdict = assess(LIVE, now=NOW, get=github)
        assert verdict.kind == "silent"
        assert "not the JSON expected" in verdict.body

    def test_unexpected_json_leaves_its_traceback_in_the_journal(self, github, caplog):
        """A bug here would read as GitHub's fault without it (CR 3)."""
        _route_compare(github, "main", {"status": "ahead"})
        with caplog.at_level("WARNING"):
            assess(LIVE, now=NOW, get=github)
        (record,) = [r for r in caplog.records if r.name == drift.logger.name]
        assert record.exc_info and record.exc_info[0] is KeyError


class TestTheGetter:
    """``drift.github``: GitHub's REST API over HTTPS, unauthenticated, bounded."""

    def test_an_answer_is_its_json_object(self, http_stub):
        http_stub.route("GET", "/compare/a...main", body={"status": "identical"})
        assert drift.github(http_stub.url)("compare/a...main") == {"status": "identical"}
        (request,) = http_stub.requests
        assert request["headers"]["Accept"] == "application/vnd.github+json"
        assert "Authorization" not in request["headers"]

    def test_the_default_is_broker_on_github(self):
        source = Path(drift.__file__).read_text()
        assert 'GITHUB_API = "https://api.github.com/repos/CannObserv/broker"' in source

    def test_a_refusal_names_githubs_message(self, http_stub):
        http_stub.route("GET", "/x", 403, {"message": "API rate limit exceeded for 1.2.3.4."})
        with pytest.raises(GitHubSilent, match="^403 API rate limit exceeded for 1.2.3.4.$"):
            drift.github(http_stub.url)("x")

    def test_a_404_is_its_own_kind(self, http_stub):
        http_stub.route("GET", "/x", 404, {"message": "Not Found"})
        with pytest.raises(GitHubNotFound, match="^404 Not Found$"):
            drift.github(http_stub.url)("x")

    def test_an_error_page_that_is_not_json_is_its_status(self, http_stub):
        http_stub.route("GET", "/x", 502, b"<html>Bad gateway</html>")
        with pytest.raises(GitHubSilent, match="^502$"):
            drift.github(http_stub.url)("x")

    @pytest.mark.parametrize("body", [b"<html>", b"[]", b""], ids=repr)
    def test_an_answer_that_is_not_a_json_object_is_refused(self, http_stub, body):
        """As deploy.sh's gate: an empty or wrong-shaped 200 never passes."""
        http_stub.route("GET", "/x", 200, body)
        with pytest.raises(GitHubSilent, match="not a JSON object"):
            drift.github(http_stub.url)("x")

    def test_unreachable_is_silent(self):
        with pytest.raises(GitHubSilent, match="^URLError"):
            drift.github("http://127.0.0.1:9")("x")

    def test_every_call_shares_one_deadline(self, http_stub, monkeypatch):
        """Bounded inside the unit's TimeoutStartSec, however many calls the walk makes."""
        now = [0.0]
        monkeypatch.setattr(drift, "time", SimpleNamespace(monotonic=lambda: now[0]))
        monkeypatch.setattr(drift, "CHECK_TIMEOUT_SECONDS", 10)
        http_stub.route("GET", "/x", body={})
        http_stub.route("GET", "/y", body={}, delay=0.5)
        get = drift.github(http_stub.url)
        get("x")
        now[0] = 9.875  # the walk so far took 9.875 s: this call gets the 0.125 s left, not 10 s
        with pytest.raises(GitHubSilent, match=r"^TimeoutError: timed out \(timeout 0\.125 s\)$"):
            get("y")

    def test_past_the_deadline_no_call_starts(self, http_stub, monkeypatch):
        monkeypatch.setattr(drift, "CHECK_TIMEOUT_SECONDS", 0)
        with pytest.raises(GitHubSilent, match="^Timeout: no GitHub call starts past 0 s$"):
            drift.github(http_stub.url)("x")
        assert http_stub.requests == []


# --- python -m src.broker.drift, end to end -----------------------------------

MONITOR = "01M46EXP45TVCVAMQXK043N1Q7"
KEY = "sk-test-0123456789abcdef"
CHECKIN = f"/api/v1/monitors/{MONITOR}/checkin"
ACCEPTED = {"monitor_id": MONITOR, "state": "ok"}


class World:
    """GitHub and co-status as local stubs; a release whose REVISION is ``LIVE``."""

    def __init__(self, tmp: Path, github, status, monkeypatch) -> None:
        self.github, self.status = github, status
        self.revision = tmp / "release" / "REVISION"
        self.revision.parent.mkdir()
        self.revision.write_text(f"{LIVE}\n")
        self.credentials = tmp / "credentials"
        self.credentials.mkdir()
        (self.credentials / "status-checkin-key").write_text(f"{KEY}\n")
        monkeypatch.setattr(revision, "REVISION_PATH", self.revision)
        monkeypatch.setattr(drift, "GITHUB_API", github.url)
        monkeypatch.setattr(checkin, "STATUS_CHECKIN_BASE", f"{status.url}/api/v1/monitors")
        monkeypatch.setenv(drift.MONITOR_ID_ENV, MONITOR)
        monkeypatch.setenv("CREDENTIALS_DIRECTORY", str(self.credentials))
        status.route("POST", CHECKIN, 202, ACCEPTED)

    def lag(self, *, hours: float, files=("src/broker/bus_health.py",)) -> None:
        """main one push ahead of live, *hours* ago, touching *files*."""
        self.github.route("GET", f"/compare/{LIVE}...main", body=compare(1, files=files))
        self.github.route(
            "GET", f"/{RUNS_PATH}", body=runs(run(1, datetime.now(UTC) - timedelta(hours=hours)))
        )

    def checkins(self) -> list[dict]:
        return [json.loads(r["body"]) for r in self.status.requests]


@pytest.fixture
def world(tmp_path, http_stub, http_stub_2, monkeypatch, _restore_root_logging) -> World:
    return World(tmp_path, http_stub, http_stub_2, monkeypatch)


@pytest.fixture
def _restore_root_logging():
    # main() reconfigures the process-wide root logger; put pytest's back after.
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    yield
    root.handlers[:] = handlers
    root.setLevel(level)


def records(capsys) -> list[dict]:
    return [json.loads(line) for line in capsys.readouterr().out.splitlines() if line]


def outcome(capsys) -> dict:
    (record,) = [r for r in records(capsys) if r["message"] == "drift check"]
    return record


class TestTheCommand:
    def test_in_sync_is_an_ok_with_every_variable_and_no_metadata(self, world, capsys):
        world.github.route(
            "GET", f"/compare/{LIVE}...main", body=compare(status="identical", files=[])
        )
        assert main([]) == 0
        (checkin_body,) = world.checkins()
        assert checkin_body == {
            "status": "ok",
            "variables": {"kind": "ok", "live": LIVE, "main": LIVE, "body": f"live {LIVE} is main"},
        }
        record = outcome(capsys)
        assert record["level"] == "INFO"
        assert (record["build"], record["kind"], record["checkin"]) == (LIVE, "ok", 202)
        assert record["logger"] == "src.broker.drift"

    def test_a_runtime_lag_past_the_grace_alerts_with_its_fault(self, world, capsys):
        world.lag(hours=9)
        assert main([]) == 0
        (checkin_body,) = world.checkins()
        assert checkin_body["status"] == "alert"
        assert checkin_body["metadata"] == {"fault": "lag"}
        assert checkin_body["variables"]["kind"] == "lag"
        assert checkin_body["variables"]["live"] == LIVE
        assert checkin_body["variables"]["main"] == sha(1)[:12]
        assert "main's CI: success - scripts/deploy.sh" in checkin_body["variables"]["body"]
        record = outcome(capsys)
        assert record["level"] == "WARNING"
        assert (record["build"], record["kind"], record["checkin"]) == (LIVE, "lag", 202)

    def test_a_docs_only_lag_is_ok_however_old(self, world, capsys):
        world.lag(hours=72, files=["docs/BUS-HEALTH.md", "deploy/redis-acl.conf"])
        assert main([]) == 0
        (checkin_body,) = world.checkins()
        assert checkin_body["status"] == "ok"
        assert "metadata" not in checkin_body
        assert outcome(capsys)["level"] == "INFO"

    def test_a_runtime_lag_within_the_grace_is_ok(self, world):
        world.lag(hours=1)
        assert main([]) == 0
        assert world.checkins()[0]["status"] == "ok"

    @pytest.mark.parametrize(("content", "which"), [(None, "dev"), ("", "unknown")])
    def test_an_unstamped_release_alerts_asking_github_nothing(self, world, content, which):
        if content is None:
            world.revision.unlink()
        else:
            world.revision.write_text(content)
        assert main([]) == 0
        assert world.github.requests == []
        (checkin_body,) = world.checkins()
        assert checkin_body["metadata"] == {"fault": "unstamped"}
        assert checkin_body["variables"]["live"] == "unknown"
        assert f"({which}" in checkin_body["variables"]["body"]

    def test_the_check_in_is_one_post_with_the_key_in_its_header_only(self, world, capsys):
        world.lag(hours=1)
        main([])
        (request,) = world.status.requests
        assert (request["method"], request["path"]) == ("POST", CHECKIN)
        assert request["headers"]["X-API-Key"] == KEY
        assert KEY not in request["body"].decode()
        assert KEY not in capsys.readouterr().out

    def test_github_silent_sends_nothing_and_fails(self, world, capsys):
        """Never a false ok: silence, which co-status's grace turns into missing."""
        world.github.route("GET", f"/compare/{LIVE}...main", 403, {"message": "rate limited"})
        assert main([]) == 1
        assert world.status.requests == []
        record = outcome(capsys)
        assert (record["level"], record["kind"], record["checkin"]) == ("WARNING", "silent", None)
        assert "rate limited" in record["body"]

    @pytest.mark.parametrize(
        ("code", "body", "reason"),
        [
            (500, b"oops", "500"),
            (401, {"detail": "invalid API key"}, "401 invalid API key"),
            # A failed template preview records nothing: a failed run (status#51 rule 2).
            (422, {"detail": {"section": "title", "message": "undefined"}}, "422"),
            (200, {}, "200"),
        ],
    )
    def test_status_refusing_fails_once_with_no_retry(self, world, capsys, code, body, reason):
        world.lag(hours=9)
        world.status.route("POST", CHECKIN, code, body)
        assert main([]) == 1
        assert len(world.status.requests) == 1
        record = outcome(capsys)
        assert record["level"] == "ERROR"
        assert record["checkin"].startswith(f"failed: {reason}")

    def test_status_unreachable_fails(self, world, monkeypatch, capsys):
        """A co-status outage costs this check its exit code, nothing else."""
        world.lag(hours=1)
        monkeypatch.setattr(checkin, "STATUS_CHECKIN_BASE", "http://127.0.0.1:9/api/v1/monitors")
        assert main([]) == 1
        assert outcome(capsys)["checkin"].startswith("failed: URLError")

    def test_no_key_fails_without_asking_status(self, world, capsys):
        (world.credentials / "status-checkin-key").write_text("\n")  # SetCredential's fallback
        world.lag(hours=1)
        assert main([]) == 1
        assert world.status.requests == []
        assert "status-checkin-key" in outcome(capsys)["checkin"]

    def test_a_malformed_key_never_reaches_the_journal(self, world, capsys):
        """A key pasted with a line break in it (processor CR 1)."""
        (world.credentials / "status-checkin-key").write_text(f"{KEY}\n{KEY}\n")
        world.lag(hours=1)
        assert main([]) == 1
        assert world.status.requests == []
        out = capsys.readouterr().out
        assert KEY not in out
        assert '"message": "drift check"' in out

    def test_no_monitor_id_fails_without_asking_status(self, world, monkeypatch, capsys):
        monkeypatch.delenv(drift.MONITOR_ID_ENV)
        world.lag(hours=1)
        assert main([]) == 1
        assert world.status.requests == []
        assert outcome(capsys)["checkin"] == f"not sent: missing {drift.MONITOR_ID_ENV}"

    def test_both_missing_are_named_in_one_line(self, world, monkeypatch, capsys):
        monkeypatch.delenv(drift.MONITOR_ID_ENV)
        (world.credentials / "status-checkin-key").write_text("\n")
        world.lag(hours=1)
        assert main([]) == 1
        assert outcome(capsys)["checkin"] == (
            f"not sent: missing {drift.MONITOR_ID_ENV}, the status-checkin-key credential"
        )

    @pytest.mark.parametrize("bad", ["../../admin", "01m5cvmefpkrgp2y48f7qttp7d", "01M5CVMEF"])
    def test_a_monitor_id_that_is_not_a_ulid_is_refused_unasked(
        self, world, monkeypatch, capsys, bad
    ):
        monkeypatch.setenv(drift.MONITOR_ID_ENV, bad)
        assert main([]) == 2
        assert world.status.requests == [] and world.github.requests == []
        (record,) = records(capsys)
        assert record["message"] == "invalid settings"
        assert record["level"] == "ERROR"

    def test_test_alert_sends_one_alert_and_asks_github_nothing(self, world, capsys):
        assert main(["--test-alert"]) == 0
        assert world.github.requests == []
        (checkin_body,) = world.checkins()
        assert checkin_body["status"] == "alert"
        assert checkin_body["metadata"] == {"fault": "test"}
        assert checkin_body["variables"]["kind"] == "test"
        assert set(checkin_body["variables"]) == {"kind", "live", "main", "body"}
        assert checkin_body["variables"]["live"] == LIVE
        assert "sent by hand" in checkin_body["variables"]["body"]
        assert outcome(capsys)["kind"] == "test"


def test_the_drift_check_imports_no_redis_and_no_co_core():
    """It holds no Redis credential and has no client to use one with (D6, B8):
    ``ACL LOG`` cannot gain an entry from it."""
    probe = "import sys, src.broker.drift; print(sorted({'redis', 'co_core'} & set(sys.modules)))"
    out = subprocess.run(
        [sys.executable, "-c", probe], cwd=REPO_ROOT, capture_output=True, text=True, check=True
    ).stdout
    assert out.strip() == "[]"
