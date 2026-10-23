"""shellcheck over this repo's own shell scripts, at commit time and in CI (#112).

The scripts were checked by nothing but ``bash -n`` and the tests that run them,
and every fix #106's review made to ``scripts/deploy.sh`` was a trap this class
of linter is built for: ``set -e`` not reaching into a command substitution, a
failed ``sudo`` with no message.

**One version, locked.** ``shellcheck-py`` sits in the ``lint`` dependency group,
so ``uv.lock`` hash-pins its wheel, ``scripts/audit_lock.sh`` audits it, and the
node, CI and every clone run the same binary. No root install on the node (a
patching-posture decision) and no hook repo fetched at run time (the unpinned
pattern #113 removed).

**Two callers, one selector.** ``scripts/shellcheck.sh`` decides which files are
this repo's: every tracked ``*.sh`` and ``.githooks/*``, never a symlink (the
``.claude/hooks/`` ones point into ``skills-vendor/``) and never ``.skills/``
(the vendored refresh hook rewrites ``doctor.sh`` there with no review, as
ruff's ``extend-exclude`` decides for ``skills-vendor/``). ``.githooks/pre-commit``
runs it over what is staged; ``ci.yml``'s ``lint + test`` job over the tree. The
hook is per clone and ``--no-verify`` skips it, so CI is the enforcement, and a
step in the existing job keeps ``scripts/deploy.sh``'s ``CI_JOBS`` gate whole.

The hook runs here in a scratch repository under a fake ``uv`` that hands each
tool to this venv's own copy, so nothing is resolved or fetched.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SELECTOR = REPO_ROOT / "scripts" / "shellcheck.sh"
HOOK = REPO_ROOT / ".githooks" / "pre-commit"
CI_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yml"
DEPLOY = REPO_ROOT / "scripts" / "deploy.sh"
CI_DOC = REPO_ROOT / "docs" / "CI.md"
VENV_BIN = Path(sys.executable).parent

#: How both callers reach the locked tools: no project, so no co-core and no
#: wheelhouse, and the lock as committed.
UV_RUN = ["run", "--frozen", "--only-group", "lint"]


def _fake_uv(tmp_path: Path) -> tuple[dict[str, str], Path]:
    """A ``uv`` that records each argv and runs ``uv run ... <tool>`` as this
    venv's ``<tool>``. Returns the environment to run under and the argv log."""
    bin_dir = tmp_path / "fake-bin"
    bin_dir.mkdir()
    log = tmp_path / "uv-argv"
    fake = bin_dir / "uv"
    prefix, n = " ".join(UV_RUN), len(UV_RUN)
    fake.write_text(
        "#!/usr/bin/env bash\n"
        f'printf \'%s\\n\' "$*" >> "{log}"\n'
        f'[ "${{*:1:{n}}}" = "{prefix}" ] || {{ echo "fake uv: $*" >&2; exit 99; }}\n'
        f"shift {n}\n"
        f'tool=$1; shift\nexec "{VENV_BIN}/$tool" "$@"\n'
    )
    fake.chmod(0o755)
    env = os.environ | {"PATH": f"{bin_dir}:{os.environ['PATH']}", "HOME": str(tmp_path)}
    return env, log


def _git(cwd: Path, *args: str, env: dict[str, str] | None = None):
    return subprocess.run(
        ["git", *args], cwd=cwd, env=env, capture_output=True, text=True, timeout=60
    )


def _own_scripts() -> set[str]:
    """What the selector should take, worked out here from the index."""
    listing = _git(REPO_ROOT, "ls-files", "-s").stdout.splitlines()
    own = set()
    for line in listing:
        mode, _, _, path = line.split(maxsplit=3)
        if mode == "120000" or path.startswith(("skills-vendor/", ".skills/")):
            continue
        if path.endswith(".sh") or path.startswith(".githooks/"):
            own.add(path)
    return own


class TestTheLock:
    def test_the_lint_group_carries_shellcheck_and_dev_includes_it(self):
        groups = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())["dependency-groups"]
        assert any(re.match(r"shellcheck-py\b", spec) for spec in groups["lint"])
        assert any(re.match(r"ruff\b", spec) for spec in groups["lint"])
        assert {"include-group": "lint"} in groups["dev"]
        assert not any(isinstance(s, str) and s.startswith("ruff") for s in groups["dev"]), (
            "ruff lives in lint alone, so the hook and CI read one pin"
        )

    def test_the_binary_is_a_hashed_linux_x86_64_wheel(self):
        """The node and CI's runner are both linux x86_64. A wheel carries the
        binary; an sdist would fetch it at build time, unhashed."""
        lock = tomllib.loads((REPO_ROOT / "uv.lock").read_text())
        (pkg,) = [p for p in lock["package"] if p["name"] == "shellcheck-py"]
        wheels = [w for w in pkg["wheels"] if "manylinux" in w["url"] and "x86_64" in w["url"]]
        assert wheels, pkg["wheels"]
        assert all(w["hash"].startswith("sha256:") for w in wheels)

    def test_the_doc_names_the_locked_version(self):
        """docs/CI.md says which shellcheck the gate runs. A bump is one
        ``uv lock`` away, so the doc is held to the lock: shellcheck-py's
        fourth field is its own packaging release, the first three the binary's."""
        lock = tomllib.loads((REPO_ROOT / "uv.lock").read_text())
        (pkg,) = [p for p in lock["package"] if p["name"] == "shellcheck-py"]
        binary = ".".join(pkg["version"].split(".")[:3])
        reported = subprocess.run(
            [VENV_BIN / "shellcheck", "--version"], capture_output=True, text=True, check=True
        ).stdout
        assert f"version: {binary}\n" in reported
        assert f"shellcheck {binary} from `shellcheck-py`" in CI_DOC.read_text()


class TestTheSelector:
    def test_it_takes_every_own_script_and_no_upstream_one(self, tmp_path):
        env, log = _fake_uv(tmp_path)
        result = subprocess.run(
            ["bash", str(SELECTOR)], cwd=tmp_path, env=env, capture_output=True, text=True
        )
        assert result.returncode == 0, result.stdout + result.stderr
        (call,) = log.read_text().splitlines()
        argv = call.split()
        assert argv[: len(UV_RUN) + 2] == [*UV_RUN, "shellcheck", "--"]
        taken = set(argv[len(UV_RUN) + 2 :])
        assert taken == _own_scripts()
        assert {"scripts/deploy.sh", "scripts/shellcheck.sh", ".githooks/pre-commit"} <= taken
        assert ".skills/doctor.sh" not in taken
        assert not any(p.startswith(".claude/") for p in taken)

    def test_the_tree_is_clean(self, tmp_path):
        """The gate starts green: the same run CI's step makes."""
        env, _ = _fake_uv(tmp_path)
        result = subprocess.run(
            ["bash", str(SELECTOR)], cwd=REPO_ROOT, env=env, capture_output=True, text=True
        )
        assert result.returncode == 0, result.stdout + result.stderr


class TestTheHook:
    @pytest.fixture
    def clone(self, tmp_path):
        """A scratch repository with the hook enabled the way docs/CI.md says."""
        env, log = _fake_uv(tmp_path)
        repo = tmp_path / "repo"
        for src in (SELECTOR, HOOK):
            dest = repo / src.relative_to(REPO_ROOT)
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dest)
        _git(repo, "init", "-q", "-b", "main")
        for key, value in (("user.name", "t"), ("user.email", "t@example.invalid")):
            _git(repo, "config", key, value)
        _git(repo, "config", "core.hooksPath", ".githooks")
        _git(repo, "add", "-A")
        seeded = _git(repo, "commit", "-q", "-m", "seed", env=env)
        assert seeded.returncode == 0, seeded.stdout + seeded.stderr
        return repo, env, log

    @staticmethod
    def _commit(repo: Path, env: dict[str, str], **files: str):
        for name, text in files.items():
            path = repo / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text)
        _git(repo, "add", "-A")
        return _git(repo, "commit", "-q", "-m", "change", env=env)

    @staticmethod
    def _head(repo: Path) -> str:
        return _git(repo, "rev-parse", "HEAD").stdout.strip()

    def test_a_planted_finding_fails_the_commit(self, clone):
        repo, env, _ = clone
        before = self._head(repo)
        result = self._commit(repo, env, **{"bad.sh": '#!/usr/bin/env bash\ncd "$1"\n'})
        assert result.returncode != 0
        assert "SC2164" in result.stdout + result.stderr
        assert self._head(repo) == before

    def test_a_clean_script_commits(self, clone):
        repo, env, log = clone
        seen = len(log.read_text().splitlines())
        result = self._commit(repo, env, **{"ok.sh": '#!/usr/bin/env bash\ncd "$1" || exit\n'})
        assert result.returncode == 0, result.stdout + result.stderr
        assert log.read_text().splitlines()[seen:] == [f"{' '.join(UV_RUN)} shellcheck -- ok.sh"]

    @pytest.mark.parametrize(
        ("text", "rule"),
        [("import os\n", "F401"), ("x = {  'a':1 }\n", "would be reformatted")],
    )
    def test_staged_python_is_ruff_checked_and_format_checked(self, clone, text, rule):
        repo, env, _ = clone
        before = self._head(repo)
        result = self._commit(repo, env, **{"mod.py": text})
        assert result.returncode != 0
        assert rule in result.stdout + result.stderr
        assert self._head(repo) == before

    def test_upstream_scripts_are_not_its_to_lint(self, clone):
        """A symlink (a ``.claude/hooks/`` one points into ``skills-vendor/``),
        ``skills-vendor/`` itself and ``.skills/`` are upstream's."""
        repo, env, log = clone
        bad = '#!/usr/bin/env bash\ncd "$1"\n'
        (repo / "upstream").mkdir()
        (repo / "upstream" / "x.sh").write_text(bad)
        os.symlink("../upstream/x.sh", repo / "linked.sh")
        _git(repo, "add", "upstream/x.sh")
        _git(repo, "commit", "-q", "--no-verify", "-m", "upstream")
        seen = len(log.read_text().splitlines())
        result = self._commit(repo, env, **{"skills-vendor/v/x.sh": bad, ".skills/doctor.sh": bad})
        assert result.returncode == 0, result.stdout + result.stderr
        assert log.read_text().splitlines()[seen:] == []

    def test_a_deletion_lints_nothing(self, clone):
        repo, env, _ = clone
        self._commit(repo, env, **{"ok.sh": "#!/usr/bin/env bash\ntrue\n"})
        _git(repo, "rm", "-q", "ok.sh")
        result = _git(repo, "commit", "-q", "-m", "rm", env=env)
        assert result.returncode == 0, result.stdout + result.stderr


class TestTheWiring:
    def test_ci_runs_it_as_a_step_of_lint_and_test(self):
        """A step of a job ``scripts/deploy.sh``'s ``CI_JOBS`` requires green,
        so its gate covers the lint with no new entry; test_deploy.py holds
        ``CI_JOBS`` to ``ci.yml``."""
        required = re.search(r"^CI_JOBS=\((.*)\)$", DEPLOY.read_text(), re.M)
        assert required, "deploy.sh names its jobs in CI_JOBS=(...)"
        assert "lint + test" in re.findall(r'"([^"]+)"', required.group(1))
        jobs = CI_WORKFLOW.read_text().split("\njobs:\n", 1)[1]
        blocks = re.split(r"^  [\w-]+:\n", jobs, flags=re.M)
        (job,) = [b for b in blocks if re.search(r"^    name: lint \+ test$", b, re.M)]
        runs = re.findall(r"^        run: (.+)$", job, re.M)
        assert "bash scripts/shellcheck.sh" in runs
        assert runs.index("uv sync --group dev") < runs.index("bash scripts/shellcheck.sh")

    def test_the_hook_runs_the_selector_and_ruff_on_whats_staged(self):
        text = HOOK.read_text()
        assert os.access(HOOK, os.X_OK)
        assert os.access(SELECTOR, os.X_OK)
        assert "scripts/shellcheck.sh --staged" in text
        assert "ruff check" in text and "ruff format --check" in text

    def test_the_install_line_is_documented(self):
        assert "git config core.hooksPath .githooks" in CI_DOC.read_text()
