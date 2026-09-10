"""The node's Redis configuration must be what this repo says it is.

Carried over from CannObserv/archiver (archiver#128) under archiver#193 D6 and
**reconciled with the node** by broker#1 Phase 5. The reconciliation is the
point: what arrived here was a drop-in overriding ``ExecStart``, because in
archiver a drop-in was the only mechanism tuning the broker. On this node it was
never the mechanism - Phase 2 appended the tuning to ``/etc/redis/redis.conf``
and gave the drop-in slot to tailnet ordering - so the repo tracked a file
nothing installed and this test pointed at a path nothing wrote. That is exactly
the drift a parity test exists to catch, and it was catching it about itself.

So the repo now tracks the three artifacts the node actually deploys:

===========================================  ===================================
``deploy/redis.conf.broker``                 appended to ``/etc/redis/redis.conf``
``deploy/redis-server.service.d/broker.conf``  the drop-in of the same name
``deploy/wait-for-tailnet-addr.sh``          ``/usr/local/sbin/``
===========================================  ===================================

The drift being guarded is sharper than a stale flag. ``maxmemory-policy
noeviction`` without an explicit ``maxmemory`` is *inert*: with the default
``maxmemory 0`` there is no ceiling to refuse writes at, so the bounded "error
and let the producer retry" degradation the config documents never engages, and
an untrimmed stream grows until the kernel OOM-kills ``redis-server``. A config
that says ``noeviction`` while the broker runs uncapped reads as protection and
provides none.

Assertions, deliberately split by what they can run against:

- **Pure, always in CI** - the tracked config declares a non-zero cap, carries
  no secret, and agrees with the drop-in about this node's tailnet address.
- **Installed parity, skips when absent** - the two files under
  ``/etc/systemd/system`` and ``/usr/local/sbin`` match their tracked copies, so
  CI and dev clones pass and only a host actually running the broker is
  asserted on.

**Scope limit, and where it is covered.** All of this compares *files*. It
cannot see a broker whose running config was changed by ``CONFIG SET``, which is
how the cap is applied without a restart.
``test_live_broker_matches_tracked_config.py`` closes that from the other side
by reading the live values back; each participant's ``check_redis_floor.sh``
reads ``maxmemory`` at its own service start, warn-only; and the bus-health
probe reports ``maxmemory 0`` as a finding every tick.

``/etc/redis/redis.conf`` itself is deliberately **not** compared here: it is
``0640 redis:redis`` and holds the credential, so a test that could read it
would need either root or a group membership that widens who can see the
password. The live-config test asserts the same content through ``CONFIG GET``
instead, which needs no privilege and catches more.
"""

import re
from pathlib import Path

import pytest

DEPLOY = Path(__file__).resolve().parents[2] / "deploy"

REPO_REDIS_CONF = DEPLOY / "redis.conf.broker"
REPO_DROPIN = DEPLOY / "redis-server.service.d" / "broker.conf"
REPO_WAIT_SCRIPT = DEPLOY / "wait-for-tailnet-addr.sh"

INSTALLED_DROPIN = Path("/etc/systemd/system/redis-server.service.d/broker.conf")
INSTALLED_WAIT_SCRIPT = Path("/usr/local/sbin/wait-for-tailnet-addr.sh")

# The placeholder the install substitutes from /etc/redis/broker-password.
REQUIREPASS_PLACEHOLDER = "__REQUIREPASS__"

_SIZE_UNITS = {
    "k": 1000,
    "kb": 1024,
    "m": 1000**2,
    "mb": 1024**2,
    "g": 1000**3,
    "gb": 1024**3,
}


def parse_directives(text: str) -> dict[str, str]:
    """The redis.conf subset this repo writes: ``<name> <value>``, one per line,
    ``#`` comments and blanks ignored.

    Judges the *value*, never its spelling, which is why the cap assertion below
    cannot be satisfied by a config that merely contains the word ``maxmemory``.
    """
    directives: dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        name, _, value = line.partition(" ")
        directives[name.strip()] = value.strip()
    return directives


def parse_size(value: str) -> int:
    """Redis's own memory-value grammar: ``1k`` is 1000 and ``1kb`` is 1024."""
    match = re.fullmatch(r"(\d+)\s*([a-zA-Z]*)", value.strip())
    assert match, f"not a redis size value: {value!r}"
    amount, unit = int(match.group(1)), match.group(2).lower()
    return amount * _SIZE_UNITS.get(unit, 1)


# --- pure: runs in CI and in a dev clone ---


def test_tracked_config_sets_an_explicit_nonzero_maxmemory() -> None:
    """noeviction WITHOUT a cap is inert (CannObserv/archiver#128). This is the
    invariant that survives any future decision about *where* the tuning lives.

    **And the policy is load-bearing beyond refusing writes** (broker#9).
    Replicator's ``replicator:cmd:*`` de-duplication keys are the only volatile
    keys on db0 - every other tenant writes streams, which never carry a TTL -
    so under any ``volatile-*`` policy that namespace is the *only* eviction
    candidate on the instance. Memory pressure would evict precisely it and
    nothing else, leaving every stream, group and PEL intact and every issuer
    none the wiser. The two failure modes are not comparable: evicting those
    keys is silent, and the first symptom is a window of duplicate fetches at
    live origins; refusing the write is loud, bounded, and every producer
    retries through it (broker#1 R5). So the change that looks safest under
    memory pressure - evict something rather than refuse writes - picks the
    silent failure, and picks the one namespace nobody would choose. The
    keyspace half of that claim is asserted live by
    ``test_live_broker_matches_tracked_config.py``.
    """
    directives = parse_directives(REPO_REDIS_CONF.read_text())
    assert directives.get("maxmemory-policy") == "noeviction"
    assert parse_size(directives["maxmemory"]) > 0


def test_tracked_config_carries_no_secret() -> None:
    """The tracked copy is templated; the value comes from
    /etc/redis/broker-password at install time. A real password committed here
    would be readable by everyone with repo access, which is a strictly larger
    set than everyone with root on the broker."""
    assert parse_directives(REPO_REDIS_CONF.read_text())["requirepass"] == REQUIREPASS_PLACEHOLDER


def test_the_tailnet_address_agrees_across_the_two_tracked_files() -> None:
    """This node's tailnet address is stated twice - once in the ``bind`` list
    and once as the argument to the boot-race wait - and the two disagreeing is
    silent in the worst way. A ``bind`` naming an address the wait does not
    check re-opens observo#473 (redis races tailscaled and crash-loops); a wait
    checking an address redis does not bind blocks the boot for nothing.

    Non-ephemeral tags plus ``tailscaled.state`` make the address stable, so
    this is not defending against churn. It is defending against a hand-edit of
    one file and not the other.
    """
    bind = parse_directives(REPO_REDIS_CONF.read_text())["bind"].split()
    tailnet = [addr for addr in bind if addr != "127.0.0.1"]
    assert len(tailnet) == 1, f"expected exactly one non-loopback bind, got {tailnet}"

    exec_start_pre = re.search(
        r"^ExecStartPre=\+\S*wait-for-tailnet-addr\.sh\s+(\S+)",
        REPO_DROPIN.read_text(),
        re.MULTILINE,
    )
    assert exec_start_pre, "the drop-in must wait for the tailnet address before redis binds it"
    assert exec_start_pre.group(1) == tailnet[0]


def test_the_dropin_does_not_override_execstart() -> None:
    """The drop-in slot is for unit ordering only (Phase 5).

    An ``ExecStart=`` here would restate tuning that redis.conf already owns,
    giving the same setting two homes and no adjudication between them - and the
    cap in particular must stay in one place, since it is half of a decision
    made in two repositories (archiver#193 R5).
    """
    assert "ExecStart=/" not in REPO_DROPIN.read_text()


# --- installed parity: skips off the broker node ---


def _assert_installed_matches(installed: Path, repo_copy: Path) -> None:
    if not installed.exists():
        pytest.skip(f"{installed} absent - not the broker node")
    assert installed.read_text() == repo_copy.read_text(), (
        f"{installed} has drifted from {repo_copy.name}"
    )


def test_installed_dropin_matches_repo() -> None:
    _assert_installed_matches(INSTALLED_DROPIN, REPO_DROPIN)


def test_installed_wait_script_matches_repo() -> None:
    """The wait script is R1's insurance and the least visible of the three: it
    lives outside both /etc and the repo's usual reach, and nothing else would
    notice it being edited in place."""
    _assert_installed_matches(INSTALLED_WAIT_SCRIPT, REPO_WAIT_SCRIPT)


def test_installed_wait_script_is_executable() -> None:
    """A non-executable ExecStartPre fails 203/EXEC, which is the same symptom
    Ubuntu's NoExecPaths=/ sandbox produces and would send the next person
    reading this down the wrong path (broker#1 Phase 2 F3)."""
    if not INSTALLED_WAIT_SCRIPT.exists():
        pytest.skip(f"{INSTALLED_WAIT_SCRIPT} absent - not the broker node")
    assert INSTALLED_WAIT_SCRIPT.stat().st_mode & 0o111
