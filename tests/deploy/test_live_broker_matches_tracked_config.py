"""The *running* broker must match the tracked config, not just the files.

The gap this closes was named as unfixable in the test it sits beside: file
parity cannot see a broker whose running config was changed by ``CONFIG SET``,
which is precisely how the cap is meant to be applied without a restart
(``deploy/README.md``, "Changing the cap"). ``CONFIG SET`` is not persisted, so
it can drift the running broker from the tracked file in either direction and no
comparison of files can tell.

Reading the values back through ``CONFIG GET`` catches both directions at once,
and it needs no privilege: the probe's own ``BROKER_REDIS_URL`` is enough, where
reading ``/etc/redis/redis.conf`` would need root or a group membership that
widens who can see the credential.

Skips unless ``BROKER_REDIS_URL`` is set and the broker answers, so CI and dev
clones pass. On the broker node, source the env first - and as
``set -a; . /etc/broker/.env; set +a``, never ``export $(cat ... | xargs)``,
which silently corrupts values.
"""

import os

import pytest

from tests.deploy.test_installed_redis_config_matches_repo import (
    REPO_REDIS_CONF,
    REQUIREPASS_PLACEHOLDER,
    parse_directives,
    parse_size,
)

# Compared as bytes rather than as spelling: redis reports `512mb` as
# 536870912, and a test that demanded the literal would fail on a config that is
# correct.
SIZE_VALUED = {"maxmemory"}

# The secret, which the tracked copy templates on purpose. Asserting the live
# broker HAS a password is worth doing; asserting WHICH one belongs nowhere a
# test failure could print it.
NOT_COMPARED = {"requirepass"}


@pytest.fixture(scope="module")
def live_config() -> dict[str, str]:
    url = os.environ.get("BROKER_REDIS_URL")
    if not url:
        pytest.skip("BROKER_REDIS_URL not set - not a host with broker credentials")

    redis = pytest.importorskip("redis")
    client = redis.Redis.from_url(url, socket_connect_timeout=2, socket_timeout=2)
    try:
        config = client.config_get("*")
    except redis.exceptions.RedisError as e:
        pytest.skip(f"broker not answering: {e!r}")
    finally:
        client.close()
    return {
        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
        for k, v in config.items()
    }


def test_every_tracked_directive_is_in_force(live_config) -> None:
    """One assertion per directive would hide the rest behind the first
    failure; drift usually arrives as a set."""
    tracked = parse_directives(REPO_REDIS_CONF.read_text())
    mismatches = []
    for name, declared in tracked.items():
        if name in NOT_COMPARED:
            continue
        live = live_config.get(name)
        if name in SIZE_VALUED:
            if live is None or parse_size(live) != parse_size(declared):
                mismatches.append((name, declared, live))
        elif live != declared:
            mismatches.append((name, declared, live))
    assert not mismatches, f"running broker differs from {REPO_REDIS_CONF.name}: {mismatches}"


def test_the_live_broker_requires_a_password(live_config) -> None:
    """D3's floor, asserted without naming the value. An empty ``requirepass``
    on a tailnet-bound broker is R2: reachable by every node the ACL admits,
    including user-owned ones."""
    assert live_config.get("requirepass", "") not in ("", REQUIREPASS_PLACEHOLDER)


def test_the_cap_is_live_and_nonzero(live_config) -> None:
    """The one that would matter on its own. ``noeviction`` with ``maxmemory 0``
    is inert, and the way it gets there is a ``CONFIG SET maxmemory 0`` that no
    file records."""
    assert live_config["maxmemory-policy"] == "noeviction"
    assert parse_size(live_config["maxmemory"]) > 0
