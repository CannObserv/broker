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

One assertion here is about the live *keyspace* rather than the live config
(``test_the_dedupe_keys_are_the_only_volatile_keys_on_the_instance``): it sits
in this module because it needs the same credential and the same "the files
cannot see this" argument, and because what it pins is the premise of
``maxmemory-policy`` being worth pinning at all - see ``docs/STREAMS.md``,
"``noeviction`` is load-bearing beyond refusing writes".

Skips unless ``BROKER_REDIS_URL`` is set and the broker answers, so CI and dev
clones pass. On the broker node, source the env first - and as
``set -a; . /etc/broker/.env; set +a``, never ``export $(cat ... | xargs)``,
which silently corrupts values.
"""

import time

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
def live_config(live_client) -> dict[str, str]:
    return live_client.config_get("*")


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


def _volatile_versus_dedupe(client) -> tuple[int, int] | None:
    """``(volatile keys, distinct dedupe keys)`` on the connection's own database,
    or ``None`` when that database is empty.

    The database index is read off the connection rather than spelled ``db0``:
    ``INFO keyspace`` reports every database and ``SCAN`` only reaches the one
    ``BROKER_REDIS_URL`` selected, so a hard-coded name would silently compare
    two different keyspaces if that URL ever carried another suffix.

    Distinct, because ``SCAN`` guarantees at-least-once and may return the same
    key twice when the hash table rehashes mid-iteration - which would overcount
    the dedupe side and fail the equality below for no reason.
    """
    db = client.get_connection_kwargs().get("db", 0)
    keyspace = client.info("keyspace").get(f"db{db}")
    if not keyspace:
        return None
    return keyspace["expires"], len(set(client.scan_iter(match="replicator:cmd:*", count=1000)))


def test_the_dedupe_keys_are_the_only_volatile_keys_on_the_instance(live_client) -> None:
    """The claim that makes ``noeviction`` load-bearing, asserted rather than
    assumed (CannObserv/broker#9).

    A ``volatile-*`` policy is only catastrophic here *because* the eviction
    candidate set is exactly one tenant's namespace. That is a property of the
    live keyspace, not of any file, and it stops being true the moment another
    service writes a key with a TTL - at which point the hazard changes shape
    and the sentence in ``docs/STREAMS.md`` becomes false. This is what goes red.

    Counted rather than checked per key, because ``brokeradmin`` holds no
    ``+ttl`` and no ``+type`` and should not: ``INFO keyspace``'s ``expires`` is
    the number of volatile keys, and ``SCAN MATCH`` gives the number of dedupe
    keys, both from what the probe's own credential already grants. The
    equality can in principle be reached by two offsetting changes; a second
    tenant's volatile key arriving on its own is the case worth catching, and
    it fails this.

    **Read twice, because the two sources disagree transiently.** ``expires``
    still counts a key whose TTL has passed until the expire cycle reclaims it,
    where ``SCAN`` already filters that key out - so a dedupe key expiring
    between the two reads presents as a non-dedupe volatile key that does not
    exist, with a failure message naming the wrong cause. The condition this
    guards is persistent, so it has to survive a second reading.

    An empty dedupe namespace is legitimate - the TTL is 24h and a quiet day
    expires them all - so zero on both sides passes.
    """
    reading = _volatile_versus_dedupe(live_client)
    if reading is None:
        pytest.skip("the database is empty - nothing to say about which keys are volatile")
    if reading[0] != reading[1]:
        # The two sources skew transiently and always in the same direction:
        # INFO's ``expires`` still counts a key whose TTL has passed until the
        # expire cycle reclaims it, while SCAN already filters that key out.
        # Measured at 2000 vs 0 inside the ~100ms reclaim window. The condition
        # worth catching is persistent, so insist on it twice rather than ship a
        # red build that names the wrong cause.
        time.sleep(0.5)
        reading = _volatile_versus_dedupe(live_client) or reading

    expires, dedupe = reading
    assert expires == dedupe, (
        f"{expires} volatile keys but {dedupe} dedupe keys - "
        "another tenant now writes a key with a TTL, so replicator's namespace "
        "is no longer the whole eviction candidate set; see docs/STREAMS.md, "
        '"Non-stream keys on db0"'
    )
