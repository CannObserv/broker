"""The *running* ACL must match the tracked one - the ``ACL SAVE`` gap.

``test_live_broker_matches_tracked_config.py`` reads every directive in
``deploy/redis.conf.broker`` back through ``CONFIG GET``, so a ``CONFIG SET``
nobody recorded fails a test. The ACL had no equivalent, and its change
mechanism is exactly the one that drifts: fix live with ``ACL SETUSER`` +
``ACL SAVE`` as ``acladmin``, *then* mirror it into ``deploy/redis-acl.conf``.
That second half rested on someone remembering four times in this epic -
eleven corrections in broker#2, step 4 of the restart window, and broker#9's
key-pattern fix - and nothing would have said anything had it been skipped
(CannObserv/broker#11).

**Both sides are read from a server, not from a file.** A byte comparison is
out: ``ACL SAVE`` rewrites the file in Redis's canonical form - ``#<sha256>``
for each password, the rule string reordered, ``-@admin`` folded into
``-@dangerous``. So the tracked file is loaded into the same throwaway
``redis-server`` the parse tests already spawn, and the two servers are asked
the same question.

WHAT IT COSTS, AND WHO PAYS. ``brokeradmin`` needs ``+acl|getuser``. That is
read-only, and it sits on the observer side of the observer/changer split that
keeps ``+acl`` off it - the split is about *changing*. Not ``acladmin``:
nothing runs as that user by design, its password is in
``/etc/redis/broker-acl-passwords`` and in no env file, so a test using it
would need either sudo or that password copied somewhere it must not go. The
grant does expose each user's password *hash* to the probe's credential;
those are unsalted sha256 of 40-character random secrets, and that credential
already reads every key and every stream on the instance.

**WHAT THIS CANNOT SEE: a user the tracked file never declared.**
``+acl|getuser`` permits ``ACL GETUSER`` and nothing else - ``ACL USERS``,
``ACL LIST``, ``ACL WHOAMI`` and ``ACL CAT`` are each denied separately,
verified on 7.0.15 - so there is no way to enumerate. Every user named in the
file is compared, and one that is *missing* live is caught by the nil reply,
but an extra identity added live and saved is invisible to a per-name lookup.
``test_no_connection_authenticates_as_an_undeclared_user`` closes the half of
that which matters, using a grant ``brokeradmin`` already holds: an untracked
user that is actually *in use* has a connection, and ``CLIENT LIST`` reports
the ``user=`` on each one. An untracked user that exists but is idle would
need ``+acl|users`` - names only, no hashes - and is not granted.

**And what it cannot catch at all: a grant that is wrong on both sides.**
Correction eleven (``replicator`` without ``+exists``) and broker#9
(``~replicator:cmd:fetch:*``) were both wrong in the file *and* live,
identically. This is drift detection, not correctness. What catches
wrong-in-both-places is a test over the *taxonomy* - see
``test_replicator_can_name_every_dedupe_namespace`` - and that shape is worth
preferring wherever a grant has a derivable source.

Skips unless ``BROKER_REDIS_URL`` is set and the broker answers, so CI and dev
clones pass. On the broker node, source the env first - and as
``set -a; . /etc/broker/.env; set +a``, never ``export $(cat ... | xargs)``.
"""

import pytest
import redis as redis_pkg

from tests.deploy.conftest import ACL_FILE
from tests.deploy.test_redis_acl import parse_users

TRACKED_USERS = tuple(sorted(parse_users(ACL_FILE.read_text())))

# Excluded from the rule comparison by construction: the throwaway server
# renders every placeholder to the same test credential, so every hash differs
# and the comparison would be nothing but noise. What is asserted about them
# instead is ``test_every_live_user_still_carries_a_password``, which is the
# assertion that was actually worth having.
NOT_COMPARED = "passwords"

# The grant this module needs, named here so the failure message can say it.
REQUIRED_GRANT = "+acl|getuser"


def _canonical(value):
    """One ACL field, in a form two servers can be compared on.

    Sets rather than sequences, and that is forced from both ends:

    - **Redis does not canonicalise key patterns.** ``~alpha ~beta`` and
      ``~beta ~alpha`` are the same grant, and ``ACL GETUSER`` reports them in
      the order they were set - so a sequence comparison would call a merely
      reordered file drift. Verified on 7.0.15.
    - **redis-py does not preserve Redis's command order either.** The server
      emits a deterministic rule string (``+ping +xadd`` and ``+xadd +ping``
      both come back identical, and ``+@all -@admin -@dangerous`` folds to
      ``+@all -@dangerous``), but ``acl_getuser`` splits that string into
      ``commands`` and ``categories`` and returns each unordered.

    So ordering is not information either side of this comparison can carry,
    and pretending otherwise would only produce false drift.
    """
    if isinstance(value, dict):  # a selector
        return frozenset((field, _canonical(v)) for field, v in value.items())
    if isinstance(value, list):
        return frozenset(_canonical(v) for v in value)
    return value


def _difference(tracked, live) -> str:
    """What changed, rather than two frozensets the reader has to diff by eye."""
    if isinstance(tracked, frozenset) and isinstance(live, frozenset):
        gained = sorted(str(v) for v in live - tracked)
        lost = sorted(str(v) for v in tracked - live)
        return f"live has extra {gained}, missing {lost}"
    return f"tracked {tracked!r} != live {live!r}"


def _getuser(client, user: str):
    try:
        return client.acl_getuser(user)
    except redis_pkg.exceptions.NoPermissionError as e:
        # Deliberately a failure and not a skip. A drift test that quietly
        # stops running when its grant is withdrawn is worth less than no test,
        # because the absence reads as a pass.
        pytest.fail(
            f"the probe's credential cannot read the live ACL ({e}) - "
            f"`ACL SETUSER brokeradmin {REQUIRED_GRANT}` as acladmin, then `ACL SAVE`, "
            f"then mirror it into {ACL_FILE.name}"
        )


@pytest.fixture(scope="module")
def tracked_rules(tracked_acl_broker) -> dict[str, dict]:
    """Every tracked user's rules, as the throwaway server understands them."""
    admin = tracked_acl_broker("acladmin")
    return {user: admin.acl_getuser(user) for user in TRACKED_USERS}


def test_every_tracked_user_has_the_same_rules_on_the_live_broker(
    tracked_rules, live_client
) -> None:
    """One assertion per user would hide the rest behind the first failure, and
    a mirror that was skipped usually left more than one grant behind."""
    mismatches = []
    for user in TRACKED_USERS:
        live = _getuser(live_client, user)
        if live is None:
            mismatches.append(f"{user}: declared in {ACL_FILE.name}, absent on the broker")
            continue
        tracked = tracked_rules[user]
        for field in sorted(set(tracked) | set(live)):
            if field == NOT_COMPARED:
                continue
            want, got = _canonical(tracked.get(field)), _canonical(live.get(field))
            if want != got:
                mismatches.append(f"{user}.{field}: {_difference(want, got)}")
    assert not mismatches, (
        f"the running ACL differs from {ACL_FILE.name}:\n  " + "\n  ".join(mismatches) + "\n"
        "Fix live as acladmin and mirror it here, in that order - the live broker is "
        "what the services are actually talking to."
    )


def test_every_live_user_still_carries_a_password(tracked_rules, live_client) -> None:
    """``nopass`` is R2 arriving by accident, and the hashes cannot be compared.

    The passwords themselves are excluded above - the throwaway server gives
    every user the same test credential - but two things about them survive the
    difference and both are worth pinning. A user that has *lost* a password is
    a user with a shared secret retired out from under it; a user that has
    gained ``nopass`` is a tailnet-bound broker answering an anonymous client,
    which is exactly the failure the ``default`` stanza in
    ``deploy/redis-acl.conf`` exists to prevent.

    ``default`` is included deliberately. It is ``off``, and ``off`` is a flag
    that leaves the password intact precisely so the rollback
    (``ACL SETUSER default on``) lands somewhere safe rather than enabling a
    ``nopass`` user holding ``+@all``.
    """
    findings = []
    for user in TRACKED_USERS:
        live = _getuser(live_client, user)
        if live is None:
            findings.append(f"{user}: absent on the broker")
            continue
        if "nopass" in live["flags"]:
            findings.append(f"{user}: nopass - authenticates with any password, or none")
        elif len(live["passwords"]) != len(tracked_rules[user]["passwords"]):
            findings.append(
                f"{user}: {len(live['passwords'])} passwords live, "
                f"{len(tracked_rules[user]['passwords'])} in {ACL_FILE.name}"
            )
    assert not findings, "credentials on the running broker: " + "; ".join(findings)


def test_no_connection_authenticates_as_an_undeclared_user(live_client) -> None:
    """The half of "an untracked user" that a per-name lookup cannot reach.

    ``+acl|getuser`` cannot enumerate, so a user added live and saved is
    invisible to the comparison above. What it cannot hide is being *used*:
    ``CLIENT LIST`` reports the ``user=`` on every connection, and
    ``brokeradmin`` already holds ``+client|list`` - granted during the cutover
    because ``flags=b`` is the only reliable way to tell a blocked consumer
    from a client that merely ran a read once.

    So this catches the untracked identity that is doing something, which is
    the one that matters, and leaves the idle one to ``+acl|users`` if that is
    ever granted. It also catches a service still authenticating as ``default``
    after step 4, which is the same shape read from the other side.
    """
    live_users = {c.get("user") for c in live_client.client_list()} - {None}
    undeclared = sorted(live_users - set(TRACKED_USERS))
    assert not undeclared, (
        f"connections authenticated as {undeclared}, which {ACL_FILE.name} does not declare - "
        "either an identity was added live and never mirrored, or one was retired here "
        "while something is still using it"
    )
