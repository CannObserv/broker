"""The *running* ACL must match the tracked one - the mirror gap.

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

**It does not verify that ``ACL SAVE`` ran**, and the title used to imply it
did. This compares the live *in-memory* ACL, so `ACL SETUSER` mirrored into the
tracked file but never saved passes every test here and then reverts at the next
restart - which on this instance is a cohort-wide event. Closing that means
reading the installed ``/etc/redis/users.acl``, which is ``0640 root:redis``:
pytest does not run as root, so such a test would skip on every host and
therefore never run. Worth recording precisely, because the reason is **not**
the one ``test_installed_redis_config_matches_repo.py`` gives for refusing to
read ``redis.conf``. That file holds the credential in plaintext; this one does
not - ``ACL SAVE`` has rewritten all seven passwords to ``#<sha256>``, verified
on the node. It is the file mode alone. Until that gap is closed, ``ACL SAVE``
is held by the runbook and by ``deploy/README.md``, both of which put it on the
line after ``ACL SETUSER``.

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

from tests.deploy.conftest import ACL_FILE, parse_users

TRACKED = parse_users(ACL_FILE.read_text())
TRACKED_USERS = tuple(sorted(TRACKED))

# Declared *and retired*: `off` in the tracked file. Being declared is not a
# licence to be connected - `default` is the one that matters today, and a
# connection still authenticated as it is step 4's straggler, which is why the
# runbook's own check reads "no user=default" rather than "only declared users".
# Derived from the flag rather than naming `default`, so retiring a second
# identity gets the check for free.
RETIRED_USERS = frozenset(name for name, rules in TRACKED.items() if "off" in rules)

# Excluded from the rule comparison by construction: the throwaway server
# renders every placeholder to the same test credential, so every hash differs
# and the comparison would be nothing but noise. What is asserted about them
# instead is ``test_every_tracked_user_still_carries_a_password``, which is the
# assertion that was actually worth having.
NOT_COMPARED = "passwords"

# The grant this module needs, named here so the failure message can say it.
REQUIRED_GRANT = "+acl|getuser"


# The three ACL fields whose value is a space-separated rule list. redis-py
# splits them for the top-level user but not inside a selector, where each
# arrives as one joined string.
_RULE_LISTS = frozenset({"commands", "keys", "channels"})


def _as_selector(selector) -> dict:
    """One selector, as a mapping, whichever protocol produced it.

    RESP3 gives a ``dict``; RESP2 - the default - gives a flat
    ``[name, value, name, value]`` list, and pairing it back up is not optional:
    flattened into a set, a selector's field *names* and its patterns become
    indistinguishable, and two selectors that merely swap which field holds a
    value compare equal.
    """
    if isinstance(selector, dict):
        return selector
    return dict(zip(selector[::2], selector[1::2], strict=True))


def _canonical(value, field: str | None = None):
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

    That applies *inside a selector* too, which is the one place redis-py does
    not split the rule lists for us: a selector's ``keys`` arrives as the single
    string ``"~alpha ~beta"``, so left alone it would reintroduce exactly the
    false drift the rest of this function exists to prevent.
    """
    if field == "selectors" and isinstance(value, list):
        return frozenset(_canonical(_as_selector(s)) for s in value)
    if isinstance(value, dict):  # a selector
        return frozenset((f, _canonical(v, f)) for f, v in value.items())
    if isinstance(value, list):
        return frozenset(_canonical(v) for v in value)
    if isinstance(value, str) and field in _RULE_LISTS:
        return frozenset(value.split())
    return value


def _render(value) -> str:
    """A canonical value as something a reader can act on.

    Without this a drifted selector reports as a bare ``frozenset({...})``,
    which is the shape the reader was being spared in the first place.
    """
    if isinstance(value, frozenset):
        return "{" + " ".join(sorted(_render(v) for v in value)) + "}"
    if isinstance(value, tuple):
        return f"{value[0]}={_render(value[1])}"
    return str(value)


def _difference(tracked, live) -> str:
    """What changed, rather than two frozensets the reader has to diff by eye."""
    if isinstance(tracked, frozenset) and isinstance(live, frozenset):
        gained = sorted(_render(v) for v in live - tracked)
        lost = sorted(_render(v) for v in tracked - live)
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


@pytest.fixture(scope="module")
def live_rules(live_client) -> dict[str, dict | None]:
    """One read of the live ACL, shared by every comparison below.

    Read once rather than per test: two passes over ``ACL GETUSER`` would also
    be two *moments*, and a grant changed between them makes the two reports
    contradict each other about the same broker.
    """
    return {user: _getuser(live_client, user) for user in TRACKED_USERS}


# What an open restart window moves on a retired identity, and all it moves.
# Measured: `ACL SETUSER default on` against the tracked file reports exactly
# `default.enabled` and `default.flags`.
_WINDOW_FIELDS = ("enabled", "flags")


def _window_note(mismatches: list[str]) -> str:
    """An open window is not drift, and mid-window is when this gets run.

    ``docs/RESTART-WINDOW.md`` step 1d runs ``uv run pytest tests/deploy`` with
    the window still open, and a window is opened by ``ACL SETUSER default on`` -
    a deliberate, temporary divergence from a tracked file that says ``off``.
    Left unexplained that reads as a finding at the worst moment, with the broker
    just restarted and someone deciding whether to roll back.

    The inverse is the valuable half: if it is still reported once the window is
    closed, **the window was never closed**, and a broker left reachable by the
    shared password is exactly what step 4 exists to prevent.
    """
    if not mismatches:
        return ""
    window_shaped = tuple(f"{user}.{field}" for user in RETIRED_USERS for field in _WINDOW_FIELDS)
    if not all(m.startswith(window_shaped) for m in mismatches):
        return ""
    return (
        "\nNOTE: that is the shape of an OPEN RESTART WINDOW and nothing else - a retired "
        "identity is live `on`. docs/RESTART-WINDOW.md step 1d runs this suite with the "
        "window still open, so mid-window this is expected. Close it "
        "(`ACL SETUSER default off` then `ACL SAVE`, as acladmin) and re-run. If it "
        "SURVIVES the close, the window was never closed - which is the finding, not this."
    )


def _version_note(tracked_acl_broker, live_client) -> str:
    """Named in the failure, because a version skew reads exactly like drift.

    The two servers are asked the same question but they are two binaries, and
    what Redis folds a rule string into moves between releases - ``-@admin``
    into ``-@dangerous`` on 7.0, the ``flags`` vocabulary across 7.x. A node
    whose redis package was upgraded without the service being restarted would
    otherwise report every category grant as drift.
    """
    tracked = tracked_acl_broker("brokeradmin").info("server")["redis_version"]
    live = live_client.info("server")["redis_version"]
    if tracked == live:
        return ""
    return (
        f"\nNOTE: the throwaway redis-server is {tracked} and the broker is {live}. "
        "Rule folding and the flags vocabulary move between releases, so some of the "
        "above may be skew rather than drift - restart the broker onto its installed "
        "binary first."
    )


def test_every_tracked_user_has_the_same_rules_on_the_live_broker(
    live_rules, tracked_rules, tracked_acl_broker, live_client
) -> None:
    """One assertion per user would hide the rest behind the first failure, and
    a mirror that was skipped usually left more than one grant behind."""
    mismatches = []
    for user in TRACKED_USERS:
        live = live_rules[user]
        if live is None:
            mismatches.append(f"{user}: declared in {ACL_FILE.name}, absent on the broker")
            continue
        tracked = tracked_rules[user]
        for field in sorted(set(tracked) | set(live)):
            if field == NOT_COMPARED:
                continue
            want = _canonical(tracked.get(field), field)
            got = _canonical(live.get(field), field)
            if want != got:
                mismatches.append(f"{user}.{field}: {_difference(want, got)}")
    assert not mismatches, (
        f"the running ACL differs from {ACL_FILE.name}:\n  "
        + "\n  ".join(mismatches)
        + "\nFix live as acladmin and mirror it here, in that order - the live broker is "
        "what the services are actually talking to."
        + _window_note(mismatches)
        + _version_note(tracked_acl_broker, live_client)
    )


def test_every_tracked_user_still_carries_a_password(live_rules, tracked_rules) -> None:
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

    Over the *tracked* names, not the live ones - ``+acl|getuser`` cannot
    enumerate, so there is no such thing here as every live user.
    """
    findings = []
    for user in TRACKED_USERS:
        live = live_rules[user]
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
    ever granted.

    **A retired identity counts as undeclared here**, and that is not a detail:
    ``default`` *is* declared - it has to be, or an aclfile makes it ``nopass``
    - so a plain "is it in the file" test passes on the one connection step 4
    exists to rule out. ``ACL SETUSER default off`` does not disconnect a client
    already authenticated as it on Redis 7.0; the straggler keeps working and
    breaks at its next restart, which is why the runbook's own check is
    ``CLIENT LIST | grep user=`` with *no* ``user=default``. This is that check.
    """
    live_users = {c.get("user") for c in live_client.client_list()} - {None}
    undeclared = sorted(live_users - set(TRACKED_USERS))
    retired = sorted(live_users & RETIRED_USERS)
    assert not undeclared and not retired, (
        f"connections authenticated as {sorted(undeclared + retired)} - "
        f"{undeclared or 'none'} {ACL_FILE.name} does not declare at all "
        "(an identity added live and never mirrored, or retired here while something is "
        f"still using it), and {retired or 'none'} it declares `off` "
        "(a straggler from the password retirement, still connected because the flip "
        "does not disconnect; it breaks at its next restart)"
    )
