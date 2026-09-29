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

WHO ASKS. ``acladmin``, since CannObserv/broker#52 - the operator, whose
``+acl`` reads the ACL and changes it, and whose password the fixture decrypts
from the node's credential store through ``sudo -n``. Until #52 this module ran
as the probe's ``brokeradmin`` with ``+acl|getuser`` alone, because
``acladmin``'s password was plaintext in a root-only file and reading it meant
copying it somewhere it must not go. That put every user's digest behind the
one credential ``exedev`` could read without sudo.

**AND IT CAN ENUMERATE, which the probe never could.** ``+acl|getuser`` permits
``ACL GETUSER`` alone - ``ACL USERS`` and ``ACL LIST`` were each denied, verified
on 7.0.15 - so an identity added live was invisible to a per-name lookup, and
three tests covered what they could: one in use (``CLIENT LIST``'s ``user=``),
one saved (the saved file), and one idle and never saved left to the next
restart. ``test_the_live_acl_declares_no_user_the_tracked_file_does_not`` now
asks ``ACL USERS`` directly; the other two stay, because each says something the
enumeration does not - who is connected, and what a restart would load.

**Nor does the in-memory comparison verify that ``ACL SAVE`` ran**: an
``ACL SETUSER`` mirrored into the tracked file but never saved passes it, then
reverts at the next restart - which on this instance is a cohort-wide event.
``test_every_tracked_user_is_saved_as_it_is_live`` closes that on the node. It
reads the broker's ``aclfile`` through ``sudo -n``, the mechanism
CannObserv/broker#49 introduced, loads it into a throwaway server the way the
tracked file is loaded, and compares every tracked user with ``ACL GETUSER``,
passwords included - the saved file carries the real digests
(CannObserv/broker#54). Reading it was never refused for the reason
``test_installed_redis_config_matches_repo.py`` gives for ``redis.conf``: that
file holds the credential in plaintext, and this one never does.
``test_a_setuser_never_saved_is_reported_and_a_save_clears_it`` proves the
mechanism anywhere ``redis-server`` is installed, against a stand-in broker.

**And what it cannot catch at all: a grant that is wrong on both sides.**
Correction eleven (``replicator`` without ``+exists``) and broker#9
(``~replicator:cmd:fetch:*``) were both wrong in the file *and* live,
identically. This is drift detection, not correctness. What catches
wrong-in-both-places is a test over the *taxonomy* - see
``test_replicator_can_name_every_dedupe_namespace`` - and that shape is worth
preferring wherever a grant has a derivable source.

The live tests skip unless ``BROKER_REDIS_URL`` is set and the broker answers,
and the ``sudo -n`` ones also skip off the node, so CI and dev clones pass. The
stand-in test for #54 needs only ``redis-server``, and so runs there too. On
the broker node, source the env first - and as
``set -a; . /etc/broker/.env; set +a``, never ``export $(cat ... | xargs)``.
"""

import hashlib
import re
import subprocess
from pathlib import Path

import pytest
import redis as redis_pkg

from src.broker.bus_health import BROKER_CREDENTIAL, _unit_credential
from tests.deploy import conftest
from tests.deploy.conftest import (
    ACL_FILE,
    OPERATOR_CREDENTIAL,
    PASSWORD,
    RENDER_SCRIPT,
    acl_server,
    node_credential,
    parse_users,
)
from tests.deploy.test_installed_redis_config_matches_repo import (
    REPO_REDIS_CONF,
    parse_directives,
)

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
# assertion that was actually worth having. The same exclusion is what lets a
# credential be rotated live without a commit or a red suite
# (CannObserv/broker#46); the count assertion below still bites on a rotation
# that adds a password without removing the old one. The real credentials are
# compared too, just not against the tracked file: against the node's own
# passwords file, by ``test_the_nodes_passwords_file_renders_the_credentials_that_are_live``.
NOT_COMPARED = "passwords"

# The node's own passwords file - `0400 root:root`, so read only through `sudo -n`,
# and only ever by the render, which emits digests (CannObserv/broker#49).
NODE_PASSWORDS = Path("/etc/redis/broker-acl-passwords")

# Every user, by digest alone. The services since CannObserv/broker#49 - each
# one's plaintext belongs to its service, or for `citest` to whatever CI target
# CannObserv/broker#53 settles on - and the operator users since
# CannObserv/broker#52: `acladmin` and `brokeradmin` are authenticated from
# encrypted credentials (NODE_CREDENTIALS), and `default`'s digest is of a value
# nobody kept. A service minted later is exempt here only for the interval
# docs/ACL-CUTOVER.md step 1 describes, and by name.
DIGEST_ONLY_USERS = TRACKED_USERS

# The users something on this node authenticates as, and the encrypted
# credential each is authenticated from (CannObserv/broker#52).
NODE_CREDENTIALS = {
    "acladmin": OPERATOR_CREDENTIAL,
    "brokeradmin": Path("/etc/credstore.encrypted") / BROKER_CREDENTIAL,
}

# `default`'s password until CannObserv/broker#52 - the one the aclfile-commented
# -out recovery path substituted into redis.conf. Retired with the plaintext.
RETIRED_REQUIREPASS_FILE = Path("/etc/redis/broker-password")

# The grant this module needs, named here so the failure message can say it.
REQUIRED_GRANT = "+acl"

# Appended to the throwaway copy of the node's saved ACL, the one user there
# whose password is known: every saved user carries its real digest
# (CannObserv/broker#54). Kept though the suite can now decrypt `acladmin`: a
# reader of its own leaves the throwaway server's `acladmin` exactly as saved.
SAVED_READER = "pytest-saved-acl-reader"


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


def _difference(want, live, want_is: str = "tracked") -> str:
    """What changed, rather than two frozensets the reader has to diff by eye."""
    if isinstance(want, frozenset) and isinstance(live, frozenset):
        gained = sorted(_render(v) for v in live - want)
        lost = sorted(_render(v) for v in want - live)
        return f"live has extra {gained}, missing {lost}"
    return f"{want_is} {want!r} != live {live!r}"


def _field_mismatches(
    user: str, want: dict, live: dict, want_is: str = "tracked", skip: str | None = None
) -> list[str]:
    """``<user>.<field>: <difference>`` for each field the two reports disagree on.

    Passwords are compared whole and reported by 12-character digest prefix -
    enough to tell two apart, and all a failure message needs to carry of a
    credential's hash.
    """
    found = []
    for field in sorted(set(want) | set(live)):
        if field == skip:
            continue
        a, b = _canonical(want.get(field), field), _canonical(live.get(field), field)
        if a == b:
            continue
        if field == "passwords":
            a, b = frozenset(h[:12] for h in a - b), frozenset(h[:12] for h in b - a)
        found.append(f"{user}.{field}: {_difference(a, b, want_is)}")
    return found


def _getuser(client, user: str):
    try:
        return client.acl_getuser(user)
    except redis_pkg.exceptions.NoPermissionError as e:
        # Deliberately a failure and not a skip. A drift test that quietly
        # stops running when its grant is withdrawn is worth less than no test,
        # because the absence reads as a pass.
        pytest.fail(
            f"acladmin cannot read the live ACL ({e}) - it has lost {REQUIRED_GRANT}, "
            f"which only a restart onto {ACL_FILE.name} can give back"
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


def _version_note(throwaway: str, live_client) -> str:
    """Named in the failure, because a version skew reads exactly like drift.

    The two servers are asked the same question but they are two binaries, and
    what Redis folds a rule string into moves between releases - ``-@admin``
    into ``-@dangerous`` on 7.0, the ``flags`` vocabulary across 7.x. A node
    whose redis package was upgraded without the service being restarted would
    otherwise report every category grant as drift.
    """
    live = live_client.info("server")["redis_version"]
    if throwaway == live:
        return ""
    return (
        f"\nNOTE: the throwaway redis-server is {throwaway} and the broker is {live}. "
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
        mismatches += _field_mismatches(user, tracked_rules[user], live, skip=NOT_COMPARED)
    assert not mismatches, (
        f"the running ACL differs from {ACL_FILE.name}:\n  "
        + "\n  ".join(mismatches)
        + "\nFix live as acladmin and mirror it here, in that order - the live broker is "
        "what the services are actually talking to. A restart window no longer flips "
        "`default` (CannObserv/broker#52), so a retired user live `on` is a finding."
        + _version_note(
            tracked_acl_broker("brokeradmin").info("server")["redis_version"], live_client
        )
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
    that leaves the password intact precisely so a flip to ``on`` lands
    somewhere safe rather than enabling a ``nopass`` user.

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


def _sudo(*argv: str, **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(["sudo", "-n", *argv], capture_output=True, check=False, **kwargs)


def _on_the_node(path: Path) -> Path:
    """``path``, reachable through ``sudo -n``, or a skip.

    Every fixture calling this takes ``live_client`` first, so a host with sudo
    but no broker credentials - a CI runner - skips before any ``sudo`` is
    attempted.
    """
    if _sudo("true").returncode:
        pytest.skip("no passwordless sudo - not the broker node")
    if _sudo("test", "-f", str(path)).returncode:
        pytest.skip(f"{path} absent - not the broker node")
    return path


@pytest.fixture(scope="module")
def node_passwords(live_client) -> Path:
    """The node's passwords file, reachable through ``sudo -n``, or a skip."""
    return _on_the_node(NODE_PASSWORDS)


def _plaintext_line(user: str) -> str:
    """An ERE for ``user``'s plaintext line, commented out or not.

    Commented counts: that is the shape #49 found archiver's leaked secret in,
    kept "as the rollback path", and the render's comment skip hides it from
    every other test. The ``=`` straight after the placeholder is what spares
    prose that merely names it.
    """
    return rf"^[[:space:]]*#?[[:space:]]*__{user.upper()}_PW__="


def _digests_only(text: str, source: str, remedy: str) -> str:
    """Text read as root, refused unread if it is not digests-only user lines.

    Two inputs, both crossing ``sudo`` into pytest. The render's output: the
    fixture below runs the *working tree's* script as root, so a broken branch
    is a real input. And the broker's saved ACL, which ``ACL SAVE`` writes by
    digest but anything with root could have replaced. Were either to carry a
    plaintext rule, or a line that is not a user line, ``parse_users``' own
    assertion would repeat that line into the test report. So this fails first,
    and says nothing about what it saw.
    """
    lines = text.splitlines()
    if any(not line.startswith("user ") for line in lines) or re.search(r"(^|\s)>", text):
        pytest.fail(
            f"{source} carries a plaintext rule or a non-user line - not shown, since "
            f"it may be a credential. {remedy}"
        )
    return text


def test_a_render_carrying_a_plaintext_rule_is_refused_without_echoing_it() -> None:
    secret = "would-be-secret-0123456789"
    for rendered in (f"user x on >{secret} ~*", f"user x on #{'a' * 64} ~*\n{secret}"):
        with pytest.raises(pytest.fail.Exception) as refused:
            _digests_only(rendered, "the render", "")
        assert secret not in str(refused.value)
    assert (
        _digests_only(f"user x on #{'a' * 64} ~*", "the render", "") == f"user x on #{'a' * 64} ~*"
    )


@pytest.fixture(scope="module")
def node_render(node_passwords) -> str:
    """The node's real passwords file, rendered through the tracked script as root.

    **The first test in this repo to call ``sudo``**, and bounded so that is
    all it is: ``-n``, so it can never prompt, and skipped where passwordless
    sudo is absent (``node_passwords``). What crosses
    back into pytest is the render's stdout, which is digests only by the
    render's own contract (``test_render_acl.py``), and its stderr, which
    names placeholders and never values.
    """
    result = _sudo(str(RENDER_SCRIPT), str(node_passwords), text=True)
    assert result.returncode == 0, (
        f"{RENDER_SCRIPT.name} refuses the node's {node_passwords} - so the dry run in "
        "docs/ACL-CUTOVER.md step 2, and a rebuild's re-render, would both fail:\n" + result.stderr
    )
    return _digests_only(
        result.stdout,
        f"{RENDER_SCRIPT.name}'s output",
        "Run it by hand into /dev/null to reproduce, and fix the render before this test.",
    )


def test_the_nodes_passwords_file_renders_the_credentials_that_are_live(
    node_render, live_rules
) -> None:
    """The half of ``NOT_COMPARED`` that can be compared: the node, with itself.

    The throwaway server cannot say anything about real credentials, but the
    node can. Its passwords file is what a rebuild re-renders and what the
    dry run loads, and nothing compared it with what the broker actually
    authenticates until CannObserv/broker#49 - which is how a digest-only
    ``archiver`` left the render refusing the file with nobody told, and how a
    rotation that skipped one of ``__DEFAULT_PW__``'s four writes
    (``docs/ACL-CUTOVER.md``) would have gone unseen until the restart that
    reverted it.

    Mid-rotation - a new password added live, the old one not yet retired -
    this fails on that user by design: the rotation is not finished.
    """
    rendered = parse_users(node_render)
    findings = []
    for user in TRACKED_USERS:
        live = live_rules[user]
        if live is None:
            continue  # reported by the rule comparison above
        want = {rule[1:] for rule in rendered.get(user, []) if rule.startswith("#")}
        got = set(live["passwords"])
        if want != got:
            findings.append(
                f"{user}: the passwords file renders {sorted(h[:12] for h in want)}, "
                f"live has {sorted(h[:12] for h in got)}"
            )
    assert not findings, (
        f"{NODE_PASSWORDS} and the live ACL disagree (digest prefixes):\n  "
        + "\n  ".join(findings)
        + "\nA rotation left half-done, or a passwords-file edit never applied live. "
        "Whichever is stale, a re-render from this file would install it."
    )


@pytest.mark.parametrize(
    ("line", "is_plaintext"),
    [
        ("__WATCHER_PW__=value", True),
        ("#__WATCHER_PW__=value", True),
        ("  # __WATCHER_PW__=value", True),
        ("__WATCHER_PW_SHA256__=" + "a" * 64, False),
        ("# __WATCHER_PW__ was rotated 2026-09-23", False),
    ],
    ids=["live", "commented", "commented-indented", "digest", "prose"],
)
def test_a_commented_plaintext_line_counts_as_plaintext(tmp_path, line, is_plaintext) -> None:
    """#49 found archiver's leaked secret kept as ``#__ARCHIVER_PW__=<value>``,
    "the rollback path". The render skips comments, so nothing else sees one."""
    held = tmp_path / "passwords"
    held.write_text(line + "\n")
    status = subprocess.run(
        ["grep", "-qE", _plaintext_line("watcher"), str(held)], check=False
    ).returncode
    assert status == (0 if is_plaintext else 1)


def test_the_node_holds_no_plaintext_for_any_user(node_passwords) -> None:
    """The digest-only state #49 and #52 set up, pinned - by exit status, so no line is read.

    The comparison above cannot see it: a plaintext line renders the same digest
    as the digest line it replaced, so a plaintext could come back - by an old
    mint recipe that writes one, by re-minting ``citest`` for
    CannObserv/broker#53, or by an operator credential "kept handy" beside its
    encrypted copy - and every other test would stay green.
    """
    found = []
    for user in DIGEST_ONLY_USERS:
        status = _sudo("grep", "-qE", _plaintext_line(user), str(node_passwords)).returncode
        assert status in (0, 1), f"grep over {node_passwords} failed (exit {status})"
        if status == 0:
            found.append(user)
    assert not found, (
        f"{node_passwords} holds plaintext for {found}, live or commented out. The operator "
        "users authenticate from /etc/credstore.encrypted/ and the rest from off the node, "
        "and a commented secret is not a rollback; replace each line with "
        '__<USER>_PW_SHA256__=<its digest> (deploy/README.md, "Changing a grant").'
    )


def test_the_retired_requirepass_file_is_gone(live_client) -> None:
    """``/etc/redis/broker-password`` held ``default``'s plaintext for the
    aclfile-commented-out recovery path (CannObserv/broker#52). That path mints
    its own ``requirepass`` now, so the file has no reader."""
    if _sudo("true").returncode:
        pytest.skip("no passwordless sudo - not the broker node")
    assert _sudo("test", "-e", str(RETIRED_REQUIREPASS_FILE)).returncode == 1, (
        f"{RETIRED_REQUIREPASS_FILE} is back. Nothing reads it; `sudo shred -u` it."
    )


EMPTY_DIGEST = hashlib.sha256(b"").hexdigest()


def _digest_of(value: str | None) -> str | None:
    """sha256 of ``value``, in a frame of its own, so a plaintext is never a test's local."""
    return None if value is None else hashlib.sha256(value.encode()).hexdigest()


def test_each_node_credential_authenticates_its_user(live_client, live_rules) -> None:
    """The encrypted credentials and the live ACL agree (CannObserv/broker#52).

    ``test_the_nodes_passwords_file_renders_the_credentials_that_are_live``
    holds the passwords file to the broker; nothing held the credentials the
    node actually authenticates FROM, so a rotation that updated the ACL and not
    the credential - or the other way round - would surface only as a probe
    that cannot connect, or an operator who cannot. Compared by digest: the
    plaintext is hashed where it is decrypted and goes no further.
    """
    findings = []
    for user, path in sorted(NODE_CREDENTIALS.items()):
        digest = _digest_of(node_credential(path))
        if digest is None:
            findings.append(f"{user}: {path} cannot be decrypted")
        elif digest not in (live_rules[user] or {}).get("passwords", []):
            findings.append(f"{user}: {path} is not a password the live user accepts")
    assert not findings, "node credentials: " + "; ".join(findings)


def test_requirepass_is_nobodys_password(live_client, live_rules) -> None:
    """The value ``CONFIG GET requirepass`` returns authenticates no user.

    Until CannObserv/broker#52 it was ``default``'s password, in three plaintext
    places, and readable through the probe's ``+config|get`` (broker#50). Now it
    is a random value minted for redis.conf and belonging to no one - both the
    running value and the one in the file, which the next restart loads.
    Compared by digest, the file's hashed inside ``sudo`` so its value never
    crosses into pytest at all - only the line count and the last line's digest,
    which is the one Redis honours. An absent or empty value would hash to the
    empty string's digest and match nobody, which is a pass that read nothing,
    so it is refused first.
    """
    live_digests = {h for rules in live_rules.values() if rules for h in rules["passwords"]}
    running = _digest_of(live_client.config_get("requirepass").get("requirepass", ""))
    on_disk = _sudo(
        "sh",
        "-c",
        "c=/etc/redis/redis.conf; grep -c '^requirepass ' $c; "
        "sed -n 's/^requirepass //p' $c | tail -n1 | tr -d '\\n' | sha256sum",
        text=True,
    )
    assert on_disk.returncode == 0, "cannot read /etc/redis/redis.conf through sudo -n"
    count, file_digest = on_disk.stdout.split()[:2]
    assert count == "1", f"/etc/redis/redis.conf has {count} requirepass lines, want 1"
    assert EMPTY_DIGEST not in (running, file_digest), (
        "a requirepass is empty - nopass by another door"
    )
    findings = [
        f"the {where} requirepass is a live user's password"
        for where, digest in (("running", running), ("redis.conf", file_digest))
        if digest in live_digests
    ]
    assert not findings, (
        "; ".join(findings) + " - it is readable through CONFIG GET, so it must belong to "
        'nobody. docs/ACL-CUTOVER.md, "requirepass belongs to no user".'
    )


def test_the_live_acl_declares_no_user_the_tracked_file_does_not(live_client) -> None:
    """The enumeration the probe's ``+acl|getuser`` could never make.

    ``ACL USERS`` lists every user in memory, idle or connected, saved or not -
    the gap the ``CLIENT LIST`` and saved-file tests below each closed half of.
    """
    extra = sorted(set(live_client.acl_users()) - set(TRACKED_USERS))
    assert not extra, (
        f"the live broker has users {extra} that {ACL_FILE.name} does not declare - "
        "added live, never mirrored. Mirror it with its reason, or `ACL DELUSER` it "
        "as acladmin and `ACL SAVE`."
    )


@pytest.fixture(scope="module")
def node_saved_acl(live_client) -> str:
    """The broker's ACL as ``ACL SAVE`` last wrote it, read through ``sudo -n``, or a skip.

    The path is the tracked ``aclfile`` directive, not ``CONFIG GET aclfile``:
    ``+config|get`` also reads ``requirepass``, so its callers are kept to the one
    its stanza names (CannObserv/broker#50), and
    ``test_every_tracked_directive_is_in_force`` already holds the live value to
    this one. It is ``0640`` and group ``redis``, and pytest runs as neither.
    What crosses back is digests and rules only - ``ACL SAVE`` never writes a
    plaintext password, and ``_digests_only`` refuses the file unread if
    something else did.
    """
    path = _on_the_node(Path(parse_directives(REPO_REDIS_CONF.read_text())["aclfile"]))
    result = _sudo("cat", str(path), text=True)
    assert result.returncode == 0, f"cannot read {path} through sudo -n:\n{result.stderr}"
    return _digests_only(
        result.stdout,
        str(path),
        "`ACL SAVE` never writes one, so something else wrote this file - and the next "
        "restart loads it. Find what before anything restarts the broker.",
    )


def _saved_rules(saved: str, workdir: Path, users) -> tuple[dict[str, dict | None], str]:
    """Each of ``users`` as a throwaway server loading ``saved`` reports it, and its version.

    Not a byte comparison against ``ACL GETUSER``, for the reason the tracked
    file is not one: the two are different spellings of the same ACL. Loading the
    saved file puts both sides through the same parser, the one the next restart
    uses. ``SAVED_READER`` is appended as the one user this suite can
    authenticate as; it is never a name the file itself declares.
    """
    assert SAVED_READER not in parse_users(saved), f"{SAVED_READER} is already declared"
    acl = workdir / "users.acl"
    acl.write_text(
        saved.rstrip("\n") + f"\nuser {SAVED_READER} on >{PASSWORD} +acl|getuser +info\n"
    )
    with acl_server(acl, workdir) as connect:
        reader = connect(SAVED_READER)
        try:
            rules = {user: reader.acl_getuser(user) for user in users}
            return rules, reader.info("server")["redis_version"]
        finally:
            reader.close()


def _saved_mismatches(saved: dict, live: dict) -> list[str]:
    """Every way a restart would change the ACL the services are talking to.

    Passwords included: the saved file carries the real digests, so unlike the
    tracked comparison nothing here needs excluding.
    """
    found = []
    for user in sorted(set(saved) | set(live)):
        want, got = saved.get(user), live.get(user)
        if want is None and got is None:
            continue  # declared here, absent from both: the rule comparison's finding
        if want is None:
            found.append(f"{user}: live, never saved - the next restart drops it")
        elif got is None:
            found.append(f"{user}: saved, absent live - the next restart brings it back")
        else:
            found += _field_mismatches(user, want, got, want_is="saved")
    return found


@pytest.fixture(scope="module")
def saved_rules(node_saved_acl, tmp_path_factory) -> tuple[dict[str, dict | None], str]:
    return _saved_rules(node_saved_acl, tmp_path_factory.mktemp("saved-acl"), TRACKED_USERS)


def test_every_tracked_user_is_saved_as_it_is_live(saved_rules, live_rules, live_client) -> None:
    """``ACL SETUSER`` without ``ACL SAVE``: live now, reverted at the next restart.

    The rule comparison above reads the broker's *memory*, so a change made
    live and mirrored into the tracked file, but never saved, passes it - and
    then reverts at a restart, which on this instance is a cohort-wide event.
    Over the tracked names, since that is all ``+acl|getuser`` can ask for live;
    the saved file's own extras are the next test's.
    """
    saved, version = saved_rules
    mismatches = _saved_mismatches(saved, live_rules)
    assert not mismatches, (
        "the broker's saved ACL differs from its live one:\n  "
        + "\n  ".join(mismatches)
        + "\nAn `ACL SETUSER` never followed by `ACL SAVE`, and a restart would revert it. "
        "If live is right, `rcli acladmin ACL SAVE`; if not, fix it live first, then save."
        + _version_note(version, live_client)
    )


def test_the_saved_acl_declares_no_user_the_tracked_file_does_not(node_saved_acl) -> None:
    """The saved half of an untracked user, which ``+acl|getuser`` cannot enumerate.

    Live, only a user in use is visible (the ``CLIENT LIST`` test below). The
    saved file lists every user it holds, and a saved one is the one that
    matters: it survives every restart. One added live and never saved is gone
    at the next.
    """
    extra = sorted(set(parse_users(node_saved_acl)) - set(TRACKED_USERS))
    assert not extra, (
        f"the broker's saved ACL declares {extra}, which {ACL_FILE.name} does not - "
        "added live and saved, never mirrored. Mirror it with its reason, or "
        "`ACL DELUSER` it as acladmin and `ACL SAVE`."
    )


def test_no_connection_authenticates_as_an_undeclared_user(live_client) -> None:
    """An untracked or retired identity that is *connected*.

    ``ACL USERS`` above catches an untracked user whether or not it is used;
    this catches the one doing something, which is the one that matters, and
    it is the only check here that sees a retired identity still in use.
    ``CLIENT LIST`` reports the ``user=`` on every connection - ``acladmin``
    holds ``+client|list``, granted during the cutover because ``flags=b`` is
    the only reliable way to tell a blocked consumer from a client that merely
    ran a read once.

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


def test_a_setuser_never_saved_is_reported_and_a_save_clears_it(tmp_path) -> None:
    """CannObserv/broker#54's done-when, proven where a ``SETUSER`` is ours to run.

    The real broker is not, so a throwaway server stands in for it, saving to a
    file this test can read without ``sudo``. Clean before the change, reported
    after it, clean again once saved: the comparison fails on exactly the unsaved
    change and on nothing the save's canonical rewrite introduces.
    """
    acl = tmp_path / "users.acl"
    acl.write_text(
        "user default off\n"
        f"user operator on >{PASSWORD} ~* &* +@all\n"
        f"user x on #{'a' * 64} ~alpha +get\n"
    )
    with acl_server(acl, tmp_path) as connect:
        broker = connect("operator")

        def compare(attempt: str) -> list[str]:
            workdir = tmp_path / attempt
            workdir.mkdir()
            saved, _ = _saved_rules(acl.read_text(), workdir, ("x",))
            return _saved_mismatches(saved, {"x": broker.acl_getuser("x")})

        assert compare("before") == []
        broker.execute_command("ACL", "SETUSER", "x", "~beta", "#" + "b" * 64)
        unsaved = compare("unsaved")
        assert [m.split(":")[0] for m in unsaved] == ["x.keys", "x.passwords"]
        assert "b" * 64 not in " ".join(unsaved), "a digest is reported by prefix only"
        broker.execute_command("ACL", "SAVE")
        assert compare("saved") == []


def test_a_node_credential_is_read_the_way_the_probe_reads_it(monkeypatch, tmp_path) -> None:
    """One newline rule for every reader of a node credential (CR 7 of broker#52).

    ``systemd-creds encrypt`` keeps whatever it is fed, so a credential minted
    with ``echo`` ends in a newline. The probe strips it; were the tests not to,
    the probe would authenticate while every digest check here failed against
    the same credential - and the runbooks hash ``$(cred ...)``, which strips it
    too.
    """
    stored = "minted-value\r\n"
    monkeypatch.setattr(
        conftest.subprocess,
        "run",
        lambda argv, **kw: subprocess.CompletedProcess(argv, 0, stdout=stored, stderr=""),
    )
    (tmp_path / BROKER_CREDENTIAL).write_text(stored)
    monkeypatch.setenv("CREDENTIALS_DIRECTORY", str(tmp_path))
    from_tests = node_credential(Path("/etc/credstore.encrypted") / BROKER_CREDENTIAL)
    assert from_tests == _unit_credential(BROKER_CREDENTIAL) == "minted-value"
