# Node credentials - `acladmin` and `brokeradmin`

The two users something on this node authenticates as: the operator (and the
deploy tests), and the probe. Since CannObserv/broker#52 neither password exists
in plaintext at rest. Each is a `systemd-creds` credential in
`/etc/credstore.encrypted/` (`0700 root`), named `broker-<user>` and encrypted
with the host key `/var/lib/systemd/credential.secret` (created on first use;
this VM has no TPM). The passwords file carries each one's digest, for the
render. Root decrypts on demand - `cred` at the top of [RESTART-WINDOW.md](RESTART-WINDOW.md) - with
no prompt; the probe's unit decrypts `broker-brokeradmin` into its own
`$CREDENTIALS_DIRECTORY` for each tick.

**What that buys, and what it does not.** Root on this node can decrypt either,
and so can `exedev`, which has passwordless sudo; nothing on this node could
change that without taking sudo away. What it removes is the plaintext in a file
- the thing that gets copied, grepped, backed up or read into an agent's
context, which is how CannObserv/archiver#251 happened - and host-bound
ciphertext is useless off the node. The host key sits on the same disk
(`systemd-creds` says "not located on encrypted media"), so an image of the disk
carries both. While a probe tick runs, systemd also grants the unit's `User=`
read on its decrypted copy, so for that second another `exedev` process can read
it - in RAM, never on disk ([deploy/broker-bus-health.service](../deploy/broker-bus-health.service)).

**Rotate one** - on exposure, not on a schedule. Run it as a script (`bash
rotate.sh`); it defines its own helpers. It adds before it retires, as #46's
rotation did, and checks each write before the next, so a failure at any step
leaves the old password working. That matters most for `acladmin`, the only
`+acl` user: losing its credential means editing `users.acl` and a cohort-wide
restart. Nothing puts the value on a command line (CannObserv/broker#47):

```bash
set -euo pipefail
u=acladmin                                 # or brokeradmin
cred() { sudo -n systemd-creds decrypt --name="broker-$1" "/etc/credstore.encrypted/broker-$1" -; }
rcli() { local u=$1 p; shift
         p="$(cred "$u")" && [ -n "$p" ] || { echo "rcli: no credential for $u" >&2; return 1; }
         REDISCLI_AUTH="$p" redis-cli --user "$u" -h localhost -p 6379 "$@"; }
digest() { printf %s "$1" | sha256sum | cut -d' ' -f1; }
mint() { (set +o pipefail; LC_ALL=C tr -dc 'A-Za-z0-9' < /dev/urandom | head -c 40); }
C=/etc/credstore.encrypted/broker-$u
OLDH="$(digest "$(cred "$u")")"
NEW="$(mint)"; [ "${#NEW}" -eq 40 ] || { echo "minted ${#NEW} chars, want 40"; exit 1; }
NEWH="$(digest "$NEW")"

# 1. ADD the new password; both authenticate until 4. Checked live, because
#    redis-cli's exit status does not say whether the reply was an error.
rcli acladmin ACL SETUSER "$u" "#$NEWH" >/dev/null
rcli acladmin ACL GETUSER "$u" | grep -qx "$NEWH" || { echo "1: not live"; exit 1; }
[ "$(rcli acladmin ACL SAVE)" = OK ]
# 2. The credential - the value on a pipe, printf being a builtin. The old
#    ciphertext stays as $C.old until 4 has run.
printf %s "$NEW" | sudo systemd-creds encrypt --name="broker-$u" - "$C.new"
unset NEW
[ "$(digest "$(sudo -n systemd-creds decrypt --name="broker-$u" "$C.new" -)")" = "$NEWH" ]
sudo chmod 0400 "$C.new" && sudo cp -p "$C" "$C.old" && sudo mv "$C.new" "$C"
# 3. The digest line the render reads.
sudo cat /etc/redis/broker-acl-passwords \
    | awk -F= -v k="__${u^^}_PW_SHA256__" -v h="$NEWH" '$1==k {print k "=" h; next} {print}' \
    | sudo install -m 0400 -o root -g root /dev/stdin /etc/redis/broker-acl-passwords.new
sudo grep -qx "__${u^^}_PW_SHA256__=$NEWH" /etc/redis/broker-acl-passwords.new \
    || { echo "3: no __${u^^}_PW_SHA256__ line to replace"; exit 1; }
sudo mv /etc/redis/broker-acl-passwords.new /etc/redis/broker-acl-passwords
# 4. RETIRE the old one - as the new credential, which proves it for acladmin.
rcli acladmin ACL SETUSER "$u" "!$OLDH" >/dev/null
[ "$(rcli acladmin ACL GETUSER "$u" | grep -cE '^[0-9a-f]{64}$')" -eq 1 ]
[ "$(rcli acladmin ACL SAVE)" = OK ]
sudo rm "$C.old"
```

For `brokeradmin` the next tick simply authenticates with the new credential
(`systemctl start broker-bus-health.service` to see it now). Between 1 and 4 the
live suite is red by design - `test_every_tracked_user_still_carries_a_password`
counts two against one - and after 4 it holds all three places to each other:
`test_each_node_credential_authenticates_its_user` and
`test_the_nodes_passwords_file_renders_the_credentials_that_are_live`.

**On a new or rebuilt node, mint rather than restore.** No copy of either
password exists off the node, and none needs to: nothing off the node
authenticates as either user, and a rebuilt node has a new host key that could
not decrypt the old credential anyway (RECOVERY.md). Run this **after** [ACL-CUTOVER.md](ACL-CUTOVER.md) step 1's
`install` of the passwords file, which truncates it, and before the first render:

```bash
set -euo pipefail
mint() { (set +o pipefail; LC_ALL=C tr -dc 'A-Za-z0-9' < /dev/urandom | head -c 40); }
sudo install -d -m 0700 -o root -g root /etc/credstore.encrypted
for u in acladmin brokeradmin; do
    NEW="$(mint)"; [ "${#NEW}" -eq 40 ] || { echo "minted ${#NEW} chars, want 40"; exit 1; }
    printf %s "$NEW" | sudo systemd-creds encrypt --name="broker-$u" - "/etc/credstore.encrypted/broker-$u"
    sudo chmod 0400 "/etc/credstore.encrypted/broker-$u"
    echo "__${u^^}_PW_SHA256__=$(printf %s "$NEW" | sha256sum | cut -d' ' -f1)" \
        | sudo tee -a /etc/redis/broker-acl-passwords >/dev/null
    unset NEW
done
```

## `requirepass` belongs to no user

It accepts plaintext only, so it is the one Redis secret-shaped value left on
disk, in `/etc/redis/redis.conf` (`0640 redis:redis`). Since CannObserv/broker#52
it is a random value minted at install and kept nowhere else ([deploy/README.md](../deploy/README.md), *Install*),
and it authenticates nobody: the aclfile's `default` line overrides it while the
aclfile loads, and `default` is a tombstone besides. Its only job is the
last-resort restart with `aclfile` commented out, where it keeps `default` from
being `nopass`; whoever makes that edit is root, and writes a fresh one in the
same edit. `test_requirepass_is_nobodys_password` holds both the running value
and the file's to that, by digest.

Until #52 it was `default`'s password, in three plaintext places -
`/etc/redis/broker-acl-passwords`, `/etc/redis/broker-password` and this
directive - and the reason `/etc/redis/broker-password` existed. That file is
gone.

## Rotating `__DEFAULT_PW__` - retired by CannObserv/broker#52

It was four writes, and they ran once: on 2026-09-23, as `acladmin`, after the
credential every service held before the cutover leaked into archiver's
journald (CannObserv/archiver#251, CannObserv/broker#46). That credential was
also the break-glass for every restart window, because opening one meant
`ACL SETUSER default on` - so one leaked string was also every future window's
key. The procedure is in [ACL-CUTOVER.md](ACL-CUTOVER.md)'s history, before a567fa5.

On 2026-09-29 #52 retired it instead of rotating it again. The window's commands
moved to `acladmin`; `default` lost every grant; its password became the digest
of a value nobody kept; `/etc/redis/broker-password` was shredded; and
`requirepass` was re-minted to belong to no user. There is nothing left to
rotate: the digest is of a value nobody has, and the tombstone would grant
nothing if it were found.


The rest of the cutover - the passwords, the dry run, each service onto its own
user, and *What `CONFIG GET requirepass` does not tell you* - is
[ACL-CUTOVER.md](ACL-CUTOVER.md).
