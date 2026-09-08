#!/bin/bash
# Block until THIS host's tailnet address is assigned (CannObserv/broker#1 R1).
#
# Probe /proc/net/fib_trie, NEVER `ip addr`: systemd's sandbox SIGSYS-kills `ip`
# and blocks AF_NETLINK *silently*, so an `ip`-based probe exits 0 and detects
# nothing (CannObserv/observo#479). observo#473 is what that costs: a tailnet-
# bound Redis started before tailscaled assigned the address, crash-looped into
# the start-rate limit, and left the broker down for two weeks of silently
# starved jobs.
#
# Match on "/32 host LOCAL", not the bare address: a peer's route would also
# contain the digits, and binding to a peer's address is not a thing we want to
# discover at runtime.
#
# Fails loudly on timeout by design. Redis 7's `-` bind prefix would let the
# broker come up loopback-only instead; that is the wrong trade here, because a
# broker nobody can reach presents as an idle cluster rather than an outage.
set -uo pipefail
ADDR="${1:?usage: wait-for-tailnet-addr.sh <addr> [timeout_s]}"
TIMEOUT="${2:-90}"
for ((i = 0; i < TIMEOUT; i++)); do
    if grep -A1 -F -- "|-- ${ADDR}" /proc/net/fib_trie 2>/dev/null | grep -q "host LOCAL"; then
        [ "$i" -gt 0 ] && echo "tailnet address ${ADDR} present after ${i}s"
        exit 0
    fi
    sleep 1
done
echo "FATAL: tailnet address ${ADDR} not assigned after ${TIMEOUT}s; refusing to start" >&2
exit 1
