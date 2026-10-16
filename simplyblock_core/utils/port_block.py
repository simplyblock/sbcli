# coding=utf-8
"""Port block helpers over the SPDK ``nvmf_port_block`` /
``nvmf_port_unblock`` / ``nvmf_get_blocked_ports`` RPCs.

Fencing is done inside SPDK only. There is no iptables fallback: the SPDK
image no longer ships iptables or the firewall webapp (ultra #218), and
25.10.x -> 26.3 upgrades are offline, so no node runs an SPDK build without
these RPCs. A method-not-found error therefore propagates like any other
RPC failure instead of being redirected to a port nothing listens on.
"""


def set_port(node, port, block, is_reject=False, timeout=0.5, retry=1):
    """Block or unblock ``port`` on ``node``.

    Budget: ``timeout`` is PER HTTP ATTEMPT and RPCClient builds
    ``Retry(total=retry, backoff_factor=1, connect=retry)``, so the wall
    clock is roughly (retry+1) x timeout plus urllib3 backoff -- not
    ``timeout``. The old defaults (5s, 2) therefore cost 12s against a host
    that is rebooting and so drops SYNs instead of refusing them
    (measured 12.009s = 5 + 5 + 2 on 2026-08-31).

    That matters because blocking a port fences a peer's client listener:
    while it is blocked the peer cannot answer keep-alives, and the client
    KATO is 4s. A fence window longer than KATO is indistinguishable from an
    outage -- on 2026-08-31 a 12s failing block on a rebooting peer left a
    healthy survivor fenced, the client lost its last path at 4.1s, and fio
    took EIO. A healthy peer answers this RPC in ~11ms, so 0.5s is ~45x the
    observed latency; two attempts bound the whole call near 1s.
    """
    rpc = node.rpc_client(timeout=timeout, retry=retry)
    if block:
        return rpc.nvmf_port_block(port, is_reject=is_reject)
    return rpc.nvmf_port_unblock(port)


def get_blocked_ports_set(node, timeout=5, retry=5):
    """One ``nvmf_get_blocked_ports`` fetch -> set of blocked port numbers.

    The health/monitor loops used to call ``is_port_blocked`` per port,
    re-fetching the identical full list each time — measured 528 of these
    RPCs per minute cluster-wide at idle (2026-07-21 baseline audit). One
    fetch per node per cycle answers every port.
    """
    blocked = node.rpc_client(timeout=timeout, retry=retry).nvmf_get_blocked_ports()
    if not blocked:
        return set()
    entries = blocked.get("blocked_ports", []) if isinstance(blocked, dict) else []
    return {int(e.get("port", -1)) for e in entries}


def is_port_blocked(node, port_id, timeout=5, retry=5):
    """Return True if ``port_id`` is currently blocked on ``node``.

    Delegates the parse to :func:`get_blocked_ports_set`.

    This USED to do ``port_id in rpc.nvmf_get_blocked_ports()``. That RPC
    returns ``{"blocked_ports": [{"port": N, ...}, ...]}``, so the ``in``
    tested the dict's KEYS and the function returned False for every port,
    always -- a blocked port read as open. Only the batched
    ``get_blocked_ports_set`` parsed the payload, which is why the monitor's
    batched path saw the fences and this one never did. It backs
    ``check_port_on_node``, so the miss was silent rather than harmless.
    """
    return int(port_id) in get_blocked_ports_set(node, timeout=timeout, retry=retry)

