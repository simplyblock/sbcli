"""Port block helpers with RPC-then-iptables fallback.

Tries the SPDK ``nvmf_port_block`` / ``nvmf_port_unblock`` /
``nvmf_get_blocked_ports`` RPCs first; on JSON-RPC method-not-found
(SPDK build without the new RPCs — in-flight upgrades) falls back to
the legacy iptables-based FirewallClient.
"""
import logging

import jc

from simplyblock_core.rpc_client import RPCErrorCode, RPCRemoteError
from simplyblock_core.fw_api_client import FirewallClient

logger = logging.getLogger(__name__)


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
    If spdk_version is R26.2-PRE-latest or empty (came from upgrade) then
     use FirewallClient(iptables). Default is to use SPDK RPC.

    Tries SPDK ``nvmf_port_block`` / ``nvmf_port_unblock`` first; on
    method-not-found falls back to ``FirewallClient.firewall_set_port``
    (iptables). Any other error from the RPC propagates as-is.
    """
    if node.spdk_version == "" or node.spdk_version == "R26.2-PRE-latest":
        fw = FirewallClient(node, timeout=timeout, retry=retry)
        action = "block" if block else "allow"
        return fw.firewall_set_port(port, "tcp", action, node.rpc_port, is_reject=is_reject)

    rpc = node.rpc_client(timeout=timeout, retry=retry)
    try:
        if block:
            return rpc.nvmf_port_block(port, is_reject=is_reject)
        return rpc.nvmf_port_unblock(port)
    except RPCRemoteError as exc:
        if exc.code != RPCErrorCode.method_not_found:
            raise
        logger.info(
            "nvmf_port_%s RPC not available on %s; falling back to iptables",
            "block" if block else "unblock", node.get_id())

    fw = FirewallClient(node, timeout=timeout, retry=retry)
    action = "block" if block else "allow"
    return fw.firewall_set_port(
        port, "tcp", action, node.rpc_port, is_reject=is_reject)


def get_blocked_ports_set(node, timeout=5, retry=5):
    """One ``nvmf_get_blocked_ports`` fetch -> set of blocked port numbers,
    or ``None`` when the node's SPDK lacks the method (legacy iptables path —
    caller falls back to per-port :func:`is_port_blocked`).

    The health/monitor loops used to call ``is_port_blocked`` per port,
    re-fetching the identical full list each time — measured 528 of these
    RPCs per minute cluster-wide at idle (2026-07-21 baseline audit). One
    fetch per node per cycle answers every port.
    """
    rpc = node.rpc_client(timeout=timeout, retry=retry)
    try:
        blocked = rpc.nvmf_get_blocked_ports()
    except RPCRemoteError as exc:
        if exc.code != RPCErrorCode.method_not_found:
            raise
        return None
    if not blocked:
        return set()
    entries = blocked.get("blocked_ports", []) if isinstance(blocked, dict) else []
    return {int(e.get("port", -1)) for e in entries}


def is_port_blocked(node, port_id, timeout=5, retry=5):
    """Return True if ``port_id`` is currently blocked on ``node``.

    Tries SPDK ``nvmf_get_blocked_ports`` first; on method-not-found
    falls back to parsing iptables output via ``FirewallClient``.
    """
    try:
        return port_id in node.rpc_client(timeout=timeout, retry=retry).nvmf_get_blocked_ports()
    except RPCRemoteError as exc:
        if exc.code != RPCErrorCode.method_not_found:
            raise
        return  _is_port_blocked_iptables(node, port_id, timeout, retry)


def _is_port_blocked_iptables(node, port_id, timeout, retry):
    """Legacy iptables-based check via FirewallClient + jc parsing."""
    fw = FirewallClient(node, timeout=timeout, retry=retry)
    iptables_output, error = fw.get_firewall(node.rpc_port)
    if isinstance(iptables_output, str):
        iptables_output = [iptables_output]
    for rules in iptables_output:
        result = jc.parse('iptables', rules)
        for chain in result:
            if chain['chain'] in ("INPUT", "OUTPUT"):  # type: ignore
                for rule in chain['rules']:  # type: ignore
                    if str(port_id) in rule['options'] and rule['target'] == 'DROP':  # type: ignore
                        return True
    return False
