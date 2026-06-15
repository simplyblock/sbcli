# coding=utf-8
"""Port block helpers with RPC-then-iptables fallback.

Tries the SPDK ``nvmf_port_block`` / ``nvmf_port_unblock`` /
``nvmf_get_blocked_ports`` RPCs first; on JSON-RPC method-not-found
(SPDK build without the new RPCs — in-flight upgrades) falls back to
the legacy iptables-based FirewallClient.
"""
import logging

from simplyblock_core.fw_api_client import FirewallClient

logger = logging.getLogger(__name__)


def _is_method_not_found(exc):
    """Identify a JSON-RPC 'Method not found' error.

    Matches the standard -32601 code and the common ``method ... not
    found`` text variants emitted by SPDK / spdk_http_proxy.
    """
    msg = str(exc).lower()
    return (
        "-32601" in msg
        or "method not found" in msg
        or "no method named" in msg
        or "unknown method" in msg
    )


def set_port(node, port, block, is_reject=False, timeout=5, retry=2):
    """Block or unblock ``port`` on ``node``.

    Tries SPDK ``nvmf_port_block`` / ``nvmf_port_unblock`` first; on
    method-not-found falls back to ``FirewallClient.firewall_set_port``
    (iptables). Any other error from the RPC propagates as-is.
    """
    rpc = node.rpc_client(timeout=timeout, retry=retry)
    try:
        if block:
            return rpc.nvmf_port_block(port, is_reject=is_reject)
        return rpc.nvmf_port_unblock(port)
    except Exception as exc:
        if not _is_method_not_found(exc):
            raise
        logger.info(
            "nvmf_port_%s RPC not available on %s; falling back to iptables",
            "block" if block else "unblock", node.get_id())

    fw = FirewallClient(node, timeout=timeout, retry=retry)
    action = "block" if block else "allow"
    return fw.firewall_set_port(
        port, "tcp", action, node.rpc_port, is_reject=is_reject)


def is_port_blocked(node, port_id, timeout=5, retry=5):
    """Return True if ``port_id`` is currently blocked on ``node``.

    Tries SPDK ``nvmf_get_blocked_ports`` first; on method-not-found
    falls back to parsing iptables output via ``FirewallClient``.
    """
    rpc = node.rpc_client(timeout=timeout, retry=retry)
    try:
        blocked = rpc.nvmf_get_blocked_ports()
    except Exception as exc:
        if not _is_method_not_found(exc):
            raise
        return _is_port_blocked_iptables(node, port_id, timeout, retry)

    if not blocked:
        return False
    entries = blocked.get("blocked_ports", []) if isinstance(blocked, dict) else []
    for entry in entries:
        if int(entry.get("port", -1)) == int(port_id):
            return True
    return False


def _is_port_blocked_iptables(node, port_id, timeout, retry):
    """Legacy iptables-based check via FirewallClient + jc parsing."""
    import jc
    fw = FirewallClient(node, timeout=timeout, retry=retry)
    iptables_output, _ = fw.get_firewall(node.rpc_port)
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
