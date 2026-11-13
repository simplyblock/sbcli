"""Two-node arbitration: the control-plane side.

docs/design/two-node-arbitration.md is the contract. ``decision`` is the pure
decision engine (no I/O, table-tested); ``arbiter`` drives it against FDB and
the nodes; ``events`` normalises what the collector delivers.
"""


def dual_node_params(arbitration: bool) -> dict:
    """The arbitration fields of ``jc_set_dual_node``, for every caller.

    Every ``jc_set_dual_node`` call on an arbitrated cluster must carry them: a
    call with only ``enable`` resets the node to its defaults, which switches
    arbitration off (campaign c1, B2: the restart flow did exactly that). There
    is no ``preferred`` field any more: at the hold deadline without a verdict
    a node always fences itself.
    """
    from simplyblock_core import constants
    return {"arbitration": bool(arbitration),
            "hold_ms": int(constants.TWO_NODE_HOLD_MS),
            "lease_ttl_ms": int(constants.TWO_NODE_LEASE_TTL_MS),
            "remote_jm_timeout_ms": int(constants.TWO_NODE_REMOTE_JM_TIMEOUT_MS)}


def set_dual_node(rpc, enable: bool, arbitrated: bool):
    """``jc_set_dual_node`` with the richest parameter set the node accepts.

    Tries the full arbitration config, then without ``remote_jm_timeout_ms``
    (nodes built before campaign c2 reject unknown fields), then the plain
    dual-node flag, which keeps the survivor from aborting and must never be
    lost. Returns ``(error, level)``: error is None on success; level is
    "full", "no_remote_jm_timeout" or "plain".
    """
    attempts = []
    if arbitrated:
        full = dual_node_params(enable)
        attempts.append(("full", full))
        reduced = {k: v for k, v in full.items() if k != "remote_jm_timeout_ms"}
        attempts.append(("no_remote_jm_timeout", reduced))
    attempts.append(("plain", {}))
    from simplyblock_core.rpc_client import RPCRemoteError
    err = None
    for level, extra in attempts:
        try:
            rpc.jc_set_dual_node(enable, **extra)
        except RPCRemoteError as e:  # the node rejected this parameter set
            err = e
            continue
        return None, level
    return err, "plain"
