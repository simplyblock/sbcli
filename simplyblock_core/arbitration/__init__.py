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
            "lease_ttl_ms": int(constants.TWO_NODE_LEASE_TTL_MS)}
