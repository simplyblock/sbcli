"""Two-node arbitration: the control-plane side.

docs/design/two-node-arbitration.md is the contract. ``decision`` is the pure
decision engine (no I/O, table-tested); ``arbiter`` drives it against FDB and
the nodes; ``events`` normalises what the collector delivers.
"""
