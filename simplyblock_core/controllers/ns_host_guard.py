"""Never remove a namespace a host still depends on.

A migrated volume keeps its NQN: during a cutover the same subsystem is
exported on the source nodes and, ANA-inaccessible, on the target nodes, and
the client's CSI connects both. Removing the namespace on one side is
harmless while the host still has a controller on the other side -- the
kernel drops that path and the block device stays. When it was the host's
last controller, the kernel deletes the block device, and a namespace
exported again later is a new disk the pod never sees: ENXIO for good.

Bed bf26 (2026-11-17): vol-4 and vol-8 had lost their source paths to port
blocks on a full cluster; an abandoned cutover then discarded its stale clone
and removed the target namespace -- the hosts' only remaining controllers --
and both pods were left without a device.

Before such a removal, ask SPDK which hosts are connected on the side that
goes and on the side that stays: a host connected only on the side that goes
is stranded, and the removal waits until the CSI has reconnected it.
"""
from collections.abc import Iterable

from simplyblock_core import utils
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCRemoteError

logger = utils.get_logger(__name__)


def volume_node_ids(lvol) -> list:
    """The nodes that export *lvol*: its primary and its HA peers."""
    ids = [getattr(lvol, "node_id", "")] + list(getattr(lvol, "nodes", None) or [])
    return list(dict.fromkeys(n for n in ids if n))


def connected_hosts(nqn: str, node_ids: Iterable[str], db=None) -> set | None:
    """Host NQNs with a controller on *nqn* on any of *node_ids*; None when a
    reachable-looking node could not be asked.

    A node that is not online serves no controller, and a node without the
    subsystem (SPDK answers with an error) has none: both contribute nothing.
    """
    db = db or DBController()
    hosts: set = set()
    for node_id in node_ids:
        try:
            node = db.get_storage_node_by_id(node_id)
        except KeyError:
            continue
        if node.status != StorageNode.STATUS_ONLINE:
            continue
        try:
            ctrls = node.rpc_client(timeout=5, retry=1).nvmf_subsystem_get_controllers(nqn) or []
        except RPCRemoteError:
            continue                                      # no such subsystem here
        except Exception as e:                            # noqa: BLE001
            logger.warning("Could not list the hosts of %s on %s: %s", nqn, node_id, e)
            return None
        hosts.update(c.get("hostnqn") for c in ctrls if isinstance(c, dict) and c.get("hostnqn"))
    return hosts


def stranded_hosts(nqn: str, removing: Iterable[str], keeping: Iterable[str],
                   db=None) -> set | None:
    """Hosts connected to *nqn* on the *removing* nodes but on none of the
    *keeping* nodes; None when that could not be established."""
    gone = connected_hosts(nqn, removing, db=db)
    if gone is None:
        return None
    if not gone:
        return set()
    stay = connected_hosts(nqn, keeping, db=db)
    if stay is None:
        return None
    return gone - stay


def removal_blocked(nqn: str, removing: Iterable[str], keeping: Iterable[str],
                    what: str, db=None) -> str | None:
    """Why removing *nqn* from *removing* must wait, or None when it is safe."""
    stranded = stranded_hosts(nqn, list(removing), list(keeping), db=db)
    if stranded is None:
        return f"{what}: could not establish which hosts use {nqn}; not removing it yet"
    if stranded:
        return (f"{what}: host(s) {', '.join(sorted(stranded))} reach {nqn} only through "
                f"the namespace being removed; waiting for them to reconnect")
    return None
