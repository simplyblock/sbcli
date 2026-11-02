import logging
import uuid

from simplyblock_core import constants
from simplyblock_core.db_controller import DBController
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.qos import QOSClass

logger = logging.getLogger()
db = DBController()


def add_class(name: str, weight: int, cluster_id: str) -> bool:

    if not name:
        logger.error("name is required")
        return False

    if weight <= 0:
        logger.error("weight is required")
        return False

    if not cluster_id:
        clusters = db.get_clusters()
        if clusters and len(clusters) == 1:
            cluster_id = clusters[0].uuid
        else:
            logger.error("cluster_id is required")
            return False

    if not cluster_id:
        logger.error("cluster_id is required")
        return False

    cluster = db.get_cluster_by_id(cluster_id)
    if cluster.status != Cluster.STATUS_UNREADY:
        logger.error(f"cluster must be in state UNREADY, current status is {cluster.status}")
        return False

    qos_classes = db.get_qos(cluster_id)
    user_classes = [c for c in qos_classes if c.class_name != "Default"]
    if name != "Default" and len(user_classes) >= constants.QOS_MAX_USER_CLASS_ID:
        # The last class id is reserved for replication traffic, see
        # constants.QOS_REPLICATION_CLASS_ID.
        logger.error(f"Can not add more than {constants.QOS_MAX_USER_CLASS_ID} qos classes besides Default")
        return False

    for qos_class in qos_classes:
        if qos_class.class_name == name:
            logger.error("qos_class already exists")
            return False

    qos_class = QOSClass()
    qos_class.uuid = str(uuid.uuid4())
    qos_class.cluster_id = cluster_id
    if name == "Default":
        qos_class.class_id = 0
    else:
        qos_class.class_id = get_next_index_id(cluster_id)
    qos_class.class_name = name
    qos_class.weight = weight
    qos_class.write_to_db()

    return True


def list_classes(cluster_id=None):
    return [
        {
            "UUID": qos_class.uuid,
            "Class ID": qos_class.class_id,
            "Name": qos_class.class_name,
            "Weight": qos_class.weight,
            "Cluster ID": qos_class.cluster_id,
        }
        for qos_class in sorted(db.get_qos(cluster_id), key=lambda x: x.class_id)
    ]


def delete_class(name, cluster_id):
    if not name:
        logger.error("name is required")
        return False

    if not cluster_id:
        clusters = db.get_clusters()
        if clusters and len(clusters) == 1:
            cluster_id = clusters[0].uuid
        else:
            logger.error("cluster_id is required")
            return False

    if not cluster_id:
        logger.error("cluster_id is required")
        return False

    cluster = db.get_cluster_by_id(cluster_id)
    if cluster.status != Cluster.STATUS_UNREADY:
        logger.error(f"cluster must be in state UNREADY, current status is {cluster.status}")
        return False

    qos_classes = db.get_qos(cluster_id)
    for qos_class in qos_classes:
        if qos_class.class_name == name:
            qos_class.remove(db.kv_store)
            return True

    logger.error(f"qos_class {name} not found")
    return False


def replication_class_available(cluster_id) -> bool:
    """True when the replication service class can be used on *cluster_id*.

    It is on for every cluster, with or without user classes. The only
    exception is a cluster created before the reservation that still has a
    user class on the reserved id; that class keeps it.
    """
    return all(c.class_id != constants.QOS_REPLICATION_CLASS_ID for c in db.get_qos(cluster_id))


def wire_priority_class(cluster_id, class_id) -> int:
    """Map a stored lvol_priority_class to the value SPDK puts into the LBA.

    Every create/register/clone path must go through here so the primary and
    its secondary/tertiary run the volume in the same class.
    """
    if not class_id or class_id <= 0:
        return 0
    if class_id == constants.QOS_REPLICATION_CLASS_ID and replication_class_available(cluster_id):
        return constants.QOS_REPLICATION_WIRE_CLASS
    cluster = db.get_cluster_by_id(cluster_id)
    if not cluster.is_qos_set():
        return 0
    return class_id + 1


def class_on_cluster(cluster_id, class_id) -> int:
    """*class_id* if *cluster_id* defines it, else 0 (Default).

    Class ids are cluster-local: a volume copied to another cluster (fail-over,
    fail-back, replication cutover) keeps its class only where it exists.
    """
    if not class_id or class_id <= 0:
        return 0
    if any(c.class_id == class_id for c in db.get_qos(cluster_id)):
        return class_id
    logger.warning(f"QoS class {class_id} not defined on cluster {cluster_id}, using Default")
    return 0


def resolve_class(cluster_id, class_id_or_name) -> int:
    """A user-facing class reference (id or name) as a class id.

    Raises ValueError for a class the cluster does not define and for the
    reserved replication class.
    """
    if class_id_or_name in (None, "", 0, "0", "Default"):
        return 0
    ref = str(class_id_or_name)
    for c in db.get_qos(cluster_id):
        if ref in (c.class_name, str(c.class_id)):
            if c.class_id == constants.QOS_REPLICATION_CLASS_ID and replication_class_available(cluster_id):
                break
            return c.class_id
    raise ValueError(f"QoS class not found: {class_id_or_name}")


def get_qos_weights_list(cluster_id=None):
    """The 8 per-class shares (percent) the data plane schedules with.

    Index = wire class: Default (0), metadata/migration (1), user class id k
    at k+1, and replication at QOS_REPLICATION_WIRE_CLASS. Weights of user
    classes, Default included, are relative to each other and split what the
    metadata and replication shares leave.
    """
    classes = db.get_qos(cluster_id)
    total_weight = 0
    default_class = None
    for qos_class in classes:
        total_weight += qos_class.weight
        if qos_class.class_id == 0:
            default_class = qos_class

    user_classes = [c for c in classes if c.class_id != 0]
    if user_classes and not default_class:
        msg = "QOS Default class was not found, please create a new one with name Default."
        raise ValueError(msg)

    meta_class_w = constants.qos_class_meta_and_migration_weight_percent
    repl_class_w = 0
    if replication_class_available(cluster_id):
        repl_class_w = constants.qos_class_replication_weight_percent
        if cluster_id:
            repl_class_w = db.get_cluster_by_id(cluster_id).qos_replication_weight
    user_w = 100 - meta_class_w - repl_class_w

    lst = [0] * 8
    lst[1] = int(meta_class_w)
    if repl_class_w:
        lst[constants.QOS_REPLICATION_WIRE_CLASS] = int(repl_class_w)
    for qos_class in user_classes:
        lst[qos_class.class_id + 1] = int(qos_class.weight/total_weight*user_w)

    # count all weights and assign the reset of a 100 to the default
    lst[0] = 100-sum(lst[1:])
    return lst


def push_qos_weights(cluster_id) -> list[str]:
    """Send the cluster's weights to every online node; the ids that failed.

    Nodes that are not online get them on their next restart or add.
    """
    from simplyblock_core.models.storage_node import StorageNode
    from simplyblock_core.rpc_client import RPCException

    weights = get_qos_weights_list(cluster_id)
    failed = []
    for node in db.get_storage_nodes_by_cluster_id(cluster_id):
        if node.status != StorageNode.STATUS_ONLINE:
            continue
        try:
            ret = node.rpc_client().alceml_set_qos_weights(weights)
        except RPCException:
            ret = False
        if not ret:
            logger.error(f"Failed to set QOS weights on node: {node.get_id()}")
            failed.append(node.get_id())
    return failed


def get_config(cluster_id) -> dict:
    """The cluster's service-class configuration, as the v2 API serves it."""
    cluster = db.get_cluster_by_id(cluster_id)
    classes = db.get_qos(cluster_id)
    default = next((c for c in classes if c.class_id == 0), None)
    return {
        "default_weight": default.weight if default else None,
        "classes": [{"id": c.class_id, "name": c.class_name, "weight": c.weight}
                    for c in classes if c.class_id != 0],
        "replication_weight": cluster.qos_replication_weight,
        "replication_class_available": replication_class_available(cluster_id),
        "weights": get_qos_weights_list(cluster_id),
    }


def set_config(cluster_id, default_weight, classes, replication_weight=None) -> list[str]:
    """Make the cluster's service classes exactly *classes*; return push failures.

    Declarative: classes not listed are deleted, listed ones are created or
    have their weight updated, and Default is created or updated with
    *default_weight*. Ids of kept classes never change, because volumes store
    them; a new class takes the lowest free id. Unlike add_class/delete_class
    this works on an active cluster: the weights are pushed to the online nodes
    right away.

    Raises PreconditionError for a request the caller could have checked (the
    API answers it with 400) and RuntimeError when a class to delete is still
    used by a volume.
    """
    if default_weight is None or default_weight <= 0:
        raise PreconditionError("default_weight must be a positive integer")
    names = [c["name"] for c in classes]
    if len(names) != len(set(names)):
        raise PreconditionError("class names must be unique")
    if len(names) > constants.QOS_MAX_USER_CLASS_ID:
        raise PreconditionError(f"at most {constants.QOS_MAX_USER_CLASS_ID} classes besides Default")
    for c in classes:
        if not c["name"] or c["name"] == "Default" or c["name"].isdigit():
            raise PreconditionError(f"invalid class name: {c['name']!r}")
        if c["weight"] <= 0:
            raise PreconditionError(f"weight of {c['name']} must be a positive integer")
    max_repl = 100 - constants.qos_class_meta_and_migration_weight_percent - 1
    if replication_weight is not None and not 0 <= replication_weight <= max_repl:
        raise PreconditionError(f"replication_weight must be between 0 and {max_repl}")

    cluster = db.get_cluster_by_id(cluster_id)
    if cluster.status == Cluster.STATUS_IN_ACTIVATION:
        raise RuntimeError(f"cluster is {cluster.status}; retry once it settles")

    existing = {c.class_name: c for c in db.get_qos(cluster_id)}
    wanted = {c["name"]: c["weight"] for c in classes}
    removed = [c for name, c in existing.items() if name != "Default" and name not in wanted]
    if removed:
        in_use = {lvol.lvol_priority_class for lvol in db.get_lvols(cluster_id)}
        busy = [c.class_name for c in removed if c.class_id in in_use]
        if busy:
            raise RuntimeError(f"classes still used by volumes: {', '.join(sorted(busy))}")

    for c in removed:
        c.remove(db.kv_store)
    default = existing.get("Default")
    if default is None:
        default = QOSClass()
        default.uuid = str(uuid.uuid4())
        default.cluster_id = cluster_id
        default.class_id = 0
        default.class_name = "Default"
    default.weight = default_weight
    default.write_to_db()
    for name, weight in wanted.items():
        qos_class = existing.get(name)
        if qos_class is None:
            qos_class = QOSClass()
            qos_class.uuid = str(uuid.uuid4())
            qos_class.cluster_id = cluster_id
            qos_class.class_id = get_next_index_id(cluster_id)
            qos_class.class_name = name
        qos_class.weight = weight
        qos_class.write_to_db()

    if replication_weight is not None and replication_weight != cluster.qos_replication_weight:
        db.atomic_update(cluster, lambda fresh: setattr(fresh, "qos_replication_weight", replication_weight))

    if cluster.status == Cluster.STATUS_UNREADY:
        return []   # activation pushes them
    return push_qos_weights(cluster_id)


def get_next_index_id(cluster_id):
    ids = []
    for qos in db.get_qos(cluster_id):
        ids.append(qos.class_id)

    for i in range(1, constants.QOS_MAX_USER_CLASS_ID + 1):
        if i not in ids:
            return i

    raise ValueError("Index out of range")
