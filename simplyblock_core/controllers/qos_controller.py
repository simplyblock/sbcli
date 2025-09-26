# coding=utf-8
import json
import logging
import uuid

from simplyblock_core import utils, constants
from simplyblock_core.db_controller import DBController
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

    qos_classes = db.get_qos(cluster_id)
    if len(qos_classes) >= 6:
        logger.error("Can not add more than 6 qos classes")
        return False

    for qos_class in qos_classes:
        if qos_class.name == name:
            logger.error("qos_class already exists")
            return False

    qos_class = QOSClass()
    qos_class.uuid = str(uuid.uuid4())
    qos_class.cluster_id = cluster_id
    qos_class.class_id = get_next_index_id(cluster_id)
    qos_class.name = name
    qos_class.weight = weight
    qos_class.write_to_db()

    cluster = db.get_cluster_by_id(cluster_id)
    cluster.enable_qos = True
    cluster.write_to_db()
    return True


def list_classes(cluster_id=None, is_json=False):
    classes = db.get_qos(cluster_id)
    data = []
    for qos_class in classes:
        data.append({
            "UUID": qos_class.uuid,
            "Class ID": qos_class.class_id,
            "Name": qos_class.name,
            "Weight": qos_class.weight,
            "Cluster ID": qos_class.cluster_id,
        })

    if is_json:
        return json.dumps(data, indent=2)
    else:
        return utils.print_table(data)


def delete_class(name):
    pass


def get_qos_weights_list(cluster_id=None):
    classes = db.get_qos(cluster_id)
    total_weight = 0
    default_class = None
    for qos_class in classes:
        total_weight += qos_class.weight
        if qos_class.class_id == 0:
            default_class = qos_class

    meta_class_w = constants.qos_class_meta_and_migration_weight_percent
    # add default and meta classes (0,1)
    lst = [int(default_class.weight/total_weight*(100-meta_class_w)),
           int(meta_class_w)]

    # add the reset of the classes
    for qos_class in classes:
        if qos_class.class_id == 0:
            continue
        lst.append(int(qos_class.weight/total_weight*(100-meta_class_w)))

    # count all weights and assign the reset of a 100 to the default
    lst[0] = 100-sum(lst[1:])
    return lst


def get_next_index_id(cluster_id):
    ids = []
    for qos in db.get_qos(cluster_id):
        ids.append(qos.class_id)

    for i in range(1,7):
        if i not in ids:
            return i

    raise ValueError("Index out of range")
