# coding=utf-8
import json
import logging
import uuid

from simplyblock_core import utils
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.qos import QOSClass

logger = logging.getLogger()
db = DBController()


def add_class(name, weight):

    if not name:
        logger.error("name is empty!")
        return False

    qos_class = QOSClass()
    qos_class.uuid = str(uuid.uuid4())
    qos_class.cluster_id = "cluster_id"
    qos_class.name = name
    qos_class.weight = weight
    qos_class.write_to_db()
    return qos_class.get_id()


def list_classes(cluster_id=None, is_json=False):
    classes = db.get_qos(cluster_id)
    data = []
    for qos_class in classes:
        data.append({
            "UUID": qos_class.uuid,
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

