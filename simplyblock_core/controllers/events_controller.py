import time
import uuid

from simplyblock_core.models.events import EventObj
from simplyblock_core.kv_store import DBController

EVENT_STATUS_CHANGE = "STATUS_CHANGE"
EVENT_OBJ_CREATED = "OBJ_CREATED"
EVENT_OBJ_DELETED = "OBJ_DELETED"
EVENT_CAPACITY = "CAPACITY"

DOMAIN_CLUSTER = "cluster"
DOMAIN_MANAGEMENT = "management"
DOMAIN_STORAGE = "storage"
DOMAIN_DISTR = "distr"

CAUSED_BY_CLI = "cli"
CAUSED_BY_API = "api"
CAUSED_BY_MONITOR = "monitor"


def log_distr_event(cluster_id, node_id, event_dict):

    ds = EventObj()
    ds.uuid = str(uuid.uuid4())
    ds.cluster_uuid = cluster_id
    ds.node_id = node_id
    ds.date = int(time.time())
    ds.domain = DOMAIN_DISTR
    ds.event_level = EventObj.LEVEL_ERROR
    ds.caused_by = CAUSED_BY_MONITOR
    ds.status = 'new'

    ds.event = event_dict['event_type']
    ds.message = event_dict['status']

    if 'storage_ID' in event_dict:
        ds.storage_id = event_dict['storage_ID']

    if 'vuid' in event_dict:
        ds.vuid = event_dict['vuid']

    ds.object_dict = event_dict

    db_controller = DBController()
    ds.write_to_db(db_controller.kv_store)
    return ds.get_id()


def log_event_cluster(cluster_id, domain, event, db_object, caused_by, message,
                      node_id=None, event_level=EventObj.LEVEL_INFO):
    """
    uuid:
    cluster_uuid: 1234
    event: STATUS_CHANGE
    domain: Cluster, Management, Storage
    object_name: cluster,
    object_dict:
    caused_by: CLI, API, MONITOR
    message:
    meta_data:
    date:
    """

    ds = EventObj()
    ds.uuid = str(uuid.uuid4())
    ds.cluster_uuid = cluster_id
    ds.date = int(time.time())
    ds.node_id = node_id
    ds.event_level = event_level

    ds.event = event
    ds.domain = domain
    ds.object_name = db_object.name
    ds.object_dict = db_object.get_clean_dict()
    ds.caused_by = caused_by
    ds.message = message

    db_controller = DBController()
    ds.write_to_db(db_controller.kv_store)
