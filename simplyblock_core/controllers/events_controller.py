import time
import uuid
import json
import logging

from simplyblock_core.models.events import EventObj
from simplyblock_core.kv_store import DBController
from simplyblock_core import constants

from graypy import GELFTCPHandler

# configure logging
logging.captureWarnings(True)
gelf_handler = GELFTCPHandler('0.0.0.0', constants.GELF_PORT)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.addHandler(gelf_handler)

py_warnings_logger = logging.getLogger("py.warnings")
py_warnings_logger.addHandler(gelf_handler)

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
    ds.date = round(time.time()*1000)
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

    log_event_based_on_level(cluster_id, event_dict['event_type'], DOMAIN_DISTR,
                         event_dict['status'], CAUSED_BY_MONITOR, EventObj.LEVEL_ERROR)

    db_controller = DBController()
    ds.write_to_db(db_controller.kv_store)
    return ds.get_id()


def log_event_cluster(cluster_id, domain, event, db_object, caused_by, message,
                      node_id=None, event_level=EventObj.LEVEL_INFO, status=None):
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
    ds.date = round(time.time()*1000)
    ds.node_id = node_id
    ds.event_level = event_level

    ds.event = event
    ds.domain = domain
    ds.object_name = db_object.name
    ds.object_dict = db_object.get_clean_dict()
    ds.caused_by = caused_by
    ds.message = message
    ds.status = status

    log_event_based_on_level(cluster_id, event, db_object.name, message, caused_by, event_level)

    db_controller = DBController()
    ds.write_to_db(db_controller.kv_store)


def log_event_based_on_level(cluster_id, event, db_object, message, caused_by, event_level):
    json_str = json.dumps({
        "cluster_id": cluster_id,
        "event": event,
        "object_name": db_object,
        "message": message,
        "caused_by": caused_by
    })

    if event_level == EventObj.LEVEL_CRITICAL:
        logger.critical(json_str)
    elif event_level == EventObj.LEVEL_WARN:
        logger.warning(json_str)
    elif event_level == EventObj.LEVEL_ERROR:
        logger.error(json_str)
    else:
        logger.info(json_str)
