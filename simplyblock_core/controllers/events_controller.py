import time
import uuid

from simplyblock_core.models.events import EventObj
from simplyblock_core.db_controller import DBController
from simplyblock_core import utils


logger = utils.get_logger(__name__)


EVENT_STATUS_CHANGE = "STATUS_CHANGE"
EVENT_OBJ_CREATED = "OBJ_CREATED"
EVENT_OBJ_DELETED = "OBJ_DELETED"
EVENT_CAPACITY = "CAPACITY"

DOMAIN_CLUSTER = "cluster"
DOMAIN_MANAGEMENT = "management"
DOMAIN_STORAGE = "storage"
DOMAIN_DISTR = "distr"
DOMAIN_JM = "jm"

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
    return ds


def log_jm_event(cluster_id, node_id, event_dict):
    """Record one FAILED JM event (jm_compression today) in the cluster
    event log. Returns the EventObj, or None for a successful event.

    Only failures are persisted. JM compression runs continuously on every
    node and emits a started AND a finished event per cycle, so writing all
    of them put two event records per node per compression into the cluster
    event log -- the log an operator scans for faults became mostly a
    compression transcript, and the events that matter scrolled off. The
    full history stays in the service log (log_event_based_on_level below
    still runs for every event, at INFO for the successful ones) and in the
    JM's own event list, which is where a compression audit belongs.

    What survives here is the signal: compression_failed, any non-zero
    error_code, and -- separately, from the collector -- the latched
    JM_COMPRESSION_BACKLOG warning raised when compression stops keeping
    up. Those are bounded by real faults rather than by a poll loop.
    """
    status = str(event_dict.get("status", ""))
    try:
        error_code = int(event_dict.get("error_code", 0) or 0)
    except (TypeError, ValueError):
        error_code = 0
    failed = error_code != 0 or status == "compression_failed"

    if not failed:
        # Service log only -- see the docstring.
        log_event_based_on_level(
            cluster_id, str(event_dict.get("event_type", "jm_event")),
            DOMAIN_JM, status, CAUSED_BY_MONITOR, EventObj.LEVEL_INFO)
        return None

    ds = EventObj()
    ds.uuid = str(uuid.uuid4())
    ds.cluster_uuid = cluster_id
    ds.node_id = node_id
    ds.date = round(time.time() * 1000)
    ds.domain = DOMAIN_JM
    ds.event_level = EventObj.LEVEL_ERROR
    ds.caused_by = CAUSED_BY_MONITOR
    ds.status = "new"

    ds.event = str(event_dict.get("event_type", "jm_event"))
    ds.message = f"{status} (error_code={error_code})"

    # jm_vuid arrives as a string ("1"); EventObj.vuid is an int with -1 for
    # "not applicable".
    try:
        ds.vuid = int(event_dict.get("jm_vuid"))
    except (TypeError, ValueError):
        ds.vuid = -1

    ds.object_dict = event_dict

    log_event_based_on_level(cluster_id, ds.event, DOMAIN_JM, ds.message,
                             CAUSED_BY_MONITOR, ds.event_level)

    db_controller = DBController()
    ds.write_to_db(db_controller.kv_store)
    return ds


def log_event_cluster(cluster_id, domain, event, db_object, caused_by, message,
                      node_id=None, event_level=EventObj.LEVEL_INFO, status=None, storage_id=None):
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
    if storage_id:
        ds.storage_id = storage_id

    log_event_based_on_level(cluster_id, event, db_object.name, message, caused_by, event_level)

    db_controller = DBController()
    ds.write_to_db(db_controller.kv_store)
    return ds.to_dict()


def log_event_based_on_level(cluster_id, event, db_object, message, caused_by, event_level):
    json_str = utils.dump_json({
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
