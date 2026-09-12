import threading
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
EVENT_LIMIT_REACHED = "LIMIT_REACHED"

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
    """Record one JM event (jm_compression today) in the cluster event log.

    Level is derived rather than fixed: a started/finished compression is
    informational, while compression_failed or any non-zero error_code is an
    error. log_distr_event hardcodes ERROR, which is right for the distrib
    events it handles (they are all faults) and wrong for these.
    """
    status = str(event_dict.get("status", ""))
    try:
        error_code = int(event_dict.get("error_code", 0) or 0)
    except (TypeError, ValueError):
        error_code = 0
    failed = error_code != 0 or status == "compression_failed"

    ds = EventObj()
    ds.uuid = str(uuid.uuid4())
    ds.cluster_uuid = cluster_id
    ds.node_id = node_id
    ds.date = round(time.time() * 1000)
    ds.domain = DOMAIN_JM
    ds.event_level = EventObj.LEVEL_ERROR if failed else EventObj.LEVEL_INFO
    ds.caused_by = CAUSED_BY_MONITOR
    ds.status = "new"

    ds.event = str(event_dict.get("event_type", "jm_event"))
    ds.message = f"{status} (error_code={error_code})" if failed else status

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


#: Minimum gap between two identical object-limit warnings from one process.
#: A limit refusal is driven by the CALLER's retry loop, not by an event in
#: the cluster: CSI re-issues a rejected create every few seconds and would
#: otherwise write an event per attempt for as long as the volume stays
#: unschedulable. The operator needs to know the limit was hit, once, not a
#: transcript of the retries.
LIMIT_EVENT_COOLDOWN_SEC = 300

#: (cluster_id, limit_key) -> monotonic time of the last warning emitted.
_limit_event_last: dict = {}
_limit_event_lock = threading.Lock()


def log_object_limit_reached(cluster_id, db_object, message, limit_key,
                             caused_by=CAUSED_BY_CLI):
    """Warn in the cluster event log that a hard object limit refused an op.

    ``limit_key`` identifies the limit AND the object it was hit on (e.g.
    ``"snapshots:<lvol_id>"``), and is what the cooldown dedupes on: a
    different volume hitting the same limit is a separate event, the same
    volume hitting it sixty times in a minute is not.

    Returns the event dict, or None when the cooldown swallowed it. Never
    raises: a failure to log an event must not turn an orderly "limit
    reached" refusal into an internal error for the caller.
    """
    key = (cluster_id, limit_key)
    now = time.monotonic()
    with _limit_event_lock:
        last = _limit_event_last.get(key)
        if last is not None and (now - last) < LIMIT_EVENT_COOLDOWN_SEC:
            return None
        _limit_event_last[key] = now

    try:
        return log_event_cluster(
            cluster_id=cluster_id,
            domain=DOMAIN_CLUSTER,
            event=EVENT_LIMIT_REACHED,
            db_object=db_object,
            caused_by=caused_by,
            event_level=EventObj.LEVEL_WARN,
            message=message)
    except Exception as e:
        logger.error("Failed to log object-limit event (%s): %s", limit_key, e)
        return None


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
