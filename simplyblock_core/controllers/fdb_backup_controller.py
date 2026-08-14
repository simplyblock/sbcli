# coding=utf-8
import datetime
import logging as lg
import re
import time
import uuid

from simplyblock_core import utils, constants
from simplyblock_core.db_controller import DBController
from simplyblock_core.fdb_agent_client import (
    MODE_KUBERNETES,
    FdbAgent,
    FdbAgentError,
    FdbAgentNotFoundError,
    KubernetesFdbAgent,
    get_fdb_agent,
)
from simplyblock_core.models.job_schedule import JobSchedule

logger = lg.getLogger()
db_controller = DBController()

def __get_fdb_agent() -> FdbAgent:
    """Return the agent executing FDB commands for this deployment.

    ``FDB_BACKUP_MODE=kubernetes`` selects the kubernetes API backend
    outright. Otherwise the management node's own mode decides, which needs
    the node anyway for the address of its docker daemon.

    :raises FdbAgentNotFoundError: the cluster has no management node
    """
    if constants.FDB_BACKUP_MODE == MODE_KUBERNETES:
        return KubernetesFdbAgent()

    nodes = db_controller.get_mgmt_nodes()
    if not nodes:
        raise FdbAgentNotFoundError("No management node found")
    return get_fdb_agent(nodes[0])

def create_backup(cluster_id):
    cluster = db_controller.get_cluster_by_id(cluster_id)
    backup_path = cluster.get_backup_path()
    if cluster.backup_s3_bucket and cluster.backup_s3_cred:
        folder = f"backup-{str(datetime.datetime.now())}"
        folder = folder.replace(" ", "-")
        folder = folder.replace(":", "-")
        folder = folder.split(".")[0]
        backup_path = f"blobstore://{cluster.backup_s3_cred}@s3.{cluster.backup_s3_region}.amazonaws.com/{folder}?bucket={cluster.backup_s3_bucket}&region={cluster.backup_s3_region}&sc=0"

    try:
        logger.info(__get_fdb_agent().exec(f"fdbbackup start -d {backup_path} -w"))
    except FdbAgentError as e:
        logger.error(f"Failed to create backup: {e}")
        return False
    return True

def list_backups(cluster_id):
    cluster = db_controller.get_cluster_by_id(cluster_id)
    backup_path = cluster.get_backup_path()
    data = []
    try:
        agent = __get_fdb_agent()
        output = agent.exec(f"fdbbackup list -b {backup_path}")
        logger.info(f"backup list from : {backup_path}")
        for line in output.splitlines():
            if not line or "backup-" not in line:
                continue

            name = line.split("/")[-1].strip()
            name = name.split("?")[0]
            size = "0"
            restorable = "0"
            date = ""
            version = "0"
            description = agent.exec(
                f"fdbbackup describe -d {cluster.get_backup_path(name)} --version-timestamps")
            for desc_line in description.splitlines():
                if desc_line and desc_line.startswith("SnapshotBytes"):
                    size = desc_line.split()[1].strip()
                if desc_line and desc_line.startswith("Restorable"):
                    restorable = desc_line.split()[1].strip()
                if desc_line and desc_line.startswith("Snapshot:"):
                    for param in desc_line.split():
                        if param.startswith("startVersion"):
                            version = param.split("=")[1].strip()
                        elif param.startswith("(") and param.endswith(")") and not date:# 2025/12/28.10:10:20+0000
                            try:
                                date = datetime.datetime.strptime(param[1:-1], "%Y/%m/%d.%H:%M:%S+0000").strftime("%Y-%m-%d %H:%M:%S")
                            except Exception:
                                date = name.replace("backup-","")
            if not date:
                date = name.replace("backup-", "")

            data.append({
                "Name": name,
                "Version": version,
                "Size": utils.humanbytes(int(size)),
                "Restorable": restorable,
                "Date": date,
            })
    except FdbAgentError as e:
        logger.error(f"Failed to list backups: {e}")
        return False

    return utils.print_table(data)



def backup_status():
    try:
        output = __get_fdb_agent().exec("fdbbackup status")
    except FdbAgentError as e:
        logger.error(f"Failed to get backup status: {e}")
        return False
    logger.info(f"backup status: \n{output.strip()}")
    return True


def backup_restore(backup_name, cluster_id):
    cluster = db_controller.get_cluster_by_id(cluster_id)
    backup_path = cluster.get_backup_path(backup_name)
    try:
        agent = __get_fdb_agent()
        logger.info(agent.exec("fdbcli --exec \"writemode on; clearrange \\\"\\\" \\xff\"").strip())
        logger.info(agent.exec(
            f"fdbrestore start -r \"{backup_path}\" --dest-cluster-file {constants.KVD_DB_FILE_PATH}").strip())
    except FdbAgentError as e:
        logger.error(f"Failed to restore backup {backup_name}: {e}")
        return False
    return True


def parse_history_param(history_string):
    if not history_string:
        logger.error("Invalid history value")
        return False
    results = re.search(r'^(\d+[hmd])(\d+[hmd])?$', history_string.lower())
    if not results:
        logger.error(f"Error parsing history string: {history_string}")
        return False
    total_seconds = 0
    for s in results.groups():
        if not s:
            continue
        ind = s[-1]
        v = int(s[:-1])
        if ind == 'd':
            total_seconds += v*60*60*24
        if ind == 'h':
            total_seconds += v*60*60
        if ind == 'm':
            total_seconds += v*60
    return int(total_seconds)


def backup_configure(cluster_id, backup_path, backup_frequency, bucket_name, region_name, backup_credentials):
    cluster = db_controller.get_cluster_by_id(cluster_id)
    if cluster:
        if backup_frequency:
            total_seconds = parse_history_param(backup_frequency)
            cluster.backup_frequency_seconds = total_seconds
        if backup_path:
            if not backup_path.startswith("file://"):
                backup_path = f"file://{backup_path}"
            cluster.backup_local_path = backup_path
            cluster.backup_s3_region = ""
            cluster.backup_s3_bucket = ""
            cluster.backup_s3_cred =  ""
            cluster.write_to_db()
            return True
        else:
            backup_path = f"blobstore://{backup_credentials}@s3.{region_name}.amazonaws.com/?bucket={bucket_name}&region={region_name}&sc=0"
            try:
                output = __get_fdb_agent().exec(f"fdbbackup list -b {backup_path}")
            except FdbAgentError as e:
                logger.error(f"Failed to list backup from s3: {backup_path}")
                logger.error(e)
                return False

            logger.info(f"backup list from : {backup_path}")
            logger.info(output)
            cluster.backup_s3_region = region_name if region_name else ""
            cluster.backup_s3_bucket = bucket_name if bucket_name else ""
            cluster.backup_s3_cred = backup_credentials if backup_credentials else ""
            cluster.write_to_db()
        return True

def add_backup_task(cluster_id):
    try:
        cluster = db_controller.get_cluster_by_id(cluster_id)
    except Exception as e:
        logger.error(f"Failed to get cluster {cluster_id}: {e}")
        return False

    tasks = get_backup_tasks(cluster_id)
    for task in tasks:
        if task.status != JobSchedule.STATUS_DONE:
            logger.info(f"Backup task found: {tasks[0].uuid}")
            return False

    task_obj = JobSchedule()
    task_obj.uuid = str(uuid.uuid4())
    task_obj.cluster_id = cluster.get_id()
    task_obj.date = int(time.time())
    task_obj.max_retry = constants.TASK_EXEC_RETRY_COUNT
    task_obj.status = JobSchedule.STATUS_NEW
    task_obj.function_name = JobSchedule.FN_FDB_BACKUP
    task_obj.write_to_db()
    logger.info(f"Backup task created: {task_obj.uuid}")
    return task_obj.uuid


def get_backup_tasks(cluster_id):
    backup_tasks = []
    tasks = db_controller.get_job_tasks(cluster_id)
    for task in tasks:
        if task.function_name == JobSchedule.FN_FDB_BACKUP:
            backup_tasks.append(task)
    return backup_tasks
