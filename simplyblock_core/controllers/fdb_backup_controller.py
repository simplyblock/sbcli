# coding=utf-8
import datetime
import logging as lg
import re
import time
import uuid

import docker

from simplyblock_core import utils, constants
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.job_schedule import JobSchedule

logger = lg.getLogger()
db_controller = DBController()


def run_bash_in_pod(namespace: str, cmd: str) -> str:
    """Exec a bash command in a running pod and return combined stdout/stderr."""
    pod_name = "simplyblock-fdb-cluster-cluster-controller"
    full_pod_name=""
    container="foundationdb"
    api = utils.get_k8s_core_client()
    k8s_core_v1 = utils.get_k8s_core_client()
    resp = k8s_core_v1.list_namespaced_pod(namespace)
    for pod in resp.items:
        if pod.metadata.name.startswith(pod_name):
            full_pod_name = pod.metadata.name
            break
    if not full_pod_name:
        raise Exception("Pod not found")

    exec_command = ["/bin/sh", "-c", cmd]
    kwargs = dict(
        name=full_pod_name,
        namespace=namespace,
        command=exec_command,
        container=container,
        stderr=True,
        stdin=False,
        stdout=True,
        tty=False,
    )

    from kubernetes.stream import stream
    resp = stream(api.connect_get_namespaced_pod_exec, **kwargs)
    return resp

def run_cmd(cmd: str, cluster):
    if cluster.mode == "docker":
        container = __get_fdb_cont()
        if container:
            res = container.exec_run(cmd=cmd)
            return res.output.decode("utf-8")
        else:
            raise Exception("fdb-backup-agent container not found")
    else:
        return run_bash_in_pod(cluster.cr_namespace, cmd)

def __get_fdb_cont():
    snode = db_controller.get_mgmt_nodes()[0]
    if not snode:
        return
    node_docker = docker.DockerClient(base_url=f"tcp://{snode.docker_ip_port}", version="auto")
    for container in node_docker.containers.list():
        if container.name.startswith("app_fdb-backup-agent"): # type: ignore[union-attr]
            return container

def create_backup(cluster_id):
    cluster = db_controller.get_cluster_by_id(cluster_id)
    backup_path = cluster.get_backup_path()
    if cluster.backup_s3_bucket and cluster.backup_s3_cred:
        folder = f"backup-{str(datetime.datetime.now())}"
        folder = folder.replace(" ", "-")
        folder = folder.replace(":", "-")
        folder = folder.split(".")[0]
        backup_path = f"blobstore://{cluster.backup_s3_cred}@s3.{cluster.backup_s3_region}.amazonaws.com/{folder}?bucket={cluster.backup_s3_bucket}&region={cluster.backup_s3_region}&sc=0"

    res = run_cmd(f"fdbbackup start -d {backup_path} -w", cluster)
    logger.info(res)
    return True

def list_backups(cluster_id):
    data = []
    cluster = db_controller.get_cluster_by_id(cluster_id)
    backup_path = cluster.get_backup_path()
    res = run_cmd(f"fdbbackup list -b {backup_path}", cluster)
    logger.info(f"backup list from : {backup_path}")
    for line in res.splitlines():
        if not line or "backup-" not in line:
            continue

        name = line.split("/")[-1].strip()
        name = name.split("?")[0]
        size = 0
        restorable = 0
        date = ""
        version = 0
        res = run_cmd(f"fdbbackup describe -d {cluster.get_backup_path(name)} --version-timestamps", cluster)
        for line in res.splitlines():
            if line and line.startswith("SnapshotBytes"):
                size = line.split()[1].strip()
            if line and line.startswith("Restorable"):
                restorable = line.split()[1].strip()
            if line and line.startswith("Snapshot:"):
                for param in line.split():
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

    return utils.print_table(data)


def backup_status():
    cluster = db_controller.get_clusters()[0]
    res = run_cmd("fdbbackup status", cluster)
    logger.info(f"backup status: \n{res.strip()}")
    return True


def backup_restore(backup_name, cluster_id):
    cluster = db_controller.get_cluster_by_id(cluster_id)
    backup_path = cluster.get_backup_path(backup_name)
    res = run_cmd("fdbcli --exec \"writemode on; clearrange \\\"\\\" \\xff\"", cluster)
    logger.info(res.strip())
    res = run_cmd(f"fdbrestore start -r \"{backup_path}\" --dest-cluster-file {constants.KVD_DB_FILE_PATH}", cluster)
    logger.info(res.strip())
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
            res = run_cmd(f"fdbbackup list -b {backup_path}", cluster)
            if res:
                logger.info(f"backup list from : {backup_path}")
                logger.info(res)
                cluster.backup_s3_region = region_name if region_name else ""
                cluster.backup_s3_bucket = bucket_name if bucket_name else ""
                cluster.backup_s3_cred = backup_credentials if backup_credentials else ""
                cluster.write_to_db()
            else:
                logger.error(f"Failed to list backup from s3: {backup_path}")
                logger.error(res)
                return False
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
