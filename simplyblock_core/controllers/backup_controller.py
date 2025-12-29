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
# backup_path = constants.KVD_DB_BACKUP_PATH
clusters = db_controller.get_clusters()
if clusters:
    cluster = clusters[0]


def __get_fdb_cont():
    snode = db_controller.get_mgmt_nodes()[0]
    if not snode:
        return
    node_docker = docker.DockerClient(base_url=f"tcp://{snode.docker_ip_port}", version="auto")
    for container in node_docker.containers.list():
        if container.name.startswith("app_fdb-server"): # type: ignore[union-attr]
            return container

def create_backup():
    container = __get_fdb_cont()
    if container:
        backup_path = cluster.backup_local_path
        if cluster.backup_s3_bucket and cluster.backup_s3_cred:
            folder = f"backup-{str(datetime.datetime.now())}"
            folder = folder.replace(" ", "-")
            folder = folder.replace(":", "-")
            folder = folder.split(".")[0]
            backup_path = f"blobstore://{cluster.backup_s3_cred}@s3.{cluster.backup_s3_region}.amazonaws.com/{folder}?bucket={cluster.backup_s3_bucket}&region={cluster.backup_s3_region}&sc=0"

        res = container.exec_run(cmd=f"fdbbackup start -d {backup_path} -w")
        cont = res.output.decode("utf-8")
        logger.info(cont)
        return True
    return False

def list_backups():
    container = __get_fdb_cont()
    data = []
    if container:
        backup_path = cluster.get_backup_path()
        res = container.exec_run(cmd=f"fdbbackup list -b {backup_path}")
        logger.info(f"backup list from : {backup_path}")
        cont = res.output.decode("utf-8")
        for line in cont.splitlines():
            if not line or "backup-" not in line:
                continue

            name = line.split("/")[-1].strip()
            name = name.split("?")[0]
            size = 0
            restorable = 0
            date = ""
            version = 0
            res = container.exec_run(cmd=f"fdbbackup describe -d {cluster.get_backup_path(name)} --version-timestamps")
            cont = res.output.decode("utf-8")
            for line in cont.splitlines():
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

    return True



def backup_status():
    container = __get_fdb_cont()
    if container:
        res = container.exec_run(cmd="fdbbackup status")
        cont = res.output.decode("utf-8")
        logger.info(f"backup status: \n{cont.strip()}")
        return True


def backup_restore(backup_name):
    container = __get_fdb_cont()
    if container:
        backup_path = cluster.get_backup_path(backup_name)
        res = container.exec_run(cmd="fdbcli --exec \"writemode on; clearrange \\\"\\\" \\xff\"")
        cont = res.output.decode("utf-8")
        logger.info(cont.strip())
        res = container.exec_run(cmd=f"fdbrestore start -r \"{backup_path}\" --dest-cluster-file {constants.KVD_DB_FILE_PATH}")
        cont = res.output.decode("utf-8")
        logger.info(cont.strip())
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


def backup_configure(backup_path, backup_frequency, bucket_name, region_name, backup_credentials):
    clusters = db_controller.get_clusters()
    if clusters:
        if backup_path:
            if not backup_path.startswith("file://"):
                backup_path = f"file://{backup_path}"
            clusters[0].backup_local_path = backup_path
        if backup_frequency:
            total_seconds = parse_history_param(backup_frequency)
            clusters[0].backup_frequency_seconds = total_seconds
        clusters[0].backup_s3_region = region_name if region_name else ""
        clusters[0].backup_s3_bucket = bucket_name if bucket_name else ""
        clusters[0].backup_s3_cred = backup_credentials if backup_credentials else ""
        clusters[0].write_to_db()
        return True

