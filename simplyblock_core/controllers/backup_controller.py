# coding=utf-8
import datetime
import logging as lg

import docker

from simplyblock_core import utils, constants
from simplyblock_core.db_controller import DBController


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

def create_backup(tag_name):
    container = __get_fdb_cont()
    if container:
        backup_path = cluster.backup_local_path
        if cluster.backup_s3_bucket and cluster.backup_s3_cred:
            folder = f"backup-{str(datetime.datetime.now())}"
            backup_path = f"blobstore://{cluster.backup_s3_cred}@s3.{cluster.backup_s3_region}.amazonaws.com/{folder}?bucket={cluster.backup_s3_bucket}&{cluster.backup_s3_region}&sc=0"

        res = container.exec_run(cmd=f"fdbbackup start -d {backup_path}")
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
            size = 0
            restorable = 0
            res = container.exec_run(cmd=f"fdbbackup describe -d {line}")
            cont = res.output.decode("utf-8")
            for line in cont.splitlines():
                if line and line.startswith("SnapshotBytes"):
                    size = line.split()[1].strip()
                if line and line.startswith("Restorable"):
                    restorable = line.split()[1].strip()
            date = datetime.datetime.strptime(name.replace("backup-",""), "%Y-%m-%d-%H-%M-%S.%f").strftime(
                "%H:%M:%S, %d/%m/%Y")

            data.append({
                "Name": name,
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


def backup_restore(backup_name):
    container = __get_fdb_cont()
    if container:
        backup_path = cluster.get_backup_path()
        res = container.exec_run(cmd=f"fdbcli --exec \"writemode on; clearrange \"\" \xff\"; fdbrestore start -r {backup_path} --dest-cluster-file {constants.KVD_DB_FILE_PATH} -v {backup_name}")
        cont = res.output.decode("utf-8")
        logger.info(cont.strip())


def backup_configure(backup_path, backup_frequency, bucket_name, region_name, backup_credentials):
    clusters = db_controller.get_clusters()
    if clusters:
        clusters[0].backup_local_path = backup_path
        clusters[0].backup_frequency_seconds = backup_frequency
        clusters[0].backup_s3_region = region_name
        clusters[0].backup_s3_bucket = bucket_name
        clusters[0].backup_s3_cred = backup_credentials
        clusters[0].write_to_db()
        return True

