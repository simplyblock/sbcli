# coding=utf-8
import datetime
import logging as lg

import docker

from simplyblock_core import utils
from simplyblock_core.db_controller import DBController


logger = lg.getLogger()
db_controller = DBController()

backup_path = "file:///etc/foundationdb/backup"
def create_backup():
    snode = db_controller.get_mgmt_nodes()[0]
    if not snode:
        logger.error("can not find node")
        return False

    node_docker = docker.DockerClient(base_url=f"tcp://{snode.docker_ip_port}", version="auto")
    for container in node_docker.containers.list():
        if container.name.startswith("app_fdb-server"):
            res = container.exec_run(cmd=f"fdbbackup start -d {backup_path}")
            cont = res.output.decode("utf-8")
            logger.info(cont)
            logger.info(f"backup start: {backup_path}")
            break

    logger.info("done")
    return True



def list_backups():
    snode = db_controller.get_mgmt_nodes()[0]
    if not snode:
        logger.error("can not find node")
        return False

    data = []

    node_docker = docker.DockerClient(base_url=f"tcp://{snode.docker_ip_port}", version="auto")
    for container in node_docker.containers.list():
        if container.name.startswith("app_fdb-server"):

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
                    "Size": utils.humanbytes(size),
                    "Restorable": restorable,
                    "Date": date,
                })


            return utils.print_table(data)

    return True



def backup_status():
    snode = db_controller.get_mgmt_nodes()[0]
    if not snode:
        logger.error("can not find node")
        return False

    node_docker = docker.DockerClient(base_url=f"tcp://{snode.docker_ip_port}", version="auto")
    for container in node_docker.containers.list():
        if container.name.startswith("app_fdb-server"):
            res = container.exec_run(cmd=f"fdbbackup status")
            cont = res.output.decode("utf-8")
            logger.info(f"backup status: \n{cont.strip()}")
            break
    return True
