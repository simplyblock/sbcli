# coding=utf-8
import logging as lg

import docker

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
            container.exec_run(cmd=f"fdbbackup start -d {backup_path}")
            logger.info(f"backup start: {backup_path}")
            break

    logger.info("done")



def backup_status():
    snode = db_controller.get_mgmt_nodes()[0]
    if not snode:
        logger.error("can not find node")
        return False

    node_docker = docker.DockerClient(base_url=f"tcp://{snode.docker_ip_port}", version="auto")
    for container in node_docker.containers.list():
        if container.name.startswith("app_fdb-server"):
            res = container.exec_run(cmd=f"fdbbackup status")
            logger.info(res.exit_code)
            logger.info(f"backup status: {res.output}")
            break


def backup_list():
    snode = db_controller.get_mgmt_nodes()[0]
    if not snode:
        logger.error("can not find node")
        return False

    node_docker = docker.DockerClient(base_url=f"tcp://{snode.docker_ip_port}", version="auto")
    for container in node_docker.containers.list():
        if container.name.startswith("app_fdb-server"):
            res = container.exec_run(cmd=f"fdbbackup list -b {backup_path}")
            logger.info(res.exit_code)
            logger.info(f"backup list: \n{res.output}")
            break

