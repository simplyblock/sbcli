# coding=utf-8
import datetime
import json
import logging
import uuid
import time
import requests

import docker

from simplyblock_core import utils, scripts
from simplyblock_core.controllers import mgmt_events
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.mgmt_node import MgmtNode


logger = logging.getLogger()


def deploy_mgmt_node(cluster_ip, cluster_id, ifname, cluster_secret):

    try:
        headers = {'Authorization': f'{cluster_id} {cluster_secret}'}
        resp = requests.get(f"http://{cluster_ip}/cluster/{cluster_id}", headers=headers)
        resp_json = resp.json()
        cluster_data = resp_json['results'][0]
        logger.info(f"Cluster found, NQN:{cluster_data['nqn']}")
        logger.debug(cluster_data)
    except Exception as e:
        logger.error("Error getting cluster data!")
        logger.error(e)
        return ""

    logger.info("Installing dependencies...")
    scripts.install_deps()
    logger.info("Installing dependencies > Done")

    if not ifname:
        ifname = "eth0"

    DEV_IP = utils.get_iface_ip(ifname)
    if not DEV_IP:
        logger.error(f"Error getting interface ip: {ifname}")
        return False

    logger.info(f"Node IP: {DEV_IP}")
    ret = scripts.configure_docker(DEV_IP)

    db_connection = cluster_data['db_connection']
    scripts.set_db_config(db_connection)
    time.sleep(1)
    hostname = utils.get_hostname()
    db_controller = DBController()
    nodes = db_controller.get_mgmt_nodes()
    if not nodes:
        logger.error("No mgmt nodes was found in the cluster!")
        return False
    for node in nodes:
        if node.hostname == hostname:
            logger.error("Node already exists in the cluster")
            return False

    logger.info("Joining docker swarm...")
    try:
        cluster_docker = utils.get_docker_client(cluster_id)
        docker_ip = cluster_docker.info()["Swarm"]["NodeAddr"]
        join_token = cluster_docker.swarm.attrs['JoinTokens']['Manager']
        node_docker = docker.DockerClient(base_url=f"tcp://{DEV_IP}:2375", version="auto")
        if node_docker.info()["Swarm"]["LocalNodeState"] == "active":
            logger.info("Node is part of another swarm, leaving swarm")
            try:
                cluster_docker.nodes.get(node_docker.info()["Swarm"]["NodeID"]).remove(force=True)
            except:
                pass
            node_docker.swarm.leave(force=True)
            time.sleep(5)

        node_docker.swarm.join([f"{docker_ip}:2377"], join_token)

        retries = 10
        while retries > 0:
            if node_docker.info()["Swarm"]["LocalNodeState"] == "active":
                break
            logger.info("Waiting for node to be active...")
            retries -= 1
            time.sleep(2)
        logger.info("Joining docker swarm > Done")
        time.sleep(5)

    except Exception as e:
        raise e

    logger.info("Adding management node object")
    node_id = add_mgmt_node(DEV_IP, cluster_id)

    # check if ha setting is required
    nodes = db_controller.get_mgmt_nodes()
    if len(nodes) >= 3:
        logger.info("Waiting for FDB container to be active...")
        fdb_cont = None
        retries = 30
        while retries > 0 and fdb_cont is None:
            logger.info("Looking for FDB container...")
            for cont in node_docker.containers.list(all=True):
                logger.debug(cont.attrs['Name'])
                if cont.attrs['Name'].startswith("/app_fdb"):
                    fdb_cont = cont
                    break
            if fdb_cont:
                logger.info("FDB container found")
                break
            else:
                retries -= 1
                time.sleep(5)

        if not fdb_cont:
            logger.warning("FDB container was not found")
        else:
            retries = 10
            while retries > 0:
                info = node_docker.containers.get(fdb_cont.attrs['Id'])
                status = info.attrs['State']["Status"]
                is_running = info.attrs['State']["Running"]
                if not is_running:
                    logger.info("Container is not running, waiting...")
                    time.sleep(3)
                    retries -= 1
                else:
                    logger.info(f"Container status: {status}, Is Running: {is_running}")
                break

        logger.info("Configuring Double DB...")
        time.sleep(3)
        scripts.set_db_config_double()
        for cl in db_controller.get_clusters():
            cl.ha_type = "ha"
            cl.write_to_db(db_controller.kv_store)

    logger.info("Node joined the cluster")
    return node_id


def add_mgmt_node(mgmt_ip, cluster_id=None):
    db_controller = DBController()
    hostname = utils.get_hostname()
    node = db_controller.get_mgmt_node_by_hostname(hostname)
    if node:
        logger.error("Node already exists in the cluster")
        return False

    node = MgmtNode()
    node.uuid = str(uuid.uuid4())
    node.hostname = hostname
    node.docker_ip_port = f"{mgmt_ip}:2375"
    node.cluster_id = cluster_id
    node.mgmt_ip = mgmt_ip
    node.status = MgmtNode.STATUS_ONLINE
    node.create_dt = str(datetime.datetime.now())

    node.write_to_db(db_controller.kv_store)

    mgmt_events.mgmt_add(node)
    logger.info("Done")
    return node.uuid


def list_mgmt_nodes(is_json):
    db_controller = DBController()
    nodes = db_controller.get_mgmt_nodes()
    data = []
    output = ""

    for node in nodes:
        logging.debug(node)
        logging.debug("*" * 20)
        data.append({
            "UUID": node.get_id(),
            "Hostname": node.hostname,
            "IP": node.mgmt_ip,
            "Status": node.status,
        })

    if not data:
        return output

    if is_json:
        output = json.dumps(data, indent=2)
    else:
        output = utils.print_table(data)
    return output


def remove_mgmt_node(uuid):
    db_controller = DBController()
    snode = db_controller.get_mgmt_node_by_id(uuid)
    if not snode:
        logger.error("can not find node")
        return False

    logging.info("Removing mgmt node")
    snode.remove(db_controller.kv_store)

    logger.info("Leaving swarm...")
    node_docker = docker.DockerClient(base_url=f"tcp://{snode.docker_ip_port}", version="auto")
    node_docker.swarm.leave()

    mgmt_events.mgmt_remove(snode)
    logging.info("done")

