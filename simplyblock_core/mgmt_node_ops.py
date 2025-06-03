# coding=utf-8
import datetime
import json
import logging
import uuid
import time
import requests
import socket

import docker
from kubernetes import client as k8s_client, config


from simplyblock_core import utils, scripts, constants
from simplyblock_core.controllers import mgmt_events
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.mgmt_node import MgmtNode

logger = logging.getLogger()


def deploy_mgmt_node(cluster_ip, cluster_id, ifname, cluster_secret, mode):

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
    scripts.install_deps(mode)
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

    if mode == "docker": 

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
        if mode == "docker":
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

        elif mode == "kubernetes":
            config.load_kube_config()
            v1 = k8s_client.CoreV1Api()
            apps_v1 = k8s_client.AppsV1Api()
            namespace = "simplyblock"
            os_statefulset_name = "simplyblock-opensearch"
            mongodb_statefulset_name = "simplyblock-mongodb"
            graylog_deploy_name = "simplyblock-graylog"
            
            response = apps_v1.patch_namespaced_stateful_set(
                name=os_statefulset_name,
                namespace=namespace,
                body=constants.os_patch
            )

            logger.info(f"Patched StatefulSet {os_statefulset_name}: {response.status.replicas} replicas")

            response = apps_v1.patch_namespaced_stateful_set(
                name=mongodb_statefulset_name,
                namespace=namespace,
                body=constants.mongodb_patch
            )

            logger.info(f"Patched StatefulSet {mongodb_statefulset_name}: {response.status.replicas} replicas")
            max_wait = 120 
            interval = 5
            waited = 0
            while waited < max_wait:
                if utils.all_pods_ready(v1, mongodb_statefulset_name, namespace, 3):
                    logger.info("All MongoDB pods are ready.")
                    break
                time.sleep(interval)
                waited += interval
            else:
                raise TimeoutError("MongoDB pods did not become ready in time.")

            utils.initiate_mongodb_rs()

            response = apps_v1.patch_namespaced_deployment(
                name=graylog_deploy_name,
                namespace=namespace,
                body=constants.graylog_patch
            )

            logger.info("Patched Graylog MongoDB URI for replicaset support")

            current_node = socket.gethostname()
            logger.info(f"Waiting for FDB pod on this node: {current_node} to be active...")
            fdb_cont = None
            retries = 30
            while retries > 0 and fdb_cont is None:
                logger.info(f"Looking for FDB pod on node {current_node}...")
                pods = v1.list_namespaced_pod(namespace=namespace, label_selector="app=simplyblock-fdb-server").items
                for pod in pods:
                    if pod.spec.node_name == current_node:
                        fdb_cont = pod
                        break

                if fdb_cont:
                    logger.info("FDB pod found")
                    break
                else:
                    retries -= 1
                    time.sleep(5)
                    
            if not fdb_cont:
                logger.warning(f"FDB pod was not found on node {current_node}...")
            else:
                retries = 10
                while retries > 0:
                    if pod.status.phase == "Running":
                        logger.info(f"FDB pod is running: {pod.metadata.name}")
                        break
                    else:
                        logger.info("pod is not running, waiting...")
                        time.sleep(3)
                        retries -= 1
                            
        logger.info("Configuring Double DB...")
        time.sleep(3)
        scripts.set_db_config_double()

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
    node_docker.swarm.leave(force=True)

    mgmt_events.mgmt_remove(snode)
    logging.info("done")

