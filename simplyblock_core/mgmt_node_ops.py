# coding=utf-8
import datetime
import json
import os
import logging
import uuid
import time
import requests

import docker
from kubernetes import client as k8s_client


from simplyblock_core import utils, scripts, constants
from simplyblock_core.controllers import mgmt_events
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.mgmt_node import MgmtNode

logger = logging.getLogger()



def deploy_mgmt_node(cluster_ip, cluster_id, ifname, mgmt_ip, cluster_secret, mode):

    try:
        headers = {'Authorization': f'{cluster_id} {cluster_secret}'}
        resp = requests.get(f"http://{cluster_ip}/api/v1/cluster/{cluster_id}", headers=headers)
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

    if mode == "docker":
        if not ifname:
            ifname = "eth0"

        dev_ip = utils.get_iface_ip(ifname)
        if not dev_ip:
            logger.error(f"Error getting interface ip: {ifname}")
            return False

        logger.info(f"Node IP: {dev_ip}")
        scripts.configure_docker(dev_ip)

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

        if not cluster_data['disable_monitoring']:
            utils.render_and_deploy_alerting_configs(cluster_data['contact_point'], cluster_data['grafana_endpoint'],
                                                                        cluster_data['uuid'], cluster_data['secret'])

        logger.info("Joining docker swarm...")
        try:
            cluster_docker = utils.get_docker_client(cluster_id)
            docker_ip = cluster_docker.info()["Swarm"]["NodeAddr"]
            join_token = cluster_docker.swarm.attrs['JoinTokens']['Manager']
            node_docker = docker.DockerClient(base_url=f"tcp://{dev_ip}:2375", version="auto")
            if node_docker.info()["Swarm"]["LocalNodeState"] == "active":
                logger.info("Node is part of another swarm, leaving swarm")
                try:
                    cluster_docker.nodes.get(node_docker.info()["Swarm"]["NodeID"]).remove(force=True)
                except Exception:
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

    elif mode == "kubernetes":
        dev_ip = mgmt_ip
        if not dev_ip:
            logger.error("Error getting ip: For Kubernetes-based deployments, please supply --mgmt-ip.")
            return False

        logger.info(f"Node IP: {dev_ip}")

        db_connection = cluster_data['db_connection']
        db_controller = DBController()
        nodes = db_controller.get_mgmt_nodes()
        if not nodes:
            logger.error("No mgmt nodes was found in the cluster!")
            return False


    logger.info("Adding management node object")
    node_id = add_mgmt_node(dev_ip, mode, cluster_id)

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

            logger.info("Configuring Double DB...")
            time.sleep(3)
            scripts.set_db_config_double()
            
        elif mode == "kubernetes":
            utils.load_kube_config_with_fallback()
            v1 = k8s_client.CoreV1Api()
            apps_v1 = k8s_client.AppsV1Api()
            api_cr = k8s_client.CustomObjectsApi()
                        
            response = apps_v1.patch_namespaced_stateful_set(
                name=constants.OS_STATEFULSET_NAME,
                namespace=constants.K8S_NAMESPACE,
                body=constants.os_patch
            )

            logger.info(f"Patched StatefulSet {constants.OS_STATEFULSET_NAME}: {response.status.replicas} replicas")

            response = api_cr.patch_namespaced_custom_object(
                group="mongodbcommunity.mongodb.com",
                version="v1",
                plural="mongodbcommunity", 
                name=constants.MONGODB_STATEFULSET_NAME,
                namespace=constants.K8S_NAMESPACE,
                body=constants.mongodb_patch
            )

            logger.info(f"Patched MongoDB CR {constants.MONGODB_STATEFULSET_NAME}")
            max_wait = 300 
            interval = 5
            waited = 0
            while waited < max_wait:
                if utils.all_pods_ready(v1, constants.MONGODB_STATEFULSET_NAME, constants.K8S_NAMESPACE, 3):
                    logger.info("All MongoDB pods are ready.")
                    break
                time.sleep(interval)
                waited += interval
            else:
                raise TimeoutError("MongoDB pods did not become ready in time.")

            monitoring_secret = os.environ.get("MONITORING_SECRET", "")
            
            graylog_patch = utils.build_graylog_patch(monitoring_secret)
            
            response = apps_v1.patch_namespaced_deployment(
                name=constants.GRAYLOG_STATEFULSET_NAME,
                namespace=constants.K8S_NAMESPACE,
                body=graylog_patch
            )

            logger.info("Patched Graylog MongoDB URI for replicaset support")

            response = apps_v1.patch_namespaced_stateful_set(
                name=constants.PROMETHEUS_STATEFULSET_NAME,
                namespace=constants.K8S_NAMESPACE,
                body=constants.prometheus_patch
            )

            logger.info(f"Patched StatefulSet {constants.PROMETHEUS_STATEFULSET_NAME}: {response.status.replicas} replicas")

    logger.info("Node joined the cluster")
    return node_id


def add_mgmt_node(mgmt_ip, mode, cluster_id=None):
    db_controller = DBController()
    hostname = ""
    if mode == "docker":
        hostname = utils.get_hostname()
    try:
        node = db_controller.get_mgmt_node_by_hostname(hostname)
        if node:
            logger.error("Node already exists in the cluster")
            return False
    except KeyError:
        pass

    node = MgmtNode()
    node.uuid = str(uuid.uuid4())
    node.hostname = hostname
    node.docker_ip_port = f"{mgmt_ip}:2375"
    node.cluster_id = cluster_id
    node.mgmt_ip = mgmt_ip
    node.mode = mode
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
    try:
        snode = db_controller.get_mgmt_node_by_id(uuid)
    except KeyError as e:
        logger.error(e)
        return False

    logging.info("Removing mgmt node")
    snode.remove(db_controller.kv_store)
    if snode.mode == "docker":
        logger.info("Leaving swarm...")
        node_docker = docker.DockerClient(base_url=f"tcp://{snode.docker_ip_port}", version="auto")
        node_docker.swarm.leave(force=True)

    elif snode.mode == "kubernetes":
        utils.load_kube_config_with_fallback()
        
    mgmt_events.mgmt_remove(snode)
    logging.info("done")

