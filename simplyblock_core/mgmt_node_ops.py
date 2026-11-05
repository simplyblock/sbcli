import datetime
import logging
import os
import time
import uuid

import requests
from kubernetes import client as k8s_client
from pydantic import SecretStr

from simplyblock_core import constants, utils
from simplyblock_core.controllers import mgmt_events
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.mgmt_node import MgmtNode

logger = logging.getLogger()


def deploy_mgmt_node(cluster_ip, cluster_id, mgmt_ip, cluster_secret: SecretStr):

    try:
        headers = {'Authorization': f'{cluster_id} {cluster_secret.get_secret_value()}'}
        resp = requests.get(f"http://{cluster_ip}/api/v1/cluster/{cluster_id}", headers=headers)
        resp_json = resp.json()
        cluster_data = resp_json['results'][0]
        logger.info(f"Cluster found, NQN:{cluster_data['nqn']}")
        logger.debug(cluster_data)
    except Exception as e:
        logger.error("Error getting cluster data!")
        logger.error(e)
        return ""

    dev_ip = mgmt_ip
    if not dev_ip:
        logger.error("Error getting ip: please supply --mgmt-ip.")
        return False

    logger.info(f"Node IP: {dev_ip}")

    db_controller = DBController()
    nodes = db_controller.get_mgmt_nodes()
    if not nodes:
        logger.error("No mgmt nodes was found in the cluster!")
        return False

    logger.info("Adding management node object")
    node_id = add_mgmt_node(dev_ip, cluster_id)

    # check if ha setting is required
    nodes = db_controller.get_mgmt_nodes()
    if len(nodes) >= 3:
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


def add_mgmt_node(mgmt_ip, cluster_id=None):
    db_controller = DBController()

    for node in db_controller.get_mgmt_nodes():
        if node.mgmt_ip == mgmt_ip:
            logger.error("Node already exists in the cluster")
            return False

    node = MgmtNode()
    node.uuid = str(uuid.uuid4())
    node.cluster_id = cluster_id
    node.mgmt_ip = mgmt_ip
    node.status = MgmtNode.STATUS_ONLINE
    node.create_dt = str(datetime.datetime.now())

    node.write_to_db(db_controller.kv_store)

    mgmt_events.mgmt_add(node)
    logger.info("Done")
    return node.uuid


def list_mgmt_nodes():
    db_controller = DBController()
    nodes = db_controller.get_mgmt_nodes()
    data = []

    for node in nodes:
        logging.debug(node)
        logging.debug("*" * 20)
        data.append({
            "UUID": node.get_id(),
            "Hostname": node.hostname,
            "IP": node.mgmt_ip,
            "Status": node.status,
        })

    return data


def remove_mgmt_node(uuid):
    db_controller = DBController()
    try:
        snode = db_controller.get_mgmt_node_by_id(uuid)
    except KeyError as e:
        logger.error(e)
        return False

    logging.info("Removing mgmt node")
    snode.remove(db_controller.kv_store)
    mgmt_events.mgmt_remove(snode)
    logging.info("done")

