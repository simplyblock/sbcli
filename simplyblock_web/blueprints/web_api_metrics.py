#!/usr/bin/env python
# encoding: utf-8
import json
import logging
import os

from flask import Blueprint
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core import db_controller

from prometheus_client import generate_latest
from flask import Response
from prometheus_client import Gauge, CollectorRegistry


logger = logging.getLogger(__name__)

bp = Blueprint("metrics", __name__)

registry = CollectorRegistry()
db = db_controller.DBController()

io_stats_keys = [
    "date",
    "read_bytes",
    "read_bytes_ps",
    "read_io_ps",
    "read_io",
    "read_latency_ps",
    "write_bytes",
    "write_bytes_ps",
    "write_io",
    "write_io_ps",
    "write_latency_ps",
    "size_total",
    "size_prov",
    "size_used",
    "size_free",
    "size_util",
    "size_prov_util",
    "read_latency_ticks",
    "record_duration",
    "record_end_time",
    "record_start_time",
    "unmap_bytes",
    "unmap_bytes_ps",
    "unmap_io",
    "unmap_io_ps",
    "unmap_latency_ps",
    "unmap_latency_ticks",
    "write_latency_ticks",
]

ng = {}
cg = {}
dg = {}
lg = {}
pg = {}

def get_device_metrics():
    global dg
    if not dg:
        labels = ['cluster', "snode", "device"]
        for k in io_stats_keys + ["status_code", "health_check"]:
            dg["device_" + k] = Gauge("device_" + k, "device_" + k, labelnames=labels, registry=registry)
    return dg

def get_snode_metrics():
    global ng
    if not ng:
        labels = ['cluster', "snode"]
        for k in io_stats_keys + ["status_code", "health_check"]:
            ng["snode_" + k] = Gauge("snode_" + k, "snode_" + k, labelnames=labels, registry=registry)
    return ng

def get_cluster_metrics():
    global cg
    if not cg:
        labels = ['cluster']
        for k in io_stats_keys + ["status_code"]:
            cg["cluster_" + k] = Gauge("cluster_" + k, "cluster_" + k, labelnames=labels, registry=registry)
    return cg

def get_lvol_metrics():
    global lg
    if not lg:
        labels = ['cluster', "pool", "lvol"]
        for k in io_stats_keys + ["status_code", "health_check"]:
            lg["lvol_" + k] = Gauge("lvol_" + k, "lvol_" + k, labelnames=labels, registry=registry)
    return lg

def get_pool_metrics():
    global pg
    if not pg:
        labels = ['cluster', "pool", "name"]
        for k in io_stats_keys + ["status_code"]:
            pg["pool_" + k] = Gauge("pool_" + k, "pool_" + k, labelnames=labels, registry=registry)
    return pg


@bp.route('/cluster/metrics', methods=['GET'])
def get_data():

    clusters = db.get_clusters()
    for cl in clusters:

        records = db.get_cluster_stats(cl, 1)
        if records:
            data = records[0].get_clean_dict()
            ng = get_cluster_metrics()
            for g in ng:
                v = g.replace("cluster_", "")
                if v in data:
                    ng[g].labels(cluster=cl.get_id()).set(data[v])
                elif v == "status_code":
                    ng[g].labels(cluster=cl.get_id()).set(cl.get_status_code())

        snodes = db.get_storage_nodes_by_cluster_id(cl.get_id())
        for node in snodes:
            logger.info("Node: %s", node.get_id())
            if node.status != StorageNode.STATUS_ONLINE:
                logger.info("Node is not online, skipping")
                continue

            if not node.nvme_devices:
                logger.error("No devices found in node: %s", node.get_id())
                continue

            records = db.get_node_stats(node, 1)
            if records:
                data = records[0].get_clean_dict()
                ng = get_snode_metrics()
                for g in ng:
                    v = g.replace("snode_", "")
                    if v in data:
                        ng[g].labels(cluster=cl.get_id(), snode=node.get_id()).set(data[v])
                    elif v == "status_code":
                        ng[g].labels(cluster=cl.get_id(), snode=node.get_id()).set(node.get_status_code())
                    elif v == "health_check":
                        ng[g].labels(cluster=cl.get_id(), snode=node.get_id()).set(node.health_check)

            for device in node.nvme_devices:

                logger.info("Getting device stats: %s", device.uuid)
                if device.status not in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_READONLY, NVMeDevice.STATUS_CANNOT_ALLOCATE]:
                    logger.info(f"Device is skipped: {device.get_id()} status: {device.status}")
                    continue

                records= db.get_device_stats(device, 1)
                if records:
                    data = records[0].get_clean_dict()
                    ng = get_device_metrics()
                    for g in ng:
                        v = g.replace("device_", "")
                        if v in data:
                            ng[g].labels(cluster=cl.get_id(), snode=node.get_id(), device=device.get_id()).set(data[v])
                        elif v == "status_code":
                            ng[g].labels(cluster=cl.get_id(), snode=node.get_id(), device=device.get_id()).set(
                                device.get_status_code())
                        elif v == "health_check":
                            ng[g].labels(cluster=cl.get_id(), snode=node.get_id(), device=device.get_id()).set(
                                device.health_check)


        for pool in db.get_pools():

            records = db.get_pool_stats(pool, 1)
            if records:
                data = records[0].get_clean_dict()
                ng = get_pool_metrics()
                for g in ng:
                    v = g.replace("pool_", "")
                    if v in data:
                        ng[g].labels(cluster=cl.get_id(), name=pool.pool_name, pool=pool.get_id()).set(data[v])
                    elif v == "status_code":
                        ng[g].labels(cluster=cl.get_id(), name=pool.pool_name, pool=pool.get_id()).set(
                            pool.get_status_code())

        for lvol in db.get_lvols(cl.get_id()):
            records = db.get_lvol_stats(lvol, limit=1)
            if records:
                data = records[0].get_clean_dict()
                ng = get_lvol_metrics()
                for g in ng:
                    v = g.replace("lvol_", "")
                    if v in data:
                        ng[g].labels(cluster=cl.get_id(), lvol=lvol.get_id(), pool=lvol.pool_name).set(data[v])
                    elif v == "status_code":
                        ng[g].labels(cluster=cl.get_id(), lvol=lvol.get_id(), pool=lvol.pool_name).set(
                            lvol.get_status_code())
                    elif v == "health_check":
                        ng[g].labels(cluster=cl.get_id(), lvol=lvol.get_id(), pool=lvol.pool_name).set(
                            lvol.health_check)


    return Response(generate_latest(registry), mimetype=str('text/plain; version=0.0.4; charset=utf-8'))
