#!/usr/bin/env python
# encoding: utf-8
import argparse
import logging
import os
import random

from flask import Flask, request, jsonify

import utils
from simplyblock_core import constants

logger_handler = logging.StreamHandler()
logger_handler.setFormatter(logging.Formatter('%(asctime)s: %(levelname)s: %(message)s'))
logger = logging.getLogger()
logger.addHandler(logger_handler)
logger.setLevel(logging.DEBUG)


app = Flask(__name__)
app.url_map.strict_slashes = False
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

bdev_info = {
    "name": "nvme_1cn1",
    "aliases": [
        "fa688ea2-2e38-5624-b834-a88e9e875df0"
    ],
    "product_name": "NVMe disk",
    "block_size": 512,
    "num_blocks": 209715200,
    "uuid": "fa688ea2-2e38-5624-b834-a88e9e875df0",
    "assigned_rate_limits": {
        "rw_ios_per_sec": 0,
        "rw_mbytes_per_sec": 0,
        "r_mbytes_per_sec": 0,
        "w_mbytes_per_sec": 0
    },
    "claimed": False,
    "zoned": False,
    "supported_io_types": {
        "read": True,
        "write": True,
        "unmap": False,
        "write_zeroes": True,
        "flush": True,
        "reset": True,
        "compare": False,
        "compare_and_write": False,
        "abort": True,
        "nvme_admin": True,
        "nvme_io": True
    },
    "driver_specific": {
        "nvme": [
            {
                "pci_address": "0000:00:1c.0",
                "trid": {
                    "trtype": "PCIe",
                    "traddr": "0000:00:1c.0"
                },
                "ctrlr_data": {
                    "cntlid": 0,
                    "vendor_id": "0x1d0f",
                    "model_number": "Amazon Elastic Block Store",
                    "serial_number": "vol00ade7db96108da77",
                    "firmware_revision": "2.0",
                    "subnqn": "nqn:2008-08.com.amazon.aws:ebs:vol00ade7db96108da77",
                    "oacs": {
                        "security": 0,
                        "format": 0,
                        "firmware": 0,
                        "ns_manage": 0
                    },
                    "multi_ctrlr": False,
                    "ana_reporting": False
                },
                "vs": {
                    "nvme_version": "1.4"
                },
                "ns_data": {
                    "id": 1,
                    "can_share": False
                }
            }
        ],
        "mp_policy": "active_passive"
    }
}

bdev_io_stats = {
    "tick_rate":2900000000,
    "ticks":2811277115586,
    "bdevs":[{
        "name":"nvme_1cn1",
        "bytes_read":45502976,
        "num_read_ops":10323,
        "bytes_written":775093760,
        "num_write_ops":82220,
        "bytes_unmapped":0,
        "num_unmap_ops":0,
        "bytes_copied":0,
        "num_copy_ops":0,
        "read_latency_ticks":15501865718,
        "max_read_latency_ticks":21379600,
        "min_read_latency_ticks":515468,
        "write_latency_ticks":202590975730,
        "max_write_latency_ticks":81989542,
        "min_write_latency_ticks":679586,
        "unmap_latency_ticks":0,
        "max_unmap_latency_ticks":0,
        "min_unmap_latency_ticks":0,
        "copy_latency_ticks":0,
        "max_copy_latency_ticks":0,
        "min_copy_latency_ticks":0,
        "io_error":{},
        "driver_specific":{}}]}
@app.route('/', methods=['GET'])
def status():
    return utils.get_response("Live")


def get_io_stats(rand):
    bdev_io_stats['bdevs'][0]["bytes_read"] *= rand
    bdev_io_stats['bdevs'][0]["num_read_ops"] *= rand
    bdev_io_stats['bdevs'][0]["bytes_written"] *= rand
    bdev_io_stats['bdevs'][0]["num_write_ops"] *= rand
    bdev_io_stats['bdevs'][0]["read_latency_ticks"] *= rand
    bdev_io_stats['bdevs'][0]["write_latency_ticks"] *= rand
    return bdev_io_stats


@app.route('/', methods=['POST'])
def rpc_method():
    data = request.get_json()
    method = data.get('method')
    params = data.get('params')
    if method == "spdk_get_version":
        result = {"version": "mock"}

    elif method == "bdev_get_iostat":
        rand_int = random.randint(1,10)
        result = get_io_stats(rand_int)

    elif method == "alceml_get_pages_usage":
        result = {"res":1,"npages_allocated":400,"npages_used":354,"npages_nmax":51100,"pba_page_size":2097152,"nvols":9}

    elif method == "ultra21_util_get_malloc_stats":
        result = {"socket_id":0,
                  "heap_totalsz_bytes":2130706432,
                  "heap_freesz_bytes":9285696,
                  "greatest_free_size":1052800,
                  "free_count":1306,
                  "alloc_count":2951,
                  "heap_allocsz_bytes":2121420736}

    else:
        result = [bdev_info]

    return jsonify({"jsonrpc":"2.0","id":1,"result":result})


MODES = [
    "storage_node",
    "caching_docker_node",
    "caching_kubernetes_node",
    "storage_node_mock",
]

parser = argparse.ArgumentParser()
parser.add_argument("mode", choices=MODES)
parser.add_argument("port", type=int, default=5000)


if __name__ == '__main__':
    args = parser.parse_args()
    port = args.port
    mode = args.mode
    if mode == "caching_docker_node":
        from blueprints import node_api_basic, node_api_caching_docker
        app.register_blueprint(node_api_basic.bp)
        app.register_blueprint(node_api_caching_docker.bp)

    if mode == "caching_kubernetes_node":
        from blueprints import node_api_basic, node_api_caching_ks
        app.register_blueprint(node_api_basic.bp)
        app.register_blueprint(node_api_caching_ks.bp)

    if mode == "storage_node":
        from blueprints import snode_ops
        app.register_blueprint(snode_ops.bp)

    if mode == "storage_node_mock":
        os.environ["MOCK_PORT"] = str(port)
        from blueprints import snode_ops_mock
        app.register_blueprint(snode_ops_mock.bp)
        # app.add_url_rule("/", view_func=rpc_method, methods=['POST'])

    app.run(host='0.0.0.0', debug=constants.LOG_WEB_DEBUG, port=port)
