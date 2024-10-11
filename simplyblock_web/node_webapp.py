#!/usr/bin/env python
# encoding: utf-8
import argparse
import logging
import os

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


@app.route('/', methods=['GET'])
def status():
    return utils.get_response("Live")


@app.route('/', methods=['POST'])
def rpc_method():
    data = request.get_json()
    method = data.get('method')
    params = data.get('params')
    info = {
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
    return jsonify({"jsonrpc":"2.0","id":1,"result":[info], "method": method, "params": params})


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

    app.run(host='0.0.0.0', debug=constants.LOG_WEB_DEBUG, port=port)
