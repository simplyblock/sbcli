#!/usr/bin/env python
# encoding: utf-8
import json
import logging
import os

from flask import Blueprint
from simplyblock_core import constants

PROMETHEUS_MULTIPROC_DIR = constants.PROMETHEUS_MULTIPROC_DIR
os.environ["PROMETHEUS_MULTIPROC_DIR"] = PROMETHEUS_MULTIPROC_DIR

from prometheus_client import generate_latest, multiprocess
from flask import Response
from prometheus_client import CollectorRegistry


logger = logging.getLogger(__name__)

bp = Blueprint("metrics", __name__)

registry = CollectorRegistry()
multiprocess.MultiProcessCollector(registry)

@bp.route('/cluster/metrics', methods=['GET'])
def get_data():
    return Response(generate_latest(registry), mimetype=str('text/plain; version=0.0.4; charset=utf-8'))
