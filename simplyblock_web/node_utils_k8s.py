#!/usr/bin/env python
# encoding: utf-8
import logging
import os
import time

from simplyblock_core.utils import get_k8s_batch_client


node_name = os.environ.get("HOSTNAME")
deployment_name = f"snode-spdk-deployment-{node_name}"
pod_name = deployment_name[:50]
namespace_id_file = '/etc/simplyblock/namespace'
default_namespace = 'default'

logger = logging.getLogger(__name__)


def get_namespace():
    if os.path.exists(namespace_id_file):
        with open(namespace_id_file, 'r') as f:
            out = f.read()
            return out
    return default_namespace

def wait_for_job_completion(job_name, namespace, timeout=60):
    batch_v1 = get_k8s_batch_client()
    for _ in range(timeout):
        job = batch_v1.read_namespaced_job(job_name, namespace)
        if job.status.succeeded and job.status.succeeded >= 1:
            return True
        elif job.status.failed and job.status.failed > 0:
            raise RuntimeError(f"Job '{job_name}' failed")
        time.sleep(3)
    raise TimeoutError(f"Timeout waiting for Job '{job_name}' to complete")
