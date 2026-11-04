import logging
import os
import time

from kubernetes.client import ApiException

from simplyblock_core.utils import get_k8s_batch_client, get_k8s_core_client

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

def wait_for_job_completion(job_name, namespace, timeout=480):
    batch_v1 = get_k8s_batch_client()
    for _ in range(timeout):
        try:
            job = batch_v1.read_namespaced_job(job_name, namespace)
        except ApiException as e:
            if e.status == 404:
                # Job completed and was garbage-collected — treat as success.
                return True
            raise
        if job.status.succeeded and job.status.succeeded >= 1:
            return True
        elif job.status.failed and job.status.failed > 0:
            raise RuntimeError(f"Job '{job_name}' failed{_job_log_tail(job_name, namespace)}")
        time.sleep(3)
    raise TimeoutError(f"Timeout waiting for Job '{job_name}' to complete")


def _job_log_tail(job_name, namespace, lines=20):
    """The last log lines of a Job's pods, for the error that reports it failed.

    A node-preparation Job that refuses prints why, and that reason is only in
    the failed pod's log. Reading it is best effort: a failure to read it must
    not replace the failure being reported.
    """
    core_v1 = get_k8s_core_client()
    try:
        pods = core_v1.list_namespaced_pod(namespace, label_selector=f"job-name={job_name}").items
        tails = [core_v1.read_namespaced_pod_log(pod.metadata.name, namespace, tail_lines=lines).rstrip()
                 for pod in pods]
    except ApiException as e:
        logger.warning(f"Could not read the log of Job '{job_name}': {e.reason}")
        return ""
    return "".join(f":\n{tail}" for tail in tails if tail)

def wait_for_job_deletion(job_name, namespace, timeout=60):
    batch_v1 = get_k8s_batch_client()

    for _ in range(timeout):
        try:
            batch_v1.read_namespaced_job(job_name, namespace)
        except ApiException as e:
            if e.status == 404:
                return True
            else:
                raise

        time.sleep(2)

    raise TimeoutError(f"Timeout waiting for Job '{job_name}' to be deleted")
