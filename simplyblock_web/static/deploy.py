import time

import yaml
from kubernetes import client, config
from kubernetes.client import ApiException

namespace='default'
deployment_name='python-interact-deployment'
pod_name = 'python-interact-deployment'

config.load_kube_config()

v1 = client.AppsV1Api()
core_v1 = client.CoreV1Api()
try:

    with open('deploy_cnode.yaml', 'r') as f:
        dep = yaml.safe_load(f)

    resp = v1.create_namespaced_deployment(body=dep, namespace=namespace)
    print(f"Deployment created: '{resp.metadata.name}'")
    # print(resp.status)
    retries = 10
    while retries > 0:
        resp = core_v1.list_namespaced_pod(namespace)
        new_pod = None
        for pod in resp.items:
            if pod.metadata.name.startswith(pod_name):
                new_pod = pod
                break

        if not new_pod:
            print("Container is not running, waiting...")
            time.sleep(3)
            retries -= 1
            continue

        status = new_pod.status.phase
        print(f"pod: {pod_name} status: {status}")

        if status == "Running":
            print(f"Container status: {status}")
            break
        else:
            print("Container is not running, waiting...")
            time.sleep(3)
            retries -= 1

except ApiException as e:
    print(e.body)
