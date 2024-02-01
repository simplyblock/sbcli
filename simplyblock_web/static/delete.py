import time

from kubernetes import client, config
from kubernetes.client import ApiException

namespace='default'
deployment_name='python-interact-deployment'
pod_name = 'python-interact-deployment'

config.load_kube_config()

v1 = client.AppsV1Api()
core_v1 = client.CoreV1Api()

try:
    resp = v1.delete_namespaced_deployment(deployment_name, namespace)
    print(resp.status)

    retries = 20
    while retries > 0:
        resp = core_v1.list_namespaced_pod(namespace)
        found = False
        for pod in resp.items:
            if pod.metadata.name.startswith(pod_name):
                found = True

        if found:
            print("Container found, waiting...")
            retries -= 1
            time.sleep(3)
        else:
            break

    print("Done")
except ApiException as e:
    print(e.body)
