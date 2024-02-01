from kubernetes import client, config
from kubernetes.client import ApiException

namespace='default'
deployment_name='dep_name'
pod_name = 'python-interact-deployment'
config.load_kube_config()

v1 = client.AppsV1Api()
try:
    resp = v1.list_namespaced_deployment(namespace)
    for dep in resp.items:
        print(f"{dep.metadata.name}")
        print(f"{dep.status}")
except ApiException as e:
    print(e.body)
