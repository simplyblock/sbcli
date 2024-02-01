
from kubernetes import client, config


namespace='default'
deployment_name='dep_name'
pod_name = 'python-interact-deployment'
config.load_kube_config()
v1 = client.CoreV1Api()
resp = v1.list_namespaced_pod(namespace)
for pod in resp.items:
    if pod.metadata.name.startswith(pod_name):
        status = pod.status.phase
        print(f"pod: {pod_name} status: {status}")
        exit(0)
print(f"pod not found: {pod_name}")
exit(1)
