import uuid
from kubernetes import client, config
from simplyblock_core.models.managed_db import ManagedDatabase
from simplyblock_core.db_controller import DBController

def create_postgresql_deployment(name, storage_class, disk_size, version, vcpu_count, memory, namespace="default"):
    # Load Kubernetes config
    config.load_kube_config()

    # Define PersistentVolumeClaim
    pvc = client.V1PersistentVolumeClaim(
        metadata=client.V1ObjectMeta(name=f"{name}-pvc"),
        spec=client.V1PersistentVolumeClaimSpec(
            access_modes=["ReadWriteOnce"],
            resources=client.V1ResourceRequirements(
                requests={"storage": disk_size}
            ),
            storage_class_name=storage_class
        )
    )

    # Create the PersistentVolumeClaim
    v1 = client.CoreV1Api()
    v1.create_namespaced_persistent_volume_claim(namespace=namespace, body=pvc)
    start_postgresql_deployment(name, version, vcpu_count, memory, namespace)

    db_controller = DBController()
    database = ManagedDatabase()
    database.uuid = str(uuid.uuid4())
    database.deployment_id = name
    database.namespace = namespace
    database.pvc_id = f"{name}-pvc"
    database.type = "postgresql"
    database.version = version
    database.vcpu_count = vcpu_count
    database.memory_size = memory
    database.disk_size = disk_size
    database.storage_class = storage_class
    database.status = "running"
    database.write_to_db(db_controller.kv_store)

def start_postgresql_deployment(deployment_name: str, version: str, vcpu_count: int, memory: str, namespace: str = "default"):
    resource_requests = {
        "cpu": str(vcpu_count),
        "memory": memory
    }
    resource_limits = {
        "cpu": str(vcpu_count * 4),
        "memory": str(int(memory.rstrip('G')) * 2) + "G"
    }

    container = client.V1Container(
        name="postgres",
        image=f"postgres:{version}",
        ports=[client.V1ContainerPort(container_port=5432)],
        env=[
            client.V1EnvVar(name="POSTGRES_USER", value="admin"),
            client.V1EnvVar(name="POSTGRES_PASSWORD", value="password"),
            client.V1EnvVar(name="PGDATA", value="/var/lib/postgresql/data/pgdata"),
        ],
        resources=client.V1ResourceRequirements(
            requests=resource_requests,
            limits=resource_limits
        ),
        volume_mounts=[client.V1VolumeMount(
            mount_path="/var/lib/postgresql/data",
            name="postgres-data"
        )]
    )

    deployment_spec = client.V1DeploymentSpec(
        replicas=1,
        selector=client.V1LabelSelector(
            match_labels={"app": deployment_name}
        ),
        template=client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(labels={"app": deployment_name}),
            spec=client.V1PodSpec(
                containers=[container],
                volumes=[client.V1Volume(
                    name="postgres-data",
                    persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                        claim_name=f"{deployment_name}-pvc"
                    )
                )]
            )
        )
    )

    deployment = client.V1Deployment(
        api_version="apps/v1",
        kind="Deployment",
        metadata=client.V1ObjectMeta(name=deployment_name),
        spec=deployment_spec
    )

    # Create the Deployment
    apps_v1 = client.AppsV1Api()
    apps_v1.create_namespaced_deployment(namespace=namespace, body=deployment)



def stop_postgresql_deployment(deployment_name: str, namespace: str = "default"):
    # Load Kubernetes config
    config.load_kube_config()

    # Delete the Deployment
    apps_v1 = client.AppsV1Api()
    try:
        apps_v1.delete_namespaced_deployment(deployment_name, namespace=namespace)
        print("Deployment stopped.")
    except client.exceptions.ApiException as e:
        print(f"Error stopping deployment: {e}")

def delete_postgresql_resources(deployment_name: str, pvc_name: str, namespace: str = "default"):
    # Load Kubernetes config
    config.load_kube_config()

    # Delete the Deployment
    apps_v1 = client.AppsV1Api()
    core_v1 = client.CoreV1Api()
    try:
        apps_v1.delete_namespaced_deployment(deployment_name, namespace=namespace)
        print("Deployment deleted.")
    except client.exceptions.ApiException as e:
        print(f"Error deleting deployment: {e}")

    # Delete the PersistentVolumeClaim
    try:
        core_v1.delete_namespaced_persistent_volume_claim(name=pvc_name, namespace=namespace)
        print("PersistentVolumeClaim deleted.")
    except client.exceptions.ApiException as e:
        print(f"Error deleting PVC: {e}")

def create_pvc_snapshot(snapshot_name: str, pvc_name: str, namespace: str = "default"):
    # Load Kubernetes config
    config.load_kube_config()

    # Create a snapshot for the PVC
    snapshot_api = client.CustomObjectsApi()
    group = "snapshot.storage.k8s.io"
    version = "v1"
    plural = "volumesnapshots"

    snapshot_body = {
        "apiVersion": "snapshot.storage.k8s.io/v1",
        "kind": "VolumeSnapshot",
        "metadata": {
            "name": snapshot_name
        },
        "spec": {
            "source": {
                "persistentVolumeClaimName": pvc_name
            }
        }
    }

    try:
        snapshot_api.create_namespaced_custom_object(
            group=group,
            version=version,
            namespace=namespace,
            plural=plural,
            body=snapshot_body
        )
        print("Snapshot created successfully.")
    except client.exceptions.ApiException as e:
        print(f"Error creating snapshot: {e}")

    # save this snapshot in DB
