from kubernetes import client, config
from simplyblock_core.models.managed_db import ManagedDatabase

def create_postgresql_deployment(name, storage_class, disk_size, postgres_version, vcpu_count, memory):
    # Load Kubernetes config
    config.load_kube_config()

    # Define resource requests and limits
    resource_requests = {
        "cpu": str(vcpu_count),
        "memory": memory
    }
    resource_limits = {
        "cpu": str(vcpu_count * 4),
        "memory": str(int(memory.rstrip('Mi')) * 2) + "Mi"
    }

    # Define PersistentVolumeClaim
    pvc = client.V1PersistentVolumeClaim(
        metadata=client.V1ObjectMeta(name="postgres-pvc"),
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
    v1.create_namespaced_persistent_volume_claim(namespace="default", body=pvc)

    # Define PostgreSQL Deployment
    container = client.V1Container(
        name="postgres",
        image=f"postgres:{postgres_version}",
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
            match_labels={"app": "postgres"}
        ),
        template=client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(labels={"app": "postgres"}),
            spec=client.V1PodSpec(
                containers=[container],
                volumes=[client.V1Volume(
                    name="postgres-data",
                    persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                        claim_name="postgres-pvc"
                    )
                )]
            )
        )
    )

    deployment = client.V1Deployment(
        api_version="apps/v1",
        kind="Deployment",
        metadata=client.V1ObjectMeta(name="postgres"),
        spec=deployment_spec
    )

    # Create the Deployment
    apps_v1 = client.AppsV1Api()
    apps_v1.create_namespaced_deployment(namespace="default", body=deployment)

    db = ManagedDatabase()
    db.name = name
    db.deployment_id = name
    db.pvc_id = f"{name}-pvc"
    db.type = "postgresql"
    db.version = postgres_version
    db.vcpu_count = vcpu_count
    db.memory_size = int(memory.rstrip('Mi'))
    db.disk_size = int(disk_size.rstrip('Gi'))
    db.storage_class = storage_class
    db.status = "created"
    db.write_to_db()


def stop_postgresql_deployment():
    # Load Kubernetes config
    config.load_kube_config()

    # Delete the Deployment
    apps_v1 = client.AppsV1Api()
    try:
        apps_v1.delete_namespaced_deployment(name="postgres", namespace="default")
        print("Deployment stopped.")
    except client.exceptions.ApiException as e:
        print(f"Error stopping deployment: {e}")

def delete_postgresql_resources():
    # Load Kubernetes config
    config.load_kube_config()

    # Delete the Deployment
    apps_v1 = client.AppsV1Api()
    core_v1 = client.CoreV1Api()
    try:
        apps_v1.delete_namespaced_deployment(name="postgres", namespace="default")
        print("Deployment deleted.")
    except client.exceptions.ApiException as e:
        print(f"Error deleting deployment: {e}")

    # Delete the PersistentVolumeClaim
    try:
        core_v1.delete_namespaced_persistent_volume_claim(name="postgres-pvc", namespace="default")
        print("PersistentVolumeClaim deleted.")
    except client.exceptions.ApiException as e:
        print(f"Error deleting PVC: {e}")

def create_pvc_snapshot(snapshot_name):
    # Load Kubernetes config
    config.load_kube_config()

    # Create a snapshot for the PVC
    snapshot_api = client.CustomObjectsApi()
    group = "snapshot.storage.k8s.io"
    version = "v1"
    namespace = "default"
    plural = "volumesnapshots"

    snapshot_body = {
        "apiVersion": "snapshot.storage.k8s.io/v1",
        "kind": "VolumeSnapshot",
        "metadata": {
            "name": snapshot_name
        },
        "spec": {
            "source": {
                "persistentVolumeClaimName": "postgres-pvc"
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

# if __name__ == "__main__":
#     # Example configuration
#     storage_class = "simplyblock-csi-sc"
#     disk_size = "10Gi"
#     postgres_version = "14"
#     vcpu_count = 1
#     memory = "512Mi"

#     create_postgresql_deployment(storage_class, disk_size, postgres_version, vcpus, memory)
