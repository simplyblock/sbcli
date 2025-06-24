import uuid, time
from kubernetes import client, config
from simplyblock_core.models.managed_db import ManagedDatabase
from simplyblock_core.db_controller import DBController

def create_postgresql_deployment(name, storage_class, disk_size, version, vcpu_count, memory, namespace="default"):
    # Load Kubernetes config
    config.load_kube_config()

    pvc_name = f"{name}-pvc"
    # Define PersistentVolumeClaim
    pvc = client.V1PersistentVolumeClaim(
        metadata=client.V1ObjectMeta(name=pvc_name),
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
    # wait for the PVC to be created
    print(f"Waiting for PVC {pvc_name} to be created...")
    time.sleep(30)
    start_postgresql_deployment(name, version, vcpu_count, memory, pvc_name, namespace)

    db_controller = DBController()
    database = ManagedDatabase()
    database.uuid = str(uuid.uuid4())
    database.deployment_id = name
    database.namespace = namespace
    database.pvc_id = pvc_name
    database.type = "postgresql"
    database.version = version
    database.vcpu_count = vcpu_count
    database.memory_size = memory
    database.disk_size = disk_size
    database.storage_class = storage_class
    database.status = "running"
    database.write_to_db(db_controller.kv_store)

def get_nodes_with_label(label_selector):
    # Load kubeconfig
    config.load_kube_config()
    
    # Initialize the API client
    v1 = client.CoreV1Api()
    
    # Retrieve nodes with the specified label
    return v1.list_node(label_selector=label_selector)
    

def start_postgresql_deployment(deployment_name: str, version: str, vcpu_count: int, memory: str, pvc_name: str, namespace: str = "default"):
    # load Kubernetes config
    config.load_kube_config()

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

    # get all kubernetes nodes with label type=simplyblock-storage-plane
    k8snodes = get_nodes_with_label("type=simplyblock-storage-plane")
    lvols = DBController().get_lvols()
    lvol = next((lvol for lvol in lvols if lvol.pvc_name == pvc_name), None)
    if not lvol:
        raise ValueError(f"LVol with name {pvc_name} not found in the database.")
    
    nodes_ids = lvol.nodes

    primary_storage_node = DBController().get_storage_node_by_id(nodes_ids[0])
    secondary_storage_node = DBController().get_storage_node_by_id(nodes_ids[1])

    k8snode_primary = ""
    k8snode_secondary = ""

    for k8snode in k8snodes.items:
        node_ips = [addr.address for addr in k8snode.status.addresses if addr.type == "InternalIP"]
        if not node_ips:
            continue

        if primary_storage_node.mgmt_ip in node_ips:
            k8snode_primary = k8snode.metadata.name
        elif secondary_storage_node.mgmt_ip in node_ips:
            k8snode_secondary = k8snode.metadata.name

    pod_affinity = client.V1Affinity(
        node_affinity=client.V1NodeAffinity(
            preferred_during_scheduling_ignored_during_execution=[
                client.V1PreferredSchedulingTerm(
                    weight=100,
                    preference=client.V1NodeSelectorTerm(
                        match_expressions=[
                            client.V1NodeSelectorRequirement(
                                key="kubernetes.io/hostname",
                                operator="In",
                                values=[k8snode_primary]
                            )
                        ]
                    )
                ),
                client.V1PreferredSchedulingTerm(
                    weight=50,
                    preference=client.V1NodeSelectorTerm(
                        match_expressions=[
                            client.V1NodeSelectorRequirement(
                                key="kubernetes.io/hostname",
                                operator="In",
                                values=[k8snode_secondary]
                            )
                        ]
                    )
                ),
            ],
        )
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
                affinity=pod_affinity,
                volumes=[client.V1Volume(
                    name="postgres-data",
                    persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                        claim_name=pvc_name
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
    snapshot_class_name = "simplyblock-csi-snapshotclass"

    snapshot_body = {
        "apiVersion": f"{group}/{version}",
        "kind": "VolumeSnapshot",
        "metadata": {
            "name": snapshot_name
        },
        "spec": {
            "volumeSnapshotClassName": snapshot_class_name,
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

    # get SNAPSHOTCONTENT of the above created snapshot
    time.sleep(5)
    try:
        snapshot_content = snapshot_api.get_namespaced_custom_object(
            group=group,
            version=version,
            namespace=namespace,
            plural="volumesnapshots",
            name=snapshot_name
        )
        print(f"Snapshot content: {snapshot_content}")
        if 'status' in snapshot_content and 'readyToUse' in snapshot_content['status']:
            snapshot_content = snapshot_content['status']['boundVolumeSnapshotContentName']
            if snapshot_content:
                return snapshot_content
            else:
                print("Snapshot is not ready yet.")
    except client.exceptions.ApiException as e:
        print(f"Error retrieving snapshot content: {e}")

def create_pvc_clone(clone_name, source_pvc_name, storage_class, disk_size, namespace="default"):
    # Load Kubernetes config
    config.load_kube_config()

    # Define the clone PersistentVolumeClaim
    clone_pvc = client.V1PersistentVolumeClaim(
        metadata=client.V1ObjectMeta(name=clone_name),
        spec=client.V1PersistentVolumeClaimSpec(
            access_modes=["ReadWriteOnce"],
            resources=client.V1ResourceRequirements(
                requests={"storage": disk_size}
            ),
            storage_class_name=storage_class,
            data_source=client.V1TypedLocalObjectReference(
                api_group="",
                kind="PersistentVolumeClaim",
                name=source_pvc_name
            )
        )
    )

    # Create the clone PersistentVolumeClaim
    v1 = client.CoreV1Api()
    try:
        v1.create_namespaced_persistent_volume_claim(namespace=namespace, body=clone_pvc)
        print("PVC clone created successfully.")
    except client.exceptions.ApiException as e:
        print(f"Error creating PVC clone: {e}")
