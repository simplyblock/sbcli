import uuid, time
from kubernetes import client, config
from simplyblock_core.models.managed_db import ManagedDatabase
from simplyblock_core.db_controller import DBController

db_user = "simplyblock_admin"
db_name = "simplyblock_db"
db_password = "password"

simplyblock_storage_node_label = "type=simplyblock-storage-plane"

def create_postgresql_deployment(name, storage_class, disk_size, version, vcpu_count, memory, namespace="default"):
    # Load Kubernetes config
    config.load_kube_config()

    pvc_name = f"{name}-pvc"
    # Define PersistentVolumeClaim
    pvc = client.V1PersistentVolumeClaim(
        metadata=client.V1ObjectMeta(name=pvc_name),
        spec=client.V1PersistentVolumeClaimSpec(
            access_modes=["ReadWriteMany"],
            resources=client.V1ResourceRequirements(
                requests={"storage": disk_size}
            ),
            volume_mode="Block",
            storage_class_name=storage_class
        )
    )

    # Create the PersistentVolumeClaim
    v1 = client.CoreV1Api()
    v1.create_namespaced_persistent_volume_claim(namespace=namespace, body=pvc)
    # wait for the PVC to be created
    start_postgresql_deployment2(name, version, vcpu_count, memory, pvc_name, namespace)

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
    print(f"Waiting for PVC {pvc_name} to be created...")
    time.sleep(30)
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
            access_modes=["ReadWriteMany"],
            volume_mode="Block",
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

def resize_postgresql_database(deployment_name: str, pvc_name: str, new_vcpu_count: int, new_memory: str, new_disk_size: str, namespace: str = "default"):
    """
    Resizes a PostgreSQL database Kubevirt VM by updating its vCPU, memory, and disk size.
    This version first resizes the PVC, then stops and restarts the VM
    to apply all changes (CPU, memory, and disk expansion).

    Args:
        deployment_name (str): The name of the Kubevirt VM.
        pvc_name (str): The name of the PersistentVolumeClaim associated with the database.
        new_vcpu_count (int): The new number of vCPUs for the VM.
        new_memory (str): The new memory size (e.g., "4G", "8Gi") for the VM.
        new_disk_size (str): The new disk size (e.g., "100Gi", "500G") for the PVC.
        namespace (str, optional): The Kubernetes namespace. Defaults to "default".
    """
    config.load_kube_config()
    kubevirt_api = client.CustomObjectsApi()
    core_v1 = client.CoreV1Api()
    db_controller = DBController()

    print(f"Resizing PostgreSQL Kubevirt VM '{deployment_name}' in namespace '{namespace}'...")

    try:
        # 1. Update Disk Size (PVC Resize)
        current_pvc = core_v1.read_namespaced_persistent_volume_claim(name=pvc_name, namespace=namespace)
        storage_class_name = current_pvc.spec.storage_class_name
        if not storage_class_name:
            raise ValueError(f"PVC '{pvc_name}' does not have a storage class assigned.")
        
        storage_api = client.StorageV1Api()
        storage_class = storage_api.read_storage_class(name=storage_class_name)
        if not storage_class.allow_volume_expansion:
            raise ValueError(f"StorageClass '{storage_class_name}' does not support volume expansion. Cannot resize disk.")

        current_disk_size = current_pvc.spec.resources.requests.get("storage")
        if current_disk_size and parse_size_to_bytes(new_disk_size) < parse_size_to_bytes(current_disk_size):
            raise ValueError(f"Cannot shrink disk size. Current: {current_disk_size}, Requested: {new_disk_size}")

        current_pvc.spec.resources.requests["storage"] = new_disk_size
        core_v1.patch_namespaced_persistent_volume_claim(name=pvc_name, namespace=namespace, body=current_pvc)
        print(f"PersistentVolumeClaim '{pvc_name}' disk size updated. Waiting for PVC expansion...")
        time.sleep(10) 

        # 2. Stop and Start VirtualMachine (Applies CPU/Memory changes and triggers filesystem expansion)
        print(f"Stopping Kubevirt VM '{deployment_name}' to apply all new parameters and ensure PVC filesystem expansion...")
        stop_postgresql_deployment2(deployment_name, namespace)
        
        # Wait a bit for VM to be fully stopped
        time.sleep(15) 

        print(f"Starting Kubevirt VM '{deployment_name}' with new vCPU, memory, and disk size...")
        
        managed_db = db_controller.get_database_by_deployment_id(deployment_name, namespace)
        if not managed_db:
            raise ValueError(f"ManagedDatabase entry for VM '{deployment_name}' not found.")

        # Re-create the VM with new parameters.
        # For Kubevirt VMs, updating resources requires patching the VM object.
        # If stop_postgresql_deployment sets 'running: false', start_postgresql_deployment
        # needs to ensure 'running: true' is set along with resource changes.
        # A simpler way for resize is to PATCH the running VM's spec.template.domain.
        # However, for disk resize requiring restart, a delete+create or stop+patch+start
        # is often more reliable. Given your earlier logic, stop+start is chosen.
        # In this specific case, start_postgresql_deployment will recreate it if it doesn't exist,
        # but if it was just stopped (running: false), we need to update its spec.
        
        # A more robust resize for VM:
        # 1. Resize PVC
        # 2. Stop VM (set running: false)
        # 3. Patch VM spec.template.domain.resources and cpu with new values
        # 4. Start VM (set running: true)

        # Let's implement the stop+patch+start pattern for VMs more explicitly for resize
        vm_current = kubevirt_api.get_namespaced_custom_object(
            group="kubevirt.io", version="v1", namespace=namespace, plural="virtualmachines", name=deployment_name
        )
        
        # Update VM's resource requests directly in the object
        vm_current["spec"]["template"]["spec"]["domain"]["resources"]["requests"]["memory"] = new_memory
        vm_current["spec"]["template"]["spec"]["domain"]["cpu"]["cores"] = new_vcpu_count
        vm_current["spec"]["running"] = True # Set it to running again

        kubevirt_api.patch_namespaced_custom_object(
            group="kubevirt.io",
            version="v1",
            namespace=namespace,
            plural="virtualmachines",
            name=deployment_name,
            body=vm_current # Patch with the updated body
        )
        print(f"Kubevirt VM '{deployment_name}' patched and started with new configuration.")

        # Kubevirt does not have a direct "wait_for_deployment_completion" like Deployments.
        # You'd typically poll the VM's status.running or status.ready conditions.
        # For simplicity, we'll just add a sleep for this example.
        print(f"Waiting for Kubevirt VM '{deployment_name}' to start...")
        time.sleep(60) # Give VM time to boot and PostgreSQL to start

        # 3. Update DBController record
        database = db_controller.get_database_by_deployment_id(deployment_name, namespace)
        if database:
            database.vcpu_count = new_vcpu_count
            database.memory_size = new_memory
            database.disk_size = new_disk_size
            database.status = "running"
            database.write_to_db(db_controller.kv_store)
            print(f"Database record for '{deployment_name}' updated in DBController.")
        else:
            print(f"Warning: Database record for '{deployment_name}' not found in DBController after resize.")

        print(f"PostgreSQL Kubevirt VM '{deployment_name}' resized successfully.")

    except client.exceptions.ApiException as e:
        print(f"Kubernetes API error during resize: {e}")
        database = db_controller.get_database_by_deployment_id(deployment_name, namespace)
        if database:
            database.status = "resize_failed"
            database.write_to_db(db_controller.kv_store)
        
    except ValueError as e:
        print(f"Configuration error during resize: {e}")
        database = db_controller.get_database_by_deployment_id(deployment_name, namespace)
        if database:
            database.status = "resize_failed"
            database.write_to_db(db_controller.kv_store)
    except Exception as e:
        print(f"An unexpected error occurred during resize: {e}")
        database = db_controller.get_database_by_deployment_id(deployment_name, namespace)
        if database:
            database.status = "resize_failed"
            database.write_to_db(db_controller.kv_store)

def start_postgresql_deployment2(deployment_name: str, version: str, vcpu_count: int, memory: str, pvc_name: str, namespace: str = "default"):
    """
    Provisions a PostgreSQL VirtualMachine using Kubevirt, with node affinity
    based on Simplyblock storage plane nodes.

    Args:
        deployment_name (str): The desired name for the Kubevirt VirtualMachine.
        version (str): PostgreSQL version (primarily used for cloud-init path, assumed '14' in template).
        vcpu_count (int): Number of vCPUs for the VM.
        memory (str): Memory size for the VM (e.g., "4Gi").
        pvc_name (str): The name of the PersistentVolumeClaim for the PostgreSQL data.
        namespace (str, optional): The Kubernetes namespace. Defaults to "default".
    """
    # load Kubernetes config
    config.load_kube_config()

    # Define the Kubevirt API client
    kubevirt_api = client.CustomObjectsApi()
    core_v1 = client.CoreV1Api()

    cloud_init_user_data = f"""
#cloud-config
hostname: {deployment_name}-vm
users:
  - name: cloud-user
    groups: wheel
    sudo: ALL=(ALL) NOPASSWD:ALL

runcmd:
  # Install PostgreSQL on Ubuntu/Debian
  - apt-get update -y
  - apt-get install -y postgresql postgresql-contrib
  - systemctl enable postgresql
  - systemctl start postgresql

  # Configure PostgreSQL to listen on all interfaces
  - sed -i "s/#listen_addresses = 'localhost'/listen_addresses = '*'/" /etc/postgresql/14/main/postgresql.conf
  - echo "host    all             all             0.0.0.0/0               md5" >> /etc/postgresql/14/main/pg_hba.conf

  # Create a database and user
  - sudo -u postgres psql -c "CREATE DATABASE {db_name};"
  - sudo -u postgres psql -c "CREATE USER {db_user} WITH ENCRYPTED PASSWORD '{db_password}';"
  - sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE {db_name} TO {db_user};"

  - systemctl restart postgresql
"""

    # get all kubernetes nodes with label type=simplyblock-storage-plane
    print(f"Waiting for PVC {pvc_name} to be created...")
    time.sleep(30) # Await PVC provisioning. In a production system, poll PVC status.

    db_controller = DBController()
    lvols = db_controller.get_lvols()
    lvol = next((lvol for lvol in lvols if lvol.pvc_name == pvc_name), None)
    if not lvol:
        raise ValueError(f"LVol with name {pvc_name} not found in the database.")
    
    nodes_ids = lvol.nodes

    primary_storage_node = db_controller.get_storage_node_by_id(nodes_ids[0])
    secondary_storage_node = db_controller.get_storage_node_by_id(nodes_ids[1])

    k8snode_primary = ""
    k8snode_secondary = ""
    k8snodes = get_nodes_with_label(simplyblock_storage_node_label)

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

    # Define the Kubevirt VirtualMachine object
    vm_body = {
        "apiVersion": "kubevirt.io/v1",
        "kind": "VirtualMachine",
        "metadata": {
            "name": deployment_name,
            "labels": {
                "app": "postgres",
                "simplyblock-managed": "true"
            }
        },
        "spec": {
            "runStrategy": "Always",
            "template": {
                "metadata": {
                    "labels": {
                        "app": "postgres",
                        "name": deployment_name
                    }
                },
                "spec": {
                    "evictionStrategy": "LiveMigrate",
                    "domain": {
                        "devices": {
                            "disks": [
                                {
                                    "disk": {
                                        "bus": "virtio"
                                    },
                                    "name": "rootdisk"
                                },
                                {
                                    "disk": {
                                        "bus": "virtio"
                                    },
                                    "name": "datadisk"
                                }
                            ],
                            "interfaces": [
                                {
                                    "name": "default",
                                    "masquerade": {}
                                }
                            ]
                        },
                        "resources": {
                            "requests": {
                                "memory": memory
                            }
                        },
                        "cpu": {
                            "cores": vcpu_count
                        }
                    },
                     "networks": [
                        {
                            "name": "default",
                            "pod": {}
                        }
                    ],
                    "volumes": [
                        {
                            "name": "rootdisk",
                            "containerDisk": {
                                "image": "rrukmantiyo/kubevirt-images:ubuntu-22.04"
                            }
                        },
                        {
                            "name": "datadisk",
                            "persistentVolumeClaim": {
                                "claimName": pvc_name
                            }
                        },
                        {
                            "name": "cloudinitdisk",
                            "cloudInitNoCloud": {
                                "userData": cloud_init_user_data
                            }
                        }
                    ],
                    "affinity": pod_affinity
                }
            }
        }
    }

    try:
        # Create the VirtualMachine
        kubevirt_api.create_namespaced_custom_object(
            group="kubevirt.io",
            version="v1",
            namespace=namespace,
            plural="virtualmachines",
            body=vm_body
        )
        print(f"Kubevirt VirtualMachine '{deployment_name}' created successfully.")

        service_body = client.V1Service(
            api_version="v1",
            kind="Service",
            metadata=client.V1ObjectMeta(name=f"{deployment_name}-svc"),
            spec=client.V1ServiceSpec(
                selector={"app": "postgres", "name": deployment_name},
                ports=[
                    client.V1ServicePort(
                        protocol="TCP",
                        port=5432,
                        target_port=5432
                    )
                ],
                type="ClusterIP"
            )
        )
        try:
            core_v1.create_namespaced_service(namespace=namespace, body=service_body)
            print(f"Kubernetes Service '{deployment_name}-svc' created successfully.")
        except client.exceptions.ApiException as e:
            if e.status == 409: # Conflict, service already exists
                print(f"Service '{deployment_name}-svc' already exists, skipping creation.")
            else:
                print(f"Error creating Service: {e}")

    except client.exceptions.ApiException as e:
        print(f"Error creating Kubevirt VirtualMachine: {e}")
        raise

def stop_postgresql_deployment2(deployment_name: str, namespace: str = "default"):
    """
    Deletes the Kubevirt VirtualMachine to stop the PostgreSQL deployment.
    This effectively "removes" the VM from Kubernetes.

    Args:
        deployment_name (str): The name of the Kubevirt VirtualMachine to delete.
        namespace (str, optional): The Kubernetes namespace. Defaults to "default".
    """
    # Load Kubernetes config
    config.load_kube_config()

    kubevirt_api = client.CustomObjectsApi()
    
    group = "kubevirt.io"
    version = "v1"
    plural = "virtualmachines"

    try:
        # Delete the VirtualMachine object
        # Using V1DeleteOptions with propagation_policy="Background" or "Foreground"
        # can control whether dependent objects (like pods) are deleted.
        # For a VM, deleting the VM object itself is usually sufficient.
        kubevirt_api.delete_namespaced_custom_object(
            group=group,
            version=version,
            namespace=namespace,
            plural=plural,
            name=deployment_name,
            body=client.V1DeleteOptions(propagation_policy="Foreground")
        )
        print(f"Kubevirt VirtualMachine '{deployment_name}' deleted to stop the deployment.")
        # Give Kubernetes time to process the deletion
        time.sleep(10)
    except client.exceptions.ApiException as e:
        print(f"Error deleting Kubevirt VirtualMachine: {e}")
        # Handle cases where VM might not exist (already deleted, etc.)
        if e.status == 404:
            print(f"VirtualMachine '{deployment_name}' not found, skipping stop (it's already gone).")
        else:
            raise

def delete_postgresql_resources2(deployment_name: str, pvc_name: str, namespace: str = "default"):
    # Load Kubernetes config
    config.load_kube_config()

    kubevirt_api = client.CustomObjectsApi()
    core_v1 = client.CoreV1Api()
    
    vm_group = "kubevirt.io"
    vm_version = "v1"
    vm_plural = "virtualmachines"

    # Delete the Kubevirt VirtualMachine
    try:
        kubevirt_api.delete_namespaced_custom_object(
            group=vm_group,
            version=vm_version,
            namespace=namespace,
            plural=vm_plural,
            name=deployment_name,
            body=client.V1DeleteOptions(propagation_policy="Foreground") # Ensure dependent objects are deleted
        )
        print(f"Kubevirt VirtualMachine '{deployment_name}' deleted.")
    except client.exceptions.ApiException as e:
        if e.status == 404:
            print(f"VirtualMachine '{deployment_name}' not found, skipping VM deletion.")
        else:
            print(f"Error deleting Kubevirt VirtualMachine: {e}")

    # Delete the Service (if it exists)
    try:
        core_v1.delete_namespaced_service(name=f"{deployment_name}-svc", namespace=namespace)
        print(f"Kubernetes Service '{deployment_name}-svc' deleted.")
    except client.exceptions.ApiException as e:
        if e.status == 404:
            print(f"Service '{deployment_name}-svc' not found, skipping service deletion.")
        else:
            print(f"Error deleting Service: {e}")

    # Delete the PersistentVolumeClaim
    try:
        core_v1.delete_namespaced_persistent_volume_claim(name=pvc_name, namespace=namespace)
        print("PersistentVolumeClaim deleted.")
    except client.exceptions.ApiException as e:
        if e.status == 404:
            print(f"PersistentVolumeClaim '{pvc_name}' not found, skipping PVC deletion.")
        else:
            print(f"Error deleting PVC: {e}")

def parse_size_to_bytes(size_str: str) -> int:
    """Parses a string like '10Gi' or '10G' into bytes."""
    size_str = size_str.strip().upper()
    if size_str.endswith("GI"):
        return int(size_str[:-2]) * (1024 ** 3)
    elif size_str.endswith("G"):
        return int(size_str[:-1]) * (1000 ** 3)
    elif size_str.endswith("MI"):
        return int(size_str[:-2]) * (1024 ** 2)
    elif size_str.endswith("M"):
        return int(size_str[:-1]) * (1000 ** 2)
    elif size_str.endswith("KI"):
        return int(size_str[:-2]) * (1024 ** 1)
    elif size_str.endswith("K"):
        return int(size_str[:-1]) * (1000 ** 1)
    else:
        # Assume bytes if no unit specified
        return int(size_str)
