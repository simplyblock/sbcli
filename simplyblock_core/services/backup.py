from datetime import datetime, timezone
import json
import os
import re
import subprocess
import shutil
import time
import requests
import uuid
import boto3
import argparse


from simplyblock_core import utils,db_controller


BACKUP_DEST=os.getenv('BACKUP_DEST')
BUCKET_NAME=os.getenv('BUCKET_NAME')

API_BASE_URL = os.getenv('CLUSTERIP')
AUTH_HEADER = {
    'Authorization': f'{os.getenv("CLUSTERID")} {os.getenv("CLUSTERSECRET")}'
}

BASE_MOUNT_PATH = "/mnt/backup"
EBS_MOUNT_PATH = "/mnt/backup-storage"






def lvol_add(lvol_info):

    url = f"{API_BASE_URL}/lvol/"
    payload = {
        "name": lvol_info.get("lvol_name"),
        "size": lvol_info.get("size"),
        "pool": lvol_info.get("lvol_pool"),        
        "crypto": lvol_info.get("lvol_crypto"),
        "max_rw_iops": lvol_info.get("lvol_max_rw_iops"),
        "max_rw_mbytes": lvol_info.get("lvol_max_rw_mbytes"),
        "max_r_mbytes": lvol_info.get("lvol_max_r_mbytes"),
        "max_w_mbytes": lvol_info.get("lvol_max_w_mbytes"),
        "max_size": lvol_info.get("lvol_max_size"),
        "prior_class": lvol_info.get("lvol_priority_class"),
        "pvc_name":lvol_info.get("lvol_pvc_name"),
        "distr_ndcs": "1",
        "distr_npcs": "1",
        "crypto_key1": lvol_info.get("lvol_crypto_key1"),
        "crypto_key2": lvol_info.get("lvol_crypto_key2"),
        "namespace": lvol_info.get("namespace"),
        "host_id": lvol_info.get("lvol_node_id"),
        "ha_type": lvol_info.get("lvol_ha_type")
    }
    lvol_name=lvol_info.get("lvol_name")
    print(f"printing the payload : {payload}")
    try:
        response = requests.post(url, json=payload, headers=AUTH_HEADER)
        response.raise_for_status()
        result = response.json()
        if not result.get("status", False):
            raise Exception(f"Failed to create lvol {lvol_name}: {result.get('error', 'Unknown error')}")
        lvol_uuid = result["results"]
        return lvol_uuid
    except requests.RequestException as e:
        raise Exception(f"Error creating lvol {lvol_name}: {e}")


def lvol_connect(lvol_uuid):

    url = f"{API_BASE_URL}/lvol/connect/{lvol_uuid}"
    try:
        response = requests.get(url, headers=AUTH_HEADER)
        response.raise_for_status()
        result = response.json()
        if not result.get("status", False):
            raise Exception(f"Failed to get connection command for lvol UUID {lvol_uuid}: {result.get('error', 'Unknown error')}")
        
        connect_command = result["results"][0]["connect"]
        print(f"Executing connection command: {connect_command}")
        
        
        match = re.search(r'--nqn=([^\s]+)', connect_command)
        if match:
            nqn_value = match.group(1)
            print(f"Extracted NQN: {nqn_value}")
        else:
            print("NQN not found in the command.")
        
        subprocess.run("sudo modprobe nvme-fabrics", shell=True, check=True)
        subprocess.run(connect_command, shell=True, check=True)
        
        return nqn_value
    
    except requests.RequestException as e:
        raise Exception(f"Error connecting lvol {lvol_uuid}: {e}")
    except subprocess.CalledProcessError as e:
        raise Exception(f"Failed to execute connection command for lvol UUID {lvol_uuid}: {e}")







def convert_pv_size(size):
    terabyte= 1e12
    gigabyte=1e9
    megabyte=1e6
    if size >= terabyte:
        size = size/1000000000000
        return str(size) + "T"
    elif size >= gigabyte:
        size = size/1000000000
        return str(size) + "G"
    else:
        size = size/1000000
        return str(size) + "M"
    
        




def list_lvols():
    """Fetches the list of lvols from the API."""
    response = requests.get(f"{API_BASE_URL}/lvol/", headers=AUTH_HEADER)
    response.raise_for_status()
    result = response.json()
    if result.get("status", False):
        return result["results"]
    else:
        raise Exception("Failed to retrieve lvols")

def create_snapshot(lvol_id, snapshot_name):
    """Creates a snapshot of a given lvol."""
    payload = {
        "lvol_id": lvol_id,
        "snapshot_name": snapshot_name
    }
    try:
        print(f"Creating snapshot with payload: {payload}")
        response = requests.post(f"{API_BASE_URL}/snapshot/", json=payload, headers=AUTH_HEADER)
        print(f"Response Status Code: {response.status_code}")
        print(f"Response Content: {response.text}")
        response.raise_for_status()
        result = response.json()
        if result.get("status", False):
            return result["results"]
        else:
            raise Exception(f"Snapshot creation failed for lvol {lvol_id}: {result.get('error', 'Unknown error')}")
    except requests.RequestException as e:
        raise Exception(f"Error during snapshot creation for lvol {lvol_id}: {e}")


def create_clone(snapshot_id, clone_name):
    payload = {
        "clone_name": clone_name,
        "snapshot_id": snapshot_id
    }
    try:
        response = requests.post(f"{API_BASE_URL}/snapshot/clone/", json=payload, headers=AUTH_HEADER)
        response.raise_for_status()
        result = response.json()
        if result.get("status", False):
            return result["results"]
        else:
            raise Exception(f"Clone creation failed for snapshot {snapshot_id}: {result.get('error', 'Unknown error')}")
    except requests.RequestException as e:
        raise Exception(f"Error during clone creation for snapshot {snapshot_id}: {e}")



def connect_lvol(lvol_id):
    response = requests.get(f"{API_BASE_URL}/lvol/connect/{lvol_id}", headers=AUTH_HEADER)
    response.raise_for_status()
    result = response.json()
    if result.get("status", False):
        connect_command = result["results"][0]["connect"]
        print(f"Connect command for lvol {lvol_id}: {connect_command}")
        return connect_command
    else:
        raise Exception(f"Failed to connect to lvol {lvol_id}: {result}")

def delete_snapshot(snapshot_id):
    try:
        response = requests.delete(f"{API_BASE_URL}/snapshot/{snapshot_id}", headers=AUTH_HEADER)
        response.raise_for_status()
        result = response.json()
        if not result.get("status", False):
            raise Exception(f"Failed to delete snapshot {snapshot_id}: {result.get('error', 'Unknown error')}")
        print(f"Snapshot {snapshot_id} deleted successfully.")
    except requests.RequestException as e:
        raise Exception(f"Error deleting snapshot {snapshot_id}: {e}")


def delete_clone(clone_id):
    try:
        response = requests.delete(f"{API_BASE_URL}/lvol/{clone_id}", headers=AUTH_HEADER)
        response.raise_for_status()
        result = response.json()
        if not result.get("status", False):
            raise Exception(f"Failed to delete clone {clone_id}: {result.get('error', 'Unknown error')}")
        print(f"Clone {clone_id} deleted successfully.")
    except requests.RequestException as e:
        raise Exception(f"Error deleting clone {clone_id}: {e}")


def parse_nvme_list_output(output, target_model):
    lines = output.splitlines()
    for line in lines:
        if target_model in line:
            return line.split()[0]
    raise Exception(f"Device with model {target_model} not found in nvme list")

def mount_device(device_name, lvol_id):

    mount_path = os.path.join(BASE_MOUNT_PATH, lvol_id)
    
    if os.path.exists(mount_path):
        print(f"Directory {mount_path} already exists. Removing it...")
        shutil.rmtree(mount_path)
    
    os.makedirs(mount_path, exist_ok=True)
    
    try:
        mount_command = f"sudo mount {device_name} {mount_path}"
        print(f"Executing mount command: {mount_command}")
        subprocess.run(mount_command, shell=True, check=True)
        print(f"Successfully mounted device {device_name} to {mount_path}")
    except subprocess.CalledProcessError as e:
        print(f"Failed to mount device {device_name} to {mount_path}")
        print(f"Command: {mount_command}")
        print(f"Error: {e}")
        print(f"Output: {e.output}")
        raise Exception(f"Mount operation failed for {device_name}") from e

    return mount_path



def unmount_device(mount_path):
    subprocess.run(f"sudo umount {mount_path}", shell=True, check=True)
    os.rmdir(mount_path)
    print(f"Unmounted and removed directory {mount_path}")




def backup_data(backup_uuid,source_path, lvol_info):
    lvol_name = lvol_info.get("pv_name", "unknown_lvol")
    print(f"Generated backup UUID: {backup_uuid} for lvol: {lvol_name}")

    s3 = boto3.client('s3')
    try:
        s3.head_bucket(Bucket=BUCKET_NAME)
        print(f"S3 bucket {BUCKET_NAME} is accessible.")
    except Exception as e:
        raise Exception(f"Cannot access S3 bucket {BUCKET_NAME}: {e}")

    s3_metadata_prefix = f"{backup_uuid}/{lvol_name}/metadata/"
    s3_data_prefix = f"{backup_uuid}/{lvol_name}/data/"

    json_object = json.dumps(lvol_info, indent=4)
    metadata_file_path = "metadata.json"
    with open(metadata_file_path, "w") as outfile:
        outfile.write(json_object)

    s3.upload_file(metadata_file_path, BUCKET_NAME, f"{s3_metadata_prefix}metadata.json")
    print(f"Uploaded metadata to s3://{BUCKET_NAME}/{s3_metadata_prefix}metadata.json")

    os.remove(metadata_file_path)

    errors = []
    for root, _, files in os.walk(source_path):
        if not files:
            print(f"Skipping empty directory: {root}")
            continue
        else:
            for file in files:
                local_file_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_file_path, source_path)
                s3_file_path = f"{s3_data_prefix}{relative_path}".replace("\\", "/")
                try:
                    s3.upload_file(local_file_path, BUCKET_NAME, s3_file_path)
                    print(f"Uploaded {local_file_path} to s3://{BUCKET_NAME}/{s3_file_path}")
                except Exception as e:
                    errors.append(f"Failed to upload {local_file_path}: {e}")
    if errors:
        print("\n".join(errors))
        raise Exception("Some files failed to upload.")

    print(f"Backup completed successfully for lvol {lvol_name} with UUID: {backup_uuid}")
    return backup_uuid


def get_lvol_info(lvol_id):
    response = requests.get(f"{API_BASE_URL}/lvol/{lvol_id}", headers=AUTH_HEADER)
    response.raise_for_status()
    result = response.json()
    if result.get("status", False):
        return result["results"]
    else:
        raise Exception("Failed to retrieve lvols info")
    
    
def backup_lvols():
    try:
        backup_uuid = os.getenv("CLUSTERID")
        
        print(f"Generated backup UUID: {backup_uuid}")
        
        lvols = list_lvols()
        
        for lvol in lvols:
            lvol_id = lvol.get('id')
            lvol_name = lvol.get('lvol_name', 'unknown_lvol') 
            lvol_pvcname = lvol.get('pvc_name')
            lvol_namespace = lvol.get('namespace')
            lvol_size = lvol.get("size")
            lvol_node_id = lvol.get("node_id")
            lvol_pool = lvol.get("pool_name")
            lvol_crypto_key1= lvol.get("crypto_key1")
            lvol_crypto_key2=lvol.get("crypto_key2")
            lvol_ha_type=lvol.get("ha_type")
            lvol_max_size=lvol.get("max_size")
            lvol_priority_class= lvol.get("lvol_priority_class")
            if lvol.get("crypto_key1") == "":
                lvol_crypto = False
            else:
                lvol_crypto= True

            
            info = get_lvol_info(lvol_id)
            lvol_max_rw_iops= info[0].get("rw_ios_per_sec")
            lvol_max_rw_mbytes=info[0].get("rw_mbytes_per_sec")
            lvol_max_r_mbytes=info[0].get("r_mbytes_per_sec")
            lvol_max_w_mbytes=info[0].get("w_mbytes_per_sec")
            
            
            
            size = convert_pv_size(lvol_size)

            
            
            print(f"Processing lvol {lvol_name} (ID: {lvol_id})")

            try:
                connect_command = connect_lvol(lvol_id)
                print(f"Executing connect command: {connect_command}")
                subprocess.run("sudo modprobe nvme-fabrics", shell=True, check=True)
                subprocess.run(connect_command, shell=True, check=False)

                result = subprocess.run("sudo nvme list", capture_output=True, text=True, shell=True, check=True)
                nvme_output = result.stdout
                device_name = parse_nvme_list_output(nvme_output, lvol_id)
                
                result = subprocess.run(f"sudo blkid {device_name}", capture_output=True, text=True, shell=True, check=True)
                output = result.stdout.strip()
    
    
                match = re.search(r'TYPE="([^"]+)"', output)
                if match:
                    fs_type = match.group(1)
                    print(f"Filesystem type: {fs_type}")
                else:
                    print("Filesystem type not found in output.")

                print(f"Device name for lvol {lvol_id}: {device_name}")

                mount_path = mount_device(device_name, lvol_id)
                
                lvol_info = {
                    "lvol_uuid": lvol_id,
                    "pv_name": lvol_name,
                    "pvc_name": lvol_pvcname,
                    "namespace": lvol_namespace,
                    "size": size,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "lvol_node_id": lvol_node_id,
                    "lvol_pool": lvol_pool,
                    "lvol_fs" : fs_type,
                    "lvol_crypto": lvol_crypto,
                    "lvol_crypto_key1": lvol_crypto_key1,
                    "lvol_crypto_key2":lvol_crypto_key2,
                    "lvol_ha_type": lvol_ha_type,
                    "lvol_max_size":lvol_max_size,
                    "lvol_priority_class":lvol_priority_class,
                    "lvol_max_rw_iops": lvol_max_rw_iops,
                    "lvol_max_rw_mbytes":lvol_max_rw_mbytes,
                    "lvol_max_r_mbytes": lvol_max_r_mbytes,
                    "lvol_max_w_mbytes":lvol_max_w_mbytes
                 }
                
                
                
                result.stdout
                time.sleep(10)

                backup_data(backup_uuid, mount_path, lvol_info)
                time.sleep(10)

                unmount_device(mount_path)

            except Exception as e:
                print(f"Failed during backup for lvol {lvol_id}: {e}")
                continue

        print(f"Backup process completed successfully for UUID: {backup_uuid}")

    except Exception as e:
        print(f"Backup process failed: {e}")
        
        
  
def restore(restore_uuid=None):
    print("Restore process started...")

    s3 = boto3.client('s3')

    if not restore_uuid:
        try:
            response = s3.list_objects_v2(Bucket=BUCKET_NAME, Delimiter='/')
            if 'CommonPrefixes' not in response:
                raise Exception(f"No UUID directories found in bucket {BUCKET_NAME}")
            uuids = [prefix['Prefix'].strip('/') for prefix in response['CommonPrefixes']]
            print(f"Available backup UUIDs: {uuids}")

            restore_uuid = input(f"Enter the UUID to restore from (or press Enter to use the first one: {uuids[0]}): ")
            if not restore_uuid:
                restore_uuid = uuids[0]
        except Exception as e:
            raise Exception(f"Failed to list UUIDs in bucket {BUCKET_NAME}: {e}")

    print(f"Selected UUID for restore: {restore_uuid}")

    try:
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=f"{restore_uuid}/", Delimiter='/')
        if 'CommonPrefixes' not in response:
            raise Exception(f"No lvol directories found in UUID {restore_uuid}")
        lvol_dirs = [prefix['Prefix'].split('/')[-2] for prefix in response['CommonPrefixes']]
        print(f"Found lvol directories: {lvol_dirs}")
    except Exception as e:
        raise Exception(f"Failed to list lvol directories in UUID {restore_uuid}: {e}")

    for lvol_name in lvol_dirs:
        print(f"Processing lvol: {lvol_name}")

        metadata_path = f"{restore_uuid}/{lvol_name}/metadata/metadata.json"
        local_metadata_file = f"metadata_{lvol_name}.json"
        try:
            s3.download_file(BUCKET_NAME, metadata_path, local_metadata_file)
            print(f"Downloaded metadata for {lvol_name}: {local_metadata_file}")

            with open(local_metadata_file, 'r') as f:
                metadata = json.load(f)

            lvol_size = metadata.get('size')
            if lvol_size.endswith(('T', 'G', 'M')):
                size_value = int(float(lvol_size[:-1]))
                size_unit = lvol_size[-1]
                lvol_size = f"{size_value}{size_unit}"
            else:
                raise ValueError(f"Invalid size format in metadata for {lvol_name}: {lvol_size}")

            pool_name = metadata.get('lvol_pool')
            if not pool_name:
                raise ValueError(f"Missing pool_name in metadata for lvol {lvol_name}")
            lvol_crypto_key1 = metadata.get("lvol_crypto_key1")
            lvol_crypto_key2 = metadata.get("lvol_crypto_key2")
            lvol_snode = metadata.get("node_id")
            lvol_namespace = metadata.get("namespace")

            lvol_name_copy = f"{lvol_name}_copy"
            lvol_ha_type = metadata.get("lvol_ha_type")
            
            lvol_max_size = metadata.get("lvol_max_size")
            lvol_priority_class =metadata.get("lvol_priority_class")
            lvol_pvc_name=metadata.get("pvc_name")
            lvol_crypto = metadata.get("lvol_crypto")
            
            

            
            lvol_info = {
                    "lvol_name": lvol_name_copy ,
                    "namespace": lvol_namespace,
                    "size": lvol_size,
                    "lvol_crypto": lvol_crypto,
                    "lvol_node_id": lvol_snode,
                    "lvol_pool": pool_name,
                    "lvol_crypto_key1": lvol_crypto_key1,
                    "lvol_crypto_key2":lvol_crypto_key2,
                    "lvol_ha_type": lvol_ha_type,
                    "lvol_max_size":lvol_max_size,
                    "lvol_priority_class":lvol_priority_class,
                    "lvol_pvc_name":lvol_pvc_name,
                    "pvc_name": metadata.get("pvc_name"),
                    "lvol_max_rw_iops": metadata.get("lvol_max_rw_iops"),
                    "lvol_max_rw_mbytes":metadata.get("lvol_max_rw_mbytes"),
                    "lvol_max_r_mbytes": metadata.get("lvol_max_r_mbytes"),
                    "lvol_max_w_mbytes":metadata.get("lvol_max_w_mbytes")
                 }
            
            

            lvol_uuid = lvol_add(lvol_info)

            nqn = lvol_connect(lvol_uuid)

            nvme_list_command = "sudo nvme list"
            print(f"Executing NVMe list command: {nvme_list_command}")
            result = subprocess.run(nvme_list_command, shell=True, check=True, capture_output=True, text=True)

            nvme_output = result.stdout
            device_name = parse_nvme_list_output(nvme_output, lvol_uuid)

            if not device_name:
                raise Exception(f"Failed to find device name for lvol {lvol_name_copy} in NVMe list")

            print(f"Device name for {lvol_name_copy}: {device_name}")
            fs = metadata.get('lvol_fs')
            mkfs_command = f"sudo mkfs.{fs} {device_name}"
            print(f"Formatting device {device_name} with command: {mkfs_command}")
            subprocess.run(mkfs_command, shell=True, check=True)

            mount_point = os.path.join(os.path.expanduser("~"), lvol_name_copy)
            os.makedirs(mount_point, exist_ok=True)
            mount_command = f"sudo mount {device_name} {mount_point}"
            print(f"Mounting device {device_name} to {mount_point} with command: {mount_command}")
            subprocess.run(mount_command, shell=True, check=True)

            data_prefix = f"{restore_uuid}/{lvol_name}/data/"
            local_data_dir = "local_data_temp"
            os.makedirs(local_data_dir, exist_ok=True)

            try:
                print(f"Downloading data directory from S3 prefix: {data_prefix}")
                response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=data_prefix)
                if 'Contents' not in response:
                    print(f"No data found for lvol {lvol_name_copy}")
                else:
                    for obj in response['Contents']:
                        s3_file_path = obj['Key']
                        relative_path = os.path.relpath(s3_file_path, data_prefix)
                        local_file_path = os.path.join(local_data_dir, relative_path)

                        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

                        s3.download_file(BUCKET_NAME, s3_file_path, local_file_path)
                        print(f"Downloaded {s3_file_path} to {local_file_path}")

                print(f"Copying data from {local_data_dir} to {mount_point}")
                shutil.copytree(local_data_dir, mount_point, dirs_exist_ok=True)
            finally:
                shutil.rmtree(local_data_dir, ignore_errors=True)

            unmount_command = f"sudo umount {mount_point}"
            print(f"Unmounting device with command: {unmount_command}")
            subprocess.run(unmount_command, shell=True, check=True)
            os.rmdir(mount_point)
            
            time.sleep(10)

            
            
            subprocess.run(f"sudo nvme disconnect -n {nqn}")

        except Exception as e:
            print(f"Failed to process lvol {lvol_name}: {e}")
            continue
        finally:
            if os.path.exists(local_metadata_file):
                os.remove(local_metadata_file)

    print(f"Restore process completed for UUID: {restore_uuid}")








    
    
logger = utils.get_logger(__name__)


db_controller = db_controller.DBController()

while True:
    currenttime = datetime.now(timezone.utc)
    schedule_datetime = datetime.strptime('03:00:00', '%H:%M:%S')
    
    # if currenttime != schedule_datetime:
    if False:
        print("backup runs at 03:00:00 UTC ")
        continue
    else:
        clusters = db_controller.get_clusters()
        for cl in clusters:
            logger.info(f"Checking cluster: {cl.get_id()}")
            cluster = db_controller.get_cluster_by_id(cl.get_id())
            print(cluster)
            if cluster.backup:
                parser = argparse.ArgumentParser(description="Backup or Restore lvols")
                parser.add_argument(
                    "--mode",
                    required=True,
                    choices=["backup", "restore"],
                    help="Specify the operation mode: backup or restore",
                )

                args = parser.parse_args()

                mode = args.mode

                if mode == "backup":
                    print("Starting backup process...")
                    # backup_lvols()
                elif mode == "restore":
                    print("skipping restore process...")
                    # restore()
                    # do nothing for restore
                else:
                    print(f"Invalid mode: {mode}")