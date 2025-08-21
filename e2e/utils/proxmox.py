import os
import time
import re
import requests
import urllib3
from logger_config import setup_logger
from typing import Tuple
from ping3 import ping

logging = setup_logger(__name__)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class InvalidIPError(Exception):
    """Custom exception for invalid IP addresses."""
    def __init__(self, ip):
        self.ip = ip
        super().__init__(f"Ivalid IP '{ip}' not found. Must be of the form 192.168.10.X")


# Constants
EXCLUDED_IPS = ["192.168.10.132", "192.168.10.171", "192.168.10.173", "192.168.10.174"]

PROXMOX_SERVERS = {
    1: "192.168.10.2",
    2: "192.168.10.3",
    3: "192.168.10.4",
    4: "192.168.10.5",
    5: "192.168.10.6",
}

NODES = {
    1: "simplyblock1",
    2: "simplyblock2",
    3: "simplyblock3",
    4: "simplyblock4",
    5: "simplyblock5",
}

def get_api_token(proxmox_id):
    env_var = f"PROXMOX_TOKEN_{proxmox_id}"
    token = os.getenv(env_var)
    if not token:
        err = f"Missing environment variable: {env_var}"
        logging.error(err)
        raise Exception(err)
    
    return f"PVEAPIToken={token}"

def wait_for_status(proxmox_ip, node, vm_id, desired_status, api_token, timeout_seconds=300):
    """
    Wait for a VM to reach a desired status.
    """
    url = f"https://{proxmox_ip}:8006/api2/json/nodes/{node}/qemu/{vm_id}/status/current"
    headers = {"Authorization": api_token}

    start_time = time.time()

    while time.time() - start_time < timeout_seconds:
        try:
            response = requests.get(url, headers=headers, verify=False)
            response.raise_for_status()
            status = response.json().get("data", {}).get("status", "")
            if status == desired_status:
                logging.info(f"VM {vm_id} is now '{desired_status}'.")
                return True
            else:
                logging.info(f"Waiting for VM {vm_id} to reach '{desired_status}'...")
        except Exception as e:
            logging.error(f"Error checking VM status: {e}")

        time.sleep(3)

def is_valid_ip(ip):
    """
    Check if the IP starts with 192.168.10.X
    """
    matched = True
    if not re.match(r"^192\.168\.10\.\d+$", ip):
        matched = False
    return matched

def get_proxmox(ip) -> Tuple[int, int]:
    """
    Check if the last octet of the IP is in the valid range.
    """
    if not is_valid_ip(ip):
        raise InvalidIPError(ip)

    last_octet = int(ip.split('.')[-1])

    if 20 <= last_octet <= 49:
        proxmox_id = 1
        vm_id = 100 + (last_octet - 20)
    elif 50 <= last_octet <= 79:
        proxmox_id = 2
        vm_id = 100 + (last_octet - 50)
    elif 80 <= last_octet <= 109:
        proxmox_id = 3
        vm_id = 100 + (last_octet - 80)
    elif 110 <= last_octet <= 139:
        proxmox_id = 4
        vm_id = 100 + (last_octet - 110)
    elif 140 <= last_octet <= 169:
        proxmox_id = 5
        vm_id = 100 + (last_octet - 140)
    else:
        proxmox_id = 0
        vm_id = 0

    return proxmox_id, vm_id

def stop_vm(proxmox_id, vm_id, timeout_seconds=300):
    """
    stops a VM on the proxmox server
    """
    proxmox_ip = PROXMOX_SERVERS[proxmox_id]
    node = NODES[proxmox_id]
    api_token = get_api_token(proxmox_id)

    headers = {"Authorization": api_token}
    stop_url = f"https://{proxmox_ip}:8006/api2/json/nodes/{node}/qemu/{vm_id}/status/stop"
    resp = requests.post(stop_url, headers=headers, verify=False)
    logging.info(f"received response status {resp.status_code}")
    wait_for_status(proxmox_ip, node, vm_id, "stopped", api_token, timeout_seconds)

def start_vm(proxmox_id, vm_id, timeout_seconds=300):
    """
    starts a VM on a Proxmox server
    """
    proxmox_ip = PROXMOX_SERVERS[proxmox_id]
    node = NODES[proxmox_id]
    api_token = get_api_token(proxmox_id)

    headers = {"Authorization": api_token}
    start_url = f"https://{proxmox_ip}:8006/api2/json/nodes/{node}/qemu/{vm_id}/status/start"
    resp = requests.post(start_url, headers=headers, verify=False)
    logging.info(f"received response status {resp.status_code}")
    wait_for_status(proxmox_ip, node, vm_id, "running", api_token, timeout_seconds)

def is_vm_reachable(ip, timeout_seconds=300):
    """
    Check if the VM is reachable via ping.
    """
    start_time = time.time()

    while time.time() - start_time < timeout_seconds:
        try:
            response = ping(ip, timeout=2)
            if response is not None:
                return True
        except Exception:
            pass

    return False
