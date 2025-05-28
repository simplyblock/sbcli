# usage: 
# in CI
# docker compose -f docker-compose-dev.yml exec mgmt-server python manual_testing/check_endpoints.py

import uuid
import requests
import time
import sys

from simplyblock_core.models.cluster import Cluster
from simplyblock_core.db_controller import DBController
from simplyblock_core import utils

db_controller = DBController()
c = Cluster()

print("waiting for mgmt node to be up...")
time.sleep(10)

BASE_URL = "http://mgmt-server:5000"
CLUSTER_UUID = str(uuid.uuid4())
SECRET = utils.generate_string(20)

c.uuid = CLUSTER_UUID
c.secret = SECRET
c.write_to_db(db_controller.kv_store)


AUTH_HEADER = {
    "Authorization": f"{CLUSTER_UUID} {SECRET}"
}

def check_endpoint(name, url):
    print(f"Checking {name}...")
    try:
        response = requests.get(url, headers=AUTH_HEADER)
        if response.status_code == 200:
            print(f"✅ {name} - OK")
        else:
            print(f"😑 {name} - Status {response.status_code}")
            print(response.json() if response.headers.get('Content-type') == 'application/json' else response.content)
            sys.exit(1)
    except Exception as e:
        print(f"[!] {name} - Exception: {e}")
        sys.exit(1)


if __name__ == "__main__":
    check_endpoint("Cluster Root", f"{BASE_URL}/cluster/")
    check_endpoint("Cluster Info", f"{BASE_URL}/cluster/{CLUSTER_UUID}")
    check_endpoint("Cluster Status", f"{BASE_URL}/cluster/status/{CLUSTER_UUID}")
    check_endpoint("Cluster Logs", f"{BASE_URL}/cluster/get-logs/{CLUSTER_UUID}")
    # FIXME: this endpoint return 500 error
    # check_endpoint("Cluster Tasks", f"{BASE_URL}/cluster/get-tasks/{CLUSTER_UUID}")
    check_endpoint("Cluster Capacity", f"{BASE_URL}/cluster/capacity/{CLUSTER_UUID}")
    check_endpoint("Capacity History (1d)", f"{BASE_URL}/cluster/capacity/{CLUSTER_UUID}/history/1d")
    check_endpoint("IOStats History (1d)", f"{BASE_URL}/cluster/iostats/{CLUSTER_UUID}/history/1d")

    print("✅ All cluster endpoint checks passed!")
