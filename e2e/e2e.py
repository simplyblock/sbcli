### simplyblock e2e tests
# import requests
# from http import HTTPStatus
import time
import traceback
# from utils import get_node_without_lvols, suspend_node, shutdown_node
from __init__ import get_all_tests
from logger_config import setup_logger

# selected the node that doesn't have lvol attached

#  cluster_secret = "AlSGu3Pi4F9jwn47j8Ww"
# cluster_uuid = "6b602adf-8fe8-4a31-bd5b-4baffad95ca1"
# cluster_ip = "44.222.172.138"

# url =f"http://{cluster_ip}/"
# headers = {
#     "Content-Type": "application/json",
#     "Authorization": f"{cluster_uuid} {cluster_secret}"
# }



# def validate(node_uuid):
#     # checks

#     #
#     # the node is in status “offline”
#     # health-check status of all nodes and devices is “true”
#     #

#     request_url = url + "/storagenode"
#     # a node which doesn't have any lvols attached
#     resp = requests.get(request_url, headers=headers)
#     if resp.status_code == HTTPStatus.OK:
#         data = resp.json()
#         for result in data['results']:
#             print("node status: ", result['status'] )
#             print("health_check: ", result['health_check'])

#     print()

#     #
#     # the cluster is in status “degraded”: observation: cluster status active. (only for HA cluster it changes)
#     #
#     request_url = url + "/cluster/" + cluster_uuid
#     # a node which doesn't have any lvols attached
#     resp = requests.get(request_url, headers=headers)
#     if resp.status_code == HTTPStatus.OK:
#         data = resp.json()
#         print("cluster status: ", data['results'][0]['status'])

#     #
#     #  the devices of the node are in status "unavailable"
#     #
#     # get the device associated with the lvol
#     # request_url = url + "/cluster/" + cluster_uuid


#     #
#     # lvols remain in “online” state
#     #
#     lvol_uuid = "13b156fe-5f94-4b65-ac23-49cbfe9193f9"
#     request_url = url + "/lvol/" + lvol_uuid

#     # a node which doesn't have any lvols attached
#     resp = requests.get(request_url, headers=headers)
#     if resp.status_code == HTTPStatus.OK:
#         data = resp.json()
#         print('lvol status: ', data['results'][0]['status'])


#     #
#     # the event log contains the records indicating the object status changes;
#     # the event log also contains records indicating read and write IO errors.
#     #
#     request_url = url + "/cluster/get-logs/" + cluster_uuid
#     resp = requests.get(request_url, headers=headers)
#     if resp.status_code == HTTPStatus.OK:
#         data = resp.json()
#         for result in data['results']:
#             if result['Message'] == "Storage node status changed from: online to: suspended":
#                 print("Event Log Verification success")
#                 # todo: also check time
#                 # todo: also check for read/write errors
#                 break
#     else:
#         print(resp.text)
#         print(resp.status_code)

#     #
#     # select a cluster map from any of the two lvols (lvol get-cluster-map) -
#     # the status changes of the node and devices are reflected in the other cluster map.
#     # Other two nodes and 4 devices remain online.
#     #


#     #
#     # check if fio is still running
#     #
#     # just check if the pod is running?
#     #

def main():
    # node_uuid = get_node_without_lvols()

    # print('suspending node...')
    # suspend_node(node_uuid)
    # shutdown_node(node_uuid)

    # print('Done.\n')

    # validate(node_uuid)
    tests = get_all_tests()
    errors = {}
    for test in tests:
        logger.info(f"Running Test {test}")
        test_obj = test()
        try:
            test_obj.setup()
            test_obj.run()
        except Exception as exp:
            logger.error(traceback.format_exc())
            errors[f"{test}"] = [exp]
        try:
            test_obj.teardown()
        except Exception as exp:
            logger.error(traceback.format_exc())
            errors[f"{test}"].append(exp)
    
    for test, exception in errors.items():
        logger.error(f"Raising exception for test: {test}")
        for exc in exception:
            raise exc
    # resume_node(node_uuid)
    # restart_node(node_uuid)
    # after restart the node status switches back to online

def generate_report():
    """
    If any of the above conditions are not true, the relevant outputs from logs or cli commands should be placed
    automatically in a bug report; we may just create a shared folder and place a textfile bug report per run
    there under the date of the run: No failure report file → everthing went ok.
    failure report file for a particular date and time → contains relevant logs of the run
    (fio output, output of sbcli sn list, sbcli sn list-devices, sbcli cluster status, sbcli cluster get-logs,
    sbcli lvol get, sbcli lvol get-cluster-map, spdk log)
    """
    pass


logger = setup_logger(__name__)
main()
