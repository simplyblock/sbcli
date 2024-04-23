
import requests
from http import HTTPStatus

cluster_secret = "AlSGu3Pi4F9jwn47j8Ww"
cluster_uuid = "6b602adf-8fe8-4a31-bd5b-4baffad95ca1"
cluster_ip = "44.222.172.138"

url = f"http://{cluster_ip}/"
headers = {
    "Content-Type": "application/json",
    "Authorization": f"{cluster_uuid} {cluster_secret}"
}

def get_node_without_lvols() -> str:
    """
    returns a single nodeID which doesn't have any lvol attached
    """

    request_url = url + "/storagenode"

    # a node which doesn't have any lvols attached
    node_uuid = ""
    resp = requests.get(request_url, headers=headers)
    if resp.status_code == HTTPStatus.OK:
        data = resp.json()
        for result in data['results']:
            if len(result['lvols']) == 0:
                node_uuid = result['uuid']
                break
    else:
        print('request failed. status_code', resp.status_code)
        print('request failed. text', resp.text)

    return node_uuid

def shutdown_node(node_uuid: str):
    """
    given a node_UUID, shutdowns the node
    """
    request_url = url + "/storagenode/shutdown/" + node_uuid
    resp = requests.get(request_url, headers=headers)
    if resp.status_code == HTTPStatus.OK:
        data = resp.json()
        print(data)
        # todo parse and display error accordingly: {'results': True, 'status': True}
    else:
        print('request failed. status_code', resp.status_code)
        print('request failed. text', resp.text)


def suspend_node(node_uuid: str):
    """
    given a node_UUID, suspends the node
    """
    request_url = url + "/storagenode/suspend/" + node_uuid
    resp = requests.get(request_url, headers=headers)
    if resp.status_code == HTTPStatus.OK:
        data = resp.json()
        print(data)
        # todo parse and display error accordingly: {'results': True, 'status': True}
    else:
        print('request failed. status_code', resp.status_code)
        print('request failed. text', resp.text)

def resume_node(node_uuid: str):
    """
    given a node_UUID, resumes the node
    """
    request_url = url + "/storagenode/resume/" + node_uuid
    resp = requests.get(request_url, headers=headers)
    if resp.status_code == HTTPStatus.OK:
        data = resp.json()
        print(data)
    else:
        print('request failed. status_code', resp.status_code)
        print('request failed. text', resp.text)

def restart_node(node_uuid: str):
    """
    given a node_UUID, restarts the node
    """
    request_url = url + "/storagenode/restart/" + node_uuid
    resp = requests.get(request_url, headers=headers)
    if resp.status_code == HTTPStatus.OK:
        data = resp.json()
        print(data)
    else:
        print('request failed. status_code', resp.status_code)
        print('request failed. text', resp.text)
