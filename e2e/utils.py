import requests
from http import HTTPStatus


class SbcliUtils:
    """Contains all API calls
    """
    
    def __init__(self, cluster_ip, url, cluster_secret, 
                 cluster_api_url, cluster_id):
        self.cluster_ip = cluster_ip
        self.cluster_id = cluster_id
        self.url = url
        self.cluster_secret = cluster_secret
        self.cluster_api_url = cluster_api_url
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"{cluster_id} {cluster_secret}"
        }

    def get_node_without_lvols(self) -> str:
        """
        returns a single nodeID which doesn't have any lvol attached
        """

        request_url = self.cluster_api_url + "/storagenode"

        # a node which doesn't have any lvols attached
        node_uuid = ""
        resp = requests.get(request_url, headers=self.headers)
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

    def shutdown_node(self, node_uuid: str):
        """
        given a node_UUID, shutdowns the node
        """
        request_url = self.cluster_api_url + "/storagenode/shutdown/" + node_uuid
        resp = requests.get(request_url, headers=self.headers)
        if resp.status_code == HTTPStatus.OK:
            data = resp.json()
            print(data)
            # TODO: parse and display error accordingly: {'results': True, 'status': True}
        else:
            print('request failed. status_code', resp.status_code)
            print('request failed. text', resp.text)


    def suspend_node(self, node_uuid: str):
        """
        given a node_UUID, suspends the node
        """
        request_url = self.cluster_api_url + "/storagenode/suspend/" + node_uuid
        resp = requests.get(request_url, headers=self.headers)
        if resp.status_code == HTTPStatus.OK:
            data = resp.json()
            print(data)
            # TODO: parse and display error accordingly: {'results': True, 'status': True}
        else:
            print('request failed. status_code', resp.status_code)
            print('request failed. text', resp.text)

    def resume_node(self, node_uuid: str):
        """
        given a node_UUID, resumes the node
        """
        request_url = self.cluster_api_url + "/storagenode/resume/" + node_uuid
        resp = requests.get(request_url, headers=self.headers)
        if resp.status_code == HTTPStatus.OK:
            data = resp.json()
            print(data)
        else:
            print('request failed. status_code', resp.status_code)
            print('request failed. text', resp.text)

    def restart_node(self, node_uuid: str):
        """
        given a node_UUID, restarts the node
        """
        request_url = self.cluster_api_url + "/storagenode/restart/" + node_uuid
        resp = requests.get(request_url, headers=self.headers)
        if resp.status_code == HTTPStatus.OK:
            data = resp.json()
            print(data)
        else:
            print('request failed. status_code', resp.status_code)
            print('request failed. text', resp.text)
