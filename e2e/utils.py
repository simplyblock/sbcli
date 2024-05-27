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

    def get_all_nodes_ip(self):
        """Return all nodes part of cluster
        """
        management_nodes = []
        storage_nodes = []
        
        data = self.get_management_nodes()

        for nodes in data["results"]:
            management_nodes.append(nodes["mgmt_ip"])

        data = self.get_storage_nodes()

        for nodes in data["results"]:
            storage_nodes.append(nodes["mgmt_ip"])

        return management_nodes, storage_nodes
    
    def get_management_nodes(self):
        request_url = self.cluster_api_url + "/mgmtnode/"
        resp = requests.get(request_url, headers=self.headers)
        if resp.status_code == HTTPStatus.OK:
            data = resp.json()
        else:
            print('request failed. status_code', resp.status_code)
            print('request failed. text', resp.text)
        return data
    
    def get_storage_nodes(self):
        request_url = self.cluster_api_url + "/storagenode/"
        resp = requests.get(request_url, headers=self.headers)
        if resp.status_code == HTTPStatus.OK:
            data = resp.json()
        else:
            print('request failed. status_code', resp.status_code)
            print('request failed. text', resp.text)
        return data
    
    def list_storage_pools(self):
        pool_data = dict()
        request_url = self.cluster_api_url + "/pool"
        resp = requests.get(request_url, 
                             headers=self.headers)
        if resp.status_code == HTTPStatus.OK:
            data = resp.json()
        else:
            print('request failed. status_code', resp.status_code)
            print('request failed. text', resp.text)
        for pool_info in data["results"]:
            pool_data[pool_info["pool_name"]] = pool_info["id"]
            
        return pool_data
    
    def get_pool_by_id(self, pool_id):
        request_url = self.cluster_api_url + "/pool/" + pool_id

        resp = requests.get(request_url, 
                            headers=self.headers)
        
        if resp.status_code == HTTPStatus.OK:
            data = resp.json()
        else:
            print('request failed. status_code', resp.status_code)
            print('request failed. text', resp.text)

        return data

    def add_storage_pool(self, pool_name):
        pools = self.list_storage_pools()
        for name in list(pools.keys()):
            if name == pool_name:
                print(f"Pool {pool_name} already exists. Exiting")
                return

        request_url = self.cluster_api_url + "/pool"
        body = {
            "name": pool_name
        }
        resp = requests.post(request_url, 
                             headers=self.headers,
                             json=body)
        if resp.status_code == HTTPStatus.OK:
            data = resp.json()
            print(data)
        else:
            print('request failed. status_code', resp.status_code)
            print('request failed. text', resp.text)

    def delete_storage_pool(self, pool_name):
        pools = self.list_storage_pools()
        pool_id = None
        for name in list(pools.keys()):
            if name == pool_name:
                pool_id = pools[name]
        
        if not pool_id:
            print("Pool does not exist. Exiting")
            return
        
        pool_data = self.get_pool_by_id(pool_id=pool_id)

        request_url = self.cluster_api_url + "/pool/" + pool_id
        header = self.headers.copy()

        header["secret"] = pool_data["results"][0]["secret"]

        resp = requests.delete(request_url, 
                               headers=header)
        if resp.status_code == HTTPStatus.OK:
            data = resp.json()
            print(data)
        else:
            print('request failed. status_code', resp.status_code)
            print('request failed. text', resp.text)
    
    def delete_all_storage_pools(self):
        pools = self.list_storage_pools()
        for name in list(pools.keys()):
            print(f"Deleting pool: {name}")
            self.delete_storage_pool(pool_name=name)

    def list_lvols(self):
        pool_data = dict()
        request_url = self.cluster_api_url + "/pool"
        resp = requests.get(request_url, 
                             headers=self.headers)
        if resp.status_code == HTTPStatus.OK:
            data = resp.json()
        else:
            print('request failed. status_code', resp.status_code)
            print('request failed. text', resp.text)
        for pool_info in data["results"]:
            pool_data[pool_info["pool_name"]] = pool_info["id"]
            
        return pool_data
    
    def get_lvol_by_id(self, pool_id):
        request_url = self.cluster_api_url + "/pool/" + pool_id

        resp = requests.get(request_url, 
                            headers=self.headers)
        
        if resp.status_code == HTTPStatus.OK:
            data = resp.json()
        else:
            print('request failed. status_code', resp.status_code)
            print('request failed. text', resp.text)

        return data

    def add_lvol(self, pool_name):
        pools = self.list_storage_pools()
        for name in list(pools.keys()):
            if name == pool_name:
                print(f"Pool {pool_name} already exists. Exiting")
                return

        request_url = self.cluster_api_url + "/pool"
        body = {
            "name": pool_name
        }
        resp = requests.post(request_url, 
                             headers=self.headers,
                             json=body)
        if resp.status_code == HTTPStatus.OK:
            data = resp.json()
            print(data)
        else:
            print('request failed. status_code', resp.status_code)
            print('request failed. text', resp.text)

    def delete_lvol(self, pool_name):
        pools = self.list_storage_pools()
        pool_id = None
        for name in list(pools.keys()):
            if name == pool_name:
                pool_id = pools[name]
        
        if not pool_id:
            print("Pool does not exist. Exiting")
            return
        
        pool_data = self.get_pool_by_id(pool_id=pool_id)

        request_url = self.cluster_api_url + "/pool/" + pool_id
        header = self.headers.copy()

        header["secret"] = pool_data["results"][0]["secret"]

        resp = requests.delete(request_url, 
                               headers=header)
        if resp.status_code == HTTPStatus.OK:
            data = resp.json()
            print(data)
        else:
            print('request failed. status_code', resp.status_code)
            print('request failed. text', resp.text)
    
    def delete_all_lvols(self):
        pools = self.list_storage_pools()
        for name in list(pools.keys()):
            print(f"Deleting pool: {name}")
            self.delete_storage_pool(pool_name=name)

