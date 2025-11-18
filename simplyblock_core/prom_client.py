import json

import requests
import logging

from requests.adapters import HTTPAdapter
from urllib3 import Retry

logger = logging.getLogger()


class PromClientException(Exception):
    def __init__(self, message):
        self.message = message


class PromClient:

    def __init__(self, cluster_ip, timeout=300, retry=5):
        self.ip_address = f"{cluster_ip}:9090"
        self.url = 'http://%s/' % self.ip_address
        self.timeout = timeout
        self.session = requests.session()
        self.session.verify = False
        self.session.headers['Content-Type'] = "application/json"
        retries = Retry(total=retry, backoff_factor=1, connect=retry, read=retry)
        self.session.mount("http://", HTTPAdapter(max_retries=retries))

    def _request(self, method, path, payload=None):
        try:
            logger.debug("Requesting path: %s, params: %s", path, payload)
            data = None
            params = None
            if payload:
                if method == "GET" :
                    params = payload
                else:
                    data = json.dumps(payload)

            response = self.session.request(method, self.url+path, data=data,
                                            timeout=self.timeout, params=params)
        except Exception as e:
            raise e

        logger.debug("Response: status_code: %s, content: %s",
                     response.status_code, response.content)
        ret_code = response.status_code

        result = None
        error = None
        if ret_code == 200:
            try:
                decoded_data = response.json()
            except Exception:
                return response.content, None

            result = decoded_data.get('results')
            error = decoded_data.get('error')
            if result is not None or error is not None:
                return result, error
            else:
                return data, None

        if ret_code in [500, 400]:
            raise PromClientException("Invalid http status: %s" % ret_code)

        if ret_code == 422:
            raise PromClientException(f"Request validation failed: '{response.text}'")

        logger.error("Unknown http status: %s", ret_code)
        return None, None

    def query(self, query_st):
        return self._request("GET", f"/api/{query_st}")

    def get_cluster_capacity(self, cluster_id, start, end, step=15):
        from prometheus_api_client import PrometheusConnect
        # Connect to Prometheus
        prom = PrometheusConnect(url="http://localhost:9090", disable_ssl=True)

        # Get all metrics

        metrics = prom.all_metrics()

        params = {
            'cluster': cluster_id,
            'start': start, #  start=2015-07-01T20:10:30.781Z
            'end': end,     #  end=2015-07-01T20:11:00.781Z
            'step': f'{step}s',   #  step=15s
        }
        return self._request("GET", "/api/v1/query", params)
