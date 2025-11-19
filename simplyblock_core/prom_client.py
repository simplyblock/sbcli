import json
import logging
import re
from datetime import datetime, timedelta

from simplyblock_core.db_controller import DBController
from simplyblock_core.models.mgmt_node import MgmtNode

from prometheus_api_client import PrometheusConnect

logger = logging.getLogger()


class PromClientException(Exception):
    def __init__(self, message):
        self.message = message


class PromClient:

    def __init__(self, cluster_id):
        db_controller = DBController()
        cluster_ip = None
        for node in db_controller.get_mgmt_nodes():
            if node.cluster_id == cluster_id and node.status == MgmtNode.STATUS_ONLINE:
                cluster_ip = node.mgmt_ip
                break
        if cluster_ip is None:
            raise PromClientException("Cluster has no online mgmt nodes")

        self.ip_address = f"{cluster_ip}:9090"
        self.url = 'http://%s/' % self.ip_address
        self.client = PrometheusConnect(url=self.url, disable_ssl=True)

    def parse_history_param(self, history_string):
        if not history_string:
            logger.error("Invalid history value")
            return False

        # process history
        results = re.search(r'^(\d+[hmd])(\d+[hmd])?$', history_string.lower())
        if not results:
            logger.error(f"Error parsing history string: {history_string}")
            logger.info("History format: xxdyyh , e.g: 1d12h, 1d, 2h, 1m")
            return False

        history_in_seconds = 0
        for s in results.groups():
            if not s:
                continue
            ind = s[-1]
            v = int(s[:-1])
            if ind == 'd':
                history_in_seconds += v * (60 * 60 * 24)
            if ind == 'h':
                history_in_seconds += v * (60 * 60)
            if ind == 'm':
                history_in_seconds += v * 60

        records_number = int(history_in_seconds / 5)
        return records_number

    def get_cluster_metrics(self, cluster_uuid, metrics_lst, history=None):
        start_time = datetime.now() - timedelta(minutes=10)
        if history:
            try:
                days,hours,minutes = self.parse_history_param(history)
                start_time = datetime.now() - timedelta(days=days, hours=hours, minutes=minutes)
            except Exception as e:
                raise PromClientException(f"Error parsing history string: {history}")

        params = {
            "cluster": cluster_uuid
        }
        data_out=[]
        for key in metrics_lst:
            metrics = self.client.get_metric_range_data(
                f"cluster_{key}", label_config=params, start_time=start_time)
            for m in metrics:
                mt_name = m['metric']["__name__"]
                mt_values = m["values"]
                for i, v in enumerate(mt_values):
                    value = v[1]
                    try:
                        value = int(value)
                    except Exception:
                        pass
                    if len(data_out) <= i:
                        data_out.append({mt_name: value})
                    else:
                        d = data_out[i]
                        if mt_name not in d:
                            d[mt_name] = value

        return data_out

cl = PromClient("c448e89a-7671-44e6-94a1-8964b1459200")
d=cl.get_cluster_capacity("c448e89a-7671-44e6-94a1-8964b1459200")
print(json.dumps(d, indent=2))