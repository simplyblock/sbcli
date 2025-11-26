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

        history_in_days = 0
        history_in_hours = 0
        history_in_minutes = 0
        for s in results.groups():
            if not s:
                continue
            ind = s[-1]
            v = int(s[:-1])
            if ind == 'd':
                history_in_days = v
            if ind == 'h':
                history_in_hours = v
            if ind == 'm':
                history_in_minutes = v

        history_in_hours += int(history_in_minutes/60)
        history_in_minutes = history_in_minutes % 60
        history_in_days += int(history_in_hours/24)
        history_in_hours = history_in_hours % 24
        return history_in_days, history_in_hours, history_in_minutes

    def get_metrics(self, key_prefix, metrics_lst, params, history=None):
        start_time = datetime.now() - timedelta(minutes=10)
        if history:
            try:
                days,hours,minutes = self.parse_history_param(history)
                start_time = datetime.now() - timedelta(days=days, hours=hours, minutes=minutes)
            except Exception:
                raise PromClientException(f"Error parsing history string: {history}")

        data_out: list[dict] = []
        for key in metrics_lst:
            metrics = self.client.get_metric_range_data(
                f"{key_prefix}_{key}", label_config=params, start_time=start_time)
            for m in metrics:
                mt_name = key
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

    def get_cluster_metrics(self, cluster_uuid, metrics_lst, history=None):
        params = {
            "cluster": cluster_uuid
        }
        return self.get_metrics("cluster", metrics_lst, params, history)

    def get_node_metrics(self, snode_uuid, metrics_lst, history=None):
        params = {
            "snode": snode_uuid
        }
        return self.get_metrics("snode", metrics_lst, params, history)

    def get_device_metrics(self, device_uuid, metrics_lst, history=None):
        params = {
            "device": device_uuid
        }
        return self.get_metrics("device", metrics_lst, params, history)

    def get_lvol_metrics(self, lvol_uuid, metrics_lst, history=None):
        params = {
            "lvol": lvol_uuid
        }
        return self.get_metrics("lvol", metrics_lst, params, history)

    def get_pool_metrics(self, pool_uuid, metrics_lst, history=None):
        params = {
            "pool": pool_uuid
        }
        return self.get_metrics("pool", metrics_lst, params, history)
