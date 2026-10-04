"""API v2 client — nested RESTful paths under /api/v2/.

Wraps the simplyblock FastAPI v2 endpoints for use in parity audit tests.
Returns raw (status_code, body_dict | None) tuples so callers can compare
both the HTTP status and the response payload across interfaces.
"""

import requests
from http import HTTPStatus
from logger_config import setup_logger
from utils.common_utils import sleep_n_sec


class SbcliUtilsV2:
    """HTTP client for the simplyblock API v2 (FastAPI).

    All resource paths are nested under::

        /api/v2/clusters/{cluster_id}/...

    Unlike the v1 wrapper (:class:`SbcliUtils`), this class:
    * uses the ``/api/v2`` prefix;
    * accepts 200, 201, 202, 204 as success statuses;
    * returns ``(status_code, body_dict | None)`` instead of just the body;
    * does **not** do client-side duplicate guards.
    """

    # HTTP status codes considered successful by v2 endpoints
    SUCCESS_CODES = {200, 201, 202, 204}

    def __init__(self, cluster_api_url, cluster_id, cluster_secret):
        # cluster_api_url is something like "http://192.168.10.210"
        # v1 wrapper stores it as "http://192.168.10.210/api/v1" already —
        # strip that suffix if present so we can build our own.
        raw = cluster_api_url.rstrip("/")
        if raw.endswith("/api/v1"):
            raw = raw[: -len("/api/v1")]
        self.base_url = raw
        self.v2_base = f"{self.base_url}/api/v2"
        self.cluster_id = cluster_id
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"{cluster_id} {cluster_secret}",
        }
        self.logger = setup_logger(__name__)

    # ── generic HTTP verbs ────────────────────────────────────────────

    def _request(self, method, path, body=None, retry=3):
        """Issue an HTTP request and return (status_code, json_body | None).

        On transient failures the call is retried up to *retry* times.
        """
        url = f"{self.v2_base}{path}"
        self.logger.info(f"[v2] {method} {path}")
        attempt = 0
        while True:
            attempt += 1
            try:
                resp = requests.request(
                    method, url,
                    headers=self.headers,
                    json=body,
                    timeout=120,
                )
                status = resp.status_code
                try:
                    data = resp.json()
                except ValueError:
                    data = None

                if status in self.SUCCESS_CODES:
                    return status, data

                # Non-success — log and maybe retry
                self.logger.warning(
                    f"[v2] {method} {path} → {status}: "
                    f"{resp.text[:300]}"
                )
                if status >= 500 and attempt <= retry:
                    sleep_n_sec(2)
                    continue
                # Client error (4xx) or retries exhausted — return as-is
                return status, data

            except requests.exceptions.RequestException as exc:
                self.logger.warning(f"[v2] {method} {path} attempt {attempt} error: {exc}")
                if attempt > retry:
                    return 0, {"error": str(exc)}
                sleep_n_sec(2)

    def _get(self, path):
        return self._request("GET", path)

    def _post(self, path, body=None):
        return self._request("POST", path, body=body)

    def _put(self, path, body=None):
        return self._request("PUT", path, body=body)

    def _delete(self, path):
        return self._request("DELETE", path)

    # ── Cluster ───────────────────────────────────────────────────────

    def list_clusters(self):
        return self._get("/clusters")

    def get_cluster(self, cluster_id=None):
        cid = cluster_id or self.cluster_id
        return self._get(f"/clusters/{cid}")

    def get_cluster_capacity(self, cluster_id=None, history=None):
        cid = cluster_id or self.cluster_id
        params = f"?history={history}" if history else ""
        return self._get(f"/clusters/{cid}/capacity{params}")

    def get_cluster_iostats(self, cluster_id=None, history=None):
        cid = cluster_id or self.cluster_id
        params = f"?history={history}" if history else ""
        return self._get(f"/clusters/{cid}/iostats{params}")

    def get_cluster_logs(self, cluster_id=None, limit=50):
        cid = cluster_id or self.cluster_id
        return self._get(f"/clusters/{cid}/logs?limit={limit}")

    def get_cluster_tasks(self, cluster_id=None):
        cid = cluster_id or self.cluster_id
        return self._get(f"/clusters/{cid}/tasks")

    # ── Storage Pool ──────────────────────────────────────────────────

    def _pool_base(self, cluster_id=None):
        cid = cluster_id or self.cluster_id
        return f"/clusters/{cid}/storage-pools"

    def list_pools(self, cluster_id=None):
        return self._get(self._pool_base(cluster_id))

    def create_pool(self, name, cluster_id=None, **kwargs):
        body = {"name": name, **kwargs}
        return self._post(self._pool_base(cluster_id), body=body)

    def get_pool(self, pool_id, cluster_id=None):
        return self._get(f"{self._pool_base(cluster_id)}/{pool_id}")

    def update_pool(self, pool_id, cluster_id=None, **kwargs):
        return self._put(f"{self._pool_base(cluster_id)}/{pool_id}", body=kwargs)

    def delete_pool(self, pool_id, cluster_id=None):
        return self._delete(f"{self._pool_base(cluster_id)}/{pool_id}")

    def get_pool_iostats(self, pool_id, cluster_id=None, limit=20):
        return self._get(
            f"{self._pool_base(cluster_id)}/{pool_id}/iostats?limit={limit}"
        )

    def add_host_to_pool(self, pool_id, host_nqn, cluster_id=None):
        return self._post(
            f"{self._pool_base(cluster_id)}/{pool_id}/host",
            body={"host_nqn": host_nqn},
        )

    def remove_host_from_pool(self, pool_id, host_nqn, cluster_id=None):
        return self._delete(
            f"{self._pool_base(cluster_id)}/{pool_id}/host"
        )

    # ── Volume ────────────────────────────────────────────────────────

    def _vol_base(self, pool_id, cluster_id=None):
        return f"{self._pool_base(cluster_id)}/{pool_id}/volumes"

    def list_volumes(self, pool_id, cluster_id=None):
        return self._get(self._vol_base(pool_id, cluster_id))

    def create_volume(self, pool_id, name, size, cluster_id=None, **kwargs):
        body = {"name": name, "size": size, **kwargs}
        return self._post(self._vol_base(pool_id, cluster_id), body=body)

    def get_volume(self, pool_id, volume_id, cluster_id=None):
        return self._get(f"{self._vol_base(pool_id, cluster_id)}/{volume_id}")

    def update_volume(self, pool_id, volume_id, cluster_id=None, **kwargs):
        return self._put(
            f"{self._vol_base(pool_id, cluster_id)}/{volume_id}",
            body=kwargs,
        )

    def delete_volume(self, pool_id, volume_id, cluster_id=None):
        return self._delete(f"{self._vol_base(pool_id, cluster_id)}/{volume_id}")

    def get_volume_connect(self, pool_id, volume_id, cluster_id=None, host_nqn=None):
        params = f"?host_nqn={host_nqn}" if host_nqn else ""
        return self._get(
            f"{self._vol_base(pool_id, cluster_id)}/{volume_id}/connect{params}"
        )

    def get_volume_capacity(self, pool_id, volume_id, cluster_id=None, history=None):
        params = f"?history={history}" if history else ""
        return self._get(
            f"{self._vol_base(pool_id, cluster_id)}/{volume_id}/capacity{params}"
        )

    def get_volume_iostats(self, pool_id, volume_id, cluster_id=None, history=None):
        params = f"?history={history}" if history else ""
        return self._get(
            f"{self._vol_base(pool_id, cluster_id)}/{volume_id}/iostats{params}"
        )

    def inflate_volume(self, pool_id, volume_id, cluster_id=None):
        return self._post(
            f"{self._vol_base(pool_id, cluster_id)}/{volume_id}/inflate"
        )

    def clone_volume(self, pool_id, volume_id, clone_name, cluster_id=None, **kwargs):
        body = {"clone_name": clone_name, **kwargs}
        return self._post(
            f"{self._vol_base(pool_id, cluster_id)}/{volume_id}/clone",
            body=body,
        )

    # ── Snapshot ──────────────────────────────────────────────────────

    def list_snapshots(self, pool_id, cluster_id=None):
        return self._get(f"{self._pool_base(cluster_id)}/{pool_id}/snapshots")

    def get_snapshot(self, pool_id, snapshot_id, cluster_id=None):
        return self._get(
            f"{self._pool_base(cluster_id)}/{pool_id}/snapshots/{snapshot_id}"
        )

    def create_snapshot(self, pool_id, volume_id, name, backup=False, cluster_id=None):
        body = {"name": name, "backup": backup}
        return self._post(
            f"{self._vol_base(pool_id, cluster_id)}/{volume_id}/snapshots",
            body=body,
        )

    def delete_snapshot(self, pool_id, snapshot_id, cluster_id=None):
        return self._delete(
            f"{self._pool_base(cluster_id)}/{pool_id}/snapshots/{snapshot_id}"
        )

    # ── Storage Node ──────────────────────────────────────────────────

    def _node_base(self, cluster_id=None):
        cid = cluster_id or self.cluster_id
        return f"/clusters/{cid}/storage-nodes"

    def list_nodes(self, cluster_id=None):
        return self._get(self._node_base(cluster_id))

    def get_node(self, node_id, cluster_id=None):
        return self._get(f"{self._node_base(cluster_id)}/{node_id}")

    def get_node_capacity(self, node_id, cluster_id=None, history=None):
        params = f"?history={history}" if history else ""
        return self._get(f"{self._node_base(cluster_id)}/{node_id}/capacity{params}")

    def get_node_iostats(self, node_id, cluster_id=None, history=None):
        params = f"?history={history}" if history else ""
        return self._get(f"{self._node_base(cluster_id)}/{node_id}/iostats{params}")

    def list_node_nics(self, node_id, cluster_id=None):
        return self._get(f"{self._node_base(cluster_id)}/{node_id}/nics")

    # ── Device ────────────────────────────────────────────────────────

    def _device_base(self, node_id, cluster_id=None):
        return f"{self._node_base(cluster_id)}/{node_id}/devices"

    def list_devices(self, node_id, cluster_id=None):
        return self._get(self._device_base(node_id, cluster_id))

    def get_device(self, node_id, device_id, cluster_id=None):
        return self._get(f"{self._device_base(node_id, cluster_id)}/{device_id}")

    def get_device_capacity(self, node_id, device_id, cluster_id=None, history=None):
        params = f"?history={history}" if history else ""
        return self._get(
            f"{self._device_base(node_id, cluster_id)}/{device_id}/capacity{params}"
        )

    def get_device_iostats(self, node_id, device_id, cluster_id=None, history=None):
        params = f"?history={history}" if history else ""
        return self._get(
            f"{self._device_base(node_id, cluster_id)}/{device_id}/iostats{params}"
        )

    # ── Management Node ───────────────────────────────────────────────

    def list_management_nodes(self):
        return self._get("/management-nodes")

    def get_management_node(self, node_id):
        return self._get(f"/management-nodes/{node_id}")

    # ── Backup ────────────────────────────────────────────────────────

    def list_backups(self, cluster_id=None):
        cid = cluster_id or self.cluster_id
        return self._get(f"/clusters/{cid}/backups")

    # ── Migration ─────────────────────────────────────────────────────

    def list_migrations(self, pool_id, volume_id, cluster_id=None):
        return self._get(
            f"{self._vol_base(pool_id, cluster_id)}/{volume_id}/migrations"
        )
