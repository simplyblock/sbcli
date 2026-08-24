"""Three-way parity audit: CLI vs API v1 vs API v2.

For each auditable operation, invokes all three interfaces against the
live cluster and compares:

* Whether the call succeeded or failed
* Which fields are present in the response
* Whether field values match across interfaces

Results are collected as a list of *findings* (dicts) and rendered into
an HTML report + JSON sidecar at the end of the run.
"""

import json
import os
import traceback

from e2e_tests.cluster_test_base import TestClusterBase
from logger_config import setup_logger
from utils.common_utils import sleep_n_sec
from utils.sbcli_utils_v2 import SbcliUtilsV2
from utils.parity_report import generate_html_report


class TestAPIParityAudit(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "api_parity_audit"
        self.logger = setup_logger(__name__)
        self.findings = []
        # Pool created for the duration of the audit
        self._audit_pool_name = "parity_audit_pool"
        self._audit_pool_id = None

    def setup(self):
        super().setup()
        self.v2 = SbcliUtilsV2(
            cluster_api_url=self.api_base_url,
            cluster_id=self.cluster_id,
            cluster_secret=self.cluster_secret,
        )

    # ── orchestrator ──────────────────────────────────────────────────

    def run(self):
        self.logger.info("=== API Parity Audit: CLI / v1 / v2 ===")

        # Phase 0: ensure a pool exists for the entire audit
        try:
            self._ensure_audit_pool()
        except Exception:
            self.logger.error(
                f"[pool.setup] failed to create audit pool: "
                f"{traceback.format_exc()}"
            )
            self._finding(
                "error", "audit_crash", "pool.setup",
                detail=traceback.format_exc(limit=3),
            )

        # Phase 1: read-only audits (safe)
        audits_readonly = [
            ("cluster.list", self._audit_cluster_list),
            ("cluster.get", self._audit_cluster_get),
            ("cluster.capacity", self._audit_cluster_capacity),
            ("cluster.iostats", self._audit_cluster_iostats),
            ("cluster.logs", self._audit_cluster_logs),
            ("node.list", self._audit_node_list),
            ("node.get", self._audit_node_get),
            ("node.capacity", self._audit_node_capacity),
            ("node.ports", self._audit_node_ports),
            ("device.list", self._audit_device_list),
            ("device.get", self._audit_device_get),
            ("pool.list", self._audit_pool_list),
            ("pool.get", self._audit_pool_get),
            ("pool.iostats", self._audit_pool_iostats),
            ("mgmt.list", self._audit_mgmt_list),
        ]

        for name, fn in audits_readonly:
            try:
                fn()
            except Exception:
                self.logger.error(f"[{name}] audit crashed: {traceback.format_exc()}")
                self._finding("error", "audit_crash", name,
                              detail=traceback.format_exc(limit=3))

        # Phase 2: create-then-compare audits
        audits_write = [
            ("pool.crud", self._audit_pool_crud),
            ("volume.crud", self._audit_volume_crud),
            ("snapshot.crud", self._audit_snapshot_crud),
        ]

        for name, fn in audits_write:
            try:
                fn()
            except Exception:
                self.logger.error(f"[{name}] audit crashed: {traceback.format_exc()}")
                self._finding("error", "audit_crash", name,
                              detail=traceback.format_exc(limit=3))

        # Cleanup the audit pool
        self._cleanup_audit_pool()

        # Generate report
        report_dir = self.docker_logs_path or os.path.join("logs")
        os.makedirs(report_dir, exist_ok=True)
        html_path = generate_html_report(
            self.findings, report_dir,
            cluster_id=self.cluster_id,
            extra_meta={"branch": os.environ.get("GITHUB_REF_NAME", "local")},
        )
        self.logger.info(f"HTML report written to {html_path}")

        # ── Print detailed summary to log ──────────────────────────────
        errors = [f for f in self.findings if f["severity"] == "error"]
        warnings = [f for f in self.findings if f["severity"] == "warning"]
        infos = [f for f in self.findings if f["severity"] == "info"]

        self.logger.info("=" * 70)
        self.logger.info("API PARITY AUDIT SUMMARY")
        self.logger.info("=" * 70)
        self.logger.info(
            f"Total findings: {len(self.findings)}  |  "
            f"Errors: {len(errors)}  |  "
            f"Warnings: {len(warnings)}  |  "
            f"Info: {len(infos)}"
        )

        # Group findings by category for readable output
        by_category = {}
        for f in self.findings:
            by_category.setdefault(f["category"], []).append(f)

        for cat, items in sorted(by_category.items()):
            self.logger.info(f"\n--- {cat.upper()} ({len(items)} findings) ---")
            for f in items:
                op = f.get("operation", "?")
                sev = f["severity"].upper()
                if cat == "missing_field":
                    field = f.get("field", "?")
                    cli = "Y" if f.get("cli") else "N"
                    v1 = "Y" if f.get("v1") else "N"
                    v2 = "Y" if f.get("v2") else "N"
                    self.logger.info(
                        f"  [{sev}] {op} field={field}  "
                        f"CLI={cli}  v1={v1}  v2={v2}"
                    )
                elif cat == "value_mismatch":
                    field = f.get("field", "?")
                    self.logger.info(
                        f"  [{sev}] {op} field={field}  "
                        f"CLI={f.get('cli')}  v1={f.get('v1')}  v2={f.get('v2')}"
                    )
                elif cat == "count_mismatch":
                    self.logger.info(
                        f"  [{sev}] {op}  "
                        f"CLI={f.get('cli')}  v1={f.get('v1')}  v2={f.get('v2')}"
                    )
                elif cat == "interface_error":
                    self.logger.info(
                        f"  [{sev}] {op} iface={f.get('interface')}  "
                        f"detail={f.get('detail', '')}  "
                        f"api_call={f.get('api_call', '')}  "
                        f"http_status={f.get('http_status', '')}  "
                        f"response={f.get('response_preview', '')}"
                    )
                elif cat == "audit_crash":
                    detail = f.get("detail", "")
                    # Show just the last line of the traceback
                    last_line = detail.strip().splitlines()[-1] if detail.strip() else ""
                    self.logger.info(f"  [{sev}] {op}: {last_line}")
                else:
                    self.logger.info(f"  [{sev}] {op}: {f.get('detail', '')}")

        self.logger.info("=" * 70)

        # Fail the test if there are error-level findings
        assert len(errors) == 0, (
            f"API parity audit found {len(errors)} error(s). "
            f"See report: {html_path}"
        )

    # ── pool lifecycle (ensure pool exists for all audits) ─────────

    def _ensure_audit_pool(self):
        """Create the audit pool if it doesn't already exist."""
        pools = self.sbcli_utils.list_storage_pools()
        if pools and self._audit_pool_name in pools:
            self._audit_pool_id = pools[self._audit_pool_name]
            self.logger.info(
                f"[pool.setup] Audit pool already exists: {self._audit_pool_id}"
            )
            return

        self.logger.info(
            f"[pool.setup] Creating audit pool '{self._audit_pool_name}'"
        )
        self.sbcli_utils.add_storage_pool(self._audit_pool_name)
        sleep_n_sec(5)

        pools = self.sbcli_utils.list_storage_pools()
        if pools and self._audit_pool_name in pools:
            self._audit_pool_id = pools[self._audit_pool_name]
            self.logger.info(
                f"[pool.setup] Audit pool created: {self._audit_pool_id}"
            )
        else:
            self.logger.error(
                f"[pool.setup] Audit pool '{self._audit_pool_name}' "
                f"not found after creation"
            )

    def _cleanup_audit_pool(self):
        """Remove the audit pool after all audits are done."""
        try:
            self.sbcli_utils.delete_storage_pool(self._audit_pool_name)
            sleep_n_sec(2)
            self.logger.info(
                f"[pool.cleanup] Deleted audit pool '{self._audit_pool_name}'"
            )
        except Exception as exc:
            self.logger.warning(f"[pool.cleanup] failed: {exc}")

    # ── finding helpers ───────────────────────────────────────────────

    def _finding(self, severity, category, operation, **kwargs):
        f = {"severity": severity, "category": category, "operation": operation}
        f.update(kwargs)
        self.findings.append(f)
        log_fn = self.logger.error if severity == "error" else self.logger.warning
        log_fn(f"[FINDING] {severity.upper()} {category} {operation}: {kwargs}")

    # ── CLI execution ─────────────────────────────────────────────────

    def _run_cli(self, cmd):
        """Run ``sbctl <cmd>`` on the first management node via SSH.

        Returns a dict with keys:
        - ``data``: parsed JSON on success, or ``None`` on failure
        - ``stdout``: raw stdout text
        - ``stderr``: raw stderr text
        - ``command``: the full command that was run
        """
        full_cmd = f"{self.base_cmd} {cmd}"
        result = {
            "data": None,
            "stdout": "",
            "stderr": "",
            "command": full_cmd,
        }

        if not self.mgmt_nodes:
            result["stderr"] = "no management nodes available"
            return result

        try:
            stdout, stderr = self.ssh_obj.exec_command(
                self.mgmt_nodes[0], full_cmd
            )
            result["stdout"] = stdout or ""
            result["stderr"] = stderr or ""

            if stdout and stdout.strip():
                # CLI may print non-JSON header lines; find the JSON part
                lines = stdout.strip().splitlines()
                # Try full output first
                try:
                    result["data"] = json.loads(stdout.strip())
                    return result
                except json.JSONDecodeError:
                    pass
                # Try last line
                for line in reversed(lines):
                    line = line.strip()
                    if line.startswith(("{", "[")):
                        try:
                            result["data"] = json.loads(line)
                            return result
                        except json.JSONDecodeError:
                            continue
            return result
        except Exception as exc:
            self.logger.warning(f"CLI '{cmd}' failed: {exc}")
            result["stderr"] = str(exc)
            return result

    # ── normalizers ───────────────────────────────────────────────────

    @staticmethod
    def _normalize_v1(data):
        """Unwrap v1 {status, results, error} envelope.

        If *data* has a ``results`` key, return its value.
        For list endpoints the value is a list; for single-get it's a
        one-element list — return the first element in that case.
        """
        if isinstance(data, dict) and "results" in data:
            results = data["results"]
            if isinstance(results, list) and len(results) == 1:
                return results[0]
            return results
        return data

    @staticmethod
    def _normalize_v2(status_and_body):
        """Unwrap v2 (status_code, body) tuple."""
        if isinstance(status_and_body, tuple):
            _, body = status_and_body
            return body
        return status_and_body

    @staticmethod
    def _normalize_cli(data):
        """CLI --json output is already parsed; may need list unwrap."""
        if isinstance(data, list) and len(data) == 1:
            return data[0]
        return data

    # ── helpers for recording interface errors with context ────────

    def _check_interface_data(self, operation, iface, data, api_call="",
                              http_status=None, raw_response=""):
        """Record an error finding if *data* is None / empty, with API details."""
        if data is None:
            detail = "returned None"
            if iface == "cli" and isinstance(raw_response, dict):
                stderr = raw_response.get("stderr", "")
                if stderr:
                    detail = f"returned None; stderr: {stderr[:300]}"
            self._finding(
                "error", "interface_error", operation,
                interface=iface,
                detail=detail,
                api_call=api_call,
                http_status=http_status,
                response_preview=str(raw_response)[:500] if raw_response else "",
            )
            return False
        return True

    # ── comparison engine ─────────────────────────────────────────────

    def _compare_dicts(self, operation, cli_data, v1_data, v2_data,
                       key_fields=None, ignore_fields=None):
        """Compare three response dicts and record findings.

        Parameters
        ----------
        key_fields : list[str] | None
            Fields whose *values* must match.  If None, only field
            presence is checked.
        ignore_fields : set[str] | None
            Fields to skip entirely (e.g. timestamps that always differ).
        """
        ignore = ignore_fields or set()

        def _fields(d):
            if isinstance(d, dict):
                return set(d.keys()) - ignore
            return set()

        cli_f = _fields(cli_data)
        v1_f = _fields(v1_data)
        v2_f = _fields(v2_data)
        all_f = cli_f | v1_f | v2_f

        for field in sorted(all_f):
            in_cli = field in cli_f
            in_v1 = field in v1_f
            in_v2 = field in v2_f
            if not (in_cli and in_v1 and in_v2):
                self._finding(
                    "warning", "missing_field", operation,
                    field=field, cli=in_cli, v1=in_v1, v2=in_v2,
                )

        if key_fields:
            for field in key_fields:
                if field in ignore:
                    continue
                cli_val = cli_data.get(field) if isinstance(cli_data, dict) else None
                v1_val = v1_data.get(field) if isinstance(v1_data, dict) else None
                v2_val = v2_data.get(field) if isinstance(v2_data, dict) else None
                # Normalize None vs missing
                vals = [cli_val, v1_val, v2_val]
                non_none = [v for v in vals if v is not None]
                if len(set(str(v) for v in non_none)) > 1:
                    self._finding(
                        "error", "value_mismatch", operation,
                        field=field,
                        cli=cli_val, v1=v1_val, v2=v2_val,
                    )

    def _compare_lists(self, operation, cli_data, v1_data, v2_data,
                       id_field="id", key_fields=None, ignore_fields=None):
        """Compare three list responses.

        First checks that counts match, then for each item (matched by
        *id_field*), does a dict comparison.
        """
        def _as_list(d):
            if isinstance(d, list):
                return d
            if isinstance(d, dict) and "results" in d:
                r = d["results"]
                return r if isinstance(r, list) else []
            return []

        cli_list = _as_list(cli_data)
        v1_list = _as_list(v1_data)
        v2_list = _as_list(v2_data)

        if not (len(cli_list) == len(v1_list) == len(v2_list)):
            # Collect sample IDs from each list for debugging
            def _ids(lst, limit=5):
                ids = []
                for item in lst[:limit]:
                    if isinstance(item, dict):
                        ids.append(
                            item.get(id_field, item.get("uuid",
                                     item.get("id", "?")))
                        )
                return ids

            self._finding(
                "error", "count_mismatch", operation,
                cli=len(cli_list), v1=len(v1_list), v2=len(v2_list),
                cli_ids=_ids(cli_list),
                v1_ids=_ids(v1_list),
                v2_ids=_ids(v2_list),
            )

        # Build lookup by id_field
        def _by_id(lst):
            out = {}
            for item in lst:
                if isinstance(item, dict):
                    key = item.get(id_field, item.get("uuid", id(item)))
                    out[str(key)] = item
            return out

        cli_map = _by_id(cli_list)
        v1_map = _by_id(v1_list)
        v2_map = _by_id(v2_list)

        all_ids = set(cli_map) | set(v1_map) | set(v2_map)
        for item_id in sorted(all_ids):
            self._compare_dicts(
                f"{operation}[{item_id[:8]}]",
                cli_map.get(item_id, {}),
                v1_map.get(item_id, {}),
                v2_map.get(item_id, {}),
                key_fields=key_fields,
                ignore_fields=ignore_fields,
            )

    # ── read-only audits ──────────────────────────────────────────────

    def _audit_cluster_list(self):
        self.logger.info("[audit] cluster.list")
        cli_result = self._run_cli("cluster list --json")
        cli = cli_result["data"]
        v1 = self.sbcli_utils.get_request(api_url="/cluster")
        v2_status, v2_body = self.v2.list_clusters()

        # Log API calls for debugging
        self.logger.info(
            f"  CLI: {cli_result['command']} → data={'yes' if cli else 'None'}"
            f"{' stderr=' + cli_result['stderr'][:200] if cli_result['stderr'] else ''}"
        )
        self.logger.info(f"  v1: GET /cluster → {type(v1).__name__}")
        self.logger.info(f"  v2: GET /clusters → status={v2_status}")

        self._check_interface_data(
            "cluster.list", "cli", cli,
            api_call=cli_result["command"], raw_response=cli_result,
        )
        self._check_interface_data(
            "cluster.list", "v1", self._normalize_v1(v1),
            api_call="GET /api/v1/cluster",
        )
        self._check_interface_data(
            "cluster.list", "v2", v2_body,
            api_call="GET /api/v2/clusters", http_status=v2_status,
        )

        self._compare_lists(
            "cluster.list",
            cli, self._normalize_v1(v1), v2_body,
            id_field="id",
            key_fields=["id", "name", "status"],
        )

    def _audit_cluster_get(self):
        self.logger.info("[audit] cluster.get")
        cid = self.cluster_id
        cli_result = self._run_cli(f"cluster get {cid} --json")
        cli = cli_result["data"]
        v1 = self.sbcli_utils.get_request(api_url=f"/cluster/{cid}")
        v2_status, v2_body = self.v2.get_cluster(cid)

        self._check_interface_data(
            "cluster.get", "cli", cli,
            api_call=cli_result["command"], raw_response=cli_result,
        )
        self._check_interface_data(
            "cluster.get", "v1", self._normalize_v1(v1),
            api_call=f"GET /api/v1/cluster/{cid}",
        )
        self._check_interface_data(
            "cluster.get", "v2", v2_body,
            api_call=f"GET /api/v2/clusters/{cid}", http_status=v2_status,
        )

        self._compare_dicts(
            "cluster.get",
            self._normalize_cli(cli),
            self._normalize_v1(v1),
            v2_body,
            key_fields=["id", "name", "status", "ha_type", "fabric"],
            ignore_fields={"updated_at", "created_at", "secret", "grafana_endpoint"},
        )

    def _audit_cluster_capacity(self):
        self.logger.info("[audit] cluster.capacity")
        cid = self.cluster_id
        cli_result = self._run_cli(f"cluster get-capacity {cid} --json")
        cli = cli_result["data"]
        v1 = self.sbcli_utils.get_cluster_capacity()
        v2_status, v2_body = self.v2.get_cluster_capacity(cid)

        self._check_interface_data(
            "cluster.capacity", "cli", cli,
            api_call=cli_result["command"], raw_response=cli_result,
        )
        self._check_interface_data(
            "cluster.capacity", "v1", self._normalize_v1(v1),
            api_call=f"GET /api/v1/cluster/capacity/{cid}",
        )
        self._check_interface_data(
            "cluster.capacity", "v2", v2_body,
            api_call=f"GET /api/v2/clusters/{cid}/capacity",
            http_status=v2_status,
        )

        self._compare_dicts(
            "cluster.capacity",
            self._normalize_cli(cli),
            self._normalize_v1(v1),
            v2_body,
            key_fields=["total_capacity", "used_capacity", "free_capacity"],
        )

    def _audit_cluster_iostats(self):
        self.logger.info("[audit] cluster.iostats")
        cid = self.cluster_id
        cli_result = self._run_cli(f"cluster get-io-stats {cid} --json")
        cli = cli_result["data"]
        v1 = self.sbcli_utils.get_io_stats(cid)
        v2_status, v2_body = self.v2.get_cluster_iostats(cid)

        self._check_interface_data(
            "cluster.iostats", "cli", cli,
            api_call=cli_result["command"], raw_response=cli_result,
        )
        self._check_interface_data(
            "cluster.iostats", "v1", self._normalize_v1(v1),
            api_call=f"GET /api/v1/cluster/iostats/{cid}",
        )
        self._check_interface_data(
            "cluster.iostats", "v2", v2_body,
            api_call=f"GET /api/v2/clusters/{cid}/iostats",
            http_status=v2_status,
        )

        # IO stats are time-varying, so only compare field presence
        self._compare_dicts(
            "cluster.iostats",
            self._normalize_cli(cli),
            self._normalize_v1(v1),
            v2_body,
        )

    def _audit_cluster_logs(self):
        self.logger.info("[audit] cluster.logs")
        cid = self.cluster_id
        cli_result = self._run_cli(f"cluster get-logs {cid} --json --limit 5")
        cli = cli_result["data"]
        v1 = self.sbcli_utils.get_cluster_logs(cluster_id=cid)
        v2_status, v2_body = self.v2.get_cluster_logs(cid, limit=5)

        # Just check that all three return lists/data
        cli_norm = self._normalize_cli(cli)
        v1_norm = self._normalize_v1(v1)

        self._check_interface_data(
            "cluster.logs", "cli", cli_norm,
            api_call=cli_result["command"], raw_response=cli_result,
        )
        self._check_interface_data(
            "cluster.logs", "v1", v1_norm,
            api_call=f"GET /api/v1/cluster/logs/{cid}",
        )
        self._check_interface_data(
            "cluster.logs", "v2", v2_body,
            api_call=f"GET /api/v2/clusters/{cid}/logs?limit=5",
            http_status=v2_status,
        )

    def _audit_node_list(self):
        self.logger.info("[audit] node.list")
        cli_result = self._run_cli("sn list --json")
        cli = cli_result["data"]
        v1 = self.sbcli_utils.get_storage_nodes()
        v2_status, v2_body = self.v2.list_nodes()

        self._check_interface_data(
            "node.list", "cli", cli,
            api_call=cli_result["command"], raw_response=cli_result,
        )
        self._check_interface_data(
            "node.list", "v1", self._normalize_v1(v1),
            api_call="GET /api/v1/storagenode",
        )
        self._check_interface_data(
            "node.list", "v2", v2_body,
            api_call=f"GET /api/v2/clusters/{self.cluster_id}/storage-nodes",
            http_status=v2_status,
        )

        self._compare_lists(
            "node.list",
            cli, self._normalize_v1(v1), v2_body,
            id_field="uuid",
            key_fields=["uuid", "hostname", "status", "mgmt_ip"],
        )

    def _audit_node_get(self):
        self.logger.info("[audit] node.get")
        if not self.sn_nodes:
            self._finding("error", "not_tested", "node.get",
                          detail="no storage nodes available")
            return

        node_id = self.sn_nodes[0]
        cli_result = self._run_cli(f"sn get {node_id} --json")
        cli = cli_result["data"]
        v1 = self.sbcli_utils.get_storage_node_details(node_id)
        v2_status, v2_body = self.v2.get_node(node_id)

        self._check_interface_data(
            "node.get", "cli", cli,
            api_call=cli_result["command"], raw_response=cli_result,
        )
        self._check_interface_data(
            "node.get", "v1", self._normalize_v1(v1),
            api_call=f"GET /api/v1/storagenode/{node_id}",
        )
        self._check_interface_data(
            "node.get", "v2", v2_body,
            api_call=f"GET /api/v2/clusters/{self.cluster_id}/storage-nodes/{node_id}",
            http_status=v2_status,
        )

        self._compare_dicts(
            "node.get",
            self._normalize_cli(cli),
            self._normalize_v1(v1),
            v2_body,
            key_fields=["uuid", "hostname", "status"],
            ignore_fields={"updated_at", "created_at"},
        )

    def _audit_node_capacity(self):
        self.logger.info("[audit] node.capacity")
        if not self.sn_nodes:
            self._finding("error", "not_tested", "node.capacity",
                          detail="no storage nodes available")
            return

        node_id = self.sn_nodes[0]
        cli_result = self._run_cli(f"sn get-capacity {node_id} --json")
        cli = cli_result["data"]
        v1 = self.sbcli_utils.get_request(api_url=f"/storagenode/capacity/{node_id}")
        v2_status, v2_body = self.v2.get_node_capacity(node_id)

        self._check_interface_data(
            "node.capacity", "cli", cli,
            api_call=cli_result["command"], raw_response=cli_result,
        )
        self._check_interface_data(
            "node.capacity", "v1", self._normalize_v1(v1),
            api_call=f"GET /api/v1/storagenode/capacity/{node_id}",
        )
        self._check_interface_data(
            "node.capacity", "v2", v2_body,
            api_call=f"GET /api/v2/clusters/{self.cluster_id}/storage-nodes/{node_id}/capacity",
            http_status=v2_status,
        )

        self._compare_dicts(
            "node.capacity",
            self._normalize_cli(cli),
            self._normalize_v1(v1),
            v2_body,
        )

    def _audit_node_ports(self):
        self.logger.info("[audit] node.ports")
        if not self.sn_nodes:
            self._finding("error", "not_tested", "node.ports",
                          detail="no storage nodes available")
            return

        node_id = self.sn_nodes[0]
        cli_result = self._run_cli(f"sn port-list {node_id} --json")
        cli = cli_result["data"]
        v1 = self.sbcli_utils.get_request(api_url=f"/storagenode/port/{node_id}")
        v2_status, v2_body = self.v2.list_node_nics(node_id)

        # Check that all return data
        self._check_interface_data(
            "node.ports", "cli", cli,
            api_call=cli_result["command"], raw_response=cli_result,
        )
        self._check_interface_data(
            "node.ports", "v1", v1,
            api_call=f"GET /api/v1/storagenode/port/{node_id}",
        )
        self._check_interface_data(
            "node.ports", "v2", v2_body,
            api_call=f"GET /api/v2/clusters/{self.cluster_id}/storage-nodes/{node_id}/nics",
            http_status=v2_status,
        )

    def _audit_device_list(self):
        self.logger.info("[audit] device.list")
        if not self.sn_nodes:
            self._finding("error", "not_tested", "device.list",
                          detail="no storage nodes available")
            return

        node_id = self.sn_nodes[0]
        cli_result = self._run_cli(f"sn list-devices {node_id} --json")
        cli = cli_result["data"]
        v1 = self.sbcli_utils.get_device_details(node_id)
        v2_status, v2_body = self.v2.list_devices(node_id)

        self._check_interface_data(
            "device.list", "cli", cli,
            api_call=cli_result["command"], raw_response=cli_result,
        )
        self._check_interface_data(
            "device.list", "v1", self._normalize_v1(v1),
            api_call=f"GET /api/v1/device/{node_id}",
        )
        self._check_interface_data(
            "device.list", "v2", v2_body,
            api_call=f"GET /api/v2/clusters/{self.cluster_id}/storage-nodes/{node_id}/devices",
            http_status=v2_status,
        )

        self._compare_lists(
            "device.list",
            cli, self._normalize_v1(v1), v2_body,
            id_field="uuid",
            key_fields=["uuid", "status"],
        )

    def _audit_device_get(self):
        self.logger.info("[audit] device.get")
        if not self.sn_nodes:
            self._finding("error", "not_tested", "device.get",
                          detail="no storage nodes available")
            return

        node_id = self.sn_nodes[0]
        # Get device list first to find a device id
        v1_devices = self.sbcli_utils.get_device_details(node_id)
        dev_list = self._normalize_v1(v1_devices)
        if not dev_list or not isinstance(dev_list, list) or len(dev_list) == 0:
            self._finding("error", "not_tested", "device.get",
                          detail="no devices on first node")
            return

        device_id = dev_list[0].get("uuid", dev_list[0].get("id"))
        cli_result = self._run_cli(f"sn get-device {device_id} --json")
        cli = cli_result["data"]
        v1 = self.sbcli_utils.get_request(api_url=f"/device/{device_id}")
        v2_status, v2_body = self.v2.get_device(node_id, device_id)

        self._check_interface_data(
            "device.get", "cli", cli,
            api_call=cli_result["command"], raw_response=cli_result,
        )
        self._check_interface_data(
            "device.get", "v1", self._normalize_v1(v1),
            api_call=f"GET /api/v1/device/{device_id}",
        )
        self._check_interface_data(
            "device.get", "v2", v2_body,
            api_call=f"GET /api/v2/clusters/{self.cluster_id}/storage-nodes/{node_id}/devices/{device_id}",
            http_status=v2_status,
        )

        self._compare_dicts(
            "device.get",
            self._normalize_cli(cli),
            self._normalize_v1(v1),
            v2_body,
            key_fields=["uuid", "status"],
            ignore_fields={"updated_at", "created_at"},
        )

    def _audit_pool_list(self):
        self.logger.info("[audit] pool.list")
        cli_result = self._run_cli("pool list --json")
        cli = cli_result["data"]
        v1 = self.sbcli_utils.get_request(api_url="/pool")
        v2_status, v2_body = self.v2.list_pools()

        self._check_interface_data(
            "pool.list", "cli", cli,
            api_call=cli_result["command"], raw_response=cli_result,
        )
        self._check_interface_data(
            "pool.list", "v1", self._normalize_v1(v1),
            api_call="GET /api/v1/pool",
        )
        self._check_interface_data(
            "pool.list", "v2", v2_body,
            api_call=f"GET /api/v2/clusters/{self.cluster_id}/storage-pools",
            http_status=v2_status,
        )

        self._compare_lists(
            "pool.list",
            cli, self._normalize_v1(v1), v2_body,
            id_field="id",
            key_fields=["id", "pool_name", "status"],
        )

    def _audit_pool_get(self):
        self.logger.info("[audit] pool.get")
        pools = self.sbcli_utils.list_storage_pools()
        if not pools:
            self._finding("error", "not_tested", "pool.get",
                          detail="no pools available — pool creation may have failed")
            return

        pool_name = list(pools.keys())[0]
        pool_id = pools[pool_name]

        cli_result = self._run_cli(f"pool get {pool_id} --json")
        cli = cli_result["data"]
        v1 = self.sbcli_utils.get_pool_by_id(pool_id)
        v2_status, v2_body = self.v2.get_pool(pool_id)

        self._check_interface_data(
            "pool.get", "cli", cli,
            api_call=cli_result["command"], raw_response=cli_result,
        )
        self._check_interface_data(
            "pool.get", "v1", self._normalize_v1(v1),
            api_call=f"GET /api/v1/pool/{pool_id}",
        )
        self._check_interface_data(
            "pool.get", "v2", v2_body,
            api_call=f"GET /api/v2/clusters/{self.cluster_id}/storage-pools/{pool_id}",
            http_status=v2_status,
        )

        self._compare_dicts(
            "pool.get",
            self._normalize_cli(cli),
            self._normalize_v1(v1),
            v2_body,
            key_fields=["id", "pool_name", "status", "max_rw_iops"],
            ignore_fields={"updated_at", "created_at", "secret"},
        )

    def _audit_pool_iostats(self):
        self.logger.info("[audit] pool.iostats")
        pools = self.sbcli_utils.list_storage_pools()
        if not pools:
            self._finding("error", "not_tested", "pool.iostats",
                          detail="no pools available — pool creation may have failed")
            return

        pool_id = list(pools.values())[0]
        cli_result = self._run_cli(f"pool get-io-stats {pool_id} --json")
        cli = cli_result["data"]
        v1 = self.sbcli_utils.get_request(api_url=f"/pool/iostats/{pool_id}")
        v2_status, v2_body = self.v2.get_pool_iostats(pool_id)

        # IO stats are time-varying; just check field presence
        self._check_interface_data(
            "pool.iostats", "cli", cli,
            api_call=cli_result["command"], raw_response=cli_result,
        )
        self._check_interface_data(
            "pool.iostats", "v1", v1,
            api_call=f"GET /api/v1/pool/iostats/{pool_id}",
        )
        self._check_interface_data(
            "pool.iostats", "v2", v2_body,
            api_call=f"GET /api/v2/clusters/{self.cluster_id}/storage-pools/{pool_id}/iostats",
            http_status=v2_status,
        )

    def _audit_mgmt_list(self):
        self.logger.info("[audit] mgmt.list")
        cli_result = self._run_cli("cp list --json")
        cli = cli_result["data"]
        v1 = self.sbcli_utils.get_management_nodes()
        v2_status, v2_body = self.v2.list_management_nodes()

        self._check_interface_data(
            "mgmt.list", "cli", cli,
            api_call=cli_result["command"], raw_response=cli_result,
        )
        self._check_interface_data(
            "mgmt.list", "v1", self._normalize_v1(v1),
            api_call="GET /api/v1/mgmtnode",
        )
        self._check_interface_data(
            "mgmt.list", "v2", v2_body,
            api_call="GET /api/v2/management-nodes",
            http_status=v2_status,
        )

        self._compare_lists(
            "mgmt.list",
            cli, self._normalize_v1(v1), v2_body,
            id_field="uuid",
            key_fields=["uuid", "hostname", "status"],
        )

    # ── write audits (create → compare → cleanup) ────────────────────

    def _audit_pool_crud(self):
        self.logger.info("[audit] pool.crud")
        pool_name = "parity_crud_pool"

        # Create via v1
        self.sbcli_utils.add_storage_pool(pool_name)
        sleep_n_sec(5)

        # List via all 3 and check pool appears
        cli_result = self._run_cli("pool list --json")
        cli = cli_result["data"]
        v1 = self.sbcli_utils.get_request(api_url="/pool")
        v2_status, v2_body = self.v2.list_pools()

        cli_norm = self._normalize_cli(cli) or []
        v1_norm = self._normalize_v1(v1) or []
        v2_norm = v2_body if isinstance(v2_body, list) else []

        def _find_pool(lst, name):
            if isinstance(lst, list):
                for p in lst:
                    if isinstance(p, dict) and p.get("pool_name") == name:
                        return p
            return None

        cli_pool = _find_pool(cli_norm, pool_name)
        v1_pool = _find_pool(v1_norm, pool_name)
        v2_pool = _find_pool(v2_norm, pool_name)

        if cli_pool and v1_pool and v2_pool:
            self._compare_dicts(
                "pool.get_after_create",
                cli_pool, v1_pool, v2_pool,
                key_fields=["id", "pool_name", "status"],
                ignore_fields={"updated_at", "created_at", "secret"},
            )
        else:
            for iface, p, api_call in [
                ("cli", cli_pool, cli_result["command"]),
                ("v1", v1_pool, "GET /api/v1/pool"),
                ("v2", v2_pool, f"GET /api/v2/clusters/{self.cluster_id}/storage-pools"),
            ]:
                if p is None:
                    self._finding(
                        "error", "interface_error", "pool.crud",
                        interface=iface,
                        detail=f"pool '{pool_name}' not found after create",
                        api_call=api_call,
                    )

        # Cleanup
        try:
            self.sbcli_utils.delete_storage_pool(pool_name)
            sleep_n_sec(2)
        except Exception as exc:
            self.logger.warning(f"pool cleanup failed: {exc}")

    def _audit_volume_crud(self):
        self.logger.info("[audit] volume.crud")

        # Use the audit pool created at Phase 0
        pools = self.sbcli_utils.list_storage_pools()
        if not pools:
            self._finding("error", "not_tested", "volume.crud",
                          detail="no pools available — pool creation failed")
            return

        pool_name = list(pools.keys())[0]
        pool_id = pools[pool_name]
        vol_name = "parity_audit_vol"

        # Create volume via v1
        self.sbcli_utils.add_lvol(
            lvol_name=vol_name,
            pool_name=pool_name,
            size="1G",
            retry=3,
        )
        sleep_n_sec(5)

        # Get volume id
        vol_id = self.sbcli_utils.get_lvol_id(vol_name)
        if not vol_id:
            self._finding("error", "interface_error", "volume.crud",
                          interface="v1",
                          detail=f"volume '{vol_name}' not found after create",
                          api_call=f"POST /api/v1/lvol (name={vol_name}, pool={pool_name}, size=1G)")
            return

        # Get via all 3
        cli_result = self._run_cli(f"lvol get {vol_id} --json")
        cli = cli_result["data"]
        v1 = self.sbcli_utils.get_lvol_details(vol_id)
        v2_status, v2_body = self.v2.get_volume(pool_id, vol_id)

        self._check_interface_data(
            "volume.get", "cli", cli,
            api_call=cli_result["command"], raw_response=cli_result,
        )
        self._check_interface_data(
            "volume.get", "v1", self._normalize_v1(v1),
            api_call=f"GET /api/v1/lvol/{vol_id}",
        )
        self._check_interface_data(
            "volume.get", "v2", v2_body,
            api_call=f"GET /api/v2/clusters/{self.cluster_id}/storage-pools/{pool_id}/volumes/{vol_id}",
            http_status=v2_status,
        )

        self._compare_dicts(
            "volume.get",
            self._normalize_cli(cli),
            self._normalize_v1(v1),
            v2_body,
            key_fields=["id", "lvol_name", "size", "status", "pool_name"],
            ignore_fields={"updated_at", "created_at", "nqn"},
        )

        # List volumes via v2 and check count
        v2_list_status, v2_list_body = self.v2.list_volumes(pool_id)
        v1_list = self.sbcli_utils.list_lvols()
        cli_list_result = self._run_cli("lvol list --json")
        cli_list = cli_list_result["data"]

        # Just check the volume appears in all three
        def _has_vol(data, name):
            if isinstance(data, dict):
                return name in data
            if isinstance(data, list):
                return any(
                    isinstance(v, dict) and v.get("lvol_name") == name
                    for v in data
                )
            return False

        for iface, data, api_call in [
            ("cli", cli_list, cli_list_result["command"]),
            ("v1", v1_list, "GET /api/v1/lvol"),
            ("v2", v2_list_body,
             f"GET /api/v2/clusters/{self.cluster_id}/storage-pools/{pool_id}/volumes"),
        ]:
            if not _has_vol(data, vol_name):
                self._finding("error", "interface_error", "volume.list_after_create",
                              interface=iface,
                              detail=f"volume '{vol_name}' not in list",
                              api_call=api_call)

        # Cleanup
        try:
            self.sbcli_utils.delete_lvol(vol_name)
            sleep_n_sec(3)
        except Exception as exc:
            self.logger.warning(f"volume cleanup failed: {exc}")

    def _audit_snapshot_crud(self):
        self.logger.info("[audit] snapshot.crud")

        # Need a volume to snapshot
        pools = self.sbcli_utils.list_storage_pools()
        if not pools:
            self._finding("error", "not_tested", "snapshot.crud",
                          detail="no pools available — pool creation failed")
            return

        pool_name = list(pools.keys())[0]
        pool_id = pools[pool_name]
        vol_name = "parity_audit_snap_vol"
        snap_name = "parity_audit_snap"

        # Create volume
        self.sbcli_utils.add_lvol(
            lvol_name=vol_name, pool_name=pool_name, size="1G", retry=3,
        )
        sleep_n_sec(5)
        vol_id = self.sbcli_utils.get_lvol_id(vol_name)
        if not vol_id:
            self._finding("error", "interface_error", "snapshot.crud",
                          interface="v1",
                          detail=f"could not create volume '{vol_name}'",
                          api_call=f"POST /api/v1/lvol (name={vol_name}, pool={pool_name}, size=1G)")
            return

        # Create snapshot via v1
        self.sbcli_utils.add_snapshot(lvol_id=vol_id, snapshot_name=snap_name)
        sleep_n_sec(5)

        # List snapshots via all 3
        cli_result = self._run_cli("snapshot list --json")
        cli = cli_result["data"]
        v1 = self.sbcli_utils.list_snapshots()
        v2_status, v2_body = self.v2.list_snapshots(pool_id)

        # Check snapshot appears
        def _has_snap(data, name):
            if isinstance(data, dict):
                return name in data
            if isinstance(data, list):
                return any(
                    isinstance(s, dict) and s.get("snap_name") == name
                    for s in data
                )
            return False

        for iface, data, api_call in [
            ("cli", cli, cli_result["command"]),
            ("v1", v1, "GET /api/v1/snapshot"),
            ("v2", v2_body,
             f"GET /api/v2/clusters/{self.cluster_id}/storage-pools/{pool_id}/snapshots"),
        ]:
            if not _has_snap(data, snap_name):
                self._finding("error", "interface_error", "snapshot.list_after_create",
                              interface=iface,
                              detail=f"snapshot '{snap_name}' not in list",
                              api_call=api_call)

        # Cleanup
        try:
            self.sbcli_utils.delete_all_snapshots()
            sleep_n_sec(3)
            self.sbcli_utils.delete_lvol(vol_name)
            sleep_n_sec(3)
        except Exception as exc:
            self.logger.warning(f"snapshot cleanup failed: {exc}")
