### simplyblock e2e tests
import argparse
import json
import os
import shutil
import subprocess
import time
import traceback
from __init__ import get_all_tests, get_security_tests, get_backup_tests, get_backup_stress_tests, ALL_TESTS
from logger_config import setup_logger
from exceptions.custom_exception import (
    TestNotFoundException,
    MultipleExceptions,
    SkippedTestsException
)
from e2e_tests.cluster_test_base import TestClusterBase
from utils.sbcli_utils import SbcliUtils
from utils.ssh_utils import SshUtils
from utils.common_utils import CommonUtils

from utils.manage_portal_util import (
    TestRunsAPI,
    detect_fe_be_tags,
    FAILURE_REASON_OTHER,
    resolve_environment_id_from_ip
)


PROFILE_KEY = "e2e"         # fixed
JIRA_TICKET = ""            # always empty, per your note
COMPLETION_COMMENT = "E2E run"

# Tests that modify cluster topology (add/remove/migrate nodes).
# When running multiple of these sequentially, a full cluster reset
# (cleanup + re-bootstrap) is performed between them so each test
# starts with a fresh cluster and clean spare nodes.
TOPOLOGY_MODIFYING_TESTS = {
    "TestAddNodesDuringFioRun",
    "TestSequentialNodeAdd",
    "TestAddNodeSnapshotCloneOnNewNode",
    "TestBackupAfterNodeAdd",
    "TestBackupWithFioOnNewNode",
    "TestAddK8sNodesDuringFioRun",
    "K8sNativeAddNodeTest",
    "K8sNativeNodeMigrationTest",
    "TestBackupAfterNodeMigration",
    "TestBackupDuringMigration",
}

def main():
    """Run complete test suite"""
    parser = argparse.ArgumentParser(description="Run simplyBlock's E2E Test Framework")
    parser.add_argument('--testname', type=str, help="The name of the test to run", default=None)
    parser.add_argument('--fio_debug', type=bool, help="Add debug flag to fio", default=False)
    
    # New arguments for ndcs, npcs, bs, chunk_bs with default values
    parser.add_argument('--ndcs', type=int, help="Number of data chunks (ndcs)", default=2)
    parser.add_argument('--npcs', type=int, help="Number of parity chunks (npcs)", default=1)
    parser.add_argument('--bs', type=int, help="Block size (bs)", default=4096)
    parser.add_argument('--chunk_bs', type=int, help="Chunk block size (chunk_bs)", default=4096)
    parser.add_argument('--run_k8s', type=bool, help="Run K8s tests", default=False)
    parser.add_argument('--run_ha', type=bool, help="Run HA tests", default=False)
    parser.add_argument('--send_debug_notification', type=bool, help="Send notification for debug", default=False)
    parser.add_argument('--new_nodes', type=str, help="New nodes to add (space-separated)", default="")
    parser.add_argument('--k3s_mnode', type=str, help="K8s master node", default="")
    parser.add_argument('--namespace', type=str, help="Kubernetes namespace", default="")
    parser.add_argument('--new_worker_nodes', type=str, help="New K8s worker node names to add (comma-separated)", default="")
    parser.add_argument('--migrate_to_worker', type=str, help="K8s worker node name to migrate a storage node onto", default="")
    parser.add_argument('--new_ssd_pcie', type=str, help="Comma-separated PCIe addresses for new SSDs on the target worker", default="")
    parser.add_argument('--reattach_volume', type=str, help="Reattach volumes after migration (True/False)", default="")
    parser.add_argument('--preserve_resources_on_failure', type=bool,
                        help="Skip K8s resource cleanup when test fails (preserve PVCs/pods for debugging)",
                        default=False)

    # Bootstrap parameters for inter-test cluster reset.
    # Defaults fall back to environment variables set by the CI workflow.
    parser.add_argument('--storage_ips', type=str,
                        help="Original storage node IPs (space-separated). Required for inter-test reset.",
                        default=os.environ.get("STORAGE_PRIVATE_IPS", ""))
    parser.add_argument('--mgmt_ip', type=str,
                        help="Management node IP. Required for inter-test reset.",
                        default=(os.environ.get("MNODES", "").split()[0]
                                 if os.environ.get("MNODES", "").strip() else ""))
    parser.add_argument('--client_ips', type=str,
                        help="Client node IPs (space-separated).",
                        default=os.environ.get("CLIENT_IP", ""))
    parser.add_argument('--ha_jm_count', type=int,
                        help="HA journal manager count for bootstrap.",
                        default=int(os.environ.get("BOOTSTRAP_HA_JM_COUNT", "3")))
    parser.add_argument('--ha_type', type=str,
                        help="HA type for bootstrap.",
                        default=os.environ.get("HA_TYPE", "ha"))
    parser.add_argument('--journal_partition', type=int,
                        help="Journal partition count for bootstrap.",
                        default=int(os.environ.get("BOOTSTRAP_JOURNAL_PARTITION", "0")))
    parser.add_argument('--max_subsys', type=int,
                        help="Max subsystems per storage node.",
                        default=int(os.environ.get("BOOTSTRAP_MAX_SUBSYS", "1024")))
    parser.add_argument('--data_nic', type=str,
                        help="Data NIC interface name.",
                        default=os.environ.get("BOOTSTRAP_DATA_NIC", "eth1"))
    parser.add_argument('--ifname', type=str,
                        help="Management interface name.",
                        default="eth0")
    parser.add_argument('--sbcli_branch', type=str,
                        help="sbcli git branch for install on nodes.",
                        default=os.environ.get("SBCLI_BRANCH", "main"))
    parser.add_argument('--spdk_image', type=str,
                        help="SPDK image override for bootstrap.",
                        default=os.environ.get("SPDK_IMAGE", ""))
    parser.add_argument('--helm_chart_path', type=str,
                        help="Path to simplyblock helm chart directory "
                             "(auto-detected from GITHUB_WORKSPACE if not set).",
                        default="")

    args = parser.parse_args()

    if args.ndcs == 0 and args.npcs == 0:
        tests = get_all_tests(custom=False, ha_test=args.run_ha)
    else:
        tests = get_all_tests(custom=True, ha_test=args.run_ha)

    test_class_run = []
    new_nodes = args.new_nodes.strip().split() if args.new_nodes else []
    new_worker_nodes = [n.strip() for n in args.new_worker_nodes.split(",") if n.strip()] if args.new_worker_nodes else []
    skipped_cases = 0

    # group keywords — run a named category of tests
    if args.testname and args.testname.strip().lower() == "security":
        test_class_run = get_security_tests()
    elif args.testname and args.testname.strip().lower() == "backup":
        test_class_run = get_backup_tests()
    elif args.testname and args.testname.strip().lower() == "backup-stress":
        test_class_run = get_backup_stress_tests()
    elif args.testname is None or len(args.testname.strip()) == 0:
        for cls in tests:
            if cls.__name__ == "TestAddNodesDuringFioRun":
                if len(new_nodes) == 0:
                    logger.warning("Skipping TestAddNodesDuringFioRun: requires --new-nodes with at least 1 IP.")
                    skipped_cases += 1
                    continue
            if cls.__name__ == "TestRestartNodeOnAnotherHost":
                if len(new_nodes) == 0:
                    logger.warning("Skipping TestRestartNodeOnAnotherHost: requires --new-nodes with atleast 1 IP.")
                    skipped_cases += 1
                    continue
            if cls.__name__ == "TestAddK8sNodesDuringFioRun":
                if not args.run_k8s:
                    continue
                if len(new_nodes) == 0:
                    logger.warning("Skipping TestAddK8sNodesDuringFioRun: requires --new-nodes with at least 1 IP.")
                    skipped_cases += 1
                    continue
            if cls.__name__ == "K8sNativeAddNodeTest":
                if not args.run_k8s:
                    continue
                if len(new_worker_nodes) == 0:
                    logger.warning("Skipping K8sNativeAddNodeTest: requires --new_worker_nodes with at least 1 node name.")
                    skipped_cases += 1
                    continue
            if cls.__name__ == "K8sNativeNodeMigrationTest":
                if not args.run_k8s:
                    continue
                if not args.migrate_to_worker.strip():
                    logger.warning("Skipping K8sNativeNodeMigrationTest: requires --migrate_to_worker with a K8s worker node name.")
                    skipped_cases += 1
                    continue
            if cls.__name__ == "TestSequentialNodeAdd":
                if len(new_nodes) < 2 and len(new_worker_nodes) < 2:
                    logger.warning("Skipping TestSequentialNodeAdd: requires --new_nodes with at least 2 IPs or --new_worker_nodes with at least 2 node names.")
                    skipped_cases += 1
                    continue
            if cls.__name__ == "TestAddNodeSnapshotCloneOnNewNode":
                if len(new_nodes) == 0 and len(new_worker_nodes) == 0:
                    logger.warning("Skipping TestAddNodeSnapshotCloneOnNewNode: requires --new_nodes or --new_worker_nodes with at least 1 entry.")
                    skipped_cases += 1
                    continue
            if cls.__name__ in ("TestBackupAfterNodeAdd", "TestBackupWithFioOnNewNode"):
                if len(new_nodes) == 0 and len(new_worker_nodes) == 0:
                    logger.warning(f"Skipping {cls.__name__}: requires --new_nodes or --new_worker_nodes with at least 1 entry.")
                    skipped_cases += 1
                    continue
            if cls.__name__ in ("TestBackupAfterNodeMigration", "TestBackupDuringMigration"):
                if args.run_k8s and not args.migrate_to_worker.strip():
                    logger.warning(f"Skipping {cls.__name__}: K8s mode requires --migrate_to_worker.")
                    skipped_cases += 1
                    continue

            test_class_run.append(cls)
    else:
        needles = [n.strip().lower().replace("_", "") for n in args.testname.split(",") if n.strip()]
        seen = set()
        for needle in needles:
            for cls in ALL_TESTS:
                if needle in cls.__name__.lower().replace("_", "") and cls not in seen:
                    if cls.__name__ == "TestAddNodesDuringFioRun" and len(new_nodes) == 0:
                        raise ValueError("TestAddNodesDuringFioRun requires --new-nodes with at least 1 IP.")
                    if cls.__name__ == "TestRestartNodeOnAnotherHost" and len(new_nodes) == 0:
                        raise ValueError("TestRestartNodeOnAnotherHost requires --new-nodes with atleast 1 new IP.")
                    if cls.__name__ == "TestAddK8sNodesDuringFioRun" and len(new_nodes) == 0:
                        if not args.run_k8s:
                            continue
                        raise ValueError("TestAddK8sNodesDuringFioRun requires --new-nodes with at least 1 IP.")
                    if cls.__name__ == "K8sNativeAddNodeTest":
                        if not args.run_k8s:
                            continue
                        if len(new_worker_nodes) == 0:
                            raise ValueError("K8sNativeAddNodeTest requires --new_worker_nodes with at least 1 node name.")
                    if cls.__name__ == "K8sNativeNodeMigrationTest":
                        if not args.run_k8s:
                            continue
                        if not args.migrate_to_worker.strip():
                            raise ValueError("K8sNativeNodeMigrationTest requires --migrate_to_worker with a K8s worker node name.")
                    if cls.__name__ == "TestSequentialNodeAdd":
                        if len(new_nodes) < 2 and len(new_worker_nodes) < 2:
                            raise ValueError("TestSequentialNodeAdd requires --new_nodes with at least 2 IPs or --new_worker_nodes with at least 2 node names.")
                    if cls.__name__ == "TestAddNodeSnapshotCloneOnNewNode":
                        if len(new_nodes) == 0 and len(new_worker_nodes) == 0:
                            raise ValueError("TestAddNodeSnapshotCloneOnNewNode requires --new_nodes or --new_worker_nodes with at least 1 entry.")
                    if cls.__name__ in ("TestBackupAfterNodeAdd", "TestBackupWithFioOnNewNode"):
                        if len(new_nodes) == 0 and len(new_worker_nodes) == 0:
                            raise ValueError(f"{cls.__name__} requires --new_nodes or --new_worker_nodes with at least 1 entry.")
                    if cls.__name__ in ("TestBackupAfterNodeMigration", "TestBackupDuringMigration"):
                        if args.run_k8s and not args.migrate_to_worker.strip():
                            raise ValueError(f"{cls.__name__} requires --migrate_to_worker in K8s mode.")
                    test_class_run.append(cls)
                    seen.add(cls)

    if not test_class_run:
        available_tests = ', '.join(cls.__name__ for cls in tests)
        print(f"Test '{args.testname}' not found. Available tests are: {available_tests}")
        raise TestNotFoundException(args.testname, available_tests)
    
    test_run_api = TestRunsAPI(PROFILE_KEY)
    try:
        cluster_base = TestClusterBase()
        ssh_obj = SshUtils(bastion_server=cluster_base.bastion_server)
        sbcli_utils = SbcliUtils(
            cluster_api_url=cluster_base.api_base_url,
            cluster_id=cluster_base.cluster_id,
            cluster_secret=cluster_base.cluster_secret
        )

        mgmt_nodes, storage_node = sbcli_utils.get_all_nodes_ip()
        mgmt_ip_for_env = mgmt_nodes[0]
        environment_id = resolve_environment_id_from_ip(mgmt_ip_for_env)
        if not environment_id:
            raise RuntimeError(f"Could not resolve environment for mgmt IP {mgmt_ip_for_env}")
        ssh_obj.connect(address=storage_node[0], bastion_server_address=cluster_base.bastion_server)

        fe_branch, fe_commit, be_branch, be_commit = detect_fe_be_tags(ssh_obj, storage_node[0])

        test_run_id = test_run_api.create_run(
            jira_ticket=JIRA_TICKET,
            github_branch_frontend=fe_branch or "unknown",
            github_branch_backend=be_branch or "unknown",
            github_commit_tag_frontend=fe_commit or "unknown",
            github_commit_tag_backend=be_commit or "unknown",
            environment_id=environment_id
        )
        logger.info(f"Test Run started: {test_run_id}")

        # Close the temp SSH connection used for tag detection
        for node, ssh in ssh_obj.ssh_connections.items():
            logger.info(f"Closing temp ssh connection for FE/BE detection: {node}")
            ssh.close()

    except Exception as e:
        logger.error("Failed to create Test Run; proceeding without external tracking.")
        logger.error(e)
        test_run_id = None

    errors = {}
    passed_cases = []
    for i, test in enumerate(test_class_run):
        logger.info(f"Running Test {test}")
        test_obj = test(fio_debug=args.fio_debug,
                        ndcs=args.ndcs,
                        npcs=args.npcs,
                        bs=args.bs,
                        chunk_bs=args.chunk_bs,
                        k8s_run=args.run_k8s,
                        new_nodes=new_nodes,
                        k3s_mnode=args.k3s_mnode,
                        namespace=args.namespace,
                        new_worker_nodes=new_worker_nodes,
                        migrate_to_worker=args.migrate_to_worker,
                        new_ssd_pcie=args.new_ssd_pcie,
                        reattach_volume=args.reattach_volume,
                        preserve_resources_on_failure=args.preserve_resources_on_failure,
                        )
        try:
            test_obj.setup()
            if i == 0:
                test_obj.cleanup_logs()
                test_obj.configure_sysctl_settings()
            test_obj.run()
            passed_cases.append(f"{test.__name__}")
        except Exception as exp:
            tb = traceback.format_exc()
            logger.error(tb)
            errors[f"{test.__name__}"] = [exp, tb]
        _test_failed = f"{test.__name__}" in errors
        _skip_k8s = _test_failed and test_obj.preserve_resources_on_failure
        if _skip_k8s:
            logger.info(f"[cleanup] Test {test.__name__} failed — preserving K8s resources for debugging (--preserve_resources_on_failure)")
        _is_bulk_run = len(test_class_run) > 1
        try:
            test_obj.collect_management_details(post_teardown=False)
            test_obj.teardown(delete_lvols=False, close_ssh=False, skip_k8s_cleanup=_skip_k8s)
            if not args.run_k8s:
                test_obj.stop_docker_logs_collect()
            else:
                test_obj.stop_k8s_log_collect()
            if _test_failed:
                test_obj.fetch_all_nodes_distrib_log()
            else:
                logger.info(f"[perf] Skipping distrib dump for passed test {test.__name__}")
            test_obj.collect_management_details(post_teardown=True)
            test_obj.teardown(delete_lvols=not _skip_k8s, close_ssh=False, skip_k8s_cleanup=_skip_k8s)
            if not args.run_k8s:
                all_nodes = test_obj._get_all_nodes()
                test_obj.ssh_obj.collect_final_docker_logs_simple(all_nodes, test_obj.docker_logs_path)
            if _is_bulk_run:
                logger.info(f"[perf] Skipping per-test Graylog export in bulk run ({len(test_class_run)} tests)")
            else:
                test_obj.export_graylog_logs()
            test_obj.extract_delay_qpair_logs()
            test_obj.teardown(delete_lvols=False, close_ssh=True)
            # pass
        except Exception as _:
            logger.error(f"Error During Teardown for test: {test.__name__}")
            logger.error(traceback.format_exc())
        finally:
            # Copy e2e/logs/ folder to NFS so automation logs are accessible post-run
            log_path = getattr(test_obj, "docker_logs_path", "")
            if log_path:
                logs_src = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
                if os.path.isdir(logs_src):
                    logs_dest = os.path.join(log_path, "automation_logs")
                    try:
                        # Flush all log handlers so buffered data is written
                        # to disk before copying — prevents 0-byte log files.
                        from logger_config import flush_all_log_handlers
                        flush_all_log_handlers()

                        shutil.copytree(logs_src, logs_dest, dirs_exist_ok=True)
                        logger.info(f"Automation logs copied to: {logs_dest}")
                        # Do NOT remove local log files here — RotatingFileHandlers
                        # hold open FDs. Deleting the directory entry causes
                        # subsequent tests to log into an unreachable inode,
                        # producing empty logs for tests 2+. Runner disk cleanup
                        # is handled by the CI workflow.
                    except Exception as _copy_err:
                        logger.warning(f"Failed to copy automation logs to NFS: {_copy_err}")
            # Copy the tee'd output.log to the test's NFS folder for
            # easy access to full raw stdout/stderr per test.
            if log_path:
                output_log = os.path.join(
                    os.path.dirname(os.path.abspath(__file__)), "output.log"
                )
                if os.path.isfile(output_log):
                    try:
                        shutil.copy2(
                            output_log,
                            os.path.join(log_path, "github_raw_output.log"),
                        )
                    except Exception:
                        pass
            if not args.run_k8s and check_for_dumps():
                # If a full reset is about to happen, core dumps from the
                # current test won't affect the fresh cluster.
                _next_idx = i + 1
                _reset_coming = (
                    _next_idx < len(test_class_run)
                    and test.__name__ in TOPOLOGY_MODIFYING_TESTS
                    and test_class_run[_next_idx].__name__ in TOPOLOGY_MODIFYING_TESTS
                )
                if _reset_coming:
                    logger.info(
                        "Core dump found, but inter-test cluster reset will "
                        "re-bootstrap a fresh cluster. Continuing."
                    )
                else:
                    logger.info(
                        "Found a core dump during test execution. "
                        "Cannot execute more tests as cluster is not stable. Exiting"
                    )
                    break
            test_obj.get_logs_path()

            # ── Inter-test cluster reset ──────────────────────────────
            # When two consecutive topology-modifying tests are queued,
            # perform a full cluster teardown + cleanup + re-bootstrap
            # so the next test starts with a clean cluster and fresh
            # spare nodes.
            _next_idx = i + 1
            if (
                _next_idx < len(test_class_run)
                and test.__name__ in TOPOLOGY_MODIFYING_TESTS
                and test_class_run[_next_idx].__name__ in TOPOLOGY_MODIFYING_TESTS
            ):
                logger.info(
                    f"[reset] Topology-modifying test {test.__name__} completed. "
                    f"Running inter-test cluster reset before "
                    f"{test_class_run[_next_idx].__name__}..."
                )
                try:
                    new_cluster_id, new_secret = inter_test_cluster_reset(
                        args=args,
                        is_k8s=args.run_k8s,
                        new_nodes=new_nodes,
                        new_worker_nodes=new_worker_nodes,
                        logger=logger,
                    )
                    os.environ["CLUSTER_ID"] = new_cluster_id
                    os.environ["CLUSTER_SECRET"] = new_secret
                    os.environ["API_BASE_URL"] = f"http://{args.mgmt_ip}"
                    logger.info(
                        f"[reset] Cluster reset complete. "
                        f"New cluster_id={new_cluster_id}"
                    )
                except Exception as reset_err:
                    logger.error(
                        f"[reset] Inter-test cluster reset failed: {reset_err}"
                    )
                    logger.error(traceback.format_exc())
                    errors["inter_test_reset"] = [
                        reset_err, traceback.format_exc()
                    ]
                    break

    failed_cases = list(errors.keys())
    skipped_cases += len(test_class_run) - (len(passed_cases) + len(failed_cases))

    logger.info(f"Number of Total Cases: {len(test_class_run)}")
    logger.info(f"Number of Passed Cases: {len(passed_cases)}")
    logger.info(f"Number of Failed Cases: {len(failed_cases)}")
    logger.info(f"Number of Skipped Cases: {skipped_cases}")

    summary = f"""
        *Total Test Cases:* {len(test_class_run)}
        *Passed Cases:* {len(passed_cases)}
        *Failed Cases:* {len(failed_cases)}
        *Skipped Cases:* {skipped_cases}

        *Test Wise Run Status:*
    """

    logger.info("Test Wise run status:")
    for test in test_class_run:
        if test.__name__ in passed_cases:
            logger.info(f"{test.__name__} PASSED CASE.")
            summary += f"✅ {test.__name__}: *PASSED*\n"
        elif test.__name__ in failed_cases:
            logger.info(f"{test.__name__} FAILED CASE.")
            summary += f"❌ {test.__name__}: *FAILED*\n"
        else:
            logger.info(f"{test.__name__} SKIPPED CASE.")
            summary += f"⚠️ {test.__name__}: *SKIPPED*\n"
    
    if args.send_debug_notification:
        # Send Slack notification
        cluster_base = TestClusterBase()
        ssh_obj = SshUtils(bastion_server=cluster_base.bastion_server)
        sbcli_utils = SbcliUtils(
            cluster_api_url=cluster_base.api_base_url,
            cluster_id=cluster_base.cluster_id,
            cluster_secret=cluster_base.cluster_secret
        )
        common_utils = CommonUtils(sbcli_utils, ssh_obj)
        common_utils.send_slack_summary("E2E Test Summary Report", summary)

    final_status = "completed" if not errors else "failed"
    failure_reason_id = FAILURE_REASON_OTHER if final_status == "failed" else None

    if test_run_id:
        try:
            test_run_api.complete_run(
                status=final_status,
                completion_comment=summary,
                completion_jira_ticket=JIRA_TICKET,
                failure_reason_id=failure_reason_id,
                errors=errors
            )
            logger.info(f"Test Run marked {final_status}.")
        except Exception as e:
            logger.error(f"Failed to update Test Run status: {e}")

    if errors:
        exc = MultipleExceptions(errors)
        logger.error(f"MultipleExceptions: {exc}")
        raise exc
    if skipped_cases:
        raise SkippedTestsException("There are SKIPPED Tests. Please check!!")
    
def inter_test_cluster_reset(args, is_k8s, new_nodes, new_worker_nodes, logger):
    """Full cluster teardown + cleanup + re-bootstrap between topology-modifying tests.

    Returns:
        tuple: (cluster_id, cluster_secret) of the newly bootstrapped cluster.
    """
    if is_k8s:
        return _k8s_cluster_reset(args, new_worker_nodes, logger)
    else:
        return _docker_cluster_reset(args, new_nodes, logger)


# ── Docker cluster reset ──────────────────────────────────────────────


def _ssh_exec(ssh_obj, node, cmd, logger, ignore_errors=True):
    """Run a command on a remote node, optionally ignoring errors."""
    try:
        return ssh_obj.exec_command(node=node, command=cmd)
    except Exception as e:
        if ignore_errors:
            logger.warning(f"[reset] Command failed on {node} (ignored): {e}")
            return ""
        raise


def _wait_node_online(ssh_obj, node, logger, timeout=600):
    """Wait for a node to respond to SSH after reboot."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            result = ssh_obj.exec_command(node=node, command="echo online")
            if "online" in str(result):
                logger.info(f"[reset] Node {node} is back online.")
                return True
        except Exception:
            pass
        time.sleep(10)
    raise RuntimeError(f"Node {node} did not come online within {timeout}s")


def _docker_cluster_reset(args, new_nodes, logger):
    """Full Docker cluster cleanup + re-bootstrap.

    Mirrors the flow in bootstrap-cluster.sh:
        cleanup_and_reboot → install_sbcli_on_node → bootstrap_cluster →
        add_storage_nodes → add_pool

    Returns:
        tuple: (cluster_id, cluster_secret)
    """
    storage_ips = args.storage_ips.strip().split() if args.storage_ips else []
    mgmt_ip = args.mgmt_ip.strip()
    client_ips = args.client_ips.strip().split() if args.client_ips else []

    if not storage_ips or not mgmt_ip:
        raise RuntimeError(
            "[reset] Cannot perform inter-test reset: --storage_ips and "
            "--mgmt_ip are required (or set STORAGE_PRIVATE_IPS / MNODES "
            "environment variables)."
        )

    bastion = os.environ.get("BASTION_SERVER", "")
    ssh_obj = SshUtils(bastion_server=bastion)

    all_storage = list(dict.fromkeys(storage_ips + new_nodes))  # deduplicated, order-preserving
    all_nodes = list(dict.fromkeys([mgmt_ip] + all_storage + client_ips))

    # Connect SSH to every node
    logger.info(f"[reset] Connecting SSH to {len(all_nodes)} node(s)...")
    for node in all_nodes:
        try:
            ssh_obj.connect(address=node, bastion_server_address=bastion)
        except Exception as e:
            logger.warning(f"[reset] SSH connect to {node} failed: {e}")

    # ── Phase 1: Cleanup ──────────────────────────────────────────────
    logger.info("[reset] Phase 1: Cleaning up all nodes...")

    # Kill processes on all nodes
    for node in all_nodes:
        _ssh_exec(ssh_obj, node, "pkill -9 fio || true ; pkill -9 tmux || true", logger)

    # Disconnect NVMe on clients
    for node in client_ips:
        _ssh_exec(
            ssh_obj, node,
            "for s in $(nvme list-subsys 2>/dev/null | grep -i lvol "
            "| awk '{print $3}'); do nvme disconnect -n $s || true; done ; "
            "umount /mnt/test_location* 2>/dev/null || true ; "
            "umount /mnt/* 2>/dev/null || true",
            logger,
        )

    # Deploy-cleaner + Docker cleanup on storage + mgmt nodes
    cleanup_cmd = (
        "sbcli sn deploy-cleaner 2>/dev/null || sbctl sn deploy-cleaner 2>/dev/null || true ; "
        "docker stop $(docker ps -aq) 2>/dev/null || true ; "
        "docker rm -f $(docker ps -aq) 2>/dev/null || true ; "
        "docker system prune -af 2>/dev/null || true ; "
        "docker volume prune -f 2>/dev/null || true ; "
        "rm -rf /etc/simplyblock"
    )
    for node in [mgmt_ip] + all_storage:
        logger.info(f"[reset]   Cleaning {node}...")
        _ssh_exec(ssh_obj, node, cleanup_cmd, logger)

    # ── Phase 2: Reboot storage nodes + disk reset ────────────────────
    logger.info(f"[reset] Phase 2: Rebooting {len(all_storage)} storage node(s)...")
    for node in all_storage:
        _ssh_exec(ssh_obj, node, "reboot || true", logger)

    # Wait for storage nodes to come back
    time.sleep(30)  # Give nodes time to start rebooting
    for node in all_storage:
        # Reconnect SSH after reboot
        try:
            ssh_obj.connect(address=node, bastion_server_address=bastion)
        except Exception:
            pass
        _wait_node_online(ssh_obj, node, logger, timeout=600)

    # Disk reset
    logger.info("[reset] Resetting disks on storage nodes...")
    disk_reset_cmd = (
        "for d in /dev/nvme*n1; do "
        "parted -s $d rm 1 2>/dev/null || true ; "
        "parted -s $d rm 2 2>/dev/null || true ; "
        "parted -s $d rm 3 2>/dev/null || true ; "
        "parted -s $d mklabel gpt 2>/dev/null || true ; "
        "done"
    )
    for node in all_storage:
        _ssh_exec(ssh_obj, node, disk_reset_cmd, logger)

    # ── Phase 3: Re-bootstrap ─────────────────────────────────────────
    logger.info("[reset] Phase 3: Re-bootstrapping cluster...")

    sbcli_cmd = "sbctl"
    branch = args.sbcli_branch or "main"
    install_cmd = (
        f"pip install --force-reinstall "
        f"git+https://github.com/simplyblock-io/sbcli.git@{branch}"
    )

    # Install sbcli on mgmt node
    logger.info(f"[reset]   Installing sbcli on mgmt {mgmt_ip}...")
    _ssh_exec(ssh_obj, mgmt_ip, install_cmd, logger, ignore_errors=False)

    # Install sbcli + configure on storage nodes (sequential to avoid races)
    for node in storage_ips:
        logger.info(f"[reset]   Installing sbcli + configuring {node}...")
        _ssh_exec(ssh_obj, node, install_cmd, logger, ignore_errors=False)
        time.sleep(5)
        configure_cmd = f"{sbcli_cmd} --dev -d sn configure --max-subsys {args.max_subsys}"
        _ssh_exec(ssh_obj, node, configure_cmd, logger, ignore_errors=False)
        deploy_cmd = f"{sbcli_cmd} sn deploy --ifname {args.ifname}"
        _ssh_exec(ssh_obj, node, deploy_cmd, logger, ignore_errors=False)

    # Wait for SPDK containers to start on storage nodes
    logger.info("[reset]   Waiting for SPDK containers to start...")
    time.sleep(30)

    # Create cluster on mgmt node
    logger.info("[reset]   Creating cluster...")
    create_cmd = (
        f"{sbcli_cmd} sn deploy-cleaner ; "
        f"{sbcli_cmd} --dev -d cluster create"
        f" --ha-type {args.ha_type}"
        f" --data-chunks-per-stripe {args.ndcs}"
        f" --parity-chunks-per-stripe {args.npcs}"
        f" --ifname {args.ifname}"
    )
    _ssh_exec(ssh_obj, mgmt_ip, create_cmd, logger, ignore_errors=False)

    # Extract cluster_id
    result = _ssh_exec(
        ssh_obj, mgmt_ip,
        f"{sbcli_cmd} cluster list | grep simplyblock | awk '{{print $2}}'",
        logger, ignore_errors=False,
    )
    cluster_id = str(result).strip().split("\n")[0].strip()
    if not cluster_id:
        raise RuntimeError("[reset] Failed to extract cluster_id after cluster create")
    logger.info(f"[reset]   Cluster created: {cluster_id}")

    # Add storage nodes
    add_cmd_base = (
        f"{sbcli_cmd} --dev -d storage-node add-node"
        f" --journal-partition {args.journal_partition}"
        f" --ha-jm-count {args.ha_jm_count}"
        f" --data-nics {args.data_nic}"
    )
    if args.spdk_image:
        add_cmd_base += f" --spdk-image {args.spdk_image}"

    for node in storage_ips:
        logger.info(f"[reset]   Adding storage node {node}...")
        add_cmd = f"{add_cmd_base} {cluster_id} {node}:5000 {args.ifname}"
        _ssh_exec(ssh_obj, mgmt_ip, add_cmd, logger, ignore_errors=False)
        time.sleep(3)

    # Activate cluster
    logger.info("[reset]   Activating cluster...")
    _ssh_exec(
        ssh_obj, mgmt_ip,
        f"{sbcli_cmd} -d cluster activate {cluster_id}",
        logger, ignore_errors=False,
    )

    # Create pool
    logger.info("[reset]   Creating pool...")
    _ssh_exec(
        ssh_obj, mgmt_ip,
        f"{sbcli_cmd} pool add testing1 {cluster_id}",
        logger, ignore_errors=False,
    )

    # Extract cluster secret
    result = _ssh_exec(
        ssh_obj, mgmt_ip,
        f"{sbcli_cmd} cluster get-secret {cluster_id}",
        logger, ignore_errors=False,
    )
    cluster_secret = str(result).strip().split("\n")[0].strip()
    logger.info("[reset]   Cluster secret obtained.")

    # Close SSH connections
    for node, ssh in ssh_obj.ssh_connections.items():
        try:
            ssh.close()
        except Exception:
            pass

    logger.info("[reset] Docker cluster reset complete.")
    return cluster_id, cluster_secret


# ── K8s cluster reset ─────────────────────────────────────────────────


def _strip_cr_metadata(cr_data):
    """Strip server-side metadata from a CR dict so it can be re-applied.

    Works on both a single resource dict and a List-kind wrapper.
    Returns a list of cleaned resource dicts.
    """
    if cr_data.get("kind", "").endswith("List"):
        items = cr_data.get("items", [])
    else:
        items = [cr_data]

    cleaned = []
    for item in items:
        for key in ("resourceVersion", "uid", "creationTimestamp",
                     "generation", "managedFields", "selfLink"):
            item.get("metadata", {}).pop(key, None)
        item.get("metadata", {}).get("annotations", {}).pop(
            "kubectl.kubernetes.io/last-applied-configuration", None
        )
        item.pop("status", None)
        cleaned.append(item)
    return cleaned


def _find_helm_chart_path(args, logger):
    """Resolve the helm chart path from CLI arg or GITHUB_WORKSPACE."""
    if args.helm_chart_path and os.path.isdir(args.helm_chart_path):
        return args.helm_chart_path

    gw = os.environ.get("GITHUB_WORKSPACE", "")
    if gw:
        candidate = os.path.join(
            gw, "simplyblock-operator",
            "helm-charts", "charts", "simplyblock-operator",
        )
        if os.path.isdir(candidate):
            logger.info(f"[reset] Auto-detected helm chart at {candidate}")
            return candidate

    raise RuntimeError(
        "[reset] Cannot find helm chart directory. "
        "Pass --helm_chart_path or ensure GITHUB_WORKSPACE is set."
    )


def _kubectl_run(cmd_args, logger, timeout=120, check=False):
    """Run a kubectl/helm command and return the CompletedProcess."""
    result = subprocess.run(
        cmd_args, capture_output=True, text=True, timeout=timeout, check=check,
    )
    if result.returncode != 0 and result.stderr.strip():
        logger.warning(f"[reset]   cmd={cmd_args[0:4]}... rc={result.returncode} "
                       f"stderr={result.stderr[:200]}")
    return result


def _k8s_cluster_reset(args, new_worker_nodes, logger):
    """Full K8s cluster cleanup + re-bootstrap.

    Saves existing helm values and custom resources (StorageCluster, Pool,
    StorageNodeSet), runs cleanup_k8s.sh, re-installs the helm chart from
    the local chart path, re-applies the CRs (with expansion workers removed
    from StorageNodeSet), and waits for the cluster to become active.

    Returns:
        tuple: (cluster_id, cluster_secret)
    """
    namespace = args.namespace or "simplyblock"
    cleanup_script = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "scripts", "cleanup_k8s.sh",
    )
    values_file = "/tmp/reset_helm_values.yaml"
    cr_files = {
        "storagecluster": "/tmp/reset_storagecluster.json",
        "pool": "/tmp/reset_pool.json",
        "storagenodeset": "/tmp/reset_storagenodeset.json",
    }

    # ── Phase 1: Save state before cleanup ────────────────────────────
    logger.info("[reset] Phase 1: Saving helm values and custom resources...")

    # 1a. Helm values
    try:
        result = _kubectl_run(
            ["helm", "get", "values", "spdk-csi", "-n", namespace, "-o", "yaml"],
            logger, timeout=120,
        )
        saved_values = result.stdout if result.returncode == 0 else ""
    except Exception as e:
        logger.warning(f"[reset]   Could not save helm values: {e}")
        saved_values = ""

    if not saved_values:
        raise RuntimeError(
            "[reset] No helm values could be saved. Cannot re-install chart."
        )
    with open(values_file, "w") as f:
        f.write(saved_values)
    logger.info("[reset]   Helm values saved.")

    # 1b. Custom resources
    for cr_kind, cr_path in cr_files.items():
        try:
            result = _kubectl_run(
                ["kubectl", "get", cr_kind, "-n", namespace, "-o", "json"],
                logger, timeout=60,
            )
            if result.returncode == 0 and result.stdout.strip():
                cr_data = json.loads(result.stdout)
                cleaned = _strip_cr_metadata(cr_data)

                # For StorageNodeSet: remove expansion workers
                if cr_kind == "storagenodeset" and new_worker_nodes:
                    expansion_set = set(new_worker_nodes)
                    for item in cleaned:
                        workers = item.get("spec", {}).get("workerNodes", [])
                        initial = [w for w in workers if w not in expansion_set]
                        item["spec"]["workerNodes"] = initial
                        logger.info(
                            f"[reset]   StorageNodeSet workers: "
                            f"{len(workers)} -> {len(initial)} "
                            f"(removed {expansion_set})"
                        )

                # Write as a List if multiple, or single resource
                if len(cleaned) == 1:
                    with open(cr_path, "w") as f:
                        json.dump(cleaned[0], f, indent=2)
                else:
                    wrapper = {
                        "apiVersion": "v1",
                        "kind": "List",
                        "items": cleaned,
                    }
                    with open(cr_path, "w") as f:
                        json.dump(wrapper, f, indent=2)
                logger.info(f"[reset]   Saved {cr_kind} ({len(cleaned)} items)")
            else:
                logger.warning(f"[reset]   No {cr_kind} found to save.")
                cr_files[cr_kind] = ""  # mark as empty
        except Exception as e:
            logger.warning(f"[reset]   Could not save {cr_kind}: {e}")
            cr_files[cr_kind] = ""

    # ── Phase 2: Find helm chart path ─────────────────────────────────
    logger.info("[reset] Phase 2: Locating helm chart...")
    chart_path = _find_helm_chart_path(args, logger)
    logger.info(f"[reset]   Chart path: {chart_path}")

    # ── Phase 3: Run cleanup_k8s.sh ───────────────────────────────────
    logger.info("[reset] Phase 3: Running cleanup_k8s.sh...")
    if os.path.isfile(cleanup_script):
        try:
            subprocess.run(
                ["bash", cleanup_script, namespace],
                timeout=600, check=False,
            )
        except Exception as e:
            logger.warning(f"[reset]   cleanup_k8s.sh error (continuing): {e}")
    else:
        logger.warning(
            f"[reset]   cleanup_k8s.sh not found at {cleanup_script}. "
            f"Falling back to inline cleanup."
        )
        _kubectl_run(
            ["helm", "uninstall", "spdk-csi", "-n", namespace],
            logger, timeout=120,
        )
        for crd in ("storagenodes", "storagenodeops", "storageclusters",
                     "storagenodesets", "pools"):
            _kubectl_run(
                ["kubectl", "delete",
                 f"{crd}.storage.simplyblock.io",
                 "--all", "-n", namespace,
                 "--ignore-not-found", "--wait=false"],
                logger, timeout=120,
            )

    # ── Phase 4: Wait for namespace deletion ──────────────────────────
    logger.info("[reset] Phase 4: Waiting for namespace deletion...")
    deadline = time.time() + 300
    while time.time() < deadline:
        result = _kubectl_run(
            ["kubectl", "get", "namespace", namespace, "--no-headers"],
            logger, timeout=30,
        )
        if result.returncode != 0 or not result.stdout.strip():
            logger.info(f"[reset]   Namespace {namespace} deleted.")
            break
        status = result.stdout.strip().split()[-1] if result.stdout.strip() else ""
        logger.info(f"[reset]   Namespace still exists (status={status}), waiting...")
        time.sleep(10)
    else:
        logger.warning("[reset]   Timed out waiting for namespace deletion. "
                       "Proceeding anyway.")

    # ── Phase 5: Re-create namespace with pod-security labels ─────────
    logger.info("[reset] Phase 5: Re-creating namespace...")
    # Use dry-run + apply to be idempotent
    create_result = _kubectl_run(
        ["kubectl", "create", "namespace", namespace,
         "--dry-run=client", "-o", "yaml"],
        logger, timeout=30,
    )
    if create_result.returncode == 0:
        apply_proc = subprocess.run(
            ["kubectl", "apply", "-f", "-"],
            input=create_result.stdout,
            capture_output=True, text=True, timeout=30,
        )
        if apply_proc.returncode != 0:
            logger.warning(f"[reset]   Namespace apply error: {apply_proc.stderr}")

    _kubectl_run(
        ["kubectl", "label", "namespace", namespace,
         "pod-security.kubernetes.io/enforce=privileged",
         "pod-security.kubernetes.io/audit=privileged",
         "pod-security.kubernetes.io/warn=privileged",
         "--overwrite"],
        logger, timeout=30,
    )
    logger.info(f"[reset]   Namespace {namespace} ready.")

    # ── Phase 6: Helm upgrade --install ───────────────────────────────
    logger.info("[reset] Phase 6: Installing helm chart...")
    _kubectl_run(
        ["helm", "upgrade", "--install", "spdk-csi", chart_path,
         "--namespace", namespace,
         "--create-namespace",
         "--timeout", "10m",
         "-f", values_file],
        logger, timeout=660, check=True,
    )
    logger.info("[reset]   Helm chart installed.")

    # ── Phase 7: Wait for CRDs + admin pod ────────────────────────────
    logger.info("[reset] Phase 7: Waiting for operator CRDs and admin pod...")

    # 7a. Wait for CRDs
    required_crd = "storageclusters.storage.simplyblock.io"
    deadline = time.time() + 120
    while time.time() < deadline:
        result = _kubectl_run(
            ["kubectl", "get", "crd", required_crd],
            logger, timeout=30,
        )
        if result.returncode == 0:
            logger.info(f"[reset]   CRD {required_crd} available.")
            break
        time.sleep(5)
    else:
        raise RuntimeError(
            f"[reset] CRD {required_crd} not available after helm install."
        )

    # 7b. Wait for admin pod
    admin_pod = ""
    deadline = time.time() + 600
    while time.time() < deadline:
        result = _kubectl_run(
            ["kubectl", "-n", namespace, "get", "pods",
             "-l", "app=simplyblock-admin-control",
             "-o", "jsonpath={.items[0].metadata.name}"],
            logger, timeout=30,
        )
        pod_name = result.stdout.strip()
        if pod_name:
            phase_result = _kubectl_run(
                ["kubectl", "-n", namespace, "get", "pod", pod_name,
                 "-o", "jsonpath={.status.phase}"],
                logger, timeout=30,
            )
            if phase_result.stdout.strip() == "Running":
                admin_pod = pod_name
                logger.info(f"[reset]   Admin pod {admin_pod} is Running.")
                break
            logger.info(f"[reset]   Admin pod {pod_name} "
                        f"phase={phase_result.stdout.strip()}")
        time.sleep(10)

    if not admin_pod:
        raise RuntimeError("[reset] Admin pod did not become ready.")

    # ── Phase 8: Re-apply custom resources ────────────────────────────
    logger.info("[reset] Phase 8: Applying custom resources...")
    # Apply in order: StorageCluster → Pool → StorageNodeSet
    apply_order = ["storagecluster", "pool", "storagenodeset"]
    for cr_kind in apply_order:
        cr_path = cr_files.get(cr_kind, "")
        if not cr_path or not os.path.isfile(cr_path):
            logger.warning(f"[reset]   Skipping {cr_kind} (no saved CR).")
            continue
        result = _kubectl_run(
            ["kubectl", "apply", "-f", cr_path],
            logger, timeout=120,
        )
        if result.returncode == 0:
            logger.info(f"[reset]   Applied {cr_kind}.")
        else:
            logger.error(f"[reset]   Failed to apply {cr_kind}: {result.stderr}")

    # ── Phase 9: Wait for storage node pods + cluster active ──────────
    logger.info("[reset] Phase 9: Waiting for storage nodes and cluster...")

    # 9a. Count expected storage nodes from the saved StorageNodeSet
    expected_snodes = 0
    sns_path = cr_files.get("storagenodeset", "")
    if sns_path and os.path.isfile(sns_path):
        with open(sns_path) as f:
            sns_data = json.load(f)
        items = sns_data.get("items", [sns_data]) if sns_data.get("kind") == "List" \
            else [sns_data]
        for item in items:
            expected_snodes += len(item.get("spec", {}).get("workerNodes", []))
    if expected_snodes == 0:
        expected_snodes = 1  # fallback
    logger.info(f"[reset]   Expecting {expected_snodes} storage node pod(s).")

    # 9b. Wait for snode-spdk pods
    deadline = time.time() + 3000
    while time.time() < deadline:
        result = _kubectl_run(
            ["kubectl", "-n", namespace, "get", "pods",
             "-l", "role=simplyblock-storage-node",
             "--no-headers"],
            logger, timeout=30,
        )
        lines = [ln for ln in result.stdout.strip().splitlines() if ln.strip()]
        running = sum(1 for ln in lines if "Running" in ln)
        if running >= expected_snodes:
            logger.info(f"[reset]   All {running}/{expected_snodes} "
                        f"storage node pods Running.")
            break
        logger.info(f"[reset]   Storage node pods: {running}/{expected_snodes} "
                    f"Running (total={len(lines)})")
        time.sleep(10)
    else:
        logger.warning("[reset]   Timed out waiting for storage node pods.")

    # 9c. Wait for cluster active
    logger.info("[reset]   Polling cluster status...")
    cluster_id = ""
    deadline = time.time() + 3000
    while time.time() < deadline:
        # Re-resolve admin pod each iteration (it may restart)
        pod_result = _kubectl_run(
            ["kubectl", "-n", namespace, "get", "pods",
             "-l", "app=simplyblock-admin-control",
             "-o", "jsonpath={.items[0].metadata.name}"],
            logger, timeout=30,
        )
        current_pod = pod_result.stdout.strip()
        if not current_pod:
            time.sleep(10)
            continue

        result = _kubectl_run(
            ["kubectl", "-n", namespace, "exec", current_pod, "--",
             "sbctl", "cluster", "list"],
            logger, timeout=60,
        )
        output = result.stdout
        for line in output.splitlines():
            if "active" in line.lower():
                parts = line.split()
                if len(parts) >= 2:
                    cluster_id = parts[1] if len(parts[1]) > 8 else parts[0]
                break
        if cluster_id:
            break

        # Fallback: try JSON format
        if not cluster_id and result.returncode == 0:
            json_result = _kubectl_run(
                ["kubectl", "-n", namespace, "exec", current_pod, "--",
                 "sbctl", "cluster", "list", "--json"],
                logger, timeout=60,
            )
            try:
                clusters = json.loads(json_result.stdout)
                for cl in clusters:
                    if cl.get("status", "").lower() == "active":
                        cluster_id = cl.get("id") or cl.get("uuid", "")
                        break
            except (json.JSONDecodeError, TypeError):
                pass
        if cluster_id:
            break

        time.sleep(10)

    if not cluster_id:
        # Last resort: try forced activation
        logger.warning("[reset]   Cluster not active yet. Attempting forced activation...")
        pod_result = _kubectl_run(
            ["kubectl", "-n", namespace, "get", "pods",
             "-l", "app=simplyblock-admin-control",
             "-o", "jsonpath={.items[0].metadata.name}"],
            logger, timeout=30,
        )
        current_pod = pod_result.stdout.strip()
        if current_pod:
            # Get cluster ID even if not active
            list_result = _kubectl_run(
                ["kubectl", "-n", namespace, "exec", current_pod, "--",
                 "sbctl", "cluster", "list", "--json"],
                logger, timeout=60,
            )
            try:
                clusters = json.loads(list_result.stdout)
                if clusters:
                    cluster_id = clusters[0].get("id") or clusters[0].get("uuid", "")
            except (json.JSONDecodeError, TypeError):
                pass

            if cluster_id:
                _kubectl_run(
                    ["kubectl", "-n", namespace, "exec", current_pod, "--",
                     "sbctl", "cluster", "activate", cluster_id],
                    logger, timeout=120,
                )
                time.sleep(60)
                # Verify
                verify_result = _kubectl_run(
                    ["kubectl", "-n", namespace, "exec", current_pod, "--",
                     "sbctl", "cluster", "list"],
                    logger, timeout=60,
                )
                if "active" not in verify_result.stdout.lower():
                    raise RuntimeError(
                        "[reset] Cluster did not become active after "
                        "forced activation."
                    )
            else:
                raise RuntimeError(
                    "[reset] Could not find any cluster after re-install."
                )

    logger.info(f"[reset]   Cluster active: {cluster_id}")

    # ── Phase 10: Extract cluster secret ──────────────────────────────
    logger.info("[reset] Phase 10: Extracting cluster secret...")
    pod_result = _kubectl_run(
        ["kubectl", "-n", namespace, "get", "pods",
         "-l", "app=simplyblock-admin-control",
         "-o", "jsonpath={.items[0].metadata.name}"],
        logger, timeout=30,
    )
    current_pod = pod_result.stdout.strip()
    secret_result = _kubectl_run(
        ["kubectl", "-n", namespace, "exec", current_pod, "--",
         "sbctl", "cluster", "get-secret", cluster_id],
        logger, timeout=60,
    )
    cluster_secret = secret_result.stdout.strip().split("\n")[0].strip()

    logger.info("[reset] K8s cluster reset complete.")
    return cluster_id, cluster_secret


def check_for_dumps():
    """Validates whether core dumps present on machines
    
    Returns:
        bool: If there are core dumps or not
    """
    logger.info("Checking for core dumps!!")
    cluster_base = TestClusterBase()
    ssh_obj = SshUtils(bastion_server=cluster_base.bastion_server)
    sbcli_utils = SbcliUtils(
        cluster_api_url=cluster_base.api_base_url,
        cluster_id=cluster_base.cluster_id,
        cluster_secret=cluster_base.cluster_secret
    )
    _, storage_nodes = sbcli_utils.get_all_nodes_ip()
    for node in storage_nodes:
        logger.info(f"**Connecting to storage nodes** - {node}")
        ssh_obj.connect(
            address=node,
            bastion_server_address=cluster_base.bastion_server,
        )
    core_exist = False
    for node in storage_nodes:
        files = ssh_obj.list_files(node, "/etc/simplyblock/")
        logger.info(f"Files in /etc/simplyblock: {files}")
        if "core.react" in files:
            core_exist = True
            break


    for node, ssh in ssh_obj.ssh_connections.items():
        logger.info(f"Closing node ssh connection for {node}")
        ssh.close()
    return core_exist


logger = setup_logger(__name__)
main()
