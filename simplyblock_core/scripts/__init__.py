import hashlib
import logging
import os
import subprocess
import tempfile

from tenacity import (
    Retrying,
    before_sleep_log,
    retry_if_exception_type,
    stop_after_delay,
    wait_fixed,
)

DIR_PATH = os.path.dirname(os.path.realpath(__file__))

logger = logging.getLogger()

#: Hard limit on one script run.
SCRIPT_TIMEOUT_SEC = 720
#: stack_deploy_wait.sh's limit inside one deploy_stack.sh phase. Kept well
#: below SCRIPT_TIMEOUT_SEC so the wait times out first and reports which
#: services never converged; if the Python limit fires first, the script is
#: killed mid-wait and that report is lost. The margin covers
#: `docker stack deploy` and the failure diagnostics deploy_stack.sh prints.
STACK_WAIT_TIMEOUT_SEC = SCRIPT_TIMEOUT_SEC - 120


def __run_script(args: list, failure_report=None):
    """Run a script. ``failure_report`` is an optional ``(label, path)``: a
    file the script fills with a secret-free account of what went wrong,
    logged at error level if the script fails. The captured output itself
    stays at debug level: scripts receive secrets as arguments, and their
    output (``bash -x`` traces above all) can echo them."""
    try:
        output = subprocess.check_output(args, timeout=SCRIPT_TIMEOUT_SEC, text=True, stderr=subprocess.STDOUT)
        if output:
            logger.debug(output.strip())
    except subprocess.CalledProcessError as e:
        logger.debug(f"Command failed with return code: {e.returncode}")
        logger.debug(f"Captured output:\n {e.output}")
        _log_failure_report(failure_report, "failed")
        raise
    except subprocess.TimeoutExpired as e:
        logger.debug(f"Captured output:\n {e.output}")
        _log_failure_report(failure_report, f"killed after {SCRIPT_TIMEOUT_SEC}s")
        raise


def _log_failure_report(failure_report, outcome):
    if failure_report is None:
        return
    label, path = failure_report
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            report = f.read().strip()
    except OSError as e:
        report = f"(no failure report: {e})"
    logger.error(f"{label} {outcome}:\n{report or '(no failure report written)'}")


def install_deps(mode):
    return __run_script(['bash', '-x', os.path.join(DIR_PATH, 'install_deps.sh'), mode])


def configure_docker(docker_ip):
    __run_script(['bash', '-x', os.path.join(DIR_PATH, 'config_docker.sh'), docker_ip])


#: deploy_stack phases, in the order create_cluster must run them. The
#: database is configured between "fdb" and "app": the simplyblock services
#: exit on an unconfigured database, so deploying them first never converges.
STACK_PHASES = ("monitoring", "fdb", "app")


def deploy_stack(cli_pass, dev_ip, image_name, graylog_password, cluster_id,
                 log_del_interval, metrics_retention_period, log_level, grafana_endpoint, disable_monitoring,
                 *, phase):
    if phase not in STACK_PHASES:
        raise ValueError(f"Unknown deploy phase {phase!r}, expected one of {STACK_PHASES}")
    pass_hash = hashlib.sha256(graylog_password.encode('utf-8')).hexdigest()
    fd, report_path = tempfile.mkstemp(prefix=f"deploy_stack_{phase}_", suffix=".log")
    os.close(fd)
    try:
        __run_script(
            ['sudo', 'bash', '-x', os.path.join(DIR_PATH, 'deploy_stack.sh'), cli_pass, dev_ip, image_name, pass_hash,
             graylog_password, cluster_id, log_del_interval, metrics_retention_period, log_level, grafana_endpoint,
             disable_monitoring, phase, str(STACK_WAIT_TIMEOUT_SEC), report_path],
            failure_report=(f"deploy_stack.sh phase {phase}", report_path))
    finally:
        os.unlink(report_path)

def deploy_k8s_stack(cli_pass, dev_ip, image_name, graylog_password, cluster_id,
                 log_del_interval, metrics_retention_period, log_level, grafana_endpoint, contact_point, k8s_namespace, disable_monitoring, tls_secret, ingress_host_source, dns_name):
    pass_hash = hashlib.sha256(graylog_password.encode('utf-8')).hexdigest()
    __run_script(
        ['sudo', 'bash', '-x', os.path.join(DIR_PATH, 'deploy_k8s_stack.sh'), cli_pass, dev_ip, image_name, pass_hash,
         graylog_password, cluster_id, log_del_interval, metrics_retention_period, log_level, grafana_endpoint, contact_point, k8s_namespace, disable_monitoring, tls_secret, ingress_host_source, dns_name])

def deploy_cleaner():
    __run_script(['sudo', 'bash', '-x', os.path.join(DIR_PATH, 'clean_local_storage_deploy.sh')])


def set_db_config(DEV_IP):
    __run_script(['sudo', 'bash', '-x', os.path.join(DIR_PATH, 'set_db_config.sh'), DEV_IP])


FDB_CLUSTER_FILE = "/etc/foundationdb/fdb.cluster"
#: How long configure_db_single waits for the fdbserver processes to accept
#: connections. Swarm reports the FDB services converged once their containers
#: run, which can be well before fdbserver listens (~110s apart, PR 1620).
FDB_REACHABLE_TIMEOUT_SEC = 300
#: How long configure_db_single waits for a configured database to report
#: itself available.
FDB_AVAILABLE_TIMEOUT_SEC = 120
#: fdbcli --timeout of one attempt. Short, so one attempt cannot use up the
#: overall wait while fdbserver is still coming up.
FDBCLI_ATTEMPT_TIMEOUT_SEC = 30
FDB_RETRY_INTERVAL_SEC = 5


class FdbConfigureError(RuntimeError):
    """The database could not be created, or did not become available."""


def _fdbcli(command):
    """Run one fdbcli command; return (exit code, combined output)."""
    result = subprocess.run(
        ['fdbcli', '--exec', command, '--timeout', str(FDBCLI_ATTEMPT_TIMEOUT_SEC), '-C', FDB_CLUSTER_FILE],
        capture_output=True, text=True, timeout=FDBCLI_ATTEMPT_TIMEOUT_SEC + 30)
    return result.returncode, (result.stdout + result.stderr).strip()


def _fdb_retrying(timeout_sec):
    return Retrying(
        stop=stop_after_delay(timeout_sec),
        wait=wait_fixed(FDB_RETRY_INTERVAL_SEC),
        retry=retry_if_exception_type((FdbConfigureError, subprocess.TimeoutExpired)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True)


def _create_database_single():
    rc, output = _fdbcli("configure new single ssd")
    # A retry after an attempt that timed out client-side but went through.
    if rc == 0 or "Database already exists" in output:
        return
    raise FdbConfigureError(f"configure new single ssd failed (rc={rc}): {output}")


def _check_database_available():
    rc, output = _fdbcli("status minimal")
    if rc != 0 or "The database is available" not in output:
        raise FdbConfigureError(f"database not available (rc={rc}): {output}")


def set_db_config_single():
    """Create a single-replica database on a freshly deployed FDB, and return
    only once it is available. Raises FdbConfigureError otherwise: every
    simplyblock service exits on an unavailable database, so carrying on
    would only turn this into a transaction timeout minutes later."""
    _fdb_retrying(FDB_REACHABLE_TIMEOUT_SEC)(_create_database_single)
    _fdb_retrying(FDB_AVAILABLE_TIMEOUT_SEC)(_check_database_available)


def set_db_config_double():
    __run_script(['bash', os.path.join(DIR_PATH, 'db_config_double.sh')])

def deploy_fdb_from_file_service(zip_path):
    args = ["sudo", 'bash']
    if logger.level == logging.DEBUG:
        args.append("-x")
    args.append(os.path.join(DIR_PATH, 'deploy_fdb.sh'))
    args.append(zip_path)
    subprocess.check_call(" ".join(args), shell=True,  text=True)
