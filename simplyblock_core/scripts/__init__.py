import hashlib
import logging
import os
import subprocess

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


def _script_messages(output) -> str:
    """The script's own output, without `bash -x` trace lines: those echo
    every command with its arguments, secrets included, so they stay at
    debug level. Only for scripts whose own output carries no secret
    (set_db_config.sh, for one, echoes the connection string)."""
    if isinstance(output, bytes):
        output = output.decode(errors="replace")
    return "\n".join(line for line in (output or "").splitlines()
                     if line.strip() and not line.startswith("+"))


def _script_name(args: list) -> str:
    return next((os.path.basename(a) for a in args if str(a).endswith(".sh")), str(args[0]))


def __run_script(args: list, report_failure_output=False):
    """Run a script. With report_failure_output, a failure also logs the
    script's own (non-trace) output at error level, so the reason is visible
    without debug logging."""
    try:
        output = subprocess.check_output(args, timeout=SCRIPT_TIMEOUT_SEC, text=True, stderr=subprocess.STDOUT)
        if output:
            logger.debug(output.strip())
    except subprocess.CalledProcessError as e:
        logger.debug(f"Command failed with return code: {e.returncode}")
        logger.debug(f"Captured output:\n {e.output}")
        if report_failure_output:
            logger.error(f"{_script_name(args)} failed with return code {e.returncode}:\n"
                         f"{_script_messages(e.output)}")
        raise
    except subprocess.TimeoutExpired as e:
        logger.debug(f"Captured output:\n {e.output}")
        if report_failure_output:
            logger.error(f"{_script_name(args)} killed after {SCRIPT_TIMEOUT_SEC}s:\n"
                         f"{_script_messages(e.output)}")
        raise


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
    __run_script(
        ['sudo', 'bash', '-x', os.path.join(DIR_PATH, 'deploy_stack.sh'), cli_pass, dev_ip, image_name, pass_hash,
         graylog_password, cluster_id, log_del_interval, metrics_retention_period, log_level, grafana_endpoint, disable_monitoring,
         phase, str(STACK_WAIT_TIMEOUT_SEC)],
        report_failure_output=True)

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


def set_db_config_single():
    __run_script(['bash', os.path.join(DIR_PATH, 'db_config_single.sh')])


def set_db_config_double():
    __run_script(['bash', os.path.join(DIR_PATH, 'db_config_double.sh')])

def deploy_fdb_from_file_service(zip_path):
    args = ["sudo", 'bash']
    if logger.level == logging.DEBUG:
        args.append("-x")
    args.append(os.path.join(DIR_PATH, 'deploy_fdb.sh'))
    args.append(zip_path)
    subprocess.check_call(" ".join(args), shell=True,  text=True)
