import hashlib
import logging
import os
import subprocess


DIR_PATH = os.path.dirname(os.path.realpath(__file__))

logger = logging.getLogger()


def __run_script(args: list):
    try:
        output = subprocess.check_output(args, timeout=720, text=True, stderr=subprocess.STDOUT)
        if output:
            logger.debug(output.strip())
    except subprocess.CalledProcessError as e:
        logger.debug(f"Command failed with return code: {e.returncode}")
        logger.debug((f"Captured output:\n {e.output}"))
        raise


def install_deps(mode):
    return __run_script(['bash', '-x', os.path.join(DIR_PATH, 'install_deps.sh'), mode])


def configure_docker(docker_ip):
    __run_script(['bash', '-x', os.path.join(DIR_PATH, 'config_docker.sh'), docker_ip])


def deploy_stack(cli_pass, dev_ip, image_name, graylog_password, cluster_id,
                 log_del_interval, metrics_retention_period, log_level, grafana_endpoint, disable_monitoring):
    pass_hash = hashlib.sha256(graylog_password.encode('utf-8')).hexdigest()
    __run_script(
        ['sudo', 'bash', '-x', os.path.join(DIR_PATH, 'deploy_stack.sh'), cli_pass, dev_ip, image_name, pass_hash,
         graylog_password, cluster_id, log_del_interval, metrics_retention_period, log_level, grafana_endpoint, disable_monitoring])

def deploy_k8s_stack(cli_pass, dev_ip, image_name, graylog_password, cluster_id,
                 log_del_interval, metrics_retention_period, log_level, grafana_endpoint, contact_point, db_connection, k8s_namespace, disable_monitoring, tls_secret, ingress_host_source, dns_name):
    pass_hash = hashlib.sha256(graylog_password.encode('utf-8')).hexdigest()
    __run_script(
        ['sudo', 'bash', '-x', os.path.join(DIR_PATH, 'deploy_k8s_stack.sh'), cli_pass, dev_ip, image_name, pass_hash,
         graylog_password, cluster_id, log_del_interval, metrics_retention_period, log_level, grafana_endpoint, contact_point, db_connection, k8s_namespace, disable_monitoring, tls_secret, ingress_host_source, dns_name])

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
