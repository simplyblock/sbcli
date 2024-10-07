import hashlib
import logging
import os
import subprocess


DIR_PATH = os.path.dirname(os.path.realpath(__file__))

logger = logging.getLogger()


def __run_script(args: list):
    process = subprocess.Popen(args, stdout=subprocess.PIPE, text=True, stderr=subprocess.STDOUT)
    while True:
        result = process.poll()
        if result is not None:
            break
        output = process.stdout.readline()
        if output:
            logger.debug(output.strip())

    return process.returncode


def install_deps():
    return __run_script(['bash', '-x', os.path.join(DIR_PATH, 'install_deps.sh')])


def configure_docker(docker_ip):
    return __run_script(['bash', '-x', os.path.join(DIR_PATH, 'config_docker.sh'), docker_ip])


def deploy_stack(cli_pass, dev_ip, image_name, graylog_password, cluster_id,
                 log_del_interval, metrics_retention_period, log_level):
    pass_hash = hashlib.sha256(graylog_password.encode('utf-8')).hexdigest()
    return __run_script(
        ['sudo', 'bash', '-x', os.path.join(DIR_PATH, 'deploy_stack.sh'), cli_pass, dev_ip, image_name, pass_hash,
         graylog_password, cluster_id, log_del_interval, metrics_retention_period, log_level])


def deploy_cleaner():
    return __run_script(['sudo', 'bash', '-x', os.path.join(DIR_PATH, 'clean_local_storage_deploy.sh')])


def set_db_config(DEV_IP):
    return __run_script(['bash', os.path.join(DIR_PATH, 'set_db_config.sh'), DEV_IP])


def set_db_config_single():
    return __run_script(['bash', os.path.join(DIR_PATH, 'db_config_single.sh')])


def set_db_config_double():
    return __run_script(['bash', os.path.join(DIR_PATH, 'db_config_double.sh')])
