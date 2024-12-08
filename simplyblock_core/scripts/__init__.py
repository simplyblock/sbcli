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
                 log_del_interval, metrics_retention_period, log_level, grafana_endpoint):
    pass_hash = hashlib.sha256(graylog_password.encode('utf-8')).hexdigest()
    return __run_script(
        ['sudo', 'bash', '-x', os.path.join(DIR_PATH, 'deploy_stack.sh'), cli_pass, dev_ip, image_name, pass_hash,
         graylog_password, cluster_id, log_del_interval, metrics_retention_period, log_level, grafana_endpoint])


def deploy_cleaner():
    return __run_script(['sudo', 'bash', '-x', os.path.join(DIR_PATH, 'clean_local_storage_deploy.sh')])


def set_db_config(DEV_IP):
    return __run_script(['sudo', 'bash', '-x', os.path.join(DIR_PATH, 'set_db_config.sh'), DEV_IP])


def set_db_config_single():
    return __run_script(['bash', os.path.join(DIR_PATH, 'db_config_single.sh')])


def set_db_config_double():
    return __run_script(['bash', os.path.join(DIR_PATH, 'db_config_double.sh')])

def deploy_fdb_from_file_service(zip_path):
    return __run_script3(['bash', os.path.join(DIR_PATH, 'deploy_fdb.sh'), zip_path])

def remove_deploy_fdb_from_file_service(zip_path):
    return __run_script(['bash', os.path.join(DIR_PATH, 'deploy_fdb_remove.sh'), zip_path])



def __run_script2(args: list):
    import subprocess

    def start(executable_file):
        return subprocess.Popen(
            executable_file,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)

    def read(process):
        return process.stdout.readline().decode("utf-8").strip()

    def write(process, message):
        process.stdin.write(f"{message.strip()}\n".encode("utf-8"))
        process.stdin.flush()

    def terminate(process):
        process.stdin.close()
        process.terminate()
        process.wait(timeout=0.2)

    process = start(args)
    write(process, "hello dummy")
    print(read(process))
    terminate(process)


def __run_script3(args: list):
    from subprocess import Popen, PIPE

    fw = open("tmpout", "wb")
    fr = open("tmpout", "r")
    p = Popen(args, stdin=PIPE, stdout=fw, stderr=fw, bufsize=1)
    p.stdin.write("sbcli-main-ha sn list\n")
    out = fr.read()
    # p.stdin.write("5\n")
    # out = fr.read()
    fw.close()
    fr.close()

