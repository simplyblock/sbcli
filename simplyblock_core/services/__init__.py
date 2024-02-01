import logging
import os
import tempfile
from string import Template

from simplyblock_core import constants
from simplyblock_core import shell_utils


logger = logging.getLogger()
SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))
SPDK_PATH = constants.SPK_DIR


class ServiceException(Exception):
    def __init__(self, message):
        self.message = message


class ServiceObject:

    SERVICE_INSTALL_SCRIPT = os.path.join(SCRIPT_PATH, 'install_service.sh')
    SERVICE_REMOVE_SCRIPT = os.path.join(SCRIPT_PATH, 'remove_service.sh')

    def __init__(self, name, cmd, args=None, env_vars=None, pre_start_cmd=None):
        self.name = name
        self.cmd = cmd
        self.args = args
        self.env_vars = env_vars
        self.pre_start_cmd = pre_start_cmd

    def _build_unit_file(self):

        cmd_line = self.cmd

        if self.args:
            cmd_line = " %s %s" % (cmd_line, " ".join(self.args))

        data = {
            'service_name': self.name,
            'cmd_line': cmd_line,
            'Environment': '',
            'ExecStartPre': ''
        }
        if self.env_vars:
            data['Environment'] = f'Environment="{self.env_vars}"'

        if self.pre_start_cmd:
            data['ExecStartPre'] = f'ExecStartPre={self.pre_start_cmd}'

        with open(os.path.join(SCRIPT_PATH, 'service_template.service'), 'r') as f:
            src = Template(f.read())
            content = src.substitute(data)

        tmp = tempfile.NamedTemporaryFile(delete=False, mode='w')
        try:
            tmp.write(content)
        finally:
            tmp.close()
        return tmp.name

    def init_service(self):
        if self.is_service_running():
            logger.info(self.name + ' service is active, restarting')
            self.restart()
            return True
        else:
            logger.info("Installing %s" % self.name)
            out, err, ret_code = self.install_service()
            logger.debug(out)
            logger.debug(err)
            if ret_code == 0:
                logger.info(self.name + ' service installed and is active')
                return True
            else:
                raise ServiceException('Error while installing service %s: %s' % (self.name, err))

    def is_service_running(self):
        out, _, ret_code = shell_utils.run_command("systemctl is-active %s" % self.name)
        return ret_code == 0

    def service_start(self):
        out, _, ret_code = shell_utils.run_command("sudo systemctl start %s" % self.name)
        return ret_code == 0

    def service_stop(self):
        out, _, ret_code = shell_utils.run_command("sudo systemctl stop %s" % self.name)
        return ret_code == 0

    def install_service(self):
        out, err, ret_code = shell_utils.run_command(
            "sudo bash %s %s %s" % (self.SERVICE_INSTALL_SCRIPT, self.name, self._build_unit_file()))
        return out, err, ret_code

    def service_remove(self):
        out, err, ret_code = shell_utils.run_command("sudo bash %s %s" % (self.SERVICE_REMOVE_SCRIPT, self.name))
        return ret_code == 0

    def restart(self):
        out, _, ret_code = shell_utils.run_command("sudo systemctl restart %s" % self.name)
        return ret_code == 0


spdk_nvmf_tgt = ServiceObject(
    "spdk_nvmf_tgt",
    os.path.join(SPDK_PATH, "build/bin/nvmf_tgt"),
    env_vars="HUGEMEM=4096",
    pre_start_cmd=f"/bin/sudo {os.path.join(SPDK_PATH, 'scripts/setup.sh')}")


alloc_bdev = ServiceObject(
    "alloc_bdev",
    f"{SPDK_PATH}/ultra/build/ultra21-alloc /JSONSPEC={SPDK_PATH}/ultra/build/alloc.json",
    env_vars="HUGEMEM=4096",
    pre_start_cmd=f"/bin/sudo {os.path.join(SPDK_PATH, 'scripts/setup.sh')}")

ultra21 = ServiceObject(
    "ultra21",
    f"bash {SPDK_PATH}/ultra/scripts/run_spdk {SPDK_PATH}")

distr = ServiceObject(
    "distr",
    f"{SPDK_PATH}/ultra/DISTR_v2/bdts {SPDK_PATH}/DISTR_v2/bdts.json 2047 0x3",
    env_vars=f'LD_LIBRARY_PATH="$LD_LIBRARY_PATH:{SPDK_PATH}/build/lib:{SPDK_PATH}/dpdk/build/lib"',
    pre_start_cmd=f"export HUGEMEM=4096 ; /bin/sudo {os.path.join(SPDK_PATH, 'scripts/setup.sh')}")
