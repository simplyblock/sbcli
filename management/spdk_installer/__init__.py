import os
import subprocess

from management import shell_utils, constants

SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))
SPK_INSTALL_SCRIPT = os.path.join(SCRIPT_PATH, 'install_spdk.sh')


class SpdkInstallerException(Exception):
    def __init__(self, message):
        self.message = message


def install_spdk():
    process = subprocess.Popen(['bash', SPK_INSTALL_SCRIPT, constants.SPK_DIR],
                               stdout=subprocess.PIPE, text=True, stderr=subprocess.PIPE)
    while True:
        output = process.stdout.readline()
        if output:
            print(output.strip())
        result = process.poll()
        if result is not None:
            break
    return process.returncode
