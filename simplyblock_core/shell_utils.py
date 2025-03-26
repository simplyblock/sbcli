# coding=utf-8
import subprocess


def run_command(cmd):
    try:
        process = subprocess.Popen(
            cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        return stdout.strip().decode("utf-8"), stderr.strip(), process.returncode
    except Exception as e:
        return "", str(e), 1
