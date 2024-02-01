import subprocess


def run_command(cmd):
    process = subprocess.Popen(
        cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    return stdout.strip().decode("utf-8"), stderr.strip(), process.returncode

out, err, rc = run_command("cat /etc/sbcli/cluster_id")

