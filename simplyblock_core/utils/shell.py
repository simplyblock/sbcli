import os
import subprocess


def run_command(cmd):
    try:
        process = subprocess.Popen(
            cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        return stdout.strip().decode("utf-8"), stderr.strip(), process.returncode
    except Exception as e:
        return "", str(e), 1



def host_net_command(args):
    """Prefix a command to run in the host's network namespace when SB_HOST_NETNS names it.

    An agent on the pod network sees only the pod's eth0, so reads of the host's NICs
    and pings out of them enter the host namespace. Unset, the command is unchanged.
    """
    netns = os.environ.get("SB_HOST_NETNS")
    return ["nsenter", f"--net={netns}", *args] if netns else list(args)


def hostname():
    """The node's short hostname. SB_NODE_NAME wins, because a pod's own hostname is the pod's name."""
    node_name = os.environ.get("SB_NODE_NAME")
    if node_name:
        return node_name.split(".")[0]
    out, _, _ = run_command("hostname -s")
    return out
