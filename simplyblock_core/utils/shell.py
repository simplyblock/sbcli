import os
import socket
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


def _host_uts_hostname():
    """The host's own hostname, read from the host's UTS namespace; None when that cannot be done.

    SB_HOST_NETNS names the host's network namespace under the host's /proc, and the UTS
    namespace sits beside it. The OS hostname is what a host-network agent reported, and it
    need not equal the Kubernetes node name.
    """
    netns = os.environ.get("SB_HOST_NETNS")
    if not netns:
        return None
    uts = os.path.join(os.path.dirname(netns), "uts")
    out, _, rc = run_command(f"nsenter --uts={uts} hostname")
    out = (out or "").strip()
    return out if rc == 0 and out else None


def full_hostname():
    """The node's hostname as `hostname` prints it: the host's own, then SB_NODE_NAME, then this process's."""
    return _host_uts_hostname() or os.environ.get("SB_NODE_NAME") or socket.gethostname()


def hostname():
    """The node's short hostname.

    On the pod network the process's own hostname is the pod's name, so the host's is read
    from its UTS namespace, and SB_NODE_NAME stands in when that cannot be entered.
    """
    name = _host_uts_hostname() or os.environ.get("SB_NODE_NAME")
    if name:
        return name.split(".")[0]
    out, _, _ = run_command("hostname -s")
    return out
