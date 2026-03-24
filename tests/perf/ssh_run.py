#!/usr/bin/env python3
"""Run a command on a remote lab node via jump host using pexpect.

Usage: ssh_run.py <command> <target_ip> [timeout]

Hop 1: SSH to jump host (key auth) - 95.216.93.11:13987
Hop 2: From jump host, SSH to lab node (password auth via pexpect)
"""
import re
import sys
import os
import shlex
import pexpect

JUMP_HOST = os.environ.get("SB_JUMP_HOST", "95.216.93.11")
JUMP_PORT = os.environ.get("SB_JUMP_PORT", "13987")
JUMP_USER = os.environ.get("SB_JUMP_USER", "simplyblock")
JUMP_KEY = os.path.expanduser(os.environ.get("SB_JUMP_KEY", "~/simplyblock"))

LAB_USER = os.environ.get("SB_LAB_USER", "root")
LAB_PASS = os.environ.get("SB_LAB_PASS", "3tango11")

_ANSI_RE = re.compile(r'(\x1b\[[^a-zA-Z]*[a-zA-Z]|\x1b\][^\x07]*\x07|\x1b\(B|\x1b\[[\?0-9]*[a-z])')


def _clean(text):
    return _ANSI_RE.sub('', text).replace('\r', '').strip()


def _enable_debug(child):
    if os.environ.get("SB_SSH_DEBUG") == "1":
        child.logfile_read = sys.stderr


def main():
    if len(sys.argv) < 3:
        print("Usage: ssh_run.py <command> <target_ip> [timeout]", file=sys.stderr)
        sys.exit(1)

    command = sys.argv[1]
    target = sys.argv[2]
    timeout = int(sys.argv[3]) if len(sys.argv) > 3 else 300

    jump_ssh = (
        f"ssh -tt -o StrictHostKeyChecking=no -o ConnectTimeout=10 "
        f"-p {JUMP_PORT} -i {shlex.quote(JUMP_KEY)} {JUMP_USER}@{JUMP_HOST}"
    )
    quoted_command = shlex.quote(command)
    remote_command = shlex.quote(f"bash -lc {quoted_command}")
    inner_ssh = (
        f"ssh -tt -o StrictHostKeyChecking=no -o ConnectTimeout=10 "
        f"{LAB_USER}@{target} {remote_command}"
    )

    try:
        child = pexpect.spawn(jump_ssh, timeout=30, encoding="utf-8", maxread=1000000)
        _enable_debug(child)
        child.expect([r"[\$#>]\s*$", r"\]\s*[\$#]"], timeout=15)

        # SSH to target node and execute the command directly there.
        child.sendline(inner_ssh)

        idx = child.expect(
            [
                r"(?i)are you sure you want to continue connecting",
                "assword:",
                r"Connection closed",
                pexpect.TIMEOUT,
                pexpect.EOF,
            ],
            timeout=15,
        )
        if idx == 0:
            child.sendline("yes")
            idx = child.expect(["assword:", r"Connection closed", pexpect.TIMEOUT, pexpect.EOF], timeout=15)
            if idx != 0:
                print("Failed to reach target password prompt after host key confirmation", file=sys.stderr)
                child.close()
                sys.exit(1)
            child.sendline(LAB_PASS)
        elif idx == 1:
            child.sendline(LAB_PASS)
        elif idx == 2:
            print("Connection closed before target login completed", file=sys.stderr)
            child.close()
            sys.exit(1)
        elif idx == 3:
            print("TIMEOUT waiting for target password prompt", file=sys.stderr)
            child.close()
            sys.exit(1)
        elif idx == 4:
            print("Target SSH exited unexpectedly before authentication", file=sys.stderr)
            child.close()
            sys.exit(1)

        # Wait for the inner ssh to finish and return to the jump-host prompt.
        child.expect([r"[\$#>]\s*$", r"\]\s*[\$#]"], timeout=timeout)
        raw = child.before if child.before else ""

        marker = "__DONE_8k2m__"
        child.sendline(f"echo {marker}=$?")
        child.expect([f"{marker}=(\\d+)"], timeout=5)
        rc = int(child.match.group(1))

        # Clean output: drop the echoed inner ssh command if present.
        output = _clean(raw)
        # The first line may be the echoed command if stty -echo didn't work
        lines = output.splitlines()
        if lines and (target in lines[0] or command[:30] in lines[0] or marker in lines[0]):
            lines = lines[1:]
        output = "\n".join(lines).strip()

        child.sendline("exit")
        child.close()

        print(output)
        sys.exit(rc)

    except pexpect.TIMEOUT:
        print(f"TIMEOUT after {timeout}s", file=sys.stderr)
        try:
            child.close()
        except Exception:
            pass
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
