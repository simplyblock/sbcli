#!/usr/bin/env python3
"""Run a command on a remote lab node via jump host using pexpect.

Usage: ssh_run.py <command> <target_ip> [timeout]

Hop 1: SSH to jump host (key auth) - 95.216.93.11:13987
Hop 2: From jump host, SSH to lab node (password auth via pexpect)
"""
import re
import sys
import os
import pexpect

JUMP_HOST = "95.216.93.11"
JUMP_PORT = "13987"
JUMP_USER = "simplyblock"
JUMP_KEY = os.path.expanduser("~/simplyblock")

LAB_USER = "root"
LAB_PASS = "3tango11"

_ANSI_RE = re.compile(r'(\x1b\[[^a-zA-Z]*[a-zA-Z]|\x1b\][^\x07]*\x07|\x1b\(B|\x1b\[[\?0-9]*[a-z])')


def _clean(text):
    return _ANSI_RE.sub('', text).replace('\r', '').strip()


def main():
    if len(sys.argv) < 3:
        print("Usage: ssh_run.py <command> <target_ip> [timeout]", file=sys.stderr)
        sys.exit(1)

    command = sys.argv[1]
    target = sys.argv[2]
    timeout = int(sys.argv[3]) if len(sys.argv) > 3 else 300

    jump_ssh = (
        f"ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 "
        f"-p {JUMP_PORT} -i {JUMP_KEY} {JUMP_USER}@{JUMP_HOST}"
    )

    try:
        child = pexpect.spawn(jump_ssh, timeout=30, encoding="utf-8", maxread=1000000)
        child.expect([r"[\$#>]\s*$", r"\]\s*[\$#]"], timeout=15)

        # SSH to target node
        inner_ssh = f"ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 {LAB_USER}@{target}"
        child.sendline(inner_ssh)

        idx = child.expect(["assword:", r"[\$#]\s*$", pexpect.TIMEOUT], timeout=15)
        if idx == 0:
            child.sendline(LAB_PASS)
            child.expect([r"[\$#]\s*$"], timeout=15)
        elif idx == 2:
            print("TIMEOUT waiting for password prompt or shell", file=sys.stderr)
            child.close()
            sys.exit(1)

        # Disable command echo, run command, capture with unique end marker
        marker = "__DONE_8k2m__"
        child.sendline("set +o history; stty -echo 2>/dev/null; export PS1='# '")
        child.expect([r"#\s*$"], timeout=5)

        child.sendline(f"{command}; echo {marker}=$?")
        child.expect([f"{marker}=(\\d+)"], timeout=timeout)
        rc = int(child.match.group(1))
        raw = child.before if child.before else ""

        # Clean output: drop first line (the echoed command) and clean ANSI
        output = _clean(raw)
        # The first line may be the echoed command if stty -echo didn't work
        lines = output.splitlines()
        if lines and (command[:30] in lines[0] or marker in lines[0]):
            lines = lines[1:]
        output = "\n".join(lines).strip()

        child.sendline("exit")
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
