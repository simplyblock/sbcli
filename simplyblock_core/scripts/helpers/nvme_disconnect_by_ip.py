#!/usr/bin/env python
# encoding: utf-8
import json
import logging
import os
import subprocess
import sys

logger = logging.getLogger(__name__)


def run_command(cmd):
    try:
        process = subprocess.Popen(
            cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        return stdout.strip().decode("utf-8"), stderr.strip(), process.returncode
    except Exception as e:
        return "", str(e), 1


def disconnect_by_ip(node_ip):
    out, err, rc = run_command("nvme list -v -o json")
    if rc != 0:
        logger.error("Error getting nvme list")
        logger.error(err)
        return []
    data = json.loads(out)
    devices = []
    if data and 'Devices' in data and data['Devices']:
        for dev in data['Devices'][0]['Subsystems']:
            if 'Controllers' in dev and dev['Controllers']:
                for controller in dev['Controllers']:
                    adr = controller['Address']
                    if node_ip in adr:
                        devices.append(controller)

    logger.info(f"Found {len(devices)} controllers")
    for dev in devices:
        out, err, rc = run_command(f"nvme disconnect -d {dev['Controller']}")
        if rc != 0:
            logger.error(f"Error disconnecting {dev['Controller']}")
            logger.error(err)


if __name__ == '__main__':
    disconnect_by_ip(sys.argv[1])
