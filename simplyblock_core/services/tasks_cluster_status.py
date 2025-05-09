import subprocess
import logging
import json
import sys
import time
from graypy import GELFTCPHandler
from simplyblock_core import constants

cluster_commands = [
    "sbcli-dev cluster show",
    "sbcli-dev cluster get-capacity",
    "sbcli-dev cluster get-io-stats",
    "sbcli-dev cluster get-logs",
    "sbcli-dev lvol list --cluster-id",
]

def setup_logger():
    """Set up the custom logger."""
    logger_handler = logging.StreamHandler(stream=sys.stdout)
    logger_handler.setFormatter(logging.Formatter('%(asctime)s: %(levelname)s: %(message)s'))
    gelf_handler = GELFTCPHandler('0.0.0.0', constants.GELF_PORT)
    logger = logging.getLogger()
    logger.addHandler(gelf_handler)
    logger.addHandler(logger_handler)
    logger.setLevel(logging.DEBUG)
    return logger

def execute_command(command):
    """Execute a shell command and return the result."""
    result = subprocess.run(command, shell=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return result

def run_cluster_commands():
    """Fetch cluster ID and execute commands, logging results."""
    try:
        cluster_id_cmd = "sbcli-dev cluster list | grep simplyblock | awk '{print $2}'"
        logger.info("Fetching cluster ID.")
        result = execute_command(cluster_id_cmd)
        cluster_id = result.stdout.strip()
        logger.info(f"Cluster ID: {cluster_id}")
        if not cluster_id:
            logger.error("Cluster ID not found.")
            return

        for command in cluster_commands:
            full_command = f"{command} {cluster_id}"
            logger.info(f"Executing command: {full_command}")
            result = execute_command(full_command)
            logger.debug(f"Output: {result.stdout}")
            if result.stderr:
                logger.error(f"Error: {result.stderr}")
    except Exception as e:
        logger.critical(f"Exception occurred: {e}")

def get_storage_node_ids():
    """Fetch the JSON output and extract storage node IDs."""
    try:
        command = "sbcli-dev storage-node list --json"
        result = execute_command(command)
        if result.stderr:
            logger.error(f"Error fetching storage node list: {result.stderr}")
            return []

        storage_nodes = json.loads(result.stdout)
        node_ids = [node["UUID"] for node in storage_nodes]
        logger.info(f"Fetched {len(node_ids)} storage node IDs.")
        return node_ids
    except Exception as e:
        logger.critical(f"Exception occurred while fetching storage node IDs: {e}")
        return []

def check_storage_node(node_id):
    """Run the storage-node check command for the given node ID."""
    try:
        command = f"sbcli-dev storage-node check {node_id}"
        logger.info(f"Checking storage node: {node_id}")
        result = execute_command(command)
        if result.stdout:
            logger.debug(f"Output for node {node_id}:\n{result.stdout}")
        if result.stderr:
            logger.error(f"Error for node {node_id}:\n{result.stderr}")
    except Exception as e:
        logger.critical(f"Exception occurred while checking storage node {node_id}: {e}")

if __name__ == "__main__":
    logger = setup_logger()
    logger.info("Starting SBCLI worker.")

    # this for every 5 minutes
    while True:
        logger.info("Running cluster commands")
        run_cluster_commands()

        logger.info("Running storage node checks")
        storage_node_ids = get_storage_node_ids()
        for node_id in storage_node_ids:
            check_storage_node(node_id)
        logger.info("Sleeping for 5 minutes...")
        time.sleep(300)
