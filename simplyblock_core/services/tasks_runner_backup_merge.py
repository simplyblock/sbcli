"""
tasks_runner_backup_merge.py - periodic service that evaluates backup policies
and triggers merges when retention limits are exceeded.
"""
import time

from simplyblock_core import constants, db_controller, utils
from simplyblock_core.controllers import activation_gate, backup_controller
from simplyblock_core.models.cluster import Cluster

logger = utils.get_logger(__name__)

db = db_controller.DBController()

def main():
    logger.info("Starting backup merge service...")
    while True:
        try:
            db.get_clusters()
        except Exception as e:
            logger.error(f"Failed to get clusters: {e}")
            time.sleep(3)
            continue
        clusters = db.get_clusters()
        for cl in clusters:
            if cl.status == Cluster.STATUS_IN_ACTIVATION:
                continue

            try:
                # A volume whose LVS a running site activation holds is left
                # for a later pass; one whose owner is not found too.
                activating = bool(activation_gate.site_activation_blocks(cl))
                owners = ({n.get_id(): n for n in db.get_storage_nodes_by_cluster_id(cl.get_id())}
                          if activating else {})
                lvols = db.get_lvols(cl.get_id())
                for lvol in lvols:
                    if activating and activation_gate.site_activation_blocks(cl, owners.get(lvol.node_id)):
                        continue
                    try:
                        backup_controller.evaluate_policy(lvol)
                    except Exception as e:
                        logger.error(f"Error evaluating policy for lvol {lvol.get_id()}: {e}")
                    try:
                        backup_controller.evaluate_schedule(lvol)
                    except Exception as e:
                        logger.error(f"Error evaluating schedule for lvol {lvol.get_id()}: {e}")
            except Exception as e:
                logger.error(f"Error processing cluster {cl.get_id()}: {e}")

        time.sleep(constants.BACKUP_MERGE_SERVICE_INTERVAL_SEC)


if __name__ == "__main__":
    main()
