#!/usr/bin/env python3
"""Disable (or re-enable) node auto-restart during a long activation.

During a post-suspension activation the lvstore recovery replays the JM
journal, which with a large journal takes many minutes per lvstore. Any
machinery that kills SPDK meanwhile (auto-restart re-queue, a queued
node_restart task) discards the replay and restarts it from record zero, so
activation can never converge. 26.2.11 has no CLI for the per-node
``auto_restart_disabled`` flag, hence this script.

Disabling does two things per node, because the flag only stops NEW restarts
from being queued (``add_node_to_auto_restart`` and the monitor's re-queue
scan both honor it) while a task that is ALREADY queued would still run:

  1. sets ``auto_restart_disabled = True`` on the node record;
  2. cancels every non-done ``node_restart`` task targeting the node.

Run inside the control-plane container:

    python3 scripts/set_auto_restart.py disable <node-id> [<node-id> ...]
    python3 scripts/set_auto_restart.py enable  <node-id> [<node-id> ...]
    python3 scripts/set_auto_restart.py status  <cluster-id>

IMPORTANT: re-enable after the activation has completed. While disabled, a
genuinely dead SPDK on these nodes will NOT be brought back automatically --
that is the point, and it is also the risk.
"""
import sys

from simplyblock_core.db_controller import DBController
from simplyblock_core.models.job_schedule import JobSchedule


def _cancel_restart_tasks(db, node):
    cancelled = 0
    for task in db.get_job_tasks(node.cluster_id):
        if task.function_name != JobSchedule.FN_NODE_RESTART:
            continue
        if task.node_id != node.get_id():
            continue
        if task.status == JobSchedule.STATUS_DONE or task.canceled:
            continue
        task.canceled = True
        task.function_result = "canceled: auto-restart disabled for activation"
        task.write_to_db(db.kv_store)
        cancelled += 1
        print(f"  canceled node_restart task {task.uuid} (was {task.status})")
    return cancelled


def main():
    if len(sys.argv) < 3 or sys.argv[1] not in ("disable", "enable", "status"):
        print(__doc__)
        return 2
    action = sys.argv[1]
    db = DBController()

    if action == "status":
        cluster_id = sys.argv[2]
        for node in db.get_storage_nodes_by_cluster_id(cluster_id):
            open_tasks = [t for t in db.get_job_tasks(cluster_id)
                          if t.function_name == JobSchedule.FN_NODE_RESTART
                          and t.node_id == node.get_id()
                          and t.status != JobSchedule.STATUS_DONE
                          and not t.canceled]
            print(f"{node.get_id()}  status={node.status}  "
                  f"auto_restart_disabled={node.auto_restart_disabled}  "
                  f"open_restart_tasks={len(open_tasks)}")
        return 0

    for node_id in sys.argv[2:]:
        node = db.get_storage_node_by_id(node_id)
        if not node:
            print(f"node not found: {node_id}")
            return 1
        if action == "disable":
            node.auto_restart_disabled = True
            node.write_to_db(db.kv_store)
            print(f"{node_id}: auto_restart_disabled=True")
            _cancel_restart_tasks(db, node)
        else:
            node.auto_restart_disabled = False
            node.write_to_db(db.kv_store)
            print(f"{node_id}: auto_restart_disabled=False (auto-restart active again)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
