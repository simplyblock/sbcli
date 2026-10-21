"""delete_backup removes one backup record, and refuses what would break a restore,
against real FoundationDB.

Deleting a backup another one chains against makes every descendant
unrestorable, and deleting a member of a group backup that still exists tears
that group backup. Both are refused with an exception rather than reported in a
return value, so a caller cannot forget to look.

The database is real. The only boundary patched is the storage node's RPC
client, which the integration tier never reaches a real node for.
"""
import uuid as uuid_mod
import zlib
from unittest.mock import patch

import pytest

from simplyblock_core.controllers.backup import controller as backup_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.models.backup import Backup, GroupBackup
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCException


def _uuid(name: str) -> str:
    return f"{zlib.crc32(name.encode()):08x}-0000-4000-8000-000000000000"


CLUSTER = _uuid("cluster")
LVOL = _uuid("lvol")


@pytest.fixture
def db():
    return DBController()


@pytest.fixture
def node(db):
    n = StorageNode()
    n.uuid = str(uuid_mod.uuid4())
    n.cluster_id = CLUSTER
    n.status = StorageNode.STATUS_ONLINE
    n.hostname = "delete-backup-host"
    n.mgmt_ip = "127.0.0.1"
    n.write_to_db(db.kv_store)
    return n


def _backup(db, node, name, prev="", group_backup_id=""):
    b = Backup()
    b.uuid = _uuid(name)
    b.cluster_id = CLUSTER
    b.lvol_id = LVOL
    b.node_id = node.uuid
    b.s3_id = zlib.crc32(name.encode()) % 1000
    b.prev_backup_id = _uuid(prev) if prev else ""
    b.group_backup_id = group_backup_id
    b.status = Backup.STATUS_COMPLETED
    b.write_to_db(db.kv_store)
    return b


def _group_backup(db):
    gb = GroupBackup()
    gb.uuid = _uuid("gb")
    gb.cluster_id = CLUSTER
    gb.status = GroupBackup.STATUS_COMPLETED
    gb.write_to_db(db.kv_store)
    return gb


def _exists(db, backup):
    try:
        db.get_backup_by_id(backup.uuid)
        return True
    except KeyError:
        return False


def test_a_backup_nothing_depends_on_is_deleted(db, node):
    leaf = _backup(db, node, "leaf")
    assert backup_controller.delete_backup(leaf.uuid) is None
    assert not _exists(db, leaf)


def test_a_backup_another_chains_against_is_refused(db, node):
    base = _backup(db, node, "base")
    _backup(db, node, "next", prev="base")
    with pytest.raises(PreconditionError, match="predecessor"):
        backup_controller.delete_backup(base.uuid)
    assert _exists(db, base)


def test_a_member_of_a_group_backup_that_exists_is_refused(db, node):
    gb = _group_backup(db)
    member = _backup(db, node, "member", group_backup_id=gb.get_id())
    with pytest.raises(PreconditionError, match="group backup"):
        backup_controller.delete_backup(member.uuid)
    assert _exists(db, member)


def test_a_member_whose_group_backup_is_gone_is_deleted(db, node):
    orphan = _backup(db, node, "orphan", group_backup_id=f"{CLUSTER}/{_uuid('gone')}")
    backup_controller.delete_backup(orphan.uuid)
    assert not _exists(db, orphan)


def test_an_unknown_backup_raises(db):
    with pytest.raises(KeyError):
        backup_controller.delete_backup(_uuid("nope"))


def test_a_failed_s3_delete_still_deletes_the_record(db, node):
    """The S3 delete is best effort: the data plane does not implement it yet."""
    leaf = _backup(db, node, "leaf")
    with patch.object(StorageNode, "rpc_client") as rpc:
        rpc.return_value.bdev_lvol_s3_delete.side_effect = RPCException("no such method")
        backup_controller.delete_backup(leaf.uuid)
    assert not _exists(db, leaf)
