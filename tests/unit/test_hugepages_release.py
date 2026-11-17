"""The huge pages simplyblock sets on a host, given back when its last storage
node leaves.

Regression: 2026-11-13, three workers redeployed several times without a reboot.
The baseline, the pages the host reserved itself, is captured once and kept in
/var/run/simplyblock until the host reboots. On 2026-10-13 the hosts' own setup
had reserved 5922 pages, which was recorded. From 2026-11-09 that setup reserved
none, and nothing removed the record: a count below simplyblock's last total is
read as a reset, never as the host lowering its reservation. Every deployment
after that added its need to 5922 pages that were nobody's: one needing 3584
asked for 9728 on hosts with 19.7 GiB of RAM, got between 1824 and 5701, and
its storage pods stayed Pending on huge pages and then on memory. A reboot
would have removed the record; release_hugepages does when the control plane
says the host's last storage node has left, so each deployment captures the
baseline again.
"""
import logging
import os
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core import storage_node_ops, utils
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.snode_client import SNodeClientException


class _Host:
    """A host's huge page counts and simplyblock's records, under a directory."""

    def __init__(self, root):
        self.records = os.path.join(root, "run")
        self.sysfs = os.path.join(root, "sys")
        os.makedirs(self.records)
        os.makedirs(self.sysfs)
        #: Pages a running process holds, which the kernel does not free.
        self.in_use = {}

    def _count_file(self, node, page_size_kb=2048):
        return os.path.join(self.sysfs, f"node{node}")

    def count(self, node):
        with open(self._count_file(node)) as f:
            return int(f.read())

    def set_count(self, node, pages):
        with open(self._count_file(node), "w") as f:
            f.write(f"{pages}\n")

    def _kernel_write(self, node, pages, page_size_kb=2048):
        self.set_count(node, max(int(pages), self.in_use.get(node, 0)))

    def record(self, kind, node, pages):
        with open(os.path.join(self.records, f"hugepages_{kind}_node{node}"), "w") as f:
            f.write(str(pages))

    def recorded(self):
        return sorted(os.listdir(self.records))


@pytest.fixture
def host(tmp_path):
    h = _Host(str(tmp_path))
    with patch.object(utils, "_HUGEPAGES_BASELINE_DIR", h.records), \
            patch.object(utils, "_nr_hugepages_path", h._count_file), \
            patch.object(utils, "_write_nr_hugepages", h._kernel_write):
        yield h


def test_gives_back_what_simplyblock_added_and_removes_its_records(host):
    host.set_count(0, 3584)
    host.record("baseline", 0, 0)
    host.record("sb", 0, 3584)

    assert utils.release_hugepages() == {0: 0}

    assert host.count(0) == 0
    assert host.recorded() == []


def test_the_hosts_own_reservation_stays(host):
    host.set_count(0, 4608)
    host.record("baseline", 0, 1024)
    host.record("sb", 0, 4608)

    assert utils.release_hugepages() == {0: 1024}
    assert host.count(0) == 1024


def test_pages_added_on_top_since_are_the_hosts(host):
    """The rule set_hugepages_if_needed applies: a count above what simplyblock
    last wrote was raised by somebody else."""
    host.set_count(0, 4096)
    host.record("baseline", 0, 0)
    host.record("sb", 0, 3584)

    assert utils.release_hugepages() == {0: 512}


def test_a_count_already_below_the_baseline_is_not_raised(host):
    """The pages were reset since. Releasing never allocates."""
    host.set_count(0, 0)
    host.record("baseline", 0, 1024)
    host.record("sb", 0, 4608)

    assert utils.release_hugepages() == {0: 0}
    assert host.count(0) == 0
    assert host.recorded() == []


def test_each_numa_node_is_released_by_its_own_records(host):
    host.set_count(0, 3584)
    host.record("baseline", 0, 0)
    host.record("sb", 0, 3584)
    host.set_count(1, 5000)
    host.record("baseline", 1, 2000)
    host.record("sb", 1, 5000)

    assert utils.release_hugepages() == {0: 0, 1: 2000}


def test_pages_still_in_use_keep_their_records(host):
    """An SPDK that still runs holds its pages. The records say whose they
    are, so they stay until the count is down, and the caller is told."""
    host.set_count(0, 3584)
    host.record("baseline", 0, 0)
    host.record("sb", 0, 3584)
    host.in_use[0] = 2788

    with pytest.raises(utils.HugePagesNotReleasedError, match="NUMA 0: 2788 huge page"):
        utils.release_hugepages()

    assert host.count(0) == 2788
    assert host.recorded() == ["hugepages_baseline_node0", "hugepages_sb_node0"]


def test_a_baseline_nothing_was_allocated_on_is_only_forgotten(host):
    """The init Job captures the baseline before the agent allocates. A host
    left at that point has only its own pages."""
    host.set_count(0, 1024)
    host.record("baseline", 0, 1024)

    assert utils.release_hugepages() == {0: 1024}
    assert host.count(0) == 1024
    assert host.recorded() == []


def test_an_allocation_without_a_baseline_is_left_alone(host):
    """How much of the count is the host's own is not known, so nothing is
    freed and the record stays."""
    host.set_count(0, 3584)
    host.record("sb", 0, 3584)

    with pytest.raises(utils.HugePagesNotReleasedError, match="NUMA 0"):
        utils.release_hugepages()

    assert host.count(0) == 3584
    assert host.recorded() == ["hugepages_sb_node0"]


def test_a_host_simplyblock_never_touched_has_nothing_to_release(host):
    os.rmdir(host.records)
    assert utils.release_hugepages() == {}


def test_the_next_deployment_starts_from_the_hosts_own_count(host):
    """The 2026-11-13 sequence with the release in it: the second deployment
    asks for its own need, not for its need on top of the first one's pages."""
    need = 3500
    host.set_count(0, 0)
    utils.set_hugepages_if_needed(0, need)
    first = host.count(0)

    utils.release_hugepages()
    assert host.count(0) == 0

    utils.set_hugepages_if_needed(0, need)
    assert host.count(0) == first


def test_a_pool_the_kernel_could_not_fill_is_reported_with_both_numbers(host, caplog):
    """The kernel allocates what it can and reports no error. The agent's log
    names the recorded baseline, which is what was wrong on 2026-11-13."""
    host.set_count(0, 0)
    host.record("baseline", 0, 5922)
    host.record("sb", 0, 9728)
    with patch.object(utils, "_write_nr_hugepages", lambda node, pages, page_size_kb=2048: host.set_count(node, 5701)), \
            caplog.at_level(logging.ERROR):
        utils.set_hugepages_if_needed(0, 3584)

    message = next(r.getMessage() for r in caplog.records if "kernel allocated" in r.getMessage())
    assert "5922 recorded as the host's own" in message
    assert "kernel allocated 5701" in message
    assert "hugepages_baseline_node0" in message


def test_a_short_pool_on_a_host_with_no_baseline_is_a_memory_shortage(host, caplog):
    host.set_count(0, 0)
    with patch.object(utils, "_write_nr_hugepages", lambda node, pages, page_size_kb=2048: host.set_count(node, 5)), \
            caplog.at_level(logging.ERROR):
        utils.set_hugepages_if_needed(0, 100)

    message = next(r.getMessage() for r in caplog.records if "kernel allocated" in r.getMessage())
    assert "kernel allocated 5" in message
    assert "too little free or unfragmented memory" in message
    assert "left over" not in message


# --- the control plane: which removal releases the host -----------------------------

def _node(node_id, host_name="worker-1", ip="10.0.0.1", status=StorageNode.STATUS_ONLINE):
    node = SimpleNamespace(status=status, mgmt_ip=ip)
    node.get_id = lambda: node_id
    node._k8s_node_label = lambda: host_name
    node.agent = MagicMock()
    node.agent.release_hugepages.return_value = (True, None)
    node.client = lambda **kw: node.agent
    return node


def _release(leaving, all_nodes):
    db = MagicMock()
    db.get_storage_nodes.return_value = all_nodes
    with patch.object(storage_node_ops, "DBController", return_value=db):
        storage_node_ops._release_host_huge_pages(leaving)


def test_the_last_storage_node_of_a_host_releases_it():
    leaving = _node("n1")
    _release(leaving, [leaving, _node("n2", host_name="worker-2", ip="10.0.0.2")])
    leaving.agent.release_hugepages.assert_called_once_with()


def test_a_host_another_storage_node_still_runs_on_keeps_its_pages():
    """Several storage nodes share a host with two sockets or nodesPerSocket > 1,
    and the pages and their records are the host's, not the node's."""
    leaving = _node("n1")
    _release(leaving, [leaving, _node("n2")])
    leaving.agent.release_hugepages.assert_not_called()


def test_a_node_of_another_cluster_on_the_same_host_counts():
    leaving = _node("n1")
    other = _node("n9", host_name="worker-1", ip="10.9.9.9")
    _release(leaving, [leaving, other])
    leaving.agent.release_hugepages.assert_not_called()


def test_nodes_already_removed_do_not_hold_the_host():
    leaving = _node("n1")
    _release(leaving, [leaving, _node("n2", status=StorageNode.STATUS_REMOVED)])
    leaving.agent.release_hugepages.assert_called_once_with()


@pytest.mark.parametrize("answer", [
    SNodeClientException("Invalid http status: 404"),  # an agent that predates the call
    (False, "NUMA 0: 2788 huge page(s) still in use"),
])
def test_a_release_that_does_not_happen_is_reported_and_does_not_stop_the_removal(answer, caplog):
    leaving = _node("n1")
    if isinstance(answer, Exception):
        leaving.agent.release_hugepages.side_effect = answer
    else:
        leaving.agent.release_hugepages.return_value = answer

    _release(leaving, [leaving])

    assert any("were not released" in r.getMessage() and r.levelname == "WARNING" for r in caplog.records)
