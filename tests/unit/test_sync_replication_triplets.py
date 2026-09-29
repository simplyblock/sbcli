"""Remote-triplet selection and the JC-context cap of sync replication (pure).

The planner parts only: ``pick_remote_triplet``, ``jc_contexts_per_node`` /
``jc_context_violation`` and ``apply_role_moves``. Everything that reads node
records lives in ``tests/integration/test_sync_replication_triplets.py``.
"""
import pytest

from simplyblock_core.controllers.cluster_expansion.planner import (
    JC_MAX_CONTEXTS_PER_NODE,
    ROLE_PRIMARY,
    ROLE_SECONDARY,
    ROLE_TERTIARY,
    RemoteTripletPlacementError,
    RoleMove,
    SiteNode,
    apply_role_moves,
    jc_context_violation,
    jc_contexts_per_node,
    pick_remote_triplet,
)


def _site(n, *, per_host=1, fd=None, label=None):
    """``n`` nodes b0..b(n-1); ``fd`` / ``label`` map the node index to a value."""
    return [SiteNode(f"b{i}", f"h{i // per_host}",
                     fd(i) if fd else -1, label(i) if label else 0)
            for i in range(n)]


def _pick_many(candidates, owners, **kwargs):
    load: dict[str, list[int]] = {}
    return [pick_remote_triplet(candidates, load, **kwargs) for _ in range(owners)], load


def _role_counts(candidates, load):
    return [sorted(load.get(c.node_id, [0, 0, 0])[slot] for c in candidates) for slot in range(3)]


class TestPickRemoteTriplet:

    def test_three_members_on_three_hosts(self):
        (triplet,), _ = _pick_many(_site(3), 1)
        assert sorted(triplet) == ["b0", "b1", "b2"]

    def test_members_are_host_disjoint_on_multi_node_hosts(self):
        candidates = _site(6, per_host=2)
        host = {c.node_id: c.host for c in candidates}
        triplets, _ = _pick_many(candidates, 6)
        for triplet in triplets:
            assert len({host[m] for m in triplet}) == 3

    def test_two_hosts_are_too_few(self):
        with pytest.raises(RemoteTripletPlacementError, match="remote tertiary"):
            pick_remote_triplet(_site(4, per_host=2), {})

    def test_no_candidate_at_all(self):
        with pytest.raises(RemoteTripletPlacementError, match="remote primary.*0 candidate"):
            pick_remote_triplet([], {})

    @pytest.mark.parametrize("sites", [(3, 3), (4, 4), (6, 6), (8, 8)])
    def test_equal_sites_give_every_node_one_role_of_each_kind(self, sites):
        owners, nodes = sites
        candidates = _site(nodes)
        _, load = _pick_many(candidates, owners)
        assert _role_counts(candidates, load) == [[1] * nodes] * 3

    def test_more_owners_than_nodes_spread_evenly(self):
        candidates = _site(3)
        _, load = _pick_many(candidates, 6)
        assert _role_counts(candidates, load) == [[2, 2, 2]] * 3

    def test_fewer_owners_than_nodes_spread_every_role(self):
        """Three owners, six nodes: nine roles, no node carries two of a kind
        and no node carries more than one role above another."""
        candidates = _site(6)
        _, load = _pick_many(candidates, 3)
        totals = [sum(load.get(c.node_id, [0, 0, 0])) for c in candidates]
        assert sum(totals) == 9 and max(totals) - min(totals) <= 1
        assert all(max(counts) == 1 for counts in _role_counts(candidates, load))

    def test_existing_load_steers_the_pick(self):
        load = {"b0": [3, 0, 0], "b1": [0, 0, 0], "b2": [0, 0, 0], "b3": [0, 0, 0]}
        triplet = pick_remote_triplet(_site(4), load)
        assert triplet[0] != "b0"
        assert load["b0"][0] == 3

    def test_members_are_failure_domain_diverse_when_possible(self):
        candidates = _site(6, fd=lambda i: i // 2)  # domains 0,0,1,1,2,2
        fd = {c.node_id: c.failure_domain for c in candidates}
        triplets, _ = _pick_many(candidates, 6, fd_on=True)
        for triplet in triplets:
            assert len({fd[m] for m in triplet}) == 3

    def test_failure_domains_are_ignored_when_off(self):
        candidates = _site(6, fd=lambda i: i // 2)
        fd = {c.node_id: c.failure_domain for c in candidates}
        (triplet,), _ = _pick_many(candidates, 1, fd_on=False)
        assert triplet == ("b0", "b1", "b2")
        assert len({fd[m] for m in triplet}) == 2

    def test_too_few_domains_relax_to_host_disjoint(self):
        candidates = _site(4, fd=lambda i: i % 2)
        (triplet,), _ = _pick_many(candidates, 1, fd_on=True)
        assert len(set(triplet)) == 3

    def test_physical_labels_are_diverse_when_possible(self):
        candidates = _site(6, label=lambda i: 1 + i // 2)
        label = {c.node_id: c.physical_label for c in candidates}
        triplets, _ = _pick_many(candidates, 6)
        for triplet in triplets:
            assert len({label[m] for m in triplet}) == 3

    def test_kept_members_stay_and_only_the_empty_slot_is_filled(self):
        candidates = _site(4)
        load: dict[str, list[int]] = {}
        triplet = pick_remote_triplet(candidates, load,
                                      keep=(candidates[2], None, candidates[0]))
        assert triplet[0] == "b2" and triplet[2] == "b0"
        assert triplet[1] in ("b1", "b3")
        assert load["b2"] == [1, 0, 0] and load["b0"] == [0, 0, 1]

    def test_kept_member_need_not_be_a_candidate(self):
        offline = SiteNode("b9", "h9")
        triplet = pick_remote_triplet(_site(3), {}, keep=(offline, None, None))
        assert triplet[0] == "b9"
        assert set(triplet[1:]) <= {"b0", "b1", "b2"}

    def test_a_new_member_avoids_the_host_of_a_kept_one(self):
        kept = SiteNode("b9", "h0")  # shares host h0 with candidate b0
        triplet = pick_remote_triplet(_site(3), {}, keep=(kept, None, None))
        assert "b0" not in triplet

    def test_a_full_keep_changes_nothing_but_the_load(self):
        candidates = _site(3)
        load: dict[str, list[int]] = {}
        assert pick_remote_triplet(candidates, load, keep=tuple(candidates)) == ("b0", "b1", "b2")
        assert load == {"b0": [1, 0, 0], "b1": [0, 1, 0], "b2": [0, 0, 1]}

    def test_keep_needs_three_slots(self):
        with pytest.raises(ValueError, match="3 slots"):
            pick_remote_triplet(_site(3), {}, keep=(None, None))


class TestJcContexts:

    def test_owner_and_every_distinct_member_count_once(self):
        counts = jc_contexts_per_node({
            "a0": ["a1", "a2", "b0", "b1", "b2"],
            "a1": ["a2", "", "b1", "b2", "b0"],
        })
        assert counts == {"a0": 1, "a1": 2, "a2": 2, "b0": 2, "b1": 2, "b2": 2}

    def test_a_member_named_twice_counts_once(self):
        assert jc_contexts_per_node({"a0": ["b0", "b0"]}) == {"a0": 1, "b0": 1}

    def test_the_cap_is_the_jc_replace_jm_limit(self):
        assert JC_MAX_CONTEXTS_PER_NODE == 16
        assert jc_context_violation({"n1": 16, "n2": 3}) is None
        reason = jc_context_violation({"n1": 17, "n2": 18, "n3": 16})
        assert reason is not None
        assert "n1 (17)" in reason and "n2 (18)" in reason and "n3" not in reason

    def test_an_unbalanced_two_site_cluster_exceeds_the_cap(self):
        """20 owners on one site, 4 nodes on the other: 15 remote roles each
        plus their own local three."""
        a = [f"a{i}" for i in range(20)]
        remote = _site(4)
        load: dict[str, list[int]] = {}
        instances = {}
        for i, owner in enumerate(a):
            local = [a[(i + 1) % 20], a[(i + 2) % 20]]
            instances[owner] = [*local, *pick_remote_triplet(remote, load)]
        b = ["b0", "b1", "b2", "b3"]
        for i, owner in enumerate(b):
            instances[owner] = [b[(i + 1) % 4], b[(i + 2) % 4]]
        counts = jc_contexts_per_node(instances)
        assert counts["b0"] == 3 + 15
        assert "b0 (18)" in jc_context_violation(counts)


class TestApplyRoleMoves:

    def test_final_layout_of_a_plan(self):
        layout = {"n1": ("n2", "n3"), "n2": ("n3", "n1"), "n3": ("n1", "n2")}
        moves = [
            RoleMove("n3", ROLE_SECONDARY, "n1", "n4"),
            RoleMove("n3", ROLE_TERTIARY, "n2", "n1"),
            RoleMove("n4", ROLE_PRIMARY, "", "n4"),
            RoleMove("n4", ROLE_SECONDARY, "", "n1"),
            RoleMove("n4", ROLE_TERTIARY, "", "n2"),
        ]
        assert apply_role_moves(layout, moves) == {
            "n1": ("n2", "n3"), "n2": ("n3", "n1"), "n3": ("n4", "n1"), "n4": ("n1", "n2")}

    def test_applying_to_a_partly_executed_layout_gives_the_same_result(self):
        layout = {"n1": ("n2", ""), "n2": ("n1", "")}
        moves = [RoleMove("n2", ROLE_SECONDARY, "n1", "n3"),
                 RoleMove("n3", ROLE_PRIMARY, "", "n3"),
                 RoleMove("n3", ROLE_SECONDARY, "", "n1")]
        done_first = apply_role_moves(layout, moves[:1])
        assert apply_role_moves(done_first, moves) == apply_role_moves(layout, moves)

    def test_input_is_not_mutated(self):
        layout = {"n1": ("n2", "")}
        apply_role_moves(layout, [RoleMove("n1", ROLE_SECONDARY, "n2", "n3")])
        assert layout == {"n1": ("n2", "")}
