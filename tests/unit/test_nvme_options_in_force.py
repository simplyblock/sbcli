"""bdev_nvme global options must actually be in force on every SPDK instance.

spdk_bdev_nvme_set_opts() refuses once any NVMe bdev controller is attached::

    if (g_bdev_nvme_init_thread != NULL) {
        if (!TAILQ_EMPTY(&g_nvme_bdev_ctrlrs)) {
            return -EPERM;
        }
    }

so the options can only ever be set BEFORE the first attach. An SPDK that
comes up without the control plane's init sequence keeps SPDK's compiled-in
defaults for its whole lifetime, and nothing later can reopen the window.

2026-09-05, node 4424: the instance serving from 19:23:44 to 22:08:48 received
none of the init RPCs -- 0 bdev_nvme_set_options, 0 transport_create,
0 framework_start_init, 0 bdev_set_options, 0 thread_set_cpumask -- yet took
4 nvmf_create_subsystem and 13 bdev_nvme_attach_controller calls. It therefore
ran with timeout_us=0 (no command timeout armed at all),
transport_ack_timeout=0 (no TCP_USER_TIMEOUT) and the default keep-alive
interval. When node 4422 was container_stopped it took 7.11 s to notice a peer
that had RST other sockets within 255 ms.
"""
import inspect

from simplyblock_core import rpc_client, storage_node_ops


class _Rpc:
    def __init__(self, effective=None, set_ok=False, get_raises=False):
        self._effective = effective
        self._set_ok = set_ok
        self._get_raises = get_raises
        self.set_calls = 0

    def get_effective_nvme_options(self):
        if self._get_raises:
            raise RuntimeError("rpc down")
        return self._effective if self._effective is not None else {}

    def bdev_nvme_set_options(self):
        self.set_calls += 1
        return self._set_ok


class _Node:
    def __init__(self, rpc):
        self._rpc = rpc
        self.online_since = "2026-09-05T19:23:44+00:00"

    def get_id(self):
        return "8c2df2a0-c76a-4e73-9141-d594d28f50d6"

    def rpc_client(self, **kw):
        return self._rpc


def _intended():
    return dict(rpc_client.nvme_bdev_opts_params())


class TestSingleSourceOfTruth:
    def test_the_setter_uses_the_shared_params(self):
        src = inspect.getsource(rpc_client.RPCClient.bdev_nvme_set_options)
        assert "nvme_bdev_opts_params()" in src

    def test_the_intended_options_carry_the_critical_knobs(self):
        p = _intended()
        for k in storage_node_ops.NVME_OPTS_CRITICAL_KEYS:
            assert k in p, k

    def test_a_command_timeout_is_actually_intended(self):
        """timeout_us == 0 means SPDK registers no timeout callback at all."""
        assert _intended()["timeout_us"] > 0

    def test_an_ack_timeout_is_actually_intended(self):
        """transport_ack_timeout == 0 means no TCP_USER_TIMEOUT is set."""
        assert _intended()["transport_ack_timeout"] > 0


class TestVerificationOfWhatIsInForce:
    def test_matching_options_are_a_single_cheap_check(self):
        rpc = _Rpc(effective=_intended())
        ok, drift = storage_node_ops.ensure_nvme_options(_Node(rpc))
        assert ok and drift == {}
        assert rpc.set_calls == 0, "no need to set when already in force"

    def test_the_incident_defaults_are_detected(self):
        """Exactly what node 4424 was running: no timeout, no ack timeout."""
        eff = _intended()
        eff.update({"timeout_us": 0, "transport_ack_timeout": 0,
                    "keep_alive_timeout_ms": 10000})
        ok, drift = storage_node_ops.ensure_nvme_options(_Node(_Rpc(effective=eff)))
        assert not ok
        assert set(drift) >= {"timeout_us", "transport_ack_timeout",
                              "keep_alive_timeout_ms"}
        assert drift["timeout_us"] == (0, _intended()["timeout_us"])

    def test_drift_is_repaired_while_the_window_is_still_open(self):
        """No controller attached yet -> set_options succeeds, so fix it."""
        eff = _intended()
        eff["timeout_us"] = 0
        rpc = _Rpc(effective=eff, set_ok=True)
        ok, drift = storage_node_ops.ensure_nvme_options(_Node(rpc))
        assert ok and "timeout_us" in drift
        assert rpc.set_calls == 1

    def test_unfixable_drift_reports_failure(self):
        """set_options refused (-EPERM): controllers exist, window is shut."""
        eff = _intended()
        eff["timeout_us"] = 0
        rpc = _Rpc(effective=eff, set_ok=False)
        ok, drift = storage_node_ops.ensure_nvme_options(_Node(rpc))
        assert not ok and "timeout_us" in drift

    def test_an_unreadable_config_does_not_guess(self):
        ok, drift = storage_node_ops.ensure_nvme_options(
            _Node(_Rpc(get_raises=True)))
        assert not ok and drift == {}

    def test_an_empty_config_does_not_blind_set(self):
        rpc = _Rpc(effective={})
        ok, _ = storage_node_ops.ensure_nvme_options(_Node(rpc))
        assert not ok
        assert rpc.set_calls == 0, "a blind set would just log EPERM noise"

    def test_the_eperm_constraint_is_documented(self):
        doc = storage_node_ops.ensure_nvme_options.__doc__ or ""
        assert "EPERM" in doc
        assert "attach" in doc


class TestReadbackPath:
    def test_effective_options_come_from_the_bdev_subsystem_config(self):
        src = inspect.getsource(rpc_client.RPCClient.get_effective_nvme_options)
        assert 'framework_get_config("bdev")' in src
        assert "bdev_nvme_set_options" in src

    def test_it_picks_the_params_of_the_right_method(self):
        class C(rpc_client.RPCClient):
            def __init__(self):
                pass

            def framework_get_config(self, name):
                return [
                    {"method": "bdev_set_options", "params": {"x": 1}},
                    {"method": "bdev_nvme_set_options",
                     "params": {"timeout_us": 8000000}},
                ]

        assert C().get_effective_nvme_options() == {"timeout_us": 8000000}

    def test_a_dict_shaped_response_is_tolerated(self):
        class C(rpc_client.RPCClient):
            def __init__(self):
                pass

            def framework_get_config(self, name):
                return {"config": [{"method": "bdev_nvme_set_options",
                                    "params": {"timeout_us": 1}}]}

        assert C().get_effective_nvme_options() == {"timeout_us": 1}

    def test_absent_entry_returns_empty(self):
        class C(rpc_client.RPCClient):
            def __init__(self):
                pass

            def framework_get_config(self, name):
                return [{"method": "bdev_set_options", "params": {}}]

        assert C().get_effective_nvme_options() == {}


class TestMonitorChecksOncePerOnlineEpisode:
    def test_the_hook_is_called_from_the_rpc_check(self):
        from simplyblock_core.services import storage_node_monitor
        src = inspect.getsource(storage_node_monitor)
        i = src.index("_note_rpc_ok(snode.get_id())")
        assert "_verify_nvme_options_once(snode)" in src[i:i + 900]

    def test_a_clean_result_is_cached_by_online_since(self, monkeypatch):
        from simplyblock_core.services import storage_node_monitor as m
        calls = {"n": 0}

        def _ok(snode, context=""):
            calls["n"] += 1
            return True, {}

        monkeypatch.setattr(storage_node_ops, "ensure_nvme_options", _ok)
        m._nvme_opts_verified.clear()
        node = _Node(_Rpc())
        m._verify_nvme_options_once(node)
        m._verify_nvme_options_once(node)
        assert calls["n"] == 1, "one RPC per online episode, not per tick"

    def test_drift_is_re_reported_every_episode(self, monkeypatch):
        """A node with no command timeout must not go quiet."""
        from simplyblock_core.services import storage_node_monitor as m
        calls = {"n": 0}

        def _bad(snode, context=""):
            calls["n"] += 1
            return False, {"timeout_us": (0, 8000000)}

        monkeypatch.setattr(storage_node_ops, "ensure_nvme_options", _bad)
        m._nvme_opts_verified.clear()
        node = _Node(_Rpc())
        m._verify_nvme_options_once(node)
        m._verify_nvme_options_once(node)
        assert calls["n"] == 2

    def test_a_new_online_episode_re_verifies(self, monkeypatch):
        from simplyblock_core.services import storage_node_monitor as m
        calls = {"n": 0}

        def _ok(snode, context=""):
            calls["n"] += 1
            return True, {}

        monkeypatch.setattr(storage_node_ops, "ensure_nvme_options", _ok)
        m._nvme_opts_verified.clear()
        node = _Node(_Rpc())
        m._verify_nvme_options_once(node)
        node.online_since = "2026-09-05T22:10:10+00:00"   # SPDK restarted
        m._verify_nvme_options_once(node)
        assert calls["n"] == 2

    def test_a_raising_check_cannot_break_the_monitor(self, monkeypatch):
        from simplyblock_core.services import storage_node_monitor as m

        def _boom(snode, context=""):
            raise RuntimeError("nope")

        monkeypatch.setattr(storage_node_ops, "ensure_nvme_options", _boom)
        m._nvme_opts_verified.clear()
        m._verify_nvme_options_once(_Node(_Rpc()))   # must not raise
