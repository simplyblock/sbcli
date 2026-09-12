"""Volume creation uses fixed (thick) provisioning by default.

`create_lvol` (the bdev_lvol_create RPC) and `bdev_lvol_register` (the replica /
recreate registration of the same blob) previously hard-coded
`thin_provision: True`. They now default to fixed provisioning -- the blob's
clusters are allocated up front at create time instead of on first write --
and the two must agree, because a replica registers the same blob its primary
created.
"""
import inspect

from simplyblock_core.rpc_client import RPCClient


class _Capt(RPCClient):
    """RPCClient that captures the RPC name + params instead of sending them."""
    def __init__(self):
        self.calls = []

    def _request(self, method, params=None, **kw):
        self.calls.append((method, params or {}))
        return True

    def _request2(self, method, params=None, **kw):
        self.calls.append((method, params or {}))
        return True


class TestCreateLvolProvisioning:
    def test_create_lvol_defaults_to_fixed(self):
        c = _Capt()
        c.create_lvol("v1", 1024, "LVS_1")
        method, params = c.calls[-1]
        assert method == "bdev_lvol_create"
        assert params["thin_provision"] is False, "default must be fixed (thick)"

    def test_create_lvol_can_opt_into_thin(self):
        c = _Capt()
        c.create_lvol("v1", 1024, "LVS_1", thin_provision=True)
        assert c.calls[-1][1]["thin_provision"] is True

    def test_register_defaults_to_fixed(self):
        c = _Capt()
        c.bdev_lvol_register("v1", "LVS_1", "uuid-1", 42)
        method, params = c.calls[-1]
        assert method == "bdev_lvol_register"
        assert params["thin_provision"] is False

    def test_register_can_opt_into_thin(self):
        c = _Capt()
        c.bdev_lvol_register("v1", "LVS_1", "uuid-1", 42, thin_provision=True)
        assert c.calls[-1][1]["thin_provision"] is True

    def test_create_and_register_share_the_default(self):
        """A replica must register with the same provisioning as its primary."""
        sig_c = inspect.signature(RPCClient.create_lvol)
        sig_r = inspect.signature(RPCClient.bdev_lvol_register)
        assert sig_c.parameters["thin_provision"].default is False
        assert sig_r.parameters["thin_provision"].default is False

    def test_no_hardcoded_thin_true_remains(self):
        src = inspect.getsource(RPCClient.create_lvol) + \
            inspect.getsource(RPCClient.bdev_lvol_register)
        assert '"thin_provision": True' not in src
