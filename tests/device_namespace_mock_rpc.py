# coding=utf-8
import json
import logging
import random
import threading
import uuid as _uuid_mod
from http.server import BaseHTTPRequestHandler, HTTPServer


logger = logging.getLogger(__name__)


class _RpcError(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code
        self.message = message


class NamespaceNodeState:
    def __init__(self, node_id, host="127.0.0.1", port=0):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.lock = threading.Lock()
        self.reset()

    def reset(self):
        self.subsystems = {}
        self.local_bdevs = {}
        self.remote_bdevs = {}
        self.controllers = {}
        self.next_namespace = {}
        self.failures = {}
        self.random_failures = {}
        self.random_failure_probability = 0.0
        self.random = random.Random(0)
        self.thread_stats = {"threads": [{"name": "app_thread", "id": 1}]}

    def next_nsid(self, nqn, requested_nsid=None):
        if requested_nsid:
            current = self.next_namespace.get(nqn, 1)
            self.next_namespace[nqn] = max(current, requested_nsid + 1)
            return requested_nsid
        self.next_namespace.setdefault(nqn, 1)
        nsid = self.next_namespace[nqn]
        self.next_namespace[nqn] += 1
        return nsid

    def configure_random_failures(self, mapping, probability=1.0, seed=0):
        self.random_failures = mapping
        self.random_failure_probability = probability
        self.random = random.Random(seed)

    def configure_failures(self, mapping):
        self.failures = {method: list(codes) for method, codes in mapping.items()}

    def maybe_fail(self, method):
        if method in self.failures and self.failures[method]:
            code = self.failures[method].pop(0)
            raise _RpcError(code, f"Scripted failure for {method}")
        if method in self.random_failures and self.random.random() < self.random_failure_probability:
            code = self.random.choice(self.random_failures[method])
            raise _RpcError(code, f"Random failure for {method}")


class _Registry:
    servers = {}

    @classmethod
    def register(cls, host, port, server):
        cls.servers[(host, str(port))] = server

    @classmethod
    def unregister(cls, host, port):
        cls.servers.pop((host, str(port)), None)

    @classmethod
    def get(cls, host, port):
        return cls.servers.get((host, str(port)))


class _RpcHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        logger.debug(fmt, *args)

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        payload = json.loads(self.rfile.read(length))
        method = payload["method"]
        params = payload.get("params", {}) or {}
        request_id = payload.get("id", 1)
        server = self.server  # type: ignore[assignment]

        try:
            with server.state.lock:
                server.state.maybe_fail(method)
                result = server.dispatch(method, params)
            self._respond({"jsonrpc": "2.0", "result": result, "id": request_id})
        except _RpcError as exc:
            self._respond({
                "jsonrpc": "2.0",
                "error": {"code": exc.code, "message": exc.message},
                "id": request_id,
            })

    def _respond(self, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class _RpcServer(HTTPServer):
    allow_reuse_address = True

    def __init__(self, host, port, state):
        super().__init__((host, port), _RpcHandler)
        self.state = state

    def dispatch(self, method, params):
        handler = getattr(self, f"_rpc_{method}", None)
        if handler is None:
            raise _RpcError(-32601, f"Method not found: {method}")
        return handler(params)

    def _all_bdevs(self):
        merged = {}
        merged.update(self.state.local_bdevs)
        merged.update(self.state.remote_bdevs)
        return merged

    def _rpc_spdk_get_version(self, params):
        return {"version": "mock-24.05", "fields": {}}

    def _rpc_nvmf_get_subsystems(self, params):
        return list(self.state.subsystems.values())

    def _rpc_nvmf_create_subsystem(self, params):
        nqn = params["nqn"]
        if nqn in self.state.subsystems:
            raise _RpcError(-17, f"subsystem {nqn} already exists")
        self.state.subsystems[nqn] = {
            "nqn": nqn,
            "serial_number": params.get("serial_number", ""),
            "model_number": params.get("model_number", ""),
            "namespaces": [],
            "listen_addresses": [],
        }
        return True

    def _rpc_nvmf_delete_subsystem(self, params):
        nqn = params["nqn"]
        if nqn not in self.state.subsystems:
            raise _RpcError(-2, f"subsystem {nqn} not found")
        del self.state.subsystems[nqn]
        return True

    def _rpc_nvmf_subsystem_add_listener(self, params):
        nqn = params["nqn"]
        if nqn not in self.state.subsystems:
            raise _RpcError(-2, f"subsystem {nqn} not found")
        listener = dict(params["listen_address"])
        listener["ana_state"] = params.get("ana_state", "optimized")
        self.state.subsystems[nqn]["listen_addresses"].append(listener)
        return True

    def _rpc_nvmf_subsystem_add_ns(self, params):
        nqn = params["nqn"]
        namespace = dict(params["namespace"])
        if nqn not in self.state.subsystems:
            raise _RpcError(-2, f"subsystem {nqn} not found")
        nsid = self.state.next_nsid(nqn, namespace.get("nsid"))
        ns = {
            "nsid": nsid,
            "name": namespace["bdev_name"],
            "bdev_name": namespace["bdev_name"],
            "uuid": namespace.get("uuid", str(_uuid_mod.uuid4())),
            "nguid": namespace.get("nguid", ""),
        }
        self.state.subsystems[nqn]["namespaces"].append(ns)
        return nsid

    def _rpc_nvmf_subsystem_remove_ns(self, params):
        nqn = params["nqn"]
        nsid = int(params["nsid"])
        if nqn not in self.state.subsystems:
            raise _RpcError(-2, f"subsystem {nqn} not found")
        before = len(self.state.subsystems[nqn]["namespaces"])
        self.state.subsystems[nqn]["namespaces"] = [
            ns for ns in self.state.subsystems[nqn]["namespaces"]
            if ns["nsid"] != nsid
        ]
        if len(self.state.subsystems[nqn]["namespaces"]) == before:
            raise _RpcError(-2, f"namespace {nsid} not found")
        return True

    def _rpc_nvmf_subsystem_listener_set_ana_state(self, params):
        nqn = params["nqn"]
        addr = params["listen_address"]["traddr"]
        port = str(params["listen_address"]["trsvcid"])
        if nqn not in self.state.subsystems:
            raise _RpcError(-2, f"subsystem {nqn} not found")
        for listener in self.state.subsystems[nqn]["listen_addresses"]:
            if listener["traddr"] == addr and str(listener["trsvcid"]) == port:
                listener["ana_state"] = params.get("ana_state", "optimized")
                return True
        raise _RpcError(-2, "listener not found")

    def _rpc_bdev_get_bdevs(self, params):
        name = params.get("name")
        bdevs = self._all_bdevs()
        if name:
            return [bdevs[name]] if name in bdevs else []
        return list(bdevs.values())

    def _rpc_bdev_alceml_create(self, params):
        name = params["name"]
        base_bdev = params["base_bdev_name"]
        self.state.local_bdevs[name] = {
            "name": name,
            "aliases": [base_bdev],
            "driver_specific": {},
        }
        return True

    def _rpc_bdev_ptnonexcl_create(self, params):
        name = params["name"]
        base_bdev = params["base_bdev_name"]
        self.state.local_bdevs[name] = {
            "name": name,
            "aliases": [base_bdev],
            "driver_specific": {},
        }
        return True

    def _rpc_bdev_ptnonexcl_delete(self, params):
        self.state.local_bdevs.pop(params["name"], None)
        return True

    def _rpc_bdev_alceml_delete(self, params):
        self.state.local_bdevs.pop(params["name"], None)
        return True

    def _rpc_bdev_passtest_create(self, params):
        self.state.local_bdevs[params["name"]] = {
            "name": params["name"],
            "aliases": [params["base_name"]],
            "driver_specific": {},
        }
        return True

    def _rpc_bdev_jm_create(self, params):
        name = params["name"]
        base_bdev = params["base_bdev_name"]
        self.state.local_bdevs[name] = {
            "name": name,
            "aliases": [base_bdev],
            "driver_specific": {},
        }
        return True

    def _rpc_bdev_nvme_get_controllers(self, params):
        name = params.get("name")
        if name:
            ctrl = self.state.controllers.get(name)
            return [ctrl] if ctrl else []
        return list(self.state.controllers.values())

    def _rpc_bdev_nvme_attach_controller(self, params):
        name = params["name"]
        trtype = params["trtype"]
        if trtype == "pcie":
            self.state.controllers[name] = {
                "name": name,
                "ctrlrs": [{
                    "state": "connected",
                    "trid": {"traddr": params["traddr"], "trsvcid": "", "trtype": trtype},
                }],
                "remote_bdevs": [f"{name}n1"],
            }
            self.state.local_bdevs[f"{name}n1"] = {
                "name": f"{name}n1",
                "aliases": [],
                "driver_specific": {},
            }
            return [f"{name}n1"]

        remote_server = _Registry.get(params["traddr"], params["trsvcid"])
        if remote_server is None:
            raise _RpcError(-2, "remote server not found")
        remote_subsystem = remote_server.state.subsystems.get(params["subnqn"])
        if remote_subsystem is None:
            raise _RpcError(-2, f"remote subsystem {params['subnqn']} not found")

        remote_bdevs = []
        for namespace in remote_subsystem["namespaces"]:
            bdev_name = f"{name}n{namespace['nsid']}"
            self.state.remote_bdevs[bdev_name] = {
                "name": bdev_name,
                "aliases": [],
                "uuid": namespace["uuid"],
                "driver_specific": {"mp_policy": "active_active" if params.get("multipath") != "disable" else "disable"},
            }
            remote_bdevs.append(bdev_name)

        controller = self.state.controllers.setdefault(name, {
            "name": name,
            "ctrlrs": [],
            "remote_bdevs": [],
        })
        controller["ctrlrs"].append({
            "state": "connected",
            "trid": {
                "traddr": params["traddr"],
                "trsvcid": str(params["trsvcid"]),
                "trtype": params["trtype"],
            },
        })
        controller["remote_bdevs"] = sorted(set(controller["remote_bdevs"] + remote_bdevs))
        return remote_bdevs

    def _rpc_bdev_nvme_detach_controller(self, params):
        name = params["name"]
        controller = self.state.controllers.pop(name, None)
        if controller:
            for bdev in controller.get("remote_bdevs", []):
                self.state.remote_bdevs.pop(bdev, None)
        return True

    def _rpc_iobuf_set_options(self, params):
        return True

    def _rpc_bdev_set_options(self, params):
        return True

    def _rpc_accel_set_options(self, params):
        return True

    def _rpc_nvmf_set_max_subsystems(self, params):
        return True

    def _rpc_sock_impl_set_options(self, params):
        return True

    def _rpc_nvmf_set_config(self, params):
        return True

    def _rpc_framework_start_init(self, params):
        return True

    def _rpc_log_set_print_level(self, params):
        return True

    def _rpc_thread_get_stats(self, params):
        return self.state.thread_stats

    def _rpc_thread_set_cpumask(self, params):
        return True

    def _rpc_bdev_nvme_set_options(self, params):
        return True

    def _rpc_nvmf_create_transport(self, params):
        return True

    def _rpc_jc_set_hint_lcpu_mask(self, params):
        return True

    def _rpc_bdev_examine(self, params):
        return True

    def _rpc_bdev_wait_for_examine(self, params):
        return True


class NamespaceMockRpcServer:
    def __init__(self, host="127.0.0.1", port=0, node_id="node"):
        self.state = NamespaceNodeState(node_id=node_id, host=host, port=port)
        self.server = _RpcServer(host, port, self.state)
        self.thread = None

    @property
    def host(self):
        return self.state.host

    @property
    def port(self):
        return self.state.port

    def start(self):
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        _Registry.register(self.state.host, self.state.port, self.server)

    def stop(self):
        _Registry.unregister(self.state.host, self.state.port)
        self.server.shutdown()
        self.server.server_close()
        if self.thread:
            self.thread.join(timeout=2)

    def reset(self):
        with self.state.lock:
            self.state.reset()

    def configure_failures(self, mapping):
        with self.state.lock:
            self.state.configure_failures(mapping)

    def configure_random_failures(self, mapping, probability=1.0, seed=0):
        with self.state.lock:
            self.state.configure_random_failures(mapping, probability=probability, seed=seed)
