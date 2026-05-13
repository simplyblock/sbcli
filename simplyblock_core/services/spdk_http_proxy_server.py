#!/usr/bin/env python3

import base64
import json
import logging
import os
import socket
import sys
import threading
import time

from http.server import HTTPServer
from http.server import ThreadingHTTPServer
from http.server import BaseHTTPRequestHandler

from simplyblock_core.kms import (
    KMSException,
    create_kms_connection_for_wrapped,
)
from simplyblock_core.settings import Settings


logger_handler = logging.StreamHandler(stream=sys.stdout)
logger_handler.setFormatter(logging.Formatter('%(asctime)s: %(levelname)s: %(message)s'))
logger = logging.getLogger()
logger.addHandler(logger_handler)
logger.setLevel(logging.INFO)

read_line_time_diff: dict = {}
recv_from_spdk_time_diff: dict = {}
def print_stats():
    while True:
        try:
            time.sleep(3)
            t = time.time_ns()
            if len(read_line_time_diff) > 0:
                read_line_time_diff_max = max(list(read_line_time_diff.values()))
                read_line_time_diff_avg = int(sum(list(read_line_time_diff.values()))/len(read_line_time_diff))
                last_3_sec = []
                for k,v in read_line_time_diff.items():
                    if k > t - 3*1000*1000*1000:
                        last_3_sec.append(v)
                if len(last_3_sec) > 0:
                    read_line_time_diff_avg_last_3_sec = int(sum(last_3_sec)/len(last_3_sec))
                else:
                    read_line_time_diff_avg_last_3_sec = 0
                logger.info(f"Periodic stats: {t}: read_line_time: max={read_line_time_diff_max} ns, avg={read_line_time_diff_avg} ns, last_3s_avg={read_line_time_diff_avg_last_3_sec} ns")
                if len(read_line_time_diff) > 10000:
                    read_line_time_diff.clear()

            if len(recv_from_spdk_time_diff) > 0:
                recv_from_spdk_time_max = max(list(recv_from_spdk_time_diff.values()))
                recv_from_spdk_time_avg = int(sum(list(recv_from_spdk_time_diff.values()))/len(recv_from_spdk_time_diff))
                last_3_sec = []
                for k,v in recv_from_spdk_time_diff.items():
                    if k > t - 3*1000*1000*1000:
                        last_3_sec.append(v)
                if len(last_3_sec) > 0:
                    recv_from_spdk_time_avg_last_3_sec = int(sum(last_3_sec)/len(last_3_sec))
                else:
                    recv_from_spdk_time_avg_last_3_sec = 0
                logger.info(f"Periodic stats: {t}: recv_from_spdk_time: max={recv_from_spdk_time_max} ns, avg={recv_from_spdk_time_avg} ns, last_3s_avg={recv_from_spdk_time_avg_last_3_sec} ns")
                if len(recv_from_spdk_time_diff) > 10000:
                    recv_from_spdk_time_diff.clear()
        except Exception as e:
            logger.error(e)


def get_env_var(name, default=None, is_required=False):
    if not name:
        logger.warning("Invalid env var name %s", name)
        return False
    if name not in os.environ and is_required:
        logger.error("env value is required: %s" % name)
        raise Exception("env value is required: %s" % name)
    return os.environ.get(name, default)

unix_sockets: list[socket] = []  # type: ignore[valid-type]
spdk_semaphore: threading.Semaphore = None  # type: ignore[assignment]  # initialized after env vars are read
spdk_ready = False


def wait_for_spdk_ready():
    """Block until SPDK responds to spdk_get_version on the unix socket."""
    global spdk_ready
    payload = json.dumps({'id': 1, 'method': 'spdk_get_version'}).encode('ascii')
    while not spdk_ready:
        sock = None
        try:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.connect(rpc_sock)
            sock.sendall(payload)
            buf = b''
            while True:
                data = sock.recv(4096)
                if data == b'':
                    break
                buf += data
                try:
                    json.loads(buf.decode('ascii'))
                    spdk_ready = True
                    logger.info("SPDK is ready (spdk_get_version responded)")
                    return
                except ValueError:
                    continue
        except (socket.error, OSError) as e:
            logger.info(f"Waiting for SPDK to be ready: {e}")
        finally:
            if sock:
                try:
                    sock.close()
                except OSError:
                    pass
        time.sleep(1)


def rpc_call(req):
    logger.info(f"active threads: {threading.active_count()}")
    logger.info(f"active unix sockets: {len(unix_sockets)}")
    req_data = json.loads(req.decode('ascii'))
    req_time = time.time_ns()
    params = ""
    if "params" in req_data:
        params = str(req_data['params'])
    logger.info(f"Request:{req_time} function: {str(req_data['method'])}, params: {params}")
    spdk_semaphore.acquire()
    try:
        return _rpc_call_inner(req, req_data, req_time)
    finally:
        spdk_semaphore.release()


def rpc_call_redacted(req):
    """Forward an RPC to SPDK without logging request params.

    Used when the JSON-RPC params contain key material (e.g.
    ``accel_crypto_key_create``) that must not be persisted to logs.
    """
    logger.info(f"active threads: {threading.active_count()}")
    logger.info(f"active unix sockets: {len(unix_sockets)}")
    req_data = json.loads(req.decode('ascii'))
    req_time = time.time_ns()
    logger.info(f"Request:{req_time} function: {str(req_data['method'])} (params redacted)")
    spdk_semaphore.acquire()
    try:
        return _rpc_call_inner(req, req_data, req_time)
    finally:
        spdk_semaphore.release()

def _rpc_call_inner(req, req_data, req_time):
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    unix_sockets.append(sock)
    try:
        sock.settimeout(TIMEOUT)
        sock.connect(rpc_sock)
        sock.sendall(req)

        if 'id' not in req_data:
            return None

        buf = ''
        closed = False
        response = None
        recv_from_spdk_time_start = time.time_ns()
        while not closed:
            newdata = sock.recv(1024*1024*1024)
            if newdata == b'':
                closed = True
            buf += newdata.decode('ascii')
            try:
                response = json.loads(buf)
            except ValueError:
                continue  # incomplete response; keep buffering
            break
        recv_from_spdk_time_end = time.time_ns()
        time_diff = recv_from_spdk_time_end - recv_from_spdk_time_start
        logger.info(f"recv_from_spdk_time_diff: {time_diff}")
        recv_from_spdk_time_diff[recv_from_spdk_time_start] = time_diff

        if not response and len(buf) > 0:
            raise ValueError('Invalid response')

        logger.info(f"Response:{req_time}")

        return buf
    except socket.timeout:
        logger.error(f"Socket timeout waiting for SPDK response (request {req_time}, function: {req_data.get('method', 'unknown')})")
        raise ValueError('SPDK response timeout')
    finally:
        try:
            sock.close()
        except OSError:
            pass
        try:
            unix_sockets.remove(sock)
        except ValueError:
            pass


CRYPTO_KEY_PATH = "/v1/crypto_key"


class ServerHandler(BaseHTTPRequestHandler):
    server_session: list[int] = []
    key = ""
    def do_HEAD(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_HEAD_no_content(self):
        self.send_response(204)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_AUTHHEAD(self):
        self.send_response(401)
        self.send_header('WWW-Authenticate', 'text/html')
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_INTERNALERROR(self):
        self.send_response(500)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def _send_status(self, status: int):
        self.send_response(status)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def _read_body(self) -> bytes:
        if "Content-Length" in self.headers:
            return self.rfile.read(int(self.headers['Content-Length']))
        if "chunked" in self.headers.get("Transfer-Encoding", ""):
            data_string = b''
            while True:
                line = self.rfile.readline().strip()
                chunk_length = int(line, 16)
                if chunk_length != 0:
                    chunk = self.rfile.read(chunk_length)
                    data_string += chunk
                self.rfile.readline()
                if chunk_length == 0:
                    break
            return data_string
        return b''

    def do_POST(self):
        req_time = time.time_ns()
        self.server_session.append(req_time)
        logger.info(f"incoming request at: {req_time}")
        logger.info(f"active server session: {len(self.server_session)}")
        if self.headers['Authorization'] != 'Basic ' + self.key:
            self.do_AUTHHEAD()
            self.server_session.remove(req_time)
            return

        try:
            read_line_time_start = time.time_ns()
            data_string = self._read_body()
            read_line_time_end = time.time_ns()
            time_diff = read_line_time_end - read_line_time_start
            logger.info(f"read_line_time_diff: {time_diff}")
            read_line_time_diff[read_line_time_start] = time_diff

            if self.path == CRYPTO_KEY_PATH:
                self._handle_crypto_key(data_string, req_time)
            else:
                self._handle_rpc_passthrough(data_string, req_time)
        finally:
            self.server_session.remove(req_time)

    def _handle_rpc_passthrough(self, data_string: bytes, req_time: int):
        try:
            response = rpc_call(data_string)
            if response is not None:
                self.do_HEAD()
                self.wfile.write(bytes(response.encode(encoding='ascii')))
            else:
                self.do_HEAD_no_content()
        except BrokenPipeError:
            logger.warning(f"BrokenPipeError: client disconnected before response could be sent (request {req_time})")
        except ValueError:
            self.do_INTERNALERROR()

    def _handle_crypto_key(self, data_string: bytes, req_time: int):
        """Unwrap a DEK pair via the proxy's KMS and forward to ``accel_crypto_key_create``.

        Request body: ``{"name": "<spdk-keyring-name>", "wrapped_keys": {...}}``
        where the ``wrapped_keys`` payload was produced by
        ``KMS.get_wrapped_data_encryption_keys`` on the control plane.
        Neither the wrapped payload nor the unwrapped plaintext is
        ever written to logs from this endpoint.
        """
        try:
            payload = json.loads(data_string.decode('ascii'))
            name = payload['name']
            wrapped = payload['wrapped_keys']
        except (ValueError, KeyError, TypeError) as e:
            logger.warning(f"Malformed crypto_key request (req {req_time}): {type(e).__name__}")
            self._send_status(400)
            return

        try:
            with create_kms_connection_for_wrapped(wrapped) as kms:
                key1, key2 = kms.unwrap_data_encryption_keys(wrapped)
        except KMSException as e:
            logger.error(f"KMS unwrap failed for crypto_key {name} (req {req_time}): {e}")
            self._send_status(502)
            return

        rpc_payload = json.dumps({
            'id': 1,
            'method': 'accel_crypto_key_create',
            'params': {
                'cipher': 'AES_XTS',
                'name': name,
                'key': key1,
                'key2': key2,
            },
        }).encode('ascii')

        try:
            response = rpc_call_redacted(rpc_payload)
        except BrokenPipeError:
            logger.warning(f"BrokenPipeError on crypto_key (req {req_time})")
            return
        except ValueError:
            self.do_INTERNALERROR()
            return

        if response is not None:
            self.do_HEAD()
            self.wfile.write(response.encode('ascii'))
        else:
            self.do_HEAD_no_content()


def run_server(host, port, user, password, is_threading_enabled=False):
    # encoding user and password
    key = base64.b64encode((user+':'+password).encode(encoding='ascii')).decode('ascii')
    print_stats_thread = threading.Thread(target=print_stats, daemon=True)
    print_stats_thread.start()
    wait_for_spdk_ready()
    try:
        ServerHandler.key = key
        httpd = (ThreadingHTTPServer if is_threading_enabled else HTTPServer)((host, port), ServerHandler)
        settings = Settings()
        context = settings.make_server_ssl_context()
        if context is not None:
            httpd.socket = context.wrap_socket(httpd.socket, server_side=True)
        httpd.timeout = TIMEOUT
        logger.info('Started RPC http proxy server')
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info('Shutting down server')
        httpd.socket.close()


TIMEOUT = int(get_env_var("TIMEOUT", is_required=False, default=60*5))
MAX_CONCURRENT_SPDK = int(get_env_var("MAX_CONCURRENT_SPDK", is_required=False, default=16))
is_threading_enabled = get_env_var("MULTI_THREADING_ENABLED", is_required=False, default=False)
server_ip = get_env_var("SERVER_IP", is_required=True, default="")
rpc_port = get_env_var("RPC_PORT", is_required=True)
rpc_username = get_env_var("RPC_USERNAME", is_required=True)
rpc_password = get_env_var("RPC_PASSWORD", is_required=True)

try:
    rpc_port = int(rpc_port)
except Exception:
    rpc_port = 8080
rpc_sock = f"/mnt/ramdisk/spdk_{rpc_port}/spdk.sock"

spdk_semaphore = threading.Semaphore(MAX_CONCURRENT_SPDK)
logger.info(f"SPDK concurrency limit: {MAX_CONCURRENT_SPDK}")

is_threading_enabled = bool(is_threading_enabled)
run_server(server_ip, rpc_port, rpc_username, rpc_password, is_threading_enabled=is_threading_enabled)
