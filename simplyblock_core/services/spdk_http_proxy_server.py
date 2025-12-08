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


logger_handler = logging.StreamHandler(stream=sys.stdout)
logger_handler.setFormatter(logging.Formatter('%(asctime)s: %(levelname)s: %(message)s'))
logger = logging.getLogger()
logger.addHandler(logger_handler)
logger.setLevel(logging.INFO)

read_line_time_diff: dict = {}
recv_from_spdk_time_diff: dict = {}
def print_stats():
    global read_line_time_diff, recv_from_spdk_time_diff
    while True:
        try:
            time.sleep(3)
            t = time.time_ns()
            read_line_time_diff_max = max(list(read_line_time_diff.values()))
            read_line_time_diff_avg = int(sum(list(read_line_time_diff.values()))/len(read_line_time_diff))
            last_3_sec = []
            for k,v in read_line_time_diff.items():
                if k > t - 3*1000*1000*1000:
                    last_3_sec.append(v)
            read_line_time_diff_avg_last_3_sec = int(sum(last_3_sec)/len(last_3_sec))
            logger.info(f"Periodic stats: {t}: read_line_time max={read_line_time_diff_max}ns, avg={read_line_time_diff_avg}ns, last 3s avg={read_line_time_diff_avg_last_3_sec}ns")
            if len(read_line_time_diff) > 10000:
                read_line_time_diff.clear()

            recv_from_spdk_time_max = max(list(recv_from_spdk_time_diff.values()))
            recv_from_spdk_time_avg = int(sum(list(recv_from_spdk_time_diff.values()))/len(recv_from_spdk_time_diff))
            last_3_sec = []
            for k,v in recv_from_spdk_time_diff.items():
                if k > t - 3*1000*1000*1000:
                    last_3_sec.append(v)
            recv_from_spdk_time_avg_last_3_sec = int(sum(last_3_sec)/len(last_3_sec))
            logger.info(f"Periodic stats: {t}: recv_from_spdk_time max={recv_from_spdk_time_max}ns, avg={recv_from_spdk_time_avg}ns, last 3s avg={recv_from_spdk_time_avg_last_3_sec}ns")
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
def rpc_call(req):
    global recv_from_spdk_time_diff
    logger.info(f"active threads: {threading.active_count()}")
    logger.info(f"active unix sockets: {len(unix_sockets)}")
    req_data = json.loads(req.decode('ascii'))
    req_time = time.time_ns()
    params = ""
    if "params" in req_data:
        params = str(req_data['params'])
    logger.info(f"Request:{req_time} function: {str(req_data['method'])}, params: {params}")
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    unix_sockets.append(sock)
    sock.settimeout(TIMEOUT)
    sock.connect(rpc_sock)
    sock.sendall(req)

    if 'id' not in req_data:
        sock.close()
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

    sock.close()
    unix_sockets.remove(sock)

    if not response and len(buf) > 0:
        raise ValueError('Invalid response')

    logger.info(f"Response:{req_time}")

    return buf


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

    def do_POST(self):
        global read_line_time_diff
        req_time = time.time_ns()
        self.server_session.append(req_time)
        logger.info(f"incoming request at: {req_time}")
        logger.info(f"active server session: {len(self.server_session)}")
        if self.headers['Authorization'] != 'Basic ' + self.key:
            self.do_AUTHHEAD()
        else:
            if "Content-Length" in self.headers:
                data_string = self.rfile.read(int(self.headers['Content-Length']))
            elif "chunked" in self.headers.get("Transfer-Encoding", ""):
                data_string = b''
                read_line_time_start = time.time_ns()
                while True:
                    line = self.rfile.readline().strip()
                    chunk_length = int(line, 16)

                    if chunk_length != 0:
                        chunk = self.rfile.read(chunk_length)
                        data_string += chunk

                    # Each chunk is followed by an additional empty newline
                    # that we have to consume.
                    self.rfile.readline()

                    # Finally, a chunk size of 0 is an end indication
                    if chunk_length == 0:
                        break
                read_line_time_end = time.time_ns()
                time_diff = read_line_time_end - read_line_time_start
                logger.info(f"read_line_time_diff: {time_diff}")
                read_line_time_diff[read_line_time_start] = time_diff
            try:
                response = rpc_call(data_string)
                if response is not None:
                    self.do_HEAD()
                    self.wfile.write(bytes(response.encode(encoding='ascii')))
                else:
                    self.do_HEAD_no_content()

            except ValueError:
                self.do_INTERNALERROR()
        self.server_session.remove(req_time)


def run_server(host, port, user, password, is_threading_enabled=False):
    # encoding user and password
    key = base64.b64encode((user+':'+password).encode(encoding='ascii')).decode('ascii')
    print_stats_thread = threading.Thread(target=print_stats, )
    print_stats_thread.start()
    try:
        ServerHandler.key = key
        httpd = (ThreadingHTTPServer if is_threading_enabled else HTTPServer)((host, port), ServerHandler)
        httpd.timeout = TIMEOUT
        logger.info('Started RPC http proxy server')
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info('Shutting down server')
        httpd.socket.close()


TIMEOUT = int(get_env_var("TIMEOUT", is_required=False, default=60*5))
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

is_threading_enabled = bool(is_threading_enabled)
run_server(server_ip, rpc_port, rpc_username, rpc_password, is_threading_enabled=is_threading_enabled)
