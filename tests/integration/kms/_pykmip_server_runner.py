"""Standalone entry point: run a PyKMIP reference server given a config path.

Run as a subprocess, not an in-process thread: ``KmipServer.start()``/
``serve()`` both call ``signal.signal(SIGINT/SIGTERM, ...)``, which Python
only permits from the main thread of a process -- a background thread raises
``ValueError: signal only works in main thread``. ``tests/integration/kms/
conftest.py`` launches this via ``subprocess.Popen``.

Usage: python _pykmip_server_runner.py <config_path> <log_path>
"""
import sys

from kmip.services.server.server import KmipServer


def main() -> None:
    config_path, log_path = sys.argv[1], sys.argv[2]
    server = KmipServer(config_path=config_path, log_path=log_path)
    server.start()
    server.serve()


if __name__ == "__main__":
    main()
