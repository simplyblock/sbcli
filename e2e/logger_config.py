import glob
import logging
import os
import shutil
import threading
from datetime import datetime
from logging.handlers import RotatingFileHandler

# Maximum size per log file: 500 MB, keep up to 3 rotated backups
LOG_MAX_BYTES = 500 * 1024 * 1024  # 500 MB
LOG_BACKUP_COUNT = 3

# Local log directory (on the runner's root filesystem)
_LOCAL_LOG_DIR = "logs"

# Background log flusher state
_log_flusher_thread = None
_log_flusher_stop = threading.Event()


def setup_logger(name):
    # Create a custom logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    os.makedirs(_LOCAL_LOG_DIR, exist_ok=True)

    # Create a rotating file handler
    current_datetime = datetime.now().strftime("%Y-%m-%d")
    log_file = os.path.join(_LOCAL_LOG_DIR, f"log_{current_datetime}.log")
    f_handler = RotatingFileHandler(
        log_file,
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT,
    )
    f_handler.setLevel(logging.INFO)

    # Create formatter and add it to the handler
    f_format = logging.Formatter('%(asctime)s - %(name)s:%(lineno)d - %(levelname)s - %(message)s')
    f_handler.setFormatter(f_format)

    c_handler = logging.StreamHandler()
    c_handler.setLevel(logging.INFO)

    # Create formatter and add it to the handler
    c_handler.setFormatter(f_format)

    # Add handler to the logger
    if not logger.handlers:  # To prevent adding multiple handlers to the same logger
        logger.addHandler(f_handler)
        logger.addHandler(c_handler)

    return logger


def _flush_rotated_logs(nfs_dest_dir):
    """Move only rotated log files (.1, .2, .3) to NFS, keeping the
    active log file in place.  Safe to call while the test is running.

    Files are renamed with a timestamp suffix to avoid overwriting
    earlier flushes when RotatingFileHandler reuses the same .1/.2/.3
    names.
    """
    if not nfs_dest_dir or not os.path.isdir(nfs_dest_dir):
        return 0

    dest = os.path.join(nfs_dest_dir, "run-logs")
    os.makedirs(dest, exist_ok=True)

    ts = datetime.now().strftime("%H%M%S")
    moved = 0
    for ext in range(1, LOG_BACKUP_COUNT + 1):
        pattern = os.path.join(_LOCAL_LOG_DIR, f"log_*.log.{ext}")
        for src_file in glob.glob(pattern):
            try:
                base = os.path.basename(src_file)
                dest_name = f"{base}.flushed_{ts}"
                shutil.copy2(src_file, os.path.join(dest, dest_name))
                os.remove(src_file)
                moved += 1
            except OSError:
                pass
    return moved


def start_log_flusher(nfs_dest_dir, interval=1800):
    """Start a background daemon thread that moves rotated log files
    to NFS every *interval* seconds (default 30 min).

    Call from test setup once ``docker_logs_path`` is known.
    Only one flusher runs at a time; subsequent calls are no-ops.
    """
    global _log_flusher_thread

    if _log_flusher_thread and _log_flusher_thread.is_alive():
        return

    _log_flusher_stop.clear()
    logger = logging.getLogger(__name__)

    def _run():
        while not _log_flusher_stop.wait(timeout=interval):
            moved = _flush_rotated_logs(nfs_dest_dir)
            if moved:
                logger.info(
                    f"[log_flusher] Moved {moved} rotated log file(s) "
                    f"to {nfs_dest_dir}/run-logs"
                )

    _log_flusher_thread = threading.Thread(
        target=_run, daemon=True, name="log-flusher"
    )
    _log_flusher_thread.start()
    logger.info(
        f"[log_flusher] Started — flushing rotated logs to "
        f"{nfs_dest_dir} every {interval}s"
    )


def stop_log_flusher():
    """Stop the background log flusher and do one final flush."""
    global _log_flusher_thread
    if _log_flusher_thread and _log_flusher_thread.is_alive():
        _log_flusher_stop.set()
        _log_flusher_thread.join(timeout=10)
        _log_flusher_thread = None


def copy_logs_to_nfs(nfs_dest_dir):
    """Copy all local log files to *nfs_dest_dir* and remove
    local copies to free disk space on the runner.

    Call this from teardown after the test is done.  The NFS-backed
    ``docker_logs_path`` has ample space; the runner root FS does not.
    """
    stop_log_flusher()

    if not nfs_dest_dir or not os.path.isdir(nfs_dest_dir):
        return

    dest = os.path.join(nfs_dest_dir, "run-logs")
    os.makedirs(dest, exist_ok=True)

    pattern = os.path.join(_LOCAL_LOG_DIR, "log_*")
    for src_file in glob.glob(pattern):
        try:
            shutil.copy2(src_file, dest)
            os.remove(src_file)
        except OSError:
            pass  # best-effort; don't crash teardown
