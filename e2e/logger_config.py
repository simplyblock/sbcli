import glob
import logging
import os
import shutil
from datetime import datetime
from logging.handlers import RotatingFileHandler

# Maximum size per log file: 500 MB, keep up to 3 rotated backups
LOG_MAX_BYTES = 500 * 1024 * 1024  # 500 MB
LOG_BACKUP_COUNT = 3

# Local log directory (on the runner's root filesystem)
_LOCAL_LOG_DIR = "logs"


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


def copy_logs_to_nfs(nfs_dest_dir):
    """Copy all local rotated log files to *nfs_dest_dir* and remove
    local copies to free disk space on the runner.

    Call this from teardown after the test is done.  The NFS-backed
    ``docker_logs_path`` has ample space; the runner root FS does not.
    """
    if not nfs_dest_dir or not os.path.isdir(nfs_dest_dir):
        return

    dest = os.path.join(nfs_dest_dir, "automation_logs")
    os.makedirs(dest, exist_ok=True)

    pattern = os.path.join(_LOCAL_LOG_DIR, "log_*")
    for src_file in glob.glob(pattern):
        try:
            shutil.copy2(src_file, dest)
            os.remove(src_file)
        except OSError:
            pass  # best-effort; don't crash teardown
