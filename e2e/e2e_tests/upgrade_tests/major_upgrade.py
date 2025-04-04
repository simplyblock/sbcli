from pathlib import Path
import threading
import json
from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger
from datetime import datetime, timedelta
import traceback
import random


class MajorUpgrade