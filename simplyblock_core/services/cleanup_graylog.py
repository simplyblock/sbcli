import subprocess
import time
import os
import sys
import logging

from simplyblock_core import constants

# Import the GELF logger
from graypy import GELFUDPHandler

logger_handler = logging.StreamHandler(stream=sys.stdout)
logger_handler.setFormatter(logging.Formatter('%(asctime)s: %(levelname)s: %(message)s'))
gelf_handler = GELFUDPHandler('0.0.0.0', constants.GELF_PORT)
logger = logging.getLogger()
logger.addHandler(gelf_handler)
logger.addHandler(logger_handler)
logger.setLevel(logging.DEBUG)

deletion_interval = os.getenv('LOG_DELETION_INTERVAL', '10m')

while True:
    delete_query = f'''
    curl -X POST "http://opensearch:9200/graylog_*/_delete_by_query" -H 'Content-Type: application/json' -d'
    {{
      "query": {{
        "range": {{
          "timestamp": {{
            "lt": "now-{deletion_interval}"
          }}
        }}
      }}
    }}'
    '''
    try:
        result = subprocess.run(delete_query, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logger.info("Logs deleted successfully.")
        logger.info(result.stdout.decode())
    except subprocess.CalledProcessError as e:
        logger.error("Failed to delete logs.")
        logger.error(e.stderr.decode())
    
    time.sleep(constants.GRAYLOG_CHECK_INTERVAL_SEC)
