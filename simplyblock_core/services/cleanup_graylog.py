import subprocess
import time
import os

from simplyblock_core import constants

deletion_interval = os.getenv('LOG_DELETION_INTERVAL', '10m')

while True:
    delete_query = '''
    curl -X POST "http://opensearch:9200/graylog_*/_delete_by_query" -H 'Content-Type: application/json' -d'
    {
      "query": {
        "range": {
          "timestamp": {
            "lt": "now-{deletion_interval}"
          }
        }
      }
    }'
    '''
    try:
        result = subprocess.run(delete_query, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print("Logs deleted successfully.")
        print(result.stdout.decode())
    except subprocess.CalledProcessError as e:
        print("Failed to delete logs.")
        print(e.stderr.decode())
    
    time.sleep(constants.GRAYLOG_CHECK_INTERVAL_SEC)
