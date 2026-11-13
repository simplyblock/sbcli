import logging
import sys

sys.path.append('/root/spdk/python')
import spdk.rpc as rpc

client = None
try:
    client = rpc.client.JSONRPCClient(sys.argv[1], 5260, 60, log_level=logging.ERROR, conn_retries=0)
except Exception as e:
    print("ERR connect:", e)


f = 'jc_journal_dump'
params = {
    "jm_vuid": 2898,
    "jm_name": "remote_jm_5edeb5aa-ab5e-44b2-90b5-77db712cca2bn1"
}
result = client.call(f, params)
print(result)
