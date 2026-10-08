#!/usr/bin/env python
import logging, sys
sys.path.append('/root/spdk/python')
import spdk.rpc as rpc

client = None
try:
    client = rpc.client.JSONRPCClient(sys.argv[1], 5260, 60, log_level=getattr(logging, "ERROR"), conn_retries=0)
except Exception as e:
    print("ERR connect:", e)

bdevs_list = [
'distrib_4580',
'distrib_6466',
'distrib_6263',
'distrib_6272',
'distrib_5489',
]


for bdev in bdevs_list:
    name = bdev
    print ("Processing bdev:", name)
    f = 'distr_debug_placement_map_dump'
    result = client.call(f, {'name': name})
    print(result)
