"""pNFS RWX functional tests (Track B of the pNFS RWX test plan).

One pod on each worker, pod anti-affinity on hostname, all of them on the
same ReadWriteMany claim. That is the topology the feature exists for and it
is the topology for every case in here.
"""
