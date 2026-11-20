"""Calls between control planes (the control-plane split).

Replication used to read and write the target cluster's records in this control
plane's own database. Once each site runs its own control plane, the source
reaches the target through the target control plane's internal peer API instead.

- ``protocol``: the request and response shapes both sides agree on.
- ``service``: the target-side half, run against the local database. The v2
  ``/_peer`` router serves it to a remote control plane; ``LocalPeerClient``
  calls it in-process.
- ``client``: ``peer_for_target`` hands out a ``PeerClient`` for a replication
  target. A target without a peer endpoint gets ``LocalPeerClient``, so a single
  control plane behaves exactly as before while the replication code moves over
  to the peer interface one call at a time.
"""
