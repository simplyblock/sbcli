"""Array pre-check: checks a storage array and a host against simplyblock's
array plug-in before (or without) a Unified DataStore deployment, and writes
a bundle to send to simplyblock.

It runs on the host to be checked, from the simplyblock image (which has the
plug-ins, nvme-cli, iscsiadm and lsblk)::

    docker run --rm -it --privileged --network host --pid host \\
      -v /dev:/dev -v /sys:/sys -v /run/udev:/run/udev:ro -v /etc:/host/etc:ro \\
      -v /run/multipathd.socket:/run/multipathd.socket -v "$PWD":/out \\
      public.ecr.aws/simply-block/simplyblock:<version> \\
      python -m simplyblock_core.storage_arrays.preflight check \\
        --vendor netapp-ontap --endpoint https://10.0.0.10 --protocol iscsi \\
        --option svm=svm1 --portal 10.0.1.20:3260 --portal 10.0.2.20:3260 --out /out

Read-only by default: the management endpoint and its certificate, the
login, the plug-in options, what the array reports about itself, and the
host's SAN setup (initiator identity, data ports, iSCSI sessions or NVMe
discovery entries, multipath settings). ``--write`` adds the LUN life cycle
on one small LUN named after the run id: host object, create, attach, find
on the host with I/O, online growth, fence and unfence, detach, delete, and
a LUN with the journal options (``--journal-option``). It removes what it
creates, also after a failure; ``cleanup --run-id`` removes what a killed
run left.

The bundle (``array-check-<vendor>-<run id>.tar.gz``):

* ``report.txt`` / ``report.json``: every check, its result and details;
* ``trace.jsonl``: every REST request of the plug-in and the array's answer,
  with credentials, tokens, cookies and session ids removed;
* ``host/``: the host's block devices, multipath, iSCSI, NVMe and FC setup.

``replay BUNDLE`` re-runs the array checks against the recorded answers, so
a customer's result can be reproduced and a plug-in fix verified without
the array.
"""
