This document describes steps for log analysis to identify known problem patterns and pre-analyse an issue.

There are three different main scenarios based on which we have to do the failure analysis:
1. test run based on e2e testing: this will result in a log file collection starting on a fileshare with /mnt/ . you will have access to those logs via the jump host. to get to the jump host, use: ssh simplyblock@95.216.93.11 -p 13987 -i ./simplyblock
2. results from a soak test run. Logs are available in a tarball or you can retrieve them by running python3 ./collect_logs.py "2026-04-08T08:40:00" 30 --use-opensearch on the management node of the cluster (or one of them, if there are multiple; in Kubernetes hyper-converged setup you must run from within the admin pod).the script itself resides in the sbcli/scripts directory.

Additional logs to collect (not included in the collector): 
if this is a soak test run also collect the outputs of the test script (usually .out and .log on mgmt node).
collect SnodeApi container logs from storage nodes, if accessible
collect dmesg logs from clients, if accessible

Always copy all of the logs back to local host for analysis!

in the .gz created by the log collector, there are control plane container logs (services reside on simplyblock management node or on distributed control plane nodes in Kubernetes) and two important logs per storage node: spdk_xxxx and spdk_proxy_80xx (each storage node has an RPC port and logs are  postfixed by that RPC port). 

3. results from a customer incident. you will be presented with readily collected logs in a particular directory.

Initially, let us establish fault tolerance level of the cluster. Cluster created with FTT=1 (--parity-chunks-per-stripe 1 with 3 HA journals) tolerates one concurrent outage, cluster created with FTT=2 (--parity-chunks-per-stripe 2 with 4 HA journals) tolerates two concurrent node outages.

First, we must understand the time of the incident. Find in soak .out, if this is a soak test run. Otherwise, ask from user. User may request to search in logs, if server time of incident is unknown to user.
Then we do a broad initial categorization of the incident:
1. was this an io error: user indicates or any indication of failing fio (exit > 0). If soak log is available, look for specific fio error. 
2. nodes or cluster in unexpected state, such as node[s] are unexpectedly down, unreachable or offline or entire cluster is suspended.
3. error to create, register or delete objects, other RPCs errors.

This information is either coming from the test script outputs or given by user. 

First step for failure in category 1:
We  must first identify the time of the incident and the affected volume (like nvme1 or nvme1n1) on the client. fio error is printed in test script output, but for exact timing look in client dmesg, searching for the block device name (e.g. nvme1n1). Once you find the error you can categorize:
a. this is a timeout issue (all paths down for too long)
b. this is an io error reported back from target (you will know already which target by the nvme name)
to find out, which volume on which storage nodes is affected, go back in time in dmesg to find the message when this block device got connected. it will give you: 
- the NQN, including the lvol uuid
- the ip addresses of primary, secondary and (if available) tertiary connections with the respective block devices
- on which of those the io error occurred

For errors in all categories, it is then important to analyse the cluster logs (part of the log collection, sbctl cluster get-logs). It will give evidence of what has happened in which sequence:
- which nodes changed status (establish: old and new status)
- port blocks and unblocks (establish: which node and which LVS port) 
- which devices changed status (to understand to which nodes those devices belong, sbctl sn nodes and sbctl list devices or sbctl cluster show can be used, they are all part of the log collection)
- which nodes raised io errors when communicating with which devices (as we have established already the node-device connection, we also know which nodes)
- if an io error is shown and the record contains "distr error" this aready means that the node has an unrecoverable error. if by that time the LVS port[s] of the leader[s] on this node (primary, secondary) are not blocked, this leads to an io error at the client. 
- the analysis will also usually reveal why a cluster got suspended: if more than FTT-level of cluster (means one or two) nodes are not online, the cluster will suspend. We will also see cascading effects normally, so that several or all of the cluster nodes will abort and go offline.
- the analysis of unexpected down state of one more nodes usually reveals a writer conflict (see below how to find it in spdk logs). 
- the analysis of distr error on node requires to understand first: was there an expected network outage on that node (test scenario, based on soak or other test output logs). If this is not the case, the distr error is unexpected and it has three main reasons:
- too many nodes already not online
- placement error (EITHER unable to recover stripe OR no available device for write). Three main possible underlying reasons here: a) error in placement algorithm b) error in connectivity with devices on other nodes c) error in cluster map, which contains information about nodes and devices in the cluster, their current status and placement itself (the cluster map could be outdated on the node or its also wrong on the mgmt. side; a fresh cluster map is sent to nodes when distribs are created and updated via event update RPCs)
- JC error: more than 1 (FTT=1) or 2 (FTT=2) journals not connected by JC or other journal error
- nvmf timeout or other nvmf or lvstore layer error (this can indirectly again be caused by a slow/stuck io in distrib/jc or it is an error on nvmf layer or in the lvstore, which is more rare) 
In general all of this errors will lead to failed IO on lvstore-level and a down state of the node, which in some cases can lead also to an abort (intentional core dump) of the node, after which it goes offline.
 
To identify the reason, we need to extract critical data from spdk logs and correlate it with the timing of events in the cluster logs and the incident itself. First extract (filter) logs by using
sbcli\scripts\extract_spdk_critical.py

1. identify potential writer conflicts and problems with hublvol connectivity

if there is an unexpected node in down state (node not affected by expected network outage test), probability of writer conflict is very high. in the extracted log for writer conflicts of the respective node (spdk_80xx, where xx identifies the respective node), you should find a writer conflict shortly (not more than 1 minute) before the time the down state was seen in the cluster log.
Now you need to identify the root cause. 
a. on which lvs did the conflict happen (example: jm_vuid= 8371)? based on this information from the logs, you will find the role of the LVS on the affected node (primary, secondary and tertiary) as well as the counterparty nodes.
b. now its necessary to find the correlating event (writer conflict) on the counterparty nodes and to correlate with any restart activity on that nodes. was there a recent restart on any of the two or three (in case there is a tertiary) nodes? if there was a restart, the restart log (in case of auto-restart: from restart task runner; in case of restart by test script: test script log output). The restart should show port blocks on counterparty nodes during recreate_lvstore(...) on restarted nodes. It should also show the connection of hublvols and any errors that may have occurred during hublvol connection (separate extracted critical log for hublvol errors).
If there was no restart, it is necessary to check why the hublvol connection was not working, look for hublvol errors and when/why they have happened. hublvols have to be connected in the following manner:
from secondary to primary. from tertiary to both primary (ana: optimized) and secondary (ana: non-optimized). Also, in case of multipathing there are two connections (one per path) per hublvol. The most common reason for a writer conflict is a not properly connected hublvol btw. secondary and primary or tertiary and primary or tertiary and secondary (in this case, only if the primary is not online).

inconsistencies in the lvol bdevs between primary, secondary and tertiary LVS (all most contain exactly the same lvol bdevs per LVS) will 
also lead to a failed hublvol redirect EVEN IF THE CONNECTION ITSELF IS ESTABLISHED. Look for the following error prints in spdk logs:
vbdev_hublvol_submit_request: FAILED

Sometimes a writer conflict can lead to subsequent issues in the cluster: if the fault tolerance limit is already reached before the writer conflict, an additional down state can lead to io interrupt. an abort of the node following the down state can lead to subsequent cluster suspension, because too many nodes are not online. 

These are very important log records for analysis of this problem:

LSTAT — blobstore monitoring poller (per role: primary/secondary/tertiary)
  blobstore.c:4863:spdk_bs_monitoring_poller: *NOTICE*: LSTAT primary [3835]  [32]  [3803]  [0]  [0]  [0]
  blobstore.c:4863:spdk_bs_monitoring_poller: *NOTICE*: LSTAT secondary [48]  [48]  [0]  [0]  [0]  [0]
  blobstore.c:4863:spdk_bs_monitoring_poller: *NOTICE*: LSTAT tertiary [48]  [48]  [0]  [0]  [0]  [0]
  Fires every ~1s. Six counters per role — likely: [Total internal I/O] [Total internal read I/O] [Total internal write I/O] [Current internal I/O] [Current internal read I/O] [Current internal write I/O]

  ---
IO redirect CNT — redirect counters per role
  lvol.c:3523:spdk_lvs_IO_redirect: *NOTICE*: IO redirect CNT SECONDARY: t[0] c[0] f[0] tc[0]
  lvol.c:3523:spdk_lvs_IO_redirect: *NOTICE*: IO redirect CNT TERTIARY: t[0] c[0] f[0] tc[0]
  Fires every ~1s. t[Total redirected I/O] c[Current redirected I/O] f[In-flight redirected I/O] tc[Total currently received I/O from NVMf]

  ---
IO hublvol CNT — Hub Lvol I/O Counters (Primary Role Only)
  lvol.c:3536:spdk_lvs_IO_hublvol: *NOTICE*: IO hublvol CNT: t[61] c[0] tc[16]
  Fires every ~1s. t[Total redirected I/O received] c[Current redirected I/O received] tc[Total currently received I/O from NVMf]

2. unrecoverable io errors 

Any unrecoverable io error on a node leaves certain marks: a) io error with distr error in storage id column in cluster logs. b) IO failure issued by lvstore. c) subsequent abort of the node with the IO failure (this is an intended consequence). 

Where does the IO failure come from?
a. DISTRIBD error, either not able to recover stripe on read or unable to write (placement of data failed), but the reasons can be different:
a.1. the cluster was already suspended at that time and/or too many nodes were already offline (>1 in case of FTT=1, >2 in case of FTT=2). In this case, we will see a DISTRIB error AND usual JC errors.
It can be evidenced from the history of the cluster logs also. This situation will always lead to an io error and abort.

a.2. for some reason, some of the device connections (remote nvme controllers) are not healthy although the counterparty nodes (to which the connection goes) and their devices are online/available. This can be a problem with the connection itself or the network. It should never happen in a production, as there are two network paths (multipathing or linux bond) and one of them should always be up. But it could theoretically also be a transient or latent issue on nvmf layer (e.g. issue with queue pairs) or the target device (subsystem, alceml). The health service should detect unhealthy controllers (lost) controllers and reconnect them. but if a controller is stuck in failure (e.g. there is a situation with duplicate queue pairs, which can occur due to a race of connecting and disconnecting and reconnecting a controller too quickly), this is not possible to be repaired. Now if the cluster at that point is already not fault tolerant (depending on FTT: one or two nodes or devices on different nodes not online), such an additional error causes an unrecoverable io error and node will abort.    

a.3. while the devices are properly connected via healthy remote nvme controllers, the DISTRIB could still have a wrong view of the node and device statuses, if the cluster map is stale or wrong. The cluster map is sent to a node when the DISTRIB bdevs are created and later is updated using distr event update RPC. On the other side, if a node sees an io error, it will report this io error back to the mgmt., which will in turn update the cluster map and notify this and other nodes with an event update. The mgmt. stores information about global state of each cluster node and device (really important is the device status as the distrib only excludes connections based on devices being reported as not online) as well as a per-node state (if one node cannot reach the devices of another node, this can be caused by the network or other failure conditions of that node experience the io error itself and not its counterparty). the per-node state is only communicated to the affected node, not the other nodes. If the cluster map is stale, this causes a discrepancy btw. the mgmt. view and the fact and it should be detected and repaired by the health service. If something does not work correctly there, this situation may persist for longer, causing this issue. Now if the cluster at that point is already not fault tolerant (depending on FTT: one or two nodes or devices on different nodes not online), such an additional error causes an unrecoverable io error and node will abort. 

a.4. a placement error. There are enough of nodes and devices online and no issues with connections (all are healthy, no stuck connections, no duplicate queue IDs), but there is still a placement error. this usually means a bug in the placement algorithm. Probability very low.

b. an error with JC/JM. If enough of nodes and devices are online, but we see an unrecoverable JC/JM error, this also causes an unrecoverable IO error and a node abort. 

There is one error in JC/JM, which is expected and recoverable, if at least 2 JMs are left (in case of 3 HA jms, one can be excluded, in case of 4 HA JMs, two can be excluded):
/root/spdk/ultra/DISTR_v2/src_code_app_spdk/bdev_distrib/parts/alg_journal.cpp:5481:helper_sync_setter: *ERROR*: JC [jm_dab8ba90-d57b-4460-967c-707f4e2f9c93] helper_sync_setter: jfi_w_set_crossjm_sync_id failed, JM is excluded from further operation.

Once the JC can reconnect to an excluded JM, it will inform:
- JC recovers one of the JMs
sample log: ctx_per_jm_RetryConnect: *NOTICE*: JC [remote_jm_7af0e99b-9173-4120-bcd5-73a56a261913n1] t_ctx_per_jm -journal manager recovered. : res, rc: 1 0


Other errors on JC (ERROR or JCERR) will usually lead to an unrecoverable IO error, examples:
- JC reader cannot find the readable JM 
sample log: JC helper_reader_jumping: jumping cycled, no readable device found with up-to-date history: vuid=5437

- JC writer cannot complete the write operation
sample log: JC helper_service_history_append has failed. when journal HA is active it needs to receive success response from at least 2 JM. n_success= 0 n_errors= 0, b_has_primary_success=0 jm_vuid= 9154

3. identify problems with nvmf layer 

There is separate file in the critical log extracts, which shows delays reported by the nvmf layer (separate log in critical logs extraction). It is also visible,
which state is affected and how long the delay is (in us). delays in state 13 are normal when the connection to another node breaks, bcs. that node leaves online state. All other cases are abnormal. In a state, which reflects underlying operations on distrib, the delay means that distrib is stuck or became far too slow. Reasons can be in JM hanging (not responding to JC) or any delay in the DISTRIB primary IO. It could also relate to heavy delays on the network or remote subsystems (targets for ALCEML devices). See below for the different counters of the CNT records (01_cnt.log) to investigate how fast IO is processed (from second to second), how much IO remains in flight and particularly where (in which stage) it remains in flight. This gives a good first insight on where the problem is. 
Also below you find a short pattern to identify IO stuck in state 2 waiting for buffers. this is a problem with buffer shortage in the target, which could be combined slow or stuck processing with incoming io request bursts from clients and other nodes.

4. identify problems with ALCEML
Problems with ALCEML could surface on the node to which they are local or on a remote node connected to those alcemls. Alceml errors are rare, but they are printed with ALCEMLD. 

5. other (seg fault)
You must distinguish a node abort (clearly visible by the aborted print) from a real seg fault. A real seg fault should never happen and requires deeper investigation

Distrib counters (CNT records):

counter[1] - total read requests
counter[2] - total write requests
counter[3] - total unmap requests
counter[4]; - completed read requests
counter[5]; - completed write requests
counter[6]; - completed unmap requests

// Combination of counters
counter[11] : error counter
counter[11]; counter[12]; - cluster map is empty
counter[11]; counter[13]; - failed to add IO request to the queue (queue is busy)
counter[11]; counter[14]; - unsupported IO request type
counter[11]; counter[15]; - IO request to the underlaying device is failed
counter[11]; counter[16]; - failed to notify about finished IO request
counter[11]; counter[17]; - read failed
counter[11]; counter[18]; - write failed
counter[11]; counter[19]; - failed to find available location (write request)
counter[11]; counter[21]; - failed to find available location (full stripes read)
counter[11]; counter[22]; - in-memory map contains invalid data (not expected case)
counter[11]; counter[23]; - read request to alceml is failed
counter[11]; counter[24]; - write request to alceml is failed
counter[11]; counter[25]; - unmap request to alceml is failed
counter[29] - inflight IOs in alcemls
counter[30] - inflight IOs in distribs

keywords:
DISTRIBD -> keyword to find errors in distrib
ALCEMLD -> keyword to find errors in alceml
JCERR -> keyword to find errors in JC


pattern of nvmf waiting for buffers (ran out of buffers):

 2026-04-09 18:24:32.059  src=ip-172-31-33-220.ec2.internal  ctr=spdk_8083  lvl=3  [2026-04-09 18:24:32.058964] tcp.c:3203:nvmf_tcp_dump_delay_req_status: *NOTICE*: time per state(us) qpair
     0x30c77f0 (QID 3): [1]=0.054 [2]=0.030 [3]=0.012 [6]=0.530 [8]=0.034 [9]=8962.684 [11]=0.026 [12]=0.017 [13]=2244043.390 [15]=0.027


  │ State │              Name               │                      Meaning                      │
  ├───────┼─────────────────────────────────┼───────────────────────────────────────────────────┤
  │ 1     │ NEW                             │ Request just allocated                            │
  ├───────┼─────────────────────────────────┼───────────────────────────────────────────────────┤
  │ 2     │ NEED_BUFFER                     │ Waiting for a DMA buffer to be assigned           │
  ├───────┼─────────────────────────────────┼───────────────────────────────────────────────────┤
  │ 3     │ HAVE_BUFFER                     │ Buffer assigned, ready to proceed                 │
  ├───────┼─────────────────────────────────┼───────────────────────────────────────────────────┤
  │ 6     │ TRANSFERRING_HOST_TO_CONTROLLER │ Receiving H2C write data from initiator           │
  ├───────┼─────────────────────────────────┼───────────────────────────────────────────────────┤
  │ 7     │ AWAITING_R2T_ACK                │ Waiting for R2T acknowledgement (write path)      │
  ├───────┼─────────────────────────────────┼───────────────────────────────────────────────────┤
  │ 8     │ READY_TO_EXECUTE                │ H2C transfer done, ready to submit to NVMe        │
  ├───────┼─────────────────────────────────┼───────────────────────────────────────────────────┤
  │ 9     │ EXECUTING                       │ Submitted to NVMe backend, waiting for completion │
  ├───────┼─────────────────────────────────┼───────────────────────────────────────────────────┤
  │ 11    │ EXECUTED                        │ NVMe completed, ready to respond                  │
  ├───────┼─────────────────────────────────┼───────────────────────────────────────────────────┤
  │ 12    │ READY_TO_COMPLETE               │ Preparing response PDU                            │
  ├───────┼─────────────────────────────────┼───────────────────────────────────────────────────┤
  │ 13    │ TRANSFERRING_CONTROLLER_TO_HOST │ Sending C2H response data to initiator            │
  ├───────┼─────────────────────────────────┼───────────────────────────────────────────────────┤
  │ 15    │ COMPLETED                       │ Done                                              │ 


If the processing hangs on the cluster-internal layer, it must be either an nvmf-level problem or network or io getting delayed/stuck in alceml --> nvme --> device. 

To look into prints showing the processing on LVS layer (below nvmf, above distrib), here are examples:

  1. LSTAT — blobstore monitoring poller (per role: primary/secondary/tertiary)
  blobstore.c:4863:spdk_bs_monitoring_poller: *NOTICE*: LSTAT primary [3835]  [32]  [3803]  [0]  [0]  [0]
  blobstore.c:4863:spdk_bs_monitoring_poller: *NOTICE*: LSTAT secondary [48]  [48]  [0]  [0]  [0]  [0]
  blobstore.c:4863:spdk_bs_monitoring_poller: *NOTICE*: LSTAT tertiary [48]  [48]  [0]  [0]  [0]  [0]
  Fires every ~1s. Six counters per role — likely: [total] [completed] [in-flight] [failed] [redirected] [?]

  ---
  2. IO redirect CNT — redirect counters per role
  lvol.c:3523:spdk_lvs_IO_redirect: *NOTICE*: IO redirect CNT SECONDARY: t[0] c[0] f[0] tc[0]
  lvol.c:3523:spdk_lvs_IO_redirect: *NOTICE*: IO redirect CNT TERTIARY: t[0] c[0] f[0] tc[0]
  Fires every ~1s. t[total] c[completed] f[failed] tc[total-cumulative?]

  ---
  3. IO hublvol CNT — hub lvol IO counters
  lvol.c:3536:spdk_lvs_IO_hublvol: *NOTICE*: IO hublvol CNT: t[61] c[0] tc[16]



 
















 






  

   