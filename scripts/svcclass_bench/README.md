# Replication service-class benchmark

Measures what the replication service class does to client IO and to
replication time. Two scenarios, each run at several replication shares:

- **(a) client IO only.** A mixed fio load on one volume per source storage
  node: six jobs per volume at iodepth 16, covering 4k to 128k blocks, reads
  and writes, and random and sequential access (`mixed.fio`).
- **(b) client IO plus replication.** The same load while every volume
  replicates into an equal target cluster at a 1-minute cadence.

The share is the percentage of the data plane's IO scheduling given to the
replication class. It is set per cluster (`qos_replication_weight`, or
`StorageCluster.spec.serviceClasses.replicationShare` through the operator).

## What is measured

| Metric | Definition |
|---|---|
| Client IOPS, MB/s | Sum over every fio job on every client, averaged per second over the window. For (a) the window is the measured period. For (b) it is the whole replication phase, and separately the ramp before steady state and the steady state itself. |
| Replication iteration | One internal snapshot of one volume: from when the snapshot is taken to when its copy exists on the target. |
| Time to steady state | From the moment replication is switched on until the slowest volume reaches steady state. A volume is steady from the first of 5 consecutive iterations, after the initial full copy, whose durations lie within max(25%, 2 s) of their median. |
| Ramp iteration time | Mean replication time per iteration before steady state, including the initial full copy. |
| Steady iteration time | Mean and p95 replication time per iteration in steady state. |
| Steady period | Time between consecutive snapshots of a volume in steady state, which is the RPO actually achieved. |
| Landing class ok | Whether every landing volume on the target ran in the replication class (read back from SPDK). |

The cadence never queues work. The next snapshot of a volume is only taken
once the previous one has replicated, so when a transfer takes longer than the
interval, the period stretches instead of a backlog forming. Steady state is
therefore judged by the durations, and "keeps up with 1 minute" shows up as a
steady period of about 60 s.

## How the share acts

The data plane schedules 8 classes by weight: Default, metadata/migration (25),
user classes, and replication (the share). Weights only divide bandwidth among
classes that are busy at that moment. With metadata idle and only client and
replication traffic competing, a share of 15/30/60 works out to roughly
20/40/80% of the scheduled bandwidth. For effective shares of 15/30/60, use
11/23/45.

## Running it

1. **Storage-node image.** On a build box with docker and AWS credentials:

   ```bash
   # locally
   git -C spdk bundle create spdk.bundle origin/new_md_journal..feature/replication-transfer-class
   # autocrlf off: a Windows checkout would put CRLF into the Dockerfile's shell blocks
   git -C ultra -c core.autocrlf=false archive --format=tar.gz origin/main > ultra.tgz
   # on the box
   build_images.sh spdk.bundle <spdk-sha> ultra.tgz \
       565979732541.dkr.ecr.us-east-1.amazonaws.com/<repo> svcclass-1
   ```

2. **Bed.** Two 3-node clusters on one control plane, and 3 clients:

   ```bash
   SPDK_IMAGE=<image from step 1> python scripts/svcclass_bench/deploy.py
   python scripts/hotfix_repl_lab.py      # control plane runs this tree's sbcli
   ```

   Tag the instances `long-term-test=true`, or the instance stopper shuts them
   down after 12 hours.

3. **Volumes.** `python scripts/svcclass_bench/bench.py prepare --size 500G`
   creates one volume per source node and connects each one on its own client.
   The device is found by its NQN and checked again before every write. Each
   volume is then filled once.

4. **Runs.** `bench.py matrix --shares 15,30,60` runs (a) and (b) for every
   share. Add `--repeat 2` to see run-to-run variance; the order of (a) and (b)
   alternates between repeats. A single run is
   `bench.py run --scenario b --share 30`. Each (b) run clears replication
   afterwards, which deletes the internal snapshots on both sides, so every
   run starts with a full initial copy.

5. **Report.** `bench.py report` writes `results/summary.md` and
   `results/summary.json`. Each run directory also keeps the raw fio logs,
   the snapshot records, and the replication service log. The `XFER-TIMING`
   lines in that log feed `scripts/xfer_timing_report.py`.

Rough duration with the defaults (5 min warm-up, 30 min measured, up to
120 min of replication per run): about 2 hours per share, 6 to 7 hours for the
matrix.

## Known limits

- Only 2 class bits reach the device layer (alceml), so class 7 is scheduled as
  Default there. The share acts in distrib's scheduler only.
- The target cluster has no client IO, so on that side the class has nothing
  to compete with. The effect is on the source, where replication reads
  compete with the client load.
- The read path's class override (`spdk_blob_io_read_with_priority`) has a
  unit test that cannot run on `new_md_journal` yet: every `blob_ut` test
  crashes in the shared blobstore setup there, with or without the change.
