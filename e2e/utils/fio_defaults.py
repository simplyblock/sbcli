"""FIO knobs that must be identical everywhere, defined once.

The values here are deliberately module-level constants rather than test
attributes or call kwargs. A test that wants a different latency ceiling than
the rest of the suite is almost always a test papering over a slow path, and
when the ceiling drifted per-test it stopped being comparable: before this
module existed the suite carried 20s in four places, 40s in a fifth, a dead
30s in a sixth, and nothing at all on any k8s job built through
create_fio_job -- so "did IO stay under the ceiling" meant a different thing
in every lane, and the docker/k8s halves of a single test disagreed.

Change the value HERE and it changes everywhere.
"""

#: Longest a single IO may take before FIO fails the job (`--max_latency`).
#:
#: This is a correctness gate, not a performance target. Storage that stalls
#: a single IO for this long is indistinguishable from storage that dropped
#: it, and every outage lane in the suite exists to prove the cluster does not
#: do that -- so a node being deliberately killed is not a licence to exceed
#: it. If a test cannot hold the ceiling through an outage, that is the
#: finding, not a reason to raise it.
#:
#: 40s, matching what continuous_k8s_native_failover used before this module
#: existed. It was briefly 20s, which is the value the scale-break lane picks
#: deliberately for a load test; for an outage lane 40s is the house number.
#: Neither choice was ever the issue -- the k8s run of 2026-09-22 measured a
#: single 4K read at 76s and a write at 64s, so the stalls that matter clear
#: either ceiling by a wide margin.
FIO_MAX_LATENCY = "40s"
