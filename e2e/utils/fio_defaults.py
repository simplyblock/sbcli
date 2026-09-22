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
#: a single IO for 20 seconds is indistinguishable from storage that dropped
#: it, and every outage lane in the suite exists to prove the cluster does not
#: do that -- so a node being deliberately killed is not a licence to exceed
#: it. If a test cannot hold 20s through an outage, that is the finding.
FIO_MAX_LATENCY = "20s"
