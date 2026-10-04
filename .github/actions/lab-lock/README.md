# Lab host lock

Reserves the lab hosts a workflow run touches, so a second run cannot reboot and
wipe machines the first one is testing on.

GitHub's `concurrency` cannot express this. A run belongs to exactly one group, so
a group cannot name a *set* of hosts; `e2e-bootstrap`'s group is keyed on `MNODES`
alone, which leaves two runs with different management nodes but overlapping
storage or client nodes free to collide. And every lab workflow picks its own
group name, so the groups never exclude each other even on identical hosts.

The implementation is [`e2e/lab_lock.sh`](../../../e2e/lab_lock.sh); this action
is the thin wrapper that workflows call. It is a JavaScript action rather than a
composite one for a single reason: only JavaScript actions get a `post` step,
and that is what releases the lock. `main.js` and `post.js` do nothing but
marshal inputs into the script.

## The lock

One file per host, `/var/lib/sb-ci/lock`:

```json
{
  "owner": "simplyblock-io/sbcli#1234567",
  "run_attempt": 1,
  "workflow": "E2E Bootstrap",
  "job": "bootstrap-and-e2e",
  "label": "run2",
  "run_url": "https://github.com/simplyblock-io/sbcli/actions/runs/1234567",
  "hosts": "192.168.10.165 192.168.10.201 192.168.10.210",
  "acquired_at": "2026-09-15T10:02:11Z"
}
```

It is on the host because the host is the resource: any workflow, on any runner
label, that can reach the host sees the same lock. It is under `/var/lib` rather
than `/run` because the run reboots the hosts it holds — a lock in tmpfs would
evaporate mid-run and have to be re-asserted on a timer.

Acquisition is `ln` of a fully-written temp file onto the lock name. `ln` fails if
the name exists, and publishes name and content in one step, so exactly one run
wins and a concurrent reader never sees a half-written lock. Hosts are taken in
sorted order, all or nothing: a run that loses a race releases whatever it already
took, so two runs contending for overlapping sets cannot wedge each other holding
half the lab.

`owner` is `repo#run_id` in one string, so the remote half settles ownership with
a single fixed-string grep and never has to parse JSON. It is keyed on `run_id`
alone, not `run_id` + `run_attempt`: a re-run keeps the id, so an attempt-scoped
owner would deadlock a re-run against the lock its own previous attempt left
behind.

Every value the remote half receives arrives as an environment assignment, and the
payload as base64, so a workflow or label name containing quotes or shell
metacharacters lands on the host verbatim rather than being evaluated.

**Held means held.** There is no lease, no expiry and no liveness probe. A lock is
released by the run that took it; anything left behind is cleared by a person.

## Lifecycle in e2e-bootstrap

`bootstrap-and-e2e` calls the action once, in `acquire` mode, after the cheap
validation but before the first step that destroys anything. Nothing calls
`release`: the action's `post` step does it when the job ends, whether the job
passed, failed or was cancelled.

The state that drives the post step is written *before* the hosts are taken, not
after, because a cancellation landing mid-`acquire` kills the rollback that
`acquire` would otherwise do itself. Releasing a host this run does not hold is
a no-op, so the early write costs nothing.

`verify` exists for workflows that spread the work over several jobs, where the
lock can change hands between them. `e2e-bootstrap` does everything in one job,
so it has nothing to verify against.

`acquire` tolerates a short handover window (`LOCK_GRACE_SECONDS`, default 120s):
`cancel-in-progress` is the documented kill switch for this lab, so a new run
routinely starts while the run it cancelled is still in its post step, releasing.
Past that window a conflict is real contention and the run fails.

## Operator runbook

Who holds a host:

```bash
ssh root@192.168.10.201 cat /var/lib/sb-ci/lock
```

Across the whole set, with reachability:

```bash
SSH_PASSWORD=... ./e2e/lab_lock.sh status \
  --hosts "192.168.10.201 192.168.10.202 192.168.10.210"
```

**A run failed to start because a host was held.** The error names the holding
run's URL — open it first. If that run is live, the lock is doing its job: wait.
If it is finished or cancelled and the lock survived anyway (a force-cancel, or a
runner that died mid-run), clear it:

```bash
SSH_PASSWORD=... ./e2e/lab_lock.sh break --force --hosts "192.168.10.201"
```

`break` removes the lock whoever owns it, which is why it demands `--force`. Only
use it once you have confirmed the holding run is not live — breaking a live lock
puts two runs on the same hosts, which is the failure this whole mechanism exists
to prevent.

To bypass locking, set `LOCK_DISABLE: "true"` in the job's `env:` block in
`e2e-bootstrap.yml` on the branch you dispatch. It is not a workflow input
because the workflow is at GitHub's 25-input cap. The run logs a warning and
reserves nothing; a concurrent run can wipe its hosts.

## Checking a change to the script

`--local` swaps ssh for a local directory, one file per named host, running the
identical bash snippets:

```bash
GITHUB_RUN_ID=1 ./e2e/lab_lock.sh acquire --local --lock-dir /tmp/x --hosts "a b"
GITHUB_RUN_ID=2 ./e2e/lab_lock.sh acquire --local --lock-dir /tmp/x --hosts "a" \
  --grace-seconds 0   # fails, naming run 1
GITHUB_RUN_ID=1 ./e2e/lab_lock.sh release --local --lock-dir /tmp/x --hosts "a b"
```

## Adopting it in another workflow

Every workflow that SSHes into `192.168.10.x` should take the lock, or it can
still stomp a locked run. Adding it is a checkout plus one step -- the release
comes with it:

```yaml
- uses: actions/checkout@v7
  with:
    sparse-checkout: |
      .github/actions/lab-lock
      e2e/lab_lock.sh
    sparse-checkout-cone-mode: false

- uses: ./.github/actions/lab-lock
  with:
    mode: acquire
    hosts: ${{ env.LAB_HOSTS }}
```

A workflow that does spread its lab work over several jobs cannot rely on this:
the post step releases at the end of *its own* job, so the next job would find
the hosts free. Such a workflow needs `acquire` in the first job, `verify` in
each later one, and `release` from a final `if: always()` job.
