"""A deadline the open-ended stress loops stop themselves at.

A continuous stress test has no end of its own -- it runs until something
breaks.  The job that hosts it does have one, ``timeout-minutes``, and when
GitHub reaches it the step is cancelled, which kills the harness wherever it
happens to be standing.  That is the worst moment to stop: mid-outage, with the
end-of-run validation, the diagnostics and the summary lines never reached.

Run 43419924336 is the example.  72h of healthy running, 452 outages, zero IO
errors -- and it ended as ``cancelled`` with ``stress.py`` still driving outages
on the lab five hours later, because cancelling the step does not stop the
process it started.

So the loops stop themselves instead, short of the job's own deadline, through
their normal end-of-run path.  Everything after the stress step in the workflow
-- the NFS copy, the Graylog export, the summary, the lab cleanup -- then runs
with the whole job timeout still ahead of it.

Two inputs, both optional, both from the environment:

``STRESS_DEADLINE_EPOCH``
    Absolute unix time to stop at.  Absolute rather than a duration so that a
    ``--resume`` run inherits the original deadline instead of restarting the
    clock, and so that a reader of the log can tell when the run was always
    going to end.

``STRESS_STOP_FILE``
    A path.  The run stops when the file appears.  ``touch`` it to end a run
    early without killing it -- the point being that the run still collects
    what it has.

Neither set means no deadline at all, which is what ``./stress.py`` run by hand
gets.

The loops call :func:`stop_reason` once per iteration and break when it returns
a string.  That string always begins with :data:`MARKER`, which the workflow's
summary step greps for to tell a planned stop from a failure.
"""

import os
import time

#: Prefix on every stop reason.  The job summary greps the run log for it, so
#: it must stay in step with ``PLANNED STOP`` in the stress workflows.
MARKER = "PLANNED STOP"

_DEADLINE_ENV = "STRESS_DEADLINE_EPOCH"
_STOP_FILE_ENV = "STRESS_STOP_FILE"


def deadline_epoch():
    """The configured stop time as a unix timestamp, or ``None``.

    A value that is not a positive integer is ignored rather than raised on: a
    malformed deadline must not be able to stop a run from starting.
    """
    raw = (os.environ.get(_DEADLINE_ENV) or "").strip()
    if not raw:
        return None
    try:
        value = int(float(raw))
    except ValueError:
        return None
    return value if value > 0 else None


def stop_file():
    """The configured stop-file path, or ``None``."""
    return (os.environ.get(_STOP_FILE_ENV) or "").strip() or None


def seconds_left():
    """Seconds until the deadline, or ``None`` when there is no deadline."""
    deadline = deadline_epoch()
    return None if deadline is None else deadline - int(time.time())


def stop_reason():
    """Why the loop should stop now, or ``None`` to keep going.

    Checked at the top of an iteration, so the iteration already in flight
    always finishes.
    """
    path = stop_file()
    if path and os.path.exists(path):
        try:
            note = open(path, encoding="utf-8", errors="replace").read().strip()
        except OSError:
            note = ""
        return (f"{MARKER}: stop file {path} is present"
                + (f" ({note[:200]})" if note else ""))

    left = seconds_left()
    if left is not None and left <= 0:
        return (f"{MARKER}: run budget reached — deadline was "
                f"{time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(deadline_epoch()))}")
    return None


def budget_note():
    """One line for the start of the run, saying when it is going to stop.

    Logged once, so that a reader eight hours in does not have to work out
    whether the run has a deadline at all.
    """
    deadline = deadline_epoch()
    path = stop_file()
    if deadline is None and not path:
        return "Run budget: none — this run stops only on failure."

    parts = []
    if deadline is not None:
        left = max(0, deadline - int(time.time()))
        parts.append(
            "stops at "
            f"{time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(deadline))} "
            f"({left // 3600}h {(left % 3600) // 60}m from now)"
        )
    if path:
        parts.append(f"or when {path} appears")
    return "Run budget: " + ", ".join(parts) + "."
