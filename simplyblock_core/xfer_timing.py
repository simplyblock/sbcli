"""Phase timing for the replication/cutover pipeline.

Soak analysis 2026-08-27 could not answer a basic question -- of the 588s a
convergence round took, how much was the DATA TRANSFER and how much was
orchestration? Round duration was the only number available, and it spans the
landing-volume create, the hub attach, the transfer, the detach, add_clone and
convert on two nodes, several DB writes, and up to TASK_EXEC_INTERVAL_SEC of
task-runner latency per state change. Every hardware-level explanation we
tested came out an order of magnitude off because the number being explained
was not a throughput.

So: emit one line per phase, parseable without guessing.

    XFER-TIMING t=1787868791.244 phase=transfer lvol=1c8874f3 snap=a0f48bf5 \
                round=2 ms=1843.2 bytes=33554432 mbps=18.2 ok=1

Every line carries its own epoch timestamp because the container clock is
skewed from the host's, so `docker service logs -t` ordering cannot be trusted
across services. Grep for XFER-TIMING and feed it to
scripts/xfer_timing_report.py.

Instrumentation only -- no behaviour change, and every helper is safe to call
from any thread and cheap enough for the hot path.
"""
import logging
import time
from contextlib import contextmanager

logger = logging.getLogger()

_PREFIX = "XFER-TIMING"


def _fmt(**fields):
    parts = []
    for k, v in fields.items():
        if v is None:
            continue
        if isinstance(v, float):
            parts.append("%s=%.3f" % (k, v))
        else:
            parts.append("%s=%s" % (k, v))
    return " ".join(parts)


def _short(value):
    """Ids are long and the interesting part is the head."""
    if value is None:
        return None
    text = str(value)
    return text[:8] if len(text) > 8 else text


def now():
    """Clock for callers that need a start marker to pair with gap().

    Exposed here so an instrumented module does not have to import time just
    to be measured -- a missing `import time` in replication_final_step.py
    would have raised NameError mid-cutover.
    """
    return time.time()


def stamp(phase, lvol=None, snap=None, round=None, **extra):
    """A point event: something happened now, with no duration."""
    logger.info("%s %s", _PREFIX, _fmt(
        t=time.time(), phase=phase, lvol=_short(lvol), snap=_short(snap),
        round=round, **extra))


@contextmanager
def phase(name, lvol=None, snap=None, round=None, **extra):
    """Time a block and emit its duration, whether it succeeds or raises.

    Usage:
        with xfer_timing.phase("hub_attach", lvol=lvol_id) as ph:
            ...
            ph["bytes"] = n          # optional, folded into the line
    """
    started = time.time()
    box: dict = {}
    ok = 1
    try:
        yield box
    except BaseException:
        ok = 0
        raise
    finally:
        elapsed_ms = (time.time() - started) * 1000.0
        fields = dict(extra)
        fields.update(box)
        nbytes = fields.pop("bytes", None)
        if nbytes:
            try:
                fields["bytes"] = int(nbytes)
                fields["mbps"] = (int(nbytes) / 1e6) / max(elapsed_ms / 1000.0, 1e-9)
            except (TypeError, ValueError):
                fields["bytes"] = nbytes
        logger.info("%s %s", _PREFIX, _fmt(
            t=time.time(), phase=name, lvol=_short(lvol), snap=_short(snap),
            round=round, ms=elapsed_ms, ok=ok, **fields))


def gap(name, since, lvol=None, snap=None, round=None, **extra):
    """Time from an earlier epoch to now -- for waits nobody is inside of.

    The dead time between one round completing and the next snapshot being
    taken is exactly this shape: no call to wrap, just two moments.
    """
    if not since:
        return
    elapsed_ms = (time.time() - float(since)) * 1000.0
    fields = dict(extra)
    nbytes = fields.pop("bytes", None)
    if nbytes:
        try:
            fields["bytes"] = int(nbytes)
            fields["mbps"] = (int(nbytes) / 1e6) / max(elapsed_ms / 1000.0, 1e-9)
        except (TypeError, ValueError):
            fields["bytes"] = nbytes
    logger.info("%s %s", _PREFIX, _fmt(
        t=time.time(), phase=name, lvol=_short(lvol), snap=_short(snap),
        round=round, ms=elapsed_ms, ok=1, **fields))
