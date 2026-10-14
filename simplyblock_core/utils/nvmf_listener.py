"""Create or update one NVMf subsystem listener, in its intended ANA state.

The rule (2026-10-06, lblk_rapid_outage run of 2026-09-26):

- a listener that does NOT exist yet is created once, with the intended
  ``ana_state`` passed to ``nvmf_subsystem_add_listener``. It is never
  created in some default state and flipped afterwards;
- a listener that DOES exist is never added a second time. When its ANA
  state has to change, ``nvmf_subsystem_listener_set_ana_state`` changes it
  (for the volume's own ANA group when ``anagrpid`` is given, so a shared
  subsystem's other volumes keep their state).

Calling ``nvmf_subsystem_add_listener`` for an existing listener is what the
restart flow's "demote the old leader" step did: SPDK answered every such
call with ``nvmf_rpc_listen_paused: Listener already exists`` (3,000 times
in that run, 39% of its 7,660 add_listener calls) and the listener's ANA
state did not change at all.
"""
import logging

logger = logging.getLogger()

CREATED = "created"
ANA_SET = "ana_set"
PRESENT = "present"


def _is_duplicate_listener_error(exc_or_err):
    """SPDK refuses a second add for the same address with -32602 and the
    message "Listener already exists" (nvmf_rpc_listen_paused)."""
    if exc_or_err is None:
        return False
    code = getattr(exc_or_err, "code", None)
    message = getattr(exc_or_err, "message", None)
    if isinstance(exc_or_err, dict):
        code = exc_or_err.get("code", code)
        message = exc_or_err.get("message", message)
    text = f"{message or ''} {exc_or_err}".lower()
    return code == -32602 or "already exists" in text


def _same_address(listener, trtype, traddr, trsvcid):
    addr = listener.get("address", listener) if isinstance(listener, dict) else {}
    return (str(addr.get("trtype", "")).upper() == str(trtype).upper()
            and addr.get("traddr") == traddr
            and str(addr.get("trsvcid")) == str(trsvcid))


def _current_ana_states(listener, anagrpid):
    """ANA states the listener reports, for ``anagrpid`` only when given."""
    states = []
    for entry in (listener.get("ana_states") or []) if isinstance(listener, dict) else []:
        if anagrpid is not None and int(entry.get("ana_group", -1)) != int(anagrpid):
            continue
        if entry.get("ana_state"):
            states.append(entry["ana_state"])
    return states


def find_listener(rpc_client, nqn, trtype, traddr, trsvcid):
    """The matching listener dict, None when absent.

    Raises when the listeners cannot be read: the caller must not guess,
    since guessing "absent" is exactly what produces a duplicate add."""
    listeners = rpc_client.listeners_list(nqn)
    for listener in listeners or []:
        if _same_address(listener, trtype, traddr, trsvcid):
            return listener
    return None


def ensure_listener(rpc_client, nqn, trtype, traddr, trsvcid, ana_state=None,
                    anagrpid=None, set_existing_ana=False):
    """Make sure ``nqn`` listens on ``trtype traddr:trsvcid``.

    Absent  -> ``nvmf_subsystem_add_listener`` with ``ana_state``; returns
               ``CREATED``.
    Present -> no add. With ``set_existing_ana`` and an ``ana_state`` that the
               listener does not already report (for ``anagrpid`` when given),
               ``nvmf_subsystem_listener_set_ana_state``; returns ``ANA_SET``.
               Otherwise ``PRESENT``.

    Raises ``RuntimeError`` when a create or an ANA change fails.
    """
    listener = find_listener(rpc_client, nqn, trtype, traddr, trsvcid)

    if listener is None:
        try:
            ret = rpc_client.listeners_create(nqn, trtype, traddr, trsvcid,
                                              ana_state=ana_state)
        except Exception as e:
            if not _is_duplicate_listener_error(e):
                raise RuntimeError(
                    f"nvmf_subsystem_add_listener {nqn} {trtype} "
                    f"{traddr}:{trsvcid} failed: {e}") from e
            # Raced with another writer; it exists now, treat as present.
            logger.info("Listener %s %s:%s on %s appeared concurrently",
                        trtype, traddr, trsvcid, nqn)
            ret = None
            listener = {}
        else:
            if not ret:
                # An RPC error raises and is handled above, so this only
                # guards a falsy-but-non-error SPDK response: look again
                # before calling it a failure.
                listener = find_listener(rpc_client, nqn, trtype, traddr, trsvcid)
                if listener is None:
                    raise RuntimeError(
                        f"nvmf_subsystem_add_listener {nqn} {trtype} "
                        f"{traddr}:{trsvcid} returned {ret!r}")
                logger.info("Listener %s %s:%s on %s appeared concurrently",
                            trtype, traddr, trsvcid, nqn)
            else:
                logger.info("Created listener %s %s:%s on %s (ana_state=%s)",
                            trtype, traddr, trsvcid, nqn, ana_state or "default")
                return CREATED

    if not (set_existing_ana and ana_state):
        return PRESENT

    current = _current_ana_states(listener, anagrpid) if listener else []
    if current and all(s == ana_state for s in current):
        return PRESENT

    ret = rpc_client.nvmf_subsystem_listener_set_ana_state(
        nqn, traddr, trsvcid, trtype=trtype, ana=ana_state, anagrpid=anagrpid)
    if not ret:
        raise RuntimeError(
            f"nvmf_subsystem_listener_set_ana_state {nqn} {trtype} "
            f"{traddr}:{trsvcid} -> {ana_state} (group {anagrpid}) "
            f"returned {ret!r}")
    logger.info("ANA %s %s:%s on %s (group %s): %s -> %s",
                trtype, traddr, trsvcid, nqn, anagrpid,
                ",".join(current) or "unknown", ana_state)
    return ANA_SET
