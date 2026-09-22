"""Metadata-journal health checks for lblk (non-NVMe) clusters.

Background
----------
On generic Linux block devices there is no 4K atomic-write guarantee, so the
blobstore protects its metadata with a journal: a 64 MiB ring at the top of each
device, 8 KiB entries of ``[4K header][4K md page]``, each carrying a CRC32C.
The design does not assume atomicity at all -- it detects torn writes and
discards them.

That last part is why a test cannot rely on error messages alone. A torn or
corrupt journal entry is **silently treated as empty by design** -- the recovery
scan just skips it. So a broken journal produces no "bad magic" and no error;
it produces wrong data later, somewhere else. The only direct evidence available
is the journal's own statistics, which is what this module reads.

Two RPCs back it, and **neither is registered in ``scripts/rpc.py``**, so they
cannot be invoked as ``rpc.py <method>``. They have to go over the socket as raw
JSON-RPC, which is what ``call_rpc`` below does.

    bdev_lvol_get_md_journal_stats   {uuid | lvs_name}
    bdev_lvol_set_md_journal_drain   {uuid | lvs_name, paused}

``set_md_journal_drain`` is explicitly a test hook. The drain otherwise keeps the
ring at roughly one entry under any load the blobstore can generate, so
"recovery with a non-empty ring" is not reachable by workload alone -- pausing
the drain is the only way to test the path that matters.
"""

import json


#: NOTICE-level lines the journal emits. Tests assert on these.
LOG_ENABLED = "md journal enabled"
LOG_RECOVERY_EMPTY = "md journal recovery: ring empty"
LOG_RECOVERY_ENTRIES = "md journal recovery:"      # "...: N entries to drain"
LOG_RESCAN = "md journal rescan: re-reading the ring on takeover"

#: Hard failures. Note none of these mention "magic" -- a bad entry is dropped
#: silently, so the blobstore CRC errors are the real signal that the journal
#: failed to protect what it was supposed to protect.
JOURNAL_FATAL_MARKERS = (
    "md journal recovery failed",
    "md journal recovery read",
    "store has an md journal but the device cannot carry it",
    "unsupported base blocklen",
    "device too small for md journal",
)

#: Write failures that the journal HANDLES. Reported, never fatal.
#:
#: "md journal append failed" was in the fatal list and should not have been.
#: Checked against append_write_cpl (blob_md_journal.c:355): on failure it rolls
#: mem_head back, decrements used_slots, completes the op with the error and
#: pumps the queue. The slot is un-allocated, so nothing is left half-written --
#: that is the journal correctly REFUSING to acknowledge an entry it could not
#: persist, which is the opposite of corruption.
#:
#: It fires during normal failover. From a real run, all inside 700us:
#:
#:   alg_io_split_parity2.cpp:922  DISTRIBD stopped write IO
#:   alg_journal.cpp:3420          received a change_leadership event
#:   blob_md_journal.c:511         md journal home write failed: -5 (retry)
#:   blob_md_journal.c:355         md journal append failed: -5
#:   lvol.c:2942                   Cannot update lvolstore on failover
#:   blobstore.c:10903             Updating failed and unfreeze IOs on failover
#:
#: The distrib layer blocks IO while leadership moves (b_block_new_io=1), so
#: the append gets EIO by design. Treating that as corruption failed a test on
#: the cluster behaving exactly as intended. The home-write marker says
#: "(retry)" in the message itself.
JOURNAL_TRANSIENT_MARKERS = (
    "md journal append failed",
    "md journal home write failed",
    "md journal entry zeroing failed",
)

#: Blobstore metadata corruption that ABORTS the operation reporting it. These
#: are the real gate: if the journal did its job they never appear.
#:
#: Checked against the SPDK source rather than collected by keyword, because
#: the two CRC messages read almost identically and only one is a defect:
#:
#:   blobstore.c:2187  "Metadata page %d crc mismatch for blobid 0x%..."
#:                     -> blob_load_final(ctx, -EINVAL). The blob fails to load.
#:
#:   blobstore.c:1959  "Extenet metadata page %d crc mismatch for blobid 0x%..."
#:                     (sic, the typo is SPDK's) -> same, an extent page fails.
BLOBSTORE_CORRUPTION_MARKERS = (
    "crc mismatch for blobid",
    "extenet metadata page",        # sic: the typo is in the SPDK source
)

#: Logged at ERRLOG but NOT a defect, so they must not fail a run.
#:
#: bs_update_cur_md_page_valid (blobstore.c:14467) is a predicate, not an error
#: path. It is how the lvstore-update scan walks the md region, and an
#: unallocated page is simply invalid: the CRC of a zeroed page does not match,
#: so it logs "Metadata page is all zero." and "crc mismatch for blob." and
#: returns false. The caller (blobstore.c:14897) then falls straight through to
#: bs_update_replay_md_chain_cpl and carries on -- nothing propagates.
#:
#: Note "for blob." versus the fatal "for blobid 0x...". One trailing word is
#: the whole difference between a normal scan terminator and a blob that failed
#: to load, and this scan runs on every failover and restart, so treating them
#: alike fails every recovery test on a healthy cluster.
BLOBSTORE_BENIGN_MARKERS = (
    "crc mismatch for blob.",
    "metadata page is all zero",
)


class MdJournalAbsent(RuntimeError):
    """The SPDK build does not carry the md journal feature at all.

    Distinct from MdJournalError, which means the journal should be there and
    is not working. Raised only on JSON-RPC -32601 (method not registered).
    """


class MdJournalError(RuntimeError):
    """Journal is absent, unhealthy, or reported corruption."""


def _last_error(text, limit=300):
    """Condense a remote stderr dump to the one line that says what went wrong.

    The payload runs `python3 -c` inside the SPDK container, so a failure comes
    back as a full Python traceback whose middle is a 2000-character hex blob
    and a run of `^^^^` carets. Embedding that verbatim made the exception
    message span many lines, and everything downstream that reports a failure --
    the workflow summary and the Slack message both grep a single line out of
    output.log -- picked up whichever fragment happened to be last, so a run
    failed with the message ") . On an lblk cluster ..." and no cause at all.

    A traceback's last non-empty line is the exception itself, which is the part
    worth keeping: "PermissionError: [Errno 13] Permission denied".
    """
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    if not lines:
        return "no stderr"
    # Carets and the hex payload are noise; the exception line is what matters.
    for line in reversed(lines):
        if not set(line) <= set("^~ "):
            return line[:limit]
    return lines[-1][:limit]


def call_rpc(ssh_obj, node, exec_prefix, sock, method, params=None,
             timeout=120, logger=None):
    """Issue a raw JSON-RPC call to an SPDK socket and return the parsed result.

    ``exec_prefix`` is however this platform reaches the SPDK container, e.g.
    ``docker exec spdk_8080`` or ``kubectl exec <pod> -c spdk-container -n ns --``.

    Raw JSON over the socket rather than ``scripts/rpc.py`` because the journal
    methods are not registered there; rpc.py would reject them as unknown
    subcommands before ever reaching SPDK.
    """
    payload = {"jsonrpc": "2.0", "id": 1, "method": method}
    if params:
        payload["params"] = params

    # Written to a file and read back rather than interpolated into the python
    # -c string: the nested quoting through ssh -> sh -> docker exec -> python
    # is a reliable source of silent breakage.
    req = json.dumps(payload)
    script = (
        "import socket,sys,json\n"
        "s=socket.socket(socket.AF_UNIX)\n"
        f"s.settimeout({timeout})\n"
        f"s.connect({sock!r})\n"
        f"s.sendall({req!r}.encode())\n"
        "buf=b''\n"
        "while True:\n"
        "    c=s.recv(65536)\n"
        "    if not c: break\n"
        "    buf+=c\n"
        "    try:\n"
        "        json.loads(buf.decode()); break\n"
        "    except Exception: pass\n"
        "sys.stdout.write(buf.decode())\n"
    )
    b64 = script.encode("utf-8").hex()
    # sudo inside the container, because the SPDK unix socket is not readable by
    # the container's default user: without it every call dies on
    # `s.connect(...)` with "PermissionError: [Errno 13] Permission denied"
    # before a single byte is sent. This matches what the rest of the suite
    # already does -- TestClusterBase._rpc_via_docker_exec runs
    # "sudo python spdk/scripts/rpc.py" inside the container on both platforms.
    #
    # Resolved in the container's own shell rather than assumed, so a container
    # that runs as root and ships no sudo still works.
    cmd = (f"{exec_prefix} sh -c \"SUDO=; command -v sudo >/dev/null 2>&1 "
           f"&& SUDO=sudo; \\$SUDO python3 -c \\\"import binascii;"
           f"exec(binascii.unhexlify('{b64}').decode())\\\"\"")

    out, err = ssh_obj.exec_command(node, cmd, timeout=timeout + 30,
                                    max_retries=1)
    blob = (out or "").strip()
    if not blob:
        raise MdJournalError(
            f"{method} returned nothing from {node}: {_last_error(err)}")
    try:
        doc = json.loads(blob)
    except ValueError:
        raise MdJournalError(f"{method} returned non-JSON from {node}: "
                             f"{blob[:400]}")
    if "error" in doc:
        # -32601 is JSON-RPC "Method not found": the RPC is not registered in
        # this SPDK binary, which means the build has no md journal at all --
        # a different thing from a journal that failed to start.
        if (doc["error"] or {}).get("code") == -32601:
            raise MdJournalAbsent(
                f"{method} is not registered on {node}: this SPDK build has "
                f"no md journal ({doc['error']})")
        raise MdJournalError(f"{method} failed on {node}: {doc['error']}")
    if logger:
        logger.info("[md-journal] %s -> %s", method, doc.get("result"))
    return doc.get("result")


def get_stats(ssh_obj, node, exec_prefix, sock, lvs_name=None, uuid=None,
              logger=None):
    """Journal statistics for one lvstore.

    An ``-ENODEV`` here means the store has no journal at all -- i.e. it was
    created before the feature, or in a mode where it was not enabled. On an
    lblk cluster that is a failure, not an absence.
    """
    params = {}
    if uuid:
        params["uuid"] = uuid
    elif lvs_name:
        params["lvs_name"] = lvs_name
    else:
        raise ValueError("one of lvs_name or uuid is required")
    return call_rpc(ssh_obj, node, exec_prefix, sock,
                    "bdev_lvol_get_md_journal_stats", params, logger=logger)


def set_drain_paused(ssh_obj, node, exec_prefix, sock, paused,
                     lvs_name=None, uuid=None, logger=None):
    """Pause or resume the background drain (test hook).

    Pausing lets entries accumulate in the ring so that recovery has something
    to replay. Always resume, or the ring fills and metadata writes block.
    """
    params = {"paused": bool(paused)}
    if uuid:
        params["uuid"] = uuid
    elif lvs_name:
        params["lvs_name"] = lvs_name
    else:
        raise ValueError("one of lvs_name or uuid is required")
    return call_rpc(ssh_obj, node, exec_prefix, sock,
                    "bdev_lvol_set_md_journal_drain", params, logger=logger)


def assert_journal_enabled(ssh_obj, node, exec_prefix, sock, lvs_name=None,
                           uuid=None, logger=None):
    """Fail unless the lvstore actually has a live, intercepting journal.

    The point of running on lblk is that the journal is doing the work that
    device atomicity would otherwise do. A cluster that came up without it looks
    healthy and is not testing the thing under test -- so this is checked
    explicitly rather than assumed.
    """
    try:
        stats = get_stats(ssh_obj, node, exec_prefix, sock,
                          lvs_name=lvs_name, uuid=uuid, logger=logger)
    except MdJournalAbsent:
        # Let the caller decide. Whether a journal-less build is acceptable is
        # a policy question about the run, not something this helper can know.
        raise
    except MdJournalError as exc:
        raise MdJournalError(
            f"no md journal on {lvs_name or uuid} at {node}: {exc}. On an lblk "
            f"cluster the journal is what replaces the 4K atomic-write "
            f"guarantee, so its absence invalidates the run.")
    if not stats or not stats.get("enabled"):
        raise MdJournalError(
            f"md journal present but not enabled on {lvs_name or uuid} at "
            f"{node}: {stats}")
    if logger:
        logger.info("[md-journal] enabled on %s: %d/%d slots used, "
                    "head=%s tail=%s", lvs_name or uuid,
                    stats.get("used_slots", 0), stats.get("num_slots", 0),
                    stats.get("mem_head"), stats.get("mem_tail"))
    return stats


def scan_log_for_corruption(text, context=""):
    """Classify SPDK log output. Returns (fatal, informational).

    Split out so it can be run over a collected log file as well as over live
    output, and unit-tested without a cluster.
    """
    low = (text or "").lower()
    fatal = [m for m in JOURNAL_FATAL_MARKERS if m in low]
    fatal += [m for m in BLOBSTORE_CORRUPTION_MARKERS if m in low]
    info = []
    # Reported so a run still shows they happened, but never fatal -- see
    # BLOBSTORE_BENIGN_MARKERS for why the near-identical wording matters.
    info += [f"benign md scan: {m}" for m in BLOBSTORE_BENIGN_MARKERS
             if m in low]
    info += [f"journal retry (expected during failover): {m}"
             for m in JOURNAL_TRANSIENT_MARKERS if m in low]
    for marker, label in ((LOG_ENABLED, "journal enabled"),
                          (LOG_RECOVERY_EMPTY, "recovery: ring empty"),
                          (LOG_RESCAN, "rescan on takeover")):
        if marker in low:
            info.append(label)
    if LOG_RECOVERY_ENTRIES in low and LOG_RECOVERY_EMPTY not in low:
        info.append("recovery: entries replayed")
    return fatal, info


def assert_no_corruption(text, context="", logger=None):
    """Raise on any journal-fatal or blobstore-CRC marker in *text*."""
    fatal, info = scan_log_for_corruption(text, context)
    if logger and info:
        logger.info("[md-journal] %s: %s", context or "log", ", ".join(info))
    if fatal:
        where = f" ({context})" if context else ""
        raise MdJournalError(
            f"metadata corruption{where}: {fatal}. The journal exists to make "
            f"this impossible on a non-atomic device, so this is a product "
            f"defect rather than a test artefact.")
    return info
