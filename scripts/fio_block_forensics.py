"""Decode fio verify headers around a corrupted offset.

Every block fio writes starts with a verify header carrying its own identity
and, crucially, the time it was written. So even with no --write_iolog, the
blocks *neighbouring* a corrupted one tell you when that region was last
written -- which is what bounds a corruption in time.

That matters because every corruption in this campaign has been either "no fio
header at all" or "a valid header belonging to a different block". Reading the
neighbours answers: was this region ever written, when, and does the bad block
carry someone else's identity?

  fio_block_forensics.py FILE OFFSET [--window 16] [--bs 4096]

Header layout (fio verify.h, struct verify_header):
    uint16 magic (0xacca) | uint16 verify_type | uint32 len
    uint64 rand_seed | uint64 offset | uint32 time_sec | uint32 time_usec
"""
import argparse
import struct
import sys
from datetime import datetime, UTC

MAGIC = 0xACCA
HDR = "<HHIQQII"
HDR_LEN = struct.calcsize(HDR)


def decode(buf):
    if len(buf) < HDR_LEN:
        return None
    magic, vtype, ln, seed, off, tsec, tusec = struct.unpack_from(HDR, buf, 0)
    return {"magic": magic, "verify_type": vtype, "len": ln, "rand_seed": seed,
            "offset": off, "time_sec": tsec, "time_usec": tusec}


def describe(h, asked):
    if h is None:
        return "short read"
    if h["magic"] != MAGIC:
        return f"NO HEADER (magic=0x{h['magic']:04x})"
    when = "-"
    if 0 < h["time_sec"] < 2**31:
        when = datetime.fromtimestamp(h["time_sec"], tz=UTC).strftime("%Y-%m-%d %H:%M:%S")
        when += f".{h['time_usec']:06d}Z"
    tag = "ok" if h["offset"] == asked else f"MISMATCH (header says {h['offset']})"
    return f"written {when}  hdr_offset={h['offset']} {tag}"


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("file")
    ap.add_argument("offset", type=int)
    ap.add_argument("--window", type=int, default=16,
                    help="blocks either side to decode (default 16)")
    ap.add_argument("--bs", type=int, default=4096, help="block size (default 4096)")
    args = ap.parse_args()

    start = args.offset - args.window * args.bs
    if start < 0:
        start = 0
    seen = []
    with open(args.file, "rb") as fh:
        for i in range(2 * args.window + 1):
            off = start + i * args.bs
            fh.seek(off)
            buf = fh.read(args.bs)
            if not buf:
                break
            h = decode(buf)
            mark = " <== TARGET" if off == args.offset else ""
            print(f"{off:>14}  {describe(h, off)}{mark}")
            if h and h["magic"] == MAGIC and 0 < h["time_sec"] < 2**31:
                seen.append(h["time_sec"])

    if seen:
        lo = datetime.fromtimestamp(min(seen), tz=UTC).strftime("%H:%M:%SZ")
        hi = datetime.fromtimestamp(max(seen), tz=UTC).strftime("%H:%M:%SZ")
        print(f"\nneighbourhood last written between {lo} and {hi} "
              f"({len(seen)} blocks with valid headers)")
    else:
        print("\nno valid fio headers in the window -- region never written, "
              "or the whole neighbourhood is wrong")
    return 0


if __name__ == "__main__":
    sys.exit(main())
