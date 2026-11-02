# pNFS RWX integrity tester — Track E

Nothing is built yet. This directory currently holds only the specification the
tester is written from.

| file | what it is |
|---|---|
| `pnfs-rwx-integrity-tester-spec.pdf` | the specification. Source of truth for the block format, the modes, and the violation and counter lists. sha256 `76dab7bf2d53dbe35e9fb9f2d183ce887c7634f3ee4023ca47825949bd139b09` |

The test plan this belongs to is at <https://claude.ai/artifact/TFd4QGqzAJjx5tNgKyP4TD>.
A local copy, together with these notes, is in `E:/sbcli-qa-docs/pnfs/`.

## Before writing any of it

The plan's day-0 gate is **S-1**, about an hour's work, and every date in the
schedule is conditional on it:

> Clone the operator StorageClass with `fstype: pnfs`, create one RWX claim, two
> pods on two workers, write and read, then check `/proc/self/mountstats` for
> `pnfs=`.

No pNFS volume has ever been provisioned on the lab. The
`nfsexports.storage.simplyblock.io` CRD exists (created 2026-10-30) but
`oc get nfsexports -A` returns nothing. The likeliest failure is the typed
`volumeDefaults.filesystem` field rejecting `pnfs`; the fallback is to build the
StorageClass standalone, bypassing the pool path.

We would also be the first RHEL-family client. The design names RHEL 9.8's
`5.14.0-687`; the lab is on 9.6 and `5.14.0-570`, with the symbol and the module
both present, so the backport landed earlier than the design assumed. If layouts
are not granted, Tracks B, C and E all degrade.

## The constraint on everything here

The black-box invariant in `e2e/AGENTS.md` applies: harness code never imports
`simplyblock_core`, `simplyblock_cli` or `simplyblock_web`. The mountstats parser
mirrors the shape of the one in `atlas-lib/nfsclient`, with a comment naming the
upstream definition, and the integrity tester itself is pure standard library.
