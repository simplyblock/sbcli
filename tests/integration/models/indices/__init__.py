"""Declared secondary indices against a real FoundationDB.

The unit tier covers key derivation as a pure function of a record
(``tests/unit/models/indices/``). What needs a real database is everything the
design turns on: that maintenance is in the *same* transaction as the entity
write, that the scan fallback and the index answer identically, that the
backfill is restartable, and that a unique violation leaves nothing behind and
does not get swallowed on the way out.
"""
