"""The declared-secondary-index subsystem: ``models/indices`` and ``index_ops``.

Everything here is a pure function of a record or of a set of findings — no
database. The transactional maintenance, the backfill and the read path are
exercised against a real FoundationDB in ``tests/integration/models/indices/``.
"""
