"""Operator-facing names for the integer topology ids.

`StorageNode.failure_domain` and `StorageNode.physical_label` are integers and
stay integers: placement, the distrib cluster map and the expansion planner all
key off them. Operators, however, think in `RACK1` / `AZ2` / `HOST1`. This module
is the translation layer — resolution at ingress, rendering at egress — and the
allocation rule that gives a new label a new id.

The registries live on the Cluster record (`failure_domain_labels`,
`physical_labels`, both label -> id) and are written only through
`DBController.claim_topology_label`, which allocates inside an FDB transaction.
"""
import re
from typing import Dict, Optional, Tuple

# Upper-case, starts with a letter, then letters/digits/_/- up to 32 chars.
# Case is not meaningful: rack1, Rack1 and RACK1 are one label.
LABEL_RE = re.compile(r"^[A-Z][A-Z0-9_-]{0,31}$")

FAILURE_DOMAIN = "failure_domain"
PHYSICAL = "physical"

# Registry field on the Cluster model per kind.
REGISTRY_FIELD = {
    FAILURE_DOMAIN: "failure_domain_labels",
    PHYSICAL: "physical_labels",
}

# Prefix used when `cluster update` backfills names for ids that predate labels.
BACKFILL_PREFIX = {
    FAILURE_DOMAIN: "FD",
    PHYSICAL: "HOST",
}


class InvalidLabelError(ValueError):
    """The operator's label is not a syntactically valid topology label."""


def normalize_label(raw) -> str:
    """Upper-case and validate. Raises InvalidLabelError on anything else."""
    if raw is None:
        raise InvalidLabelError("label is empty")
    label = str(raw).strip().upper()
    if not LABEL_RE.match(label):
        raise InvalidLabelError(
            f"invalid topology label '{raw}': expected a name like RACK1, AZ2 or "
            f"HOST1 — a letter followed by up to 31 letters, digits, '_' or '-'")
    return label


def parse_failure_domain_arg(raw) -> Tuple[Optional[int], Optional[str]]:
    """Split the operator's ``--failure-domain`` value into (id, label).

    An all-digits value keeps its legacy meaning — *that* integer id, not a
    label named "7" — so scripts, CI bootstraps and the k8s operator written
    against the integer API keep working. Anything else is a label to resolve
    or allocate. Exactly one element of the pair is non-None.
    """
    if raw is None:
        return None, None
    text = str(raw).strip()
    if text == "":
        return None, None
    if text.lstrip("+-").isdigit():
        return int(text), None
    return None, normalize_label(text)


def label_for_id(registry: Optional[Dict[str, int]], id_: int) -> Optional[str]:
    """Reverse lookup: the label registered for ``id_``, or None."""
    if not registry or id_ is None or id_ < 0:
        return None
    for label, mapped in registry.items():
        if mapped == id_:
            return label
    return None


def render(registry: Optional[Dict[str, int]], id_: int) -> str:
    """What the operator should see for ``id_``.

    Falls back to the integer: a cluster that has not been backfilled yet, or an
    id created by a legacy integer call, still has to display as something.
    """
    if id_ is None or id_ < 0:
        return ""
    return label_for_id(registry, id_) or str(id_)


def next_free_id(used) -> int:
    """The id a new label gets: one past the highest in use.

    Not the lowest free id — reusing an id an operator has just retired would
    silently move nodes into a domain that used to mean something else. ``used``
    must span both the registry and the ids already on node records, so an id
    chosen explicitly by a legacy integer call is never handed out again.
    """
    highest = -1
    for value in used:
        if value is not None and value > highest:
            highest = int(value)
    return highest + 1


def backfill_label(kind: str, id_: int) -> str:
    """The derived name `cluster update` gives an id that predates labels."""
    return f"{BACKFILL_PREFIX[kind]}{id_}"
