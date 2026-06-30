"""Check HA-journal distribution across failure domains.

For each node's JC, the journal members are the node's own JM plus its
remote jm_ids. With N HA journals and >=N failure domains, every member MUST
sit in a distinct failure domain — otherwise losing one whole domain removes
>1 journal at once. Flags any JC whose members collide in a domain, and counts
how many journals the current outage (offline nodes) removed per JC."""
from simplyblock_core.db_controller import DBController

db = DBController()
sns = db.get_storage_nodes()
fd = {n.get_id(): n.failure_domain for n in sns}
status = {n.get_id(): n.status for n in sns}
def short(u):
    return u[:8]

print("node      fd  status")
for n in sns:
    print(f"{short(n.get_id())}  {fd[n.get_id()]}   {status[n.get_id()]}")

offline = {nid for nid, s in status.items() if s != "online"}
print(f"\noffline/non-online nodes: {sorted(short(o) for o in offline)}")
print(f"failure domains: {sorted(set(fd.values()))}\n")

for n in sns:
    members = [n.get_id()] + list(n.jm_ids or [])
    doms = [fd.get(m) for m in members]
    # collisions: same domain used by >1 journal member
    seen = {}
    for m in members:
        seen.setdefault(fd.get(m), []).append(short(m))
    collided = {d: ms for d, ms in seen.items() if len(ms) > 1}
    n_down = sum(1 for m in members if m in offline)
    flag = ""
    if collided:
        flag += f"  <== SAME-DOMAIN COLLISION {collided}"
    if n_down >= 3:
        flag += f"  <== {n_down}/{len(members)} JOURNALS DOWN (quorum lost)"
    elif n_down == 2:
        flag += f"  <== {n_down}/{len(members)} journals down (at limit)"
    print(f"JC of {short(n.get_id())} (fd {fd[n.get_id()]}): "
          f"members={[short(m) for m in members]} domains={doms} down={n_down}{flag}")
