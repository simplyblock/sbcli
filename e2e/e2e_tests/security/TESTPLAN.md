# Security Feature Test Plan

**Feature**: NVMe-oF Volume Security — DH-HMAC-CHAP, Allowed Hosts, Dynamic Host Management
**Scope**: E2E + Stress tests (TLS excluded — deferred to a separate plan)

---

## Terminology

| Term | Meaning |
|---|---|
| `sec-options` | JSON file passed to `volume add --sec-options`; sets `dhchap_key` / `dhchap_ctrlr_key` |
| `allowed-hosts` | JSON array of host NQNs passed to `volume add --allowed-hosts` |
| `dhchap_key` | Host authenticates **to** the controller (host → ctrl direction) |
| `dhchap_ctrlr_key` | Controller authenticates **to** the host (ctrl → host direction) |
| Bidirectional | Both `dhchap_key: true` and `dhchap_ctrlr_key: true` |
| NQN whitelist-only | `--allowed-hosts` present but no `--sec-options` (no DHCHAP) |
| `host-nqn` | Client-side NQN passed to `volume connect --host-nqn`; required to get embedded DHCHAP keys |
| Crypto lvol | AES-256-XTS encrypted lvol (`--encrypt`) |

---

## Security Combination Matrix

|   | No Auth | Host-only DHCHAP | Ctrl-only DHCHAP | Bidirectional DHCHAP | NQN whitelist-only |
|---|---|---|---|---|---|
| **No Crypto** | plain | auth_host | auth_ctrl | auth | allowed_nqn |
| **Crypto** | crypto | – | – | crypto_auth | crypto_allowed |
| **Crypto + Allowed hosts** | – | – | – | crypto_auth_allowed | – |

---

## Test Cases

### TC-SEC-001–009 · DH-HMAC-CHAP — Basic

| ID | Title | Steps | Expected Result | Automated | Class |
|---|---|---|---|---|---|
| TC-SEC-001 | Create lvol with host-only DHCHAP | `volume add --sec-options {"dhchap_key":true, "dhchap_ctrlr_key":false}` | lvol created; `volume get` shows key config | ✅ | `TestLvolDhcapDirections` |
| TC-SEC-002 | Create lvol with ctrl-only DHCHAP | `volume add --sec-options {"dhchap_key":false, "dhchap_ctrlr_key":true}` | lvol created; `volume get` shows ctrl key config | ✅ | `TestLvolDhcapDirections` |
| TC-SEC-003 | Create lvol with bidirectional DHCHAP | `volume add --sec-options {"dhchap_key":true, "dhchap_ctrlr_key":true}` | lvol created with both keys | ✅ | `TestLvolDhcapDirections` |
| TC-SEC-004 | Connect auth lvol with `--host-nqn` | `volume connect <id> --host-nqn <nqn>` | Connect string returned; `--dhchap-secret` and/or `--dhchap-ctrl-secret` flags present | ✅ | `TestLvolAllowedHostsPositive` |
| TC-SEC-005 | Connect auth lvol **without** `--host-nqn` | `volume connect <id>` (no host-nqn) | Connect string returned **without** embedded DHCHAP keys | ✅ | `TestLvolAllowedHostsPositive`, `TestLvolSecurityNegativeConnect` |
| TC-SEC-006 | `volume get-secret` returns credentials | `volume get-secret <id> <registered-nqn>` | Non-empty credential output | ✅ | `TestLvolAllowedHostsPositive`, `TestLvolCryptoWithAllowedHosts` |
| TC-SEC-007 | `volume get` shows security config | `volume get <id>` after creating auth lvol | Output includes allowed_hosts / key fields | ⚠️ Manual | — |
| TC-SEC-008 | `get-secret` for unregistered NQN → error | `volume get-secret <id> <unregistered-nqn>` | Error or empty result | ✅ | `TestLvolSecurityNegativeHostOps` |
| TC-SEC-009 | DHCHAP auth lvol + wrong NQN at kernel level | Get connect string with wrong NQN; attempt `nvme connect` | DHCHAP negotiation fails; block device does NOT appear | ⚠️ Partial (logged) | `TestLvolSecurityNegativeConnect` |

---

### TC-SEC-010–019 · Allowed Hosts

| ID | Title | Steps | Expected Result | Automated | Class |
|---|---|---|---|---|---|
| TC-SEC-010 | Create lvol with single NQN in `--allowed-hosts` | `volume add ... --sec-options ... --allowed-hosts [nqn]` | lvol created; host NQN visible in `volume get` | ✅ | `TestLvolAllowedHostsPositive` |
| TC-SEC-011 | Connect with registered NQN → success | `volume connect <id> --host-nqn <registered-nqn>` | Connect string with DHCHAP keys; `nvme connect` succeeds; FIO runs | ✅ | `TestLvolAllowedHostsPositive` |
| TC-SEC-012 | Connect with **unregistered** NQN → rejected | `volume connect <id> --host-nqn <wrong-nqn>` | Error returned or empty connect string | ✅ | `TestLvolAllowedHostsNegative` |
| TC-SEC-013 | No `--host-nqn` when allowed-hosts set | `volume connect <id>` without host-nqn | Connect string returned but **without** DHCHAP keys | ✅ | `TestLvolSecurityNegativeConnect` |
| TC-SEC-014 | Lvol with multiple NQNs in `--allowed-hosts` | Pass list with 2 NQNs; inspect `volume get` | Both NQNs visible in lvol detail output | ✅ | `TestLvolMultipleAllowedHosts` |
| TC-SEC-015 | `volume get` shows all allowed hosts | `volume get <id>` after creation with N hosts | All N NQNs present in output | ✅ | `TestLvolMultipleAllowedHosts` |
| TC-SEC-016 | Empty `--allowed-hosts` list `[]` → handled | `volume add ... --allowed-hosts []` | Error or graceful handling (no crash, clear message) | ✅ | `TestLvolSecurityNegativeCreation` |
| TC-SEC-017 | Malformed JSON in `--allowed-hosts` (object not array) | `volume add ... --allowed-hosts /tmp/bad.json` | CLI error; lvol NOT created | ✅ | `TestLvolSecurityNegativeCreation` |
| TC-SEC-018 | NQN whitelist only (no DHCHAP) | `--allowed-hosts [nqn]` without `--sec-options` | Connect string for allowed NQN works; **no DHCHAP keys** in string; unregistered NQN rejected | ✅ | `TestLvolAllowedHostsNoDhchap` |
| TC-SEC-019 | FIO on allowed-hosts+DHCHAP lvol | Connect with correct NQN, mount, run FIO | Data written and validated without errors | ✅ | `TestLvolAllowedHostsPositive` |

---

### TC-SEC-020–029 · Dynamic Host Management

| ID | Title | Steps | Expected Result | Automated | Class |
|---|---|---|---|---|---|
| TC-SEC-020 | `add-host` with sec-options to plain lvol | `volume add-host <id> <nqn> --sec-options ...` | Host NQN appears in `volume get`; connect string with keys succeeds | ✅ | `TestLvolDynamicHostManagement` |
| TC-SEC-021 | `add-host` to existing auth lvol | Create auth lvol; add second host NQN | Second NQN connects with DHCHAP keys | ✅ | `TestLvolMultipleAllowedHosts` |
| TC-SEC-022 | `volume get` updated after `add-host` | `volume get <id>` after `add-host` | New NQN present in detail output | ✅ | `TestLvolDynamicHostManagement` |
| TC-SEC-023 | FIO works after `add-host` with correct NQN | Connect using newly added NQN; mount; FIO | FIO completes without errors | ✅ | `TestLvolDynamicHostManagement` |
| TC-SEC-024 | `remove-host` → access revoked | `volume remove-host <id> <nqn>` then `volume connect ... --host-nqn` | Connect string rejected for removed NQN | ✅ | `TestLvolDynamicHostManagement` |
| TC-SEC-025 | Remaining host unaffected after `remove-host` | Remove one of two NQNs; verify other still connects | Other NQN produces valid connect string; `volume get` shows it | ✅ | `TestLvolMultipleAllowedHosts` |
| TC-SEC-026 | `remove-host` for NQN not in list → error | `volume remove-host <id> <never-added-nqn>` | CLI error / non-zero exit | ✅ | `TestLvolSecurityNegativeHostOps` |
| TC-SEC-027 | `add-host` duplicate NQN → handled gracefully | `add-host` same NQN twice | No crash; NQN not duplicated in details | ✅ | `TestLvolSecurityNegativeHostOps` |
| TC-SEC-028 | `get-secret` after `remove-host` → error | Remove host; then `volume get-secret <id> <removed-nqn>` | Error or empty result | ✅ | `TestLvolSecurityNegativeHostOps` |
| TC-SEC-029 | Remove host then re-add same NQN | `remove-host` then `add-host` same NQN | Host re-appears in details; connect string valid again | ✅ | `TestLvolSecurityNegativeHostOps` |

---

### TC-SEC-030–037 · Security Combination Matrix

| ID | Title | Combination | FIO Validated | Automated | Class |
|---|---|---|---|---|---|
| TC-SEC-030 | Plain lvol | no crypto, no auth | ✅ | ✅ | `TestLvolSecurityCombinations` |
| TC-SEC-031 | Crypto-only lvol | AES crypto, no auth | ✅ | ✅ | `TestLvolSecurityCombinations` |
| TC-SEC-032 | Auth-only lvol | no crypto, bidirectional DHCHAP | ✅ | ✅ | `TestLvolSecurityCombinations` |
| TC-SEC-033 | Crypto + bidirectional DHCHAP | AES + DHCHAP | ✅ | ✅ | `TestLvolSecurityCombinations` |
| TC-SEC-034 | NQN whitelist only | no crypto, no DHCHAP, NQN restriction | ✅ | ✅ | `TestLvolAllowedHostsNoDhchap` |
| TC-SEC-035 | Auth + allowed-hosts | no crypto, DHCHAP + NQN whitelist | ✅ | ✅ | `TestLvolAllowedHostsPositive` |
| TC-SEC-036 | Crypto + auth + allowed-hosts | full security stack | ✅ | ✅ | `TestLvolCryptoWithAllowedHosts` |
| TC-SEC-037 | Isolation: mixed types side-by-side | 4 types concurrently, FIO on each | ✅ | ✅ | `TestLvolSecurityCombinations` |

---

### TC-SEC-040–044 · Data Integrity & FIO

| ID | Title | Steps | Expected Result | Automated | Class |
|---|---|---|---|---|---|
| TC-SEC-040 | FIO randrw on all 4 core security types | Mount each, run parallel FIO, validate logs | All FIO logs report 0 errors | ✅ | `TestLvolSecurityCombinations` |
| TC-SEC-041 | MD5 checksum preserved across disconnect/reconnect on auth lvol | Write data, unmount, disconnect, reconnect, remount, verify MD5 | Checksums match | ⚠️ Manual | — |
| TC-SEC-042 | Concurrent FIO on 4 security types simultaneously | Start all FIO threads at once | No interference; all logs valid | ✅ | `TestLvolSecurityCombinations` |
| TC-SEC-043 | FIO throughput — auth vs plain | Compare FIO bandwidth on auth and plain lvol of same size | Auth overhead within acceptable range (informational; no hard limit enforced) | ⚠️ Manual | — |
| TC-SEC-044 | FIO on crypto+allowed-hosts lvol | Full security stack, 2-min randrw FIO | Validates E2E data path with full security | ✅ | `TestLvolCryptoWithAllowedHosts` |

---

### TC-SEC-050–057 · Negative / Edge Cases — Inputs

| ID | Title | Steps | Expected Result | Automated | Class |
|---|---|---|---|---|---|
| TC-SEC-050 | `--sec-options` file path does not exist | `volume add ... --sec-options /tmp/no_such_file.json` | CLI error; lvol NOT created | ✅ | `TestLvolSecurityNegativeCreation` |
| TC-SEC-051 | `--allowed-hosts` JSON is object not array | Pass `{"nqn":"..."}` instead of `["nqn.x"]` | CLI error; lvol NOT created | ✅ | `TestLvolSecurityNegativeCreation` |
| TC-SEC-052 | `--sec-options` with both keys false | `{"dhchap_key":false,"dhchap_ctrlr_key":false}` | Lvol created with no auth (equivalent to omitting sec-options); no error | ⚠️ Manual | — |
| TC-SEC-053 | `--allowed-hosts` with empty array `[]` | `volume add ... --allowed-hosts []` | Error or graceful handling; no crash | ✅ | `TestLvolSecurityNegativeCreation` |
| TC-SEC-054 | Tampered DHCHAP key in connect string | Modify `--dhchap-secret` value before `nvme connect` | DHCHAP negotiation fails at kernel; block device does NOT appear | ✅ (logged) | `TestLvolSecurityNegativeConnect` |
| TC-SEC-055 | `add-host` with invalid NQN format | `volume add-host <id> "not-a-valid-nqn!@#"` | CLI error returned | ✅ | `TestLvolSecurityNegativeCreation` |
| TC-SEC-056 | Delete lvol that has active `allowed-hosts` | Create auth+allowed lvol; delete it | Delete succeeds cleanly; lvol no longer appears in `lvol list` | ✅ | `TestLvolSecurityNegativeConnect` |
| TC-SEC-057 | `add-host` with only one DHCHAP direction | `--sec-options {"dhchap_key":true,"dhchap_ctrlr_key":false}` on add-host | Only host-direction key added; ctrl direction unauthenticated | ⚠️ Manual | — |

---

### TC-SEC-060–068 · Stress & Failover

| ID | Title | Outage Type | Security Types Covered | Automated | Class |
|---|---|---|---|---|---|
| TC-SEC-060 | Continuous failover — graceful shutdown | Graceful shutdown + restart | plain, crypto, auth, crypto_auth | ✅ | `RandomSecurityFailoverTest` |
| TC-SEC-061 | Continuous failover — container stop (crash) | SPDK process kill | plain, crypto, auth, crypto_auth | ✅ | `RandomSecurityFailoverTest` |
| TC-SEC-062 | Continuous failover — full network interrupt | `nmcli dev disconnect` | plain, crypto, auth, crypto_auth | ✅ | `RandomSecurityFailoverTest` |
| TC-SEC-063 | Continuous failover — partial network outage | Port-level block/unblock | plain, crypto, auth, crypto_auth | ✅ | `RandomSecurityFailoverTest` |
| TC-SEC-064 | Continuous failover — all 6 security types | All 4 outage types | plain, crypto, auth, crypto_auth, auth_allowed, crypto_auth_allowed | ✅ | `RandomAllSecurityFailoverTest` |
| TC-SEC-065 | Auth lvol NVMe auto-reconnects after outage | Graceful shutdown | auth, crypto_auth | ⚠️ Manual (verify kernel reconnect logs) | — |
| TC-SEC-066 | Wrong-NQN rejection maintained after failover | Any outage type | auth_allowed | ✅ (logged per iteration) | `RandomAllSecurityFailoverTest` |
| TC-SEC-067 | `add-host` during active FIO + outage | Container stop mid-FIO | auth | ⚠️ Manual | — |
| TC-SEC-068 | `remove-host` during active FIO | Graceful shutdown mid-FIO | auth_allowed | ⚠️ Manual | — |

---

## Coverage Summary

| Category | Total TCs | Fully Automated | Partial / Logged | Manual |
|---|---|---|---|---|
| DH-HMAC-CHAP basic | 9 | 7 | 1 | 1 |
| Allowed hosts | 10 | 10 | 0 | 0 |
| Dynamic host management | 10 | 10 | 0 | 0 |
| Security combination matrix | 8 | 8 | 0 | 0 |
| Data integrity & FIO | 5 | 3 | 0 | 2 |
| Negative / edge cases | 8 | 6 | 1 | 1 |
| Stress & failover | 9 | 5 | 1 | 3 |
| **Total** | **59** | **49** | **3** | **7** |

---

## Automated Test Class Reference

| Class | File | TCs Covered |
|---|---|---|
| `TestLvolSecurityCombinations` | `test_lvol_security.py` | TC-030–033, TC-037, TC-040, TC-042 |
| `TestLvolAllowedHostsPositive` | `test_lvol_security.py` | TC-004–006, TC-010–011, TC-019, TC-035 |
| `TestLvolAllowedHostsNegative` | `test_lvol_security.py` | TC-012 |
| `TestLvolAllowedHostsNoDhchap` | `test_lvol_security.py` | TC-018, TC-034 |
| `TestLvolDynamicHostManagement` | `test_lvol_security.py` | TC-020, TC-022–024 |
| `TestLvolCryptoWithAllowedHosts` | `test_lvol_security.py` | TC-006, TC-036, TC-044 |
| `TestLvolDhcapDirections` | `test_lvol_security.py` | TC-001–003 |
| `TestLvolMultipleAllowedHosts` | `test_lvol_security.py` | TC-014–015, TC-021, TC-025 |
| `TestLvolSecurityNegativeHostOps` | `test_lvol_security.py` | TC-008, TC-026–029 |
| `TestLvolSecurityNegativeCreation` | `test_lvol_security.py` | TC-016–017, TC-050–051, TC-053, TC-055 |
| `TestLvolSecurityNegativeConnect` | `test_lvol_security.py` | TC-005, TC-009, TC-013, TC-054, TC-056 |
| `RandomSecurityFailoverTest` | `continuous_failover_ha_security.py` | TC-060–063 |
| `RandomAllSecurityFailoverTest` | `continuous_failover_ha_security.py` | TC-064, TC-066 |

---

## Manual Test Execution Notes

**TC-SEC-007** — `volume get` output inspection
Run `sbcli-dev volume get <id>` after creating an auth lvol and confirm `allowed_hosts`, `dhchap_key`, and/or `dhchap_ctrlr_key` fields appear in the output.

**TC-SEC-009 / TC-SEC-054** — Kernel-level DHCHAP rejection
These are marked *partial* because `exec_command` does not reliably capture the `nvme connect` exit code. Verify by checking that no new block device appears in `lsblk` after the connect attempt, and by inspecting `dmesg` for `nvme: authentication failed` entries.

**TC-SEC-041** — MD5 across disconnect/reconnect
Script outline: write data → `md5sum` → unmount → `nvme disconnect` → `nvme connect --host-nqn` → remount → `md5sum` → compare.

**TC-SEC-043** — Throughput comparison
Run the same FIO job (sequential write, 128K bs, 60 s) on a plain lvol and an auth lvol of equal size on the same storage node. Log bandwidth. Auth overhead is expected to be < 5% for typical DHCHAP digest algorithms.

**TC-SEC-065** — NVMe auto-reconnect after outage
After a storage-node crash, watch `dmesg -w` on the client for `nvme: controller reconnected` messages on auth controllers. Confirm the same device path is reused and FIO continues without errors.

**TC-SEC-067 / TC-SEC-068** — Dynamic host ops during I/O
These require manual coordination: start FIO on one terminal, trigger `add-host`/`remove-host` from another, observe no FIO error on the active host and that the removed host's next connect attempt fails.

---

## Not In Scope (TLS)

TLS (`--tls` cluster-create parameter) is deferred to a separate test plan cycle. All TLS-related test cases will be added once the feature is available in the test environment.
