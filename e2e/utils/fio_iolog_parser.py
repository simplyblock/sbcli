# import os
# import sys
# import argparse
# import logging
# from typing import List, Tuple, Optional, Dict
# from collections import defaultdict
# from pathlib import Path
# import re

# LOG_DIR = "iolog-debug"
# FULL_LOG_FILE = os.path.join(LOG_DIR, "fio_iolog_debug.log")
# os.makedirs(LOG_DIR, exist_ok=True)

# logging.basicConfig(
#     level=logging.DEBUG,
#     format="%(asctime)s [%(levelname)s] %(message)s",
#     handlers=[
#         logging.FileHandler(FULL_LOG_FILE, mode='w'),
#         logging.StreamHandler(sys.stdout)
#     ]
# )

# def extract_offsets_from_log(log_file: str) -> List[int]:
#     offsets = set()
#     pattern = re.compile(r"offset (\d+), length")
#     try:
#         with open(log_file, "r") as f:
#             for line in f:
#                 match = pattern.search(line)
#                 if match:
#                     offsets.add(int(match.group(1)))
#     except Exception as e:
#         logging.warning(f"Failed to read {log_file}: {e}")
#     logging.debug(f"Extracted offsets from {log_file}: {offsets}")
#     return sorted(list(offsets))

# def parse_iolog_file(file_path: str) -> List[Tuple[float, int, int, str]]:
#     entries = []
#     if not os.path.exists(file_path):
#         logging.warning(f"File not found: {file_path}")
#         return entries
#     with open(file_path, "r") as f:
#         for line in f:
#             parts = line.strip().split()
#             if len(parts) == 4:
#                 try:
#                     ts = float(parts[0])
#                     offset = int(parts[1])
#                     length = int(parts[2])
#                     op = parts[3].upper()
#                     entries.append((ts, offset, length, op))
#                 except ValueError:
#                     continue
#             elif len(parts) == 5:
#                 try:
#                     ts = float(parts[0])
#                     op = parts[2].upper()
#                     offset = int(parts[3])
#                     length = int(parts[4])
#                     entries.append((ts, offset, length, op))
#                 except ValueError:
#                     continue
#             else:
#                 logging.debug(f"Skipped malformed line in {file_path}: {line.strip()}")
#     logging.debug(f"Parsed {len(entries)} entries from {file_path}")
#     return entries

# def merge_iolog_files(file_list: List[str]) -> List[Tuple[float, int, int, str]]:
#     all_entries = []
#     for file_path in file_list:
#         all_entries.extend(parse_iolog_file(file_path))
#     logging.debug(f"Merged {len(all_entries)} total entries from {len(file_list)} files")
#     return sorted(all_entries, key=lambda x: x[0])

# def find_matches(entries: List[Tuple[float, int, int, str]], target_offsets: List[int], threshold: int = 4096) -> Dict[int, Dict[str, List[Tuple[float, int, int, str]]]]:
#     results = {}
#     for target in target_offsets:
#         exact_matches = [e for e in entries if e[1] == target]
#         if exact_matches:
#             results[target] = {"type": "exact", "entries": exact_matches}
#         else:
#             closest = min(
#                 (e for e in entries if abs(e[1] - target) <= threshold),
#                 key=lambda e: abs(e[1] - target),
#                 default=None
#             )
#             if closest:
#                 results[target] = {"type": "closest", "entries": [closest]}
#             else:
#                 results[target] = {"type": "not_found", "entries": []}
#     logging.debug(f"Match results: {results}")
#     return results

# def format_timestamp(ts: float) -> str:
#     return f"{ts / 1_000_000:.3f}s" 

# def save_results_to_logfile(entity: str, results: Dict[int, Dict[str, List[Tuple[float, int, int, str]]]]):
#     log_file = os.path.join(LOG_DIR, f"fio_md5_offset_trace_{entity}.log")
#     with open(log_file, "w") as f:
#         for offset, result in results.items():
#             match_type = result["type"]
#             entries = result["entries"]
#             if match_type == "exact":
#                 for entry in entries:
#                     ts_fmt = format_timestamp(entry[0])
#                     msg = f"[EXACT]   Offset={offset} at {ts_fmt} | Length={entry[2]} | Op={entry[3]}"
#                     f.write(msg + "\n")
#                     logging.info(f"[{entity}] {msg}")
#             elif match_type == "closest":
#                 entry = entries[0]
#                 ts_fmt = format_timestamp(entry[0])
#                 diff = abs(entry[1] - offset)
#                 msg = f"[CLOSEST] Offset={offset} → Closest Offset={entry[1]} at {ts_fmt} | Δ={diff} bytes | Op={entry[3]}"
#                 f.write(msg + "\n")
#                 logging.info(f"[{entity}] {msg}")
#             else:
#                 msg = f"[MISS]    Offset={offset} → No match within threshold"
#                 f.write(msg + "\n")
#                 logging.info(f"[{entity}] {msg}")

# def collect_fio_entities(iolog_dir: str) -> Dict[str, Dict[str, List[str]]]:
#     grouped = {}
#     for file in os.listdir(iolog_dir):
#         full_path = os.path.join(iolog_dir, file)
#         print(full_path)
#         if file.endswith(".log") and ("cl" in file or "lv" in file):
#             print(f"File is fio log: {file}")
#             entity = file.split("_fio")[0].replace("local-", "")
#             entity = entity.split(".")[0].split("-")[0]
#             if entity in grouped:
#                 grouped[entity]["log"] = full_path
#             else:
#                 grouped[entity] = {"iologs": [], "log": full_path}
#         elif "fio_iolog" in file:
#             print(f"File is fio iolog:{file}")
#             entity = file.split("_fio_iolog")[0].replace("local-", "")
#             if entity in grouped:
#                 grouped[entity]["iologs"].append(full_path)
#             else:
#                 grouped[entity] = {"iologs": [full_path], "log": None}
#         else:
#             print(f"File is state or others:{file}")
#     logging.debug(f"Grouped entities: {grouped}")
#     return grouped

# def main():
#     parser = argparse.ArgumentParser(description="Auto-analyze FIO iologs per entity.")
#     parser.add_argument("--iolog_dir", required=True, help="Directory containing all fio logs and iolog files")
#     parser.add_argument("--threshold", type=int, default=4096, help="Max offset deviation for 'closest' match")
#     args = parser.parse_args()

#     if not os.path.isdir(args.iolog_dir):
#         logging.error(f"Invalid directory: {args.iolog_dir}")
#         sys.exit(1)

#     entity_map = collect_fio_entities(args.iolog_dir)

#     for entity, files in entity_map.items():
#         log_file = files["log"]
#         iologs = files["iologs"]

#         logging.info(f"Processing entity: {entity}")
#         logging.debug(f"Log file: {log_file}")
#         logging.debug(f"Iolog files: {iologs}")

#         if not log_file or not iologs:
#             logging.warning(f"Skipping {entity}: missing .log or .iolog.* files")
#             continue

#         offsets = extract_offsets_from_log(log_file)
#         if not offsets:
#             logging.warning(f"No MD5 offset failures found for {entity}")
#             continue

#         entries = merge_iolog_files(iologs)
#         matches = find_matches(entries, offsets, threshold=args.threshold)
#         save_results_to_logfile(entity, matches)

# if __name__ == "__main__":
#     main()


import os
import sys
import argparse
import logging
from typing import List, Tuple, Optional, Dict
from collections import defaultdict
from pathlib import Path

LOG_DIR = "iolog-debug"
FULL_LOG_FILE = os.path.join(LOG_DIR, "fio_iolog_debug.log")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(FULL_LOG_FILE, mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)

def extract_offsets_from_log(log_file: str) -> List[int]:
    offsets = set()
    import re
    pattern = re.compile(r"offset (\d+), length")
    try:
        with open(log_file, "r") as f:
            for line in f:
                match = pattern.search(line)
                if match:
                    offsets.add(int(match.group(1)))
    except Exception as e:
        logging.warning(f"Failed to read {log_file}: {e}")
    logging.debug(f"Extracted offsets from {log_file}: {offsets}")
    return sorted(list(offsets))

def parse_iolog_file(file_path: str) -> List[Tuple[float, int, int, str, int]]:
    entries = []
    if not os.path.exists(file_path):
        logging.warning(f"File not found: {file_path}")
        return entries
    with open(file_path, "r") as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) != 5:
                continue
            try:
                ts = float(parts[0])
                fio_path = parts[1]
                op = parts[2].upper()
                offset = int(parts[3])
                length = int(parts[4])
                job_id = -1
                try:
                    tokens = fio_path.split(".")
                    job_id = int(tokens[-2]) if len(tokens) > 2 else -1
                except:
                    pass
                entries.append((ts, offset, length, op, job_id))
            except Exception:
                continue
    logging.debug(f"Parsed {len(entries)} entries from {file_path}")
    return entries

def merge_iolog_files(file_list: List[str]) -> List[Tuple[float, int, int, str, int]]:
    all_entries = []
    for file_path in file_list:
        all_entries.extend(parse_iolog_file(file_path))
    logging.debug(f"Merged {len(all_entries)} total entries from {len(file_list)} files")
    return sorted(all_entries, key=lambda x: x[0])

def find_failure_points(entries: List[Tuple[float, int, int, str, int]], target_offsets: List[int], threshold: int = 4096) -> Dict[int, Dict[str, Optional[Tuple[float, int, int, str, int]]]]:
    results = {}
    grouped_by_job: Dict[int, List[Tuple[float, int, int, str, int]]] = defaultdict(list)
    for e in entries:
        grouped_by_job[e[4]].append(e)

    for offset in target_offsets:
        found = False
        for job_id, job_entries in grouped_by_job.items():
            writes = [e for e in job_entries if e[1] == offset and e[3] == 'WRITE']
            reads = [e for e in job_entries if e[1] == offset and e[3] == 'READ']
            if not writes or not reads:
                continue
            last_write_time = max(w[0] for w in writes)
            reads_after_write = [r for r in reads if r[0] > last_write_time]
            if reads_after_write:
                fail_read = reads_after_write[0]
                results[offset] = {"job_id": job_id, "entry": fail_read, "full_trace": job_entries}
                found = True
                break
        if not found:
            results[offset] = {"job_id": None, "entry": None, "full_trace": []}
    return results

def format_timestamp(ts: float) -> str:
    return f"{ts / 1_000_000:.3f}s"

def save_results_to_logfile(entity: str, results: Dict[int, Dict[str, Optional[Tuple[float, int, int, str, int]]]], width: int = 120):
    log_file = os.path.join(LOG_DIR, f"fio_md5_offset_trace_{entity}.log")
    with open(log_file, "w") as f:
        for offset, result in results.items():
            entry = result["entry"]
            job_id = result["job_id"]
            trace = result.get("full_trace", [])

            f.write("\n" + "=" * width + "\n")
            f.write(f"Offset: {offset:<12} | Job: {job_id}\n")
            f.write("=" * width + "\n")

            if not trace:
                f.write("[MISS] No trace found for offset\n\n")
                continue

            for e in trace:
                ts_fmt = format_timestamp(e[0])
                prefix = "[FAIL_POINT]" if entry and e == entry else ""
                f.write(f"{prefix:<12} Time={ts_fmt:<10} | Offset={e[1]:<10} | Op={e[3]:<5} | Job={e[4]}\n")

    logging.info(f"Saved annotated trace logs for {entity}")

def collect_fio_entities(iolog_dir: str) -> Dict[str, Dict[str, List[str]]]:
    grouped = {}
    for file in os.listdir(iolog_dir):
        full_path = os.path.join(iolog_dir, file)
        if file.endswith(".log") and ("cl" in file or "lv" in file):
            entity = file.split("_fio")[0].replace("local-", "").split(".")[0].split("-")[0]
            if entity in grouped:
                grouped[entity]["log"] = full_path
            else:
                grouped[entity] = {"iologs": [], "log": full_path}
        elif "fio_iolog" in file:
            entity = file.split("_fio_iolog")[0].replace("local-", "")
            if entity in grouped:
                grouped[entity]["iologs"].append(full_path)
            else:
                grouped[entity] = {"iologs": [full_path], "log": None}
    return grouped

def main():
    parser = argparse.ArgumentParser(description="Auto-analyze FIO iologs per entity with per-job failure analysis.")
    parser.add_argument("--iolog_dir", required=True, help="Directory containing all fio logs and iolog files")
    parser.add_argument("--threshold", type=int, default=4096, help="Max offset deviation for 'closest' match")
    args = parser.parse_args()

    if not os.path.isdir(args.iolog_dir):
        logging.error(f"Invalid directory: {args.iolog_dir}")
        sys.exit(1)

    entity_map = collect_fio_entities(args.iolog_dir)

    for entity, files in entity_map.items():
        log_file = files["log"]
        iologs = files["iologs"]

        logging.info(f"Processing entity: {entity}")
        logging.debug(f"Log file: {log_file}")
        logging.debug(f"Iolog files: {iologs}")

        if not log_file or not iologs:
            logging.warning(f"Skipping {entity}: missing .log or .iolog.* files")
            continue

        offsets = extract_offsets_from_log(log_file)
        if not offsets:
            logging.warning(f"No MD5 offset failures found for {entity}")
            continue

        entries = merge_iolog_files(iologs)
        matches = find_failure_points(entries, offsets, threshold=args.threshold)
        save_results_to_logfile(entity, matches)

if __name__ == "__main__":
    main()
