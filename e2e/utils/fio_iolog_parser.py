import os
import sys
import argparse
import logging
from typing import List, Tuple, Optional, Dict

LOG_DIR = "/logs"
MATCH_LOG_FILE = os.path.join(LOG_DIR, "fio_md5_offset_trace.log")
FULL_LOG_FILE = os.path.join(LOG_DIR, "fio_iolog_debug.log")

# Setup logging
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(FULL_LOG_FILE, mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)

def parse_iolog_file(file_path: str) -> List[Tuple[float, int, int, str]]:
    """
    Parses a FIO iolog file into a list of (timestamp, offset, length, operation).

    Returns:
        List[Tuple]: Parsed entries
    """
    entries = []
    if not os.path.exists(file_path):
        logging.warning(f"File not found: {file_path}")
        return entries

    with open(file_path, "r") as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) != 4:
                continue
            try:
                ts = float(parts[0])
                offset = int(parts[1])
                length = int(parts[2])
                op = parts[3].upper()
                entries.append((ts, offset, length, op))
            except ValueError:
                continue

    logging.info(f"Parsed {len(entries)} entries from {file_path}")
    return entries

def merge_iolog_files(file_list: List[str]) -> List[Tuple[float, int, int, str]]:
    all_entries = []
    for file_path in file_list:
        all_entries.extend(parse_iolog_file(file_path))
    merged = sorted(all_entries, key=lambda x: x[0])
    logging.info(f"Merged total {len(merged)} IO entries from {len(file_list)} files")
    return merged

def find_matches(
    entries: List[Tuple[float, int, int, str]],
    target_offsets: List[int],
    threshold: int = 4096
) -> Dict[int, Dict[str, Optional[Tuple[float, int, int, str]]]]:
    results = {}
    for target in target_offsets:
        match = next((e for e in entries if e[1] == target), None)
        if match:
            results[target] = {"type": "exact", "entry": match}
        else:
            closest = min(
                (e for e in entries if abs(e[1] - target) <= threshold),
                key=lambda e: abs(e[1] - target),
                default=None
            )
            if closest:
                results[target] = {"type": "closest", "entry": closest}
            else:
                results[target] = {"type": "not_found", "entry": None}
    return results

def save_results_to_logfile(results: Dict[int, Dict[str, Optional[Tuple[float, int, int, str]]]]):
    with open(MATCH_LOG_FILE, "w") as f:
        for offset, result in results.items():
            match_type = result["type"]
            entry = result["entry"]
            if match_type == "exact":
                msg = f"[EXACT]   Offset={offset} at {entry[0]:.3f}s | Length={entry[2]} | Op={entry[3]}"
            elif match_type == "closest":
                diff = abs(entry[1] - offset)
                msg = f"[CLOSEST] Offset={offset} → Closest Offset={entry[1]} at {entry[0]:.3f}s | Δ={diff} bytes | Op={entry[3]}"
            else:
                msg = f"[MISS]    Offset={offset} → No match within threshold"

            f.write(msg + "\n")
            logging.info(msg)

    logging.info(f"Final offset match results written to {MATCH_LOG_FILE}")

def main():
    parser = argparse.ArgumentParser(description="Parse and match FIO iolog entries for MD5 error analysis.")
    parser.add_argument("iolog_files", nargs="+", help="List of iolog files (fio_iolog.0, .1, etc.)")
    parser.add_argument("--offsets", nargs="+", type=int, required=True, help="Offsets to match from MD5 failures")
    parser.add_argument("--threshold", type=int, default=4096, help="Max bytes deviation for 'closest' match")
    parser.add_argument("--merge", action="store_true", help="Merge iologs before checking offsets")

    args = parser.parse_args()

    if args.merge:
        logging.info("Merging all iolog files for unified trace analysis...")
        entries = merge_iolog_files(args.iolog_files)
    else:
        entries = []
        for f in args.iolog_files:
            entries.extend(parse_iolog_file(f))
        entries = sorted(entries, key=lambda x: x[0])
        logging.info(f"Collected {len(entries)} total IO entries (no merge)")

    if not entries:
        logging.error("No valid IO entries found.")
        sys.exit(1)

    results = find_matches(entries, args.offsets, threshold=args.threshold)
    save_results_to_logfile(results)

if __name__ == "__main__":
    main()
