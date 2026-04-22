#!/usr/bin/env python3
"""Merge paginated OpenSearch JSON exports into a single CSV.

Usage: python3 merge_logs.py <node_name>

Expects files at /tmp/<node>_p*.json, outputs to /tmp/graylog_export/<node>_all.csv
"""
import json
import glob
import sys
import os

node = sys.argv[1]
files = sorted(glob.glob(f"/tmp/{node}_p*.json"), key=lambda f: int(f.split("_p")[1].split(".")[0]))
os.makedirs("/tmp/graylog_export", exist_ok=True)
out = open(f"/tmp/graylog_export/{node}_all.csv", "w")
total = 0
for fn in files:
    d = json.load(open(fn))
    for h in reversed(d.get("hits", {}).get("hits", [])):
        s = h["_source"]
        out.write(s.get("timestamp", "") + "|" + s.get("message", "").replace(chr(10), " ") + chr(10))
        total += 1
print(f"{node}: {total} lines")
