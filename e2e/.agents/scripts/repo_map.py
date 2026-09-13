#!/usr/bin/env python3
"""Generate a compact, token-cheap map of a large external repo.

Why this exists
---------------
Several RCAs in sbcli-rca-archive start from a single SPDK log line, for example

    lvol.c:2942:lvs_update_on_failover_cpl: *ERROR*: Forcing application shutdown via abort

and then stall, because the SPDK source is not checked out next to sbcli. The
analysis has to fall back on inference ("-84 is probably EILSEQ, probably the
CRC mismatch") instead of reading the code. Checking the repo out fixes that,
but a tree with hundreds of thousands of files is expensive to explore by
grepping, and most of it is irrelevant.

So: index each repo once into a small Markdown map, and read the map instead.
The map answers the two questions that actually come up:

    "where does this log message come from?"   -> log-string index
    "where is this function defined?"          -> symbol index

Usage
-----
    python3 .agents/scripts/repo_map.py                 # all repos in the config
    python3 .agents/scripts/repo_map.py --only spdk     # one repo
    python3 .agents/scripts/repo_map.py --check         # staleness only, no writes

Config: .agents/repo-map.config.json (see repo-map.config.example.json).
Output: .agents/repo-maps/<name>.md, which is gitignored by default because it
is a derived artefact and its size scales with someone else's repo.
"""

import argparse
import json
import os
import re
import subprocess
import sys
from collections import Counter
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
AGENTS_DIR = os.path.dirname(HERE)
CONFIG = os.path.join(AGENTS_DIR, "repo-map.config.json")
OUT_DIR = os.path.join(AGENTS_DIR, "repo-maps")

# Symbol patterns per language. Deliberately conservative: a missed symbol costs
# one grep, a wrong one costs confusion every time the map is read.
SYMBOL_PATTERNS = {
    "c": [
        # SPDK, like most kernel-style C, puts the return type on its own line:
        #
        #     static void
        #     lvs_update_on_failover_cpl(void *cb_arg, int lvolerrno)
        #     {
        #
        # so the function name sits at column 0. Matching only same-line
        # signatures found 76 symbols across 970 SPDK files, which is to say it
        # did not work at all. This first pattern is the one that matters here.
        r"^([a-z_][a-z0-9_]{3,})\s*\([^;]*\)\s*$",
        # int foo(...)  /  static void bar(...)  all on one line
        r"^[A-Za-z_][A-Za-z0-9_ \t\*]{0,60}\b([a-z_][a-z0-9_]{3,})\s*\([^;]*\)\s*\{?\s*$",
    ],
    "cpp": [
        r"^([A-Za-z_][A-Za-z0-9_:]{3,})\s*\([^;]*\)\s*(?:const)?\s*$",
        r"^[A-Za-z_][A-Za-z0-9_:<> \t\*&]{0,80}\b([A-Za-z_][A-Za-z0-9_]{3,})\s*\([^;]*\)\s*(const)?\s*\{?\s*$",
        r"^\s*(?:class|struct)\s+([A-Za-z_][A-Za-z0-9_]*)",
    ],
    "python": [r"^\s*(?:async\s+)?(?:def|class)\s+([A-Za-z_][A-Za-z0-9_]*)"],
    "go": [r"^func\s+(?:\([^)]*\)\s*)?([A-Za-z_][A-Za-z0-9_]*)"],
    "yaml": [],
}

# Message extraction.
#
# The first version of this matched only `logger.error("...")` and found 700
# strings in simplyblock_core, none of which were the messages that actually
# appear in RCAs. Real messages look like this:
#
#     msg = (f"Cannot create snapshot from lvol {lvol_id}: "
#     reason = (f"Node {peer.get_id()} is already shutting down in this cluster, "
#             f"port fence held {elapsed:.3f}s at {where}, over the "
#     "[RESTART] Unblocking %s for %s while it is NO LONGER leader -- "
#
# They are assigned to variables, split across continuation lines, and use
# three different formatting styles. So index every string literal that reads
# like a human sentence and let the heuristic below do the filtering.
STRING_RE = {
    "c": re.compile(r'"((?:[^"\\]|\\.){15,160})"'),
    "cpp": re.compile(r'"((?:[^"\\]|\\.){15,160})"'),
    "go": re.compile(r'"((?:[^"\\]|\\.){15,160})"'),
    "python": re.compile(r"(?:rf|fr|f|r|b)?[\"']((?:[^\"'\\]|\\.){15,160})[\"']"),
}

# Things that are strings but are not messages.
_NOT_MESSAGE = re.compile(
    r"^(?:https?://|/|SELECT |INSERT |\{|[a-z_]+=|%[sd]$|[a-z_]+$)")


def looks_like_message(text):
    """True for text a human would recognise in a log line.

    Cheap and deliberately imperfect: a false positive costs one wasted line
    in the map, a false negative costs a grep through the whole repo, which
    is the thing this file exists to avoid.
    """
    if text.count(" ") < 2:
        return False
    if not re.search(r"[A-Za-z]{3}", text):
        return False
    if _NOT_MESSAGE.match(text.strip()):
        return False
    letters = sum(c.isalpha() or c.isspace() for c in text)
    return letters >= len(text) * 0.6


def normalise_message(text):
    """Collapse format placeholders so {lvol_id}, %s and {} all match a real
    log line the same way, and so near-duplicates fold together."""
    t = re.sub(r"\{[^}]*\}", "{}", text)
    t = re.sub(r"%[-0-9.]*[a-zA-Z]", "{}", t)
    return re.sub(r"\s+", " ", t).strip()

EXT_LANG = {
    ".c": "c", ".h": "c",
    ".cpp": "cpp", ".cc": "cpp", ".hpp": "cpp", ".cxx": "cpp",
    ".py": "python",
    ".go": "go",
    ".yaml": "yaml", ".yml": "yaml",
}

DEFAULT_EXCLUDES = [
    ".git", "node_modules", "vendor", "build", "dist", "__pycache__",
    ".venv", "venv", "target", "third_party", "test/spdk_test", "docs/output",
]


def run(cmd, cwd=None):
    try:
        p = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True,
                           timeout=600)
        return p.stdout
    except Exception:
        return ""


def git_head(path):
    """Short SHA and branch, used to detect a stale map. Empty when the path is
    not a git checkout, in which case staleness cannot be determined."""
    sha = run(["git", "rev-parse", "--short", "HEAD"], cwd=path).strip()
    branch = run(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=path).strip()
    return sha, branch


def find_rg():
    """Locate a REAL ripgrep binary, or return None.

    Deliberately strict. On this machine `command -v rg` answers "rg" because it
    is a Git Bash alias, not an executable on PATH, so a naive check passes and
    then every subprocess call fails with WinError 2. A hook that depends on
    that would break silently, so the scanner below works without ripgrep and
    only uses it when it is genuinely present.
    """
    import shutil
    for name in ("rg", "rg.exe", "ripgrep"):
        path = shutil.which(name)
        if path:
            return path
    return None


def iter_source_files(root, langs):
    """Walk the tree once, yielding (abs_path, rel_path, lang)."""
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in DEFAULT_EXCLUDES]
        for fn in filenames:
            lang = EXT_LANG.get(os.path.splitext(fn)[1].lower())
            if lang and lang in langs:
                ap = os.path.join(dirpath, fn)
                rp = os.path.relpath(ap, root).replace("\\", "/")
                yield ap, rp, lang


def scan(root, langs, sym_res, log_res):
    """Single pass over the tree collecting symbols and log strings.

    Pure Python on purpose: no ripgrep, no ctags, nothing to install. A repo the
    size of SPDK takes tens of seconds, and this runs rarely.
    """
    syms, logs = [], []
    for ap, rp, lang in iter_source_files(root, langs):
        try:
            with open(ap, "r", encoding="utf-8", errors="ignore") as fh:
                for lineno, line in enumerate(fh, 1):
                    if len(line) > 400:
                        continue
                    for rx in sym_res.get(lang, ()):
                        m = rx.match(line)
                        if m:
                            syms.append((rp, lineno, m.group(1)))
                            break
                    srx = log_res.get(lang)
                    if srx:
                        for m in srx.finditer(line):
                            text = m.group(1)
                            if looks_like_message(text):
                                logs.append((rp, lineno,
                                             normalise_message(text)))
                                break
        except OSError:
            continue
    return syms, logs


def layout(root, langs):
    """Top directories with file counts, so the reader knows where to look."""
    counts = Counter()
    exts = Counter()
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in DEFAULT_EXCLUDES]
        rel = os.path.relpath(dirpath, root).replace("\\", "/")
        top = rel.split("/")[0] if rel != "." else "."
        for f in filenames:
            ext = os.path.splitext(f)[1].lower()
            if EXT_LANG.get(ext) in langs:
                counts[top] += 1
                exts[ext] += 1
    return counts, exts


def build_map(name, cfg, check_only=False):
    root = cfg["path"]
    if not os.path.isdir(root):
        return None, f"{name}: path does not exist: {root}"

    langs = cfg.get("languages") or ["c", "cpp", "python", "go"]
    sha, branch = git_head(root)
    out_path = os.path.join(OUT_DIR, f"{name}.md")

    if check_only:
        if not os.path.exists(out_path):
            return None, f"{name}: NO MAP (run repo_map.py to build it)"
        head_line = ""
        with open(out_path, encoding="utf-8") as fh:
            for line in fh:
                if line.startswith("head:"):
                    head_line = line.strip()
                    break
        if sha and sha not in head_line:
            return None, (f"{name}: map is STALE (map {head_line!r}, "
                          f"repo now at {sha})")
        return None, f"{name}: map current ({sha})"

    counts, exts = layout(root, langs)

    sym_res = {lang: tuple(re.compile(p) for p in pats)
               for lang, pats in SYMBOL_PATTERNS.items() if lang in langs}
    log_res = {lang: rx for lang, rx in STRING_RE.items() if lang in langs}
    syms, logs = scan(root, langs, sym_res, log_res)

    # De-duplicate, keeping the first definition seen for each name.
    seen_sym = {}
    for path, lineno, nm in syms:
        if nm not in seen_sym:
            seen_sym[nm] = (path, lineno)

    seen_log = {}
    for path, lineno, msg in logs:
        key = msg[:90]
        if key not in seen_log:
            seen_log[key] = (path, lineno)

    max_syms = cfg.get("max_symbols", 4000)
    max_logs = cfg.get("max_log_strings", 3000)

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Three files, on purpose.
    #
    # The index is big: simplyblock_core alone yields ~4,400 messages, and a
    # C repo the size of SPDK will yield far more. Reading that whole file
    # would cost more context than grepping the original repo, which would
    # defeat the point. So the .md stays small and human-readable, and the
    # two bulky indexes are plain TSV meant to be grepped, never read.
    os.makedirs(OUT_DIR, exist_ok=True)
    msg_path = os.path.join(OUT_DIR, name + ".messages.tsv")
    sym_path = os.path.join(OUT_DIR, name + ".symbols.tsv")

    with open(msg_path, "w", encoding="utf-8") as fh:
        for msg, (path, lineno) in sorted(seen_log.items())[:max_logs]:
            fh.write("%s\t%s:%d\n" % (msg, path, lineno))
    with open(sym_path, "w", encoding="utf-8") as fh:
        for nm, (path, lineno) in sorted(seen_sym.items())[:max_syms]:
            fh.write("%s\t%s:%d\n" % (nm, path, lineno))

    lines = [
        "# repo map: " + name,
        "",
        "path: " + root,
        "head: %s (%s)" % (sha, branch),
        "generated: " + now,
        "languages: " + ", ".join(langs),
        "",
        "## how to use this",
        "",
        "GREP the two TSV files, do not read them. They are large by design;",
        "reading one costs more context than it saves.",
        "",
        "```bash",
        "# where does a log message come from?  (the usual RCA entry point)",
        "grep -i 'Forcing application shutdown' .agents/repo-maps/%s.messages.tsv" % name,
        "",
        "# where is a function defined?",
        "grep -P '^lvs_update_on_failover_cpl\\t' .agents/repo-maps/%s.symbols.tsv" % name,
        "```",
        "",
        "Placeholders are normalised, so a message logged as",
        "`f\"port fence held {elapsed:.3f}s\"` is indexed as",
        "`port fence held {}s`. Grep for the stable words, not the values.",
        "",
        "| index | entries | file |",
        "|---|---|---|",
        "| messages | %d | `%s.messages.tsv` |" % (min(len(seen_log), max_logs), name),
        "| symbols  | %d | `%s.symbols.tsv` |" % (min(len(seen_sym), max_syms), name),
        "",
        "## layout",
        "",
        "```",
    ]
    for top, n in counts.most_common(30):
        lines.append("  %-34s %6d files" % (top + "/", n))
    lines += ["```", "", "## file types", "", "```"]
    for ext, n in exts.most_common(12):
        lines.append("  %-8s %6d" % (ext, n))
    lines += ["```", ""]

    body = "\n".join(lines) + "\n"
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write(body)

    def _kb(f):
        return os.path.getsize(f) / 1024

    return out_path, ("%s: %d symbols, %d messages -> %s (%.0f KB map, "
                      "%.0f KB messages, %.0f KB symbols)" % (
                          name, len(seen_sym), len(seen_log), out_path,
                          _kb(out_path), _kb(msg_path), _kb(sym_path)))

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--only", help="build just this repo name")
    ap.add_argument("--check", action="store_true",
                    help="report staleness without writing anything")
    ap.add_argument("--config", default=CONFIG)
    args = ap.parse_args()

    if not os.path.exists(args.config):
        print(f"no config at {args.config}")
        print("copy .agents/repo-map.config.example.json and edit the paths")
        return 1
    with open(args.config, encoding="utf-8") as fh:
        cfg = json.load(fh)

    repos = cfg.get("repos", {})
    if args.only:
        repos = {k: v for k, v in repos.items() if k == args.only}
        if not repos:
            print(f"no repo named {args.only!r} in the config")
            return 1

    rc = 0
    for name, rcfg in repos.items():
        try:
            _, msg = build_map(name, rcfg, check_only=args.check)
            print(msg)
            if args.check and ("STALE" in msg or "NO MAP" in msg):
                rc = 2
        except Exception as exc:                      # noqa: BLE001
            print(f"{name}: FAILED: {type(exc).__name__}: {exc}")
            rc = 1
    return rc


if __name__ == "__main__":
    sys.exit(main())
