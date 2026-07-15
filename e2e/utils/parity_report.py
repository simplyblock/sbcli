"""HTML + JSON report generator for CLI / v1 / v2 parity audit."""

import json
import os
from datetime import datetime, timezone


def generate_html_report(findings, output_dir, cluster_id="", extra_meta=None):
    """Write an HTML report and a JSON sidecar to *output_dir*.

    Parameters
    ----------
    findings : list[dict]
        Each dict has keys: severity, category, operation, field,
        detail, cli, v1, v2 (presence or value depending on category).
    output_dir : str
        Directory to write ``api_parity_report.html`` and
        ``api_parity_findings.json`` into.
    cluster_id : str
        Cluster identifier shown in the header.
    extra_meta : dict | None
        Extra key-value pairs rendered in the header.

    Returns
    -------
    str
        Absolute path to the HTML file.
    """
    os.makedirs(output_dir, exist_ok=True)

    html_path = os.path.join(output_dir, "api_parity_report.html")
    json_path = os.path.join(output_dir, "api_parity_findings.json")

    # Write JSON sidecar
    with open(json_path, "w") as f:
        json.dump(findings, f, indent=2, default=str)

    # Counts
    errors = [f for f in findings if f.get("severity") == "error"]
    warnings = [f for f in findings if f.get("severity") == "warning"]
    infos = [f for f in findings if f.get("severity") == "info"]

    # Group by category
    categories = {}
    for f in findings:
        cat = f.get("category", "unknown")
        categories.setdefault(cat, []).append(f)

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    html = _build_html(
        findings=findings,
        errors=errors,
        warnings=warnings,
        infos=infos,
        categories=categories,
        ts=ts,
        cluster_id=cluster_id,
        extra_meta=extra_meta or {},
    )

    with open(html_path, "w") as f:
        f.write(html)

    return html_path


# ── private helpers ───────────────────────────────────────────────────

_CSS = """
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
       margin: 0; padding: 20px; background: #f5f5f5; color: #333; }
.container { max-width: 1200px; margin: 0 auto; }
h1 { color: #1a1a2e; border-bottom: 3px solid #16213e; padding-bottom: 10px; }
h2 { color: #16213e; margin-top: 30px; }
.meta { color: #666; font-size: 0.9em; margin-bottom: 20px; }
.summary { display: flex; gap: 15px; margin-bottom: 30px; }
.card { padding: 15px 25px; border-radius: 8px; color: #fff; font-size: 1.1em;
        min-width: 120px; text-align: center; }
.card-total { background: #16213e; }
.card-error { background: #e74c3c; }
.card-warning { background: #f39c12; }
.card-info { background: #3498db; }
.card .count { font-size: 2em; font-weight: bold; display: block; }
table { border-collapse: collapse; width: 100%; margin-bottom: 20px;
        background: #fff; border-radius: 6px; overflow: hidden;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
th { background: #16213e; color: #fff; padding: 10px 14px; text-align: left;
     font-weight: 600; font-size: 0.85em; text-transform: uppercase; }
td { padding: 8px 14px; border-bottom: 1px solid #eee; font-size: 0.9em; }
tr:hover { background: #f8f9fa; }
.sev-error { color: #e74c3c; font-weight: bold; }
.sev-warning { color: #f39c12; font-weight: bold; }
.sev-info { color: #3498db; }
.check { color: #27ae60; }
.cross { color: #e74c3c; }
.badge { display: inline-block; padding: 2px 8px; border-radius: 4px;
         font-size: 0.8em; font-weight: 600; }
.badge-error { background: #fde8e8; color: #e74c3c; }
.badge-warning { background: #fef3e2; color: #f39c12; }
.badge-info { background: #e8f4fd; color: #3498db; }
.mono { font-family: 'SF Mono', 'Consolas', monospace; font-size: 0.85em; }
.empty { color: #999; font-style: italic; }
"""


def _esc(s):
    """HTML-escape a string."""
    if s is None:
        return '<span class="empty">—</span>'
    s = str(s)
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _sev_class(severity):
    return f"sev-{severity}"


def _badge(severity):
    return f'<span class="badge badge-{severity}">{severity.upper()}</span>'


def _presence(val):
    """Render a boolean presence check/cross."""
    if val is True:
        return '<span class="check">&#10003;</span>'
    if val is False:
        return '<span class="cross">&#10007;</span>'
    return _esc(val)


def _build_html(findings, errors, warnings, infos, categories, ts,
                cluster_id, extra_meta):
    parts = []
    parts.append(f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>API Parity Audit Report</title>
<style>{_CSS}</style>
</head>
<body>
<div class="container">
<h1>SimplyBlock API Parity Audit Report</h1>
<div class="meta">
  Generated: {ts} | Cluster: <strong>{_esc(cluster_id)}</strong>""")

    for k, v in extra_meta.items():
        parts.append(f" | {_esc(k)}: <strong>{_esc(v)}</strong>")

    parts.append("</div>")

    # ── summary cards ──
    parts.append('<div class="summary">')
    parts.append(f'<div class="card card-total"><span class="count">{len(findings)}</span>Total</div>')
    parts.append(f'<div class="card card-error"><span class="count">{len(errors)}</span>Errors</div>')
    parts.append(f'<div class="card card-warning"><span class="count">{len(warnings)}</span>Warnings</div>')
    parts.append(f'<div class="card card-info"><span class="count">{len(infos)}</span>Info</div>')
    parts.append("</div>")

    # ── operation not tested ──
    not_tested = [f for f in findings if f.get("category") == "not_tested"]
    if not_tested:
        parts.append("<h2>Operations Not Tested</h2>")
        parts.append("<table><tr><th>Operation</th><th>CLI</th><th>v1</th><th>v2</th><th>Reason</th></tr>")
        for f in not_tested:
            parts.append(
                f'<tr><td class="mono">{_esc(f.get("operation"))}</td>'
                f"<td>{_presence(f.get('cli'))}</td>"
                f"<td>{_presence(f.get('v1'))}</td>"
                f"<td>{_presence(f.get('v2'))}</td>"
                f'<td>{_esc(f.get("detail"))}</td></tr>'
            )
        parts.append("</table>")

    # ── missing fields ──
    missing = [f for f in findings if f.get("category") == "missing_field"]
    if missing:
        parts.append("<h2>Missing Fields (field present in some interfaces but not all)</h2>")
        parts.append("<table><tr><th>Severity</th><th>Operation</th><th>Field</th>"
                     "<th>CLI</th><th>v1</th><th>v2</th></tr>")
        for f in missing:
            parts.append(
                f"<tr><td>{_badge(f['severity'])}</td>"
                f'<td class="mono">{_esc(f.get("operation"))}</td>'
                f'<td class="mono">{_esc(f.get("field"))}</td>'
                f"<td>{_presence(f.get('cli'))}</td>"
                f"<td>{_presence(f.get('v1'))}</td>"
                f"<td>{_presence(f.get('v2'))}</td></tr>"
            )
        parts.append("</table>")

    # ── value mismatches ──
    mismatches = [f for f in findings if f.get("category") == "value_mismatch"]
    if mismatches:
        parts.append("<h2>Value Mismatches (same field, different values)</h2>")
        parts.append("<table><tr><th>Severity</th><th>Operation</th><th>Field</th>"
                     "<th>CLI</th><th>v1</th><th>v2</th></tr>")
        for f in mismatches:
            parts.append(
                f"<tr><td>{_badge(f['severity'])}</td>"
                f'<td class="mono">{_esc(f.get("operation"))}</td>'
                f'<td class="mono">{_esc(f.get("field"))}</td>'
                f'<td class="mono">{_esc(f.get("cli"))}</td>'
                f'<td class="mono">{_esc(f.get("v1"))}</td>'
                f'<td class="mono">{_esc(f.get("v2"))}</td></tr>'
            )
        parts.append("</table>")

    # ── interface errors ──
    iface_errors = [f for f in findings if f.get("category") == "interface_error"]
    if iface_errors:
        parts.append("<h2>Interface Errors (one interface returned an error)</h2>")
        parts.append("<table><tr><th>Severity</th><th>Operation</th><th>Interface</th>"
                     "<th>Detail</th></tr>")
        for f in iface_errors:
            parts.append(
                f"<tr><td>{_badge(f['severity'])}</td>"
                f'<td class="mono">{_esc(f.get("operation"))}</td>'
                f'<td>{_esc(f.get("interface"))}</td>'
                f'<td>{_esc(f.get("detail"))}</td></tr>'
            )
        parts.append("</table>")

    # ── count mismatches ──
    counts = [f for f in findings if f.get("category") == "count_mismatch"]
    if counts:
        parts.append("<h2>Count Mismatches (different number of items returned)</h2>")
        parts.append("<table><tr><th>Severity</th><th>Operation</th>"
                     "<th>CLI count</th><th>v1 count</th><th>v2 count</th></tr>")
        for f in counts:
            parts.append(
                f"<tr><td>{_badge(f['severity'])}</td>"
                f'<td class="mono">{_esc(f.get("operation"))}</td>'
                f'<td>{_esc(f.get("cli"))}</td>'
                f'<td>{_esc(f.get("v1"))}</td>'
                f'<td>{_esc(f.get("v2"))}</td></tr>'
            )
        parts.append("</table>")

    # ── all findings (raw) ──
    remaining = [f for f in findings
                 if f.get("category") not in
                 ("not_tested", "missing_field", "value_mismatch",
                  "interface_error", "count_mismatch")]
    if remaining:
        parts.append("<h2>Other Findings</h2>")
        parts.append("<table><tr><th>Severity</th><th>Category</th><th>Operation</th>"
                     "<th>Field</th><th>Detail</th></tr>")
        for f in remaining:
            parts.append(
                f"<tr><td>{_badge(f['severity'])}</td>"
                f'<td>{_esc(f.get("category"))}</td>'
                f'<td class="mono">{_esc(f.get("operation"))}</td>'
                f'<td class="mono">{_esc(f.get("field", ""))}</td>'
                f'<td>{_esc(f.get("detail", ""))}</td></tr>'
            )
        parts.append("</table>")

    if not findings:
        parts.append('<p class="empty">No findings — all interfaces are in parity.</p>')

    parts.append("</div></body></html>")
    return "\n".join(parts)
