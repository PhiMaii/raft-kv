#!/usr/bin/env python3
"""Renders the benchmark history as SVG charts, and status badges.

Usage: scripts/bench-graphs.py HISTORY_DIR OUT_DIR [--sim-stats FILE]
                                                    [--mutation-stats FILE]

For every series (machine) in HISTORY_DIR (`bench/history/*.jsonl`), writes
to OUT_DIR/SERIES/:

  throughput.svg   throughput per milestone, with the spread of its runs
  p99.svg          99th-percentile latency per milestone
  percentiles.svg  p50, p99 and p99.9 of every scenario in the latest record
  comparisons.svg  the latest record's A/B scenarios side by side

With --sim-stats and --mutation-stats (JSON from the nightly simulation),
also writes shields.io endpoint badges to OUT_DIR/badges/.

Uses only the standard library, so it runs the same locally and in CI. The
charts follow the light or dark color scheme of whoever views them.
"""

import argparse
import json
import math
import os
from html import escape

# Validated categorical slots 1-3 (scripts: dataviz validate_palette.js,
# all pairs, light and dark), and chart ink.
STYLE = """
<style>
  svg { font-family: system-ui, -apple-system, "Segoe UI", sans-serif; }
  .surface { fill: #fcfcfb; }
  .title { fill: #0b0b0b; font-size: 15px; font-weight: 600; }
  .subtitle { fill: #52514e; font-size: 12px; }
  .label { fill: #52514e; font-size: 12px; }
  .value { fill: #0b0b0b; font-size: 12px; font-variant-numeric: tabular-nums; }
  .tick { fill: #898781; font-size: 11px; font-variant-numeric: tabular-nums; }
  .grid { stroke: #e1e0d9; stroke-width: 1; }
  .axis { stroke: #c3c2b7; stroke-width: 1; }
  .link { stroke: #c3c2b7; stroke-width: 2; }
  .run { fill: #52514e; }
  .s1 { stroke: #2a78d6; fill: #2a78d6; }
  .s2 { stroke: #eb6834; fill: #eb6834; }
  .s3 { stroke: #1baf7a; fill: #1baf7a; }
  .ring { stroke: #fcfcfb; stroke-width: 2; }
  .line { fill: none; stroke-width: 2; }
  @media (prefers-color-scheme: dark) {
    .surface { fill: #1a1a19; }
    .title, .value { fill: #f0efec; }
    .subtitle, .label { fill: #c3c2b7; }
    .run { fill: #c3c2b7; }
    .grid { stroke: #2c2c2a; }
    .axis, .link { stroke: #383835; }
    .s1 { stroke: #3987e5; fill: #3987e5; }
    .s2 { stroke: #d95926; fill: #d95926; }
    .s3 { stroke: #199e70; fill: #199e70; }
    .ring { stroke: #1a1a19; }
  }
</style>
"""

# Human names for the scenarios in scripts/bench.sh.
SCENARIOS = {
    "writes": "Writes, 32 clients",
    "writes-128-clients": "Writes, 128 clients",
    "writes-5-nodes": "Writes, 5 nodes",
    "writes-unbatched": "Writes, unbatched",
    "writes-persist-first": "Writes, fsync before send",
    "mixed": "Mixed 50 / 50",
    "reads": "95 % reads, ReadIndex",
    "reads-through-log": "95 % reads, through the log",
    "single-client": "One client, 50 / 50",
}

WIDTH = 720


def fmt(value):
    """A number with thousands separators and sensible precision."""
    if value == 0:
        return "0"
    if value >= 100:
        return f"{value:,.0f}"
    if value >= 10:
        return f"{value:.1f}"
    return f"{value:.2f}"


def marker(shape, x, y, css, title):
    """A data marker (8 px) with a surface ring and a hover title."""
    tip = f"<title>{escape(title)}</title>"
    if shape == "circle":
        return f'<circle class="{css} ring" cx="{x:.1f}" cy="{y:.1f}" r="5">{tip}</circle>'
    if shape == "square":
        return (
            f'<rect class="{css} ring" x="{x - 4.5:.1f}" y="{y - 4.5:.1f}" '
            f'width="9" height="9" rx="1.5">{tip}</rect>'
        )
    points = f"{x:.1f},{y - 6:.1f} {x + 6:.1f},{y:.1f} {x:.1f},{y + 6:.1f} {x - 6:.1f},{y:.1f}"
    return f'<polygon class="{css} ring" points="{points}">{tip}</polygon>'


def svg(height, title, subtitle, body, description):
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {WIDTH} {height}" '
        f'width="{WIDTH}" height="{height}" role="img" aria-labelledby="t d">'
        f'<title id="t">{escape(title)}</title><desc id="d">{escape(description)}</desc>'
        f"{STYLE}"
        f'<rect class="surface" width="{WIDTH}" height="{height}" rx="8"/>'
        f'<text class="title" x="24" y="32">{escape(title)}</text>'
        f'<text class="subtitle" x="24" y="52">{escape(subtitle)}</text>'
        f"{body}</svg>\n"
    )


def nice_max(value):
    """A round upper bound for a linear axis starting at 0."""
    if value <= 0:
        return 1
    exponent = 10 ** math.floor(math.log10(value))
    for step in (1, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10):
        if value <= step * exponent:
            return step * exponent
    return 10 * exponent


def by_label(records):
    """Records grouped per milestone, in the order they were recorded."""
    groups = {}
    for record in sorted(records, key=lambda r: r["date"]):
        groups.setdefault((record["label"], record["commit"]), {})[record["scenario"]] = record
    return list(groups.items())


def history_chart(records, machine, title, unit, lines, value_of, spread_of=None):
    """A line per scenario across milestones, on one linear axis from 0."""
    points = by_label(records)
    left, right, top, bottom = 72, WIDTH - 190, 96, 300
    height = bottom + 56
    values = [
        value
        for _, scenarios in points
        for scenario, *_ in lines
        if scenario in scenarios
        for value in ([value_of(scenarios[scenario])] + (spread_of(scenarios[scenario]) if spread_of else []))
    ]
    top_value = nice_max(max(values) * 1.05 if values else 1)
    count = len(points)

    def x_at(i):
        return (left + right) / 2 if count == 1 else left + i * (right - left) / (count - 1)

    def y_at(v):
        return bottom - v / top_value * (bottom - top)

    body = []
    for step in range(5):
        v = top_value * step / 4
        y = y_at(v)
        body.append(f'<line class="{"axis" if step == 0 else "grid"}" x1="{left}" x2="{right}" y1="{y:.1f}" y2="{y:.1f}"/>')
        tick = f"{v:,.0f}" if v == int(v) else fmt(v)
        body.append(f'<text class="tick" x="{left - 8}" y="{y + 4:.1f}" text-anchor="end">{tick}</text>')
    body.append(f'<text class="tick" x="{left - 8}" y="{top - 14}" text-anchor="end">{escape(unit)}</text>')
    for i, ((label, commit), _) in enumerate(points):
        body.append(
            f'<text class="tick" x="{x_at(i):.1f}" y="{bottom + 20}" text-anchor="middle">{escape(label)}</text>'
            f'<text class="tick" x="{x_at(i):.1f}" y="{bottom + 34}" text-anchor="middle">{commit[:7]}</text>'
        )
    # Legend in one row above the plot.
    lx = left
    for scenario, name, css, shape in lines:
        body.append(marker(shape, lx + 5, 76, css, name))
        body.append(f'<text class="label" x="{lx + 16}" y="80">{escape(name)}</text>')
        lx += 22 + 7 * len(name)
    end_labels = []
    for scenario, name, css, shape in lines:
        series = [(i, s[scenario]) for i, (_, s) in enumerate(points) if scenario in s]
        if not series:
            continue
        if len(series) > 1:
            path = " ".join(
                f'{"M" if n == 0 else "L"}{x_at(i):.1f},{y_at(value_of(r)):.1f}'
                for n, (i, r) in enumerate(series)
            )
            body.append(f'<path class="{css} line" d="{path}"/>')
        for i, record in series:
            x, y = x_at(i), y_at(value_of(record))
            if spread_of:
                low, high = min(spread_of(record)), max(spread_of(record))
                body.append(
                    f'<line class="{css}" stroke-width="2" x1="{x:.1f}" x2="{x:.1f}" '
                    f'y1="{y_at(low):.1f}" y2="{y_at(high):.1f}" opacity="0.5"/>'
                )
            runs = ""
            if spread_of:
                runs = "; runs " + " / ".join(fmt(v) for v in spread_of(record))
            tip = f"{record['label']} ({record['commit'][:7]}): {name} {fmt(value_of(record))} {unit}{runs}"
            body.append(marker(shape, x, y, css, tip))
        i, last = series[-1]
        end_labels.append([y_at(value_of(last)) + 4, x_at(i) + 12, f"{fmt(value_of(last))} · {name}"])
    # Push end labels apart where values are close, keeping their order.
    end_labels.sort()
    for n in range(1, len(end_labels)):
        end_labels[n][0] = max(end_labels[n][0], end_labels[n - 1][0] + 16)
    for y, x, text in end_labels:
        body.append(f'<text class="value" x="{x:.1f}" y="{y:.1f}">{escape(text)}</text>')
    subtitle = f"{machine}; every node on this one machine, real fsync"
    if spread_of:
        subtitle += "; bars span the three runs"
    description = "; ".join(
        f"{label}: "
        + ", ".join(f"{name} {fmt(value_of(s[sc]))}" for sc, name, *_ in lines if sc in s)
        for (label, _), s in points
    )
    return svg(height, title, subtitle, "".join(body), description)


def operation(record, op):
    return record[op] if record[op]["count"] else None


def percentiles_chart(latest, machine):
    """p50, p99 and p99.9 per scenario of the latest record, on a log axis."""
    rows = [
        (scenario, op)
        for scenario in SCENARIOS
        if scenario in latest
        for op in ("writes", "reads")
        if operation(latest[scenario], op)
    ]
    left, right, top = 250, WIDTH - 40, 104
    row = 26
    bottom = top + row * len(rows)
    height = bottom + 52
    low_ms, high_ms = 0.1, 1000.0

    def x_at(ms):
        ms = min(max(ms, low_ms), high_ms)
        return left + (math.log10(ms) - math.log10(low_ms)) / (
            math.log10(high_ms) - math.log10(low_ms)
        ) * (right - left)

    body = []
    for ms in (0.1, 1, 10, 100, 1000):
        x = x_at(ms)
        body.append(f'<line class="grid" x1="{x:.1f}" x2="{x:.1f}" y1="{top - 8}" y2="{bottom}"/>')
        body.append(f'<text class="tick" x="{x:.1f}" y="{bottom + 18}" text-anchor="middle">{ms:g} ms</text>')
    marks = [("p50_us", "p50", "s1", "circle"), ("p99_us", "p99", "s2", "square"), ("p999_us", "p99.9", "s3", "diamond")]
    lx = left
    for _, name, css, shape in marks:
        body.append(marker(shape, lx + 5, 80, css, name))
        body.append(f'<text class="label" x="{lx + 16}" y="84">{name}</text>')
        lx += 70
    for n, (scenario, op) in enumerate(rows):
        y = top + n * row + row / 2
        stats = latest[scenario][op]
        name = f"{SCENARIOS[scenario]} · {op}"
        body.append(f'<text class="label" x="{left - 14}" y="{y + 4:.1f}" text-anchor="end">{escape(name)}</text>')
        xs = [x_at(stats[key] / 1000) for key, *_ in marks]
        body.append(f'<line class="link" x1="{xs[0]:.1f}" x2="{xs[-1]:.1f}" y1="{y:.1f}" y2="{y:.1f}"/>')
        for (key, label, css, shape), x in zip(marks, xs):
            body.append(marker(shape, x, y, css, f"{name}: {label} {fmt(stats[key] / 1000)} ms"))
    record = next(iter(latest.values()))
    subtitle = f"{record['label']} ({record['commit'][:7]}), {machine}; log scale"
    description = "; ".join(
        f"{SCENARIOS[s]} {op}: p50 {fmt(latest[s][op]['p50_us'] / 1000)} ms, "
        f"p99 {fmt(latest[s][op]['p99_us'] / 1000)} ms, p99.9 {fmt(latest[s][op]['p999_us'] / 1000)} ms"
        for s, op in rows
    )
    return svg(height, "Latency percentiles", subtitle, "".join(body), description)


COMPARISONS = [
    ("Batching", [("writes", "on"), ("writes-unbatched", "off")]),
    ("Leader fsync", [("writes", "send first"), ("writes-persist-first", "fsync first")]),
    ("Read path", [("reads", "ReadIndex"), ("reads-through-log", "through the log")]),
    ("Cluster size", [("writes", "3 nodes"), ("writes-5-nodes", "5 nodes")]),
]


def comparisons_chart(latest, machine):
    """Pairs of scenarios as horizontal bars, with each run as a dot."""
    panels = [(t, [(s, n) for s, n in pairs if s in latest]) for t, pairs in COMPARISONS]
    panels = [(t, pairs) for t, pairs in panels if len(pairs) == 2]
    panel_w, panel_h = (WIDTH - 72) / 2, 104
    top = 76
    height = top + panel_h * math.ceil(len(panels) / 2) + 24
    body = []
    for k, (title, pairs) in enumerate(panels):
        px = 24 + (k % 2) * (panel_w + 24)
        py = top + (k // 2) * panel_h
        label_w, bar_x0 = 112, px + 112
        bar_w = panel_w - label_w - 64
        peak = nice_max(max(max(latest[s]["runs"]) for s, _ in pairs))
        body.append(f'<text class="label" x="{px}" y="{py + 14}" font-weight="600">{escape(title)}</text>')
        for j, (scenario, name) in enumerate(pairs):
            record = latest[scenario]
            y = py + 28 + j * 30
            w = record["throughput"] / peak * bar_w
            runs = " / ".join(fmt(v) for v in record["runs"])
            tip = f"{SCENARIOS[scenario]}: {fmt(record['throughput'])} ops/s (median); runs {runs}"
            body.append(f'<text class="label" x="{bar_x0 - 10}" y="{y + 14}" text-anchor="end">{escape(name)}</text>')
            body.append(f'<line class="axis" x1="{bar_x0}" x2="{bar_x0}" y1="{y - 2}" y2="{y + 22}"/>')
            # Flat at the baseline, rounded at the data end.
            body.append(
                f'<path class="s1" d="M{bar_x0},{y} h{w - 4:.1f} a4,4 0 0 1 4,4 v12 a4,4 0 0 1 -4,4 '
                f'h{-(w - 4):.1f} z"><title>{escape(tip)}</title></path>'
            )
            for run in record["runs"]:
                rx = bar_x0 + run / peak * bar_w
                body.append(f'<circle class="run ring" cx="{rx:.1f}" cy="{y + 10}" r="3"><title>run: {fmt(run)} ops/s</title></circle>')
            outermost = max(w, max(record["runs"]) / peak * bar_w)
            body.append(f'<text class="value" x="{bar_x0 + outermost + 10:.1f}" y="{y + 14}">{fmt(record["throughput"])}</text>')
    record = next(iter(latest.values()))
    subtitle = (
        f"Throughput in ops/s, {record['label']} ({record['commit'][:7]}), {machine}; "
        "bars are medians, dots the three runs"
    )
    description = "; ".join(
        f"{t}: " + ", ".join(f"{n} {fmt(latest[s]['throughput'])} ops/s" for s, n in pairs)
        for t, pairs in panels
    )
    return svg(height, "A/B comparisons", subtitle, "".join(body), description)


def machine_name(record):
    m = record["machine"]
    return f"{m['cpu']}, {m['memory_gib']} GiB, {m['os']} {m['os_version']}"


def render_series(records, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    machine = machine_name(records[-1])
    latest_date = max(r["date"] for r in records)
    latest = {r["scenario"]: r for r in records if r["date"] == latest_date}
    charts = {
        "throughput.svg": history_chart(
            records,
            machine,
            "Throughput per milestone",
            "ops/s",
            [
                ("reads", "95 % reads", "s1", "circle"),
                ("writes", "writes", "s2", "square"),
                ("mixed", "mixed", "s3", "diamond"),
            ],
            lambda r: r["throughput"],
            lambda r: r["runs"],
        ),
        "p99.svg": history_chart(
            records,
            machine,
            "99th-percentile latency per milestone",
            "ms",
            [
                ("reads", "reads (95 % reads)", "s1", "circle"),
                ("writes", "writes (writes only)", "s2", "square"),
            ],
            lambda r: (r["reads"] if r["scenario"] == "reads" else r["writes"])["p99_us"] / 1000,
        ),
        "percentiles.svg": percentiles_chart(latest, machine),
        "comparisons.svg": comparisons_chart(latest, machine),
    }
    for name, content in charts.items():
        with open(os.path.join(out_dir, name), "w") as f:
            f.write(content)
    return list(charts)


def badge(path, label, message, ok):
    with open(path, "w") as f:
        json.dump(
            {
                "schemaVersion": 1,
                "label": label,
                "message": message,
                "color": "brightgreen" if ok else "red",
            },
            f,
        )


def render_badges(sim_path, mutation_path, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    if sim_path and os.path.exists(sim_path):
        with open(sim_path) as f:
            sim = json.load(f)
        badge(
            os.path.join(out_dir, "simulation.json"),
            "nightly simulation",
            f"{sim['seeds']:,} seeds, {sim['failed']} failed",
            sim["failed"] == 0,
        )
    if mutation_path and os.path.exists(mutation_path):
        with open(mutation_path) as f:
            mutations = json.load(f)
        caught = sum(1 for m in mutations["mutations"] if m["caught"] > 0)
        total = len(mutations["mutations"])
        badge(
            os.path.join(out_dir, "mutations.json"),
            "injected bugs caught",
            f"{caught} / {total}",
            caught == total,
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("history_dir")
    parser.add_argument("out_dir")
    parser.add_argument("--sim-stats")
    parser.add_argument("--mutation-stats")
    args = parser.parse_args()

    for name in sorted(os.listdir(args.history_dir)):
        if not name.endswith(".jsonl"):
            continue
        with open(os.path.join(args.history_dir, name)) as f:
            records = [json.loads(line) for line in f if line.strip()]
        if not records:
            continue
        series = name[: -len(".jsonl")]
        charts = render_series(records, os.path.join(args.out_dir, series))
        print(f"{series}: {', '.join(charts)}")
    render_badges(args.sim_stats, args.mutation_stats, os.path.join(args.out_dir, "badges"))


if __name__ == "__main__":
    main()
