#!/usr/bin/env python3
"""Render the dependency-free join-ordering scalability benchmark."""

from __future__ import annotations

import csv
import hashlib
import html
import json
import math
import platform
import shutil
import statistics
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

WIDTH = 1200
HEIGHT = 760
MARGIN = {"left": 105, "right": 40, "top": 85, "bottom": 90}
COLORS = {
    "adaptive": "#0072B2",
    "dphyp": "#D55E00",
    "linearized_dp": "#009E73",
    "goo_dp": "#CC79A7",
    "DpHyp": "#D55E00",
    "LinearizedDp": "#009E73",
    "GooDp": "#CC79A7",
    "chain": "#0072B2",
    "star": "#009E73",
    "clique": "#D55E00",
    "borrowed_union": "#0072B2",
    "allocating_assign": "#D55E00",
    "in_place_assign": "#009E73",
}


def quantile(values: list[float], probability: float) -> float:
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = probability * (len(ordered) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def summary(values: list[float]) -> dict[str, float]:
    return {
        "min": min(values),
        "p10": quantile(values, 0.10),
        "median": statistics.median(values),
        "p90": quantile(values, 0.90),
        "p95": quantile(values, 0.95),
        "max": max(values),
    }


class Svg:
    def __init__(self, title: str, description: str):
        self.parts = [
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{HEIGHT}" '
            f'viewBox="0 0 {WIDTH} {HEIGHT}" role="img" aria-labelledby="title desc">',
            f"<title id=\"title\">{html.escape(title)}</title>",
            f"<desc id=\"desc\">{html.escape(description)}</desc>",
            '<rect width="100%" height="100%" fill="#ffffff"/>',
            '<style>text{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;fill:#202124}'
            ".title{font-size:28px;font-weight:600}.subtitle{font-size:15px;fill:#5f6368}"
            ".axis{stroke:#5f6368;stroke-width:1.2}.grid{stroke:#dadce0;stroke-width:1}"
            ".tick{font-size:13px;fill:#5f6368}.label{font-size:16px;font-weight:500}"
            ".legend{font-size:14px}.annotation{font-size:13px;font-weight:500}</style>",
        ]

    def line(self, x1, y1, x2, y2, stroke="#202124", width=1, dash=None, opacity=1):
        dash_attr = f' stroke-dasharray="{dash}"' if dash else ""
        self.parts.append(
            f'<line x1="{x1:.2f}" y1="{y1:.2f}" x2="{x2:.2f}" y2="{y2:.2f}" '
            f'stroke="{stroke}" stroke-width="{width}" opacity="{opacity}"{dash_attr}/>'
        )

    def rect(self, x, y, width, height, fill, stroke="none", opacity=1, radius=0):
        self.parts.append(
            f'<rect x="{x:.2f}" y="{y:.2f}" width="{width:.2f}" height="{height:.2f}" '
            f'rx="{radius}" fill="{fill}" stroke="{stroke}" opacity="{opacity}"/>'
        )

    def circle(self, x, y, radius, fill, stroke="#ffffff", width=1.5):
        self.parts.append(
            f'<circle cx="{x:.2f}" cy="{y:.2f}" r="{radius}" fill="{fill}" '
            f'stroke="{stroke}" stroke-width="{width}"/>'
        )

    def polyline(self, points, stroke, width=3, dash=None):
        encoded = " ".join(f"{x:.2f},{y:.2f}" for x, y in points)
        dash_attr = f' stroke-dasharray="{dash}"' if dash else ""
        self.parts.append(
            f'<polyline points="{encoded}" fill="none" stroke="{stroke}" '
            f'stroke-width="{width}" stroke-linejoin="round" stroke-linecap="round"{dash_attr}/>'
        )

    def text(self, x, y, value, css="tick", anchor="middle", rotate=None, fill=None):
        transform = f' transform="rotate({rotate} {x} {y})"' if rotate else ""
        fill_attr = f' fill="{fill}"' if fill else ""
        self.parts.append(
            f'<text x="{x:.2f}" y="{y:.2f}" class="{css}" text-anchor="{anchor}"'
            f'{transform}{fill_attr}>{html.escape(str(value))}</text>'
        )

    def finish(self) -> str:
        return "\n".join(self.parts + ["</svg>", ""])


def chart_header(svg: Svg, title: str, subtitle: str):
    svg.text(MARGIN["left"], 38, title, "title", "start")
    svg.text(MARGIN["left"], 66, subtitle, "subtitle", "start")


def log_scale(value, minimum, maximum, start, end):
    value = max(value, minimum)
    fraction = (math.log10(value) - math.log10(minimum)) / (
        math.log10(maximum) - math.log10(minimum)
    )
    return start + fraction * (end - start)


def linear_scale(value, minimum, maximum, start, end):
    return start + (value - minimum) / (maximum - minimum) * (end - start)


def draw_log_axes(svg: Svg, x_values, y_min, y_max, x_label, y_label):
    left = MARGIN["left"]
    right = WIDTH - MARGIN["right"]
    top = MARGIN["top"]
    bottom = HEIGHT - MARGIN["bottom"]
    svg.line(left, top, left, bottom, stroke="#5f6368", width=1.2)
    svg.line(left, bottom, right, bottom, stroke="#5f6368", width=1.2)
    for exponent in range(math.floor(math.log10(y_min)), math.ceil(math.log10(y_max)) + 1):
        value = 10**exponent
        if y_min <= value <= y_max:
            y = log_scale(value, y_min, y_max, bottom, top)
            svg.line(left, y, right, y, stroke="#dadce0")
            svg.text(left - 12, y + 5, format_duration_ms(value), anchor="end")
    for value in x_values:
        x = log_scale(value, min(x_values), max(x_values), left, right)
        svg.line(x, bottom, x, bottom + 6, stroke="#5f6368")
        svg.text(x, bottom + 25, value)
    svg.text((left + right) / 2, HEIGHT - 32, x_label, "label")
    svg.text(28, (top + bottom) / 2, y_label, "label", rotate=-90)
    return left, right, top, bottom


def format_duration_ms(value: float) -> str:
    if value >= 1_000:
        return f"{value / 1_000:g} s"
    if value >= 1:
        return f"{value:g} ms"
    return f"{value * 1_000:g} us"


def load_rows(path: Path):
    with path.open(newline="") as source:
        rows = list(csv.DictReader(source))
    for row in rows:
        row["relations"] = int(row["relations"])
        row["query_id"] = int(row["query_id"])
        row["repetition"] = int(row["repetition"])
        row["operations"] = int(row["operations"])
        row["duration_ns"] = int(row["duration_ns"])
        row["candidate_operators"] = int(row["candidate_operators"])
        row["duration_ms"] = row["duration_ns"] / row["operations"] / 1_000_000
    return rows


def aggregate_random(rows):
    grouped = defaultdict(list)
    for row in rows:
        if row["suite"] == "random_tree_algorithms":
            grouped[(row["relations"], row["variant"])].append(row)
    result = {}
    for key, group in grouped.items():
        timings = [row["duration_ms"] for row in group]
        candidates = [row["candidate_operators"] for row in group]
        result[key] = {
            **summary(timings),
            "count": len(group),
            "median_candidates": statistics.median(candidates),
            "selection": Counter(row["selected_algorithm"] for row in group),
        }
    return result


def figure_algorithm_scaling(aggregate, destination: Path):
    title = "Join-ordering time across random tree queries"
    subtitle = "Median with p10-p90 range; 10 deterministic queries per size; log-log axes"
    svg = Svg(title, subtitle)
    chart_header(svg, title, subtitle)
    all_sizes = sorted({key[0] for key in aggregate})
    left, right, top, bottom = draw_log_axes(
        svg, [10, 20, 40, 70, 100, 128, 192, 256], 0.01, 10_000, "Relations", "Optimization time"
    )
    variants = ["adaptive", "dphyp", "linearized_dp", "goo_dp"]
    labels = {
        "adaptive": "Adaptive",
        "dphyp": "DPhyp",
        "linearized_dp": "Linearized DP",
        "goo_dp": "GOO/DP",
    }
    for index, variant in enumerate(variants):
        points = []
        entries = sorted((size, value) for (size, name), value in aggregate.items() if name == variant)
        for size, value in entries:
            x = log_scale(size, min(all_sizes), max(all_sizes), left, right)
            y = log_scale(value["median"], 0.01, 10_000, bottom, top)
            low = log_scale(value["p10"], 0.01, 10_000, bottom, top)
            high = log_scale(value["p90"], 0.01, 10_000, bottom, top)
            svg.line(x, high, x, low, COLORS[variant], width=1.5, opacity=0.7)
            svg.line(x - 4, high, x + 4, high, COLORS[variant], width=1.5)
            svg.line(x - 4, low, x + 4, low, COLORS[variant], width=1.5)
            points.append((x, y))
        svg.polyline(points, COLORS[variant], dash="8 5" if variant == "dphyp" else None)
        for x, y in points:
            svg.circle(x, y, 5, COLORS[variant])
        legend_x = left + index * 230
        svg.line(legend_x, top + 20, legend_x + 28, top + 20, COLORS[variant], width=3)
        svg.circle(legend_x + 14, top + 20, 4, COLORS[variant])
        svg.text(legend_x + 38, top + 25, labels[variant], "legend", "start")
    destination.write_text(svg.finish())


def figure_adaptive_policy(rows, aggregate, destination: Path):
    title = "Adaptive policy transitions and their cost"
    subtitle = "Algorithm share across 10 random trees; bars show adaptive median time"
    svg = Svg(title, subtitle)
    chart_header(svg, title, subtitle)
    sizes = sorted({row["relations"] for row in rows if row["suite"] == "random_tree_algorithms" and row["variant"] == "adaptive"})
    left, right, top, bottom = draw_log_axes(
        svg, sizes, 0.1, 10_000, "Relations", "Adaptive optimization time"
    )
    bar_width = 24
    for size in sizes:
        value = aggregate[(size, "adaptive")]
        x = log_scale(size, min(sizes), max(sizes), left, right)
        y = log_scale(value["median"], 0.1, 10_000, bottom, top)
        selection = value["selection"]
        total = sum(selection.values())
        offset = 0.0
        for algorithm in ["DpHyp", "LinearizedDp", "GooDp"]:
            fraction = selection[algorithm] / total
            if fraction:
                segment = (bottom - y) * fraction
                svg.rect(x - bar_width / 2, bottom - offset - segment, bar_width, segment, COLORS[algorithm])
                offset += segment
        mix = "/".join(f"{name}:{selection[name]}" for name in ["DpHyp", "LinearizedDp", "GooDp"] if selection[name])
        svg.text(x, y - 9, mix, "annotation")
    for index, (algorithm, label) in enumerate([
        ("DpHyp", "DPhyp"),
        ("LinearizedDp", "Linearized DP"),
        ("GooDp", "GOO/DP"),
    ]):
        x = left + index * 230
        svg.rect(x, top + 7, 18, 18, COLORS[algorithm], radius=2)
        svg.text(x + 28, top + 21, label, "legend", "start")
    destination.write_text(svg.finish())


def figure_work_scaling(aggregate, destination: Path):
    title = "Elapsed time versus materialized candidate operators"
    subtitle = "Random tree medians; labels are relation counts; log-log axes"
    svg = Svg(title, subtitle)
    chart_header(svg, title, subtitle)
    entries = [
        (size, variant, value)
        for (size, variant), value in aggregate.items()
        if variant in {"adaptive", "linearized_dp", "goo_dp"}
    ]
    x_min, x_max = 10, 100_000
    y_min, y_max = 0.01, 10_000
    left, right, top, bottom = draw_log_axes(
        svg, [10, 100, 1_000, 10_000, 100_000], y_min, y_max, "Candidate operators", "Optimization time"
    )
    for index, variant in enumerate(["adaptive", "linearized_dp", "goo_dp"]):
        points = []
        for size, name, value in sorted(entries):
            if name != variant:
                continue
            x = log_scale(value["median_candidates"], x_min, x_max, left, right)
            y = log_scale(value["median"], y_min, y_max, bottom, top)
            points.append((x, y, size))
        svg.polyline([(x, y) for x, y, _ in points], COLORS[variant], width=2)
        label_offset = {
            "adaptive": (8, 17),
            "linearized_dp": (8, -8),
            "goo_dp": (8, -8),
        }[variant]
        for x, y, size in points:
            svg.circle(x, y, 5, COLORS[variant])
            svg.text(x + label_offset[0], y + label_offset[1], size, "tick", "start")
        legend_x = left + index * 250
        svg.circle(legend_x, top + 18, 5, COLORS[variant])
        label = {
            "adaptive": "Adaptive",
            "linearized_dp": "Linearized DP",
            "goo_dp": "GOO/DP",
        }[variant]
        svg.text(legend_x + 12, top + 23, label, "legend", "start")
    destination.write_text(svg.finish())


def figure_relation_set(rows, destination: Path):
    title = "RelationSet cost at the 64-relation representation boundary"
    subtitle = "Median union/subset/disjoint workload; old allocating assignment vs in-place |="
    svg = Svg(title, subtitle)
    chart_header(svg, title, subtitle)
    groups = defaultdict(list)
    for row in rows:
        if row["suite"] == "relation_set" and row["shape"] == "mixed_set_ops":
            groups[(row["variant"], row["relations"])].append(
                row["duration_ns"] / row["operations"]
            )
    sizes = sorted({size for _, size in groups})
    medians = {
        key: statistics.median(values)
        for key, values in groups.items()
    }
    left, right = MARGIN["left"], WIDTH - MARGIN["right"]
    top, bottom = MARGIN["top"], HEIGHT - MARGIN["bottom"]
    y_max = max(10, math.ceil(max(medians.values()) / 10) * 10)
    svg.line(left, top, left, bottom, stroke="#5f6368", width=1.2)
    svg.line(left, bottom, right, bottom, stroke="#5f6368", width=1.2)
    for tick in range(0, y_max + 1, 10):
        y = linear_scale(tick, 0, y_max, bottom, top)
        svg.line(left, y, right, y, stroke="#dadce0")
        svg.text(left - 12, y + 5, tick, anchor="end")
    x_positions = {}
    for index, size in enumerate(sizes):
        x = linear_scale(index, 0, len(sizes) - 1, left, right)
        x_positions[size] = x
        svg.text(x, bottom + 25, size)
    variants = [
        ("borrowed_union", "Borrowed union"),
        ("allocating_assign", "Allocating assignment (old)"),
        ("in_place_assign", "In-place assignment"),
    ]
    for variant_index, (variant, label) in enumerate(variants):
        points = [
            (
                x_positions[size],
                linear_scale(medians[(variant, size)], 0, y_max, bottom, top),
            )
            for size in sizes
        ]
        svg.polyline(points, COLORS[variant], width=2.5)
        for x, y in points:
            svg.circle(x, y, 5, COLORS[variant])
        legend_x = left + variant_index * 285
        svg.line(legend_x, top + 18, legend_x + 28, top + 18, COLORS[variant], width=3)
        svg.text(legend_x + 38, top + 23, label, "legend", "start")
    boundary_x = (x_positions[64] + x_positions[65]) / 2
    svg.line(boundary_x, top, boundary_x, bottom, stroke="#D55E00", width=2, dash="7 5")
    svg.text(boundary_x + 8, top + 48, "Inline64 -> dynamic", "annotation", "start")
    svg.text((left + right) / 2, HEIGHT - 32, "Relation count", "label")
    svg.text(28, (top + bottom) / 2, "Nanoseconds per workload iteration", "label", rotate=-90)
    destination.write_text(svg.finish())


def figure_topologies(rows, destination: Path):
    title = "Adaptive runtime is highly topology-sensitive"
    subtitle = "One deterministic query per topology; log-log axes"
    svg = Svg(title, subtitle)
    chart_header(svg, title, subtitle)
    grouped = defaultdict(list)
    for row in rows:
        if row["suite"] == "adaptive_topologies":
            grouped[(row["shape"], row["relations"])].append(row["duration_ms"])
    all_sizes = sorted({size for _, size in grouped})
    left, right, top, bottom = draw_log_axes(
        svg, [10, 20, 40, 70, 100, 128, 192], 0.1, 10_000, "Relations", "Adaptive optimization time"
    )
    for index, shape in enumerate(["chain", "star", "clique"]):
        points = []
        for (name, size), values in sorted(grouped.items(), key=lambda item: item[0][1]):
            if name != shape:
                continue
            x = log_scale(size, min(all_sizes), max(all_sizes), left, right)
            y = log_scale(statistics.median(values), 0.1, 10_000, bottom, top)
            points.append((x, y))
        svg.polyline(points, COLORS[shape])
        for x, y in points:
            svg.circle(x, y, 5, COLORS[shape])
        legend_x = left + index * 220
        svg.line(legend_x, top + 18, legend_x + 28, top + 18, COLORS[shape], width=3)
        svg.text(legend_x + 38, top + 23, shape.title(), "legend", "start")
    destination.write_text(svg.finish())


def write_tables(rows, aggregate, output_dir: Path):
    adaptive_rows = []
    for (relations, variant), value in sorted(aggregate.items()):
        if variant != "adaptive":
            continue
        mix = "; ".join(f"{name}={count}" for name, count in sorted(value["selection"].items()))
        adaptive_rows.append({
            "relations": relations,
            "algorithm_mix": mix,
            "min_ms": value["min"],
            "p10_ms": value["p10"],
            "median_ms": value["median"],
            "p90_ms": value["p90"],
            "max_ms": value["max"],
            "median_candidate_operators": value["median_candidates"],
        })
    with (output_dir / "adaptive_summary.csv").open("w", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=adaptive_rows[0].keys())
        writer.writeheader()
        writer.writerows(adaptive_rows)

    all_rows = []
    for (relations, variant), value in sorted(aggregate.items()):
        all_rows.append({
            "relations": relations,
            "variant": variant,
            "samples": value["count"],
            "min_ms": value["min"],
            "p10_ms": value["p10"],
            "median_ms": value["median"],
            "p90_ms": value["p90"],
            "p95_ms": value["p95"],
            "max_ms": value["max"],
            "median_candidate_operators": value["median_candidates"],
        })
    with (output_dir / "algorithm_summary.csv").open("w", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=all_rows[0].keys())
        writer.writeheader()
        writer.writerows(all_rows)

    lines = [
        "# Adaptive random-tree timing summary",
        "",
        "Times are milliseconds for one complete `JoinOrdering::run` invocation.",
        "",
        "| Relations | Selected algorithms (of 10) | Min | P10 | Median | P90 | Max | Median candidates |",
        "|---:|:---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in adaptive_rows:
        lines.append(
            f"| {row['relations']} | {row['algorithm_mix']} | {row['min_ms']:.3f} | "
            f"{row['p10_ms']:.3f} | {row['median_ms']:.3f} | {row['p90_ms']:.3f} | "
            f"{row['max_ms']:.3f} | {row['median_candidate_operators']:.0f} |"
        )
    (output_dir / "adaptive_summary.md").write_text("\n".join(lines) + "\n")

    relation_groups = defaultdict(list)
    for row in rows:
        if row["suite"] == "relation_set":
            relation_groups[(row["shape"], row["variant"], row["relations"])].append(
                row["duration_ns"] / row["operations"]
            )
    relation_rows = []
    for (shape, variant, relations), values in sorted(relation_groups.items()):
        values_summary = summary(values)
        relation_rows.append({
            "shape": shape,
            "variant": variant,
            "relations": relations,
            "samples": len(values),
            "min_ns": values_summary["min"],
            "p10_ns": values_summary["p10"],
            "median_ns": values_summary["median"],
            "p90_ns": values_summary["p90"],
            "max_ns": values_summary["max"],
        })
    with (output_dir / "relation_set_summary.csv").open("w", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=relation_rows[0].keys())
        writer.writeheader()
        writer.writerows(relation_rows)

    relation_lines = [
        "# RelationSet timing summary",
        "",
        "Times are nanoseconds per workload iteration; rows show selected representation boundaries.",
        "",
        "| Workload | Variant | Relations | Median | P10 | P90 |",
        "|:---|:---|---:|---:|---:|---:|",
    ]
    for row in relation_rows:
        if row["relations"] not in {64, 65, 256, 1_024}:
            continue
        relation_lines.append(
            f"| {row['shape']} | {row['variant']} | {row['relations']} | "
            f"{row['median_ns']:.1f} | {row['p10_ns']:.1f} | {row['p90_ns']:.1f} |"
        )
    (output_dir / "relation_set_summary.md").write_text(
        "\n".join(relation_lines) + "\n"
    )


def command_output(command):
    try:
        return subprocess.check_output(command, text=True, stderr=subprocess.DEVNULL).strip()
    except (OSError, subprocess.CalledProcessError):
        return "unavailable"


def write_readme(rows, aggregate, output_dir: Path):
    adaptive_10 = aggregate[(10, "adaptive")]["median"]
    adaptive_100 = aggregate[(100, "adaptive")]["median"]
    adaptive_256 = aggregate[(256, "adaptive")]["median"]
    linear_256 = aggregate[(256, "linearized_dp")]["median"]
    speedup = adaptive_256 / linear_256
    relset = defaultdict(list)
    for row in rows:
        if row["suite"] == "relation_set":
            relset[(row["shape"], row["variant"], row["relations"])].append(
                row["duration_ns"] / row["operations"]
            )
    median_relset = {key: statistics.median(values) for key, values in relset.items()}
    boundary = (
        median_relset[("mixed_set_ops", "borrowed_union", 65)]
        / median_relset[("mixed_set_ops", "borrowed_union", 64)]
    )
    assign_speedup = (
        median_relset[("mixed_set_ops", "allocating_assign", 256)]
        / median_relset[("mixed_set_ops", "in_place_assign", 256)]
    )
    build_speedup = (
        median_relset[("set_build", "incremental_with", 1_024)]
        / median_relset[("set_build", "from_iter", 1_024)]
    )
    generated_at = datetime.now(timezone.utc).date().isoformat()
    query_count = len({
        row["query_id"]
        for row in rows
        if row["suite"] == "random_tree_algorithms"
        and row["variant"] == "adaptive"
        and row["relations"] == 10
    })
    cpu = command_output(["sysctl", "-n", "machdep.cpu.brand_string"])
    if cpu == "unavailable":
        cpu = platform.processor() or platform.machine()

    text = f"""# Adaptive Join Ordering Performance Artifacts

Generated on {generated_at} from commit `{command_output(['git', 'rev-parse', 'HEAD'])}`.

## Headline results

- Adaptive median planning time grows from **{adaptive_10:.3f} ms at 10 relations** to
  **{adaptive_100:.3f} ms at 100** and **{adaptive_256 / 1_000:.3f} s at 256**.
- At 256 relations, forced linearized DP takes **{linear_256:.3f} ms**, or **{speedup:.1f}x less
  time** than the current adaptive choice (`GooDp`) on these sparse random trees.
- The policy chooses DPhyp for every 10-relation query, is mixed at 20 relations, uses linearized
  DP from 30 through 100, and switches to GOO/DP at 128 relations.
- Crossing from the inline 64-bit `RelationSet` representation to the dynamic representation
  increases the mixed set-operation microbenchmark by **{boundary:.1f}x** at 64 -> 65 relations.
- At 256 relations, in-place `|=` is **{assign_speedup:.1f}x faster** than the previous
  allocate-and-replace formulation.
- At 1,024 relations, one-pass `FromIterator` is **{build_speedup:.1f}x faster** than repeated
  singleton insertion and union.

## Relationship to Neumann and Radke (SIGMOD 2018)

Reference: [Adaptive Optimization of Very Large Join Queries](https://db.in.tum.de/~radke/papers/hugejoins.pdf).

The experiment mirrors the paper's median optimization-time plots and appendix distributions:
deterministic random tree join graphs, increasing relation counts, multiple algorithms, and
min/quantile/median/max summaries. The paper uses 100 queries per size and its `Cout` cost model;
this local run uses {query_count} queries per size and a constant-time enumeration cost so it
isolates search and data-structure overhead.

The comparison is directional, not a hardware-normalized reproduction. The paper's adaptive
system uses GOO/linearized-DP for very large joins and reports roughly 10-70 ms around 100
relations, about 500 ms around 700 relations, and less than 20 seconds for 5,000 relations. optd's
current large-query path is GOO with bounded exact DPhyp repair (`GooDp`), not GOO/linearized-DP.
The measured {adaptive_256 / 1_000:.1f}-second median at only 256 relations identifies the
large-query path as the main remaining scalability gap.

## Methodology

- Hardware/platform: {cpu}; `{platform.platform()}`.
- Toolchain: `{command_output(['rustc', '--version'])}`.
- Workload: 10 deterministic random recursive trees per size; one timed pass per query.
- Timed region: `JoinOrdering::run`, excluding query construction, cloning, and CSV output.
- Sizes: 10, 20, 30, 40, 70, 100, 128, 192, and 256 relations.
- Forced DPhyp is limited to 10-18 relations to avoid unbounded exponential runs.
- No timeout samples or extrapolated values are included.

Reproduce from the repository root:

```bash
cargo bench -p optd-core --bench join_ordering_scalability -- \\
  "$PWD/artifacts/join_ordering_scalability/raw_measurements.csv" 10 1
python3 optd/core/benches/render_join_ordering_scalability.py \\
  artifacts/join_ordering_scalability/raw_measurements.csv \\
  artifacts/join_ordering_scalability
```

## Files

- `raw_measurements.csv`: every measurement.
- `adaptive_summary.csv` / `.md`: paper-style adaptive distribution table.
- `algorithm_summary.csv`: all algorithm distributions.
- `relation_set_summary.csv` / `.md`: dynamic-set operation and construction distributions.
- `figure_1_algorithm_scaling.*`: paper-style optimization-time curves.
- `figure_2_adaptive_policy.*`: policy choices and transition costs.
- `figure_3_work_scaling.*`: elapsed time versus materialized candidates.
- `figure_4_relation_set_boundary.*`: inline/dynamic representation boundary.
- `figure_5_topology_sensitivity.*`: chain, star, and clique behavior.
- `manifest.json`: SHA-256 inventory.

## Interpretation cautions

- These are optimizer-kernel timings, not SQL parsing, execution, or end-to-end query latency.
- A constant-time cost model makes algorithmic/data-structure effects visible but understates the
  production cardinality-costing overhead.
- One timing per random graph gives a workload distribution, as in the paper, rather than repeated
  microbenchmark confidence intervals for an identical graph.
- Candidate counts are appended IR operators, not the number of pair-connectivity checks. GOO's
  pair search therefore consumes much more time than its materialized-candidate count suggests.
"""
    (output_dir / "README.md").write_text(text)


def render_pngs(output_dir: Path):
    renderer = shutil.which("rsvg-convert")
    if not renderer:
        return
    for svg in sorted(output_dir.glob("figure_*.svg")):
        subprocess.run(
            [renderer, "-w", "1800", "-o", str(svg.with_suffix(".png")), str(svg)],
            check=True,
        )


def write_manifest(output_dir: Path):
    files = []
    for path in sorted(output_dir.iterdir()):
        if path.name == "manifest.json" or not path.is_file():
            continue
        files.append({
            "file": path.name,
            "bytes": path.stat().st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        })
    metadata = {
        "generated_at": datetime.now(timezone.utc).date().isoformat(),
        "platform": platform.platform(),
        "files": files,
    }
    (output_dir / "manifest.json").write_text(json.dumps(metadata, indent=2) + "\n")


def main():
    if len(sys.argv) != 3:
        raise SystemExit("usage: render_join_ordering_scalability.py RAW.csv OUTPUT_DIR")
    source = Path(sys.argv[1]).resolve()
    output_dir = Path(sys.argv[2]).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    rows = load_rows(source)
    aggregate = aggregate_random(rows)
    figure_algorithm_scaling(aggregate, output_dir / "figure_1_algorithm_scaling.svg")
    figure_adaptive_policy(rows, aggregate, output_dir / "figure_2_adaptive_policy.svg")
    figure_work_scaling(aggregate, output_dir / "figure_3_work_scaling.svg")
    figure_relation_set(rows, output_dir / "figure_4_relation_set_boundary.svg")
    figure_topologies(rows, output_dir / "figure_5_topology_sensitivity.svg")
    write_tables(rows, aggregate, output_dir)
    write_readme(rows, aggregate, output_dir)
    render_pngs(output_dir)
    write_manifest(output_dir)


if __name__ == "__main__":
    main()
