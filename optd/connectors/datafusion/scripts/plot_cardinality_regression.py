#! /usr/bin/env python3
"""Plot cardinality-regression q-error distributions from report.json."""

from __future__ import annotations

import argparse
import csv
import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt  # type: ignore[import-not-found]
import pandas as pd  # type: ignore[import-not-found]
import seaborn as sns  # type: ignore[import-not-found]

NORMALIZATION_LEVELS = ("none", "wrappers", "row-preserving", "joins")
WRAPPER_OPERATORS = frozenset({"output", "sort"})
ROW_PRESERVING_OPERATORS = WRAPPER_OPERATORS | frozenset(
    {"projection", "rename", "map"}
)
JOIN_OPERATORS = frozenset({"join", "cross_product"})


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path, help="Path to cardinality-regression report.json")
    parser.add_argument(
        "--output",
        type=Path,
        help="Output directory (defaults to the report's directory)",
    )
    parser.add_argument(
        "--summary",
        action="store_true",
        help="Also write a summary-by-join-count CSV",
    )
    parser.add_argument(
        "--normalization",
        choices=(*NORMALIZATION_LEVELS, "all"),
        default="none",
        help=(
            "Operator normalization aggressiveness: none keeps every measurement; "
            "wrappers drops output/sort; row-preserving also drops projection/rename/map; "
            "joins keeps only join/cross-product measurements"
        ),
    )
    parser.add_argument(
        "--compare-report",
        type=Path,
        help=(
            "PostgreSQL report.json to compare with the primary optd report; writes a "
            "side-by-side CSV and prints a Markdown table"
        ),
    )
    parser.add_argument(
        "--suite",
        action="append",
        help="Include one suite (repeat for an aggregate); defaults to all report suites",
    )
    parser.add_argument(
        "--suite-matrix",
        action="store_true",
        help="Generate artifacts for every shared suite and for their aggregate",
    )
    parser.add_argument(
        "--artifact-suffix",
        help="Filename suffix for a non-matrix suite selection",
    )
    return parser.parse_args()


def includes_operator(operator: str, normalization: str) -> bool:
    if normalization == "none":
        return True
    if normalization == "wrappers":
        return operator not in WRAPPER_OPERATORS
    if normalization == "row-preserving":
        return operator not in ROW_PRESERVING_OPERATORS
    if normalization == "joins":
        return operator in JOIN_OPERATORS
    raise ValueError(f"unknown normalization level {normalization!r}")


def load_groups(
    report_path: Path,
    normalization: str = "none",
    suites: set[str] | None = None,
) -> tuple[list[int], list[list[float]]]:
    groups: dict[int, list[float]] = defaultdict(list)
    try:
        measurements = json.loads(report_path.read_text())
        if not isinstance(measurements, list):
            raise ValueError("expected a JSON array")
        for measurement in measurements:
            suite = str(measurement.get("suite") or "tpch").lower()
            if suites is not None and suite not in suites:
                continue
            operator = str(measurement["operator"])
            if not includes_operator(operator, normalization):
                continue
            join_count = int(measurement["join_count"])
            q_error = float(measurement["q_error"])
            if math.isfinite(q_error) and q_error >= 1.0:
                groups[join_count].append(q_error)
    except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError) as error:
        raise ValueError(f"failed to read {report_path}: {error}") from error

    if not groups:
        raise ValueError(
            f"{report_path} contains no finite q-error measurements "
            f"after {normalization!r} normalization and suite filtering"
        )

    join_counts = sorted(groups)
    return join_counts, [groups[join_count] for join_count in join_counts]


def percentile(values: list[float], fraction: float) -> float:
    position = fraction * (len(values) - 1)
    lower = math.floor(position)
    upper = min(lower + 1, len(values) - 1)
    weight = position - lower
    return values[lower] * (1.0 - weight) + values[upper] * weight


def group_statistics(group: list[float]) -> dict[str, float | int]:
    values = sorted(group)
    q1 = percentile(values, 0.25)
    q3 = percentile(values, 0.75)
    iqr = q3 - q1
    lower_fence = max(1.0, q1 - 1.5 * iqr)
    upper_fence = q3 + 1.5 * iqr
    return {
        "count": len(values),
        "min": values[0],
        "geometric_mean": math.exp(
            sum(math.log(value) for value in values) / len(values)
        ),
        "first_quartile": q1,
        "median": percentile(values, 0.5),
        "third_quartile": q3,
        "p95": percentile(values, 0.95),
        "max": values[-1],
        "lower_whisker": next(value for value in values if value >= lower_fence),
        "upper_whisker": next(
            value for value in reversed(values) if value <= upper_fence
        ),
        "q_error_at_least_10_percent": (
            100.0 * sum(value >= 10.0 for value in values) / len(values)
        ),
    }


def write_summary(
    path: Path, join_counts: list[int], groups: list[list[float]]
) -> None:
    fields = [
        "join_count",
        "count",
        "min",
        "geometric_mean",
        "first_quartile",
        "median",
        "third_quartile",
        "p95",
        "max",
        "lower_whisker",
        "upper_whisker",
        "q_error_at_least_10_percent",
    ]
    with path.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        for join_count, group in zip(join_counts, groups, strict=True):
            writer.writerow({"join_count": join_count, **group_statistics(group)})


def comparison_rows(
    normalization: str,
    ours_join_counts: list[int],
    ours_groups: list[list[float]],
    postgres_join_counts: list[int],
    postgres_groups: list[list[float]],
) -> list[dict[str, Any]]:
    ours = dict(zip(ours_join_counts, ours_groups, strict=True))
    postgres = dict(zip(postgres_join_counts, postgres_groups, strict=True))
    rows = []
    for join_count in sorted(ours.keys() | postgres.keys()):
        row: dict[str, Any] = {
            "normalization": normalization,
            "join_count": join_count,
        }
        for prefix, groups in (("ours", ours), ("postgres", postgres)):
            statistics = (
                group_statistics(groups[join_count]) if join_count in groups else None
            )
            for metric in (
                "count",
                "geometric_mean",
                "median",
                "p95",
                "q_error_at_least_10_percent",
            ):
                row[f"{prefix}_{metric}"] = (
                    statistics[metric] if statistics is not None else ""
                )
        rows.append(row)
    return rows


def write_comparison(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = [
        "normalization",
        "join_count",
        "ours_count",
        "ours_geometric_mean",
        "ours_median",
        "ours_p95",
        "ours_q_error_at_least_10_percent",
        "postgres_count",
        "postgres_geometric_mean",
        "postgres_median",
        "postgres_p95",
        "postgres_q_error_at_least_10_percent",
    ]
    with path.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def comparison_markdown(rows: list[dict[str, Any]]) -> str:
    def display(value: Any) -> str:
        if value == "":
            return "—"
        if isinstance(value, float):
            return f"{value:.3g}"
        return str(value)

    headings = (
        "Normalization",
        "Joins",
        "Ours n",
        "Ours median",
        "Ours p95",
        "Ours ≥10%",
        "Postgres n",
        "Postgres median",
        "Postgres p95",
        "Postgres ≥10%",
    )
    lines = [
        "| " + " | ".join(headings) + " |",
        "|" + "|".join("---" for _ in headings) + "|",
    ]
    for row in rows:
        values = (
            row["normalization"],
            row["join_count"],
            row["ours_count"],
            row["ours_median"],
            row["ours_p95"],
            row["ours_q_error_at_least_10_percent"],
            row["postgres_count"],
            row["postgres_median"],
            row["postgres_p95"],
            row["postgres_q_error_at_least_10_percent"],
        )
        lines.append("| " + " | ".join(display(value) for value in values) + " |")
    return "\n".join(lines)


def plot_frame(join_counts: list[int], groups: list[list[float]]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"join_count": join_count, "q_error": q_error}
            for join_count, values in zip(join_counts, groups, strict=True)
            for q_error in values
        ]
    )


def configure_axes(
    ax: Any, title: str, normalization: str, suite_label: str = ""
) -> None:
    if suite_label:
        title = f"{title} — {suite_label}"
    if normalization != "none":
        title = f"{title} [{normalization} normalization]"
    ax.set_title(title)
    ax.set_xlabel("Number of joins")
    ax.set_ylabel("Q-error (log scale)")
    ax.set_yscale("log")
    ax.grid(axis="y", which="both", alpha=0.25)


def comparison_plot_frame(
    ours_join_counts: list[int],
    ours_groups: list[list[float]],
    postgres_join_counts: list[int],
    postgres_groups: list[list[float]],
) -> pd.DataFrame:
    rows = []
    for engine, join_counts, groups in (
        ("optd", ours_join_counts, ours_groups),
        ("PostgreSQL", postgres_join_counts, postgres_groups),
    ):
        rows.extend(
            {"engine": engine, "join_count": join_count, "q_error": q_error}
            for join_count, values in zip(join_counts, groups, strict=True)
            for q_error in values
        )
    return pd.DataFrame(rows)


def write_comparison_box_plot(
    ours_join_counts: list[int],
    ours_groups: list[list[float]],
    postgres_join_counts: list[int],
    postgres_groups: list[list[float]],
    path: Path,
    normalization: str,
    suite_label: str = "",
) -> None:
    join_counts = sorted(set(ours_join_counts) | set(postgres_join_counts))
    figure, ax = plt.subplots(figsize=(max(8.0, len(join_counts) * 0.75), 5.5))
    sns.boxplot(
        data=comparison_plot_frame(
            ours_join_counts,
            ours_groups,
            postgres_join_counts,
            postgres_groups,
        ),
        x="join_count",
        y="q_error",
        hue="engine",
        hue_order=["optd", "PostgreSQL"],
        order=join_counts,
        showmeans=True,
        palette={"optd": "#8b5cf6", "PostgreSQL": "#f59e0b"},
        ax=ax,
    )
    configure_axes(
        ax,
        "optd vs. PostgreSQL q-error by number of joins",
        normalization,
        suite_label,
    )
    ax.legend(title="Engine")
    figure.tight_layout()
    figure.savefig(path)
    figure.savefig(path.with_suffix(".png"), dpi=180)
    plt.close(figure)


def write_box_plot(
    join_counts: list[int],
    groups: list[list[float]],
    path: Path,
    normalization: str = "none",
    suite_label: str = "",
) -> None:
    figure, ax = plt.subplots(figsize=(max(8.0, len(join_counts) * 0.55), 5.5))
    sns.boxplot(
        data=plot_frame(join_counts, groups),
        x="join_count",
        y="q_error",
        order=join_counts,
        showmeans=True,
        ax=ax,
    )
    configure_axes(
        ax, "Q-error by number of joins (box plot)", normalization, suite_label
    )
    figure.tight_layout()
    figure.savefig(path)
    plt.close(figure)


def write_violin_plot(
    join_counts: list[int],
    groups: list[list[float]],
    path: Path,
    normalization: str = "none",
    suite_label: str = "",
) -> None:
    frame = plot_frame(join_counts, groups)
    figure, ax = plt.subplots(figsize=(max(8.0, len(join_counts) * 0.55), 5.5))
    sns.violinplot(
        data=frame,
        x="join_count",
        y="q_error",
        order=join_counts,
        inner="quart",
        cut=0,
        color="#8b5cf6",
        ax=ax,
    )
    sns.stripplot(
        data=frame,
        x="join_count",
        y="q_error",
        order=join_counts,
        color="#4c1d95",
        size=2.5,
        alpha=0.45,
        ax=ax,
    )
    configure_axes(
        ax, "Q-error by number of joins (violin plot)", normalization, suite_label
    )
    figure.tight_layout()
    figure.savefig(path)
    plt.close(figure)


def report_suites(report_path: Path) -> set[str]:
    try:
        measurements = json.loads(report_path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"failed to read {report_path}: {error}") from error
    if not isinstance(measurements, list):
        raise ValueError(f"{report_path} must contain a JSON array")
    return {str(row.get("suite") or "tpch").lower() for row in measurements}


def artifact_selections(args: argparse.Namespace) -> list[tuple[set[str] | None, str]]:
    requested = {suite.lower() for suite in (args.suite or [])}
    if args.suite_matrix and requested:
        raise ValueError("--suite and --suite-matrix cannot be combined")
    if args.suite_matrix:
        suites = report_suites(args.report)
        if args.compare_report is not None:
            suites &= report_suites(args.compare_report)
        if not suites:
            raise ValueError("reports have no shared benchmark suites")
        ordered = sorted(suites)
        selections: list[tuple[set[str] | None, str]] = [
            ({suite}, suite) for suite in ordered
        ]
        if len(ordered) > 1:
            selections.append((set(ordered), "all"))
        return selections
    suffix = args.artifact_suffix or ("+".join(sorted(requested)) if requested else "")
    return [(requested or None, suffix)]


def main() -> None:
    args = parse_args()
    output_dir = args.output or args.report.parent
    output_dir.mkdir(parents=True, exist_ok=True)
    normalizations = (
        NORMALIZATION_LEVELS
        if args.normalization == "all"
        else (args.normalization,)
    )

    suite_labels = {"tpch": "TPC-H", "job": "JOB", "tpcds": "TPC-DS"}
    for suites, artifact_suffix in artifact_selections(args):
        comparison = []
        suite_order = {"tpch": 0, "job": 1, "tpcds": 2}
        ordered_suites = sorted(
            suites or [],
            key=lambda suite: (suite_order.get(suite, len(suite_order)), suite),
        )
        suite_label = " + ".join(
            suite_labels.get(suite, suite.upper()) for suite in ordered_suites
        )
        selection_suffix = f"-{artifact_suffix}" if artifact_suffix else ""
        for normalization in normalizations:
            join_counts, groups = load_groups(args.report, normalization, suites)
            include_normalization = normalization != "none" or args.normalization == "all"
            normalization_suffix = f"-{normalization}" if include_normalization else ""
            suffix = f"{normalization_suffix}{selection_suffix}"
            box_plot = output_dir / f"qerror-boxplot{suffix}.svg"
            violin_plot = output_dir / f"qerror-violin{suffix}.svg"
            write_box_plot(
                join_counts, groups, box_plot, normalization, suite_label
            )
            write_violin_plot(
                join_counts, groups, violin_plot, normalization, suite_label
            )
            print(f"wrote {box_plot}")
            print(f"wrote {violin_plot}")
            if args.summary:
                summary = output_dir / f"summary-by-join-count{suffix}.csv"
                write_summary(summary, join_counts, groups)
                print(f"wrote {summary}")

            if args.compare_report is not None:
                postgres_join_counts, postgres_groups = load_groups(
                    args.compare_report, normalization, suites
                )
                comparison_box_plot = (
                    output_dir / f"qerror-comparison-boxplot{suffix}.svg"
                )
                write_comparison_box_plot(
                    join_counts,
                    groups,
                    postgres_join_counts,
                    postgres_groups,
                    comparison_box_plot,
                    normalization,
                    suite_label,
                )
                print(f"wrote {comparison_box_plot}")
                comparison.extend(
                    comparison_rows(
                        normalization,
                        join_counts,
                        groups,
                        postgres_join_counts,
                        postgres_groups,
                    )
                )

        if args.compare_report is not None:
            comparison_level = "all" if args.normalization == "all" else args.normalization
            comparison_path = output_dir / (
                f"comparison-by-join-count-{comparison_level}{selection_suffix}.csv"
            )
            write_comparison(comparison_path, comparison)
            print(f"wrote {comparison_path}")
            print(comparison_markdown(comparison))


if __name__ == "__main__":
    main()
