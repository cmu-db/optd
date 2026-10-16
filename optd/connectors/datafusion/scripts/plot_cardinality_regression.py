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
        help="Also write summary-by-join-count.csv",
    )
    return parser.parse_args()


def load_groups(report_path: Path) -> tuple[list[int], list[list[float]]]:
    groups: dict[int, list[float]] = defaultdict(list)
    try:
        measurements = json.loads(report_path.read_text())
        if not isinstance(measurements, list):
            raise ValueError("expected a JSON array")
        for measurement in measurements:
            join_count = int(measurement["join_count"])
            q_error = float(measurement["q_error"])
            if math.isfinite(q_error) and q_error >= 1.0:
                groups[join_count].append(q_error)
    except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError) as error:
        raise ValueError(f"failed to read {report_path}: {error}") from error

    if not groups:
        raise ValueError(f"{report_path} contains no finite q-error measurements")

    join_counts = sorted(groups)
    return join_counts, [groups[join_count] for join_count in join_counts]


def percentile(values: list[float], fraction: float) -> float:
    position = fraction * (len(values) - 1)
    lower = math.floor(position)
    upper = min(lower + 1, len(values) - 1)
    weight = position - lower
    return values[lower] * (1.0 - weight) + values[upper] * weight


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
        "max",
        "lower_whisker",
        "upper_whisker",
    ]
    with path.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        for join_count, group in zip(join_counts, groups, strict=True):
            values = sorted(group)
            q1 = percentile(values, 0.25)
            q3 = percentile(values, 0.75)
            iqr = q3 - q1
            lower_fence = max(1.0, q1 - 1.5 * iqr)
            upper_fence = q3 + 1.5 * iqr
            writer.writerow(
                {
                    "join_count": join_count,
                    "count": len(values),
                    "min": values[0],
                    "geometric_mean": math.exp(
                        sum(math.log(value) for value in values) / len(values)
                    ),
                    "first_quartile": q1,
                    "median": percentile(values, 0.5),
                    "third_quartile": q3,
                    "max": values[-1],
                    "lower_whisker": next(
                        value for value in values if value >= lower_fence
                    ),
                    "upper_whisker": next(
                        value for value in reversed(values) if value <= upper_fence
                    ),
                }
            )


def plot_frame(join_counts: list[int], groups: list[list[float]]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"join_count": join_count, "q_error": q_error}
            for join_count, values in zip(join_counts, groups, strict=True)
            for q_error in values
        ]
    )


def configure_axes(ax: Any, title: str) -> None:
    ax.set_title(title)
    ax.set_xlabel("Number of joins")
    ax.set_ylabel("Q-error (log scale)")
    ax.set_yscale("log")
    ax.grid(axis="y", which="both", alpha=0.25)


def write_box_plot(
    join_counts: list[int], groups: list[list[float]], path: Path
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
    configure_axes(ax, "Q-error by number of joins (box plot)")
    figure.tight_layout()
    figure.savefig(path)
    plt.close(figure)


def write_violin_plot(
    join_counts: list[int], groups: list[list[float]], path: Path
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
    configure_axes(ax, "Q-error by number of joins (violin plot)")
    figure.tight_layout()
    figure.savefig(path)
    plt.close(figure)


def main() -> None:
    args = parse_args()
    join_counts, groups = load_groups(args.report)
    output_dir = args.output or args.report.parent
    output_dir.mkdir(parents=True, exist_ok=True)

    box_plot = output_dir / "qerror-boxplot.svg"
    violin_plot = output_dir / "qerror-violin.svg"
    write_box_plot(join_counts, groups, box_plot)
    write_violin_plot(join_counts, groups, violin_plot)
    print(f"wrote {box_plot}")
    print(f"wrote {violin_plot}")
    if args.summary:
        summary = output_dir / "summary-by-join-count.csv"
        write_summary(summary, join_counts, groups)
        print(f"wrote {summary}")


if __name__ == "__main__":
    main()
