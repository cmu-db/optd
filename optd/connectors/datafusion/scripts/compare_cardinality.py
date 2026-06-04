#!/usr/bin/env python3
"""Compare optd cardinality estimates with EXPLAIN ANALYZE metrics.

The script is intentionally lightweight and dataset-agnostic. It expects an
optd-cli setup flag such as --tpch or --job, extracts SQL from SLT result files
or plain .sql files, then writes one TSV row per query.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import statistics
import subprocess
import sys
import time
from pathlib import Path


FIELDS = [
    "dataset",
    "query",
    "status",
    "optimizer_seconds",
    "analyze_seconds",
    "optd_root_estimate",
    "optd_max_estimate",
    "optd_max_join_estimate",
    "actual_root_rows",
    "actual_max_rows",
    "metric_count",
    "optd_scan_count",
    "physical_source_count",
    "optd_join_count",
    "physical_join_count",
    "optd_aggregate_count",
    "physical_aggregate_count",
    "optd_operator_counts",
    "physical_operator_counts",
    "error",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", required=True, help="Dataset label, e.g. tpch or job.")
    parser.add_argument(
        "--cli",
        default="target/release/optd-cli",
        help="Path to optd-cli. Defaults to target/release/optd-cli.",
    )
    parser.add_argument(
        "--queries",
        required=True,
        type=Path,
        help="Directory of .slt/.sql files, or a single query file.",
    )
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        help="TSV output path.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=15.0,
        help="Default EXPLAIN ANALYZE timeout in seconds.",
    )
    parser.add_argument(
        "--optimizer-timeout",
        type=float,
        default=60.0,
        help="Timeout for explain_optimizer_json in seconds.",
    )
    parser.add_argument(
        "--setup-flag",
        action="append",
        default=[],
        help="Flag passed to optd-cli to register data, e.g. --tpch. May repeat.",
    )
    parser.add_argument(
        "--timeout-override",
        action="append",
        default=[],
        metavar="REGEX=SECONDS",
        help="Per-query timeout override. Example: '^16=8'. May repeat.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Run only the first N queries after sorting.",
    )
    parser.add_argument(
        "--keep-going",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Continue after per-query errors. Enabled by default.",
    )
    return parser.parse_args()


def query_files(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    files = list(path.glob("*.slt")) + list(path.glob("*.sql"))
    return sorted(files, key=query_sort_key)


def query_sort_key(path: Path) -> tuple[int, str]:
    match = re.match(r"q?(\d+)", path.stem)
    if match:
        return (int(match.group(1)), path.stem)
    return (10**9, path.stem)


def extract_sql(path: Path) -> str:
    text = path.read_text()
    if path.suffix == ".sql":
        return text.strip().rstrip(";")

    lines = text.splitlines()
    in_query = False
    parts: list[str] = []
    for line in lines:
        if not in_query:
            if line.startswith("query"):
                in_query = True
            continue
        if line.strip() == "----":
            break
        if line.startswith("#") or not line.strip():
            continue
        parts.append(line)

    sql = "\n".join(parts).strip().rstrip(";")
    if not sql:
        raise ValueError(f"no SLT query block found in {path}")
    return sql


def sql_string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def timeout_for(query: str, default: float, overrides: list[tuple[re.Pattern[str], float]]) -> float:
    for pattern, seconds in overrides:
        if pattern.search(query):
            return seconds
    return default


def parse_timeout_overrides(values: list[str]) -> list[tuple[re.Pattern[str], float]]:
    overrides = []
    for value in values:
        if "=" not in value:
            raise ValueError(f"timeout override must be REGEX=SECONDS: {value}")
        pattern, seconds = value.rsplit("=", 1)
        overrides.append((re.compile(pattern), float(seconds)))
    return overrides


def run_cli(cli: str, setup_flags: list[str], sql: str, timeout: float) -> tuple[float, str]:
    command = [
        cli,
        *setup_flags,
        "-c",
        "SET optd.log_explain_steps = false; " + sql,
    ]
    started = time.time()
    process = subprocess.run(
        command,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    elapsed = time.time() - started
    if process.returncode != 0:
        raise RuntimeError((process.stderr or process.stdout).strip())
    return elapsed, process.stdout


def table_second_column(stdout: str) -> str:
    lines: list[str] = []
    for line in stdout.splitlines():
        if not line.startswith("|"):
            continue
        parts = [part.strip() for part in line.strip("|").split("|")]
        if len(parts) < 2 or parts[1] == "plan":
            continue
        lines.append(parts[1])
    return "\n".join(lines)


def table_first_cell(stdout: str) -> str:
    lines: list[str] = []
    for line in stdout.splitlines():
        if not line.startswith("|"):
            continue
        content = line[1:-1]
        if content.startswith(" "):
            content = content[1:]
        lines.append(content.rstrip())
    return "\n".join(lines)


def parse_optimizer_json(stdout: str) -> dict[str, object]:
    text = table_first_cell(stdout)
    start = text.find("{")
    end = text.rfind("}") + 1
    if start < 0 or end <= start:
        raise ValueError("explain_optimizer_json output did not contain JSON")
    payload = json.loads(text[start:end])
    root = payload["passes"][-1]["root"]

    counts: dict[str, int] = {}
    estimates: list[float] = []
    join_estimates: list[float] = []

    def walk(node: dict[str, object]) -> None:
        op = str(node.get("op", "?"))
        counts[op] = counts.get(op, 0) + 1
        value = node.get("estimated_rows")
        if value is not None:
            try:
                estimate = float(value)
                estimates.append(estimate)
                if op in {"join", "cross_product"}:
                    join_estimates.append(estimate)
            except ValueError:
                pass
        for child in node.get("children", []):
            walk(child)

    walk(root)
    return {
        "root_estimate": float(root["estimated_rows"]) if "estimated_rows" in root else None,
        "max_estimate": max(estimates) if estimates else None,
        "max_join_estimate": max(join_estimates) if join_estimates else None,
        "scan_count": counts.get("scan", 0),
        "join_count": counts.get("join", 0) + counts.get("cross_product", 0),
        "aggregate_count": counts.get("aggregation", 0),
        "operator_counts": counts,
    }


def parse_explain_analyze(stdout: str) -> dict[str, object]:
    text = table_second_column(stdout)
    output_rows = [int(value) for value in re.findall(r"output_rows=(\d+)", text)]
    counts: dict[str, int] = {}
    for raw in text.splitlines():
        match = re.match(r"(?:[│├└─\s]*)?([A-Za-z][A-Za-z0-9_]*Exec)\b", raw.strip())
        if not match:
            continue
        op = match.group(1)
        counts[op] = counts.get(op, 0) + 1

    return {
        "root_rows": output_rows[0] if output_rows else None,
        "max_rows": max(output_rows) if output_rows else None,
        "metric_count": len(output_rows),
        "source_count": sum(
            count
            for op, count in counts.items()
            if op in {"DataSourceExec", "CsvExec", "ParquetExec"} or "DataSource" in op
        ),
        "join_count": sum(count for op, count in counts.items() if "JoinExec" in op),
        "aggregate_count": sum(count for op, count in counts.items() if "AggregateExec" in op),
        "operator_counts": counts,
    }


def format_counts(counts: dict[str, int]) -> str:
    return ",".join(f"{key}:{value}" for key, value in sorted(counts.items()))


def empty_row(dataset: str, query: str) -> dict[str, object]:
    row = {field: "" for field in FIELDS}
    row["dataset"] = dataset
    row["query"] = query
    return row


def write_summary(rows: list[dict[str, object]]) -> None:
    statuses: dict[tuple[str, str], int] = {}
    for row in rows:
        key = (str(row["dataset"]), str(row["status"]))
        statuses[key] = statuses.get(key, 0) + 1
    print("status:")
    for (dataset, status), count in sorted(statuses.items()):
        print(f"  {dataset} {status}: {count}")

    completed = [row for row in rows if row["status"] == "ok"]
    slow = sorted(
        completed,
        key=lambda row: float(row["analyze_seconds"] or 0.0),
        reverse=True,
    )[:10]
    if slow:
        print("slowest:")
        for row in slow:
            print(f"  {row['dataset']} {row['query']}: {row['analyze_seconds']}s")

    mismatches = [
        row
        for row in completed
        if row["optd_scan_count"] != row["physical_source_count"]
        or row["optd_join_count"] != row["physical_join_count"]
    ]
    print(f"shape mismatches: {len(mismatches)}")

    ratios = []
    for row in completed:
        estimate = parse_float(row["optd_max_join_estimate"])
        actual = parse_float(row["actual_max_rows"])
        if estimate and actual and estimate > 0 and actual > 0:
            ratios.append((estimate / actual, row))
    if ratios:
        logs = sorted(math.log10(ratio) for ratio, _ in ratios if ratio > 0)
        print(
            "max join estimate / actual max rows: "
            f"median={10 ** statistics.median(logs):.3e}, "
            f"p90={10 ** percentile(logs, 0.9):.3e}, "
            f"max={10 ** max(logs):.3e}"
        )
        print("top estimate misses:")
        for ratio, row in sorted(ratios, reverse=True, key=lambda item: item[0])[:10]:
            print(f"  {row['dataset']} {row['query']}: {ratio:.3e}")


def parse_float(value: object) -> float | None:
    if value in ("", None):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def percentile(sorted_values: list[float], fraction: float) -> float:
    if not sorted_values:
        raise ValueError("percentile needs at least one value")
    index = min(len(sorted_values) - 1, math.ceil(fraction * len(sorted_values)) - 1)
    return sorted_values[index]


def main() -> int:
    args = parse_args()
    overrides = parse_timeout_overrides(args.timeout_override)
    files = query_files(args.queries)
    if args.limit is not None:
        files = files[: args.limit]

    rows: list[dict[str, object]] = []
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="") as output:
        writer = csv.DictWriter(output, delimiter="\t", fieldnames=FIELDS)
        writer.writeheader()
        for path in files:
            query = path.stem
            row = empty_row(args.dataset, query)
            try:
                sql = extract_sql(path)
                elapsed, stdout = run_cli(
                    args.cli,
                    args.setup_flag,
                    "SELECT explain_optimizer_json(" + sql_string_literal(sql) + ");",
                    args.optimizer_timeout,
                )
                row["optimizer_seconds"] = f"{elapsed:.3f}"
                optimizer = parse_optimizer_json(stdout)
                row["optd_root_estimate"] = optimizer["root_estimate"]
                row["optd_max_estimate"] = optimizer["max_estimate"]
                row["optd_max_join_estimate"] = optimizer["max_join_estimate"]
                row["optd_scan_count"] = optimizer["scan_count"]
                row["optd_join_count"] = optimizer["join_count"]
                row["optd_aggregate_count"] = optimizer["aggregate_count"]
                row["optd_operator_counts"] = format_counts(optimizer["operator_counts"])

                timeout = timeout_for(query, args.timeout, overrides)
                elapsed, stdout = run_cli(
                    args.cli,
                    args.setup_flag,
                    "EXPLAIN ANALYZE " + sql + ";",
                    timeout,
                )
                row["analyze_seconds"] = f"{elapsed:.3f}"
                analyze = parse_explain_analyze(stdout)
                row["actual_root_rows"] = analyze["root_rows"]
                row["actual_max_rows"] = analyze["max_rows"]
                row["metric_count"] = analyze["metric_count"]
                row["physical_source_count"] = analyze["source_count"]
                row["physical_join_count"] = analyze["join_count"]
                row["physical_aggregate_count"] = analyze["aggregate_count"]
                row["physical_operator_counts"] = format_counts(analyze["operator_counts"])
                row["status"] = "ok"
            except subprocess.TimeoutExpired:
                row["status"] = "timeout"
                row["error"] = "timeout"
            except Exception as error:  # noqa: BLE001 - CLI tool should record all failures.
                row["status"] = "error"
                row["error"] = str(error).replace("\n", " ")[:1000]
                if not args.keep_going:
                    writer.writerow(row)
                    rows.append(row)
                    raise

            writer.writerow(row)
            output.flush()
            rows.append(row)
            print(
                f"{row['dataset']} {row['query']} {row['status']} "
                f"opt={row['optimizer_seconds']} analyze={row['analyze_seconds']}",
                flush=True,
            )

    write_summary(rows)
    return 0


if __name__ == "__main__":
    sys.exit(main())
