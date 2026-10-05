#!/usr/bin/env python3
"""Collect chosen-plan PostgreSQL cardinality q-errors from benchmark queries."""

from __future__ import annotations

import argparse
import json
import math
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

JOIN_NODE_TYPES = frozenset({"Hash Join", "Merge Join", "Nested Loop"})
SUBPLAN_RELATIONSHIPS = frozenset({"InitPlan", "SubPlan"})
SCAN_NODE_TYPES = frozenset(
    {
        "Seq Scan",
        "Index Scan",
        "Index Only Scan",
        "Bitmap Heap Scan",
        "Tid Scan",
        "Tid Range Scan",
        "CTE Scan",
        "Named Tuplestore Scan",
        "WorkTable Scan",
        "Foreign Scan",
        "Custom Scan",
        "Sample Scan",
        "Values Scan",
    }
)
WRAPPER_NODE_TYPES = frozenset(
    {
        "Bitmap Index Scan",
        "Gather",
        "Gather Merge",
        "Hash",
        "LockRows",
        "Materialize",
        "Memoize",
    }
)
AGGREGATION_NODE_TYPES = frozenset(
    {"Aggregate", "Group", "GroupAggregate", "HashAggregate", "SetOp", "Unique", "WindowAgg"}
)
SET_OPERATION_NODE_TYPES = frozenset({"Append", "Merge Append", "Recursive Union"})


@dataclass(frozen=True)
class QuerySpec:
    suite: str
    name: str
    sql: str


SUITE_QUERY_PATHS = {
    "tpch": Path("optd/connectors/datafusion/tests/slt/tpch/results"),
    "job": Path("optd/connectors/datafusion/tests/slt/job/results"),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--suite",
        choices=("all", *SUITE_QUERY_PATHS),
        default="all",
        help="Benchmark suite to measure (default: both TPC-H and JOB)",
    )
    parser.add_argument(
        "--queries",
        type=Path,
        help="Override the query path when exactly one suite is selected",
    )
    parser.add_argument(
        "--tpch-queries",
        type=Path,
        default=SUITE_QUERY_PATHS["tpch"],
        help="TPC-H .sql/.slt file or query directory",
    )
    parser.add_argument(
        "--job-queries",
        type=Path,
        default=SUITE_QUERY_PATHS["job"],
        help="JOB .sql/.slt file or query directory",
    )
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        help="Output directory for report.json",
    )
    parser.add_argument(
        "--container",
        default="optd-postgres-18",
        help="Docker container running PostgreSQL",
    )
    parser.add_argument("--database", default="optd_bench")
    parser.add_argument("--user", default="optd")
    parser.add_argument(
        "--statement-timeout",
        type=int,
        default=300,
        help="Per-query timeout in seconds",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Measure only the first N queries from each selected suite",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Keep completed queries from an existing output report",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Record query failures and continue measuring the suite",
    )
    return parser.parse_args()


def first_slt_query(text: str) -> str | None:
    in_query = False
    lines = []
    for line in text.splitlines():
        if in_query and line.strip() == "----":
            break
        if in_query:
            lines.append(line)
        elif line.startswith("query ") or line == "query":
            in_query = True
    if not in_query:
        return None
    return "\n".join(lines).strip().removesuffix(";")


def natural_query_key(path: Path) -> tuple[int, str]:
    match = re.match(r"\D*(\d+)", path.stem)
    return (int(match.group(1)) if match else sys.maxsize, path.stem.lower())


def load_query_specs(path: Path, suite: str) -> list[QuerySpec]:
    if not path.exists():
        raise ValueError(f"{suite} query path does not exist: {path}")
    paths = [path] if path.is_file() else sorted(
        (
            child
            for child in path.iterdir()
            if child.suffix.lower() in {".sql", ".slt"}
        ),
        key=natural_query_key,
    )
    queries = []
    for query_path in paths:
        text = query_path.read_text()
        sql = first_slt_query(text) if query_path.suffix.lower() == ".slt" else text.strip().removesuffix(";")
        if not sql:
            raise ValueError(f"{query_path} contains no non-empty query")
        queries.append(QuerySpec(suite, query_path.stem, sql))
    if not queries:
        raise ValueError(f"{path} contains no .sql or .slt query files")
    return queries


def run_explain(
    query: QuerySpec,
    container: str,
    database: str,
    user: str,
    statement_timeout: int,
) -> dict[str, Any]:
    sql = f"""
BEGIN READ ONLY;
SET LOCAL statement_timeout = '{statement_timeout}s';
SET LOCAL max_parallel_workers_per_gather = 0;
SET LOCAL jit = off;
EXPLAIN (ANALYZE, VERBOSE, COSTS TRUE, TIMING FALSE, SUMMARY FALSE, FORMAT JSON)
{query.sql.rstrip().removesuffix(';')};
ROLLBACK;
"""
    command = [
        "docker",
        "exec",
        "-i",
        container,
        "psql",
        "-X",
        "-q",
        "-A",
        "-t",
        "-v",
        "ON_ERROR_STOP=1",
        "-U",
        user,
        "-d",
        database,
    ]
    result = subprocess.run(
        command,
        input=sql,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(f"PostgreSQL failed for {query.name}: {detail}")
    try:
        explain = json.loads(result.stdout.strip())
        return explain[0]["Plan"]
    except (json.JSONDecodeError, IndexError, KeyError, TypeError) as error:
        raise RuntimeError(
            f"invalid EXPLAIN JSON for {query.name}: {result.stdout.strip()}"
        ) from error


def operator_name(node: dict[str, Any]) -> str:
    node_type = str(node["Node Type"])
    if node_type in JOIN_NODE_TYPES:
        return "join"
    if node_type == "Function Scan":
        return "table_function"
    if node_type in SCAN_NODE_TYPES:
        return "scan"
    if node_type in AGGREGATION_NODE_TYPES:
        return "aggregation"
    if node_type == "Limit":
        return "limit"
    if node_type in {"Sort", "Incremental Sort"}:
        return "sort"
    if node_type == "ProjectSet":
        return "table_function"
    if node_type == "Result":
        return "map" if node.get("Plans") else "const_scan"
    if node_type in SET_OPERATION_NODE_TYPES:
        return "set_operation"
    if node_type in WRAPPER_NODE_TYPES or node_type == "Subquery Scan":
        return "output"
    return re.sub(r"[^a-z0-9]+", "_", node_type.lower()).strip("_")


def child_contributes_to_join_count(child: dict[str, Any]) -> bool:
    return child.get("Parent Relationship") not in SUBPLAN_RELATIONSHIPS


def subtree_join_count(node: dict[str, Any]) -> int:
    children = node.get("Plans", [])
    return int(str(node["Node Type"]) in JOIN_NODE_TYPES) + sum(
        subtree_join_count(child)
        for child in children
        if child_contributes_to_join_count(child)
    )


def row_q_error(estimated_rows: float, actual_rows: float) -> float:
    if not math.isfinite(estimated_rows) or estimated_rows < 0:
        raise ValueError(f"invalid PostgreSQL row estimate {estimated_rows}")
    if not math.isfinite(actual_rows) or actual_rows < 0:
        raise ValueError(f"invalid PostgreSQL actual row count {actual_rows}")
    estimated = max(estimated_rows, 1.0)
    actual = max(actual_rows, 1.0)
    return max(estimated, actual) / min(estimated, actual)


def collect_measurements(
    suite: str,
    query_name: str,
    node: dict[str, Any],
    path: str = "0",
) -> list[dict[str, Any]]:
    estimated_rows = float(node["Plan Rows"])
    actual_rows = float(node["Actual Rows"])
    measurement = {
        "engine": "postgres",
        "suite": suite,
        "query": query_name,
        "node_path": path,
        "operator": operator_name(node),
        "postgres_node_type": str(node["Node Type"]),
        "join_count": subtree_join_count(node),
        "estimated_rows": estimated_rows,
        "actual_rows": actual_rows,
        "actual_loops": float(node["Actual Loops"]),
        "q_error": row_q_error(estimated_rows, actual_rows),
    }
    measurements = [measurement]
    for index, child in enumerate(node.get("Plans", [])):
        measurements.extend(
            collect_measurements(suite, query_name, child, f"{path}.{index}")
        )
    return measurements


def write_report(measurements: list[dict[str, Any]], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    report = output_dir / "report.json"
    report.write_text(json.dumps(measurements, indent=2) + "\n")
    return report


def load_existing_report(output_dir: Path) -> list[dict[str, Any]]:
    report = output_dir / "report.json"
    if not report.exists():
        return []
    try:
        measurements = json.loads(report.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"failed to read {report}: {error}") from error
    if not isinstance(measurements, list):
        raise ValueError(f"{report} must contain a JSON array")
    return measurements


def write_errors(errors: list[dict[str, str]], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "errors.json"
    path.write_text(json.dumps(errors, indent=2) + "\n")
    return path


def main() -> None:
    args = parse_args()
    if args.statement_timeout <= 0:
        raise ValueError("--statement-timeout must be positive")
    if args.limit is not None and args.limit <= 0:
        raise ValueError("--limit must be positive")
    suites = tuple(SUITE_QUERY_PATHS) if args.suite == "all" else (args.suite,)
    if args.queries is not None and len(suites) != 1:
        raise ValueError("--queries requires --suite tpch or --suite job")
    queries = []
    for suite in suites:
        path = args.queries or getattr(args, f"{suite}_queries")
        suite_queries = load_query_specs(path, suite)
        queries.extend(suite_queries[: args.limit] if args.limit is not None else suite_queries)
    if not queries:
        raise ValueError("query selection contains no queries")

    measurements = load_existing_report(args.output) if args.resume else []
    if args.resume:
        legacy_rows = [row for row in measurements if not row.get("suite")]
        if legacy_rows and len(suites) != 1:
            raise ValueError(
                "cannot resume a multi-suite run from a legacy report without suite fields"
            )
        for row in legacy_rows:
            row["suite"] = suites[0]
    completed_queries = {
        (str(row["suite"]), str(row["query"])) for row in measurements
    }
    errors = []
    for query in queries:
        query_key = (query.suite, query.name)
        display_name = f"{query.suite}/{query.name}"
        if query_key in completed_queries:
            print(f"skipping completed {display_name}", file=sys.stderr)
            continue
        print(f"measuring {display_name}", file=sys.stderr)
        try:
            plan = run_explain(
                query,
                args.container,
                args.database,
                args.user,
                args.statement_timeout,
            )
        except RuntimeError as error:
            errors.append(
                {"suite": query.suite, "query": query.name, "error": str(error)}
            )
            error_report = write_errors(errors, args.output)
            if args.continue_on_error:
                print(str(error), file=sys.stderr)
                continue
            print(f"wrote {error_report}", file=sys.stderr)
            raise
        measurements.extend(
            collect_measurements(query.suite, query.name, plan)
        )
        write_report(measurements, args.output)

    report = write_report(measurements, args.output)
    print(f"wrote {report}", file=sys.stderr)
    if errors:
        error_report = write_errors(errors, args.output)
        print(f"wrote {error_report}", file=sys.stderr)
    else:
        (args.output / "errors.json").unlink(missing_ok=True)


if __name__ == "__main__":
    main()
