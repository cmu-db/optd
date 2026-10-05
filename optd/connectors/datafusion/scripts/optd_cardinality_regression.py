#!/usr/bin/env python3
"""Run the optd cardinality collector one query per child process with checkpoints."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

SUITE_QUERY_PATHS = {
    "tpch": Path("optd/connectors/datafusion/tests/slt/tpch/results"),
    "job": Path("optd/connectors/datafusion/tests/slt/job/results"),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", choices=("all", *SUITE_QUERY_PATHS), default="all")
    parser.add_argument("--queries", type=Path, help="Single-suite query path override")
    parser.add_argument("--tpch-queries", type=Path, default=SUITE_QUERY_PATHS["tpch"])
    parser.add_argument("--job-queries", type=Path, default=SUITE_QUERY_PATHS["job"])
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument(
        "--binary",
        type=Path,
        default=Path("target/release/cardinality-regression"),
    )
    parser.add_argument("--limit", type=int, help="First N queries from each suite")
    parser.add_argument("--query-timeout", type=int, help="Per-query child timeout in seconds")
    parser.add_argument("--target-partitions", type=int)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--continue-on-error", action="store_true")
    return parser.parse_args()


def natural_query_key(path: Path) -> tuple[int, str]:
    match = re.match(r"\D*(\d+)", path.stem)
    return (int(match.group(1)) if match else sys.maxsize, path.stem.lower())


def query_files(path: Path, suite: str) -> list[Path]:
    if not path.exists():
        raise ValueError(f"{suite} query path does not exist: {path}")
    if path.is_file():
        paths = [path]
    else:
        paths = sorted(
            (
                child
                for child in path.iterdir()
                if child.suffix.lower() in {".sql", ".slt"}
            ),
            key=natural_query_key,
        )
    if not paths:
        raise ValueError(f"{path} contains no .sql or .slt query files")
    return paths


def load_json_array(path: Path) -> list[dict[str, Any]]:
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"failed to read {path}: {error}") from error
    if not isinstance(value, list):
        raise ValueError(f"{path} must contain a JSON array")
    return value


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def main() -> None:
    args = parse_args()
    if args.limit is not None and args.limit <= 0:
        raise ValueError("--limit must be positive")
    if args.query_timeout is not None and args.query_timeout <= 0:
        raise ValueError("--query-timeout must be positive")
    if not args.binary.is_file():
        raise ValueError(f"collector binary does not exist: {args.binary}")

    suites = tuple(SUITE_QUERY_PATHS) if args.suite == "all" else (args.suite,)
    if args.queries is not None and len(suites) != 1:
        raise ValueError("--queries requires --suite tpch or --suite job")

    report_path = args.output / "report.json"
    measurements = load_json_array(report_path) if args.resume and report_path.exists() else []
    for row in measurements:
        row.setdefault("suite", "tpch")
    completed = {(str(row["suite"]), str(row["query"])) for row in measurements}
    errors: list[dict[str, str]] = []

    for suite in suites:
        path = args.queries or getattr(args, f"{suite}_queries")
        paths = query_files(path, suite)
        if args.limit is not None:
            paths = paths[: args.limit]
        for query_path in paths:
            key = (suite, query_path.stem)
            display_name = "/".join(key)
            if key in completed:
                print(f"skipping completed {display_name}", file=sys.stderr)
                continue

            checkpoint = args.output / "checkpoints" / suite / query_path.stem
            command = [
                str(args.binary),
                "--dataset",
                suite,
                "--queries",
                str(query_path),
                "--output",
                str(checkpoint),
            ]
            if args.target_partitions is not None:
                command.extend(["--target-partitions", str(args.target_partitions)])
            print(f"measuring {display_name}", file=sys.stderr)
            try:
                result = subprocess.run(
                    command,
                    check=False,
                    timeout=args.query_timeout,
                )
                if result.returncode != 0:
                    detail = f"collector exited with status {result.returncode}"
                    if result.returncode in {-9, 137}:
                        detail += " (likely killed for memory pressure)"
                    raise RuntimeError(detail)
                query_measurements = load_json_array(checkpoint / "report.json")
            except (OSError, subprocess.TimeoutExpired, RuntimeError, ValueError) as error:
                failure = {"suite": suite, "query": query_path.stem, "error": str(error)}
                errors.append(failure)
                write_json(args.output / "errors.json", errors)
                if args.continue_on_error:
                    print(f"failed {display_name}: {error}", file=sys.stderr)
                    continue
                raise RuntimeError(f"failed {display_name}: {error}") from error

            for row in query_measurements:
                row["suite"] = suite
            measurements.extend(query_measurements)
            completed.add(key)
            write_json(report_path, measurements)

    write_json(report_path, measurements)
    errors_path = args.output / "errors.json"
    if errors:
        write_json(errors_path, errors)
        print(f"wrote {errors_path}", file=sys.stderr)
    else:
        errors_path.unlink(missing_ok=True)
    print(f"wrote {report_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
