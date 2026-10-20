#!/usr/bin/env python3
"""Collect isolated TPC-H/JOB samples and emit github-action-benchmark custom JSON.

No publishing. Each query has a subprocess deadline; any failed query prevents a complete suite's
scalar export, but raw successes/failures and the selection manifest remain available for diagnosis.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import statistics
import subprocess
import sys
import time

from optd_cardinality_regression import query_files, write_json
from query_benchmark_models import (
    STATISTICS_POLICY, ActionMetric, CollectorOptions, DatasetFile, ProcessResult,
    ProcessState, QueryOutcome, QuerySelection, RunConfiguration, RunManifest, RunMetadata,
    Status, SuiteIdentity, SuiteReport, WorkerFailure, WorkerReport, WorkerSample, json_value,
)

ROOT = Path(__file__).resolve().parents[4]
QUERY_DIR = ROOT / "optd/connectors/datafusion/tests/slt"
DATA_DIR = ROOT / "optd/connectors/datafusion/data"


def positive(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> CollectorOptions:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", choices=("all", "tpch", "job"), default="all")
    parser.add_argument("--queries", type=Path, help="Single-suite file/directory override")
    parser.add_argument("--output", type=Path, required=True, help="New or empty output directory")
    parser.add_argument("--binary", type=Path, default=ROOT / "target/release/query-benchmark")
    parser.add_argument("--limit", type=positive, help="First N naturally sorted queries per suite")
    parser.add_argument("--samples", type=positive, default=3)
    parser.add_argument("--warmups", type=positive, default=1)
    parser.add_argument("--target-partitions", type=positive, default=1)
    parser.add_argument("--query-timeout", type=positive, default=900, help="Seconds including registration, statistics, warmups, and all samples")
    return CollectorOptions(**vars(parser.parse_args(argv)))


def digest(path: Path) -> str:
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def command_output(*command: str) -> str:
    return subprocess.check_output(command, cwd=ROOT, text=True).strip()


def metadata(binary: Path) -> RunMetadata:
    # These describe this collector checkout, NOT an attested build of an arbitrary --binary.
    return RunMetadata(
        collector_checkout_sha=command_output("git", "rev-parse", "HEAD"),
        collector_checkout_status=command_output("git", "status", "--porcelain"),
        binary_sha256=digest(binary),
        collector_cargo_lock_sha256=digest(ROOT / "Cargo.lock"),
        collector_rustc=command_output("rustc", "--version"),
        platform=sys.platform,
        machine=os.uname().machine,
        cpu_count=os.cpu_count(),
        runner_os=os.environ.get("RUNNER_OS"),
        runner_arch=os.environ.get("RUNNER_ARCH"),
        runner_image=os.environ.get("ImageVersion"),
        workflow_run_id=os.environ.get("GITHUB_RUN_ID"),
        workflow_run_attempt=os.environ.get("GITHUB_RUN_ATTEMPT"),
    )


def dataset_identity(suite: str) -> list[DatasetFile]:
    directory = DATA_DIR / ("tpch/sf-0.1" if suite == "tpch" else "job")
    # These full hashes can warm the OS cache; this is not a cold-cache experiment.
    return [DatasetFile(str(path.relative_to(directory)), path.stat().st_size, digest(path))
            for path in sorted(directory.rglob("*.parquet"))]


def validate_worker(report: WorkerReport, args: CollectorOptions, suite: str) -> list[WorkerSample]:
    expected_directory = DATA_DIR / ("tpch/sf-0.1" if suite == "tpch" else "job")
    if report.dataset != suite or report.dataset_directory.resolve() != expected_directory.resolve():
        raise ValueError("worker dataset path differs from the data hashed by this collector; rebuild in this worktree")
    if report.execution_path != "direct_physical" or report.correctness != "not_checked":
        raise ValueError("unexpected execution/correctness policy")
    if report.statistics != STATISTICS_POLICY:
        raise ValueError("unexpected statistics policy")
    if report.warmups_completed != args.warmups:
        raise ValueError("incomplete warmups")
    if len(report.samples) != args.samples:
        raise ValueError("incomplete measured samples")
    if len({sample.output_rows for sample in report.samples}) != 1:
        raise ValueError("inconsistent output row counts (not a semantic correctness check)")
    return report.samples


def scalar_metrics(suite: str, query: str, cohort: str, samples: list[WorkerSample]) -> list[ActionMetric]:
    metrics = []
    for phase in ("planning_ms", "execution_ms"):
        values = [getattr(sample, phase) for sample in samples]
        metrics.append(ActionMetric(
            name=f"v1/{suite}/{cohort}/{query}/direct/{phase}", unit="ms",
            value=statistics.median(values),
            range=f"min={min(values):.6g}, max={max(values):.6g}",
            extra=f"samples={len(values)}; warm cache; correctness not checked; binary build provenance unknown; planning includes statistics/import",
        ))
    return metrics


def run_child(command: list[str], log: Path, timeout: int) -> ProcessResult:
    with log.open("w") as output:
        # Terminate descendants too, not just a wrapper process.
        with subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT, start_new_session=True) as child:
            try:
                return ProcessResult(ProcessState.FINISHED, child.wait(timeout=timeout))
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL)
                child.wait()
                return ProcessResult(ProcessState.TIMEOUT, child.returncode)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.queries is not None and args.suite == "all":
        raise ValueError("--queries requires --suite tpch or job")
    binary = args.binary.resolve()
    if not binary.is_file():
        raise ValueError(f"build the query-benchmark binary first: {binary}")
    if args.output.exists() and any(args.output.iterdir()):
        raise ValueError("output directory must be new or empty; do not mix source revisions or stale exports")
    suites = ("tpch", "job") if args.suite == "all" else (args.suite,)
    selections: dict[str, list[Path]] = {}
    for suite in suites:
        paths = query_files(args.queries or QUERY_DIR / suite / "results", suite)
        paths = paths[:args.limit] if args.limit else paths
        if len({path.stem for path in paths}) != len(paths):
            raise ValueError(f"duplicate query names in {suite}")
        selections[suite] = paths
    args.output.mkdir(parents=True, exist_ok=True)
    manifest = RunManifest(
        started_unix=time.time(), metadata=metadata(binary),
        config=RunConfiguration(args.samples, args.warmups, args.target_partitions, args.query_timeout),
    )
    manifest_path = args.output / "manifest.json"
    write_json(manifest_path, json_value(manifest))
    failed = False
    for suite, paths in selections.items():
        selection = [QuerySelection(path.stem, str(path), digest(path)) for path in paths]
        identity = SuiteIdentity(
            queries=[item.identity() for item in selection], data=dataset_identity(suite),
            partitions=args.target_partitions, environment=manifest.metadata.comparison_environment(),
        )
        cohort = hashlib.sha256(json.dumps(json_value(identity), sort_keys=True).encode()).hexdigest()[:16]
        suite_report = SuiteReport(cohort=cohort, identity=identity, selection=selection)
        manifest.suites[suite] = suite_report
        write_json(manifest_path, json_value(manifest))
        metrics: list[ActionMetric] = []
        for path in paths:
            checkpoint = args.output / "raw" / suite / path.stem
            checkpoint.mkdir(parents=True)
            report_path = checkpoint / "report.json"
            command = [str(binary), "--dataset", suite, "--query", str(path.resolve()),
                       "--output", str(report_path.resolve()), "--samples", str(args.samples),
                       "--warmups", str(args.warmups), "--target-partitions", str(args.target_partitions)]
            print(f"measuring {suite}/{path.stem}", file=sys.stderr)
            record = QueryOutcome(query=path.stem, report=str(report_path.relative_to(args.output)))
            try:
                process = run_child(command, checkpoint / "worker.log", args.query_timeout)
                if process.state == ProcessState.TIMEOUT:
                    record.status = Status.TIMEOUT
                    record.error = f"exceeded {args.query_timeout}s; process group terminated"
                elif process.returncode is not None and process.returncode < 0:
                    record.status = Status.CRASH
                    record.error = f"worker exited {process.returncode}"
                elif process.returncode != 0:
                    raw = json.loads(report_path.read_text()) if report_path.exists() else {}
                    failure = WorkerFailure.from_json(raw, process.returncode)
                    record.status, record.error = failure.status, failure.error
                else:
                    report = WorkerReport.from_json(json.loads(report_path.read_text()))
                    samples = validate_worker(report, args, suite)
                    if not identity.data:
                        raise ValueError("no dataset files identified; refusing an unattributed successful result")
                    metrics.extend(scalar_metrics(suite, path.stem, cohort, samples))
                    record.status = Status.SUCCESS
            except (OSError, ValueError, TypeError, KeyError) as error:
                record.status, record.error = Status.ERROR, str(error)
            suite_report.queries.append(record)
            write_json(manifest_path, json_value(manifest))
        complete = all(query.status == Status.SUCCESS for query in suite_report.queries)
        suite_report.status = Status.SUCCESS if complete else Status.FAILED
        if complete:
            write_json(args.output / f"{suite}.json", json_value(metrics))
        else:
            failed = True
        write_json(manifest_path, json_value(manifest))
    manifest.status = Status.FAILED if failed else Status.SUCCESS
    manifest.finished_unix = time.time()
    write_json(manifest_path, json_value(manifest))
    return 1 if failed else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f"benchmark setup failed: {error}", file=sys.stderr)
        sys.exit(1)
