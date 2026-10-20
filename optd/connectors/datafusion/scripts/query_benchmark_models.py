"""Typed collector records and explicit validation at the Rust-worker JSON boundary.

Dataclasses provide structure, not automatic runtime validation. Only from_json methods accept
untrusted JSON; orchestration and aggregation operate on records after those checks.
"""
from __future__ import annotations

from dataclasses import dataclass, field, fields, is_dataclass
from enum import StrEnum
import math
from pathlib import Path
from typing import Any

STATISTICS_POLICY = "runtime_aggregate_cache_warmed_by_query_warmups"
TIMINGS = ("sql_planning_ms", "statistics_import_ms", "optimizer_ms",
           "physical_planning_ms", "planning_ms", "execution_ms")


class Status(StrEnum):
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    ERROR = "error"
    UNSUPPORTED = "unsupported"
    CRASH = "crash"
    TIMEOUT = "timeout"


class ProcessState(StrEnum):
    FINISHED = "finished"
    TIMEOUT = "timeout"


@dataclass(frozen=True)
class CollectorOptions:
    suite: str
    queries: Path | None
    output: Path
    binary: Path
    limit: int | None
    samples: int
    warmups: int
    target_partitions: int
    query_timeout: int


@dataclass(frozen=True)
class RunConfiguration:
    samples: int
    warmups: int
    target_partitions: int
    query_timeout_seconds: int
    statistics: str = STATISTICS_POLICY
    execution_path: str = "direct_physical"
    correctness: str = "not_checked"
    cache_policy: str = "warm"


@dataclass(frozen=True)
class RunMetadata:
    collector_checkout_sha: str
    collector_checkout_status: str
    binary_sha256: str
    collector_cargo_lock_sha256: str
    collector_rustc: str
    platform: str
    machine: str
    cpu_count: int | None
    binary_build_provenance: str = "unknown"
    runner_os: str | None = None
    runner_arch: str | None = None
    runner_image: str | None = None
    workflow_run_id: str | None = None
    workflow_run_attempt: str | None = None

    def comparison_environment(self) -> ComparisonEnvironment:
        return ComparisonEnvironment(self.collector_rustc, self.platform, self.machine,
                                     self.cpu_count, self.runner_image)


@dataclass(frozen=True)
class ComparisonEnvironment:
    collector_rustc: str
    platform: str
    machine: str
    cpu_count: int | None
    runner_image: str | None


@dataclass(frozen=True)
class DatasetFile:
    file: str
    bytes: int
    sha256: str


@dataclass(frozen=True)
class QueryIdentity:
    query: str
    sha256: str


@dataclass(frozen=True)
class QuerySelection:
    query: str
    file: str
    sha256: str

    def identity(self) -> QueryIdentity:
        return QueryIdentity(self.query, self.sha256)


@dataclass(frozen=True)
class SuiteIdentity:
    queries: list[QueryIdentity]
    data: list[DatasetFile]
    partitions: int
    environment: ComparisonEnvironment
    schema_version: int = 1
    statistics: str = STATISTICS_POLICY


@dataclass(frozen=True)
class WorkerSample:
    """Validated projection of timings/row count; full diagnostics stay in raw report.json."""
    sql_planning_ms: float
    statistics_import_ms: float
    optimizer_ms: float
    physical_planning_ms: float
    planning_ms: float
    execution_ms: float
    output_rows: int

    @classmethod
    def from_json(cls, value: object) -> WorkerSample:
        if not isinstance(value, dict):
            raise ValueError("invalid sample record")
        timings = {}
        for name in TIMINGS:
            number = value.get(name)
            if isinstance(number, bool) or not isinstance(number, (int, float)):
                raise ValueError(f"invalid {name}")
            try:
                parsed = float(number)
            except OverflowError as error:
                raise ValueError(f"invalid {name}") from error
            if not math.isfinite(parsed) or parsed < 0:
                raise ValueError(f"invalid {name}")
            timings[name] = parsed
        count = value.get("output_rows")
        if type(count) is not int or count < 0:
            raise ValueError("invalid output row count")
        return cls(**timings, output_rows=count)


@dataclass(frozen=True)
class WorkerReport:
    schema_version: int
    status: Status
    dataset: str
    dataset_directory: Path
    execution_path: str
    correctness: str
    statistics: str
    warmups_completed: int
    samples: list[WorkerSample]

    @classmethod
    def from_json(cls, value: object) -> WorkerReport:
        if not isinstance(value, dict) or type(value.get("schema_version")) is not int or value["schema_version"] != 1 or value.get("status") != Status.SUCCESS:
            raise ValueError("worker did not produce a successful version-1 report")
        for name in ("dataset", "dataset_directory", "execution_path", "correctness", "statistics"):
            if not isinstance(value.get(name), str) or not value[name]:
                raise ValueError(f"invalid worker {name}")
        warmups = value.get("warmups_completed")
        if type(warmups) is not int or warmups < 0:
            raise ValueError("invalid warmup count")
        samples = value.get("samples")
        if not isinstance(samples, list):
            raise ValueError("invalid worker samples")
        return cls(value["schema_version"], Status.SUCCESS, value["dataset"], Path(value["dataset_directory"]),
                   value["execution_path"], value["correctness"], value["statistics"], warmups,
                   [WorkerSample.from_json(sample) for sample in samples])


@dataclass(frozen=True)
class WorkerFailure:
    status: Status
    error: str

    @classmethod
    def from_json(cls, value: object, returncode: int | None) -> WorkerFailure:
        if not isinstance(value, dict):
            raise ValueError("invalid worker failure report")
        status = Status.UNSUPPORTED if value.get("status") == Status.UNSUPPORTED else Status.ERROR
        message = value.get("error")
        if message is not None and not isinstance(message, str):
            raise ValueError("invalid worker error message")
        return cls(status, message or f"worker exited {returncode}")


@dataclass(frozen=True)
class ProcessResult:
    state: ProcessState
    returncode: int | None


@dataclass(frozen=True)
class ActionMetric:
    name: str
    unit: str
    value: float
    range: str
    extra: str


@dataclass
class QueryOutcome:
    query: str
    report: str
    status: Status = Status.ERROR
    error: str | None = field(default=None, metadata={"omit_none": True})


@dataclass
class SuiteReport:
    cohort: str
    identity: SuiteIdentity
    selection: list[QuerySelection]
    queries: list[QueryOutcome] = field(default_factory=list)
    status: Status = Status.RUNNING


@dataclass
class RunManifest:
    started_unix: float
    metadata: RunMetadata
    config: RunConfiguration
    schema_version: int = 1
    status: Status = Status.RUNNING
    suites: dict[str, SuiteReport] = field(default_factory=dict)
    finished_unix: float | None = field(default=None, metadata={"omit_none": True})


def json_value(value: object) -> Any:
    """Explicit serialization boundary; preserve v1 keys, enum strings, and nullable metadata."""
    if is_dataclass(value) and not isinstance(value, type):
        return {item.name: json_value(getattr(value, item.name)) for item in fields(value)
                if not (item.metadata.get("omit_none") and getattr(value, item.name) is None)}
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {key: json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_value(item) for item in value]
    return value
