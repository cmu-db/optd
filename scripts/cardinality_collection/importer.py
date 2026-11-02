"""Read-only, idempotent importer for cardinality-regression report directories."""
from __future__ import annotations

import json
import math
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .database import optd_dirty_source_fingerprint

COMMIT_RE = re.compile(r"^[0-9a-fA-F]{7,}$")


def _json(path: Path) -> Any:
    try:
        with path.open(encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"cannot read {path}: {error}") from error


def _report_locations(run_dir: Path, legacy: bool) -> dict[str, Path]:
    base = run_dir if legacy else run_dir / "data"
    return {engine: base / engine / "report.json" for engine in ("optd", "postgres") if (base / engine / "report.json").is_file()}


def _run_identity(commit: str | None, created_at: str) -> str:
    parsed = datetime.fromisoformat(created_at.replace("Z", "+00:00")).astimezone(timezone.utc)
    short = commit.lower()[:7] if commit and COMMIT_RE.fullmatch(commit) else "unknown"
    return f"{short}-{parsed:%Y%m%d-%H%M%SZ}"


def _finite_number(value: Any, field: str) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(float(value)):
        raise ValueError(f"{field} must be a finite number")
    return float(value)


def _parent_path(path: str) -> str | None:
    return path.rsplit(".", 1)[0] if "." in path else None


def _get_id(connection: sqlite3.Connection, table: str, **where: Any) -> int:
    if table == "suites":
        row = connection.execute("SELECT id FROM suites WHERE name=?", (where["name"],)).fetchone()
    elif table == "queries":
        row = connection.execute(
            "SELECT id FROM queries WHERE suite_id=? AND name=?",
            (where["suite_id"], where["name"]),
        ).fetchone()
    else:
        raise ValueError(f"unsupported ID lookup table: {table}")
    if row is None:
        raise RuntimeError(f"missing {table} row after insert")
    return int(row[0])


def _last_row_id(cursor: sqlite3.Cursor) -> int:
    if cursor.lastrowid is None:
        raise RuntimeError("SQLite insert did not return a row id")
    return int(cursor.lastrowid)


def _errors(run_dir: Path, legacy: bool) -> str | None:
    base = run_dir if legacy else run_dir / "data"
    collected: dict[str, Any] = {}
    for engine in ("optd", "postgres"):
        path = base / engine / "errors.json"
        if path.is_file():
            try:
                collected[engine] = _json(path)
            except json.JSONDecodeError:
                collected[engine] = {"unreadable": str(path)}
    return json.dumps(collected, sort_keys=True) if collected else None


def import_run(connection: sqlite3.Connection, run_directory: str | Path, *, repo_root: str | Path | None = None) -> str:
    """Import one current or legacy run directory atomically; re-import replaces its rows."""
    run_dir = Path(run_directory).resolve()
    metadata_path = run_dir / "data" / "run-metadata.json"
    legacy = not metadata_path.exists() and (run_dir / "optd" / "report.json").is_file()
    reports = _report_locations(run_dir, legacy)
    if "optd" not in reports:
        raise ValueError(f"no optd report under {run_dir}")
    metadata = _json(metadata_path) if metadata_path.is_file() else {}
    if not isinstance(metadata, dict):
        raise ValueError(f"{metadata_path} must contain an object")
    raw_commit = metadata.get("git_commit")
    commit = raw_commit if isinstance(raw_commit, str) and COMMIT_RE.fullmatch(raw_commit) else None
    raw_created_at = metadata.get("created_at")
    if isinstance(raw_created_at, str):
        try:
            created_at = datetime.fromisoformat(raw_created_at.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
        except ValueError as error:
            raise ValueError(f"invalid created_at in {metadata_path}") from error
    else:
        created_at = datetime.fromtimestamp(reports["optd"].stat().st_mtime, timezone.utc).isoformat()
    run_id = _run_identity(commit, created_at)
    limitation = None if commit else "Commit is unknown: metadata did not provide a proven git commit; timestamp may come from report filesystem metadata."
    root = Path(repo_root) if repo_root else Path.cwd()
    fingerprint = optd_dirty_source_fingerprint(root)
    build_id = f"{(commit or 'unknown').lower()[:7]}-{fingerprint[:16]}"

    parsed_reports: dict[str, list[dict[str, Any]]] = {}
    for engine, report in reports.items():
        content = _json(report)
        if not isinstance(content, list):
            raise ValueError(f"{report} must contain a JSON array")
        if not all(isinstance(row, dict) for row in content):
            raise ValueError(f"{report} contains a non-object row")
        parsed_reports[engine] = content

    with connection:
        connection.execute("INSERT OR IGNORE INTO builds(id, git_commit, source_fingerprint, created_at) VALUES (?, ?, ?, ?)", (build_id, commit, fingerprint, created_at))
        connection.execute("DELETE FROM runs WHERE id = ?", (run_id,))
        connection.execute("INSERT INTO runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))", (run_id, build_id, created_at, commit, str(run_dir), int(legacy), limitation, _errors(run_dir, legacy)))
        ordinal = 0
        for engine, rows in parsed_reports.items():
            for row in rows:
                suite_name = str(row.get("suite") or "unspecified")
                query_name = str(row.get("query") or "unknown")
                connection.execute("INSERT OR IGNORE INTO suites(name) VALUES (?)", (suite_name,))
                suite_id = _get_id(connection, "suites", name=suite_name)
                connection.execute("INSERT OR IGNORE INTO queries(suite_id, name) VALUES (?, ?)", (suite_id, query_name))
                query_id = _get_id(connection, "queries", suite_id=suite_id, name=query_name)
                # A collector has one plan per query/engine in a run; duplicate rows belong to it.
                existing = connection.execute("SELECT id FROM executions WHERE run_id=? AND query_id=? AND engine=?", (run_id, query_id, engine)).fetchone()
                if existing is None:
                    cursor = connection.execute("INSERT INTO executions(run_id, query_id, engine, ordinal) VALUES (?, ?, ?, ?)", (run_id, query_id, engine, ordinal))
                    execution_id = _last_row_id(cursor)
                    kind = "optimizer_relational" if engine == "optd" else "physical"
                    plan_id = _last_row_id(connection.execute("INSERT INTO plans(execution_id, kind) VALUES (?, ?)", (execution_id, kind)))
                    ordinal += 1
                else:
                    execution_id = int(existing[0])
                    plan_id = int(connection.execute("SELECT id FROM plans WHERE execution_id=?", (execution_id,)).fetchone()[0])
                path = str(row.get("node_path") or "0")
                operator = str(row.get("operator") or "unknown")
                join_count = int(row.get("join_count", 0))
                estimated = _finite_number(row.get("estimated_rows"), "estimated_rows")
                actual = _finite_number(row.get("actual_rows"), "actual_rows")
                q_error = _finite_number(row.get("q_error"), "q_error")
                loops_value = row.get("actual_loops")
                loops = _finite_number(loops_value, "actual_loops") if loops_value is not None else None
                connection.execute("INSERT INTO plan_nodes(plan_id,node_path,parent_node_path,operator,postgres_node_type,join_count,estimated_rows,actual_rows,actual_loops,q_error,raw_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (plan_id, path, _parent_path(path), operator, row.get("postgres_node_type"), join_count, estimated, actual, loops, q_error, json.dumps(row, sort_keys=True)))
    return run_id
