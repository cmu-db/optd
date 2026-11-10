"""SQLite checkpoint writer shared by cardinality collectors."""
from __future__ import annotations

import json
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .database import connect, optd_dirty_source_fingerprint
from .importer import _finite_number, _get_id, _parent_path

COMMIT_RE = re.compile(r"^[0-9a-fA-F]{7,}$")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def repository_commit(repo_root: str | Path) -> str | None:
    try:
        value = subprocess.run(
            ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None
    return value if COMMIT_RE.fullmatch(value) else None


def collection_run_id(repo_root: str | Path, started_at: str) -> str:
    commit = repository_commit(repo_root)
    prefix = commit[:7].lower() if commit else "unknown"
    timestamp = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
    return f"{prefix}-{timestamp.astimezone(timezone.utc):%Y%m%d-%H%M%SZ}"


def _load_report(path: Path) -> list[dict[str, Any]]:
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"cannot read {path}: {error}") from error
    if not isinstance(value, list) or not all(isinstance(row, dict) for row in value):
        raise ValueError(f"{path} must contain a JSON array of objects")
    return value


def _last_row_id(cursor: Any) -> int:
    if cursor.lastrowid is None:
        raise RuntimeError("SQLite insert did not return a row id")
    return int(cursor.lastrowid)


def dump_report(
    database_path: str | Path,
    report_path: str | Path,
    *,
    engine: str,
    run_id: str,
    started_at: str,
    repo_root: str | Path,
    source_path: str | Path,
) -> None:
    """Replace one engine's measurements in a run with the current report checkpoint."""
    if engine not in {"optd", "postgres"}:
        raise ValueError(f"unsupported engine: {engine}")
    report = Path(report_path)
    rows = _load_report(report)
    root = Path(repo_root).resolve()
    commit = repository_commit(root)
    fingerprint = optd_dirty_source_fingerprint(root)
    build_id = f"{(commit or 'unknown')[:7].lower()}-{fingerprint[:16]}"
    connection = connect(database_path)
    try:
        with connection:
            connection.execute(
                "INSERT OR IGNORE INTO builds(id, git_commit, source_fingerprint, created_at) VALUES (?, ?, ?, ?)",
                (build_id, commit, fingerprint, started_at),
            )
            connection.execute(
                """INSERT INTO runs(id, build_id, created_at, git_commit, source_path, legacy, metadata_limitation, errors_json, imported_at)
                   VALUES (?, ?, ?, ?, ?, 0, ?, NULL, datetime('now'))
                   ON CONFLICT(id) DO UPDATE SET build_id=excluded.build_id, git_commit=excluded.git_commit,
                       source_path=excluded.source_path, imported_at=datetime('now')""",
                (
                    run_id,
                    build_id,
                    started_at,
                    commit,
                    str(Path(source_path).resolve()),
                    None if commit else "Commit is unknown: git metadata was unavailable during collection.",
                ),
            )
            connection.execute(
                "DELETE FROM executions WHERE run_id=? AND engine=?", (run_id, engine)
            )
            ordinal = 0
            execution_ids: dict[tuple[int, str], tuple[int, int]] = {}
            for row in rows:
                suite_name = str(row.get("suite") or "unspecified")
                query_name = str(row.get("query") or "unknown")
                connection.execute("INSERT OR IGNORE INTO suites(name) VALUES (?)", (suite_name,))
                suite_id = _get_id(connection, "suites", name=suite_name)
                connection.execute(
                    "INSERT OR IGNORE INTO queries(suite_id, name) VALUES (?, ?)",
                    (suite_id, query_name),
                )
                query_id = _get_id(connection, "queries", suite_id=suite_id, name=query_name)
                key = (query_id, query_name)
                if key not in execution_ids:
                    execution_id = _last_row_id(
                        connection.execute(
                            "INSERT INTO executions(run_id, query_id, engine, ordinal) VALUES (?, ?, ?, ?)",
                            (run_id, query_id, engine, ordinal),
                        )
                    )
                    kind = "optimizer_relational" if engine == "optd" else "physical"
                    plan_id = _last_row_id(
                        connection.execute(
                            "INSERT INTO plans(execution_id, kind) VALUES (?, ?)",
                            (execution_id, kind),
                        )
                    )
                    execution_ids[key] = (execution_id, plan_id)
                    ordinal += 1
                plan_id = execution_ids[key][1]
                path = str(row.get("node_path") or "0")
                loops_value = row.get("actual_loops")
                loops = _finite_number(loops_value, "actual_loops") if loops_value is not None else None
                connection.execute(
                    """INSERT INTO plan_nodes(
                           plan_id,node_path,parent_node_path,operator,postgres_node_type,join_count,
                           estimated_rows,actual_rows,actual_loops,q_error,raw_json
                       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        plan_id,
                        path,
                        _parent_path(path),
                        str(row.get("operator") or "unknown"),
                        row.get("postgres_node_type"),
                        int(row.get("join_count", 0)),
                        _finite_number(row.get("estimated_rows"), "estimated_rows"),
                        _finite_number(row.get("actual_rows"), "actual_rows"),
                        loops,
                        _finite_number(row.get("q_error"), "q_error"),
                        json.dumps(row, sort_keys=True),
                    ),
                )
    finally:
        connection.close()
