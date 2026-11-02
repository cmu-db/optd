"""SQLite storage and deterministic source identity helpers for cardinality collection."""
from __future__ import annotations

import hashlib
import os
import sqlite3
import subprocess
from pathlib import Path

SCHEMA_VERSION = 1


def connect(path: str | Path) -> sqlite3.Connection:
    database_path = Path(path)
    database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA journal_mode = WAL")
    connection.execute("PRAGMA busy_timeout = 5000")
    initialize(connection)
    return connection


def initialize(connection: sqlite3.Connection) -> None:
    """Apply the append-only MVP migrations."""
    connection.execute("CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY)")
    applied = {row[0] for row in connection.execute("SELECT version FROM schema_migrations")}
    if 1 not in applied:
        connection.executescript(
            """
            CREATE TABLE builds (
                id TEXT PRIMARY KEY,
                git_commit TEXT,
                source_fingerprint TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE runs (
                id TEXT PRIMARY KEY,
                build_id TEXT REFERENCES builds(id),
                created_at TEXT NOT NULL,
                git_commit TEXT,
                source_path TEXT NOT NULL,
                legacy INTEGER NOT NULL,
                metadata_limitation TEXT,
                errors_json TEXT,
                imported_at TEXT NOT NULL
            );
            CREATE TABLE suites (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE);
            CREATE TABLE queries (
                id INTEGER PRIMARY KEY,
                suite_id INTEGER NOT NULL REFERENCES suites(id),
                name TEXT NOT NULL,
                UNIQUE(suite_id, name)
            );
            CREATE TABLE executions (
                id INTEGER PRIMARY KEY,
                run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
                query_id INTEGER NOT NULL REFERENCES queries(id),
                engine TEXT NOT NULL,
                ordinal INTEGER NOT NULL,
                UNIQUE(run_id, query_id, engine)
            );
            CREATE TABLE plans (
                id INTEGER PRIMARY KEY,
                execution_id INTEGER NOT NULL UNIQUE REFERENCES executions(id) ON DELETE CASCADE,
                kind TEXT NOT NULL
            );
            CREATE TABLE plan_nodes (
                id INTEGER PRIMARY KEY,
                plan_id INTEGER NOT NULL REFERENCES plans(id) ON DELETE CASCADE,
                node_path TEXT NOT NULL,
                parent_node_path TEXT,
                operator TEXT NOT NULL,
                postgres_node_type TEXT,
                join_count INTEGER NOT NULL,
                estimated_rows REAL NOT NULL,
                actual_rows REAL NOT NULL,
                actual_loops REAL,
                q_error REAL NOT NULL,
                raw_json TEXT NOT NULL,
                UNIQUE(plan_id, node_path)
            );
            CREATE INDEX plan_nodes_plan_path ON plan_nodes(plan_id, node_path);
            CREATE INDEX executions_run_engine ON executions(run_id, engine);
            """
        )
        connection.execute("INSERT INTO schema_migrations(version) VALUES (1)")
    connection.commit()


def optd_dirty_source_fingerprint(repo_root: str | Path) -> str:
    """Hash optd source changes deterministically, including untracked source files.

    This deliberately uses bytes rather than timestamps so identical dirty trees yield the
    same value on different machines.  It is provenance only, not a build reproducibility claim.
    """
    root = Path(repo_root).resolve()
    paths = ("optd", "Cargo.toml", "Cargo.lock")
    digest = hashlib.sha256()
    try:
        diff = subprocess.run(
            ["git", "-C", str(root), "diff", "--binary", "HEAD", "--", *paths],
            check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        ).stdout
        digest.update(diff)
        status = subprocess.run(
            ["git", "-C", str(root), "ls-files", "--others", "--exclude-standard", "-z", "--", *paths],
            check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        ).stdout
        for name in sorted(item for item in status.split(b"\0") if item):
            digest.update(name + b"\0")
            digest.update((root / os.fsdecode(name)).read_bytes())
    except (OSError, subprocess.CalledProcessError):
        # A stable marker is more useful than failing an import outside a git checkout.
        digest.update(b"unavailable-git-source")
    return digest.hexdigest()
