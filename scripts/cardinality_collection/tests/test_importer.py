from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from ..collector import dump_report
from ..database import connect, optd_dirty_source_fingerprint
from ..importer import import_run


class ImporterTests(unittest.TestCase):
    def test_import_is_idempotent_and_preserves_postgres_loop_semantics(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "data/optd").mkdir(parents=True)
            (root / "data/postgres").mkdir()
            (root / "data/run-metadata.json").write_text(json.dumps({"created_at": "2025-01-02T03:04:05Z", "git_commit": "abcdef012345"}))
            (root / "data/optd/report.json").write_text(json.dumps([{"suite": "tpc", "query": "q1", "node_path": "0", "operator": "Join", "join_count": 1, "estimated_rows": 4, "actual_rows": 2, "q_error": 2}]))
            (root / "data/postgres/report.json").write_text(json.dumps([{"suite": "tpc", "query": "q1", "node_path": "0.1", "operator": "Hash Join", "postgres_node_type": "Hash Join", "join_count": 1, "estimated_rows": 4, "actual_rows": 2, "actual_loops": 3, "q_error": 2}]))
            connection = connect(root / "db.sqlite")
            run_id = import_run(connection, root, repo_root=root)
            self.assertEqual(run_id, import_run(connection, root, repo_root=root))
            nodes = connection.execute("SELECT actual_rows, actual_loops, parent_node_path FROM plan_nodes WHERE postgres_node_type IS NOT NULL").fetchall()
            self.assertEqual(tuple(nodes[0]), (2.0, 3.0, "0"))
            self.assertEqual(connection.execute("SELECT count(*) FROM plan_nodes").fetchone()[0], 2)
            self.assertEqual({row[0] for row in connection.execute("SELECT kind FROM plans")}, {"optimizer_relational", "physical"})

    def test_collector_dump_replaces_one_engine_without_removing_the_other(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = root / "nested/cardinality.sqlite"
            report = root / "report.json"
            common = {"suite": "tpch", "query": "q1", "node_path": "0", "join_count": 0, "estimated_rows": 2, "actual_rows": 1, "q_error": 2}
            report.write_text(json.dumps([{**common, "operator": "scan"}]))
            arguments = {"run_id": "shared-run", "started_at": "2025-01-02T03:04:05Z", "repo_root": root, "source_path": root}
            dump_report(database, report, engine="optd", **arguments)
            dump_report(database, report, engine="postgres", **arguments)
            report.write_text(json.dumps([{**common, "operator": "selection", "q_error": 3}]))
            dump_report(database, report, engine="optd", **arguments)

            connection = connect(database)
            self.assertEqual(
                connection.execute("SELECT count(*) FROM executions WHERE run_id='shared-run'").fetchone()[0],
                2,
            )
            self.assertEqual(
                connection.execute("SELECT operator FROM plan_nodes n JOIN plans p ON p.id=n.plan_id JOIN executions e ON e.id=p.execution_id WHERE e.engine='optd'").fetchone()[0],
                "selection",
            )
            connection.close()

    def test_dirty_fingerprint_is_stable(self) -> None:
        root = Path(__file__).resolve().parents[3]
        self.assertEqual(optd_dirty_source_fingerprint(root), optd_dirty_source_fingerprint(root))


if __name__ == "__main__":
    unittest.main()
