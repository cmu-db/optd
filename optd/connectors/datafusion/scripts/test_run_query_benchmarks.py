"""Local emitter tests; no real dataset, publishing, or production access."""
from dataclasses import FrozenInstanceError, replace
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import query_benchmark_models as models
import run_query_benchmarks as benchmark


def worker_report(values=(10.0, 20.0, 15.0)):
    """Rust-shaped JSON fixture; production dictionaries stop at the parsing boundary."""
    return {"schema_version": 1, "status": "success", "execution_path": "direct_physical",
            "dataset": "tpch", "dataset_directory": str(benchmark.DATA_DIR / "tpch/sf-0.1"),
            "correctness": "not_checked", "statistics": models.STATISTICS_POLICY,
            "warmups_completed": 1,
            "samples": [{**{field: value for field in models.TIMINGS}, "output_rows": 2} for value in values]}


def options(**overrides):
    return replace(benchmark.parse_args(["--output", "unused"]), **overrides)


def validate_report(value, args=None):
    return benchmark.validate_worker(models.WorkerReport.from_json(value), args or options(), "tpch")


class EmitterTests(unittest.TestCase):
    def test_validates_samples_and_rejects_partial_or_invalid_reports(self):
        samples = validate_report(worker_report())
        self.assertEqual(len(samples), 3)
        self.assertIsInstance(samples[0], models.WorkerSample)
        for field, value in [("samples", []), ("status", "error"), ("warmups_completed", 0),
                             ("execution_path", "logical"), ("correctness", "verified"),
                             ("schema_version", True), ("warmups_completed", True),
                             ("dataset_directory", None), ("samples", [None])]:
            report = worker_report()
            report[field] = value
            with self.assertRaises(ValueError):
                validate_report(report)
        for value in [float("nan"), float("inf"), 10**400, -1, True, "1"]:
            report = worker_report()
            report["samples"][0]["execution_ms"] = value
            with self.assertRaises(ValueError):
                validate_report(report)
        report = worker_report()
        report["samples"][0]["output_rows"] = 3
        with self.assertRaises(ValueError):
            validate_report(report)

    def test_rejects_cross_worktree_dataset_attribution(self):
        report = worker_report()
        report["dataset_directory"] = "/another/worktree/data/tpch/sf-0.1"
        with self.assertRaisesRegex(ValueError, "differs from the data hashed"):
            validate_report(report)

    def test_configuration_and_samples_are_immutable(self):
        config = options()
        self.assertIsInstance(config, models.CollectorOptions)
        with self.assertRaises(FrozenInstanceError):
            config.samples = 0
        with self.assertRaises(FrozenInstanceError):
            validate_report(worker_report())[0].execution_ms = -1

    def test_checkout_metadata_does_not_attest_binary_source(self):
        with tempfile.TemporaryDirectory() as temp:
            binary = Path(temp) / "custom-worker"
            binary.write_bytes(b"not an attested build")
            with patch.object(benchmark, "command_output", return_value="collector metadata"):
                meta = benchmark.metadata(binary)
            self.assertIsInstance(meta, models.RunMetadata)
            self.assertEqual(meta.binary_build_provenance, "unknown")
            serialized = models.json_value(meta)
            self.assertIn("collector_checkout_sha", serialized)
            self.assertNotIn("source_sha", serialized)
            self.assertNotIn("rustc", serialized)
            self.assertIsNone(serialized["runner_image"])

    def test_exports_true_median_ms_and_not_significance(self):
        samples = validate_report(worker_report((2, 4, 8, 10)), options(samples=4))
        metrics = benchmark.scalar_metrics("tpch", "q1", "cohort", samples)
        self.assertIsInstance(metrics[0], models.ActionMetric)
        self.assertEqual([metric.value for metric in metrics], [6, 6])
        self.assertEqual(metrics[0].unit, "ms")
        self.assertIn("correctness not checked", metrics[0].extra)
        self.assertEqual(metrics[0].name, "v1/tpch/cohort/q1/direct/planning_ms")
        serialized = models.json_value(metrics)
        self.assertEqual(set(serialized[0]), {"name", "unit", "value", "range", "extra"})
        json.dumps(serialized, allow_nan=False)

    def test_serialization_preserves_v1_cohort_and_optional_fields(self):
        environment = models.ComparisonEnvironment("rust", "linux", "aarch64", 4, None)
        identity = models.SuiteIdentity([models.QueryIdentity("q1", "abc")],
                                        [models.DatasetFile("data.parquet", 10, "def")], 1, environment)
        expected = {"queries": [{"query": "q1", "sha256": "abc"}],
                    "data": [{"file": "data.parquet", "bytes": 10, "sha256": "def"}],
                    "partitions": 1, "schema_version": 1, "statistics": models.STATISTICS_POLICY,
                    "environment": {"collector_rustc": "rust", "platform": "linux", "machine": "aarch64",
                                    "cpu_count": 4, "runner_image": None}}
        self.assertEqual(json.dumps(models.json_value(identity), sort_keys=True), json.dumps(expected, sort_keys=True))
        record = models.QueryOutcome("q1", "raw/report.json", status=models.Status.SUCCESS)
        self.assertEqual(models.json_value(record), {"query": "q1", "report": "raw/report.json", "status": "success"})
        record.error = "message"
        self.assertEqual(models.json_value(record)["error"], "message")
        first = models.SuiteReport("cohort", identity, [])
        second = models.SuiteReport("cohort", identity, [])
        first.queries.append(record)
        self.assertEqual(second.queries, [])

    def test_worker_failure_boundary_validates_messages(self):
        failure = models.WorkerFailure.from_json({"status": "unsupported", "error": "not implemented"}, 1)
        self.assertEqual(failure.status, models.Status.UNSUPPORTED)
        self.assertEqual(models.WorkerFailure.from_json({"status": "running"}, 2).status, models.Status.ERROR)
        with self.assertRaises(ValueError):
            models.WorkerFailure.from_json({"error": {"not": "text"}}, 1)

    def run_fake_collection(self, statuses):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        queries = root / "queries"
        queries.mkdir()
        for index in range(len(statuses)):
            (queries / f"q{index + 1}.sql").write_text("SELECT 1")
        binary = root / "query-benchmark"
        binary.touch()
        output = root / "output"
        records = iter(statuses)

        def fake_child(command, log, timeout):
            status = next(records)
            report_path = Path(command[command.index("--output") + 1])
            report = worker_report()
            if status == "success":
                report_path.write_text(json.dumps(report))
                return models.ProcessResult(models.ProcessState.FINISHED, 0)
            if status == "unsupported":
                report.update(status="unsupported", error="unsupported fixture")
                report_path.write_text(json.dumps(report))
                return models.ProcessResult(models.ProcessState.FINISHED, 1)
            report_path.write_text("{")  # partial checkpoint cannot hide a timeout or crash
            if status == "timeout":
                return models.ProcessResult(models.ProcessState.TIMEOUT, -9)
            return models.ProcessResult(models.ProcessState.FINISHED, -11)

        meta = models.RunMetadata("abc", "", "binary-hash", "lock-hash", "test", "test", "test", 1)
        with patch.object(benchmark, "run_child", side_effect=fake_child), \
             patch.object(benchmark, "metadata", return_value=meta), \
             patch.object(benchmark, "dataset_identity", return_value=[models.DatasetFile("data.parquet", 1, "abc")]):
            result = benchmark.main(["--suite", "tpch", "--queries", str(queries),
                                     "--binary", str(binary), "--output", str(output)])
        return result, output, json.loads((output / "manifest.json").read_text())

    def test_complete_suite_emits_action_json_and_raw_manifest(self):
        result, output, manifest = self.run_fake_collection(["success", "success"])
        self.assertEqual(result, 0)
        self.assertEqual(manifest["status"], "success")
        self.assertEqual(len(json.loads((output / "tpch.json").read_text())), 4)
        self.assertTrue((output / "raw/tpch/q1/report.json").is_file())

    def test_failure_retains_other_results_but_cannot_publish_partial_suite(self):
        result, output, manifest = self.run_fake_collection(["success", "unsupported", "timeout", "crash"])
        self.assertEqual(result, 1)
        self.assertFalse((output / "tpch.json").exists())
        self.assertEqual([query["status"] for query in manifest["suites"]["tpch"]["queries"]],
                         ["success", "unsupported", "timeout", "crash"])
        self.assertTrue((output / "raw/tpch/q1/report.json").exists())

    def test_refuses_to_overwrite_a_previous_run(self):
        _, output, _ = self.run_fake_collection(["success"])
        with self.assertRaisesRegex(ValueError, "new or empty"):
            benchmark.main(["--binary", str(output.parent / "query-benchmark"), "--output", str(output)])

    def test_process_results_cover_real_exit_and_timeout(self):
        with tempfile.TemporaryDirectory() as temp:
            log = Path(temp) / "worker.log"
            result = benchmark.run_child([sys.executable, "-c", "print('done')"], log, 5)
            self.assertEqual(result, models.ProcessResult(models.ProcessState.FINISHED, 0))
            self.assertIn("done", log.read_text())
            result = benchmark.run_child([sys.executable, "-c", "import time; time.sleep(30)"], log, 1)
            self.assertEqual(result.state, models.ProcessState.TIMEOUT)
            self.assertLess(result.returncode, 0)

    def test_natural_selection_and_duplicate_names(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            binary = root / "worker"
            binary.touch()
            for name in ["q10.sql", "q2.sql", "q1.sql"]:
                (root / name).write_text("SELECT 1")
            self.assertEqual([path.name for path in benchmark.query_files(root, "tpch")], ["q1.sql", "q2.sql", "q10.sql"])
            (root / "q1.slt").write_text("query I\nSELECT 1\n----\n1\n")
            with self.assertRaisesRegex(ValueError, "duplicate"):
                benchmark.main(["--suite", "tpch", "--queries", str(root), "--binary", str(binary), "--output", str(root / "out")])


if __name__ == "__main__":
    unittest.main()
