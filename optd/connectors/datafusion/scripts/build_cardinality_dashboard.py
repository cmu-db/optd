#!/usr/bin/env python3
"""Build a self-contained cardinality-estimation status and q-error dashboard."""

from __future__ import annotations

import argparse
import json
import math
import re
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

FEATURES = [
    {
        "area": "Statistics foundation",
        "name": "Cardinality and per-column profiles with bounds and provenance",
        "status": "done",
        "evidence": "optd/core/src/analysis.rs: CardinalityProfile, ColumnProfile, Estimate",
    },
    {
        "area": "Statistics foundation",
        "name": "Catalog row count, null count, min/max, and NDV consumption",
        "status": "done",
        "evidence": "docs/cardinality_estimation_v1.md: Base Statistics",
    },
    {
        "area": "Statistics foundation",
        "name": "Query-local HLL and SpaceSaving consumers",
        "status": "done",
        "evidence": "docs/cardinality_estimation_v1.md: Summary and Implementation Progress",
    },
    {
        "area": "Statistics foundation",
        "name": "Production collection and persistence of HLL/SpaceSaving sketches",
        "status": "pending",
        "evidence": "todo-for-completenes.local.md: Statistics collection and storage",
    },
    {
        "area": "Filters",
        "name": "Equality, range, AND, OR, NULL-aware fallback selectivity",
        "status": "done",
        "evidence": "docs/cardinality_estimation_v1.md: Operator Propagation",
    },
    {
        "area": "Filters",
        "name": "Occupancy-based filtered NDV and tightened literal bounds",
        "status": "done",
        "evidence": "docs/cardinality_estimation_v1.md: Implementation Progress",
    },
    {
        "area": "Filters",
        "name": "Histograms for non-uniform ranges",
        "status": "pending",
        "evidence": "optd/core/src/analysis.rs TODO(statistics): histogram intersection",
    },
    {
        "area": "Filters",
        "name": "MCV-aware equality, IN/NOT IN, LIKE/prefix, and conditional statistics",
        "status": "pending",
        "evidence": "todo-for-completenes.local.md: Filter and expression estimation",
    },
    {
        "area": "Joins",
        "name": "Equivalence classes and redundant equality-edge suppression",
        "status": "done",
        "evidence": "docs/cardinality_estimation_v1.md: Join Estimation",
    },
    {
        "area": "Joins",
        "name": "Directional semi/anti coverage and ordered-domain overlap",
        "status": "done",
        "evidence": "docs/cardinality_estimation_v1.md: Join Estimation",
    },
    {
        "area": "Joins",
        "name": "Population-safe single-column unique/FK inference",
        "status": "done",
        "evidence": "docs/cardinality_estimation_v1.md: Implementation Progress",
    },
    {
        "area": "Joins",
        "name": "Sampled multi-column NDV and intersection-capable domain overlap",
        "status": "pending",
        "evidence": "todo-for-completenes.local.md: Join estimation",
    },
    {
        "area": "Joins",
        "name": "Residual-predicate survival using fanout/correlation",
        "status": "pending",
        "evidence": "optd/core/src/analysis.rs TODO(statistics): residual predicates",
    },
    {
        "area": "Joins",
        "name": "Composite unique/FK inference and directional outer-join unmatched rows",
        "status": "pending",
        "evidence": "todo-for-completenes.local.md: Join estimation / Outer joins",
    },
    {
        "area": "Operators",
        "name": "Projection, rename, map, aggregation, sort, limit, and join propagation",
        "status": "done",
        "evidence": "docs/cardinality_estimation_v1.md: Operator Propagation",
    },
    {
        "area": "Operators",
        "name": "Aggregation group count from grouping-key NDVs",
        "status": "partial",
        "evidence": "docs/cardinality_estimation_v1.md: Aggregation; multi-column correlation remains pending",
    },
    {
        "area": "Operators",
        "name": "Generic expression-statistics transform provider",
        "status": "pending",
        "evidence": "todo-for-completenes.local.md: Filter and expression estimation",
    },
    {
        "area": "Catalog",
        "name": "Automatic PK/unique/FK import from DataFusion-facing providers",
        "status": "pending",
        "evidence": "todo-for-completenes.local.md: Catalog constraints",
    },
    {
        "area": "Regression harness",
        "name": "Per-subtree q-error grouped by recursive join count",
        "status": "done",
        "evidence": "optd/connectors/datafusion/src/cardinality_regression.rs",
    },
    {
        "area": "Regression harness",
        "name": "Chosen-plan PostgreSQL collector with checkpoint/resume",
        "status": "done",
        "evidence": "optd/connectors/datafusion/scripts/postgres_cardinality_regression.py",
    },
    {
        "area": "Regression harness",
        "name": "Normalization levels and optd/PostgreSQL comparison plots",
        "status": "done",
        "evidence": "optd/connectors/datafusion/scripts/plot_cardinality_regression.py",
    },
    {
        "area": "JOB matched probes",
        "name": "Parse JOB and enumerate canonical connected logical probes",
        "status": "partial",
        "evidence": "optd/connectors/datafusion/scripts/job_logical_probes.py; generated manifest requires backend validation",
    },
    {
        "area": "JOB matched probes",
        "name": "Paired optd/PostgreSQL probe execution and shared actual cardinalities",
        "status": "pending",
        "evidence": "No matched-probe collector/report exists in the current tree",
    },
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--optd-report",
        type=Path,
        default=Path("target/cardinality-regression/tpch/report.json"),
    )
    parser.add_argument(
        "--postgres-report",
        type=Path,
        default=Path("target/cardinality-regression/postgres/report.json"),
    )
    parser.add_argument(
        "--queries",
        "--tpch-queries",
        dest="queries",
        type=Path,
        default=Path("optd/connectors/datafusion/tests/slt/tpch/results"),
        help="SQLLogicTest query directory for the selected report suite",
    )
    parser.add_argument(
        "--suite-label",
        default="TPC-H",
        help="Benchmark-suite label shown beside report-derived charts and metrics",
    )
    parser.add_argument(
        "--job-queries",
        type=Path,
        default=Path("optd/connectors/datafusion/tests/slt/job/results"),
    )
    parser.add_argument(
        "--job-manifest",
        type=Path,
        default=Path("target/cardinality-regression/job-probes.json"),
    )
    parser.add_argument(
        "--job-data",
        type=Path,
        default=Path("optd/connectors/datafusion/data/job"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/cardinality-regression/dashboard.html"),
    )
    return parser.parse_args()


def load_json(path: Path) -> Any:
    if not path.exists():
        return None
    try:
        contents = path.read_text()
    except OSError as error:
        raise ValueError(f"failed to read {path}: {error}") from error
    try:
        return json.loads(contents)
    except json.JSONDecodeError as error:
        raise ValueError(f"invalid JSON in {path}: {error}") from error


def geometric_mean(values: list[float]) -> float:
    return math.exp(sum(math.log(value) for value in values) / len(values))


def natural_query_key(name: str) -> tuple[int, str]:
    match = re.match(r"\D*(\d+)", name)
    return (int(match.group(1)) if match else 1_000_000, name)


def summarize(values: list[float]) -> dict[str, float | int]:
    ordered = sorted(values)
    return {
        "n": len(ordered),
        "min": min(ordered),
        "gm": geometric_mean(ordered),
        "median": statistics.median(ordered),
        "max": max(ordered),
        "p95": ordered[min(len(ordered) - 1, math.ceil(len(ordered) * 0.95) - 1)],
        "over10": 100.0 * sum(value >= 10.0 for value in ordered) / len(ordered),
    }


def first_slt_query(path: Path) -> str:
    if not path.exists():
        return ""
    lines = path.read_text().splitlines()
    in_query = False
    query = []
    for line in lines:
        if in_query and line.strip() == "----":
            break
        if in_query:
            query.append(line)
        elif line.startswith("query"):
            in_query = True
    return " ".join(query).strip()


def query_signals(sql: str) -> list[str]:
    upper = sql.upper()
    signals = []
    checks = [
        ("LIKE", "LIKE/text"),
        (" BETWEEN ", "range"),
        (" OR ", "OR correlation"),
        (" IN (", "IN predicate"),
        ("EXISTS", "EXISTS"),
        ("GROUP BY", "grouping"),
        ("DISTINCT", "distinct"),
        ("LIMIT", "limit"),
        ("SUBSTRING(", "derived expression"),
        ("HAVING", "post-aggregation filter"),
        ("<", "inequality"),
        (">", "inequality"),
    ]
    for token, label in checks:
        if token in upper and label not in signals:
            signals.append(label)
    if re.search(r"\b(?:NOT\s+)?IN\s*\(\s*SELECT", upper) and "IN subquery" not in signals:
        signals.append("IN subquery")
    if re.search(r"[=<>]\s*\(\s*SELECT", upper) and "scalar subquery" not in signals:
        signals.append("scalar subquery")
    if upper.count("SELECT") > 1 and "subquery" not in signals:
        signals.append("subquery")
    return signals


def improvement_for(operator: str, join_count: int, signals: list[str]) -> str:
    if "derived expression" in signals:
        return (
            "A generic expression-statistics transform provider, plus statistics for the derived "
            "value and correlation with surrounding predicates."
        )
    if {"IN subquery", "scalar subquery", "EXISTS"} & set(signals) and "grouping" in signals:
        return (
            "Subquery-aware semi-join coverage, sampled grouped-key distributions, and conditional "
            "statistics for the post-aggregation predicate."
        )
    if operator == "aggregation":
        return "Sampled multi-column NDV and functional dependencies for grouping keys."
    if join_count > 0 or operator in {"join", "cross_product"}:
        return (
            "Multi-column NDV/overlap, persisted samples or intersection sketches, "
            "residual fanout modeling, and composite FK/unique metadata."
        )
    if "LIKE/text" in signals:
        return "LIKE/prefix statistics plus MCV/text-distribution statistics."
    if "OR correlation" in signals or "IN/subquery" in signals:
        return "MCV decomposition, first-class IN semantics, and correlation-aware OR estimation."
    if "range" in signals or "inequality" in signals:
        return "Histograms or samples for non-uniform ranges and conditional distributions."
    return "Persisted richer column statistics, conditional statistics, and expression transforms."


def cause_for(
    worst: dict[str, Any], join_count: int, signals: list[str], metric: dict[str, Any]
) -> str:
    if metric["max"] < 10.0:
        return "No ≥10× outlier in this bucket; remaining error is within the current heuristic range."
    operator = str(worst.get("operator", "unknown"))
    estimate = float(worst["estimated_rows"])
    actual = float(worst["actual_rows"])
    direction = "underestimate" if estimate < actual else "overestimate"
    if operator in {"output", "sort", "limit"}:
        return f"{direction.capitalize()} inherited from the upstream subtree; this wrapper preserves or caps rows."
    if "derived expression" in signals:
        return (
            f"{direction.capitalize()} around a derived expression; arbitrary computed columns are "
            "currently opaque to statistics propagation, so downstream selectivity uses weaker evidence."
        )
    if {"IN subquery", "scalar subquery", "EXISTS"} & set(signals) and "grouping" in signals:
        return (
            f"{direction.capitalize()} through a grouped subquery/semi-join shape; post-aggregation "
            "coverage and correlation are not represented by sampled conditional statistics."
        )
    if operator == "aggregation":
        return (
            f"{direction.capitalize()} at aggregation; grouping-key NDVs are combined without sampled "
            "multi-column correlation or functional-dependency reduction."
        )
    if join_count > 0 or operator in {"join", "cross_product"}:
        return (
            f"{direction.capitalize()} after {join_count} joins; errors can compound from uniform-domain "
            "overlap, independent composite keys, residual predicates, skew, or fanout assumptions."
        )
    signal_text = ", ".join(signals) if signals else "filter predicates"
    return (
        f"{direction.capitalize()} in a zero-join subtree with {signal_text}; the current estimator lacks "
        "histograms/conditional distributions for several non-uniform predicate shapes."
    )


def report_payload(
    report: list[dict[str, Any]] | None,
    query_sql: dict[tuple[str, str], str],
    engine: str,
    legacy_suite: str,
) -> dict[str, Any]:
    if not report:
        return {"available": False, "rows": [], "overall": [], "queryTotals": []}
    grouped: dict[tuple[str, str, int], list[dict[str, Any]]] = defaultdict(list)
    overall: dict[int, list[float]] = defaultdict(list)
    by_query: dict[tuple[str, str], list[float]] = defaultdict(list)
    for row in report:
        q_error = float(row["q_error"])
        if not math.isfinite(q_error) or q_error < 1.0:
            continue
        suite = str(row.get("suite") or legacy_suite).lower()
        query = str(row["query"])
        join_count = int(row["join_count"])
        grouped[(suite, query, join_count)].append(row)
        overall[join_count].append(q_error)
        by_query[(suite, query)].append(q_error)

    rows = []
    for (suite, query, join_count), measurements in sorted(
        grouped.items(),
        key=lambda item: (item[0][0], natural_query_key(item[0][1]), item[0][2]),
    ):
        metric = summarize([float(row["q_error"]) for row in measurements])
        wrapper_operators = {"output", "sort", "projection", "rename", "map"}
        worst = max(
            measurements,
            key=lambda row: (
                float(row["q_error"]),
                str(row["operator"]) not in wrapper_operators,
            ),
        )
        operators = Counter(str(row["operator"]) for row in measurements)
        signals = query_signals(query_sql.get((suite, query), ""))
        rows.append(
            {
                "suite": suite,
                "query": query,
                "joinCount": join_count,
                **metric,
                "operators": ", ".join(
                    f"{name}×{count}" for name, count in operators.most_common()
                ),
                "worstOperator": worst["operator"],
                "worstPath": worst["node_path"],
                "worstEstimated": worst["estimated_rows"],
                "worstActual": worst["actual_rows"],
                "cause": (
                    cause_for(worst, join_count, signals, metric)
                    if engine == "optd"
                    else "Observed PostgreSQL chosen-plan error; no internal PostgreSQL cause is inferred from the optd codebase."
                ),
                "improvement": (
                    improvement_for(str(worst["operator"]), join_count, signals)
                    if engine == "optd"
                    else "Use strict probe-ID pairing on JOB before attributing estimator differences between engines."
                ),
                "signals": signals,
            }
        )
    return {
        "available": True,
        "measurements": len(report),
        "queryCount": len(by_query),
        "rows": rows,
        "overall": [
            {"joinCount": join_count, **summarize(values)}
            for join_count, values in sorted(overall.items())
        ],
        "queryTotals": [
            {"suite": suite, "query": query, **summarize(values)}
            for (suite, query), values in sorted(
                by_query.items(),
                key=lambda item: (item[0][0], natural_query_key(item[0][1])),
            )
        ],
    }


def build_feature_impact(
    optd_payload: dict[str, Any], query_sql: dict[tuple[str, str], str]
) -> dict[str, Any]:
    cluster_specs: list[dict[str, Any]] = [
        {
            "id": "filters",
            "name": "Distribution-aware filters",
            "shortName": "Filter distributions",
            "features": "Histograms, MCV decomposition, IN/LIKE statistics, and conditional distributions",
            "color": "#2b8a83",
        },
        {
            "id": "joins",
            "name": "Join correlation and fanout",
            "shortName": "Join correlation",
            "features": "Multi-column NDV/overlap, intersection samples, residual fanout, and composite constraints",
            "color": "#7654d4",
        },
        {
            "id": "subqueries",
            "name": "Subquery and semi/anti coverage",
            "shortName": "Subquery coverage",
            "features": "Context-aware semi/anti coverage and post-aggregation conditional statistics",
            "color": "#d17a22",
        },
        {
            "id": "aggregation",
            "name": "Aggregation dependencies",
            "shortName": "Aggregation NDV",
            "features": "Sampled multi-column group NDV and functional-dependency reduction",
            "color": "#2878bd",
        },
        {
            "id": "expressions",
            "name": "Expression statistics lineage",
            "shortName": "Expression lineage",
            "features": "Generic transforms for derived values, bounds, NDV, null behavior, and sketches",
            "color": "#c04464",
        },
        {
            "id": "skew",
            "name": "Persisted skew and population statistics",
            "shortName": "Skew and freshness",
            "features": "Production HLL/SpaceSaving collection, samples, freshness, and predicate-conditioned statistics",
            "color": "#6b7f2a",
        },
    ]
    rows_by_query: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in optd_payload.get("rows", []):
        rows_by_query[(str(row["suite"]), str(row["query"]))].append(row)
    totals = {
        (str(row["suite"]), str(row["query"])): row
        for row in optd_payload.get("queryTotals", [])
    }
    query_impacts = []
    for (suite, query), total in sorted(
        totals.items(), key=lambda item: (item[0][0], natural_query_key(item[0][1]))
    ):
        rows = rows_by_query[(suite, query)]
        signals = set(query_signals(query_sql.get((suite, query), "")))
        worst = max(rows, key=lambda row: float(row["max"]))
        memberships = []
        if any(
            int(row["joinCount"]) == 0 and float(row["max"]) >= 10.0
            for row in rows
        ):
            memberships.append("filters")
        if int(worst["joinCount"]) > 0 and worst["worstOperator"] in {
            "join",
            "selection",
        }:
            memberships.append("joins")
        if signals & {"IN subquery", "scalar subquery", "EXISTS"}:
            memberships.append("subqueries")
        if worst["worstOperator"] == "aggregation":
            memberships.append("aggregation")
        if "derived expression" in signals:
            memberships.append("expressions")
        if float(total["max"]) >= 100.0 and signals & {
            "LIKE/text",
            "OR correlation",
        }:
            memberships.append("skew")
        if not memberships:
            memberships.append("joins" if int(worst["joinCount"]) > 0 else "filters")
        query_impacts.append(
            {
                "key": f"{suite}/{query}",
                "suite": suite,
                "query": query,
                "memberships": memberships,
                "maxQError": total["max"],
                "geometricMean": total["gm"],
                "highError": float(total["max"]) >= 10.0,
                "signals": sorted(signals),
            }
        )
    for cluster in cluster_specs:
        cluster["queries"] = [
            row["key"]
            for row in query_impacts
            if cluster["id"] in row["memberships"]
        ]
        cluster["highErrorQueries"] = [
            row["key"]
            for row in query_impacts
            if row["highError"] and cluster["id"] in row["memberships"]
        ]
    return {"clusters": cluster_specs, "queries": query_impacts, "threshold": 10.0}


def build_job_status(args: argparse.Namespace) -> dict[str, Any]:
    query_count = len(list(args.job_queries.glob("*.slt"))) if args.job_queries.exists() else 0
    table_count = len(list(args.job_data.glob("*.parquet"))) if args.job_data.exists() else 0
    manifest = load_json(args.job_manifest)
    join_counts: Counter[int] = Counter()
    manifest_queries = set()
    if isinstance(manifest, list):
        for row in manifest:
            join_counts[int(row["join_count"])] += 1
            manifest_queries.add(str(row["query"]))
    return {
        "queryCount": query_count,
        "tableCount": table_count,
        "manifestAvailable": isinstance(manifest, list),
        "probeCount": len(manifest) if isinstance(manifest, list) else 0,
        "manifestQueryCount": len(manifest_queries),
        "maxJoinCount": max(join_counts, default=None),
        "probesByJoinCount": [
            {"joinCount": join_count, "count": count}
            for join_count, count in sorted(join_counts.items())
        ],
        "pairedReportAvailable": False,
    }


def build_plot_artifacts(output: Path, suites: list[str]) -> dict[str, Any]:
    comparison_dir = output.parent / "comparison"
    levels = [
        ("none", "All measured nodes", "Raw operator subtrees"),
        ("wrappers", "Wrapper-normalized", "Removes Output and Sort"),
        (
            "row-preserving",
            "Row-preserving normalized",
            "Also removes Projection, Rename, and Map",
        ),
        ("joins", "Join-only", "Keeps only joins and cross products"),
    ]
    suite_selections: list[tuple[str, ...]] = [(suite,) for suite in suites]
    if len(suites) > 1:
        suite_selections.append(tuple(suites))
    selections = {}
    for selected in suite_selections:
        key = "+".join(selected)
        suffix = selected[0] if len(selected) == 1 else "all"
        normalizations = []
        for level, label, description in levels:
            files = [
                (
                    "optd vs. PostgreSQL box plot",
                    "Comparison plot",
                    f"qerror-comparison-boxplot-{level}-{suffix}.svg",
                ),
                (
                    "optd box plot",
                    "Distribution plot",
                    f"qerror-boxplot-{level}-{suffix}.svg",
                ),
                (
                    "optd violin plot",
                    "Distribution plot",
                    f"qerror-violin-{level}-{suffix}.svg",
                ),
                (
                    "Summary by join count",
                    "CSV",
                    f"summary-by-join-count-{level}-{suffix}.csv",
                ),
            ]
            normalizations.append(
                {
                    "id": level,
                    "label": label,
                    "description": description,
                    "artifacts": [
                        {
                            "name": name,
                            "kind": kind,
                            "href": f"comparison/{filename}",
                            "available": (comparison_dir / filename).exists(),
                        }
                        for name, kind, filename in files
                    ],
                }
            )
        comparison_filename = f"comparison-by-join-count-all-{suffix}.csv"
        selections[key] = {
            "suites": list(selected),
            "normalizations": normalizations,
            "comparisonCsv": {
                "name": "Combined comparison metrics",
                "kind": "CSV",
                "href": f"comparison/{comparison_filename}",
                "available": (comparison_dir / comparison_filename).exists(),
            },
        }
    return {"selections": selections}


def html_template(data_json: str) -> str:
    return r'''<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Cardinality Estimation Feature Dashboard</title>
<style>
:root{--bg:#f5f7fb;--panel:#ffffff;--panel2:#eef3f9;--text:#182236;--muted:#607086;--line:#d9e1ec;--cyan:#087f79;--purple:#7654d4;--amber:#c97a08;--red:#c93c4d;--green:#198754;--blue:#2878bd}
*{box-sizing:border-box} body{margin:0;background:linear-gradient(145deg,#f8fafc,#f2f5fa 45%,#edf2f8);color:var(--text);font:14px/1.45 Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif} a{color:var(--cyan)}
header{padding:30px clamp(18px,4vw,58px) 20px;border-bottom:1px solid var(--line);background:rgba(255,255,255,.9);position:sticky;top:0;z-index:10;backdrop-filter:blur(14px)}
h1{font-size:clamp(25px,4vw,42px);margin:0 0 7px;letter-spacing:-.035em}.subtitle{color:var(--muted);max-width:1050px}.stamp{font-size:12px;color:#718096;margin-top:8px}
main{padding:24px clamp(14px,3vw,48px) 70px;max-width:1800px;margin:auto}.grid{display:grid;gap:16px}.kpis{grid-template-columns:repeat(auto-fit,minmax(170px,1fr));margin-bottom:20px}.kpi,.panel{background:linear-gradient(160deg,#ffffff,#fbfcfe);border:1px solid var(--line);border-radius:14px;box-shadow:0 12px 35px rgba(45,65,95,.09)}.kpi{padding:18px}.kpi .value{font-size:29px;font-weight:760;letter-spacing:-.03em}.kpi .label{color:var(--muted);font-size:12px;text-transform:uppercase;letter-spacing:.08em}.kpi .note{font-size:12px;color:#718096;margin-top:5px}
.panel{padding:20px;margin:16px 0}.panel h2{margin:0 0 6px;font-size:21px}.panel h3{margin:18px 0 8px}.suite-tag{display:inline-flex;vertical-align:middle;margin-left:7px;padding:2px 8px;border:1px solid rgba(8,127,121,.3);border-radius:999px;background:rgba(8,127,121,.07);color:var(--cyan);font-size:11px;font-weight:800;letter-spacing:.05em}.help{color:var(--muted);font-size:13px;margin-bottom:15px}.two{grid-template-columns:repeat(auto-fit,minmax(340px,1fr))}.three{grid-template-columns:repeat(auto-fit,minmax(280px,1fr))}
.badge{display:inline-flex;align-items:center;border-radius:999px;padding:3px 9px;font-size:11px;font-weight:750;text-transform:uppercase;letter-spacing:.06em}.done{background:rgba(104,211,145,.14);color:var(--green);border:1px solid rgba(104,211,145,.35)}.partial{background:rgba(246,173,85,.14);color:var(--amber);border:1px solid rgba(246,173,85,.35)}.pending{background:rgba(252,129,129,.12);color:var(--red);border:1px solid rgba(252,129,129,.3)}
.feature{padding:13px 0;border-top:1px solid rgba(217,225,236,.9)}.feature:first-child{border-top:0}.feature-title{display:flex;align-items:flex-start;gap:9px;font-weight:650}.evidence{color:var(--muted);font-size:12px;margin:5px 0 0 63px}.area-title{color:var(--cyan);font-size:12px;text-transform:uppercase;letter-spacing:.1em;margin:17px 0 6px}
.progress{height:10px;border-radius:8px;background:#e5eaf1;overflow:hidden;display:flex;margin:12px 0}.progress>i{display:block;height:100%}.progress .pdone{background:var(--green)}.progress .ppartial{background:var(--amber)}.progress .ppending{background:var(--red)}
.suite-picker{display:flex;flex-wrap:wrap;align-items:center;gap:9px}.suite-picker label{display:flex;align-items:center;gap:6px;border:1px solid var(--line);border-radius:8px;background:#fff;padding:7px 10px;font-weight:750;cursor:pointer}.suite-picker input{accent-color:var(--cyan)}.controls{display:flex;flex-wrap:wrap;gap:10px;margin:15px 0}.controls input,.controls select{background:#ffffff;color:var(--text);border:1px solid var(--line);border-radius:8px;padding:9px 11px;min-width:150px}.controls label{display:flex;align-items:center;gap:6px;color:var(--muted)}
.table-wrap{overflow:auto;max-height:72vh;border:1px solid var(--line);border-radius:10px}table{width:100%;border-collapse:separate;border-spacing:0;min-width:1350px}th{position:sticky;top:0;background:#edf2f7;z-index:2;color:#4a5b70;font-size:11px;text-transform:uppercase;letter-spacing:.06em;text-align:left;padding:10px;border-bottom:1px solid var(--line);cursor:pointer}td{padding:9px 10px;border-bottom:1px solid rgba(217,225,236,.8);vertical-align:top}tr:hover td{background:rgba(8,127,121,.045)}td.num{font-variant-numeric:tabular-nums;text-align:right}.qgood{color:var(--green)}.qwarn{color:var(--amber)}.qbad{color:var(--red);font-weight:700}.cause{min-width:320px}.improve{min-width:330px;color:#40536b}.small{font-size:12px;color:var(--muted)}
.chart{display:flex;align-items:flex-end;gap:8px;height:220px;padding:15px 6px 0;border-bottom:1px solid var(--line)}.bar-group{flex:1;min-width:27px;display:flex;gap:3px;align-items:flex-end;height:100%;position:relative}.bar{flex:1;border-radius:5px 5px 0 0;min-height:2px;position:relative}.bar:hover:after{content:attr(data-tip);position:absolute;bottom:100%;left:50%;transform:translateX(-50%);background:#1f2937;color:#ffffff;border:1px solid #111827;padding:6px;border-radius:6px;white-space:nowrap;font-size:11px;z-index:3}.bar.optd{background:linear-gradient(var(--purple),#5f3db8)}.bar.pg{background:linear-gradient(var(--amber),#aa6508)}.axis-label{text-align:center;color:var(--muted);font-size:11px;margin-top:4px}.legend{display:flex;gap:14px;color:var(--muted);font-size:12px}.dot{width:9px;height:9px;border-radius:50%;display:inline-block;margin-right:5px}.artifact-controls{display:flex;flex-wrap:wrap;align-items:end;gap:12px;margin:14px 0}.artifact-controls label{display:grid;gap:4px;color:var(--muted);font-size:11px;font-weight:750;text-transform:uppercase;letter-spacing:.06em}.artifact-controls select{min-width:260px;border:1px solid var(--line);border-radius:8px;background:#fff;color:var(--text);padding:9px 11px;font-size:13px}.artifact-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:10px}.artifact-card{display:block;border:1px solid var(--line);border-radius:10px;padding:13px 14px;background:#fff;color:var(--text);text-decoration:none;transition:transform .15s,border-color .15s,box-shadow .15s}.artifact-card:hover{transform:translateY(-2px);border-color:var(--cyan);box-shadow:0 7px 20px rgba(45,65,95,.1)}.artifact-card .kind{color:var(--muted);font-size:10px;font-weight:800;text-transform:uppercase;letter-spacing:.07em}.artifact-card .name{font-weight:750;margin-top:3px}.artifact-card .open{color:var(--cyan);font-size:12px;margin-top:8px}.artifact-card.missing{opacity:.5;cursor:not-allowed}.artifact-description{color:var(--muted);font-size:12px;margin:4px 0 12px}.plot-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}.plot-panel{border:1px solid var(--line);border-radius:12px;background:#fff;padding:12px;min-width:0}.plot-panel h3{margin:0 0 2px;font-size:15px}.plot-panel .plot-description{margin-bottom:8px}.plot-frame{position:relative;display:grid;place-items:center;min-height:320px;border:1px solid #e4eaf2;border-radius:8px;background:#fff;overflow:auto;padding:8px}.plot-frame img{display:block;width:100%;height:auto;max-height:520px;object-fit:contain}.plot-placeholder{color:var(--muted);text-align:center;padding:70px 20px}.plot-actions{display:flex;flex-wrap:wrap;gap:7px;margin-top:9px}.plot-actions a{display:inline-flex;align-items:center;border:1px solid var(--line);border-radius:7px;padding:6px 9px;background:#fff;color:var(--cyan);text-decoration:none;font-size:11px;font-weight:700}.plot-actions a:hover{border-color:var(--cyan)}.plot-actions a.disabled{pointer-events:none;opacity:.45}.callout{border-left:3px solid var(--cyan);padding:10px 13px;background:rgba(8,127,121,.055);border-radius:0 8px 8px 0}.warning{border-left-color:var(--amber);background:rgba(201,122,8,.06)}.danger{border-left-color:var(--red);background:rgba(201,60,77,.055)}
.impact-controls{display:grid;grid-template-columns:minmax(420px,1fr) minmax(260px,.55fr);gap:14px;margin:0 0 14px;padding:12px;border:1px solid var(--line);border-radius:10px;background:#f8fafc}.impact-control-title{font-size:11px;font-weight:800;text-transform:uppercase;letter-spacing:.07em;color:var(--muted);margin-bottom:7px}.impact-scope{display:flex;align-items:center;gap:8px;margin-bottom:10px;padding:7px 9px;border:1px solid var(--line);border-radius:8px;background:#fff}.impact-scope label{display:flex;align-items:center;gap:7px;font-weight:750;cursor:pointer}.impact-scope input{accent-color:var(--cyan)}.impact-scope .small{margin-left:auto}.impact-toggles{display:flex;flex-wrap:wrap;gap:7px}.optimization-toggle{display:inline-flex;align-items:center;gap:6px;border:1px solid var(--line);border-left:4px solid var(--cluster);border-radius:8px;background:#fff;padding:6px 9px;font-size:12px;font-weight:650;cursor:pointer}.optimization-toggle:has(input:not(:checked)){opacity:.45;background:#f2f4f7}.optimization-toggle input{accent-color:var(--cluster)}.impact-control-actions{display:flex;gap:6px;margin-top:8px}.impact-control-actions button{border:1px solid var(--line);border-radius:6px;background:#fff;padding:5px 9px;color:#40536b;cursor:pointer}.impact-control-actions button:hover{border-color:var(--cyan);color:var(--cyan)}.impact-focus select{width:100%;background:#fff;color:var(--text);border:1px solid var(--line);border-radius:7px;padding:8px}.impact-focus-summary{margin-top:8px;padding:8px 10px;border-radius:7px;background:#fff;border:1px solid var(--line);font-size:12px}.membership-list{display:flex;gap:5px;flex-wrap:wrap;margin-top:6px}.membership-chip{border-radius:999px;padding:2px 7px;font-size:10px;font-weight:750;color:#fff}.membership-chip.off{filter:grayscale(1);opacity:.38}.impact-layout{display:grid;grid-template-columns:minmax(560px,1.55fr) minmax(300px,.75fr);gap:20px;align-items:start}.impact-map-wrap{position:relative;border:1px solid var(--line);border-radius:12px;background:radial-gradient(circle at 46% 42%,#fff 0,#f8fafc 72%);overflow:hidden}.impact-map{display:block;width:100%;min-height:520px;touch-action:none}.impact-map svg{display:block;width:100%;height:auto;min-height:520px}.impact-toolbar{position:absolute;top:10px;right:10px;z-index:3;display:flex;align-items:center;gap:5px;padding:5px;border:1px solid var(--line);border-radius:9px;background:rgba(255,255,255,.94);box-shadow:0 3px 12px rgba(45,65,95,.12)}.impact-toolbar button{width:31px;height:29px;border:1px solid var(--line);border-radius:6px;background:#fff;color:#253349;font-weight:800;cursor:pointer}.impact-toolbar button:hover{border-color:var(--cyan);color:var(--cyan)}.impact-toolbar .zoom-value{min-width:44px;text-align:center;color:var(--muted);font-size:11px}.venn-area path{transition:fill-opacity .16s,stroke-width .16s}.venn-area text{font-family:inherit;font-weight:800;fill:#253349;pointer-events:none}.query-focus-marker{pointer-events:none}.query-focus-marker circle{fill:#182236;stroke:#fff;stroke-width:3;filter:drop-shadow(0 2px 3px rgba(31,41,55,.35))}.query-focus-marker text{fill:#fff;font-size:11px;font-weight:850;text-anchor:middle;dominant-baseline:central}.query-focus-marker .query-focus-halo{fill:none;stroke:#182236;stroke-width:2;stroke-dasharray:4 4;opacity:.65}.impact-tooltip{position:fixed;z-index:100;max-width:330px;padding:10px 12px;border:1px solid #26364d;border-radius:8px;background:rgba(24,34,54,.96);color:#fff;box-shadow:0 8px 30px rgba(20,30,45,.25);font-size:12px;pointer-events:none;opacity:0;transition:opacity .12s}.impact-tooltip b{font-size:13px}.impact-tooltip .tip-muted{color:#cbd5e1;margin-top:3px}.impact-card{border:1px solid var(--line);border-left:4px solid var(--cluster);border-radius:9px;padding:11px 12px;margin-bottom:9px;background:#fff}.impact-card b{display:block;margin-bottom:3px}.impact-queries{display:flex;gap:5px;flex-wrap:wrap;margin-top:7px}.query-chip{border:1px solid var(--line);border-radius:999px;background:#f7f9fc;padding:2px 7px;font-size:11px;font-weight:700;cursor:pointer}.query-chip:hover{border-color:var(--cluster);color:var(--cluster)}
.milestone{display:grid;grid-template-columns:30px 1fr;gap:10px;padding:9px 0}.step{width:25px;height:25px;border-radius:50%;display:grid;place-items:center;font-weight:750;background:#e3edf5;color:var(--cyan)}code{background:#f0f4f8;border:1px solid #d5dee9;padding:2px 5px;border-radius:5px;color:#17645f}.source-list li{margin:7px 0;color:#4a5b70}.hidden{display:none!important}@media(max-width:950px){.impact-controls,.impact-layout,.plot-grid{grid-template-columns:1fr}.impact-map,.impact-map svg{min-height:430px}}@media(max-width:700px){header{position:static}.panel{padding:14px}.cause,.improve{min-width:270px}.impact-map,.impact-map svg{min-width:700px}.impact-map-wrap{overflow:auto}.impact-controls{min-width:0}.optimization-toggle{font-size:11px}}
</style>
</head>
<body>
<header><h1>Cardinality Estimation Feature Dashboard</h1><div class="subtitle">Current repository snapshot: estimator capabilities, regression infrastructure, JOB matched-probe readiness, and per-query subtree q-error distributions.</div><div class="stamp" id="stamp"></div></header>
<main>
<section class="grid kpis" id="kpis"></section>
<section class="panel"><h2>Benchmark suites</h2><div class="help">Select one or more suites. Metrics and feature memberships are aggregated over exactly this selection; generated SVGs switch to the corresponding canonical suite artifact.</div><div id="suite-picker" class="suite-picker"></div></section>
<section class="panel"><h2>Feature clusters and likely query impact <span class="suite-tag suite-name"></span></h2><div class="help">Area-proportional Euler diagram rendered with venn.js and D3. Circle sizes and intersections come from the current query-to-feature memberships rather than fixed geometry. Turn optimizations on or off, focus a query to mark its exact active intersection, hover an area for its query list, and use zoom or pan for inspection. The default scope contains queries with a ≥10× outlier; enable “Include all measured queries” to place the complete suite in the diagram. Four structural clusters are enabled initially for readability; expression-lineage and skew clusters remain available as toggles. “Likely impact” remains diagnostic prioritization, not a guaranteed fix.</div><div class="impact-controls"><div><div class="impact-control-title">Query and optimization scope</div><div class="impact-scope"><label><input id="impact-all-queries" type="checkbox">Include all measured queries</label><span id="impact-query-scope-note" class="small"></span></div><div id="impact-toggles" class="impact-toggles"></div><div class="impact-control-actions"><button id="impact-all" type="button">All on</button><button id="impact-none" type="button">All off</button><button id="impact-query-only" type="button">Only focused query’s features</button></div></div><div class="impact-focus"><div class="impact-control-title">Locate a query</div><select id="impact-query-focus"></select><div id="impact-focus-summary" class="impact-focus-summary"></div></div></div><div class="impact-layout"><div class="impact-map-wrap"><div class="impact-toolbar" aria-label="Feature map zoom controls"><button id="impact-zoom-out" type="button" title="Zoom out">−</button><span id="impact-zoom-value" class="zoom-value">100%</span><button id="impact-zoom-in" type="button" title="Zoom in">+</button><button id="impact-zoom-reset" type="button" title="Reset zoom">↺</button></div><div id="impact-map" class="impact-map" role="img" aria-label="Zoomable area-proportional feature overlap diagram"></div></div><div id="impact-details"></div></div></section>
<section class="grid two">
  <div class="panel"><h2>Implementation status</h2><div class="help">Status is derived from current implementation/docs and linked to repository evidence.</div><div id="progress"></div><div id="feature-summary"></div></div>
  <div class="panel"><h2>JOB matched-probe readiness</h2><div class="help">JOB is the target for a paired logical-subexpression comparison; no paired q-error report exists yet.</div><div id="job-status"></div></div>
</section>
<section class="panel"><h2>Generated q-error plots <span class="suite-tag suite-name"></span></h2><div class="help">These plots aggregate all measured queries in the selected report suite, but never combine different suites. Query selection affects the impact map and metrics table, not these static suite-wide SVGs. They are the canonical artifacts produced by <code>just regression-all</code> from the optd and PostgreSQL report pair. All four normalization/grouping views are shown together in a 2×2 grid. Use the plot-type selector to switch every panel between comparison box, optd box, and optd violin plots.</div><div class="artifact-controls"><label>Plot type<select id="plot-kind"></select></label><a id="comparison-csv-link" class="small" target="_blank" rel="noopener">Open combined comparison CSV</a></div><div id="plot-grid" class="plot-grid"></div></section>
<section class="panel">
  <h2>Per-query, per-subtree-join-size q-error <span class="suite-tag suite-name"></span></h2>
  <div class="help">This table contains only the selected report suites. Min / geometric mean / median / max are calculated from every measured node in the selected query and recursive join-count bucket. Cause text is a code-backed diagnostic hypothesis, not proof of a single root cause.</div>
  <div class="controls">
    <label>Engine <select id="engine"><option value="optd">optd</option><option value="postgres">PostgreSQL</option></select></label>
    <label>Query <select id="query-filter"><option value="">All queries</option></select></label>
    <label>Join count <select id="join-filter"><option value="">All sizes</option></select></label>
    <label>Minimum max q-error <input id="threshold" type="number" min="1" value="1" step="1"></label>
    <label>Search <input id="search" placeholder="operator, cause, feature…"></label>
  </div>
  <div class="small" id="row-count"></div>
  <div class="table-wrap"><table><thead><tr>
    <th data-sort="suite">Suite</th><th data-sort="query">Query</th><th data-sort="joinCount">Joins</th><th data-sort="n">n</th><th data-sort="min">Min</th><th data-sort="gm">GM</th><th data-sort="median">Median</th><th data-sort="max">Max</th><th data-sort="over10">≥10×</th><th>Measured operators</th><th>Worst node</th><th>Likely cause</th><th>Improvement features</th>
  </tr></thead><tbody id="metrics-body"></tbody></table></div>
</section>
<section class="grid two">
 <div class="panel"><h2>What is done</h2><div id="done-list"></div></div>
 <div class="panel"><h2>What is pending</h2><div id="pending-list"></div></div>
</section>
<section class="panel"><h2>Evidence and interpretation boundaries</h2><div class="grid two"><div><h3>Primary repository evidence</h3><ul class="source-list" id="sources"></ul></div><div><h3>Important limitations</h3><div class="callout warning"><span class="suite-name"></span> metrics are real current artifacts, but they compare independently selected plans. They should diagnose estimator behavior, not serve as a paired estimator leaderboard.</div><div class="callout danger" style="margin-top:10px">The JOB manifest has canonical probes, but generated SQL has not yet been validated end-to-end and neither engine has produced a paired JOB report.</div></div></div></section>
</main>
<div id="impact-tooltip" class="impact-tooltip" role="tooltip"></div>
<script src="https://cdn.jsdelivr.net/npm/d3@7.9.0/dist/d3.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/venn.js@0.2.20/build/venn.min.js"></script>
<script>
const DATA=__DATA__;
const fmt=v=>{if(v===null||v===undefined)return'—';if(v>=10000)return v.toExponential(2);if(v>=100)return v.toFixed(1);if(v>=10)return v.toFixed(2);return v.toFixed(3).replace(/0+$/,'').replace(/\.$/,'')};
const qClass=v=>v>=100?'qbad':v>=10?'qwarn':'qgood';
const suiteById=Object.fromEntries(DATA.suites.map(suite=>[suite.id,suite])),suiteOrder=DATA.suites.map(suite=>suite.id);let activeSuites=new Set(suiteOrder);const activeSuiteLabel=()=>suiteOrder.filter(id=>activeSuites.has(id)).map(id=>suiteById[id].label).join(' + ');const activeSuiteKey=()=>suiteOrder.filter(id=>activeSuites.has(id)).join('+');const refreshSuiteLabels=()=>{const label=activeSuiteLabel();document.querySelectorAll('.suite-name').forEach(node=>node.textContent=label);document.getElementById('stamp').textContent=`Generated ${DATA.generatedAt} · report suites: ${label} · source reports and repository state listed below`};refreshSuiteLabels();
const fs=DATA.featureStatus, optd=DATA.engines.optd, pg=DATA.engines.postgres, job=DATA.job;
const kpis=[['Implemented features',fs.done,`${fs.total} tracked dashboard capabilities`],['Partial features',fs.partial,'Useful implementation exists; known gaps remain'],['Pending features',fs.pending,'Backlog evidenced in current code/docs'],[`${DATA.benchmarkSuite} optd nodes`,optd.measurements||0,`${optd.queryCount||0} queries measured`],['JOB logical probes',job.probeCount||0,`${job.manifestQueryCount||0}/${job.queryCount||0} queries represented`],['JOB paired results',job.pairedReportAvailable?'ready':'not yet','optd + PostgreSQL + shared actual']];
document.getElementById('kpis').innerHTML=kpis.map(x=>`<div class="kpi"><div class="label">${x[0]}</div><div class="value">${x[1]}</div><div class="note">${x[2]}</div></div>`).join('');
const impact=DATA.featureImpact;
const clusterById=Object.fromEntries(impact.clusters.map(c=>[c.id,c])),impactByKey=Object.fromEntries(impact.queries.map(query=>[query.key,query]));
let activeImpactIds=new Set(impact.clusters.slice(0,4).map(c=>c.id));
let showAllImpactQueries=false;
let focusedImpactQuery=[...impact.queries].sort((a,b)=>b.maxQError-a.maxQError)[0]?.key||'';
const visibleImpactQueries=()=>impact.queries.filter(query=>activeSuites.has(query.suite)&&(showAllImpactQueries||query.highError));
const selectImpactQuery=(key,scrollToTable=true)=>{document.getElementById('engine').value='optd';document.getElementById('query-filter').value=key;document.getElementById('threshold').value='1';renderTable();if(scrollToTable)document.getElementById('metrics-body').closest('.panel').scrollIntoView({behavior:'smooth',block:'start'})};
function renderImpactDetails(){const visible=new Set(visibleImpactQueries().map(query=>query.key));document.getElementById('impact-details').innerHTML=impact.clusters.map(cluster=>{const queries=cluster.queries.filter(key=>visible.has(key));return`<div class="impact-card" style="--cluster:${cluster.color}"><b>${cluster.name}</b><div class="small">${cluster.features}</div><div class="impact-queries">${queries.map(key=>`<button class="query-chip" data-query="${key}" style="--cluster:${cluster.color}">${suiteById[impactByKey[key].suite].label} · ${impactByKey[key].query}</button>`).join('')||'<span class="small">No queries in the current scope</span>'}</div></div>`}).join('');document.querySelectorAll('.query-chip').forEach(node=>node.addEventListener('click',()=>{focusedImpactQuery=node.dataset.query;renderImpactControls();renderImpactMap();selectImpactQuery(node.dataset.query)}))}
const combinations=(values,size,start=0,prefix=[],result=[])=>{if(prefix.length===size){result.push(prefix);return result}for(let index=start;index<values.length;index++)combinations(values,size,index+1,[...prefix,values[index]],result);return result};
const queriesForSets=sets=>visibleImpactQueries().filter(query=>sets.every(id=>query.memberships.includes(id)));
function buildVennAreas(activeIds){const areas=[];for(let size=1;size<=activeIds.length;size++){for(const sets of combinations(activeIds,size)){const queries=queriesForSets(sets);if(!queries.length)continue;const cluster=sets.length===1?clusterById[sets[0]]:null;areas.push({sets,size:queries.length,label:cluster?`${cluster.shortName}\n${queries.length}`:' ',queries:queries.map(query=>query.key)})}}return areas}
function renderImpactControls(){
  const visible=visibleImpactQueries();
  if(!visible.some(query=>query.key===focusedImpactQuery))focusedImpactQuery=[...visible].sort((a,b)=>b.maxQError-a.maxQError)[0]?.key||'';
  document.getElementById('impact-all-queries').checked=showAllImpactQueries;
  document.getElementById('impact-query-scope-note').textContent=`${visible.length}/${impact.queries.length} queries shown`;
  document.getElementById('impact-toggles').innerHTML=impact.clusters.map(cluster=>`<label class="optimization-toggle" style="--cluster:${cluster.color}"><input type="checkbox" value="${cluster.id}" ${activeImpactIds.has(cluster.id)?'checked':''}>${cluster.shortName}</label>`).join('');
  document.querySelectorAll('#impact-toggles input').forEach(input=>input.addEventListener('change',()=>{if(input.checked)activeImpactIds.add(input.value);else activeImpactIds.delete(input.value);renderImpactControls();renderImpactMap()}));
  const focus=document.getElementById('impact-query-focus');
  focus.innerHTML=visible.map(query=>`<option value="${query.key}">${suiteById[query.suite].label} · ${query.query} · max ${fmt(query.maxQError)}</option>`).join('');
  focus.value=focusedImpactQuery;
  const query=impactByKey[focusedImpactQuery];
  document.getElementById('impact-focus-summary').innerHTML=query?`<b>${suiteById[query.suite].label} · ${query.query}</b> · max ${fmt(query.maxQError)}, GM ${fmt(query.geometricMean)}<div class="membership-list">${query.memberships.map(id=>`<span class="membership-chip ${activeImpactIds.has(id)?'':'off'}" style="background:${clusterById[id].color}">${clusterById[id].shortName}</span>`).join('')}</div><div class="small" style="margin-top:5px">Dark marker = this query’s exact intersection among currently enabled optimizations. Muted chips are hidden from the projection.</div>`:'No query is available in this scope.';
  renderImpactDetails();
}
function renderImpactMap(){
  const hostElement=document.getElementById('impact-map');
  if(!window.d3||!window.venn){hostElement.innerHTML='<div style="padding:220px 20px;text-align:center;color:#c93c4d">D3 or venn.js failed to load; check network access and reload.</div>';return}
  const activeIds=impact.clusters.map(c=>c.id).filter(id=>activeImpactIds.has(id));
  if(!activeIds.length){hostElement.innerHTML='<div style="padding:220px 20px;text-align:center;color:#607086">No optimizations enabled. Use the toggles or “All on” to restore the diagram.</div>';document.getElementById('impact-zoom-value').textContent='100%';return}
  const areas=buildVennAreas(activeIds),host=d3.select(hostElement);host.html('');
  const chart=venn.VennDiagram().width(950).height(520).padding(30).duration(0).wrap(true).styled(false).fontSize('13px');
  const result=chart(host.datum(areas));
  const svg=host.select('svg').attr('viewBox','0 0 950 520').attr('preserveAspectRatio','xMidYMid meet').attr('aria-label',`${activeIds.length} active optimization sets`);
  const groups=svg.selectAll('g.venn-area');
  groups.select('path').style('fill',d=>d.sets.length===1?clusterById[d.sets[0]].color:'#ffffff').style('fill-opacity',d=>d.sets.length===1?.2:.025).style('stroke',d=>d.sets.length===1?clusterById[d.sets[0]].color:'#506176').style('stroke-opacity',d=>d.sets.length===1?.9:.18).style('stroke-width',d=>d.sets.length===1?2:1);
  groups.select('text').style('font-size',d=>d.sets.length===1?'13px':'10px');
  const areaNodes=groups.nodes(),zoomLayer=svg.append('g').attr('class','venn-zoom-layer');areaNodes.forEach(node=>zoomLayer.node().appendChild(node));
  const tooltip=d3.select('#impact-tooltip');
  zoomLayer.selectAll('g.venn-area').on('mouseenter',(event,d)=>{venn.sortAreas(host,d);const queries=queriesForSets(d.sets);d3.select(event.currentTarget).select('path').style('fill-opacity',d.sets.length===1?.32:.14).style('stroke-width',3);tooltip.style('opacity',1).html(`<b>${d.sets.map(id=>clusterById[id].name).join(' ∩ ')}</b><div class="tip-muted">${queries.length} matching ${queries.length===1?'query':'queries'}: ${queries.map(q=>`${suiteById[q.suite].label} · ${q.query}`).join(', ')}</div>`)}).on('mousemove',event=>tooltip.style('left',`${event.clientX+14}px`).style('top',`${event.clientY+14}px`)).on('mouseleave',event=>{const d=d3.select(event.currentTarget).datum();d3.select(event.currentTarget).select('path').style('fill-opacity',d.sets.length===1?.2:.025).style('stroke-width',d.sets.length===1?2:1);tooltip.style('opacity',0)}).on('click',(event,d)=>{const queries=queriesForSets(d.sets);if(queries.length===1){focusedImpactQuery=queries[0].key;renderImpactControls();renderImpactMap();selectImpactQuery(focusedImpactQuery)}});
  const focus=impactByKey[focusedImpactQuery],activeMembership=focus?activeIds.filter(id=>focus.memberships.includes(id)):[];
  if(focus&&activeMembership.length){const centre=result.textCentres[activeMembership.toString()];if(centre&&!centre.disjoint){const marker=zoomLayer.append('g').attr('class','query-focus-marker').attr('transform',`translate(${centre.x},${centre.y})`);marker.append('circle').attr('class','query-focus-halo').attr('r',27);marker.append('circle').attr('r',20);marker.append('text').text(focus.query)}}
  const zoomValue=document.getElementById('impact-zoom-value'),zoom=d3.zoom().scaleExtent([.65,5]).translateExtent([[-250,-180],[1200,700]]).on('zoom',event=>{zoomLayer.attr('transform',event.transform);zoomValue.textContent=`${Math.round(event.transform.k*100)}%`});svg.call(zoom).on('dblclick.zoom',null);
  const applyZoom=factor=>svg.transition().duration(220).call(zoom.scaleBy,factor);document.getElementById('impact-zoom-in').onclick=()=>applyZoom(1.35);document.getElementById('impact-zoom-out').onclick=()=>applyZoom(1/1.35);document.getElementById('impact-zoom-reset').onclick=()=>svg.transition().duration(260).call(zoom.transform,d3.zoomIdentity);
}
renderImpactControls();renderImpactMap();
document.getElementById('impact-query-focus').addEventListener('change',event=>{focusedImpactQuery=event.target.value;renderImpactControls();renderImpactMap();selectImpactQuery(focusedImpactQuery,false)});
document.getElementById('impact-all-queries').addEventListener('change',event=>{showAllImpactQueries=event.target.checked;renderImpactControls();renderImpactMap()});
document.getElementById('impact-all').onclick=()=>{activeImpactIds=new Set(impact.clusters.map(c=>c.id));renderImpactControls();renderImpactMap()};
document.getElementById('impact-none').onclick=()=>{activeImpactIds=new Set();renderImpactControls();renderImpactMap()};
document.getElementById('impact-query-only').onclick=()=>{const query=impactByKey[focusedImpactQuery];activeImpactIds=new Set(query?.memberships||[]);renderImpactControls();renderImpactMap()};
const total=fs.total;document.getElementById('progress').innerHTML=`<div class="progress"><i class="pdone" style="width:${100*fs.done/total}%"></i><i class="ppartial" style="width:${100*fs.partial/total}%"></i><i class="ppending" style="width:${100*fs.pending/total}%"></i></div><div class="legend"><span class="qgood">${fs.done} done</span><span class="qwarn">${fs.partial} partial</span><span class="qbad">${fs.pending} pending</span></div>`;
const grouped={};DATA.features.forEach(f=>(grouped[f.area]??=[]).push(f));document.getElementById('feature-summary').innerHTML=Object.entries(grouped).map(([area,items])=>`<div class="area-title">${area}</div>${items.map(f=>`<div class="feature"><div class="feature-title"><span class="badge ${f.status}">${f.status}</span><span>${f.name}</span></div><div class="evidence">${f.evidence}</div></div>`).join('')}`).join('');
document.getElementById('job-status').innerHTML=`<div class="grid three"><div class="kpi"><div class="value">${job.tableCount}/21</div><div class="label">Parquet tables</div></div><div class="kpi"><div class="value">${job.queryCount}</div><div class="label">JOB queries</div></div><div class="kpi"><div class="value">${job.probeCount}</div><div class="label">Generated probes</div></div></div><h3>Readiness sequence</h3>${DATA.jobMilestones.map((m,i)=>`<div class="milestone"><span class="step">${i+1}</span><div><b>${m.name}</b> <span class="badge ${m.status}">${m.status}</span><div class="small">${m.detail}</div></div></div>`).join('')}`;
document.getElementById('suite-picker').innerHTML=DATA.suites.map(suite=>`<label><input type="checkbox" value="${suite.id}" checked>${suite.label}</label>`).join('');
const plotData=DATA.plotArtifacts,plotKindSelector=document.getElementById('plot-kind');const artifactLink=(artifact,label)=>artifact.available?`<a href="${artifact.href}" target="_blank" rel="noopener">${label}</a>`:`<a class="disabled">${label} · not generated</a>`;function renderPlotArtifacts(){const selection=plotData.selections[activeSuiteKey()],previous=plotKindSelector.value;if(!selection){document.getElementById('plot-grid').innerHTML='<div class="plot-placeholder">No generated artifact exists for this suite selection.</div>';return}const firstPlots=selection.normalizations[0].artifacts.filter(artifact=>artifact.kind!=='CSV');plotKindSelector.innerHTML=firstPlots.map((artifact,index)=>`<option value="${index}">${artifact.name}</option>`).join('');plotKindSelector.value=previous!==''&&firstPlots[Number(previous)]?previous:'0';const plotIndex=Number(plotKindSelector.value)||0;document.getElementById('plot-grid').innerHTML=selection.normalizations.map(level=>{const plots=level.artifacts.filter(artifact=>artifact.kind!=='CSV'),artifact=plots[plotIndex]||plots[0],summary=level.artifacts.find(row=>row.kind==='CSV');return`<article class="plot-panel"><h3>${level.label}</h3><div class="plot-description">${level.description}</div><div class="plot-frame">${artifact.available?`<img src="${artifact.href}" alt="${activeSuiteLabel()}: ${level.label}: ${artifact.name}">`:`<div class="plot-placeholder">This plot has not been generated. Run <code>just regression-all</code>.</div>`}</div><div class="plot-actions">${artifactLink(artifact,'Open full-size SVG')}${artifactLink(summary,'Summary CSV')}</div></article>`}).join('');const csv=selection.comparisonCsv,csvLink=document.getElementById('comparison-csv-link');csvLink.href=csv.href;csvLink.style.pointerEvents=csv.available?'auto':'none';csvLink.style.opacity=csv.available?'1':'.45';csvLink.textContent=csv.available?'Open selected-suite comparison CSV':'Selected-suite comparison CSV · not generated'}plotKindSelector.addEventListener('change',renderPlotArtifacts);renderPlotArtifacts();
const qsel=document.getElementById('query-filter'),jsel=document.getElementById('join-filter');function populateQueryFilter(){const previous=qsel.value,queries=DATA.queries.filter(query=>activeSuites.has(query.suite));qsel.innerHTML='<option value="">All queries</option>'+queries.map(query=>`<option value="${query.key}">${query.label}</option>`).join('');qsel.value=queries.some(query=>query.key===previous)?previous:''}populateQueryFilter();DATA.joinCounts.forEach(j=>jsel.insertAdjacentHTML('beforeend',`<option value="${j}">${j}</option>`));
let sortKey='query',sortAsc=true;document.querySelectorAll('th[data-sort]').forEach(th=>th.onclick=()=>{const k=th.dataset.sort;if(sortKey===k)sortAsc=!sortAsc;else{sortKey=k;sortAsc=true}renderTable()});
function renderTable(){const engine=document.getElementById('engine').value;let rows=[...(DATA.engines[engine].rows||[])];const q=qsel.value,j=jsel.value,t=Number(document.getElementById('threshold').value||1),s=document.getElementById('search').value.toLowerCase();rows=rows.filter(r=>activeSuites.has(r.suite)&&(!q||`${r.suite}/${r.query}`===q)&&(!j||String(r.joinCount)===j)&&r.max>=t&&(!s||JSON.stringify(r).toLowerCase().includes(s)));rows.sort((a,b)=>{let av=a[sortKey],bv=b[sortKey];if(sortKey==='query'){const an=Number((av.match(/\d+/)||[9999])[0]),bn=Number((bv.match(/\d+/)||[9999])[0]);if(an!==bn)return(sortAsc?1:-1)*(an-bn)}return(sortAsc?1:-1)*(av>bv?1:av<bv?-1:0)});document.getElementById('row-count').textContent=`${rows.length} query/join-size buckets shown for ${activeSuiteLabel()}`;document.getElementById('metrics-body').innerHTML=rows.map(r=>`<tr><td><span class="badge done">${suiteById[r.suite]?.label||r.suite}</span></td><td><b>${r.query}</b><div class="small">${(r.signals||[]).join(', ')}</div></td><td class="num">${r.joinCount}</td><td class="num">${r.n}</td><td class="num ${qClass(r.min)}">${fmt(r.min)}</td><td class="num ${qClass(r.gm)}">${fmt(r.gm)}</td><td class="num ${qClass(r.median)}">${fmt(r.median)}</td><td class="num ${qClass(r.max)}">${fmt(r.max)}</td><td class="num ${qClass(r.over10>=20?10:1)}">${r.over10.toFixed(1)}%</td><td>${r.operators}</td><td><b>${r.worstOperator}</b><div class="small">path ${r.worstPath}<br>est ${fmt(Number(r.worstEstimated))} / actual ${fmt(Number(r.worstActual))}</div></td><td class="cause">${r.cause}</td><td class="improve">${r.improvement}</td></tr>`).join('')}
['engine','query-filter','join-filter','threshold','search'].forEach(id=>document.getElementById(id).addEventListener(id==='search'||id==='threshold'?'input':'change',renderTable));qsel.addEventListener('change',()=>{const query=impactByKey[qsel.value];if(!query)return;if(!query.highError&&!showAllImpactQueries)showAllImpactQueries=true;focusedImpactQuery=query.key;renderImpactControls();renderImpactMap()});document.querySelectorAll('#suite-picker input').forEach(input=>input.addEventListener('change',()=>{const selected=[...document.querySelectorAll('#suite-picker input:checked')].map(node=>node.value);if(!selected.length){input.checked=true;return}activeSuites=new Set(selected);refreshSuiteLabels();populateQueryFilter();renderImpactControls();renderImpactMap();renderPlotArtifacts();renderTable()}));renderTable();
const done=DATA.features.filter(f=>f.status==='done'),pending=DATA.features.filter(f=>f.status!=='done');document.getElementById('done-list').innerHTML=done.map(f=>`<div class="feature"><div class="feature-title"><span class="badge done">done</span>${f.name}</div><div class="evidence">${f.evidence}</div></div>`).join('');document.getElementById('pending-list').innerHTML=pending.map(f=>`<div class="feature"><div class="feature-title"><span class="badge ${f.status}">${f.status}</span>${f.name}</div><div class="evidence">${f.evidence}</div></div>`).join('');document.getElementById('sources').innerHTML=DATA.sources.map(s=>`<li><code>${s.path}</code> — ${s.role}</li>`).join('');
</script>
</body></html>'''.replace("__DATA__", data_json)


def main() -> None:
    args = parse_args()
    optd_report = load_json(args.optd_report)
    postgres_report = load_json(args.postgres_report)
    suite_aliases = {
        "tpch": "tpch",
        "tpc-h": "tpch",
        "tpcds": "tpcds",
        "tpc-ds": "tpcds",
        "job": "job",
    }
    legacy_suite = suite_aliases.get(args.suite_label.strip().lower(), "tpch")
    report_rows = [*(optd_report or []), *(postgres_report or [])]
    suite_order = {"tpch": 0, "job": 1, "tpcds": 2}
    query_pairs = sorted(
        {
            (str(row.get("suite") or legacy_suite).lower(), str(row["query"]))
            for row in report_rows
        },
        key=lambda item: (
            suite_order.get(item[0], len(suite_order)),
            item[0],
            natural_query_key(item[1]),
        ),
    )
    query_paths = {"tpch": args.queries, "job": args.job_queries}
    query_sql: dict[tuple[str, str], str] = {
        (suite, query): first_slt_query(
            query_paths.get(suite, args.queries) / f"{query}.slt"
        )
        for suite, query in query_pairs
    }
    engines = {
        "optd": report_payload(optd_report, query_sql, "optd", legacy_suite),
        "postgres": report_payload(
            postgres_report, query_sql, "postgres", legacy_suite
        ),
    }
    statuses = Counter(feature["status"] for feature in FEATURES)
    job = build_job_status(args)
    all_join_counts = sorted(
        {
            row["joinCount"]
            for engine in engines.values()
            for row in engine.get("rows", [])
        }
    )
    suite_labels = {"tpch": "TPC-H", "tpcds": "TPC-DS", "job": "JOB"}
    suites = sorted(
        {suite for suite, _ in query_pairs},
        key=lambda suite: (suite_order.get(suite, len(suite_order)), suite),
    )
    suite_label = " + ".join(suite_labels.get(suite, suite.upper()) for suite in suites)
    data = {
        "generatedAt": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
        "benchmarkSuite": suite_label,
        "features": FEATURES,
        "featureStatus": {
            "done": statuses["done"],
            "partial": statuses["partial"],
            "pending": statuses["pending"],
            "total": len(FEATURES),
        },
        "engines": engines,
        "featureImpact": build_feature_impact(engines["optd"], query_sql),
        "plotArtifacts": build_plot_artifacts(args.output, suites),
        "suites": [
            {"id": suite, "label": suite_labels.get(suite, suite.upper())}
            for suite in suites
        ],
        "queries": [
            {
                "suite": suite,
                "query": query,
                "key": f"{suite}/{query}",
                "label": f"{suite_labels.get(suite, suite.upper())} · {query}",
            }
            for suite, query in query_pairs
        ],
        "joinCounts": all_join_counts,
        "job": job,
        "jobMilestones": [
            {
                "name": "Dataset and suite",
                "status": "done" if job["tableCount"] == 21 and job["queryCount"] == 113 else "partial",
                "detail": f"{job['tableCount']} Parquet tables and {job['queryCount']} SQLLogicTest queries are present.",
            },
            {
                "name": "Canonical logical-probe manifest",
                "status": "partial" if job["manifestAvailable"] else "pending",
                "detail": f"{job['probeCount']} connected probes generated through {job['maxJoinCount']} joins; backend SQL validation is still required.",
            },
            {
                "name": "PostgreSQL JOB loader and chosen-plan collector",
                "status": "done",
                "detail": "The JOB Parquet loader, benchmark indexes, and suite-aware chosen-plan collector are implemented; this is still distinct from canonical matched-probe execution.",
            },
            {
                "name": "optd root-probe collector",
                "status": "pending",
                "detail": "The existing collector measures chosen-plan subtrees, not one estimate per canonical probe ID.",
            },
            {
                "name": "Paired results and dashboard metrics",
                "status": "pending",
                "detail": "Requires equal probe IDs and one shared actual cardinality before engine comparison.",
            },
        ],
        "sources": [
            {"path": str(args.optd_report), "role": "Current optd per-subtree q-error measurements"},
            {"path": str(args.postgres_report), "role": "Current PostgreSQL chosen-plan q-error measurements"},
            {"path": str(args.queries), "role": f"{suite_label} SQL text used for query diagnostics"},
            {"path": "optd/core/src/analysis.rs", "role": "Authoritative estimator implementation and TODO markers"},
            {"path": "docs/cardinality_estimation_v1.md", "role": "Estimator model and implementation checklist"},
            {"path": "todo-for-completenes.local.md", "role": "Current local implementation backlog"},
            {"path": "optd/connectors/datafusion/src/cardinality_regression.rs", "role": "optd regression measurement semantics"},
            {"path": "optd/connectors/datafusion/scripts/postgres_cardinality_regression.py", "role": "PostgreSQL chosen-plan measurement semantics"},
            {"path": "optd/connectors/datafusion/scripts/job_logical_probes.py", "role": "Current JOB canonical-probe generator"},
            {"path": str(args.job_manifest), "role": "Generated JOB probe inventory"},
        ],
    }
    encoded = json.dumps(data, separators=(",", ":")).replace("</", "<\\/")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(html_template(encoded))
    print(f"wrote {args.output}")


if __name__ == "__main__":
    main()
