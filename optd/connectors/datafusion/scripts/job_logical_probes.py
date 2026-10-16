#!/usr/bin/env python3
"""Generate canonical connected-subexpression probes for JOB queries."""

from __future__ import annotations

import argparse
import itertools
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Relation:
    table: str
    alias: str


@dataclass(frozen=True)
class Predicate:
    sql: str
    aliases: frozenset[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--queries",
        required=True,
        type=Path,
        help="One JOB .sql/.slt file or a directory containing JOB queries",
    )
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        help="Path for the generated probe manifest JSON",
    )
    parser.add_argument(
        "--max-joins",
        type=int,
        help="Generate only probes with at most this many joins",
    )
    parser.add_argument("--limit", type=int, help="Use only the first N query files")
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


def load_queries(path: Path) -> list[tuple[str, str]]:
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
            raise ValueError(f"{query_path} contains no query")
        queries.append((query_path.stem, sql))
    if not queries:
        raise ValueError(f"{path} contains no JOB queries")
    return queries


def top_level_keyword(sql: str, keyword: str, start: int = 0) -> int:
    keyword = keyword.upper()
    depth = 0
    in_string = False
    index = start
    while index < len(sql):
        character = sql[index]
        if in_string:
            if character == "'":
                if index + 1 < len(sql) and sql[index + 1] == "'":
                    index += 2
                    continue
                in_string = False
            index += 1
            continue
        if character == "'":
            in_string = True
            index += 1
            continue
        if character == "(":
            depth += 1
            index += 1
            continue
        if character == ")":
            depth -= 1
            index += 1
            continue
        if depth == 0 and sql[index : index + len(keyword)].upper() == keyword:
            before = sql[index - 1] if index else " "
            after_index = index + len(keyword)
            after = sql[after_index] if after_index < len(sql) else " "
            if not (before.isalnum() or before == "_") and not (
                after.isalnum() or after == "_"
            ):
                return index
        index += 1
    return -1


def split_top_level(sql: str, delimiter: str) -> list[str]:
    parts = []
    start = 0
    depth = 0
    in_string = False
    index = 0
    while index < len(sql):
        character = sql[index]
        if in_string:
            if character == "'":
                if index + 1 < len(sql) and sql[index + 1] == "'":
                    index += 2
                    continue
                in_string = False
            index += 1
            continue
        if character == "'":
            in_string = True
        elif character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
        elif depth == 0 and sql.startswith(delimiter, index):
            parts.append(sql[start:index].strip())
            start = index + len(delimiter)
            index = start
            continue
        index += 1
    parts.append(sql[start:].strip())
    return parts


def split_conjunctions(sql: str) -> list[str]:
    parts = []
    start = 0
    depth = 0
    in_string = False
    between_pending = False
    index = 0
    while index < len(sql):
        character = sql[index]
        if in_string:
            if character == "'":
                if index + 1 < len(sql) and sql[index + 1] == "'":
                    index += 2
                    continue
                in_string = False
            index += 1
            continue
        if character == "'":
            in_string = True
            index += 1
            continue
        if character == "(":
            depth += 1
            index += 1
            continue
        if character == ")":
            depth -= 1
            index += 1
            continue
        if depth == 0 and (character.isalpha() or character == "_"):
            end = index + 1
            while end < len(sql) and (sql[end].isalnum() or sql[end] == "_"):
                end += 1
            word = sql[index:end].upper()
            if word == "BETWEEN":
                between_pending = True
            elif word == "AND":
                if between_pending:
                    between_pending = False
                else:
                    parts.append(sql[start:index].strip())
                    start = end
            index = end
            continue
        index += 1
    parts.append(sql[start:].strip())
    return [part for part in parts if part]


def without_string_literals(sql: str) -> str:
    output = []
    in_string = False
    index = 0
    while index < len(sql):
        character = sql[index]
        if in_string:
            if character == "'":
                if index + 1 < len(sql) and sql[index + 1] == "'":
                    index += 2
                    continue
                in_string = False
            output.append(" ")
        elif character == "'":
            in_string = True
            output.append(" ")
        else:
            output.append(character)
        index += 1
    return "".join(output)


def parse_job_query(sql: str) -> tuple[list[Relation], list[Predicate]]:
    from_index = top_level_keyword(sql, "FROM")
    where_index = top_level_keyword(sql, "WHERE", from_index + 4)
    if from_index < 0 or where_index < 0:
        raise ValueError("JOB query must have top-level FROM and WHERE clauses")
    from_sql = sql[from_index + len("FROM") : where_index].strip()
    where_sql = sql[where_index + len("WHERE") :].strip().removesuffix(";")

    relations = []
    for item in split_top_level(from_sql, ","):
        match = re.fullmatch(
            r'\s*([A-Za-z_][A-Za-z0-9_$]*)\s+(?:AS\s+)?([A-Za-z_][A-Za-z0-9_$]*)\s*',
            item,
            flags=re.IGNORECASE,
        )
        if not match:
            raise ValueError(f"unsupported JOB relation syntax: {item}")
        relations.append(Relation(match.group(1), match.group(2)))

    aliases = {relation.alias for relation in relations}
    predicates = []
    for predicate_sql in split_conjunctions(where_sql):
        unquoted = without_string_literals(predicate_sql)
        referenced = frozenset(
            alias
            for alias in aliases
            if re.search(rf"\b{re.escape(alias)}\s*\.", unquoted, re.IGNORECASE)
        )
        if not referenced:
            raise ValueError(f"predicate references no known alias: {predicate_sql}")
        predicates.append(Predicate(predicate_sql, referenced))
    return relations, predicates


def is_connected(subset: frozenset[str], predicates: list[Predicate]) -> bool:
    if len(subset) <= 1:
        return True
    adjacency = {alias: set() for alias in subset}
    for predicate in predicates:
        referenced = predicate.aliases & subset
        if len(referenced) < 2 or not predicate.aliases <= subset:
            continue
        for left, right in itertools.combinations(referenced, 2):
            adjacency[left].add(right)
            adjacency[right].add(left)
    visited = set()
    pending = [next(iter(subset))]
    while pending:
        alias = pending.pop()
        if alias in visited:
            continue
        visited.add(alias)
        pending.extend(adjacency[alias] - visited)
    return visited == set(subset)


def generate_probes(
    query_name: str,
    sql: str,
    max_joins: int | None,
) -> list[dict[str, object]]:
    relations, predicates = parse_job_query(sql)
    relation_by_alias = {relation.alias: relation for relation in relations}
    aliases = sorted(relation_by_alias)
    max_relations = len(aliases)
    if max_joins is not None:
        max_relations = min(max_relations, max_joins + 1)

    probes = []
    for relation_count in range(1, max_relations + 1):
        for selected in itertools.combinations(aliases, relation_count):
            subset = frozenset(selected)
            if not is_connected(subset, predicates):
                continue
            applicable = [
                predicate for predicate in predicates if predicate.aliases <= subset
            ]
            from_clause = ", ".join(
                f"{relation_by_alias[alias].table} AS {alias}" for alias in selected
            )
            where_clause = " AND ".join(
                f"({predicate.sql})" for predicate in applicable
            )
            probe_sql = f"SELECT 1 FROM {from_clause}"
            if where_clause:
                probe_sql += f" WHERE {where_clause}"
            probe_id = f"{query_name}/{'-'.join(selected)}"
            probes.append(
                {
                    "probe_id": probe_id,
                    "query": query_name,
                    "relations": list(selected),
                    "join_count": relation_count - 1,
                    "operator": "join" if relation_count > 1 else "scan",
                    "sql": probe_sql,
                }
            )
    return probes


def main() -> None:
    args = parse_args()
    if args.max_joins is not None and args.max_joins < 0:
        raise ValueError("--max-joins must be non-negative")
    queries = load_queries(args.queries)
    if args.limit is not None:
        queries = queries[: args.limit]
    probes = []
    for query_name, sql in queries:
        query_probes = generate_probes(query_name, sql, args.max_joins)
        print(f"{query_name}: {len(query_probes)} probes", file=sys.stderr)
        probes.extend(query_probes)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(probes, indent=2) + "\n")
    print(f"wrote {len(probes)} probes to {args.output}", file=sys.stderr)


if __name__ == "__main__":
    main()
