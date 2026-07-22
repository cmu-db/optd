#!/usr/bin/env python3
"""Summarize inclusive and self samples from a presymbolicated Samply profile."""

from __future__ import annotations

import argparse
import gzip
import json
from collections import Counter
from pathlib import Path


def load_json(path: Path):
    opener = gzip.open if path.suffix == ".gz" else Path.open
    with opener(path, "rt") as source:
        return json.load(source)


def sidecar_path(profile_path: Path) -> Path:
    stem = profile_path.with_suffix("") if profile_path.suffix == ".gz" else profile_path
    return stem.with_name(f"{stem.name}.syms.json")


def symbol_maps(profile, sidecar):
    strings = sidecar["string_table"]
    by_debug_name = {
        entry["debug_name"]: {
            address: strings[entry["symbol_table"][symbol_index]["symbol"]]
            for address, symbol_index in entry["known_addresses"]
        }
        for entry in sidecar["data"]
    }
    return {
        index: by_debug_name.get(library["debugName"], {})
        for index, library in enumerate(profile["libs"])
    }


def frame_name(thread, frame_index, symbols_by_library):
    frame_table = thread["frameTable"]
    function_table = thread["funcTable"]
    resource_table = thread["resourceTable"]
    strings = thread["stringArray"]

    function_index = frame_table["func"][frame_index]
    fallback = strings[function_table["name"][function_index]]
    resource_index = function_table["resource"][function_index]
    if resource_index is None or resource_index < 0:
        return fallback
    library_index = resource_table["lib"][resource_index]
    if library_index is None or library_index < 0:
        return fallback
    address = frame_table["address"][frame_index]
    return symbols_by_library.get(library_index, {}).get(address, fallback)


def sample_stack(thread, stack_index, symbols_by_library):
    stack_table = thread["stackTable"]
    frames = []
    while stack_index is not None:
        frames.append(
            frame_name(thread, stack_table["frame"][stack_index], symbols_by_library)
        )
        stack_index = stack_table["prefix"][stack_index]
    return frames


def summarize(profile, sidecar, stack_filter: str):
    symbols_by_library = symbol_maps(profile, sidecar)
    inclusive = Counter()
    self_samples = Counter()
    thread_samples = Counter()
    total = 0

    for thread in profile["threads"]:
        for stack_index in thread["samples"]["stack"]:
            if stack_index is None:
                continue
            frames = sample_stack(thread, stack_index, symbols_by_library)
            if stack_filter and not any(stack_filter in frame for frame in frames):
                continue
            total += 1
            thread_samples[f"{thread['name']} ({thread['tid']})"] += 1
            self_samples[frames[0]] += 1
            inclusive.update(set(frames))

    return total, inclusive, self_samples, thread_samples


def print_table(title: str, counts: Counter, total: int, limit: int):
    print(f"## {title}\n")
    print("| Function | Samples | Share |")
    print("|:---|---:|---:|")
    for function, count in counts.most_common(limit):
        escaped = function.replace("|", "\\|").replace("`", "'")
        print(f"| `{escaped}` | {count:,} | {count / total:.1%} |")
    print()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("profile", type=Path)
    parser.add_argument("--filter", default="JoinOrdering")
    parser.add_argument("--limit", type=int, default=25)
    args = parser.parse_args()

    profile = load_json(args.profile)
    symbols = load_json(sidecar_path(args.profile))
    total, inclusive, self_samples, thread_samples = summarize(
        profile, symbols, args.filter
    )
    if total == 0:
        raise SystemExit(f"no samples matched stack filter {args.filter!r}")

    print(f"# Samply summary: {args.profile.name}\n")
    print(f"Stack filter: `{args.filter}`. Matching samples: **{total:,}**.\n")
    print_table("Inclusive samples", inclusive, total, args.limit)
    print_table("Self samples", self_samples, total, args.limit)
    print_table("Matching threads", thread_samples, total, args.limit)


if __name__ == "__main__":
    main()
