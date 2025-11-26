#!/usr/bin/env python3
"""Spot deviations of a given field across all rows in the Parquet file."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List

import pyarrow.parquet as pq


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check if a specific field differs across rows and list deviations."
    )
    parser.add_argument(
        "field",
        help="Dot-separated path to the field (e.g., doc.fewshot_samples or model_response.text)",
    )
    parser.add_argument(
        "--path",
        type=Path,
        default=Path("details_community_lexam-en-idk_0_2025-11-18T18-17-53.491808.parquet"),
        help="Path to the Parquet file (default: details_community_lexam-en-idk_0_2025-11-18T18-17-53.491808.parquet)",
    )
    parser.add_argument(
        "--show",
        type=int,
        default=5,
        help="How many sample row numbers to show per unique value (default: 5)",
    )
    return parser.parse_args()


def traverse(row: Dict[str, Any], path: List[str]) -> Any:
    cur: Any = row
    for key in path:
        if isinstance(cur, dict) and key in cur:
            cur = cur[key]
        else:
            return "__MISSING__"
    return cur


def normalize(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def main() -> None:
    args = parse_args()
    if not args.path.exists():
        raise SystemExit(f"Parquet file not found: {args.path}")

    table = pq.read_table(args.path)
    rows = table.to_pylist()

    path_parts = args.field.split(".")
    buckets: Dict[str, List[int]] = {}
    values: Dict[str, Any] = {}

    for idx, row in enumerate(rows, start=1):
        val = traverse(row, path_parts)
        key = normalize(val)
        buckets.setdefault(key, []).append(idx)
        values[key] = val

    total = len(rows)
    unique = len(buckets)

    print(f"Checked field: {args.field}")
    print(f"Total rows: {total}")
    print(f"Unique values: {unique}")

    if unique == 1:
        print("No deviations; all rows share the same value.")
        only_key = next(iter(values))
        print(f"Value: {only_key}")
        return

    print("\nDeviations found:")
    # for key, indices in buckets.items():
    #     sample_rows = indices[: args.show]
    #     more = "..." if len(indices) > args.show else ""
    #     print(f"- Occurs in {len(indices)} rows ({', '.join(map(str, sample_rows))}{more}): {key}")


if __name__ == "__main__":
    main()
