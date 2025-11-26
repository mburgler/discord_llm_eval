#!/usr/bin/env python3
"""Count how many choices each document has and summarize distribution."""

from __future__ import annotations

from collections import Counter
from pathlib import Path

import pyarrow.parquet as pq


def main() -> None:
    source = Path("details_community_lexam-en-idk_0_2025-11-18T18-17-53.491808.parquet")
    if not source.exists():
        raise SystemExit(f"Parquet file not found: {source}")

    table = pq.read_table(source)
    rows = table.to_pylist()

    counts = Counter()
    per_row = []

    for idx, row in enumerate(rows, start=1):
        choices = row.get("doc", {}).get("choices", [])
        n = len(choices) if isinstance(choices, list) else 0
        counts[n] += 1
        per_row.append((idx, n, choices))

    print(f"Total rows: {len(rows)}")
    print("Choice count distribution (choices -> rows):")
    for num, freq in sorted(counts.items()):
        print(f"- {num}: {freq}")

    # List rows whose choice count differs from the mode
    if counts:
        mode_count = counts.most_common(1)[0][0]
        outliers = [(i, n, c) for (i, n, c) in per_row if n != mode_count]
        print(f"\nRows differing from mode ({mode_count} choices): {len(outliers)}")
        for i, n, c in outliers:
            print(f"#{i}: {n} choices -> {c}")


if __name__ == "__main__":
    main()
