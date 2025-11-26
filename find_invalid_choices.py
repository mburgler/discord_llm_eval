#!/usr/bin/env python3
"""Find documents whose choices include options outside A–E and check fewshot_samples consistency."""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq

ALLOWED = {"A", "B", "C", "D", "E"}


def normalize(choice: str) -> str:
    return choice.strip().upper()


def main() -> None:
    source = Path("details_community_lexam-en-idk_0_2025-11-18T18-17-53.491808.parquet")
    if not source.exists():
        raise SystemExit(f"Parquet file not found: {source}")

    table = pq.read_table(source)
    rows = table.to_pylist()

    bad = []
    fewshot_values = []
    for idx, row in enumerate(rows, start=1):
        choices = row.get("doc", {}).get("choices", [])
        normalized = [normalize(c) for c in choices if isinstance(c, str)]
        if any(c not in ALLOWED for c in normalized):
            bad.append({"number": idx, "choices": choices})

        fs = row.get("doc", {}).get("fewshot_samples", [])
        fewshot_values.append((idx, fs))

    print(f"Total rows: {len(rows)}")
    print(f"Rows with non-A–E choices: {len(bad)}")
    if bad:
        print("\nListing all deviating rows:")
        for entry in bad:
            print(f"#{entry['number']}: {entry['choices']}")

    # Check fewshot_samples consistency
    unique_fs = {}
    for idx, fs in fewshot_values:
        key = json_dumps(fs)
        unique_fs.setdefault(key, []).append(idx)

    if len(unique_fs) == 1:
        print("\nfewshot_samples: consistent across all rows.")
    else:
        print(f"\nfewshot_samples deviations: {len(unique_fs)} unique values found.")
        for val, indices in unique_fs.items():
            sample_rows = indices[:10]
            more = "..." if len(indices) > 10 else ""
            print(f"- Occurs in {len(indices)} rows ({', '.join(map(str, sample_rows))}{more}): {val}")


def json_dumps(obj) -> str:
    import json

    return json.dumps(obj, ensure_ascii=False, sort_keys=True)


if __name__ == "__main__":
    main()
