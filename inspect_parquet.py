#!/usr/bin/env python3
"""Inspect a Parquet file and print a quick summary."""

from __future__ import annotations

import argparse
from pathlib import Path

import pyarrow.parquet as pq


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Show schema, column stats, and a small preview of a Parquet file."
    )
    parser.add_argument("path", type=Path, help="Path to the Parquet file to inspect")
    parser.add_argument(
        "--rows",
        type=int,
        default=5,
        help="Number of rows to preview (default: 5)",
    )
    parser.add_argument("--csv", type=Path, help="Optional path to export the full table as CSV")
    parser.add_argument("--json", type=Path, help="Optional path to export the full table as newline-delimited JSON")
    parser.add_argument(
        "--retrieve",
        type=int,
        help="Retrieve a specific row (1-based) and print it as pretty JSON",
    )
    parser.add_argument(
        "--retrieve-save",
        type=Path,
        help="Optional path to save the retrieved row as JSON (named row_<n>.json)",
    )
    args = parser.parse_args()

    if not args.path.exists():
        parser.error(f"File not found: {args.path}")

    table = pq.read_table(args.path)

    print(f"File: {args.path}")
    print(f"Rows: {table.num_rows:,} | Columns: {table.num_columns}")

    print("\nSchema:")
    print(table.schema)

    print("\nColumns:")
    for name, column in zip(table.schema.names, table.itercolumns()):
        print(f"- {name}: type={column.type}, nulls={column.null_count}")

    if table.schema.metadata:
        print("\nMetadata:")
        for key, value in table.schema.metadata.items():
            print(f"- {key.decode()}: {value.decode(errors='replace')}")

    preview_count = min(max(args.rows, 0), table.num_rows)
    if preview_count:
        print(f"\nFirst {preview_count} rows:")
        for idx, row in enumerate(table.slice(0, preview_count).to_pylist()):
            print(f"{idx}: {row}")
    else:
        print("\nNo rows to preview.")

    if args.retrieve is not None:
        idx = args.retrieve - 1  # user-friendly 1-based indexing
        if idx < 0 or idx >= table.num_rows:
            parser.error(f"Row {args.retrieve} is out of range (1..{table.num_rows})")
        import json

        row = table.slice(idx, 1).to_pylist()[0]
        print(f"\nRow {args.retrieve} as JSON:")
        pretty = json.dumps(row, indent=2, ensure_ascii=False)
        print(pretty)

        if args.retrieve_save:
            out_dir = args.retrieve_save
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / f"row_{args.retrieve}.json"
            out_path.write_text(pretty, encoding="utf-8")
            print(f"Saved row {args.retrieve} to: {out_path}")

    if args.csv or args.json:
        import json

        rows = table.to_pylist()

        if args.csv:
            import csv

            # Serialize nested objects as JSON strings for CSV friendliness.
            def to_serializable(row: dict) -> dict:
                return {k: json.dumps(v, ensure_ascii=False) for k, v in row.items()}

            serializable_rows = [to_serializable(r) for r in rows]
            fieldnames = list(serializable_rows[0].keys()) if serializable_rows else ["doc", "metric", "model_response"]

            with args.csv.open("w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(serializable_rows)
            print(f"\nCSV written to: {args.csv}")

        if args.json:
            # NDJSON: one JSON object per line
            with args.json.open("w", encoding="utf-8") as f:
                for row in rows:
                    json.dump(row, f, ensure_ascii=False)
                    f.write("\n")
            print(f"NDJSON written to: {args.json}")


if __name__ == "__main__":
    main()
