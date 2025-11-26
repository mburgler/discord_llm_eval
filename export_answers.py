#!/usr/bin/env python3
"""Export choices and gold/prediction pairs from the LEXam parquet file."""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq


def main() -> None:
    source = Path("details_community_lexam-en-idk_0_2025-11-18T18-17-53.491808.parquet")
    if not source.exists():
        raise SystemExit(f"Parquet file not found: {source}")

    out_dir = Path("analysis/answers")
    out_dir.mkdir(parents=True, exist_ok=True)

    table = pq.read_table(source)
    rows = table.to_pylist()

    choices_out = []
    gp_out = []

    for idx, row in enumerate(rows, start=1):
        doc = row.get("doc", {})
        choices_out.append(
            {
                "number": idx,
                "choices": doc.get("choices", []),
            }
        )
        specific = doc.get("specific", {}) or {}
        gp_out.append(
            {
                "number": idx,
                "extracted_golds": specific.get("extracted_golds", []),
                "extracted_predictions": specific.get("extracted_predictions", []),
            }
        )

    (out_dir / "choices.json").write_text(
        json.dumps(choices_out, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (out_dir / "golds_predictions.json").write_text(
        json.dumps(gp_out, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    print(f"Wrote {len(rows)} choice entries to {out_dir/'choices.json'}")
    print(f"Wrote {len(rows)} gold/prediction entries to {out_dir/'golds_predictions.json'}")


if __name__ == "__main__":
    main()
