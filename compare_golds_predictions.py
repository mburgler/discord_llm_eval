#!/usr/bin/env python3
"""Compute match stats between extracted golds and predictions."""

from __future__ import annotations

import json
from pathlib import Path
from typing import List


def load_pairs(path: Path) -> List[dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError(f"Expected list in {path}")
    return data


def main() -> None:
    src = Path("analysis/answers/golds_predictions.json")
    if not src.exists():
        raise SystemExit(f"File not found: {src}")

    records = load_pairs(src)

    total = len(records)
    matches = 0
    mismatches = []

    for rec in records:
        golds = rec.get("extracted_golds", [])
        preds = rec.get("extracted_predictions", [])
        if golds == preds:
            matches += 1
        else:
            mismatches.append(
                {
                    "number": rec.get("number"),
                    "extracted_golds": golds,
                    "extracted_predictions": preds,
                }
            )

    print(f"Total: {total}")
    print(f"Matches: {matches}")
    print(f"Mismatches: {len(mismatches)}")
    accuracy = matches / total if total else 0.0
    print(f"Accuracy: {accuracy:.4f}")

    if mismatches:
        print("\nExamples of mismatches (up to 10):")
        for rec in mismatches[:10]:
            print(
                f"#{rec['number']}: gold={rec['extracted_golds']} pred={rec['extracted_predictions']}"
            )


if __name__ == "__main__":
    main()
