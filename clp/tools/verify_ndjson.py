#!/usr/bin/env python3

import json
import sys
from collections import Counter
from pathlib import Path


def load_records(path: Path) -> Counter[str]:
    records: Counter[str] = Counter()
    with path.open("r", encoding="utf-8") as input_file:
        for line_number, line in enumerate(input_file, 1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(f"{path}:{line_number}: {error}") from error
            canonical = json.dumps(
                record,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            records[canonical] += 1
    return records


def main() -> int:
    if len(sys.argv) != 3:
        print(f"Usage: {Path(sys.argv[0]).name} EXPECTED.jsonl ACTUAL.jsonl", file=sys.stderr)
        return 2

    expected_path = Path(sys.argv[1])
    actual_path = Path(sys.argv[2])
    expected = load_records(expected_path)
    actual = load_records(actual_path)
    if expected != actual:
        print(
            f"Mismatch: expected {sum(expected.values())} records, "
            f"got {sum(actual.values())}",
            file=sys.stderr,
        )
        return 1

    print(f"OK: {sum(expected.values())} JSON records match")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
