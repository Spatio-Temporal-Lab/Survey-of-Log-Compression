#!/usr/bin/env python3

import csv
import json
import sys
from pathlib import Path


def main() -> int:
    if len(sys.argv) != 3:
        print(f"Usage: {Path(sys.argv[0]).name} INPUT.csv OUTPUT.jsonl", file=sys.stderr)
        return 2

    input_path = Path(sys.argv[1])
    output_path = Path(sys.argv[2])
    with input_path.open("r", encoding="utf-8", newline="") as input_file:
        reader = csv.DictReader(input_file)
        with output_path.open("w", encoding="utf-8", newline="\n") as output_file:
            for row in reader:
                json.dump(row, output_file, ensure_ascii=False, separators=(",", ":"))
                output_file.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
