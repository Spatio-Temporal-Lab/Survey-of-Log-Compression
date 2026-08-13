"""Build and query a LogCloud index from a plain-text log file.

Each input line is stored as one row in a Parquet column named ``log`` because
Rottnest's LogCloud API indexes Parquet columns rather than raw text files.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from rottnest.indices.logcloud_index import index_files_logcloud, search_index_logcloud


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path, help="plain-text .log file")
    parser.add_argument(
        "--work-dir",
        type=Path,
        required=True,
        help="empty directory in which Parquet and index files will be written",
    )
    parser.add_argument("--query", default="syslogd", help="substring to search for")
    parser.add_argument("--limit", type=int, default=10, help="maximum search result count")
    parser.add_argument(
        "--max-lines",
        type=int,
        default=None,
        help="only convert the first N lines (useful for a quick smoke test)",
    )
    parser.add_argument("--name", default="dataset", help="index file prefix")
    return parser.parse_args()


def read_lines(source: Path, max_lines: int | None) -> list[str]:
    lines: list[str] = []
    with source.open("r", encoding="utf-8", errors="replace") as stream:
        for line_number, line in enumerate(stream, start=1):
            if max_lines is not None and line_number > max_lines:
                break
            lines.append(line.rstrip("\r\n"))
    return lines


def main() -> None:
    args = parse_args()
    source = args.source.resolve()
    work_dir = args.work_dir.resolve()

    if not source.is_file():
        raise FileNotFoundError(source)
    work_dir.mkdir(parents=True, exist_ok=True)
    if any(work_dir.iterdir()):
        raise RuntimeError(f"work directory must be empty: {work_dir}")

    lines = read_lines(source, args.max_lines)
    if not lines:
        raise RuntimeError(f"no log lines found in {source}")

    parquet_path = work_dir / "input.parquet"
    table = pa.table({"log": pa.array(lines, type=pa.large_string())})
    pq.write_table(table, parquet_path, compression="zstd")
    print(f"Converted {len(lines):,} lines to {parquet_path}")

    previous_cwd = Path.cwd()
    try:
        os.chdir(work_dir)
        index_files_logcloud([str(parquet_path)], "log", name=args.name)
        result = search_index_logcloud([args.name], args.query, args.limit)
    finally:
        os.chdir(previous_cwd)

    if result is None or result.height == 0:
        raise RuntimeError(f"query returned no rows: {args.query!r}")
    if not all(args.query in value for value in result["log"].to_list()):
        raise AssertionError("LogCloud returned a row that does not contain the query")

    index_files = sorted(work_dir.glob(f"{args.name}.*"))
    print("Index files:")
    for path in index_files:
        print(f"  {path.name}: {path.stat().st_size:,} bytes")
    print(result)


if __name__ == "__main__":
    main()
