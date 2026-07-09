#!/usr/bin/env python3
"""Bridge a plain log file into the CLP Python logging library."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from clp_logging.handlers import CLPFileHandler


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("clp_loglib_file_adapter")
    logger.handlers.clear()
    logger.propagate = False
    logger.setLevel(logging.INFO)

    handler = CLPFileHandler(output_path)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)

    with input_path.open("rb") as stream:
        for raw_line in stream:
            logger.info(raw_line.rstrip(b"\r\n").decode("utf-8", errors="replace"))

    logger.removeHandler(handler)
    handler.close()
    logging.shutdown()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
