#!/usr/bin/env python3

import argparse
import os

from LogBlock.LogBlock import LogBlock


DATASETS = {
    "Apache": {
        "format": (
            r"\[<Date:3> <Month:3> <Day:2> <Hour:2>:<Minute:2>:<Second:2> "
            r"<Year:4>\] \[<Level>\] <Content>"
        ),
        "regex": [r"(\d+\.){3}\d+"],
    },
}


def main():
    parser = argparse.ArgumentParser(description="Run LogBlock on one log file.")
    parser.add_argument("input", help="Input log file")
    parser.add_argument("output", help="Output directory")
    parser.add_argument("--dataset", choices=DATASETS, default="Apache")
    parser.add_argument(
        "--disable-step",
        type=int,
        choices=range(1, 5),
        help="Disable one LogBlock heuristic (1-4)",
    )
    args = parser.parse_args()

    input_path = os.path.abspath(args.input)
    settings = DATASETS[args.dataset]
    processor = LogBlock(
        log_format=settings["format"],
        indir=os.path.dirname(input_path),
        logName=os.path.basename(input_path),
        outdir=os.path.abspath(args.output),
        rex=settings["regex"],
        disable_step=args.disable_step,
    )
    processor.run()

    with open(input_path, "rb") as input_file:
        input_lines = sum(1 for _ in input_file)
    output_files = [
        os.path.join(processor.savePath, name)
        for name in sorted(os.listdir(processor.savePath))
        if os.path.isfile(os.path.join(processor.savePath, name))
    ]
    output_bytes = sum(os.path.getsize(path) for path in output_files)

    print(f"input_lines={input_lines}")
    print(f"parsed_rows={len(processor.df_log)}")
    print(f"failed_lines={len(processor.failtomatchList)}")
    print(f"output_files={len(output_files)}")
    print(f"output_bytes={output_bytes}")
    for path in output_files:
        print(path)


if __name__ == "__main__":
    main()
