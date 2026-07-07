#!/usr/bin/env python3
"""Create GitHub-sized sampled datasets for the survey repository."""

from __future__ import annotations

import argparse
import csv
import hashlib
import random
import shutil
from dataclasses import dataclass
from pathlib import Path


DEFAULT_LIMIT = 95_000_000
DEFAULT_SEED = 20260707
DEFAULT_CHUNKS = 16


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    sources: tuple[str, ...]
    output: str
    glob: bool = False


DATASETS = [
    DatasetSpec("HDFS", ("HDFS/HDFS_full.log",), "HDFS/HDFS_full.log"),
    DatasetSpec("Hadoop", ("Hadoop/**/*.log",), "Hadoop/Hadoop_sample.log", True),
    DatasetSpec("Spark", ("Spark/**/*.log",), "Spark/Spark_sample.log", True),
    DatasetSpec("Zookeeper", ("Zookeeper/Zookeeper_full.log",), "Zookeeper/Zookeeper_full.log"),
    DatasetSpec("OpenStack", ("OpenStack/OpenStack_full.log",), "OpenStack/OpenStack_full.log"),
    DatasetSpec("BGL", ("BGL/BGL_full.log",), "BGL/BGL_full.log"),
    DatasetSpec("HPC", ("HPC/HPC_full.log",), "HPC/HPC_full.log"),
    DatasetSpec("Thunderbird", ("Thunderbird/Thunderbird_full.log",), "Thunderbird/Thunderbird_full.log"),
    DatasetSpec("Windows", ("Windows.log",), "Windows.log"),
    DatasetSpec("Linux", ("Linux/Linux_full.log",), "Linux/Linux_full.log"),
    DatasetSpec("Mac", ("Mac/Mac_full.log",), "Mac/Mac_full.log"),
    DatasetSpec("Android_v1", ("Android/Android_full.log",), "Android/Android_full.log"),
    DatasetSpec("HealthApp", ("HealthApp/HealthApp_full.log",), "HealthApp/HealthApp_full.log"),
    DatasetSpec("Apache", ("Apache/Apache_full.log",), "Apache/Apache_full.log"),
    DatasetSpec("OpenSSH", ("OpenSSH/OpenSSH_full.log",), "OpenSSH/OpenSSH_full.log"),
    DatasetSpec("Proxifier", ("Proxifier/Proxifier_full.log",), "Proxifier/Proxifier_full.log"),
    DatasetSpec("spark-event-logs", ("spark-event-logs/app-*",), "spark-event-logs/app-sampled", True),
    DatasetSpec("elasticsearch", ("elasticsearch/elasticsearch.log",), "elasticsearch/elasticsearch.log"),
    DatasetSpec("cockroachdb", ("cockroachdb/cockroach.node1.log",), "cockroachdb/cockroach.node1.log"),
    DatasetSpec("oceanbase", ("oceanbase/*.log*",), "oceanbase/observer.sample.log", True),
]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(8 * 1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def stable_seed(base_seed: int, label: str) -> int:
    digest = hashlib.blake2b(label.encode("utf-8"), digest_size=4).digest()
    return base_seed + int.from_bytes(digest, "big")


def resolve_sources(source_root: Path, spec: DatasetSpec) -> list[Path]:
    paths: list[Path] = []
    for pattern in spec.sources:
        if spec.glob:
            paths.extend(sorted(source_root.glob(pattern)))
        else:
            paths.append(source_root / pattern)
    return [path for path in paths if path.is_file()]


def copy_or_sample(
    sources: list[Path],
    destination: Path,
    limit: int,
    seed: int,
    chunks: int,
    label: str,
) -> tuple[str, int, int]:
    source_size = sum(path.stat().st_size for path in sources)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if source_size <= limit and len(sources) == 1:
        shutil.copy2(sources[0], destination)
        return "full_copy", source_size, destination.stat().st_size

    weighted = [(path, path.stat().st_size) for path in sources if path.stat().st_size > 0]
    if not weighted:
        raise RuntimeError(f"{label}: no non-empty source files")
    total = sum(size for _, size in weighted)
    rng = random.Random(stable_seed(seed, label))
    chunk_budget = max(1, limit // max(1, chunks))
    plans: list[tuple[str, int, Path]] = []
    for _ in range(max(1, chunks) * 3):
        pick = rng.randrange(total)
        cumulative = 0
        chosen = weighted[-1][0]
        chosen_size = weighted[-1][1]
        for path, size in weighted:
            cumulative += size
            if pick < cumulative:
                chosen = path
                chosen_size = size
                break
        start = rng.randrange(chosen_size) if chosen_size > 1 else 0
        plans.append((str(chosen), start, chosen))
    plans.sort(key=lambda item: (item[0], item[1]))

    written = 0
    with destination.open("wb") as output:
        for _, start, source in plans:
            if written >= limit:
                break
            budget = min(chunk_budget, limit - written)
            chunk_written = 0
            with source.open("rb") as input_stream:
                if start > 0:
                    input_stream.seek(start)
                    input_stream.readline()
                while written < limit and chunk_written < budget:
                    line = input_stream.readline()
                    if not line:
                        break
                    if len(line) > limit - written:
                        break
                    output.write(line)
                    written += len(line)
                    chunk_written += len(line)
                    if line and not line.endswith(b"\n") and written < limit:
                        output.write(b"\n")
                        written += 1
                        chunk_written += 1
    if written == 0:
        raise RuntimeError(f"{label}: sampling produced no complete lines")
    return "random_line_chunks", source_size, destination.stat().st_size


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--limit-bytes", type=int, default=DEFAULT_LIMIT)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--chunks", type=int, default=DEFAULT_CHUNKS)
    args = parser.parse_args()

    source_root = Path(args.source_root).resolve()
    output_root = Path(args.output_root).resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    manifest_path = Path(args.manifest).resolve()
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    rows: list[dict[str, str | int]] = []
    for spec in DATASETS:
        sources = resolve_sources(source_root, spec)
        destination = output_root / spec.output
        if not sources:
            rows.append({
                "dataset": spec.name,
                "status": "missing_source",
                "source_files": 0,
                "source_size_bytes": 0,
                "uploaded_path": spec.output,
                "uploaded_size_bytes": 0,
                "sample_method": "",
                "seed": "",
                "sha256": "",
            })
            continue
        method, source_size, uploaded_size = copy_or_sample(
            sources, destination, args.limit_bytes, args.seed, args.chunks, spec.name
        )
        rows.append({
            "dataset": spec.name,
            "status": "ok",
            "source_files": len(sources),
            "source_size_bytes": source_size,
            "uploaded_path": spec.output.replace("\\", "/"),
            "uploaded_size_bytes": uploaded_size,
            "sample_method": method,
            "seed": args.seed if method != "full_copy" else "",
            "sha256": sha256_file(destination),
        })
        print(f"{spec.name}: {method}, {uploaded_size} bytes")

    with manifest_path.open("w", newline="", encoding="utf-8-sig") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
