#!/usr/bin/env python3
"""Run the log-compression experiment matrix and keep going after failures."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import random
import shlex
import shutil
import signal
import statistics
import subprocess
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


RESULT_FIELDS = [
    "case_id",
    "experiment",
    "method",
    "dataset",
    "query_id",
    "parameter",
    "parameter_value",
    "status",
    "stage",
    "input_path",
    "source_size_bytes",
    "sampled",
    "sample_method",
    "sample_seed",
    "raw_size_bytes",
    "line_count",
    "compressed_size_bytes",
    "cr_compressed_over_raw",
    "compression_time_s",
    "decompression_time_s",
    "compression_throughput_mib_s",
    "decompression_throughput_mib_s",
    "ingest_events_s",
    "query_n",
    "query_p50_ms",
    "query_p95_ms",
    "query_mean_ms",
    "query_std_ms",
    "peak_rss_kib",
    "correct",
    "cache_clear",
    "error_type",
    "error_message",
    "started_at",
    "finished_at",
    "work_dir",
    "stdout_log",
    "stderr_log",
    "notes",
]


class Unsupported(RuntimeError):
    pass


class MissingDependency(RuntimeError):
    pass


@dataclass
class StageResult:
    name: str
    returncode: int
    elapsed_s: float
    peak_rss_kib: int | None
    stdout_path: Path
    stderr_path: Path
    timed_out: bool = False


def utc_now() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def slug(value: Any) -> str:
    text = str(value)
    clean = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in text)
    return clean[:160] or "none"


def q(value: os.PathLike[str] | str) -> str:
    return shlex.quote(str(value))


def mib_per_second(size_bytes: int | None, seconds: float | None) -> float | None:
    if not size_bytes or not seconds or seconds <= 0:
        return None
    return size_bytes / 1024 / 1024 / seconds


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def total_size(paths: Iterable[Path]) -> int:
    seen: set[Path] = set()
    size = 0
    for path in paths:
        if not path.exists():
            continue
        if path.is_dir():
            members = (member for member in path.rglob("*") if member.is_file())
        else:
            members = [path]
        for member in members:
            resolved = member.resolve()
            if resolved not in seen:
                seen.add(resolved)
                size += member.stat().st_size
    return size


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(8 * 1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


class ExperimentRunner:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.root = Path(args.root).resolve()
        self.suite = Path(__file__).resolve().parent
        self.config = json.loads((self.suite / "experiment_config.json").read_text("utf-8"))
        self.datasets = {item["name"]: item for item in self.config["datasets"]}
        self.methods = {item["name"]: item for item in self.config["methods"]}
        self.defaults = self.config["defaults"]
        self.timeout = args.timeout_seconds or int(self.defaults["stage_timeout_seconds"])
        self.max_input_bytes = (
            int(args.max_input_bytes)
            if args.max_input_bytes is not None
            else int(self.defaults.get("max_input_bytes", 100 * 1024 * 1024))
        )
        self.sample_chunks = int(args.sample_chunks or self.defaults.get("sample_chunks", 16))
        self.sample_seed = int(args.sample_seed if args.sample_seed is not None else self.defaults.get("sample_seed", 20260707))
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        self.run_dir = (
            Path(args.resume).resolve()
            if args.resume
            else self.root / "experiment_results" / f"run-{stamp}"
        )
        self.work_root = self.run_dir / "work"
        self.logs_root = self.run_dir / "logs"
        self.cache_root = self.root / "experiment_results" / "_input_cache"
        self.query_cache = self.run_dir / "_query_cache"
        self.results_path = self.run_dir / "results.csv"
        self.events_path = self.run_dir / "events.jsonl"
        self.exceptions_path = self.run_dir / "exceptions.jsonl"
        self.plan_path = self.run_dir / "plan.json"
        self.summary_path = self.run_dir / "summary.json"
        self.completed: set[str] = set()
        self.build_attempted: dict[str, tuple[bool, str]] = {}
        self.input_meta: dict[Path, tuple[int, int]] = {}
        self.sample_sources_size: dict[Path, int] = {}
        self.interrupted = False

    def emit(self, event: str, **payload: Any) -> None:
        record = {"time": utc_now(), "event": event, **payload}
        with self.events_path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
        print(f"[{record['time']}] {event}: {payload.get('case_id', payload.get('message', ''))}", flush=True)

    def record_exception(self, case: dict[str, Any], exc: BaseException, stage: str = "") -> None:
        record = {
            "time": utc_now(),
            "case": case,
            "stage": stage,
            "type": type(exc).__name__,
            "message": str(exc),
            "traceback": traceback.format_exc(),
        }
        with self.exceptions_path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")

    def record_issue(self, case: dict[str, Any], issue_type: str, message: str, stage: str) -> None:
        record = {
            "time": utc_now(),
            "case": case,
            "stage": stage,
            "type": issue_type,
            "message": message,
            "traceback": "",
        }
        with self.exceptions_path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")

    def append_result(self, row: dict[str, Any]) -> None:
        exists = self.results_path.exists() and self.results_path.stat().st_size > 0
        normalized = {field: row.get(field, "") for field in RESULT_FIELDS}
        with self.results_path.open("a", newline="", encoding="utf-8-sig") as stream:
            writer = csv.DictWriter(stream, fieldnames=RESULT_FIELDS)
            if not exists:
                writer.writeheader()
            writer.writerow(normalized)

    def load_completed(self) -> None:
        if not self.results_path.exists():
            return
        with self.results_path.open("r", encoding="utf-8-sig", newline="") as stream:
            for row in csv.DictReader(stream):
                if row.get("case_id"):
                    self.completed.add(row["case_id"])

    def make_case_id(self, case: dict[str, Any]) -> str:
        parts = [
            case["experiment"],
            case["method"],
            case["dataset"],
            case.get("query_id", ""),
            case.get("parameter", ""),
            case.get("parameter_value", ""),
        ]
        return "__".join(slug(part) for part in parts)

    def plan(self) -> list[dict[str, Any]]:
        selected_experiments = {x.strip() for x in self.args.experiments.split(",") if x.strip()}
        selected_methods = (
            {x.strip() for x in self.args.methods.split(",") if x.strip()}
            if self.args.methods
            else set(self.methods)
        )
        selected_datasets = (
            {x.strip() for x in self.args.datasets.split(",") if x.strip()}
            if self.args.datasets
            else set(self.datasets)
        )
        unknown_methods = selected_methods - set(self.methods)
        unknown_datasets = selected_datasets - set(self.datasets)
        if unknown_methods:
            raise ValueError(f"未知方法: {sorted(unknown_methods)}")
        if unknown_datasets:
            raise ValueError(f"未知数据集: {sorted(unknown_datasets)}")

        if self.args.smoke_test:
            selected_experiments = {"main"}
            selected_datasets = {"Apache"}
            selected_methods &= {
                name for name, method in self.methods.items() if method["adapter"] != "unavailable"
            }

        cases: list[dict[str, Any]] = []
        if "main" in selected_experiments:
            for method in self.methods:
                if method not in selected_methods:
                    continue
                for dataset in self.datasets:
                    if dataset in selected_datasets:
                        cases.append({"experiment": "main", "method": method, "dataset": dataset})

        if "sensitivity" in selected_experiments:
            sensitivity_dataset = self.defaults["sensitivity_dataset"]
            scale_dataset = self.defaults["scale_dataset"]
            for method_name, method in self.methods.items():
                if method_name not in selected_methods:
                    continue
                if sensitivity_dataset in selected_datasets:
                    for parameter in method.get("parameter_sweeps", []):
                        for value in self.config["sensitivity"][parameter]:
                            cases.append(
                                {
                                    "experiment": "sensitivity",
                                    "method": method_name,
                                    "dataset": sensitivity_dataset,
                                    "parameter": parameter,
                                    "parameter_value": value,
                                }
                            )
                if scale_dataset in selected_datasets:
                    for value in self.config["sensitivity"]["raw_size_bytes"]:
                        cases.append(
                            {
                                "experiment": "sensitivity",
                                "method": method_name,
                                "dataset": scale_dataset,
                                "parameter": "raw_size_bytes",
                                "parameter_value": value,
                            }
                        )

        if "query" in selected_experiments:
            for method_name, method in self.methods.items():
                if method_name not in selected_methods or not method.get("query_capable"):
                    continue
                for dataset in self.datasets:
                    if dataset not in selected_datasets:
                        continue
                    for query in self.config["queries"]:
                        cases.append(
                            {
                                "experiment": "query",
                                "method": method_name,
                                "dataset": dataset,
                                "query_id": query["id"],
                            }
                        )

        for case in cases:
            case["case_id"] = self.make_case_id(case)
        return cases

    def prepare_run(self, cases: list[dict[str, Any]]) -> None:
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.work_root.mkdir(exist_ok=True)
        self.logs_root.mkdir(exist_ok=True)
        self.cache_root.mkdir(parents=True, exist_ok=True)
        self.query_cache.mkdir(exist_ok=True)
        self.plan_path.write_text(
            json.dumps(
                {
                    "created_at": utc_now(),
                    "root": str(self.root),
                    "config": self.config,
                    "cases": cases,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        self.load_completed()

    def dataset_file(self, dataset_name: str, sample_limit: int | None = None) -> Path:
        spec = self.datasets[dataset_name]
        if "file" in spec:
            path = self.root / spec["file"]
            if not path.is_file():
                raise FileNotFoundError(f"数据集文件不存在: {path}")
            return path

        matches = sorted(path for path in self.root.glob(spec["glob"]) if path.is_file())
        if not matches:
            raise FileNotFoundError(f"数据集 glob 无匹配: {spec['glob']}")
        total = sum(path.stat().st_size for path in matches)
        if sample_limit and sample_limit > 0 and total > sample_limit:
            return self.random_sample_paths(matches, sample_limit, dataset_name)
        cache = self.cache_root / f"{slug(dataset_name)}.log"
        manifest = self.cache_root / f"{slug(dataset_name)}.manifest.json"
        signature = [
            {"path": str(path.relative_to(self.root)), "size": path.stat().st_size, "mtime_ns": path.stat().st_mtime_ns}
            for path in matches
        ]
        if cache.exists() and manifest.exists():
            try:
                if json.loads(manifest.read_text("utf-8")) == signature:
                    return cache
            except (OSError, json.JSONDecodeError):
                pass
        temp = cache.with_suffix(".tmp")
        with temp.open("wb") as output:
            for path in matches:
                with path.open("rb") as source:
                    shutil.copyfileobj(source, output, length=8 * 1024 * 1024)
                if path.stat().st_size and not self.file_ends_with_newline(path):
                    output.write(b"\n")
        temp.replace(cache)
        manifest.write_text(json.dumps(signature, ensure_ascii=False, indent=2), "utf-8")
        return cache

    @staticmethod
    def file_ends_with_newline(path: Path) -> bool:
        if path.stat().st_size == 0:
            return True
        with path.open("rb") as stream:
            stream.seek(-1, os.SEEK_END)
            return stream.read(1) == b"\n"

    def prefix_file(self, source: Path, limit: int, label: str) -> Path:
        if source.stat().st_size < limit:
            raise Unsupported(
                f"{source.name} 只有 {source.stat().st_size} 字节，小于数据规模实验要求的 {limit} 字节"
            )
        destination = self.cache_root / "scales" / f"{slug(label)}-{limit}.log"
        if destination.exists() and 0 < destination.stat().st_size <= limit:
            return destination
        destination.parent.mkdir(parents=True, exist_ok=True)
        temp = destination.with_suffix(".tmp")
        written = 0
        with source.open("rb") as input_stream, temp.open("wb") as output:
            while written < limit:
                line = input_stream.readline()
                if not line or written + len(line) > limit:
                    break
                output.write(line)
                written += len(line)
        if written == 0:
            raise Unsupported("无法生成按完整行截取的数据规模样本")
        temp.replace(destination)
        return destination

    def smoke_file(self, source: Path, dataset: str) -> Path:
        limit = min(source.stat().st_size, 5 * 1024 * 1024)
        if source.stat().st_size <= limit:
            return source
        return self.prefix_file(source, limit, f"smoke-{dataset}")

    def stable_sample_seed(self, label: str) -> int:
        digest = hashlib.blake2b(label.encode("utf-8"), digest_size=4).digest()
        return self.sample_seed + int.from_bytes(digest, "big")

    def random_sample_paths(self, sources: list[Path], limit: int, label: str) -> Path:
        destination = self.cache_root / "samples" / f"{slug(label)}-{limit}-seed{self.sample_seed}.log"
        manifest = destination.with_suffix(".manifest.json")
        signature = {
            "limit": limit,
            "seed": self.sample_seed,
            "chunks": self.sample_chunks,
            "sources": [
                {
                    "path": str(path.relative_to(self.root)) if path.is_relative_to(self.root) else str(path),
                    "size": path.stat().st_size,
                    "mtime_ns": path.stat().st_mtime_ns,
                }
                for path in sources
            ],
        }
        if destination.exists() and manifest.exists():
            try:
                if json.loads(manifest.read_text("utf-8")) == signature and destination.stat().st_size <= limit:
                    self.sample_sources_size[destination.resolve()] = sum(item["size"] for item in signature["sources"])
                    return destination
            except (OSError, json.JSONDecodeError):
                pass

        destination.parent.mkdir(parents=True, exist_ok=True)
        weighted = [(path, path.stat().st_size) for path in sources if path.stat().st_size > 0]
        total = sum(size for _, size in weighted)
        if total <= 0:
            raise Unsupported("输入数据为空，无法采样")

        rng = random.Random(self.stable_sample_seed(label))
        chunks = max(1, self.sample_chunks)
        chunk_budget = max(1, limit // chunks)
        plans: list[tuple[str, int, Path]] = []
        for _ in range(chunks * 3):
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

        temp = destination.with_suffix(".tmp")
        written = 0
        with temp.open("wb") as output:
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
            raise Unsupported("随机采样没有得到完整日志行，请降低采样块数或检查输入文件格式")
        temp.replace(destination)
        manifest.write_text(json.dumps(signature, ensure_ascii=False, indent=2), "utf-8")
        self.sample_sources_size[destination.resolve()] = total
        return destination

    def sampled_input_file(self, source: Path, dataset: str) -> Path:
        if self.max_input_bytes <= 0 or source.stat().st_size <= self.max_input_bytes:
            return source
        return self.random_sample_paths([source], self.max_input_bytes, dataset)

    def sample_metrics(self, source: Path, input_file: Path) -> dict[str, Any]:
        input_resolved = input_file.resolve()
        sampled = source.resolve() != input_resolved or input_resolved in self.sample_sources_size
        return {
            "input_path": str(input_file),
            "source_size_bytes": self.sample_sources_size.get(input_resolved, source.stat().st_size),
            "sampled": sampled,
            "sample_method": "random_line_chunks" if sampled else "none",
            "sample_seed": self.sample_seed if sampled else "",
        }

    def metadata(self, path: Path) -> tuple[int, int]:
        resolved = path.resolve()
        if resolved not in self.input_meta:
            size = path.stat().st_size
            proc = subprocess.run(
                ["wc", "-l", str(path)],
                text=True,
                capture_output=True,
                check=True,
            )
            self.input_meta[resolved] = (size, int(proc.stdout.split()[0]))
        return self.input_meta[resolved]

    def stage_dir(self, case: dict[str, Any]) -> Path:
        directory = self.work_root / case["case_id"]
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    def parse_peak_rss(self, path: Path) -> int | None:
        if not path.exists():
            return None
        for line in path.read_text("utf-8", errors="replace").splitlines():
            if "Maximum resident set size (kbytes)" in line:
                try:
                    return int(line.rsplit(":", 1)[1].strip())
                except ValueError:
                    return None
        return None

    def run_command(
        self,
        case: dict[str, Any],
        stage: str,
        command: str,
        cwd: Path,
        timeout: int | None = None,
        measure: bool = True,
    ) -> StageResult:
        log_prefix = self.logs_root / case["case_id"]
        stdout_path = log_prefix.with_name(log_prefix.name + f".{slug(stage)}.stdout.log")
        stderr_path = log_prefix.with_name(log_prefix.name + f".{slug(stage)}.stderr.log")
        resource_path = log_prefix.with_name(log_prefix.name + f".{slug(stage)}.resource.log")
        timeout = timeout or self.timeout
        argv = ["bash", "-lc", command]
        if measure and Path("/usr/bin/time").exists():
            argv = ["/usr/bin/time", "-v", "-o", str(resource_path), *argv]
        self.emit("stage_start", case_id=case["case_id"], stage=stage, command=command)
        start = time.perf_counter()
        with stdout_path.open("wb") as stdout, stderr_path.open("wb") as stderr:
            process = subprocess.Popen(
                argv,
                cwd=cwd,
                stdout=stdout,
                stderr=stderr,
                start_new_session=True,
            )
            timed_out = False
            try:
                returncode = process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                timed_out = True
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
                returncode = 124
        elapsed = time.perf_counter() - start
        result = StageResult(
            name=stage,
            returncode=returncode,
            elapsed_s=elapsed,
            peak_rss_kib=self.parse_peak_rss(resource_path),
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            timed_out=timed_out,
        )
        self.emit(
            "stage_finish",
            case_id=case["case_id"],
            stage=stage,
            returncode=returncode,
            elapsed_s=round(elapsed, 6),
            timed_out=timed_out,
        )
        if returncode != 0:
            tail = stderr_path.read_text("utf-8", errors="replace")[-3000:]
            if timed_out:
                raise TimeoutError(f"{stage} 超过 {timeout}s；stderr 尾部: {tail}")
            raise RuntimeError(f"{stage} 返回 {returncode}；stderr 尾部: {tail}")
        return result

    def method_root(self, method: dict[str, Any]) -> Path:
        runtime_dir = method.get("runtime_dir")
        if runtime_dir:
            runtime_path = Path(runtime_dir)
            if runtime_path.is_dir():
                return runtime_path
        fallback_dirs = {
            "denum": "Denum",
            "delog": "Delog",
            "cowic": "cowic",
            "logarchive": "log_archive_v0",
            "pbc": "pbc",
            "clp": "clp",
            "loggrep": "LogGrep",
            "logcrisp": "logcrisp-vendored-code",
            "elise": "ELISE-2021",
            "logblock": "suppmaterial-21-kundi-logblock",
        }
        return self.root / fallback_dirs[method["adapter"]]

    def binary_paths(self, method: dict[str, Any]) -> list[Path]:
        adapter = method["adapter"]
        base = self.method_root(method)
        mapping = {
            "denum": [base / "denum_compress"],
            "delog": [base / "Delog_compress", base / "decompress"],
            "cowic": [base / "bin/compressor_cmd_tool"],
            "logarchive": [base / "bin/Archiver"],
            "pbc": [base / "bin/pbc"],
            "clp": [base / "build/core/clp", base / "build/core/clg"],
            "loggrep": [
                base / "compression/THULR",
                base / "cmdline_loggrep/thulr_cmdline",
            ],
            "logcrisp": [
                base / "LogCrisp_trainer_var/Trainer",
                base / "LogCrisp_compression_var/Compressor",
                base / "LogCrisp_compression_var/decompressTest/DeCompressor",
            ],
        }
        paths = mapping.get(adapter, [])
        if adapter == "pbc" and method.get("defaults", {}).get("compress_method") == "pbc_fsst":
            paths = [*paths, base / "bin/pbc_fsst_file"]
        return paths

    def ensure_ready(self, method: dict[str, Any], case: dict[str, Any]) -> None:
        adapter = method["adapter"]
        binaries = self.binary_paths(method)
        if binaries and all(path.is_file() for path in binaries):
            return
        if not binaries:
            if adapter == "elise":
                self.require_python_modules(["numpy"], method.get("python", sys.executable))
            elif adapter == "logblock":
                self.require_python_modules(["pandas"], method.get("python", sys.executable))
            return
        cache_key = method["name"]
        if cache_key in self.build_attempted:
            ok, message = self.build_attempted[cache_key]
            if not ok:
                raise MissingDependency(message)
            return
        if self.args.no_build:
            message = "缺少可执行文件，且使用了 --no-build: " + ", ".join(str(x) for x in binaries)
            self.build_attempted[cache_key] = (False, message)
            raise MissingDependency(message)
        build_command = method.get("build")
        if not build_command:
            raise MissingDependency("缺少可执行文件且配置中没有构建命令")
        build_case = dict(case)
        build_case["case_id"] = f"_build__{slug(method['name'])}"
        try:
            self.run_command(build_case, "build", build_command, self.root, timeout=max(self.timeout, 3600))
        except Exception as exc:
            message = f"构建 {method['name']} 失败: {exc}"
            self.build_attempted[cache_key] = (False, message)
            raise MissingDependency(message) from exc
        missing = [str(path) for path in binaries if not path.is_file()]
        if missing:
            message = "构建命令成功但仍缺少可执行文件: " + ", ".join(missing)
            self.build_attempted[cache_key] = (False, message)
            raise MissingDependency(message)
        self.build_attempted[cache_key] = (True, "")

    @staticmethod
    def require_python_modules(modules: list[str], python: str) -> None:
        command = [python, "-c", "import " + ",".join(modules)]
        proc = subprocess.run(command, text=True, capture_output=True)
        if proc.returncode:
            raise MissingDependency(f"缺少 Python 模块 {modules}: {proc.stderr.strip()}")

    def link_input(self, work: Path, dataset_cli: str, input_file: Path) -> Path:
        target_dir = work / "Logs" / dataset_cli
        target_dir.mkdir(parents=True, exist_ok=True)
        target = target_dir / f"{dataset_cli}.log"
        if target.exists() or target.is_symlink():
            target.unlink()
        target.symlink_to(input_file.resolve())
        return target

    def sample_lines(self, source: Path, destination: Path, ratio: float) -> Path:
        _, lines = self.metadata(source)
        count = max(1, math.ceil(lines * ratio))
        if destination.exists():
            return destination
        with source.open("rb") as input_stream, destination.open("wb") as output:
            for _ in range(count):
                line = input_stream.readline()
                if not line:
                    break
                output.write(line)
        return destination

    def base_metrics(
        self,
        input_file: Path,
        method: dict[str, Any],
        compressed_paths: list[Path],
        compress: StageResult,
        decompress: StageResult | None = None,
        restored: Path | None = None,
        include_paths: list[Path] | None = None,
        notes: str = "",
    ) -> dict[str, Any]:
        raw_size, line_count = self.metadata(input_file)
        compressed_size = total_size(compressed_paths + (include_paths or []))
        if raw_size > 0 and compressed_size <= 0:
            raise RuntimeError("压缩阶段没有产生非空压缩产物")
        correct: bool | str = ""
        if restored and restored.is_file():
            correct = sha256_file(input_file) == sha256_file(restored)
        return {
            "status": "FAILED_CORRECTNESS" if correct is False else "OK",
            "stage": "verify" if correct is False else "complete",
            "raw_size_bytes": raw_size,
            "line_count": line_count,
            "compressed_size_bytes": compressed_size,
            "cr_compressed_over_raw": compressed_size / raw_size if raw_size else "",
            "compression_time_s": compress.elapsed_s,
            "decompression_time_s": decompress.elapsed_s if decompress else "",
            "compression_throughput_mib_s": mib_per_second(raw_size, compress.elapsed_s),
            "decompression_throughput_mib_s": (
                mib_per_second(raw_size, decompress.elapsed_s) if decompress else ""
            ),
            "ingest_events_s": (
                line_count / compress.elapsed_s if method.get("mode") == "streaming" and compress.elapsed_s else ""
            ),
            "peak_rss_kib": max(
                [value for value in [compress.peak_rss_kib, decompress.peak_rss_kib if decompress else None] if value is not None],
                default="",
            ),
            "correct": correct,
            "stdout_log": str(compress.stdout_path),
            "stderr_log": str(compress.stderr_path),
            "notes": (
                (notes + " " if notes else "")
                + ("解压结果的 SHA-256 与原始输入不一致。" if correct is False else "")
            ),
        }

    def run_adapter(
        self, case: dict[str, Any], method: dict[str, Any], input_file: Path, work: Path
    ) -> dict[str, Any]:
        adapter = method["adapter"]
        if adapter == "unavailable":
            raise MissingDependency(method["reason"])
        function = getattr(self, f"run_{adapter}")
        return function(case, method, input_file, work)

    def ensure_format_supported(self, case: dict[str, Any], method: dict[str, Any]) -> None:
        dataset_group = self.datasets[case["dataset"]].get("group", "")
        if "input_groups" in method:
            allowed = set(method["input_groups"])
        elif method["adapter"] in {"logarchive", "logblock", "elise", "denum", "cowic", "clp", "loggrep", "logcrisp"}:
            allowed = {"Text/半结构化"}
        else:
            allowed = set()
        if allowed and dataset_group not in allowed:
            raise Unsupported(
                f"format mismatch: {method['name']} is configured for {sorted(allowed)}, "
                f"but {case['dataset']} belongs to {dataset_group}"
            )

    def run_denum(self, case: dict[str, Any], method: dict[str, Any], input_file: Path, work: Path) -> dict[str, Any]:
        method_dir = self.method_root(method)
        cli_name = self.datasets[case["dataset"]].get("cli_name", case["dataset"])
        self.link_input(work, cli_name, input_file)
        block_lines = int(
            case.get("parameter_value")
            if case.get("parameter") == "block_lines"
            else method["defaults"]["block_lines"]
        )
        command = f"{q(method_dir / 'denum_compress')} {q(cli_name)} {block_lines} 1"
        compress = self.run_command(case, "compress", command, work)
        archives = list((work / "output" / cli_name).glob("compressed*.xz"))
        if not archives:
            raise RuntimeError("Denum 没有生成 compressed*.xz")
        return self.base_metrics(
            input_file,
            method,
            archives,
            compress,
            notes="Denum 当前 C++ 入口没有通用完整解压 CLI，因此解压指标留空。",
        )

    def run_delog(self, case: dict[str, Any], method: dict[str, Any], input_file: Path, work: Path) -> dict[str, Any]:
        method_dir = self.method_root(method)
        cli_name = self.datasets[case["dataset"]].get("cli_name", case["dataset"])
        self.link_input(work, cli_name, input_file)
        defaults = method["defaults"]
        block_lines = int(case.get("parameter_value") if case.get("parameter") == "block_lines" else defaults["block_lines"])
        threads = int(case.get("parameter_value") if case.get("parameter") == "threads" else defaults["threads"])
        compress_cmd = " ".join(
            [
                q(method_dir / "Delog_compress"),
                q(cli_name),
                "text",
                str(block_lines),
                str(threads),
                str(defaults["frequency_threshold"]),
                q(defaults["compression_kernel"]),
                q(defaults["processing_mode"]),
            ]
        )
        compress = self.run_command(case, "compress", compress_cmd, work)
        archive_dir = work / "output" / cli_name
        restored = work / f"{cli_name}.restored.log"
        decompress_cmd = f"{q(method_dir / 'decompress')} {q(archive_dir)} {q(restored)} {threads}"
        decompress = self.run_command(case, "decompress", decompress_cmd, work)
        return self.base_metrics(input_file, method, [archive_dir], compress, decompress, restored)

    def run_cowic(self, case: dict[str, Any], method: dict[str, Any], input_file: Path, work: Path) -> dict[str, Any]:
        binary = self.method_root(method) / "bin/compressor_cmd_tool"
        seed = self.sample_lines(input_file, work / "seed.log", float(method["defaults"]["sample_ratio"]))
        model = work / "model.mdl"
        archive = work / "archive"
        restored = work / "restored.log"
        training = self.run_command(case, "train", f"{q(binary)} -t {q(seed)} -m {q(model)}", work)
        compress = self.run_command(
            case, "compress", f"{q(binary)} -c {q(input_file)} -m {q(model)} -o {q(archive)}", work
        )
        decompress = self.run_command(
            case, "decompress", f"{q(binary)} -d {q(archive)} -m {q(model)} -o {q(restored)}", work
        )
        metrics = self.base_metrics(
            input_file,
            method,
            [work / "archive.dat", work / "archive.idx"],
            compress,
            decompress,
            restored,
            include_paths=list(work.glob("model.mdl*")),
            notes=f"模型训练时间 {training.elapsed_s:.6f}s；压缩大小包含模型和索引。",
        )
        metrics["peak_rss_kib"] = max(
            [x for x in [metrics["peak_rss_kib"] or None, training.peak_rss_kib] if x is not None],
            default="",
        )
        return metrics

    def run_clp(
        self, case: dict[str, Any], method: dict[str, Any], input_file: Path, work: Path
    ) -> dict[str, Any]:
        if case["experiment"] == "query":
            return self.run_clp_query(case, method, input_file, work)
        binary = self.method_root(method) / "build/core/clp"
        archive = work / "archive"
        restored_dir = work / "restored"
        defaults = method["defaults"]
        block_bytes = int(
            case.get("parameter_value")
            if case.get("parameter") == "block_bytes"
            else defaults["block_bytes"]
        )
        compress_cmd = (
            f"{q(binary)} c --compression-level {int(defaults['compression_level'])} "
            f"--target-encoded-file-size {block_bytes} "
            f"--remove-path-prefix {q(input_file.parent)} {q(archive)} {q(input_file)}"
        )
        compress = self.run_command(case, "compress", compress_cmd, work)
        decompress = self.run_command(
            case,
            "decompress",
            f"{q(binary)} x {q(archive)} {q(restored_dir)}",
            work,
        )
        restored_candidates = [
            path for path in restored_dir.rglob(input_file.name) if path.is_file()
        ]
        restored = restored_candidates[0] if len(restored_candidates) == 1 else None
        return self.base_metrics(
            input_file,
            method,
            [archive],
            compress,
            decompress,
            restored,
            notes="使用 /home/abebts130613 下已构建的 CLP 文本 CLI。",
        )

    def ensure_clp_query_archive(
        self, case: dict[str, Any], method: dict[str, Any], input_file: Path
    ) -> Path:
        cache = self.query_cache / "CLP" / slug(case["dataset"])
        archive = cache / "archive"
        marker = cache / "READY"
        if marker.exists() and archive.is_dir():
            return archive
        cache.mkdir(parents=True, exist_ok=True)
        binary = self.method_root(method) / "build/core/clp"
        defaults = method["defaults"]
        cache_case = dict(case)
        cache_case["case_id"] = f"_query_prepare__CLP__{slug(case['dataset'])}"
        command = (
            f"rm -rf {q(archive)} && {q(binary)} c "
            f"--compression-level {int(defaults['compression_level'])} "
            f"--target-encoded-file-size {int(defaults['block_bytes'])} "
            f"--remove-path-prefix {q(input_file.parent)} {q(archive)} {q(input_file)}"
        )
        self.run_command(cache_case, "compress_for_query", command, cache)
        if total_size([archive]) <= 0:
            raise RuntimeError("CLP 查询准备返回成功，但压缩目录为空")
        marker.write_text(utc_now(), "utf-8")
        return archive

    def run_clp_query(
        self, case: dict[str, Any], method: dict[str, Any], input_file: Path, work: Path
    ) -> dict[str, Any]:
        query = next(item for item in self.config["queries"] if item["id"] == case["query_id"])
        if "clp" not in query:
            raise Unsupported(f"CLP 文本 CLI 不支持 {case['query_id']} 的 {query['kind']} 统一语义")
        archive = self.ensure_clp_query_archive(case, method, input_file)
        binary = self.method_root(method) / "build/core/clg"
        repetitions = int(self.defaults["query_repetitions"])
        warmups = int(self.defaults["query_warmups"])
        elapsed_ms: list[float] = []
        output_hashes: list[str] = []
        cache_states: list[str] = []
        for index in range(warmups + repetitions):
            cleared, state = self.clear_cache()
            cache_states.append(state)
            if not cleared and not self.args.allow_warm_cache:
                raise Unsupported(
                    "无法按工作簿要求在每次查询前清空 OS page cache；可显式传 --allow-warm-cache 降级运行"
                )
            result = self.run_command(
                case,
                f"query_{case['query_id']}_{index + 1}",
                f"{q(binary)} -i {q(archive)} {q(query['clp'])}",
                work,
                measure=False,
            )
            if index >= warmups:
                elapsed_ms.append(result.elapsed_s * 1000)
                output_hashes.append(sha256_file(result.stdout_path))
        return {
            "status": "OK",
            "stage": "query",
            "raw_size_bytes": input_file.stat().st_size,
            "line_count": self.metadata(input_file)[1],
            "query_n": repetitions,
            "query_p50_ms": percentile(elapsed_ms, 0.50),
            "query_p95_ms": percentile(elapsed_ms, 0.95),
            "query_mean_ms": statistics.mean(elapsed_ms),
            "query_std_ms": statistics.pstdev(elapsed_ms),
            "correct": len(set(output_hashes)) == 1,
            "cache_clear": "; ".join(sorted(set(cache_states))),
            "notes": "使用 WSL 中已验证的 clg；correct 表示重复查询输出哈希一致。",
        }

    def run_logarchive(
        self, case: dict[str, Any], method: dict[str, Any], input_file: Path, work: Path
    ) -> dict[str, Any]:
        binary = self.method_root(method) / "bin/Archiver"
        archive = work / "archive.bin"
        restored = work / "restored.log"
        defaults = method["defaults"]
        compress_cmd = (
            f"{q(binary)} -c --jhistory {int(defaults['jhistory'])} "
            f"--buckets {int(defaults['buckets'])} --{defaults['strategy']} "
            f"< {q(input_file)} > {q(archive)}"
        )
        compress = self.run_command(case, "compress", compress_cmd, work)
        decompress = self.run_command(
            case, "decompress", f"{q(binary)} -d < {q(archive)} > {q(restored)}", work
        )
        return self.base_metrics(input_file, method, [archive], compress, decompress, restored)

    def run_pbc(self, case: dict[str, Any], method: dict[str, Any], input_file: Path, work: Path) -> dict[str, Any]:
        binary = self.method_root(method) / "bin/pbc"
        pattern = work / "patterns.pat"
        archive = work / "archive.pbc"
        restored = work / "restored.log"
        defaults = method["defaults"]
        train_cmd = (
            f"{q(binary)} --train-pattern -i {q(input_file)} -p {q(pattern)} "
            f"--compress-method {q(defaults['compress_method'])} "
            f"--pattern-size {int(defaults['pattern_size'])} "
            f"--train-data-number {int(defaults['train_data_number'])} "
            f"--train-thread-num {int(defaults['train_threads'])}"
        )
        training = self.run_command(case, "train", train_cmd, work)
        if defaults["compress_method"] == "pbc_fsst":
            helper = self.method_root(method) / "bin/pbc_fsst_file"
            compress = self.run_command(
                case,
                "compress",
                f"{q(helper)} -c {q(input_file)} {q(pattern)} {q(archive)}",
                work,
            )
            decompress = self.run_command(
                case,
                "decompress",
                f"{q(helper)} -d {q(archive)} {q(pattern)} {q(restored)}",
                work,
            )
            adapter_note = "PBC-F uses pbc_fsst_file so the archive is produced by PBC_FSST."
        else:
            compress = self.run_command(
                case,
                "compress",
                f"{q(binary)} --compress -i {q(input_file)} -p {q(pattern)} -o {q(archive)}",
                work,
            )
            decompress = self.run_command(
                case,
                "decompress",
                f"{q(binary)} --decompress -i {q(archive)} -p {q(pattern)} -o {q(restored)}",
                work,
            )
            adapter_note = "Generic pbc file CLI; compress-method is applied during pattern training."
        metrics = self.base_metrics(
            input_file,
            method,
            [archive],
            compress,
            decompress,
            restored,
            include_paths=[pattern],
            notes=f"{adapter_note} Pattern training time {training.elapsed_s:.6f}s; compressed size includes pattern file.",
        )
        metrics["peak_rss_kib"] = max(
            [x for x in [metrics["peak_rss_kib"] or None, training.peak_rss_kib] if x is not None],
            default="",
        )
        return metrics

    def run_elise(self, case: dict[str, Any], method: dict[str, Any], input_file: Path, work: Path) -> dict[str, Any]:
        script = self.method_root(method) / "elise_plain.py"
        python = method.get("python", "python3")
        if not script.is_file():
            raise MissingDependency(f"缺少 {script}")
        defaults = method["defaults"]
        model = work / "model.npz"
        archive = work / "archive.elise"
        restored = work / "restored.log"
        train_cmd = (
            f"{q(python)} {q(script)} train -i {q(input_file)} -m {q(model)} "
            f"--max-bytes {int(defaults['training_bytes'])} "
            f"--timesteps {int(defaults['timesteps'])} --hidden-size {int(defaults['hidden_size'])} "
            f"--batch-size {int(defaults['batch_size'])} --epochs {int(defaults['epochs'])} "
            f"--seed {int(defaults['seed'])}"
        )
        training = self.run_command(case, "train", train_cmd, work)
        compress = self.run_command(
            case,
            "compress",
            f"{q(python)} {q(script)} compress -i {q(input_file)} -m {q(model)} -o {q(archive)} "
            f"--streams {int(defaults['streams'])}",
            work,
        )
        decompress = self.run_command(
            case,
            "decompress",
            f"{q(python)} {q(script)} decompress -i {q(archive)} -m {q(model)} -o {q(restored)}",
            work,
        )
        metrics = self.base_metrics(
            input_file,
            method,
            [archive],
            compress,
            decompress,
            restored,
            include_paths=[model, Path(str(model) + ".json")],
            notes=f"模型训练时间 {training.elapsed_s:.6f}s；使用仓库现代纯文本入口，大小包含模型。",
        )
        metrics["peak_rss_kib"] = max(
            [x for x in [metrics["peak_rss_kib"] or None, training.peak_rss_kib] if x is not None],
            default="",
        )
        return metrics

    def run_logblock(
        self, case: dict[str, Any], method: dict[str, Any], input_file: Path, work: Path
    ) -> dict[str, Any]:
        if case["dataset"] != "Apache":
            raise Unsupported("独立 LogBlock 入口只定义 Apache 日志格式，未对其他数据集套用错误格式")
        script = self.method_root(method) / "run_logblock.py"
        python = method.get("python", "python3")
        output = work / "preprocessed"
        archive = work / "logblock.tar.xz"
        restored_dir = work / "unpacked"
        command = (
            f"{q(python)} {q(script)} {q(input_file)} {q(output)} --dataset Apache "
            f"&& tar -cJf {q(archive)} -C {q(output)} ."
        )
        compress = self.run_command(case, "preprocess_compress", command, work)
        decompress = self.run_command(
            case,
            "decompress_container",
            f"mkdir -p {q(restored_dir)} && tar -xJf {q(archive)} -C {q(restored_dir)}",
            work,
        )
        return self.base_metrics(
            input_file,
            method,
            [archive],
            compress,
            decompress,
            notes="LogBlock 公开包仅能解开预处理结果，不能完整还原原始日志；correct 留空。",
        )

    def split_loggrep(self, input_file: Path, chunks: Path, block_bytes: int) -> None:
        if chunks.exists() and any(chunks.iterdir()):
            return
        chunks.mkdir(parents=True, exist_ok=True)
        command = (
            f"split -C {block_bytes} -d -a 6 --additional-suffix=.log "
            f"{q(input_file)} {q(chunks / 'block-')}"
        )
        proc = subprocess.run(["bash", "-lc", command], text=True, capture_output=True)
        if proc.returncode:
            raise RuntimeError(f"LogGrep 分块失败: {proc.stderr}")

    def run_loggrep(
        self, case: dict[str, Any], method: dict[str, Any], input_file: Path, work: Path
    ) -> dict[str, Any]:
        if case["experiment"] == "query":
            return self.run_loggrep_query(case, method, input_file, work)
        block_bytes = int(
            case.get("parameter_value")
            if case.get("parameter") == "block_bytes"
            else method["defaults"]["block_bytes"]
        )
        chunks = work / "chunks"
        archive = work / "archive"
        script = self.method_root(method) / "compression/LogGrep-compression.py"
        compress_cmd = (
            f"mkdir -p {q(chunks)} && split -C {block_bytes} -d -a 6 "
            f"--additional-suffix=.log {q(input_file)} {q(chunks / 'block-')} "
            f"&& cd {q(script.parent)} && python3 {q(script.name)} -I {q(chunks)} -O {q(archive)}"
        )
        compress = self.run_command(case, "split_compress", compress_cmd, work)
        if total_size([archive]) <= 0:
            raise RuntimeError("LogGrep 命令返回成功，但压缩目录为空")
        return self.base_metrics(
            input_file,
            method,
            [archive],
            compress,
            notes="LogGrep 公开包不提供完整解压入口；分块时间计入压缩时间。",
        )

    def ensure_loggrep_query_archive(
        self, case: dict[str, Any], method: dict[str, Any], input_file: Path
    ) -> Path:
        cache = self.query_cache / "LogGrep" / slug(case["dataset"])
        archive = cache / "archive"
        marker = cache / "READY"
        if marker.exists() and archive.is_dir():
            return archive
        cache.mkdir(parents=True, exist_ok=True)
        chunks = cache / "chunks"
        script = self.method_root(method) / "compression/LogGrep-compression.py"
        block_bytes = int(method["defaults"]["block_bytes"])
        cache_case = dict(case)
        cache_case["case_id"] = f"_query_prepare__LogGrep__{slug(case['dataset'])}"
        command = (
            f"rm -rf {q(chunks)} {q(archive)} && mkdir -p {q(chunks)} "
            f"&& split -C {block_bytes} -d -a 6 --additional-suffix=.log "
            f"{q(input_file)} {q(chunks / 'block-')} "
            f"&& cd {q(script.parent)} && python3 {q(script.name)} -I {q(chunks)} -O {q(archive)}"
        )
        self.run_command(cache_case, "compress_for_query", command, cache)
        if total_size([archive]) <= 0:
            raise RuntimeError("LogGrep 查询准备返回成功，但压缩目录为空")
        marker.write_text(utc_now(), "utf-8")
        return archive

    def clear_cache(self) -> tuple[bool, str]:
        try:
            subprocess.run(["sync"], check=True, timeout=60)
            Path("/proc/sys/vm/drop_caches").write_text("3\n", encoding="ascii")
            return True, "cleared"
        except Exception as exc:
            return False, f"failed: {type(exc).__name__}: {exc}"

    def run_loggrep_query(
        self, case: dict[str, Any], method: dict[str, Any], input_file: Path, work: Path
    ) -> dict[str, Any]:
        query = next(item for item in self.config["queries"] if item["id"] == case["query_id"])
        if "loggrep" not in query:
            raise Unsupported(f"LogGrep 公开 CLI 不支持 {case['query_id']} 的 {query['kind']} 语义")
        archive = self.ensure_loggrep_query_archive(case, method, input_file)
        binary = self.method_root(method) / "cmdline_loggrep/thulr_cmdline"
        repetitions = int(self.defaults["query_repetitions"])
        warmups = int(self.defaults["query_warmups"])
        elapsed_ms: list[float] = []
        output_hashes: list[str] = []
        cache_states: list[str] = []
        for index in range(warmups + repetitions):
            cleared, state = self.clear_cache()
            cache_states.append(state)
            if not cleared and not self.args.allow_warm_cache:
                raise Unsupported(
                    "无法按工作簿要求在每次查询前清空 OS page cache；可显式传 --allow-warm-cache 降级运行"
                )
            stage = f"query_{case['query_id']}_{index + 1}"
            result = self.run_command(
                case,
                stage,
                f"{q(binary)} {q(archive)} {q(query['loggrep'])}",
                work,
                measure=False,
            )
            if index >= warmups:
                elapsed_ms.append(result.elapsed_s * 1000)
                output_hashes.append(sha256_file(result.stdout_path))
        return {
            "status": "OK",
            "stage": "query",
            "raw_size_bytes": input_file.stat().st_size,
            "line_count": self.metadata(input_file)[1],
            "query_n": repetitions,
            "query_p50_ms": percentile(elapsed_ms, 0.50),
            "query_p95_ms": percentile(elapsed_ms, 0.95),
            "query_mean_ms": statistics.mean(elapsed_ms),
            "query_std_ms": statistics.pstdev(elapsed_ms),
            "correct": len(set(output_hashes)) == 1,
            "cache_clear": "; ".join(sorted(set(cache_states))),
            "notes": "correct 表示重复查询输出哈希一致；公开 CLI 未提供统一 matched-count，无法和 rg/jq 自动核对。",
        }

    def run_logcrisp(
        self, case: dict[str, Any], method: dict[str, Any], input_file: Path, work: Path
    ) -> dict[str, Any]:
        method_dir = self.method_root(method)
        trainer = method_dir / "LogCrisp_trainer_var/Trainer"
        compressor = method_dir / "LogCrisp_compression_var/Compressor"
        decompressor = (
            method_dir / "LogCrisp_compression_var/decompressTest/DeCompressor"
        )
        seed = self.sample_lines(input_file, work / "sample.log", float(method["defaults"]["sample_ratio"]))
        model_dir = work / "model"
        archive_dir = work / "archive"
        internal = work / "internal_decompressed"
        model_dir.mkdir()
        archive_dir.mkdir()
        internal.mkdir()
        model_prefix = model_dir / "template"
        output_prefix = archive_dir / "block0"
        training = self.run_command(
            case, "train", f"{q(trainer)} -I {q(seed)} -O {q(model_prefix)}", work
        )
        compress = self.run_command(
            case,
            "compress",
            f"{q(compressor)} -I {q(input_file)} -O {q(output_prefix)} -T {q(model_prefix)} -P 0",
            work,
        )
        archive_candidates = list(archive_dir.rglob("*"))
        zst = next((path for path in archive_candidates if path.is_file() and path.suffix == ".zst"), None)
        if zst is None:
            raise RuntimeError("LogCrisp 没有生成 .zst 文件")
        decompress = self.run_command(
            case, "internal_decompress", f"{q(decompressor)} -I {q(zst)} -O {q(internal)}/", work
        )
        metrics = self.base_metrics(
            input_file,
            method,
            [archive_dir],
            compress,
            decompress,
            include_paths=[model_dir],
            notes=(
                f"训练时间 {training.elapsed_s:.6f}s；压缩大小包含模板。公开包只输出内部单元，"
                "不具备原文所述聚合查询与完整日志重建。"
            ),
        )
        metrics["peak_rss_kib"] = max(
            [x for x in [metrics["peak_rss_kib"] or None, training.peak_rss_kib] if x is not None],
            default="",
        )
        return metrics

    def result_base(self, case: dict[str, Any], work: Path, started: str) -> dict[str, Any]:
        return {
            "case_id": case["case_id"],
            "experiment": case["experiment"],
            "method": case["method"],
            "dataset": case["dataset"],
            "query_id": case.get("query_id", ""),
            "parameter": case.get("parameter", ""),
            "parameter_value": case.get("parameter_value", ""),
            "started_at": started,
            "work_dir": str(work),
        }

    def run_case(self, case: dict[str, Any]) -> None:
        if case["case_id"] in self.completed:
            self.emit("case_resume_skip", case_id=case["case_id"])
            return
        started = utc_now()
        work = self.stage_dir(case)
        base = self.result_base(case, work, started)
        self.emit("case_start", case_id=case["case_id"])
        try:
            method = self.methods[case["method"]]
            if method["adapter"] == "unavailable":
                raise MissingDependency(method["reason"])
            self.ensure_format_supported(case, method)
            self.ensure_ready(method, case)
            source_file = self.dataset_file(
                case["dataset"],
                self.max_input_bytes if not case.get("parameter") == "raw_size_bytes" else None,
            )
            input_file = source_file
            if case.get("parameter") == "raw_size_bytes":
                if self.max_input_bytes > 0 and int(case["parameter_value"]) > self.max_input_bytes:
                    raise Unsupported(
                        f"raw_size_bytes={case['parameter_value']} exceeds --max-input-bytes={self.max_input_bytes}; "
                        "raise the cap or set --max-input-bytes 0 for full scale experiments"
                    )
                input_file = self.prefix_file(
                    input_file, int(case["parameter_value"]), case["dataset"]
                )
            if self.args.smoke_test:
                input_file = self.smoke_file(input_file, case["dataset"])
            else:
                input_file = self.sampled_input_file(input_file, case["dataset"])
            metrics = self.run_adapter(case, method, input_file, work)
            metrics.update(self.sample_metrics(source_file, input_file))
            if metrics.get("status") == "FAILED_CORRECTNESS":
                self.record_issue(
                    case,
                    "CorrectnessMismatch",
                    "解压结果的 SHA-256 与原始输入不一致",
                    "verify",
                )
            row = {**base, **metrics, "finished_at": utc_now()}
        except Unsupported as exc:
            row = {
                **base,
                "status": "SKIPPED_UNSUPPORTED",
                "stage": "preflight",
                "error_type": type(exc).__name__,
                "error_message": str(exc),
                "finished_at": utc_now(),
            }
        except FileNotFoundError as exc:
            row = {
                **base,
                "status": "SKIPPED_MISSING_DATASET",
                "stage": "preflight",
                "error_type": type(exc).__name__,
                "error_message": str(exc),
                "finished_at": utc_now(),
            }
            self.record_exception(case, exc, "preflight")
        except MissingDependency as exc:
            row = {
                **base,
                "status": "SKIPPED_UNAVAILABLE",
                "stage": "preflight",
                "error_type": type(exc).__name__,
                "error_message": str(exc),
                "finished_at": utc_now(),
            }
            self.record_exception(case, exc, "preflight")
        except TimeoutError as exc:
            row = {
                **base,
                "status": "TIMEOUT",
                "stage": "run",
                "error_type": type(exc).__name__,
                "error_message": str(exc),
                "finished_at": utc_now(),
            }
            self.record_exception(case, exc, "run")
        except Exception as exc:
            row = {
                **base,
                "status": "FAILED",
                "stage": "run",
                "error_type": type(exc).__name__,
                "error_message": str(exc),
                "finished_at": utc_now(),
            }
            self.record_exception(case, exc, "run")
        self.append_result(row)
        self.completed.add(case["case_id"])
        self.emit("case_finish", case_id=case["case_id"], status=row["status"])

    def summarize(self, cases: list[dict[str, Any]]) -> dict[str, Any]:
        counts: dict[str, int] = {}
        if self.results_path.exists():
            with self.results_path.open("r", encoding="utf-8-sig", newline="") as stream:
                for row in csv.DictReader(stream):
                    counts[row["status"]] = counts.get(row["status"], 0) + 1
        summary = {
            "finished_at": utc_now(),
            "planned_cases": len(cases),
            "recorded_cases": sum(counts.values()),
            "status_counts": counts,
            "results_csv": str(self.results_path),
            "exceptions_jsonl": str(self.exceptions_path),
            "events_jsonl": str(self.events_path),
            "stop_file": str(self.run_dir / "STOP"),
            "interrupted": self.interrupted,
        }
        self.summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), "utf-8")
        return summary

    def run(self) -> int:
        cases = self.plan()
        if self.args.dry_run:
            print(json.dumps({"cases": len(cases), "preview": cases[:20]}, ensure_ascii=False, indent=2))
            return 0
        self.prepare_run(cases)
        self.emit("run_start", message=f"{len(cases)} cases", run_dir=str(self.run_dir))
        for case in cases:
            if self.interrupted or (self.run_dir / "STOP").exists():
                self.emit("run_stopped", message="检测到中断或 STOP 文件")
                break
            self.run_case(case)
        summary = self.summarize(cases)
        self.emit("run_finish", message=json.dumps(summary["status_counts"], ensure_ascii=False))
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 130 if self.interrupted else 0


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="根据日志压缩合并.xlsx的一致口径运行全部非消融实验。"
    )
    parser.add_argument("--root", required=True, help="工作区根目录（WSL/Linux 路径）")
    parser.add_argument("--experiments", default="main,sensitivity,query")
    parser.add_argument("--methods", default="", help="逗号分隔；空值表示全部")
    parser.add_argument("--datasets", default="", help="逗号分隔；空值表示全部")
    parser.add_argument("--timeout-seconds", type=int, default=0)
    parser.add_argument("--resume", default="", help="继续已有 run-* 目录")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--smoke-test", action="store_true")
    parser.add_argument("--allow-warm-cache", action="store_true")
    parser.add_argument("--max-input-bytes", type=int, default=None, help="Cap each effective input file; 0 disables sampling")
    parser.add_argument("--sample-chunks", type=int, default=0, help="Number of random whole-line chunks per sampled dataset")
    parser.add_argument("--sample-seed", type=int, default=None, help="Base seed for reproducible random sampling")
    parser.add_argument("--build", action="store_true", help="兼容选项；默认即会构建缺失二进制")
    parser.add_argument("--no-build", action="store_true", help="不构建缺失二进制，只记录为不可用")
    return parser


def main() -> int:
    args = create_parser().parse_args()
    runner = ExperimentRunner(args)

    def handle_signal(signum: int, _frame: Any) -> None:
        runner.interrupted = True
        print(f"\n收到信号 {signum}，当前子进程结束后停止。", file=sys.stderr, flush=True)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    try:
        return runner.run()
    except Exception as exc:
        print(f"启动失败: {type(exc).__name__}: {exc}", file=sys.stderr)
        traceback.print_exc()
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
