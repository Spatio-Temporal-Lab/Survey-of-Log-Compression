# 日志压缩统一实验程序

这个目录把 `日志压缩合并.xlsx` 里的主实验、参数敏感性和查询工作负载整理成一个统一 runner。目标是：一条命令跑完整个实验矩阵，失败不中断，并把每个 case 的状态、时间、压缩率、吞吐、内存、正确性和异常原因写入结果文件。

## 一键运行

在 Windows PowerShell 中，从 `D:\DOWNLOAD\论文` 运行：

```powershell
.\experiment_suite\run_all_experiments.ps1
```

默认会通过 WSL Ubuntu 执行，结果写到：

```text
D:\DOWNLOAD\论文\experiment_results\run-YYYYMMDD-HHMMSS-ffffff\
```

默认输入上限是 `100MiB`。如果原始数据集超过这个大小，runner 会用固定 seed 做随机整行块采样，保证实际输入不超过 100MiB；小数据集直接使用原文件。

默认计划会跳过 `oceanbase`，同时过滤掉公开代码不可用的方法和已知不支持的数据/查询组合，让常规批量运行尽量只包含可执行、可比较的 case。若需要保留完整矩阵用于审计，可以加 `-IncludeUnavailable` 和 `-IncludeUnsupported`。

## 建议先跑

只看计划，不执行：

```powershell
.\experiment_suite\run_all_experiments.ps1 -DryRun
```

跑一个 Apache 冒烟测试：

```powershell
.\experiment_suite\run_all_experiments.ps1 -SmokeTest -AllowWarmCache
```

只跑部分算法/数据集：

```powershell
.\experiment_suite\run_all_experiments.ps1 `
  -Experiments "main,sensitivity,query" `
  -Methods "Denum,PBC,PBC-F,LogGrep,CLP-Text" `
  -Datasets "Apache,Windows,BGL"
```

把采样上限改成 50MiB：

```powershell
.\experiment_suite\run_all_experiments.ps1 -MaxInputMiB 50
```

禁用采样、跑全量数据：

```powershell
.\experiment_suite\run_all_experiments.ps1 -MaxInputMiB 0
```

恢复一次中断的运行：

```powershell
.\experiment_suite\run_all_experiments.ps1 `
  -Resume "D:\DOWNLOAD\论文\experiment_results\run-YYYYMMDD-HHMMSS-ffffff"
```

运行中想停，可以在对应 `run-*` 目录下新建空文件 `STOP`。runner 会在当前 case 结束后停止；已经写入 `results.csv` 的 case 恢复时不会重跑。

## 输出文件

- `results.csv`：统一结果表，每完成一个 case 立即追加。
- `summary.json`：状态计数、结果路径和是否中断。
- `events.jsonl`：run/case/stage 开始和结束事件。
- `exceptions.jsonl`：失败、缺依赖、缺数据、不支持等异常详情。
- `plan.json`：本次实际使用的配置和完整 case 列表。
- `logs/`：每个阶段的 stdout、stderr 和 `/usr/bin/time -v` 资源日志。
- `work/`：每个 case 隔离的中间文件、压缩产物和解压产物。

## 状态说明

- `OK`：执行完成；如果支持解压，会做 SHA-256 正确性校验。
- `FAILED_CORRECTNESS`：解压输出与输入不一致。
- `FAILED`：执行失败。
- `TIMEOUT`：阶段超时。
- `SKIPPED_UNAVAILABLE`：源码、依赖或公开入口不可用。
- `SKIPPED_MISSING_DATASET`：数据集文件缺失。
- `SKIPPED_UNSUPPORTED`：算法不支持该数据格式、查询语义或参数组合。

## 重要口径

- 压缩率字段 `cr_compressed_over_raw` 是 `compressed_size / raw_size`，越小越好；表格里如需 `Raw/Compressed` 可以取倒数。
- 结果中新增 `source_size_bytes`、`sampled`、`sample_method`、`sample_seed` 和 `input_path`，用于区分原始数据规模和本次实际输入规模。
- Text/半结构化方法默认不跑 JSON 数据集；格式不匹配写入 `SKIPPED_UNSUPPORTED`。
- LogBlock 公开代码没有完整逆变换，因此只能记录预处理产物压缩/解包，不把它标成完整无损压缩器。
- LogCrisp 公开代码没有论文中的聚合查询引擎，因此只测训练、压缩和内部单元解压。
- CLP-logging Library 官方公开仓库没有独立可复现实验入口，runner 会记录不可用，不用 CLP-Text/CLP-JSON 冒充它。
- PBC-F 使用 `pbc_fsst_file` helper 执行真实 `PBC_FSST` 文件压缩/解压；普通 `pbc -c/-d` 不作为 PBC-F 口径。
