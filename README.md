# Survey of Log Compression

This repository contains the local reproduction code, unified experiment runner, and sampled experiment datasets for the log-compression survey.

## Contents

- `experiment_suite/`: one-command runner generated from `日志压缩合并.xlsx`.
- Algorithm source/reproduction directories: `clp/`, `cowic/`, `Delog/`, `Denum-G-main/`, `ELISE-2021/`, `log_archive_v0/`, `logcrisp-vendored-code/`, `LogGrep/`, `LogLite/`, `LogReducer/`, `LogShrink/`, `logzip/`, `pbc/`, and `suppmaterial-21-kundi-logblock/`.
- `datasets/`: sampled experiment inputs. Large raw datasets are randomly sampled by whole log-line chunks to stay below GitHub's single-file limit.
- `datasets_manifest.csv`: generated manifest with source size, uploaded size, sampling method, and SHA-256 of the uploaded file.
- `tools/create_sampled_datasets.py`: reproducible dataset sampling script.

## Run Experiments

From Windows PowerShell:

```powershell
cd D:\DOWNLOAD\论文\Survey-of-Log-Compression
.\experiment_suite\run_all_experiments.ps1 -DryRun
.\experiment_suite\run_all_experiments.ps1 -SmokeTest -AllowWarmCache
.\experiment_suite\run_all_experiments.ps1
```

The runner defaults to a 100MiB effective input cap for large datasets. The uploaded datasets are also sampled for repository portability; see `datasets_manifest.csv` for exact source and sample sizes.

## Dataset Policy

The local raw dataset folder is about 67GB and contains files up to 28GB. Those files cannot be committed to a normal GitHub repository directly. For this repository, large datasets are represented by deterministic sampled files capped below 100MB per file, using seed `20260707`.

To regenerate the uploaded dataset folder from the local raw data:

```powershell
python .\tools\create_sampled_datasets.py `
  --source-root D:\DOWNLOAD\论文\datasets `
  --output-root .\datasets `
  --manifest .\datasets_manifest.csv
```

