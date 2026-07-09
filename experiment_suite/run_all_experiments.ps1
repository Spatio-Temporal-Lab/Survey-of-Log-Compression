[CmdletBinding()]
param(
    [string]$Experiments = "main,sensitivity,query",
    [string]$Methods = "",
    [string]$Datasets = "",
    [switch]$Build,
    [switch]$NoBuild,
    [switch]$DryRun,
    [switch]$SmokeTest,
    [switch]$AllowWarmCache,
    [switch]$IncludeUnavailable,
    [switch]$IncludeUnsupported,
    [int]$MaxInputMiB = 100,
    [int]$SampleChunks = 16,
    [int]$SampleSeed = 20260707,
    [int]$TimeoutSeconds = 0,
    [string]$Resume = ""
)

$ErrorActionPreference = "Stop"
$utf8NoBom = New-Object System.Text.UTF8Encoding $false
[Console]::OutputEncoding = $utf8NoBom
$OutputEncoding = $utf8NoBom

$suite = Split-Path -Parent $MyInvocation.MyCommand.Path
$repo = Split-Path -Parent $suite
$config = Get-Content -Raw -Encoding UTF8 (Join-Path $suite "experiment_config.json") | ConvertFrom-Json
$distro = $config.wsl_distribution

$repoForWsl = $repo -replace '\\', '/'
$linuxRepo = (& wsl.exe -d $distro -- wslpath -a $repoForWsl).Trim()
if (-not $linuxRepo) {
    throw "Could not convert the workspace path for WSL distribution '$distro'."
}

$runner = "${linuxRepo}/experiment_suite/run_experiments.py"
& wsl.exe -d $distro -- test -f $runner
if ($LASTEXITCODE -ne 0) {
    throw "WSL cannot find the experiment runner at '$runner'. If this path contains mojibake such as 璁烘枃, reopen PowerShell and rerun this UTF-8 fixed script."
}

$arguments = @("-d", $distro, "-u", "root", "--", "python3", $runner, "--root", $linuxRepo, "--experiments", $Experiments)
if ($Methods) { $arguments += @("--methods", $Methods) }
if ($Datasets) { $arguments += @("--datasets", $Datasets) }
if ($Build) { $arguments += "--build" }
if ($NoBuild) { $arguments += "--no-build" }
if ($DryRun) { $arguments += "--dry-run" }
if ($SmokeTest) { $arguments += "--smoke-test" }
if ($AllowWarmCache) { $arguments += "--allow-warm-cache" }
if ($IncludeUnavailable) { $arguments += "--include-unavailable" }
if ($IncludeUnsupported) { $arguments += "--include-unsupported" }
if ($MaxInputMiB -ge 0) { $arguments += @("--max-input-bytes", "$($MaxInputMiB * 1024 * 1024)") }
if ($SampleChunks -gt 0) { $arguments += @("--sample-chunks", "$SampleChunks") }
if ($SampleSeed -gt 0) { $arguments += @("--sample-seed", "$SampleSeed") }
if ($TimeoutSeconds -gt 0) { $arguments += @("--timeout-seconds", "$TimeoutSeconds") }
if ($Resume) {
    $resumeForWsl = $Resume -replace '\\', '/'
    $linuxResume = (& wsl.exe -d $distro -- wslpath -a $resumeForWsl).Trim()
    $arguments += @("--resume", $linuxResume)
}

Write-Host "Starting log-compression experiments (WSL: $distro)..."
& wsl.exe @arguments
exit $LASTEXITCODE
