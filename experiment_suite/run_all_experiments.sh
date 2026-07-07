#!/usr/bin/env bash
set -uo pipefail
suite_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_dir="$(cd "${suite_dir}/.." && pwd)"
exec python3 "${suite_dir}/run_experiments.py" --root "${repo_dir}" "$@"
