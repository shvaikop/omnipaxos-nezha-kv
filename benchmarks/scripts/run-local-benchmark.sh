#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCHMARKS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${BENCHMARKS_DIR}"
uv run local_benchmarks.py "$@"
