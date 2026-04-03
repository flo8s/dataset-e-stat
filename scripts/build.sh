#!/usr/bin/env bash
set -euo pipefail
target="${1:-local}"
uv run fdl pull "$target" || true
uv run fdl run "$target" -- python main.py
uv run fdl push "$target"
