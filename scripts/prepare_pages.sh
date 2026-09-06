#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
exec python3 -B "${ROOT_DIR}/scripts/prepare_pages.py" --root "${ROOT_DIR}" "$@"
