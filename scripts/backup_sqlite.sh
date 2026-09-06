#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DB_PATH="${PROJECT_ROOT}/output/ahaki.sqlite"
BACKUP_DIR="/Users/nishitani/Library/CloudStorage/GoogleDrive-fedorp4pdk@gmail.com/マイドライブ/99_その他/AhakiStudyViewer/backups"

BACKUP_DIR="${AHAKI_BACKUP_DIR:-${BACKUP_DIR}}"
exec "${AHAKI_PYTHON:-python3}" "${PROJECT_ROOT}/scripts/backup_database.py" --db "${DB_PATH}" --backup-dir "${BACKUP_DIR}"
