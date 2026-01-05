#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DB_PATH="${PROJECT_ROOT}/kokushitxt/output/hikkei.sqlite"
BACKUP_DIR="/Users/nishitani/Library/CloudStorage/GoogleDrive-fedorp4pdk@gmail.com/マイドライブ/99_その他/AhakiStudyViewer/backups"

if [[ ! -f "${DB_PATH}" ]]; then
  echo "SQLiteが見つかりません: ${DB_PATH}" >&2
  exit 1
fi

mkdir -p "${BACKUP_DIR}"

timestamp="$(date "+%Y%m%d_%H%M%S")"
backup_path="${BACKUP_DIR}/hikkei_${timestamp}.sqlite"

cp "${DB_PATH}" "${backup_path}"
echo "バックアップ完了: ${backup_path}"
