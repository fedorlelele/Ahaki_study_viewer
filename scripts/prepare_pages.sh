#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DOCS_DIR="${ROOT_DIR}/docs"

rm -rf "${DOCS_DIR}"
mkdir -p "${DOCS_DIR}/output/web"

cp -R "${ROOT_DIR}/web_app" "${DOCS_DIR}/"

if [ -d "${ROOT_DIR}/output/web" ]; then
  cp -R "${ROOT_DIR}/output/web/"* "${DOCS_DIR}/output/web/"
fi

cat > "${DOCS_DIR}/index.html" <<'EOF'
<!doctype html>
<html lang="ja">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Ahaki Study Viewer</title>
  </head>
  <body>
    <h1>Ahaki Study Viewer</h1>
    <p><a href="./web_app/">WebUIを開く</a></p>
  </body>
</html>
EOF

echo "Docs prepared: ${DOCS_DIR}"
