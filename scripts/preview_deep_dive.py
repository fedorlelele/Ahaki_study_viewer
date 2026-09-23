"""Serve the real ASV UI with a local draft; never connect to production services."""
import argparse
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import re
import sqlite3
from urllib.parse import parse_qs, unquote, urlsplit

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "output/ahaki.sqlite"


class PreviewHandler(SimpleHTTPRequestHandler):
    draft = ""
    serial = "A34-029"
    model = "GPT-6"

    def send_bytes(self, body, content_type="application/json; charset=utf-8", status=200):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_json(self, data, status=200):
        self.send_bytes(json.dumps(data, ensure_ascii=False).encode(), status=status)

    def do_GET(self):
        parsed = urlsplit(self.path)
        path = unquote(parsed.path)
        if path == "/web_app/config.js":
            self.send_bytes(b'window.SUPABASE_URL="";window.SUPABASE_KEY="";window.AI_API_BASE="/preview-api";window.ADMIN_API_BASE="/preview-api";', "text/javascript; charset=utf-8")
            return
        if path.startswith("/preview-api/"):
            self.serve_api(path.removeprefix("/preview-api"), parse_qs(parsed.query))
            return
        vendor = {
            "/preview-vendor/marked.js": ROOT / "node_modules/marked/lib/marked.umd.js",
            "/preview-vendor/purify.js": ROOT / "node_modules/dompurify/dist/purify.min.js",
            "/preview-vendor/katex.js": ROOT / "node_modules/katex/dist/katex.min.js",
        }
        if path in vendor:
            self.send_bytes(vendor[path].read_bytes(), "text/javascript; charset=utf-8")
            return
        target = (ROOT / path.lstrip("/")).resolve()
        allowed = [ROOT / "web_app", ROOT / "output/web"]
        if not any(target.is_relative_to(folder) for folder in allowed) and target != ROOT / "config/subtopics_catalog.json":
            self.send_error(404)
            return
        if target == ROOT / "web_app" or target == ROOT / "web_app/index.html":
            text = (ROOT / "web_app/index.html").read_text()
            text = re.sub(r'<!-- Cloudflare Web Analytics -->.*?<!-- End Cloudflare Web Analytics -->', '', text, flags=re.S)
            text = re.sub(r'<script src="https://cdn.jsdelivr.net/npm/@supabase/[^<]+</script>', '', text)
            text = text.replace('https://cdn.jsdelivr.net/npm/marked@18.0.5/lib/marked.umd.js', '/preview-vendor/marked.js')
            text = text.replace('https://cdn.jsdelivr.net/npm/dompurify@3.4.15/dist/purify.min.js', '/preview-vendor/purify.js')
            text = text.replace('https://cdn.jsdelivr.net/npm/katex@0.18.7/dist/katex.min.js', '/preview-vendor/katex.js')
            self.send_bytes(text.encode(), "text/html; charset=utf-8")
            return
        if not target.is_file() or any(part.startswith(".") for part in target.relative_to(ROOT).parts):
            self.send_error(404)
            return
        super().do_GET()

    def do_HEAD(self):
        self.send_error(405)

    def serve_api(self, path, query):
        if path == "/ai/config":
            self.send_json({"public_generation": False, "tts_enabled": False})
        elif path == "/stats/answers":
            serials = query.get("serials", [""])[0].split(",")
            self.send_json({"ok": True, "items": [{"serial": serial, "total": 0, "correct": 0} for serial in serials if serial]})
        elif path == "/ai/deep_dive_index":
            with sqlite3.connect(f"file:{DB}?mode=ro", uri=True) as conn:
                serials = [row[0] for row in conn.execute("SELECT serial FROM deep_dive_explanations")]
            self.send_json({"serials": serials})
        elif path == "/ai/deep_dive":
            serial = query.get("serial", [""])[0]
            if serial == self.serial:
                row = {"serial": serial, "explanation": self.draft, "tags": ["筋紡錘", "Ⅰa群線維", "伸張反射"], "model_name": self.model, "review_status": "ai_fact_checked", "updated_at": "2026-09-23T00:00:00Z"}
            else:
                with sqlite3.connect(f"file:{DB}?mode=ro", uri=True) as conn:
                    conn.row_factory = sqlite3.Row
                    stored = conn.execute("SELECT * FROM deep_dive_explanations WHERE serial=?", (serial,)).fetchone()
                row = dict(stored) if stored else None
                if row:
                    row["tags"] = json.loads(row.pop("tags_json") or "[]")
            self.send_json({"found": bool(row), "data": row})
        elif path == "/ai/question_beginner_qa_batch":
            self.send_json({"items_by_serial": {}})
        else:
            self.send_json({"ok": True, "items": [], "serials": [], "counts": {}})

    def do_POST(self):
        # Preview interaction never writes to the database, sends feedback, or generates content.
        self.send_json({"message": "プレビューでは保存を行いません。"}, status=405)

    def log_message(self, fmt, *args):
        if args and str(args[1] if len(args) > 1 else "") not in ("200", "304"):
            super().log_message(fmt, *args)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8766)
    parser.add_argument("--serial", default="A34-029")
    parser.add_argument("--model", default="GPT-6")
    parser.add_argument("--content", type=Path, default=ROOT / "web_app/content/deep-dives/A34-029.md")
    args = parser.parse_args()
    PreviewHandler.draft = args.content.read_text()
    PreviewHandler.serial = args.serial
    PreviewHandler.model = args.model
    handler = partial(PreviewHandler, directory=str(ROOT))
    server = ThreadingHTTPServer(("127.0.0.1", args.port), handler)
    print(f"http://127.0.0.1:{args.port}/web_app/?q={args.serial}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
