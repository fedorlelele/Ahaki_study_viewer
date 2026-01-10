#!/usr/bin/env python3
import argparse
import json
import os
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib import request
from urllib.error import HTTPError, URLError

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))
import local_admin_app  # noqa: E402


def load_env(path):
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def build_prompt(db_path, catalog_path, sample_path, limit, unannotated, order_mode,
                 exam_type, exam_session, subject, serials):
    conn = sqlite3.connect(db_path)
    subtopic_catalog = {}
    if catalog_path.exists():
        subtopic_catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    sample_text = sample_path.read_text(encoding="utf-8") if sample_path.exists() else ""
    records = local_admin_app.select_questions(
        conn,
        serials=serials,
        limit=limit,
        unannotated=unannotated,
        order_mode=order_mode,
        exam_type=exam_type,
        exam_session=exam_session,
        subject=subject,
        kinds=["explanation", "tag", "subtopic"],
    )
    conn.close()
    if not records:
        return "", ""
    _, _, _, combined_jsonl = local_admin_app.build_jsonl(records, subtopic_catalog)
    prompt = local_admin_app.build_combined_prompt(sample_text, combined_jsonl)
    return prompt, combined_jsonl


def call_gemini(api_key, model, prompt, thinking_budget):
    model_name = model.replace("models/", "")
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={api_key}"
    body = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.2, "maxOutputTokens": 8192},
    }
    if thinking_budget is not None:
        body["generationConfig"]["thinkingConfig"] = {"thinkingBudget": thinking_budget}

    data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    try:
        with request.urlopen(req, timeout=120) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except HTTPError as err:
        payload = err.read().decode("utf-8", errors="replace")
        return {"_error": f"HTTP {err.code}", "_payload": payload}
    except URLError as err:
        return {"_error": f"URL error: {err.reason}"}


def extract_jsonl(text):
    lines = []
    in_code = False
    for raw in text.splitlines():
        line = raw.strip()
        if line.startswith("```"):
            in_code = not in_code
            continue
        if not line:
            continue
        if not in_code and not line.startswith("{"):
            continue
        if line.startswith("{") and line.endswith("}"):
            lines.append(line)
    if not lines:
        block = "\n".join([ln for ln in text.splitlines() if ln.strip().startswith("{")])
        lines = [ln.strip() for ln in block.splitlines() if ln.strip().startswith("{")]
    valid = []
    for line in lines:
        try:
            json.loads(line)
            valid.append(line)
        except json.JSONDecodeError:
            continue
    return "\n".join(valid)


def list_models(api_key):
    url = f"https://generativelanguage.googleapis.com/v1beta/models?key={api_key}"
    req = request.Request(url, method="GET")
    try:
        with request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except HTTPError as err:
        payload = err.read().decode("utf-8", errors="replace")
        return {"_error": f"HTTP {err.code}", "_payload": payload}
    except URLError as err:
        return {"_error": f"URL error: {err.reason}"}


def read_usage(path):
    if not path.exists():
        return {"date": "", "count": 0}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"date": "", "count": 0}


def write_usage(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_args():
    parser = argparse.ArgumentParser(description="Run Gemini combined generation batches.")
    parser.add_argument("--db", default="output/ahaki.sqlite", help="Path to SQLite DB.")
    parser.add_argument("--subtopics", default="config/subtopics_catalog.json",
                        help="Path to subtopics catalog JSON.")
    parser.add_argument("--prompt-sample", default="resources/custum_prompt_sample.txt",
                        help="Path to explanation prompt sample text.")
    parser.add_argument("--limit", type=int, default=20, help="Questions per batch.")
    parser.add_argument(
        "--batches",
        type=int,
        default=1,
        help="How many batches to run. Use 0 to run until max-per-day.",
    )
    parser.add_argument("--sleep-seconds", type=int, default=20, help="Sleep between batches.")
    parser.add_argument("--max-per-day", type=int, default=25, help="Daily request cap.")
    parser.add_argument("--order", choices=["new", "serial"], default="new",
                        help="Order for selecting questions.")
    parser.add_argument("--exam-type", default="", help="Filter by exam type (A/B).")
    parser.add_argument("--exam-session", default="", help="Filter by exam session number.")
    parser.add_argument("--subject", default="", help="Filter by subject.")
    parser.add_argument("--serials", default="", help="Explicit serials or ranges.")
    parser.add_argument("--unannotated", action="store_true", default=True,
                        help="Select only missing explanation/tag/subtopic.")
    parser.add_argument("--all", dest="unannotated", action="store_false",
                        help="Allow already-annotated questions.")
    parser.add_argument("--mode-exp", default="append", choices=["append", "replace", "skip"])
    parser.add_argument("--mode-tag", default="append", choices=["append", "replace", "skip"])
    parser.add_argument("--mode-sub", default="append", choices=["append", "replace", "skip"])
    parser.add_argument("--version", default="auto", help="Explanation version or auto.")
    parser.add_argument("--model", default="gemini-3.0-flash-preview", help="Gemini model id.")
    parser.add_argument("--thinking-budget", type=int, default=1024,
                        help="Thinking budget; set to 0 to disable.")
    parser.add_argument("--api-key", default="", help="Override GEMINI_API_KEY.")
    parser.add_argument("--list-models", action="store_true",
                        help="List available models and exit.")
    parser.add_argument("--output-dir", default="output/gemini_batches",
                        help="Where to save model outputs.")
    parser.add_argument("--dry-run", action="store_true", help="Only print prompt.")
    parser.add_argument("--rebuild-web", action="store_true", default=True,
                        help="Rebuild web JSON after import.")
    parser.add_argument("--no-rebuild-web", dest="rebuild_web", action="store_false",
                        help="Do not rebuild web JSON after import.")
    return parser.parse_args()


def main():
    args = parse_args()
    repo_root = Path(__file__).resolve().parent.parent
    load_env(repo_root / ".env")

    api_key = args.api_key or os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        print("GEMINI_API_KEY is not set. Add it to .env or pass --api-key.", file=sys.stderr)
        return 1

    if args.list_models:
        resp = list_models(api_key)
        if "_error" in resp:
            print(f"API error: {resp['_error']}\n{resp.get('_payload','')}", file=sys.stderr)
            return 1
        for model in resp.get("models", []):
            name = model.get("name", "")
            methods = ", ".join(model.get("supportedGenerationMethods", []) or [])
            print(f"{name} ({methods})")
        return 0

    usage_path = Path("output/gemini_usage.json")
    usage = read_usage(usage_path)
    today = datetime.now().strftime("%Y-%m-%d")
    if usage.get("date") != today:
        usage = {"date": today, "count": 0}

    total_batches = args.batches if args.batches > 0 else args.max_per_day
    for batch_index in range(total_batches):
        if usage["count"] >= args.max_per_day:
            print("Daily limit reached. Stop.")
            break

        prompt, _ = build_prompt(
            Path(args.db),
            Path(args.subtopics),
            Path(args.prompt_sample),
            limit=args.limit,
            unannotated=args.unannotated,
            order_mode=args.order,
            exam_type=args.exam_type,
            exam_session=args.exam_session,
            subject=args.subject,
            serials=args.serials,
        )
        if not prompt:
            print("No questions matched the selection.")
            break

        if args.dry_run:
            print(prompt)
            break

        thinking_budget = args.thinking_budget if args.thinking_budget > 0 else None
        response = call_gemini(api_key, args.model, prompt, thinking_budget)
        if "_error" in response:
            payload = response.get("_payload", "")
            if thinking_budget and "thinking" in payload.lower():
                response = call_gemini(api_key, args.model, prompt, None)
            else:
                print(f"API error: {response['_error']}\n{payload}", file=sys.stderr)
                return 1

        parts = []
        for cand in response.get("candidates", []):
            content = cand.get("content", {})
            for part in content.get("parts", []):
                text = part.get("text")
                if text:
                    parts.append(text)
        raw_text = "\n".join(parts)
        jsonl_text = extract_jsonl(raw_text)
        if not jsonl_text:
            print("No JSONL found in response.", file=sys.stderr)
            return 1

        out_dir = Path(args.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = out_dir / f"explanations_tags_subtopics_batch_filled_{ts}.jsonl"
        out_path.write_text(jsonl_text + "\n", encoding="utf-8")

        version = None if args.version == "auto" else int(args.version)
        counts = local_admin_app.import_combined(
            args.db,
            jsonl_text,
            args.mode_exp,
            version,
            args.mode_tag,
            args.mode_sub,
        )
        print(f"Imported: {counts} -> {out_path}")

        if args.rebuild_web:
            msg = local_admin_app.run_build_web(repo_root)
            print(f"Rebuild: {msg}")

        usage["count"] += 1
        write_usage(usage_path, usage)

        if batch_index < args.batches - 1:
            time.sleep(max(args.sleep_seconds, 1))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
