import argparse
import json
import sqlite3
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(
        description="Export questions to JSONL for LLM explanations."
    )
    parser.add_argument(
        "--db",
        default="kokushitxt/output/hikkei.sqlite",
        help="Path to SQLite database.",
    )
    parser.add_argument(
        "--out",
        default="kokushitxt/output/explanations_batch.jsonl",
        help="Output JSONL path.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of questions to export.",
    )
    parser.add_argument(
        "--serials",
        default="",
        help="Comma-separated serials to export (overrides limit).",
    )
    parser.add_argument(
        "--prompt-template",
        default="samples/custum_prompt_sample.txt",
        help="Path to the prompt sample text.",
    )
    parser.add_argument(
        "--prompt-out",
        default="",
        help="Optional path to write a ready-to-paste prompt.",
    )
    return parser.parse_args()


def fetch_questions(conn, serials, limit):
    if serials:
        serial_list = [s.strip() for s in serials.split(",") if s.strip()]
        placeholders = ",".join("?" for _ in serial_list)
        query = f"""
            SELECT
                q.serial,
                s.name AS subject,
                q.case_text,
                q.stem,
                q.choices_json,
                q.answer_index,
                q.answer_text
            FROM questions q
            LEFT JOIN subjects s ON q.subject_id = s.id
            WHERE q.serial IN ({placeholders})
            ORDER BY q.serial
        """
        return conn.execute(query, serial_list).fetchall()

    query = """
        SELECT
            q.serial,
            s.name AS subject,
            q.case_text,
            q.stem,
            q.choices_json,
            q.answer_index,
            q.answer_text
        FROM questions q
        LEFT JOIN subjects s ON q.subject_id = s.id
        WHERE NOT EXISTS (
            SELECT 1 FROM explanations e WHERE e.question_id = q.id
        )
        ORDER BY q.serial
        LIMIT ?
    """
    return conn.execute(query, (limit,)).fetchall()


def main():
    args = parse_args()
    db_path = Path(args.db)
    out_path = Path(args.out)
    prompt_template_path = Path(args.prompt_template)
    prompt_out_path = Path(args.prompt_out) if args.prompt_out else None

    conn = sqlite3.connect(db_path)
    rows = fetch_questions(conn, args.serials, args.limit)
    conn.close()

    out_path.parent.mkdir(parents=True, exist_ok=True)

    with out_path.open("w", encoding="utf-8") as f:
        for row in rows:
            serial, subject, case_text, stem, choices_json, answer_index, answer_text = row
            choices = json.loads(choices_json)
            record = {
                "serial": serial,
                "subject": subject,
                "case_text": case_text,
                "stem": stem,
                "choices": choices,
                "answer_index": answer_index,
                "answer_text": answer_text,
                "explanation": "",
                "source": "llm",
            }
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    print(f"Exported {len(rows)} questions to {out_path}")

    if prompt_out_path:
        template_text = prompt_template_path.read_text(encoding="utf-8")
        prompt_out_path.parent.mkdir(parents=True, exist_ok=True)
        prompt_text = (
            f"{template_text.strip()}\n\n"
            "【指示】\n"
            "以下は【国家試験問題】のデータである。質問は不要で、そのまま解説を作成する。\n"
            "出力はJSONLのみとし、各行のexplanationを埋め、他のキーは変更しない。\n"
            "回答は画面表示ではなく、JSONLファイルとして保存して返す。\n"
            "ファイル名は explanations_batch_filled.jsonl とする。\n"
            "だ・である調で簡潔に、長くなりすぎない説明にする。\n\n"
            "【国家試験問題(JSONL)】\n"
        )
        jsonl_text = out_path.read_text(encoding="utf-8").strip()
        prompt_out_path.write_text(
            f"{prompt_text}{jsonl_text}\n",
            encoding="utf-8",
        )
        print(f"Prompt saved: {prompt_out_path}")


if __name__ == "__main__":
    main()
