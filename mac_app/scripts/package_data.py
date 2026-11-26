"""Build the offline Mac question resource from the existing Pages publication."""

import argparse
import json
import os
from pathlib import Path
import tempfile


WEB = Path(__file__).resolve().parents[2] / "docs" / "output" / "web"
FIELDS = (
    "serial", "exam_type", "exam_session", "subject", "case_text", "stem", "choices",
    "answer_index", "answer_indices", "answer_none", "answer_variants", "answer_notes",
    "explanation_latest", "explanation_latest_model_name", "explanation_latest_review_status",
    "tags", "subtopics",
)


def package(destination):
    with (WEB / "questions_manifest.json").open(encoding="utf-8") as source:
        manifest = json.load(source)
    with (WEB / "questions.json").open(encoding="utf-8") as source:
        rows = json.load(source)
    total = manifest["total"]
    if not rows or len(rows) != total or len({row["serial"] for row in rows}) != total:
        raise ValueError("Published question count or serials disagree with the manifest")
    if not manifest["generated_at"]:
        raise ValueError("Published manifest is missing its data version")

    snapshot = {
        "manifest": {"generated_at": manifest["generated_at"], "total": total},
        "questions": [{k: row[k] for k in FIELDS if k in row} for row in rows],
    }
    destination.mkdir(parents=True, exist_ok=True)
    output = destination / "ASVQuestions.json"
    # Keep an existing bundle resource intact on an interrupted rebuild.
    with tempfile.NamedTemporaryFile(mode="w", dir=destination, encoding="utf-8", delete=False) as handle:
        temporary = Path(handle.name)
        try:
            json.dump(snapshot, handle, ensure_ascii=False, separators=(",", ":"))
            handle.close()
            temporary.chmod(0o644)
            os.replace(temporary, output)
        finally:
            temporary.unlink(missing_ok=True)
    print(f"{total} 問を収録しました。 {output}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("destination", type=Path, help=".app/Contents/Resources directory")
    package(parser.parse_args().destination)
