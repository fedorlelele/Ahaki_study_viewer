import sqlite3


STATUS_AI = "ai"
STATUS_TEACHER_APPROVED = "teacher_approved"
STATUS_TEACHER_EDITED = "teacher_edited"

DEFAULT_MODEL_NAME = "Gemini3Flash"
CASE_TEXT_REWRITE_MODEL_NAME = "GPT5.5"
CASE_TEXT_REWRITE_SOURCE = "codex_case_text_rewrite_20260616"

VALID_REVIEW_STATUSES = {
    STATUS_AI,
    STATUS_TEACHER_APPROVED,
    STATUS_TEACHER_EDITED,
}

LEGACY_SOURCE_METADATA = {
    "llm": (DEFAULT_MODEL_NAME, STATUS_AI),
    "ai": (DEFAULT_MODEL_NAME, STATUS_AI),
    "llm_checked": (DEFAULT_MODEL_NAME, STATUS_TEACHER_APPROVED),
    "teacher": (DEFAULT_MODEL_NAME, STATUS_TEACHER_EDITED),
    "human": (DEFAULT_MODEL_NAME, STATUS_TEACHER_EDITED),
    "llm_teacher": (DEFAULT_MODEL_NAME, STATUS_TEACHER_EDITED),
    "ai_teacher": (DEFAULT_MODEL_NAME, STATUS_TEACHER_EDITED),
    CASE_TEXT_REWRITE_SOURCE: (CASE_TEXT_REWRITE_MODEL_NAME, STATUS_AI),
    f"{CASE_TEXT_REWRITE_SOURCE}_checked": (
        CASE_TEXT_REWRITE_MODEL_NAME,
        STATUS_TEACHER_APPROVED,
    ),
    f"{CASE_TEXT_REWRITE_SOURCE}_teacher": (
        CASE_TEXT_REWRITE_MODEL_NAME,
        STATUS_TEACHER_EDITED,
    ),
}

KNOWN_MODEL_SOURCE = {
    DEFAULT_MODEL_NAME: {
        STATUS_AI: "llm",
        STATUS_TEACHER_APPROVED: "llm_checked",
        STATUS_TEACHER_EDITED: "teacher",
    },
    CASE_TEXT_REWRITE_MODEL_NAME: {
        STATUS_AI: CASE_TEXT_REWRITE_SOURCE,
        STATUS_TEACHER_APPROVED: f"{CASE_TEXT_REWRITE_SOURCE}_checked",
        STATUS_TEACHER_EDITED: f"{CASE_TEXT_REWRITE_SOURCE}_teacher",
    },
}

MODEL_SOURCE_PREFIX = "model:"
MODEL_STATUS_SUFFIXES = {
    ":checked": STATUS_TEACHER_APPROVED,
    ":approved": STATUS_TEACHER_APPROVED,
    ":teacher_approved": STATUS_TEACHER_APPROVED,
    ":teacher": STATUS_TEACHER_EDITED,
    ":edited": STATUS_TEACHER_EDITED,
    ":teacher_edited": STATUS_TEACHER_EDITED,
}


def normalize_review_status(value):
    status = str(value or "").strip()
    if not status:
        return ""
    aliases = {
        "checked": STATUS_TEACHER_APPROVED,
        "approved": STATUS_TEACHER_APPROVED,
        "teacher_checked": STATUS_TEACHER_APPROVED,
        "teacher_approved": STATUS_TEACHER_APPROVED,
        "ai": STATUS_AI,
        "llm": STATUS_AI,
        "raw": STATUS_AI,
        "teacher": STATUS_TEACHER_EDITED,
        "edited": STATUS_TEACHER_EDITED,
        "human": STATUS_TEACHER_EDITED,
        "teacher_edited": STATUS_TEACHER_EDITED,
    }
    return aliases.get(status, status if status in VALID_REVIEW_STATUSES else "")


def parse_model_source(source):
    text = str(source or "").strip()
    if not text.startswith(MODEL_SOURCE_PREFIX):
        return None
    payload = text[len(MODEL_SOURCE_PREFIX) :]
    for suffix, status in sorted(
        MODEL_STATUS_SUFFIXES.items(),
        key=lambda item: len(item[0]),
        reverse=True,
    ):
        if payload.endswith(suffix):
            model_name = payload[: -len(suffix)].strip()
            return model_name, status
    return payload.strip(), STATUS_AI


def derive_explanation_metadata(source=None, model_name=None, review_status=None):
    source_text = str(source or "").strip()
    normalized_model = str(model_name or "").strip()
    normalized_status = normalize_review_status(review_status)

    if source_text in LEGACY_SOURCE_METADATA:
        legacy_model, legacy_status = LEGACY_SOURCE_METADATA[source_text]
        normalized_model = normalized_model or legacy_model
        normalized_status = normalized_status or legacy_status

    parsed_model_source = parse_model_source(source_text)
    if parsed_model_source:
        parsed_model, parsed_status = parsed_model_source
        normalized_model = normalized_model or parsed_model
        normalized_status = normalized_status or parsed_status

    if not normalized_model and source_text:
        for suffix, status in (
            ("_checked", STATUS_TEACHER_APPROVED),
            ("_approved", STATUS_TEACHER_APPROVED),
            ("_teacher", STATUS_TEACHER_EDITED),
            ("_edited", STATUS_TEACHER_EDITED),
        ):
            if source_text.endswith(suffix):
                normalized_model = source_text[: -len(suffix)]
                normalized_status = normalized_status or status
                break

    if not normalized_model and source_text:
        normalized_model = source_text
    if not normalized_status:
        normalized_status = STATUS_AI

    return {
        "source": source_text,
        "model_name": normalized_model,
        "review_status": normalized_status,
    }


def build_explanation_source(model_name, review_status=STATUS_AI, fallback_source=""):
    model = str(model_name or "").strip()
    status = normalize_review_status(review_status) or STATUS_AI
    fallback = str(fallback_source or "").strip()

    if fallback:
        meta = derive_explanation_metadata(fallback)
        if meta["model_name"] == model and meta["review_status"] == status:
            return fallback

    if model in KNOWN_MODEL_SOURCE:
        return KNOWN_MODEL_SOURCE[model][status]

    if not model:
        return fallback or KNOWN_MODEL_SOURCE[DEFAULT_MODEL_NAME][status]

    suffix = ""
    if status == STATUS_TEACHER_APPROVED:
        suffix = ":checked"
    elif status == STATUS_TEACHER_EDITED:
        suffix = ":teacher"
    return f"{MODEL_SOURCE_PREFIX}{model}{suffix}"


def explanation_columns(conn):
    rows = conn.execute("PRAGMA table_info(explanations)").fetchall()
    return {row[1] for row in rows}


def ensure_explanation_metadata_schema(conn):
    columns = explanation_columns(conn)
    added_review_status = "review_status" not in columns
    if "model_name" not in columns:
        conn.execute("ALTER TABLE explanations ADD COLUMN model_name TEXT")
        columns.add("model_name")
    if "review_status" not in columns:
        conn.execute(
            "ALTER TABLE explanations ADD COLUMN review_status TEXT DEFAULT 'ai'"
        )
        columns.add("review_status")

    # Acquire the write lock before any latest-version read by callers.
    if not conn.in_transaction:
        conn.execute("BEGIN IMMEDIATE")
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_explanations_question_version ON explanations(question_id, version)")

    rows = conn.execute(
        "SELECT id, source, model_name, review_status FROM explanations"
    ).fetchall()
    for row_id, source, model_name, review_status in rows:
        original_model_name = model_name or ""
        original_review_status = review_status or ""
        source_text = str(source or "").strip()
        if source_text in LEGACY_SOURCE_METADATA:
            legacy_model, legacy_status = LEGACY_SOURCE_METADATA[source_text]
            model_name = model_name or legacy_model
            if not review_status or (added_review_status and review_status == STATUS_AI):
                review_status = legacy_status
        parsed_model_source = parse_model_source(source_text)
        if parsed_model_source:
            parsed_model, parsed_status = parsed_model_source
            model_name = model_name or parsed_model
            if not review_status or (added_review_status and review_status == STATUS_AI):
                review_status = parsed_status
        meta = derive_explanation_metadata(source, model_name, review_status)
        if original_model_name == meta["model_name"] and (
            original_review_status == meta["review_status"]
        ):
            continue
        conn.execute(
            """
            UPDATE explanations
            SET model_name = ?, review_status = ?
            WHERE id = ?
            """,
            (meta["model_name"], meta["review_status"], row_id),
        )


def insert_explanation(
    cursor,
    question_id,
    body,
    version,
    source=None,
    model_name=None,
    review_status=None,
):
    if not cursor.connection.in_transaction:
        cursor.execute("BEGIN IMMEDIATE")
    latest = cursor.execute(
        "SELECT version, model_name FROM explanations WHERE question_id = ? ORDER BY version DESC, id DESC LIMIT 1",
        (question_id,),
    ).fetchone()
    latest_version = latest[0] if latest else 0
    if version is None:
        version = latest_version + 1
    if isinstance(version, bool) or not isinstance(version, int) or version <= latest_version:
        raise ValueError(f"question_id={question_id}: version must exceed {latest_version}")
    status = normalize_review_status(review_status) or derive_explanation_metadata(source)["review_status"]
    if latest and not model_name and status in {STATUS_TEACHER_APPROVED, STATUS_TEACHER_EDITED}:
        model_name = latest[1]
    meta = derive_explanation_metadata(source, model_name, review_status)
    final_source = source or build_explanation_source(
        meta["model_name"],
        meta["review_status"],
    )
    cursor.execute(
        """
        INSERT INTO explanations(
            question_id,
            body,
            version,
            source,
            model_name,
            review_status
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            question_id,
            body,
            version,
            final_source,
            meta["model_name"],
            meta["review_status"],
        ),
    )


def migrate_database(path):
    conn = sqlite3.connect(path)
    try:
        ensure_explanation_metadata_schema(conn)
        conn.commit()
    finally:
        conn.close()
