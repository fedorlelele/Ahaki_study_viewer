"""Shared answer contract for the Viewer, search, and coverage exports.

Keep the original answer text. Media-specific answers are separate from the
default accepted answers; an invalid structured index is never silently ignored.
"""
from __future__ import annotations

import json
import re
import unicodedata


class QuestionContractError(ValueError):
    pass


def _array(value, field, serial):
    if value is None or value == "":
        return None
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError) as exc:
            raise QuestionContractError(f"{serial}: {field} is not valid JSON") from exc
    if not isinstance(value, list):
        raise QuestionContractError(f"{serial}: {field} must be an array")
    return value


def _indices(values, count, serial, field, strict):
    result = []
    for value in values or []:
        try:
            if isinstance(value, bool) or str(value).strip() != str(int(value)):
                raise ValueError()
            index = int(value)
            if not 1 <= index <= count:
                raise ValueError()
        except (ValueError, TypeError, OverflowError):
            if strict:
                raise QuestionContractError(f"{serial}: {field} contains out-of-range/non-integer index {value!r} (1..{count})")
            continue
        if index not in result:
            result.append(index)
    return sorted(result)


def parse_answer_text(text, choice_count=4):
    """Parse the default part of legacy answer text, including full-width digits."""
    normalized = unicodedata.normalize("NFKC", str(text or ""))
    default_text = re.split(r"[（(]\s*点字\s*[:：]", normalized, maxsplit=1)[0]
    if "なし" in default_text:
        return [], True
    if "すべて" in default_text or "全て" in default_text:
        return list(range(1, choice_count + 1)), False
    values = re.findall(r"\d+", default_text)
    return _indices(values, choice_count, "answer_text", "answer_text", True), False


def resolve_question_answers(record, strict=True):
    record = dict(record)
    serial = str(record.get("serial") or "question")
    choices = _array(record.get("choices", record.get("choices_json")), "choices", serial)
    count = len(choices) if choices is not None else 4
    if count < 1:
        raise QuestionContractError(f"{serial}: choices must not be empty")
    raw = str(record.get("answer_text") or "")
    normalized = unicodedata.normalize("NFKC", raw)
    media_match = re.search(r"[（(]\s*点字\s*[:：]([^）)]+)[）)]", normalized)
    none_value = record.get("answer_none", False)
    if none_value not in (None, False, True, 0, 1, "0", "1", ""):
        raise QuestionContractError(f"{serial}: invalid answer_none")
    answer_none = none_value in (True, 1, "1")
    structured = _array(record.get("answer_indices", record.get("answer_indices_json")), "answer_indices", serial)
    indices = _indices(structured, count, serial, "answer_indices", strict)
    single_value = record.get("answer_index")
    single = _indices([single_value] if single_value is not None else [], count, serial, "answer_index", strict)
    if strict and single and indices and single[0] not in indices:
        raise QuestionContractError(f"{serial}: answer_index contradicts answer_indices")
    if answer_none:
        if strict and (indices or single):
            raise QuestionContractError(f"{serial}: answer_none contradicts numeric answers")
        indices = []
    elif not indices:
        indices = single
        if not indices and raw:
            indices, answer_none = parse_answer_text(raw, count)

    variants = {"default": indices}
    notes = str(record.get("answer_notes") or "")
    explicit = record.get("answer_variants")
    if isinstance(explicit, dict):
        for medium, values in explicit.items():
            variants[str(medium)] = _indices(_array(values, "answer_variants", serial), count, serial, "answer_variants", strict)
        if strict and variants.get("default") != indices:
            raise QuestionContractError(f"{serial}: default answer variant contradicts answer_indices")
    elif media_match:
        # The legacy stored union is not the default answer set. Preserve each
        # medium from the explicit annotation, retaining the source verbatim.
        default, default_none = parse_answer_text(normalized[:media_match.start()], count)
        braille, braille_none = parse_answer_text(media_match.group(1), count)
        if default_none or braille_none:
            raise QuestionContractError(f"{serial}: unsupported media-specific no-answer condition")
        if strict and indices and set(indices) != set(default) | set(braille):
            raise QuestionContractError(f"{serial}: media answers contradict stored accepted answers")
        variants = {"default": default, "braille": braille}
        indices = default
        raw_note = re.search(r"[（(]\s*点字\s*[:：]([^）)]+)[）)]", raw)
        notes = raw_note.group(0) if raw_note else media_match.group(0)
    if answer_none and strict and any(variants.values()):
        raise QuestionContractError(f"{serial}: answer variants contradict answer_none")
    return {
        "answer_index": indices[0] if indices else None,
        "answer_indices": indices,
        "answer_none": answer_none,
        "answer_text": raw,
        "answer_variants": variants,
        "answer_notes": notes,
    }
