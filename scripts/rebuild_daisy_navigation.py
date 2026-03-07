#!/usr/bin/env python3
import argparse
import csv
import html
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


QUESTION_RE = re.compile(
    r"^((?:あん摩|はりきゅう)第[０-９0-9]+回、第[０-９0-9]+問)、"
)
CHOICE_RE = re.compile(r"^([０-９0-9]+)、")


@dataclass
class NavEntry:
    entry_id: str
    level: int
    href: str
    label: str
    is_actual_segment: bool
    segment_index: Optional[int]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Rebuild DAISY 2.02 ncc.html navigation from a VOICEPEAK CSV export."
    )
    parser.add_argument("--csv", required=True, help="Path to the VOICEPEAK CSV file.")
    parser.add_argument("--daisy-dir", required=True, help="Path to the DAISY directory.")
    parser.add_argument(
        "--backup-suffix",
        default=".bak",
        help="Suffix for backup files when writing. Set empty string to disable backups.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and print a summary without writing files.",
    )
    return parser.parse_args()


def read_csv_rows(path: Path) -> list[list[str]]:
    with path.open(encoding="utf-8", newline="") as fh:
        rows = list(csv.reader(fh))
    return [[str(cell or "").strip() for cell in row] for row in rows]


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip())


def make_shift_jis_safe(text: str) -> str:
    value = normalize_text(text)
    if not value:
        return ""
    replacements = {
        "⁺": "+",
        "⁻": "-",
        "²": "2",
        "³": "3",
        "₃": "3",
        "₄": "4",
        "₅": "5",
        "₆": "6",
        "₇": "7",
        "₈": "8",
        "₉": "9",
        "₀": "0",
    }
    for src, dst in replacements.items():
        value = value.replace(src, dst)
    value = unicodedata.normalize("NFKC", value)
    try:
        value.encode("shift_jis")
        return value
    except UnicodeEncodeError:
        return value.encode("shift_jis", errors="replace").decode("shift_jis")


def is_case_text(cell: str) -> bool:
    return normalize_text(cell).startswith("症例文")


def is_question_start(cell: str) -> bool:
    return bool(QUESTION_RE.match(normalize_text(cell)))


def is_choice(cell: str) -> bool:
    return bool(CHOICE_RE.match(normalize_text(cell)))


def is_answer(cell: str) -> bool:
    return normalize_text(cell).startswith("解答、")


def is_explanation(cell: str) -> bool:
    return normalize_text(cell).startswith("解説、")


def is_deep_dive_start(cell: str) -> bool:
    return normalize_text(cell).startswith("深堀り解説、")


def extract_question_label(cell: str) -> str:
    text = normalize_text(cell)
    match = QUESTION_RE.match(text)
    if not match:
        return make_shift_jis_safe(text[:60] or "問題文")
    return make_shift_jis_safe(match.group(1).replace("、", " "))


def extract_cell_heading(cell: str) -> str:
    text = normalize_text(cell)
    if not text:
        return ""
    if is_case_text(text):
        return make_shift_jis_safe("症例文")
    if is_question_start(text):
        return extract_question_label(text)
    if is_choice(text):
        match = CHOICE_RE.match(text)
        return make_shift_jis_safe(match.group(1) if match else text[:20])
    if is_answer(text):
        return make_shift_jis_safe("解答")
    if is_explanation(text):
        return make_shift_jis_safe("解説")
    if is_deep_dive_start(text):
        text = text.split("、", 1)[1] if "、" in text else text
    if "、" in text:
        head = text.split("、", 1)[0].strip()
        if head:
            return make_shift_jis_safe(head)
    return make_shift_jis_safe(text[:60])


def build_navigation(rows: list[list[str]]) -> tuple[list[NavEntry], dict[int, str], int]:
    entries: list[NavEntry] = []
    smil_titles: dict[int, str] = {}
    segment_index = 1
    synthetic_count = 0

    for row in rows:
        if len(row) <= 1:
            continue
        cells = [normalize_text(cell) for cell in row[1:] if normalize_text(cell)]
        in_deep_dive = False

        for cell in cells:
            href = f"ptk{segment_index:06d}.smil#bookid_{segment_index:06d}"
            actual_entry_id = f"heading_{segment_index:06d}"

            if is_deep_dive_start(cell):
                synthetic_count += 1
                entries.append(
                    NavEntry(
                        entry_id=f"heading_extra_{segment_index:06d}",
                        level=2,
                        href=href,
                        label="深堀り解説",
                        is_actual_segment=False,
                        segment_index=None,
                    )
                )
                actual_label = extract_cell_heading(cell)
                entries.append(
                    NavEntry(
                        entry_id=actual_entry_id,
                        level=3,
                        href=href,
                        label=actual_label,
                        is_actual_segment=True,
                        segment_index=segment_index,
                    )
                )
                smil_titles[segment_index] = actual_label
                in_deep_dive = True
                segment_index += 1
                continue

            if in_deep_dive:
                actual_label = extract_cell_heading(cell)
                entries.append(
                    NavEntry(
                        entry_id=actual_entry_id,
                        level=3,
                        href=href,
                        label=actual_label,
                        is_actual_segment=True,
                        segment_index=segment_index,
                    )
                )
                smil_titles[segment_index] = actual_label
                segment_index += 1
                continue

            level = 1 if is_case_text(cell) or is_question_start(cell) else 2
            actual_label = extract_cell_heading(cell)
            entries.append(
                NavEntry(
                    entry_id=actual_entry_id,
                    level=level,
                    href=href,
                    label=actual_label,
                    is_actual_segment=True,
                    segment_index=segment_index,
                )
            )
            smil_titles[segment_index] = actual_label
            segment_index += 1

    return entries, smil_titles, synthetic_count


def update_meta_content(text: str, meta_name: str, value: str) -> str:
    pattern = re.compile(
        rf'(<meta\s+name="{re.escape(meta_name)}"\s+content=")([^"]*)(")',
        re.IGNORECASE,
    )
    escaped = html.escape(value, quote=True)
    replaced, count = pattern.subn(
        lambda match: f"{match.group(1)}{escaped}{match.group(3)}", text, count=1
    )
    if count != 1:
        raise ValueError(f"meta '{meta_name}' not found")
    return replaced


def render_nav_entries(entries: list[NavEntry]) -> str:
    lines = []
    for idx, entry in enumerate(entries):
        tag = f"h{entry.level}"
        if idx == 0 and entry.level == 1:
            css_class = "title"
        elif entry.level == 1:
            css_class = "section"
        elif entry.level == 2:
            css_class = "subsection"
        else:
            css_class = "subsubsection"
        label = html.escape(entry.label or "見出し")
        lines.append(
            f'\t<{tag} id="{entry.entry_id}" class="{css_class}"><a href="{entry.href}">{label}</a></{tag}>'
        )
    return "\n".join(lines) + "\n"


def rewrite_ncc_text(original: str, entries: list[NavEntry]) -> str:
    updated = update_meta_content(original, "ncc:tocItems", str(len(entries)))
    updated = update_meta_content(updated, "ncc:depth", "3")

    body_match = re.search(r"<body>\s*(.*?)\s*</body>", updated, re.DOTALL | re.IGNORECASE)
    if not body_match:
        raise ValueError("ncc.html body not found")
    nav_html = render_nav_entries(entries)
    updated = (
        updated[: body_match.start(1)]
        + nav_html
        + updated[body_match.end(1) :]
    )
    return updated


def rewrite_smil_title(smil_path: Path, title: str) -> str:
    original = smil_path.read_text(encoding="shift_jis")
    return update_meta_content(original, "title", title or "見出し")


def maybe_backup(path: Path, suffix: str):
    if not suffix:
        return
    backup_path = path.with_name(path.name + suffix)
    if not backup_path.exists():
        backup_path.write_bytes(path.read_bytes())


def resolve_source_text(path: Path, backup_suffix: str) -> str:
    if path.exists() and path.stat().st_size > 0:
        return path.read_text(encoding="shift_jis")
    if backup_suffix:
        backup_path = path.with_name(path.name + backup_suffix)
        if backup_path.exists() and backup_path.stat().st_size > 0:
            return backup_path.read_text(encoding="shift_jis")
    raise FileNotFoundError(f"Readable source file not found for {path}")


def write_shift_jis(path: Path, text: str):
    tmp_path = path.with_name(path.name + ".tmp")
    with tmp_path.open("w", encoding="shift_jis", newline="\r\n") as fh:
        fh.write(text)
    tmp_path.replace(path)


def main():
    args = parse_args()
    csv_path = Path(args.csv)
    daisy_dir = Path(args.daisy_dir)
    ncc_path = daisy_dir / "ncc.html"

    rows = read_csv_rows(csv_path)
    entries, smil_titles, synthetic_count = build_navigation(rows)
    segment_count = sum(max(len(row) - 1, 0) for row in rows)

    if segment_count != len(smil_titles):
        raise ValueError(
            f"CSV segment count mismatch: expected {segment_count}, built {len(smil_titles)}"
        )

    missing = [
        idx
        for idx in range(1, segment_count + 1)
        if not (daisy_dir / f"ptk{idx:06d}.smil").exists()
    ]
    if missing:
        raise FileNotFoundError(f"Missing SMIL files for segments: {missing[:10]}")

    original_ncc = resolve_source_text(ncc_path, args.backup_suffix)
    rewritten_ncc = rewrite_ncc_text(original_ncc, entries)

    if args.dry_run:
        print(f"csv_rows={len(rows)}")
        print(f"segment_count={segment_count}")
        print(f"nav_entries={len(entries)}")
        print(f"synthetic_deep_dive_entries={synthetic_count}")
        print(f"first_labels={[entry.label for entry in entries[:12]]}")
        return

    maybe_backup(ncc_path, args.backup_suffix)
    write_shift_jis(ncc_path, rewritten_ncc)

    for idx, title in smil_titles.items():
        smil_path = daisy_dir / f"ptk{idx:06d}.smil"
        maybe_backup(smil_path, args.backup_suffix)
        rewritten_smil = rewrite_smil_title(smil_path, title)
        write_shift_jis(smil_path, rewritten_smil)

    print(f"updated_ncc={ncc_path}")
    print(f"updated_smil={len(smil_titles)}")
    print(f"nav_entries={len(entries)}")
    print(f"synthetic_deep_dive_entries={synthetic_count}")


if __name__ == "__main__":
    main()
