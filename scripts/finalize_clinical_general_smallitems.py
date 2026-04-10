import csv
from collections import Counter, defaultdict
from pathlib import Path


INPUT_CSV = Path("docs/output/clinical_general_textbook_section_reorganized.csv")
OUTPUT_CSV = Path("docs/output/clinical_general_final_smallitem_classification.csv")
SUMMARY_MD = Path("docs/output/clinical_general_final_smallitem_summary.md")

HOLDOUT = "保留（教科書節対応外・各論寄り）"
CHAPTER4_PREFIX = "第４章 "


SECTION_TO_SMALLITEM = {
    "第１章 第６節 関連用語の理解": "診察の概要",
    "第２章 第１節 医療面接（問診）": "医療面接",
    "第２章 第２節 視診": "視診",
    "第２章 第３節 打診": "打診",
    "第２章 第４節 聴診": "聴診",
    "第２章 第５節 触診": "触診",
    "第２章 第６節 測定法": "測定法",
    "第２章 第７節 神経系の診察": "神経系の診察",
    "第２章 第８節 その他の身体機能の診察法": "運動機能・整形外科的検査",
    "第３章 第１節 一般検査": "一般検査",
    "第３章 第２節 生化学的検査": "生化学的検査",
    "第３章 第３節 生理学的検査および画像診断の概要": "生理・画像検査",
    "第５章 第１節 患者さんの心理": "患者心理",
}


def map_smallitem(section: str, section_subtopic: str) -> tuple[str, str]:
    if section == HOLDOUT:
        return section_subtopic, "holdout_subtopic"
    if section.startswith(CHAPTER4_PREFIX):
        return "治療法", "chapter4_unified"
    smallitem = SECTION_TO_SMALLITEM.get(section)
    if smallitem:
        return smallitem, "section_name"
    raise ValueError(f"Unhandled section: {section}")


def main():
    with INPUT_CSV.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    output_rows = []
    smallitem_counts = Counter()
    source_section_counts = defaultdict(Counter)
    basis_counts = Counter()

    for row in rows:
        final_smallitem, basis = map_smallitem(
            row["textbook_section"], row["section_subtopic"]
        )
        new_row = dict(row)
        new_row["final_smallitem"] = final_smallitem
        new_row["final_smallitem_basis"] = basis
        output_rows.append(new_row)

        smallitem_counts[final_smallitem] += 1
        source_section_counts[final_smallitem][row["textbook_section"]] += 1
        basis_counts[basis] += 1

    fieldnames = list(output_rows[0].keys())
    with OUTPUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(output_rows)

    lines = [
        "# 臨床医学総論 最終小項目サマリー",
        "",
        f"入力CSV: `{INPUT_CSV}`",
        f"出力CSV: `{OUTPUT_CSV}`",
        "",
        f"総問題数: {len(output_rows)}",
        "",
        "## 最終小項目別件数",
        "",
    ]
    for smallitem, count in smallitem_counts.most_common():
        lines.append(f"- {smallitem}: {count}問")

    lines.extend(
        [
            "",
            "## 付与ルール別件数",
            "",
        ]
    )
    for basis, count in basis_counts.most_common():
        lines.append(f"- {basis}: {count}問")

    lines.extend(
        [
            "",
            "## 節名の読み替え",
            "",
            "- 第１章 第６節 関連用語の理解 -> 診察の概要",
            "- 第２章 第８節 その他の身体機能の診察法 -> 運動機能・整形外科的検査",
            "- 第３章 第３節 生理学的検査および画像診断の概要 -> 生理・画像検査",
            "- 第５章 第１節 患者さんの心理 -> 患者心理",
            "- 第４章の各節 -> 治療法",
        ]
    )

    lines.extend(["", "## 最終小項目ごとの出典節", ""])
    for smallitem, count in smallitem_counts.most_common():
        lines.append(f"### {smallitem} ({count}問)")
        for section, section_count in source_section_counts[smallitem].most_common():
            lines.append(f"- {section}: {section_count}問")
        lines.append("")

    SUMMARY_MD.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
