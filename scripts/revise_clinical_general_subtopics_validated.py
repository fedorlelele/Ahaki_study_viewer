import csv
from collections import Counter, defaultdict
from pathlib import Path


INPUT_CSV = Path("docs/output/clinical_general_subtopic_reassignment_proposal.csv")
OUTPUT_CSV = Path("docs/output/clinical_general_subtopic_reassignment_validated.csv")
SUMMARY_MD = Path("docs/output/clinical_general_subtopic_reassignment_validated_summary.md")
REVIEW_CSV = Path("docs/output/clinical_general_subtopic_reassignment_validated_review.csv")


TEXTBOOK_SECTION = {
    "医療面接": "第2章 第1節 医療面接（問診）",
    "医療面接（循環器主訴）": "第2章 第1節 医療面接（問診）",
    "医療面接（呼吸器主訴）": "第2章 第1節 医療面接（問診）",
    "医療面接（消化器主訴）": "第2章 第1節 医療面接（問診）",
    "医療面接（感染・全身主訴）": "第2章 第1節 医療面接（問診）",
    "医療面接（感覚器・泌尿婦人科主訴）": "第2章 第1節 医療面接（問診）",
    "医療面接（運動器主訴）": "第2章 第1節 医療面接（問診）",
    "医療面接（その他主訴）": "第2章 第1節 医療面接（問診）",
    "顔貌・顔色": "第2章 第2節 視診",
    "姿勢・歩行": "第2章 第2節 視診",
    "皮膚・発疹": "第2章 第2節 視診",
    "浮腫・体型・変形": "第2章 第2節 視診",
    "局所視診（眼・口腔・胸腹部・末梢）": "第2章 第2節 視診",
    "打診": "第2章 第3節 打診",
    "聴診": "第2章 第4節 聴診",
    "触診": "第2章 第5節 触診",
    "血圧": "第2章 第6節 測定法",
    "脈拍・循環動態": "第2章 第6節 測定法",
    "体温・熱型": "第2章 第6節 測定法",
    "身体計測・体格栄養": "第2章 第6節 測定法",
    "呼吸・意識レベル・測定総論": "第2章 第6節 測定法",
    "感覚・知覚": "第2章 第7節 神経系の診察",
    "反射の基礎・中枢": "第2章 第7節 神経系の診察",
    "反射異常・病的反射": "第2章 第7節 神経系の診察",
    "運動麻痺・小脳": "第2章 第7節 神経系の診察",
    "脳神経・高次脳機能・局在診断": "第2章 第7節 神経系の診察",
    "自律神経・意識・髄膜刺激": "第2章 第7節 神経系の診察",
    "頚肩腕・上肢テスト": "第2章 第8節 その他の身体機能の診察法",
    "腰下肢・神経伸展テスト": "第2章 第8節 その他の身体機能の診察法",
    "股・膝関節テスト": "第2章 第8節 その他の身体機能の診察法",
    "ROM・歩行・筋力・スポーツ障害": "第2章 第8節 その他の身体機能の診察法",
    "一般検査：尿検査": "第3章 第1節 一般検査",
    "一般検査：糞便検査": "第3章 第1節 一般検査",
    "一般検査：血液検査": "第3章 第1節 一般検査",
    "生化学的検査": "第3章 第2節 生化学的検査",
    "生理機能検査": "第3章 第3節 生理機能検査",
    "呼吸機能検査": "第3章 第4節 呼吸機能検査",
    "画像診断": "第3章 第5節 画像診断",
    "治療総論": "第4章 第1節 治療法総論",
    "薬物療法": "第4章 第2節 薬物療法",
    "食事療法": "第4章 第3節 食事療法",
    "理学療法": "第4章 第4節 理学療法",
    "作業療法・言語聴覚療法": "第4章 第5節 リハビリテーション関連療法",
    "特殊療法・救急治療": "第4章 第7節 その他の療法",
    "患者心理・心身症": "第5章 第1節 患者さんの心理",
    "心理検査": "第5章 第2節 心理検査",
    "心理療法": "第5章 第3節 心理療法",
}


def normalize(text: str) -> str:
    text = (text or "").replace("\n", " ").replace("\u3000", " ").lower()
    while "  " in text:
        text = text.replace("  ", " ")
    return text


def contains_any(text: str, patterns) -> bool:
    return any(pattern in text for pattern in patterns)


def map_visual(text: str) -> tuple[str, str]:
    if contains_any(
        text,
        [
            "顔貌",
            "顔色",
            "顔面",
            "蒼白",
            "黄疸",
            "チアノーゼ",
            "満月様",
            "仮面様",
            "無欲状顔貌",
            "ヒポクラテス顔貌",
        ],
    ):
        return "顔貌・顔色", "visual:face-color"
    if contains_any(
        text,
        [
            "姿勢",
            "体位",
            "起坐",
            "起坐呼吸",
            "後弓反張",
            "歩行",
            "鶏歩",
            "分回し",
            "失調性歩行",
            "小刻み歩行",
            "ウェルニッケ",
            "前かがみ",
            "アヒル歩行",
            "間欠跛行",
        ],
    ):
        return "姿勢・歩行", "visual:posture-gait"
    if contains_any(
        text,
        [
            "発疹",
            "皮疹",
            "湿疹",
            "紅斑",
            "丘疹",
            "膿疱",
            "水疱",
            "紫斑",
            "発赤",
            "熱傷",
            "皮下出血",
            "皮膚",
        ],
    ):
        return "皮膚・発疹", "visual:skin-rash"
    if contains_any(
        text,
        [
            "浮腫",
            "肥満",
            "やせ",
            "痩せ",
            "変形",
            "樽状胸",
            "水牛様",
            "バッファローハンプ",
            "胸郭",
            "関節変形",
            "脊柱",
        ],
    ):
        return "浮腫・体型・変形", "visual:edema-habitus"
    return "局所視診（眼・口腔・胸腹部・末梢）", "visual:local"


def map_measure(text: str) -> tuple[str, str]:
    if contains_any(text, ["血圧", "高血圧", "低血圧", "コロトコフ", "abi", "上肢下肢血圧比"]):
        return "血圧", "measure:blood-pressure"
    if contains_any(
        text,
        [
            "脈拍",
            "頻脈",
            "徐脈",
            "不整脈",
            "心房細動",
            "ショック",
            "循環",
            "失血",
            "乏血性ショック",
        ],
    ):
        return "脈拍・循環動態", "measure:pulse-circulation"
    if contains_any(
        text,
        ["体温", "発熱", "高熱", "微熱", "熱型", "稽留熱", "弛張熱", "間歇熱", "波状熱", "周期熱", "熱射病"],
    ):
        return "体温・熱型", "measure:temperature"
    if contains_any(
        text,
        [
            "身体計測",
            "身長",
            "体重",
            "頭囲",
            "胸囲",
            "腹囲",
            "四肢長",
            "周径",
            "bmi",
            "肥満",
            "やせ",
            "痩せ",
            "低身長",
            "高身長",
            "体格",
            "栄養状態",
            "寒さに敏感",
            "体重増加",
        ],
    ):
        return "身体計測・体格栄養", "measure:anthropometry"
    return "呼吸・意識レベル・測定総論", "measure:respiration-consciousness"


def map_neuro(text: str) -> tuple[str, str]:
    if contains_any(
        text,
        [
            "感覚",
            "知覚",
            "触覚",
            "痛覚",
            "温度覚",
            "深部感覚",
            "振動覚",
            "関節覚",
            "二点弁別覚",
            "立体認知",
            "デルマトーム",
            "手袋・靴下型",
            "知覚障害",
        ],
    ):
        return "感覚・知覚", "neuro:sensation"
    if "反射" in text:
        if contains_any(
            text,
            [
                "病的反射",
                "錐体路",
                "バビンスキー",
                "ホフマン",
                "消失",
                "亢進",
                "障害",
                "徴候",
                "病態",
            ],
        ):
            return "反射異常・病的反射", "neuro:abnormal-reflex"
        return "反射の基礎・中枢", "neuro:basic-reflex"
    if contains_any(
        text,
        [
            "運動麻痺",
            "上位運動ニューロン",
            "下位運動ニューロン",
            "筋萎縮",
            "振戦",
            "静止時振戦",
            "小脳",
            "失調",
            "筋原性",
            "母指球",
            "近位",
            "下垂手",
            "神経痛",
            "テタニー",
        ],
    ):
        return "運動麻痺・小脳", "neuro:motor-cerebellar"
    if contains_any(
        text,
        [
            "脳神経",
            "三叉",
            "動眼",
            "滑車",
            "外転",
            "顔面神経",
            "舌咽",
            "迷走",
            "副神経",
            "舌下",
            "嗄声",
            "失語",
            "高次脳機能",
            "認知症",
            "知能低下",
            "精神発達遅滞",
            "アルツハイマー",
            "失明原因",
        ],
    ):
        return "脳神経・高次脳機能・局在診断", "neuro:cranial-higher"
    return "自律神経・意識・髄膜刺激", "neuro:autonomic-consciousness"


def map_ortho(text: str) -> tuple[str, str]:
    if contains_any(
        text,
        [
            "spurling",
            "jackson",
            "adson",
            "wright",
            "morley",
            "eaton",
            "yergason",
            "speed test",
            "speed",
            "phalen",
            "froment",
            "アイヒホッフ",
            "頚腕",
            "頚肩腕",
            "手根管",
            "肩関節",
            "胸郭出口症候群",
        ],
    ):
        return "頚肩腕・上肢テスト", "ortho:cervical-upper"
    if contains_any(
        text,
        [
            "slr",
            "straight leg raising",
            "ラセーグ",
            "ブラガード",
            "ボンネット",
            "kemp",
            "ケンプ",
            "大腿神経伸展",
            "ニュートン",
            "椎間板ヘルニア",
            "脊柱管狭窄症",
            "腰下肢",
            "下肢伸展挙上",
        ],
    ):
        return "腰下肢・神経伸展テスト", "ortho:lumbar-lower"
    if contains_any(
        text,
        [
            "トレンデレンブルグ",
            "トーマス",
            "パトリック",
            "マクマレー",
            "mcmurray",
            "アプレイ",
            "ラックマン",
            "内反ストレス",
            "外反ストレス",
            "股関節",
            "膝関節",
            "開排制限",
        ],
    ):
        return "股・膝関節テスト", "ortho:hip-knee"
    return "ROM・歩行・筋力・スポーツ障害", "ortho:rom-mmt-sports"


def map_interview_symptom(text: str) -> tuple[str, str]:
    if contains_any(
        text,
        [
            "月経",
            "無月経",
            "妊娠",
            "難聴",
            "耳鳴",
            "視野狭窄",
            "失明",
            "尿閉",
            "腎盂腎炎",
            "感音性難聴",
            "伝音性難聴",
        ],
    ):
        return "医療面接（感覚器・泌尿婦人科主訴）", "interview:sensory-uro-gyn"
    if contains_any(
        text,
        [
            "骨折",
            "病的骨折",
            "腰痛",
            "関節痛",
            "大腿",
            "膝関節の痛み",
            "運動器",
            "痛みが放散",
            "安静時痛",
            "骨粗鬆症",
            "骨腫瘍",
        ],
    ):
        return "医療面接（運動器主訴）", "interview:musculoskeletal"
    if contains_any(
        text,
        [
            "腹痛",
            "下痢",
            "便秘",
            "急性腹症",
            "吐血",
            "嘔吐",
            "コーヒー残渣様",
            "右季肋部痛",
            "左下腹部痛",
            "空腹時の腹痛",
            "腹部症状",
        ],
    ):
        return "医療面接（消化器主訴）", "interview:digestive-complaint"
    if contains_any(text, ["胸痛", "動悸", "心臓", "心不全", "心疾患", "動脈疾患", "門脈圧亢進"]):
        return "医療面接（循環器主訴）", "interview:circulatory-complaint"
    if contains_any(text, ["咳", "痰", "咳嗽", "呼吸器症状", "喘息", "呼吸困難", "気胸", "喀痰"]):
        return "医療面接（呼吸器主訴）", "interview:respiratory-complaint"
    if contains_any(
        text,
        [
            "感染",
            "インフルエンザ",
            "ウイルス",
            "経皮",
            "経口感染",
            "呼吸器を介して感染",
            "発熱",
            "疲労",
            "食中毒",
            "易感染性",
            "急性炎症",
        ],
    ):
        return "医療面接（感染・全身主訴）", "interview:infection-general"
    return "医療面接（その他主訴）", "interview:misc"


def revise_row(row: dict) -> tuple[str, str, str, str]:
    stem_text = normalize(row.get("stem", ""))
    tags_text = normalize(row.get("tags", ""))
    text = normalize(" ".join([row.get("stem", ""), row.get("tags", ""), row.get("basis", ""), row.get("current_subtopics", "")]))
    original = row["proposed_primary"]

    # Validation-driven corrections that should override the previous bucket.
    if "ハンター舌炎".lower() in stem_text or "ハンター舌炎".lower() in tags_text:
        return (
            "局所視診（眼・口腔・胸腹部・末梢）",
            TEXTBOOK_SECTION["局所視診（眼・口腔・胸腹部・末梢）"],
            "high",
            "validated:hanter-glossitis-is-visual-oral-finding",
        )
    if "出血傾向".lower() in stem_text:
        return (
            "一般検査：血液検査",
            TEXTBOOK_SECTION["一般検査：血液検査"],
            "high",
            "validated:bleeding-tendency-belongs-to-blood-test",
        )
    if "貧血".lower() in stem_text and not contains_any(stem_text, ["顔色", "蒼白", "眼瞼結膜", "口唇", "舌炎", "スプーン状爪"]):
        return (
            "一般検査：血液検査",
            TEXTBOOK_SECTION["一般検査：血液検査"],
            "medium",
            "validated:anemia-default-to-blood-test",
        )
    if contains_any(stem_text, ["起坐位", "起坐呼吸", "後弓反張", "海老", "体位"]) and not contains_any(
        stem_text, ["ドレナージ", "気道異物", "一次救命", "胸骨圧迫", "窒息"]
    ):
        return (
            "姿勢・歩行",
            TEXTBOOK_SECTION["姿勢・歩行"],
            "medium",
            "validated:general-posture-position-to-visual",
        )
    if contains_any(stem_text, ["窒息", "気道異物", "異物除去"]) or (
        "誤嚥".lower() in stem_text and contains_any(stem_text, ["初期対応", "除去", "予防"])
    ):
        return (
            "特殊療法・救急治療",
            TEXTBOOK_SECTION["特殊療法・救急治療"],
            "high",
            "validated:airway-emergency-treatment",
        )

    # Obvious small-category fixes.
    if contains_any(stem_text, ["心電図", "ホルター", "脳波", "筋電図"]) and original in {"生化学的検査", "主要症候・病態"}:
        return (
            "生理機能検査",
            TEXTBOOK_SECTION["生理機能検査"],
            "high",
            "validated:physiologic-function-test",
        )

    if original == "視診":
        subtopic, reason = map_visual(text)
        return subtopic, TEXTBOOK_SECTION[subtopic], "high", reason
    if original == "測定法・バイタルサイン":
        subtopic, reason = map_measure(text)
        return subtopic, TEXTBOOK_SECTION[subtopic], "high", reason
    if original == "神経系の診察":
        subtopic, reason = map_neuro(text)
        return subtopic, TEXTBOOK_SECTION[subtopic], "high", reason
    if original == "整形外科的徒手検査・機能評価":
        subtopic, reason = map_ortho(text)
        return subtopic, TEXTBOOK_SECTION[subtopic], "high", reason
    if original == "医療面接":
        if contains_any(
            stem_text,
            [
                "腹痛",
                "下痢",
                "便秘",
                "動悸",
                "胸痛",
                "月経",
                "多尿",
                "頻尿",
                "安静時痛",
                "放散部位",
                "危険因子",
            ],
        ):
            subtopic, reason = map_interview_symptom(stem_text)
            return subtopic, TEXTBOOK_SECTION[subtopic], "medium", reason
        return original, TEXTBOOK_SECTION[original], row["confidence"], "carry:general-interview"
    if original == "主要症候・病態":
        if contains_any(text, ["浮腫", "肥満", "やせ", "発疹", "黄疸", "リンパ節腫脹", "巨大舌", "腹壁静脈怒張"]):
            subtopic, reason = map_visual(text)
            return subtopic, TEXTBOOK_SECTION[subtopic], "medium", f"overflow->{reason}"
        if contains_any(text, ["発熱", "ショック", "高血圧", "低血糖", "呼吸の異常", "呼吸異常", "過換気"]):
            if "低血糖" in text:
                return "生化学的検査", TEXTBOOK_SECTION["生化学的検査"], "medium", "overflow->biochem:low-glucose"
            subtopic, reason = map_measure(text)
            return subtopic, TEXTBOOK_SECTION[subtopic], "medium", f"overflow->{reason}"
        if contains_any(text, ["めまい", "頭痛", "意識障害", "意識喪失", "失神", "認知症"]):
            subtopic, reason = map_neuro(text)
            return subtopic, TEXTBOOK_SECTION[subtopic], "medium", f"overflow->{reason}"
        if contains_any(text, ["貧血", "出血傾向"]):
            return (
                "一般検査：血液検査",
                TEXTBOOK_SECTION["一般検査：血液検査"],
                "medium",
                "overflow->blood-test",
            )
        subtopic, reason = map_interview_symptom(stem_text)
        return subtopic, TEXTBOOK_SECTION[subtopic], "medium", f"overflow->{reason}"

    return original, TEXTBOOK_SECTION.get(original, ""), row["confidence"], "carry:original"


def main() -> None:
    rows = []
    with INPUT_CSV.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            revised_primary, revised_section, revised_confidence, revised_basis = revise_row(row)
            new_row = dict(row)
            new_row["revised_primary"] = revised_primary
            new_row["revised_chapter_section"] = revised_section
            new_row["revised_confidence"] = revised_confidence
            new_row["revised_basis"] = revised_basis
            rows.append(new_row)

    fieldnames = list(rows[0].keys())
    insert_after = fieldnames.index("proposed_primary") + 1
    ordered = fieldnames[:insert_after] + [
        "revised_primary",
        "revised_chapter_section",
        "revised_confidence",
        "revised_basis",
    ] + [name for name in fieldnames[insert_after:] if name not in {"revised_primary", "revised_chapter_section", "revised_confidence", "revised_basis"}]

    with OUTPUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=ordered)
        writer.writeheader()
        writer.writerows(rows)

    counts = Counter(row["revised_primary"] for row in rows)
    confidence_counts = Counter(row["revised_confidence"] for row in rows)
    changed = [row for row in rows if row["revised_primary"] != row["proposed_primary"]]
    section_counts = Counter(row["revised_chapter_section"] for row in rows)

    review_rows = [
        row
        for row in rows
        if row["revised_confidence"] == "low" or row["revised_primary"] == "医療面接（その他主訴）"
    ]
    with REVIEW_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "serial",
                "proposed_primary",
                "revised_primary",
                "revised_chapter_section",
                "revised_confidence",
                "revised_basis",
                "stem",
                "tags",
            ],
        )
        writer.writeheader()
        for row in review_rows:
            writer.writerow({key: row[key] for key in writer.fieldnames})

    max_bucket = counts.most_common(1)[0]
    over_40 = [(name, count) for name, count in counts.items() if count > 40]

    lines = [
        "# 臨床医学総論 再割当て案 修正版サマリー",
        "",
        f"出力CSV: `{OUTPUT_CSV}`",
        f"レビューCSV: `{REVIEW_CSV}`",
        "",
        f"総問題数: {len(rows)}",
        f"分類変更件数: {len(changed)}",
        f"最大件数小項目: {max_bucket[0]} ({max_bucket[1]}問)",
        "",
        "## 修正版小項目別件数",
        "",
    ]
    for name, count in counts.most_common():
        lines.append(f"- {name}: {count}問")

    lines.extend(["", "## 章・節別件数", ""])
    for name, count in section_counts.most_common():
        lines.append(f"- {name}: {count}問")

    lines.extend(["", "## 信頼度件数", ""])
    for name, count in confidence_counts.most_common():
        lines.append(f"- {name}: {count}問")

    lines.extend(["", "## 40問超の小項目", ""])
    if over_40:
        for name, count in sorted(over_40, key=lambda item: item[1], reverse=True):
            lines.append(f"- {name}: {count}問")
    else:
        lines.append("- なし")

    lines.extend(["", "## 変更の先頭30件", ""])
    for row in changed[:30]:
        lines.append(
            f"- {row['serial']}: {row['proposed_primary']} -> {row['revised_primary']} / {row['stem']}"
        )

    SUMMARY_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
