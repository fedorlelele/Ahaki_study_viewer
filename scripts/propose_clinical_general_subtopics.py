import argparse
import csv
import json
import sqlite3
from collections import defaultdict
from pathlib import Path


SUBJECT = "臨床医学総論"
SUPPLEMENTAL_BUCKET = "主要症候・病態"

CATEGORY_ORDER = [
    "診察総論",
    "診療記録・POS",
    "診察関連用語",
    "医療面接",
    "視診",
    "打診",
    "聴診",
    "触診",
    "測定法・バイタルサイン",
    "神経系の診察",
    "整形外科的徒手検査・機能評価",
    "一般検査：尿検査",
    "一般検査：糞便検査",
    "一般検査：血液検査",
    "生化学的検査",
    "生理機能検査",
    "呼吸機能検査",
    "画像診断",
    "治療総論",
    "薬物療法",
    "食事療法",
    "理学療法",
    "作業療法・言語聴覚療法",
    "特殊療法・救急治療",
    "患者心理・心身症",
    "心理検査",
    "心理療法",
    SUPPLEMENTAL_BUCKET,
]

CURRENT_SUBTOPIC_MAP = {
    "医療面接": [("医療面接", 8)],
    "視診": [("視診", 8)],
    "打診": [("打診", 8)],
    "聴診": [("聴診", 8)],
    "触診": [("触診", 8)],
    "身体計測": [("測定法・バイタルサイン", 6), ("整形外科的徒手検査・機能評価", 2)],
    "生命徴候": [("測定法・バイタルサイン", 8)],
    "神経系の検査": [("神経系の診察", 6), ("生理機能検査", 2)],
    "運動機能検査": [("整形外科的徒手検査・機能評価", 6), ("神経系の診察", 2)],
    "臨床検査法": [
        ("一般検査：尿検査", 2),
        ("一般検査：糞便検査", 2),
        ("一般検査：血液検査", 2),
        ("生化学的検査", 2),
        ("生理機能検査", 2),
        ("呼吸機能検査", 2),
        ("画像診断", 2),
    ],
    "治療法": [
        ("治療総論", 2),
        ("薬物療法", 2),
        ("食事療法", 1),
        ("理学療法", 1),
        ("作業療法・言語聴覚療法", 1),
        ("特殊療法・救急治療", 2),
        ("心理療法", 1),
    ],
    "症候": [(SUPPLEMENTAL_BUCKET, 3)],
}

STRONG_PATTERNS = {
    "診察総論": [
        "インフォームド・コンセント",
        "ラポール",
        "プライバシー",
        "患者中心",
        "診察の意義",
        "一般的心得",
        "身だしなみ",
        "言葉遣い",
    ],
    "診療記録・POS": [
        "pos",
        "soap",
        "カルテ",
        "診療録",
        "問題志向",
        "問題リスト",
        "初期計画",
        "経過記録",
        "退院時要約",
    ],
    "診察関連用語": ["予後", "転帰", "自覚症状", "他覚症状"],
    "医療面接": [
        "医療面接",
        "問診",
        "主訴",
        "現病歴",
        "既往歴",
        "家族歴",
        "患者背景",
        "開かれた質問",
        "閉じた質問",
        "focused question",
        "multiple choice question",
    ],
    "視診": [
        "視診",
        "病的顔貌",
        "顔色",
        "黄疸",
        "チアノーゼ",
        "浮腫",
        "姿勢",
        "歩行",
        "不随意運動",
        "皮膚の異常",
        "爪の異常",
        "リンパ節",
        "眼球突出",
        "瞳孔",
        "眼底",
    ],
    "打診": ["打診", "打診音", "清音", "濁音", "鼓音", "肺肝境界", "心濁音界"],
    "聴診": [
        "聴診",
        "呼吸音",
        "副雑音",
        "ラ音",
        "胸膜摩擦音",
        "心音",
        "心雑音",
        "腸蠕動音",
        "振水音",
        "血管雑音",
    ],
    "触診": [
        "触診",
        "圧痛",
        "脾腫",
        "肝腫",
        "声音振盪",
        "心尖拍動",
        "thrill",
        "リンパ節",
        "甲状腺",
        "腹部の触診",
    ],
    "測定法・バイタルサイン": [
        "バイタルサイン",
        "生命徴候",
        "身体計測",
        "頭囲",
        "身長",
        "体重",
        "四肢長",
        "周径",
        "体温",
        "稽留熱",
        "弛張熱",
        "間歇熱",
        "波状熱",
        "周期熱",
        "脈拍",
        "血圧",
        "呼吸数",
        "高血圧",
        "低血圧",
        "頻脈",
        "徐脈",
    ],
    "神経系の診察": [
        "感覚検査",
        "表在感覚",
        "深部感覚",
        "複合感覚",
        "振動覚",
        "関節覚",
        "二点弁別覚",
        "立体認知",
        "反射",
        "深部反射",
        "病的反射",
        "表在反射",
        "自律神経反射",
        "筋トーヌス",
        "筋萎縮",
        "線維束攣縮",
        "運動麻痺",
        "運動失調",
        "協調運動",
        "平衡障害",
        "脳神経",
        "項部硬直",
        "ケルニッヒ",
        "ブルジンスキー",
        "見当識",
        "錐体外路",
        "意識障害",
        "意識喪失",
        "めまい",
    ],
    "整形外科的徒手検査・機能評価": [
        "徒手検査",
        "徒手による整形外科的検査法",
        "slr",
        "spurling",
        "jackson",
        "adson",
        "wright",
        "morley",
        "eaton",
        "dawburn",
        "yergason",
        "speed test",
        "phalen",
        "froment",
        "eichhoff",
        "kemp",
        "lasegue",
        "bragard",
        "bonnet",
        "thomas test",
        "trendelenburg",
        "patrick test",
        "mcmurray",
        "apley",
        "lachman",
        "関節可動域",
        "rom",
        "徒手筋力",
        "mmt",
        "adl",
        "骨塩定量",
    ],
    "一般検査：尿検査": [
        "尿検査",
        "尿量",
        "多尿",
        "乏尿",
        "無尿",
        "尿比重",
        "尿ph",
        "尿蛋白",
        "蛋白尿",
        "尿糖",
        "尿潜血",
        "血尿",
        "高比重尿",
        "低比重尿",
        "ウロビリノゲン",
        "ケトン体",
        "尿沈渣",
        "尿崩症",
        "頻尿",
    ],
    "一般検査：糞便検査": ["糞便", "便潜血", "寄生虫", "タール便", "灰白色便", "便の色", "便検査"],
    "一般検査：血液検査": [
        "血液検査",
        "赤血球",
        "白血球",
        "血小板",
        "ヘモグロビン",
        "ヘマトクリット",
        "網赤血球",
        "赤沈",
        "白血球分画",
        "mcv",
        "mch",
        "mchc",
        "血算",
        "汎血球減少",
        "出血傾向",
    ],
    "生化学的検査": [
        "生化学",
        "ast",
        "got",
        "alt",
        "gpt",
        "γ-gtp",
        "γ-gt",
        "alp",
        "ldh",
        "ck",
        "アミラーゼ",
        "リパーゼ",
        "総コレステロール",
        "hdl",
        "トリグリセリド",
        "ビリルビン",
        "血糖",
        "ヘモグロビンa1c",
        "hba1c",
        "crp",
        "rf",
        "抗核抗体",
        "抗ccp",
        "クームス",
        "aso",
        "梅毒血清反応",
        "hiv",
        "腫瘍マーカー",
        "電解質",
        "ナトリウム",
        "カリウム",
        "クロール",
        "カルシウム",
        "無機リン",
        "血清鉄",
        "血液ガス",
        "血清ガス",
        "aptt",
        "pt ",
        "プロトロンビン時間",
        "活性化部分トロンボプラスチン時間",
        "アルブミン",
        "総蛋白",
        "尿素窒素",
        "クレアチニン",
        "尿酸",
    ],
    "生理機能検査": [
        "心電図",
        "ecg",
        "脳波",
        "eeg",
        "筋電図",
        "emg",
        "神経伝導速度",
        "ncv",
        "オージオメトリー",
        "オージオメーター",
        "聴力検査",
    ],
    "呼吸機能検査": [
        "呼吸機能検査",
        "肺活量",
        "努力肺活量",
        "1秒量",
        "1秒率",
        "スパイロ",
        "分時換気量",
        "1回換気量",
        "肺胞換気量",
        "死腔換気量",
        "％vc",
        "%vc",
        "基礎代謝率",
    ],
    "画像診断": [
        "画像診断",
        "x線",
        "エックス線",
        "レントゲン",
        "ct",
        "mri",
        "シンチ",
        "pet",
        "spect",
        "超音波",
        "エコー",
        "造影",
        "内視鏡",
        "サーモグラフィ",
        "ri",
        "放射性同位元素",
    ],
    "治療総論": ["自然治癒力", "原因療法", "対症療法", "救命療法", "保存療法", "生活指導", "治療の意義"],
    "薬物療法": [
        "薬物療法",
        "抗菌薬",
        "抗生物質",
        "抗ウイルス",
        "抗アレルギー",
        "ステロイド",
        "向精神薬",
        "抗不安薬",
        "抗うつ薬",
        "抗痙攣薬",
        "降圧",
        "強心薬",
        "狭心症治療薬",
        "不整脈治療薬",
        "去痰薬",
        "気管支拡張薬",
        "下剤",
        "止痢薬",
        "制吐薬",
        "抗潰瘍薬",
        "ホルモン剤",
        "ビタミン剤",
        "利尿剤",
        "抗パーキンソン薬",
        "抗悪性腫瘍薬",
        "抗結核薬",
        "鎮痛薬",
        "解熱薬",
        "麻薬性鎮痛薬",
        "非麻薬性鎮痛薬",
        "抗血栓",
        "インスリン",
    ],
    "食事療法": ["食事療法", "糖尿病食", "塩分制限", "カロリー制限", "肥満症の食事療法", "腎臓疾患の食事療法"],
    "理学療法": [
        "理学療法",
        "運動療法",
        "物理療法",
        "温熱療法",
        "水治療法",
        "温泉療法",
        "光線療法",
        "電気療法",
        "牽引",
        "低周波",
        "レーザー療法",
    ],
    "作業療法・言語聴覚療法": ["作業療法", "言語聴覚療法", "失語", "嚥下", "構音", "st"],
    "特殊療法・救急治療": [
        "一次救命処置",
        "二次救命処置",
        "bls",
        "aed",
        "心肺蘇生",
        "心臓マッサージ",
        "胸骨圧迫",
        "人工呼吸",
        "気道確保",
        "手術",
        "麻酔",
        "神経ブロック",
        "放射線療法",
        "icu",
        "ccu",
        "透析",
        "ペースメーカー",
        "気管切開",
        "輸液",
        "輸血",
        "血漿交換",
        "体位ドレナージ",
        "ネブライザー",
        "緩和ケア",
        "骨髄移植",
        "腎移植",
        "免疫療法",
        "γ-グロブリン",
        "高圧酸素",
        "滅菌",
        "消毒",
        "洗浄",
        "ターミナルケア",
        "終末期",
    ],
    "患者心理・心身症": [
        "心身相関",
        "心身症",
        "神経症",
        "ストレス関連障害",
        "身体表現性障害",
        "ptsd",
        "パニック障害",
        "強迫",
        "恐怖症",
        "自律神経失調",
        "心気症",
        "抑うつ",
        "不安",
    ],
    "心理検査": [
        "知能検査",
        "ビネー",
        "ウェクスラー",
        "wisc",
        "wais",
        "性格検査",
        "y-g",
        "mmpi",
        "ロールシャッハ",
        "cmi",
        "bs-pop",
    ],
    "心理療法": [
        "心理療法",
        "カウンセリング",
        "精神分析",
        "認知療法",
        "行動療法",
        "認知行動療法",
        "自律神経訓練法",
    ],
    SUPPLEMENTAL_BUCKET: [
        "胸痛",
        "腹痛",
        "腰痛",
        "下痢",
        "便秘",
        "感染経路",
        "耳鳴",
        "やせ",
        "肥満",
    ],
}

SOFT_PATTERNS = {
    "視診": ["顔面", "皮膚", "爪", "黄染", "浮腫", "歩行異常", "顔色", "体位", "姿勢"],
    "聴診": ["喘鳴", "雑音", "呼吸音", "心雑音", "ラ音"],
    "触診": ["圧痛", "腫大", "触れる", "拍動"],
    "測定法・バイタルサイン": ["高血圧", "低血圧", "頻脈", "徐脈", "発熱", "体温", "脈圧"],
    "神経系の診察": ["めまい", "意識", "失調", "感覚", "神経痛", "脳神経", "対光反射", "瞳孔", "錐体路"],
    "整形外科的徒手検査・機能評価": ["膝関", "股関節", "肩関節", "関節", "運動機能", "可動域", "筋力", "徒手", "テスト"],
    "一般検査：尿検査": ["尿", "尿路", "膀胱", "腎盂", "腎炎"],
    "一般検査：糞便検査": ["便", "消化管出血"],
    "一般検査：血液検査": ["貧血", "白血病", "血球", "血小板", "血液疾患"],
    "生化学的検査": ["血清", "電解質", "内分泌", "肝機能検査"],
    "生理機能検査": ["検査", "機能検査"],
    "呼吸機能検査": ["換気", "肺気量", "スパイロメトリー"],
    "画像診断": ["検査法", "画像", "放射", "描出"],
    "薬物療法": ["薬", "薬剤", "投与", "禁忌", "副作用"],
    "食事療法": ["栄養", "食事"],
    "理学療法": ["物理療法", "治療体操", "アイシング"],
    "作業療法・言語聴覚療法": ["作業療法", "言語"],
    "特殊療法・救急治療": ["救急", "応急処置", "処置", "集中治療", "適応"],
    "患者心理・心身症": ["精神", "心理", "知能低下", "認知症"],
    "心理検査": ["質問票", "スクリーニング"],
    "心理療法": ["面接", "療法"],
}

DIRECT_CATEGORIES = {
    "診察総論",
    "診療記録・POS",
    "診察関連用語",
    "医療面接",
    "視診",
    "打診",
    "聴診",
    "触診",
    "測定法・バイタルサイン",
    "神経系の診察",
    "整形外科的徒手検査・機能評価",
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Propose reassigned subtopics for 臨床医学総論 without updating SQL."
    )
    parser.add_argument("--db", default="output/ahaki.sqlite", help="SQLite DB path.")
    parser.add_argument(
        "--csv-out",
        default="docs/output/clinical_general_subtopic_reassignment_proposal.csv",
        help="CSV proposal output path.",
    )
    parser.add_argument(
        "--md-out",
        default="docs/output/clinical_general_subtopic_reassignment_summary.md",
        help="Markdown summary output path.",
    )
    return parser.parse_args()


def load_questions(db_path: Path):
    query = """
        SELECT
            q.serial,
            q.exam_type,
            q.exam_session,
            q.case_text,
            q.stem,
            q.choices_json,
            COALESCE((
                SELECT GROUP_CONCAT(st.name, ' / ')
                FROM question_subtopics qs
                JOIN subtopics st ON st.id = qs.subtopic_id
                WHERE qs.question_id = q.id
                ORDER BY st.name
            ), '') AS current_subtopics,
            COALESCE((
                SELECT GROUP_CONCAT(t.label, ' / ')
                FROM question_tags qt
                JOIN tags t ON t.id = qt.tag_id
                WHERE qt.question_id = q.id
                ORDER BY t.label
            ), '') AS tags
        FROM questions q
        JOIN subjects s ON s.id = q.subject_id
        WHERE s.name = ?
        ORDER BY q.exam_session, q.serial
    """
    conn = sqlite3.connect(db_path)
    rows = conn.execute(query, (SUBJECT,)).fetchall()
    conn.close()
    return rows


def normalize_text(text: str) -> str:
    return text.lower().replace("　", " ").replace("（", "(").replace("）", ")")


def split_labels(value: str):
    if not value:
        return []
    return [part.strip() for part in value.split(" / ") if part.strip()]


def add_score(scores, reasons, target, weight, reason):
    scores[target] += weight
    if reason not in reasons[target]:
        reasons[target].append(reason)


def apply_patterns(scores, reasons, target, text, patterns, weight, prefix):
    matches = []
    for pattern in patterns:
        if pattern.lower() in text:
            matches.append(pattern)
    for pattern in matches[:5]:
        add_score(scores, reasons, target, weight, f"{prefix}:{pattern}")


def classify_question(row):
    (
        serial,
        exam_type,
        exam_session,
        case_text,
        stem,
        choices_json,
        current_subtopics_raw,
        tags_raw,
    ) = row

    current_subtopics = split_labels(current_subtopics_raw)
    tags = split_labels(tags_raw)
    try:
        choices = json.loads(choices_json) if choices_json else []
    except json.JSONDecodeError:
        choices = []

    text_blob = " ".join(
        [stem or "", case_text or "", " ".join(choices), " ".join(tags), " ".join(current_subtopics)]
    )
    text = normalize_text(text_blob)

    scores = defaultdict(int)
    reasons = defaultdict(list)

    for current in current_subtopics:
        for target, weight in CURRENT_SUBTOPIC_MAP.get(current, []):
            add_score(scores, reasons, target, weight, f"current:{current}")

    for target, patterns in STRONG_PATTERNS.items():
        apply_patterns(scores, reasons, target, text, patterns, 3, "strong")
    for target, patterns in SOFT_PATTERNS.items():
        apply_patterns(scores, reasons, target, text, patterns, 1, "soft")

    if any(label in current_subtopics for label in ("症候",)) and not any(
        direct in current_subtopics for direct in ("視診", "打診", "聴診", "触診", "生命徴候", "身体計測", "神経系の検査", "運動機能検査", "臨床検査法", "治療法", "医療面接")
    ):
        add_score(scores, reasons, SUPPLEMENTAL_BUCKET, 2, "fallback:single-symptom-bucket")

    if not scores:
        add_score(scores, reasons, SUPPLEMENTAL_BUCKET, 1, "fallback:no-match")

    ranked = sorted(
        CATEGORY_ORDER,
        key=lambda category: (-scores.get(category, 0), CATEGORY_ORDER.index(category)),
    )
    primary = ranked[0]
    secondary = ""
    if len(ranked) > 1 and scores.get(ranked[1], 0) > 0 and scores.get(primary, 0) - scores.get(ranked[1], 0) <= 2:
        secondary = ranked[1]

    primary_score = scores.get(primary, 0)
    secondary_score = scores.get(secondary, 0) if secondary else 0
    gap = primary_score - secondary_score

    if primary == SUPPLEMENTAL_BUCKET:
        fit_level = "overflow"
    elif any(reason.startswith("current:") for reason in reasons[primary]) or primary in DIRECT_CATEGORIES:
        fit_level = "direct"
    else:
        fit_level = "nearest"

    if primary == SUPPLEMENTAL_BUCKET:
        confidence = "low"
    elif fit_level == "direct":
        if primary_score >= 10 and gap >= 2:
            confidence = "high"
        elif primary_score >= 6:
            confidence = "medium"
        else:
            confidence = "low"
    else:
        if primary_score >= 10 and gap >= 3:
            confidence = "medium"
        else:
            confidence = "low"

    basis = "; ".join(reasons[primary][:8])
    secondary_basis = "; ".join(reasons[secondary][:5]) if secondary else ""

    return {
        "serial": serial,
        "exam_type": exam_type,
        "exam_session": exam_session,
        "current_subtopics": current_subtopics_raw,
        "proposed_primary": primary,
        "proposed_secondary": secondary,
        "fit_level": fit_level,
        "confidence": confidence,
        "primary_score": primary_score,
        "secondary_score": secondary_score,
        "basis": basis,
        "secondary_basis": secondary_basis,
        "tags": tags_raw,
        "stem": stem,
    }


def write_csv(records, out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "serial",
        "exam_type",
        "exam_session",
        "current_subtopics",
        "proposed_primary",
        "proposed_secondary",
        "fit_level",
        "confidence",
        "primary_score",
        "secondary_score",
        "basis",
        "secondary_basis",
        "tags",
        "stem",
    ]
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)


def render_top_counts(records, key):
    counts = defaultdict(int)
    for record in records:
        counts[record[key]] += 1
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))


def write_summary(records, out_path: Path, csv_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)

    by_primary = render_top_counts(records, "proposed_primary")
    by_fit = render_top_counts(records, "fit_level")
    by_confidence = render_top_counts(records, "confidence")
    low_conf = [record for record in records if record["confidence"] == "low"]
    overflow = [record for record in records if record["proposed_primary"] == SUPPLEMENTAL_BUCKET]

    lines = []
    lines.append(f"# {SUBJECT} 再割当て案サマリー")
    lines.append("")
    lines.append("出力ファイル:")
    lines.append(f"- CSV: `{csv_path}`")
    lines.append("")
    lines.append(f"総問題数: {len(records)}")
    lines.append("")
    lines.append("## 提案小項目別件数")
    lines.append("")
    for category, count in by_primary:
        lines.append(f"- {category}: {count}問")
    lines.append("")
    lines.append("## 適合レベル件数")
    lines.append("")
    for label, count in by_fit:
        lines.append(f"- {label}: {count}問")
    lines.append("")
    lines.append("## 信頼度件数")
    lines.append("")
    for label, count in by_confidence:
        lines.append(f"- {label}: {count}問")
    lines.append("")
    lines.append("## 補助分類が必要な候補")
    lines.append("")
    lines.append(
        f"- `{SUPPLEMENTAL_BUCKET}` に入った問題: {len(overflow)}問"
    )
    lines.append("- 章・節だけでは吸収しにくい、症候中心または総合病態型の旧出題を仮置きしている。")
    lines.append("")
    lines.append("## 低信頼度の先頭50件")
    lines.append("")
    for record in low_conf[:50]:
        lines.append(
            f"- {record['serial']}: {record['proposed_primary']} / {record['fit_level']} / {record['stem']}"
        )
    lines.append("")
    lines.append("## 補助分類候補の先頭50件")
    lines.append("")
    for record in overflow[:50]:
        lines.append(
            f"- {record['serial']}: {record['current_subtopics']} -> {record['proposed_primary']} / {record['stem']}"
        )
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main():
    args = parse_args()
    db_path = Path(args.db)
    csv_path = Path(args.csv_out)
    md_path = Path(args.md_out)

    rows = load_questions(db_path)
    records = [classify_question(row) for row in rows]

    write_csv(records, csv_path)
    write_summary(records, md_path, csv_path)

    print(f"Loaded {len(records)} questions from {db_path}")
    print(f"CSV written: {csv_path}")
    print(f"Summary written: {md_path}")


if __name__ == "__main__":
    main()
