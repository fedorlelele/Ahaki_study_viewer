import csv
import json
import sqlite3
from collections import Counter
from pathlib import Path


DB_PATH = Path("output/ahaki.sqlite")
OUTPUT_CSV = Path("docs/output/clinical_general_textbook_section_classification.csv")
SUMMARY_MD = Path("docs/output/clinical_general_textbook_section_classification_summary.md")
HOLDOUT_CSV = Path("docs/output/clinical_general_textbook_section_holdout.csv")
REVIEW_CSV = Path("docs/output/clinical_general_textbook_section_review.csv")

SUBJECT = "臨床医学総論"
HOLDOUT = "保留（教科書節対応外・各論寄り）"


SECTIONS = [
    {
        "name": "第１章 第１節 意義",
        "stem_terms": ["診察の意義", "診断の基礎", "診断と治療", "経過観察", "診療は"],
        "choice_terms": ["診断", "治療", "経過観察"],
        "min_score": 3,
        "require_stem": True,
    },
    {
        "name": "第１章 第２節 一般的心得",
        "stem_terms": ["ラポール", "プライバシー", "インフォームド・コンセント", "守秘義務", "身だしなみ", "言葉遣い", "敬語"],
        "choice_terms": ["ラポール", "プライバシー", "インフォームド・コンセント", "守秘義務"],
        "min_score": 3,
    },
    {
        "name": "第１章 第３節 診察の内容",
        "stem_terms": ["身体診察", "診察の内容", "全身的な診察", "局所的な診察"],
        "choice_terms": ["医療面接", "身体診察", "全身", "局所"],
        "min_score": 3,
        "require_stem": True,
    },
    {
        "name": "第１章 第４節 記録の目的と内容",
        "stem_terms": ["カルテ", "診療録", "記録の目的", "記録の内容"],
        "choice_terms": ["カルテ", "診療録"],
        "min_score": 3,
    },
    {
        "name": "第１章 第５節 POS",
        "stem_terms": ["pos", "soap", "問題志向", "問題リスト", "初期計画", "経過記録", "退院時要約"],
        "choice_terms": ["pos", "soap", "subjective", "objective", "assessment", "plan"],
        "min_score": 3,
    },
    {
        "name": "第１章 第６節 関連用語の理解",
        "stem_terms": ["予後", "転帰", "自覚症状", "他覚症状", "他覚的身体所見"],
        "choice_terms": ["予後", "転帰", "自覚症状", "他覚症状", "他覚的身体所見"],
        "min_score": 3,
    },
    {
        "name": "第２章 第１節 医療面接（問診）",
        "stem_terms": [
            "医療面接",
            "問診",
            "open ended",
            "closed question",
            "focused question",
            "multiple choice question",
            "現病歴",
            "既往歴",
            "家族歴",
            "患者背景",
            "説明モデル",
            "共感的態度",
            "支持的態度",
            "要約と確認",
        ],
        "choice_terms": ["現病歴", "既往歴", "家族歴", "患者背景"],
        "min_score": 3,
        "require_stem": True,
    },
    {
        "name": "第２章 第２節 視診",
        "stem_terms": [
            "視診",
            "顔貌",
            "顔色",
            "黄疸",
            "チアノーゼ",
            "蒼白",
            "姿勢",
            "歩行",
            "浮腫",
            "皮疹",
            "発疹",
            "原発疹",
            "続発疹",
            "皮膚の色調",
            "巨大舌",
            "腹壁静脈怒張",
            "樽状胸",
            "スプーン状爪",
            "ばち指",
            "アセトン臭",
            "手指振せん",
            "リンパ節腫脹",
            "脊柱の生理的弯曲",
        ],
        "choice_terms": [
            "顔貌",
            "顔色",
            "黄疸",
            "チアノーゼ",
            "蒼白",
            "浮腫",
            "皮疹",
            "発疹",
            "巨大舌",
            "腹壁静脈怒張",
            "スプーン状爪",
            "樽状胸",
            "ばち指",
            "歩行",
            "姿勢",
        ],
        "min_score": 2,
    },
    {
        "name": "第２章 第３節 打診",
        "stem_terms": ["打診", "打診音", "清音", "濁音", "鼓音", "過清音", "肺肝境界", "心濁音界"],
        "choice_terms": ["打診", "清音", "濁音", "鼓音", "過清音"],
        "min_score": 3,
    },
    {
        "name": "第２章 第４節 聴診",
        "stem_terms": [
            "聴診",
            "心音",
            "心雑音",
            "呼吸音",
            "副雑音",
            "ラ音",
            "胸膜摩擦音",
            "腸蠕動音",
            "振水音",
            "血管雑音",
            "静脈雑音",
            "喘鳴",
        ],
        "choice_terms": ["心音", "心雑音", "呼吸音", "副雑音", "ラ音", "腸蠕動音", "振水音", "血管雑音"],
        "min_score": 2,
    },
    {
        "name": "第２章 第５節 触診",
        "stem_terms": [
            "触診",
            "圧痛",
            "反跳痛",
            "筋性防御",
            "板状硬",
            "脾腫",
            "肝腫",
            "肝臓",
            "脾臓",
            "腫瘤",
            "波動",
            "マックバーネー",
            "ランツ点",
            "モンロー点",
            "リンパ節",
            "甲状腺",
            "心尖拍動",
            "声音振盪",
            "thrill",
            "指診",
            "拍動が触れ",
        ],
        "choice_terms": ["圧痛", "反跳痛", "筋性防御", "脾腫", "肝腫", "腫瘤", "波動", "甲状腺", "リンパ節", "指診"],
        "min_score": 2,
    },
    {
        "name": "第２章 第６節 測定法",
        "stem_terms": [
            "バイタルサイン",
            "生命徴候",
            "身体計測",
            "頭囲",
            "身長",
            "体重",
            "四肢長",
            "上肢長",
            "下肢長",
            "周径",
            "体温",
            "熱型",
            "稽留熱",
            "弛張熱",
            "間歇熱",
            "波状熱",
            "周期熱",
            "脈拍",
            "血圧",
            "徐脈",
            "頻脈",
            "呼吸異常",
            "呼吸数",
            "血圧測定",
            "意識障害",
            "傾眠",
            "昏迷",
            "昏睡",
            "低身長",
            "肥満",
            "やせ",
        ],
        "choice_terms": ["血圧", "脈拍", "体温", "熱型", "徐脈", "頻脈", "高血圧", "低血圧", "ショック", "体重", "身長"],
        "min_score": 2,
    },
    {
        "name": "第２章 第７節 神経系の診察",
        "stem_terms": [
            "感覚",
            "知覚",
            "振動覚",
            "関節覚",
            "二点弁別",
            "立体認知",
            "デルマトーム",
            "反射",
            "深部反射",
            "表在反射",
            "病的反射",
            "錐体路",
            "上位運動ニューロン",
            "下位運動ニューロン",
            "脳神経",
            "髄膜刺激",
            "項部硬直",
            "ケルニッヒ",
            "ブルジンスキー",
            "小脳",
            "運動麻痺",
            "筋萎縮",
            "自律神経",
            "三叉神経",
            "動眼神経",
        ],
        "choice_terms": ["感覚", "知覚", "振動覚", "関節覚", "反射", "病的反射", "脳神経", "項部硬直", "ケルニッヒ", "ブルジンスキー"],
        "min_score": 2,
    },
    {
        "name": "第２章 第８節 その他の身体機能の診察法",
        "stem_terms": [
            "徒手検査",
            "スパーリング",
            "ジャクソン",
            "アドソン",
            "モーレイ",
            "ライトテスト",
            "イートンテスト",
            "ペインフルアーク",
            "ダウバーン",
            "ヤーガソン",
            "speed test",
            "ファレン",
            "フローマン",
            "アイヒホッフ",
            "ケンプ",
            "slr",
            "ラセーグ",
            "ブラガード",
            "ボンネット",
            "大腿神経伸展",
            "ニュートン",
            "トーマス",
            "トレンデレンブルグ",
            "パトリック",
            "マックマレー",
            "アプレイ",
            "ラックマン",
            "内反ストレス",
            "外反ストレス",
            "rom",
            "mmt",
            "adl",
            "歩行周期",
            "良肢位",
            "回外運動",
            "外転運動",
            "球関節",
            "関節可動域",
            "手根管症候群",
            "手根管症侯群",
            "チネル徴候",
        ],
        "choice_terms": ["徒手検査", "slr", "ラセーグ", "rom", "mmt", "adl", "トレンデレンブルグ", "マックマレー"],
        "min_score": 2,
    },
    {
        "name": "第３章 第１節 一般検査",
        "stem_terms": [
            "尿検査",
            "尿量",
            "多尿",
            "乏尿",
            "無尿",
            "糞便",
            "便潜血",
            "尿比重",
            "高比重尿",
            "低比重尿",
            "蛋白尿",
            "血尿",
            "尿沈渣",
            "尿所見",
            "尿に蛋白",
            "尿中",
            "ケトン体",
            "ウロビリノゲン",
            "尿中ビリルビン",
            "赤血球数",
            "ヘモグロビン量",
            "ヘマトクリット",
            "白血球数",
            "血小板",
            "血沈",
            "貧血",
            "髄液",
            "クウェッケンシュテッド",
            "培養",
            "薬剤感受性",
        ],
        "choice_terms": ["蛋白尿", "血尿", "尿比重", "赤血球数", "ヘモグロビン量", "ヘマトクリット", "白血球数", "血小板", "血沈", "髄液", "培養"],
        "min_score": 2,
    },
    {
        "name": "第３章 第２節 生化学的検査",
        "stem_terms": [
            "got",
            "gpt",
            "ast",
            "alt",
            "γ-gtp",
            "ggtp",
            "alp",
            "ldh",
            "ck",
            "amylase",
            "crp",
            "血糖",
            "高血糖",
            "低カリウム",
            "ナトリウム",
            "カリウム",
            "クロール",
            "カルシウム",
            "血清リン",
            "クレアチニン",
            "尿酸",
            "アルブミン",
            "a/g",
            "コレステロール",
            "中性脂肪",
            "腫瘍マーカー",
            "血清ガス",
            "pco2",
            "po2",
            "hbA1c".lower(),
            "フェリチン",
            "tibc".lower(),
        ],
        "choice_terms": ["got", "gpt", "ast", "alt", "γ-gtp", "ldh", "ck", "amylase", "crp", "血糖", "クレアチニン", "腫瘍マーカー", "コレステロール"],
        "min_score": 2,
    },
    {
        "name": "第３章 第３節 生理学的検査および画像診断の概要",
        "stem_terms": [
            "心電図",
            "ecg",
            "脳波",
            "eeg",
            "筋電図",
            "emg",
            "神経伝導速度",
            "ncv",
            "呼吸機能検査",
            "肺活量",
            "1秒率",
            "%vc".lower(),
            "ct",
            "mri",
            "x線",
            "エックス線",
            "超音波",
            "エコー",
            "spect".lower(),
            "pet".lower(),
            "画像診断",
            "シンチ",
            "内視鏡",
            "ctr".lower(),
        ],
        "choice_terms": ["心電図", "脳波", "筋電図", "呼吸機能", "ct", "mri", "x線", "超音波", "エコー", "pet", "spect"],
        "min_score": 2,
    },
    {
        "name": "第４章 第１節 治療の意義と分類",
        "stem_terms": ["原因療法", "対症療法", "救命療法", "リハビリテーション", "保存療法", "生活指導", "治療法の分類"],
        "choice_terms": ["原因療法", "対症療法", "救命療法", "保存療法", "生活指導"],
        "min_score": 3,
    },
    {
        "name": "第４章 第２節 薬物療法",
        "stem_terms": [
            "薬物療法",
            "薬",
            "投与法",
            "副作用",
            "抗菌薬",
            "抗生物質",
            "抗ウイルス",
            "鎮痛薬",
            "解熱薬",
            "インスリン",
            "ワルファリン",
            "利尿薬",
            "筋弛緩薬",
            "抗パーキンソン",
            "抗自律神経薬",
            "ヒスタミン",
            "プロスタグランジン",
            "抗凝固",
            "血栓溶解薬",
        ],
        "choice_terms": ["薬", "剤", "抗菌薬", "抗生物質", "インスリン", "ワルファリン", "利尿薬"],
        "min_score": 3,
    },
    {
        "name": "第４章 第３節 食事療法",
        "stem_terms": ["食事療法", "食品交換表", "塩分制限", "カロリー制限", "半飢餓", "高蛋白", "高カロリー", "フェニルケトン尿症"],
        "choice_terms": ["食事療法", "塩分制限", "カロリー制限", "高蛋白", "高カロリー"],
        "min_score": 3,
    },
    {
        "name": "第４章 第４節 理学療法",
        "stem_terms": [
            "理学療法",
            "運動療法",
            "温熱療法",
            "水治療法",
            "温泉療法",
            "光線療法",
            "電気療法",
            "ホットパック",
            "パラフィン",
            "ハバード浴",
            "渦流浴",
            "赤外線",
            "紫外線",
            "低周波",
            "高周波",
            "ジアテルミー",
        ],
        "choice_terms": ["理学療法", "運動療法", "温熱療法", "水治療法", "電気療法", "ホットパック", "パラフィン", "低周波", "高周波"],
        "min_score": 2,
    },
    {
        "name": "第４章 第５節 作業療法",
        "stem_terms": ["作業療法", "adl訓練", "自助具", "職業前作業療法"],
        "choice_terms": ["作業療法", "adl訓練", "自助具"],
        "min_score": 3,
    },
    {
        "name": "第４章 第６節 言語聴覚療法",
        "stem_terms": ["言語聴覚療法", "失語症", "構音障害", "嚥下障害", "言語発達遅滞"],
        "choice_terms": ["言語聴覚療法", "失語症", "構音障害", "嚥下障害"],
        "min_score": 3,
    },
    {
        "name": "第４章 第７節 その他の療法",
        "stem_terms": [
            "手術",
            "透析",
            "腹膜透析",
            "血液透析",
            "輸血",
            "骨髄移植",
            "γ-グロブリン",
            "免疫療法",
            "心肺蘇生",
            "一次救命",
            "二次救命",
            "aed",
            "気道確保",
            "人工呼吸",
            "胸骨圧迫",
            "高圧酸素",
            "消毒",
            "滅菌",
            "洗浄",
            "ターミナルケア",
            "ネブライザー",
        ],
        "choice_terms": ["透析", "輸血", "骨髄移植", "心肺蘇生", "一次救命", "aed", "高圧酸素", "消毒", "滅菌", "洗浄"],
        "min_score": 2,
    },
    {
        "name": "第５章 第１節 患者さんの心理",
        "stem_terms": ["心身相関", "心身症", "神経症", "ストレス関連", "身体表現性", "ptsd", "不定愁訴", "パニック障害", "強迫性障害"],
        "choice_terms": ["心身相関", "心身症", "神経症", "ptsd", "不定愁訴"],
        "min_score": 3,
    },
    {
        "name": "第５章 第２節 心理学的検査",
        "stem_terms": ["知能検査", "性格検査", "ビネー", "ウェクスラー", "wisc", "wais", "yg", "mmpi", "ロールシャッハ", "cmi", "bs-pop"],
        "choice_terms": ["知能検査", "性格検査", "ビネー", "ウェクスラー", "ロールシャッハ", "cmi", "bs-pop"],
        "min_score": 3,
    },
    {
        "name": "第５章 第３節 心理療法",
        "stem_terms": ["心理療法", "カウンセリング", "精神分析", "認知療法", "行動療法", "認知行動療法", "自律訓練法", "シュルツ"],
        "choice_terms": ["カウンセリング", "精神分析", "認知療法", "行動療法", "認知行動療法", "自律訓練法"],
        "min_score": 3,
    },
]


def normalize(text: str) -> str:
    text = (text or "").replace("\n", " ").replace("\u3000", " ").lower()
    while "  " in text:
        text = text.replace("  ", " ")
    return text


def load_questions():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT
          q.id,
          q.serial,
          q.exam_type,
          q.exam_session,
          q.stem,
          q.choices_json,
          GROUP_CONCAT(st.name, ' / ') AS current_subtopics
        FROM questions q
        LEFT JOIN question_subtopics qs ON qs.question_id = q.id
        LEFT JOIN subtopics st ON st.id = qs.subtopic_id
        WHERE q.subject_id = (SELECT id FROM subjects WHERE name = ?)
        GROUP BY q.id
        ORDER BY q.serial
        """,
        (SUBJECT,),
    ).fetchall()
    conn.close()
    return rows


def score_section(stem: str, choices: str, section: dict):
    score = 0
    reasons = []
    stem_hits = 0
    for term in section["stem_terms"]:
        norm = normalize(term)
        if norm in stem:
            score += 3
            reasons.append(f"stem:{term}")
            stem_hits += 1
    for term in section.get("choice_terms", []):
        norm = normalize(term)
        if norm in choices and f"stem:{term}" not in reasons:
            score += 1
            reasons.append(f"choice:{term}")
    if section.get("require_stem") and stem_hits == 0:
        return 0, []
    return score, reasons


def classify_question(stem: str, choices_text: str):
    stem_norm = normalize(stem)
    choices_norm = normalize(choices_text)
    holdout_patterns = [
        "主訴とする疾患",
        "主訴としない疾患",
        "を主訴とする",
        "を主訴としない",
        "貧血の症状",
        "貧血を疑う症状",
        "低血糖の症状",
    ]
    if any(pattern in stem for pattern in holdout_patterns):
        return HOLDOUT, "hold", "manual_holdout:chief_complaint_disease_question"
    scored = []
    for section in SECTIONS:
        score, reasons = score_section(stem_norm, choices_norm, section)
        if score >= section["min_score"]:
            scored.append((section["name"], score, reasons))
    if not scored:
        return HOLDOUT, "hold", "no_direct_section_match"
    scored.sort(key=lambda item: (item[1], len(item[2])), reverse=True)
    best_name, best_score, best_reasons = scored[0]
    second_score = scored[1][1] if len(scored) > 1 else -1
    margin = best_score - second_score
    if best_score < 3:
        return HOLDOUT, "low", "weak_match"
    if margin <= 0:
        return HOLDOUT, "low", "score_tie"
    confidence = "high" if best_score >= 6 or margin >= 2 else "medium"
    return best_name, confidence, "; ".join(best_reasons[:6])


def main():
    rows = []
    for row in load_questions():
        try:
            choices = json.loads(row["choices_json"])
            if not isinstance(choices, list):
                choices = []
        except json.JSONDecodeError:
            choices = []
        choices_text = " / ".join(str(choice) for choice in choices)
        section, confidence, reason = classify_question(row["stem"], choices_text)
        rows.append(
            {
                "serial": row["serial"],
                "exam_type": row["exam_type"],
                "exam_session": row["exam_session"],
                "current_subtopics": row["current_subtopics"] or "",
                "textbook_section": section,
                "distribution_status": "配布可" if section != HOLDOUT else "保留",
                "confidence": confidence,
                "reason": reason,
                "stem": row["stem"],
                "choices": choices_text,
            }
        )

    with OUTPUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "serial",
                "exam_type",
                "exam_session",
                "current_subtopics",
                "textbook_section",
                "distribution_status",
                "confidence",
                "reason",
                "stem",
                "choices",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    holdouts = [row for row in rows if row["textbook_section"] == HOLDOUT]
    with HOLDOUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(holdouts)

    reviews = [row for row in rows if row["confidence"] != "high" and row["textbook_section"] != HOLDOUT]
    with REVIEW_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(reviews)

    section_counts = Counter(row["textbook_section"] for row in rows)
    status_counts = Counter(row["distribution_status"] for row in rows)
    confidence_counts = Counter(row["confidence"] for row in rows)

    lines = [
        "# 臨床医学総論 教科書章節対応分類サマリー",
        "",
        f"出力CSV: `{OUTPUT_CSV}`",
        f"保留CSV: `{HOLDOUT_CSV}`",
        f"要確認CSV: `{REVIEW_CSV}`",
        "",
        f"総問題数: {len(rows)}",
        "",
        "## 配布可 / 保留",
        "",
    ]
    for name, count in status_counts.items():
        lines.append(f"- {name}: {count}問")
    lines.extend(["", "## 節別件数", ""])
    for name, count in section_counts.most_common():
        lines.append(f"- {name}: {count}問")
    lines.extend(["", "## 判定信頼度", ""])
    for name, count in confidence_counts.items():
        lines.append(f"- {name}: {count}問")
    lines.extend(["", "## 保留の先頭50件", ""])
    for row in holdouts[:50]:
        lines.append(f"- {row['serial']}: {row['stem']}")

    SUMMARY_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
