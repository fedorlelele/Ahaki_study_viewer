from pathlib import Path

from scripts.import_exam_txt_to_sqlite import parse_exam_file


def write_exam_file(tmp_path: Path, name: str, body: str) -> Path:
    path = tmp_path / name
    path.write_text(body, encoding="utf-8")
    return path


def test_parse_patient_context_without_shourei_keyword(tmp_path):
    path = write_exam_file(
        tmp_path,
        "A23.txt",
        """第23回あん摩マッサージ指圧師試験
《東洋医学概論》
次の文で示す患者について、問題100、問題101の問いに答えよ。
「55歳の女性。１か月前から大腿部・下腿部の後側にだるい痛みがある。」

問題100　本患者の八綱病証で適切なのはどれか。
 1. 表証
 2. 熱証
 3. 虚証
 4. 陽証
解答　３．

問題101　本症例の経脈病証はどれか。
 1. 膀胱経
 2. 胆経
 3. 腎経
 4. 肝経
解答　１．
""",
    )

    questions = {question["serial"]: question for question in parse_exam_file(path)}

    assert questions["A23-100"]["case_text"] == (
        "次の文で示す患者について、A23-100、A23-101の問いに答えよ。\n"
        "「55歳の女性。１か月前から大腿部・下腿部の後側にだるい痛みがある。」"
    )
    assert questions["A23-101"]["case_text"] == questions["A23-100"]["case_text"]


def test_parse_shared_context_without_patient_or_shourei_keyword(tmp_path):
    path = write_exam_file(
        tmp_path,
        "A14.txt",
        """第14回あん摩マッサージ指圧師試験
《東洋医学臨床論》
末梢性顔面神経麻痺について、次の問題136、問題137の問に答えよ。

問題136　罹患神経への施術部位はどれか。
 1. 前額部
 2. 頬部
 3. 下顎部
 4. 後頚部
解答　２．

問題137　適切な対応はどれか。
 1. 温罨法
 2. 冷罨法
 3. 固定
 4. 安静
解答　１．
""",
    )

    questions = {question["serial"]: question for question in parse_exam_file(path)}

    assert questions["A14-136"]["case_text"] == (
        "末梢性顔面神経麻痺について、次のA14-136、A14-137の問に答えよ。"
    )
    assert questions["A14-137"]["case_text"] == questions["A14-136"]["case_text"]


def test_parse_combo_choices_inserts_missing_separator(tmp_path):
    path = write_exam_file(
        tmp_path,
        "B34.txt",
        """第34回はり師・きゆう師試験
《臨床医学各論》
問題63　スポーツ外傷・障害と罹患部位の組合せで正しいのはどれか。
 1. オスグッド病 大腿骨外側顆
 2. ジャンパー膝 脛骨遠位端
 3. マレット指 中節骨近位端
 4. 野球肘（外側型） 上腕骨小頭
解答　４．

問題64　疾患と分類の組合せで正しいのはどれか。
 1. COVID-19 … 感染症
 2. A - B
 3. C－D
 4. E F G
解答　１．
""",
    )

    questions = {question["serial"]: question for question in parse_exam_file(path)}

    assert questions["B34-063"]["choices"] == [
        "オスグッド病 … 大腿骨外側顆",
        "ジャンパー膝 … 脛骨遠位端",
        "マレット指 … 中節骨近位端",
        "野球肘（外側型） … 上腕骨小頭",
    ]
    assert "1. オスグッド病 … 大腿骨外側顆" in questions["B34-063"]["raw_text"]
    assert questions["B34-064"]["choices"] == [
        "COVID-19 … 感染症",
        "A - B",
        "C－D",
        "E F G",
    ]
