import unittest

from scripts.run_gemini_practice_senryu import (
    build_requested_counts,
    parse_bundle_response,
    parse_features,
    parse_practice_items,
    validate_requested_sections,
)


class RunGeminiPracticeSenryuTests(unittest.TestCase):
    def test_parse_features_defaults_to_all(self):
        self.assertEqual(parse_features(""), ["mcq", "tf", "short", "senryu"])

    def test_build_requested_counts_uses_remaining_slots(self):
        requested = build_requested_counts(
            ["mcq", "tf", "short", "senryu"],
            {"mcq": 5, "tf": 2, "short": 0},
            1,
            force=False,
        )
        self.assertEqual(requested, {"tf": 3, "short": 5, "senryu": 2})

    def test_parse_bundle_response_extracts_all_sections(self):
        text = """
        {
          "practice_mcq": {
            "items": [
              {"focus":"鑑別","stem":"Aはどれか。","choices":["a","b","c","d"],"answer_index":1,"answer_text":"","explanation":"解説1"},
              {"focus":"鑑別","stem":"Bはどれか。","choices":["a","b","c","d"],"answer_index":2,"answer_text":"","explanation":"解説2"},
              {"focus":"鑑別","stem":"Cはどれか。","choices":["a","b","c","d"],"answer_index":3,"answer_text":"","explanation":"解説3"},
              {"focus":"鑑別","stem":"Dはどれか。","choices":["a","b","c","d"],"answer_index":4,"answer_text":"","explanation":"解説4"},
              {"focus":"鑑別","stem":"Eはどれか。","choices":["a","b","c","d"],"answer_index":1,"answer_text":"","explanation":"解説5"}
            ]
          },
          "practice_tf": {
            "items": [
              {"focus":"定義","stem":"Aである。","choices":[],"answer_index":null,"answer_text":"○","explanation":"解説A"},
              {"focus":"定義","stem":"Bである。","choices":[],"answer_index":null,"answer_text":"✕","explanation":"解説B"}
            ]
          },
          "practice_short": {
            "items": [
              {"focus":"用語","stem":"Aを答えなさい。","choices":[],"answer_index":null,"answer_text":"答えA","explanation":"解説SA"}
            ]
          },
          "senryu": {
            "items": [
              {"senryu":"春の風","commentary":"解説1です。十分な長さがあります。"},
              {"senryu":"夏の空","commentary":"解説2です。十分な長さがあります。"}
            ]
          }
        }
        """
        parsed = parse_bundle_response(text)
        self.assertEqual(len(parsed["mcq"]), 5)
        self.assertEqual(len(parsed["tf"]), 2)
        self.assertEqual(len(parsed["short"]), 1)
        self.assertEqual(len(parsed["senryu"]), 2)

    def test_validate_requested_sections_accepts_valid_bundle(self):
        parsed = {
            "mcq": [
                {"focus": "鑑別", "stem": "Aはどれか。十分な長さです。", "choices": ["a", "b", "c", "d"], "answer_index": 1, "answer_text": "", "explanation": "十分な長さの解説です。"},
                {"focus": "鑑別", "stem": "Bはどれか。十分な長さです。", "choices": ["a", "b", "c", "d"], "answer_index": 2, "answer_text": "", "explanation": "十分な長さの解説です。"},
                {"focus": "鑑別", "stem": "Cはどれか。十分な長さです。", "choices": ["a", "b", "c", "d"], "answer_index": 3, "answer_text": "", "explanation": "十分な長さの解説です。"},
                {"focus": "鑑別", "stem": "Dはどれか。十分な長さです。", "choices": ["a", "b", "c", "d"], "answer_index": 4, "answer_text": "", "explanation": "十分な長さの解説です。"},
                {"focus": "鑑別", "stem": "Eはどれか。十分な長さです。", "choices": ["a", "b", "c", "d"], "answer_index": 1, "answer_text": "", "explanation": "十分な長さの解説です。"},
            ],
            "senryu": [
                {"senryu": "春の風覚えるツボよよく巡る", "commentary": "十分な長さの解説です。学習ポイントも書かれています。"},
                {"senryu": "夏の空脈の流れを忘れずに", "commentary": "十分な長さの解説です。学習ポイントも書かれています。"},
                {"senryu": "秋の雲陰陽そっと見分けよう", "commentary": "十分な長さの解説です。学習ポイントも書かれています。"},
            ],
        }
        self.assertEqual(validate_requested_sections(parsed, {"mcq": 5, "senryu": 3}), "")

    def test_parse_practice_items_converts_zero_based_mcq_answer_index(self):
        items = parse_practice_items(
            [
                {
                    "focus": "代謝",
                    "stem": "肝臓について正しいのはどれか。",
                    "choices": ["a", "b", "c", "d"],
                    "answer_index": 0,
                    "answer_text": "",
                    "explanation": "解説です。",
                }
            ],
            "mcq",
        )
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["answer_index"], 1)


if __name__ == "__main__":
    unittest.main()
