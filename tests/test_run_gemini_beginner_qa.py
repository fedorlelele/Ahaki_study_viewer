import unittest

from scripts.run_gemini_beginner_qa import (
    extract_retry_delay_seconds,
    is_daily_quota_exhausted,
    parse_beginner_qa_response,
    validate_beginner_qa_items,
)


class RunGeminiBeginnerQaTests(unittest.TestCase):
    def test_parse_beginner_qa_response_keeps_five_items(self):
        text = """
        {
          "items": [
            {"order": 2, "focus": "ひっかけ", "question": "なぜ2番ではないの？", "answer": "2番は一見よさそうですが、問題文の条件と合いません。だから正解にはなりません。"},
            {"order": 1, "focus": "決め手", "question": "どこを見れば正解が分かるの？", "answer": "いちばん大事なのは症状と所見の組み合わせです。その一致を見ると正解を絞れます。"},
            {"order": 3, "focus": "用語", "question": "この言葉はどういう意味？", "answer": "試験でよく出る基本用語です。まずは短い意味を押さえ、そのあと例と結びつけると理解しやすいです。"},
            {"order": 4, "focus": "読み取り", "question": "症例文のどこが大事？", "answer": "年齢、主訴、経過の3つがヒントになります。そこを先に拾うと迷いにくくなります。"},
            {"order": 5, "focus": "覚え方", "question": "どう覚えればいい？", "answer": "似た選択肢とセットで比べると覚えやすいです。違いを一言で言えるようにすると定着します。"}
          ]
        }
        """
        items = parse_beginner_qa_response(text)
        self.assertEqual(len(items), 5)
        self.assertEqual(items[0]["order"], 1)
        self.assertEqual(items[0]["focus"], "決め手")

    def test_validate_beginner_qa_items_rejects_wrong_count(self):
        items = [
            {"order": 1, "focus": "決め手", "question": "どこを見るの？", "answer": "ここを見ると分かります。"},
        ]
        self.assertIn("exactly 5", validate_beginner_qa_items(items))

    def test_extract_retry_delay_seconds_from_quota_error(self):
        text = """
        HTTP 429: {
          "error": {
            "message": "Please retry in 22h22m15.181069156s.",
            "details": [
              {
                "@type": "type.googleapis.com/google.rpc.RetryInfo",
                "retryDelay": "80535s"
              }
            ]
          }
        }
        """
        self.assertEqual(extract_retry_delay_seconds(text), 80535.0)

    def test_is_daily_quota_exhausted_detects_per_day_metric(self):
        text = """
        Quota exceeded for metric: generativelanguage.googleapis.com/generate_requests_per_model_per_day
        """
        self.assertTrue(is_daily_quota_exhausted(text))


if __name__ == "__main__":
    unittest.main()
