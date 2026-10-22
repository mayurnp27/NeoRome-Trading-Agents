from datetime import datetime, timedelta, timezone
import unittest

from neorome.token_social import eligible, normalize

UTC = timezone.utc
NOW = datetime(2026, 9, 23, 12, tzinfo=UTC)
MINT = "11111111111111111111111111111111"


class TokenSocialTests(unittest.TestCase):
    def test_contract_evidence_drives_auditable_signals(self):
        users = []
        posts = []
        for index in range(3):
            users.append({"id": str(index), "username": "u" + str(index),
                          "created_at": (NOW - timedelta(days=365)).isoformat(),
                          "public_metrics": {"followers_count": 1000}})
            posts.append({"id": "p" + str(index), "author_id": str(index),
                          "created_at": (NOW - timedelta(minutes=30-index*10)).isoformat(),
                          "text": MINT + " community meme", "public_metrics": {"like_count": 5}})
        evidence = normalize({"data": posts, "includes": {"users": users}}, MINT,
                             NOW - timedelta(minutes=1), NOW, "a" * 64)
        self.assertTrue(eligible(evidence)[0])
        self.assertEqual(evidence["signals"]["unique_credible_authors"], 3)
        self.assertEqual(evidence["signals"]["narrative_clusters"]["meme"], 3)
        self.assertIsNone(evidence["signals"]["known_influencer_correlation"])

    def test_rug_warning_and_bot_concentration_fail_closed(self):
        payload = {"data": [{"id": "p", "author_id": "u", "created_at": NOW.isoformat(),
                             "text": MINT + " cannot sell rug", "public_metrics": {}}],
                   "includes": {"users": [{"id": "u", "username": "new",
                     "created_at": (NOW-timedelta(days=1)).isoformat(),
                     "public_metrics": {"followers_count": 1}}]}}
        evidence = normalize(payload, MINT, NOW, NOW, "b" * 64)
        ok, reasons = eligible(evidence)
        self.assertFalse(ok)
        self.assertIn("x_rug_warning_present", reasons)
        self.assertIn("x_bot_concentration_too_high_or_missing", reasons)


if __name__ == "__main__": unittest.main()
