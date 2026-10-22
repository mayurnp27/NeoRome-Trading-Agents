import tempfile
import unittest
from pathlib import Path
from neorome.paper_portfolio import inspect
from neorome.paper_ledger import PaperLedger, ValidationError


class PortfolioTests(unittest.TestCase):
    def test_restart_preserves_funded_risk_enforced_book(self):
        with tempfile.TemporaryDirectory() as state:
            first = inspect(state)
            second = inspect(state)
            self.assertEqual(first['snapshot']['available_cash_cents'], 80000)
            self.assertEqual(second['snapshot']['available_cash_cents'], 80000)
            self.assertTrue(second['market_risk_enforced'])
            self.assertFalse(second['broker_execution_enabled'])
            self.assertEqual(len(PaperLedger(Path(state)/'paper.sqlite3', enforce_market_risk=True).audit()), 0)

    def test_refuses_existing_unprotected_book(self):
        with tempfile.TemporaryDirectory() as state:
            PaperLedger(Path(state)/'paper.sqlite3', enforce_market_risk=False)
            with self.assertRaises(ValidationError):
                inspect(state)
