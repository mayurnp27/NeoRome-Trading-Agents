import json
from pathlib import Path
import tempfile
import unittest
from neorome.paper_rehearsal import run, DEFAULT_FIXTURE

class RehearsalTests(unittest.TestCase):
    def test_four_desks_losses_rejections_and_restart(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'run'
            result = run(path)
            self.assertEqual(result['initial']['paper_nav_cents'], 80000)
            self.assertEqual(result['final']['paper_nav_cents'], 79290)
            self.assertEqual(result['final']['realized_pnl_cents'], -710)
            self.assertEqual(sum(d['decision']['accepted'] for d in result['decisions']), 4)
            self.assertTrue(all(result['checks'].values()))
            balances = {b['desk']: b['available_cash_cents'] for b in result['desk_balances']}
            self.assertEqual(balances, {'equities':30048,'prediction':9590,'options':29700,'crypto':9952})
            self.assertEqual(result['broker_requests'], 0)
            with self.assertRaises(FileExistsError):
                run(path)

    def test_observed_inputs_cannot_enter_fixture_runner(self):
        with tempfile.TemporaryDirectory() as tmp:
            data = json.loads(DEFAULT_FIXTURE.read_text())
            data['source_kind'] = 'observed'
            fixture = Path(tmp) / 'not-synthetic.json'
            fixture.write_text(json.dumps(data))
            with self.assertRaises(ValueError):
                run(Path(tmp) / 'run', fixture)

if __name__ == '__main__':
    unittest.main()
