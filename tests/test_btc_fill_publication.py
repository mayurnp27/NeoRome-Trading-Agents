"""Post-commit source visibility receipts do not change the modeled trade."""
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
import json
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
from neorome import btc_fast_stochastic_paper as fast, btc_stochastic_paper as slow
from tests.test_btc_fast_stochastic_paper import evidence as fast_input, OPEN
from tests.test_btc_stochastic_paper import make_input as slow_input


class FillPublicationTests(unittest.TestCase):
    def inputs(self):
        return [(fast, fast_input()),
                (slow, slow_input())]

    def test_receipt_is_after_committed_fill_and_preserves_quote_clock(self):
        for module, data in self.inputs():
            with self.subTest(module=module.__name__), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                wall = int(OPEN.timestamp() * 1e9) + 12_000_000_000
                calls = []
                publish = module._publish_fill
                def checked(root, fill):
                    with sqlite3.connect(next(root.glob('*.sqlite3'))) as connection:
                        self.assertEqual(connection.execute('select count(*) from fills where fill_id=?', (fill.fill_id,)).fetchone()[0], 1)
                    calls.append(fill)
                    return publish(root, fill)
                with patch.object(module, '_publication_clock', return_value=(wall, 987654321)), patch.object(module, '_publish_fill', side_effect=checked):
                    result = module.run_once(data, root, now=OPEN.replace(second=12))
                self.assertEqual(result['status'], 'filled', result)
                self.assertEqual(len(calls), 1)
                fill = calls[0]
                event = json.loads(module._event_path(root, 'fill-publication-' + fill.proposal_id).read_text())
                self.assertEqual(event['payload']['capture_wall_ns'], wall)
                self.assertEqual(event['payload']['capture_monotonic_ns'], 987654321)
                self.assertEqual(event['payload']['fill'], result['fill'])
                self.assertEqual(result['fill']['filled_at'], data['book2']['received_at'])
                self.assertNotEqual(event['at'], result['fill']['filled_at'])
                self.assertEqual(module.ALLOCATION_BPS, 2000)
                with patch.object(module, '_publication_clock', side_effect=AssertionError('retry must not freshen receipt')):
                    self.assertEqual(module._publish_fill(root, fill), event)
                modified = replace(fill, total_cost_cents=fill.total_cost_cents + 1)
                with self.assertRaises(module.BTCStochasticPaperError):
                    module._publish_fill(root, modified)

    def test_tampered_or_symlink_receipt_rejected(self):
        for module, data in self.inputs():
            with self.subTest(module=module.__name__), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                result = module.run_once(data, root, now=OPEN.replace(second=12))
                self.assertEqual(result['status'], 'filled', result)
                from neorome.paper_ledger import Fill
                fill = Fill(**result['fill'])
                path = module._event_path(root, 'fill-publication-' + fill.proposal_id)
                event = json.loads(path.read_text()); event['at'] = '2026-10-17T00:00:00Z'
                path.write_text(json.dumps(event))
                with self.assertRaises(module.BTCStochasticPaperError): module._publish_fill(root, fill)
                path.unlink(); target = root/'target'; target.write_text('{}'); path.symlink_to(target)
                with self.assertRaises(module.BTCStochasticPaperError): module._publish_fill(root, fill)

if __name__ == '__main__': unittest.main()
