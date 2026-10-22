from datetime import datetime, timedelta, timezone
import json
import tempfile
from pathlib import Path

from neorome.jupiter_quotes import SOL, USDC
from neorome.token_outcome_collector import run_once

UTC = timezone.utc
NOW = datetime(2026, 9, 26, 12, 0, tzinfo=UTC)


class Quotes:
    def quote(self, incoming, outgoing, amount):
        return {"requested_at": NOW.isoformat().replace("+00:00", "Z"), "received_at": NOW.isoformat().replace("+00:00", "Z"),
                "response_sha256": "a" * 64, "quote": {"inputMint": incoming, "outputMint": outgoing,
                "inAmount": str(amount), "outAmount": "100", "otherAmountThreshold": "90", "priceImpact": "0.1", "priceImpactUnit": "percentage_points"}}


def test_collector_uses_exact_horizon_quote_and_records_missed_windows():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory); (root / "candidates").mkdir()
        entry_at = NOW - timedelta(minutes=15)
        candidate = {"content_sha256": "b" * 64, "mint": SOL, "acquired_token_raw": "90",
                     "entry_quote": {"received_at": entry_at.isoformat().replace("+00:00", "Z")}}
        (root / "candidates" / "b.json").write_text(json.dumps(candidate))
        result = run_once(root, now=NOW, quote_client=Quotes())
        assert result == {"status": "ok", "requests": 1, "candidates_updated": 1, "orders_sent": 0}
        value = json.loads(next((root / "outcomes").glob("*.json")).read_text())
        assert value["horizons"]["15"]["status"] == "observed_route"
        later = run_once(root, now=NOW + timedelta(hours=5), quote_client=Quotes())
        assert later["requests"] == 0
        value = json.loads(next((root / "outcomes").glob("*.json")).read_text())
        assert value["horizons"]["60"]["status"] == "unresolved_missing_route_window"
        assert value["horizons"]["240"]["status"] == "unresolved_missing_route_window"
