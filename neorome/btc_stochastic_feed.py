"""Low-memory public-data driver for an isolated BTC stochastic paper strategy.

Only public Kalshi GETs and an actual restricted TradingView stdio MCP call exist.
There is no credential loader or venue-order interface.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
import fcntl
import hashlib
import json
from pathlib import Path
import re
import time
import urllib.error
import urllib.parse
import urllib.request

from . import btc_stochastic_paper as paper
from . import tradingview_mcp_signal as tv_signal

UTC = timezone.utc
SERIES = "KXBTC15M"
KALSHI = "https://external-api.kalshi.com/trade-api/v2"
ROOT = paper.DEFAULT_STATE_ROOT
RECOVERY = Path("/var/lib/neorome-paper-recovery/status.json")
GLOBAL_STOP = Path("/var/lib/neorome-paper/STOP")


class FeedError(ValueError):
    pass


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise FeedError("public_data_redirect_refused")


def iso(stamp=None):
    return (stamp or datetime.now(UTC)).astimezone(UTC).isoformat()


def stamp(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def atomic(path, value):
    if path.is_symlink():
        raise FeedError("state_symlink_refused")
    temporary = path.with_name(path.name + ".tmp")
    if temporary.is_symlink():
        raise FeedError("state_symlink_refused")
    temporary.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")
    temporary.chmod(0o600)
    temporary.replace(path)


def archive_cycle(root, result):
    """Preserve the decision denominator, including zero-call abstentions."""
    directory = root / "cycles"
    if directory.is_symlink():
        raise FeedError("cycle_archive_symlink_refused")
    directory.mkdir(mode=0o700, exist_ok=True)
    body = json.dumps(result, sort_keys=True, separators=(",", ":"), allow_nan=False).encode() + b"\n"
    target = directory / (stamp(result["as_of"]).strftime("%Y%m%dT%H%M%S%fZ") + "-" +
                          hashlib.sha256(body).hexdigest()[:16] + ".json")
    with target.open("xb") as stream:
        stream.write(body)
    target.chmod(0o600)


class PublicData:
    def __init__(self, root):
        self.root = root
        self.opener = urllib.request.build_opener(NoRedirect)
        self.calls = 0

    def capture(self, kind, ticker=None):
        if kind == "chart":
            url, body = None, None
        elif kind == "series":
            url, body = KALSHI + "/series/" + SERIES, None
        elif kind == "markets":
            url, body = KALSHI + "/markets?series_ticker=KXBTC15M&status=open&limit=10&exchange_index=2", None
        elif kind in {"market", "book"}:
            if not isinstance(ticker, str) or not re.fullmatch(r"KXBTC15M-[A-Z0-9-]{1,60}", ticker):
                raise FeedError("venue_native_ticker_required")
            suffix = "/orderbook?depth=100&exchange_index=2" if kind == "book" else "?exchange_index=2"
            url, body = KALSHI + "/markets/" + urllib.parse.quote(ticker, safe="") + suffix, None
        else:
            raise FeedError("public_endpoint_not_allowed")
        if self.calls >= 8:
            raise FeedError("per_cycle_request_budget_exhausted")
        counter = self.root / "public-data-budget.json"
        day = datetime.now(UTC).date().isoformat()
        count = json.loads(counter.read_text()) if counter.exists() else {"date": day, "reserved_requests": 0}
        if count.get("date") != day:
            count = {"date": day, "reserved_requests": 0}
        used = count.get("reserved_requests")
        if type(used) is not int or not 0 <= used < 1000:
            raise FeedError("daily_public_request_budget_exhausted_or_invalid")
        atomic(counter, {"date": day, "reserved_requests": used + 1})
        self.calls += 1
        if kind == "chart":
            try:
                value = tv_signal.capture()
            except (tv_signal.TradingViewMCPError, OSError, ValueError) as exc:
                raise FeedError("tradingview_mcp_capture_failed: " + str(exc)[:120]) from exc
            metadata = value["data"]["source_metadata"]
            record = {"kind": kind, "ticker": ticker, "started_at": metadata["request_started_at"],
                      "received_at": iso(), "rest_date": metadata["http_date"],
                      "cache_age": metadata["cache_age"], "http_status": 200,
                      "data": value["data"], "provider": "tradingview_mcp",
                      "mcp_server": value["mcp_server"], "tool": value["tool"],
                      "mcp_protocol_version": value["mcp_protocol_version"],
                      "mcp_transcript_sha256": value["mcp_transcript_sha256"]}
        else:
            request = urllib.request.Request(url, data=body, headers={"User-Agent": "NeoRome-Paper-Research/1.0",
                                             "Accept": "application/json", "Content-Type": "application/json",
                                             "Cache-Control": "no-cache"}, method="GET")
            started = iso()
            with self.opener.open(request, timeout=10) as response:
                raw = response.read(2_000_001)
                if len(raw) > 2_000_000 or response.status != 200:
                    raise FeedError("public_response_invalid")
                date = response.headers.get("Date")
                if not date:
                    raise FeedError("public_response_date_missing")
                record = {"kind": kind, "ticker": ticker, "started_at": started, "received_at": iso(),
                          "rest_date": iso(parsedate_to_datetime(date)), "cache_age": response.headers.get("Age"),
                          "http_status": response.status, "data": json.loads(raw)}
        age = (stamp(record["received_at"]) - stamp(record["rest_date"])).total_seconds()
        if age < -2 or age > 15 or (record["cache_age"] is not None and float(record["cache_age"]) > 15):
            raise FeedError("public_response_stale_or_future")
        record["source_hash"] = digest(record)
        archive = self.root / "captures"
        archive.mkdir(mode=0o700, exist_ok=True)
        target = archive / (datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ") + "-" + kind + "-" + record["source_hash"][:12] + ".json")
        if target.exists():
            raise FeedError("immutable_capture_collision")
        target.write_text(json.dumps(record, allow_nan=False) + "\n")
        target.chmod(0o600)
        return record


def book(record):
    return {k: record[k] for k in ["rest_date", "received_at", "source_hash"]} | {
        "orderbook_fp": record["data"].get("orderbook_fp"), "ticker": record.get("ticker")}


def entry_health(now):
    if GLOBAL_STOP.exists():
        return "global_manual_stop"
    try:
        value = json.loads(RECOVERY.read_text())
        age = (now - stamp(value["updated_at"])).total_seconds()
        if value.get("operational_healthy") is not True or not 0 <= age <= 7200:
            return "recovery_not_fresh_and_verified"
    except (OSError, KeyError, ValueError, TypeError):
        return "recovery_status_missing_or_invalid"
    return None


def cycle(root=ROOT):
    root = Path(root)
    if root.is_symlink():
        raise FeedError("state_root_symlink_refused")
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    with (root / "feed.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {"status": "busy", "venue_orders_sent": 0}
        paper.initialize(root)
        before = paper.snapshot(root)
        client = PublicData(root)
        now = datetime.now(UTC)
        result = {"profile": paper.STRATEGY_ID, "as_of": iso(now), "mode": "internal-paper",
                  "venue_orders_sent": 0, "live_execution_enabled": False}
        active = before.get("active_position") or before.get("pending_reservation")
        try:
            pending = before.get("pending_reservation")
            blocker = entry_health(now) if pending else None
            expired_pending = pending and now > stamp(pending["expiry_at"]) - timedelta(
                seconds=900 - paper.MAX_SIGNAL_AGE_SECONDS)
            if pending and (expired_pending or blocker or before.get("manual_stop_active")):
                # Cash release must not depend on an available external feed.
                result["action"] = paper.run_once({"entry_blocker_reason": blocker}, root, now)
            elif active:
                ticker = active["instrument"]
                meta = client.capture("market", ticker)
                market = meta["data"]["market"]
                if market.get("status") in {"settled", "finalized"} and market.get("result") in {"yes", "no"}:
                    evidence = {"settlement": {"ticker": ticker, "status": market["status"], "result": market["result"],
                                                "observed_at": meta["received_at"], "source_hash": meta["source_hash"]}}
                else:
                    captured = client.capture("book", ticker)
                    evidence = {"book2": book(captured), "market": {"ticker": ticker},
                                "entry_blocker_reason": entry_health(datetime.now(UTC))} if before.get("pending_reservation") else {"mark_book": book(captured)}
                result["action"] = paper.run_once(evidence, root, datetime.now(UTC))
            elif now.timestamp() % 900 > 90 and (root / tv_signal.STATE_NAME).exists():
                result.update(status="abstain", reason="waiting_next_quarter_boundary")
            elif before.get("manual_stop_active"):
                result.update(status="abstain", reason="manual_paper_stop")
            elif entry_health(now):
                result.update(status="abstain", reason=entry_health(now))
            else:
                chart = client.capture("chart")
                evaluated = tv_signal.evaluate(chart, root, datetime.now(UTC))
                result.update(evaluated)
                selected = evaluated["selected_signal"]
                if not selected:
                    result.update(status="abstain")
                else:
                    result.pop("reason", None)
                    source_close = selected["bar_close_timestamp"]
                    series = client.capture("series")
                    series_data = series["data"].get("series", {})
                    if series_data.get("ticker") != SERIES or series_data.get("exchange_index") != 2:
                        raise FeedError("series_or_matching_engine_changed")
                    markets = client.capture("markets")
                    eligible = [m for m in markets["data"].get("markets", [])
                                if int(stamp(m["open_time"]).timestamp()) == source_close]
                    if len(eligible) != 1:
                        raise FeedError("exact_current_market_not_discovered")
                    market = eligible[0] | {"source_hash": markets["source_hash"]}
                    first = client.capture("book", market["ticker"])
                    time.sleep(2.05)
                    second = client.capture("book", market["ticker"])
                    signal = selected
                    evidence = {"signal": signal, "market": market, "fee": {"fee_type": series_data.get("fee_type"),
                                "fee_multiplier": series_data.get("fee_multiplier"), "source_hash": series["source_hash"]},
                                "book1": book(first), "book2": book(second), "mark_book": book(second)}
                    health = entry_health(datetime.now(UTC))
                    if health:
                        result.update(status="abstain", reason=health)
                    else:
                        result["action"] = paper.run_once(evidence, root, datetime.now(UTC))
            result.setdefault("status", result.get("action", {}).get("status", "checked"))
        except (FeedError, tv_signal.TradingViewMCPError, paper.BTCStochasticPaperError, urllib.error.URLError, OSError, KeyError, ValueError, TypeError) as exc:
            result.update(status="abstain", reason="data_or_paper_admission_failed", error_type=type(exc).__name__)
            if isinstance(exc, (FeedError, tv_signal.TradingViewMCPError, paper.BTCStochasticPaperError)):
                result["admission_detail"] = str(exc)[:200]
        result["provider_requests_this_cycle"] = client.calls
        result["snapshot"] = paper.snapshot(root)
        audit = result["snapshot"].pop("audit_events", [])
        result["snapshot"]["audit_events_count"] = len(audit)
        result["snapshot"]["audit_events_hash"] = digest(audit)
        result["finished_at"] = iso()
        archive_cycle(root, result)
        atomic(root / "feed-status.json", result)
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--initialize", action="store_true")
    args = parser.parse_args()
    value = paper.snapshot(ROOT) if args.initialize else cycle()
    print(json.dumps(value, allow_nan=False))


if __name__ == "__main__":
    main()
