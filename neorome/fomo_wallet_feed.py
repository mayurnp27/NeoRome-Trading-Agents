"""Bounded, read-only FOMO alert feed adapter.

This module collects provider discovery evidence only. It has no trading or
wallet-verification side effects. Credentials are read from FOMO_API_KEY and
are never included in persisted state or diagnostics.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import math
import os
import re
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, Request, build_opener
from urllib.parse import urlencode

WS_ENDPOINT = "wss://api.fomoapi.io/ws/alerts"
LEADERBOARD_ENDPOINT = "https://api.fomoapi.io/v2/leaderboard/7d"
MAX_RECONNECTS_PER_DAY = 24
MAX_CREDIT_RESERVATION = 250
MAX_ALERT_AGE_SECONDS = 120
MAX_RESPONSE_BYTES = 2 * 1024 * 1024
MAX_STATE_BYTES = 8 * 1024 * 1024
BASE58 = re.compile(r"^[1-9A-HJ-NP-Za-km-z]+$")
SAFE_ID = re.compile(r"^[A-Za-z0-9_.:@/-]{1,200}$")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _day(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).date().isoformat()


def _timestamp(value: Any) -> datetime | None:
    try:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            value = float(value)
            if abs(value) > 100_000_000_000:
                value /= 1000
            return datetime.fromtimestamp(value, timezone.utc)
        if isinstance(value, str) and value.strip():
            v = value.strip()
            try:
                return datetime.fromtimestamp(float(v) / (1000 if float(v) > 100_000_000_000 else 1), timezone.utc)
            except ValueError:
                return datetime.fromisoformat(v.replace("Z", "+00:00")).astimezone(timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None
    return None


def _valid_id(value: Any) -> bool:
    return isinstance(value, str) and bool(SAFE_ID.fullmatch(value))


def _valid_wallet(value: Any) -> bool:
    return isinstance(value, str) and 32 <= len(value) <= 44 and bool(BASE58.fullmatch(value))


def _json_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _atomic_json(path: Path, value: Any) -> None:
    raw = _json_bytes(value)
    if len(raw) > MAX_STATE_BYTES:
        raise ValueError("FOMO state exceeds size limit")
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(name, 0o600)
        os.replace(name, path)
        dfd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)
    finally:
        try:
            os.unlink(name)
        except FileNotFoundError:
            pass


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class FomoWalletFeed:
    """Durable state and bounded polling/streaming operations for FOMO."""

    def __init__(self, state_dir: str | Path, *, clock: Callable[[], datetime] = _utcnow,
                 opener: Any = None, websocket_module: Any = None):
        self.root = Path(state_dir)
        self.state_path = self.root / "fomo-state.json"
        self.alerts_path = self.root / "fomo-alerts.json"
        self.lock_path = self.root / "fomo-feed.lock"
        self.clock = clock
        self.opener = opener or build_opener(_NoRedirect())
        self.websocket_module = websocket_module

    def _lock(self):
        self.root.mkdir(parents=True, exist_ok=True)
        stream = self.lock_path.open("a+")
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            stream.close()
            raise RuntimeError("FOMO feed already running")
        return stream

    def _read(self, path: Path, default: dict[str, Any]) -> dict[str, Any]:
        try:
            if path.stat().st_size > MAX_STATE_BYTES:
                raise ValueError("FOMO state exceeds size limit")
            value = json.loads(path.read_text("utf-8"))
            if not isinstance(value, dict):
                raise ValueError("invalid FOMO state")
            return value
        except FileNotFoundError:
            return default

    def _state(self) -> dict[str, Any]:
        return self._read(self.state_path, {"version": 1, "mappings": {}, "candidates": {}, "event_ids": [],
            "budget_day": "", "reconnects": 0, "reserved_credits": 0, "leaderboard_day": "",
            "leaderboard_reserved": False, "leaderboard_limit": 0,
            "leaderboard_attempt": {}, "leaderboard_snapshot": None, "stop_day": ""})

    def refresh_leaderboard(self, api_key: str | None = None) -> dict[str, Any]:
        """Reserve and make at most one capped leaderboard GET per UTC day."""
        key = api_key if api_key is not None else os.environ.get("FOMO_API_KEY", "")
        if not key:
            return {"status": "missing_key", "requests": 0}
        now = self.clock()
        day = _day(now)
        state = self._state()
        attempt = state.get("leaderboard_attempt")
        already_attempted = (isinstance(attempt, dict) and attempt.get("day") == day
                             and attempt.get("limit") == 10)
        current_limit = state.get("leaderboard_limit", 0)
        if state.get("leaderboard_day") == day and current_limit >= 10:
            already_attempted = True
        if already_attempted:
            return {"status": "already_attempted_today", "requests": 0}
        # Durable reservation is written before the request, including on failures.
        if state.get("budget_day") != day:
            state.update({"budget_day": day, "reconnects": 0, "reserved_credits": 0, "stop_day": ""})
        # Each physical GET reserves the fixed 250-credit amount. An upgrade
        # from the legacy limit-8 attempt is a second request and reserves once
        # more; the versioned attempt marker below prevents retries on failure.
        state["reserved_credits"] = int(state.get("reserved_credits", 0)) + MAX_CREDIT_RESERVATION
        state["leaderboard_reserved_day"] = day
        state["leaderboard_day"] = day
        state["leaderboard_limit"] = 10
        state["leaderboard_attempt"] = {"day": day, "limit": 10, "version": "top10-v1"}
        state["leaderboard_reserved"] = True
        _atomic_json(self.state_path, state)
        url = f"{LEADERBOARD_ENDPOINT}?{urlencode({'limit': 10})}"
        request = Request(url, headers={"Authorization": f"Bearer {key}", "Accept": "application/json"})
        try:
            response = self.opener.open(request, timeout=15)
            with response:
                raw = response.read(MAX_RESPONSE_BYTES + 1)
            if len(raw) > MAX_RESPONSE_BYTES:
                state = self._state()
                state["leaderboard_status"] = "response_too_large"
                _atomic_json(self.state_path, state)
                return {"status": "response_too_large", "requests": 1}
            payload = json.loads(raw)
            candidates, mappings = self._parse_leaderboard(payload)
            state = self._state()
            state["candidates"] = candidates
            state["mappings"] = mappings
            state["leaderboard_status"] = "ok"
            frozen_at = _iso(self.clock())
            state["leaderboard_snapshot"] = {
                "version": "provider-frozen-top10-v1", "day": day,
                "source": LEADERBOARD_ENDPOINT, "limit": 10,
                "frozen_at": frozen_at,
                "response_sha256": hashlib.sha256(raw).hexdigest(),
                "entries": sorted(candidates.values(), key=lambda entry: entry["rank"]),
            }
            _atomic_json(self.state_path, state)
            return {"status": "ok", "requests": 1, "candidates": len(candidates),
                    "frozen_at": frozen_at}
        except Exception as exc:
            # Never surface URL, headers, response body or exception text (they may contain key material).
            state = self._state()
            state["leaderboard_status"] = "failed_" + type(exc).__name__
            _atomic_json(self.state_path, state)
            return {"status": "request_failed", "requests": 1}

    @staticmethod
    def _parse_leaderboard(payload: Any) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
        rows = payload
        if isinstance(payload, dict):
            data = payload.get("data", payload)
            rows = data.get("traders", data.get("leaderboard", data.get("users", data if isinstance(data, list) else []))) if isinstance(data, dict) else data
        if not isinstance(rows, list):
            return {}, {}
        candidates, mappings = {}, {}
        seen_ids, seen_wallets = set(), set()
        for rank, row in enumerate(rows[:10], start=1):
            if not isinstance(row, dict):
                continue
            user_id = row.get("userId", row.get("user_id", row.get("id")))
            wallets = row.get("wallets", {})
            wallet = row.get("wallet", row.get("address", row.get("publicKey")))
            if wallet is None and isinstance(wallets, dict):
                wallet = wallets.get("solana")
            pnl = row.get("pnlUsd", row.get("pnl", row.get("profitLoss", row.get("profit"))))
            trades = row.get("trades", row.get("tradeCount", row.get("totalTrades")))
            try:
                pnl_value, trade_value = float(pnl), float(trades)
                positive = math.isfinite(pnl_value) and math.isfinite(trade_value) and pnl_value > 0 and trade_value > 0
            except (TypeError, ValueError):
                positive = False
            if (not (_valid_id(user_id) and _valid_wallet(wallet) and positive)
                    or user_id in seen_ids or wallet in seen_wallets):
                continue
            seen_ids.add(user_id)
            seen_wallets.add(wallet)
            handle = row.get("handle", row.get("username", row.get("name")))
            candidate = {"user_id": user_id, "wallet": wallet, "handle": handle if isinstance(handle, str) else "",
                         "pnl": pnl_value, "trades": trade_value, "rank": rank,
                         "provider_claimed": True, "source": LEADERBOARD_ENDPOINT}
            candidates[user_id] = candidate
            mappings[user_id] = {"wallet": wallet, "handle": candidate["handle"],
                                 "provider_claimed": True, "rank": rank,
                                 "source": LEADERBOARD_ENDPOINT}
        return candidates, mappings

    def _alert_outcome(self, code: str, *, provider_at: str | None = None,
                       observed_at: str | None = None) -> None:
        """Bounded diagnostic counters only; never persist raw frames or identities."""
        state = self._state()
        today = _day(self.clock())
        diagnostics = state.get("alert_diagnostics")
        if not isinstance(diagnostics, dict) or diagnostics.get("day") != today:
            diagnostics = {"version": "alert-funnel-v1", "day": today, "counts": {}}
        previous_counts = diagnostics.get("counts")
        allowed = {"invalid_shape", "non_alert", "replay", "non_solana", "invalid_side_or_identity",
                   "invalid_mint", "invalid_or_stale_timestamp", "duplicate_or_invalid_event_cache",
                   "unmapped_user", "duplicate_queue", "accepted"}
        counts = {key: value for key, value in previous_counts.items() if key in allowed} if isinstance(previous_counts, dict) else {}
        if not isinstance(previous_counts, dict):
            counts = {}
        value = counts.get(code, 0)
        counts[code] = min((value if type(value) is int and value >= 0 else 0) + 1, 1000000)
        diagnostics["counts"] = counts
        diagnostics["updated_at"] = _iso(self.clock())
        if code == "accepted":
            diagnostics["last_accepted_provider_at"] = provider_at
            diagnostics["last_accepted_observed_at"] = observed_at
        state["alert_diagnostics"] = diagnostics
        _atomic_json(self.state_path, state)
        return None

    def accept_event(self, event: Any, *, observed_at: datetime | None = None) -> dict[str, Any] | None:
        """Validate and durably enqueue one fresh mapped Solana buy/sell alert."""
        if not isinstance(event, dict):
            return self._alert_outcome("invalid_shape")
        event_type = str(event.get("type", event.get("event", ""))).lower()
        if event_type != "alert":
            return self._alert_outcome("non_alert")
        if event.get("replay") is True:
            return self._alert_outcome("replay")
        chain = str(event.get("chain", event.get("network", ""))).lower()
        if chain not in {"solana", "sol"}:
            return self._alert_outcome("non_solana")
        event_id = event.get("eventId", event.get("event_id", event.get("id")))
        user_id = event.get("userId", event.get("user_id"))
        side = str(event.get("alertType", event.get("side", event.get("typeOfTrade", "")))).lower()
        if side not in {"buy", "sell"} or not _valid_id(event_id) or not _valid_id(user_id):
            return self._alert_outcome("invalid_side_or_identity")
        mint = event.get("mint", event.get("tokenAddress", event.get("token")))
        if not _valid_wallet(mint):
            return self._alert_outcome("invalid_mint")
        provider_dt = _timestamp(event.get("ts", event.get("timestamp", event.get("createdAt", event.get("time")))))
        received = observed_at or self.clock()
        if provider_dt is None or (received - provider_dt).total_seconds() > MAX_ALERT_AGE_SECONDS or provider_dt > received:
            return self._alert_outcome("invalid_or_stale_timestamp")
        state = self._state()
        ids = state.get("event_ids", [])
        if not isinstance(ids, list) or event_id in ids:
            return self._alert_outcome("duplicate_or_invalid_event_cache")
        mapping = state.get("mappings", {}).get(user_id)
        if not isinstance(mapping, dict) or not _valid_wallet(mapping.get("wallet")):
            return self._alert_outcome("unmapped_user")
        observed_at = received.astimezone(timezone.utc)
        signature = event.get("txHash", event.get("signature", event.get("transactionHash")))
        if not (isinstance(signature, str) and 64 <= len(signature) <= 88 and BASE58.fullmatch(signature)):
            signature = None
        record = {"event_id": event_id, "user_id": user_id, "wallet": mapping["wallet"], "mint": mint,
                  "side": side, "observed_at": _iso(observed_at), "provider_at": _iso(provider_dt),
                  "source_sha256": hashlib.sha256(_json_bytes(event)).hexdigest()}
        if signature:
            record["signature"] = signature
        alert_doc = self._read(self.alerts_path, {"version": 1, "alerts": []})
        queue = alert_doc.get("alerts", [])
        if not isinstance(queue, list):
            queue = []
        # The queue is written first so a crash between the two atomic files cannot
        # make a replay appear new and enqueue the same event twice.
        if any(isinstance(item, dict) and item.get("event_id") == event_id for item in queue):
            return self._alert_outcome("duplicate_queue")
        queue.append(record)
        alert_doc["alerts"] = queue[-50:]
        _atomic_json(self.alerts_path, alert_doc)
        ids.append(event_id)
        state["event_ids"] = ids[-100:]
        _atomic_json(self.state_path, state)
        self._alert_outcome("accepted", provider_at=record["provider_at"], observed_at=record["observed_at"])
        return record

    def _reserve_reconnect(self) -> bool:
        state, today = self._state(), _day(self.clock())
        if state.get("budget_day") != today:
            state.update({"budget_day": today, "reconnects": 0, "reserved_credits": 0, "stop_day": ""})
        if state.get("stop_day") == today or int(state.get("reconnects", 0)) >= MAX_RECONNECTS_PER_DAY:
            _atomic_json(self.state_path, state)
            return False
        state["reconnects"] = int(state.get("reconnects", 0)) + 1
        state["reserved_credits"] = int(state.get("reserved_credits", 0)) + MAX_CREDIT_RESERVATION
        _atomic_json(self.state_path, state)
        return True

    def _connection_metadata(self, *, status: str, welcome: dict[str, Any] | None = None,
                             message_count: int | None = None) -> None:
        state = self._state()
        connection = state.get("connection", {})
        if not isinstance(connection, dict):
            connection = {}
        connection["status"] = status
        connection["updated_at"] = _iso(self.clock())
        if message_count is not None:
            connection["message_count"] = message_count
            connection["last_message_at"] = _iso(self.clock())
        if welcome is not None:
            # Persist a small whitelist only; never store provider text or raw frames.
            connection["welcome"] = {"stream": "alerts", "chain": "solana"}
            for key in ("buffered", "heartbeatSeconds", "delaySeconds"):
                value = welcome.get(key)
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    connection["welcome"][key] = value
            if isinstance(welcome.get("realtime"), bool):
                connection["welcome"]["realtime"] = welcome["realtime"]
        state["connection"] = connection
        _atomic_json(self.state_path, state)

    def run(self, *, max_connections: int | None = None, sleep: Callable[[float], None] = time.sleep) -> None:
        """Run the durable-budgeted stream. Intended for a supervised service."""
        key = os.environ.get("FOMO_API_KEY", "")
        if not key:
            raise RuntimeError("FOMO_API_KEY is required")
        lock = self._lock()
        try:
            self.refresh_leaderboard(key)
            count, delay = 0, 1.0
            while max_connections is None or count < max_connections:
                # This is a durable daily reservation and becomes a no-op after the first attempt.
                self.refresh_leaderboard(key)
                if not self._reserve_reconnect():
                    if max_connections is not None:
                        break
                    sleep(60.0)
                    continue
                count += 1
                try:
                    wsmod = self.websocket_module
                    if wsmod is None:
                        import websocket as wsmod  # type: ignore[no-redef]
                    url = WS_ENDPOINT + "?" + urlencode({"key": key, "chain": "solana"})
                    ws = wsmod.create_connection(url, timeout=30)
                    self._connection_metadata(status="connected", message_count=0)
                    message_count = 0
                    try:
                        while True:
                            raw = ws.recv()
                            if raw is None or raw == "":
                                break
                            if isinstance(raw, bytes):
                                if len(raw) > MAX_RESPONSE_BYTES:
                                    continue
                            elif not isinstance(raw, str) or len(raw.encode("utf-8")) > MAX_RESPONSE_BYTES:
                                continue
                            try:
                                event = json.loads(raw)
                            except (ValueError, TypeError):
                                continue
                            message_count += 1
                            event_kind = event.get("type") if isinstance(event, dict) else None
                            if event_kind == "welcome":
                                self._connection_metadata(status="connected", welcome=event, message_count=message_count)
                            elif event_kind == "heartbeat":
                                self._connection_metadata(status="connected", message_count=message_count)
                                self.refresh_leaderboard(key)
                            else:
                                self._connection_metadata(status="connected", message_count=message_count)
                                self.accept_event(event)
                    finally:
                        close_code = getattr(ws, "close_status_code", None)
                        ws.close()
                    self._connection_metadata(status="disconnected", message_count=message_count)
                    if close_code == 1008:
                        state = self._state()
                        state["stop_day"] = _day(self.clock())
                        state["last_error"] = "provider_stop_1008"
                        _atomic_json(self.state_path, state)
                        sleep(60.0)
                        continue
                    sleep(delay)
                    delay = min(delay * 2, 60.0)
                except Exception as exc:
                    code = getattr(exc, "status_code", None)
                    if code is None:
                        code = getattr(exc, "status", None)
                    if code in (401, 402, 1008):
                        state = self._state()
                        state["stop_day"] = _day(self.clock())
                        state["last_error"] = f"provider_stop_{code}"
                        _atomic_json(self.state_path, state)
                        sleep(60.0)
                        continue
                    # Error type only; websocket exception strings often embed credentialed URLs.
                    state = self._state()
                    state["last_error"] = "stream_error_" + type(exc).__name__
                    _atomic_json(self.state_path, state)
                    sleep(delay)
                    delay = min(delay * 2, 60.0)
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)
            lock.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", required=True)
    args = parser.parse_args(argv)
    try:
        FomoWalletFeed(args.state_dir).run()
    except KeyboardInterrupt:
        return 0
    except Exception as exc:
        # Keep diagnostics credential-free and stable.
        print(f"fomo feed stopped: {type(exc).__name__}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
