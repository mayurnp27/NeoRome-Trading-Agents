"""Budgeted, shared, read-only cache for X token evidence.

This module is deliberately a thin wrapper around ``token_social.XClient``. It
does not own credentials, contact X except through that client, or synthesize
evidence when a request is skipped or fails.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import time
from typing import Callable

from .token_social import SocialError, XClient
from .jupiter_quotes import validate_mint


DEFAULT_CACHE_DIR = "/var/lib/neorome-memecoin-program/x-social"
CACHE_TTL = timedelta(seconds=60)
COOLDOWN = timedelta(minutes=15)
LOCK_WAIT_SECONDS = 25.0
LOCK_POLL_SECONDS = 0.05
USER_COST_CENTS = 1
MAX_RESULTS = 20


def _utc_now(clock: Callable[[], datetime]) -> datetime:
    value = clock()
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise SocialError("X cache clock invalid")
    return value.astimezone(timezone.utc)


def _iso(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError
    return parsed.astimezone(timezone.utc)


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _normalized_hash(evidence: dict) -> str:
    body = dict(evidence)
    body.pop("normalized_sha256", None)
    body.pop("x_collection", None)
    return hashlib.sha256(_canonical(body)).hexdigest()


def _safe_component(path: Path) -> None:
    """Reject symlinks in the cache path, including pre-existing final files."""
    absolute = path.absolute()
    current = Path(absolute.anchor)
    for part in absolute.parts[1:]:
        current = current / part
        try:
            current.lstat()
        except FileNotFoundError:
            continue
        if current.is_symlink():
            raise SocialError("X cache path rejected")
        if current != absolute and not current.is_dir():
            raise SocialError("X cache path invalid")


def _ensure_cache_dir(path: Path) -> None:
    _safe_component(path)
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    _safe_component(path)
    if not path.is_dir():
        raise SocialError("X cache directory unavailable")


@contextmanager
def _exclusive_lock(path: Path, wait_seconds: float = LOCK_WAIT_SECONDS):
    _safe_component(path.parent)
    flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags, 0o600)
    except OSError:
        raise SocialError("X cache lock unavailable") from None
    try:
        if not os.path.isfile(path) or os.path.islink(path):
            raise SocialError("X cache lock rejected")
        deadline = time.monotonic() + wait_seconds
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise SocialError("X cache busy")
                time.sleep(LOCK_POLL_SECONDS)
        yield
    finally:
        try:
            os.close(fd)
        except OSError:
            pass


def _validate_evidence(value: object, mint: str, now: datetime, *, require_fresh: bool) -> dict:
    if not isinstance(value, dict):
        raise ValueError
    if (type(value.get("schema_version")) is not int or value.get("schema_version") != 1 or value.get("provider") != "x"
            or value.get("source_kind") != "observed" or value.get("query_kind") != "contract_address"
            or value.get("mint") != mint or value.get("complete") is not True
            or not isinstance(value.get("signals"), dict) or not isinstance(value.get("posts"), list)
            or len(value.get("posts", [])) > MAX_RESULTS):
        raise ValueError
    requested = _iso(value.get("requested_at"))
    received = _iso(value.get("received_at"))
    if requested > now or received > now or received < requested:
        raise ValueError
    raw_hash = value.get("response_sha256")
    if not isinstance(raw_hash, str) or not re.fullmatch(r"[0-9a-f]{64}", raw_hash):
        raise ValueError
    for post in value["posts"]:
        if (not isinstance(post, dict) or not isinstance(post.get("id"), str)
                or not isinstance(post.get("text"), str) or mint not in post["text"]):
            raise ValueError
        created = _iso(post.get("created_at"))
        author_created = _iso(post.get("author_created_at"))
        if created > now or author_created > now:
            raise ValueError
    expected = value.get("normalized_sha256")
    if not isinstance(expected, str) or not re.fullmatch(r"[0-9a-f]{64}", expected) or _normalized_hash(value) != expected:
        raise ValueError
    if require_fresh and (now - received > CACHE_TTL or received - now > timedelta(0)):
        raise ValueError
    return value


def _stamp_evidence(value: dict) -> dict:
    result = json.loads(json.dumps(value))
    result.pop("x_collection", None)
    result.pop("normalized_sha256", None)
    result["normalized_sha256"] = _normalized_hash(result)
    return result


class BudgetedXClient:
    """A shared cache and daily cost gate around the existing XClient."""

    def __init__(self, cache_dir: str | os.PathLike = DEFAULT_CACHE_DIR,
                 daily_budget_cents: int = 0, exception_budget_cents: int = 0,
                 client=None, clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc)):
        if type(daily_budget_cents) is not int or not 0 <= daily_budget_cents <= 100_000:
            raise ValueError("daily_budget_cents must be a nonnegative integer")
        if type(exception_budget_cents) is not int or not 0 <= exception_budget_cents <= 300:
            raise ValueError("exception_budget_cents must be an integer from 0 through 300")
        self.cache_dir = Path(cache_dir)
        self.daily_budget_cents = daily_budget_cents
        self.exception_budget_cents = exception_budget_cents
        self.client = client if client is not None else XClient()
        self.clock = clock
        self.last_request_metadata = {
            "api_requests": 0, "cache_hit": False, "estimated_cost_cents": 0,
            "reserved_cost_cents": 0, "budget_status": "not_started",
        }

    def _db(self):
        db_path = self.cache_dir / "evidence.sqlite3"
        _safe_component(db_path)
        try:
            conn = sqlite3.connect(db_path, timeout=5, isolation_level=None)
            os.chmod(db_path, 0o600)
        except (OSError, sqlite3.Error):
            raise SocialError("X cache database unavailable") from None
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("CREATE TABLE IF NOT EXISTS evidence (mint TEXT PRIMARY KEY, body TEXT NOT NULL)")
        conn.execute("CREATE TABLE IF NOT EXISTS daily_spend (day TEXT PRIMARY KEY, normal_cents INTEGER NOT NULL, exception_cents INTEGER NOT NULL)")
        conn.execute("CREATE TABLE IF NOT EXISTS control (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        conn.execute("CREATE TABLE IF NOT EXISTS request_audit (id INTEGER PRIMARY KEY AUTOINCREMENT, at TEXT NOT NULL, mint TEXT NOT NULL, max_results INTEGER NOT NULL, normal_reserved INTEGER NOT NULL, exception_reserved INTEGER NOT NULL, exception_reason TEXT, status TEXT NOT NULL, http_status INTEGER)")
        return conn

    def _metadata(self, **fields):
        self.last_request_metadata = {
            "api_requests": 0, "cache_hit": False, "estimated_cost_cents": 0,
            "reserved_cost_cents": 0, "normal_reserved_cost_cents": 0,
            "exception_reserved_cost_cents": 0, "cache_status": "not_checked",
            "budget_status": "not_started", **fields,
        }

    def search_contract(self, mint: str, max_results: int = 20, exception_reason: str | None = None) -> dict:
        validate_mint(mint)
        if type(max_results) is not int or not 10 <= max_results <= MAX_RESULTS:
            raise SocialError("X result bound must be 10 through 20")
        reason = exception_reason.strip() if isinstance(exception_reason, str) else ""
        if len(reason) > 160 or (reason and (len(reason) < 10 or
                not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9 .,_:/-]{8,159}", reason))):
            raise SocialError("X budget exception reason invalid")
        _ensure_cache_dir(self.cache_dir)
        # 0.5c per result plus 1c per possible unique author, rounded up once.
        estimated = (3 * max_results + 1) // 2
        cache_status = "miss"
        self._metadata(estimated_cost_cents=estimated, cache_status=cache_status, budget_status="checking")
        lock_path = self.cache_dir / "x-social.lock"
        with _exclusive_lock(lock_path):
            # Advance from the lock-acquired time so a fresh result installed by
            # another waiter cannot appear to come from the future.
            now = _utc_now(self.clock)
            conn = self._db()
            try:
                cached = conn.execute("SELECT body FROM evidence WHERE mint=?", (mint,)).fetchone()
                if cached:
                    try:
                        if not isinstance(cached[0], str) or len(cached[0].encode("utf-8")) > 2_000_000:
                            raise ValueError
                        evidence = _validate_evidence(json.loads(cached[0]), mint, now, require_fresh=True)
                    except (ValueError, TypeError, json.JSONDecodeError):
                        cache_status = "expired_or_invalid"
                        self._metadata(estimated_cost_cents=estimated, cache_status=cache_status, budget_status="checking")
                        conn.execute("DELETE FROM evidence WHERE mint=?", (mint,))
                    else:
                        self._metadata(cache_hit=True, cache_status="fresh", budget_status="cache_hit", source_requested_at=evidence["requested_at"],
                                       source_received_at=evidence["received_at"], source_sha256=evidence["response_sha256"],
                                       normalized_sha256=evidence["normalized_sha256"])
                        result = json.loads(json.dumps(evidence))
                        result["x_collection"] = dict(self.last_request_metadata)
                        return result

                cooldown = conn.execute("SELECT value FROM control WHERE key='cooldown_until'").fetchone()
                if cooldown:
                    try:
                        until = _iso(cooldown[0])
                    except ValueError:
                        until = now
                    if until > now:
                        self._metadata(cache_status=cache_status, budget_status="provider_cooldown")
                        raise SocialError("X provider cooldown active")

                day = now.date().isoformat()
                row = conn.execute("SELECT normal_cents, exception_cents FROM daily_spend WHERE day=?", (day,)).fetchone()
                normal_used, exception_used = row if row else (0, 0)
                normal_left = max(0, self.daily_budget_cents - normal_used)
                normal_reserve = min(estimated, normal_left)
                exception_reserve = estimated - normal_reserve
                if exception_reserve:
                    if not reason:
                        self._metadata(estimated_cost_cents=estimated, cache_status=cache_status, budget_status="budget_exhausted")
                        raise SocialError("X daily budget exhausted")
                    exception_left = max(0, self.exception_budget_cents - exception_used)
                    if exception_reserve > exception_left:
                        self._metadata(estimated_cost_cents=estimated, cache_status=cache_status,
                                       budget_status="budget_exhausted", exception_reason=reason)
                        raise SocialError("X daily budget exhausted")

                # Reservation is charged before the attempt and never refunded: a
                # timeout or provider failure can still incur the quoted cost.
                attempted_at = _utc_now(self.clock).isoformat()
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    conn.execute("INSERT INTO daily_spend(day,normal_cents,exception_cents) VALUES(?,?,?) "
                                 "ON CONFLICT(day) DO UPDATE SET normal_cents=normal_cents+excluded.normal_cents, "
                                 "exception_cents=exception_cents+excluded.exception_cents",
                                 (day, normal_reserve, exception_reserve))
                    audit_id = conn.execute(
                        "INSERT INTO request_audit(at,mint,max_results,normal_reserved,exception_reserved,exception_reason,status) VALUES(?,?,?,?,?,?,?)",
                        (attempted_at, mint, max_results, normal_reserve, exception_reserve,
                         reason if exception_reserve else None, "reserved"),
                    ).lastrowid
                    conn.execute("COMMIT")
                except sqlite3.Error:
                    if conn.in_transaction:
                        conn.execute("ROLLBACK")
                    raise SocialError("X budget reservation unavailable") from None
                self._metadata(api_requests=1, reserved_cost_cents=estimated, budget_status="reserved",
                               cache_status=cache_status,
                               normal_reserved_cost_cents=normal_reserve,
                               exception_reserved_cost_cents=exception_reserve,
                               exception_reason=reason if exception_reserve else None)
                try:
                    evidence = self.client.search_contract(mint, max_results=max_results)
                    evidence = _stamp_evidence(evidence)
                    evidence = _validate_evidence(evidence, mint, _utc_now(self.clock), require_fresh=True)
                except Exception as exc:
                    text = str(exc)
                    match = re.fullmatch(r"X search HTTP (402|429)", text)
                    if match:
                        http_status = int(match.group(1))
                        until = (_utc_now(self.clock) + COOLDOWN).isoformat()
                        conn.execute("INSERT INTO control(key,value) VALUES('cooldown_until',?) "
                                     "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (until,))
                        conn.execute("UPDATE request_audit SET status='provider_limit',http_status=? WHERE id=?", (http_status, audit_id))
                        self._metadata(api_requests=1, reserved_cost_cents=estimated, budget_status="provider_cooldown",
                                       normal_reserved_cost_cents=normal_reserve, exception_reserved_cost_cents=exception_reserve,
                                       cache_status=cache_status, http_status=http_status,
                                       sanitized_reason="provider_rate_or_payment_limit", exception_reason=reason if exception_reserve else None)
                        raise SocialError("X provider cooldown started") from None
                    conn.execute("UPDATE request_audit SET status='provider_unavailable' WHERE id=?", (audit_id,))
                    self._metadata(api_requests=1, reserved_cost_cents=estimated, budget_status="provider_unavailable",
                                   normal_reserved_cost_cents=normal_reserve, exception_reserved_cost_cents=exception_reserve,
                                   cache_status=cache_status, sanitized_reason="provider_request_failed",
                                   exception_reason=reason if exception_reserve else None)
                    raise SocialError("X evidence unavailable") from None
                body = json.dumps(evidence, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
                if len(body.encode("utf-8")) > 2_000_000 or len(evidence["posts"]) > max_results:
                    conn.execute("UPDATE request_audit SET status='invalid_evidence' WHERE id=?", (audit_id,))
                    self._metadata(api_requests=1, reserved_cost_cents=estimated, budget_status="provider_unavailable",
                                   normal_reserved_cost_cents=normal_reserve, exception_reserved_cost_cents=exception_reserve,
                                   cache_status=cache_status, sanitized_reason="provider_response_invalid")
                    raise SocialError("X evidence response invalid")
                conn.execute("UPDATE request_audit SET status='collected' WHERE id=?", (audit_id,))
                conn.execute("INSERT INTO evidence(mint,body) VALUES(?,?) ON CONFLICT(mint) DO UPDATE SET body=excluded.body",
                             (mint, body))
                self._metadata(api_requests=1, reserved_cost_cents=estimated, budget_status="collected",
                               cache_status=cache_status,
                               normal_reserved_cost_cents=normal_reserve, exception_reserved_cost_cents=exception_reserve,
                               source_requested_at=evidence["requested_at"], source_received_at=evidence["received_at"],
                               source_sha256=evidence["response_sha256"], normalized_sha256=evidence["normalized_sha256"],
                               exception_reason=reason if exception_reserve else None)
                result = json.loads(body)
                result["x_collection"] = dict(self.last_request_metadata)
                return result
            finally:
                conn.close()


__all__ = ["BudgetedXClient", "SocialError", "DEFAULT_CACHE_DIR"]
