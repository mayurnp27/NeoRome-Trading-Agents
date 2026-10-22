"""The isolated, exactly-$100 internal paper account for prediction markets.

This is an accounting boundary, not a venue adapter.  It deliberately shares
none of the firm's multi-desk ledger limits: the only funded account is the
prediction sleeve and its maximum funded loss is its $100 cash balance.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from typing import Any

from .paper_ledger import DESKS, PaperLedger, RiskLimits, ValidationError

UTC = timezone.utc
VERSION = "prediction-full-port-paper-v1"
FULL_PORT_CENTS = 10_000
FULL_PORT_BALANCES = {"prediction": FULL_PORT_CENTS, "equities": 0, "options": 0, "crypto": 0}
FULL_PORT_LIMITS = RiskLimits(
    aggregate_cap_cents=FULL_PORT_CENTS,
    group_cap_cents=FULL_PORT_CENTS,
    per_idea_cents={"prediction": FULL_PORT_CENTS, "equities": 1, "options": 1, "crypto": 1},
)
DEFAULT_LEDGER = "/var/lib/neorome-prediction-paper/paper.sqlite3"


class PredictionFullPortError(ValueError):
    pass


def assert_full_port_book(book: PaperLedger) -> None:
    """Refuse a look-alike ledger with funding or limits outside the sleeve."""
    if not book.enforce_market_risk:
        raise PredictionFullPortError("prediction full-port market-risk ledger is required")
    balances = {balance.desk: balance.initial_cash_cents for balance in book.balances()}
    if balances != FULL_PORT_BALANCES:
        raise PredictionFullPortError("prediction full-port ledger must contain exactly the $100 prediction sleeve")
    if (book.limits.aggregate_cap_cents != FULL_PORT_LIMITS.aggregate_cap_cents
            or book.limits.group_cap_cents != FULL_PORT_LIMITS.group_cap_cents
            or dict(book.limits.per_idea_cents) != dict(FULL_PORT_LIMITS.per_idea_cents)):
        raise PredictionFullPortError("prediction full-port risk limits are required")
    if set(balances) != set(DESKS) or sum(balances.values()) != FULL_PORT_CENTS:
        raise PredictionFullPortError("prediction full-port ledger funding is invalid")


def initialize(path: str | os.PathLike[str] = DEFAULT_LEDGER, *, now: datetime | None = None) -> PaperLedger:
    """Create or reopen the fixed isolated account; no trade is created here."""
    source = Path(path)
    if source.exists() and source.is_symlink():
        raise PredictionFullPortError("prediction full-port ledger symlink refused")
    try:
        book = PaperLedger(source, balances=FULL_PORT_BALANCES, limits=FULL_PORT_LIMITS,
                           enforce_market_risk=True, clock=lambda: now or datetime.now(UTC))
    except (OSError, ValidationError) as exc:
        raise PredictionFullPortError("prediction full-port ledger configuration is invalid") from exc
    assert_full_port_book(book)
    return book


def open_existing(path: str | os.PathLike[str] = DEFAULT_LEDGER, *, now: datetime | None = None) -> PaperLedger:
    source = Path(path)
    if source.is_symlink() or not source.is_file():
        raise PredictionFullPortError("prediction full-port ledger must already exist as a regular file")
    return initialize(source, now=now)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", default=DEFAULT_LEDGER)
    parser.add_argument("--initialize", action="store_true", help="create or verify the isolated paper account")
    args = parser.parse_args(argv)
    if not args.initialize:
        print(json.dumps({"status": "abstained", "reason": "--initialize is required", "orders_sent": 0}, sort_keys=True))
        return 0
    try:
        book = initialize(args.ledger)
        print(json.dumps({"status": "initialized", "version": VERSION, "paper_nav_cents": book.snapshot().paper_nav_cents,
                          "orders_sent": 0, "venue_requests": 0}, sort_keys=True))
        return 0
    except PredictionFullPortError as exc:
        print(json.dumps({"status": "abstained", "reason": str(exc), "orders_sent": 0}, sort_keys=True))
        return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["DEFAULT_LEDGER", "FULL_PORT_BALANCES", "FULL_PORT_CENTS", "FULL_PORT_LIMITS", "PredictionFullPortError", "assert_full_port_book", "initialize", "open_existing"]
