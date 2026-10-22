from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from desks.equities.walkforward import (
    REGISTERED_STRATEGIES,
    WalkForwardError,
    WalkForwardSpec,
    evaluate,
    generate_trades,
    parse_bars,
    revalidation_due,
    retain_record,
)


UTC = timezone.utc


def _intraday_rows(day: str = "2026-01-02") -> list[dict[str, object]]:
    start = datetime.fromisoformat(f"{day}T09:30:00-05:00")
    close = datetime.fromisoformat(f"{day}T16:00:00-05:00")
    rows: list[dict[str, object]] = []
    for offset in range(386):
        stamp = start + timedelta(minutes=offset)
        price = 100.0
        if offset < 15:
            high = 101.0
            row_close = 100.0
        elif offset == 15:  # completed ORB signal bar
            high = 103.0
            row_close = 102.0
        elif offset == 29:  # 09:59 close below the next bar's VWAP reclaim context
            high = 100.0
            row_close = 99.0
        elif offset == 30:  # 10:00 completed VWAP reclaim signal bar
            high = 102.0
            row_close = 101.0
        else:
            high = 101.0
            row_close = 100.0
        if offset == 16:
            price = 102.0  # next-bar entry after ORB
        if offset == 31:
            price = 101.5  # next-bar entry after VWAP reclaim
        if offset == 385:
            price = 104.0  # scheduled 15:55 exit bar
        rows.append(
            {
                "symbol": "TEST",
                "timestamp": stamp.isoformat(),
                "open": price,
                "high": max(high, price, row_close),
                "low": min(99.0, price, row_close),
                "close": row_close,
                "volume": 100000,
                "session_close": close.isoformat(),
                "vwap": 100.0,
            }
        )
    return rows


def _spec() -> WalkForwardSpec:
    return WalkForwardSpec(
        "2026-01-01",
        "2026-01-01",
        "2026-01-02",
        "2026-01-02",
        "2026-01-03",
        "2026-01-03",
        purge_days=0,
        cadence="daily",
    )


def test_registry_contains_frozen_overnight_and_intraday_candidates() -> None:
    assert set(REGISTERED_STRATEGIES) == {
        "overnight_monthly_v1",
        "intraday_orb15_v1",
        "intraday_vwap_reclaim_v1",
    }


def test_naive_timestamps_are_rejected() -> None:
    row = _intraday_rows()[0]
    row["timestamp"] = "2026-01-02T09:30:00"
    with pytest.raises(WalkForwardError, match="timezone"):
        parse_bars([row])


def test_orb_entry_uses_next_completed_bar_and_scheduled_exit() -> None:
    bars = parse_bars(_intraday_rows())
    trades = generate_trades("intraday_orb15_v1", bars)
    assert len(trades) == 1
    assert trades[0].entry_timestamp.endswith("14:46:00Z")
    assert trades[0].exit_timestamp.endswith("20:55:00Z")


def test_vwap_reclaim_entry_uses_next_bar() -> None:
    trades = generate_trades("intraday_vwap_reclaim_v1", parse_bars(_intraday_rows()))
    assert len(trades) == 1
    assert trades[0].entry_timestamp.endswith("15:01:00Z")


def test_walkforward_emits_all_cost_trials_and_purged_splits() -> None:
    record = evaluate("intraday_orb15_v1", _intraday_rows(), _spec(), costs_bps_per_side=(0, 15, 30))
    assert record["schema_version"] == "EQ-WALKFORWARD-v1"
    assert record["data_limits"]["point_in_time_universe_verified"] is False
    assert record["data_limits"]["incomplete_sessions"] == 0
    assert record["acceptance"]["eligible_for_promotion"] is False
    assert set(record["trials"]) == {"0.0", "15.0", "30.0"}
    assert record["trials"]["0.0"]["validation"]["trade_count"] == 1
    assert record["trials"]["30.0"]["validation"]["net_total_return"] < record["trials"]["0.0"]["validation"]["net_total_return"]


def test_record_is_content_addressed_and_immutable(tmp_path) -> None:
    record = evaluate("intraday_orb15_v1", _intraday_rows(), _spec(), costs_bps_per_side=(15,))
    first = retain_record(tmp_path, record)
    second = retain_record(tmp_path, record)
    assert first == second
    assert (tmp_path / "intraday_orb15_v1").joinpath(first["sha256"] + ".json").exists()


def test_revalidation_cadence_is_pure_and_calendar_based() -> None:
    assert revalidation_due(None, datetime.now(UTC), "monthly")
    last = datetime(2026, 1, 31, tzinfo=UTC)
    assert not revalidation_due(last, datetime(2026, 2, 1, tzinfo=UTC), "weekly")
    assert revalidation_due(last, datetime(2026, 2, 1, tzinfo=UTC), "monthly")
    with pytest.raises(WalkForwardError):
        revalidation_due(last, datetime(2026, 1, 1, tzinfo=UTC), "daily")
