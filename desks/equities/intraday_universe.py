"""Point-in-time membership gate for EQ-INTRADAY research.

This module deliberately does not download a universe.  It accepts a retained,
versioned historical membership extract and refuses the common shortcut of
using today's listed symbols for old sessions.  A caller can only form an
exploratory universe if the source explicitly covers delisted names.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Mapping

import pandas as pd


class UniverseValidationError(ValueError):
    """Raised when membership evidence cannot support a PIT research claim."""


REQUIRED_COLUMNS = {
    "permanent_id",
    "symbol",
    "effective_start",
    "effective_end",
    "known_at",
    "security_type",
    "exchange",
}
ORDINARY_TYPES = {"common_stock", "ordinary_share"}


@dataclass(frozen=True)
class UniverseEvidence:
    source: str
    source_version: str
    captured_at: pd.Timestamp
    includes_delisted: bool
    coverage_start: date
    coverage_end: date


def _timestamp(value: Any, label: str, *, allow_missing: bool = False) -> pd.Timestamp | None:
    if value is None or pd.isna(value):
        if allow_missing:
            return None
        raise UniverseValidationError(f"{label} is required")
    try:
        parsed = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise UniverseValidationError(f"{label} is not a date") from exc
    if parsed.tzinfo is not None:
        parsed = parsed.tz_convert("UTC").tz_localize(None)
    return parsed.normalize()


def parse_evidence(payload: Mapping[str, Any]) -> UniverseEvidence:
    """Validate coverage metadata supplied with an immutable membership extract."""

    if not isinstance(payload, Mapping):
        raise UniverseValidationError("universe evidence must be an object")
    source = payload.get("source")
    version = payload.get("source_version")
    if not isinstance(source, str) or not source.strip():
        raise UniverseValidationError("universe evidence source is required")
    if not isinstance(version, str) or not version.strip():
        raise UniverseValidationError("universe evidence source_version is required")
    if payload.get("includes_delisted") is not True:
        raise UniverseValidationError("membership source must explicitly include delisted securities")
    captured_at = _timestamp(payload.get("captured_at"), "universe evidence captured_at")
    coverage_start = _timestamp(payload.get("coverage_start"), "universe evidence coverage_start")
    coverage_end = _timestamp(payload.get("coverage_end"), "universe evidence coverage_end")
    assert captured_at is not None and coverage_start is not None and coverage_end is not None
    if coverage_end < coverage_start:
        raise UniverseValidationError("universe evidence coverage is inverted")
    return UniverseEvidence(source.strip(), version.strip(), captured_at, True, coverage_start.date(), coverage_end.date())


def validate_membership(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize a historical membership extract without choosing a source."""

    if not isinstance(frame, pd.DataFrame) or frame.empty:
        raise UniverseValidationError("membership extract is empty")
    missing = sorted(REQUIRED_COLUMNS - set(frame.columns))
    if missing:
        raise UniverseValidationError("membership extract is missing: " + ", ".join(missing))
    result = frame.copy()
    for column in ("permanent_id", "symbol", "security_type", "exchange"):
        result[column] = result[column].astype(str).str.strip()
        if result[column].eq("").any() or result[column].str.lower().eq("nan").any():
            raise UniverseValidationError(f"membership {column} cannot be empty")
    result["effective_start"] = [
        _timestamp(value, "membership effective_start") for value in result["effective_start"]
    ]
    result["effective_end"] = [
        _timestamp(value, "membership effective_end", allow_missing=True) for value in result["effective_end"]
    ]
    result["known_at"] = [
        _timestamp(value, "membership known_at") for value in result["known_at"]
    ]
    if any(end is not None and end <= start for start, end in zip(result["effective_start"], result["effective_end"])):
        raise UniverseValidationError("membership effective_end must be after effective_start")
    result["security_type"] = result["security_type"].str.lower()
    result = result.sort_values(["permanent_id", "effective_start", "symbol"], kind="mergesort").reset_index(drop=True)
    for permanent_id, group in result.groupby("permanent_id", sort=False):
        previous_end: pd.Timestamp | None = None
        open_interval = False
        for row in group.itertuples(index=False):
            if open_interval:
                raise UniverseValidationError(f"open-ended membership interval overlaps a later row for {permanent_id}")
            if previous_end is not None and row.effective_start < previous_end:
                raise UniverseValidationError(f"overlapping membership intervals for {permanent_id}")
            previous_end = row.effective_end
            if previous_end is None:
                open_interval = True
    return result


def members_on(
    membership: pd.DataFrame,
    evidence: UniverseEvidence,
    as_of: Any,
) -> pd.DataFrame:
    """Return eligible ordinary shares active on *as_of* under retained PIT evidence."""

    day = _timestamp(as_of, "as_of")
    assert day is not None
    if not (evidence.coverage_start <= day.date() <= evidence.coverage_end):
        raise UniverseValidationError("as_of is outside verified membership coverage")
    normalized = validate_membership(membership)
    active = normalized[(normalized["effective_start"] <= day) & (
        normalized["effective_end"].isna() | (normalized["effective_end"] > day)
    ) & (normalized["known_at"] <= day)]
    ordinary = active[active["security_type"].isin(ORDINARY_TYPES)].copy()
    if ordinary["permanent_id"].duplicated().any():
        raise UniverseValidationError("multiple active rows for a permanent identifier")
    return ordinary.sort_values(["permanent_id", "symbol"], kind="mergesort").reset_index(drop=True)


__all__ = ["UniverseEvidence", "UniverseValidationError", "members_on", "parse_evidence", "validate_membership"]
