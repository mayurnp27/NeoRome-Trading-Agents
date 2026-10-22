"""Provider-neutral quality checks for aligned intraday bars.

This module deliberately retains both provider records.  A discrepancy is a
reason to quarantine a bar from a merit evaluation, not evidence that either
provider is right or wrong.  It is offline-only and has no provider or broker
dependencies.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from math import isfinite
from numbers import Real
from typing import Any, Iterable, Mapping, Sequence


DEFAULT_FIELDS = ("open", "high", "low", "close")
_MINUTE = timedelta(minutes=1)


class BarQualityInputError(ValueError):
    """Raised when a source cannot be compared without guessing."""


@dataclass(frozen=True)
class FieldDifference:
    left: float
    right: float
    absolute: float
    relative: float
    tolerance: float

    def as_dict(self) -> dict[str, float]:
        return {
            "left": self.left,
            "right": self.right,
            "absolute": self.absolute,
            "relative": self.relative,
            "tolerance": self.tolerance,
        }


@dataclass(frozen=True)
class BarComparison:
    """One aligned timestamp, retaining raw observations from each source."""

    timestamp: datetime
    left_timestamp: str | None
    right_timestamp: str | None
    left_bar: dict[str, Any] | None
    right_bar: dict[str, Any] | None
    differences: dict[str, FieldDifference]
    reasons: tuple[str, ...]
    quarantined: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat(),
            "left_timestamp": self.left_timestamp,
            "right_timestamp": self.right_timestamp,
            "left_bar": self.left_bar,
            "right_bar": self.right_bar,
            "differences": {key: value.as_dict() for key, value in self.differences.items()},
            "reasons": list(self.reasons),
            "quarantined": self.quarantined,
        }


@dataclass(frozen=True)
class QuarantineSegment:
    """A contiguous run of quarantined one-minute timestamps."""

    start: datetime
    end: datetime
    timestamps: tuple[datetime, ...]
    reasons: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "timestamps": [item.isoformat() for item in self.timestamps],
            "bar_count": len(self.timestamps),
            "reasons": list(self.reasons),
        }


@dataclass(frozen=True)
class BarQualityReport:
    """Complete comparison evidence and derived quarantine counts."""

    left_source: str
    right_source: str
    fields: tuple[str, ...]
    absolute_tolerance: float
    relative_tolerance: float
    comparisons: tuple[BarComparison, ...]
    segments: tuple[QuarantineSegment, ...]

    @property
    def counts(self) -> dict[str, int]:
        quarantined = [item for item in self.comparisons if item.quarantined]
        return {
            "left_bars": sum(item.left_bar is not None for item in self.comparisons),
            "right_bars": sum(item.right_bar is not None for item in self.comparisons),
            "aligned_bars": sum(item.left_bar is not None and item.right_bar is not None for item in self.comparisons),
            "timestamp_mismatch_bars": sum("missing_left_bar" in item.reasons or "missing_right_bar" in item.reasons for item in self.comparisons),
            "material_discrepancy_bars": sum(bool(item.differences) for item in self.comparisons),
            "quarantined_bars": len(quarantined),
            "clean_aligned_bars": sum(not item.quarantined for item in self.comparisons if item.left_bar is not None and item.right_bar is not None),
            "quarantined_segments": len(self.segments),
        }

    def as_dict(self) -> dict[str, Any]:
        return {
            "left_source": self.left_source,
            "right_source": self.right_source,
            "fields": list(self.fields),
            "thresholds": {
                "absolute_tolerance": self.absolute_tolerance,
                "relative_tolerance": self.relative_tolerance,
            },
            "counts": self.counts,
            "comparisons": [item.as_dict() for item in self.comparisons],
            "segments": [item.as_dict() for item in self.segments],
            "source_preference": None,
        }


def _timestamp(value: Any, label: str) -> tuple[datetime, str]:
    original = str(value)
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError as exc:
            raise BarQualityInputError(f"{label} is not an ISO timestamp") from exc
    else:
        raise BarQualityInputError(f"{label} must be a datetime or ISO timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise BarQualityInputError(f"{label} must include a timezone")
    return parsed.astimezone(timezone.utc), original


def _number(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise BarQualityInputError(f"{label} must be a finite number")
    result = float(value)
    if not isfinite(result):
        raise BarQualityInputError(f"{label} must be a finite number")
    if result <= 0:
        raise BarQualityInputError(f"{label} must be positive")
    return result


def _normalize_source(bars: Iterable[Mapping[str, Any]], fields: Sequence[str], source: str) -> list[tuple[datetime, str, dict[str, Any]]]:
    if isinstance(bars, (str, bytes)):
        raise BarQualityInputError(f"{source} bars must be an iterable of mappings")
    try:
        rows = list(bars)
    except TypeError as exc:
        raise BarQualityInputError(f"{source} bars must be an iterable of mappings") from exc
    normalized: list[tuple[datetime, str, dict[str, Any]]] = []
    previous: datetime | None = None
    seen: set[datetime] = set()
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            raise BarQualityInputError(f"{source} bar {index} must be a mapping")
        if "timestamp" not in row:
            raise BarQualityInputError(f"{source} bar {index} is missing timestamp")
        parsed, original_timestamp = _timestamp(row["timestamp"], f"{source} bar {index} timestamp")
        if parsed in seen:
            raise BarQualityInputError(f"{source} contains duplicate timestamp {original_timestamp}")
        if previous is not None and parsed <= previous:
            raise BarQualityInputError(f"{source} timestamps must be strictly increasing")
        seen.add(parsed)
        previous = parsed
        copy = dict(row)
        copy["timestamp"] = row["timestamp"]
        values: dict[str, float] = {}
        for field in fields:
            if field not in row:
                raise BarQualityInputError(f"{source} bar {index} is missing {field}")
            values[field] = _number(row[field], f"{source} bar {index} {field}")
        if all(field in values for field in DEFAULT_FIELDS):
            if values["high"] < max(values["open"], values["close"], values["low"]):
                raise BarQualityInputError(f"{source} bar {index} has an inconsistent high")
            if values["low"] > min(values["open"], values["close"], values["high"]):
                raise BarQualityInputError(f"{source} bar {index} has an inconsistent low")
        normalized.append((parsed, original_timestamp, copy))
    return normalized


def _field_difference(left: Any, right: Any, absolute_tolerance: float, relative_tolerance: float) -> FieldDifference | None:
    left_value = float(left)
    right_value = float(right)
    absolute = abs(left_value - right_value)
    scale = max(abs(left_value), abs(right_value), 1.0)
    relative = absolute / scale
    tolerance = max(absolute_tolerance, relative_tolerance * scale)
    if absolute <= tolerance:
        return None
    return FieldDifference(left_value, right_value, absolute, relative, tolerance)


def _segments(comparisons: Sequence[BarComparison]) -> tuple[QuarantineSegment, ...]:
    quarantined = [item for item in comparisons if item.quarantined]
    result: list[QuarantineSegment] = []
    current: list[BarComparison] = []
    for item in quarantined:
        if current and item.timestamp - current[-1].timestamp != _MINUTE:
            result.append(_segment(current))
            current = []
        current.append(item)
    if current:
        result.append(_segment(current))
    return tuple(result)


def _segment(items: Sequence[BarComparison]) -> QuarantineSegment:
    reasons = tuple(sorted({reason for item in items for reason in item.reasons}))
    return QuarantineSegment(items[0].timestamp, items[-1].timestamp, tuple(item.timestamp for item in items), reasons)


def compare_aligned_bars(
    left_bars: Iterable[Mapping[str, Any]],
    right_bars: Iterable[Mapping[str, Any]],
    *,
    absolute_tolerance: float,
    relative_tolerance: float = 0.0,
    fields: Sequence[str] = DEFAULT_FIELDS,
    left_source: str = "left",
    right_source: str = "right",
) -> BarQualityReport:
    """Compare two timestamp-aligned sources and quarantine disagreements.

    Tolerances are explicit caller inputs.  The returned evidence never picks a
    winning source: both raw bars and field-level values remain available.
    """
    if not isinstance(absolute_tolerance, Real) or isinstance(absolute_tolerance, bool) or not isfinite(float(absolute_tolerance)) or absolute_tolerance < 0:
        raise BarQualityInputError("absolute_tolerance must be a finite non-negative number")
    if not isinstance(relative_tolerance, Real) or isinstance(relative_tolerance, bool) or not isfinite(float(relative_tolerance)) or relative_tolerance < 0:
        raise BarQualityInputError("relative_tolerance must be a finite non-negative number")
    field_tuple = tuple(fields)
    if not field_tuple or len(set(field_tuple)) != len(field_tuple) or any(not isinstance(field, str) or not field for field in field_tuple):
        raise BarQualityInputError("fields must contain unique non-empty names")
    left = _normalize_source(left_bars, field_tuple, left_source)
    right = _normalize_source(right_bars, field_tuple, right_source)
    left_by_time = {item[0]: item for item in left}
    right_by_time = {item[0]: item for item in right}
    comparisons: list[BarComparison] = []
    for timestamp in sorted(set(left_by_time) | set(right_by_time)):
        left_item = left_by_time.get(timestamp)
        right_item = right_by_time.get(timestamp)
        left_bar = left_item[2] if left_item else None
        right_bar = right_item[2] if right_item else None
        reasons: list[str] = []
        differences: dict[str, FieldDifference] = {}
        if left_item is None:
            reasons.append("missing_left_bar")
        if right_item is None:
            reasons.append("missing_right_bar")
        if left_item is not None and right_item is not None:
            for field in field_tuple:
                difference = _field_difference(left_bar[field], right_bar[field], float(absolute_tolerance), float(relative_tolerance))
                if difference is not None:
                    differences[field] = difference
                    reasons.append(f"material_{field}_difference")
        comparisons.append(BarComparison(
            timestamp=timestamp,
            left_timestamp=left_item[1] if left_item else None,
            right_timestamp=right_item[1] if right_item else None,
            left_bar=left_bar,
            right_bar=right_bar,
            differences=differences,
            reasons=tuple(reasons),
            quarantined=bool(reasons),
        ))
    frozen_fields = tuple(field_tuple)
    return BarQualityReport(left_source, right_source, frozen_fields, float(absolute_tolerance), float(relative_tolerance), tuple(comparisons), _segments(comparisons))


assess_bar_quality = compare_aligned_bars

