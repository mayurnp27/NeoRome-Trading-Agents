"""Small, safety-first Alpaca paper trading adapter.

The module intentionally has no dependency on the Alpaca SDK.  Credentials are
loaded lazily from the macOS Keychain, so validation and previews are entirely
offline.
"""

from __future__ import annotations

import argparse
import datetime as _datetime
import http.client
import json
import math
import re
import socket
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, fields
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Mapping, Optional, Tuple


PAPER_API_URL = "https://paper-api.alpaca.markets"
KEYCHAIN_ACCOUNT = "neorome"
KEYCHAIN_KEY_ID_SERVICE = "neorome.alpaca.paper.key-id"
KEYCHAIN_SECRET_SERVICE = "neorome.alpaca.paper.secret-key"
_CLIENT_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,48}$")
_OCC_SYMBOL_RE = re.compile(
    r"^(?P<underlying>[A-Z]{1,6})(?P<expiry>\d{6})(?P<right>[CP])(?P<strike>\d{8})$"
)


class PaperAdapterError(Exception):
    """Base class for safe, user-facing adapter errors."""


class ValidationError(PaperAdapterError, ValueError):
    pass


class CredentialError(PaperAdapterError):
    pass


class ApiError(PaperAdapterError):
    def __init__(self, status: int, message: str = "Alpaca paper API request failed"):
        self.status = status
        super().__init__(message)


class RedirectBlockedError(PaperAdapterError):
    pass


class ResponseError(PaperAdapterError):
    pass


class AmbiguousOrderError(PaperAdapterError):
    """A mutation may have reached Alpaca but its result is unknown."""


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise RedirectBlockedError("HTTP redirects are not permitted for paper API requests")


@dataclass(frozen=True)
class OrderRequest:
    """A simple, quantity-based order supported by the paper adapter."""

    symbol: str
    qty: Any
    side: str
    asset_class: str
    client_order_id: str
    type: str = "market"
    time_in_force: str = "day"
    limit_price: Any = None
    position_intent: Optional[str] = None
    # Kept as explicit fields so callers receive a useful validation error.
    notional: Any = None
    legs: Any = None
    order_class: Optional[str] = None
    multileg: Optional[bool] = None

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "OrderRequest":
        if not isinstance(value, Mapping):
            raise ValidationError("order must be a JSON object")
        allowed = {field.name for field in fields(cls)}
        unknown = sorted(set(value) - allowed)
        if unknown:
            raise ValidationError("unsupported order field: " + ", ".join(str(k) for k in unknown))
        try:
            return cls(**dict(value))
        except TypeError as exc:
            # Dataclass's missing-field message is safe but less stable and can
            # accidentally expose arbitrary mapping details.
            raise ValidationError("order requires symbol, qty, side, asset_class, and client_order_id") from None

    def _decimal(self, value: Any, label: str) -> Decimal:
        if isinstance(value, bool) or value is None:
            raise ValidationError(label + " must be a positive finite number")
        try:
            number = Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError):
            raise ValidationError(label + " must be a positive finite number") from None
        if not number.is_finite() or number <= 0:
            raise ValidationError(label + " must be a positive finite number")
        return number

    @staticmethod
    def _decimal_text(value: Decimal) -> str:
        text = format(value, "f")
        if "." in text:
            text = text.rstrip("0").rstrip(".")
        return text or "0"

    def validate(self) -> "OrderRequest":
        if not isinstance(self.symbol, str) or not self.symbol.strip():
            raise ValidationError("symbol is required")
        asset_class = self.asset_class.lower() if isinstance(self.asset_class, str) else ""
        if asset_class not in ("us_equity", "us_option", "crypto"):
            raise ValidationError("asset_class must be us_equity, us_option, or crypto")
        side = self.side.lower() if isinstance(self.side, str) else ""
        if side not in ("buy", "sell"):
            raise ValidationError("side must be buy or sell")
        order_type = self.type.lower() if isinstance(self.type, str) else ""
        if order_type not in ("market", "limit"):
            raise ValidationError("type must be market or limit")
        tif = self.time_in_force.lower() if isinstance(self.time_in_force, str) else ""
        if not isinstance(self.client_order_id, str) or not _CLIENT_ID_RE.fullmatch(self.client_order_id):
            raise ValidationError("client_order_id must be 1-48 letters, digits, hyphens, or underscores")
        quantity = self._decimal(self.qty, "qty")
        if order_type == "limit":
            self._decimal(self.limit_price, "limit_price")
        elif self.limit_price is not None:
            raise ValidationError("limit_price is only valid for limit orders")
        if self.notional is not None:
            raise ValidationError("notional orders are not supported")
        if self.legs is not None:
            raise ValidationError("multi-leg orders are not supported")
        if self.order_class is not None:
            raise ValidationError("multi-leg orders are not supported")
        if self.multileg:
            raise ValidationError("multi-leg orders are not supported")

        if asset_class == "us_option":
            if quantity != quantity.to_integral_value():
                raise ValidationError("us_option qty must be a whole number of contracts")
            if tif != "day":
                raise ValidationError("us_option time_in_force must be day")
            intent = self.position_intent.lower() if isinstance(self.position_intent, str) else ""
            if intent not in ("buy_to_open", "buy_to_close", "sell_to_open", "sell_to_close"):
                raise ValidationError("us_option requires a valid position_intent")
            if not intent.startswith(side + "_"):
                raise ValidationError("position_intent side must match side")
        elif asset_class == "crypto":
            if tif not in ("gtc", "ioc"):
                raise ValidationError("crypto time_in_force must be gtc or ioc")
            if self.position_intent is not None:
                raise ValidationError("position_intent is only valid for us_option")
        else:
            if self.position_intent is not None:
                raise ValidationError("position_intent is only valid for us_option")
            if tif not in ("day", "gtc", "opg", "cls", "ioc", "fok"):
                raise ValidationError("us_equity time_in_force is not supported")
            # Fractional equity quantities are accepted only for day orders.
            if quantity != quantity.to_integral_value() and tif != "day":
                raise ValidationError("fractional us_equity qty requires day time_in_force")
        return self

    def payload(self) -> Dict[str, str]:
        self.validate()
        quantity = self._decimal(self.qty, "qty")
        payload = {
            "symbol": self.symbol,
            "qty": self._decimal_text(quantity),
            "side": self.side.lower(),
            "type": self.type.lower(),
            "time_in_force": self.time_in_force.lower(),
            "client_order_id": self.client_order_id,
        }
        if self.type.lower() == "limit":
            payload["limit_price"] = self._decimal_text(self._decimal(self.limit_price, "limit_price"))
        if self.asset_class.lower() == "us_option":
            payload["position_intent"] = self.position_intent.lower()  # type: ignore
        return payload


@dataclass(frozen=True)
class OptionLeg:
    """One option leg in a typed multi-leg order request."""

    symbol: str
    ratio_qty: Any
    side: str
    position_intent: str

    def validate(self) -> "OptionLeg":
        if not isinstance(self.symbol, str):
            raise ValidationError("option leg symbol must be a valid OCC symbol")
        match = _OCC_SYMBOL_RE.fullmatch(self.symbol)
        if match is None:
            raise ValidationError("option leg symbol must be a valid OCC symbol")
        try:
            _datetime.date(
                2000 + int(match.group("expiry")[:2]),
                int(match.group("expiry")[2:4]),
                int(match.group("expiry")[4:]),
            )
        except ValueError:
            raise ValidationError("option leg symbol must have a valid expiration") from None
        if int(match.group("strike")) <= 0:
            raise ValidationError("option leg symbol must have a positive strike")

        ratio = _positive_decimal(self.ratio_qty, "option leg ratio_qty")
        if ratio != ratio.to_integral_value():
            raise ValidationError("option leg ratio_qty must be a whole number")
        side = self.side.lower() if isinstance(self.side, str) else ""
        if side not in ("buy", "sell"):
            raise ValidationError("option leg side must be buy or sell")
        intent = self.position_intent.lower() if isinstance(self.position_intent, str) else ""
        if intent not in ("buy_to_open", "buy_to_close", "sell_to_open", "sell_to_close"):
            raise ValidationError("option leg requires a valid position_intent")
        if not intent.startswith(side + "_"):
            raise ValidationError("option leg position_intent side must match side")
        return self

    def payload(self) -> Dict[str, str]:
        self.validate()
        ratio = _positive_decimal(self.ratio_qty, "option leg ratio_qty")
        return {
            "symbol": self.symbol,
            "ratio_qty": _decimal_text(ratio),
            "side": self.side.lower(),
            "position_intent": self.position_intent.lower(),
        }


def _positive_decimal(value: Any, label: str) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise ValidationError(label + " must be a positive finite number")
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        raise ValidationError(label + " must be a positive finite number") from None
    if not number.is_finite() or number <= 0:
        raise ValidationError(label + " must be a positive finite number")
    return number


def _decimal_text(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


@dataclass(frozen=True)
class MultiLegOrderRequest:
    """A constrained, typed two-leg vertical options order."""

    legs: Any
    qty: Any
    limit_price: Any
    client_order_id: str
    type: str = "limit"
    time_in_force: str = "day"
    order_class: str = "mleg"

    def __post_init__(self) -> None:
        # A frozen request must also hold an immutable leg collection.
        if isinstance(self.legs, list):
            object.__setattr__(self, "legs", tuple(self.legs))

    @staticmethod
    def _option_details(symbol: str) -> Tuple[str, str, str, Decimal]:
        match = _OCC_SYMBOL_RE.fullmatch(symbol)
        if match is None:
            raise ValidationError("option leg symbol must be a valid OCC symbol")
        return (
            match.group("underlying"),
            match.group("expiry"),
            match.group("right"),
            Decimal(int(match.group("strike"))) / Decimal(1000),
        )

    def validate(self) -> "MultiLegOrderRequest":
        if not isinstance(self.legs, (tuple, list)) or len(self.legs) != 2:
            raise ValidationError("mleg requires exactly two option legs")
        if any(not isinstance(leg, OptionLeg) for leg in self.legs):
            raise ValidationError("mleg legs must be OptionLeg instances")
        for leg in self.legs:
            leg.validate()
        first, second = self.legs
        first_details = self._option_details(first.symbol)
        second_details = self._option_details(second.symbol)
        if first_details[:3] != second_details[:3]:
            raise ValidationError("mleg legs must share underlying, expiration, and call/put right")
        if first_details[3] == second_details[3]:
            raise ValidationError("mleg legs must have distinct strikes")
        if _positive_decimal(first.ratio_qty, "option leg ratio_qty") != Decimal(1) or _positive_decimal(
            second.ratio_qty, "option leg ratio_qty"
        ) != Decimal(1):
            raise ValidationError("vertical mleg leg ratio_qty must be 1:1")

        quantity = _positive_decimal(self.qty, "qty")
        if quantity != quantity.to_integral_value():
            raise ValidationError("mleg qty must be a whole number of contracts")
        if not isinstance(self.client_order_id, str) or not _CLIENT_ID_RE.fullmatch(self.client_order_id):
            raise ValidationError("client_order_id must be 1-48 letters, digits, hyphens, or underscores")
        order_type = self.type.lower() if isinstance(self.type, str) else ""
        if order_type != "limit":
            raise ValidationError("mleg type must be limit")
        tif = self.time_in_force.lower() if isinstance(self.time_in_force, str) else ""
        if tif != "day":
            raise ValidationError("mleg time_in_force must be day")
        order_class = self.order_class.lower() if isinstance(self.order_class, str) else ""
        if order_class != "mleg":
            raise ValidationError("mleg order_class must be mleg")
        if first.side.lower() == second.side.lower():
            raise ValidationError("mleg legs must have opposite sides")
        if not first.position_intent.lower().endswith("_to_open") or not second.position_intent.lower().endswith(
            "_to_open"
        ):
            raise ValidationError("mleg legs must have matching open intent")
        if first.position_intent.lower() == second.position_intent.lower():
            raise ValidationError("mleg legs must have matching open intent")

        if isinstance(self.limit_price, bool) or self.limit_price is None:
            raise ValidationError("limit_price must be a nonzero finite number")
        try:
            price = Decimal(str(self.limit_price))
        except (InvalidOperation, ValueError, TypeError):
            raise ValidationError("limit_price must be a nonzero finite number") from None
        if not price.is_finite() or price == 0:
            raise ValidationError("limit_price must be a nonzero finite number")
        width = abs(first_details[3] - second_details[3])
        if abs(price) >= width:
            raise ValidationError("abs(limit_price) must be less than the vertical strike width")

        low, high = sorted((first_details[3], second_details[3]))
        low_leg = first if first_details[3] == low else second
        high_leg = first if first_details[3] == high else second
        right = first_details[2]
        debit_orientation = (
            low_leg.side.lower() == "buy" if right == "C" else high_leg.side.lower() == "buy"
        )
        if (price > 0) != debit_orientation:
            raise ValidationError("limit_price sign is inconsistent with the vertical call/put orientation")
        return self

    def payload(self) -> Dict[str, Any]:
        self.validate()
        quantity = _positive_decimal(self.qty, "qty")
        price = Decimal(str(self.limit_price))
        return {
            "order_class": "mleg",
            "qty": _decimal_text(quantity),
            "type": "limit",
            "limit_price": _decimal_text(price),
            "time_in_force": "day",
            "client_order_id": self.client_order_id,
            "legs": [leg.payload() for leg in self.legs],
        }


def _keychain_value(service: str) -> str:
    try:
        result = subprocess.run(
            ["/usr/bin/security", "find-generic-password", "-a", KEYCHAIN_ACCOUNT, "-s", service, "-w"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
            text=True,
        )
    except (OSError, subprocess.SubprocessError):
        raise CredentialError("Unable to load paper API credentials from Keychain") from None
    value = result.stdout.strip()
    if not value:
        raise CredentialError("Paper API credential is empty")
    return value


class PaperClient:
    """Alpaca paper API client with dry-run-by-default mutations."""

    def __init__(
        self,
        credential_provider: Optional[Any] = None,
        timeout: float = 10.0,
        opener: Optional[Any] = None,
    ):
        if not isinstance(timeout, (int, float)) or isinstance(timeout, bool) or not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("timeout must be a positive finite number")
        self._credential_provider = _keychain_value if credential_provider is None else credential_provider
        self._timeout = timeout
        self._credentials_cache: Optional[Tuple[str, str]] = None
        self._opener = urllib.request.build_opener(_NoRedirectHandler()) if opener is None else opener

    def _credential(self, service: str) -> str:
        try:
            provider = self._credential_provider
            if isinstance(provider, Mapping):
                value = provider[service]
            else:
                value = provider(service)
            if (
                not isinstance(value, str)
                or not value
                or not value.isascii()
                or any(character.isspace() or ord(character) < 32 for character in value)
            ):
                raise ValueError("empty credential")
            return value
        except Exception:
            raise CredentialError("Unable to load paper API credentials") from None

    def _credentials(self) -> Tuple[str, str]:
        if self._credentials_cache is None:
            self._credentials_cache = (
                self._credential(KEYCHAIN_KEY_ID_SERVICE),
                self._credential(KEYCHAIN_SECRET_SERVICE),
            )
        return self._credentials_cache

    def _open(self, request: urllib.request.Request):
        if callable(self._opener) and not hasattr(self._opener, "open"):
            return self._opener(request, timeout=self._timeout)
        return self._opener.open(request, timeout=self._timeout)

    def _request(self, method: str, path: str, payload: Optional[Mapping[str, str]] = None):
        key_id, secret = self._credentials()
        body = None if payload is None else json.dumps(payload, separators=(",", ":")).encode("utf-8")
        request = urllib.request.Request(
            PAPER_API_URL + path,
            data=body,
            headers={
                "APCA-API-KEY-ID": key_id,
                "APCA-API-SECRET-KEY": secret,
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            method=method,
        )
        return self._open(request)

    @staticmethod
    def _decode(response: Any, allow_empty: bool = False) -> Any:
        try:
            raw = response.read()
            if allow_empty and raw in (b"", ""):
                return None
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            return json.loads(raw)
        except (UnicodeDecodeError, ValueError, TypeError):
            raise ResponseError("Alpaca returned malformed JSON") from None

    @staticmethod
    def _close(response: Any) -> None:
        if response is not None and hasattr(response, "close"):
            try:
                response.close()
            except Exception:
                # Closing must never mask the safe API error or successful result.
                pass

    def _json(self, method: str, path: str) -> Any:
        response = None
        try:
            response = self._request(method, path)
            status = getattr(response, "status", 200)
            if 300 <= status < 400:
                raise RedirectBlockedError("HTTP redirects are not permitted for paper API requests")
            if status >= 400:
                raise ApiError(status)
            return self._decode(response)
        except urllib.error.HTTPError as exc:
            raise ApiError(exc.code) from None
        except (socket.timeout, TimeoutError, urllib.error.URLError, http.client.HTTPException, OSError):
            raise PaperAdapterError("Alpaca paper API request failed") from None
        finally:
            self._close(response)

    def _mutation_json(
        self,
        method: str,
        path: str,
        payload: Optional[Mapping[str, str]] = None,
        allow_empty: bool = False,
    ) -> Any:
        response = None
        try:
            response = self._request(method, path, payload)
            status = getattr(response, "status", 200)
            if 300 <= status < 400:
                raise RedirectBlockedError("HTTP redirects are not permitted for paper API requests")
            if status >= 500:
                raise AmbiguousOrderError(
                    "Order result is ambiguous; reconcile using the SAME client_order_id before retrying"
                )
            if status >= 400:
                raise ApiError(status)
            try:
                return self._decode(response, allow_empty=allow_empty)
            except ResponseError:
                raise AmbiguousOrderError(
                    "Order result is ambiguous; reconcile using the SAME client_order_id before retrying"
                ) from None
        except AmbiguousOrderError:
            raise
        except urllib.error.HTTPError as exc:
            if exc.code >= 500:
                raise AmbiguousOrderError(
                    "Order result is ambiguous; reconcile using the SAME client_order_id before retrying"
                ) from None
            raise ApiError(exc.code) from None
        except (socket.timeout, TimeoutError, urllib.error.URLError, http.client.HTTPException, OSError):
            raise AmbiguousOrderError(
                "Order result is ambiguous; reconcile using the SAME client_order_id before retrying"
            ) from None
        finally:
            self._close(response)

    def preview_order(self, order: Any) -> Dict[str, Any]:
        if not isinstance(order, (OrderRequest, MultiLegOrderRequest)):
            order = OrderRequest.from_mapping(order)
        return order.payload()

    def get_account(self) -> Any:
        return self._json("GET", "/v2/account")

    account = get_account

    def get_positions(self) -> Any:
        return self._json("GET", "/v2/positions")

    positions = get_positions

    def get_order_by_client_id(self, client_order_id: str) -> Any:
        if not isinstance(client_order_id, str) or not _CLIENT_ID_RE.fullmatch(client_order_id):
            raise ValidationError("client_order_id must be 1-48 letters, digits, hyphens, or underscores")
        query = urllib.parse.urlencode({"client_order_id": client_order_id})
        return self._json("GET", "/v2/orders:by_client_order_id?" + query)

    def get_order_by_client_order_id(self, client_order_id: str) -> Any:
        """Compatibility spelling for the Alpaca client-order-id lookup."""
        return self.get_order_by_client_id(client_order_id)

    get_order = get_order_by_client_id

    def submit_order(self, order: Any, dry_run: bool = True) -> Any:
        if not isinstance(dry_run, bool):
            raise ValidationError("dry_run must be a boolean")
        request = order if isinstance(order, (OrderRequest, MultiLegOrderRequest)) else OrderRequest.from_mapping(order)
        payload = request.payload()
        if dry_run:
            return payload
        result = self._mutation_json("POST", "/v2/orders", payload)
        if not isinstance(result, dict) or not isinstance(result.get("id"), str) or not result["id"]:
            raise AmbiguousOrderError(
                "Order result is ambiguous; reconcile using the SAME client_order_id before retrying"
            )
        return result

    def cancel_order(self, order_id: str, dry_run: bool = True) -> Any:
        if not isinstance(dry_run, bool):
            raise ValidationError("dry_run must be a boolean")
        if not isinstance(order_id, str) or not order_id.strip():
            raise ValidationError("order_id is required")
        if dry_run:
            return {"order_id": order_id, "dry_run": True}
        path = "/v2/orders/" + urllib.parse.quote(order_id, safe="")
        result = self._mutation_json("DELETE", path, allow_empty=True)
        if result is not None and not isinstance(result, dict):
            raise AmbiguousOrderError(
                "Cancellation result is ambiguous; reconcile order state before retrying"
            )
        return result


def _status_view(account: Any) -> Dict[str, Any]:
    if not isinstance(account, dict):
        raise ResponseError("Alpaca returned an unexpected account response")
    allowed = {
        "status", "currency", "options_approved_level", "options_trading_level",
        "trading_blocked", "account_blocked", "trade_suspended_by_user",
    }
    return {key: account[key] for key in sorted(allowed) if key in account}


def _load_preview_order(inline: Optional[str], filename: Optional[str]) -> OrderRequest:
    try:
        if filename:
            with open(filename, "r") as handle:
                value = json.load(handle)
        elif inline:
            value = json.loads(inline)
        else:
            raise ValidationError("preview requires JSON or --file")
    except (OSError, ValueError):
        raise ValidationError("preview order JSON could not be read") from None
    return OrderRequest.from_mapping(value)


def main(argv: Optional[Any] = None) -> int:
    parser = argparse.ArgumentParser(description="Alpaca paper account and dry-run order tools")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("account", help="show safe account status fields")
    subparsers.add_parser("status", help="alias for account")
    preview = subparsers.add_parser("preview", help="validate and print an order payload offline")
    preview.add_argument("order_json", nargs="?", help="inline order JSON")
    preview.add_argument("--file", dest="filename", help="path to order JSON")
    args = parser.parse_args(argv)
    try:
        if args.command == "preview":
            order = _load_preview_order(args.order_json, args.filename)
            print(json.dumps(order.payload(), sort_keys=True))
            return 0
        if args.command in ("account", "status"):
            print(json.dumps(_status_view(PaperClient().get_account()), sort_keys=True))
            return 0
        parser.error("a command is required")
    except (PaperAdapterError, ValueError) as exc:
        print("error: " + str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
