"""Bounded X evidence for Solana token attention. Read only; never posts."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
import re
import subprocess
import urllib.error
import urllib.parse
import urllib.request

from .jupiter_quotes import NoRedirect, QuoteError, validate_mint

ENDPOINT = "https://api.x.com/2/tweets/search/recent"
RUG_TERMS = ("rug", "honeypot", "scam", "cannot sell", "freeze authority", "mint authority")
NARRATIVES = {
    "ai": (" ai ", "agent", "model"),
    "meme": ("meme", "community", "viral"),
    "defi": ("defi", "yield", "liquidity"),
    "gaming": ("game", "gaming"),
}


class SocialError(ValueError):
    pass


def load_bearer() -> str:
    value = os.environ.get("NEOROME_X_BEARER_TOKEN") or os.environ.get("X_BEARER_TOKEN")
    if value is None:
        try:
            result = subprocess.run(
                ["/usr/bin/security", "find-generic-password", "-s", "neorome.x.bearer", "-w"],
                capture_output=True, timeout=30, check=False,
            )
        except (OSError, subprocess.SubprocessError):
            raise SocialError("X credential lookup failed") from None
        if result.returncode:
            raise SocialError("X credential unavailable")
        try:
            value = result.stdout.decode().rstrip("\r\n")
        except UnicodeError:
            raise SocialError("X credential encoding invalid") from None
    if not value or not re.fullmatch(r"[\x21-\x7e]+", value):
        raise SocialError("X credential format invalid")
    return value


def _iso(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError
    return parsed.astimezone(timezone.utc)


class XClient:
    def __init__(self, credential_loader=load_bearer, opener=None, clock=lambda: datetime.now(timezone.utc)):
        self.credential_loader = credential_loader
        self.opener = opener or urllib.request.build_opener(NoRedirect)
        self.clock = clock

    def search_contract(self, mint: str, max_results: int = 100) -> dict:
        validate_mint(mint)
        if type(max_results) is not int or not 10 <= max_results <= 100:
            raise SocialError("X result bound must be 10 through 100")
        params = {
            "query": f'"{mint}" -is:retweet', "max_results": str(max_results),
            "tweet.fields": "created_at,public_metrics,author_id", "expansions": "author_id",
            "user.fields": "username,created_at,public_metrics,verified",
        }
        requested = self.clock()
        request = urllib.request.Request(
            ENDPOINT + "?" + urllib.parse.urlencode(params),
            headers={"Authorization": "Bearer " + self.credential_loader(), "Accept": "application/json",
                     "User-Agent": "NeoRome-Token-Social/1.0"}, method="GET",
        )
        try:
            with self.opener.open(request, timeout=20) as response:
                if response.status != 200:
                    raise SocialError("X search rejected")
                body = response.read(2_000_001)
        except urllib.error.HTTPError as exc:
            status = exc.code
            try: exc.close()
            except Exception: pass
            raise SocialError(f"X search HTTP {status}") from None
        except (SocialError, QuoteError):
            raise
        except Exception:
            raise SocialError("X search transport failed") from None
        if len(body) > 2_000_000:
            raise SocialError("X response too large")
        try:
            payload = json.loads(body)
        except (ValueError, UnicodeError):
            raise SocialError("X returned malformed JSON") from None
        received = self.clock()
        return normalize(payload, mint, requested, received, hashlib.sha256(body).hexdigest())


def normalize(payload, mint, requested, received, source_hash):
    if not isinstance(payload, dict) or not isinstance(payload.get("data", []), list):
        raise SocialError("X response schema invalid")
    users = {str(x.get("id")): x for x in payload.get("includes", {}).get("users", []) if isinstance(x, dict)}
    posts = []
    for raw in payload.get("data", []):
        if not isinstance(raw, dict) or mint not in str(raw.get("text", "")):
            continue
        try:
            created = _iso(raw["created_at"])
            author = users[str(raw["author_id"])]
            author_created = _iso(author["created_at"])
            metrics = raw.get("public_metrics", {})
            followers = int(author.get("public_metrics", {}).get("followers_count", 0))
            engagement = sum(int(metrics.get(k, 0)) for k in ("like_count", "reply_count", "quote_count", "retweet_count"))
        except (KeyError, TypeError, ValueError, OverflowError):
            raise SocialError("X evidence fields incomplete") from None
        posts.append({"id": str(raw.get("id")), "created_at": created.isoformat(),
                      "author_id": str(raw["author_id"]), "author_username": str(author.get("username", "")),
                      "author_created_at": author_created.isoformat(), "followers": followers,
                      "verified": bool(author.get("verified", False)), "engagement": engagement,
                      "text": str(raw.get("text", ""))[:1000]})
    posts.sort(key=lambda x: x["created_at"])
    now = received.astimezone(timezone.utc)
    credible = {p["author_id"] for p in posts if p["followers"] >= 100 and (now - _iso(p["author_created_at"])).days >= 90}
    bots = [p for p in posts if p["followers"] < 10 or (now - _iso(p["author_created_at"])).days < 30]
    midpoint = requested - (requested - min((_iso(p["created_at"]) for p in posts), default=requested)) / 2
    early = sum(_iso(p["created_at"]) < midpoint for p in posts)
    late = len(posts) - early
    warning_posts = [p["id"] for p in posts if any(term in (" " + p["text"].lower() + " ") for term in RUG_TERMS)]
    narrative = {name: sum(any(term in (" " + p["text"].lower() + " ") for term in terms) for p in posts)
                 for name, terms in NARRATIVES.items()}
    hours = max(Decimal("0.0167"), Decimal(str((requested - min((_iso(p["created_at"]) for p in posts), default=requested)).total_seconds())) / Decimal(3600))
    return {"schema_version": 1, "provider": "x", "source_kind": "observed", "query_kind": "contract_address",
            "mint": mint, "requested_at": requested.isoformat(), "received_at": received.isoformat(),
            "response_sha256": source_hash, "posts": posts,
            "signals": {"mentions": len(posts), "mention_velocity_per_hour": str(Decimal(len(posts)) / hours),
                        "unique_authors": len({p['author_id'] for p in posts}),
                        "unique_credible_authors": len(credible), "credible_author_ids": sorted(credible),
                        "propagation_acceleration_mentions": late - early,
                        "total_engagement": sum(p["engagement"] for p in posts),
                        "bot_post_fraction": str(Decimal(len(bots)) / Decimal(len(posts))) if posts else None,
                        "rug_warning_post_ids": warning_posts, "narrative_clusters": narrative,
                        "known_influencer_correlation": None,
                        "known_influencer_gap": "no predeclared verified influencer allowlist"},
            "complete": True}


def eligible(evidence: dict) -> tuple[bool, list[str]]:
    signals = evidence.get("signals", {})
    reasons = []
    if signals.get("mentions", 0) < 3: reasons.append("x_contract_mentions_below_3")
    if signals.get("unique_credible_authors", 0) < 2: reasons.append("x_credible_authors_below_2")
    if signals.get("rug_warning_post_ids"): reasons.append("x_rug_warning_present")
    bot = signals.get("bot_post_fraction")
    if bot is None or Decimal(str(bot)) > Decimal("0.50"): reasons.append("x_bot_concentration_too_high_or_missing")
    return not reasons, reasons


__all__ = ["XClient", "SocialError", "eligible", "normalize", "load_bearer"]
