"""Small helpers every service reaches for.

They live here so that a rule ("a station code is three upper-case letters")
is decided once, not once per service.
"""

import hashlib
import re
from datetime import UTC, datetime

_MONEY = re.compile(r"^\s*(-?)(\d+)(?:\.(\d{1,2}))?\s*$")
_EMAIL = re.compile(r"[^\s@]+@[^\s@]+\.[^\s@]+")
_PHONE = re.compile(r"\+?\d[\d\s-]{6,}\d")
_SPACES = re.compile(r"\s+")


def parse_money(text: str) -> int:
    """'12.34' -> 1234 minor units. Raises on anything that is not a plain decimal."""
    m = _MONEY.match(text)
    if not m:
        raise ValueError(f"not a money amount: {text!r}")
    sign, whole, frac = m.group(1), m.group(2), m.group(3) or ""
    minor = int(whole) * 100 + int(frac.ljust(2, "0"))
    return -minor if sign else minor


def format_money(minor: int, currency: str = "EUR") -> str:
    """1234, 'EUR' -> '12.34 EUR'."""
    sign = "-" if minor < 0 else ""
    minor = abs(int(round(minor)))
    return f"{sign}{minor // 100}.{minor % 100:02d} {currency}"


def normalize_station_code(code: str) -> str:
    """'  mad-01 ' -> 'MAD01': upper-case alphanumerics, nothing else."""
    return re.sub(r"[^A-Z0-9]", "", code.upper())


def stable_hash(*parts: str) -> str:
    """A short, stable digest of the given parts, for keys and fingerprints."""
    return hashlib.sha256("|".join(parts).encode()).hexdigest()[:16]


def hours_until(at: datetime, now: datetime | None = None) -> float:
    """Hours from now until at; negative when at is in the past."""
    now = now or datetime.now(UTC)
    if at.tzinfo is None:
        at = at.replace(tzinfo=UTC)
    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)
    return (at - now).total_seconds() / 3600


def backoff_delay_ms(attempt: int) -> int:
    """Exponential backoff with a 30 s ceiling: 200, 400, 800 ... ms."""
    return min(30_000, 200 * 2 ** max(0, attempt))


def redact_pii(text: str) -> str:
    """Mask e-mail addresses and phone numbers in free text before it is logged."""
    return _PHONE.sub("[phone]", _EMAIL.sub("[email]", text))


def normalize(text: str) -> str:
    """Trim and collapse whitespace. Several services keep a local normalize of their own; this is the shared one."""
    return _SPACES.sub(" ", text.strip())
