from datetime import UTC, datetime

import pytest

from tt_lib.util import (
    backoff_delay_ms,
    format_money,
    hours_until,
    normalize,
    normalize_station_code,
    parse_money,
    redact_pii,
    stable_hash,
)


def test_money() -> None:
    assert parse_money("12.34") == 1234
    assert parse_money("7") == 700
    assert parse_money("-0.5") == -50
    with pytest.raises(ValueError):
        parse_money("12,34")
    assert format_money(1234) == "12.34 EUR"
    assert format_money(-5, "GBP") == "-0.05 GBP"


def test_station_hash_hours() -> None:
    assert normalize_station_code(" mad-01 ") == "MAD01"
    assert stable_hash("a", "b") == stable_hash("a", "b") != stable_hash("ab")
    assert len(stable_hash("x")) == 16
    now = datetime(2026, 1, 1, tzinfo=UTC)
    assert hours_until(datetime(2026, 1, 1, 6, tzinfo=UTC), now) == 6


def test_backoff_redact_normalize() -> None:
    assert [backoff_delay_ms(i) for i in range(4)] == [200, 400, 800, 1600]
    assert backoff_delay_ms(20) == 30_000
    assert redact_pii("mail ana@example.com or +34 600 123 456") == "mail [email] or [phone]"
    assert normalize("  a   b \n c ") == "a b c"
