from __future__ import annotations

from src.ingestion.ii import _normalize_transaction_description


def test_normalize_subscription_description() -> None:
    assert _normalize_transaction_description("SUBSCRIPTION") == "subscription"


def test_normalize_isa_subscription_with_tax_year_prefix() -> None:
    description = "2026/27 ISA Subscription re 3733842"
    assert _normalize_transaction_description(description) == "subscription"
