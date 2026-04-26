from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from src.ingestion.ii import _normalize_transaction_description, parse_ii_transactions


def test_normalize_subscription_description() -> None:
    assert _normalize_transaction_description("SUBSCRIPTION") == "subscription"


def test_normalize_isa_subscription_with_tax_year_prefix() -> None:
    description = "2026/27 ISA Subscription re 3733842"
    assert _normalize_transaction_description(description) == "subscription"


def test_parse_ii_transactions_strips_repeated_bom_prefix(tmp_path: Path) -> None:
    csv_content = (
        "\ufeff\ufeff\ufeffDate,Settlement Date,Symbol,Sedol,Quantity,Price,Description,Reference,Debit,Credit,Running Balance\n"
        "13/02/2026,13/02/2026,n/a,n/a,n/a,n/a,GROSS INTEREST,n/a,n/a,£1.32,£1.32\n"
    )
    path = tmp_path / "transactions.csv"
    path.write_text(csv_content, encoding="utf-8")

    records = list(parse_ii_transactions(path, account_name="acct", broker="ii"))

    assert len(records) == 1
    assert records[0].description == "account interest"
    assert records[0].credit == Decimal("1.32")
