from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

from src.ingestion.ii import (
    _normalize_transaction_description,
    parse_ii_holdings,
    parse_ii_transactions,
)


def test_normalize_subscription_description() -> None:
    assert _normalize_transaction_description("SUBSCRIPTION") == "subscription"


def test_normalize_isa_subscription_with_tax_year_prefix() -> None:
    description = "2026/27 ISA Subscription re 3733842"
    assert _normalize_transaction_description(description) == "subscription"


def test_normalize_fee_transfer_description() -> None:
    assert _normalize_transaction_description("Fee Transfer") == "fees"


def test_normalize_s_date_trade_description() -> None:
    assert _normalize_transaction_description("1130 ISHS III 8.83 S Date 03/08/26") == "buy/sell"


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


def test_parse_ii_holdings_skips_pdf_totals_rows(monkeypatch, tmp_path: Path) -> None:
    rows = [
        {
            "Symbol": "SWDA",
            "Name": "iShares Core MSCI World ETF USD Acc GBP",
            "Qty": "150",
            "Price": "10,615.00p",
            "Market Value £": "£15,922.50",
            "Book Cost": "£16,093.72",
            "Gain/Loss": "£-171.22",
            "Gain/Loss %": "-1.06%",
            "Average Price": "10,729.1467p",
        },
        {
            "Symbol": "Totals",
            "Name": "",
            "Qty": "",
            "Price": "",
            "Market Value £": "£15,922.50",
            "Book Cost": "£16,093.72",
            "Gain/Loss": "£-171.22",
            "Gain/Loss %": "-1.06%",
            "Average Price": "",
        },
        {
            "Symbol": "GBP",
            "Name": "",
            "Qty": "",
            "Price": "",
            "Market Value £": "£15,922.50",
            "Book Cost": "£16,093.72",
            "Gain/Loss": "£-171.22",
            "Gain/Loss %": "-1.06%",
            "Average Price": "",
        },
    ]
    monkeypatch.setattr("src.ingestion.ii.read_dict_rows", lambda _: iter(rows))

    records = list(
        parse_ii_holdings(
            tmp_path / "holdings_trading_thiago_20260731.pdf",
            account_name="trading_thiago",
            broker="ii",
            valuation_date=date(2026, 7, 31),
        )
    )

    assert len(records) == 1
    assert records[0].symbol == "SWDA"
    assert records[0].market_value == Decimal("15922.50")
