from __future__ import annotations

import csv
from datetime import date
from pathlib import Path

from src.reporting.tax_report import (
    previous_uk_tax_year,
    summarize_tax_reports,
    write_tax_reports,
)


TRANSACTION_FIELDNAMES = [
    "transaction_id",
    "account_name",
    "broker",
    "trade_date",
    "settlement_date",
    "symbol",
    "sedol",
    "quantity",
    "price",
    "description",
    "reference",
    "debit",
    "credit",
    "running_balance",
    "currency",
    "source_file",
]


def _write_transactions_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=TRANSACTION_FIELDNAMES)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def test_previous_uk_tax_year_for_date_after_april_fifth() -> None:
    tax_year = previous_uk_tax_year(today=date(2026, 5, 17))
    assert tax_year.start_date == date(2025, 4, 6)
    assert tax_year.end_date == date(2026, 4, 5)


def test_previous_uk_tax_year_for_date_before_april_fifth() -> None:
    tax_year = previous_uk_tax_year(today=date(2026, 2, 1))
    assert tax_year.start_date == date(2024, 4, 6)
    assert tax_year.end_date == date(2025, 4, 5)


def test_summarize_tax_reports_by_owner_for_previous_tax_year(tmp_path: Path) -> None:
    transactions_root = tmp_path / "normalized"

    _write_transactions_csv(
        transactions_root / "trading_kirill" / "transactions_normalized.csv",
        rows=[
            {
                "transaction_id": "k_buy_pre_tax_year",
                "account_name": "trading_kirill",
                "broker": "ii",
                "trade_date": "2025-03-01",
                "settlement_date": "2025-03-01",
                "symbol": "AAA",
                "sedol": "",
                "quantity": "10",
                "price": "100",
                "description": "buy",
                "reference": "",
                "debit": "1000",
                "credit": "",
                "running_balance": "",
                "currency": "GBP",
                "source_file": "k.csv",
            },
            {
                "transaction_id": "k_sell_in_tax_year",
                "account_name": "trading_kirill",
                "broker": "ii",
                "trade_date": "2025-05-01",
                "settlement_date": "2025-05-01",
                "symbol": "AAA",
                "sedol": "",
                "quantity": "4",
                "price": "150",
                "description": "sell",
                "reference": "",
                "debit": "",
                "credit": "600",
                "running_balance": "",
                "currency": "GBP",
                "source_file": "k.csv",
            },
            {
                "transaction_id": "k_dividend_in_tax_year",
                "account_name": "trading_kirill",
                "broker": "ii",
                "trade_date": "2025-06-10",
                "settlement_date": "2025-06-10",
                "symbol": "AAA",
                "sedol": "",
                "quantity": "",
                "price": "",
                "description": "dividend",
                "reference": "",
                "debit": "",
                "credit": "40",
                "running_balance": "",
                "currency": "GBP",
                "source_file": "k.csv",
            },
            {
                "transaction_id": "k_interest_in_tax_year",
                "account_name": "trading_kirill",
                "broker": "ii",
                "trade_date": "2026-04-05",
                "settlement_date": "2026-04-05",
                "symbol": "",
                "sedol": "",
                "quantity": "",
                "price": "",
                "description": "account interest",
                "reference": "",
                "debit": "",
                "credit": "5",
                "running_balance": "",
                "currency": "GBP",
                "source_file": "k.csv",
            },
            {
                "transaction_id": "k_dividend_after_tax_year",
                "account_name": "trading_kirill",
                "broker": "ii",
                "trade_date": "2026-04-06",
                "settlement_date": "2026-04-06",
                "symbol": "AAA",
                "sedol": "",
                "quantity": "",
                "price": "",
                "description": "dividend",
                "reference": "",
                "debit": "",
                "credit": "999",
                "running_balance": "",
                "currency": "GBP",
                "source_file": "k.csv",
            },
        ],
    )
    _write_transactions_csv(
        transactions_root / "isa_kirill" / "transactions_normalized.csv",
        rows=[
            {
                "transaction_id": "k_isa_should_be_excluded",
                "account_name": "isa_kirill",
                "broker": "ii",
                "trade_date": "2025-07-01",
                "settlement_date": "2025-07-01",
                "symbol": "",
                "sedol": "",
                "quantity": "",
                "price": "",
                "description": "dividend",
                "reference": "",
                "debit": "",
                "credit": "999",
                "running_balance": "",
                "currency": "GBP",
                "source_file": "k_isa.csv",
            }
        ],
    )

    _write_transactions_csv(
        transactions_root / "trading_thiago" / "transactions_normalized.csv",
        rows=[
            {
                "transaction_id": "t_buy_pre_tax_year",
                "account_name": "trading_thiago",
                "broker": "ii",
                "trade_date": "2025-01-01",
                "settlement_date": "2025-01-01",
                "symbol": "BBB",
                "sedol": "",
                "quantity": "10",
                "price": "20",
                "description": "buy",
                "reference": "",
                "debit": "200",
                "credit": "",
                "running_balance": "",
                "currency": "GBP",
                "source_file": "t.csv",
            },
            {
                "transaction_id": "t_interest_before_tax_year",
                "account_name": "trading_thiago",
                "broker": "ii",
                "trade_date": "2025-04-05",
                "settlement_date": "2025-04-05",
                "symbol": "",
                "sedol": "",
                "quantity": "",
                "price": "",
                "description": "account interest",
                "reference": "",
                "debit": "",
                "credit": "77",
                "running_balance": "",
                "currency": "GBP",
                "source_file": "t.csv",
            },
            {
                "transaction_id": "t_dividend_start_tax_year",
                "account_name": "trading_thiago",
                "broker": "ii",
                "trade_date": "2025-04-06",
                "settlement_date": "2025-04-06",
                "symbol": "BBB",
                "sedol": "",
                "quantity": "",
                "price": "",
                "description": "dividend",
                "reference": "",
                "debit": "",
                "credit": "20",
                "running_balance": "",
                "currency": "GBP",
                "source_file": "t.csv",
            },
            {
                "transaction_id": "t_sell_in_tax_year",
                "account_name": "trading_thiago",
                "broker": "ii",
                "trade_date": "2026-01-15",
                "settlement_date": "2026-01-15",
                "symbol": "BBB",
                "sedol": "",
                "quantity": "5",
                "price": "30",
                "description": "sell",
                "reference": "",
                "debit": "",
                "credit": "150",
                "running_balance": "",
                "currency": "GBP",
                "source_file": "t.csv",
            },
        ],
    )

    summaries = summarize_tax_reports(
        transactions_root,
        owners=["kirill", "thiago"],
        today=date(2026, 5, 17),
    )

    kirill = summaries["kirill"]
    assert kirill.tax_year_start == date(2025, 4, 6)
    assert kirill.tax_year_end == date(2026, 4, 5)
    assert [event.event_type for event in kirill.events] == ["sell", "dividend", "account interest"]
    assert kirill.realized_gains_total == 200
    assert kirill.dividend_total == 40
    assert kirill.interest_total == 5
    assert kirill.reportable_total == 245

    thiago = summaries["thiago"]
    assert [event.event_type for event in thiago.events] == ["dividend", "sell"]
    assert thiago.realized_gains_total == 50
    assert thiago.dividend_total == 20
    assert thiago.interest_total == 0
    assert thiago.reportable_total == 70


def test_write_tax_reports_creates_owner_files_with_total_rows(tmp_path: Path) -> None:
    transactions_root = tmp_path / "normalized"
    _write_transactions_csv(
        transactions_root / "trading_kirill" / "transactions_normalized.csv",
        rows=[
            {
                "transaction_id": "k_buy",
                "account_name": "trading_kirill",
                "broker": "ii",
                "trade_date": "2025-01-01",
                "settlement_date": "2025-01-01",
                "symbol": "AAA",
                "sedol": "",
                "quantity": "1",
                "price": "10",
                "description": "buy",
                "reference": "",
                "debit": "10",
                "credit": "",
                "running_balance": "",
                "currency": "GBP",
                "source_file": "k.csv",
            },
            {
                "transaction_id": "k_sell",
                "account_name": "trading_kirill",
                "broker": "ii",
                "trade_date": "2025-06-01",
                "settlement_date": "2025-06-01",
                "symbol": "AAA",
                "sedol": "",
                "quantity": "1",
                "price": "11",
                "description": "sell",
                "reference": "",
                "debit": "",
                "credit": "11",
                "running_balance": "",
                "currency": "GBP",
                "source_file": "k.csv",
            },
        ],
    )
    _write_transactions_csv(
        transactions_root / "isa_kirill" / "transactions_normalized.csv",
        rows=[
            {
                "transaction_id": "k_isa_interest_excluded",
                "account_name": "isa_kirill",
                "broker": "ii",
                "trade_date": "2025-07-05",
                "settlement_date": "2025-07-05",
                "symbol": "",
                "sedol": "",
                "quantity": "",
                "price": "",
                "description": "account interest",
                "reference": "",
                "debit": "",
                "credit": "100",
                "running_balance": "",
                "currency": "GBP",
                "source_file": "k_isa.csv",
            }
        ],
    )
    _write_transactions_csv(
        transactions_root / "trading_thiago" / "transactions_normalized.csv",
        rows=[
            {
                "transaction_id": "t_interest",
                "account_name": "trading_thiago",
                "broker": "ii",
                "trade_date": "2025-07-01",
                "settlement_date": "2025-07-01",
                "symbol": "",
                "sedol": "",
                "quantity": "",
                "price": "",
                "description": "account interest",
                "reference": "",
                "debit": "",
                "credit": "2",
                "running_balance": "",
                "currency": "GBP",
                "source_file": "t.csv",
            },
        ],
    )

    summaries = summarize_tax_reports(
        transactions_root,
        owners=["kirill", "thiago"],
        today=date(2026, 5, 17),
    )
    output_root = tmp_path / "reports"
    written = write_tax_reports(summaries, output_root)

    assert sorted(path.name for path in written) == ["tax_report_kirill.csv", "tax_report_thiago.csv"]

    kirill_rows = list(csv.DictReader((output_root / "tax_report_kirill.csv").open("r", encoding="utf-8")))
    thiago_rows = list(csv.DictReader((output_root / "tax_report_thiago.csv").open("r", encoding="utf-8")))

    assert kirill_rows[-1]["row_type"] == "total"
    assert kirill_rows[-1]["reportable_total"] == "1.00"
    assert thiago_rows[-1]["row_type"] == "total"
    assert thiago_rows[-1]["interest_amount"] == "2.00"
