from __future__ import annotations

import csv
from decimal import Decimal
from pathlib import Path

from src.reporting.income_report import summarize_income


def _write_transactions_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
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
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def test_summarize_income_tracks_subscription_like_debit_card_payment(tmp_path: Path) -> None:
    # Build one normalized account file in the same layout as production data.
    base = tmp_path / "normalized"
    transactions_path = base / "acct" / "transactions_normalized.csv"
    _write_transactions_csv(
        transactions_path,
        rows=[
            {
                "transaction_id": "t1",
                "account_name": "acct",
                "broker": "ii",
                "trade_date": "2026-04-10",
                "settlement_date": "2026-04-10",
                "symbol": "",
                "sedol": "",
                "quantity": "",
                "price": "",
                "description": "debit card payment",
                "reference": "",
                "debit": "100",
                "credit": "",
                "running_balance": "",
                "currency": "GBP",
                "source_file": "in.csv",
            },
            {
                "transaction_id": "t2",
                "account_name": "acct",
                "broker": "ii",
                "trade_date": "2026-04-11",
                "settlement_date": "2026-04-11",
                "symbol": "",
                "sedol": "",
                "quantity": "",
                "price": "",
                "description": "subscription",
                "reference": "",
                "debit": "40",
                "credit": "",
                "running_balance": "",
                "currency": "GBP",
                "source_file": "in.csv",
            },
            {
                "transaction_id": "t3",
                "account_name": "acct",
                "broker": "ii",
                "trade_date": "2026-04-12",
                "settlement_date": "2026-04-12",
                "symbol": "",
                "sedol": "",
                "quantity": "",
                "price": "",
                "description": "subscription",
                "reference": "",
                "debit": "",
                "credit": "10",
                "running_balance": "",
                "currency": "GBP",
                "source_file": "in.csv",
            },
        ],
    )

    summary = summarize_income(base)

    totals = {(row.month, row.description): row.total_amount for row in summary}
    assert totals[("2026-04", "debit card payment")] == Decimal("-100")
    assert totals[("2026-04", "subscription")] == Decimal("-30")
