from __future__ import annotations

import csv
from decimal import Decimal
from pathlib import Path

from src.reporting.historical_performance_report import (
    summarize_historical_performance,
    write_historical_performance_report,
    write_historical_performance_reports,
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

HOLDING_FIELDNAMES = [
    "snapshot_id",
    "account_name",
    "broker",
    "valuation_date",
    "symbol",
    "name",
    "quantity",
    "price",
    "average_price",
    "market_value",
    "book_cost",
    "gain_loss",
    "gain_loss_pct",
    "currency",
    "source_file",
]


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_sample_normalized_data(base: Path) -> None:
    _write_csv(
        base / "acct_a" / "transactions_normalized.csv",
        TRANSACTION_FIELDNAMES,
        rows=[
            {
                "transaction_id": "a_sub_1",
                "account_name": "acct_a",
                "broker": "ii",
                "trade_date": "2026-01-02",
                "settlement_date": "2026-01-02",
                "symbol": "",
                "sedol": "",
                "quantity": "",
                "price": "",
                "description": "subscription",
                "reference": "",
                "debit": "200",
                "credit": "",
                "running_balance": "",
                "currency": "GBP",
                "source_file": "in.csv",
            },
            {
                "transaction_id": "a_buy_aaa",
                "account_name": "acct_a",
                "broker": "ii",
                "trade_date": "2026-01-05",
                "settlement_date": "2026-01-05",
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
                "source_file": "in.csv",
            },
            {
                "transaction_id": "a_buy_bbb",
                "account_name": "acct_a",
                "broker": "ii",
                "trade_date": "2026-01-07",
                "settlement_date": "2026-01-07",
                "symbol": "BBB",
                "sedol": "",
                "quantity": "2",
                "price": "250",
                "description": "buy",
                "reference": "",
                "debit": "500",
                "credit": "",
                "running_balance": "",
                "currency": "GBP",
                "source_file": "in.csv",
            },
            {
                "transaction_id": "a_dividend",
                "account_name": "acct_a",
                "broker": "ii",
                "trade_date": "2026-02-01",
                "settlement_date": "2026-02-01",
                "symbol": "",
                "sedol": "",
                "quantity": "",
                "price": "",
                "description": "dividend",
                "reference": "",
                "debit": "",
                "credit": "50",
                "running_balance": "",
                "currency": "GBP",
                "source_file": "in.csv",
            },
            {
                "transaction_id": "a_fees",
                "account_name": "acct_a",
                "broker": "ii",
                "trade_date": "2026-02-15",
                "settlement_date": "2026-02-15",
                "symbol": "",
                "sedol": "",
                "quantity": "",
                "price": "",
                "description": "fees",
                "reference": "",
                "debit": "5",
                "credit": "",
                "running_balance": "",
                "currency": "GBP",
                "source_file": "in.csv",
            },
            {
                "transaction_id": "a_interest",
                "account_name": "acct_a",
                "broker": "ii",
                "trade_date": "2026-03-01",
                "settlement_date": "2026-03-01",
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
                "source_file": "in.csv",
            },
            {
                "transaction_id": "a_debit_card",
                "account_name": "acct_a",
                "broker": "ii",
                "trade_date": "2026-03-15",
                "settlement_date": "2026-03-15",
                "symbol": "",
                "sedol": "",
                "quantity": "",
                "price": "",
                "description": "debit card payment",
                "reference": "",
                "debit": "50",
                "credit": "",
                "running_balance": "",
                "currency": "GBP",
                "source_file": "in.csv",
            },
        ],
    )
    _write_csv(
        base / "acct_b" / "transactions_normalized.csv",
        TRANSACTION_FIELDNAMES,
        rows=[
            {
                "transaction_id": "b_sub_1",
                "account_name": "acct_b",
                "broker": "ii",
                "trade_date": "2026-01-03",
                "settlement_date": "2026-01-03",
                "symbol": "",
                "sedol": "",
                "quantity": "",
                "price": "",
                "description": "subscription",
                "reference": "",
                "debit": "300",
                "credit": "",
                "running_balance": "",
                "currency": "GBP",
                "source_file": "in.csv",
            },
            {
                "transaction_id": "b_buy_ccc",
                "account_name": "acct_b",
                "broker": "ii",
                "trade_date": "2026-01-10",
                "settlement_date": "2026-01-10",
                "symbol": "CCC",
                "sedol": "",
                "quantity": "5",
                "price": "200",
                "description": "buy",
                "reference": "",
                "debit": "1000",
                "credit": "",
                "running_balance": "",
                "currency": "GBP",
                "source_file": "in.csv",
            },
            {
                "transaction_id": "b_dividend",
                "account_name": "acct_b",
                "broker": "ii",
                "trade_date": "2026-02-10",
                "settlement_date": "2026-02-10",
                "symbol": "",
                "sedol": "",
                "quantity": "",
                "price": "",
                "description": "dividend",
                "reference": "",
                "debit": "",
                "credit": "10",
                "running_balance": "",
                "currency": "GBP",
                "source_file": "in.csv",
            },
            {
                "transaction_id": "b_fees",
                "account_name": "acct_b",
                "broker": "ii",
                "trade_date": "2026-02-12",
                "settlement_date": "2026-02-12",
                "symbol": "",
                "sedol": "",
                "quantity": "",
                "price": "",
                "description": "fees",
                "reference": "",
                "debit": "1",
                "credit": "",
                "running_balance": "",
                "currency": "GBP",
                "source_file": "in.csv",
            },
        ],
    )

    _write_csv(
        base / "acct_a" / "holdings_2026-02-28_normalized.csv",
        HOLDING_FIELDNAMES,
        rows=[
            {
                "snapshot_id": "a_0228_aaa",
                "account_name": "acct_a",
                "broker": "ii",
                "valuation_date": "2026-02-28",
                "symbol": "AAA",
                "name": "AAA plc",
                "quantity": "10",
                "price": "110",
                "average_price": "",
                "market_value": "1100",
                "book_cost": "",
                "gain_loss": "",
                "gain_loss_pct": "",
                "currency": "GBP",
                "source_file": "holdings.csv",
            },
            {
                "snapshot_id": "a_0228_bbb",
                "account_name": "acct_a",
                "broker": "ii",
                "valuation_date": "2026-02-28",
                "symbol": "BBB",
                "name": "BBB plc",
                "quantity": "2",
                "price": "275",
                "average_price": "",
                "market_value": "550",
                "book_cost": "",
                "gain_loss": "",
                "gain_loss_pct": "",
                "currency": "GBP",
                "source_file": "holdings.csv",
            },
        ],
    )
    _write_csv(
        base / "acct_a" / "holdings_2026-03-31_normalized.csv",
        HOLDING_FIELDNAMES,
        rows=[
            {
                "snapshot_id": "a_0331_aaa",
                "account_name": "acct_a",
                "broker": "ii",
                "valuation_date": "2026-03-31",
                "symbol": "AAA",
                "name": "AAA plc",
                "quantity": "10",
                "price": "90",
                "average_price": "",
                "market_value": "900",
                "book_cost": "",
                "gain_loss": "",
                "gain_loss_pct": "",
                "currency": "GBP",
                "source_file": "holdings.csv",
            },
            {
                "snapshot_id": "a_0331_bbb",
                "account_name": "acct_a",
                "broker": "ii",
                "valuation_date": "2026-03-31",
                "symbol": "BBB",
                "name": "BBB plc",
                "quantity": "2",
                "price": "240",
                "average_price": "",
                "market_value": "480",
                "book_cost": "",
                "gain_loss": "",
                "gain_loss_pct": "",
                "currency": "GBP",
                "source_file": "holdings.csv",
            },
        ],
    )
    _write_csv(
        base / "acct_b" / "holdings_2026-02-28_normalized.csv",
        HOLDING_FIELDNAMES,
        rows=[
            {
                "snapshot_id": "b_0228_ccc",
                "account_name": "acct_b",
                "broker": "ii",
                "valuation_date": "2026-02-28",
                "symbol": "CCC",
                "name": "CCC plc",
                "quantity": "5",
                "price": "210",
                "average_price": "",
                "market_value": "1050",
                "book_cost": "",
                "gain_loss": "",
                "gain_loss_pct": "",
                "currency": "GBP",
                "source_file": "holdings.csv",
            }
        ],
    )


def test_summarize_historical_performance_combines_unrealized_distributed_and_nominal_invested(
    tmp_path: Path,
) -> None:
    base = tmp_path / "normalized"
    _write_sample_normalized_data(base)

    rows = summarize_historical_performance(base, base)
    by_key = {(row.account_name, row.valuation_date.isoformat()): row for row in rows}

    acct_a_feb = by_key[("acct_a", "2026-02-28")]
    assert acct_a_feb.realized_pnl == Decimal("0.00")
    assert acct_a_feb.unrealized_gain == Decimal("150.00")
    assert acct_a_feb.distributed_cashflow == Decimal("50.00")
    assert acct_a_feb.fees == Decimal("-5.00")
    assert acct_a_feb.distributed_net_of_fees == Decimal("45.00")
    assert acct_a_feb.nominal_invested == Decimal("1500.00")
    assert acct_a_feb.total_pnl == Decimal("195.00")

    acct_a_mar = by_key[("acct_a", "2026-03-31")]
    assert acct_a_mar.realized_pnl == Decimal("0.00")
    assert acct_a_mar.unrealized_gain == Decimal("-120.00")
    assert acct_a_mar.distributed_cashflow == Decimal("52.00")
    assert acct_a_mar.fees == Decimal("-5.00")
    assert acct_a_mar.distributed_net_of_fees == Decimal("47.00")
    assert acct_a_mar.nominal_invested == Decimal("1500.00")
    assert acct_a_mar.total_pnl == Decimal("-73.00")

    acct_b_feb = by_key[("acct_b", "2026-02-28")]
    assert acct_b_feb.realized_pnl == Decimal("0.00")
    assert acct_b_feb.unrealized_gain == Decimal("50.00")
    assert acct_b_feb.distributed_cashflow == Decimal("10.00")
    assert acct_b_feb.fees == Decimal("-1.00")
    assert acct_b_feb.distributed_net_of_fees == Decimal("9.00")
    assert acct_b_feb.nominal_invested == Decimal("1000.00")
    assert acct_b_feb.total_pnl == Decimal("59.00")


def test_write_historical_performance_report_uses_unprefixed_columns(tmp_path: Path) -> None:
    base = tmp_path / "normalized"
    _write_sample_normalized_data(base)
    rows = summarize_historical_performance(base, base)

    account_rows = [row for row in rows if row.account_name == "acct_a"]
    output_path = tmp_path / "acct_a_report.csv"
    write_historical_performance_report(account_rows, output_path)

    with output_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        csv_rows = list(reader)

    assert reader.fieldnames == [
        "valuation_date",
        "total_pnl",
        "realized_pnl",
        "unrealized_gain",
        "distributed_cashflow",
        "fees",
        "distributed_net_of_fees",
        "nominal_invested",
        "performance_pct",
    ]
    assert csv_rows[0]["valuation_date"] == "2026-02-28"
    assert csv_rows[0]["total_pnl"] == "195.00"
    assert csv_rows[0]["realized_pnl"] == "0.00"
    assert csv_rows[0]["nominal_invested"] == "1500.00"
    assert csv_rows[0]["performance_pct"] == "13.00"


def test_write_historical_performance_reports_outputs_per_account_and_aggregated(tmp_path: Path) -> None:
    base = tmp_path / "normalized"
    _write_sample_normalized_data(base)
    rows = summarize_historical_performance(base, base)

    output_base = tmp_path / "historical.csv"
    written = write_historical_performance_reports(rows, output_base)

    written_names = sorted(path.name for path in written)
    assert written_names == [
        "historical_acct_a.csv",
        "historical_acct_b.csv",
        "historical_aggregated.csv",
    ]

    aggregated_path = tmp_path / "historical_aggregated.csv"
    with aggregated_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        csv_rows = list(reader)

    assert csv_rows[0]["valuation_date"] == "2026-02-28"
    assert csv_rows[0]["total_pnl"] == "254.00"
    assert csv_rows[0]["realized_pnl"] == "0.00"
    assert csv_rows[0]["unrealized_gain"] == "200.00"
    assert csv_rows[0]["distributed_cashflow"] == "60.00"
    assert csv_rows[0]["fees"] == "-6.00"
    assert csv_rows[0]["distributed_net_of_fees"] == "54.00"
    assert csv_rows[0]["nominal_invested"] == "2500.00"
    assert csv_rows[0]["performance_pct"] == "10.16"

    assert csv_rows[1]["valuation_date"] == "2026-03-31"
    assert csv_rows[1]["total_pnl"] == "-14.00"
    assert csv_rows[1]["realized_pnl"] == "0.00"
    assert csv_rows[1]["unrealized_gain"] == "-70.00"
    assert csv_rows[1]["distributed_cashflow"] == "62.00"
    assert csv_rows[1]["fees"] == "-6.00"
    assert csv_rows[1]["distributed_net_of_fees"] == "56.00"
    assert csv_rows[1]["nominal_invested"] == "2500.00"
    assert csv_rows[1]["performance_pct"] == "-0.56"


def test_nominal_invested_uses_snapshot_book_cost(tmp_path: Path) -> None:
    base = tmp_path / "normalized"
    _write_csv(
        base / "acct_credit" / "transactions_normalized.csv",
        TRANSACTION_FIELDNAMES,
        rows=[
            {
                "transaction_id": "credit_sub_1",
                "account_name": "acct_credit",
                "broker": "ii",
                "trade_date": "2026-04-01",
                "settlement_date": "2026-04-01",
                "symbol": "",
                "sedol": "",
                "quantity": "",
                "price": "",
                "description": "subscription",
                "reference": "",
                "debit": "",
                "credit": "1000",
                "running_balance": "",
                "currency": "GBP",
                "source_file": "in.csv",
            },
            {
                "transaction_id": "credit_buy_1",
                "account_name": "acct_credit",
                "broker": "ii",
                "trade_date": "2026-04-05",
                "settlement_date": "2026-04-05",
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
                "source_file": "in.csv",
            },
        ],
    )
    _write_csv(
        base / "acct_credit" / "holdings_2026-04-24_normalized.csv",
        HOLDING_FIELDNAMES,
        rows=[
            {
                "snapshot_id": "credit_h_1",
                "account_name": "acct_credit",
                "broker": "ii",
                "valuation_date": "2026-04-24",
                "symbol": "AAA",
                "name": "AAA plc",
                "quantity": "1",
                "price": "10",
                "average_price": "",
                "market_value": "10",
                "book_cost": "",
                "gain_loss": "",
                "gain_loss_pct": "",
                "currency": "GBP",
                "source_file": "holdings.csv",
            }
        ],
    )

    rows = summarize_historical_performance(base, base)
    assert len(rows) == 1
    assert rows[0].nominal_invested == Decimal("10.00")


def test_realized_pnl_is_included_in_total_pnl(tmp_path: Path) -> None:
    base = tmp_path / "normalized"
    _write_csv(
        base / "acct_realized" / "transactions_normalized.csv",
        TRANSACTION_FIELDNAMES,
        rows=[
            {
                "transaction_id": "r_buy",
                "account_name": "acct_realized",
                "broker": "ii",
                "trade_date": "2026-01-01",
                "settlement_date": "2026-01-01",
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
                "source_file": "in.csv",
            },
            {
                "transaction_id": "r_sell",
                "account_name": "acct_realized",
                "broker": "ii",
                "trade_date": "2026-01-10",
                "settlement_date": "2026-01-10",
                "symbol": "AAA",
                "sedol": "",
                "quantity": "5",
                "price": "120",
                "description": "sell",
                "reference": "",
                "debit": "",
                "credit": "600",
                "running_balance": "",
                "currency": "GBP",
                "source_file": "in.csv",
            },
        ],
    )
    _write_csv(
        base / "acct_realized" / "holdings_2026-01-31_normalized.csv",
        HOLDING_FIELDNAMES,
        rows=[
            {
                "snapshot_id": "r_h_1",
                "account_name": "acct_realized",
                "broker": "ii",
                "valuation_date": "2026-01-31",
                "symbol": "AAA",
                "name": "AAA plc",
                "quantity": "5",
                "price": "130",
                "average_price": "",
                "market_value": "650",
                "book_cost": "",
                "gain_loss": "",
                "gain_loss_pct": "",
                "currency": "GBP",
                "source_file": "holdings.csv",
            }
        ],
    )

    rows = summarize_historical_performance(base, base)
    assert len(rows) == 1
    row = rows[0]
    assert row.realized_pnl == Decimal("100.00")
    assert row.unrealized_gain == Decimal("150.00")
    assert row.total_pnl == Decimal("250.00")
