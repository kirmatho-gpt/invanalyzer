from __future__ import annotations

import csv
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from scripts.normalize_holdings import normalize_holdings
from src.normalization.holdings import HOLDING_FIELDNAMES
from src.reporting.historical_performance_report import summarize_historical_performance
from src.reporting.unrealized_gain_report import (
    UNREALIZED_GAIN_FIELDNAMES,
    collect_holdings_snapshot_dates,
    summarize_unrealized_gains,
    write_combined_unrealized_gain_report,
    write_unrealized_gain_reports,
)


HOLDINGS_HEADER = (
    "snapshot_id,account_name,broker,valuation_date,symbol,name,quantity,price,"
    "average_price,market_value,book_cost,gain_loss,gain_loss_pct,currency,source_file\n"
)
TRANSACTIONS_HEADER = (
    "transaction_id,account_name,broker,trade_date,settlement_date,symbol,sedol,"
    "quantity,price,description,reference,debit,credit,running_balance,currency,source_file\n"
)


def _write_trading_kirill_fixture(root: Path) -> None:
    account_root = root / "trading_kirill"
    account_root.mkdir(parents=True, exist_ok=True)
    (account_root / "transactions_normalized.csv").write_text(
        TRANSACTIONS_HEADER
        + "t1,trading_kirill,ii,2026-02-01,,B2PLJM6,,445.051,,buy,,19998.50,,,GBP,transactions.csv\n",
        encoding="utf-8",
    )
    (account_root / "holdings_2026-02-27_normalized.csv").write_text(
        HOLDINGS_HEADER
        + "h1,trading_kirill,ii,2026-02-27,B2PLJM6,Artemis SmartGARP UK Eq I Acc GBP,445.051,47.0966,,20960.39,,,,GBP,holdings_old.csv\n",
        encoding="utf-8",
    )
    (account_root / "holdings_2026-05-27_normalized.csv").write_text(
        HOLDINGS_HEADER,
        encoding="utf-8",
    )


def test_normalize_holdings_writes_header_only_file_for_empty_snapshot(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    output_root = tmp_path / "normalized"
    config_path = tmp_path / "accounts.json"
    raw_root.mkdir(parents=True, exist_ok=True)
    config_path.write_text('{"accounts": {"trading_kirill": "ii"}}', encoding="utf-8")
    (raw_root / "holdings_trading_kirill_20260527.csv").write_text(
        "Symbol,Name,Qty,Price,Market Value £,Book Cost,Gain/Loss,Gain/Loss %,Average Price\n",
        encoding="utf-8",
    )

    normalize_holdings(raw_root, output_root, config_path)

    output_path = output_root / "trading_kirill" / "holdings_2026-05-27_normalized.csv"
    assert output_path.exists()
    assert output_path.read_text(encoding="utf-8").splitlines() == [
        ",".join(HOLDING_FIELDNAMES),
    ]


def test_combined_latest_report_omits_rows_when_latest_snapshot_is_empty(tmp_path: Path) -> None:
    _write_trading_kirill_fixture(tmp_path)
    rows = summarize_unrealized_gains(tmp_path, tmp_path)
    snapshot_dates = collect_holdings_snapshot_dates(tmp_path)
    output_path = tmp_path / "unrealized_gains_report.csv"

    write_combined_unrealized_gain_report(
        rows,
        output_path,
        latest_only=True,
        snapshot_dates_by_account=snapshot_dates,
    )

    assert snapshot_dates == {
        "trading_kirill": {date(2026, 2, 27), date(2026, 5, 27)},
    }
    assert output_path.read_text(encoding="utf-8").splitlines() == [
        ",".join(UNREALIZED_GAIN_FIELDNAMES),
    ]


def test_summarize_unrealized_gains_handles_missing_cost_basis(tmp_path: Path) -> None:
    account_root = tmp_path / "isa_kirill"
    account_root.mkdir(parents=True, exist_ok=True)
    (account_root / "holdings_2026-07-31_normalized.csv").write_text(
        HOLDINGS_HEADER
        + "h1,isa_kirill,ii,2026-07-31,B7VNK95,Fund,394.78,10,,3947.80,,,,GBP,holdings.pdf\n",
        encoding="utf-8",
    )

    rows = summarize_unrealized_gains(tmp_path, tmp_path)

    assert len(rows) == 1
    assert rows[0].book_cost == Decimal("0.00")
    assert rows[0].unrealized_gain == Decimal("3947.80")
    assert rows[0].unrealized_gain_pct is None


def test_weekend_snapshots_keep_separate_filename_dates(tmp_path: Path) -> None:
    account_root = tmp_path / "isa_kirill"
    account_root.mkdir(parents=True, exist_ok=True)
    (account_root / "holdings_2026-09-25_normalized.csv").write_text(
        HOLDINGS_HEADER
        + "h1,isa_kirill,ii,2026-09-25,AAA,Fund,1,110,,110,,,,GBP,holdings.pdf\n",
        encoding="utf-8",
    )
    (account_root / "holdings_2026-09-27_normalized.csv").write_text(
        HOLDINGS_HEADER
        + "h1,isa_kirill,ii,2026-09-25,AAA,Fund,1,125,,125,,,,GBP,holdings.pdf\n"
        + "h2,isa_kirill,ii,,BBB,Fund,1,50,,50,,,,GBP,holdings.pdf\n",
        encoding="utf-8",
    )

    snapshot_dates = collect_holdings_snapshot_dates(tmp_path)
    rows = summarize_unrealized_gains(tmp_path, tmp_path)
    output_root = tmp_path / "reports" / "unrealized"

    write_unrealized_gain_reports(
        rows,
        output_root,
        snapshot_dates_by_account=snapshot_dates,
    )

    assert snapshot_dates == {"isa_kirill": {date(2026, 9, 25), date(2026, 9, 27)}}
    assert [row.valuation_date for row in rows] == [
        date(2026, 9, 25),
        date(2026, 9, 27),
        date(2026, 9, 27),
    ]
    assert [row.market_value for row in rows] == [
        Decimal("110.00"), Decimal("125.00"), Decimal("50.00"),
    ]
    assert (output_root / "isa_kirill" / "unrealized_gains_2026-09-25.csv").exists()
    assert (output_root / "isa_kirill" / "unrealized_gains_2026-09-27.csv").exists()

    # Latest selection must also work when callers omit the snapshot inventory.
    combined_path = tmp_path / "combined.csv"
    write_combined_unrealized_gain_report(rows, combined_path)
    with combined_path.open(encoding="utf-8", newline="") as handle:
        combined_rows = list(csv.DictReader(handle))
    assert len(combined_rows) == 2
    assert {row["valuation_date"] for row in combined_rows} == {"2026-09-27"}


def test_weekend_snapshot_date_is_not_overridden_when_row_date_is_not_previous_friday(
    tmp_path: Path,
) -> None:
    account_root = tmp_path / "isa_kirill"
    account_root.mkdir(parents=True, exist_ok=True)
    (account_root / "holdings_2026-09-27_normalized.csv").write_text(
        HOLDINGS_HEADER
        + "h1,isa_kirill,ii,2026-09-24,AAA,Fund,1,125,,125,,,,GBP,holdings.pdf\n",
        encoding="utf-8",
    )

    snapshot_dates = collect_holdings_snapshot_dates(tmp_path)

    assert snapshot_dates == {"isa_kirill": {date(2026, 9, 27)}}


def test_header_only_weekend_snapshot_keeps_filename_report_date(tmp_path: Path) -> None:
    account_root = tmp_path / "isa_kirill"
    account_root.mkdir(parents=True, exist_ok=True)
    (account_root / "holdings_2026-09-25_normalized.csv").write_text(
        HOLDINGS_HEADER
        + "h1,isa_kirill,ii,2026-09-25,AAA,Fund,1,125,,125,,,,GBP,holdings.pdf\n",
        encoding="utf-8",
    )
    (account_root / "holdings_2026-09-27_normalized.csv").write_text(
        HOLDINGS_HEADER,
        encoding="utf-8",
    )

    snapshot_dates = collect_holdings_snapshot_dates(tmp_path)
    rows = summarize_unrealized_gains(tmp_path, tmp_path)

    assert snapshot_dates == {"isa_kirill": {date(2026, 9, 25), date(2026, 9, 27)}}
    assert [row.valuation_date for row in rows] == [date(2026, 9, 25)]


def test_weekend_snapshot_with_filename_dates_keeps_weekend_report_date(
    tmp_path: Path,
) -> None:
    account_root = tmp_path / "isa_kirill"
    account_root.mkdir(parents=True, exist_ok=True)
    (account_root / "holdings_2026-09-27_normalized.csv").write_text(
        HOLDINGS_HEADER
        + "h1,isa_kirill,ii,2026-09-27,AAA,Fund,1,125,,125,,,,GBP,holdings.pdf\n",
        encoding="utf-8",
    )

    snapshot_dates = collect_holdings_snapshot_dates(tmp_path)
    rows = summarize_unrealized_gains(tmp_path, tmp_path)

    assert snapshot_dates == {"isa_kirill": {date(2026, 9, 27)}}
    assert [row.valuation_date for row in rows] == [date(2026, 9, 27)]


@pytest.mark.parametrize(
    ("filename_date", "broker_date"),
    [("2026-10-28", "2026-10-27"), ("2026-09-27", "2026-09-25")],
)
def test_mismatched_dates_use_filename_throughout_reports(
    tmp_path: Path, filename_date: str, broker_date: str,
) -> None:
    normalized_root = tmp_path / "normalized"
    account_root = normalized_root / "isa_thiago"
    account_root.mkdir(parents=True)
    (account_root / f"holdings_{filename_date}_normalized.csv").write_text(
        HOLDINGS_HEADER
        + f"h1,isa_thiago,hsbc,{broker_date},AAA,Fund,2,100,,200,,,,GBP,holdings.csv\n"
        + "h2,isa_thiago,hsbc,,BBB,Fund,1,50,,50,,,,GBP,holdings.csv\n",
        encoding="utf-8",
    )
    (account_root / "transactions_normalized.csv").write_text(
        TRANSACTIONS_HEADER
        + f"t1,isa_thiago,hsbc,{broker_date},,AAA,,1,100,buy,,100,,,GBP,transactions.csv\n"
        + f"t2,isa_thiago,hsbc,{filename_date},,AAA,,1,50,buy,,50,,,GBP,transactions.csv\n",
        encoding="utf-8",
    )
    output_root = tmp_path / "reports" / "unrealized"
    report_account_root = output_root / "isa_thiago"
    report_account_root.mkdir(parents=True)
    stale_report = report_account_root / f"unrealized_gains_{broker_date}.csv"
    stale_report.write_text("old report", encoding="utf-8")
    notes = report_account_root / "unrealized_gains_notes.csv"
    notes.write_text("notes", encoding="utf-8")
    other_account_report = output_root / "other_account" / f"unrealized_gains_{broker_date}.csv"
    other_account_report.parent.mkdir()
    other_account_report.write_text("other account", encoding="utf-8")

    rows = summarize_unrealized_gains(normalized_root, normalized_root)
    snapshot_dates = collect_holdings_snapshot_dates(normalized_root)
    write_unrealized_gain_reports(rows, output_root, snapshot_dates_by_account=snapshot_dates)
    combined_path = tmp_path / "reports" / "unrealized_gains_report.csv"
    write_combined_unrealized_gain_report(
        rows, combined_path, snapshot_dates_by_account=snapshot_dates,
    )

    report_date = date.fromisoformat(filename_date)
    assert snapshot_dates == {"isa_thiago": {report_date}}
    assert [row.valuation_date for row in rows] == [report_date, report_date]
    assert rows[0].book_cost == Decimal("150.00")
    assert rows[0].unrealized_gain == Decimal("50.00")
    for path in (combined_path, report_account_root / f"unrealized_gains_{filename_date}.csv"):
        with path.open(encoding="utf-8", newline="") as handle:
            csv_rows = list(csv.DictReader(handle))
        assert len(csv_rows) == 2
        assert {row["valuation_date"] for row in csv_rows} == {filename_date}
    assert not stale_report.exists()
    assert notes.read_text(encoding="utf-8") == "notes"
    assert other_account_report.read_text(encoding="utf-8") == "other account"

    snapshots = summarize_historical_performance(normalized_root, normalized_root)
    assert [snapshot.valuation_date for snapshot in snapshots] == [report_date]
    assert snapshots[0].unrealized_gain == Decimal("100.00")
    assert snapshots[0].nominal_invested == Decimal("150.00")


def test_per_snapshot_report_writes_empty_latest_snapshot_file(tmp_path: Path) -> None:
    _write_trading_kirill_fixture(tmp_path)
    rows = summarize_unrealized_gains(tmp_path, tmp_path)
    snapshot_dates = collect_holdings_snapshot_dates(tmp_path)
    output_root = tmp_path / "reports" / "unrealized"

    write_unrealized_gain_reports(
        rows,
        output_root,
        snapshot_dates_by_account=snapshot_dates,
    )

    latest_output = output_root / "trading_kirill" / "unrealized_gains_2026-05-27.csv"
    assert latest_output.read_text(encoding="utf-8").splitlines() == [
        ",".join(UNREALIZED_GAIN_FIELDNAMES),
    ]


def test_historical_performance_includes_empty_latest_snapshot(tmp_path: Path) -> None:
    _write_trading_kirill_fixture(tmp_path)

    snapshots = summarize_historical_performance(tmp_path, tmp_path)

    latest_snapshot = next(
        snapshot for snapshot in snapshots if snapshot.valuation_date == date(2026, 5, 27)
    )
    assert latest_snapshot.account_name == "trading_kirill"
    assert latest_snapshot.unrealized_gain == Decimal("0.00")
    assert latest_snapshot.nominal_invested == Decimal("0.00")
