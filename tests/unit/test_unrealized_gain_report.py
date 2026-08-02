from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

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
