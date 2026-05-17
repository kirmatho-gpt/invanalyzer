from __future__ import annotations

from datetime import date
from pathlib import Path

from scripts.process_all import (
    _collect_holdings_dates_by_account,
    _collect_raw_holdings_dates_by_account,
    _ii_common_snapshot_warnings,
    _merge_dates_by_account,
    _majority_snapshot_gap_warnings,
    _missing_holdings_snapshot_warnings,
)


def test_collect_holdings_dates_by_account_parses_dates_from_paths(tmp_path: Path) -> None:
    normalized_root = tmp_path / "normalized"
    (normalized_root / "acct_a").mkdir(parents=True, exist_ok=True)
    (normalized_root / "acct_b").mkdir(parents=True, exist_ok=True)

    (normalized_root / "acct_a" / "holdings_2026-04-01_normalized.csv").write_text("", encoding="utf-8")
    (normalized_root / "acct_a" / "holdings_2026-04-30_normalized.csv").write_text("", encoding="utf-8")
    (normalized_root / "acct_b" / "holdings_2026-04-30_normalized.csv").write_text("", encoding="utf-8")

    result = _collect_holdings_dates_by_account(normalized_root)

    assert result == {
        "acct_a": {date(2026, 4, 1), date(2026, 4, 30)},
        "acct_b": {date(2026, 4, 30)},
    }


def test_majority_snapshot_gap_warnings_detects_missing_account() -> None:
    snapshot_dates = {
        "acct_a": {date(2026, 4, 1), date(2026, 4, 30)},
        "acct_b": {date(2026, 4, 1)},
        "acct_c": {date(2026, 4, 1), date(2026, 4, 30)},
    }

    warnings = _majority_snapshot_gap_warnings(snapshot_dates)

    assert len(warnings) == 1
    assert "2026-04-30" in warnings[0]
    assert "missing=acct_b" in warnings[0]


def test_majority_snapshot_gap_warnings_ignores_non_majority_dates() -> None:
    snapshot_dates = {
        "acct_a": {date(2026, 4, 1)},
        "acct_b": set(),
        "acct_c": set(),
    }

    warnings = _majority_snapshot_gap_warnings(snapshot_dates)

    assert warnings == []


def test_ii_common_snapshot_warnings_detects_missing_ii_account() -> None:
    snapshot_dates = {
        "ii_a": {date(2026, 4, 1)},
        "ii_b": {date(2026, 4, 1)},
        "ii_c": set(),
    }

    warnings = _ii_common_snapshot_warnings(snapshot_dates, ["ii_a", "ii_b", "ii_c"])

    assert len(warnings) == 1
    assert "II common snapshot missing on 2026-04-01" in warnings[0]
    assert "missing=ii_c" in warnings[0]


def test_ii_common_snapshot_warnings_reports_when_no_data_found() -> None:
    warnings = _ii_common_snapshot_warnings({}, ["ii_a", "ii_b"])

    assert len(warnings) == 1
    assert "no holdings snapshots found for II accounts" in warnings[0]


def test_missing_holdings_snapshot_warnings_reports_accounts_without_snapshots() -> None:
    warnings = _missing_holdings_snapshot_warnings(
        {"acct_a": {date(2026, 4, 1)}, "acct_b": set()},
        ["acct_a", "acct_b", "acct_c"],
    )

    assert warnings == [
        "Missing holdings snapshots for account 'acct_b'",
        "Missing holdings snapshots for account 'acct_c'",
    ]


def test_collect_raw_holdings_dates_by_account_counts_empty_files_as_present(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw_holdings"
    raw_root.mkdir(parents=True, exist_ok=True)
    (raw_root / "holdings_acct_a_2026-04-01.csv").write_text("", encoding="utf-8")

    snapshot_dates = _collect_raw_holdings_dates_by_account(raw_root)

    assert snapshot_dates == {"acct_a": {date(2026, 4, 1)}}


def test_merge_dates_by_account_unions_normalized_and_raw_dates() -> None:
    normalized_dates = {"acct_a": {date(2026, 4, 30)}}
    raw_dates = {"acct_a": {date(2026, 4, 1)}, "acct_b": {date(2026, 4, 1)}}

    merged = _merge_dates_by_account(normalized_dates, raw_dates)

    assert merged == {
        "acct_a": {date(2026, 4, 1), date(2026, 4, 30)},
        "acct_b": {date(2026, 4, 1)},
    }
