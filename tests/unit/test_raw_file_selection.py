from __future__ import annotations

from pathlib import Path

from scripts.normalize_holdings import _find_holding_files
from scripts.normalize_transactions import _find_transaction_files


def test_find_transaction_files_prefers_pdf_with_same_stem(tmp_path: Path) -> None:
    (tmp_path / "transactions_trading_thiago_20260731.csv").write_text("", encoding="utf-8")
    (tmp_path / "transactions_trading_thiago_20260731.pdf").write_text("", encoding="utf-8")
    (tmp_path / "transactions_trading_kirill_20260731.csv").write_text("", encoding="utf-8")

    assert [path.name for path in _find_transaction_files(tmp_path)] == [
        "transactions_trading_kirill_20260731.csv",
        "transactions_trading_thiago_20260731.pdf",
    ]


def test_find_holding_files_prefers_pdf_with_same_stem(tmp_path: Path) -> None:
    (tmp_path / "holdings_trading_thiago_20260731.txt").write_text("", encoding="utf-8")
    (tmp_path / "holdings_trading_thiago_20260731.csv").write_text("", encoding="utf-8")
    (tmp_path / "holdings_trading_thiago_20260731.pdf").write_text("", encoding="utf-8")

    assert [path.name for path in _find_holding_files(tmp_path)] == [
        "holdings_trading_thiago_20260731.pdf",
    ]
