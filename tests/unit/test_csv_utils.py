from __future__ import annotations

import sys
from pathlib import Path

from src.ingestion.csv_utils import read_dict_rows


class _FakePage:
    def extract_tables(self) -> list[list[list[str]]]:
        return [
            [
                ["Symbol", "Name", "Qty"],
                ["AAA", "Name\nWith 1- 5 and 0. 625", "117282.\n29"],
            ],
            [
                ["GBP", "£1,000.00"],
                ["Total", "£1,000.00"],
            ],
        ]


class _FakePdf:
    pages = [_FakePage()]

    def __enter__(self) -> "_FakePdf":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        return None


class _FakePdfPlumber:
    @staticmethod
    def open(path: Path) -> _FakePdf:
        return _FakePdf()


def test_read_dict_rows_extracts_pdf_tables_and_skips_non_header_tables(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setitem(sys.modules, "pdfplumber", _FakePdfPlumber)

    rows = list(read_dict_rows(tmp_path / "holdings_account_20260731.pdf"))

    assert rows == [
        {
            "Symbol": "AAA",
            "Name": "Name With 1-5 and 0.625",
            "Qty": "117282.29",
        }
    ]
