from __future__ import annotations

import csv
import io
import re
from pathlib import Path
from typing import Iterator, Optional


PDF_TABLE_HEADER_COLUMNS = {
    "Date",
    "Settlement Date",
    "Symbol",
    "Sedol",
    "Quantity",
    "Price",
    "Description",
    "Reference",
    "Debit",
    "Credit",
    "Running Balance",
    "Name",
    "Qty",
    "Market Value £",
    "Market Value",
    "Book Cost",
    "Gain/Loss",
    "Gain/Loss %",
    "Average Price",
    "Transaction Date",
    "Transaction Description",
    "Product Short Name",
    "Product Code",
    "Product Name",
    "No. of Units",
    "Valuation Date",
}


def _clean_pdf_cell(value: Optional[str]) -> str:
    if value is None:
        return ""
    cleaned = " ".join(value.replace("\ufeff", "").split())
    cleaned = re.sub(r"(?<=\d)\.\s+(?=\d)", ".", cleaned)
    cleaned = re.sub(r"(?<=\w)-\s+(?=\w)", "-", cleaned)
    return cleaned


def _looks_like_pdf_table_header(header: list[str]) -> bool:
    return len(PDF_TABLE_HEADER_COLUMNS.intersection(header)) >= 2


def read_csv_dict_rows(path: Path) -> Iterator[dict[str, Optional[str]]]:
    # Some broker exports contain multiple UTF-8 BOM markers at the file start.
    # Strip all leading BOM chars before csv parsing so headers match expected names.
    content = path.read_text(encoding="utf-8-sig").lstrip("\ufeff")
    reader = csv.DictReader(io.StringIO(content))
    for row in reader:
        yield {(key.lstrip("\ufeff") if key is not None else key): value for key, value in row.items()}


def read_pdf_dict_rows(path: Path) -> Iterator[dict[str, Optional[str]]]:
    try:
        import pdfplumber
    except ImportError as exc:
        raise RuntimeError(
            "PDF ingestion requires pdfplumber. Install it or use the bundled Codex Python runtime."
        ) from exc

    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            for table in page.extract_tables():
                if not table:
                    continue
                header = [_clean_pdf_cell(cell) for cell in table[0]]
                if not _looks_like_pdf_table_header(header):
                    continue

                for raw_row in table[1:]:
                    values = [_clean_pdf_cell(cell) for cell in raw_row]
                    if not any(values):
                        continue
                    yield dict(zip(header, values))


def read_dict_rows(path: Path) -> Iterator[dict[str, Optional[str]]]:
    if path.suffix.lower() == ".pdf":
        yield from read_pdf_dict_rows(path)
        return
    yield from read_csv_dict_rows(path)
