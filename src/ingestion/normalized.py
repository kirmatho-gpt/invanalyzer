from __future__ import annotations

import csv
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Iterator, Optional

from src.normalization.holdings import HoldingRecord
from src.normalization.transactions import TransactionRecord


def _parse_date(value: str) -> Optional[date]:
    if not value:
        return None
    return date.fromisoformat(value)


def _parse_decimal(value: str) -> Optional[Decimal]:
    if not value:
        return None
    return Decimal(value)


def _valuation_date_from_path(path: Path) -> Optional[date]:
    stem = path.stem
    for token in stem.split("_"):
        try:
            return date.fromisoformat(token)
        except ValueError:
            continue
    return None


def read_normalized_transactions(path: Path) -> Iterator[TransactionRecord]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            yield TransactionRecord(
                transaction_id=row.get("transaction_id", ""),
                account_name=row.get("account_name", ""),
                broker=row.get("broker", ""),
                trade_date=_parse_date(row.get("trade_date", "")),
                settlement_date=_parse_date(row.get("settlement_date", "")),
                symbol=row.get("symbol") or None,
                sedol=row.get("sedol") or None,
                quantity=_parse_decimal(row.get("quantity", "")),
                price=_parse_decimal(row.get("price", "")),
                description=row.get("description") or None,
                reference=row.get("reference") or None,
                debit=_parse_decimal(row.get("debit", "")),
                credit=_parse_decimal(row.get("credit", "")),
                running_balance=_parse_decimal(row.get("running_balance", "")),
                currency=row.get("currency") or None,
                source_file=row.get("source_file", ""),
            )


def read_normalized_holdings(path: Path) -> Iterator[HoldingRecord]:
    fallback_date = _valuation_date_from_path(path)
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            valuation_date = _parse_date(row.get("valuation_date", "")) or fallback_date
            symbol = row.get("symbol") or ""
            if valuation_date is None or not symbol:
                continue
            yield HoldingRecord(
                snapshot_id=row.get("snapshot_id", ""),
                account_name=row.get("account_name", ""),
                broker=row.get("broker", ""),
                valuation_date=valuation_date,
                symbol=symbol,
                name=row.get("name") or None,
                quantity=_parse_decimal(row.get("quantity", "")),
                price=_parse_decimal(row.get("price", "")),
                average_price=_parse_decimal(row.get("average_price", "")),
                market_value=_parse_decimal(row.get("market_value", "")),
                book_cost=_parse_decimal(row.get("book_cost", "")),
                gain_loss=_parse_decimal(row.get("gain_loss", "")),
                gain_loss_pct=_parse_decimal(row.get("gain_loss_pct", "")),
                currency=row.get("currency") or None,
                source_file=row.get("source_file", ""),
            )