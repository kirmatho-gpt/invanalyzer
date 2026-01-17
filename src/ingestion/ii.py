from __future__ import annotations

import csv
import hashlib
from dataclasses import asdict
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Iterable, Optional

from normalization.transactions import TransactionRecord


DATE_FORMAT = "%d/%m/%Y"


def _parse_date(value: str) -> Optional[datetime.date]:
    if not value or value.strip().lower() == "n/a":
        return None
    return datetime.strptime(value.strip(), DATE_FORMAT).date()


def _parse_decimal(value: str) -> Optional[Decimal]:
    if not value or value.strip().lower() == "n/a":
        return None
    cleaned = value.replace("£", "").replace(",", "").strip()
    if not cleaned:
        return None
    return Decimal(cleaned)


def _normalize_text(value: str) -> Optional[str]:
    if not value or value.strip().lower() == "n/a":
        return None
    return value.strip()


def _transaction_id(record: TransactionRecord) -> str:
    raw = "|".join(
        [
            record.account_name,
            record.broker,
            record.trade_date.isoformat() if record.trade_date else "",
            record.settlement_date.isoformat() if record.settlement_date else "",
            record.symbol or "",
            record.sedol or "",
            f"{record.quantity}" if record.quantity is not None else "",
            f"{record.price}" if record.price is not None else "",
            record.description or "",
            record.reference or "",
            f"{record.debit}" if record.debit is not None else "",
            f"{record.credit}" if record.credit is not None else "",
            f"{record.running_balance}" if record.running_balance is not None else "",
            record.currency or "",
        ]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def parse_ii_transactions(path: Path, account_name: str, broker: str) -> Iterable[TransactionRecord]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            trade_date = _parse_date(row.get("Date", ""))
            settlement_date = _parse_date(row.get("Settlement Date", ""))
            symbol = _normalize_text(row.get("Symbol", ""))
            sedol = _normalize_text(row.get("Sedol", ""))
            quantity = _parse_decimal(row.get("Quantity", ""))
            price = _parse_decimal(row.get("Price", ""))
            description = _normalize_text(row.get("Description", ""))
            reference = _normalize_text(row.get("Reference", ""))
            debit = _parse_decimal(row.get("Debit", ""))
            credit = _parse_decimal(row.get("Credit", ""))
            running_balance = _parse_decimal(row.get("Running Balance", ""))

            record = TransactionRecord(
                transaction_id="",
                account_name=account_name,
                broker=broker,
                trade_date=trade_date,
                settlement_date=settlement_date,
                symbol=symbol,
                sedol=sedol,
                quantity=quantity,
                price=price,
                description=description,
                reference=reference,
                debit=debit,
                credit=credit,
                running_balance=running_balance,
                currency="GBP",
                source_file=path.name,
            )
            record = record.__class__(**{**asdict(record), "transaction_id": _transaction_id(record)})
            yield record
