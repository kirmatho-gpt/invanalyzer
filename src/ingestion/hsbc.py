from __future__ import annotations

import csv
import hashlib
from dataclasses import asdict
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Iterable, Optional

from src.normalization.transactions import TransactionRecord


DATE_FORMATS = ("%d %b %Y", "%d %b %Y %H:%M")

DEBIT_DESCRIPTIONS = {"bought"}
CREDIT_DESCRIPTIONS = {"sold", "cash dividend received", "interest received"}


def _parse_date(value: str) -> Optional[datetime.date]:
    if not value or value.strip().lower() == "n/a":
        return None
    cleaned = value.strip()
    for date_format in DATE_FORMATS:
        try:
            return datetime.strptime(cleaned, date_format).date()
        except ValueError:
            continue
    raise ValueError(f"Unrecognized date format: {value}")


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


def _normalize_transaction_description(description: Optional[str]) -> Optional[str]:
    if description is None:
        return None
    cleaned = description.strip()
    if not cleaned:
        return None
    normalized = cleaned.casefold()
    if normalized == "bought":
        return "buy"
    if normalized == "sold":
        return "sell"
    if normalized == "cash dividend received":
        return "dividend"
    if normalized == "interest received":
        return "account interest"
    raise ValueError(f"Unexpected transaction description: {description}")


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


def _settled_amount_to_cash_flow(
    description: Optional[str],
    settled_amount: Optional[Decimal],
) -> tuple[Optional[Decimal], Optional[Decimal]]:
    if settled_amount is None:
        return None, None
    if not description:
        return None, None
    normalized = description.strip().lower()
    if normalized in DEBIT_DESCRIPTIONS:
        return settled_amount, None
    if normalized in CREDIT_DESCRIPTIONS:
        return None, settled_amount
    return None, None


def parse_hsbc_transactions(path: Path, account_name: str, broker: str) -> Iterable[TransactionRecord]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            trade_date = _parse_date(row.get("Transaction Date", ""))
            settlement_date = trade_date
            description = _normalize_transaction_description(
                _normalize_text(row.get("Transaction Description", ""))
            )
            symbol = _normalize_text(row.get("Product Short Name", ""))
            sedol = _normalize_text(row.get("Product Code", ""))
            quantity = _parse_decimal(row.get("No. of Units", ""))
            price = _parse_decimal(row.get("Deal Price", ""))
            reference = _normalize_text(row.get("Transaction Reference", ""))
            settled_amount = _parse_decimal(row.get("Settled Amount", ""))
            debit, credit = _settled_amount_to_cash_flow(description, settled_amount)
            currency = _normalize_text(row.get("Settlement Currency", "")) or _normalize_text(
                row.get("Price Currency", "")
            )

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
                running_balance=None,
                currency=currency,
                source_file=path.name,
            )
            record = record.__class__(**{**asdict(record), "transaction_id": _transaction_id(record)})
            yield record
