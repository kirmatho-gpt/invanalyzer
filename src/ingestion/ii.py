from __future__ import annotations

import csv
import hashlib
from dataclasses import asdict
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Iterable, Optional

from src.normalization.holdings import HoldingRecord
from src.normalization.transactions import TransactionRecord


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


def _parse_percent(value: str) -> Optional[Decimal]:
    if not value or value.strip().lower() == "n/a":
        return None
    cleaned = value.replace("%", "").replace(",", "").strip()
    if not cleaned:
        return None
    return Decimal(cleaned)


def _parse_price(value: str) -> Optional[Decimal]:
    if not value or value.strip().lower() == "n/a":
        return None
    cleaned = value.replace("£", "").replace(",", "").strip()
    if not cleaned:
        return None
    if cleaned.endswith("p"):
        pence = cleaned[:-1].strip()
        if not pence:
            return None
        return Decimal(pence) / Decimal("100")
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


def _snapshot_id(record: HoldingRecord) -> str:
    raw = "|".join(
        [
            record.account_name,
            record.broker,
            record.valuation_date.isoformat() if record.valuation_date else "",
            record.symbol or "",
            record.name or "",
            f"{record.quantity}" if record.quantity is not None else "",
            f"{record.price}" if record.price is not None else "",
            f"{record.average_price}" if record.average_price is not None else "",
            f"{record.market_value}" if record.market_value is not None else "",
            f"{record.book_cost}" if record.book_cost is not None else "",
            f"{record.gain_loss}" if record.gain_loss is not None else "",
            f"{record.gain_loss_pct}" if record.gain_loss_pct is not None else "",
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
                symbol=symbol or sedol, # replace symbol with sedol if missing
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


def parse_ii_holdings(
    path: Path,
    account_name: str,
    broker: str,
    valuation_date: datetime.date,
) -> Iterable[HoldingRecord]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            symbol = _normalize_text(row.get("Symbol", ""))
            name = _normalize_text(row.get("Name", ""))
            if not symbol:
                continue
            quantity = _parse_decimal(row.get("Qty", ""))
            price = _parse_price(row.get("Price", ""))
            market_value = _parse_decimal(row.get("Market Value £", "")) or _parse_decimal(row.get("Market Value", ""))
            book_cost = _parse_decimal(row.get("Book Cost", ""))
            gain_loss = _parse_decimal(row.get("Gain/Loss", ""))
            gain_loss_pct = _parse_percent(row.get("Gain/Loss %", ""))
            average_price = _parse_price(row.get("Average Price", ""))

            record = HoldingRecord(
                snapshot_id="",
                account_name=account_name,
                broker=broker,
                valuation_date=valuation_date,
                symbol=symbol,
                name=name,
                quantity=quantity,
                price=price,
                average_price=average_price,
                market_value=market_value,
                book_cost=book_cost,
                gain_loss=gain_loss,
                gain_loss_pct=gain_loss_pct,
                currency="GBP",
                source_file=path.name,
            )
            record = record.__class__(**{**asdict(record), "snapshot_id": _snapshot_id(record)})
            yield record
