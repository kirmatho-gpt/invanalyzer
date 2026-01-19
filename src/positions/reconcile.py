from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Dict, Iterable, Optional


@dataclass(frozen=True)
class TransactionRow:
    account_name: str
    trade_date: Optional[date]
    settlement_date: Optional[date]
    symbol: Optional[str]
    quantity: Optional[Decimal]
    description: Optional[str]
    debit: Optional[Decimal]
    credit: Optional[Decimal]


@dataclass(frozen=True)
class PositionMismatch:
    account_name: str
    valuation_date: date
    symbol: str
    holdings_quantity: Decimal
    transaction_quantity: Decimal
    delta: Decimal


def _parse_date(value: str) -> Optional[date]:
    if not value:
        return None
    return date.fromisoformat(value)


def _parse_decimal(value: str) -> Optional[Decimal]:
    if not value:
        return None
    return Decimal(value)


def _read_transactions(path: Path) -> Iterable[TransactionRow]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            yield TransactionRow(
                account_name=row.get("account_name", ""),
                trade_date=_parse_date(row.get("trade_date", "")),
                settlement_date=_parse_date(row.get("settlement_date", "")),
                symbol=row.get("symbol") or None,
                quantity=_parse_decimal(row.get("quantity", "")),
                description=row.get("description") or None,
                debit=_parse_decimal(row.get("debit", "")),
                credit=_parse_decimal(row.get("credit", "")),
            )


def _read_holdings(path: Path) -> Dict[str, Decimal]:
    holdings: Dict[str, Decimal] = {}
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            symbol = row.get("symbol") or ""
            quantity = _parse_decimal(row.get("quantity", ""))
            if not symbol or quantity is None:
                continue
            holdings[symbol] = quantity
    return holdings


def _infer_signed_quantity(record: TransactionRow) -> Optional[Decimal]:
    if record.quantity is None:
        return None
    if record.debit is not None and record.credit is None:
        return record.quantity
    if record.credit is not None and record.debit is None:
        return -record.quantity

    return None


def build_positions(
    transactions: Iterable[TransactionRow],
    valuation_date: date,
) -> Dict[str, Decimal]:
    positions: Dict[str, Decimal] = {}
    for record in transactions:
        effective_date = record.trade_date or record.settlement_date
        if not effective_date or effective_date > valuation_date:
            continue
        if not record.symbol:
            continue
        signed_quantity = _infer_signed_quantity(record)
        if signed_quantity is None:
            continue
        positions[record.symbol] = positions.get(record.symbol, Decimal("0")) + signed_quantity
    return positions


def reconcile_positions(
    transactions_path: Path,
    holdings_path: Path,
    valuation_date: date,
) -> list[PositionMismatch]:
    transactions = list(_read_transactions(transactions_path))
    positions = build_positions(transactions, valuation_date)
    holdings = _read_holdings(holdings_path)

    mismatches: list[PositionMismatch] = []
    symbols = set(positions) | set(holdings)
    account_name = transactions[0].account_name if transactions else holdings_path.parent.name

    for symbol in sorted(symbols):
        holdings_qty = holdings.get(symbol, Decimal("0"))
        transaction_qty = positions.get(symbol, Decimal("0"))
        delta = holdings_qty - transaction_qty
        if delta != 0:
            mismatches.append(
                PositionMismatch(
                    account_name=account_name,
                    valuation_date=valuation_date,
                    symbol=symbol,
                    holdings_quantity=holdings_qty,
                    transaction_quantity=transaction_qty,
                    delta=delta,
                )
            )
    return mismatches
