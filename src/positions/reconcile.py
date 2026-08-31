from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Dict, Iterable, Optional

from src.positions.transaction_utils import build_positions, infer_signed_quantity_from_fields


IMMATERIAL_POSITION_DELTA = Decimal("1")


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


@dataclass(frozen=True)
class PendingSettlementTransaction:
    trade_date: date
    settlement_date: date
    description: str
    quantity: Decimal
    signed_quantity: Decimal


@dataclass(frozen=True)
class PendingSettlementDifference:
    account_name: str
    valuation_date: date
    symbol: str
    holdings_quantity: Decimal
    transaction_quantity: Decimal
    delta: Decimal
    pending_transactions: tuple[PendingSettlementTransaction, ...]


@dataclass(frozen=True)
class ReconciliationResult:
    mismatches: list[PositionMismatch]
    pending_settlements: list[PendingSettlementDifference]


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


def _pending_settlement_transactions(
    transactions: Iterable[TransactionRow],
    valuation_date: date,
    symbol: str,
) -> tuple[PendingSettlementTransaction, ...]:
    pending_transactions: list[PendingSettlementTransaction] = []
    for record in transactions:
        if record.symbol != symbol:
            continue
        if record.trade_date is None or record.settlement_date is None:
            continue
        if not (record.trade_date <= valuation_date < record.settlement_date):
            continue
        signed_quantity = infer_signed_quantity_from_fields(
            record.quantity,
            record.description,
        )
        if signed_quantity is None or record.quantity is None:
            continue
        pending_transactions.append(
            PendingSettlementTransaction(
                trade_date=record.trade_date,
                settlement_date=record.settlement_date,
                description=record.description or "",
                quantity=record.quantity,
                signed_quantity=signed_quantity,
            )
        )
    return tuple(pending_transactions)


def _as_pending_settlement_difference(
    mismatch: PositionMismatch,
    transactions: Iterable[TransactionRow],
) -> Optional[PendingSettlementDifference]:
    pending_transactions = _pending_settlement_transactions(
        transactions,
        mismatch.valuation_date,
        mismatch.symbol,
    )
    if not pending_transactions:
        return None

    pending_delta = sum(
        (transaction.signed_quantity for transaction in pending_transactions),
        Decimal("0"),
    )
    if pending_delta != mismatch.delta:
        return None

    return PendingSettlementDifference(
        account_name=mismatch.account_name,
        valuation_date=mismatch.valuation_date,
        symbol=mismatch.symbol,
        holdings_quantity=mismatch.holdings_quantity,
        transaction_quantity=mismatch.transaction_quantity,
        delta=mismatch.delta,
        pending_transactions=pending_transactions,
    )


def reconcile_positions_detailed(
    transactions_path: Path,
    holdings_path: Path,
    valuation_date: date,
) -> ReconciliationResult:
    transactions = list(_read_transactions(transactions_path))
    positions = build_positions(transactions, valuation_date)
    holdings = _read_holdings(holdings_path)

    mismatches: list[PositionMismatch] = []
    pending_settlements: list[PendingSettlementDifference] = []
    symbols = set(positions) | set(holdings)
    account_name = transactions[0].account_name if transactions else holdings_path.parent.name

    for symbol in sorted(symbols):
        holdings_qty = holdings.get(symbol, Decimal("0"))
        transaction_qty = positions.get(symbol, Decimal("0"))
        delta = holdings_qty - transaction_qty
        if delta != 0 and abs(delta) >= IMMATERIAL_POSITION_DELTA:
            mismatch = PositionMismatch(
                account_name=account_name,
                valuation_date=valuation_date,
                symbol=symbol,
                holdings_quantity=holdings_qty,
                transaction_quantity=transaction_qty,
                delta=delta,
            )
            pending_settlement = _as_pending_settlement_difference(mismatch, transactions)
            if pending_settlement is not None:
                pending_settlements.append(pending_settlement)
            else:
                mismatches.append(mismatch)
    return ReconciliationResult(
        mismatches=mismatches,
        pending_settlements=pending_settlements,
    )


def reconcile_positions(
    transactions_path: Path,
    holdings_path: Path,
    valuation_date: date,
) -> list[PositionMismatch]:
    return reconcile_positions_detailed(
        transactions_path,
        holdings_path,
        valuation_date,
    ).mismatches
