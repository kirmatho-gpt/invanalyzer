from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Dict, Iterable, Optional, Protocol

from src.normalization.transactions import TransactionRecord


@dataclass(frozen=True)
class PositionCost:
    quantity: Decimal
    book_cost: Decimal


class TransactionLike(Protocol):
    trade_date: Optional[date]
    settlement_date: Optional[date]
    symbol: Optional[str]
    quantity: Optional[Decimal]
    description: Optional[str]


def effective_date(record: TransactionRecord) -> Optional[date]:
    return record.trade_date or record.settlement_date


def infer_signed_quantity(record: TransactionRecord) -> Optional[Decimal]:
    return infer_signed_quantity_from_fields(record.quantity, record.description)


def infer_signed_quantity_from_fields(
    quantity: Optional[Decimal],
    description: Optional[str],
) -> Optional[Decimal]:
    if quantity is None:
        return None
    if description == "buy":
        return quantity
    if description == "sell":
        return -quantity
    return None


def transaction_value(record: TransactionRecord, signed_quantity: Decimal) -> Optional[Decimal]:
    if record.price is not None and record.quantity is not None:
        return record.price * record.quantity
    if signed_quantity > 0 and record.debit is not None:
        return record.debit
    if signed_quantity < 0 and record.credit is not None:
        return record.credit
    return None


def build_positions(
    transactions: Iterable[TransactionLike],
    valuation_date: date,
) -> Dict[str, Decimal]:
    positions: Dict[str, Decimal] = {}
    for record in transactions:
        effective = record.trade_date or record.settlement_date
        if not effective or effective > valuation_date:
            continue
        if not record.symbol:
            continue
        signed_quantity = infer_signed_quantity_from_fields(record.quantity, record.description)
        if signed_quantity is None:
            continue
        positions[record.symbol] = positions.get(record.symbol, Decimal("0")) + signed_quantity
    return positions


def apply_transaction_to_position_costs(
    record: TransactionRecord,
    positions: Dict[str, PositionCost],
) -> Optional[Decimal]:
    signed_quantity = infer_signed_quantity(record)
    if signed_quantity is None or not record.symbol:
        return None
    if signed_quantity > 0:
        value = transaction_value(record, signed_quantity) or Decimal("0")
        position = positions.get(record.symbol, PositionCost(quantity=Decimal("0"), book_cost=Decimal("0")))
        positions[record.symbol] = PositionCost(
            quantity=position.quantity + signed_quantity,
            book_cost=position.book_cost + value,
        )
        return None
    proceeds = transaction_value(record, signed_quantity)
    if proceeds is None:
        return None
    sell_quantity = -signed_quantity
    position = positions.get(record.symbol, PositionCost(quantity=Decimal("0"), book_cost=Decimal("0")))
    if position.quantity == 0:
        average_cost = Decimal("0")
    else:
        average_cost = position.book_cost / position.quantity
    cost_basis = average_cost * sell_quantity
    positions[record.symbol] = PositionCost(
        quantity=position.quantity - sell_quantity,
        book_cost=position.book_cost - cost_basis,
    )
    return proceeds - cost_basis
