from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Dict, Iterable, Optional

from src.ingestion.normalized import read_normalized_holdings, read_normalized_transactions
from src.normalization.holdings import HoldingRecord
from src.normalization.transactions import TransactionRecord


@dataclass(frozen=True)
class PositionCost:
    quantity: Decimal
    book_cost: Decimal


@dataclass(frozen=True)
class UnrealizedGainRow:
    account_name: str
    valuation_date: date
    symbol: str
    name: Optional[str]
    quantity: Optional[Decimal]
    price: Optional[Decimal]
    book_cost: Decimal
    market_value: Decimal
    unrealized_gain: Decimal
    unrealized_gain_pct: Optional[Decimal]
    currency: Optional[str]

    def to_dict(self) -> Dict[str, str]:
        return {
            "account_name": self.account_name,
            "valuation_date": self.valuation_date.isoformat(),
            "symbol": self.symbol,
            "name": self.name or "",
            "quantity": f"{self.quantity}" if self.quantity is not None else "",
            "price": f"{self.price}" if self.price is not None else "",
            "book_cost": f"{self.book_cost}",
            "market_value": f"{self.market_value}",
            "unrealized_gain": f"{self.unrealized_gain}",
            "unrealized_gain_pct": (
                f"{self.unrealized_gain_pct}" if self.unrealized_gain_pct is not None else ""
            ),
            "currency": self.currency or "",
        }


def _effective_date(record: TransactionRecord) -> Optional[date]:
    return record.trade_date or record.settlement_date


def _infer_signed_quantity(record: TransactionRecord) -> Optional[Decimal]:
    if record.quantity is None:
        return None
    if record.description == "buy":
        return record.quantity
    if record.description == "sell":
        return -record.quantity
    return None


def _transaction_value(record: TransactionRecord, signed_quantity: Decimal) -> Optional[Decimal]:
    if record.price is not None and record.quantity is not None:
        return record.price * record.quantity
    if signed_quantity > 0 and record.debit is not None:
        return record.debit
    if signed_quantity < 0 and record.credit is not None:
        return record.credit
    return None


def _apply_transaction(record: TransactionRecord, positions: Dict[str, PositionCost]) -> None:
    signed_quantity = _infer_signed_quantity(record)
    if signed_quantity is None or not record.symbol:
        return
    symbol = record.symbol
    position = positions.get(symbol, PositionCost(quantity=Decimal("0"), book_cost=Decimal("0")))
    if signed_quantity > 0:
        value = _transaction_value(record, signed_quantity) or Decimal("0")
        positions[symbol] = PositionCost(
            quantity=position.quantity + signed_quantity,
            book_cost=position.book_cost + value,
        )
        return

    sell_quantity = -signed_quantity
    if position.quantity == 0:
        positions[symbol] = PositionCost(quantity=position.quantity - sell_quantity, book_cost=position.book_cost)
        return
    average_cost = position.book_cost / position.quantity if position.quantity != 0 else Decimal("0")
    positions[symbol] = PositionCost(
        quantity=position.quantity - sell_quantity,
        book_cost=position.book_cost - (average_cost * sell_quantity),
    )


def _book_costs_by_date(
    transactions: Iterable[TransactionRecord],
    valuation_dates: Iterable[date],
) -> Dict[date, Dict[str, PositionCost]]:
    sorted_dates = sorted(set(valuation_dates))
    sorted_transactions = sorted(
        [record for record in transactions if _effective_date(record)],
        key=lambda record: _effective_date(record),
    )
    positions: Dict[str, PositionCost] = {}
    results: Dict[date, Dict[str, PositionCost]] = {}
    index = 0

    for valuation_date in sorted_dates:
        while index < len(sorted_transactions):
            record = sorted_transactions[index]
            effective_date = _effective_date(record)
            if effective_date is None or effective_date > valuation_date:
                break
            _apply_transaction(record, positions)
            index += 1
        results[valuation_date] = {symbol: PositionCost(pos.quantity, pos.book_cost) for symbol, pos in positions.items()}
    return results


def _market_value(holding: HoldingRecord) -> Decimal:
    if holding.market_value is not None:
        return holding.market_value
    if holding.quantity is not None and holding.price is not None:
        return holding.quantity * holding.price
    return Decimal("0")


def summarize_unrealized_gains(
    transactions_root: Path,
    holdings_root: Path,
    accounts: Optional[Iterable[str]] = None,
) -> list[UnrealizedGainRow]:
    account_filter = {name.strip() for name in accounts or [] if name.strip()}

    transactions_by_account: Dict[str, list[TransactionRecord]] = {}
    for path in sorted(transactions_root.rglob("transactions_normalized.csv")):
        account_name = path.parent.name
        if account_filter and account_name not in account_filter:
            continue
        transactions_by_account[account_name] = list(read_normalized_transactions(path))

    holdings_by_account: Dict[str, list[HoldingRecord]] = {}
    valuation_dates_by_account: Dict[str, set[date]] = {}

    for path in sorted(holdings_root.rglob("holdings_*_normalized.csv")):
        for holding in read_normalized_holdings(path):
            account_name = holding.account_name or path.parent.name
            if account_filter and account_name not in account_filter:
                continue
            holdings_by_account.setdefault(account_name, []).append(holding)
            valuation_dates_by_account.setdefault(account_name, set()).add(holding.valuation_date)

    rows: list[UnrealizedGainRow] = []
    for account_name, holdings in holdings_by_account.items():
        transactions = transactions_by_account.get(account_name, [])
        valuation_dates = valuation_dates_by_account.get(account_name, set())
        book_costs_by_date = _book_costs_by_date(transactions, valuation_dates)

        for holding in holdings:
            positions = book_costs_by_date.get(holding.valuation_date, {})
            position = positions.get(holding.symbol, PositionCost(quantity=Decimal("0"), book_cost=Decimal("0")))
            market_value = _market_value(holding)
            book_cost = position.book_cost
            unrealized_gain = market_value - book_cost
            unrealized_gain_pct = None
            if book_cost != 0:
                unrealized_gain_pct = (unrealized_gain / book_cost)
            rows.append(
                UnrealizedGainRow(
                    account_name=account_name,
                    valuation_date=holding.valuation_date,
                    symbol=holding.symbol,
                    name=holding.name,
                    quantity=holding.quantity,
                    price=holding.price,
                    market_value=round(market_value, 2),
                    book_cost=round(book_cost, 2),
                    unrealized_gain=round(unrealized_gain, 2),
                    unrealized_gain_pct=round(unrealized_gain_pct, 4),
                    currency=holding.currency,
                )
            )

    rows.sort(key=lambda row: (row.account_name, row.valuation_date, row.symbol))
    return rows


def write_unrealized_gain_reports(rows: Iterable[UnrealizedGainRow], output_root: Path) -> None:
    grouped: Dict[tuple[str, date], list[UnrealizedGainRow]] = {}
    for row in rows:
        grouped.setdefault((row.account_name, row.valuation_date), []).append(row)

    for (account_name, valuation_date), grouped_rows in grouped.items():
        output_path = (
            output_root
            / account_name
            / f"unrealized_gains_{valuation_date.isoformat()}.csv"
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(grouped_rows[0].to_dict().keys()))
            writer.writeheader()
            for row in grouped_rows:
                writer.writerow(row.to_dict())


def _latest_rows_by_account(rows: Iterable[UnrealizedGainRow]) -> list[UnrealizedGainRow]:
    rows_list = list(rows)
    latest_by_account: Dict[str, date] = {}
    for row in rows_list:
        current = latest_by_account.get(row.account_name)
        if current is None or row.valuation_date > current:
            latest_by_account[row.account_name] = row.valuation_date
    latest_rows = [
        row for row in rows_list if row.valuation_date == latest_by_account.get(row.account_name)
    ]
    latest_rows.sort(key=lambda row: (row.account_name, row.valuation_date, row.symbol))
    return latest_rows


def write_combined_unrealized_gain_report(
    rows: Iterable[UnrealizedGainRow],
    output_path: Path,
    *,
    latest_only: bool = True,
) -> None:
    rows_list = _latest_rows_by_account(rows) if latest_only else list(rows)
    if not rows_list:
        return
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows_list[0].to_dict().keys()))
        writer.writeheader()
        for row in rows_list:
            writer.writerow(row.to_dict())
