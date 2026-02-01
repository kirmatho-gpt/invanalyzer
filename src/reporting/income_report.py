from __future__ import annotations

import csv
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Dict, Iterable, Iterator, Optional, Tuple


from src.ingestion.normalized import read_normalized_transactions
from src.normalization.transactions import TransactionRecord
from src.positions.transaction_utils import (
    PositionCost,
    apply_transaction_to_position_costs,
    effective_date,
)


INCOME_DESCRIPTIONS = {"dividend", "account interest", "fees", "cash advantage"}


@dataclass(frozen=True)
class IncomeRecord:
    account_name: str
    trade_date: Optional[date]
    symbol: Optional[str]
    description: Optional[str]
    debit: Optional[Decimal]
    credit: Optional[Decimal]


@dataclass(frozen=True)
class IncomeSummary:
    account_name: str
    month: str
    symbol: str
    description: str
    total_amount: Decimal

    def to_dict(self) -> Dict[str, str]:
        return {
            "account_name": self.account_name,
            "month": self.month,
            "symbol": self.symbol,
            "description": self.description,
            "total_amount": f"{self.total_amount}",
        }


def _income_records_from_transactions(transactions: Iterable[TransactionRecord]) -> Iterator[IncomeRecord]:
    positions: Dict[str, PositionCost] = {}
    sorted_transactions = sorted(
        [record for record in transactions if effective_date(record)],
        key=lambda record: effective_date(record),
    )
    for record in sorted_transactions:
        effective_date_value = effective_date(record)
        if record.description in INCOME_DESCRIPTIONS:
            yield IncomeRecord(
                account_name=record.account_name,
                trade_date=effective_date_value,
                symbol=record.symbol,
                description=record.description,
                debit=record.debit,
                credit=record.credit,
            )
            continue
        if record.description in {"buy", "sell"}:
            gain = apply_transaction_to_position_costs(record, positions)
            if record.description == "buy":
                continue
            if gain is None:
                continue
            debit = None
            credit = None
            if gain >= 0:
                credit = gain
            else:
                debit = -gain
            yield IncomeRecord(
                account_name=record.account_name,
                trade_date=effective_date_value,
                symbol=record.symbol,
                description="sell of securities",
                debit=debit,
                credit=credit,
            )


def _month_key(value: Optional[date]) -> str:
    if value is None:
        return "unknown"
    return value.strftime("%Y-%m")


def _amount(record: IncomeRecord) -> Decimal:
    debit = record.debit or Decimal("0")
    credit = record.credit or Decimal("0")
    return credit - debit


def summarize_income(
    transactions_root: Path,
    accounts: Optional[Iterable[str]] = None,
) -> list[IncomeSummary]:
    account_filter = {name.strip() for name in accounts or [] if name.strip()}
    totals: Dict[Tuple[str, str, str, str], Decimal] = defaultdict(lambda: Decimal("0"))
    for path in sorted(transactions_root.rglob("transactions_normalized.csv")):
        account_name = path.parent.name
        if account_filter and account_name not in account_filter:
            continue
        transactions = list(read_normalized_transactions(path))
        for record in _income_records_from_transactions(transactions):
            if account_filter and record.account_name not in account_filter:
                continue
            month = _month_key(record.trade_date)
            symbol = record.symbol or "CASH"
            description = record.description or "unknown"
            account_name = record.account_name or "unknown"
            totals[(account_name, month, symbol, description)] += _amount(record)

    summary = [
        IncomeSummary(
            account_name=account_name,
            month=month,
            symbol=symbol,
            description=description,
            total_amount=total,
        )
        for (account_name, month, symbol, description), total in totals.items()
    ]
    summary.sort(key=lambda row: (row.account_name, row.month, row.symbol, row.description))
    return summary


def write_income_report(rows: Iterable[IncomeSummary], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rows_list = list(rows)
    if not rows_list:
        return
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows_list[0].to_dict().keys()))
        writer.writeheader()
        for row in rows_list:
            writer.writerow(row.to_dict())
