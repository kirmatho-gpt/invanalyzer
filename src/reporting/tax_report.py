from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Dict, Optional, Sequence

from src.ingestion.normalized import read_normalized_transactions
from src.normalization.transactions import TransactionRecord
from src.positions.transaction_utils import (
    PositionCost,
    apply_transaction_to_position_costs,
    effective_date,
    infer_signed_quantity,
    transaction_value,
)


TAX_EVENT_DESCRIPTIONS = {"sell", "dividend", "account interest"}


@dataclass(frozen=True)
class TaxYearWindow:
    start_date: date
    end_date: date

    def contains(self, value: date) -> bool:
        return self.start_date <= value <= self.end_date


@dataclass(frozen=True)
class TaxEvent:
    owner: str
    account_name: str
    event_date: date
    event_type: str
    symbol: Optional[str]
    description: str
    quantity: Optional[Decimal]
    proceeds: Optional[Decimal]
    cost_basis: Optional[Decimal]
    realized_gain: Decimal
    dividend_amount: Decimal
    interest_amount: Decimal
    reportable_total: Decimal
    source_file: str
    tax_year_start: date
    tax_year_end: date

    def to_dict(self) -> Dict[str, str]:
        return {
            "tax_year_start": self.tax_year_start.isoformat(),
            "tax_year_end": self.tax_year_end.isoformat(),
            "row_type": "event",
            "owner": self.owner,
            "event_date": self.event_date.isoformat(),
            "account_name": self.account_name,
            "event_type": self.event_type,
            "symbol": self.symbol or "",
            "description": self.description,
            "quantity": f"{self.quantity}" if self.quantity is not None else "",
            "proceeds": f"{self.proceeds}" if self.proceeds is not None else "",
            "cost_basis": f"{self.cost_basis}" if self.cost_basis is not None else "",
            "realized_gain": f"{self.realized_gain}",
            "dividend_amount": f"{self.dividend_amount}",
            "interest_amount": f"{self.interest_amount}",
            "reportable_total": f"{self.reportable_total}",
            "source_file": self.source_file,
        }


@dataclass(frozen=True)
class TaxOwnerSummary:
    owner: str
    tax_year_start: date
    tax_year_end: date
    events: tuple[TaxEvent, ...]
    realized_gains_total: Decimal
    dividend_total: Decimal
    interest_total: Decimal
    reportable_total: Decimal

    def total_row(self) -> Dict[str, str]:
        return {
            "tax_year_start": self.tax_year_start.isoformat(),
            "tax_year_end": self.tax_year_end.isoformat(),
            "row_type": "total",
            "owner": self.owner,
            "event_date": "",
            "account_name": "ALL",
            "event_type": "TOTAL",
            "symbol": "",
            "description": "Totals for tax assessment form",
            "quantity": "",
            "proceeds": "",
            "cost_basis": "",
            "realized_gain": f"{self.realized_gains_total}",
            "dividend_amount": f"{self.dividend_total}",
            "interest_amount": f"{self.interest_total}",
            "reportable_total": f"{self.reportable_total}",
            "source_file": "",
        }


def previous_uk_tax_year(today: Optional[date] = None) -> TaxYearWindow:
    as_of = today or date.today()
    current_year_end = date(as_of.year, 4, 5)
    if as_of > current_year_end:
        end_date = current_year_end
    else:
        end_date = date(as_of.year - 1, 4, 5)
    start_date = date(end_date.year - 1, 4, 6)
    return TaxYearWindow(start_date=start_date, end_date=end_date)


def _owner_for_account(account_name: str, owners: Sequence[str]) -> Optional[str]:
    normalized_account_name = account_name.strip().casefold()
    for owner in owners:
        normalized_owner = owner.strip().casefold()
        if not normalized_owner:
            continue
        if normalized_account_name == normalized_owner:
            return normalized_owner
        if normalized_account_name.endswith(f"_{normalized_owner}"):
            return normalized_owner
    return None


def _is_trading_account(account_name: str) -> bool:
    normalized_account_name = account_name.strip().casefold()
    return normalized_account_name == "trading" or normalized_account_name.startswith("trading_")


def _cash_amount(record: TransactionRecord) -> Decimal:
    return (record.credit or Decimal("0")) - (record.debit or Decimal("0"))


def _round_money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"))


def summarize_tax_reports(
    transactions_root: Path,
    *,
    owners: Optional[Sequence[str]] = None,
    today: Optional[date] = None,
) -> Dict[str, TaxOwnerSummary]:
    owners_normalized = [owner.strip().casefold() for owner in (owners or ["kirill", "thiago"]) if owner.strip()]
    tax_year = previous_uk_tax_year(today=today)

    events_by_owner: Dict[str, list[TaxEvent]] = {owner: [] for owner in owners_normalized}
    realized_totals: Dict[str, Decimal] = {owner: Decimal("0") for owner in owners_normalized}
    dividend_totals: Dict[str, Decimal] = {owner: Decimal("0") for owner in owners_normalized}
    interest_totals: Dict[str, Decimal] = {owner: Decimal("0") for owner in owners_normalized}

    for path in sorted(transactions_root.rglob("transactions_normalized.csv")):
        account_name = path.parent.name
        if not _is_trading_account(account_name):
            continue
        owner = _owner_for_account(account_name, owners_normalized)
        if owner is None:
            continue

        transactions = sorted(
            [record for record in read_normalized_transactions(path) if effective_date(record) is not None],
            key=lambda record: effective_date(record),
        )
        positions: Dict[str, PositionCost] = {}
        for record in transactions:
            record_date = effective_date(record)
            if record_date is None:
                continue

            if record.description not in TAX_EVENT_DESCRIPTIONS:
                if record.description in {"buy", "sell"}:
                    apply_transaction_to_position_costs(record, positions)
                continue

            if record.description == "sell":
                realized_gain = apply_transaction_to_position_costs(record, positions)
                if realized_gain is None:
                    continue
                if not tax_year.contains(record_date):
                    continue
                signed_quantity = infer_signed_quantity(record)
                proceeds = transaction_value(record, signed_quantity) if signed_quantity is not None else None
                cost_basis = None
                if proceeds is not None:
                    cost_basis = proceeds - realized_gain
                event = TaxEvent(
                    owner=owner,
                    account_name=account_name,
                    event_date=record_date,
                    event_type="sell",
                    symbol=record.symbol,
                    description="sell of securities",
                    quantity=record.quantity,
                    proceeds=_round_money(proceeds) if proceeds is not None else None,
                    cost_basis=_round_money(cost_basis) if cost_basis is not None else None,
                    realized_gain=_round_money(realized_gain),
                    dividend_amount=Decimal("0.00"),
                    interest_amount=Decimal("0.00"),
                    reportable_total=_round_money(realized_gain),
                    source_file=record.source_file,
                    tax_year_start=tax_year.start_date,
                    tax_year_end=tax_year.end_date,
                )
                events_by_owner[owner].append(event)
                realized_totals[owner] += event.realized_gain
                continue

            if record.description in {"dividend", "account interest"}:
                if not tax_year.contains(record_date):
                    continue
                amount = _round_money(_cash_amount(record))
                dividend_amount = Decimal("0.00")
                interest_amount = Decimal("0.00")
                event_type = record.description
                if record.description == "dividend":
                    dividend_amount = amount
                    dividend_totals[owner] += amount
                else:
                    interest_amount = amount
                    interest_totals[owner] += amount
                event = TaxEvent(
                    owner=owner,
                    account_name=account_name,
                    event_date=record_date,
                    event_type=event_type,
                    symbol=record.symbol,
                    description=record.description,
                    quantity=None,
                    proceeds=None,
                    cost_basis=None,
                    realized_gain=Decimal("0.00"),
                    dividend_amount=dividend_amount,
                    interest_amount=interest_amount,
                    reportable_total=amount,
                    source_file=record.source_file,
                    tax_year_start=tax_year.start_date,
                    tax_year_end=tax_year.end_date,
                )
                events_by_owner[owner].append(event)

    summaries: Dict[str, TaxOwnerSummary] = {}
    for owner in owners_normalized:
        owner_events = sorted(
            events_by_owner.get(owner, []),
            key=lambda event: (event.event_date, event.account_name, event.event_type, event.symbol or ""),
        )
        realized_total = _round_money(realized_totals.get(owner, Decimal("0")))
        dividend_total = _round_money(dividend_totals.get(owner, Decimal("0")))
        interest_total = _round_money(interest_totals.get(owner, Decimal("0")))
        reportable_total = _round_money(realized_total + dividend_total + interest_total)
        summaries[owner] = TaxOwnerSummary(
            owner=owner,
            tax_year_start=tax_year.start_date,
            tax_year_end=tax_year.end_date,
            events=tuple(owner_events),
            realized_gains_total=realized_total,
            dividend_total=dividend_total,
            interest_total=interest_total,
            reportable_total=reportable_total,
        )
    return summaries


def write_tax_reports(
    summaries_by_owner: Dict[str, TaxOwnerSummary],
    output_root: Path,
) -> list[Path]:
    output_root.mkdir(parents=True, exist_ok=True)
    written_paths: list[Path] = []
    fieldnames = [
        "tax_year_start",
        "tax_year_end",
        "row_type",
        "owner",
        "event_date",
        "account_name",
        "event_type",
        "symbol",
        "description",
        "quantity",
        "proceeds",
        "cost_basis",
        "realized_gain",
        "dividend_amount",
        "interest_amount",
        "reportable_total",
        "source_file",
    ]

    for owner, summary in sorted(summaries_by_owner.items()):
        output_path = output_root / f"tax_report_{owner}.csv"
        with output_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for event in summary.events:
                writer.writerow(event.to_dict())
            writer.writerow(summary.total_row())
        written_paths.append(output_path)
    return written_paths
