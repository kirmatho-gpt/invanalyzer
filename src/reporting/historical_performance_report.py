from __future__ import annotations

import csv
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Dict, Iterable, Optional

from src.ingestion.normalized import read_normalized_transactions
from src.normalization.transactions import TransactionRecord
from src.positions.transaction_utils import (
    PositionCost,
    apply_transaction_to_position_costs,
    effective_date,
)
from src.reporting.unrealized_gain_report import (
    collect_holdings_snapshot_dates,
    summarize_unrealized_gains,
)


DISTRIBUTED_CASHFLOW_DESCRIPTIONS = {"dividend", "account interest", "cash advantage"}
FEE_DESCRIPTION = "fees"


@dataclass(frozen=True)
class DistributedCashflowTotals:
    distributed_cashflow: Decimal
    fees: Decimal
    net_of_fees: Decimal


@dataclass(frozen=True)
class HistoricalPerformanceSnapshot:
    account_name: str
    valuation_date: date
    realized_pnl: Decimal
    unrealized_gain: Decimal
    distributed_cashflow: Decimal
    fees: Decimal
    distributed_net_of_fees: Decimal
    nominal_invested: Decimal
    total_pnl: Decimal

    def to_dict(self) -> Dict[str, str]:
        return {
            "account_name": self.account_name,
            "valuation_date": self.valuation_date.isoformat(),
            "realized_pnl": f"{self.realized_pnl}",
            "unrealized_gain": f"{self.unrealized_gain}",
            "distributed_cashflow": f"{self.distributed_cashflow}",
            "fees": f"{self.fees}",
            "distributed_net_of_fees": f"{self.distributed_net_of_fees}",
            "nominal_invested": f"{self.nominal_invested}",
            "total_pnl": f"{self.total_pnl}",
        }


def _round_money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"))


def _cash_amount(record: TransactionRecord) -> Decimal:
    debit = record.debit or Decimal("0")
    credit = record.credit or Decimal("0")
    return credit - debit


def _distributed_totals_by_date(
    transactions: Iterable[TransactionRecord],
    valuation_dates: Iterable[date],
) -> Dict[date, DistributedCashflowTotals]:
    sorted_dates = sorted(set(valuation_dates))
    dated_transactions = [
        (effective_date(record), record)
        for record in transactions
        if effective_date(record) is not None
    ]
    dated_transactions.sort(key=lambda item: item[0])

    distributed_cashflow = Decimal("0")
    fees = Decimal("0")
    results: Dict[date, DistributedCashflowTotals] = {}
    index = 0

    for valuation_date in sorted_dates:
        while index < len(dated_transactions):
            transaction_date, record = dated_transactions[index]
            if transaction_date is None or transaction_date > valuation_date:
                break
            amount = _cash_amount(record)
            if record.description in DISTRIBUTED_CASHFLOW_DESCRIPTIONS:
                distributed_cashflow += amount
            elif record.description == FEE_DESCRIPTION:
                fees += amount
            index += 1
        results[valuation_date] = DistributedCashflowTotals(
            distributed_cashflow=distributed_cashflow,
            fees=fees,
            net_of_fees=distributed_cashflow + fees,
        )
    return results


def _load_transactions_by_account(
    transactions_root: Path,
    account_filter: set[str],
) -> Dict[str, list[TransactionRecord]]:
    transactions_by_account: Dict[str, list[TransactionRecord]] = defaultdict(list)
    for path in sorted(transactions_root.rglob("transactions_normalized.csv")):
        account_name = path.parent.name
        if account_filter and account_name not in account_filter:
            continue
        transactions_by_account[account_name].extend(read_normalized_transactions(path))
    return dict(transactions_by_account)


def _realized_pnl_by_date(
    transactions: Iterable[TransactionRecord],
    valuation_dates: Iterable[date],
) -> Dict[date, Decimal]:
    sorted_dates = sorted(set(valuation_dates))
    sorted_transactions = sorted(
        [record for record in transactions if effective_date(record)],
        key=lambda record: effective_date(record),
    )
    positions: Dict[str, PositionCost] = {}
    realized_pnl = Decimal("0")
    results: Dict[date, Decimal] = {}
    index = 0

    for valuation_date in sorted_dates:
        while index < len(sorted_transactions):
            record = sorted_transactions[index]
            transaction_date = effective_date(record)
            if transaction_date is None or transaction_date > valuation_date:
                break
            if record.description in {"buy", "sell"}:
                gain = apply_transaction_to_position_costs(record, positions)
                if gain is not None:
                    realized_pnl += gain
            index += 1
        results[valuation_date] = realized_pnl
    return results


def summarize_historical_performance(
    transactions_root: Path,
    holdings_root: Path,
    accounts: Optional[Iterable[str]] = None,
) -> list[HistoricalPerformanceSnapshot]:
    account_filter = {name.strip() for name in accounts or [] if name.strip()}
    unrealized_rows = summarize_unrealized_gains(
        transactions_root=transactions_root,
        holdings_root=holdings_root,
        accounts=accounts,
    )

    unrealized_totals: Dict[tuple[str, date], Decimal] = defaultdict(lambda: Decimal("0"))
    nominal_invested_totals: Dict[tuple[str, date], Decimal] = defaultdict(lambda: Decimal("0"))
    valuation_dates_by_account: Dict[str, set[date]] = defaultdict(set)
    for account_name, snapshot_dates in collect_holdings_snapshot_dates(
        holdings_root,
        accounts,
    ).items():
        valuation_dates_by_account[account_name].update(snapshot_dates)
    for row in unrealized_rows:
        key = (row.account_name, row.valuation_date)
        unrealized_totals[key] += row.unrealized_gain
        nominal_invested_totals[key] += row.book_cost
        valuation_dates_by_account[row.account_name].add(row.valuation_date)

    transactions_by_account = _load_transactions_by_account(transactions_root, account_filter)

    snapshots: list[HistoricalPerformanceSnapshot] = []
    zero_totals = DistributedCashflowTotals(
        distributed_cashflow=Decimal("0"),
        fees=Decimal("0"),
        net_of_fees=Decimal("0"),
    )
    for account_name in sorted(valuation_dates_by_account.keys()):
        valuation_dates = valuation_dates_by_account[account_name]
        transactions = transactions_by_account.get(account_name, [])
        distributed_totals_by_date = _distributed_totals_by_date(
            transactions=transactions,
            valuation_dates=valuation_dates,
        )
        realized_pnl_by_date = _realized_pnl_by_date(
            transactions=transactions,
            valuation_dates=valuation_dates,
        )
        for valuation_date in sorted(valuation_dates):
            distributed = distributed_totals_by_date.get(valuation_date, zero_totals)
            realized_pnl = realized_pnl_by_date.get(valuation_date, Decimal("0"))
            key = (account_name, valuation_date)
            unrealized_gain = unrealized_totals[key]
            nominal_invested = nominal_invested_totals[key]
            total_pnl = realized_pnl + unrealized_gain + distributed.net_of_fees
            snapshots.append(
                HistoricalPerformanceSnapshot(
                    account_name=account_name,
                    valuation_date=valuation_date,
                    realized_pnl=_round_money(realized_pnl),
                    unrealized_gain=_round_money(unrealized_gain),
                    distributed_cashflow=_round_money(distributed.distributed_cashflow),
                    fees=_round_money(distributed.fees),
                    distributed_net_of_fees=_round_money(distributed.net_of_fees),
                    nominal_invested=_round_money(nominal_invested),
                    total_pnl=_round_money(total_pnl),
                )
            )

    snapshots.sort(key=lambda row: (row.account_name, row.valuation_date))
    return snapshots


def aggregate_historical_performance(
    rows: Iterable[HistoricalPerformanceSnapshot],
    *,
    account_name: str = "aggregated",
) -> list[HistoricalPerformanceSnapshot]:
    rows_list = list(rows)
    if not rows_list:
        return []

    dates = sorted({row.valuation_date for row in rows_list})
    rows_by_account: Dict[str, Dict[date, HistoricalPerformanceSnapshot]] = defaultdict(dict)
    for row in rows_list:
        rows_by_account[row.account_name][row.valuation_date] = row

    totals_by_date: Dict[date, Dict[str, Decimal]] = {
        valuation_date: {
            "realized_pnl": Decimal("0"),
            "unrealized_gain": Decimal("0"),
            "distributed_cashflow": Decimal("0"),
            "fees": Decimal("0"),
            "distributed_net_of_fees": Decimal("0"),
            "nominal_invested": Decimal("0"),
            "total_pnl": Decimal("0"),
        }
        for valuation_date in dates
    }

    for snapshots_by_date in rows_by_account.values():
        latest_snapshot: Optional[HistoricalPerformanceSnapshot] = None
        for valuation_date in dates:
            snapshot = snapshots_by_date.get(valuation_date)
            if snapshot is not None:
                latest_snapshot = snapshot
            if latest_snapshot is None:
                continue
            totals = totals_by_date[valuation_date]
            totals["realized_pnl"] += latest_snapshot.realized_pnl
            totals["unrealized_gain"] += latest_snapshot.unrealized_gain
            totals["distributed_cashflow"] += latest_snapshot.distributed_cashflow
            totals["fees"] += latest_snapshot.fees
            totals["distributed_net_of_fees"] += latest_snapshot.distributed_net_of_fees
            totals["nominal_invested"] += latest_snapshot.nominal_invested
            totals["total_pnl"] += latest_snapshot.total_pnl

    aggregated_rows: list[HistoricalPerformanceSnapshot] = []
    for valuation_date in dates:
        totals = totals_by_date[valuation_date]
        aggregated_rows.append(
            HistoricalPerformanceSnapshot(
                account_name=account_name,
                valuation_date=valuation_date,
                realized_pnl=_round_money(totals["realized_pnl"]),
                unrealized_gain=_round_money(totals["unrealized_gain"]),
                distributed_cashflow=_round_money(totals["distributed_cashflow"]),
                fees=_round_money(totals["fees"]),
                distributed_net_of_fees=_round_money(totals["distributed_net_of_fees"]),
                nominal_invested=_round_money(totals["nominal_invested"]),
                total_pnl=_round_money(totals["total_pnl"]),
            )
        )
    return aggregated_rows


def _performance_pct_as_string(snapshot: HistoricalPerformanceSnapshot) -> str:
    if snapshot.nominal_invested == 0:
        return ""
    performance_pct = (snapshot.total_pnl / snapshot.nominal_invested) * Decimal("100")
    return f"{_round_money(performance_pct)}"


def build_historical_performance_table(
    rows: Iterable[HistoricalPerformanceSnapshot],
) -> tuple[list[str], list[dict[str, str]]]:
    rows_by_date = aggregate_historical_performance(rows, account_name="")
    if not rows_by_date:
        return ["valuation_date"], []

    fieldnames = [
        "valuation_date",
        "total_pnl",
        "realized_pnl",
        "unrealized_gain",
        "distributed_cashflow",
        "fees",
        "distributed_net_of_fees",
        "nominal_invested",
        "performance_pct",
    ]
    table_rows = [
        {
            "valuation_date": snapshot.valuation_date.isoformat(),
            "total_pnl": f"{snapshot.total_pnl}",
            "realized_pnl": f"{snapshot.realized_pnl}",
            "unrealized_gain": f"{snapshot.unrealized_gain}",
            "distributed_cashflow": f"{snapshot.distributed_cashflow}",
            "fees": f"{snapshot.fees}",
            "distributed_net_of_fees": f"{snapshot.distributed_net_of_fees}",
            "nominal_invested": f"{snapshot.nominal_invested}",
            "performance_pct": _performance_pct_as_string(snapshot),
        }
        for snapshot in rows_by_date
    ]
    return fieldnames, table_rows


def write_historical_performance_report(
    rows: Iterable[HistoricalPerformanceSnapshot],
    output_path: Path,
) -> None:
    fieldnames, table_rows = build_historical_performance_table(rows)
    if not table_rows:
        return
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in table_rows:
            writer.writerow(row)


def _sanitize_filename_token(value: str) -> str:
    sanitized = "".join(char if char.isalnum() or char in {"-", "_"} else "_" for char in value.strip())
    sanitized = sanitized.strip("_")
    return sanitized or "account"


def _resolve_output_layout(output_path: Path) -> tuple[Path, str]:
    if output_path.suffix.lower() == ".csv":
        return output_path.parent, output_path.stem
    return output_path, "historical_performance"


def write_historical_performance_reports(
    rows: Iterable[HistoricalPerformanceSnapshot],
    output_path: Path,
) -> list[Path]:
    rows_list = list(rows)
    if not rows_list:
        return []

    output_dir, base_name = _resolve_output_layout(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    written_paths: list[Path] = []
    account_names = sorted({row.account_name for row in rows_list})
    for account_name in account_names:
        account_rows = [row for row in rows_list if row.account_name == account_name]
        account_output_path = output_dir / f"{base_name}_{_sanitize_filename_token(account_name)}.csv"
        write_historical_performance_report(account_rows, account_output_path)
        written_paths.append(account_output_path)

    aggregated_rows = aggregate_historical_performance(rows_list, account_name="aggregated")
    aggregated_output_path = output_dir / f"{base_name}_aggregated.csv"
    write_historical_performance_report(aggregated_rows, aggregated_output_path)
    written_paths.append(aggregated_output_path)
    return written_paths
