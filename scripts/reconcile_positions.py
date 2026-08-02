#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

# Add project root to sys.path so 'src' can be imported when running directly
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import argparse
from dataclasses import dataclass
from datetime import date

from src.positions.reconcile import (
    PendingSettlementDifference,
    PositionMismatch,
    reconcile_positions_detailed,
)


@dataclass(frozen=True)
class ReconciliationMessages:
    mismatches: list[str]
    pending_settlements: list[str]


def _extract_valuation_date(path: Path) -> date:
    stem = path.stem
    tokens = stem.split("_")
    for token in tokens:
        try:
            return date.fromisoformat(token)
        except ValueError:
            continue
    raise ValueError(f"Unable to find valuation date in {path.name}")


def _find_holdings_files(root: Path) -> list[Path]:
    return sorted(root.glob("**/holdings_*_normalized.csv"))


def _format_mismatch(diff: PositionMismatch) -> str:
    return " | ".join(
        [
            f"account={diff.account_name}",
            f"date={diff.valuation_date.isoformat()}",
            f"symbol={diff.symbol}",
            f"holdings={diff.holdings_quantity}",
            f"transactions={diff.transaction_quantity}",
            f"delta={diff.delta}",
        ]
    )


def _format_pending_settlement(diff: PendingSettlementDifference) -> str:
    pending = "; ".join(
        (
            f"{transaction.description} {transaction.quantity} "
            f"trade_date={transaction.trade_date.isoformat()} "
            f"settlement_date={transaction.settlement_date.isoformat()}"
        )
        for transaction in diff.pending_transactions
    )
    return " | ".join(
        [
            f"account={diff.account_name}",
            f"date={diff.valuation_date.isoformat()}",
            f"symbol={diff.symbol}",
            f"holdings={diff.holdings_quantity}",
            f"transactions={diff.transaction_quantity}",
            f"delta={diff.delta}",
            f"pending={pending}",
        ]
    )


def reconcile_root_detailed(normalized_root: Path) -> ReconciliationMessages:
    mismatches: list[str] = []
    pending_settlements: list[str] = []
    for holdings_path in _find_holdings_files(normalized_root):
        transactions_path = holdings_path.parent / "transactions_normalized.csv"
        if not transactions_path.exists():
            mismatches.append(
                f"{holdings_path}: missing transactions_normalized.csv in {holdings_path.parent}"
            )
            continue
        valuation_date = _extract_valuation_date(holdings_path)
        result = reconcile_positions_detailed(
            transactions_path,
            holdings_path,
            valuation_date,
        )
        for diff in result.pending_settlements:
            pending_settlements.append(_format_pending_settlement(diff))
        for diff in result.mismatches:
            mismatches.append(_format_mismatch(diff))
    return ReconciliationMessages(
        mismatches=mismatches,
        pending_settlements=pending_settlements,
    )


def reconcile_root(normalized_root: Path) -> list[str]:
    return reconcile_root_detailed(normalized_root).mismatches


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reconcile normalized holdings files against positions built from transactions."
    )
    parser.add_argument(
        "--normalized-root",
        type=Path,
        required=True,
        help="Root directory containing normalized account folders.",
    )
    args = parser.parse_args()

    result = reconcile_root_detailed(args.normalized_root)
    if result.pending_settlements:
        print("Pending settlement reconciliation differences found:")
        for pending_settlement in result.pending_settlements:
            print(f"- {pending_settlement}")

    if result.mismatches:
        print("Position reconciliation mismatches found:")
        for mismatch in result.mismatches:
            print(f"- {mismatch}")
        raise SystemExit(1)

    print("No unexplained position reconciliation mismatches found.")


if __name__ == "__main__":
    main()
