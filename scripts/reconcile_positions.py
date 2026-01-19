#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

# Add project root to sys.path so 'src' can be imported when running directly
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import argparse
from datetime import date

from src.positions.reconcile import reconcile_positions


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


def reconcile_root(normalized_root: Path) -> list[str]:
    mismatches: list[str] = []
    for holdings_path in _find_holdings_files(normalized_root):
        transactions_path = holdings_path.parent / "transactions_normalized.csv"
        if not transactions_path.exists():
            mismatches.append(
                f"{holdings_path}: missing transactions_normalized.csv in {holdings_path.parent}"
            )
            continue
        valuation_date = _extract_valuation_date(holdings_path)
        diffs = reconcile_positions(transactions_path, holdings_path, valuation_date)
        for diff in diffs:
            mismatches.append(
                " | ".join(
                    [
                        f"account={diff.account_name}",
                        f"date={diff.valuation_date.isoformat()}",
                        f"symbol={diff.symbol}",
                        f"holdings={diff.holdings_quantity}",
                        f"transactions={diff.transaction_quantity}",
                        f"delta={diff.delta}",
                    ]
                )
            )
    return mismatches


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

    mismatches = reconcile_root(args.normalized_root)
    if mismatches:
        print("Position reconciliation mismatches found:")
        for mismatch in mismatches:
            print(f"- {mismatch}")
        raise SystemExit(1)

    print("Positions match normalized holdings snapshots.")


if __name__ == "__main__":
    main()
