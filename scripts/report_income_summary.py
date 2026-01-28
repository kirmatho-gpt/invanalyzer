#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Add project root to sys.path so 'src' can be imported when running directly
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.reporting.income_report  import summarize_income, write_income_report


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Generate a monthly income report (dividends, interest, fees) "
            "across all portfolios from normalized transactions."
        )
    )
    parser.add_argument(
        "--transactions",
        dest="transactions_root",
        type=Path,
        required=True,
        help="Root directory containing normalized transaction CSVs.",
    )
    parser.add_argument(
        "--accounts",
        nargs="*",
        default=None,
        help=(
            "Optional list of account names to include in the report. "
            "When omitted, all accounts are included."
        ),
    )
    parser.add_argument(
        "--output",
        dest="output_path",
        type=Path,
        required=True,
        help="Path to write the aggregated income report CSV.",
    )
    args = parser.parse_args()

    summary = summarize_income(args.transactions_root, accounts=args.accounts)
    write_income_report(summary, args.output_path)


if __name__ == "__main__":
    main()