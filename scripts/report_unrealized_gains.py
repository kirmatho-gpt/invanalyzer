#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Add project root to sys.path so 'src' can be imported when running directly
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.reporting.unrealized_gain_report import (
    summarize_unrealized_gains,
    write_unrealized_gain_reports,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Generate unrealized gain/loss reports for each holdings snapshot "
            "using transaction-derived book cost and holdings market values."
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
        "--holdings",
        dest="holdings_root",
        type=Path,
        required=True,
        help="Root directory containing normalized holdings snapshot CSVs.",
    )
    parser.add_argument(
        "--output",
        dest="output_root",
        type=Path,
        required=True,
        help="Directory to write unrealized gain reports.",
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
    args = parser.parse_args()

    rows = summarize_unrealized_gains(
        args.transactions_root,
        args.holdings_root,
        accounts=args.accounts,
    )
    write_unrealized_gain_reports(rows, args.output_root)


if __name__ == "__main__":
    main()