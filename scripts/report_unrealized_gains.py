#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Add project root to sys.path so 'src' can be imported when running directly
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.reporting.unrealized_gain_report import (
    summarize_unrealized_gains,
    write_combined_unrealized_gain_report,
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
    parser.add_argument(
        "--combined-output",
        dest="combined_output",
        type=Path,
        default=None,
        help=(
            "Optional path to write a single combined unrealized gains report "
            "across all accounts."
        ),
    )
    parser.add_argument(
        "--combined-all-dates",
        action="store_true",
        help=(
            "Include all valuation dates in the combined report. "
            "By default, only the latest holdings per account are included."
        ),
    )
    args = parser.parse_args()

    rows = summarize_unrealized_gains(
        args.transactions_root,
        args.holdings_root,
        accounts=args.accounts,
    )
    write_unrealized_gain_reports(rows, args.output_root)
    if args.combined_output:
        write_combined_unrealized_gain_report(
            rows,
            args.combined_output,
            latest_only=not args.combined_all_dates,
        )


if __name__ == "__main__":
    main()
