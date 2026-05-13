#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Add project root to sys.path so 'src' can be imported when running directly
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.reporting.historical_performance_report import (
    summarize_historical_performance,
    write_historical_performance_reports,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Generate historical performance reports by holdings snapshot date, "
            "combining unrealized gain and distributed cashflows net of fees."
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
        dest="output_path",
        type=Path,
        required=True,
        help=(
            "Output directory or CSV base path. "
            "Creates one report per account plus one aggregated report."
        ),
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

    rows = summarize_historical_performance(
        transactions_root=args.transactions_root,
        holdings_root=args.holdings_root,
        accounts=args.accounts,
    )
    write_historical_performance_reports(rows, args.output_path)


if __name__ == "__main__":
    main()
