#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Add project root to sys.path so 'src' can be imported when running directly
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.reporting.tax_report import summarize_tax_reports, write_tax_reports


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Generate tax reports by owner for the previous UK tax year. "
            "Includes sell events with realized gains plus dividend and account interest events "
            "from trading accounts only."
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
        "--output",
        dest="output_root",
        type=Path,
        required=True,
        help="Directory to write owner tax reports (tax_report_<owner>.csv).",
    )
    parser.add_argument(
        "--owners",
        nargs="*",
        default=["kirill", "thiago"],
        help=(
            "Owners to generate tax reports for. "
            "Only trading accounts are considered; owner is matched by suffix, e.g. 'trading_kirill'."
        ),
    )
    args = parser.parse_args()

    summaries = summarize_tax_reports(
        transactions_root=args.transactions_root,
        owners=args.owners,
    )
    write_tax_reports(summaries, args.output_root)


if __name__ == "__main__":
    main()
