#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path
# Add project root to sys.path so 'src' can be imported when running directly
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import argparse
import csv
import re
from datetime import datetime
from typing import Dict, Iterable, List, Tuple

from src.config import load_account_brokers
from src.ingestion.ii import parse_ii_holdings
from src.normalization.holdings import HoldingRecord


DATE_PATTERNS = (
    (re.compile(r"\d{4}-\d{2}-\d{2}"), "%Y-%m-%d"),
    (re.compile(r"\d{8}"), "%Y%m%d"),
    (re.compile(r"\d{2}-\d{2}-\d{4}"), "%d-%m-%Y"),
)


def _find_holding_files(root: Path) -> List[Path]:
    patterns = ["holdings_*_*.csv", "holdings_*_*.txt"]
    files = []
    for pattern in patterns:
        files.extend(root.glob(pattern)) 
    return sorted(files)


def _extract_date_from_stem(stem: str) -> Tuple[datetime.date, str]:
    for pattern, date_format in DATE_PATTERNS:
        match = pattern.search(stem)
        if match:
            return datetime.strptime(match.group(0), date_format).date(), match.group(0)
    raise ValueError(f"Could not find valuation date in filename: {stem}")


def _extract_account_name(path: Path) -> Tuple[str, datetime.date]:
    stem = path.stem
    if not stem.startswith("holdings_"):
        raise ValueError(f"Unexpected holdings filename: {path.name}")
    valuation_date, date_token = _extract_date_from_stem(stem)
    account_part = stem[len("holdings_") :]
    account_name = account_part.replace(date_token, "").strip("_")
    if not account_name:
        raise ValueError(f"Unable to determine account name from filename: {path.name}")
    return account_name, valuation_date


def _broker_for_account(account_name: str, mapping: Dict[str, str]) -> str:
    return mapping.get(account_name) or mapping.get("*") or "ii"


def _parse_holdings(
    path: Path,
    broker: str,
    account_name: str,
    valuation_date: datetime.date,
) -> Iterable[HoldingRecord]:
    if broker == "ii":
        return parse_ii_holdings(
            path,
            account_name=account_name,
            broker=broker,
            valuation_date=valuation_date,
        )
    raise ValueError(f"Unsupported broker: {broker}")


def _write_holdings(records: Iterable[HoldingRecord], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    records_list = list(records)
    if not records_list:
        return
    records_list.sort(key=lambda r: (r.symbol or "", r.name or ""))
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(records_list[0].to_dict().keys()))
        writer.writeheader()
        for record in records_list:
            writer.writerow(record.to_dict())


def normalize_holdings(raw_root: Path, output_root: Path, config_path: Path) -> None:
    account_brokers = load_account_brokers(config_path)
    seen_ids = set()
    grouped: Dict[Tuple[str, datetime.date], List[HoldingRecord]] = {}

    for path in _find_holding_files(raw_root):
        account_name, valuation_date = _extract_account_name(path)
        broker = _broker_for_account(account_name, account_brokers)
        for record in _parse_holdings(
            path,
            broker=broker,
            account_name=account_name,
            valuation_date=valuation_date,
        ):
            if record.snapshot_id in seen_ids:
                continue
            seen_ids.add(record.snapshot_id)
            grouped.setdefault((account_name, valuation_date), []).append(record)

    for (account_name, valuation_date), records in grouped.items():
        output_path = (
            output_root
            / account_name
            / f"holdings_{valuation_date.isoformat()}_normalized.csv"
        )
        _write_holdings(records, output_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize broker holdings snapshot files.")
    parser.add_argument(
        "--input",
        dest="input_root",
        type=Path,
        required=True,
        help="Directory containing raw holdings snapshot files (outside the repo).",
    )
    parser.add_argument(
        "--output",
        dest="output_root",
        type=Path,
        required=True,
        help="Directory to write normalized holdings files (outside the repo).",
    )
    parser.add_argument(
        "--config",
        dest="config_path",
        type=Path,
        default=Path("src/config/accounts.json"),
        help="Path to account-to-broker config JSON.",
    )
    args = parser.parse_args()
    normalize_holdings(args.input_root, args.output_root, args.config_path)


if __name__ == "__main__":
    main()
