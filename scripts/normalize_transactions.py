#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path
# Add project root to sys.path so 'src' can be imported when running directly
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import argparse
import csv
from pathlib import Path
from typing import Dict, Iterable, List

from src.config import load_account_brokers
from src.ingestion.ii import parse_ii_transactions
from src.ingestion.hsbc import parse_hsbc_transactions
from src.normalization.transactions import TransactionRecord


def _find_transaction_files(root: Path) -> List[Path]:
    patterns = ["transactions_*_*.csv", "transactions_*_*.txt"]
    files = []
    for pattern in patterns:
        files.extend(root.glob(pattern)) 
    return sorted(files)


def _extract_account_name(path: Path) -> str:
    stem = path.stem
    parts = stem.split("_")
    if len(parts) < 3:
        raise ValueError(f"Unexpected transaction filename: {path.name}")
    return "_".join(parts[1:-1])


def _broker_for_account(account_name: str, mapping: Dict[str, str]) -> str:
    return mapping.get(account_name) or mapping.get("*") or "ii"


def _parse_transactions(path: Path, broker: str, account_name: str) -> Iterable[TransactionRecord]:
    if broker == "ii":
        return parse_ii_transactions(path, account_name=account_name, broker=broker)
    elif broker == "hsbc":
        return parse_hsbc_transactions(path, account_name=account_name, broker=broker)
    raise ValueError(f"Unsupported broker: {broker}")


def _write_transactions(records: Iterable[TransactionRecord], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    records_list = list(records)
    if not records_list:
        return
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(records_list[0].to_dict().keys()))
        writer.writeheader()
        for record in records_list:
            writer.writerow(record.to_dict())


def normalize_transactions(raw_root: Path, output_root: Path, config_path: Path) -> None:
    account_brokers = load_account_brokers(config_path)
    seen_ids = set()
    grouped: Dict[str, List[TransactionRecord]] = {}

    for path in _find_transaction_files(raw_root):
        account_name = _extract_account_name(path)
        broker = _broker_for_account(account_name, account_brokers)
        for record in _parse_transactions(path, broker=broker, account_name=account_name):
            if record.transaction_id in seen_ids:
                continue
            seen_ids.add(record.transaction_id)
            grouped.setdefault(account_name, []).append(record)
    
    for account_name in grouped:
        grouped[account_name].sort(key=lambda r: r.trade_date)

    for account_name, records in grouped.items():
        output_path = output_root / account_name / "transactions_normalized.csv"
        _write_transactions(records, output_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize broker transaction files.")
    parser.add_argument(
        "--input",
        dest="input_root",
        type=Path,
        required=True,
        help="Directory containing raw transaction files (outside the repo).",
    )
    parser.add_argument(
        "--output",
        dest="output_root",
        type=Path,
        required=True,
        help="Directory to write normalized transaction files (outside the repo).",
    )
    parser.add_argument(
        "--config",
        dest="config_path",
        type=Path,
        default=Path("src/config/accounts.json"),
        help="Path to account-to-broker config JSON.",
    )
    args = parser.parse_args()
    normalize_transactions(args.input_root, args.output_root, args.config_path)


if __name__ == "__main__":
    main()
