#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Dict, Iterable, Sequence

# Add project root to sys.path so 'src' can be imported when running directly
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.config import load_account_brokers
from src.ingestion.normalized import read_normalized_holdings
from src.reporting.historical_performance_report import (
    summarize_historical_performance,
    write_historical_performance_reports,
)
from src.reporting.income_report import summarize_income, write_income_report
from src.reporting.tax_report import summarize_tax_reports, write_tax_reports
from src.reporting.unrealized_gain_report import (
    collect_holdings_snapshot_dates,
    summarize_unrealized_gains,
    write_combined_unrealized_gain_report,
    write_unrealized_gain_reports,
)

from scripts.normalize_holdings import (
    _extract_account_name as _extract_holding_account_name,
    _extract_date_from_stem,
    _find_holding_files,
    _write_holdings,
    normalize_holdings,
)
from scripts.normalize_transactions import (
    _find_transaction_files,
    normalize_transactions,
)
from scripts.reconcile_positions import reconcile_root_detailed


def _extract_valuation_date_from_holdings_filename(path: Path) -> date:
    stem = path.stem
    for token in stem.split("_"):
        try:
            return date.fromisoformat(token)
        except ValueError:
            continue
    raise ValueError(f"Unable to find valuation date in holdings filename: {path.name}")


def _collect_holdings_dates_by_account(
    normalized_root: Path,
    *,
    accounts: set[str] | None = None,
) -> dict[str, set[date]]:
    dates_by_account: dict[str, set[date]] = defaultdict(set)
    for path in sorted(normalized_root.rglob("holdings_*_normalized.csv")):
        account_name = path.parent.name
        if accounts and account_name not in accounts:
            continue
        valuation_date = _extract_valuation_date_from_holdings_filename(path)
        dates_by_account[account_name].add(valuation_date)
    return dict(dates_by_account)


def _collect_raw_holdings_dates_by_account(
    holdings_root: Path,
    *,
    accounts: set[str] | None = None,
) -> dict[str, set[date]]:
    dates_by_account: dict[str, set[date]] = defaultdict(set)
    for path in _find_holding_files(holdings_root):
        account_name, valuation_date = _extract_holding_account_name(path)
        if accounts and account_name not in accounts:
            continue
        dates_by_account[account_name].add(valuation_date)
    return dict(dates_by_account)


def _extract_transaction_account_and_date(path: Path) -> tuple[str, date]:
    stem = path.stem
    if not stem.startswith("transactions_"):
        raise ValueError(f"Unexpected transaction filename: {path.name}")
    transaction_date, date_token = _extract_date_from_stem(stem)
    account_part = stem[len("transactions_") :]
    account_name = account_part.replace(date_token, "").strip("_")
    if not account_name:
        raise ValueError(f"Unable to determine account name from filename: {path.name}")
    return account_name, transaction_date


def _collect_raw_transaction_dates_by_account(
    transactions_root: Path,
    *,
    accounts: set[str] | None = None,
) -> dict[str, set[date]]:
    dates_by_account: dict[str, set[date]] = defaultdict(set)
    for path in _find_transaction_files(transactions_root):
        account_name, transaction_date = _extract_transaction_account_and_date(path)
        if accounts and account_name not in accounts:
            continue
        dates_by_account[account_name].add(transaction_date)
    return dict(dates_by_account)


def _potentially_missing_raw_file_warnings(
    transaction_dates_by_account: dict[str, set[date]],
    holdings_dates_by_account: dict[str, set[date]],
) -> list[str]:
    warnings: list[str] = []
    accounts = sorted(
        set(transaction_dates_by_account.keys()) | set(holdings_dates_by_account.keys())
    )

    for account_name in accounts:
        transaction_dates = transaction_dates_by_account.get(account_name, set())
        holdings_dates = holdings_dates_by_account.get(account_name, set())
        for snapshot_date in sorted(transaction_dates - holdings_dates):
            warnings.append(
                (
                    f"Potentially missing holdings file for account '{account_name}' "
                    f"on {snapshot_date.isoformat()}: transactions file is present, "
                    "but no holdings file was found. Treating the portfolio snapshot as empty."
                )
            )
        for transaction_date in sorted(holdings_dates - transaction_dates):
            warnings.append(
                (
                    f"Potentially missing transactions file for account '{account_name}' "
                    f"on {transaction_date.isoformat()}: holdings file is present, "
                    "but no transactions file was found. Treating this as no new transactions."
                )
            )

    return warnings


def _write_empty_holdings_snapshots_for_missing_raw_holdings(
    normalized_root: Path,
    transaction_dates_by_account: dict[str, set[date]],
    holdings_dates_by_account: dict[str, set[date]],
) -> list[tuple[str, date]]:
    created: list[tuple[str, date]] = []
    for account_name, transaction_dates in sorted(transaction_dates_by_account.items()):
        missing_holdings_dates = sorted(
            transaction_dates - holdings_dates_by_account.get(account_name, set())
        )
        for snapshot_date in missing_holdings_dates:
            output_path = (
                normalized_root
                / account_name
                / f"holdings_{snapshot_date.isoformat()}_normalized.csv"
            )
            _write_holdings([], output_path)
            created.append((account_name, snapshot_date))
    return created


def _merge_dates_by_account(
    first: dict[str, set[date]],
    second: dict[str, set[date]],
) -> dict[str, set[date]]:
    merged = {account: set(snapshot_dates) for account, snapshot_dates in first.items()}
    for account, snapshot_dates in second.items():
        merged.setdefault(account, set()).update(snapshot_dates)
    return merged


def _majority_snapshot_gap_warnings(
    dates_by_account: dict[str, set[date]],
    *,
    coverage_threshold: float = 0.5,
) -> list[str]:
    accounts = sorted(dates_by_account.keys())
    if len(accounts) < 2:
        return []

    total_accounts = len(accounts)
    warnings: list[str] = []
    all_dates = sorted({snapshot_date for dates in dates_by_account.values() for snapshot_date in dates})

    for snapshot_date in all_dates:
        present_accounts = [account for account in accounts if snapshot_date in dates_by_account.get(account, set())]
        coverage = len(present_accounts) / total_accounts
        if coverage <= coverage_threshold:
            continue

        missing_accounts = [account for account in accounts if snapshot_date not in dates_by_account.get(account, set())]
        if not missing_accounts:
            continue

        warnings.append(
            (
                f"Majority snapshot gap on {snapshot_date.isoformat()}: "
                f"missing={', '.join(missing_accounts)}; "
                f"present={', '.join(present_accounts)}"
            )
        )

    return warnings


def _ii_common_snapshot_warnings(
    dates_by_account: dict[str, set[date]],
    ii_accounts: Sequence[str],
) -> list[str]:
    ii_accounts_set = {account for account in ii_accounts}
    if len(ii_accounts_set) < 2:
        return []

    ii_dates_by_account: dict[str, set[date]] = {
        account: dates_by_account.get(account, set()) for account in sorted(ii_accounts_set)
    }
    union_dates = sorted({snapshot_date for dates in ii_dates_by_account.values() for snapshot_date in dates})
    if not union_dates:
        return [f"II snapshot coverage gap: no holdings snapshots found for II accounts ({', '.join(sorted(ii_accounts_set))})."]

    warnings: list[str] = []
    for snapshot_date in union_dates:
        missing_accounts = [
            account for account, dates in ii_dates_by_account.items() if snapshot_date not in dates
        ]
        if not missing_accounts:
            continue
        present_accounts = [
            account for account, dates in ii_dates_by_account.items() if snapshot_date in dates
        ]
        warnings.append(
            (
                f"II common snapshot missing on {snapshot_date.isoformat()}: "
                f"missing={', '.join(missing_accounts)}; "
                f"present={', '.join(present_accounts)}"
            )
        )
    return warnings


def _missing_transaction_file_warnings(
    normalized_root: Path,
    expected_accounts: Iterable[str],
) -> list[str]:
    warnings: list[str] = []
    for account_name in sorted(set(expected_accounts)):
        path = normalized_root / account_name / "transactions_normalized.csv"
        if not path.exists():
            warnings.append(
                f"Missing normalized transactions file for account '{account_name}': {path}"
            )
    return warnings


def _missing_holdings_snapshot_warnings(
    dates_by_account: dict[str, set[date]],
    expected_accounts: Iterable[str],
) -> list[str]:
    warnings: list[str] = []
    for account_name in sorted(set(expected_accounts)):
        if dates_by_account.get(account_name):
            continue
        warnings.append(
            f"Missing holdings snapshots for account '{account_name}'"
        )
    return warnings


def _holdings_valuation_date_mismatch_warnings(
    normalized_root: Path,
    *,
    accounts: set[str] | None = None,
) -> list[str]:
    warnings: list[str] = []
    for path in sorted(normalized_root.rglob("holdings_*_normalized.csv")):
        account_name = path.parent.name
        if accounts and account_name not in accounts:
            continue

        filename_date = _extract_valuation_date_from_holdings_filename(path)
        row_dates = {
            holding.valuation_date
            for holding in read_normalized_holdings(path)
            if holding.valuation_date != filename_date
        }
        if not row_dates:
            continue

        warnings.append(
            (
                f"Holdings valuation date mismatch for account '{account_name}': "
                f"{path.name} is dated {filename_date.isoformat()} by filename "
                f"but contains row valuation_date values "
                f"{', '.join(snapshot_date.isoformat() for snapshot_date in sorted(row_dates))}. "
                "Please check the raw holdings file name against the broker valuation date."
            )
        )
    return warnings


def _print_warning_block(title: str, warnings: list[str]) -> None:
    if not warnings:
        return
    line = "!" * 92
    print(line)
    print(f"WARNING: {title}")
    for warning in warnings:
        print(f"- {warning}")
    print(line)


def _ii_accounts_from_config(
    account_brokers: Dict[str, str],
    account_filter: set[str],
) -> list[str]:
    ii_accounts = [
        account_name
        for account_name, broker in account_brokers.items()
        if account_name != "*" and broker == "ii"
    ]
    if account_filter:
        ii_accounts = [account for account in ii_accounts if account in account_filter]
    return sorted(ii_accounts)


def run_full_pipeline(
    *,
    transactions_input_root: Path,
    holdings_input_root: Path,
    normalized_root: Path,
    reports_root: Path,
    config_path: Path,
    accounts: Sequence[str] | None = None,
    tax_report_owners: Sequence[str] | None = None,
) -> None:
    account_filter = {name.strip() for name in (accounts or []) if name.strip()}
    account_brokers = load_account_brokers(config_path)
    configured_accounts = sorted(
        account_name for account_name in account_brokers.keys() if account_name != "*"
    )
    filtered_accounts = [
        name for name in configured_accounts if not account_filter or name in account_filter
    ]
    expected_accounts = sorted(account_filter) if account_filter else filtered_accounts

    print("Step 1/6: Normalize transactions")
    normalize_transactions(transactions_input_root, normalized_root, config_path)

    print("Step 2/6: Normalize holdings")
    normalize_holdings(holdings_input_root, normalized_root, config_path)

    print("Step 3/6: Validate snapshot coverage and log gaps")
    raw_transaction_dates_by_account = _collect_raw_transaction_dates_by_account(
        transactions_input_root,
        accounts=account_filter or None,
    )
    raw_holdings_dates_by_account = _collect_raw_holdings_dates_by_account(
        holdings_input_root,
        accounts=account_filter or None,
    )
    _write_empty_holdings_snapshots_for_missing_raw_holdings(
        normalized_root,
        raw_transaction_dates_by_account,
        raw_holdings_dates_by_account,
    )
    normalized_holdings_dates_by_account = _collect_holdings_dates_by_account(
        normalized_root,
        accounts=account_filter or None,
    )
    holdings_dates_by_account = _merge_dates_by_account(
        normalized_holdings_dates_by_account,
        raw_holdings_dates_by_account,
    )
    for account_name in expected_accounts:
        holdings_dates_by_account.setdefault(account_name, set())
    _print_warning_block(
        "Potentially missing raw files",
        _potentially_missing_raw_file_warnings(
            raw_transaction_dates_by_account,
            raw_holdings_dates_by_account,
        ),
    )
    _print_warning_block(
        "Missing normalized transactions",
        _missing_transaction_file_warnings(normalized_root, expected_accounts),
    )
    _print_warning_block(
        "Missing normalized holdings snapshots",
        _missing_holdings_snapshot_warnings(holdings_dates_by_account, expected_accounts),
    )
    _print_warning_block(
        "Majority holdings snapshot gaps across accounts",
        _majority_snapshot_gap_warnings(holdings_dates_by_account),
    )
    _print_warning_block(
        "II common snapshot gaps",
        _ii_common_snapshot_warnings(
            holdings_dates_by_account,
            _ii_accounts_from_config(account_brokers, account_filter),
        ),
    )
    _print_warning_block(
        "Holdings valuation date mismatches",
        _holdings_valuation_date_mismatch_warnings(
            normalized_root,
            accounts=account_filter or None,
        ),
    )

    print("Step 4/6: Reconcile holdings positions against transactions")
    reconcile_roots = [normalized_root]
    if account_filter:
        reconcile_roots = [normalized_root / account_name for account_name in sorted(account_filter)]
    reconciliation_mismatches: list[str] = []
    pending_settlement_differences: list[str] = []
    for root in reconcile_roots:
        if not root.exists():
            continue
        reconciliation_result = reconcile_root_detailed(root)
        reconciliation_mismatches.extend(reconciliation_result.mismatches)
        pending_settlement_differences.extend(
            reconciliation_result.pending_settlements
        )
    _print_warning_block(
        "Pending settlement reconciliation differences",
        pending_settlement_differences,
    )
    _print_warning_block(
        "Position reconciliation mismatches",
        reconciliation_mismatches,
    )

    print("Step 5/7: Generate income and unrealized gain reports")
    reports_root.mkdir(parents=True, exist_ok=True)
    income_rows = summarize_income(normalized_root, accounts=accounts)
    write_income_report(income_rows, reports_root / "income_report.csv")

    unrealized_rows = summarize_unrealized_gains(
        transactions_root=normalized_root,
        holdings_root=normalized_root,
        accounts=accounts,
    )
    snapshot_dates_by_account = collect_holdings_snapshot_dates(
        normalized_root,
        accounts=accounts,
    )
    write_unrealized_gain_reports(
        unrealized_rows,
        reports_root / "unrealized",
        snapshot_dates_by_account=snapshot_dates_by_account,
    )
    write_combined_unrealized_gain_report(
        unrealized_rows,
        reports_root / "unrealized_gains_report.csv",
        latest_only=True,
        snapshot_dates_by_account=snapshot_dates_by_account,
    )

    print("Step 6/7: Generate historical performance reports")
    historical_rows = summarize_historical_performance(
        transactions_root=normalized_root,
        holdings_root=normalized_root,
        accounts=accounts,
    )
    write_historical_performance_reports(
        historical_rows,
        reports_root / "historical_performance_report.csv",
    )

    print("Step 7/7: Generate tax reports")
    tax_summaries = summarize_tax_reports(
        transactions_root=normalized_root,
        owners=tax_report_owners or ["kirill", "thiago"],
    )
    write_tax_reports(tax_summaries, reports_root)

    print("Full processing pipeline completed.")
    print(f"Normalized data root: {normalized_root}")
    print(f"Reports root: {reports_root}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run the complete processing pipeline in one go: normalize transactions and holdings, "
            "log holdings snapshot coverage gaps, reconcile positions, and generate reports."
        )
    )
    parser.add_argument(
        "--transactions-input",
        dest="transactions_input_root",
        type=Path,
        required=True,
        help="Directory containing raw transaction files.",
    )
    parser.add_argument(
        "--holdings-input",
        dest="holdings_input_root",
        type=Path,
        required=True,
        help="Directory containing raw holdings snapshot files.",
    )
    parser.add_argument(
        "--normalized-output",
        dest="normalized_root",
        type=Path,
        required=True,
        help="Directory to write normalized outputs.",
    )
    parser.add_argument(
        "--reports-output",
        dest="reports_root",
        type=Path,
        required=True,
        help="Directory to write generated report outputs.",
    )
    parser.add_argument(
        "--config",
        dest="config_path",
        type=Path,
        default=Path("src/config/accounts.json"),
        help="Path to account-to-broker config JSON.",
    )
    parser.add_argument(
        "--accounts",
        nargs="*",
        default=None,
        help=(
            "Optional account names to process. "
            "When omitted, all configured accounts are processed."
        ),
    )
    parser.add_argument(
        "--tax-report-owners",
        nargs="*",
        default=["kirill", "thiago"],
        help=(
            "Owners for tax reports. "
            "Writes one file per owner: tax_report_<owner>.csv."
        ),
    )
    args = parser.parse_args()

    run_full_pipeline(
        transactions_input_root=args.transactions_input_root,
        holdings_input_root=args.holdings_input_root,
        normalized_root=args.normalized_root,
        reports_root=args.reports_root,
        config_path=args.config_path,
        accounts=args.accounts,
        tax_report_owners=args.tax_report_owners,
    )


if __name__ == "__main__":
    main()
