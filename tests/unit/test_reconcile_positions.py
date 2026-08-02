from __future__ import annotations

from pathlib import Path

from scripts.reconcile_positions import reconcile_root_detailed


def _write_transactions(account_root: Path, rows: list[str]) -> None:
    (account_root / "transactions_normalized.csv").write_text(
        "account_name,trade_date,settlement_date,symbol,quantity,description,debit,credit\n"
        + "".join(rows),
        encoding="utf-8",
    )


def _write_holdings(account_root: Path, snapshot_date: str, rows: list[str]) -> None:
    (account_root / f"holdings_{snapshot_date}_normalized.csv").write_text(
        "symbol,quantity\n" + "".join(rows),
        encoding="utf-8",
    )


def test_reconcile_root_reports_pending_settlement_buy_separately(tmp_path: Path) -> None:
    account_root = tmp_path / "acct"
    account_root.mkdir(parents=True)
    _write_transactions(
        account_root,
        [
            "acct,2026-02-12,2026-02-18,AAA,10,buy,,\n",
        ],
    )
    _write_holdings(account_root, "2026-02-16", ["AAA,10\n"])

    result = reconcile_root_detailed(tmp_path)

    assert result.mismatches == []
    assert len(result.pending_settlements) == 1
    assert "account=acct" in result.pending_settlements[0]
    assert "symbol=AAA" in result.pending_settlements[0]
    assert "delta=10" in result.pending_settlements[0]
    assert "pending=buy 10 trade_date=2026-02-12 settlement_date=2026-02-18" in result.pending_settlements[0]


def test_reconcile_root_reports_pending_settlement_sell_separately(tmp_path: Path) -> None:
    account_root = tmp_path / "acct"
    account_root.mkdir(parents=True)
    _write_transactions(
        account_root,
        [
            "acct,2026-01-01,2026-01-03,AAA,10,buy,,\n",
            "acct,2026-01-04,2026-01-07,AAA,4,sell,,100\n",
        ],
    )
    _write_holdings(account_root, "2026-01-05", ["AAA,6\n"])

    result = reconcile_root_detailed(tmp_path)

    assert result.mismatches == []
    assert len(result.pending_settlements) == 1
    assert "delta=-4" in result.pending_settlements[0]
    assert "pending=sell 4 trade_date=2026-01-04 settlement_date=2026-01-07" in result.pending_settlements[0]


def test_reconcile_root_keeps_unexplained_delta_as_mismatch(tmp_path: Path) -> None:
    account_root = tmp_path / "acct"
    account_root.mkdir(parents=True)
    _write_transactions(
        account_root,
        [
            "acct,2026-01-01,2026-01-03,AAA,10,buy,,\n",
        ],
    )
    _write_holdings(account_root, "2026-01-05", ["AAA,8\n"])

    result = reconcile_root_detailed(tmp_path)

    assert result.pending_settlements == []
    assert len(result.mismatches) == 1
    assert "account=acct" in result.mismatches[0]
    assert "symbol=AAA" in result.mismatches[0]
    assert "delta=-2" in result.mismatches[0]
