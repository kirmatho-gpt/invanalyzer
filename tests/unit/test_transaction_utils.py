from __future__ import annotations

from datetime import date
from decimal import Decimal

from src.normalization.transactions import TransactionRecord
from src.positions.transaction_utils import (
    PositionCost,
    apply_transaction_to_position_costs,
    build_positions,
    effective_date,
    infer_signed_quantity,
    infer_signed_quantity_from_fields,
    transaction_value,
)


def make_record(
    *,
    trade_date: date | None = None,
    settlement_date: date | None = None,
    symbol: str | None = "AAPL",
    quantity: Decimal | None = None,
    price: Decimal | None = None,
    description: str | None = None,
    debit: Decimal | None = None,
    credit: Decimal | None = None,
) -> TransactionRecord:
    return TransactionRecord(
        transaction_id="t1",
        account_name="acct",
        broker="broker",
        trade_date=trade_date,
        settlement_date=settlement_date,
        symbol=symbol,
        sedol=None,
        quantity=quantity,
        price=price,
        description=description,
        reference=None,
        debit=debit,
        credit=credit,
        running_balance=None,
        currency="USD",
        source_file="file.csv",
    )


def test_effective_date_prefers_trade_date() -> None:
    trade = date(2024, 1, 2)
    settlement = date(2024, 1, 3)
    record = make_record(trade_date=trade, settlement_date=settlement)
    assert effective_date(record) == trade


def test_effective_date_uses_settlement_when_no_trade() -> None:
    settlement = date(2024, 1, 3)
    record = make_record(trade_date=None, settlement_date=settlement)
    assert effective_date(record) == settlement


def test_effective_date_none_when_missing_dates() -> None:
    record = make_record(trade_date=None, settlement_date=None)
    assert effective_date(record) is None


def test_infer_signed_quantity_from_fields() -> None:
    qty = Decimal("10")
    assert infer_signed_quantity_from_fields(qty, "buy") == qty
    assert infer_signed_quantity_from_fields(qty, "sell") == -qty
    assert infer_signed_quantity_from_fields(qty, "dividend") is None
    assert infer_signed_quantity_from_fields(None, "buy") is None


def test_infer_signed_quantity_uses_record_fields() -> None:
    record = make_record(quantity=Decimal("2"), description="sell")
    assert infer_signed_quantity(record) == Decimal("-2")


def test_transaction_value_prefers_price_times_quantity() -> None:
    record = make_record(quantity=Decimal("3"), price=Decimal("12"), debit=Decimal("99"))
    assert transaction_value(record, signed_quantity=Decimal("3")) == Decimal("36")


def test_transaction_value_uses_debit_for_buys() -> None:
    record = make_record(quantity=None, price=None, debit=Decimal("123"))
    assert transaction_value(record, signed_quantity=Decimal("5")) == Decimal("123")


def test_transaction_value_uses_credit_for_sells() -> None:
    record = make_record(quantity=None, price=None, credit=Decimal("321"))
    assert transaction_value(record, signed_quantity=Decimal("-5")) == Decimal("321")


def test_transaction_value_none_when_missing_fields() -> None:
    record = make_record(quantity=None, price=None, debit=None, credit=None)
    assert transaction_value(record, signed_quantity=Decimal("-1")) is None


def test_build_positions_filters_and_sums() -> None:
    valuation_date = date(2024, 1, 10)
    records = [
        make_record(trade_date=date(2024, 1, 1), quantity=Decimal("10"), description="buy"),
        make_record(trade_date=date(2024, 1, 5), quantity=Decimal("3"), description="sell"),
        make_record(trade_date=date(2024, 1, 11), quantity=Decimal("2"), description="buy"),
        make_record(trade_date=None, settlement_date=None, quantity=Decimal("1"), description="buy"),
        make_record(trade_date=date(2024, 1, 2), symbol=None, quantity=Decimal("1"), description="buy"),
        make_record(trade_date=date(2024, 1, 2), quantity=Decimal("1"), description="dividend"),
        make_record(
            trade_date=None,
            settlement_date=date(2024, 1, 4),
            symbol="MSFT",
            quantity=Decimal("4"),
            description="buy",
        ),
    ]

    positions = build_positions(records, valuation_date)

    assert positions == {
        "AAPL": Decimal("7"),
        "MSFT": Decimal("4"),
    }


def test_build_positions_uses_settlement_date_before_trade_date() -> None:
    valuation_date = date(2024, 1, 10)
    records = [
        make_record(
            trade_date=date(2024, 1, 8),
            settlement_date=date(2024, 1, 12),
            quantity=Decimal("10"),
            description="buy",
        ),
        make_record(
            trade_date=date(2024, 1, 8),
            settlement_date=date(2024, 1, 10),
            symbol="MSFT",
            quantity=Decimal("4"),
            description="buy",
        ),
    ]

    positions = build_positions(records, valuation_date)

    assert positions == {"MSFT": Decimal("4")}


def test_apply_transaction_to_position_costs_buy_updates_position() -> None:
    positions: dict[str, PositionCost] = {}
    record = make_record(
        trade_date=date(2024, 1, 1),
        quantity=Decimal("5"),
        price=Decimal("10"),
        description="buy",
    )

    realized = apply_transaction_to_position_costs(record, positions)

    assert realized is None
    assert positions["AAPL"] == PositionCost(quantity=Decimal("5"), book_cost=Decimal("50"))


def test_apply_transaction_to_position_costs_sell_realizes_pnl() -> None:
    positions = {"AAPL": PositionCost(quantity=Decimal("10"), book_cost=Decimal("1000"))}
    record = make_record(
        trade_date=date(2024, 1, 2),
        quantity=Decimal("4"),
        price=Decimal("125"),
        description="sell",
    )

    realized = apply_transaction_to_position_costs(record, positions)

    assert realized == Decimal("100")
    assert positions["AAPL"] == PositionCost(quantity=Decimal("6"), book_cost=Decimal("600"))


def test_apply_transaction_to_position_costs_sell_without_proceeds_no_change() -> None:
    positions = {"AAPL": PositionCost(quantity=Decimal("10"), book_cost=Decimal("1000"))}
    record = make_record(
        trade_date=date(2024, 1, 2),
        quantity=Decimal("4"),
        price=None,
        description="sell",
        credit=None,
    )

    realized = apply_transaction_to_position_costs(record, positions)

    assert realized is None
    assert positions["AAPL"] == PositionCost(quantity=Decimal("10"), book_cost=Decimal("1000"))


def test_apply_transaction_to_position_costs_ignores_missing_symbol() -> None:
    positions: dict[str, PositionCost] = {}
    record = make_record(
        trade_date=date(2024, 1, 1),
        symbol=None,
        quantity=Decimal("5"),
        price=Decimal("10"),
        description="buy",
    )

    realized = apply_transaction_to_position_costs(record, positions)

    assert realized is None
    assert positions == {}
