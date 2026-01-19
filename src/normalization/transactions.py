from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Dict, Optional


def normalize_transaction_description(description: Optional[str]) -> Optional[str]:
    if description is None:
        return None
    cleaned = description.strip()
    if not cleaned:
        return None
    normalized = cleaned.casefold()
    if normalized == "gross interest":
        return "account interest"
    if normalized == "debit card payment":
        return "debit card payment"
    if normalized == "total monthly fee":
        return "fees"
    if normalized.startswith("div "):
        return "dividend"
    raise ValueError(f"Unexpected transaction description: {description}")


@dataclass(frozen=True)
class TransactionRecord:
    transaction_id: str
    account_name: str
    broker: str
    trade_date: Optional[date]
    settlement_date: Optional[date]
    symbol: Optional[str]
    sedol: Optional[str]
    quantity: Optional[Decimal]
    price: Optional[Decimal]
    description: Optional[str]
    reference: Optional[str]
    debit: Optional[Decimal]
    credit: Optional[Decimal]
    running_balance: Optional[Decimal]
    currency: Optional[str]
    source_file: str

    def to_dict(self) -> Dict[str, str]:
        return {
            "transaction_id": self.transaction_id,
            "account_name": self.account_name,
            "broker": self.broker,
            "trade_date": self.trade_date.isoformat() if self.trade_date else "",
            "settlement_date": self.settlement_date.isoformat() if self.settlement_date else "",
            "symbol": self.symbol or "",
            "sedol": self.sedol or "",
            "quantity": f"{self.quantity}" if self.quantity is not None else "",
            "price": f"{self.price}" if self.price is not None else "",
            "description": self.description or "",
            "reference": self.reference or "",
            "debit": f"{self.debit}" if self.debit is not None else "",
            "credit": f"{self.credit}" if self.credit is not None else "",
            "running_balance": f"{self.running_balance}" if self.running_balance is not None else "",
            "currency": self.currency or "",
            "source_file": self.source_file,
        }
