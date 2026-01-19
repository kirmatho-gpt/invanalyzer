from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Dict, Optional


@dataclass(frozen=True)
class HoldingRecord:
    snapshot_id: str
    account_name: str
    broker: str
    valuation_date: Optional[date]
    symbol: Optional[str]
    name: Optional[str]
    quantity: Optional[Decimal]
    price: Optional[Decimal]
    average_price: Optional[Decimal]
    market_value: Optional[Decimal]
    book_cost: Optional[Decimal]
    gain_loss: Optional[Decimal]
    gain_loss_pct: Optional[Decimal]
    currency: Optional[str]
    source_file: str

    def to_dict(self) -> Dict[str, str]:
        return {
            "snapshot_id": self.snapshot_id,
            "account_name": self.account_name,
            "broker": self.broker,
            "valuation_date": self.valuation_date.isoformat() if self.valuation_date else "",
            "symbol": self.symbol or "",
            "name": self.name or "",
            "quantity": f"{self.quantity}" if self.quantity is not None else "",
            "price": f"{self.price}" if self.price is not None else "",
            "average_price": f"{self.average_price}" if self.average_price is not None else "",
            "market_value": f"{self.market_value}" if self.market_value is not None else "",
            "book_cost": f"{self.book_cost}" if self.book_cost is not None else "",
            "gain_loss": f"{self.gain_loss}" if self.gain_loss is not None else "",
            "gain_loss_pct": f"{self.gain_loss_pct}" if self.gain_loss_pct is not None else "",
            "currency": self.currency or "",
            "source_file": self.source_file,
        }
