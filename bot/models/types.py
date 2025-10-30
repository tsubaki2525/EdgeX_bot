from __future__ import annotations

from enum import Enum
from typing import Optional
from pydantic import BaseModel
import time


class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"


class OrderStatus(str, Enum):
    NEW = "NEW"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"


class Ticker(BaseModel):
    symbol: str
    price: float
    ts_ms: int


class Balance(BaseModel):
    asset: str
    free: float
    locked: float = 0.0


class OrderRequest(BaseModel):
    symbol: str
    side: OrderSide
    type: OrderType
    quantity: float
    price: Optional[float] = None
    client_order_id: Optional[str] = None


class Order(BaseModel):
    id: str
    request: OrderRequest
    status: OrderStatus
    filled_quantity: float
    average_price: float
    ts_ms: int

    @staticmethod
    def now_ms() -> int:
        return int(time.time() * 1000)
