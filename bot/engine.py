from __future__ import annotations

import asyncio
import os
import random
from typing import Callable, Awaitable
from loguru import logger

from bot.adapters.base import ExchangeAdapter
from bot.models.types import OrderRequest, OrderSide, OrderType


class BotEngine:
    def __init__(
        self,
        adapter: ExchangeAdapter,
        symbol: str,
        strategy: Callable[[float], Awaitable[float]],
        poll_interval_sec: float = 0.5,
    ) -> None:
        self.adapter = adapter
        self.symbol = symbol
        self.strategy = strategy
        self.poll_interval_sec = poll_interval_sec
        self._running = False

    async def run(self) -> None:
        await self.adapter.connect()
        self._running = True
        logger.info("engine started")
        # 指値オフセット（bps）。BUYは+、SELLは- 方向に適用
        try:
            limit_offset_bps = float(os.getenv("EDGEX_LIMIT_OFFSET_BPS", "10"))
        except Exception:
            limit_offset_bps = 10.0
        try:
            while self._running:
                try:
                    ticker = await self.adapter.get_ticker(self.symbol)
                except Exception as e:
                    logger.warning("ticker failed: {}", e)
                    await asyncio.sleep(self.poll_interval_sec * 2)
                    continue
                size = await self.strategy(ticker.price)
                anchor = getattr(self.strategy, "last_anchor", None)
                diff_bps = getattr(self.strategy, "last_diff_bps", None)
                logger.info(
                    "tick price={} anchor={} diff_bps={} signal_size={}",
                    ticker.price,
                    anchor,
                    diff_bps,
                    size,
                )
                if size != 0:
                    side = OrderSide.BUY if size > 0 else OrderSide.SELL
                    # 指値価格を現在値からオフセット
                    if side == OrderSide.BUY:
                        limit_price = ticker.price * (1.0 + limit_offset_bps / 10000.0)
                    else:
                        limit_price = ticker.price * (1.0 - limit_offset_bps / 10000.0)
                    order = OrderRequest(
                        symbol=self.symbol,
                        side=side,
                        type=OrderType.LIMIT,
                        quantity=abs(size),
                        price=limit_price,
                    )
                    try:
                        result = await self.adapter.place_order(order)
                        logger.info(
                            "order result: status={}, qty={}, avg_px={}",
                            result.status,
                            result.filled_quantity,
                            result.average_price,
                        )
                        try:
                            bals = await self.adapter.fetch_balances()
                            logger.info("balances: {}", bals)
                        except Exception:
                            pass
                    except Exception as e:
                        logger.warning("order failed: {}", e)
                await asyncio.sleep(self.poll_interval_sec)
                # 軽いジッターで巡回時間を拡散（429/同時集中回避）
                await asyncio.sleep(self.poll_interval_sec * random.uniform(-0.2, 0.2))
        finally:
            await self.adapter.close()
            logger.info("engine stopped")

    def stop(self) -> None:
        self._running = False
