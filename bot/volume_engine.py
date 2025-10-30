"""
Volume Trading Engine
取引量を稼ぐためのボット
"""

import asyncio
from decimal import Decimal
from typing import Optional
from datetime import datetime
from loguru import logger

from bot.models.types import OrderSide, OrderRequest, OrderType, TimeInForce
from bot.adapters.base import ExchangeAdapter


class VolumeEngine:
    """取引量稼ぎエンジン"""
    
    def __init__(
        self,
        adapter: ExchangeAdapter,
        contract_id: str,
        size: Decimal,
        entry_offset_usd: Decimal,
        exit_offset_usd: Decimal,
        hold_time_seconds: int,
        reorder_interval_seconds: int,
    ):
        self.adapter = adapter
        self.contract_id = contract_id
        self.size = size
        self.entry_offset = entry_offset_usd
        self.exit_offset = exit_offset_usd
        self.hold_time = hold_time_seconds
        self.reorder_interval = reorder_interval_seconds
        
        self.position_size = Decimal("0")
        self.entry_price: Optional[Decimal] = None
        self.entry_time: Optional[datetime] = None
        self.buy_order_id: Optional[str] = None
        self.sell_order_id: Optional[str] = None
        
        self.total_volume = Decimal("0")
        self.total_pnl = Decimal("0")
        self.cycle_count = 0
        
    async def run(self):
        logger.info("volume engine started: contract_id={} size={} entry_offset={} exit_offset={} hold_time={}s",
                   self.contract_id, self.size, self.entry_offset, self.exit_offset, self.hold_time)
        
        await self.adapter.connect()
        
        try:
            while True:
                try:
                    if self.position_size == 0:
                        await self._entry_phase()
                    else:
                        await self._exit_phase()
                except Exception as e:
                    logger.error("volume engine error: {}", e)
                    await asyncio.sleep(5)
        finally:
            await self.adapter.close()
    
    async def _is_order_filled(self, order_id: str) -> bool:
        """注文が約定したかチェック（アクティブな注文リストから消えたら約定）"""
        try:
            active_orders = await self.adapter.list_active_orders(self.contract_id)
            for order in active_orders:
                oid = order.get("orderId") or order.get("id")
                if str(oid) == str(order_id):
                    # まだアクティブ = 未約定
                    return False
            # アクティブリストにない = 約定済み
            return True
        except Exception as e:
            logger.debug("failed to check order status: {}", e)
            return False
    
    async def _entry_phase(self):
        logger.info("=== ENTRY PHASE ===")
        
        ticker = await self.adapter.get_ticker(self.contract_id)
        mid_price = Decimal(str(ticker.price))
        logger.info("current price: {}", mid_price)
        
        buy_price = mid_price - self.entry_offset
        sell_price = mid_price + self.entry_offset
        
        logger.info("placing entry orders: buy={} sell={}", buy_price, sell_price)
        
        buy_order_req = OrderRequest(
            symbol=self.contract_id,
            side=OrderSide.BUY,
            type=OrderType.LIMIT,
            quantity=self.size,
            price=buy_price,
            time_in_force=TimeInForce.POST_ONLY  # ← 追加：MAKER専用
        )
        buy_order = await self.adapter.place_order(buy_order_req)
        self.buy_order_id = buy_order.id
        
        sell_order_req = OrderRequest(
            symbol=self.contract_id,
            side=OrderSide.SELL,
            type=OrderType.LIMIT,
            quantity=self.size,
            price=sell_price,
            time_in_force=TimeInForce.POST_ONLY  # ← 追加：MAKER専用
        )
        sell_order = await self.adapter.place_order(sell_order_req)
        self.sell_order_id = sell_order.id
        
        logger.info("entry orders placed: buy_id={} sell_id={}", self.buy_order_id, self.sell_order_id)
        
        while True:
            await asyncio.sleep(2)
            
            buy_filled = await self._is_order_filled(self.buy_order_id)
            if buy_filled:
                logger.info("BUY order filled! price={}", buy_price)
                await self.adapter.cancel_order(self.sell_order_id)
                self.sell_order_id = None
                self.position_size = self.size
                self.entry_price = buy_price
                self.entry_time = datetime.now()
                logger.info("position: LONG {} @ {}", self.position_size, self.entry_price)
                break
            
            sell_filled = await self._is_order_filled(self.sell_order_id)
            if sell_filled:
                logger.info("SELL order filled! price={}", sell_price)
                await self.adapter.cancel_order(self.buy_order_id)
                self.buy_order_id = None
                self.position_size = -self.size
                self.entry_price = sell_price
                self.entry_time = datetime.now()
                logger.info("position: SHORT {} @ {}", abs(self.position_size), self.entry_price)
                break
        
        self.total_volume += self.size
        logger.info("total volume: {}", self.total_volume)
    
    async def _exit_phase(self):
        elapsed = (datetime.now() - self.entry_time).total_seconds()
        if elapsed < self.hold_time:
            wait_time = self.hold_time - elapsed
            logger.info("holding position... wait {}s", int(wait_time))
            await asyncio.sleep(min(wait_time, 10))
            return
        
        logger.info("=== EXIT PHASE ===")
        logger.info("hold time reached, starting exit process")
        
        while self.position_size != 0:
            ticker = await self.adapter.get_ticker(self.contract_id)
            mid_price = Decimal(str(ticker.price))
            
            if self.position_size > 0:
                exit_price = mid_price + self.exit_offset
                logger.info("placing exit SELL order: price={}", exit_price)
                exit_order_req = OrderRequest(
                    symbol=self.contract_id,
                    side=OrderSide.SELL,
                    type=OrderType.LIMIT,
                    quantity=abs(self.position_size),
                    price=exit_price,
                    time_in_force=TimeInForce.POST_ONLY  # ← 追加：MAKER専用
                )
            else:
                exit_price = mid_price - self.exit_offset
                logger.info("placing exit BUY order: price={}", exit_price)
                exit_order_req = OrderRequest(
                    symbol=self.contract_id,
                    side=OrderSide.BUY,
                    type=OrderType.LIMIT,
                    quantity=abs(self.position_size),
                    price=exit_price,
                    time_in_force=TimeInForce.POST_ONLY  # ← 追加：MAKER専用
                )
            
            exit_order = await self.adapter.place_order(exit_order_req)
            exit_order_id = exit_order.id
            
            for _ in range(self.reorder_interval):
                await asyncio.sleep(1)
                
                if await self._is_order_filled(exit_order_id):
                    logger.info("EXIT order filled! price={}", exit_price)
                    
                    if self.position_size > 0:
                        pnl = (exit_price - self.entry_price) * self.size
                    else:
                        pnl = (self.entry_price - exit_price) * self.size
                    
                    self.total_pnl += pnl
                    self.total_volume += abs(self.position_size)
                    self.cycle_count += 1
                    
                    logger.info("position closed: pnl={} total_pnl={} total_volume={} cycles={}",
                               pnl, self.total_pnl, self.total_volume, self.cycle_count)
                    
                    self.position_size = Decimal("0")
                    self.entry_price = None
                    self.entry_time = None
                    return
            
            logger.info("exit order not filled, cancelling and reordering...")
            try:
                await self.adapter.cancel_order(exit_order_id)
            except Exception as e:
                logger.debug("cancel failed (maybe already filled): {}", e)
