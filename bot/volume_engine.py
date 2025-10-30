"""
Volume Trading Engine
取引量を稼ぐためのボット

目的:
- 取引量を最大化
- 価格変動リスクを最小化
- 全て指値注文のみ
- ポジション保有時間: 最低2分
"""

import asyncio
from decimal import Decimal
from typing import Optional
from datetime import datetime
from loguru import logger

from bot.models.types import OrderSide, OrderRequest, OrderType
from bot.adapters.base import ExchangeAdapter


class VolumeEngine:
    """取引量稼ぎエンジン"""
    
    def __init__(
        self,
        adapter: ExchangeAdapter,
        symbol: str,
        size: Decimal,
        entry_offset_usd: Decimal,
        exit_offset_usd: Decimal,
        hold_time_seconds: int,
        reorder_interval_seconds: int,
    ):
        """
        Args:
            adapter: 取引所アダプター
            symbol: 取引ペア（例: BTC-USD-PERP）
            size: 1回の取引数量（BTC）
            entry_offset_usd: エントリー時の現在価格からの距離（USD）
            exit_offset_usd: 決済時の現在価格からの距離（USD）
            hold_time_seconds: 最低保有時間（秒）
            reorder_interval_seconds: 決済注文の再発注間隔（秒）
        """
        self.adapter = adapter
        self.symbol = symbol
        self.size = size
        self.entry_offset = entry_offset_usd
        self.exit_offset = exit_offset_usd
        self.hold_time = hold_time_seconds
        self.reorder_interval = reorder_interval_seconds
        
        # 状態管理
        self.position_size = Decimal("0")
        self.entry_price: Optional[Decimal] = None
        self.entry_time: Optional[datetime] = None
        self.buy_order_id: Optional[str] = None
        self.sell_order_id: Optional[str] = None
        
        # 統計
        self.total_volume = Decimal("0")
        self.total_pnl = Decimal("0")
        self.cycle_count = 0
        
    async def run(self):
        """ボット開始（グリッドボットと同じメソッド名）"""
        logger.info("volume engine started: symbol={} size={} entry_offset={} exit_offset={} hold_time={}s",
                   self.symbol, self.size, self.entry_offset, self.exit_offset, self.hold_time)
        
        # アダプター接続
        await self.adapter.connect()
        
        while True:
            try:
                if self.position_size == 0:
                    await self._entry_phase()
                else:
                    await self._exit_phase()
                    
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error("volume engine error: {}", e)
                await asyncio.sleep(5)
    
    async def _entry_phase(self):
        """エントリーフェーズ: 買い1本、売り1本を出す"""
        logger.info("=== ENTRY PHASE ===")
        
        # 現在価格を取得
        ticker = await self.adapter.get_ticker(self.symbol)
        mid_price = Decimal(str(ticker.price))
        logger.info("current price: {}", mid_price)
        
        # 買い注文と売り注文を出す
        buy_price = mid_price - self.entry_offset
        sell_price = mid_price + self.entry_offset
        
        logger.info("placing entry orders: buy={} sell={}", buy_price, sell_price)
        
        # 買い注文
        buy_order_req = OrderRequest(
            symbol=self.symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=self.size,
            price=buy_price
        )
        buy_order = await self.adapter.place_order(buy_order_req)
        self.buy_order_id = buy_order.order_id
        
        # 売り注文
        sell_order_req = OrderRequest(
            symbol=self.symbol,
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            quantity=self.size,
            price=sell_price
        )
        sell_order = await self.adapter.place_order(sell_order_req)
        self.sell_order_id = sell_order.order_id
        
        logger.info("entry orders placed: buy_id={} sell_id={}", self.buy_order_id, self.sell_order_id)
        
        # 約定を待つ
        while True:
            await asyncio.sleep(2)
            
            # 買い注文の約定確認
            buy_order_status = await self.adapter.get_order(self.buy_order_id)
            if buy_order_status.is_filled():
                logger.info("BUY order filled! price={}", buy_price)
                # 売り注文をキャンセル
                await self.adapter.cancel_order(self.sell_order_id)
                self.sell_order_id = None
                # ポジション更新
                self.position_size = self.size
                self.entry_price = buy_price
                self.entry_time = datetime.now()
                logger.info("position: LONG {} @ {}", self.position_size, self.entry_price)
                break
            
            # 売り注文の約定確認
            sell_order_status = await self.adapter.get_order(self.sell_order_id)
            if sell_order_status.is_filled():
                logger.info("SELL order filled! price={}", sell_price)
                # 買い注文をキャンセル
                await self.adapter.cancel_order(self.buy_order_id)
                self.buy_order_id = None
                # ポジション更新
                self.position_size = -self.size
                self.entry_price = sell_price
                self.entry_time = datetime.now()
                logger.info("position: SHORT {} @ {}", abs(self.position_size), self.entry_price)
                break
        
        # 取引量を記録
        self.total_volume += self.size
        logger.info("total volume: {}", self.total_volume)
    
    async def _exit_phase(self):
        """決済フェーズ: 2分待機後、決済注文を出す"""
        # 2分待機
        elapsed = (datetime.now() - self.entry_time).total_seconds()
        if elapsed < self.hold_time:
            wait_time = self.hold_time - elapsed
            logger.info("holding position... wait {}s", int(wait_time))
            await asyncio.sleep(min(wait_time, 10))
            return
        
        logger.info("=== EXIT PHASE ===")
        logger.info("hold time reached, starting exit process")
        
        # 決済注文を出す（約定するまで繰り返す）
        while self.position_size != 0:
            # 現在価格を取得
            ticker = await self.adapter.get_ticker(self.symbol)
            mid_price = Decimal(str(ticker.price))
            
            # 決済注文を出す
            if self.position_size > 0:
                # ロングポジション → 売り注文
                exit_price = mid_price + self.exit_offset
                logger.info("placing exit SELL order: price={}", exit_price)
                exit_order_req = OrderRequest(
                    symbol=self.symbol,
                    side=OrderSide.SELL,
                    order_type=OrderType.LIMIT,
                    quantity=abs(self.position_size),
                    price=exit_price
                )
            else:
                # ショートポジション → 買い注文
                exit_price = mid_price - self.exit_offset
                logger.info("placing exit BUY order: price={}", exit_price)
                exit_order_req = OrderRequest(
                    symbol=self.symbol,
                    side=OrderSide.BUY,
                    order_type=OrderType.LIMIT,
                    quantity=abs(self.position_size),
                    price=exit_price
                )
            
            exit_order = await self.adapter.place_order(exit_order_req)
            exit_order_id = exit_order.order_id
            
            # 1分間待つ（約定を確認）
            for _ in range(self.reorder_interval):
                await asyncio.sleep(1)
                
                # 約定確認
                exit_order_status = await self.adapter.get_order(exit_order_id)
                if exit_order_status.is_filled():
                    logger.info("EXIT order filled! price={}", exit_price)
                    
                    # 損益計算
                    if self.position_size > 0:
                        pnl = (exit_price - self.entry_price) * self.size
                    else:
                        pnl = (self.entry_price - exit_price) * self.size
                    
                    self.total_pnl += pnl
                    self.total_volume += abs(self.position_size)
                    self.cycle_count += 1
                    
                    logger.info("position closed: pnl={} total_pnl={} total_volume={} cycles={}",
                               pnl, self.total_pnl, self.total_volume, self.cycle_count)
                    
                    # ポジションをリセット
                    self.position_size = Decimal("0")
                    self.entry_price = None
                    self.entry_time = None
                    return
            
            # 約定しなかった → 注文をキャンセルして再発注
            logger.info("exit order not filled, cancelling and reordering...")
            try:
                await self.adapter.cancel_order(exit_order_id)
            except Exception as e:
                logger.debug("cancel failed (maybe already filled): {}", e)
