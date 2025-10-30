"""
Volume Trading Engine
取引量を稼ぐためのエンジン
"""

import asyncio
from decimal import Decimal
from loguru import logger

from bot.adapters.base import ExchangeAdapter
from bot.models.types import OrderRequest, OrderSide, OrderType, TimeInForce


class VolumeEngine:
    """取引量を稼ぐためのエンジン
    
    動作:
    1. エントリー: 現在価格の上下にMAKER注文を配置
    2. 約定待機: どちらかの注文が約定するまで待つ
    3. ポジション保持: 指定時間（デフォルト120秒）ポジションを保持
    4. エグジット: ポジションを決済
    5. 待機: 指定時間（デフォルト60秒）待機
    6. 1に戻る
    """

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
        self.entry_offset_usd = entry_offset_usd
        self.exit_offset_usd = exit_offset_usd
        self.hold_time_seconds = hold_time_seconds
        self.reorder_interval_seconds = reorder_interval_seconds
        
        # 状態管理
        self.position_side: str | None = None  # "LONG" or "SHORT"
        self.position_size: Decimal = Decimal("0")
        self.position_price: Decimal = Decimal("0")
        self.total_volume: Decimal = Decimal("0")
        self.total_pnl: Decimal = Decimal("0")
        self.cycle_count: int = 0
        
        # 注文ID
        self.buy_order_id: str | None = None
        self.sell_order_id: str | None = None

    async def run(self):
        """メインループ"""
        await self.adapter.connect()
        logger.info("=== 取引量ボット起動 ===")
        logger.info("契約ID: {}", self.contract_id)
        logger.info("サイズ: {} BTC", self.size)
        logger.info("エントリーオフセット: {} USD", self.entry_offset_usd)
        logger.info("エグジットオフセット: {} USD", self.exit_offset_usd)
        logger.info("ポジション保持時間: {} 秒", self.hold_time_seconds)
        logger.info("サイクル間隔: {} 秒", self.reorder_interval_seconds)
        
        try:
            while True:
                await self._entry_phase()
                await self._hold_phase()
                await self._exit_phase()
                
                logger.info("=== サイクル完了 ===")
                logger.info("待機時間: {} 秒", self.reorder_interval_seconds)
                await asyncio.sleep(self.reorder_interval_seconds)
                
        finally:
            await self.adapter.close()
            logger.info("取引量ボット停止")

    async def _entry_phase(self):
        """エントリーフェーズ: 両建て注文を発注"""
        logger.info("=== エントリーフェーズ ===")
        
        # 現在価格を取得
        ticker = await self.adapter.get_ticker(self.contract_id)
        current_price = ticker.price
        logger.info("現在価格: ${:.1f}", current_price)
        
        # エントリー価格を計算
        buy_price = float(Decimal(str(current_price)) - self.entry_offset_usd)
        sell_price = float(Decimal(str(current_price)) + self.entry_offset_usd)
        logger.info("エントリー注文配置: 買い=${:.1f} 売り=${:.1f}", buy_price, sell_price)
        
        # 買い注文
        buy_order_req = OrderRequest(
            symbol=self.contract_id,
            side=OrderSide.BUY,
            type=OrderType.LIMIT,
            quantity=self.size,
            price=buy_price,
            time_in_force=TimeInForce.POST_ONLY  # ← MAKER注文（手数料リベート）
        )
        
        # 売り注文
        sell_order_req = OrderRequest(
            symbol=self.contract_id,
            side=OrderSide.SELL,
            type=OrderType.LIMIT,
            quantity=self.size,
            price=sell_price,
            time_in_force=TimeInForce.POST_ONLY  # ← MAKER注文（手数料リベート）
        )
        
        # 注文を発注
        buy_order = await self.adapter.place_order(buy_order_req)
        sell_order = await self.adapter.place_order(sell_order_req)
        
        self.buy_order_id = buy_order.id
        self.sell_order_id = sell_order.id
        
        logger.info("エントリー注文発注完了: 買いID={} 売りID={}", self.buy_order_id, self.sell_order_id)
        
        # 約定待機
        logger.info("約定待機中...")
        while True:
            await asyncio.sleep(1)
            
            # アクティブ注文を確認
            active_orders = await self.adapter.list_active_orders(self.contract_id)
            active_ids = {o.id for o in active_orders}
            
            # 買い注文が約定したか確認
            if self.buy_order_id not in active_ids:
                logger.info("買い注文約定! 価格=${:.1f}", buy_price)
                self.position_side = "LONG"
                self.position_size = self.size
                self.position_price = Decimal(str(buy_price))
                self.total_volume += self.size
                
                # 売り注文をキャンセル
                if self.sell_order_id in active_ids:
                    await self.adapter.cancel_order(self.sell_order_id)
                    logger.info("売り注文キャンセル: ID={}", self.sell_order_id)
                break
            
            # 売り注文が約定したか確認
            if self.sell_order_id not in active_ids:
                logger.info("売り注文約定! 価格=${:.1f}", sell_price)
                self.position_side = "SHORT"
                self.position_size = -self.size
                self.position_price = Decimal(str(sell_price))
                self.total_volume += self.size
                
                # 買い注文をキャンセル
                if self.buy_order_id in active_ids:
                    await self.adapter.cancel_order(self.buy_order_id)
                    logger.info("買い注文キャンセル: ID={}", self.buy_order_id)
                break
        
        logger.info("ポジション: {} {} @ ${:.1f}", self.position_side, self.position_size, self.position_price)
        logger.info("累計取引量: {} BTC", self.total_volume)

    async def _hold_phase(self):
        """ホールドフェーズ: ポジションを保持"""
        logger.info("=== ポジション保持フェーズ ===")
        logger.info("保持時間: {} 秒", self.hold_time_seconds)
        
        for i in range(self.hold_time_seconds, 0, -10):
            logger.info("ポジション保持中... 残り {} 秒", i)
            await asyncio.sleep(10)

    async def _exit_phase(self):
        """エグジットフェーズ: ポジションを決済"""
        logger.info("=== エグジットフェーズ ===")
        logger.info("保持時間終了、決済処理開始")
        
        # 現在価格を取得
        ticker = await self.adapter.get_ticker(self.contract_id)
        current_price = ticker.price
        
        # エグジット価格を計算
        if self.position_side == "LONG":
            # ロングポジション → 売りで決済
            exit_price = float(Decimal(str(current_price)) - self.exit_offset_usd)
            exit_side = OrderSide.SELL
            logger.info("決済注文配置（売り）: 価格=${:.1f}", exit_price)
        else:
            # ショートポジション → 買いで決済
            exit_price = float(Decimal(str(current_price)) + self.exit_offset_usd)
            exit_side = OrderSide.BUY
            logger.info("決済注文配置（買い）: 価格=${:.1f}", exit_price)
        
        # 決済注文
        exit_order_req = OrderRequest(
            symbol=self.contract_id,
            side=exit_side,
            type=OrderType.LIMIT,
            quantity=abs(self.position_size),
            price=exit_price,
            time_in_force=TimeInForce.POST_ONLY  # ← MAKER注文（手数料リベート）
        )
        
        exit_order = await self.adapter.place_order(exit_order_req)
        exit_order_id = exit_order.id
        logger.info("決済注文発注完了: ID={}", exit_order_id)
        
        # 約定待機
        logger.info("決済約定待機中...")
        while True:
            await asyncio.sleep(1)
            
            # アクティブ注文を確認
            active_orders = await self.adapter.list_active_orders(self.contract_id)
            active_ids = {o.id for o in active_orders}
            
            # 決済注文が約定したか確認
            if exit_order_id not in active_ids:
                logger.info("決済注文約定! 価格=${:.1f}", exit_price)
                break
        
        # PnL計算
        if self.position_side == "LONG":
            pnl = (Decimal(str(exit_price)) - self.position_price) * self.position_size
        else:
            pnl = (self.position_price - Decimal(str(exit_price))) * abs(self.position_size)
        
        self.total_pnl += pnl
        self.cycle_count += 1
        
        logger.info("ポジション決済完了: PnL=${:.3f} 累計PnL=${:.3f} 累計取引量={} BTC サイクル数={}", 
                   pnl, self.total_pnl, self.total_volume, self.cycle_count)
        
        # ポジションをリセット
        self.position_side = None
        self.position_size = Decimal("0")
        self.position_price = Decimal("0")
