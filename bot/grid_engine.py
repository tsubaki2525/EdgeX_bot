"""
Grid Trading Engine
グリッド戦略エンジン
"""

import asyncio
import os
from typing import Dict, Optional
import time
from loguru import logger

from bot.adapters.base import ExchangeAdapter
from bot.models.types import OrderRequest, OrderSide, OrderType, TimeInForce
from bot.utils.trade_logger import TradeLogger

class GridEngine:
    """**STEP毎に両サイドへグリッド指値を差し続けなくしたエンジン.
    
    - 剥ぎさない限りキャンセル/差し直しは一切しない
    - 片側levels本オーダー (ENVで指定) を設定価格を中心に両側配置
    - 約定したら価格が戻らない限り再配置しない
    - 価格が動いたら新しい価格帯に不足分の追加（過去の注文は放置）
    """

    def __init__(
        self,
        adapter: ExchangeAdapter,
        symbol: str,
        poll_interval_sec: float = 1.0,
    ) -> None:
        self.adapter = adapter
        self.symbol = symbol
        self.poll_interval_sec = max(1.5, float(poll_interval_sec))
        self._running = False
        self._loop_iter: int = 0

        self.size = float(os.getenv("EDGEX_GRID_SIZE", os.getenv("EDGEX_SIZE", "0.01")))
        self.step = float(os.getenv("EDGEX_GRID_STEP_USD", "100"))
        # 両側の価格幅(固定) だけ使い込む
        self.first_offset = float(os.getenv("EDGEX_GRID_FIRST_OFFSET_USD", "100"))
        self.levels = int(os.getenv("EDGEX_GRID_LEVELS_PER_SIDE", "5"))
        logger.info(
            "グリッド設定: グリッド幅={}USD 初回オフセット={}USD レベル数={} サイズ={}BTC",
            self.step,
            self.first_offset,
            self.levels,
            self.size,
        )

        # レート制限回避のための遅延時間調整
        try:
            self.op_spacing_sec = float(os.getenv("EDGEX_GRID_OP_SPACING_SEC", "1.2"))
        except Exception:
            self.op_spacing_sec = 1.2

        # 初回配置済みフラグ（複数回はfirst_offsetは適用しない一度だけ）
        self.initialized = False

        # 既に出した価格（重複防止）
        self.placed_buy_px_to_id: Dict[float, str] = {}
        self.placed_sell_px_to_id: Dict[float, str] = {}

        self.tlog = TradeLogger()
        # closed PnL poll interval (sec). 0 to disable.
        try:
            self.closed_poll_sec = float(os.getenv("EDGEX_GRID_CLOSED_PNL_SEC", "30"))
        except Exception:
            self.closed_poll_sec = 30.0
        self._last_closed_id: str | None = None
        self._last_closed_poll_ts: float = 0.0

        # 中心価格（ステップでバケット化して固定）
        self._grid_center: float | None = None

        # 1ループあたりの新規発注上限（片側）
        try:
            self.max_new_per_loop = int(os.getenv("EDGEX_GRID_MAX_NEW_PER_LOOP", "2"))
        except Exception:
            self.max_new_per_loop = 2

    async def run(self) -> None:
        await self.adapter.connect()
        self._running = True
        logger.info(
            "グリッドエンジン起動: グリッド幅={}USD レベル数={} サイズ={}BTC",
            self.step,
            self.levels,
            self.size,
        )
        try:
            while self._running:
                try:
                    self._loop_iter += 1
                    logger.debug("グリッドループ開始: iter={} 配置済み買い={}本 配置済み売り={}本 初期化済み={}", 
                                self._loop_iter, len(self.placed_buy_px_to_id), len(self.placed_sell_px_to_id), self.initialized)

                    # 現在価格取得
                    try:
                        ticker = await self.adapter.get_ticker(self.symbol)
                        mid_price = ticker.price  # type: ignore[attr-defined]
                    except Exception as e:
                        logger.warning("中間価格の取得に失敗: {}", e)
                        await asyncio.sleep(self.poll_interval_sec)
                        continue

                    # グリッド配置
                    await self._ensure_grid(mid_price)

                    # 約定確認と補充
                    await self._replenish_if_filled()

                except Exception as e:
                    logger.warning("グリッドループエラー: {}", e)
                    logger.debug("グリッドループ終了: iter={} 待機時間={}秒", self._loop_iter, self.poll_interval_sec)
                    await asyncio.sleep(self.poll_interval_sec)

                # 定期: クローズ損益の新規行を取り込み
                await self._poll_closed_pnl_once()

        finally:
            await self.adapter.close()
            logger.info("グリッドエンジン停止")

    async def _ensure_grid(self, mid_price: float):
        """
        ステップでバケット化した中心価格を基準にグリッドを配置
        初回 → first_offset を使う
        2回目以降 → step だけ使う
        """
        def _bucketize(price: float) -> float:
            step = float(self.step)
            if step <= 0:
                return price
            return round(price / step) * step

        # バケット化した中心を更新（ステップ境界を跨いだ時のみ）
        new_center = _bucketize(mid_price)
        if self._grid_center is None:
            self._grid_center = new_center
        elif new_center != self._grid_center:
            self._grid_center = new_center
        # 初回配置
        if not self.initialized:
            logger.info("初回グリッド配置: 中心価格=${:.1f}", self._grid_center)
            
            # 買いグリッド（初回）
            for i in range(self.levels):
                offset = self.first_offset + (i * self.step)
                px = self._grid_center - offset  # type: ignore[operator]
                if px not in self.placed_buy_px_to_id:
                    await self._place_order(OrderSide.BUY, px)
                    await asyncio.sleep(self.op_spacing_sec)
            
            # 売りグリッド（初回）
            for i in range(self.levels):
                offset = self.first_offset + (i * self.step)
                px = self._grid_center + offset  # type: ignore[operator]
                if px not in self.placed_sell_px_to_id:
                    await self._place_order(OrderSide.SELL, px)
                    await asyncio.sleep(self.op_spacing_sec)
            
            self.initialized = True
            logger.info("初回グリッド配置完了: 買い{}本 売り{}本", 
                       len(self.placed_buy_px_to_id), len(self.placed_sell_px_to_id))
        else:
            # 2回目以降: 固定中心を使い、片側あたり新規を上限化
            new_buys = 0
            new_sells = 0
            # 買いグリッド（追加）
            for i in range(self.levels):
                offset = i * self.step
                px = self._grid_center - offset  # type: ignore[operator]
                if px not in self.placed_buy_px_to_id and new_buys < self.max_new_per_loop:
                    await self._place_order(OrderSide.BUY, px)
                    new_buys += 1
                    await asyncio.sleep(self.op_spacing_sec)
            
            # 売りグリッド（追加）
            for i in range(self.levels):
                offset = i * self.step
                px = self._grid_center + offset  # type: ignore[operator]
                if px not in self.placed_sell_px_to_id and new_sells < self.max_new_per_loop:
                    await self._place_order(OrderSide.SELL, px)
                    new_sells += 1
                    await asyncio.sleep(self.op_spacing_sec)

    async def _place_order(self, side: OrderSide, price: float):
        """注文を発注"""
        req = OrderRequest(
            symbol=self.symbol,
            side=side,
            type=OrderType.LIMIT,
            quantity=self.size,
            price=price,
            time_in_force=TimeInForce.POST_ONLY  # ← MAKER注文（手数料リベート）
        )
        
        try:
            order = await self.adapter.place_order(req)
            if side == OrderSide.BUY:
                self.placed_buy_px_to_id[price] = order.id
                logger.info("買い注文発注: 価格=${:.1f} ID={}", price, order.id)
            else:
                self.placed_sell_px_to_id[price] = order.id
                logger.info("売り注文発注: 価格=${:.1f} ID={}", price, order.id)
        except Exception as e:
            logger.error("注文発注エラー: side={} price={} error={}", side, price, e)

    async def _replenish_if_filled(self):
        """約定した注文を確認し、補充する"""
        try:
            active_orders = await self.adapter.list_active_orders(self.symbol)
            # EdgeXアダプタは dict を返すため堅牢にIDを抽出する
            active_ids = set()
            for o in active_orders:
                try:
                    if isinstance(o, dict):
                        oid = (
                            o.get("orderId")
                            or o.get("id")
                            or o.get("order_id")
                            or o.get("clientOrderId")
                            or o.get("client_order_id")
                        )
                    else:
                        oid = getattr(o, "id", None) or getattr(o, "orderId", None)
                    if oid:
                        active_ids.add(str(oid))
                except Exception:
                    continue
            
            # 買い注文の約定確認
            filled_buy_prices = []
            for px, oid in list(self.placed_buy_px_to_id.items()):
                if oid not in active_ids:
                    logger.info("買い注文約定: 価格=${:.1f} ID={}", px, oid)
                    filled_buy_prices.append(px)
            
            # 売り注文の約定確認
            filled_sell_prices = []
            for px, oid in list(self.placed_sell_px_to_id.items()):
                if oid not in active_ids:
                    logger.info("売り注文約定: 価格=${:.1f} ID={}", px, oid)
                    filled_sell_prices.append(px)
            
            # 約定した注文を削除
            for px in filled_buy_prices:
                del self.placed_buy_px_to_id[px]
            for px in filled_sell_prices:
                del self.placed_sell_px_to_id[px]
            
            if filled_buy_prices or filled_sell_prices:
                logger.info("約定確認完了: 買い{}本 売り{}本", 
                           len(filled_buy_prices), len(filled_sell_prices))
        
        except Exception as e:
            logger.error("約定確認エラー: {}", e)

    async def _poll_closed_pnl_once(self):
        """定期的にクローズ済みPnLを取得"""
        if self.closed_poll_sec <= 0:
            return
        
        now = time.time()
        if now - self._last_closed_poll_ts < self.closed_poll_sec:
            return
        
        self._last_closed_poll_ts = now
        
        try:
            # ここでクローズ済みPnLを取得する処理を実装
            # 現在は未実装のため、スキップ
            pass
        except Exception as e:
            logger.error("クローズ済みPnL取得エラー: {}", e)
