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

        # 1ループあたりの新規発注上限（片側）: 明示指定があれば適用（任意）
        try:
            self.max_new_per_loop = int(os.getenv("EDGEX_GRID_MAX_NEW_PER_LOOP", "0"))
        except Exception:
            self.max_new_per_loop = 0

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
        現在価格Pから内側Xを空け、P±(X + k*N) の等差列だけに指値を配置。
        - 買い: P - (X + k*N)
        - 売り: P + (X + k*N)
        既に置いてある価格はスキップ。自サイドの最も近い注文とN未満にならないようにする。
        """
        if self.step <= 0:
            return

        def _has_min_gap(side_map: Dict[float, str], px: float) -> bool:
            for opx in side_map.keys():
                if abs(opx - px) < self.step - 1e-9:
                    return False
            return True

        # 候補を作る
        buy_targets = [float(mid_price) - (self.first_offset + i * self.step) for i in range(self.levels)]
        sell_targets = [float(mid_price) + (self.first_offset + i * self.step) for i in range(self.levels)]

        # 以降はターゲットに合わせて一斉キャンセルは行わない（アンカー方式）

        # 片側あたり新規上限が設定されていれば適用
        new_buys = 0
        new_sells = 0

        # 買い配置（P−X より内側は生成しない設計だが、念のためチェック）
        for px in buy_targets:
            if px <= 0:
                continue
            if px >= (mid_price - 1e-9):
                continue
            if px in self.placed_buy_px_to_id:
                continue
            if not _has_min_gap(self.placed_buy_px_to_id, px):
                continue
            if self.max_new_per_loop and new_buys >= self.max_new_per_loop:
                break
            await self._place_order(OrderSide.BUY, px)
            new_buys += 1
            await asyncio.sleep(self.op_spacing_sec)

        # 売り配置（P＋X より内側は生成しない設計だが、念のためチェック）
        for px in sell_targets:
            if px in self.placed_sell_px_to_id:
                continue
            if not _has_min_gap(self.placed_sell_px_to_id, px):
                continue
            if px <= (mid_price + 1e-9):
                continue
            if self.max_new_per_loop and new_sells >= self.max_new_per_loop:
                break
            await self._place_order(OrderSide.SELL, px)
            new_sells += 1
            await asyncio.sleep(self.op_spacing_sec)

        if not self.initialized:
            self.initialized = True
            logger.info("初回グリッド配置完了: 買い{}本 売り{}本", 
                       len(self.placed_buy_px_to_id), len(self.placed_sell_px_to_id))

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
            # 自己クロス防止: 反対サイドに同値があればスキップ
            if side == OrderSide.BUY and price in self.placed_sell_px_to_id:
                logger.debug("自己クロス回避: BUYをスキップ 価格=${:.1f}", price)
                return
            if side == OrderSide.SELL and price in self.placed_buy_px_to_id:
                logger.debug("自己クロス回避: SELLをスキップ 価格=${:.1f}", price)
                return
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

            # === アンカー方式の補充ロジック ===
            # BUYが約定した場合: 
            #  - 反対側(SELL)の一番遠い指値(最大価格)を1つキャンセル
            #  - SELLを一番近い側に1つ追加（現在の最安SELLよりNだけ内側=より近い価格）
            #  - BUYを一番外側（現在の最安BUYよりNだけ外側=より安い価格）に1つ追加
            if filled_buy_prices:
                # 反対側の一番遠いSELLをキャンセル
                if self.placed_sell_px_to_id:
                    far_sell_px = max(self.placed_sell_px_to_id.keys())
                    far_sell_id = self.placed_sell_px_to_id.pop(far_sell_px)
                    try:
                        await self.adapter.cancel_order(far_sell_id)
                    except Exception:
                        logger.debug("cancel far SELL failed (ignore): id={} px={}", far_sell_id, far_sell_px)
                    await asyncio.sleep(self.op_spacing_sec)
                # SELLを一番近い側に追加
                base_near_sell = min(self.placed_sell_px_to_id.keys()) if self.placed_sell_px_to_id else (max(filled_buy_prices) + self.step)
                new_near_sell = base_near_sell - self.step
                if new_near_sell not in self.placed_sell_px_to_id and new_near_sell > 0:
                    await self._place_order(OrderSide.SELL, new_near_sell)
                    await asyncio.sleep(self.op_spacing_sec)
                # BUYを一番外側に追加
                base_outer_buy = min(self.placed_buy_px_to_id.keys()) if self.placed_buy_px_to_id else (min(filled_buy_prices) - self.step)
                new_outer_buy = base_outer_buy - self.step
                if new_outer_buy > 0 and new_outer_buy not in self.placed_buy_px_to_id:
                    await self._place_order(OrderSide.BUY, new_outer_buy)
                    await asyncio.sleep(self.op_spacing_sec)

            # SELLが約定した場合:
            #  - 反対側(BUY)の一番遠い指値(最小価格)を1つキャンセル
            #  - BUYを一番近い側に1つ追加（現在の最高BUYよりNだけ内側=より高い価格）
            #  - SELLを一番外側（現在の最高SELLよりNだけ外側=より高い価格）に1つ追加
            if filled_sell_prices:
                # 反対側の一番遠いBUYをキャンセル
                if self.placed_buy_px_to_id:
                    far_buy_px = min(self.placed_buy_px_to_id.keys())
                    far_buy_id = self.placed_buy_px_to_id.pop(far_buy_px)
                    try:
                        await self.adapter.cancel_order(far_buy_id)
                    except Exception:
                        logger.debug("cancel far BUY failed (ignore): id={} px={}", far_buy_id, far_buy_px)
                    await asyncio.sleep(self.op_spacing_sec)
                # BUYを一番近い側に追加
                base_near_buy = max(self.placed_buy_px_to_id.keys()) if self.placed_buy_px_to_id else (min(filled_sell_prices) - self.step)
                new_near_buy = base_near_buy + self.step
                if new_near_buy not in self.placed_buy_px_to_id and new_near_buy > 0:
                    await self._place_order(OrderSide.BUY, new_near_buy)
                    await asyncio.sleep(self.op_spacing_sec)
                # SELLを一番外側に追加
                base_outer_sell = max(self.placed_sell_px_to_id.keys()) if self.placed_sell_px_to_id else (max(filled_sell_prices) + self.step)
                new_outer_sell = base_outer_sell + self.step
                if new_outer_sell not in self.placed_sell_px_to_id:
                    await self._place_order(OrderSide.SELL, new_outer_sell)
                    await asyncio.sleep(self.op_spacing_sec)
        
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
