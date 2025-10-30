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
    """$STEPごとに両サイドへグリッド指値を差しっぱなしにするエンジン。

    - 刺さらない限りキャンセル/差し替えは一切しない
    - 片側levels本ずつ（ENVで調整）を現在価格を中心に新規配置
    - 価格が動いたら新しい価格帯に不足分のみ追加（過去の注文は放置）
    """

    def __init__(
        self,
        adapter: ExchangeAdapter,
        symbol: str,
        poll_interval_sec: float = 2.5,
    ) -> None:
        self.adapter = adapter
        self.symbol = symbol
        self.poll_interval_sec = max(1.5, float(poll_interval_sec))
        self._running = False
        self._loop_iter: int = 0

        # ENV
        self.size = float(os.getenv("EDGEX_GRID_SIZE", os.getenv("EDGEX_MM_SIZE", os.getenv("EDGEX_SIZE", "0.01"))))
        self.step = float(os.getenv("EDGEX_GRID_STEP_USD", "100"))
        # 最前列（現在価格に最も近い価格）だけは別オフセット（既定: 100 USD）
        self.first_offset = float(os.getenv("EDGEX_GRID_FIRST_OFFSET_USD", "100"))
        self.levels = int(os.getenv("EDGEX_GRID_LEVELS_PER_SIDE", "5"))
        logger.info(
            "グリッド設定: グリッド幅={}USD 初回オフセット={}USD レベル数={} サイズ={}BTC",
            self.step,
            self.first_offset,
            self.levels,
            self.size,
        )
        # レート制限回避のための連続発注間隔
        try:
            self.op_spacing_sec = float(os.getenv("EDGEX_GRID_OP_SPACING_SEC", "1.2"))
        except Exception:
            self.op_spacing_sec = 1.2
        # 初期配置済みフラグ（最前列±first_offsetは起動時に一度だけ）
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

    async def run(self) -> None:
        await self.adapter.connect()
        self._running = False
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
                    logger.debug("グリッドループ開始: iter={} 配置済み買い={} 配置済み売り={} 初期化済み={}", 
                                self._loop_iter, len(self.placed_buy_px_to_id), len(self.placed_sell_px_to_id), self.initialized)
                    
                    # 現在価格取得
                    try:
                        mid_price = await self.adapter.get_mid_price(self.symbol)  # type: ignore[attr-defined]
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

    async def _ensure_grid(self, mid_price: float) -> None:
        # 最良気配からスタートし、「初回のみ±first_offset」を出し、その後は±stepで片側levels本ずつ
        if self.step <= 0:
            return
        bid = ask = None
        try:
            bid, ask = await self.adapter.get_best_bid_ask(self.symbol)  # type: ignore[attr-defined]
        except Exception:
            pass
        logger.debug("グリッド配置確認: 中間価格={} 最良買い={} 最良売り={} 初回オフセット={} グリッド幅={} レベル数={}", 
                    mid_price, bid, ask, self.first_offset, self.step, self.levels)

        # フォールバック: 最良が無いときはmidから±step
        if bid is None:
            bid = mid_price
        if ask is None:
            ask = mid_price

        # BUY側
        buys = []
        if not self.initialized:
            # 初回のみ: first_offsetで1本
            buys.append(float(bid) - self.first_offset)
            # その後、stepずつ外側へ (levels-1) 本
            for j in range(1, self.levels):
                buys.append(float(bid) - self.first_offset - j * self.step)
        else:
            # 2回目以降: bidからstepずつ外側へlevels本
            for j in range(1, self.levels + 1):
                buys.append(float(bid) - j * self.step)
        
        logger.debug("計画された買い注文価格 ({}本): {}", len(buys), sorted([round(px, 2) for px in buys]))
        for px in buys:
            if px <= 0:
                continue
            if self._not_placed(px, side=OrderSide.BUY):
                logger.info("買い注文配置: 価格=${:.2f} (未配置)", px)
                await self._place_limit(side=OrderSide.BUY, price=px)

        # SELL側
        sells = []
        if not self.initialized:
            # 初回のみ: first_offsetで1本
            sells.append(float(ask) + self.first_offset)
            # その後、stepずつ外側へ (levels-1) 本
            for j in range(1, self.levels):
                sells.append(float(ask) + self.first_offset + j * self.step)
        else:
            # 2回目以降: askからstepずつ外側へlevels本
            for j in range(1, self.levels + 1):
                sells.append(float(ask) + j * self.step)
        
        logger.debug("計画された売り注文価格 ({}本): {}", len(sells), sorted([round(px, 2) for px in sells]))
        for px in sells:
            if self._not_placed(px, side=OrderSide.SELL):
                logger.info("売り注文配置: 価格=${:.2f} (未配置)", px)
                await self._place_limit(side=OrderSide.SELL, price=px)

        self.initialized = True
        logger.info(
            "グリッド初期化完了: 買い注文={} 売り注文={}",
            sorted([round(px, 2) for px in self.placed_buy_px_to_id.keys()]),
            sorted([round(px, 2) for px in self.placed_sell_px_to_id.keys()]),
        )
        self.tlog.log_event(event="GRID_INIT", symbol=self.symbol, data={
            "buys": sorted(list(self.placed_buy_px_to_id.keys())),
            "sells": sorted(list(self.placed_sell_px_to_id.keys())),
        })

    def _not_placed(self, price: float, side: OrderSide) -> bool:
        m = self.placed_buy_px_to_id if side == OrderSide.BUY else self.placed_sell_px_to_id
        # 価格は丸め後に微妙にズレる可能性があるが、キーは丸め前の目標価格で管理する
        return price not in m

    async def _place_limit(self, side: OrderSide, price: float) -> None:
        # レート制限に当たったら短い待機で2回まで再試行
        attempts = 3
        for attempt in range(attempts):
            try:
                logger.debug("注文発注試行: 試行回数={} サイド={} 価格={} サイズ={}", attempt + 1, side, price, self.size)
                order = OrderRequest(
                    symbol=self.symbol,
                    side=side,
                    type=OrderType.LIMIT,
                    quantity=self.size,
                    price=price,
                    time_in_force=TimeInForce.POST_ONLY,  # ← POST_ONLY追加
                )
                res = await self.adapter.place_order(order)
                if side == OrderSide.BUY:
                    self.placed_buy_px_to_id[price] = res.id
                else:
                    self.placed_sell_px_to_id[price] = res.id
                logger.info("グリッド注文配置完了: サイド={} 価格=${:.2f} サイズ={} 注文ID={}", side, price, self.size, res.id)
                self.tlog.log_order(
                    action="GRID_PLACE",
                    symbol=self.symbol,
                    side=side.value if hasattr(side, "value") else str(side),
                    size=self.size,
                    price=price,
                    order_id=res.id,
                )
                await asyncio.sleep(self.op_spacing_sec)
                return
            except Exception as e:
                msg = str(e)
                logger.warning("注文発注失敗 (試行{}/{}): {}", attempt + 1, attempts, msg)
                self.tlog.log_event(event="GRID_PLACE_FAIL", symbol=self.symbol, data={
                    "side": (side.value if hasattr(side, "value") else str(side)),
                    "price": price,
                    "reason": msg,
                    "attempt": attempt + 1,
                })
                # minSellPrice/maxBuyPriceにクランプして1回だけ再試行
                if ("minSellPrice" in msg or "maxBuyPrice" in msg) and attempt < attempts - 1:
                    try:
                        import re
                        if side == OrderSide.SELL:
                            m = re.search(r"minSellPrice': '([0-9.]+)", msg)
                            if m:
                                clamp = float(m.group(1))
                                if clamp > 0:
                                    price = max(price, clamp)
                                    logger.info("売り価格を最小価格にクランプ: ${:.2f} → 再試行", clamp)
                                    self.tlog.log_event(event="GRID_CLAMP", symbol=self.symbol, data={
                                        "side": "SELL",
                                        "clampTo": clamp,
                                    })
                        else:
                            m = re.search(r"maxBuyPrice': '([0-9.]+)", msg)
                            if m:
                                clamp = float(m.group(1))
                                if clamp > 0:
                                    price = min(price, clamp)
                                    logger.info("買い価格を最大価格にクランプ: ${:.2f} → 再試行", clamp)
                                    self.tlog.log_event(event="GRID_CLAMP", symbol=self.symbol, data={
                                        "side": "BUY",
                                        "clampTo": clamp,
                                    })
                    except Exception:
                        pass
                    await asyncio.sleep(max(1.0, self.op_spacing_sec))
                    continue
                if ("rateLimit" in msg or "rateLimitWindows" in msg or "429" in msg) and attempt < attempts - 1:
                    wait_s = max(1.0, self.op_spacing_sec)
                    logger.info("レート制限検出: {}秒待機後に再試行", wait_s)
                    self.tlog.log_event(event="GRID_RATE_LIMIT", symbol=self.symbol, data={
                        "wait_sec": wait_s,
                    })
                    await asyncio.sleep(wait_s)
                    continue
                logger.error("注文発注最終失敗: サイド={} 価格={} 理由={}", side, price, msg)
                self.tlog.log_event(event="GRID_PLACE_FAIL_FINAL", symbol=self.symbol, data={
                    "side": (side.value if hasattr(side, "value") else str(side)),
                    "price": price,
                    "reason": msg,
                })
                await asyncio.sleep(self.op_spacing_sec)
                return



    async def _replenish_if_filled(self) -> None:
        """約定を検出してグリッドを補充する。

        - SELL約定: 市場近くにBUYを追加し、SELL側を上方に延長
        - BUY約定: 市場近くにSELLを追加し、BUY側を下方に延長
        各サイド最大self.levels本まで保持
        """
        try:
            bid, ask = await self.adapter.get_best_bid_ask(self.symbol)  # type: ignore[attr-defined]
        except Exception as e:
            logger.debug("補充時の最良気配取得失敗: {}", e)
            self.tlog.log_event(event="GRID_BEST_BID_ASK_FAIL", symbol=self.symbol, data={"error": str(e)})
            bid = ask = None
        try:
            active = await self.adapter.list_active_orders(self.symbol)  # type: ignore[attr-defined]
        except Exception as e:
            logger.warning("アクティブ注文の取得失敗: {}", e)
            self.tlog.log_event(event="GRID_ACTIVE_FETCH_FAIL", symbol=self.symbol, data={"error": str(e)})
            active = []

        # アクティブ注文IDのセットを構築
        active_ids = set()
        for o in active:
            oid = None
            if hasattr(o, "id"):
                oid = o.id
            elif hasattr(o, "order_id"):
                oid = o.order_id
            elif isinstance(o, dict):
                oid = o.get("id") or o.get("order_id") or o.get("orderId")
            if oid:
                active_ids.add(str(oid))

        logger.debug(
            "補充確認: 最良買い={} 最良売り={} アクティブ注文数={} アクティブID数={} 配置済み買い={} 配置済み売り={}",
            bid,
            ask,
            len(active),
            len(active_ids),
            len(self.placed_buy_px_to_id),
            len(self.placed_sell_px_to_id),
        )

        # 約定済み価格を検出
        filled_buy_prices = []
        filled_sell_prices = []
        for px, oid in list(self.placed_buy_px_to_id.items()):
            if str(oid) not in active_ids:
                filled_buy_prices.append(px)
                self.placed_buy_px_to_id.pop(px, None)
        for px, oid in list(self.placed_sell_px_to_id.items()):
            if str(oid) not in active_ids:
                filled_sell_prices.append(px)
                self.placed_sell_px_to_id.pop(px, None)

        logger.debug(
            "補充: 約定検出 → 買い約定={} 売り約定={}",
            sorted(filled_buy_prices),
            sorted(filled_sell_prices),
        )

        if filled_sell_prices:
            filled_sell_prices.sort()
            logger.info("補充トリガー: 売り注文約定 価格={}", [round(px, 2) for px in filled_sell_prices])
            self.tlog.log_event(
                event="GRID_FILL_DETECTED",
                symbol=self.symbol,
                data={"side": "SELL", "prices": filled_sell_prices},
            )
            for filled_px in filled_sell_prices:
                new_buy_px = filled_px - self.step
                if bid is not None and bid > 0:
                    new_buy_px = min(new_buy_px, float(bid))
                while new_buy_px and new_buy_px > 0 and new_buy_px in self.placed_buy_px_to_id:
                    new_buy_px -= self.step
                if new_buy_px and new_buy_px > 0 and self._not_placed(new_buy_px, OrderSide.BUY):
                    logger.info("補充: 売り約定を補うため買い注文配置 → 価格=${:.2f}", new_buy_px)
                    await self._place_limit(OrderSide.BUY, new_buy_px)
            for filled_px in filled_sell_prices:
                reference = max(self.placed_sell_px_to_id.keys(), default=filled_px)
                reference = max(reference, filled_px)
                new_sell_px = reference + self.step
                if ask is not None and ask > 0:
                    new_sell_px = max(new_sell_px, float(ask))
                while new_sell_px in self.placed_sell_px_to_id:
                    new_sell_px += self.step
                if self._not_placed(new_sell_px, OrderSide.SELL):
                    logger.info("補充: 売り側延長 → 価格=${:.2f}", new_sell_px)
                    await self._place_limit(OrderSide.SELL, new_sell_px)

        if filled_buy_prices:
            filled_buy_prices.sort(reverse=True)
            logger.info("補充トリガー: 買い注文約定 価格={}", [round(px, 2) for px in filled_buy_prices])
            self.tlog.log_event(
                event="GRID_FILL_DETECTED",
                symbol=self.symbol,
                data={"side": "BUY", "prices": filled_buy_prices},
            )
            for filled_px in filled_buy_prices:
                new_sell_px = filled_px + self.step
                if ask is not None and ask > 0:
                    new_sell_px = max(new_sell_px, float(ask))
                while new_sell_px in self.placed_sell_px_to_id:
                    new_sell_px += self.step
                if self._not_placed(new_sell_px, OrderSide.SELL):
                    logger.info("補充: 買い約定を補うため売り注文配置 → 価格=${:.2f}", new_sell_px)
                    await self._place_limit(OrderSide.SELL, new_sell_px)
            for filled_px in filled_buy_prices:
                deepest = min(self.placed_buy_px_to_id.keys(), default=filled_px)
                new_buy_px = deepest - self.step
                while new_buy_px and new_buy_px > 0 and new_buy_px in self.placed_buy_px_to_id:
                    new_buy_px -= self.step
                if new_buy_px and new_buy_px > 0 and self._not_placed(new_buy_px, OrderSide.BUY):
                    logger.info("補充: 買い側延長 → 価格=${:.2f}", new_buy_px)
                    await self._place_limit(OrderSide.BUY, new_buy_px)

        # レベル数超過時は最も遠い注文をキャンセル
        if len(self.placed_buy_px_to_id) > self.levels:
            to_cancel_px = sorted(self.placed_buy_px_to_id.keys())[0]
            oid = self.placed_buy_px_to_id.pop(to_cancel_px)
            try:
                await self.adapter.cancel_order(oid)
                logger.info("レベル数超過: 買い注文キャンセル ID={} 価格=${:.2f}", oid, to_cancel_px)
            except Exception as e:
                logger.debug("買い注文キャンセル失敗: ID={} 価格=${:.2f} エラー={}", oid, to_cancel_px, e)
        if len(self.placed_sell_px_to_id) > self.levels:
            to_cancel_px = sorted(self.placed_sell_px_to_id.keys())[-1]
            oid = self.placed_sell_px_to_id.pop(to_cancel_px)
            try:
                await self.adapter.cancel_order(oid)
                logger.info("レベル数超過: 売り注文キャンセル ID={} 価格=${:.2f}", oid, to_cancel_px)
            except Exception as e:
                logger.debug("売り注文キャンセル失敗: ID={} 価格=${:.2f} エラー={}", oid, to_cancel_px, e)

    def stop(self) -> None:
        self._running = False


    async def _poll_closed_pnl_once(self) -> None:
        now = time.time()
        if self.closed_poll_sec <= 0:
            return
        if (now - self._last_closed_poll_ts) < self.closed_poll_sec:
            return
        self._last_closed_poll_ts = now

        try:
            client = getattr(self.adapter, "_client", None)
            if client is None:
                return
            from edgex_sdk.models.position import GetClosedPnlParams
            params = GetClosedPnlParams(
                filter_coin_id_list=[],
                filter_contract_id_list=[self.symbol],
                size="100",
                offset_data="",
            )
            resp = await client.position.get_closed_pnl(params)
            rows = []
            if hasattr(resp, "data") and hasattr(resp.data, "rows"):
                rows = resp.data.rows or []
            elif isinstance(resp, dict):
                data = resp.get("data", {})
                if isinstance(data, dict):
                    rows = data.get("rows", [])
            for row in rows:
                row_id = getattr(row, "id", None) or (row.get("id") if isinstance(row, dict) else None)
                if row_id and (self._last_closed_id is None or str(row_id) > str(self._last_closed_id)):
                    self._last_closed_id = str(row_id)
                    pnl = getattr(row, "pnl", None) or (row.get("pnl") if isinstance(row, dict) else None)
                    logger.info("決済済みPnL検出: ID={} PnL={}", row_id, pnl)
                    self.tlog.log_event(event="GRID_CLOSED_PNL", symbol=self.symbol, data={
                        "id": row_id,
                        "pnl": pnl,
                    })
        except Exception as e:
            logger.debug("決済済みPnLポーリング失敗: {}", e)
