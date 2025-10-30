from __future__ import annotations

import asyncio
import os
from typing import Dict, Optional
import time
from loguru import logger

from bot.adapters.base import ExchangeAdapter
from bot.models.types import OrderRequest, OrderSide, OrderType
from bot.utils.trade_logger import TradeLogger


class GridEngine:
    """$STEPごとに両サイドへグリッド指値を差しっぱなしにするエンジン。

    - 刺さらない限りキャンセル/差し替えは一切しない
    - 片側10本ずつ（ENVで調整）を現在価格を中心に新規配置
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
        logger.debug(
            "grid env raw: STEP_USD={} FIRST_OFFSET_USD={} LEVELS={} SIZE={}",
            os.getenv("EDGEX_GRID_STEP_USD"),
            os.getenv("EDGEX_GRID_FIRST_OFFSET_USD"),
            os.getenv("EDGEX_GRID_LEVELS_PER_SIDE"),
            (os.getenv("EDGEX_GRID_SIZE") or os.getenv("EDGEX_MM_SIZE") or os.getenv("EDGEX_SIZE")),
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
        self._running = True
        logger.info(
            "grid engine started: step_usd={} levels={} size={}",
            self.step,
            self.levels,
            self.size,
        )
        try:
            while self._running:
                try:
                    self._loop_iter += 1
                    logger.debug(
                        "grid loop start: iter={} placed_buy={} placed_sell={} initialized={}",
                        self._loop_iter,
                        len(self.placed_buy_px_to_id),
                        len(self.placed_sell_px_to_id),
                        self.initialized,
                    )
                    if not self.initialized:
                        # 初回だけ並べる
                        mid = None
                        try:
                            bid, ask = await self.adapter.get_best_bid_ask(self.symbol)  # type: ignore[attr-defined]
                            logger.debug("init phase: best bid={} ask={}", bid, ask)
                            if bid and ask:
                                mid = (float(bid) + float(ask)) / 2.0
                        except Exception as e:
                            logger.debug("best bid/ask fetch failed on init: {}", e)
                        if mid is None:
                            t = await self.adapter.get_ticker(self.symbol)
                            mid = float(t.price)
                        logger.debug("init phase: mid price decided={}", mid)
                        await self._ensure_grid(mid)
                    else:
                        # 初期配置後: 約定検知→補充（価格ドリフトでは追加しない）
                        logger.debug("replenish phase: start")
                        await self._replenish_if_filled()
                except Exception as e:
                    logger.warning("grid loop error: {}", e)
                logger.debug("grid loop end: iter={} sleep_sec={}", self._loop_iter, self.poll_interval_sec)
                await asyncio.sleep(self.poll_interval_sec)
                # 定期: クローズ損益の新規行を取り込み
                await self._poll_closed_pnl_once()
        finally:
            await self.adapter.close()
            logger.info("grid engine stopped")

    async def _ensure_grid(self, mid_price: float) -> None:
        # 最良気配からスタートし、「初回のみ±first_offset」を出し、その後は±stepで片側levels本ずつ
        if self.step <= 0:
            return
        bid = ask = None
        try:
            bid, ask = await self.adapter.get_best_bid_ask(self.symbol)  # type: ignore[attr-defined]
        except Exception:
            pass
        logger.debug("ensure_grid: inputs mid={} best_bid={} best_ask={} first_offset={} step={} levels={}", mid_price, bid, ask, self.first_offset, self.step, self.levels)

        # フォールバック: 最良が無いときはmidから±step
        if bid is None:
            bid = mid_price
        if ask is None:
            ask = mid_price

        # BUY側
        buys = []
        if not self.initialized:
            buys.append(float(bid) - self.first_offset)
        # 以降は -step 間隔（既存最小から外側へ）
        base_buy_start = float(bid) - (self.first_offset if not self.initialized else 0.0)
        for j in range(1, self.levels if not self.initialized else self.levels + 1):
            buys.append(base_buy_start - j * self.step)
        logger.debug("ensure_grid: planned BUY prices ({}): {}", len(buys), sorted([round(px, 8) for px in buys]))
        for px in buys:
            if px <= 0:
                continue
            if self._not_placed(px, side=OrderSide.BUY):
                logger.debug("ensure_grid: place BUY at {} (not placed yet)", px)
                await self._place_limit(side=OrderSide.BUY, price=px)

        # SELL側
        sells = []
        if not self.initialized:
            sells.append(float(ask) + self.first_offset)
        base_sell_start = float(ask) + (self.first_offset if not self.initialized else 0.0)
        for j in range(1, self.levels if not self.initialized else self.levels + 1):
            sells.append(base_sell_start + j * self.step)
        logger.debug("ensure_grid: planned SELL prices ({}): {}", len(sells), sorted([round(px, 8) for px in sells]))
        for px in sells:
            if self._not_placed(px, side=OrderSide.SELL):
                logger.debug("ensure_grid: place SELL at {} (not placed yet)", px)
                await self._place_limit(side=OrderSide.SELL, price=px)

        self.initialized = True
        logger.info(
            "grid initialized: buys={} sells={}",
            sorted(list(self.placed_buy_px_to_id.keys())),
            sorted(list(self.placed_sell_px_to_id.keys())),
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
                logger.debug("place_limit: attempt={} side={} price={} size={}", attempt + 1, side, price, self.size)
                order = OrderRequest(
                    symbol=self.symbol,
                    side=side,
                    type=OrderType.LIMIT,
                    quantity=self.size,
                    price=price,
                )
                res = await self.adapter.place_order(order)
                if side == OrderSide.BUY:
                    self.placed_buy_px_to_id[price] = res.id
                else:
                    self.placed_sell_px_to_id[price] = res.id
                logger.info("grid placed: side={} px={} size={} id={}", side, price, self.size, res.id)
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
                logger.warning("grid place failed: side={} px={} reason={}", side, price, msg)
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
                            m = re.search(r"minSellPrice': '([0-9.]+)" , msg)
                            if m:
                                clamp = float(m.group(1))
                                if clamp > 0:
                                    price = max(price, clamp)
                                    logger.info("clamp SELL price to minSellPrice={} and retry", clamp)
                                    self.tlog.log_event(event="GRID_CLAMP", symbol=self.symbol, data={
                                        "side": "SELL",
                                        "clampTo": clamp,
                                    })
                        else:
                            m = re.search(r"maxBuyPrice': '([0-9.]+)" , msg)
                            if m:
                                clamp = float(m.group(1))
                                if clamp > 0:
                                    price = min(price, clamp)
                                    logger.info("clamp BUY price to maxBuyPrice={} and retry", clamp)
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
                    logger.info("rate limit detected, wait {}s then retry", wait_s)
                    self.tlog.log_event(event="GRID_RATE_LIMIT", symbol=self.symbol, data={
                        "wait_s": wait_s,
                        "attempt": attempt + 1,
                    })
                    await asyncio.sleep(wait_s)
                    continue
                logger.warning("grid place failed (final): {}", e)
                self.tlog.log_event(event="GRID_PLACE_FAIL_FINAL", symbol=self.symbol, data={
                    "side": (side.value if hasattr(side, "value") else str(side)),
                    "price": price,
                    "reason": msg,
                })
                await asyncio.sleep(self.op_spacing_sec)
                return



    async def _replenish_if_filled(self) -> None:
        """Detect fills from the active-order snapshot and rebuild the grid around price.

        - SELL fill: add a BUY close to the market and extend the SELL ladder upward.
        - BUY fill: add a SELL close to the market and extend the BUY ladder downward.
        Keeps at most self.levels orders on each side.
        """
        try:
            bid, ask = await self.adapter.get_best_bid_ask(self.symbol)  # type: ignore[attr-defined]
        except Exception as e:
            logger.debug("best bid/ask fetch failed in replenish: {}", e)
            self.tlog.log_event(event="GRID_BEST_BID_ASK_FAIL", symbol=self.symbol, data={"error": str(e)})
            bid = ask = None
        try:
            active = await self.adapter.list_active_orders(self.symbol)  # type: ignore[attr-defined]
        except Exception as e:
            logger.warning("active orders fetch failed: {}", e)
            self.tlog.log_event(event="GRID_ACTIVE_FETCH_FAIL", symbol=self.symbol, data={"error": str(e)})
            active = []

        # Build a set of active order IDs regardless of side; extract robustly across shapes
        active_ids = set()
        try:
            for it in (active or []):
                if not isinstance(it, dict):
                    continue
                oid = (
                    str((it.get("raw") or {}).get("orderId") or it.get("orderId") or it.get("id") or "")
                )
                if oid:
                    active_ids.add(oid)
        except Exception:
            pass

        logger.debug(
            "replenish: best_bid={} best_ask={} active_count={} active_ids={} placed_buy={} placed_sell={}",
            bid,
            ask,
            (0 if active is None else len(active)),
            len(active_ids),
            len(self.placed_buy_px_to_id),
            len(self.placed_sell_px_to_id),
        )

        if active is None or len(active) == 0:
            logger.debug("skip replenish: active orders empty")
            self.tlog.log_event(event="GRID_SKIP_REPLENISH_EMPTY_ACTIVE", symbol=self.symbol, data={})
            return

        filled_buy_prices: list[float] = []
        filled_sell_prices: list[float] = []

        # Detect fills via ID disappearance, side is inferred from which map held the id
        for px, oid in list(self.placed_buy_px_to_id.items()):
            if str(oid) not in active_ids:
                filled_buy_prices.append(px)
                self.placed_buy_px_to_id.pop(px, None)
        for px, oid in list(self.placed_sell_px_to_id.items()):
            if str(oid) not in active_ids:
                filled_sell_prices.append(px)
                self.placed_sell_px_to_id.pop(px, None)

        logger.debug(
            "replenish: detected fills -> BUY_filled={} SELL_filled={}",
            sorted(filled_buy_prices),
            sorted(filled_sell_prices),
        )

        if filled_sell_prices:
            filled_sell_prices.sort()
            logger.info("replenish trigger: detected SELL fill prices={}", filled_sell_prices)
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
                    logger.debug("replenish: place BUY to replace SELL fill -> new_buy_px={}", new_buy_px)
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
                    logger.debug("replenish: extend SELL ladder -> new_sell_px={}", new_sell_px)
                    await self._place_limit(OrderSide.SELL, new_sell_px)

        if filled_buy_prices:
            filled_buy_prices.sort(reverse=True)
            logger.info("replenish trigger: detected BUY fill prices={}", filled_buy_prices)
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
                    logger.debug("replenish: place SELL to replace BUY fill -> new_sell_px={}", new_sell_px)
                    await self._place_limit(OrderSide.SELL, new_sell_px)
            for filled_px in filled_buy_prices:
                deepest = min(self.placed_buy_px_to_id.keys(), default=filled_px)
                new_buy_px = deepest - self.step
                while new_buy_px and new_buy_px > 0 and new_buy_px in self.placed_buy_px_to_id:
                    new_buy_px -= self.step
                if new_buy_px and new_buy_px > 0 and self._not_placed(new_buy_px, OrderSide.BUY):
                    logger.debug("replenish: extend BUY ladder -> new_buy_px={}", new_buy_px)
                    await self._place_limit(OrderSide.BUY, new_buy_px)

        if len(self.placed_buy_px_to_id) > self.levels:
            to_cancel_px = sorted(self.placed_buy_px_to_id.keys())[0]
            oid = self.placed_buy_px_to_id.pop(to_cancel_px)
            try:
                await self.adapter.cancel_order(oid)
            except Exception:
                logger.debug("force cancel buy id={} px={} to keep levels", oid, to_cancel_px)
        if len(self.placed_sell_px_to_id) > self.levels:
            to_cancel_px = sorted(self.placed_sell_px_to_id.keys())[-1]
            oid = self.placed_sell_px_to_id.pop(to_cancel_px)
            try:
                await self.adapter.cancel_order(oid)
            except Exception:
                logger.debug("force cancel sell id={} px={} to keep levels", oid, to_cancel_px)

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
            rows = None

            async def _call_get_page(fn):
                import inspect as _inspect
                from types import SimpleNamespace
                params_named = {}
                try:
                    sig = _inspect.signature(fn)
                    names = sig.parameters.keys()
                except Exception:
                    names = []
                if "account_id" in names:
                    params_named["account_id"] = self.adapter.account_id  # type: ignore[attr-defined]
                elif "accountId" in names:
                    params_named["accountId"] = str(self.adapter.account_id)  # type: ignore[attr-defined]
                if "size" in names:
                    params_named["size"] = 50
                elif "pageSize" in names:
                    params_named["pageSize"] = 50
                if "params" in names and len(names) == 1:
                    # SDKによっては dataclass Params を要求（属性アクセス: .size 等）
                    params_obj = None
                    try:
                        from edgex_sdk.account.types import GetPositionTransactionPageParams  # type: ignore
                        params_obj = GetPositionTransactionPageParams()
                        # 該当フィールド名の差異を吸収
                        if hasattr(params_obj, "account_id"):
                            setattr(params_obj, "account_id", self.adapter.account_id)  # type: ignore[attr-defined]
                        elif hasattr(params_obj, "accountId"):
                            setattr(params_obj, "accountId", str(self.adapter.account_id))  # type: ignore[attr-defined]
                        if hasattr(params_obj, "size"):
                            setattr(params_obj, "size", str(params_named.get("size", params_named.get("pageSize", 50))))
                        elif hasattr(params_obj, "pageSize"):
                            setattr(params_obj, "pageSize", str(params_named.get("pageSize", params_named.get("size", 50))))
                    except Exception:
                        params_obj = None
                    if params_obj is None:
                        params_obj = SimpleNamespace(
                            accountId=params_named.get("accountId", str(self.adapter.account_id)),  # type: ignore[attr-defined]
                            size=str(params_named.get("size", params_named.get("pageSize", 50))),
                        )
                    return await fn(params=params_obj)
                return await fn(**params_named) if params_named else await fn()

            if hasattr(client, "account") and hasattr(client.account, "get_position_transaction_page"):
                res = await _call_get_page(client.account.get_position_transaction_page)
                rows = ((res or {}).get("data") or {}).get("dataList")
            elif hasattr(client, "get_position_transaction_page"):
                res = await _call_get_page(client.get_position_transaction_page)
                rows = ((res or {}).get("data") or {}).get("dataList")
            if not rows:
                return
            new_rows = []
            for r in rows:
                rid = str(r.get("id") or "")
                if self._last_closed_id is None or rid > self._last_closed_id:
                    new_rows.append(r)
            if new_rows:
                new_rows = sorted(new_rows, key=lambda r: r.get("id"))
                self._last_closed_id = str(new_rows[-1].get("id"))
                appended = self.tlog.log_closed_rows(new_rows)
                if appended:
                    logger.info("closed pnl appended {} rows", appended)
        except Exception as e:
            logger.debug("closed pnl poll failed: {}", e)

