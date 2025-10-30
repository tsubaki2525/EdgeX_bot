from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass
from typing import Optional
from loguru import logger

from bot.adapters.base import ExchangeAdapter
from bot.models.types import OrderRequest, OrderSide, OrderType
from bot.utils.trade_logger import TradeLogger
import httpx


@dataclass
class WorkingOrders:
    bid_id: Optional[str] = None
    bid_px: Optional[float] = None
    ask_id: Optional[str] = None
    ask_px: Optional[float] = None
    tp_id: Optional[str] = None
    tp_px: Optional[float] = None
    last_quote_ts: float = 0.0
    # pre-linked TP (placed together with entry quotes when flat)
    bid_tp_id: Optional[str] = None
    bid_tp_px: Optional[float] = None
    ask_tp_id: Optional[str] = None
    ask_tp_px: Optional[float] = None


class MarketMakerEngine:
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

        # params
        self.quote_bps = float(os.getenv("EDGEX_MM_QUOTE_BPS", os.getenv("EDGEX_LIMIT_OFFSET_BPS", "10")))
        self.reprice_bps = float(os.getenv("EDGEX_MM_REPRICE_BPS", "15"))
        self.ttl_sec = float(os.getenv("EDGEX_MM_TTL_SEC", "30"))
        self.size = float(os.getenv("EDGEX_MM_SIZE", os.getenv("EDGEX_SIZE", "0.1")))
        self.breakeven_bps = float(os.getenv("EDGEX_MM_BREAKEVEN_BPS", "0"))  # 追加目標（bps）。0で純プラテン
        # 手数料（bps）。個別指定が無ければ EDGEX_FEE_BPS を両側に適用
        self.fee_in_bps = float(os.getenv("EDGEX_ENTRY_FEE_BPS", os.getenv("EDGEX_FEE_BPS", "0")))
        self.fee_out_bps = float(os.getenv("EDGEX_EXIT_FEE_BPS", os.getenv("EDGEX_FEE_BPS", "0")))

        # state
        self.orders = WorkingOrders()
        self.position_size: float = 0.0
        self.entry_price: Optional[float] = None
        self.always_two_sided = (os.getenv("EDGEX_MM_ALWAYS_TWO_SIDED", "false").lower() in ("1", "true", "yes"))
        self.place_tp_with_entry = (os.getenv("EDGEX_MM_PLACE_TP_WITH_ENTRY", "true").lower() in ("1", "true", "yes"))
        self.logger_csv = TradeLogger()
        # 取引所レート制限回避のため、連続発注の間隔（秒）
        try:
            self.op_spacing_sec = float(os.getenv("EDGEX_MM_OP_SPACING_SEC", "1.2"))
        except Exception:
            self.op_spacing_sec = 1.2
        # closed PnL poll interval (sec)
        try:
            # 既定は無効化（0）。必要なら .env で秒数を指定して有効化
            self.closed_poll_sec = float(os.getenv("EDGEX_MM_CLOSED_PNL_SEC", "0"))
        except Exception:
            self.closed_poll_sec = 0.0
        self._last_closed_id: str | None = None
        # 片側クオート指定（BUY/SELL）。指定があればフラット時は片側のみ提示
        self.one_side: str | None = (os.getenv("EDGEX_MM_SIDE") or "").upper() or None
        # closed pnl poll timer
        self._last_closed_poll_ts: float = 0.0
        # always use best bid/ask instead of mid (forced ON)
        self.use_best = True
        # makerギャップ（bps）: 最良気配の内側に必ず入れる
        try:
            self.maker_gap_bps = float(os.getenv("EDGEX_MM_MAKER_GAP_BPS", "2"))
        except Exception:
            self.maker_gap_bps = 2.0
        # 逆行時の緊急クローズ閾値（bps）。既定は手数料往復分に相当
        try:
            self.stop_bps = float(os.getenv("EDGEX_MM_STOP_BPS", str(max(0.0, self.fee_in_bps + self.fee_out_bps))))
        except Exception:
            self.stop_bps = self.fee_in_bps + self.fee_out_bps

    async def run(self) -> None:
        await self.adapter.connect()
        self._running = True
        logger.info(
            "mm engine started: quote_bps={} reprice_bps={} ttl_sec={} size={} breakeven_bps={}",
            self.quote_bps,
            self.reprice_bps,
            self.ttl_sec,
            self.size,
            self.breakeven_bps,
        )
        try:
            while self._running:
                try:
                    ticker = await self.adapter.get_ticker(self.symbol)
                except Exception as e:
                    logger.warning("ticker failed: {}", e)
                    await asyncio.sleep(self.poll_interval_sec * 2)
                    continue

                now = time.time()

                if self.position_size == 0.0:
                    # flat
                    need_requote = False
                    if self.orders.last_quote_ts == 0:
                        need_requote = True
                    elif (now - self.orders.last_quote_ts) >= self.ttl_sec:
                        need_requote = True
                    else:
                        ref = (self.orders.bid_px or ticker.price)
                        move_bps = abs(ticker.price / ref - 1.0) * 10000.0
                        if move_bps >= self.reprice_bps:
                            need_requote = True

                    if need_requote:
                        # cancel existing; infer fill if cancel fails
                        await self._try_cancel_quotes(infer_fill=True)

                        if self.position_size == 0.0:
                            if self.one_side in ("BUY", "SELL"):
                                # 片側のみ提示
                                side = OrderSide.BUY if self.one_side == "BUY" else OrderSide.SELL
                                if self.use_best:
                                    bid, ask = await self.adapter.get_best_bid_ask(self.symbol)  # type: ignore[attr-defined]
                                else:
                                    bid, ask = None, None
                                gap = self.maker_gap_bps / 10000.0
                                if side == OrderSide.BUY:
                                    base_px = bid if bid is not None else ticker.price
                                    px = base_px * (1.0 - gap)  # maker内側
                                    if self.quote_bps:
                                        px *= (1.0 - self.quote_bps / 10000.0)
                                else:
                                    base_px = ask if ask is not None else ticker.price
                                    px = base_px * (1.0 + gap)
                                    if self.quote_bps:
                                        px *= (1.0 + self.quote_bps / 10000.0)
                                oid = await self._place(self.symbol, side, self.size, px)
                                if side == OrderSide.BUY:
                                    self.orders.bid_id = oid; self.orders.bid_px = px
                                    self.orders.ask_id = None; self.orders.ask_px = None
                                else:
                                    self.orders.ask_id = oid; self.orders.ask_px = px
                                    self.orders.bid_id = None; self.orders.bid_px = None
                                self.orders.last_quote_ts = now
                                logger.info("quoted(one-side): side={} px={} size={}", side, px, self.size)
                                self.logger_csv.log_order(action="QUOTE_ONE", symbol=self.symbol, side=side.value if hasattr(side, "value") else str(side), size=self.size, price=px, order_id=oid)

                                if self.place_tp_with_entry:
                                    # エントリー想定のTP先置き
                                    tp_px, tp_side = self._calc_tp_with_fees(side == OrderSide.BUY, px, max(0.0, self.breakeven_bps))
                                    tp_oid = await self._place(self.symbol, tp_side, self.size, tp_px)
                                    if side == OrderSide.BUY:
                                        self.orders.bid_tp_id = tp_oid; self.orders.bid_tp_px = tp_px
                                    else:
                                        self.orders.ask_tp_id = tp_oid; self.orders.ask_tp_px = tp_px
                                    self.logger_csv.log_order(action="PRE_TP_ONE", symbol=self.symbol, side=tp_side.value if hasattr(tp_side, "value") else str(tp_side), size=self.size, price=tp_px, order_id=tp_oid)
                            else:
                                # 従来の両サイド（fallback）
                                if self.use_best:
                                    bid, ask = await self.adapter.get_best_bid_ask(self.symbol)  # type: ignore[attr-defined]
                                else:
                                    bid, ask = None, None
                                gap = self.maker_gap_bps / 10000.0
                                base_bid = bid if bid is not None else ticker.price
                                base_ask = ask if ask is not None else ticker.price
                                bid_px = base_bid * (1.0 - gap)
                                ask_px = base_ask * (1.0 + gap)
                                if self.quote_bps:
                                    bid_px *= (1.0 - self.quote_bps / 10000.0)
                                    ask_px *= (1.0 + self.quote_bps / 10000.0)
                                self.orders.bid_id = await self._place(self.symbol, OrderSide.BUY, self.size, bid_px)
                                await asyncio.sleep(self.op_spacing_sec)
                                self.orders.ask_id = await self._place(self.symbol, OrderSide.SELL, self.size, ask_px)
                                self.orders.bid_px = bid_px
                                self.orders.ask_px = ask_px
                                self.orders.last_quote_ts = now
                                logger.info("quoted: bid_px={} ask_px={} size={}", bid_px, ask_px, self.size)
                                self.logger_csv.log_order(action="QUOTE_BID", symbol=self.symbol, side="BUY", size=self.size, price=bid_px, order_id=self.orders.bid_id)
                                self.logger_csv.log_order(action="QUOTE_ASK", symbol=self.symbol, side="SELL", size=self.size, price=ask_px, order_id=self.orders.ask_id)
                                if self.place_tp_with_entry:
                                    tp_px_long, tp_side_long = self._calc_tp_with_fees(True, bid_px, max(0.0, self.breakeven_bps))
                                    self.orders.bid_tp_id = await self._place(self.symbol, tp_side_long, self.size, tp_px_long)
                                    self.orders.bid_tp_px = tp_px_long
                                    self.logger_csv.log_order(action="PRE_TP_LONG", symbol=self.symbol, side=tp_side_long.value if hasattr(tp_side_long, "value") else str(tp_side_long), size=self.size, price=tp_px_long, order_id=self.orders.bid_tp_id)
                                    tp_px_short, tp_side_short = self._calc_tp_with_fees(False, ask_px, max(0.0, self.breakeven_bps))
                                    await asyncio.sleep(self.op_spacing_sec)
                                    self.orders.ask_tp_id = await self._place(self.symbol, tp_side_short, self.size, tp_px_short)
                                    self.orders.ask_tp_px = tp_px_short
                                    self.logger_csv.log_order(action="PRE_TP_SHORT", symbol=self.symbol, side=tp_side_short.value if hasattr(tp_side_short, "value") else str(tp_side_short), size=self.size, price=tp_px_short, order_id=self.orders.ask_tp_id)
                                    logger.info("pre-TP placed: long_tp_px={} short_tp_px={}", tp_px_long, tp_px_short)
                        else:
                            # we detected a fill via cancel failure; will place TP in next branch
                            pass

                if self.position_size != 0.0:
                    # have position: place TP at breakeven (or better)
                    is_long = self.position_size > 0
                    target_bps = max(0.0, self.breakeven_bps)
                    if self.entry_price is None:
                        self.entry_price = ticker.price
                    tp_px, side = self._calc_tp_with_fees(is_long, self.entry_price, target_bps)
                    # maker内側に補正（TP側も跨ぎ回避）
                    try:
                        bid, ask = await self.adapter.get_best_bid_ask(self.symbol)  # type: ignore[attr-defined]
                    except Exception:
                        bid, ask = None, None
                    gap = self.maker_gap_bps / 10000.0
                    if side == OrderSide.SELL and ask is not None:
                        tp_px = max(tp_px, ask * (1.0 + gap))
                    if side == OrderSide.BUY and bid is not None:
                        tp_px = min(tp_px, bid * (1.0 - gap))

                    if not self.orders.tp_id:
                        self.orders.tp_id = await self._place(self.symbol, side, abs(self.position_size), tp_px)
                        self.orders.tp_px = tp_px
                        logger.info("placed TP: px={} size={} side={}", tp_px, abs(self.position_size), side)
                        self.logger_csv.log_order(action="TP_PLACE", symbol=self.symbol, side=side.value if hasattr(side, "value") else str(side), size=abs(self.position_size), price=tp_px, order_id=self.orders.tp_id)
                    else:
                        # optional: reprice TP if far from current (keep simple)
                        pass

                    # keep quoting on one side while holding position (optional)
                    if self.always_two_sided:
                        # 要件: ポジションを持っていない方向だけ（= エクスポージャを減らす側）を提示
                        quote_side = OrderSide.SELL if is_long else OrderSide.BUY
                        want_px = (
                            ticker.price * (1.0 + self.quote_bps / 10000.0)
                            if quote_side == OrderSide.SELL
                            else ticker.price * (1.0 - self.quote_bps / 10000.0)
                        )
                        # decide current working id/px
                        wid, wpx = (self.orders.bid_id, self.orders.bid_px) if quote_side == OrderSide.BUY else (self.orders.ask_id, self.orders.ask_px)
                        need_quote = False
                        if wid is None:
                            need_quote = True
                        else:
                            # reprice by movement or ttl
                            moved_bps = abs((ticker.price / (wpx or ticker.price)) - 1.0) * 10000.0
                            if moved_bps >= self.reprice_bps:
                                # cancel and replace
                                try:
                                    await self.adapter.cancel_order(wid)
                                except Exception:
                                    pass
                                if quote_side == OrderSide.BUY:
                                    self.orders.bid_id = None; self.orders.bid_px = None
                                else:
                                    self.orders.ask_id = None; self.orders.ask_px = None
                                need_quote = True
                        if need_quote:
                            oid = await self._place(self.symbol, quote_side, self.size, want_px)
                            if quote_side == OrderSide.BUY:
                                self.orders.bid_id = oid; self.orders.bid_px = want_px
                            else:
                                self.orders.ask_id = oid; self.orders.ask_px = want_px
                            logger.info("quote while position: side={} px={} size={}", quote_side, want_px, self.size)
                            self.logger_csv.log_order(action="QUOTE_POS", symbol=self.symbol, side=quote_side.value if hasattr(quote_side, "value") else str(quote_side), size=self.size, price=want_px, order_id=oid)

                # PnLログ（含み損益、手数料控除）
                try:
                    if self.position_size != 0.0 and self.entry_price is not None:
                        unreal, unreal_net = self._unrealized_pnl_with_fees(ticker.price)
                        logger.info("pnl unreal: gross={} net(after fees)={}", unreal, unreal_net)
                        # 逆行ストップ（成行相当で即時クローズ）
                        if self.stop_bps > 0:
                            # ネット損失が stop_bps を超えたら市場クローズ
                            thresh_value = abs(self.position_size) * self.entry_price * (self.stop_bps / 10000.0)
                            if -unreal_net >= thresh_value:
                                await self._force_exit_market()
                except Exception:
                    pass

                await asyncio.sleep(self.poll_interval_sec)
                # 定期: クローズ損益の新規行を取り込み
                await self._poll_closed_pnl_once()
        finally:
            await self.adapter.close()
            logger.info("mm engine stopped")

    async def _place(self, symbol: str, side: OrderSide, size: float, price: float) -> Optional[str]:
        retries = 2
        delay = max(1.0, self.op_spacing_sec)
        for attempt in range(retries + 1):
            try:
                order = OrderRequest(symbol=symbol, side=side, type=OrderType.LIMIT, quantity=size, price=price)
                res = await self.adapter.place_order(order)
                return res.id
            except Exception as e:
                msg = str(e)
                logger.warning("place failed: {}", msg)
                if ("rateLimit" in msg or "429" in msg or "rateLimitWindows" in msg) and attempt < retries:
                    await asyncio.sleep(delay)
                    continue
                return None

    async def _try_cancel_quotes(self, infer_fill: bool) -> None:
        # cancel bid
        if self.orders.bid_id:
            try:
                await self.adapter.cancel_order(self.orders.bid_id)
            except Exception as e:
                if infer_fill:
                    # assume bid got filled -> long
                    self.position_size = self.size
                    self.entry_price = self.orders.bid_px
                    logger.info("assume filled LONG via cancel failure: {}", e)
                    self.logger_csv.log_event(event="FILL_LONG_ASSUME", symbol=self.symbol, data={"entry_px": self.entry_price, "size": self.position_size})
                    # keep its linked TP if exists
                    if self.orders.bid_tp_id:
                        self.orders.tp_id = self.orders.bid_tp_id
                        self.orders.tp_px = self.orders.bid_tp_px
                    # cancel opposite side (ask + its TP)
                    if self.orders.ask_id:
                        try:
                            await self.adapter.cancel_order(self.orders.ask_id)
                        except Exception:
                            pass
                        finally:
                            self.orders.ask_id = None; self.orders.ask_px = None
                    if self.orders.ask_tp_id:
                        try:
                            await self.adapter.cancel_order(self.orders.ask_tp_id)
                        except Exception:
                            pass
                        finally:
                            self.orders.ask_tp_id = None; self.orders.ask_tp_px = None
            finally:
                self.orders.bid_id = None
                self.orders.bid_px = None
                # if cancel succeeded (no fill), also cancel its pre-TP
                if self.position_size == 0.0 and self.orders.bid_tp_id:
                    try:
                        await self.adapter.cancel_order(self.orders.bid_tp_id)
                    except Exception:
                        pass
                    finally:
                        self.orders.bid_tp_id = None; self.orders.bid_tp_px = None
        # cancel ask
        if self.orders.ask_id:
            try:
                await self.adapter.cancel_order(self.orders.ask_id)
            except Exception as e:
                if infer_fill:
                    # assume ask got filled -> short
                    self.position_size = -self.size
                    self.entry_price = self.orders.ask_px
                    logger.info("assume filled SHORT via cancel failure: {}", e)
                    self.logger_csv.log_event(event="FILL_SHORT_ASSUME", symbol=self.symbol, data={"entry_px": self.entry_price, "size": self.position_size})
                    # keep its linked TP if exists
                    if self.orders.ask_tp_id:
                        self.orders.tp_id = self.orders.ask_tp_id
                        self.orders.tp_px = self.orders.ask_tp_px
                    # cancel opposite side (bid + its TP)
                    if self.orders.bid_id:
                        try:
                            await self.adapter.cancel_order(self.orders.bid_id)
                        except Exception:
                            pass
                        finally:
                            self.orders.bid_id = None; self.orders.bid_px = None
                    if self.orders.bid_tp_id:
                        try:
                            await self.adapter.cancel_order(self.orders.bid_tp_id)
                        except Exception:
                            pass
                        finally:
                            self.orders.bid_tp_id = None; self.orders.bid_tp_px = None
            finally:
                self.orders.ask_id = None
                self.orders.ask_px = None
                # if cancel succeeded (no fill), also cancel its pre-TP
                if self.position_size == 0.0 and self.orders.ask_tp_id:
                    try:
                        await self.adapter.cancel_order(self.orders.ask_tp_id)
                    except Exception:
                        pass
                    finally:
                        self.orders.ask_tp_id = None; self.orders.ask_tp_px = None
        # cancel TP (when flat)
        if self.position_size == 0.0 and self.orders.tp_id:
            try:
                await self.adapter.cancel_order(self.orders.tp_id)
            except Exception:
                pass
            finally:
                self.orders.tp_id = None
                self.orders.tp_px = None

    async def _force_exit_market(self) -> None:
        """Emergency market-like close at best available (by placing aggressive limit)."""
        if self.position_size == 0.0:
            return
        qty = abs(self.position_size)
        # fetch best
        try:
            bid, ask = await self.adapter.get_best_bid_ask(self.symbol)  # type: ignore[attr-defined]
        except Exception:
            bid, ask = None, None
        side = OrderSide.SELL if self.position_size > 0 else OrderSide.BUY
        # aggressive limit: cross by 10 bps to ensure fill
        cross = 0.001
        if side == OrderSide.SELL:
            px = (bid or 0.0) * (1.0 - cross) if bid else None
        else:
            px = (ask or 0.0) * (1.0 + cross) if ask else None
        order = OrderRequest(symbol=self.symbol, side=side, type=OrderType.LIMIT, quantity=qty, price=px)
        try:
            res = await self.adapter.place_order(order)
            logger.warning("force exit placed: side={} qty={} px={}", side, qty, px)
            self.logger_csv.log_event(event="FORCE_EXIT", symbol=self.symbol, data={"side": str(side), "qty": qty, "px": px, "order_id": res.id})
        except Exception as e:
            logger.error("force exit failed: {}", e)

    def stop(self) -> None:
        self._running = False

    def _calc_tp_with_fees(self, is_long: bool, entry_px: float, target_bps: float) -> tuple[float, OrderSide]:
        fe_in = self.fee_in_bps / 10000.0
        fe_out = self.fee_out_bps / 10000.0
        if is_long:
            # Solve: (exit - entry) - (entry*fe_in + exit*fe_out) >= 0 at minimum
            base_exit = (entry_px * (1.0 + fe_in)) / max(1e-12, (1.0 - fe_out))
            exit_px = base_exit * (1.0 + target_bps / 10000.0)
            return exit_px, OrderSide.SELL
        else:
            # Short: (entry - exit) - (entry*fe_in + exit*fe_out) >= 0
            base_exit = (entry_px * (1.0 - fe_in)) / (1.0 + fe_out)
            exit_px = base_exit * (1.0 - target_bps / 10000.0)
            return exit_px, OrderSide.BUY

    def _unrealized_pnl_with_fees(self, current_px: float) -> tuple[float, float]:
        """Return (gross, net_after_fees) in quote currency (USDT想定)。
        片道手数料は entry/exit にそれぞれ fee_in/fee_out を適用。
        """
        assert self.entry_price is not None
        qty = abs(self.position_size)
        entry = self.entry_price
        if self.position_size > 0:
            gross = (current_px - entry) * qty
            fees = entry * qty * (self.fee_in_bps / 10000.0) + current_px * qty * (self.fee_out_bps / 10000.0)
        else:
            gross = (entry - current_px) * qty
            fees = entry * qty * (self.fee_in_bps / 10000.0) + current_px * qty * (self.fee_out_bps / 10000.0)
        return gross, gross - fees

    async def _poll_closed_pnl_once(self) -> None:
        now = time.time()
        # throttle by dedicated timer
        if self.closed_poll_sec <= 0:
            return
        if (now - self._last_closed_poll_ts) < self.closed_poll_sec:
            return
        self._last_closed_poll_ts = now

        try:
            # Use underlying SDK client if available; adapt to parameter names dynamically.
            client = getattr(self.adapter, "_client", None)
            if client is None:
                return
            rows = None
            async def _call_get_page(fn):
                import inspect as _inspect
                params_named = {}
                try:
                    sig = _inspect.signature(fn)
                    names = sig.parameters.keys()
                except Exception:
                    names = []
                # account id param
                if "account_id" in names:
                    params_named["account_id"] = self.adapter.account_id
                elif "accountId" in names:
                    params_named["accountId"] = str(self.adapter.account_id)
                # size param
                if "size" in names:
                    params_named["size"] = 50
                elif "pageSize" in names:
                    params_named["pageSize"] = 50
                # some SDKs expect a single dict arg named 'params'
                if "params" in names and len(names) == 1:
                    call_params = {
                        "accountId": params_named.get("accountId", str(self.adapter.account_id)),
                        "size": str(params_named.get("size", params_named.get("pageSize", 50))),
                    }
                    return await fn(params=call_params)
                return await fn(**params_named) if params_named else await fn()

            if hasattr(client, "account") and hasattr(client.account, "get_position_transaction_page"):
                res = await _call_get_page(client.account.get_position_transaction_page)
                rows = ((res or {}).get("data") or {}).get("dataList")
            elif hasattr(client, "get_position_transaction_page"):
                res = await _call_get_page(client.get_position_transaction_page)
                rows = ((res or {}).get("data") or {}).get("dataList")
            if not rows:
                return
            # filter new rows by id
            new_rows = []
            for r in rows:
                rid = str(r.get("id") or "")
                if self._last_closed_id is None or rid > self._last_closed_id:
                    new_rows.append(r)
            if new_rows:
                new_rows = sorted(new_rows, key=lambda r: r.get("id"))
                self._last_closed_id = str(new_rows[-1].get("id"))
                appended = self.logger_csv.log_closed_rows(new_rows)
                if appended:
                    logger.info("closed pnl appended {} rows", appended)
        except Exception as e:
            logger.debug("closed pnl poll failed: {}", e)


