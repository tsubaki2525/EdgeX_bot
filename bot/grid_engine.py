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

        # 既存の“このBotが出していない注文”を徐々に整理して、levels本に保つ
        try:
            self.enforce_levels = str(os.getenv("EDGEX_GRID_ENFORCE_LEVELS", "1")).lower() in ("1", "true", "yes")
        except Exception:
            self.enforce_levels = True

        # 1ループあたりの新規発注上限（片側）: 明示指定があれば適用（任意）
        try:
            self.max_new_per_loop = int(os.getenv("EDGEX_GRID_MAX_NEW_PER_LOOP", "0"))
        except Exception:
            self.max_new_per_loop = 0

        # 価格追従（乖離補正）設定
        try:
            self.follow_enable = str(os.getenv("EDGEX_GRID_FOLLOW_ENABLE", "1")).lower() in ("1", "true", "yes")
        except Exception:
            self.follow_enable = True
        try:
            # X からの許容バンドを N ステップ分だけ広げる（例: 1 -> X+1*N までは許容）
            self.follow_slack_steps = int(os.getenv("EDGEX_GRID_FOLLOW_SLACK_STEPS", "1"))
        except Exception:
            self.follow_slack_steps = 1
        try:
            # 1ループで寄せる最大本数（過度な再配置を抑制）
            self.max_shift_per_loop = int(os.getenv("EDGEX_GRID_MAX_SHIFT_PER_LOOP", "1"))
        except Exception:
            self.max_shift_per_loop = 1

    def _has_min_gap(self, side_map: Dict[float, str], px: float) -> bool:
        """Return True if `px` is at least `self.step` away from all existing prices in `side_map`."""
        for existing_price in side_map.keys():
            if abs(existing_price - px) < self.step - 1e-9:
                return False
        return True

    async def run(self) -> None:
        await self.adapter.connect()
        self._running = True
        logger.info(
            "グリッドエンジン起動: グリッド幅={}USD レベル数={} サイズ={}BTC",
            self.step,
            self.levels,
            self.size,
        )
        logger.debug(
            "grid boot env: step(N)={} offset(X)={} levels={} max_new_per_loop={} enforce_levels={} size={}",
            self.step,
            self.first_offset,
            self.levels,
            self.max_new_per_loop,
            getattr(self, "enforce_levels", True),
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

                    logger.debug(
                        "loop ctx: P={} X={} N={} levels={} placed_buy={} placed_sell={}",
                        mid_price,
                        self.first_offset,
                        self.step,
                        self.levels,
                        sorted(self.placed_buy_px_to_id.keys()),
                        sorted(self.placed_sell_px_to_id.keys()),
                    )

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

        # 初期配置後:
        # - 片側が全滅していたら、その片側だけ現在価格Pから再配置（挟み込みを回復）
        # - 両側に1本以上あれば、ここでは新規発注しない（補充は約定側で行う）
        if self.initialized:
            need_buy_seed = len(self.placed_buy_px_to_id) == 0
            need_sell_seed = len(self.placed_sell_px_to_id) == 0
            # 片側が空なら再シード（初期の挟み込みを回復）
            if need_buy_seed or need_sell_seed:
                buy_targets = [float(mid_price) - (self.first_offset + i * self.step) for i in range(self.levels)]
                sell_targets = [float(mid_price) + (self.first_offset + i * self.step) for i in range(self.levels)]
                logger.info("再配置: need_buy={} need_sell={} P={} X={} N={}", need_buy_seed, need_sell_seed, mid_price, self.first_offset, self.step)
                # BUY再種まき
                if need_buy_seed:
                    new_buys = 0
                    for px in buy_targets:
                        if px <= 0:
                            continue
                        if px >= (mid_price - 1e-9):
                            continue
                        if px in self.placed_buy_px_to_id:
                            continue
                        if not self._has_min_gap(self.placed_buy_px_to_id, px):
                            continue
                        await self._place_order(OrderSide.BUY, px)
                        new_buys += 1
                        await asyncio.sleep(self.op_spacing_sec)
                        if new_buys >= self.levels:
                            break
                # SELL再種まき
                if need_sell_seed:
                    new_sells = 0
                    for px in sell_targets:
                        if px <= (mid_price + 1e-9):
                            continue
                        if px in self.placed_sell_px_to_id:
                            continue
                        if not self._has_min_gap(self.placed_sell_px_to_id, px):
                            continue
                        await self._place_order(OrderSide.SELL, px)
                        new_sells += 1
                        await asyncio.sleep(self.op_spacing_sec)
                        if new_sells >= self.levels:
                            break
                return

            # 両サイドに1本以上ある場合: 追従（価格乖離の自動シフト）
            if self.follow_enable and self.step > 0:

                # BUY側: 近い買いが P-(X+slack*N) より遠くにあるなら、遠い買いを1本消して内側へ1ステップ寄せる
                try:
                    shifts = 0
                    if self.placed_buy_px_to_id:
                        nearest_buy = max(self.placed_buy_px_to_id.keys())  # 市場に最も近い買い
                        desired_min_buy = float(mid_price) - (self.first_offset + self.follow_slack_steps * self.step)
                        while nearest_buy < desired_min_buy - 1e-9 and shifts < self.max_shift_per_loop:
                            if len(self.placed_buy_px_to_id) <= 0:
                                break
                            far_buy_px = min(self.placed_buy_px_to_id.keys())
                            far_buy_id = self.placed_buy_px_to_id.pop(far_buy_px)
                            try:
                                await self.adapter.cancel_order(far_buy_id)
                                logger.info("追従: 遠いBUYキャンセル px={}", far_buy_px)
                            except Exception:
                                logger.debug("追従: 遠いBUYキャンセル失敗(無視) id={} px={}", far_buy_id, far_buy_px)
                            await asyncio.sleep(self.op_spacing_sec)

                            new_buy_px = nearest_buy + self.step
                            # 安全: 現在価格の内側には置かない
                            if new_buy_px >= (mid_price - 1e-9):
                                break
                            if new_buy_px in self.placed_buy_px_to_id:
                                nearest_buy = new_buy_px
                                shifts += 1
                                continue
                            if not self._has_min_gap(self.placed_buy_px_to_id, new_buy_px):
                                logger.debug("追従: BUY gap違反でスキップ new_px={}", new_buy_px)
                                break
                            await self._place_order(OrderSide.BUY, new_buy_px)
                            nearest_buy = new_buy_px
                            shifts += 1
                            await asyncio.sleep(self.op_spacing_sec)
                        if shifts:
                            logger.debug("追従BUY: nearest={} desired_min={} shifts={}", nearest_buy, desired_min_buy, shifts)
                except Exception as e:
                    logger.debug("追従BUY処理スキップ: {}", e)

                # SELL側: 近い売りが P+(X+slack*N) より遠くにあるなら、遠い売りを1本消して内側へ1ステップ寄せる
                try:
                    shifts = 0
                    if self.placed_sell_px_to_id:
                        nearest_sell = min(self.placed_sell_px_to_id.keys())  # 市場に最も近い売り
                        desired_max_sell = float(mid_price) + (self.first_offset + self.follow_slack_steps * self.step)
                        while nearest_sell > desired_max_sell + 1e-9 and shifts < self.max_shift_per_loop:
                            if len(self.placed_sell_px_to_id) <= 0:
                                break
                            far_sell_px = max(self.placed_sell_px_to_id.keys())
                            far_sell_id = self.placed_sell_px_to_id.pop(far_sell_px)
                            try:
                                await self.adapter.cancel_order(far_sell_id)
                                logger.info("追従: 遠いSELLキャンセル px={}", far_sell_px)
                            except Exception:
                                logger.debug("追従: 遠いSELLキャンセル失敗(無視) id={} px={}", far_sell_id, far_sell_px)
                            await asyncio.sleep(self.op_spacing_sec)

                            new_sell_px = nearest_sell - self.step
                            # 安全: 現在価格の内側には置かない
                            if new_sell_px <= (mid_price + 1e-9):
                                break
                            if new_sell_px in self.placed_sell_px_to_id:
                                nearest_sell = new_sell_px
                                shifts += 1
                                continue
                            if not self._has_min_gap(self.placed_sell_px_to_id, new_sell_px):
                                logger.debug("追従: SELL gap違反でスキップ new_px={}", new_sell_px)
                                break
                            await self._place_order(OrderSide.SELL, new_sell_px)
                            nearest_sell = new_sell_px
                            shifts += 1
                            await asyncio.sleep(self.op_spacing_sec)
                        if shifts:
                            logger.debug("追従SELL: nearest={} desired_max={} shifts={}", nearest_sell, desired_max_sell, shifts)
                except Exception as e:
                    logger.debug("追従SELL処理スキップ: {}", e)
                # フォロー後に本数不足があれば外側に補充（levels維持）
                try:
                    # 片側あたりの新規上限を考慮
                    add_buys = 0
                    add_sells = 0
                    # BUY不足: 最外側(min)から外側へ足す
                    while len(self.placed_buy_px_to_id) < self.levels:
                        if not self.placed_buy_px_to_id:
                            break
                        next_buy = min(self.placed_buy_px_to_id.keys()) - self.step
                        if next_buy <= (mid_price - 1e-9) and self._has_min_gap(self.placed_buy_px_to_id, next_buy):
                            if self.max_new_per_loop and add_buys >= self.max_new_per_loop:
                                break
                            await self._place_order(OrderSide.BUY, next_buy)
                            add_buys += 1
                            await asyncio.sleep(self.op_spacing_sec)
                        else:
                            break
                    # SELL不足: 最外側(max)から外側へ足す
                    while len(self.placed_sell_px_to_id) < self.levels:
                        if not self.placed_sell_px_to_id:
                            break
                        next_sell = max(self.placed_sell_px_to_id.keys()) + self.step
                        if next_sell >= (mid_price + 1e-9) and self._has_min_gap(self.placed_sell_px_to_id, next_sell):
                            if self.max_new_per_loop and add_sells >= self.max_new_per_loop:
                                break
                            await self._place_order(OrderSide.SELL, next_sell)
                            add_sells += 1
                            await asyncio.sleep(self.op_spacing_sec)
                        else:
                            break
                    if add_buys or add_sells:
                        logger.debug("levels補充: add_buys={} add_sells={} now buy={} sell={}", add_buys, add_sells, len(self.placed_buy_px_to_id), len(self.placed_sell_px_to_id))
                except Exception as e:
                    logger.debug("levels補充スキップ: {}", e)
                return
            buy_targets = [float(mid_price) - (self.first_offset + i * self.step) for i in range(self.levels)]
            sell_targets = [float(mid_price) + (self.first_offset + i * self.step) for i in range(self.levels)]
            logger.info("再配置: need_buy={} need_sell={} P={} X={} N={}", need_buy_seed, need_sell_seed, mid_price, self.first_offset, self.step)
            # BUY再種まき
            if need_buy_seed:
                new_buys = 0
                for px in buy_targets:
                    if px <= 0:
                        continue
                    if px >= (mid_price - 1e-9):
                        continue
                    if px in self.placed_buy_px_to_id:
                        continue
                    if not _has_min_gap(self.placed_buy_px_to_id, px):
                        continue
                    await self._place_order(OrderSide.BUY, px)
                    new_buys += 1
                    await asyncio.sleep(self.op_spacing_sec)
                    if new_buys >= self.levels:
                        break
            # SELL再種まき
            if need_sell_seed:
                new_sells = 0
                for px in sell_targets:
                    if px <= (mid_price + 1e-9):
                        continue
                    if px in self.placed_sell_px_to_id:
                        continue
                    if not _has_min_gap(self.placed_sell_px_to_id, px):
                        continue
                    await self._place_order(OrderSide.SELL, px)
                    new_sells += 1
                    await asyncio.sleep(self.op_spacing_sec)
                    if new_sells >= self.levels:
                        break
            return

        # 候補を作る
        buy_targets = [float(mid_price) - (self.first_offset + i * self.step) for i in range(self.levels)]
        sell_targets = [float(mid_price) + (self.first_offset + i * self.step) for i in range(self.levels)]
        logger.debug("ensure(init): P={} X={} N={} buy_targets={} sell_targets={}", mid_price, self.first_offset, self.step, buy_targets, sell_targets)

        # 以降はターゲットに合わせて一斉キャンセルは行わない（アンカー方式）

        # 片側あたり新規上限が設定されていれば適用
        new_buys = 0
        new_sells = 0

        # 買い配置（P−X より内側は生成しない設計だが、念のためチェック）
        for px in buy_targets:
            if px <= 0:
                continue
            if px >= (mid_price - 1e-9):
                logger.debug("skip(init BUY): inside X (px={} >= P)", px)
                continue
            if px in self.placed_buy_px_to_id:
                logger.debug("skip(init BUY): already placed px={}", px)
                continue
            if not self._has_min_gap(self.placed_buy_px_to_id, px):
                logger.debug("skip(init BUY): gap < N at px={}", px)
                continue
            if self.max_new_per_loop and new_buys >= self.max_new_per_loop:
                break
            await self._place_order(OrderSide.BUY, px)
            new_buys += 1
            await asyncio.sleep(self.op_spacing_sec)

        # 売り配置（P＋X より内側は生成しない設計だが、念のためチェック）
        for px in sell_targets:
            if px in self.placed_sell_px_to_id:
                logger.debug("skip(init SELL): already placed px={}", px)
                continue
            if not self._has_min_gap(self.placed_sell_px_to_id, px):
                logger.debug("skip(init SELL): gap < N at px={}", px)
                continue
            if px <= (mid_price + 1e-9):
                logger.debug("skip(init SELL): inside X (px={} <= P)", px)
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
            # 同サイドの取引所OPENともN間隔を確認（丸め差異を吸収）
            try:
                active = await self.adapter.list_active_orders(self.symbol)
            except Exception:
                active = []
            # 候補と既存価格の距離がN未満ならスキップ
            def _extract_px(row: dict) -> float | None:
                try:
                    raw = row.get("price") or row.get("px") or row.get("0")
                    return float(raw) if raw is not None else None
                except Exception:
                    return None
            for row in (active or []):
                if not isinstance(row, dict):
                    continue
                # サイド判定（無ければスキップ）
                s = str(row.get("side") or row.get("orderSide") or "").upper()
                if (side == OrderSide.BUY and s not in ("BUY", "LONG")) or (side == OrderSide.SELL and s not in ("SELL", "SHORT")):
                    continue
                apx = _extract_px(row)
                if apx is None:
                    continue
                if abs(apx - price) < (self.step - 1e-9):
                    logger.debug("N間隔未満のためスキップ: side={} cand={} exist={}", side, price, apx)
                    return

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
                logger.debug("replenish BUY: near_sell_base={} -> new_near_sell={} outer_buy_base(current)={}", base_near_sell, new_near_sell, min(self.placed_buy_px_to_id.keys()) if self.placed_buy_px_to_id else None)
                if new_near_sell not in self.placed_sell_px_to_id and new_near_sell > 0:
                    await self._place_order(OrderSide.SELL, new_near_sell)
                    await asyncio.sleep(self.op_spacing_sec)
                # BUYを一番外側に追加
                base_outer_buy = min(self.placed_buy_px_to_id.keys()) if self.placed_buy_px_to_id else (min(filled_buy_prices) - self.step)
                new_outer_buy = base_outer_buy - self.step
                logger.debug("replenish BUY: base_outer_buy={} -> new_outer_buy={}", base_outer_buy, new_outer_buy)
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
                logger.debug("replenish SELL: near_buy_base={} -> new_near_buy={} outer_sell_base(current)={}", base_near_buy, new_near_buy, max(self.placed_sell_px_to_id.keys()) if self.placed_sell_px_to_id else None)
                if new_near_buy not in self.placed_buy_px_to_id and new_near_buy > 0:
                    await self._place_order(OrderSide.BUY, new_near_buy)
                    await asyncio.sleep(self.op_spacing_sec)
                # SELLを一番外側に追加
                base_outer_sell = max(self.placed_sell_px_to_id.keys()) if self.placed_sell_px_to_id else (max(filled_sell_prices) + self.step)
                new_outer_sell = base_outer_sell + self.step
                logger.debug("replenish SELL: base_outer_sell={} -> new_outer_sell={}", base_outer_sell, new_outer_sell)
                if new_outer_sell not in self.placed_sell_px_to_id:
                    await self._place_order(OrderSide.SELL, new_outer_sell)
                    await asyncio.sleep(self.op_spacing_sec)
        
        except Exception as e:
            logger.error("約定確認エラー: {}", e)
            return

        # 余剰オーダーの整理（このBotが出していないOPEN注文を徐々に解消）
        if self.enforce_levels:
            try:
                placed_ids = set(self.placed_buy_px_to_id.values()) | set(self.placed_sell_px_to_id.values())
                # 抽出関数
                def _oid(row: dict) -> str:
                    return str(row.get("orderId") or row.get("id") or row.get("order_id") or "")
                # 未管理のOPEN注文
                unknown = []
                for row in (active_orders or []):
                    if not isinstance(row, dict):
                        continue
                    oid = _oid(row)
                    if not oid or oid in placed_ids:
                        continue
                    status = str(row.get("status") or "").upper()
                    if status and status != "OPEN":
                        continue
                    unknown.append(oid)
                # 1ループで最大3件だけキャンセルし、徐々に整理
                for oid in unknown[:3]:
                    try:
                        await self.adapter.cancel_order(oid)
                        logger.info("余剰注文をキャンセル: id={}", oid)
                    except Exception:
                        logger.debug("余剰注文キャンセル失敗(無視): id={}", oid)
                    await asyncio.sleep(self.op_spacing_sec)
            except Exception as e:
                logger.debug("余剰整理スキップ: {}", e)

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
