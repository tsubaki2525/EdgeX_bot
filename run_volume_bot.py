"""
Volume Trading Bot
取引量を稼ぐためのボット起動スクリプト
"""

import asyncio
import os
from decimal import Decimal
from loguru import logger
from dotenv import load_dotenv

from bot.adapters.edgex_sdk import EdgeXSDKAdapter
from bot.volume_engine import VolumeEngine


async def main():
    load_dotenv()
    
    # ログ設定
    try:
        os.makedirs("logs", exist_ok=True)
        logger.add(
            os.path.join("logs", "run_volume_bot.log"),
            level="DEBUG",
            rotation="10 MB",
            retention="14 days",
            encoding="utf-8",
            enqueue=True,
            backtrace=False,
            diagnose=False,
            format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<8} | {name}:{function}:{line} - {message}",
        )
    except Exception:
        pass
    
    # 環境変数から設定を読み込む
    base_url = os.getenv("EDGEX_BASE_URL", "https://api.edgex.exchange")
    account_id = os.getenv("EDGEX_ACCOUNT_ID")
    l2_private_key = os.getenv("EDGEX_L2_PRIVATE_KEY") or os.getenv("EDGEX_STARK_PRIVATE_KEY")
    
    if not account_id:
        raise ValueError("EDGEX_ACCOUNT_ID is not set")
    if not l2_private_key:
        raise ValueError("EDGEX_L2_PRIVATE_KEY (or EDGEX_STARK_PRIVATE_KEY) is not set")
    
    # ボット設定
    symbol = os.getenv("EDGEX_VOLUME_SYMBOL", "BTC-USD-PERP")
    size = Decimal(os.getenv("EDGEX_VOLUME_SIZE", "0.01"))
    entry_offset = Decimal(os.getenv("EDGEX_VOLUME_ENTRY_OFFSET_USD", "10"))
    exit_offset = Decimal(os.getenv("EDGEX_VOLUME_EXIT_OFFSET_USD", "10"))
    hold_time = int(os.getenv("EDGEX_VOLUME_HOLD_TIME_SECONDS", "120"))  # 2分
    reorder_interval = int(os.getenv("EDGEX_VOLUME_REORDER_INTERVAL_SECONDS", "60"))  # 1分
    
    logger.info("=== Volume Trading Bot ===")
    logger.info("base_url: {}", base_url)
    logger.info("account_id: {}", account_id)
    logger.info("symbol: {}", symbol)
    logger.info("size: {}", size)
    logger.info("entry_offset: {} USD", entry_offset)
    logger.info("exit_offset: {} USD", exit_offset)
    logger.info("hold_time: {} seconds", hold_time)
    logger.info("reorder_interval: {} seconds", reorder_interval)
    
    # アダプター初期化（グリッドボットと同じ方法）
    adapter = EdgeXSDKAdapter(
        base_url=base_url,
        account_id=int(account_id),
        stark_private_key=l2_private_key,
    )
    
    # エンジン初期化
    engine = VolumeEngine(
        adapter=adapter,
        symbol=symbol,
        size=size,
        entry_offset_usd=entry_offset,
        exit_offset_usd=exit_offset,
        hold_time_seconds=hold_time,
        reorder_interval_seconds=reorder_interval,
    )
    
    # ボット開始
    await engine.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("stopped by user")
