import os
import asyncio
import yaml
from loguru import logger
from dotenv import load_dotenv
from urllib.parse import urlparse

from bot.adapters.edgex_sdk import EdgeXSDKAdapter
from bot.grid_engine import GridEngine


async def main() -> None:
    load_dotenv()
    # logs ディレクトリへファイル出力（全レベル）
    try:
        os.makedirs("logs", exist_ok=True)
        logger.add(
            os.path.join("logs", "run_edgex_grid.log"),
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
        # ファイル出力に失敗しても実行は継続（標準出力は残す）
        pass
    with open("configs/edgex.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    base_url = os.getenv("EDGEX_BASE_URL", cfg.get("base_url"))
    api_id = (
        os.getenv("EDGEX_ACCOUNT_ID")
        or os.getenv("EDGEX_API_ID")
        or cfg.get("account_id")
        or cfg.get("api_id")
    )
    sdk_key = os.getenv("EDGEX_STARK_PRIVATE_KEY") or os.getenv("EDGEX_L2_KEY")

    symbol_param = os.getenv("EDGEX_SYMBOL_PARAM", cfg.get("symbol_param", "contractId"))
    contract_id_env = os.getenv("EDGEX_CONTRACT_ID")
    symbol_env = os.getenv("EDGEX_SYMBOL")
    symbol_cfg = cfg.get("symbol") or cfg.get("contract_id")
    symbol = contract_id_env or symbol_env or symbol_cfg

    parsed = urlparse(base_url or "")
    if not parsed.scheme or not parsed.netloc:
        raise SystemExit("EDGEX_BASE_URL が不正です（https://ホスト名 を設定してください）")
    if parsed.hostname and "example" in parsed.hostname:
        raise SystemExit("EDGEX_BASE_URL がプレースホルダです。実際のAPIベースURLに置き換えてください。")
    logger.info("edgex base_url={}, symbol_param={}, symbol={}", base_url, symbol_param, symbol)

    poll_interval_raw = os.getenv("EDGEX_POLL_INTERVAL_SEC") or cfg.get("poll_interval_sec", 2.5)
    try:
        poll_interval = float(poll_interval_raw)
    except Exception:
        poll_interval = 2.5
    if poll_interval < 1.5:
        poll_interval = 1.5

    if not sdk_key:
        raise SystemExit("EDGEX_STARK_PRIVATE_KEY (or EDGEX_L2_KEY) が未設定です")
    if not api_id:
        raise SystemExit("EDGEX_ACCOUNT_ID が未設定です")
    adapter = EdgeXSDKAdapter(
        base_url=base_url,
        account_id=int(api_id),
        stark_private_key=sdk_key,
    )

    engine = GridEngine(
        adapter=adapter,
        symbol=symbol,
        poll_interval_sec=poll_interval,
    )

    await engine.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("stopped by user")


