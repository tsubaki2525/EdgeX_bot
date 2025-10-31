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
    # 設定ファイルは任意（無ければ空dict）
    try:
        with open("configs/edgex.yaml", "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
    except FileNotFoundError:
        cfg = {}

    # URLは未指定なら商用既定（変更不要なら設定しなくてOK）
    base_url = os.getenv("EDGEX_BASE_URL") or cfg.get("base_url") or "https://pro.edgex.exchange"
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
    # シンボル未指定ならBTC-PERPの既定ID（EdgeXの例: 10000001）
    symbol = contract_id_env or symbol_env or symbol_cfg or "10000001"

    parsed = urlparse(base_url or "")
    if not parsed.scheme or not parsed.netloc:
        raise SystemExit("EDGEX_BASE_URL が不正です（https://ホスト名 を設定してください）")
    if parsed.hostname and "example" in parsed.hostname:
        raise SystemExit("EDGEX_BASE_URL がプレースホルダです。実際のAPIベースURLに置き換えてください。")
    logger.info("edgex base_url={}, symbol_param={}, symbol={}", base_url, symbol_param, symbol)

    # スプレッドシート許可リストで口座縛り（任意機能）
    # 環境変数:
    #  - SHEET_CREDENTIALS_JSON: GoogleサービスアカウントJSONファイルのパス
    #  - SHEET_ALLOWED_SPREADSHEET_ID: スプレッドシートID
    #  - SHEET_ALLOWED_RANGE: 読み取りレンジ（既定: "A:A"、シート名付き可 例: "Allowlist!A:A"）
    try:
        creds_path = os.getenv("SHEET_CREDENTIALS_JSON")
        sheet_id = os.getenv("SHEET_ALLOWED_SPREADSHEET_ID")
        sheet_range = os.getenv("SHEET_ALLOWED_RANGE", "A:A")
        if creds_path and sheet_id:
            try:
                import gspread  # type: ignore
                gc = gspread.service_account(filename=creds_path)
                sh = gc.open_by_key(sheet_id)
                # 範囲にシート名が無ければ先頭シート名を付与
                range_name = sheet_range
                if "!" not in range_name:
                    range_name = f"{sh.sheet1.title}!{range_name}"
                resp = sh.values_get(range_name)
                ids = set()
                for row in resp.get("values", []) or []:
                    if not row:
                        continue
                    v = str(row[0]).strip()
                    if v:
                        ids.add(v)
                if not ids:
                    logger.warning("許可リストが空です。全拒否に該当します range={}", range_name)
                acct = str(api_id)
                if acct not in ids:
                    raise SystemExit(f"このアカウントは許可されていません（account_id={acct}）")
                logger.info("許可リスト認証OK: account_id={} (rows={})", acct, len(ids))
            except SystemExit:
                raise
            except Exception as e:
                # 設定されているが読めない場合は安全側で停止
                raise SystemExit(f"許可リストの読み取りに失敗しました: {e}")
        else:
            logger.debug("許可リスト連携は無効（環境未設定）")
    except SystemExit:
        raise
    except Exception:
        # 予期せぬ例外は続行させず安全側終了
        raise

    # ループ間隔は未指定なら2.5秒（稼働安定の既定値）
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


