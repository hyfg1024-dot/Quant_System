#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
BACKTEST_DIR = ROOT_DIR / "apps" / "backtest"
if str(BACKTEST_DIR) not in sys.path:
    sys.path.insert(0, str(BACKTEST_DIR))

from src.paper_trader import PaperTrader


def _log(message: str) -> None:
    print(f"[PAPER-DAILY] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {message}", flush=True)


def run_once(as_of: str = "today") -> int:
    trader = PaperTrader(base_dir=BACKTEST_DIR, logger=_log)
    active = trader._load_active()
    active_count = sum(1 for row in active if str(row.get("status", "active")).strip().lower() == "active")
    if active_count <= 0:
        _log("无 active 模拟盘，本次跳过。")
        trader.build_dashboard()
        return 0

    _log(f"开始更新 active 模拟盘，共 {active_count} 个，目标日期 {as_of}")
    rows = trader.update_all(as_of=as_of)
    ok_count = sum(1 for row in rows if not row.get("error"))
    err_count = sum(1 for row in rows if row.get("error"))
    for row in rows:
        rid = str(row.get("run_id", "")).strip()
        if row.get("error"):
            _log(f"ERR {rid}: {row.get('error')}")
        else:
            _log(f"OK {rid}: +{int(row.get('updated_days', 0) or 0)} 天, last={row.get('last_update_date', '-')}")
    _log(f"更新完成：成功 {ok_count}，失败 {err_count}")
    return 0 if err_count == 0 else 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="每日收盘后自动更新 active 模拟盘")
    parser.add_argument("--once", action="store_true", help="执行一次后退出")
    parser.add_argument("--as-of", default="today", help="更新到日期（YYYY-MM-DD 或 today）")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return run_once(as_of=args.as_of)


if __name__ == "__main__":
    raise SystemExit(main())
