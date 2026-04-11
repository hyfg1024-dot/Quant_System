"""CLI entry for HK paper trading."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from src.paper_trader import PaperTradeError, PaperTrader


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="港股多空策略模拟盘")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_start = sub.add_parser("start", help="从策略创建模拟盘")
    p_start.add_argument("--config", required=True, help="策略配置文件路径")
    p_start.add_argument("--universe", default="config/universe.yaml", help="universe 配置路径")
    p_start.add_argument("--run-id", default="", help="可选运行ID，不填则按策略名+日期生成")
    p_start.add_argument("--as-of", default="today", help="建仓日期（YYYY-MM-DD 或 today）")

    p_upd = sub.add_parser("update", help="更新模拟盘")
    p_upd.add_argument("--run-id", default="", help="指定 run_id；不填且 --all 时更新全部活跃盘")
    p_upd.add_argument("--all", action="store_true", help="更新全部活跃模拟盘")
    p_upd.add_argument("--as-of", default="today", help="更新到日期（YYYY-MM-DD 或 today）")

    p_status = sub.add_parser("status", help="查看模拟盘状态")
    p_status.add_argument("--run-id", default="", help="可选，指定 run_id")

    sub.add_parser("dashboard", help="重建 dashboard.html")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    base = Path(__file__).resolve().parent
    trader = PaperTrader(base_dir=base, logger=print)

    try:
        if args.cmd == "start":
            run_id = trader.start(
                config_path=Path(args.config),
                universe_path=Path(args.universe),
                run_id=args.run_id,
                as_of=args.as_of,
            )
            print(f"[OK] 模拟盘已创建/复用: {run_id}")
            print(f"[OK] Dashboard: {(base / 'paper_trades' / 'dashboard.html').resolve()}")
            return 0

        if args.cmd == "update":
            if args.all:
                rows = trader.update_all(as_of=args.as_of)
                print(f"[OK] 已更新 {len(rows)} 个运行")
                for x in rows:
                    rid = x.get("run_id", "")
                    if x.get("error"):
                        print(f"  [ERR] {rid}: {x.get('error')}")
                    else:
                        print(f"  [OK] {rid}: +{x.get('updated_days', 0)} 天, last={x.get('last_update_date', '-')}")
                print(f"[OK] Dashboard: {(base / 'paper_trades' / 'dashboard.html').resolve()}")
                return 0

            if not args.run_id:
                print("[ERROR] update 模式需要 --run-id 或 --all")
                return 2

            row = trader.update(run_id=args.run_id, as_of=args.as_of)
            print(f"[OK] {row.get('run_id')}: +{row.get('updated_days', 0)} 天, last={row.get('last_update_date', '-')}")
            if row.get("message"):
                print(f"[INFO] {row.get('message')}")
            print(f"[OK] Dashboard: {(base / 'paper_trades' / 'dashboard.html').resolve()}")
            return 0

        if args.cmd == "status":
            rows = trader.status(run_id=args.run_id)
            if not rows:
                print("[INFO] 暂无模拟盘")
                return 0
            for x in rows:
                eq = "-" if x.equity != x.equity else f"{x.equity:,.2f}"
                cr = "-" if x.cum_return != x.cum_return else f"{x.cum_return:.2%}"
                print(f"{x.run_id} | {x.strategy_name} | {x.last_update_date} | equity={eq} | ret={cr} | {x.status}")
            return 0

        if args.cmd == "dashboard":
            p = trader.build_dashboard()
            print(f"[OK] Dashboard: {p.resolve()}")
            return 0

        print("[ERROR] 未知命令")
        return 2

    except (PaperTradeError, ValueError) as exc:
        print(f"[ERROR] {exc}")
        return 2
    except Exception as exc:
        print(f"[ERROR] {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
