#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import Iterable


ROOT_DIR = Path(__file__).resolve().parents[1]
FILTER_DIR = ROOT_DIR / "apps" / "filter"
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
if str(FILTER_DIR) not in sys.path:
    sys.path.insert(0, str(FILTER_DIR))

from filter_engine import refresh_market_snapshot  # noqa: E402


def _log(message: str) -> None:
    print(f"[FOUNDATION-WEEKLY] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {message}", flush=True)


def run_once(scopes: Iterable[str] = ("A", "HK")) -> int:
    err_count = 0
    for scope in scopes:
        label = "A股" if scope == "A" else "港股"
        _log(f"开始{label}基础快照周更")
        try:
            stats = refresh_market_snapshot(
                max_stocks=0,
                enrich_top_n=0,
                force_refresh=False,
                rotate_enrich=False,
                market_scope=scope,
                enrich_segment="sz_main" if scope == "A" else "energy",
                weekly_mode=True,
                safe_mode=True,
                only_missing_enrich=False,
            )
        except Exception as exc:
            err_count += 1
            _log(f"ERR {label}: {exc}")
            continue
        if stats.get("skipped"):
            _log(f"SKIP {label}: {stats.get('reason', '-')}")
        else:
            row_count = int(stats.get("row_count", 0) or 0)
            source = stats.get("source_summary") or stats.get("source") or "-"
            _log(f"OK {label}: row_count={row_count}, source={source}")
    _log(f"基础库周更完成：失败 {err_count}")
    return 0 if err_count == 0 else 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="每周更新基础市场快照")
    parser.add_argument("--once", action="store_true", help="执行一次后退出")
    parser.add_argument("--scope", choices=["A", "HK", "AH"], default="AH", help="更新范围")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    scopes = ("A", "HK") if args.scope == "AH" else (args.scope,)
    return run_once(scopes=scopes)


if __name__ == "__main__":
    raise SystemExit(main())
