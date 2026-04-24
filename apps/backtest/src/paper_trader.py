"""Lightweight paper-trading engine for HK long-short strategies.

Design constraints:
- Reuse existing config/data modules without changing them.
- Persist with JSON/CSV only.
- Idempotent daily updates (dedupe by date).
- HK trading-day judgement uses yfinance-returned index dates.
- Apply HK board-lot rounding for every order.
"""

from __future__ import annotations

import json
import math
import re
import shutil
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import numpy as np
import pandas as pd

try:
    import yaml
except Exception:  # pragma: no cover
    yaml = None

from .config_loader import StrategyConfig, UniverseConfig, load_strategy, load_universe
from .data_manager import DataManager


class PaperTradeError(RuntimeError):
    """Raised when paper-trade operation fails."""


DEFAULT_BOARD_LOTS: Dict[str, int] = {
    # real estate sample
    "1109.HK": 2000,
    "0688.HK": 1000,
    "0960.HK": 1000,
    "2007.HK": 1000,
    "1918.HK": 1000,
    "1908.HK": 1000,
    "3383.HK": 1000,
    "3377.HK": 1000,
    "3883.HK": 1000,
    # defensive/tech sample
    "0883.HK": 1000,
    "1088.HK": 500,
    "0941.HK": 500,
    "0939.HK": 1000,
    "9866.HK": 100,
    "9868.HK": 100,
    "0241.HK": 1000,
}


@dataclass
class PaperRunSummary:
    """Short status for one paper run."""

    run_id: str
    strategy_name: str
    created_at: str
    last_update_date: str
    equity: float
    cum_return: float
    status: str
    run_dir: str


class PaperTrader:
    """Paper-trading manager with JSON/CSV persistence."""

    def __init__(self, base_dir: Path, logger=print) -> None:
        self.base_dir = Path(base_dir)
        self.logger = logger

        self.data_dir = self.base_dir / "data"
        self.paper_dir = self.base_dir / "paper_trades"
        self.paper_trash_dir = self.paper_dir / "_trash"
        self.active_path = self.paper_dir / ".active"
        self.dashboard_path = self.paper_dir / "dashboard.html"

        self.dm = DataManager(data_dir=self.data_dir, logger=self.logger)

        self.paper_dir.mkdir(parents=True, exist_ok=True)
        self.paper_trash_dir.mkdir(parents=True, exist_ok=True)
        self._ensure_active_registry()
        self._ensure_dashboard_stub()

    # -------- public APIs --------

    def start(
        self,
        config_path: Path,
        universe_path: Path,
        run_id: str = "",
        as_of: str = "today",
    ) -> str:
        """Create one paper run from strategy config and open initial position."""
        cfg_path = self._resolve_path(config_path)
        uni_path = self._resolve_path(universe_path)
        cfg, universe, board_lot_overrides = self._load_strategy_with_overrides(cfg_path, uni_path)

        as_of_date = self._parse_day(as_of)
        init_date = self._latest_hk_trading_day(as_of_date)
        if init_date is None:
            raise PaperTradeError(f"{as_of_date} 附近未识别到港股交易日，无法建仓")

        run_key = self._decide_run_id(cfg.strategy_name, run_id=run_id, as_of=as_of_date)
        run_dir = self.paper_dir / run_key
        state_path = run_dir / "state.json"

        if state_path.exists():
            self.logger(f"[PAPER] 已存在运行目录，直接复用: {run_key}")
            self._upsert_active(
                {
                    "run_id": run_key,
                    "strategy_name": cfg.strategy_name,
                    "created_at": self._now_iso(),
                    "last_update_date": self._safe_read_state_last_date(state_path),
                    "status": "active",
                    "run_dir": str(run_dir),
                }
            )
            self.build_dashboard()
            return run_key

        run_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(cfg_path, run_dir / "config.yaml")

        codes = self._strategy_codes(cfg)
        calendar = pd.DatetimeIndex([pd.Timestamp(init_date)])
        aligned = self._load_aligned_prices(codes=codes, start=init_date - timedelta(days=180), end=init_date, calendar=calendar)

        weights_long, weights_short = self._resolve_weights(cfg=cfg, aligned=aligned)

        board_lots = {c: int(board_lot_overrides.get(c, DEFAULT_BOARD_LOTS.get(c, 1000))) for c in codes}
        shares = {c: 0.0 for c in codes}
        entry_price = {c: float("nan") for c in codes}
        short_enabled = {c: True for c in weights_short.keys()}

        dt = calendar[0]
        px = {c: self._safe_float(aligned[c].at[dt, "adj_close"]) for c in codes}
        tradable = {c: bool(aligned[c].at[dt, "tradable"]) and not bool(aligned[c].at[dt, "suspended"]) for c in codes}

        cash = float(cfg.capital.total_hkd)
        trades: List[Dict[str, Any]] = []

        cash, fee_long, fee_short = self._rebalance_to_target(
            dt=dt,
            cfg=cfg,
            shares=shares,
            cash=cash,
            px_exec=px,
            tradable=tradable,
            long_weights=weights_long,
            short_weights=weights_short,
            short_enabled=short_enabled,
            board_lots=board_lots,
            entry_price=entry_price,
            trades=trades,
            reason="init",
        )

        # build entry price for all opened names
        for c in codes:
            if shares.get(c, 0.0) != 0.0 and np.isfinite(px.get(c, np.nan)):
                entry_price[c] = float(px[c])

        equity = self._equity(cash=cash, shares=shares, prices=px)
        long_mv, short_mv = self._long_short_mv(shares=shares, prices=px)

        state = {
            "run_id": run_key,
            "strategy_name": cfg.strategy_name,
            "config_path": str(cfg_path),
            "universe_path": str(uni_path),
            "created_at": self._now_iso(),
            "last_update_date": str(init_date),
            "initial_capital_hkd": float(cfg.capital.total_hkd),
            "cash": float(cash),
            "shares": {k: float(v) for k, v in shares.items()},
            "entry_price": self._json_float_map(entry_price),
            "prev_price": self._json_float_map(px),
            "board_lots": {k: int(max(1, int(v))) for k, v in board_lots.items()},
            "short_enabled": {k: bool(v) for k, v in short_enabled.items()},
            "costs": {
                "trade_fee_total": float(fee_long + fee_short),
                "borrow_fee_total": 0.0,
            },
            "flags": {
                "terminated": False,
                "long_stop_triggered": [],
                "short_stop_triggered": [],
            },
        }

        self._write_json(state_path, state)
        self._write_json(run_dir / "trades.json", trades)

        snap_row = {
            "date": str(init_date),
            "equity": equity,
            "cash": float(cash),
            "long_mv": long_mv,
            "short_mv": short_mv,
            "gross_exposure": long_mv + short_mv,
            "net_exposure": long_mv - short_mv,
            "day_pnl": 0.0,
            "trade_fee": float(fee_long + fee_short),
            "borrow_fee": 0.0,
            "cum_return": (equity / float(cfg.capital.total_hkd) - 1.0) if cfg.capital.total_hkd else np.nan,
        }
        self._append_snapshots(run_dir / "snapshots.csv", [snap_row])

        self._upsert_active(
            {
                "run_id": run_key,
                "strategy_name": cfg.strategy_name,
                "created_at": state["created_at"],
                "last_update_date": str(init_date),
                "status": "active",
                "run_dir": str(run_dir),
            }
        )
        self.build_dashboard()
        self.logger(f"[PAPER] 初始化完成: {run_key} @ {init_date}")
        return run_key

    def update(self, run_id: str, as_of: str = "today") -> Dict[str, Any]:
        """Incrementally update one paper run to as_of (idempotent by date)."""
        run_dir = self.paper_dir / run_id
        state_path = run_dir / "state.json"
        if not state_path.exists():
            raise PaperTradeError(f"找不到模拟盘: {run_id}")

        state = self._read_json(state_path)
        cfg_path = self._resolve_path(state.get("config_path", run_dir / "config.yaml"))
        uni_path = self._resolve_path(state.get("universe_path", self.base_dir / "config" / "universe.yaml"))
        cfg, _universe, board_lot_overrides = self._load_strategy_with_overrides(cfg_path, uni_path)

        as_of_date = self._parse_day(as_of)
        last_update_date = self._parse_day(state.get("last_update_date", "today"))
        if as_of_date <= last_update_date:
            return {
                "run_id": run_id,
                "updated_days": 0,
                "last_update_date": str(last_update_date),
                "message": f"无需更新：last_update_date={last_update_date}",
            }

        trading_days = self._hk_trading_days(start=last_update_date + timedelta(days=1), end=as_of_date)
        if trading_days.empty:
            return {
                "run_id": run_id,
                "updated_days": 0,
                "last_update_date": str(last_update_date),
                "message": f"{last_update_date + timedelta(days=1)} -> {as_of_date} 无港股交易日",
            }

        # Idempotence guard from snapshots.csv
        snapshots_path = run_dir / "snapshots.csv"
        existing = self._load_snapshots(snapshots_path)
        existing_days = set(existing["date"].astype(str).tolist()) if not existing.empty else set()

        trading_days = pd.DatetimeIndex([d for d in trading_days if str(d.date()) not in existing_days])
        if trading_days.empty:
            return {
                "run_id": run_id,
                "updated_days": 0,
                "last_update_date": str(last_update_date),
                "message": "本次交易日都已存在快照，跳过（幂等）",
            }

        codes = self._strategy_codes(cfg)
        start_fetch = min(last_update_date - timedelta(days=200), self._parse_day(str(cfg.backtest.start_date)))
        aligned = self._load_aligned_prices(codes=codes, start=start_fetch, end=as_of_date, calendar=trading_days)
        weights_long, weights_short = self._resolve_weights(cfg=cfg, aligned=aligned)
        rebalance_dates = self._get_rebalance_dates(trading_days, cfg.rebalance.frequency, cfg.rebalance.day)

        shares = {k: float(v) for k, v in dict(state.get("shares", {})).items()}
        for c in codes:
            shares.setdefault(c, 0.0)
        entry_price = self._parse_float_map(state.get("entry_price", {}), keys=codes)
        prev_price = self._parse_float_map(state.get("prev_price", {}), keys=codes)
        short_enabled_raw = state.get("short_enabled", {})
        short_enabled = {c: bool(short_enabled_raw.get(c, True)) for c in weights_short.keys()}

        board_lots_state = state.get("board_lots", {})
        board_lots = {c: int(board_lots_state.get(c, board_lot_overrides.get(c, DEFAULT_BOARD_LOTS.get(c, 1000)))) for c in codes}
        board_lots = {c: max(1, int(v)) for c, v in board_lots.items()}

        costs = state.get("costs", {})
        trade_fee_total = float(costs.get("trade_fee_total", 0.0))
        borrow_fee_total = float(costs.get("borrow_fee_total", 0.0))

        flags = state.get("flags", {})
        terminated = bool(flags.get("terminated", False))
        long_stop_triggered: Set[str] = set([str(x) for x in flags.get("long_stop_triggered", [])])
        short_stop_triggered: Set[str] = set([str(x) for x in flags.get("short_stop_triggered", [])])

        cash = float(state.get("cash", 0.0))

        trades = self._read_json(run_dir / "trades.json")
        if not isinstance(trades, list):
            trades = []

        new_rows: List[Dict[str, Any]] = []

        long_codes = set([p.code for p in cfg.long_positions])
        short_codes = set([p.code for p in cfg.short_positions])

        for dt in trading_days:
            px = {c: self._safe_float(aligned[c].at[dt, "adj_close"]) for c in codes}
            tradable = {c: bool(aligned[c].at[dt, "tradable"]) and not bool(aligned[c].at[dt, "suspended"]) for c in codes}

            # mark-to-market day pnl (cash unchanged)
            day_pnl = 0.0
            for c in codes:
                p0 = prev_price.get(c, np.nan)
                p1 = px.get(c, np.nan)
                if np.isfinite(p0) and np.isfinite(p1):
                    day_pnl += float(shares.get(c, 0.0)) * (p1 - p0)

            # borrow fee accrual
            short_notional = sum(abs(float(shares.get(c, 0.0))) * px.get(c, np.nan) for c in short_codes if float(shares.get(c, 0.0)) < 0 and np.isfinite(px.get(c, np.nan)))
            borrow_fee = short_notional * float(cfg.costs.short_borrow_rate) / 252.0
            cash -= borrow_fee
            borrow_fee_total += borrow_fee

            trade_fee_day = 0.0

            # stop-loss controls
            if not terminated:
                # single long stop
                for c in sorted(long_codes):
                    if c in long_stop_triggered:
                        continue
                    sh = float(shares.get(c, 0.0))
                    if sh <= 0:
                        continue
                    ep = entry_price.get(c, np.nan)
                    cp = px.get(c, np.nan)
                    if not np.isfinite(ep) or not np.isfinite(cp):
                        continue
                    ret = cp / ep - 1.0
                    if ret <= float(cfg.stop_loss.single_long_stop) and tradable.get(c, False):
                        action = str(cfg.stop_loss.single_long_action).lower()
                        tgt = sh * 0.5 if action == "halve" else 0.0
                        cash, fee = self._trade_to_target(
                            dt=dt,
                            code=c,
                            target_shares=tgt,
                            shares=shares,
                            cash=cash,
                            px_exec=px,
                            board_lot=board_lots.get(c, 1000),
                            commission_rate=float(cfg.costs.commission_rate),
                            slippage=float(cfg.costs.slippage),
                            entry_price=entry_price,
                            trades=trades,
                            reason="single_long_stop",
                        )
                        trade_fee_day += fee
                        trade_fee_total += fee
                        long_stop_triggered.add(c)

                # single short stop
                for c in sorted(short_codes):
                    if c in short_stop_triggered:
                        continue
                    sh = float(shares.get(c, 0.0))
                    if sh >= 0:
                        continue
                    if not short_enabled.get(c, True):
                        continue
                    ep = entry_price.get(c, np.nan)
                    cp = px.get(c, np.nan)
                    if not np.isfinite(ep) or not np.isfinite(cp):
                        continue
                    adverse = cp / ep - 1.0
                    if adverse >= float(cfg.stop_loss.single_short_stop) and tradable.get(c, False):
                        action = str(cfg.stop_loss.single_short_action).lower()
                        if action == "close":
                            cash, fee = self._trade_to_target(
                                dt=dt,
                                code=c,
                                target_shares=0.0,
                                shares=shares,
                                cash=cash,
                                px_exec=px,
                                board_lot=board_lots.get(c, 1000),
                                commission_rate=float(cfg.costs.commission_rate),
                                slippage=float(cfg.costs.slippage),
                                entry_price=entry_price,
                                trades=trades,
                                reason="single_short_stop",
                            )
                            trade_fee_day += fee
                            trade_fee_total += fee
                            short_enabled[c] = False
                        short_stop_triggered.add(c)

                # portfolio-level stop
                eq_before_reb = self._equity(cash=cash, shares=shares, prices=px)
                total_ret = eq_before_reb / float(cfg.capital.total_hkd) - 1.0
                if total_ret <= float(cfg.stop_loss.portfolio_stop):
                    for c, sh in list(shares.items()):
                        if abs(sh) <= 1e-12:
                            continue
                        if not tradable.get(c, False):
                            continue
                        cash, fee = self._trade_to_target(
                            dt=dt,
                            code=c,
                            target_shares=0.0,
                            shares=shares,
                            cash=cash,
                            px_exec=px,
                            board_lot=board_lots.get(c, 1000),
                            commission_rate=float(cfg.costs.commission_rate),
                            slippage=float(cfg.costs.slippage),
                            entry_price=entry_price,
                            trades=trades,
                            reason="portfolio_stop",
                        )
                        trade_fee_day += fee
                        trade_fee_total += fee
                    terminated = True

            # scheduled rebalance
            if (not terminated) and (dt in rebalance_dates):
                cash, fee_l, fee_s = self._rebalance_to_target(
                    dt=dt,
                    cfg=cfg,
                    shares=shares,
                    cash=cash,
                    px_exec=px,
                    tradable=tradable,
                    long_weights=weights_long,
                    short_weights=weights_short,
                    short_enabled=short_enabled,
                    board_lots=board_lots,
                    entry_price=entry_price,
                    trades=trades,
                    reason="rebalance",
                )
                fee = fee_l + fee_s
                trade_fee_day += fee
                trade_fee_total += fee

            equity = self._equity(cash=cash, shares=shares, prices=px)
            long_mv, short_mv = self._long_short_mv(shares=shares, prices=px)

            new_rows.append(
                {
                    "date": str(dt.date()),
                    "equity": float(equity),
                    "cash": float(cash),
                    "long_mv": float(long_mv),
                    "short_mv": float(short_mv),
                    "gross_exposure": float(long_mv + short_mv),
                    "net_exposure": float(long_mv - short_mv),
                    "day_pnl": float(day_pnl),
                    "trade_fee": float(trade_fee_day),
                    "borrow_fee": float(borrow_fee),
                    "cum_return": float(equity / float(cfg.capital.total_hkd) - 1.0) if cfg.capital.total_hkd else np.nan,
                }
            )

            prev_price = {c: float(px.get(c, np.nan)) for c in codes}

        self._append_snapshots(snapshots_path, new_rows)
        self._write_json(run_dir / "trades.json", trades)

        state["cash"] = float(cash)
        state["shares"] = {k: float(v) for k, v in shares.items()}
        state["entry_price"] = self._json_float_map(entry_price)
        state["prev_price"] = self._json_float_map(prev_price)
        state["board_lots"] = {k: int(v) for k, v in board_lots.items()}
        state["short_enabled"] = {k: bool(v) for k, v in short_enabled.items()}
        state["costs"] = {
            "trade_fee_total": float(trade_fee_total),
            "borrow_fee_total": float(borrow_fee_total),
        }
        state["flags"] = {
            "terminated": bool(terminated),
            "long_stop_triggered": sorted(list(long_stop_triggered)),
            "short_stop_triggered": sorted(list(short_stop_triggered)),
        }
        state["last_update_date"] = str(trading_days[-1].date())

        self._write_json(state_path, state)

        self._upsert_active(
            {
                "run_id": run_id,
                "strategy_name": state.get("strategy_name", cfg.strategy_name),
                "created_at": state.get("created_at", self._now_iso()),
                "last_update_date": state.get("last_update_date", ""),
                "status": "active" if not terminated else "stopped",
                "run_dir": str(run_dir),
            }
        )
        self.build_dashboard()

        return {
            "run_id": run_id,
            "updated_days": int(len(new_rows)),
            "last_update_date": state["last_update_date"],
            "terminated": bool(terminated),
        }

    def update_all(self, as_of: str = "today") -> List[Dict[str, Any]]:
        """Update all active runs."""
        rows = self._load_active()
        out: List[Dict[str, Any]] = []
        for row in rows:
            rid = str(row.get("run_id", "")).strip()
            if not rid:
                continue
            status = str(row.get("status", "active"))
            if status != "active":
                continue
            try:
                out.append(self.update(rid, as_of=as_of))
            except Exception as exc:
                out.append({"run_id": rid, "updated_days": 0, "error": str(exc)})
        self.build_dashboard()
        return out

    def status(self, run_id: str = "") -> List[PaperRunSummary]:
        """List paper-run summaries."""
        runs = self._load_active()
        out: List[PaperRunSummary] = []

        for row in runs:
            rid = str(row.get("run_id", "")).strip()
            if run_id and rid != run_id:
                continue
            run_dir = Path(str(row.get("run_dir", self.paper_dir / rid)))
            snaps = self._load_snapshots(run_dir / "snapshots.csv")
            if snaps.empty:
                equity = float("nan")
                cum_ret = float("nan")
                last_date = str(row.get("last_update_date", ""))
            else:
                last = snaps.iloc[-1]
                equity = float(last.get("equity", np.nan))
                cum_ret = float(last.get("cum_return", np.nan))
                last_date = str(last.get("date", row.get("last_update_date", "")))

            out.append(
                PaperRunSummary(
                    run_id=rid,
                    strategy_name=str(row.get("strategy_name", "")),
                    created_at=str(row.get("created_at", "")),
                    last_update_date=last_date,
                    equity=equity,
                    cum_return=cum_ret,
                    status=str(row.get("status", "active")),
                    run_dir=str(run_dir),
                )
            )
        return out

    def build_dashboard(self) -> Path:
        """Backward-compatible alias."""
        return self.generate_dashboard()

    def archive_runs_for_strategy(self, config_path: Path, strategy_name: str = "") -> Dict[str, Any]:
        """Archive paper runs linked to one strategy and remove them from active registry."""
        target_cfg = self._resolve_path(config_path)
        target_cfg_text = str(target_cfg.resolve()) if target_cfg.exists() else str(target_cfg)
        target_name = str(strategy_name or "").strip()

        active_rows = self._load_active()
        keep_rows: List[Dict[str, Any]] = []
        archived: List[Dict[str, Any]] = []
        seen_run_ids: Set[str] = set()

        for row in active_rows:
            rid = str(row.get("run_id", "")).strip()
            if not rid:
                continue
            run_dir = Path(str(row.get("run_dir", self.paper_dir / rid)))
            if not run_dir.is_absolute():
                run_dir = self._resolve_path(run_dir)
            state_path = run_dir / "state.json"
            match = self._run_matches_strategy(
                state_path=state_path,
                target_config_path=target_cfg_text,
                target_strategy_name=target_name,
                fallback_row=row,
            )
            if not match:
                keep_rows.append(row)
                continue
            seen_run_ids.add(rid)
            archived.append(self._archive_run_dir(run_dir=run_dir, run_id=rid))

        for state_path in sorted(self.paper_dir.glob("*/state.json")):
            if self.paper_trash_dir in state_path.parents:
                continue
            run_dir = state_path.parent
            rid = run_dir.name
            if rid in seen_run_ids:
                continue
            if not self._run_matches_strategy(
                state_path=state_path,
                target_config_path=target_cfg_text,
                target_strategy_name=target_name,
                fallback_row=None,
            ):
                continue
            archived.append(self._archive_run_dir(run_dir=run_dir, run_id=rid))

        self._save_active(keep_rows)
        self.build_dashboard()
        return {
            "count": len(archived),
            "run_ids": [str(x.get("run_id", "")) for x in archived],
            "archived": archived,
        }

    def restore_runs_for_strategy(self, config_path: Path, strategy_name: str = "") -> Dict[str, Any]:
        """Restore archived paper runs linked to one strategy and re-register them as active."""
        target_cfg = self._resolve_path(config_path)
        target_cfg_text = str(target_cfg.resolve()) if target_cfg.exists() else str(target_cfg)
        target_name = str(strategy_name or "").strip()
        restored: List[Dict[str, Any]] = []

        for state_path in sorted(self.paper_trash_dir.glob("*/state.json")):
            run_dir = state_path.parent
            rid = run_dir.name.split("__", 1)[0]
            if not self._run_matches_strategy(
                state_path=state_path,
                target_config_path=target_cfg_text,
                target_strategy_name=target_name,
                fallback_row=None,
            ):
                continue
            restored_info = self._restore_run_dir(run_dir=run_dir, run_id=rid)
            restored.append(restored_info)
            state = {}
            try:
                state = self._read_json(Path(restored_info["restored_dir"]) / "state.json")
            except Exception:
                state = {}
            self._upsert_active(
                {
                    "run_id": str(state.get("run_id", rid)),
                    "strategy_name": str(state.get("strategy_name", target_name)),
                    "created_at": str(state.get("created_at", "")),
                    "last_update_date": str(state.get("last_update_date", "")),
                    "status": "active",
                    "run_dir": str(restored_info["restored_dir"]),
                }
            )

        self.build_dashboard()
        return {
            "count": len(restored),
            "run_ids": [str(x.get("run_id", "")) for x in restored],
            "restored": restored,
        }

    def generate_dashboard(self) -> Path:
        """Render advanced HTML dashboard for all paper runs."""
        payload = self._collect_dashboard_payload()
        now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        data_js = json.dumps(payload, ensure_ascii=False)

        html = f"""<!doctype html>
<html lang='zh-CN'>
<head>
  <meta charset='utf-8' />
  <meta name='viewport' content='width=device-width, initial-scale=1' />
  <title>模拟盘管理中心</title>
  <script src='https://cdn.plot.ly/plotly-2.35.2.min.js'></script>
  <style>
    :root {{
      --bg: #0d1117;
      --panel: #161b22;
      --panel-2: #21262d;
      --text: #c9d1d9;
      --muted: #8b949e;
      --up: #3fb950;
      --down: #f85149;
      --accent: #58a6ff;
      --border: #30363d;
      --radius: 12px;
      --btn-radius: 8px;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Hiragino Sans GB", "Microsoft YaHei", sans-serif;
      background: var(--bg);
      color: var(--text);
    }}
    .mono {{ font-family: "JetBrains Mono", "Fira Code", Menlo, Consolas, monospace; }}
    .wrap {{ max-width: 1440px; margin: 0 auto; padding: 16px 16px 28px; }}
    .topbar {{
      position: sticky; top: 0; z-index: 20;
      background: rgba(13, 17, 23, 0.96);
      backdrop-filter: blur(6px);
      border-bottom: 1px solid var(--border);
      margin: -16px -16px 0;
      padding: 14px 16px;
    }}
    .topbar-row {{
      display: flex; justify-content: space-between; align-items: center; gap: 16px;
      max-width: 1440px; margin: 0 auto;
    }}
    .title {{ margin: 0; font-size: 24px; font-weight: 700; letter-spacing: .2px; }}
    .sub {{ margin-top: 4px; color: var(--muted); font-size: 14px; }}
    .actions {{ display: flex; gap: 10px; flex-wrap: wrap; }}
    .btn {{
      border-radius: var(--btn-radius);
      border: 1px solid var(--border);
      color: var(--text);
      background: transparent;
      font-size: 14px;
      padding: 8px 14px;
      cursor: pointer;
    }}
    .btn.primary {{ background: var(--accent); color: #05152a; border-color: #79b8ff; font-weight: 600; }}
    .btn.secondary {{ background: var(--panel-2); }}
    .btn.small {{ padding: 6px 10px; font-size: 12px; }}
    .btn:hover {{ opacity: .92; }}
    .section {{ margin-top: 18px; }}
    .kpi-grid {{
      display: grid; gap: 16px;
      grid-template-columns: repeat(4, minmax(0, 1fr));
    }}
    .kpi {{
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      padding: 16px;
      min-height: 126px;
    }}
    .kpi .label {{ color: var(--muted); font-size: 12px; letter-spacing: .4px; }}
    .kpi .value {{ margin-top: 6px; font-size: 26px; font-weight: 700; }}
    .kpi .delta {{ margin-top: 2px; font-size: 13px; }}
    .spark {{ width: 100%; height: 34px; margin-top: 8px; }}
    .cards-head {{
      display: flex; justify-content: space-between; align-items: center; gap: 8px; margin-bottom: 12px;
    }}
    .cards {{
      display: grid;
      gap: 16px;
      grid-template-columns: repeat(3, minmax(0, 1fr));
    }}
    .card {{
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      padding: 20px;
      display: flex;
      flex-direction: column;
      gap: 10px;
    }}
    .card-top {{
      display: flex; justify-content: space-between; align-items: center; color: var(--muted); font-size: 12px;
    }}
    .status-dot {{
      display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 6px;
      background: #6e7681;
    }}
    .status-dot.active {{ background: var(--up); }}
    .strategy-name {{ font-size: 22px; font-weight: 700; line-height: 1.2; }}
    .strategy-desc {{
      color: var(--muted); font-size: 13px; min-height: 32px;
      display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden;
    }}
    .split {{
      display: grid; grid-template-columns: 1fr 1fr; gap: 8px;
      align-items: end;
    }}
    .metric-big {{ font-size: 30px; font-weight: 700; line-height: 1; }}
    .metric-tag {{ color: var(--muted); font-size: 12px; margin-top: 4px; }}
    .card-curve {{
      border: 1px solid var(--border);
      background: rgba(255, 255, 255, 0.015);
      border-radius: 10px;
      height: 88px;
      display: flex; align-items: center;
      padding: 10px 8px;
    }}
    .card-foot {{
      color: var(--muted);
      font-size: 12px;
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 6px 10px;
    }}
    .card-actions {{ display: flex; gap: 8px; justify-content: flex-end; margin-top: 2px; }}
    details.panel {{
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      padding: 12px 14px;
      margin-top: 18px;
    }}
    details.panel > summary {{
      cursor: pointer; font-weight: 600; color: var(--text);
      list-style: none;
    }}
    details.panel > summary::-webkit-details-marker {{ display: none; }}
    .panel-sub {{ color: var(--muted); font-size: 13px; margin-top: 4px; }}
    .plot {{
      width: 100%;
      min-height: 360px;
      margin-top: 10px;
      border: 1px solid var(--border);
      border-radius: 10px;
      overflow: hidden;
    }}
    .table-wrap {{
      margin-top: 18px;
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      overflow: hidden;
    }}
    .table-head {{
      display: flex; justify-content: space-between; align-items: center;
      border-bottom: 1px solid var(--border);
      padding: 12px 14px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
    }}
    th, td {{
      padding: 10px 12px;
      border-bottom: 1px solid rgba(48, 54, 61, .65);
      text-align: left;
    }}
    th {{
      color: var(--muted);
      background: #11161f;
      font-size: 12px;
      letter-spacing: .35px;
      cursor: pointer;
      user-select: none;
      white-space: nowrap;
    }}
    tbody tr:hover {{ background: rgba(88, 166, 255, 0.06); }}
    .num {{ text-align: right; font-variant-numeric: tabular-nums; }}
    .pos {{ color: var(--up); font-weight: 700; }}
    .neg {{ color: var(--down); font-weight: 700; }}
    .row-detail td {{
      background: rgba(255, 255, 255, 0.02);
      border-bottom: 1px solid rgba(48, 54, 61, .65);
      padding: 10px;
    }}
    .detail-box {{
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 10px;
      background: #11161f;
    }}
    .mini-table th, .mini-table td {{
      padding: 6px 8px;
      font-size: 12px;
      border-bottom: 1px solid rgba(48, 54, 61, .4);
    }}
    .mini-table tr:last-child td {{ border-bottom: none; }}
    .warn-row {{ outline: 1px solid rgba(255, 187, 0, .55); }}
    .idle-list {{ margin-top: 10px; display: grid; gap: 8px; }}
    .idle-item {{
      background: #11161f; border: 1px solid var(--border); border-radius: 10px;
      padding: 10px 12px; display: flex; justify-content: space-between; align-items: center; gap: 10px;
    }}
    .muted {{ color: var(--muted); }}
    @media (max-width: 1200px) {{
      .cards {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
      .kpi-grid {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
    }}
    @media (max-width: 768px) {{
      .cards, .kpi-grid {{ grid-template-columns: 1fr; }}
      .topbar-row {{ flex-direction: column; align-items: flex-start; }}
      .actions {{ width: 100%; }}
    }}
  </style>
</head>
<body>
  <div class='topbar'>
    <div class='topbar-row'>
      <div>
        <h1 class='title'>模拟盘管理中心</h1>
        <div class='sub' id='top-summary'>加载中...</div>
      </div>
      <div class='actions'>
        <button class='btn primary' type='button' onclick='window.dispatchEvent(new Event("pt-refresh"))'>刷新状态</button>
        <button class='btn secondary' type='button' onclick='window.dispatchEvent(new Event("pt-update-all"))'>全部更新</button>
      </div>
    </div>
  </div>

  <div class='wrap'>
    <div class='sub'>报表时间：{self._esc(now_text)}</div>

    <section class='section'>
      <div id='kpi-grid' class='kpi-grid'></div>
    </section>

    <section class='section'>
      <div class='cards-head'>
        <h2 style='margin:0'>策略卡片</h2>
        <div class='actions'>
          <button class='btn small secondary' type='button' data-sort='cum_return'>按收益</button>
          <button class='btn small secondary' type='button' data-sort='last_update_date'>按日期</button>
          <button class='btn small secondary' type='button' data-sort='equity'>按权益</button>
        </div>
      </div>
      <div id='cards' class='cards'></div>
    </section>

    <details class='panel' open>
      <summary>净值曲线对比（活跃策略）</summary>
      <div class='panel-sub'>默认归一化到 1.0，图例可点击显隐。</div>
      <div id='nav-compare' class='plot'></div>
    </details>

    <section class='table-wrap'>
      <div class='table-head'>
        <h3 style='margin:0'>运行清单</h3>
        <div class='muted' id='table-summary'>-</div>
      </div>
      <div style='overflow:auto'>
        <table id='runs-table'>
          <thead>
            <tr>
              <th data-key='strategy_name'>策略名称</th>
              <th data-key='status'>状态</th>
              <th data-key='run_days'>已记录交易日</th>
              <th class='num' data-key='equity'>权益(HKD)</th>
              <th class='num' data-key='cum_return'>累计收益率</th>
              <th class='num' data-key='today_return'>今日</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </section>

    <details class='panel'>
      <summary>未启动策略 (<span id='idle-count'>0</span>)</summary>
      <div class='idle-list' id='idle-list'></div>
    </details>
  </div>

  <script>
    const DATA = {data_js};
    const STATE = {{
      sortKey: "cum_return",
      sortDesc: true,
      tableSortKey: "cum_return",
      tableSortDesc: true
    }};

    function isNum(v) {{
      return typeof v === "number" && Number.isFinite(v);
    }}
    function fmtNum(v, digits = 2) {{
      return isNum(v) ? Number(v).toLocaleString("en-US", {{ minimumFractionDigits: digits, maximumFractionDigits: digits }}) : "-";
    }}
    function fmtPct(v, digits = 2) {{
      return isNum(v) ? `${{(v * 100).toFixed(digits)}}%` : "-";
    }}
    function clsBySign(v) {{
      if (!isNum(v)) return "";
      return v >= 0 ? "pos" : "neg";
    }}
    function colorBySign(v) {{
      if (!isNum(v)) return "var(--muted)";
      return v >= 0 ? "var(--up)" : "var(--down)";
    }}
    function statusBadge(status) {{
      return (status || "").toLowerCase() === "active" ? "🟢 运行中" : "⚪ 已停止";
    }}
    function sparklineSvg(values, color) {{
      const vals = (values || []).filter((x) => isNum(x));
      if (!vals.length) return "<svg class='spark'></svg>";
      const min = Math.min(...vals);
      const max = Math.max(...vals);
      const span = (max - min) || 1;
      const w = 300;
      const h = 34;
      const pts = vals.map((v, i) => {{
        const x = (i / Math.max(1, vals.length - 1)) * w;
        const y = h - ((v - min) / span) * (h - 2) - 1;
        return `${{x.toFixed(1)}},${{y.toFixed(1)}}`;
      }}).join(" ");
      return `<svg class="spark" viewBox="0 0 ${{w}} ${{h}}" preserveAspectRatio="none"><polyline fill="none" stroke="${{color}}" stroke-width="2" points="${{pts}}"/></svg>`;
    }}
    function tail(arr, n) {{
      return Array.isArray(arr) ? arr.slice(Math.max(0, arr.length - n)) : [];
    }}
    function getTotalSeries(runs) {{
      const map = new Map();
      runs.forEach((r) => {{
        (r.snapshots || []).forEach((s) => {{
          if (!isNum(s.equity) || !s.date) return;
          map.set(s.date, (map.get(s.date) || 0) + s.equity);
        }});
      }});
      const dates = Array.from(map.keys()).sort();
      return dates.map((d) => ({{ date: d, equity: map.get(d) }}));
    }}

    function renderTopSummary() {{
      const s = DATA.summary || {{}};
      const totalRetHtml = `<span style="color:${{colorBySign(s.total_return)}}">${{fmtPct(s.total_return)}}</span>`;
      const txt = `${{s.active_count || 0}}个活跃策略 · 总权益: HKD ${{fmtNum(s.total_equity)}} · 累计收益率: ${{totalRetHtml}}`;
      document.getElementById("top-summary").innerHTML = txt;
    }}

    function renderKpis() {{
      const s = DATA.summary || {{}};
      const activeRuns = (DATA.runs || []).filter((r) => (r.status || "").toLowerCase() === "active");
      const totalSeries = tail(getTotalSeries(activeRuns).map((x) => x.equity), 30);
      const best = s.best_run || {{}};
      const worst = s.worst_run || {{}};
      const bestRun = (DATA.runs || []).find((r) => r.run_id === best.run_id);
      const worstRun = (DATA.runs || []).find((r) => r.run_id === worst.run_id);
      const cards = [
        {{
          label: "总权益(HKD)",
          value: fmtNum(s.total_equity),
          delta: fmtPct(s.total_return),
          deltaCls: clsBySign(s.total_return),
          spark: sparklineSvg(totalSeries, colorBySign(s.total_return)),
        }},
        {{
          label: "今日盈亏",
          value: fmtNum(s.total_today_pnl),
          delta: fmtPct(s.total_today_return),
          deltaCls: clsBySign(s.total_today_return),
          spark: sparklineSvg(totalSeries.map((v, i, arr) => i === 0 ? 0 : v - arr[i - 1]), colorBySign(s.total_today_return)),
        }},
        {{
          label: "最佳策略",
          value: best.strategy_name || "-",
          delta: fmtPct(best.cum_return),
          deltaCls: clsBySign(best.cum_return),
          spark: sparklineSvg(tail(((bestRun || {{}}).snapshots || []).map((x) => x.equity), 30), colorBySign(best.cum_return)),
        }},
        {{
          label: "最差策略",
          value: worst.strategy_name || "-",
          delta: fmtPct(worst.cum_return),
          deltaCls: clsBySign(worst.cum_return),
          spark: sparklineSvg(tail(((worstRun || {{}}).snapshots || []).map((x) => x.equity), 30), colorBySign(worst.cum_return)),
        }},
      ];
      const el = document.getElementById("kpi-grid");
      el.innerHTML = cards.map((x) => `
        <article class="kpi">
          <div class="label">${{x.label}}</div>
          <div class="value mono">${{x.value}}</div>
          <div class="delta ${{x.deltaCls}}">${{x.delta}}</div>
          ${{x.spark}}
        </article>
      `).join("");
    }}

    function sortedRunsForCards() {{
      const runs = [...(DATA.runs || [])].filter((r) => (r.status || "").toLowerCase() === "active");
      const key = STATE.sortKey;
      runs.sort((a, b) => {{
        const va = a[key];
        const vb = b[key];
        if (typeof va === "string" || typeof vb === "string") {{
          return STATE.sortDesc ? String(vb || "").localeCompare(String(va || "")) : String(va || "").localeCompare(String(vb || ""));
        }}
        const na = isNum(va) ? va : -1e100;
        const nb = isNum(vb) ? vb : -1e100;
        return STATE.sortDesc ? (nb - na) : (na - nb);
      }});
      return runs;
    }}

    function renderCards() {{
      const runs = sortedRunsForCards();
      const box = document.getElementById("cards");
      if (!runs.length) {{
        box.innerHTML = `<div class="muted">暂无活跃策略</div>`;
        return;
      }}
      box.innerHTML = runs.map((r) => {{
        const equity = isNum(r.equity) ? fmtNum(r.equity) : "-";
        const curve = sparklineSvg(tail((r.snapshots || []).map((x) => x.equity), 30), colorBySign(r.cum_return));
        const tagCls = clsBySign(r.cum_return);
        const statusCls = (r.status || "").toLowerCase() === "active" ? "active" : "";
        return `
          <article class="card">
            <div class="card-top">
              <div><span class="status-dot ${{statusCls}}"></span>${{(r.status || "stopped").toLowerCase()}}</div>
              <div class="muted">⋮</div>
            </div>
            <div class="strategy-name">${{r.strategy_name || r.strategy_file || r.run_id}}</div>
            <div class="strategy-desc">${{r.description || "无策略说明"}}</div>
            <div class="split">
              <div>
                <div class="metric-big mono">${{equity}}</div>
                <div class="metric-tag">权益(HKD)</div>
              </div>
              <div>
                <div class="metric-big mono ${{tagCls}}">${{fmtPct(r.cum_return)}}</div>
                <div class="metric-tag">累计收益率</div>
              </div>
            </div>
            <div class="card-curve">${{curve}}</div>
            <div class="card-foot">
              <div>多头 ${{r.long_count || 0}} 只 · 空头 ${{r.short_count || 0}} 只</div>
              <div>已记录 ${{r.run_days || 0}} 个交易日</div>
              <div class="mono">现金: ${{fmtNum(r.cash)}}</div>
              <div>下次调仓: ${{r.next_rebalance || "-"}}</div>
              <div class="mono">融券费: ${{fmtNum(r.borrow_fee_total)}}</div>
              <div class="mono">交易费: ${{fmtNum(r.trade_fee_total)}}</div>
            </div>
            <div class="card-actions">
              <button class="btn small primary" type="button" title="请回到应用内执行更新">更新到今日</button>
              <button class="btn small secondary" type="button" data-run-detail="${{r.run_id}}">查看详情</button>
            </div>
          </article>
        `;
      }}).join("");
      box.querySelectorAll("[data-run-detail]").forEach((btn) => {{
        btn.addEventListener("click", () => {{
          const rid = btn.getAttribute("data-run-detail");
          const row = document.querySelector(`tr[data-run-id="${{rid}}"]`);
          if (row) row.click();
          row && row.scrollIntoView({{ behavior: "smooth", block: "center" }});
        }});
      }});
    }}

    function renderCompareChart() {{
      const runs = (DATA.runs || []).filter((r) => (r.status || "").toLowerCase() === "active");
      const traces = [];
      runs.forEach((r) => {{
        const snaps = (r.snapshots || []).filter((x) => x && x.date && isNum(x.equity));
        if (!snaps.length) return;
        const base = snaps[0].equity || 1;
        const x = snaps.map((s) => s.date);
        const y = snaps.map((s) => s.equity / base);
        traces.push({{
          type: "scatter",
          mode: "lines",
          x,
          y,
          name: r.strategy_name || r.run_id,
          line: {{ width: 2 }}
        }});
      }});
      const layout = {{
        paper_bgcolor: "#161b22",
        plot_bgcolor: "#161b22",
        margin: {{ l: 46, r: 12, t: 12, b: 36 }},
        font: {{ color: "#c9d1d9", size: 12 }},
        legend: {{ orientation: "h" }},
        xaxis: {{ gridcolor: "#30363d", zeroline: false }},
        yaxis: {{ gridcolor: "#30363d", zeroline: false, tickformat: ".2f" }},
      }};
      Plotly.newPlot("nav-compare", traces, layout, {{ displaylogo: false, responsive: true }});
    }}

    function sortedRunsForTable() {{
      const runs = [...(DATA.runs || [])];
      const key = STATE.tableSortKey;
      runs.sort((a, b) => {{
        const va = a[key];
        const vb = b[key];
        if (typeof va === "string" || typeof vb === "string") {{
          return STATE.tableSortDesc ? String(vb || "").localeCompare(String(va || "")) : String(va || "").localeCompare(String(vb || ""));
        }}
        const na = isNum(va) ? va : -1e100;
        const nb = isNum(vb) ? vb : -1e100;
        return STATE.tableSortDesc ? (nb - na) : (na - nb);
      }});
      return runs;
    }}

    function renderDetailRow(run) {{
      const posRows = (run.positions || []).map((p) => {{
        const cls = clsBySign(p.pnl_pct);
        const warn = p.near_stop ? "warn-row" : "";
        return `
          <tr class="${{warn}}">
            <td>${{p.direction || "-"}}</td>
            <td>${{p.code || "-"}}</td>
            <td>${{p.name || "-"}}</td>
            <td class="num mono">${{fmtNum(p.shares, 0)}}</td>
            <td class="num mono">${{fmtNum(p.cost_price)}}</td>
            <td class="num mono">${{fmtNum(p.last_price)}}</td>
            <td class="num mono ${{cls}}">${{fmtNum(p.pnl_amount)}}</td>
            <td class="num mono ${{cls}}">${{fmtPct(p.pnl_pct)}}</td>
            <td class="num mono">${{fmtPct(p.weight)}}</td>
          </tr>
        `;
      }}).join("");

      return `
        <tr class="row-detail">
          <td colspan="6">
            <div class="detail-box">
              <div class="muted" style="margin-bottom:8px;">持仓明细（接近止损线的条目会标黄框）</div>
              <table class="mini-table" style="width:100%; border-collapse:collapse;">
                <thead>
                  <tr>
                    <th>方向</th><th>代码</th><th>名称</th>
                    <th class="num">股数</th><th class="num">成本价</th><th class="num">现价</th>
                    <th class="num">盈亏</th><th class="num">盈亏%</th><th class="num">占比</th>
                  </tr>
                </thead>
                <tbody>
                  ${{posRows || '<tr><td colspan="9" class="muted">暂无持仓数据</td></tr>'}}
                </tbody>
              </table>
            </div>
          </td>
        </tr>
      `;
    }}

    function renderRunsTable() {{
      const runs = sortedRunsForTable();
      document.getElementById("table-summary").textContent = `共 ${{runs.length}} 条`;
      const tbody = document.querySelector("#runs-table tbody");
      const htmlRows = [];
      runs.forEach((r) => {{
        const rid = r.run_id || "";
        htmlRows.push(`
          <tr data-run-id="${{rid}}">
            <td>${{r.strategy_name || r.strategy_file || rid}}</td>
            <td>${{statusBadge(r.status)}}</td>
            <td>${{r.run_days || 0}}</td>
            <td class="num mono">${{fmtNum(r.equity)}}</td>
            <td class="num mono ${{clsBySign(r.cum_return)}}">${{fmtPct(r.cum_return)}}</td>
            <td class="num mono ${{clsBySign(r.today_return)}}">${{fmtPct(r.today_return)}}</td>
          </tr>
        `);
      }});
      tbody.innerHTML = htmlRows.join("");

      tbody.querySelectorAll("tr[data-run-id]").forEach((row) => {{
        row.addEventListener("click", () => {{
          const rid = row.getAttribute("data-run-id");
          const next = row.nextElementSibling;
          if (next && next.classList.contains("row-detail")) {{
            next.remove();
            return;
          }}
          const run = (DATA.runs || []).find((x) => (x.run_id || "") === rid);
          if (!run) return;
          row.insertAdjacentHTML("afterend", renderDetailRow(run));
        }});
      }});
    }}

    function bindSortActions() {{
      document.querySelectorAll("[data-sort]").forEach((btn) => {{
        btn.addEventListener("click", () => {{
          const key = btn.getAttribute("data-sort");
          if (STATE.sortKey === key) {{
            STATE.sortDesc = !STATE.sortDesc;
          }} else {{
            STATE.sortKey = key;
            STATE.sortDesc = true;
          }}
          renderCards();
        }});
      }});
      document.querySelectorAll("#runs-table th[data-key]").forEach((th) => {{
        th.addEventListener("click", () => {{
          const key = th.getAttribute("data-key");
          if (STATE.tableSortKey === key) {{
            STATE.tableSortDesc = !STATE.tableSortDesc;
          }} else {{
            STATE.tableSortKey = key;
            STATE.tableSortDesc = true;
          }}
          renderRunsTable();
        }});
      }});
    }}

    function renderIdleStrategies() {{
      const items = DATA.idle_strategies || [];
      document.getElementById("idle-count").textContent = String(items.length);
      const box = document.getElementById("idle-list");
      if (!items.length) {{
        box.innerHTML = '<div class="muted">暂无未启动策略</div>';
        return;
      }}
      box.innerHTML = items.map((x) => `
        <div class="idle-item">
          <div>
            <div style="font-weight:600;">${{x.strategy_name || x.file}}</div>
            <div class="muted">${{x.description || x.file}}</div>
          </div>
          <button class="btn small secondary" type="button" title="请在应用内点击启动">启动模拟盘</button>
        </div>
      `).join("");
    }}

    function init() {{
      renderTopSummary();
      renderKpis();
      bindSortActions();
      renderCards();
      renderCompareChart();
      renderRunsTable();
      renderIdleStrategies();
    }}
    init();
  </script>
</body>
</html>
"""
        self.dashboard_path.write_text(html, encoding="utf-8")
        return self.dashboard_path

    def _collect_dashboard_payload(self) -> Dict[str, Any]:
        runs: List[Dict[str, Any]] = []
        active_rows = self._load_active()
        universe_name_map = self._universe_code_name_map()
        used_configs: Set[str] = set()

        for row in active_rows:
            rid = str(row.get("run_id", "")).strip()
            if not rid:
                continue
            run_dir = Path(str(row.get("run_dir", self.paper_dir / rid)))
            if not run_dir.is_absolute():
                run_dir = self._resolve_path(run_dir)

            state_path = run_dir / "state.json"
            state = self._read_json(state_path) if state_path.exists() else {}
            snapshots = self._load_snapshots(run_dir / "snapshots.csv")
            trades = self._read_json(run_dir / "trades.json") if (run_dir / "trades.json").exists() else []
            if not isinstance(trades, list):
                trades = []

            config_path = self._resolve_path(state.get("config_path", run_dir / "config.yaml"))
            meta = self._read_strategy_meta(config_path)
            used_configs.add(str(config_path.resolve()) if config_path.exists() else str(config_path))

            status = str(row.get("status", "active")).strip() or "active"
            if bool(dict(state.get("flags", {})).get("terminated", False)):
                status = "stopped"

            equity = float("nan")
            cum_return = float("nan")
            today_return = float("nan")
            today_pnl = float("nan")
            last_date = str(row.get("last_update_date", "")).strip()
            snap_records: List[Dict[str, Any]] = []
            run_days = 0
            if not snapshots.empty:
                run_days = int(len(snapshots))
                snapshots["date"] = snapshots["date"].astype(str)
                snap_records = [
                    {
                        "date": str(x.get("date", "")),
                        "equity": self._safe_float(x.get("equity", np.nan)),
                        "cum_return": self._safe_float(x.get("cum_return", np.nan)),
                    }
                    for x in snapshots.tail(240).to_dict("records")
                ]
                last = snapshots.iloc[-1]
                equity = self._safe_float(last.get("equity", np.nan))
                cum_return = self._safe_float(last.get("cum_return", np.nan))
                today_pnl = self._safe_float(last.get("day_pnl", np.nan))
                last_date = str(last.get("date", last_date))
                if len(snapshots) >= 2:
                    prev_eq = self._safe_float(snapshots.iloc[-2].get("equity", np.nan))
                    if np.isfinite(prev_eq) and prev_eq != 0 and np.isfinite(equity):
                        today_return = equity / prev_eq - 1.0

            initial_capital = self._safe_float(state.get("initial_capital_hkd", np.nan))
            if np.isfinite(equity) and np.isfinite(initial_capital) and initial_capital != 0:
                cum_return = equity / initial_capital - 1.0

            positions = self._build_position_rows(state=state, meta=meta, universe_name_map=universe_name_map)
            next_rebalance = self._next_rebalance_date(config_path=config_path, after_date=last_date)
            costs = dict(state.get("costs", {}))

            runs.append(
                {
                    "run_id": rid,
                    "strategy_name": str(meta.get("strategy_name") or state.get("strategy_name") or row.get("strategy_name") or rid),
                    "strategy_file": str(config_path.name if config_path.exists() else ""),
                    "description": str(meta.get("description", "")),
                    "status": status,
                    "created_at": str(state.get("created_at", row.get("created_at", ""))),
                    "last_update_date": last_date,
                    "equity": float(equity) if np.isfinite(equity) else None,
                    "cum_return": float(cum_return) if np.isfinite(cum_return) else None,
                    "today_return": float(today_return) if np.isfinite(today_return) else None,
                    "today_pnl": float(today_pnl) if np.isfinite(today_pnl) else None,
                    "initial_capital_hkd": float(initial_capital) if np.isfinite(initial_capital) else None,
                    "cash": self._safe_float(state.get("cash", np.nan)),
                    "run_days": run_days,
                    "long_count": int(meta.get("long_count", 0)),
                    "short_count": int(meta.get("short_count", 0)),
                    "next_rebalance": next_rebalance,
                    "borrow_fee_total": self._safe_float(costs.get("borrow_fee_total", np.nan)),
                    "trade_fee_total": self._safe_float(costs.get("trade_fee_total", np.nan)),
                    "snapshots": snap_records,
                    "positions": positions,
                    "trades": trades[-120:],
                }
            )

        runs.sort(key=lambda x: (x.get("last_update_date", ""), x.get("strategy_name", "")), reverse=True)

        active_runs = [r for r in runs if str(r.get("status", "")).lower() == "active"]
        total_equity = sum(float(r["equity"]) for r in active_runs if isinstance(r.get("equity"), (int, float)))
        total_initial = sum(float(r["initial_capital_hkd"]) for r in active_runs if isinstance(r.get("initial_capital_hkd"), (int, float)))
        total_today_pnl = sum(float(r["today_pnl"]) for r in active_runs if isinstance(r.get("today_pnl"), (int, float)))

        total_return = float("nan")
        if np.isfinite(total_equity) and np.isfinite(total_initial) and total_initial > 0:
            total_return = total_equity / total_initial - 1.0

        total_today_return = float("nan")
        prev_total = total_equity - total_today_pnl
        if np.isfinite(prev_total) and prev_total > 0 and np.isfinite(total_today_pnl):
            total_today_return = total_today_pnl / prev_total

        valid_runs = [r for r in active_runs if isinstance(r.get("cum_return"), (int, float))]
        best_run = max(valid_runs, key=lambda x: float(x["cum_return"])) if valid_runs else None
        worst_run = min(valid_runs, key=lambda x: float(x["cum_return"])) if valid_runs else None

        idle_strategies: List[Dict[str, Any]] = []
        strategy_dir = self.base_dir / "config" / "strategies"
        if strategy_dir.exists():
            for p in sorted(strategy_dir.glob("*.yaml"), key=lambda x: (x.stat().st_mtime, x.name.lower()), reverse=True):
                key = str(p.resolve())
                if key in used_configs:
                    continue
                meta = self._read_strategy_meta(p)
                idle_strategies.append(
                    {
                        "file": p.name,
                        "strategy_name": str(meta.get("strategy_name") or p.stem),
                        "description": str(meta.get("description", "")),
                    }
                )

        return {
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "summary": {
                "active_count": len(active_runs),
                "total_equity": float(total_equity) if np.isfinite(total_equity) else None,
                "total_return": float(total_return) if np.isfinite(total_return) else None,
                "total_today_pnl": float(total_today_pnl) if np.isfinite(total_today_pnl) else None,
                "total_today_return": float(total_today_return) if np.isfinite(total_today_return) else None,
                "best_run": {
                    "run_id": best_run.get("run_id", "") if best_run else "",
                    "strategy_name": best_run.get("strategy_name", "") if best_run else "",
                    "cum_return": best_run.get("cum_return", None) if best_run else None,
                },
                "worst_run": {
                    "run_id": worst_run.get("run_id", "") if worst_run else "",
                    "strategy_name": worst_run.get("strategy_name", "") if worst_run else "",
                    "cum_return": worst_run.get("cum_return", None) if worst_run else None,
                },
            },
            "runs": runs,
            "idle_strategies": idle_strategies,
        }

    def _universe_code_name_map(self) -> Dict[str, str]:
        out: Dict[str, str] = {}
        uni_path = self.base_dir / "config" / "universe.yaml"
        if not uni_path.exists():
            return out
        try:
            universe = load_universe(uni_path)
            for sec in universe.sectors.values():
                for group in sec.groups.values():
                    for stk in group.stocks:
                        code = str(stk.code).strip().upper()
                        if code:
                            out[code] = str(stk.name or code)
        except Exception:
            return out
        return out

    def _read_strategy_meta(self, config_path: Path) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "strategy_name": config_path.stem,
            "description": "",
            "long_count": 0,
            "short_count": 0,
            "long_codes": [],
            "short_codes": [],
            "long_stop": None,
            "short_stop": None,
            "rebalance_freq": "monthly",
            "rebalance_day": 1,
        }
        if not config_path.exists() or yaml is None:
            return out
        try:
            raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
            if not isinstance(raw, dict):
                return out
            out["strategy_name"] = str(raw.get("strategy_name", out["strategy_name"]))
            out["description"] = str(raw.get("description", ""))
            long_pos = raw.get("long_positions", [])
            short_pos = raw.get("short_positions", [])
            if isinstance(long_pos, list):
                out["long_codes"] = [str(x.get("code", "")).strip().upper() for x in long_pos if isinstance(x, dict)]
                out["long_count"] = len([x for x in out["long_codes"] if x])
            if isinstance(short_pos, list):
                out["short_codes"] = [str(x.get("code", "")).strip().upper() for x in short_pos if isinstance(x, dict)]
                out["short_count"] = len([x for x in out["short_codes"] if x])

            stop = raw.get("stop_loss", {})
            if isinstance(stop, dict):
                out["long_stop"] = self._safe_float(stop.get("single_long_stop", np.nan))
                out["short_stop"] = self._safe_float(stop.get("single_short_stop", np.nan))
            reb = raw.get("rebalance", {})
            if isinstance(reb, dict):
                out["rebalance_freq"] = str(reb.get("frequency", "monthly")).lower()
                day = int(self._safe_float(reb.get("day", 1))) if np.isfinite(self._safe_float(reb.get("day", np.nan))) else 1
                out["rebalance_day"] = max(1, day)
        except Exception:
            return out
        return out

    def _build_position_rows(self, state: Dict[str, Any], meta: Dict[str, Any], universe_name_map: Dict[str, str]) -> List[Dict[str, Any]]:
        shares_raw = state.get("shares", {})
        if not isinstance(shares_raw, dict):
            return []
        entry = state.get("entry_price", {})
        prev = state.get("prev_price", {})
        if not isinstance(entry, dict):
            entry = {}
        if not isinstance(prev, dict):
            prev = {}

        rows: List[Dict[str, Any]] = []
        gross = 0.0
        for code, shv in shares_raw.items():
            sh = self._safe_float(shv)
            px = self._safe_float(prev.get(code, np.nan))
            if abs(sh) <= 1e-12 or not np.isfinite(px):
                continue
            gross += abs(sh * px)
        if gross <= 0:
            gross = 1.0

        long_stop = self._safe_float(meta.get("long_stop", np.nan))
        short_stop = self._safe_float(meta.get("short_stop", np.nan))

        for code, shv in shares_raw.items():
            sh = self._safe_float(shv)
            if abs(sh) <= 1e-12:
                continue
            code_u = str(code).strip().upper()
            cp = self._safe_float(prev.get(code_u, prev.get(code, np.nan)))
            ep = self._safe_float(entry.get(code_u, entry.get(code, np.nan)))
            mv = abs(sh * cp) if np.isfinite(cp) else np.nan

            pnl_amount = float("nan")
            pnl_pct = float("nan")
            near_stop = False
            if np.isfinite(cp) and np.isfinite(ep) and ep > 0:
                pnl_amount = sh * (cp - ep)
                direction_sign = 1.0 if sh > 0 else -1.0
                pnl_pct = (cp / ep - 1.0) * direction_sign
                if sh > 0 and np.isfinite(long_stop):
                    near_stop = pnl_pct <= (long_stop * 0.8)
                if sh < 0 and np.isfinite(short_stop):
                    adverse = (cp / ep - 1.0)
                    near_stop = adverse >= (short_stop * 0.8)

            rows.append(
                {
                    "direction": "🟢多" if sh > 0 else "🔴空",
                    "code": code_u,
                    "name": universe_name_map.get(code_u, code_u),
                    "shares": float(sh),
                    "cost_price": float(ep) if np.isfinite(ep) else None,
                    "last_price": float(cp) if np.isfinite(cp) else None,
                    "pnl_amount": float(pnl_amount) if np.isfinite(pnl_amount) else None,
                    "pnl_pct": float(pnl_pct) if np.isfinite(pnl_pct) else None,
                    "weight": float((mv / gross) if np.isfinite(mv) else np.nan),
                    "near_stop": bool(near_stop),
                }
            )
        rows.sort(key=lambda x: (x.get("direction", ""), x.get("code", "")))
        return rows

    def _next_rebalance_date(self, config_path: Path, after_date: str) -> str:
        if not config_path.exists():
            return "-"
        meta = self._read_strategy_meta(config_path)
        freq = str(meta.get("rebalance_freq", "monthly")).lower()
        day = int(meta.get("rebalance_day", 1))
        try:
            d0 = self._parse_day(after_date)
        except Exception:
            return "-"
        cal = self._hk_trading_days(start=d0 + timedelta(days=1), end=d0 + timedelta(days=200))
        if cal.empty:
            return "-"
        dates = sorted(self._get_rebalance_dates(cal, freq, day))
        for dt in dates:
            if dt.date() > d0:
                return str(dt.date())
        return "-"

    # -------- core helpers --------

    def _resolve_weights(self, cfg: StrategyConfig, aligned: Dict[str, pd.DataFrame]) -> Tuple[Dict[str, float], Dict[str, float]]:
        long_codes = [p.code for p in cfg.long_positions if p.code in aligned]
        short_codes = [p.code for p in cfg.short_positions if p.code in aligned]

        mode = str(cfg.weighting_mode).lower()
        if mode == "equal":
            return ({c: 1.0 / len(long_codes) for c in long_codes} if long_codes else {}, {c: 1.0 / len(short_codes) for c in short_codes} if short_codes else {})

        if mode == "inverse_volatility":
            def inv_vol(codes: Sequence[str]) -> Dict[str, float]:
                vals: Dict[str, float] = {}
                for c in codes:
                    ret = aligned[c]["adj_close"].pct_change().dropna().tail(120)
                    vol = float(ret.std()) if not ret.empty else np.nan
                    vals[c] = 1.0 / vol if np.isfinite(vol) and vol > 0 else 0.0
                total = sum(vals.values())
                if total <= 0 and codes:
                    return {c: 1.0 / len(codes) for c in codes}
                return {c: vals[c] / total for c in codes}

            return inv_vol(long_codes), inv_vol(short_codes)

        lw = {p.code: float(p.weight) for p in cfg.long_positions if p.code in aligned}
        sw = {p.code: float(p.weight) for p in cfg.short_positions if p.code in aligned}
        return lw, sw

    def _rebalance_to_target(
        self,
        dt: pd.Timestamp,
        cfg: StrategyConfig,
        shares: Dict[str, float],
        cash: float,
        px_exec: Dict[str, float],
        tradable: Dict[str, bool],
        long_weights: Dict[str, float],
        short_weights: Dict[str, float],
        short_enabled: Dict[str, bool],
        board_lots: Dict[str, int],
        entry_price: Dict[str, float],
        trades: List[Dict[str, Any]],
        reason: str,
    ) -> Tuple[float, float, float]:
        fee_long = 0.0
        fee_short = 0.0

        equity_now = self._equity(cash=cash, shares=shares, prices=px_exec)
        long_cap = equity_now * float(cfg.capital.long_pct)
        short_cap = equity_now * float(cfg.capital.short_pct)

        for code, w in long_weights.items():
            px = px_exec.get(code, np.nan)
            if not np.isfinite(px) or px <= 0:
                continue
            target = (long_cap * w) / px
            if tradable.get(code, False):
                cash, fee = self._trade_to_target(
                    dt=dt,
                    code=code,
                    target_shares=target,
                    shares=shares,
                    cash=cash,
                    px_exec=px_exec,
                    board_lot=board_lots.get(code, 1000),
                    commission_rate=float(cfg.costs.commission_rate),
                    slippage=float(cfg.costs.slippage),
                    entry_price=entry_price,
                    trades=trades,
                    reason=reason,
                )
                fee_long += fee

        enabled_short_codes = [c for c in short_weights.keys() if short_enabled.get(c, True)]
        if enabled_short_codes:
            w_sum = sum(short_weights[c] for c in enabled_short_codes)
            for code in enabled_short_codes:
                px = px_exec.get(code, np.nan)
                if not np.isfinite(px) or px <= 0:
                    continue
                w = short_weights[code] / w_sum if w_sum > 0 else 1.0 / len(enabled_short_codes)
                target = -(short_cap * w) / px
                if tradable.get(code, False):
                    cash, fee = self._trade_to_target(
                        dt=dt,
                        code=code,
                        target_shares=target,
                        shares=shares,
                        cash=cash,
                        px_exec=px_exec,
                        board_lot=board_lots.get(code, 1000),
                        commission_rate=float(cfg.costs.commission_rate),
                        slippage=float(cfg.costs.slippage),
                        entry_price=entry_price,
                        trades=trades,
                        reason=reason,
                    )
                    fee_short += fee

        return cash, fee_long, fee_short

    def _trade_to_target(
        self,
        dt: pd.Timestamp,
        code: str,
        target_shares: float,
        shares: Dict[str, float],
        cash: float,
        px_exec: Dict[str, float],
        board_lot: int,
        commission_rate: float,
        slippage: float,
        entry_price: Dict[str, float],
        trades: List[Dict[str, Any]],
        reason: str,
    ) -> Tuple[float, float]:
        px = self._safe_float(px_exec.get(code, np.nan))
        if not np.isfinite(px) or px <= 0:
            return cash, 0.0

        lot = max(1, int(board_lot))
        target_lot = self._round_to_lot(target_shares, lot)

        cur = float(shares.get(code, 0.0))
        delta = float(target_lot - cur)
        if abs(delta) < 1e-12:
            return cash, 0.0

        commission_rate = float(commission_rate)
        slippage = float(slippage)

        if delta > 0:
            px_trade = px * (1.0 + slippage)
            notional = delta * px_trade
            fee = notional * commission_rate
            cash -= (notional + fee)
            action = "BUY" if cur >= 0 else "COVER"
        else:
            qty = abs(delta)
            px_trade = px * (1.0 - slippage)
            notional = qty * px_trade
            fee = notional * commission_rate
            cash += (notional - fee)
            action = "SELL" if cur > 0 else "SHORT"

        new_pos = cur + delta
        shares[code] = float(new_pos)

        if abs(new_pos) <= 1e-12:
            entry_price[code] = float("nan")
        elif abs(cur) <= 1e-12 or (np.sign(cur) != np.sign(new_pos)):
            entry_price[code] = float(px)

        trades.append(
            {
                "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "date": str(dt.date()),
                "code": code,
                "action": action,
                "delta_shares": float(delta),
                "target_shares": float(target_lot),
                "board_lot": int(lot),
                "price": float(px_trade),
                "notional": float(notional),
                "fee": float(fee),
                "reason": reason,
            }
        )
        return cash, float(fee)

    # -------- data/calendar helpers --------

    def _latest_hk_trading_day(self, as_of: date) -> Optional[date]:
        start = as_of - timedelta(days=40)
        idx = self._hk_trading_days(start=start, end=as_of)
        if idx.empty:
            return None
        return idx[-1].date()

    def _hk_trading_days(self, start: date, end: date) -> pd.DatetimeIndex:
        if start > end:
            return pd.DatetimeIndex([])
        try:
            fetch_end = end if start < end else (end + timedelta(days=1))
            df = self.dm.fetch_index_data("^HSI", start=str(start), end=str(fetch_end))
            idx = pd.DatetimeIndex(pd.to_datetime(df.index).tz_localize(None)).sort_values().unique()
            idx = idx[(idx >= pd.Timestamp(start)) & (idx <= pd.Timestamp(end))]
            return pd.DatetimeIndex(idx)
        except Exception as exc:
            self.logger(f"[WARN] 读取 ^HSI 交易日失败: {exc}")
            return pd.DatetimeIndex([])

    def _load_aligned_prices(
        self,
        codes: Sequence[str],
        start: date,
        end: date,
        calendar: pd.DatetimeIndex,
    ) -> Dict[str, pd.DataFrame]:
        out: Dict[str, pd.DataFrame] = {}
        for code in codes:
            df = self.dm.fetch_stock_data(code, start=str(start), end=str(end))
            if df.empty:
                raise PaperTradeError(f"{code} 无法获取价格数据")
            prepared = self.dm.prepare_for_calendar(df=df, calendar=calendar, max_suspend_days=30)
            out[code] = prepared.aligned
        return out

    def _get_rebalance_dates(self, calendar: pd.DatetimeIndex, frequency: str, day: int) -> Set[pd.Timestamp]:
        if calendar.empty:
            return set()
        freq = str(frequency or "monthly").lower()
        nth = max(1, int(day or 1))
        s = pd.Series(calendar, index=calendar)

        if freq == "daily":
            return set(calendar)

        if freq == "weekly":
            groups = s.groupby([calendar.isocalendar().year, calendar.isocalendar().week])
        elif freq == "quarterly":
            groups = s.groupby([calendar.year, calendar.quarter])
        else:
            groups = s.groupby([calendar.year, calendar.month])

        out: Set[pd.Timestamp] = set()
        for _, g in groups:
            idx = min(nth - 1, len(g) - 1)
            out.add(pd.Timestamp(g.iloc[idx]))
        return out

    # -------- filesystem/serialization helpers --------

    def _strategy_codes(self, cfg: StrategyConfig) -> List[str]:
        codes = [p.code for p in cfg.long_positions] + [p.code for p in cfg.short_positions]
        return sorted(set([str(c).strip().upper() for c in codes if str(c).strip()]))

    def _load_strategy_with_overrides(
        self,
        config_path: Path,
        universe_path: Path,
    ) -> Tuple[StrategyConfig, UniverseConfig, Dict[str, int]]:
        universe = load_universe(universe_path)
        cfg = load_strategy(config_path, universe)

        board_lots: Dict[str, int] = {}
        if yaml is not None:
            try:
                raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
                if isinstance(raw, dict):
                    # 支持两种位置: top-level board_lots / paper_trade.board_lots
                    top = raw.get("board_lots", {})
                    if isinstance(top, dict):
                        for k, v in top.items():
                            try:
                                board_lots[str(k).strip().upper()] = max(1, int(v))
                            except Exception:
                                pass
                    pt = raw.get("paper_trade", {})
                    if isinstance(pt, dict):
                        b2 = pt.get("board_lots", {})
                        if isinstance(b2, dict):
                            for k, v in b2.items():
                                try:
                                    board_lots[str(k).strip().upper()] = max(1, int(v))
                                except Exception:
                                    pass
            except Exception:
                pass

        return cfg, universe, board_lots

    def _ensure_active_registry(self) -> None:
        if not self.active_path.exists():
            self.active_path.write_text("[]\n", encoding="utf-8")

    def _ensure_dashboard_stub(self) -> None:
        if not self.dashboard_path.exists():
            self.dashboard_path.write_text("<html><body><h3>暂无模拟盘</h3></body></html>\n", encoding="utf-8")

    def _load_active(self) -> List[Dict[str, Any]]:
        try:
            obj = json.loads(self.active_path.read_text(encoding="utf-8"))
            if isinstance(obj, list):
                out = [x for x in obj if isinstance(x, dict)]
                return out
        except Exception:
            pass
        return []

    def _save_active(self, rows: List[Dict[str, Any]]) -> None:
        self.active_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")

    def _upsert_active(self, row: Dict[str, Any]) -> None:
        rid = str(row.get("run_id", "")).strip()
        if not rid:
            return
        rows = self._load_active()
        found = False
        for i, x in enumerate(rows):
            if str(x.get("run_id", "")).strip() == rid:
                rows[i] = row
                found = True
                break
        if not found:
            rows.append(row)
        rows.sort(key=lambda x: (str(x.get("last_update_date", "")), str(x.get("run_id", ""))), reverse=True)
        self._save_active(rows)

    def _run_matches_strategy(
        self,
        *,
        state_path: Path,
        target_config_path: str,
        target_strategy_name: str,
        fallback_row: Optional[Dict[str, Any]],
    ) -> bool:
        state: Dict[str, Any] = {}
        try:
            if state_path.exists():
                state = self._read_json(state_path)
        except Exception:
            state = {}

        config_path_text = str(state.get("config_path", "")).strip()
        strategy_name = str(state.get("strategy_name", "")).strip()
        if not config_path_text and isinstance(fallback_row, dict):
            strategy_name = strategy_name or str(fallback_row.get("strategy_name", "")).strip()
        if config_path_text:
            try:
                resolved = self._resolve_path(Path(config_path_text))
                config_path_text = str(resolved.resolve()) if resolved.exists() else str(resolved)
            except Exception:
                pass
        if target_config_path and config_path_text and config_path_text == target_config_path:
            return True
        return bool(target_strategy_name and strategy_name and strategy_name == target_strategy_name)

    def _archive_run_dir(self, run_dir: Path, run_id: str) -> Dict[str, Any]:
        if not run_dir.exists():
            return {"run_id": run_id, "archived_dir": "", "missing": True}
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        dst = self.paper_trash_dir / f"{run_dir.name}__{ts}"
        idx = 1
        while dst.exists():
            dst = self.paper_trash_dir / f"{run_dir.name}__{ts}_{idx}"
            idx += 1
        shutil.move(str(run_dir), str(dst))
        return {"run_id": run_id, "archived_dir": str(dst), "missing": False}

    def _restore_run_dir(self, run_dir: Path, run_id: str) -> Dict[str, Any]:
        if not run_dir.exists():
            return {"run_id": run_id, "restored_dir": "", "missing": True}
        base_name = run_dir.name.split("__", 1)[0]
        dst = self.paper_dir / base_name
        idx = 1
        while dst.exists():
            dst = self.paper_dir / f"{base_name}_restored_{idx}"
            idx += 1
        shutil.move(str(run_dir), str(dst))
        return {"run_id": run_id, "restored_dir": str(dst), "missing": False}

    def _safe_read_state_last_date(self, state_path: Path) -> str:
        try:
            obj = self._read_json(state_path)
            return str(obj.get("last_update_date", ""))
        except Exception:
            return ""

    def _load_snapshots(self, path: Path) -> pd.DataFrame:
        if not path.exists():
            return pd.DataFrame()
        try:
            df = pd.read_csv(path)
            if "date" not in df.columns:
                return pd.DataFrame()
            return df.sort_values("date").reset_index(drop=True)
        except Exception:
            return pd.DataFrame()

    def _append_snapshots(self, path: Path, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return
        new_df = pd.DataFrame(rows)
        old_df = self._load_snapshots(path)
        if old_df.empty:
            out = new_df
        else:
            out = pd.concat([old_df, new_df], ignore_index=True)
        out["date"] = out["date"].astype(str)
        out = out.drop_duplicates(subset=["date"], keep="last").sort_values("date").reset_index(drop=True)
        path.parent.mkdir(parents=True, exist_ok=True)
        out.to_csv(path, index=False)

    # -------- utility --------

    def _equity(self, cash: float, shares: Dict[str, float], prices: Dict[str, float]) -> float:
        val = float(cash)
        for code, sh in shares.items():
            px = self._safe_float(prices.get(code, np.nan))
            if np.isfinite(px):
                val += float(sh) * px
        return float(val)

    def _long_short_mv(self, shares: Dict[str, float], prices: Dict[str, float]) -> Tuple[float, float]:
        long_mv = 0.0
        short_mv = 0.0
        for code, sh in shares.items():
            px = self._safe_float(prices.get(code, np.nan))
            if not np.isfinite(px):
                continue
            v = float(sh) * px
            if sh >= 0:
                long_mv += v
            else:
                short_mv += abs(v)
        return float(long_mv), float(short_mv)

    def _round_to_lot(self, target_shares: float, lot: int) -> float:
        q = abs(float(target_shares))
        if q < 1e-12:
            return 0.0
        lots = math.floor(q / float(lot))
        qty = float(lots * lot)
        if qty <= 0:
            return 0.0
        return qty if target_shares >= 0 else -qty

    def _decide_run_id(self, strategy_name: str, run_id: str, as_of: date) -> str:
        custom = str(run_id or "").strip()
        if custom:
            return self._sanitize_name(custom)
        base = self._sanitize_name(strategy_name)
        if not base:
            base = "paper_strategy"
        return f"{base}_{as_of.strftime('%Y%m%d')}"

    def _sanitize_name(self, text: str) -> str:
        t = str(text or "").strip()
        t = re.sub(r"[\\/:*?\"<>|\x00-\x1f]", "_", t)
        t = re.sub(r"\s+", "", t)
        t = t.strip("._")
        return t or "unnamed"

    def _parse_day(self, value: Any) -> date:
        t = str(value).strip().lower()
        if t == "today" or not t:
            return date.today()
        return pd.Timestamp(t).date()

    def _resolve_path(self, p: Any) -> Path:
        path = Path(str(p))
        return path if path.is_absolute() else (self.base_dir / path)

    def _safe_float(self, x: Any) -> float:
        try:
            return float(x)
        except Exception:
            return float("nan")

    def _json_float_map(self, d: Dict[str, Any]) -> Dict[str, Optional[float]]:
        out: Dict[str, Optional[float]] = {}
        for k, v in d.items():
            f = self._safe_float(v)
            out[str(k)] = float(f) if np.isfinite(f) else None
        return out

    def _parse_float_map(self, obj: Any, keys: Iterable[str]) -> Dict[str, float]:
        src = obj if isinstance(obj, dict) else {}
        out: Dict[str, float] = {}
        for k in keys:
            v = src.get(k, np.nan)
            out[k] = self._safe_float(v)
        return out

    def _write_json(self, path: Path, obj: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

    def _read_json(self, path: Path) -> Any:
        return json.loads(path.read_text(encoding="utf-8"))

    def _esc(self, text: Any) -> str:
        s = str(text)
        return (
            s.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
        )

    def _now_iso(self) -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
