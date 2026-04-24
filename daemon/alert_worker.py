from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
import requests

try:
    import yaml
except Exception as exc:  # pragma: no cover
    yaml = None  # type: ignore[assignment]
    _YAML_IMPORT_ERROR = exc
else:
    _YAML_IMPORT_ERROR = None

try:
    from apscheduler.schedulers.blocking import BlockingScheduler
    from apscheduler.triggers.cron import CronTrigger
except Exception as exc:  # pragma: no cover
    BlockingScheduler = None  # type: ignore[assignment]
    CronTrigger = None  # type: ignore[assignment]
    _APS_IMPORT_ERROR = exc
else:
    _APS_IMPORT_ERROR = None

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TRADING_DIR = PROJECT_ROOT / "apps" / "trading"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(TRADING_DIR) not in sys.path:
    sys.path.insert(0, str(TRADING_DIR))

from fast_engine import (  # noqa: E402
    fetch_intraday_flow,
    fetch_multi_timeframe_indicators,
    fetch_realtime_quotes_batch,
)
from slow_engine import get_stock_pool  # noqa: E402
from shared.db_manager import list_positions  # noqa: E402

CONFIG_PATH = PROJECT_ROOT / "config" / "alert_rules.yaml"
STATE_PATH = PROJECT_ROOT / "data" / "alert_worker_state.json"
LOCAL_TZ = "Asia/Shanghai"
DEFAULT_TRADING_WINDOWS = (
    (time(9, 30), time(11, 30)),
    (time(13, 0), time(15, 0)),
)

LOGGER = logging.getLogger("alert_worker")


@dataclass
class SymbolContext:
    symbol: str
    name: str
    group: str


@dataclass
class AlertEvent:
    symbol: str
    name: str
    group: str
    rule_id: str
    title: str
    message: str
    fingerprint: str


def _ensure_dependencies() -> None:
    if yaml is None:
        raise RuntimeError(f"PyYAML 不可用: {_YAML_IMPORT_ERROR}")


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if text in {"", "-", "--", "None", "nan", "NaN"}:
        return None
    try:
        return float(text)
    except Exception:
        return None


def _normalize_symbol(value: Any) -> str:
    raw = str(value or "").strip()
    digits = "".join(ch for ch in raw if ch.isdigit())
    if raw.lower().startswith("hk"):
        return digits[-5:].zfill(5) if digits else ""
    if len(digits) == 5:
        return digits.zfill(5)
    if len(digits) >= 6:
        return digits[-6:].zfill(6)
    return raw


def _load_yaml_config(path: Path = CONFIG_PATH) -> Dict[str, Any]:
    _ensure_dependencies()
    if not path.exists():
        raise FileNotFoundError(f"告警配置不存在: {path}")
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError("alert_rules.yaml 顶层必须是对象")
    return data


def _load_state(path: Path = STATE_PATH) -> Dict[str, Any]:
    if not path.exists():
        return {"sent": {}, "rule_state": {}}
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            data.setdefault("sent", {})
            data.setdefault("rule_state", {})
            return data
    except Exception:
        pass
    return {"sent": {}, "rule_state": {}}


def _save_state(state: Dict[str, Any], path: Path = STATE_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def _parse_trading_windows(config: Dict[str, Any]) -> Tuple[Tuple[time, time], ...]:
    schedule_cfg = (config.get("schedule") or {}) if isinstance(config.get("schedule"), dict) else {}
    raw_windows = schedule_cfg.get("trading_windows") or []
    parsed: List[Tuple[time, time]] = []
    for item in raw_windows:
        text = str(item or "").strip()
        if not text or "-" not in text:
            continue
        start_text, end_text = [part.strip() for part in text.split("-", 1)]
        try:
            start_dt = datetime.strptime(start_text, "%H:%M").time()
            end_dt = datetime.strptime(end_text, "%H:%M").time()
        except ValueError:
            continue
        parsed.append((start_dt, end_dt))
    return tuple(parsed) if parsed else DEFAULT_TRADING_WINDOWS


def _config_timezone(config: Dict[str, Any]) -> str:
    schedule_cfg = (config.get("schedule") or {}) if isinstance(config.get("schedule"), dict) else {}
    return str(schedule_cfg.get("timezone") or LOCAL_TZ)


def _is_trading_time(config: Dict[str, Any], now: Optional[datetime] = None) -> bool:
    tz_name = _config_timezone(config)
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo(LOCAL_TZ)
    current = now or datetime.now(tz)
    if current.weekday() >= 5:
        return False
    t = current.time()
    for start, end in _parse_trading_windows(config):
        if start <= t <= end:
            return True
    return False


def _build_trading_cron_specs(
    windows: Sequence[Tuple[time, time]],
    interval_minutes: int,
) -> List[Tuple[str, str]]:
    """Convert trading windows into compact APScheduler cron specs."""
    specs: List[Tuple[str, str]] = []
    anchor = datetime(2000, 1, 3)
    for start, end in windows:
        current = datetime.combine(anchor.date(), start)
        end_dt = datetime.combine(anchor.date(), end)
        minutes_by_hour: Dict[int, List[int]] = {}
        while current <= end_dt:
            minutes_by_hour.setdefault(current.hour, []).append(current.minute)
            current += timedelta(minutes=interval_minutes)
        for hour, minutes in sorted(minutes_by_hour.items()):
            minute_spec = ",".join(str(item) for item in sorted(set(minutes)))
            specs.append((str(hour), minute_spec))
    return specs


def _resolve_symbol_contexts() -> List[SymbolContext]:
    seen: set[str] = set()
    out: List[SymbolContext] = []
    for group in ("holding", "watch"):
        for code, name in get_stock_pool(group):
            symbol = _normalize_symbol(code)
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            out.append(SymbolContext(symbol=symbol, name=str(name or symbol), group=group))
    return out


async def _fetch_symbol_snapshot(symbol: str, quote: Dict[str, Any]) -> Dict[str, Any]:
    intraday_task = asyncio.to_thread(fetch_intraday_flow, symbol)
    intraday_df = await intraday_task
    indicators = await asyncio.to_thread(fetch_multi_timeframe_indicators, symbol, intraday_df)
    return {
        "quote": quote or {},
        "intraday": intraday_df if isinstance(intraday_df, pd.DataFrame) else pd.DataFrame(),
        "indicators": indicators if isinstance(indicators, dict) else {},
    }


async def _fetch_market_contexts(contexts: Sequence[SymbolContext]) -> Dict[str, Dict[str, Any]]:
    symbols = [item.symbol for item in contexts]
    quote_map = await asyncio.to_thread(fetch_realtime_quotes_batch, symbols, 1.5)
    tasks = [
        _fetch_symbol_snapshot(item.symbol, quote_map.get(item.symbol, {}))
        for item in contexts
    ]
    raw_results = await asyncio.gather(*tasks, return_exceptions=True)
    out: Dict[str, Dict[str, Any]] = {}
    for item, result in zip(contexts, raw_results):
        if isinstance(result, Exception):
            out[item.symbol] = {"error": str(result), "quote": quote_map.get(item.symbol, {})}
        else:
            out[item.symbol] = result
    return out


def _state_key(symbol: str, rule_id: str) -> str:
    return f"{symbol}:{rule_id}"


def _symbol_rule_config(config: Dict[str, Any], symbol: str) -> Dict[str, Any]:
    symbol_map = config.get("symbols", {}) or {}
    if not isinstance(symbol_map, dict):
        return {}
    one = symbol_map.get(symbol) or symbol_map.get(str(symbol).zfill(6)) or symbol_map.get(str(symbol).zfill(5))
    return one if isinstance(one, dict) else {}


def _mean_recent_volume_lot(intraday_df: pd.DataFrame, lookback: int) -> Optional[float]:
    if intraday_df is None or intraday_df.empty or "volume_lot" not in intraday_df.columns:
        return None
    volume = pd.to_numeric(intraday_df["volume_lot"], errors="coerce").dropna()
    if volume.empty:
        return None
    tail = volume.tail(max(1, int(lookback)))
    if tail.empty:
        return None
    return _to_float(tail.mean())


def _format_price(value: Optional[float]) -> str:
    if value is None:
        return "-"
    return f"{value:.2f}"


def _load_positions_map() -> Dict[str, Dict[str, Any]]:
    try:
        df = list_positions()
    except Exception as exc:
        LOGGER.warning("读取持仓失败: %s", exc)
        return {}
    if df is None or df.empty:
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for row in df.to_dict("records"):
        symbol = _normalize_symbol(row.get("code"))
        if not symbol:
            continue
        out[symbol] = row
    return out


def _build_price_break_events(
    ctx: SymbolContext,
    market_data: Dict[str, Any],
    config: Dict[str, Any],
    state: Dict[str, Any],
) -> List[AlertEvent]:
    rule_cfg = (config.get("price_break") or {}) if isinstance(config.get("price_break"), dict) else {}
    if not rule_cfg.get("enabled", True):
        return []

    per_symbol = _symbol_rule_config(config, ctx.symbol)
    threshold = _to_float((per_symbol.get("psychological_price") if isinstance(per_symbol, dict) else None))
    if threshold is None:
        threshold = _to_float(rule_cfg.get("default_psychological_price"))
    if threshold is None or threshold <= 0:
        return []

    price = _to_float((market_data.get("quote") or {}).get("current_price"))
    if price is None:
        return []

    relation = "equal"
    if price > threshold:
        relation = "above"
    elif price < threshold:
        relation = "below"

    key = _state_key(ctx.symbol, "price_break")
    prior = str((state.get("rule_state") or {}).get(key, "")).strip()
    state.setdefault("rule_state", {})[key] = relation
    if prior in {"", relation, "equal"} or relation == "equal":
        return []

    direction = "向上突破" if relation == "above" else "向下跌破"
    message = (
        f"- 标的: `{ctx.symbol}` {ctx.name}\n"
        f"- 分组: `{ctx.group}`\n"
        f"- 当前价: `{_format_price(price)}`\n"
        f"- 心理价位: `{_format_price(threshold)}`\n"
        f"- 动作: `{direction}`"
    )
    return [
        AlertEvent(
            symbol=ctx.symbol,
            name=ctx.name,
            group=ctx.group,
            rule_id="A",
            title=f"规则A 价格{direction}",
            message=message,
            fingerprint=f"{ctx.symbol}:A:{relation}:{threshold}",
        )
    ]


def _build_order_book_events(
    ctx: SymbolContext,
    market_data: Dict[str, Any],
    config: Dict[str, Any],
    state: Dict[str, Any],
) -> List[AlertEvent]:
    rule_cfg = (config.get("order_book_dump") or {}) if isinstance(config.get("order_book_dump"), dict) else {}
    if not rule_cfg.get("enabled", True):
        return []

    threshold_lot = _to_float(rule_cfg.get("ask_volume_threshold_lot"))
    volume_multiplier = _to_float(rule_cfg.get("recent_volume_multiplier")) or 1.0
    recent_lookback = int(rule_cfg.get("recent_lookback_bars", 20) or 20)
    if threshold_lot is None or threshold_lot <= 0:
        return []

    quote = market_data.get("quote") or {}
    order_book = quote.get("order_book_5") or {}
    asks = order_book.get("sell") or []
    if not asks:
        return []

    candidate = None
    for row in asks:
        vol = _to_float(row.get("volume_lot"))
        if vol is None:
            continue
        if candidate is None or vol > _to_float(candidate.get("volume_lot") or 0):
            candidate = row
    if candidate is None:
        return []

    ask_lot = _to_float(candidate.get("volume_lot"))
    ask_price = _to_float(candidate.get("price"))
    avg_minute_lot = _mean_recent_volume_lot(market_data.get("intraday"), recent_lookback)
    volume_ratio = None
    if ask_lot is not None and avg_minute_lot is not None and avg_minute_lot > 0:
        volume_ratio = ask_lot / avg_minute_lot

    triggered = bool(
        ask_lot is not None
        and ask_lot >= threshold_lot
        and volume_ratio is not None
        and volume_ratio >= volume_multiplier
    )

    key = _state_key(ctx.symbol, "order_book_dump")
    prior = bool((state.get("rule_state") or {}).get(key, False))
    state.setdefault("rule_state", {})[key] = triggered
    if not triggered or prior:
        return []

    message = (
        f"- 标的: `{ctx.symbol}` {ctx.name}\n"
        f"- 分组: `{ctx.group}`\n"
        f"- 卖盘档位: `卖{candidate.get('level', '?')}`\n"
        f"- 挂单价格: `{_format_price(ask_price)}`\n"
        f"- 当前卖单量: `{int(ask_lot or 0):,} 手`\n"
        f"- 近{recent_lookback}根均量: `{_format_price(avg_minute_lot)}` 手\n"
        f"- 放大倍数: `{_format_price(volume_ratio)}` 倍"
    )
    return [
        AlertEvent(
            symbol=ctx.symbol,
            name=ctx.name,
            group=ctx.group,
            rule_id="B",
            title="规则B 盘口出现大卖单",
            message=message,
            fingerprint=f"{ctx.symbol}:B:{candidate.get('level')}:{int(ask_lot or 0)}",
        )
    ]


def _build_technical_events(
    ctx: SymbolContext,
    market_data: Dict[str, Any],
    config: Dict[str, Any],
    state: Dict[str, Any],
) -> List[AlertEvent]:
    rule_cfg = (config.get("technical_extreme") or {}) if isinstance(config.get("technical_extreme"), dict) else {}
    if not rule_cfg.get("enabled", True):
        return []

    oversold = _to_float(rule_cfg.get("oversold_rsi")) or 25.0
    overbought = _to_float(rule_cfg.get("overbought_rsi")) or 75.0

    indicators = market_data.get("indicators") or {}
    day_rsi = _to_float(((indicators.get("day") or {}).get("rsi6")))
    intra_rsi = _to_float(((indicators.get("intraday") or {}).get("rsi6")))
    quote = market_data.get("quote") or {}
    current_price = _to_float(quote.get("current_price"))

    status = "neutral"
    reason = ""
    if intra_rsi is not None and intra_rsi <= oversold:
        status = "oversold"
        reason = f"分时 RSI6={intra_rsi:.2f}"
    elif day_rsi is not None and day_rsi <= oversold:
        status = "oversold"
        reason = f"日线 RSI6={day_rsi:.2f}"
    elif intra_rsi is not None and intra_rsi >= overbought:
        status = "overbought"
        reason = f"分时 RSI6={intra_rsi:.2f}"
    elif day_rsi is not None and day_rsi >= overbought:
        status = "overbought"
        reason = f"日线 RSI6={day_rsi:.2f}"

    key = _state_key(ctx.symbol, "technical_extreme")
    prior = str((state.get("rule_state") or {}).get(key, "neutral"))
    state.setdefault("rule_state", {})[key] = status
    if status == "neutral" or prior == status:
        return []

    direction = "超卖警惕" if status == "oversold" else "超买警惕"
    message = (
        f"- 标的: `{ctx.symbol}` {ctx.name}\n"
        f"- 分组: `{ctx.group}`\n"
        f"- 当前价: `{_format_price(current_price)}`\n"
        f"- 触发依据: `{reason or '-'}`\n"
        f"- 日线 RSI6: `{_format_price(day_rsi)}`\n"
        f"- 分时 RSI6: `{_format_price(intra_rsi)}`"
    )
    return [
        AlertEvent(
            symbol=ctx.symbol,
            name=ctx.name,
            group=ctx.group,
            rule_id="C",
            title=f"规则C 技术指标{direction}",
            message=message,
            fingerprint=f"{ctx.symbol}:C:{status}",
        )
    ]


def _build_position_risk_events(
    ctx: SymbolContext,
    market_data: Dict[str, Any],
    config: Dict[str, Any],
    state: Dict[str, Any],
    positions_map: Dict[str, Dict[str, Any]],
) -> List[AlertEvent]:
    rule_cfg = (config.get("position_risk") or {}) if isinstance(config.get("position_risk"), dict) else {}
    if not rule_cfg.get("enabled", True):
        return []

    position = positions_map.get(ctx.symbol)
    if not position:
        return []

    price = _to_float((market_data.get("quote") or {}).get("current_price"))
    if price is None:
        return []

    stop_loss = _to_float(position.get("stop_loss"))
    take_profit = _to_float(position.get("take_profit"))
    near_stop_pct = max(0.0, _to_float(rule_cfg.get("warn_near_stop_pct")) or 0.0)
    near_take_pct = max(0.0, _to_float(rule_cfg.get("warn_near_take_pct")) or 0.0)
    status = "neutral"
    title = ""
    fingerprint = ""
    if stop_loss is not None and stop_loss > 0 and price <= stop_loss:
        status = "stop_loss"
        title = "规则D 持仓触发止损"
        fingerprint = f"{ctx.symbol}:D:stop_loss:{stop_loss}"
    elif take_profit is not None and take_profit > 0 and price >= take_profit:
        status = "take_profit"
        title = "规则D 持仓触发止盈"
        fingerprint = f"{ctx.symbol}:D:take_profit:{take_profit}"
    elif stop_loss is not None and stop_loss > 0 and near_stop_pct > 0:
        near_stop_threshold = stop_loss * (1.0 + near_stop_pct / 100.0)
        if stop_loss < price <= near_stop_threshold:
            status = "near_stop_loss"
            title = "规则D 持仓接近止损"
            fingerprint = f"{ctx.symbol}:D:near_stop_loss:{stop_loss}:{near_stop_pct}"
    elif take_profit is not None and take_profit > 0 and near_take_pct > 0:
        near_take_threshold = take_profit * (1.0 - near_take_pct / 100.0)
        if near_take_threshold <= price < take_profit:
            status = "near_take_profit"
            title = "规则D 持仓接近止盈"
            fingerprint = f"{ctx.symbol}:D:near_take_profit:{take_profit}:{near_take_pct}"

    key = _state_key(ctx.symbol, "position_risk")
    prior = str((state.get("rule_state") or {}).get(key, "neutral"))
    state.setdefault("rule_state", {})[key] = status
    if status == "neutral" or prior == status:
        return []

    avg_cost = _to_float(position.get("avg_cost"))
    quantity = int(_to_float(position.get("quantity")) or 0)
    stop_gap_pct = ((price / stop_loss - 1.0) * 100.0) if stop_loss and stop_loss > 0 else None
    take_gap_pct = ((take_profit / price - 1.0) * 100.0) if take_profit and price > 0 else None
    message = (
        f"- 标的: `{ctx.symbol}` {ctx.name}\n"
        f"- 当前价: `{_format_price(price)}`\n"
        f"- 持仓成本: `{_format_price(avg_cost)}`\n"
        f"- 持仓数量: `{quantity:,}`\n"
        f"- 止损价: `{_format_price(stop_loss)}`\n"
        f"- 止盈价: `{_format_price(take_profit)}`\n"
        f"- 距离止损: `{_format_price(stop_gap_pct)}%`\n"
        f"- 距离止盈: `{_format_price(take_gap_pct)}%`"
    )
    return [
        AlertEvent(
            symbol=ctx.symbol,
            name=ctx.name,
            group=ctx.group,
            rule_id="D",
            title=title,
            message=message,
            fingerprint=fingerprint,
        )
    ]


def _build_intraday_change_events(
    ctx: SymbolContext,
    market_data: Dict[str, Any],
    config: Dict[str, Any],
    state: Dict[str, Any],
) -> List[AlertEvent]:
    rule_cfg = (config.get("intraday_change") or {}) if isinstance(config.get("intraday_change"), dict) else {}
    if not rule_cfg.get("enabled", True):
        return []

    lookback = max(1, int(rule_cfg.get("lookback_bars", 5) or 5))
    up_pct = _to_float(rule_cfg.get("up_pct")) or 2.0
    down_pct = abs(_to_float(rule_cfg.get("down_pct")) or 2.0)
    intraday_df = market_data.get("intraday")
    if intraday_df is None or intraday_df.empty or "price" not in intraday_df.columns:
        return []

    price_series = pd.to_numeric(intraday_df["price"], errors="coerce").dropna().reset_index(drop=True)
    if len(price_series) <= lookback:
        return []

    latest_price = _to_float(price_series.iloc[-1])
    base_price = _to_float(price_series.iloc[-(lookback + 1)])
    if latest_price is None or base_price is None or base_price <= 0:
        return []

    change_pct = (latest_price / base_price - 1.0) * 100.0
    status = "neutral"
    direction = ""
    if change_pct >= up_pct:
        status = "up"
        direction = "拉升异动"
    elif change_pct <= -down_pct:
        status = "down"
        direction = "跳水异动"

    key = _state_key(ctx.symbol, "intraday_change")
    prior = str((state.get("rule_state") or {}).get(key, "neutral"))
    state.setdefault("rule_state", {})[key] = status
    if status == "neutral" or prior == status:
        return []

    message = (
        f"- 标的: `{ctx.symbol}` {ctx.name}\n"
        f"- 分组: `{ctx.group}`\n"
        f"- 当前价: `{_format_price(latest_price)}`\n"
        f"- {lookback}根前价格: `{_format_price(base_price)}`\n"
        f"- {lookback}根涨跌幅: `{change_pct:.2f}%`"
    )
    return [
        AlertEvent(
            symbol=ctx.symbol,
            name=ctx.name,
            group=ctx.group,
            rule_id="E",
            title=f"规则E {lookback}根分时{direction}",
            message=message,
            fingerprint=f"{ctx.symbol}:E:{status}:{lookback}",
        )
    ]


def _build_breakout_volume_events(
    ctx: SymbolContext,
    market_data: Dict[str, Any],
    config: Dict[str, Any],
    state: Dict[str, Any],
) -> List[AlertEvent]:
    rule_cfg = (config.get("breakout_volume") or {}) if isinstance(config.get("breakout_volume"), dict) else {}
    if not rule_cfg.get("enabled", True):
        return []

    lookback = max(3, int(rule_cfg.get("lookback_bars", 20) or 20))
    volume_multiplier = _to_float(rule_cfg.get("volume_multiplier")) or 3.0
    buffer_pct = _to_float(rule_cfg.get("breakout_buffer_pct")) or 0.0
    intraday_df = market_data.get("intraday")
    if intraday_df is None or intraday_df.empty:
        return []
    required = {"price", "volume_lot"}
    if not required.issubset(intraday_df.columns):
        return []

    frame = intraday_df.copy()
    frame["price"] = pd.to_numeric(frame["price"], errors="coerce")
    frame["volume_lot"] = pd.to_numeric(frame["volume_lot"], errors="coerce")
    frame = frame.dropna(subset=["price", "volume_lot"]).reset_index(drop=True)
    if len(frame) <= lookback:
        return []

    recent = frame.tail(lookback + 1).reset_index(drop=True)
    latest = recent.iloc[-1]
    prior = recent.iloc[:-1]
    if prior.empty:
        return []

    latest_price = _to_float(latest.get("price"))
    latest_volume = _to_float(latest.get("volume_lot"))
    recent_high = _to_float(prior["price"].max())
    recent_low = _to_float(prior["price"].min())
    avg_volume = _to_float(prior["volume_lot"].mean())
    if latest_price is None or latest_volume is None or avg_volume is None or avg_volume <= 0:
        return []

    volume_ratio = latest_volume / avg_volume
    high_threshold = recent_high * (1.0 + buffer_pct / 100.0) if recent_high is not None else None
    low_threshold = recent_low * (1.0 - buffer_pct / 100.0) if recent_low is not None else None

    status = "neutral"
    direction = ""
    if high_threshold is not None and latest_price > high_threshold and volume_ratio >= volume_multiplier:
        status = "up"
        direction = "放量突破"
    elif low_threshold is not None and latest_price < low_threshold and volume_ratio >= volume_multiplier:
        status = "down"
        direction = "放量下破"

    key = _state_key(ctx.symbol, "breakout_volume")
    prior_status = str((state.get("rule_state") or {}).get(key, "neutral"))
    state.setdefault("rule_state", {})[key] = status
    if status == "neutral" or prior_status == status:
        return []

    message = (
        f"- 标的: `{ctx.symbol}` {ctx.name}\n"
        f"- 分组: `{ctx.group}`\n"
        f"- 当前价: `{_format_price(latest_price)}`\n"
        f"- 区间高点: `{_format_price(recent_high)}`\n"
        f"- 区间低点: `{_format_price(recent_low)}`\n"
        f"- 最新分时量: `{int(latest_volume):,} 手`\n"
        f"- 区间均量: `{_format_price(avg_volume)}` 手\n"
        f"- 放量倍数: `{_format_price(volume_ratio)}` 倍"
    )
    return [
        AlertEvent(
            symbol=ctx.symbol,
            name=ctx.name,
            group=ctx.group,
            rule_id="F",
            title=f"规则F {direction}",
            message=message,
            fingerprint=f"{ctx.symbol}:F:{status}:{lookback}",
        )
    ]


def _apply_rule_cooldown(events: Iterable[AlertEvent], config: Dict[str, Any], state: Dict[str, Any]) -> List[AlertEvent]:
    push_cfg = (config.get("push") or {}) if isinstance(config.get("push"), dict) else {}
    cooldown_minutes = int(push_cfg.get("cooldown_minutes", 30) or 30)
    now = datetime.now().timestamp()
    sent_map = state.setdefault("sent", {})
    kept: List[AlertEvent] = []
    for event in events:
        last_sent = _to_float(sent_map.get(event.fingerprint))
        if last_sent is not None and (now - last_sent) < cooldown_minutes * 60:
            continue
        sent_map[event.fingerprint] = now
        kept.append(event)
    return kept


def _build_alerts_for_symbol(
    ctx: SymbolContext,
    market_data: Dict[str, Any],
    config: Dict[str, Any],
    state: Dict[str, Any],
    positions_map: Dict[str, Dict[str, Any]],
) -> List[AlertEvent]:
    if market_data.get("error"):
        LOGGER.warning("%s %s 数据抓取失败: %s", ctx.group, ctx.symbol, market_data.get("error"))
        return []

    events: List[AlertEvent] = []
    events.extend(_build_price_break_events(ctx, market_data, config, state))
    events.extend(_build_order_book_events(ctx, market_data, config, state))
    events.extend(_build_technical_events(ctx, market_data, config, state))
    events.extend(_build_position_risk_events(ctx, market_data, config, state, positions_map))
    events.extend(_build_intraday_change_events(ctx, market_data, config, state))
    events.extend(_build_breakout_volume_events(ctx, market_data, config, state))
    return events


def _env_value(env_name: Optional[str]) -> Optional[str]:
    if not env_name:
        return None
    value = os.getenv(str(env_name).strip())
    return str(value).strip() if value else None


def _escape_markdown_v2(text: str) -> str:
    special = r"_*[]()~`>#+-=|{}.!"
    out = []
    for ch in str(text):
        if ch in special:
            out.append("\\" + ch)
        else:
            out.append(ch)
    return "".join(out)


def _render_telegram_message(events: Sequence[AlertEvent]) -> str:
    parts = ["*Quant Alert Worker*"]
    for event in events:
        parts.append(
            "\n".join(
                [
                    "",
                    f"*{_escape_markdown_v2(event.title)}*",
                    _escape_markdown_v2(event.message),
                ]
            )
        )
    return "\n".join(parts)


def _render_markdown_card(events: Sequence[AlertEvent]) -> Tuple[str, str]:
    title = f"Quant Alerts x {len(events)}"
    lines = [f"# {title}", ""]
    for event in events:
        lines.append(f"## {event.title}")
        lines.append(event.message)
        lines.append("")
    return title, "\n".join(lines).strip()


def _send_to_telegram(channel_cfg: Dict[str, Any], events: Sequence[AlertEvent], timeout_sec: float) -> None:
    token = _env_value(channel_cfg.get("bot_token_env"))
    chat_id = _env_value(channel_cfg.get("chat_id_env"))
    if not token or not chat_id:
        raise RuntimeError("Telegram 配置缺少 bot token 或 chat id 环境变量")
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": _render_telegram_message(events),
        "parse_mode": "MarkdownV2",
        "disable_web_page_preview": True,
    }
    resp = requests.post(url, json=payload, timeout=timeout_sec)
    resp.raise_for_status()


def _send_to_pushplus(channel_cfg: Dict[str, Any], events: Sequence[AlertEvent], timeout_sec: float) -> None:
    token = _env_value(channel_cfg.get("token_env"))
    if not token:
        raise RuntimeError("PushPlus 配置缺少 token 环境变量")
    title, body = _render_markdown_card(events)
    payload = {
        "token": token,
        "title": title,
        "content": body,
        "template": "markdown",
    }
    resp = requests.post("http://www.pushplus.plus/send", json=payload, timeout=timeout_sec)
    resp.raise_for_status()


def _send_to_serverchan(channel_cfg: Dict[str, Any], events: Sequence[AlertEvent], timeout_sec: float) -> None:
    sendkey = _env_value(channel_cfg.get("sendkey_env"))
    if not sendkey:
        raise RuntimeError("ServerChan 配置缺少 sendkey 环境变量")
    title, body = _render_markdown_card(events)
    url = f"https://sctapi.ftqq.com/{sendkey}.send"
    resp = requests.post(url, data={"title": title, "desp": body}, timeout=timeout_sec)
    resp.raise_for_status()


def send_alerts(events: Sequence[AlertEvent], config: Dict[str, Any], dry_run: bool = False) -> None:
    if not events:
        return
    push_cfg = (config.get("push") or {}) if isinstance(config.get("push"), dict) else {}
    channels = push_cfg.get("channels") or ["telegram"]
    if not isinstance(channels, list):
        channels = [str(channels)]
    timeout_sec = _to_float(push_cfg.get("request_timeout_sec")) or 8.0

    if dry_run:
        title, body = _render_markdown_card(events)
        LOGGER.info("[DRY RUN] %s\n%s", title, body)
        return

    for channel in channels:
        channel_name = str(channel).strip().lower()
        if channel_name == "telegram":
            _send_to_telegram(push_cfg.get("telegram") or {}, events, timeout_sec)
        elif channel_name == "pushplus":
            _send_to_pushplus(push_cfg.get("pushplus") or {}, events, timeout_sec)
        elif channel_name == "serverchan":
            _send_to_serverchan(push_cfg.get("serverchan") or {}, events, timeout_sec)
        else:
            raise ValueError(f"不支持的推送通道: {channel_name}")


def _build_test_events(config: Dict[str, Any]) -> List[AlertEvent]:
    contexts = _resolve_symbol_contexts()
    if contexts:
        ctx = contexts[0]
    else:
        ctx = SymbolContext(symbol="000000", name="测试标的", group="watch")
    symbol_cfg = _symbol_rule_config(config, ctx.symbol)
    threshold = _to_float((symbol_cfg or {}).get("psychological_price")) or 0.0
    title = "规则A 价格向上突破（测试）"
    message = (
        f"- 标的: `{ctx.symbol}` {ctx.name}\n"
        f"- 分组: `{ctx.group}`\n"
        f"- 测试类型: `synthetic`\n"
        f"- 心理价位: `{_format_price(threshold)}`\n"
        f"- 说明: `这是一条人工触发的后台告警联调消息`"
    )
    return [
        AlertEvent(
            symbol=ctx.symbol,
            name=ctx.name,
            group=ctx.group,
            rule_id="TEST",
            title=title,
            message=message,
            fingerprint=f"{ctx.symbol}:TEST:{datetime.now().strftime('%Y%m%d%H%M%S')}",
        )
    ]


def run_monitors(*, config_path: Path = CONFIG_PATH, dry_run: bool = False, force_run: bool = False) -> List[AlertEvent]:
    config = _load_yaml_config(config_path)
    if not force_run and not _is_trading_time(config):
        LOGGER.info("当前不在交易时段，跳过扫描")
        return []

    contexts = _resolve_symbol_contexts()
    if not contexts:
        LOGGER.info("股票池为空，跳过扫描")
        return []

    LOGGER.info("开始扫描 %d 只股票", len(contexts))
    market_map = asyncio.run(_fetch_market_contexts(contexts))
    state = _load_state()
    positions_map = _load_positions_map()

    all_events: List[AlertEvent] = []
    for ctx in contexts:
        market_data = market_map.get(ctx.symbol, {})
        all_events.extend(_build_alerts_for_symbol(ctx, market_data, config, state, positions_map))

    filtered = _apply_rule_cooldown(all_events, config, state)
    _save_state(state)
    if filtered:
        send_alerts(filtered, config=config, dry_run=dry_run)
        LOGGER.info("本轮发送 %d 条告警", len(filtered))
    else:
        LOGGER.info("本轮无新增告警")
    return filtered


def send_test_trigger(*, config_path: Path = CONFIG_PATH, dry_run: bool = False) -> List[AlertEvent]:
    config = _load_yaml_config(config_path)
    events = _build_test_events(config)
    send_alerts(events, config=config, dry_run=dry_run)
    LOGGER.info("已发送 %d 条测试告警", len(events))
    return events


def start_scheduler(*, config_path: Path = CONFIG_PATH, dry_run: bool = False) -> None:
    if BlockingScheduler is None or CronTrigger is None:
        raise RuntimeError(f"APScheduler 不可用: {_APS_IMPORT_ERROR}")

    config = _load_yaml_config(config_path)
    schedule_cfg = (config.get("schedule") or {}) if isinstance(config.get("schedule"), dict) else {}
    interval_minutes = int(schedule_cfg.get("interval_minutes", 3) or 3)
    interval_minutes = max(1, interval_minutes)
    timezone = _config_timezone(config)

    scheduler = BlockingScheduler(timezone=timezone, job_defaults={"coalesce": True, "max_instances": 1})
    cron_specs = _build_trading_cron_specs(_parse_trading_windows(config), interval_minutes)
    for idx, (hour_spec, minute_spec) in enumerate(cron_specs, start=1):
        scheduler.add_job(
            run_monitors,
            trigger=CronTrigger(day_of_week="mon-fri", hour=hour_spec, minute=minute_spec),
            kwargs={"config_path": config_path, "dry_run": dry_run, "force_run": False},
            id=f"quant_alert_worker_{idx}",
            replace_existing=True,
        )
    LOGGER.info(
        "alert worker 已启动，按 %s 每 %d 分钟扫描交易窗口: %s",
        timezone,
        interval_minutes,
        ", ".join(f"{start.strftime('%H:%M')}-{end.strftime('%H:%M')}" for start, end in _parse_trading_windows(config)),
    )
    scheduler.start()


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Quant alert worker")
    parser.add_argument("--config", default=str(CONFIG_PATH), help="告警规则配置文件路径")
    parser.add_argument("--once", action="store_true", help="仅执行一轮扫描后退出")
    parser.add_argument("--force-run", action="store_true", help="忽略交易时段检查，直接执行扫描")
    parser.add_argument("--test-trigger", action="store_true", help="发送一条人工构造的测试告警")
    parser.add_argument("--dry-run", action="store_true", help="不真实推送，只打印告警")
    parser.add_argument("--debug", action="store_true", help="输出调试日志")
    return parser


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    config_path = Path(args.config).expanduser().resolve()
    if args.test_trigger:
        send_test_trigger(config_path=config_path, dry_run=args.dry_run)
        return
    if args.once:
        run_monitors(config_path=config_path, dry_run=args.dry_run, force_run=args.force_run)
        return
    start_scheduler(config_path=config_path, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
