from __future__ import annotations

import copy
import json
import os
import re
from datetime import date, datetime
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st
import yaml

OPENAI_AVAILABLE = True
OPENAI_IMPORT_ERROR = None
try:
    from openai import OpenAI
except Exception as _exc:  # pragma: no cover
    OPENAI_AVAILABLE = False
    OPENAI_IMPORT_ERROR = _exc
    OpenAI = None  # type: ignore[assignment]

from apps.trading.slow_engine import get_stock_pool
from shared.db_manager import (
    Position,
    get_positions_overview,
    init_db,
    list_position_flows,
    list_positions,
    remove_position,
    suggest_position_size,
    upsert_position,
)
from shared.ui_shell import render_section_intro, render_status_row

APP_VERSION = "PORT-20260421-03"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
ALERT_RULES_PATH = PROJECT_ROOT / "config" / "alert_rules.yaml"
LOCAL_PREFS_PATH = PROJECT_ROOT / "data" / "local_user_prefs.json"
ALERT_RULE_PARSER_PROMPT = """你是量化风控配置解析器。你的任务是把用户的自然语言偏好，转换成可直接写入 YAML / 持仓表的 JSON 配置草案。

严格要求：
1. 只输出一个 JSON 对象，不要输出 Markdown、解释、代码块。
2. 只允许输出这些顶层键：
   - order_book_dump
   - technical_extreme
   - position_risk
   - intraday_change
   - breakout_volume
   - symbols
   - positions
3. 每个顶层键下只允许这些字段：
   - order_book_dump: enabled, ask_volume_threshold_lot, recent_volume_multiplier, recent_lookback_bars
   - technical_extreme: enabled, oversold_rsi, overbought_rsi
   - position_risk: enabled, warn_near_stop_pct, warn_near_take_pct
   - intraday_change: enabled, lookback_bars, up_pct, down_pct
   - breakout_volume: enabled, lookback_bars, volume_multiplier, breakout_buffer_pct
   - symbols: 以股票代码为 key，对象里只允许 psychological_price
   - positions: 数组；每个元素只允许 code, market, stop_loss, take_profit
4. 如果用户没有提到某个字段，不要编造，不要输出该字段。
5. 可以根据描述做合理推断：
   - “激进/灵敏/短线” => 阈值略更敏感
   - “保守/减少噪音” => 阈值略更严格
6. positions 只允许修改输入里已经存在的持仓代码；symbols 只允许使用输入里给出的股票代码。
7. 数值必须是 JSON number，enabled 必须是 JSON boolean。
8. 不要输出 null，不要输出注释，不要输出额外字段。"""


def _fmt_price(value: object) -> str:
    num = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(num):
        return "-"
    return f"{float(num):,.2f}"


def _fmt_pct(value: object) -> str:
    num = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(num):
        return "-"
    return f"{float(num):.2f}%"


def _load_local_prefs() -> dict:
    try:
        if not LOCAL_PREFS_PATH.exists():
            return {}
        with LOCAL_PREFS_PATH.open("r", encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _resolve_deepseek_api_key() -> str:
    raw = ""
    if st.session_state.get("deepseek_api_key_input"):
        raw = str(st.session_state["deepseek_api_key_input"])
    if not raw:
        prefs = _load_local_prefs()
        if prefs.get("deepseek_api_key"):
            raw = str(prefs.get("deepseek_api_key", ""))
    if not raw:
        try:
            secret_key = st.secrets.get("DEEPSEEK_API_KEY", "")
            if secret_key:
                raw = str(secret_key)
        except Exception:
            pass
    if not raw:
        raw = os.getenv("DEEPSEEK_API_KEY", "")
    key = raw.strip().split()[0] if raw.strip() else ""
    return key.strip("“”\"'`")


def _validate_deepseek_api_key(key: str) -> None:
    if not key:
        raise RuntimeError("未配置 DEEPSEEK_API_KEY。请在侧栏填写，或设置环境变量 DEEPSEEK_API_KEY。")
    if not key.startswith("sk-"):
        raise RuntimeError("API Key 格式异常：应以 sk- 开头。")
    if not re.fullmatch(r"sk-[A-Za-z0-9._-]+", key):
        raise RuntimeError("API Key 包含非法字符。请重新粘贴纯 key。")


def _extract_json_object(text: str) -> dict:
    raw = (text or "").strip()
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        pass
    start = raw.find("{")
    end = raw.rfind("}")
    if start != -1 and end > start:
        try:
            obj = json.loads(raw[start : end + 1])
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}
    return {}


def _build_holdings_context(positions_df: pd.DataFrame) -> list[dict]:
    if positions_df is None or positions_df.empty:
        return []
    rows: list[dict] = []
    for row in positions_df.to_dict("records"):
        rows.append(
            {
                "market": str(row.get("market") or "").strip(),
                "code": str(row.get("code") or "").strip(),
                "name": str(row.get("name") or "").strip(),
                "avg_cost": pd.to_numeric(pd.Series([row.get("avg_cost")]), errors="coerce").iloc[0],
                "quantity": pd.to_numeric(pd.Series([row.get("quantity")]), errors="coerce").iloc[0],
                "stop_loss": pd.to_numeric(pd.Series([row.get("stop_loss")]), errors="coerce").iloc[0],
                "take_profit": pd.to_numeric(pd.Series([row.get("take_profit")]), errors="coerce").iloc[0],
            }
        )
    return rows


def _build_symbol_context(current_config: dict, positions_df: pd.DataFrame) -> list[dict]:
    frame = _build_rule_a_frame(positions_df)
    if frame.empty:
        return []
    rows: list[dict] = []
    for row in frame.to_dict("records"):
        rows.append(
            {
                "code": str(row.get("code") or "").strip(),
                "name": str(row.get("name") or "").strip(),
                "source": str(row.get("source") or "").strip(),
                "psychological_price": pd.to_numeric(pd.Series([row.get("psychological_price")]), errors="coerce").iloc[0],
            }
        )
    return rows


def _call_deepseek_alert_rule_parser(user_text: str, current_config: dict, positions_df: pd.DataFrame) -> dict:
    if not OPENAI_AVAILABLE or OpenAI is None:
        raise RuntimeError(f"缺少 openai 依赖: {OPENAI_IMPORT_ERROR}")
    api_key = _resolve_deepseek_api_key()
    _validate_deepseek_api_key(api_key)
    client = OpenAI(api_key=api_key, base_url="https://api.deepseek.com/v1", timeout=60.0, max_retries=0)
    user_payload = {
        "current_config": {
            "order_book_dump": current_config.get("order_book_dump", {}),
            "technical_extreme": current_config.get("technical_extreme", {}),
            "position_risk": current_config.get("position_risk", {}),
            "intraday_change": current_config.get("intraday_change", {}),
            "breakout_volume": current_config.get("breakout_volume", {}),
        },
        "current_holdings": _build_holdings_context(positions_df),
        "available_symbols": _build_symbol_context(current_config, positions_df),
        "user_request": str(user_text or "").strip(),
    }
    response = client.chat.completions.create(
        model="deepseek-chat",
        messages=[
            {"role": "system", "content": ALERT_RULE_PARSER_PROMPT},
            {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
        ],
        temperature=0.2,
        max_tokens=900,
        top_p=0.9,
    )
    content = (response.choices[0].message.content or "").strip()
    obj = _extract_json_object(content)
    if not obj:
        raise RuntimeError("DeepSeek 未返回有效 JSON 草案。")
    return obj


def _clip_float(value: object, minimum: float, maximum: float) -> float:
    num = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(num):
        raise ValueError("数值无效")
    return float(min(max(float(num), minimum), maximum))


def _clip_int(value: object, minimum: int, maximum: int) -> int:
    num = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(num):
        raise ValueError("整数无效")
    return int(min(max(int(round(float(num))), minimum), maximum))


def _normalize_rule_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    if text in {"true", "1", "yes", "y", "on", "启用", "开启"}:
        return True
    if text in {"false", "0", "no", "n", "off", "禁用", "关闭"}:
        return False
    raise ValueError("布尔值无效")


def _normalize_alert_rule_draft(draft: dict) -> dict:
    if not isinstance(draft, dict):
        raise ValueError("草案必须是 JSON 对象")
    out: dict = {}

    def _section(name: str) -> dict:
        raw = draft.get(name)
        if raw is None:
            return {}
        if not isinstance(raw, dict):
            raise ValueError(f"{name} 必须是对象")
        return raw

    sec = _section("order_book_dump")
    if sec:
        one = {}
        if "enabled" in sec:
            one["enabled"] = _normalize_rule_bool(sec["enabled"])
        if "ask_volume_threshold_lot" in sec:
            one["ask_volume_threshold_lot"] = _clip_int(sec["ask_volume_threshold_lot"], 100, 500000)
        if "recent_volume_multiplier" in sec:
            one["recent_volume_multiplier"] = _clip_float(sec["recent_volume_multiplier"], 1.0, 50.0)
        if "recent_lookback_bars" in sec:
            one["recent_lookback_bars"] = _clip_int(sec["recent_lookback_bars"], 5, 240)
        if one:
            out["order_book_dump"] = one

    sec = _section("technical_extreme")
    if sec:
        one = {}
        if "enabled" in sec:
            one["enabled"] = _normalize_rule_bool(sec["enabled"])
        if "oversold_rsi" in sec:
            one["oversold_rsi"] = _clip_float(sec["oversold_rsi"], 1.0, 49.0)
        if "overbought_rsi" in sec:
            one["overbought_rsi"] = _clip_float(sec["overbought_rsi"], 51.0, 99.0)
        if one:
            out["technical_extreme"] = one

    sec = _section("position_risk")
    if sec:
        one = {}
        if "enabled" in sec:
            one["enabled"] = _normalize_rule_bool(sec["enabled"])
        if "warn_near_stop_pct" in sec:
            one["warn_near_stop_pct"] = _clip_float(sec["warn_near_stop_pct"], 0.0, 20.0)
        if "warn_near_take_pct" in sec:
            one["warn_near_take_pct"] = _clip_float(sec["warn_near_take_pct"], 0.0, 20.0)
        if one:
            out["position_risk"] = one

    sec = _section("intraday_change")
    if sec:
        one = {}
        if "enabled" in sec:
            one["enabled"] = _normalize_rule_bool(sec["enabled"])
        if "lookback_bars" in sec:
            one["lookback_bars"] = _clip_int(sec["lookback_bars"], 1, 60)
        if "up_pct" in sec:
            one["up_pct"] = _clip_float(sec["up_pct"], 0.1, 20.0)
        if "down_pct" in sec:
            one["down_pct"] = _clip_float(sec["down_pct"], 0.1, 20.0)
        if one:
            out["intraday_change"] = one

    sec = _section("breakout_volume")
    if sec:
        one = {}
        if "enabled" in sec:
            one["enabled"] = _normalize_rule_bool(sec["enabled"])
        if "lookback_bars" in sec:
            one["lookback_bars"] = _clip_int(sec["lookback_bars"], 3, 240)
        if "volume_multiplier" in sec:
            one["volume_multiplier"] = _clip_float(sec["volume_multiplier"], 1.0, 20.0)
        if "breakout_buffer_pct" in sec:
            one["breakout_buffer_pct"] = _clip_float(sec["breakout_buffer_pct"], 0.0, 10.0)
        if one:
            out["breakout_volume"] = one

    sec = _section("symbols")
    if sec:
        one: dict = {}
        for code, value in sec.items():
            symbol = str(code or "").strip()
            if not symbol or not isinstance(value, dict):
                continue
            item: dict = {}
            if "psychological_price" in value:
                item["psychological_price"] = _clip_float(value["psychological_price"], 0.01, 100000.0)
            if item:
                one[symbol] = item
        if one:
            out["symbols"] = one

    positions_raw = draft.get("positions")
    if positions_raw is not None:
        if not isinstance(positions_raw, list):
            raise ValueError("positions 必须是数组")
        rows: list[dict] = []
        for item in positions_raw:
            if not isinstance(item, dict):
                continue
            code = str(item.get("code") or "").strip()
            if not code:
                continue
            row: dict = {"code": code}
            if "market" in item:
                row["market"] = str(item.get("market") or "").strip().upper() or "A"
            if "stop_loss" in item:
                row["stop_loss"] = _clip_float(item["stop_loss"], 0.01, 100000.0)
            if "take_profit" in item:
                row["take_profit"] = _clip_float(item["take_profit"], 0.01, 100000.0)
            if len(row) > 1:
                rows.append(row)
        if rows:
            out["positions"] = rows

    if not out:
        raise ValueError("草案没有可应用的告警参数")
    return out


def _merge_alert_rule_draft(config: dict, draft: dict) -> dict:
    merged = copy.deepcopy(config)
    for section, changes in draft.items():
        if section in {"symbols", "positions"}:
            continue
        base = merged.get(section)
        if not isinstance(base, dict):
            base = {}
        base.update(changes)
        merged[section] = base
    return merged


def _flatten_alert_rule_changes(current_config: dict, draft: dict) -> pd.DataFrame:
    rows: list[dict] = []
    for section, changes in draft.items():
        current_section = current_config.get(section) if isinstance(current_config.get(section), dict) else {}
        for field, new_value in changes.items():
            old_value = current_section.get(field)
            rows.append(
                {
                    "规则": section,
                    "字段": field,
                    "当前值": old_value,
                    "新值": new_value,
                }
            )
    return pd.DataFrame(rows)


def _normalize_ai_draft_against_context(draft: dict, positions_df: pd.DataFrame, current_config: dict) -> dict:
    normalized = copy.deepcopy(draft)
    valid_symbols = {str(row.get("code") or "").strip() for row in _build_symbol_context(current_config, positions_df)}
    valid_positions: dict[str, str] = {}
    if positions_df is not None and not positions_df.empty:
        for row in positions_df.to_dict("records"):
            code = str(row.get("code") or "").strip()
            market = str(row.get("market") or "A").strip().upper() or "A"
            if code:
                valid_positions[code] = market

    if isinstance(normalized.get("symbols"), dict):
        normalized["symbols"] = {k: v for k, v in normalized["symbols"].items() if k in valid_symbols}
        if not normalized["symbols"]:
            normalized.pop("symbols", None)

    if isinstance(normalized.get("positions"), list):
        rows: list[dict] = []
        for item in normalized["positions"]:
            if not isinstance(item, dict):
                continue
            code = str(item.get("code") or "").strip()
            if code not in valid_positions:
                continue
            row = dict(item)
            row["market"] = valid_positions[code]
            rows.append(row)
        if rows:
            normalized["positions"] = rows
        else:
            normalized.pop("positions", None)

    if not normalized:
        raise ValueError("草案没有命中当前可修改的持仓或股票池")
    return normalized


def _flatten_symbol_changes(current_config: dict, draft: dict) -> pd.DataFrame:
    symbols_draft = draft.get("symbols")
    if not isinstance(symbols_draft, dict) or not symbols_draft:
        return pd.DataFrame()
    symbol_cfg = current_config.get("symbols") if isinstance(current_config.get("symbols"), dict) else {}
    rows: list[dict] = []
    for code, item in symbols_draft.items():
        if not isinstance(item, dict):
            continue
        old_price = None
        if isinstance(symbol_cfg.get(code), dict):
            old_price = symbol_cfg.get(code, {}).get("psychological_price")
        rows.append({"代码": code, "字段": "psychological_price", "当前值": old_price, "新值": item.get("psychological_price")})
    return pd.DataFrame(rows)


def _flatten_position_changes(positions_df: pd.DataFrame, draft: dict) -> pd.DataFrame:
    rows_draft = draft.get("positions")
    if not isinstance(rows_draft, list) or not rows_draft:
        return pd.DataFrame()
    current_map: dict[tuple[str, str], dict] = {}
    if positions_df is not None and not positions_df.empty:
        for row in positions_df.to_dict("records"):
            current_map[(str(row.get("market") or "A").strip().upper(), str(row.get("code") or "").strip())] = row
    rows: list[dict] = []
    for item in rows_draft:
        if not isinstance(item, dict):
            continue
        market = str(item.get("market") or "A").strip().upper()
        code = str(item.get("code") or "").strip()
        current = current_map.get((market, code), {})
        if "stop_loss" in item:
            rows.append({"持仓": f"{market}-{code}", "字段": "stop_loss", "当前值": current.get("stop_loss"), "新值": item.get("stop_loss")})
        if "take_profit" in item:
            rows.append({"持仓": f"{market}-{code}", "字段": "take_profit", "当前值": current.get("take_profit"), "新值": item.get("take_profit")})
    return pd.DataFrame(rows)


def _apply_ai_draft(current_config: dict, positions_df: pd.DataFrame, draft: dict) -> None:
    merged = _merge_alert_rule_draft(current_config, draft)
    symbols_draft = draft.get("symbols")
    if isinstance(symbols_draft, dict):
        symbol_cfg = merged.get("symbols") if isinstance(merged.get("symbols"), dict) else {}
        for code, item in symbols_draft.items():
            if not isinstance(item, dict):
                continue
            base = symbol_cfg.get(code)
            if not isinstance(base, dict):
                base = {}
            if "psychological_price" in item:
                base["psychological_price"] = round(float(item["psychological_price"]), 2)
            symbol_cfg[code] = base
        merged["symbols"] = symbol_cfg
    _save_alert_rules(merged)

    positions_draft = draft.get("positions")
    if not isinstance(positions_draft, list) or not positions_draft:
        return
    current_map: dict[tuple[str, str], dict] = {}
    if positions_df is not None and not positions_df.empty:
        for row in positions_df.to_dict("records"):
            current_map[(str(row.get("market") or "A").strip().upper(), str(row.get("code") or "").strip())] = row
    for item in positions_draft:
        if not isinstance(item, dict):
            continue
        market = str(item.get("market") or "A").strip().upper()
        code = str(item.get("code") or "").strip()
        current = current_map.get((market, code))
        if not current:
            continue
        upsert_position(
            Position(
                market=market,
                code=code,
                name=str(current.get("name") or ""),
                avg_cost=float(pd.to_numeric(pd.Series([current.get("avg_cost")]), errors="coerce").iloc[0]),
                quantity=int(pd.to_numeric(pd.Series([current.get("quantity")]), errors="coerce").fillna(0).iloc[0]),
                stop_loss=float(item["stop_loss"]) if "stop_loss" in item else (
                    float(pd.to_numeric(pd.Series([current.get("stop_loss")]), errors="coerce").iloc[0])
                    if not pd.isna(pd.to_numeric(pd.Series([current.get("stop_loss")]), errors="coerce").iloc[0])
                    else None
                ),
                take_profit=float(item["take_profit"]) if "take_profit" in item else (
                    float(pd.to_numeric(pd.Series([current.get("take_profit")]), errors="coerce").iloc[0])
                    if not pd.isna(pd.to_numeric(pd.Series([current.get("take_profit")]), errors="coerce").iloc[0])
                    else None
                ),
                open_date=current.get("open_date"),
            ),
            note="AI_RULE_ASSISTANT",
        )


def _load_alert_rules() -> dict:
    if not ALERT_RULES_PATH.exists():
        return {}
    with ALERT_RULES_PATH.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError("alert_rules.yaml 顶层必须是对象")
    return data


def _save_alert_rules(config: dict) -> None:
    ALERT_RULES_PATH.parent.mkdir(parents=True, exist_ok=True)
    with ALERT_RULES_PATH.open("w", encoding="utf-8") as f:
        yaml.safe_dump(config, f, allow_unicode=True, sort_keys=False)


def _build_rule_a_frame(positions_df: pd.DataFrame) -> pd.DataFrame:
    config = _load_alert_rules()
    symbols_cfg = config.get("symbols") if isinstance(config.get("symbols"), dict) else {}
    position_map: dict[str, dict] = {}
    if positions_df is not None and not positions_df.empty:
        for row in positions_df.to_dict("records"):
            code = str(row.get("code") or "").strip()
            if code:
                position_map[code] = row
    rows: list[dict] = []
    seen: set[str] = set()

    def _add_row(code: object, name: object, source: str) -> None:
        symbol = str(code or "").strip()
        if not symbol or symbol in seen:
            return
        seen.add(symbol)
        symbol_cfg = symbols_cfg.get(symbol) if isinstance(symbols_cfg, dict) else {}
        price = symbol_cfg.get("psychological_price") if isinstance(symbol_cfg, dict) else None
        position = position_map.get(symbol, {})
        rows.append(
            {
                "code": symbol,
                "name": str(name or "").strip(),
                "source": source,
                "psychological_price": pd.to_numeric(price, errors="coerce"),
                "stop_loss": pd.to_numeric(position.get("stop_loss"), errors="coerce"),
                "take_profit": pd.to_numeric(position.get("take_profit"), errors="coerce"),
            }
        )

    if not positions_df.empty:
        for row in positions_df.itertuples(index=False):
            _add_row(row.code, row.name, "holding")

    for pool_group in ("holding", "watch"):
        for code, name in get_stock_pool(pool_group):
            _add_row(code, name, pool_group)

    for code, symbol_cfg in (symbols_cfg or {}).items():
        if not isinstance(symbol_cfg, dict):
            continue
        _add_row(code, "", "alert_rules")

    return pd.DataFrame(rows, columns=["code", "name", "source", "psychological_price", "stop_loss", "take_profit"])


def _build_weight_chart(overview_df: pd.DataFrame) -> alt.Chart:
    chart_df = overview_df.loc[:, ["code", "name", "market_value", "weight_in_position_pct"]].copy()
    chart_df["label"] = chart_df.apply(
        lambda row: f"{row['code']} {str(row['name'] or '').strip()}".strip(),
        axis=1,
    )
    return (
        alt.Chart(chart_df)
        .mark_arc(innerRadius=54, outerRadius=112)
        .encode(
            theta=alt.Theta("market_value:Q", title="持仓市值"),
            color=alt.Color("label:N", legend=alt.Legend(title="持仓")),
            tooltip=[
                alt.Tooltip("label:N", title="标的"),
                alt.Tooltip("market_value:Q", title="市值", format=",.2f"),
                alt.Tooltip("weight_in_position_pct:Q", title="仓位占比", format=".2f"),
            ],
        )
        .properties(height=300)
    )


def _render_position_form() -> None:
    render_section_intro(
        "持仓录入",
        "录入或覆盖一笔当前持仓，作为后续浮盈亏、权重和止损止盈监控的基础数据。",
        kicker="Position",
        pills=("平均成本", "止损止盈", "开仓日期"),
    )
    with st.form("portfolio_position_form", clear_on_submit=False):
        col1, col2, col3 = st.columns(3)
        market = col1.selectbox("市场", options=["A", "HK"], index=0)
        code = col2.text_input("代码", placeholder="600188 / 603871 / 00389")
        name = col3.text_input("名称", placeholder="可选")

        col4, col5, col6 = st.columns(3)
        avg_cost = col4.number_input("买入平均成本", min_value=0.0, value=10.0, step=0.01, format="%.2f")
        quantity = col5.number_input("持仓数量", min_value=1, value=100, step=100)
        open_date = col6.date_input("开仓日期", value=date.today())

        col7, col8, col9 = st.columns(3)
        stop_loss = col7.number_input("预期止损价", min_value=0.0, value=0.0, step=0.01, format="%.2f")
        take_profit = col8.number_input("预期止盈价", min_value=0.0, value=0.0, step=0.01, format="%.2f")
        note = col9.text_input("备注", placeholder="OPEN / 调仓原因")

        submitted = st.form_submit_button("保存持仓", width="stretch")

    if not submitted:
        return

    try:
        upsert_position(
            Position(
                market=market,
                code=code,
                name=name,
                avg_cost=float(avg_cost),
                quantity=int(quantity),
                stop_loss=float(stop_loss) if stop_loss > 0 else None,
                take_profit=float(take_profit) if take_profit > 0 else None,
                open_date=open_date,
            ),
            note=note,
        )
    except Exception as exc:
        st.error(f"保存持仓失败: {exc}")
        return

    st.success("持仓已写入")
    st.rerun()


def _render_delete_panel(positions_df: pd.DataFrame) -> None:
    if positions_df.empty:
        return
    render_section_intro(
        "持仓删除",
        "删除后会同时写入一条 CLOSE 流水，便于保留仓位调整痕迹。",
        kicker="Close",
        pills=("持仓流水", "关闭仓位"),
    )
    options = [
        (
            f"{row.market}-{row.code} {str(row.name or '').strip()}".strip(),
            row.market,
            row.code,
        )
        for row in positions_df.itertuples(index=False)
    ]
    labels = [item[0] for item in options]
    picked = st.selectbox("选择要删除的持仓", options=labels, key="portfolio_delete_select")
    delete_note = st.text_input("删除备注", key="portfolio_delete_note", placeholder="平仓/移出组合原因")
    if st.button("删除持仓", type="secondary", width="stretch"):
        _, market, code = options[labels.index(picked)]
        try:
            ok = remove_position(code=code, market=market, note=delete_note)
        except Exception as exc:
            st.error(f"删除失败: {exc}")
            return
        if ok:
            st.success(f"已删除 {market}-{code}")
            st.rerun()
        st.warning("未找到该持仓")


def _render_risk_dashboard(total_equity: float, overview_df: pd.DataFrame) -> None:
    total_mv = float(overview_df["market_value"].sum()) if not overview_df.empty else 0.0
    total_pnl = float(overview_df["pnl_amount"].sum()) if not overview_df.empty else 0.0
    pos_count = int(len(overview_df))
    invested_ratio = (total_mv / total_equity * 100.0) if total_equity > 0 else 0.0
    render_status_row(
        (
            ("组合总净值", f"{total_equity:,.0f}"),
            ("持仓标的数", str(pos_count)),
            ("持仓市值", f"{total_mv:,.0f}"),
            ("资金使用率", f"{invested_ratio:.2f}%"),
            ("浮动盈亏", f"{total_pnl:,.2f}"),
        )
    )

    if overview_df.empty:
        st.info("当前没有持仓，先录入一笔仓位后再查看风险看板。")
        return

    render_section_intro(
        "组合看板",
        "左侧看仓位饼图，右侧看单票浮盈亏与止损止盈状态，尽量在一屏内完成仓位巡检。",
        kicker="Dashboard",
        pills=("权重", "PnL", "止损止盈"),
    )
    left, right = st.columns([1.05, 1.35], gap="large")
    with left:
        st.altair_chart(_build_weight_chart(overview_df), width="stretch")
    with right:
        display_df = overview_df.copy()
        display_df["标的"] = display_df.apply(
            lambda row: f"{row['market']}-{row['code']} {str(row['name'] or '').strip()}".strip(),
            axis=1,
        )
        display_df["现价"] = display_df["current_price"].map(_fmt_price)
        display_df["成本"] = display_df["avg_cost"].map(_fmt_price)
        display_df["浮盈亏"] = display_df["pnl_amount"].map(_fmt_price)
        display_df["浮盈亏%"] = display_df["pnl_pct"].map(_fmt_pct)
        display_df["净值占比"] = display_df["weight_in_equity_pct"].map(_fmt_pct)
        display_df["仓位占比"] = display_df["weight_in_position_pct"].map(_fmt_pct)
        display_df["止损"] = display_df["stop_loss"].map(_fmt_price)
        display_df["止盈"] = display_df["take_profit"].map(_fmt_price)
        st.dataframe(
            display_df[
                ["标的", "现价", "成本", "浮盈亏", "浮盈亏%", "净值占比", "仓位占比", "止损", "止盈", "risk_status"]
            ].rename(columns={"risk_status": "风险状态"}),
            width="stretch",
            hide_index=True,
        )


def _render_position_sizer(total_equity: float) -> None:
    render_section_intro(
        "仓单规模建议器",
        "按 ATR20 和单笔风险不超过总净值 1% 的规则，快速给出建议股数和预计占用资金。",
        kicker="Sizer",
        pills=("ATR20", "1% 风险", "手数约束"),
    )
    with st.form("portfolio_sizer_form", clear_on_submit=False):
        c1, c2, c3 = st.columns(3)
        market = c1.selectbox("市场", options=["A", "HK"], index=0, key="portfolio_sizer_market")
        code = c2.text_input("代码", key="portfolio_sizer_code", placeholder="600188 / 00700")
        entry_price = c3.number_input("入场价", min_value=0.0, value=0.0, step=0.01, format="%.2f")

        c4, c5, c6 = st.columns(3)
        stop_loss = c4.number_input("止损价", min_value=0.0, value=0.0, step=0.01, format="%.2f")
        risk_pct = c5.number_input("单笔风险上限(%)", min_value=0.1, value=1.0, step=0.1, format="%.1f")
        lot_size = c6.number_input("每手股数", min_value=1, value=100, step=1)

        submitted = st.form_submit_button("计算建议仓位", width="stretch")

    if not submitted:
        return

    try:
        result = suggest_position_size(
            code=code,
            market=market,
            total_equity=total_equity,
            entry_price=float(entry_price) if entry_price > 0 else None,
            stop_loss=float(stop_loss) if stop_loss > 0 else None,
            risk_pct=float(risk_pct),
            lot_size=int(lot_size),
        )
    except Exception as exc:
        st.error(f"仓位建议计算失败: {exc}")
        return

    render_status_row(
        (
            ("ATR20", f"{result['atr20']:.4f}"),
            ("单股风险", f"{result['risk_per_share']:.4f}"),
            ("风险预算", f"{result['risk_amount']:.2f}"),
            ("建议手数", str(result["suggested_lots"])),
            ("建议股数", f"{result['suggested_shares']:,}"),
            ("占用资金", f"{result['capital_needed']:,.2f}"),
        )
    )


def _render_price_param_editor(positions_df: pd.DataFrame) -> pd.DataFrame:
    editor_df = _build_rule_a_frame(positions_df)
    if editor_df.empty:
        st.caption("当前没有持仓或自选股，先在股票池或持仓面板里录入标的。")
        return pd.DataFrame(columns=["code", "name", "psychological_price", "stop_loss", "take_profit"])

    st.caption("价格参数：统一维护心理价位、止损价和止盈价；非持仓股票只会保存心理价位。")
    return st.data_editor(
        editor_df.loc[:, ["code", "name", "psychological_price", "stop_loss", "take_profit"]].copy(),
        width="stretch",
        hide_index=True,
        column_config={
            "code": st.column_config.TextColumn("代码", disabled=True),
            "name": st.column_config.TextColumn("名称", disabled=True),
            "psychological_price": st.column_config.NumberColumn("心理价位", min_value=0.0, step=0.01, format="%.2f"),
            "stop_loss": st.column_config.NumberColumn("止损价", min_value=0.0, step=0.01, format="%.2f"),
            "take_profit": st.column_config.NumberColumn("止盈价", min_value=0.0, step=0.01, format="%.2f"),
        },
        disabled=["code", "name"],
        key="portfolio_price_param_editor",
    )


def _apply_price_param_editor(config: dict, positions_df: pd.DataFrame, edited_df: pd.DataFrame) -> None:
    symbols_cfg = config.get("symbols") if isinstance(config.get("symbols"), dict) else {}
    if not isinstance(symbols_cfg, dict):
        symbols_cfg = {}

    position_map: dict[tuple[str, str], dict] = {}
    if positions_df is not None and not positions_df.empty:
        for row in positions_df.to_dict("records"):
            market = str(row.get("market") or "A").strip().upper() or "A"
            code = str(row.get("code") or "").strip()
            if code:
                position_map[(market, code)] = row

    for row in edited_df.to_dict("records"):
        code = str(row.get("code") or "").strip()
        if not code:
            continue
        price = pd.to_numeric(pd.Series([row.get("psychological_price")]), errors="coerce").iloc[0]
        symbol_cfg = symbols_cfg.get(code)
        if not isinstance(symbol_cfg, dict):
            symbol_cfg = {}
        if pd.isna(price) or float(price) <= 0:
            symbol_cfg.pop("psychological_price", None)
            if symbol_cfg:
                symbols_cfg[code] = symbol_cfg
            else:
                symbols_cfg.pop(code, None)
        else:
            symbol_cfg["psychological_price"] = round(float(price), 2)
            symbols_cfg[code] = symbol_cfg

    config["symbols"] = symbols_cfg
    _save_alert_rules(config)

    for (market, code), current in position_map.items():
        matched = next((one for one in edited_df.to_dict("records") if str(one.get("code") or "").strip() == code), None)
        if not matched:
            continue
        stop_loss_val = pd.to_numeric(pd.Series([matched.get("stop_loss")]), errors="coerce").iloc[0]
        take_profit_val = pd.to_numeric(pd.Series([matched.get("take_profit")]), errors="coerce").iloc[0]
        avg_cost_val = pd.to_numeric(pd.Series([current.get("avg_cost")]), errors="coerce").iloc[0]
        quantity_val = pd.to_numeric(pd.Series([current.get("quantity")]), errors="coerce").fillna(0).iloc[0]
        upsert_position(
            Position(
                market=market,
                code=code,
                name=str(current.get("name") or ""),
                avg_cost=float(avg_cost_val),
                quantity=int(quantity_val),
                stop_loss=float(stop_loss_val) if not pd.isna(stop_loss_val) and float(stop_loss_val) > 0 else None,
                take_profit=float(take_profit_val) if not pd.isna(take_profit_val) and float(take_profit_val) > 0 else None,
                open_date=current.get("open_date"),
            ),
            note="PRICE_PARAM_EDITOR",
        )


def _render_alert_rule_ai_assistant(positions_df: pd.DataFrame) -> None:
    render_section_intro(
        "自然语言配置",
        "用一句自然语言描述你的风控需求，DeepSeek 先生成草案，再由你确认后同时更新告警参数、心理价位和现有持仓止盈止损。",
        kicker="DeepSeek",
        pills=("告警参数", "心理价位", "止盈止损"),
    )
    prompt = st.text_area(
        "用自然语言描述你想要的告警风格",
        key="portfolio_alert_rule_prompt",
        height=120,
        placeholder="例如：嘉友国际心理价位设 13.2，止损 12.6，止盈 14.5。我偏短线，5分钟异动 1.5%，RSI 用 28/72，放量突破至少 4 倍量，接近止损 0.8% 提醒，接近止盈 1.2% 提醒。",
    )
    c1, c2 = st.columns([1.2, 1.0])
    if c1.button("生成参数草案", width="stretch"):
        if not str(prompt or "").strip():
            st.warning("先输入自然语言描述。")
        else:
            try:
                current_config = _load_alert_rules()
                raw_draft = _call_deepseek_alert_rule_parser(prompt, current_config, positions_df)
                normalized_draft = _normalize_alert_rule_draft(raw_draft)
                normalized_draft = _normalize_ai_draft_against_context(normalized_draft, positions_df, current_config)
            except Exception as exc:
                st.error(f"生成参数草案失败: {exc}")
            else:
                st.session_state["portfolio_alert_rule_draft"] = normalized_draft
                st.session_state["portfolio_alert_rule_raw_draft"] = raw_draft
                st.success("草案已生成，先看预览再决定是否应用。")
    if c2.button("清空草案", width="stretch"):
        st.session_state.pop("portfolio_alert_rule_draft", None)
        st.session_state.pop("portfolio_alert_rule_raw_draft", None)
        st.rerun()

    draft = st.session_state.get("portfolio_alert_rule_draft")
    if not isinstance(draft, dict) or not draft:
        st.caption("当前还没有 DeepSeek 生成的告警参数草案。")
        return

    try:
        current_config = _load_alert_rules()
    except Exception as exc:
        st.error(f"读取当前告警配置失败: {exc}")
        return

    st.markdown("**草案 JSON**")
    st.json(draft)
    diff_df = _flatten_alert_rule_changes(current_config, draft)
    if not diff_df.empty:
        st.markdown("**通用告警参数变更**")
        st.dataframe(diff_df, width="stretch", hide_index=True)
    symbol_diff_df = _flatten_symbol_changes(current_config, draft)
    if not symbol_diff_df.empty:
        st.markdown("**心理价位变更**")
        st.dataframe(symbol_diff_df, width="stretch", hide_index=True)
    position_diff_df = _flatten_position_changes(positions_df, draft)
    if not position_diff_df.empty:
        st.markdown("**持仓止盈止损变更**")
        st.dataframe(position_diff_df, width="stretch", hide_index=True)

    if st.button("应用草案", type="primary", width="stretch"):
        try:
            _apply_ai_draft(current_config, positions_df, draft)
        except Exception as exc:
            st.error(f"应用草案失败: {exc}")
            return
        st.session_state.pop("portfolio_alert_rule_draft", None)
        st.session_state.pop("portfolio_alert_rule_raw_draft", None)
        st.success("DeepSeek 草案已应用到仓位风控配置")
        st.rerun()


def _render_alert_rule_settings(positions_df: pd.DataFrame) -> None:
    render_section_intro(
        "告警参数",
        "统一管理单票价格参数、盘口卖压、RSI、持仓止盈止损、分时异动和放量突破规则，后台 worker 会直接读取这里的参数。",
        kicker="Rules",
        pills=("价格参数", "B", "C", "D", "E", "F"),
    )
    try:
        config = _load_alert_rules()
    except Exception as exc:
        st.error(f"读取告警参数失败: {exc}")
        return

    order_cfg = config.get("order_book_dump") if isinstance(config.get("order_book_dump"), dict) else {}
    tech_cfg = config.get("technical_extreme") if isinstance(config.get("technical_extreme"), dict) else {}
    position_cfg = config.get("position_risk") if isinstance(config.get("position_risk"), dict) else {}
    change_cfg = config.get("intraday_change") if isinstance(config.get("intraday_change"), dict) else {}
    breakout_cfg = config.get("breakout_volume") if isinstance(config.get("breakout_volume"), dict) else {}

    with st.form("portfolio_alert_rule_form", clear_on_submit=False):
        price_editor_df = _render_price_param_editor(positions_df)
        st.markdown("**规则B：盘口大卖单**")
        b1, b2, b3 = st.columns(3)
        order_enabled = b1.checkbox("启用规则B", value=bool(order_cfg.get("enabled", True)))
        ask_volume_threshold_lot = b2.number_input(
            "万手大单阈值(手)", min_value=100, value=int(order_cfg.get("ask_volume_threshold_lot", 10000) or 10000), step=100
        )
        recent_volume_multiplier = b3.number_input(
            "成交量放大倍数", min_value=1.0, value=float(order_cfg.get("recent_volume_multiplier", 8) or 8), step=0.5, format="%.1f"
        )
        recent_lookback_bars = st.number_input(
            "规则B参考最近分时根数", min_value=5, value=int(order_cfg.get("recent_lookback_bars", 20) or 20), step=1
        )

        st.markdown("**规则C：技术指标超买超卖**")
        c1, c2, c3 = st.columns(3)
        technical_enabled = c1.checkbox("启用规则C", value=bool(tech_cfg.get("enabled", True)))
        oversold_rsi = c2.number_input(
            "超卖 RSI", min_value=1.0, max_value=50.0, value=float(tech_cfg.get("oversold_rsi", 25) or 25), step=1.0, format="%.1f"
        )
        overbought_rsi = c3.number_input(
            "超买 RSI", min_value=50.0, max_value=99.0, value=float(tech_cfg.get("overbought_rsi", 75) or 75), step=1.0, format="%.1f"
        )

        st.markdown("**规则D：持仓止盈止损触发**")
        d1, d2, d3 = st.columns(3)
        position_enabled = d1.checkbox("启用规则D", value=bool(position_cfg.get("enabled", True)))
        warn_near_stop_pct = d2.number_input(
            "接近止损提醒(%)",
            min_value=0.0,
            value=float(position_cfg.get("warn_near_stop_pct", 1.0) or 1.0),
            step=0.1,
            format="%.1f",
        )
        warn_near_take_pct = d3.number_input(
            "接近止盈提醒(%)",
            min_value=0.0,
            value=float(position_cfg.get("warn_near_take_pct", 1.0) or 1.0),
            step=0.1,
            format="%.1f",
        )

        st.markdown("**规则E：分时涨跌幅异动**")
        e1, e2, e3, e4 = st.columns(4)
        intraday_change_enabled = e1.checkbox("启用规则E", value=bool(change_cfg.get("enabled", True)))
        intraday_lookback_bars = e2.number_input(
            "回看分时根数", min_value=1, value=int(change_cfg.get("lookback_bars", 5) or 5), step=1
        )
        intraday_up_pct = e3.number_input(
            "拉升阈值(%)", min_value=0.1, value=float(change_cfg.get("up_pct", 2.0) or 2.0), step=0.1, format="%.1f"
        )
        intraday_down_pct = e4.number_input(
            "跳水阈值(%)", min_value=0.1, value=float(change_cfg.get("down_pct", 2.0) or 2.0), step=0.1, format="%.1f"
        )

        st.markdown("**规则F：放量突破 / 下破**")
        f1, f2, f3, f4 = st.columns(4)
        breakout_enabled = f1.checkbox("启用规则F", value=bool(breakout_cfg.get("enabled", True)))
        breakout_lookback_bars = f2.number_input(
            "区间回看根数", min_value=3, value=int(breakout_cfg.get("lookback_bars", 20) or 20), step=1
        )
        breakout_volume_multiplier = f3.number_input(
            "量能放大倍数", min_value=1.0, value=float(breakout_cfg.get("volume_multiplier", 3.0) or 3.0), step=0.5, format="%.1f"
        )
        breakout_buffer_pct = f4.number_input(
            "突破缓冲(%)", min_value=0.0, value=float(breakout_cfg.get("breakout_buffer_pct", 0.0) or 0.0), step=0.1, format="%.1f"
        )

        submitted = st.form_submit_button("保存告警参数", width="stretch")

    if not submitted:
        return

    try:
        _apply_price_param_editor(config, positions_df, price_editor_df)
        config["order_book_dump"] = {
            "enabled": bool(order_enabled),
            "ask_volume_threshold_lot": int(ask_volume_threshold_lot),
            "recent_volume_multiplier": float(recent_volume_multiplier),
            "recent_lookback_bars": int(recent_lookback_bars),
        }
        config["technical_extreme"] = {
            "enabled": bool(technical_enabled),
            "oversold_rsi": float(oversold_rsi),
            "overbought_rsi": float(overbought_rsi),
        }
        config["position_risk"] = {
            "enabled": bool(position_enabled),
            "warn_near_stop_pct": float(warn_near_stop_pct),
            "warn_near_take_pct": float(warn_near_take_pct),
        }
        config["intraday_change"] = {
            "enabled": bool(intraday_change_enabled),
            "lookback_bars": int(intraday_lookback_bars),
            "up_pct": float(intraday_up_pct),
            "down_pct": float(intraday_down_pct),
        }
        config["breakout_volume"] = {
            "enabled": bool(breakout_enabled),
            "lookback_bars": int(breakout_lookback_bars),
            "volume_multiplier": float(breakout_volume_multiplier),
            "breakout_buffer_pct": float(breakout_buffer_pct),
        }
        _save_alert_rules(config)
    except Exception as exc:
        st.error(f"保存告警参数失败: {exc}")
        return
    st.success("告警参数已保存")
    st.rerun()


def _render_flows() -> None:
    render_section_intro(
        "持仓流水",
        "保留开仓、调仓和平仓动作，避免风控看板只剩静态快照。",
        kicker="Flows",
        pills=("OPEN", "ADJUST", "CLOSE"),
    )
    flows_df = list_position_flows(limit=80)
    if flows_df.empty:
        st.caption("当前没有持仓流水。")
        return
    flow_view = flows_df.copy()
    flow_view["flow_time"] = pd.to_datetime(flow_view["flow_time"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
    st.dataframe(
        flow_view.rename(
            columns={
                "flow_time": "时间",
                "market": "市场",
                "code": "代码",
                "name": "名称",
                "action": "动作",
                "quantity_delta": "数量变化",
                "quantity_after": "当前数量",
                "avg_cost": "成本",
                "stop_loss": "止损",
                "take_profit": "止盈",
                "note": "备注",
            }
        ),
        width="stretch",
        hide_index=True,
    )


def render_portfolio_page(*, embedded: bool = True) -> None:
    del embedded
    try:
        init_db()
    except Exception as exc:
        st.error(f"仓位风控模块初始化失败: {exc}")
        st.info("当前页面依赖 DuckDB。本机缺少 `duckdb` 时，先执行 `python3 -m pip install duckdb`。")
        return

    total_equity = st.number_input(
        "组合总净值",
        min_value=1000.0,
        value=float(st.session_state.get("portfolio_total_equity", 1000000.0)),
        step=10000.0,
        format="%.2f",
        key="portfolio_total_equity",
    )

    positions_df = list_positions()
    overview_df = get_positions_overview(total_equity=total_equity)

    _render_position_form()
    _render_delete_panel(positions_df)
    _render_risk_dashboard(total_equity, overview_df)
    _render_position_sizer(total_equity)
    _render_alert_rule_ai_assistant(positions_df)
    _render_alert_rule_settings(positions_df)
    _render_flows()
