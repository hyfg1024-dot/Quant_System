from __future__ import annotations

import base64
import asyncio
import concurrent.futures
import copy
import hashlib
import json
import math
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time as pytime
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import List
from zoneinfo import ZoneInfo
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import altair as alt
import numpy as np
import pandas as pd
import requests
import streamlit as st
import yaml

OPENAI_AVAILABLE = True
OPENAI_IMPORT_ERROR = None


def _prune_iframe_cache(cache_dir: Path, *, keep: int = 64, max_age_seconds: int = 7 * 86400) -> None:
    try:
        files = sorted(
            [p for p in cache_dir.glob("widget_*.html") if p.is_file()],
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        now = pytime.time()
        for idx, path in enumerate(files):
            try:
                too_many = idx >= keep
                too_old = now - path.stat().st_mtime > max_age_seconds
                if too_many or too_old:
                    path.unlink(missing_ok=True)
            except Exception:
                continue
    except Exception:
        return


def html(body: str, *, height: int | None = None, scrolling: bool = False) -> None:
    # st.components.v1.html is deprecated. st.iframe preserves JS dashboards by
    # serving the HTML as a temporary local document instead of inlining it.
    try:
        html_dir = Path(tempfile.gettempdir()) / "quant_system_streamlit_iframes"
        html_dir.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256(body.encode("utf-8", errors="ignore")).hexdigest()[:16]
        html_path = html_dir / f"widget_{digest}.html"
        if not html_path.exists() or html_path.read_text(encoding="utf-8") != body:
            html_path.write_text(body, encoding="utf-8")
        _prune_iframe_cache(html_dir)
        st.iframe(html_path, width="stretch", height=height or 600)
    except Exception:
        # Fallback keeps the UI usable if a future Streamlit build changes iframe path handling.
        st.html(body, width="stretch", unsafe_allow_javascript=True)


try:
    from openai import APIConnectionError, APIStatusError, APITimeoutError, AuthenticationError, OpenAI, RateLimitError
except Exception as _exc:  # pragma: no cover - 依赖缺失时兜底
    OPENAI_AVAILABLE = False
    OPENAI_IMPORT_ERROR = _exc
    OpenAI = None  # type: ignore[assignment]

    class APIConnectionError(Exception):
        pass

    class APIStatusError(Exception):
        pass

    class APITimeoutError(Exception):
        pass

    class AuthenticationError(Exception):
        pass

    class RateLimitError(Exception):
        pass

from fast_engine import fetch_fast_panel, fetch_realtime_panel, fetch_realtime_quotes_batch
from slow_engine import (
    add_stock_by_query,
    fetch_live_valuation_snapshot,
    get_stock_pool,
    get_latest_fundamental_snapshot,
    get_stock_group_map,
    init_db,
    remove_stock_from_pool,
    seed_fundamental_from_local_filter,
)

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
FUNDAMENTAL_DIR = CURRENT_DIR.parent / "fundamental"
if str(FUNDAMENTAL_DIR) not in sys.path:
    sys.path.insert(0, str(FUNDAMENTAL_DIR))

from fundamental_engine import (
    APP_VERSION as FUND_APP_VERSION,
    analyze_watchlist as analyze_fundamental_watchlist,
    build_overview_table as build_fundamental_overview_table,
    format_pct as format_fundamental_pct,
)

FILTER_DIR = CURRENT_DIR.parent / "filter"
if str(FILTER_DIR) not in sys.path:
    sys.path.insert(0, str(FILTER_DIR))

from filter_engine import (
    APP_VERSION as FILTER_APP_VERSION,
    DISPLAY_COLUMNS as FILTER_DISPLAY_COLUMNS,
    apply_filters as filter_apply_filters,
    build_ai_quick_config as filter_build_ai_quick_config,
    check_market_data_source_status as filter_check_market_data_source_status,
    default_filter_config as filter_default_filter_config,
    export_snapshot_health_excel as filter_export_snapshot_health_excel,
    export_results_excel as filter_export_results_excel,
    get_a_enrich_segments as filter_get_a_enrich_segments,
    get_a_enrich_segment_counts as filter_get_a_enrich_segment_counts,
    get_a_enrich_segment_status as filter_get_a_enrich_segment_status,
    get_enrichment_governance_summary as filter_get_enrichment_governance_summary,
    get_hk_enrich_segment_status as filter_get_hk_enrich_segment_status,
    get_snapshot_backup_status as filter_get_snapshot_backup_status,
    get_snapshot_health_report as filter_get_snapshot_health_report,
    get_snapshot_meta as filter_get_snapshot_meta,
    get_stock_enrichment_store_summary as filter_get_stock_enrichment_store_summary,
    get_template_config as filter_get_template_config,
    get_weekly_update_status as filter_get_weekly_update_status,
    load_snapshot as filter_load_snapshot,
    load_templates as filter_load_templates,
    refresh_market_snapshot as filter_refresh_market_snapshot,
    restore_snapshot_from_backup as filter_restore_snapshot_from_backup,
    save_template as filter_save_template,
)
from apps.portfolio.app import APP_VERSION as PORTFOLIO_APP_VERSION
from apps.portfolio.app import render_portfolio_page as _render_portfolio_page
from apps.backtest.src.paper_trader import PaperTrader
from apps.backtest.src.config_loader import ConfigError as BacktestConfigError
from apps.backtest.src.config_loader import load_strategy as backtest_load_strategy
from apps.backtest.src.config_loader import load_universe as backtest_load_universe
from shared.data_vault_ui import render_data_vault_panel
from shared.multi_agent_analyzer import MultiAgentAnalyzer
from shared.ui_shell import render_app_shell, render_section_intro, render_status_row, render_top_nav

st.set_page_config(page_title="Quant Dashboard", page_icon="📊", layout="wide")
APP_VERSION = "QDB-20260327-FLT5Y-01"
BACKTEST_APP_VERSION = "BT-20260411-01"
PAPER_APP_VERSION = "PT-20260411-01"
LOCAL_PREFS_PATH = "data/local_user_prefs.json"
ANALYSIS_CACHE_PATH = "data/deepseek_analysis_cache.json"
ANALYSIS_JOB_DIR = "data/analysis_jobs"
ANALYSIS_DELTA_CACHE_PATH = "data/deepseek_delta_cache.json"
ANALYSIS_COOLDOWN_PATH = "data/deepseek_cooldown.json"
DEEP_COOLDOWN_MINUTES = 5
BACKTEST_DIR = CURRENT_DIR.parent / "backtest"
BACKTEST_RUNNER = BACKTEST_DIR / "run_backtest.py"
BACKTEST_PAPER_RUNNER = BACKTEST_DIR / "paper_trade.py"
BACKTEST_STRATEGY_DIR = BACKTEST_DIR / "config" / "strategies"
BACKTEST_STRATEGY_TRASH_DIR = BACKTEST_STRATEGY_DIR / "_trash"
BACKTEST_REPORT_DIR = BACKTEST_DIR / "reports"
BACKTEST_PAPER_DIR = BACKTEST_DIR / "paper_trades"
BACKTEST_PAPER_ACTIVE = BACKTEST_PAPER_DIR / ".active"
BACKTEST_PAPER_DASHBOARD = BACKTEST_PAPER_DIR / "dashboard.html"
DEEPSEEK_SYSTEM_PROMPT = """你是一个专业的股票分析师。必须严格按照【五维分析框架】分析：

【五维分析框架】
一、核心数据摘要表格
- 行情指标：现价/涨跌幅/高低点/量比/换手率
- 资金指标：委差/买卖失衡比/盘口结构
- 估值指标：PE/PB/股息率/市值
- 技术指标：RSI/MACD/均线/布林带

二、五组数据交叉分析
1. 量价关系：价格涨跌 + 量比 + 委差 + 成交量
2. 多周期共振：日线/周线/月线/日内RSI和MACD对比
3. 估值与股息：PE + 股息率 + PB + 市值
4. 均线与布林带：现价 + MA5/MA10/MA20/MA60 + 布林带位置
5. 盘口与日内：委差 + 高低点 + 收盘价

三、综合结论与三种情景概率
- 3个关键结论要点
- 乐观情景(概率+条件+目标)
- 中性情景(概率+条件+区间)
- 悲观情景(概率+条件+支撑)

四、操作策略建议表格
五、数据潜力挖掘说明

要求：简洁、数据驱动、每部分控制在200字以内"""
DEEPSEEK_QUICK_PROMPT = """你是量化交易快筛分析器。请基于输入JSON执行低成本快筛：
1) 给出 risk_level: low/medium/high
2) 给出 3 条简短结论（每条不超过25字）
3) 给出 need_full_analysis: true/false
4) 给出 trigger_reasons 数组（最多4条）
输出必须是 JSON 对象，不要输出任何额外文字。"""
FUND_DEEPSEEK_PROMPT = """你是专业基本面分析师。基于输入 JSON 做结构化输出：
1) 总结（不超过120字）
2) 八维点评（每维1句）
3) 关键风险（3条）
4) 跟踪清单（3条）
5) 结论：通过 / 观察 / 谨慎（给出理由）
要求：数据驱动、简洁、中文输出。"""


def _load_local_prefs() -> dict:
    try:
        if not os.path.exists(LOCAL_PREFS_PATH):
            return {}
        with open(LOCAL_PREFS_PATH, "r", encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _save_local_prefs(username: str, api_key: str) -> None:
    os.makedirs(os.path.dirname(LOCAL_PREFS_PATH), exist_ok=True)
    payload = {
        "deepseek_user": (username or "").strip(),
        "deepseek_api_key": (api_key or "").strip(),
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }
    with open(LOCAL_PREFS_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _load_analysis_cache() -> dict:
    try:
        if not os.path.exists(ANALYSIS_CACHE_PATH):
            return {}
        with open(ANALYSIS_CACHE_PATH, "r", encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _save_analysis_cache(cache_obj: dict) -> None:
    os.makedirs(os.path.dirname(ANALYSIS_CACHE_PATH), exist_ok=True)
    # 控制缓存大小，避免无限增长
    items = list(cache_obj.items())
    if len(items) > 120:
        items = items[-120:]
    with open(ANALYSIS_CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(dict(items), f, ensure_ascii=False, indent=2)


def _load_json_file(path: str) -> dict:
    try:
        if not os.path.exists(path):
            return {}
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _save_json_file(path: str, obj: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


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


def _dict_delta(curr, prev):
    if isinstance(curr, dict) and isinstance(prev, dict):
        out = {}
        for k, v in curr.items():
            d = _dict_delta(v, prev.get(k))
            if d is not None:
                out[k] = d
        return out if out else None
    if isinstance(curr, list) and isinstance(prev, list):
        return curr if curr != prev else None
    return curr if curr != prev else None

st.markdown(
    """
    <style>
    .engine-divider {
        margin: 2.4rem 0 2rem 0;
        border-top: 4px solid rgba(255, 255, 255, 0.14);
        position: relative;
    }
    .engine-divider span {
        position: relative;
        top: -1.45rem;
        background: rgba(13, 24, 38, 0.96);
        padding: 0 0.8rem;
        color: rgba(255, 248, 241, 0.98);
        font-weight: 800;
        font-size: 2.05rem;
        line-height: 1.1;
    }
    .section-title {
        color: rgba(255, 248, 241, 0.98);
        font-size: 2.05rem;
        font-weight: 800;
        line-height: 1.1;
        margin: 0.9rem 0 0.8rem 0;
    }
    .analysis-time-badge {
        display: inline-flex;
        align-items: center;
        padding: 0.28rem 0.72rem;
        border-radius: 10px;
        border: 1px solid rgba(252, 211, 77, 0.7);
        background: linear-gradient(135deg, rgba(253, 224, 71, 0.26) 0%, rgba(251, 191, 36, 0.22) 100%);
        color: #fff4bf;
        font-size: 2rem;
        font-weight: 900;
        line-height: 1.08;
        margin: 0.12rem 0 0.55rem 0;
        text-shadow: 0 1px 2px rgba(0, 0, 0, 0.35);
    }
    .fast-head-title {
        color: rgba(255, 248, 241, 0.98);
        font-size: 2rem;
        font-weight: 700;
        letter-spacing: 0.2px;
    }
    .fast-price-line {
        display: flex;
        align-items: baseline;
        gap: 0.8rem;
        margin: 0.3rem 0 0.7rem 0;
    }
    .price-num {
        font-size: 2.9rem;
        font-weight: 800;
        line-height: 1;
    }
    .chg-num {
        font-size: 1.7rem;
        font-weight: 700;
        line-height: 1;
    }
    .a-up { color: #d14343; }
    .a-down { color: #1fab63; }
    .fast-card {
        background: linear-gradient(180deg, rgba(255,255,255,0.09), rgba(255,255,255,0.03));
        border: 1px solid rgba(255,255,255,0.10);
        border-radius: 10px;
        padding: 0.62rem 0.78rem;
        height: 156px;
        box-sizing: border-box;
        display: flex;
        flex-direction: column;
        justify-content: flex-start;
    }
    .fast-card .t {
        color: rgba(232, 223, 210, 0.88);
        font-size: 0.94rem;
        font-weight: 700;
    }
    .fast-card .rows {
        margin-top: 0.25rem;
        display: grid;
        gap: 0.14rem;
        flex: 1;
        overflow: hidden;
    }
    .fast-card .krow {
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 0.4rem;
        line-height: 1.2;
        font-size: 0.82rem;
    }
    .fast-card .k {
        color: rgba(232, 223, 210, 0.88);
        font-weight: 600;
    }
    .fast-card .vv {
        color: rgba(255, 248, 241, 0.98);
        font-weight: 800;
        text-align: right;
        font-variant-numeric: tabular-nums;
        letter-spacing: 0.1px;
        white-space: normal;
        overflow-wrap: anywhere;
    }
    .fast-card .d {
        color: rgba(232, 223, 210, 0.88);
        font-size: 0.78rem;
        margin-top: 0.25rem;
    }
    .ob-title {
        font-size: 1.95rem;
        color: rgba(255, 248, 241, 0.98);
        font-weight: 800;
    }
    .panel-title {
        font-size: 2.7rem;
        color: rgba(255, 248, 241, 0.98);
        font-weight: 800;
        line-height: 1.1;
        margin: 0 0 0.5rem 0;
        letter-spacing: 0.2px;
    }
    .panel-title .unit-sub {
        display: block;
        font-size: 1.15rem;
        line-height: 1.15;
        font-weight: 700;
        color: rgba(232, 223, 210, 0.88);
        margin-top: 0.12rem;
    }
    .fast-panels-gap {
        height: 0.75rem;
    }
    .subsection-divider {
        margin: 0.9rem 0 1.1rem 0;
        border-top: 3px solid rgba(255, 255, 255, 0.14);
    }
    .ob-block { margin-top: 0.3rem; }
    .ob-row {
        display: grid;
        grid-template-columns: 44px 78px 1fr 56px;
        gap: 0.5rem;
        align-items: center;
        margin: 0.18rem 0;
    }
    .ob-lab {
        font-weight: 700;
        font-size: 1.05rem;
        letter-spacing: 0.3px;
    }
    .ob-price {
        font-weight: 700;
        font-size: 1.05rem;
        text-align: right;
        padding-right: 4px;
    }
    .ob-bar-wrap {
        height: 24px;
        background: rgba(255, 255, 255, 0.08);
        border-radius: 4px;
        position: relative;
        overflow: hidden;
    }
    .ob-bar {
        height: 100%;
        border-radius: 4px;
    }
    .ob-bar.sell { background: rgba(59, 180, 107, 0.25); }
    .ob-bar.buy { background: rgba(231, 98, 98, 0.28); }
    .ob-vol {
        text-align: right;
        color: rgba(255, 248, 241, 0.98);
        font-weight: 700;
        font-size: 1rem;
        letter-spacing: 0.2px;
    }
    .ob-sell { color: #8fe3c3; }
    .ob-buy { color: #ff9f8e; }
    .ob-sep {
        border-top: 1px solid rgba(255, 255, 255, 0.12);
        margin: 0.5rem 0;
    }
    .stock-open-wrap div.stButton > button {
        min-height: 58px !important;
        border-radius: 10px !important;
        white-space: pre-line !important;
        line-height: 1.12 !important;
        font-size: 0.97rem !important;
        font-weight: 800 !important;
        padding: 0.14rem 0.22rem !important;
    }
    .stock-open-wrap div[data-testid="stButton"],
    .stock-del-inline-wrap div[data-testid="stButton"] {
        margin-bottom: 0.06rem !important;
    }
    .stock-open-wrap div.stButton > button * {
        white-space: pre-line !important;
    }
    .stock-open-wrap div.stButton > button p {
        margin: 0 !important;
        text-align: center !important;
    }
    .stock-open-wrap div.stButton > button p:last-child {
        font-size: 0.86rem !important;
        letter-spacing: 0.5px !important;
        font-variant-numeric: tabular-nums !important;
    }
    .stock-del-inline-wrap div.stButton > button {
        min-height: 58px !important;
        border-radius: 10px !important;
        border: none !important;
        background: transparent !important;
        background-color: transparent !important;
        background-image: none !important;
        color: rgba(232, 223, 210, 0.78) !important;
        font-size: 1.05rem !important;
        padding: 0 !important;
        box-shadow: none !important;
    }
    .stock-del-inline-wrap div.stButton > button:hover,
    .stock-del-inline-wrap div.stButton > button:focus,
    .stock-del-inline-wrap div.stButton > button:active {
        background: transparent !important;
        background-color: transparent !important;
        background-image: none !important;
        color: rgba(255, 248, 241, 0.98) !important;
        border: none !important;
        box-shadow: none !important;
    }
    .watch-split-divider {
        min-height: 0;
        border-left: 2px solid rgba(255, 255, 255, 0.14);
        margin: 0.2rem auto 0 auto;
        width: 1px;
    }
    .group-title {
        color: rgba(255, 248, 241, 0.98);
        font-size: 1.6rem;
        font-weight: 800;
        line-height: 1.12;
        margin: 0 0 0.45rem 0;
    }
    .rsi-switch .stButton > button {
        height: 32px !important;
        padding: 0 0.45rem !important;
        border-radius: 8px !important;
        font-size: 0.9rem !important;
        font-weight: 800 !important;
    }
    .rsi-switch .stButton > button[kind="primary"] {
        background: #89addd !important;
        border: 1px solid #5f89c3 !important;
        color: #0f2a52 !important;
        box-shadow: inset 0 0 0 2px #3f6ea8 !important;
    }
    .rsi-switch-day .stButton > button { background: #dbeafe !important; color: #1e3a8a !important; border: 1px solid #93c5fd !important; }
    .rsi-switch-week .stButton > button { background: #dcfce7 !important; color: #166534 !important; border: 1px solid #86efac !important; }
    .rsi-switch-month .stButton > button { background: #fef3c7 !important; color: #92400e !important; border: 1px solid #fcd34d !important; }
    .rsi-switch-intra .stButton > button { background: #fee2e2 !important; color: #991b1b !important; border: 1px solid #fca5a5 !important; }
    .score-panel {
        border: 1px solid rgba(255,255,255,0.10);
        border-radius: 14px;
        padding: 14px 16px;
        background: linear-gradient(180deg, rgba(255,255,255,0.09), rgba(255,255,255,0.03));
        min-height: 112px;
    }
    .score-panel .label {
        color: rgba(232, 223, 210, 0.88);
        font-size: 1.02rem;
        font-weight: 700;
    }
    .score-panel .value {
        color: rgba(255, 248, 241, 0.98);
        font-size: 2.3rem;
        font-weight: 800;
        line-height: 1.25;
        margin-top: 8px;
    }
    .fnd-focus-title {
        margin: 0 0 1.35rem 0;
        color: rgba(255, 248, 241, 0.98);
        font-size: 2.35rem;
        line-height: 1.1;
        font-family: "Iowan Old Style", "Palatino Linotype", "Book Antiqua", Georgia, serif;
        font-weight: 700;
    }
    .fnd-card {
        border: 1px solid rgba(255,255,255,0.10);
        border-radius: 14px;
        padding: 12px 14px;
        min-height: 208px;
        background: linear-gradient(180deg, rgba(255,255,255,0.09), rgba(255,255,255,0.03));
        display: flex;
        flex-direction: column;
    }
    .fnd-card h4 {
        margin: 0 0 6px 0;
        font-size: 1.45rem;
        color: rgba(255, 248, 241, 0.98);
    }
    .fnd-card .score {
        font-size: 1.75rem;
        font-weight: 800;
        margin: 2px 0 6px 0;
        color: rgba(255, 248, 241, 0.98);
    }
    .fnd-card .desc {
        color: rgba(232, 223, 210, 0.88);
        font-size: 1.0rem;
        line-height: 1.42;
        min-height: 4.26em;
    }
    .fnd-card .desc .line {
        display: block;
        min-height: 1.42em;
    }
    .fnd-card .desc .line-empty {
        visibility: hidden;
    }
    .fnd-overview-head {
        color: rgba(230, 221, 208, 0.72);
        font-size: 1rem;
        font-weight: 700;
        padding: 0 0 0.45rem 0;
    }
    .fnd-overview-cell {
        color: rgba(246, 239, 229, 0.94);
        font-size: 1rem;
        line-height: 1.2;
        padding-top: 0.45rem;
    }
    .fnd-overview-cell.is-muted {
        color: rgba(222, 214, 202, 0.78);
    }
    .fnd-overview-row-divider {
        height: 1px;
        background: rgba(255,255,255,0.10);
        margin: 0;
    }
    [class*="st-key-tr_fnd_name_wrap_"] {
        padding-top: 0.35rem;
    }
    [class*="st-key-tr_fnd_name_wrap_"] div.stButton {
        margin: 0 !important;
    }
    [class*="st-key-tr_fnd_name_wrap_"] div.stButton > button,
    [class*="st-key-tr_fnd_name_wrap_"] div.stButton > button:hover,
    [class*="st-key-tr_fnd_name_wrap_"] div.stButton > button:focus,
    [class*="st-key-tr_fnd_name_wrap_"] div.stButton > button:active {
        min-height: 1.9rem !important;
        height: 1.9rem !important;
        padding: 0 1rem !important;
        border-radius: 999px !important;
        border: 1px solid rgba(196, 235, 232, 0.72) !important;
        background: linear-gradient(180deg, rgba(139, 198, 194, 0.98), rgba(101, 160, 157, 1)) !important;
        box-shadow: inset 0 1px 0 rgba(255,255,255,0.10), 0 8px 18px rgba(0,0,0,0.10) !important;
        justify-content: center !important;
        color: #122033 !important;
        -webkit-text-fill-color: #122033 !important;
        font-size: 1rem !important;
        font-weight: 800 !important;
    }
    [class*="st-key-tr_fnd_name_wrap_"] div.stButton > button span,
    [class*="st-key-tr_fnd_name_wrap_"] div.stButton > button p,
    [class*="st-key-tr_fnd_name_wrap_"] div.stButton > button div {
        color: #122033 !important;
        -webkit-text-fill-color: #122033 !important;
    }
    [class*="st-key-tr_fnd_name_wrap_active_"] div.stButton > button {
        border-color: rgba(232, 246, 244, 0.92) !important;
        box-shadow: inset 0 1px 0 rgba(255,255,255,0.14), 0 0 0 1px rgba(255,255,255,0.10), 0 10px 20px rgba(0,0,0,0.12) !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

init_db()

if "active_page" not in st.session_state:
    st.session_state["active_page"] = "trading"
if "flt_cfg" not in st.session_state:
    st.session_state["flt_cfg"] = filter_default_filter_config()
if "flt_result" not in st.session_state:
    st.session_state["flt_result"] = None
if "flt_show_ops_panel" not in st.session_state:
    st.session_state["flt_show_ops_panel"] = False

st.sidebar.markdown("---")
st.sidebar.subheader("股票池管理")
with st.sidebar.form("tr_stock_pool_add_form", clear_on_submit=True):
    new_query = st.text_input(
        "新增股票（代码或名称）", value="", placeholder="例如 600036 / 00700 / 腾讯控股"
    )
    add_cols = st.columns(2)
    add_holding = add_cols[0].form_submit_button("加入持仓", width="stretch")
    add_watch = add_cols[1].form_submit_button("加入观察", width="stretch")
if add_holding or add_watch:
    pool_group = "holding" if add_holding else "watch"
    group_text = "持仓" if pool_group == "holding" else "观察"
    try:
        code, name = add_stock_by_query(new_query, pool_group=pool_group)
        seed_fundamental_from_local_filter(code, name)
        st.session_state["fast_selected_code"] = code
        st.session_state["fast_selected_name"] = name
        st.session_state["fast_recently_added_code"] = code
        st.session_state["fast_auto_fetch_after_add"] = True
        st.session_state["fast_refresh_message"] = f"正在获取 {name} ({code}) 的实时盘口..."
        st.session_state.pop(f"fast_panel_cache_{code}", None)
        st.sidebar.success(f"已加入{group_text}: {code} - {name}")
        st.rerun()
    except Exception as exc:
        st.sidebar.error(f"添加失败: {exc}")

pool_rows_for_sidebar = get_stock_pool()
pool_name_map = {code: name for code, name in pool_rows_for_sidebar}
if pool_rows_for_sidebar:
    remove_code = st.sidebar.selectbox(
        "删除股票",
        options=[code for code, _ in pool_rows_for_sidebar],
        format_func=lambda c: f"{pool_name_map.get(c, c)} ({c})",
    )
    if st.sidebar.button("删除选中", width="stretch"):
        remove_stock_from_pool(remove_code)
        if st.session_state.get("fast_selected_code") == remove_code:
            st.session_state.pop("fast_selected_code", None)
            st.session_state.pop("fast_selected_name", None)
        st.rerun()

st.sidebar.markdown("---")
st.sidebar.subheader("DeepSeek API")
if "_prefs_loaded" not in st.session_state:
    _prefs = _load_local_prefs()
    if "deepseek_user_input" not in st.session_state:
        st.session_state["deepseek_user_input"] = _prefs.get("deepseek_user", "")
    if "deepseek_api_key_input" not in st.session_state:
        st.session_state["deepseek_api_key_input"] = _prefs.get("deepseek_api_key", "")
    st.session_state["_last_saved_prefs"] = {
        "deepseek_user": st.session_state.get("deepseek_user_input", ""),
        "deepseek_api_key": st.session_state.get("deepseek_api_key_input", ""),
    }
    st.session_state["_prefs_loaded"] = True

analysis_user_input = st.sidebar.text_input(
    "用户名（用于区分不同使用者）",
    key="deepseek_user_input",
)
analysis_api_key_input = st.sidebar.text_input(
    "API Key（可留空，读取环境变量）",
    type="password",
    key="deepseek_api_key_input",
)

_curr_user = (analysis_user_input or "").strip()
_curr_key = (analysis_api_key_input or "").strip()
_last = st.session_state.get("_last_saved_prefs", {})
if _curr_user != _last.get("deepseek_user", "") or _curr_key != _last.get("deepseek_api_key", ""):
    _save_local_prefs(_curr_user, _curr_key)
    st.session_state["_last_saved_prefs"] = {
        "deepseek_user": _curr_user,
        "deepseek_api_key": _curr_key,
    }

_allowed_pages = {"trading", "fundamental", "filter", "portfolio", "backtest", "paper"}
try:
    _qp_page = str(st.query_params.get("page", "")).strip().lower()
except Exception:
    _qp_page = ""
if _qp_page not in _allowed_pages:
    _qp_page = "trading"
if st.session_state.get("active_page") not in _allowed_pages:
    st.session_state["active_page"] = _qp_page

_active_page = st.session_state.get("active_page", "trading")
_nav_selected = render_top_nav(_active_page)
if _nav_selected != _active_page:
    st.session_state["active_page"] = _nav_selected
    try:
        st.query_params["page"] = _nav_selected
    except Exception:
        pass
    st.rerun()
_active_page = st.session_state.get("active_page", "trading")
try:
    if str(st.query_params.get("page", "")).strip().lower() != _active_page:
        st.query_params["page"] = _active_page
except Exception:
    pass

_group_map = get_stock_group_map()
_holding_count = sum(1 for code, _name in pool_rows_for_sidebar if _group_map.get(code) == "holding")
_watch_count = sum(1 for code, _name in pool_rows_for_sidebar if _group_map.get(code) != "holding")
render_app_shell(
    _active_page,
    version={
        "fundamental": FUND_APP_VERSION,
        "trading": APP_VERSION,
        "filter": FILTER_APP_VERSION,
        "portfolio": PORTFOLIO_APP_VERSION,
        "backtest": BACKTEST_APP_VERSION,
        "paper": PAPER_APP_VERSION,
    }.get(_active_page, APP_VERSION),
    badges={
        "fundamental": ("八维评分", "观察名单", "结构化结论"),
        "trading": ("实时盘口", "分时结构", "DeepSeek 分析"),
        "filter": ("两段筛选", "快照更新", "结果导出"),
        "portfolio": ("持仓台账", "浮盈亏", "ATR风控"),
        "backtest": ("多空回测", "成本建模", "HTML报告"),
        "paper": ("策略入口", "逐日更新", "模拟看板"),
    }.get(_active_page, ("实时盘口", "分时结构", "DeepSeek 分析")),
    metrics={
        "fundamental": (
            ("当前关注", f"{len(pool_rows_for_sidebar)} 只"),
            ("研究视角", "评分 + 文本 + AI"),
            ("池子结构", f"持仓 {_holding_count} / 观察 {_watch_count}"),
        ),
        "trading": (
            ("当前关注", f"{len(pool_rows_for_sidebar)} 只"),
            ("池子结构", f"持仓 {_holding_count} / 观察 {_watch_count}"),
            ("工作流", "盘口 -> 研判 -> 决策"),
        ),
        "filter": (
            ("股票池联动", f"{len(pool_rows_for_sidebar)} 只"),
            ("筛选方式", "两段排雷"),
            ("工作流", "快照 -> 条件 -> 导出"),
        ),
        "portfolio": (
            ("风险口径", "单笔 1%"),
            ("组合对象", "真实持仓"),
            ("工作流", "录入 -> 监控 -> 调整"),
        ),
        "backtest": (
            ("策略配置", "YAML可编辑"),
            ("回测区间", "2021-01-01 -> today"),
            ("工作流", "更新数据 -> 执行 -> 复盘"),
        ),
        "paper": (
            ("执行模式", "逐日模拟"),
            ("交易日口径", "港股实际交易日"),
            ("工作流", "启动 -> 更新 -> 看板"),
        ),
    }.get(
        _active_page,
        (
            ("当前关注", f"{len(pool_rows_for_sidebar)} 只"),
            ("池子结构", f"持仓 {_holding_count} / 观察 {_watch_count}"),
            ("工作流", "盘口 -> 研判 -> 决策"),
        ),
    ),
    show_hero=_active_page != "filter",
)

rows = get_latest_fundamental_snapshot()
if st.session_state.get("active_page") == "trading" and not rows:
    st.info("数据库暂无慢引擎快照，请先在左侧添加股票。")
    st.stop()

def _format_display_time(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    text = str(v).strip()
    if not text:
        return None
    dt = pd.to_datetime(text, errors="coerce")
    if pd.isna(dt):
        dt = pd.to_datetime(text, format="%Y%m%d%H%M%S", errors="coerce")
    if pd.isna(dt):
        return None
    return dt.strftime("%m-%d %H:%M:%S")


def _json_safe(v):
    if isinstance(v, dict):
        return {k: _json_safe(val) for k, val in v.items()}
    if isinstance(v, (list, tuple)):
        return [_json_safe(x) for x in v]
    if isinstance(v, pd.DataFrame):
        return _json_safe(v.to_dict(orient="records"))
    if isinstance(v, pd.Series):
        return _json_safe(v.to_dict())
    if isinstance(v, datetime):
        return v.isoformat(timespec="seconds")
    if isinstance(v, pd.Timestamp):
        return v.isoformat()
    if hasattr(v, "item"):
        try:
            return _json_safe(v.item())
        except Exception:
            pass
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    return v


def _build_analysis_payload(payload: dict) -> dict:
    """构造给 DeepSeek 的精简快照，尽量压缩 token。"""
    safe = _json_safe(payload)
    slow = safe.get("slow_engine", {}) or {}
    fe = safe.get("fast_engine", {}) or {}
    quote = fe.get("quote", {}) or {}
    compact = dict(fe.get("compact_metrics", {}) or {})
    compact.pop("cards_snapshot", None)

    out = {
        "meta": {
            "generated_at": (safe.get("meta", {}) or {}).get("generated_at"),
            "app": (safe.get("meta", {}) or {}).get("app"),
            "analysis_user": (safe.get("meta", {}) or {}).get("analysis_user"),
        },
        "stock": safe.get("stock", {}),
        "slow_engine": {
            "date": slow.get("date"),
            "pe_dynamic": slow.get("pe_dynamic"),
            "pe_static": slow.get("pe_static"),
            "pe_rolling": slow.get("pe_rolling"),
            "pb": slow.get("pb"),
            "dividend_yield": slow.get("dividend_yield"),
            "boll_index": slow.get("boll_index"),
        },
        "fast_engine": {},
    }

    out["fast_engine"]["quote"] = {
        "current_price": quote.get("current_price"),
        "change_pct": quote.get("change_pct"),
        "change_amount": quote.get("change_amount"),
        "open": quote.get("open"),
        "prev_close": quote.get("prev_close"),
        "high": quote.get("high"),
        "low": quote.get("low"),
        "volume": quote.get("volume"),
        "amount": quote.get("amount"),
        "turnover_rate": quote.get("turnover_rate"),
        "volume_ratio": quote.get("volume_ratio"),
        "vwap": quote.get("vwap"),
        "premium_pct": quote.get("premium_pct"),
        "quote_time": quote.get("quote_time"),
    }
    out["fast_engine"]["compact_metrics"] = compact
    out["fast_engine"]["order_book_5"] = fe.get("order_book_5")
    out["fast_engine"]["depth_note"] = fe.get("depth_note")
    out["fast_engine"]["error"] = fe.get("error")

    intraday = fe.get("intraday", [])
    if isinstance(intraday, list):
        # 仅保留最近24条，压缩 token
        out["fast_engine"]["intraday_recent"] = intraday[-24:]
        out["fast_engine"]["intraday_count"] = len(intraday)
    return out


def _build_quick_payload(payload: dict, stock_code: str) -> dict:
    safe = _json_safe(payload)
    fe = safe.get("fast_engine", {}) or {}
    compact = fe.get("compact_metrics", {}) or {}
    snap = {
        "price": (compact.get("snapshot", {}) or {}).get("current_price"),
        "change_pct": (compact.get("snapshot", {}) or {}).get("change_pct"),
        "high": (compact.get("snapshot", {}) or {}).get("high"),
        "low": (compact.get("snapshot", {}) or {}).get("low"),
        "volume": (compact.get("trading", {}) or {}).get("volume"),
        "amount": (compact.get("trading", {}) or {}).get("amount"),
        "volume_ratio": (compact.get("trading", {}) or {}).get("volume_ratio"),
        "turnover_rate": (compact.get("trading", {}) or {}).get("turnover_rate"),
        "vwap": (compact.get("trading", {}) or {}).get("vwap"),
        "premium_pct": (compact.get("trading", {}) or {}).get("premium_pct"),
        "amplitude_pct": (compact.get("trading", {}) or {}).get("amplitude_pct"),
        "imbalance_bid_ask": (compact.get("order_book_summary", {}) or {}).get("imbalance_bid_ask"),
        "spread": (compact.get("order_book_summary", {}) or {}).get("spread"),
        "order_diff": (compact.get("order_book_summary", {}) or {}).get("order_diff"),
        "pe_dynamic": (compact.get("valuation", {}) or {}).get("pe_dynamic"),
        "pe_rolling": (compact.get("valuation", {}) or {}).get("pe_rolling"),
        "pb": (compact.get("valuation", {}) or {}).get("pb"),
        "dividend_yield": (compact.get("valuation", {}) or {}).get("dividend_yield"),
        "rsi6": (compact.get("technical", {}) or {}).get("rsi6"),
        "macd_hist": (compact.get("technical", {}) or {}).get("macd_hist"),
    }
    snap = _json_safe(snap)

    delta_cache = _load_json_file(ANALYSIS_DELTA_CACHE_PATH)
    prev_snap = delta_cache.get(str(stock_code), {})
    delta = _dict_delta(snap, prev_snap) or {}
    delta_cache[str(stock_code)] = snap
    _save_json_file(ANALYSIS_DELTA_CACHE_PATH, delta_cache)

    return {
        "meta": safe.get("meta", {}),
        "stock": safe.get("stock", {}),
        "snapshot": snap,
        "delta": delta,
        "has_delta": bool(delta),
        "delta_keys": list(delta.keys()) if isinstance(delta, dict) else [],
    }


def _trigger_rules(analysis_payload: dict, quick_struct: dict) -> dict:
    fe = ((analysis_payload or {}).get("fast_engine", {}) or {}).get("compact_metrics", {}) or {}
    snapshot = fe.get("snapshot", {}) or {}
    trading = fe.get("trading", {}) or {}
    valuation = fe.get("valuation", {}) or {}
    tech = fe.get("technical", {}) or {}
    ob = fe.get("order_book_summary", {}) or {}

    risk = str((quick_struct or {}).get("risk_level", "")).lower()
    quick_need = bool((quick_struct or {}).get("need_full_analysis", False))
    premium = trading.get("premium_pct")
    imbalance = ob.get("imbalance_bid_ask")
    pe_dyn = valuation.get("pe_dynamic")
    dy = valuation.get("dividend_yield")
    rsi6 = tech.get("rsi6")
    macd = tech.get("macd_hist")
    chg = snapshot.get("change_pct")

    cond_risk = risk in {"medium", "high"}
    cond_conflict = (
        (pe_dyn is not None and pe_dyn <= 15 and ((rsi6 is not None and rsi6 < 35) or (macd is not None and macd < 0)))
        or (dy is not None and dy >= 4.0 and rsi6 is not None and rsi6 < 35)
    )
    cond_vwap = premium is not None and abs(float(premium)) >= 1.5
    cond_ob = imbalance is not None and (float(imbalance) >= 2.2 or float(imbalance) <= 0.45)
    cond_jump = chg is not None and abs(float(chg)) >= 3.0

    reasons = []
    if quick_need:
        reasons.append("快筛建议深析")
    if cond_risk:
        reasons.append(f"风险等级={risk or 'unknown'}")
    if cond_conflict:
        reasons.append("估值与动量冲突")
    if cond_vwap:
        reasons.append("现价偏离VWAP过大")
    if cond_ob:
        reasons.append("盘口失衡异常")
    if cond_jump:
        reasons.append("涨跌幅波动较大")

    should_deep = bool(reasons)
    return {"should_deep": should_deep, "reasons": reasons}


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
            # 没有 secrets.toml 时，Streamlit 会抛出异常；这里静默回退到环境变量
            pass
    if not raw:
        raw = os.getenv("DEEPSEEK_API_KEY", "")

    key = raw.strip().split()[0] if raw.strip() else ""
    # 常见粘贴错误：中文引号/括号/注释混入
    key = key.strip("“”\"'`")
    return key


def _validate_api_key(key: str) -> None:
    if not key:
        raise RuntimeError("未配置 DEEPSEEK_API_KEY。请在侧栏填写，或设置环境变量 DEEPSEEK_API_KEY。")
    if not key.startswith("sk-"):
        raise RuntimeError("API Key 格式异常：应以 sk- 开头。")
    if not re.fullmatch(r"sk-[A-Za-z0-9._-]+", key):
        raise RuntimeError("API Key 包含非法字符（可能混入中文符号/空格）。请重新粘贴纯 key。")


def _run_async(coro):
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    import threading

    box: Dict[str, Any] = {"value": None, "error": None}

    def _runner() -> None:
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            box["value"] = loop.run_until_complete(coro)
        except Exception as exc:  # pragma: no cover
            box["error"] = exc
        finally:
            loop.close()

    th = threading.Thread(target=_runner, daemon=True)
    th.start()
    th.join()
    if box["error"] is not None:
        raise box["error"]
    return box["value"]


def _call_deepseek_with_prompt(
    user_content: str,
    system_prompt: str,
    max_tokens: int = 1500,
    temperature: float = 0.3,
    top_p: float = 0.9,
) -> tuple[str, dict, float, float]:
    if (not OPENAI_AVAILABLE) or (OpenAI is None):
        hint = "缺少 openai 依赖，请先安装：cd /Users/wellthen/Desktop/TEST/Quant_System/apps/trading && source venv/bin/activate && pip install -r requirements.txt"
        if OPENAI_IMPORT_ERROR is not None:
            raise RuntimeError(f"{hint}；原始错误: {OPENAI_IMPORT_ERROR}")
        raise RuntimeError(hint)

    api_key = _resolve_deepseek_api_key()
    _validate_api_key(api_key)
    client = OpenAI(api_key=api_key, base_url="https://api.deepseek.com/v1", timeout=60.0, max_retries=0)

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_content},
    ]
    t0 = pytime.time()
    last_conn_error = None
    response = None

    for attempt in range(1, 5):
        try:
            response = client.chat.completions.create(
                model="deepseek-chat",
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
                top_p=top_p,
            )
            break
        except (APIConnectionError, APITimeoutError) as exc:
            last_conn_error = exc
            if attempt < 4:
                pytime.sleep(0.8 * attempt)
                continue
        except Exception:
            raise

    if response is None:
        # 兜底直连请求，规避 SDK 在个别网络环境下的连接异常
        url = "https://api.deepseek.com/v1/chat/completions"
        payload = {
            "model": "deepseek-chat",
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "top_p": top_p,
        }
        try:
            session = requests.Session()
            retry = Retry(
                total=4,
                connect=4,
                read=4,
                backoff_factor=0.8,
                status_forcelist=(429, 500, 502, 503, 504),
                allowed_methods=frozenset(["POST"]),
                raise_on_status=False,
            )
            adapter = HTTPAdapter(max_retries=retry)
            session.mount("https://", adapter)
            r = session.post(
                url,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                    "Connection": "close",
                    "Accept-Encoding": "identity",
                },
                json=payload,
                timeout=(20, 90),
            )
            r.raise_for_status()
            raw = r.json()
        except requests.exceptions.RequestException as req_exc:
            raise RuntimeError(f"网络重试与直连兜底均失败: {req_exc}; 上次SDK异常: {last_conn_error}") from req_exc

        report = (((raw.get("choices") or [{}])[0].get("message") or {}).get("content") or "").strip()
        if not report:
            raise RuntimeError("DeepSeek 未返回有效分析内容（直连兜底）")

        usage_raw = raw.get("usage") or {}
        cache_hit_tokens = int(usage_raw.get("prompt_cache_hit_tokens") or 0)
        cache_miss_tokens = int(usage_raw.get("prompt_cache_miss_tokens") or 0)
        completion_tokens = int(usage_raw.get("completion_tokens") or 0)
        prompt_tokens = int(usage_raw.get("prompt_tokens") or 0)
        elapsed = pytime.time() - t0
    else:
        elapsed = pytime.time() - t0

        usage = response.usage
        cache_hit_tokens = getattr(usage, "prompt_cache_hit_tokens", 0) or 0
        cache_miss_tokens = getattr(usage, "prompt_cache_miss_tokens", 0) or 0
        completion_tokens = getattr(usage, "completion_tokens", 0) or 0
        prompt_tokens = getattr(usage, "prompt_tokens", 0) or 0
        report = (response.choices[0].message.content or "").strip()

    cost = (
        cache_hit_tokens / 1_000_000 * 0.028
        + cache_miss_tokens / 1_000_000 * 0.28
        + completion_tokens / 1_000_000 * 0.42
    )

    if not report:
        raise RuntimeError("DeepSeek 未返回有效分析内容")

    usage_dict = {
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": (prompt_tokens + completion_tokens),
        "prompt_cache_hit_tokens": cache_hit_tokens,
        "prompt_cache_miss_tokens": cache_miss_tokens,
    }
    return report, usage_dict, cost, elapsed


BACKTEST_AI_STRATEGY_SYSTEM_PROMPT = """你是港股多空回测策略生成器。
任务：根据用户自然语言需求，生成一份可直接落库的 YAML 策略草案。

硬约束：
1. 只输出 YAML，禁止 Markdown 代码块，禁止解释文字。
2. 只能使用候选股票池中明确给出的股票代码。
3. long_positions 与 short_positions 权重和都必须等于 1.0。
4. capital.long_pct + capital.short_pct + capital.cash_buffer_pct 必须等于 1.0。
5. rebalance.frequency 只能是 daily/weekly/monthly/quarterly。
6. 必须同时包含多头和空头，且都不能为空。
7. market 固定为港股，不要输出 A 股或美股代码。
8. 优先生成“能运行”的保守草案，不要追求花哨。

输出字段结构固定为：
strategy_name
description
sector
backtest.start_date
backtest.end_date
capital.total
capital.rmb_to_hkd_rate
capital.long_pct
capital.short_pct
capital.cash_buffer_pct
long_positions[].code
long_positions[].weight
short_positions[].code
short_positions[].weight
weighting_mode
rebalance.frequency
rebalance.day
costs.commission_rate
costs.slippage
costs.short_borrow_rate
stop_loss.single_long_stop
stop_loss.single_long_action
stop_loss.single_short_stop
stop_loss.single_short_action
stop_loss.portfolio_stop
stop_loss.portfolio_action
sensitivity.borrow_rates

默认偏好：
- capital.total = 1000000
- capital.rmb_to_hkd_rate = 1.0
- weighting_mode = manual
- end_date = today
- 不确定时使用 monthly 调仓
- 不确定时多头 0.65 / 空头 0.20 / 现金 0.15
- 不确定时 stop_loss 采用现有策略中的保守参数
"""


def _bt_strip_yaml_block(text: str) -> str:
    s = (text or "").strip()
    if s.startswith("```"):
        s = re.sub(r"^```(?:yaml|yml)?\s*", "", s, flags=re.IGNORECASE)
        s = re.sub(r"\s*```$", "", s)
    return s.strip()


def _bt_normalize_strategy_yaml_text(strategy_text: str) -> str:
    text = _bt_strip_yaml_block(strategy_text)
    if not text:
        return ""
    try:
        raw = yaml.safe_load(text) or {}
    except Exception:
        return text
    if not isinstance(raw, dict):
        return text

    raw["strategy_name"] = str(raw.get("strategy_name", "")).strip() or "AI策略草案"
    raw["description"] = str(raw.get("description", "")).strip() or "由 AI 根据自然语言需求生成的港股多空策略草案"
    raw["sector"] = str(raw.get("sector", "") or "").strip()

    backtest = raw.get("backtest") or {}
    if not isinstance(backtest, dict):
        backtest = {}
    backtest["start_date"] = str(backtest.get("start_date", "")).strip() or "2023-01-01"
    backtest["end_date"] = str(backtest.get("end_date", "")).strip() or "today"
    raw["backtest"] = backtest

    capital = raw.get("capital") or {}
    if not isinstance(capital, dict):
        capital = {}
    capital["total"] = float(capital.get("total", 1000000) or 1000000)
    capital["rmb_to_hkd_rate"] = float(capital.get("rmb_to_hkd_rate", 1.0) or 1.0)
    capital["long_pct"] = float(capital.get("long_pct", 0.65) or 0.65)
    capital["short_pct"] = float(capital.get("short_pct", 0.20) or 0.20)
    capital["cash_buffer_pct"] = float(capital.get("cash_buffer_pct", 0.15) or 0.15)
    raw["capital"] = capital

    raw["weighting_mode"] = str(raw.get("weighting_mode", "manual")).strip().lower() or "manual"

    rebalance = raw.get("rebalance") or {}
    if not isinstance(rebalance, dict):
        rebalance = {}
    rebalance["frequency"] = str(rebalance.get("frequency", "monthly")).strip().lower() or "monthly"
    rebalance["day"] = int(rebalance.get("day", 1) or 1)
    raw["rebalance"] = rebalance

    costs = raw.get("costs") or {}
    if not isinstance(costs, dict):
        costs = {}
    costs["commission_rate"] = float(costs.get("commission_rate", 0.0015) or 0.0015)
    costs["slippage"] = float(costs.get("slippage", 0.001) or 0.001)
    costs["short_borrow_rate"] = float(costs.get("short_borrow_rate", 0.08) or 0.08)
    raw["costs"] = costs

    stop_loss = raw.get("stop_loss") or {}
    if not isinstance(stop_loss, dict):
        stop_loss = {}
    stop_loss["single_long_stop"] = float(stop_loss.get("single_long_stop", -0.20) or -0.20)
    stop_loss["single_long_action"] = str(stop_loss.get("single_long_action", "halve")).strip().lower() or "halve"
    stop_loss["single_short_stop"] = float(stop_loss.get("single_short_stop", 0.30) or 0.30)
    stop_loss["single_short_action"] = str(stop_loss.get("single_short_action", "close")).strip().lower() or "close"
    stop_loss["portfolio_stop"] = float(stop_loss.get("portfolio_stop", -0.12) or -0.12)
    stop_loss["portfolio_action"] = str(stop_loss.get("portfolio_action", "close_all")).strip().lower() or "close_all"
    raw["stop_loss"] = stop_loss

    sensitivity = raw.get("sensitivity") or {}
    if not isinstance(sensitivity, dict):
        sensitivity = {}
    borrow_rates = sensitivity.get("borrow_rates", [0.03, 0.05, 0.08, 0.12])
    if not isinstance(borrow_rates, list) or not borrow_rates:
        borrow_rates = [0.03, 0.05, 0.08, 0.12]
    sensitivity["borrow_rates"] = [float(x) for x in borrow_rates]
    raw["sensitivity"] = sensitivity

    return yaml.safe_dump(raw, allow_unicode=True, sort_keys=False)


def _bt_load_universe_prompt_text() -> str:
    universe_path = BACKTEST_DIR / "config" / "universe.yaml"
    raw = yaml.safe_load(universe_path.read_text(encoding="utf-8")) or {}
    sectors = raw.get("sectors") or {}
    chunks: list[str] = []
    for sec_key, sec_obj in sectors.items():
        if not isinstance(sec_obj, dict):
            continue
        sec_name = str(sec_obj.get("name", sec_key)).strip()
        groups = sec_obj.get("groups") or {}
        parts: list[str] = []
        for grp_key, grp_obj in groups.items():
            if not isinstance(grp_obj, dict):
                continue
            grp_name = str(grp_obj.get("name", grp_key)).strip()
            stocks = grp_obj.get("stocks") or []
            stock_txt = []
            for st_obj in stocks:
                if not isinstance(st_obj, dict):
                    continue
                code = str(st_obj.get("code", "")).strip()
                name = str(st_obj.get("name", "")).strip()
                tags = st_obj.get("tags") or []
                tag_txt = f"（{'/'.join(str(t) for t in tags[:3])}）" if tags else ""
                if code and name:
                    stock_txt.append(f"{code} {name}{tag_txt}")
            if stock_txt:
                parts.append(f"- {grp_name}: " + "；".join(stock_txt))
        if parts:
            chunks.append(f"[{sec_name}]\n" + "\n".join(parts))
    return "\n\n".join(chunks)


def _bt_validate_strategy_yaml_text(strategy_text: str) -> tuple[bool, str]:
    text = _bt_normalize_strategy_yaml_text(strategy_text)
    if not text:
        return False, "AI 未生成有效 YAML。"
    universe_path = BACKTEST_DIR / "config" / "universe.yaml"
    try:
        with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False, encoding="utf-8") as tf:
            tf.write(text)
            temp_path = Path(tf.name)
        universe = backtest_load_universe(universe_path)
        backtest_load_strategy(temp_path, universe)
        return True, ""
    except BacktestConfigError as exc:
        return False, f"策略校验失败: {exc}"
    except Exception as exc:
        return False, f"策略解析失败: {exc}"
    finally:
        try:
            if 'temp_path' in locals() and temp_path.exists():
                temp_path.unlink()
        except Exception:
            pass


def _bt_generate_ai_strategy_draft(user_prompt: str, template_name: str = "") -> dict:
    prompt = (user_prompt or "").strip()
    if not prompt:
        raise RuntimeError("策略描述为空。")
    universe_text = _bt_load_universe_prompt_text()
    template_block = ""
    if template_name and template_name != "(不使用模板)":
        tp = BACKTEST_STRATEGY_DIR / template_name
        if tp.exists():
            template_block = f"\n\n参考模板（可借鉴风格，但不要照抄）：\n{tp.read_text(encoding='utf-8', errors='replace')}\n"
    user_content = f"""用户需求：
{prompt}

候选股票池（只能从这里选）：
{universe_text}
{template_block}

请直接输出 YAML 草案。
"""
    report, usage, cost, elapsed = _call_deepseek_with_prompt(
        user_content=user_content,
        system_prompt=BACKTEST_AI_STRATEGY_SYSTEM_PROMPT,
        max_tokens=2200,
        temperature=0.2,
        top_p=0.9,
    )
    yaml_text = _bt_normalize_strategy_yaml_text(report)
    ok, err = _bt_validate_strategy_yaml_text(yaml_text)
    parsed = yaml.safe_load(yaml_text) if ok else {}
    return {
        "yaml_text": yaml_text,
        "valid": ok,
        "error": err,
        "usage": usage,
        "cost": cost,
        "elapsed": elapsed,
        "strategy_name": str((parsed or {}).get("strategy_name", "")).strip() if isinstance(parsed, dict) else "",
        "description": str((parsed or {}).get("description", "")).strip() if isinstance(parsed, dict) else "",
    }


def _call_deepseek_analysis(json_text: str) -> tuple[str, dict, float, float]:
    return _call_deepseek_with_prompt(
        user_content=json_text,
        system_prompt=DEEPSEEK_SYSTEM_PROMPT,
        max_tokens=1500,
        temperature=0.3,
        top_p=0.9,
    )


def _call_multi_agent_analysis(json_text: str, stock_code: str, stock_name: str) -> tuple[str, dict, float, float]:
    if (not OPENAI_AVAILABLE) or (OpenAI is None):
        hint = "缺少 openai 依赖，请先安装：cd /Users/wellthen/Desktop/TEST/Quant_System/apps/trading && source venv/bin/activate && pip install -r requirements.txt"
        if OPENAI_IMPORT_ERROR is not None:
            raise RuntimeError(f"{hint}；原始错误: {OPENAI_IMPORT_ERROR}")
        raise RuntimeError(hint)

    api_key = _resolve_deepseek_api_key()
    _validate_api_key(api_key)
    t0 = pytime.time()
    analyzer = MultiAgentAnalyzer(
        api_key=api_key,
        base_url="https://api.deepseek.com/v1",
        model="deepseek-chat",
        timeout_sec=90.0,
        max_retries=0,
    )
    result = _run_async(
        analyzer.analyze(
            payload_json=json_text,
            stock_code=str(stock_code or ""),
            stock_name=str(stock_name or ""),
        )
    )
    final_report = str(result.get("final_markdown", "") or "").strip()
    if not final_report:
        raise RuntimeError("多智能体分析未返回有效文本")

    usage_total = (result.get("usage") or {}).copy()
    usage_breakdown = result.get("usage_breakdown") or {}
    expert_usage = usage_breakdown.get("experts") or {}
    judge_usage = usage_breakdown.get("judge") or {}
    usage_total["expert_prompt_tokens"] = int(expert_usage.get("prompt_tokens", 0) or 0)
    usage_total["expert_completion_tokens"] = int(expert_usage.get("completion_tokens", 0) or 0)
    usage_total["judge_prompt_tokens"] = int(judge_usage.get("prompt_tokens", 0) or 0)
    usage_total["judge_completion_tokens"] = int(judge_usage.get("completion_tokens", 0) or 0)
    cost = float(result.get("cost", 0.0) or 0.0)
    elapsed = float(result.get("elapsed", 0.0) or (pytime.time() - t0))
    return final_report, usage_total, cost, elapsed


def _sanitize_deepseek_report(text: str) -> str:
    s = str(text or "").strip()
    if not s:
        return s

    # 去掉模型常见抬头（标题型）
    s = re.sub(r"^\s*#{1,6}\s*DeepSeek[^\n]*\n+", "", s, flags=re.IGNORECASE)
    s = re.sub(r"^\s*DeepSeek[^\n]*\n+", "", s, flags=re.IGNORECASE)

    # 去掉常见开场客套句
    s = re.sub(
        r"^\s*(好的|当然|明白了|收到)[，,。!\s]*作为[^\n。！？]*[。！？]?\s*",
        "",
        s,
        flags=re.IGNORECASE,
    )

    # 若仍以“我将/我会...进行分析”开头，继续剥离一次
    s = re.sub(r"^\s*我将[^\n。！？]*[。！？]?\s*", "", s)
    s = re.sub(r"^\s*我会[^\n。！？]*[。！？]?\s*", "", s)

    return s.strip()


def _call_deepseek_fundamental(json_text: str) -> tuple[str, dict, float, float]:
    return _call_deepseek_with_prompt(
        user_content=json_text,
        system_prompt=FUND_DEEPSEEK_PROMPT,
        max_tokens=1200,
        temperature=0.3,
        top_p=0.9,
    )


def _clean_text_no_na(text: str) -> str:
    s = str(text or "")
    for bad in ["N/A", "n/a", "nan", "None", "--", "null"]:
        s = s.replace(bad, "")
    return re.sub(r"\s+", " ", s).strip(" ，。；、")


def _split_sentences(text: str):
    clean = _clean_text_no_na(text)
    if not clean:
        return []
    parts = re.split(r"[。！？；\n]+", clean)
    return [p.strip() for p in parts if p.strip()]


def _format_card_desc_lines(text: str, max_lines: int = 3) -> str:
    raw = _clean_text_no_na(text)
    if not raw:
        lines = []
    else:
        parts = re.split(r"[\/／|]+", raw)
        lines = []
        for part in parts:
            sub = _split_sentences(part)
            if sub:
                lines.extend(sub)
        lines = [x.strip() for x in lines if x.strip()]
    lines = lines[:max_lines]
    while len(lines) < max_lines:
        lines.append("")
    html_lines = []
    for one in lines:
        if one:
            html_lines.append(f"<span class='line'>{one}</span>")
        else:
            html_lines.append("<span class='line line-empty'>.</span>")
    return "".join(html_lines)


def _shared_watchlist_rows():
    pool_rows = get_stock_pool()
    group_map = get_stock_group_map()
    out = []
    for code, name in pool_rows:
        out.append(
            {
                "code": str(code),
                "name": str(name).strip() or str(code),
                "type": "持仓" if group_map.get(str(code), "watch") == "holding" else "观察",
            }
        )
    return out


def _ensure_fundamental_state(force_refresh: bool = False):
    if "fnd_deepseek_reports" not in st.session_state:
        st.session_state["fnd_deepseek_reports"] = {}
    watchlist = _shared_watchlist_rows()
    hash_text = json.dumps(watchlist, ensure_ascii=False, sort_keys=True)
    wl_hash = hashlib.md5(hash_text.encode("utf-8")).hexdigest()

    stale = (
        force_refresh
        or "fnd_rows" not in st.session_state
        or st.session_state.get("fnd_watchlist_hash", "") != wl_hash
    )
    if stale:
        st.session_state["fnd_rows"] = analyze_fundamental_watchlist(watchlist, force_refresh=force_refresh)
        st.session_state["fnd_watchlist_hash"] = wl_hash
        if st.session_state["fnd_rows"]:
            valid_codes = {str(x.get("code", "")) for x in st.session_state["fnd_rows"]}
            if st.session_state.get("fnd_selected_code") not in valid_codes:
                st.session_state["fnd_selected_code"] = st.session_state["fnd_rows"][0]["code"]
        else:
            st.session_state["fnd_selected_code"] = ""
    return watchlist, st.session_state.get("fnd_rows", [])


def _render_fundamental_page():
    st.caption(f"版本号: {FUND_APP_VERSION}")
    top_cols = st.columns([1, 5], vertical_alignment="center")
    if top_cols[0].button("刷新基本面", width="stretch", key="refresh_fundamental_now"):
        _ensure_fundamental_state(force_refresh=True)
        st.rerun()

    watchlist, rows_fnd = _ensure_fundamental_state(force_refresh=False)
    if not watchlist:
        st.warning("当前股票池为空，请先在左侧添加股票。")
        return
    if not rows_fnd:
        st.info("正在生成基本面数据，请稍后刷新。")
        return

    row = next(
        (
            x
            for x in rows_fnd
            if str(x.get("code", "")) == str(st.session_state.get("fnd_selected_code", ""))
        ),
        rows_fnd[0],
    )
    render_section_intro(
        "研究名单",
        "保持列表总览和评分板之间的切换距离足够短，让你能先扫一遍股票池，再快速下钻到单只标的。",
        kicker="Overview",
        pills=("股票池总览", "打开评分板", "支持快速切换"),
    )
    render_status_row(
        (
            ("名单规模", f"{len(rows_fnd)} 只"),
            ("当前标的", f"{row.get('name', '')} ({row.get('code', '')})"),
            ("当前结论", _clean_text_no_na(str(row.get("conclusion", "观察")))),
        )
    )
    st.subheader("股票列表")
    df = build_fundamental_overview_table(rows_fnd).copy()
    widths = [1.0, 1.15, 0.75, 0.75, 0.85, 1.35]
    header_cols = st.columns(widths, gap="small")
    for col, header in zip(header_cols, df.columns):
        col.markdown(f"<div class='fnd-overview-head'>{header}</div>", unsafe_allow_html=True)
    st.markdown("<div class='fnd-overview-row-divider'></div>", unsafe_allow_html=True)

    selected_code = str(st.session_state.get("fnd_selected_code", rows_fnd[0].get("code", "") if rows_fnd else ""))
    for row in rows_fnd:
        cols = st.columns(widths, gap="small")
        cols[0].markdown(f"<div class='fnd-overview-cell'>{row.get('code', '')}</div>", unsafe_allow_html=True)
        with cols[1]:
            wrap_key = (
                f"tr_fnd_name_wrap_active_{row.get('code', '')}"
                if str(row.get("code", "")) == selected_code
                else f"tr_fnd_name_wrap_{row.get('code', '')}"
            )
            with st.container(key=wrap_key):
                if st.button(str(row.get("name", "")), key=f"tr_fnd_name_pick_{row.get('code', '')}", type="tertiary"):
                    st.session_state["fnd_selected_code"] = row.get("code", "")
                    st.rerun()
        cols[2].markdown(f"<div class='fnd-overview-cell'>{row.get('total_score', '')}</div>", unsafe_allow_html=True)
        cols[3].markdown(f"<div class='fnd-overview-cell'>{row.get('type', '')}</div>", unsafe_allow_html=True)
        cols[4].markdown(f"<div class='fnd-overview-cell'>{format_fundamental_pct(row.get('dividend_yield'))}</div>", unsafe_allow_html=True)
        cols[5].markdown(f"<div class='fnd-overview-cell is-muted'>{row.get('analysis_at', '')}</div>", unsafe_allow_html=True)
        st.markdown("<div class='fnd-overview-row-divider'></div>", unsafe_allow_html=True)

    st.divider()
    row = next((x for x in rows_fnd if str(x.get("code", "")) == str(st.session_state.get("fnd_selected_code", ""))), rows_fnd[0])
    st.markdown(
        f"<div class='fnd-focus-title'>{row.get('name', '')}（{row.get('code', '')}）</div>",
        unsafe_allow_html=True,
    )
    score = float(row.get("total_score", 0.0) or 0.0)
    conclusion = _clean_text_no_na(str(row.get("conclusion", "观察")))
    coverage = format_fundamental_pct(float((row.get("coverage_ratio") or 0.0) * 100.0))
    sp_cols = st.columns(3, gap="small")
    for col, (label, value) in zip(sp_cols, [("总分", f"{score:.1f}"), ("结论", conclusion), ("覆盖率", coverage)]):
        col.markdown(
            f"""
<div class="score-panel">
  <div class="label">{label}</div>
  <div class="value">{value}</div>
</div>
""",
            unsafe_allow_html=True,
        )

    dims = row.get("dimensions", []) or []
    if dims:
        render_section_intro(
            "八维拆解",
            "把各维度拆成独立卡片，便于你快速发现强项、短板和需要二次验证的地方。",
            kicker="Dimensions",
            pills=("八维卡片", "便于横向比较", "统一句长"),
        )
        st.subheader("八维评分")
        for i in range(0, len(dims), 4):
            cols = st.columns(4, gap="small")
            for j, card in enumerate(dims[i : i + 4]):
                title = _clean_text_no_na(card.get("title", ""))
                score_txt = _clean_text_no_na(f"{card.get('score', 0)} / {card.get('max_score', 5)}")
                desc = _format_card_desc_lines(str(card.get("comment", "")))
                with cols[j]:
                    st.markdown(
                        f"""
<div class="fnd-card">
  <h4>{title}</h4>
  <div class="score">{score_txt}</div>
  <div class="desc">{desc}</div>
</div>
""",
                        unsafe_allow_html=True,
                    )

    st.divider()
    render_section_intro(
        "总结与输出",
        "把总结、复制 JSON 和 DeepSeek 深挖放在一起，形成研究闭环。",
        kicker="Narrative",
        pills=("总结文本", "复制 JSON", "DeepSeek 深挖"),
    )
    st.subheader("总结性文本")
    lines = _split_sentences(str(row.get("summary_text", "")))
    if lines:
        st.markdown(
            "<div style='line-height:1.8;color:rgba(242,235,225,0.94);font-size:1.05rem;'>"
            + "<br>".join(lines)
            + "</div>",
            unsafe_allow_html=True,
        )
    else:
        st.info("暂无总结。")

    code = str(row.get("code", ""))
    json_payload = json.dumps(row, ensure_ascii=False, indent=2)
    payload_hash = hashlib.sha1(json_payload.encode("utf-8")).hexdigest()[:12]
    json_b64 = base64.b64encode(json_payload.encode("utf-8")).decode("ascii")
    btn1, btn2 = st.columns([1, 1], gap="small")
    with btn1:
        html(
            f"""
            <div style="margin-top:0.1rem;">
              <button id="fnd-copy-json-{code}"
                style="width:100%;height:42px;border-radius:999px;border:1px solid rgba(255,255,255,0.14);background:linear-gradient(135deg,#d9ece7 0%,#76b6b5 100%);color:#11202f;font-size:1rem;font-weight:800;cursor:pointer;box-shadow:0 12px 24px rgba(0,0,0,0.18);">
                复制JSON
              </button>
              <div id="fnd-copy-msg-{code}" style="margin-top:0.35rem;color:rgba(239,229,216,0.82);font-size:0.86rem;"></div>
            </div>
            <script>
              const btn = document.getElementById("fnd-copy-json-{code}");
              const msg = document.getElementById("fnd-copy-msg-{code}");
              const text = decodeURIComponent(escape(window.atob("{json_b64}")));
              btn.onclick = async function () {{
                try {{
                  await navigator.clipboard.writeText(text);
                  msg.textContent = "已复制";
                }} catch(e) {{
                  msg.textContent = "复制失败，请重试";
                }}
              }};
            </script>
            """,
            height=88,
        )
    with btn2:
        if st.button("DeepSeek分析", key=f"fnd_deepseek_{code}", width="stretch"):
            progress = st.progress(0, text="正在准备分析任务...")
            pytime.sleep(0.08)
            progress.progress(35, text="正在压缩数据...")
            pytime.sleep(0.08)
            progress.progress(70, text="正在连接 DeepSeek...")
            try:
                report, usage, cost, elapsed = _call_deepseek_fundamental(json_payload)
                progress.progress(100, text="分析完成")
                pytime.sleep(0.1)
                progress.empty()
                st.session_state["fnd_deepseek_reports"][code] = {
                    "report": (report or "").strip(),
                    "usage": usage,
                    "cost": cost,
                    "elapsed": elapsed,
                    "at": datetime.now().strftime("%m-%d %H:%M:%S"),
                    "input_hash": payload_hash,
                }
            except Exception as exc:
                progress.empty()
                st.error(f"DeepSeek 分析失败: {exc}")

    deep = st.session_state.get("fnd_deepseek_reports", {}).get(code)
    if deep:
        st.divider()
        st.subheader("DeepSeek分析结果")
        st.caption(
            f"分析时间: {deep.get('at','')} ｜耗时: {deep.get('elapsed',0):.2f}s ｜"
            f"Tokens: {deep.get('usage',{}).get('total_tokens',0)} ｜"
            f"预估成本: {deep.get('cost',0):.4f} 元 ｜"
            f"数据指纹: {deep.get('input_hash', '--')}"
        )
        report_text = (deep.get("report", "") or "").strip()
        st.markdown(report_text)
        st.text_area("分析文本（可复制）", value=report_text, height=260, key=f"fnd_report_{code}")


def _has_rearview_enabled(cfg: dict) -> bool:
    d5 = cfg.get("rearview_5y", {}) if isinstance(cfg, dict) else {}
    return any(bool(v) for k, v in d5.items() if str(k).endswith("_enabled"))


def _build_stage1_config(cfg: dict) -> dict:
    out = copy.deepcopy(cfg)
    d5 = out.get("rearview_5y", {})
    for k in list(d5.keys()):
        if str(k).endswith("_enabled"):
            d5[k] = False
    return out


def _build_stage2_config(cfg: dict) -> dict:
    out = copy.deepcopy(cfg)
    risk = out.get("risk", {})
    for key in [
        "exclude_st",
        "exclude_investigation",
        "exclude_penalty",
        "exclude_fund_occupation",
        "exclude_illegal_reduce",
        "require_standard_audit",
        "exclude_sunset_industry",
        "exclude_no_dividend_5y",
        "pledge_ratio_max_enabled",
        "audit_change_max_enabled",
    ]:
        if key in risk:
            risk[key] = False

    for group in ["quality", "valuation", "growth_liquidity"]:
        g = out.get(group, {})
        for k in list(g.keys()):
            if str(k).endswith("_enabled"):
                g[k] = False
    return out


def _concat_dedup(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    if df1 is None or df1.empty:
        merged = df2.copy() if isinstance(df2, pd.DataFrame) else pd.DataFrame()
    elif df2 is None or df2.empty:
        merged = df1.copy()
    else:
        merged = pd.concat([df1, df2], ignore_index=True)
    if isinstance(merged, pd.DataFrame) and (not merged.empty) and ("code" in merged.columns):
        merged = merged.drop_duplicates(subset=["code"], keep="first")
    return merged


def _safe_str(v: object) -> str:
    return str(v).strip() if v is not None else ""


def _safe_int(v: object) -> int:
    try:
        return int(float(str(v)))
    except Exception:
        return 0


def _parse_dt_text(value: object) -> datetime | None:
    text = _safe_str(value)
    if not text:
        return None
    candidates = (
        (text[:19].replace("T", " "), "%Y-%m-%d %H:%M:%S"),
        (text[:10], "%Y-%m-%d"),
    )
    for candidate, fmt in candidates:
        try:
            return datetime.strptime(candidate, fmt)
        except Exception:
            continue
    return None


def _flt_hk_segment_health(row: dict) -> dict:
    total = max(0, int(row.get("count", 0) or 0))
    covered = max(0, int(row.get("persisted_count", 0) or 0))
    missing = max(0, total - covered)
    coverage = (covered / total) if total else 0.0
    last_dt = _parse_dt_text(row.get("last_enriched_at"))
    age_days = (datetime.now() - last_dt).days if last_dt else None
    if total <= 0:
        label, color = "无样本", "#8292a5"
    elif covered <= 0:
        label, color = "未建立", "#8292a5"
    elif coverage < 0.5:
        label, color = "覆盖不足", "#ef6461"
    elif age_days is None:
        label, color = "时间未知", "#8292a5"
    elif age_days <= 7 and coverage >= 0.95:
        label, color = "新鲜", "#35c46a"
    elif age_days <= 30:
        label, color = "可用偏旧", "#e2b84d"
    elif age_days <= 90:
        label, color = "需要更新", "#e8914b"
    else:
        label, color = "已过期", "#ef6461"
    if last_dt is None:
        recent = "--"
    elif age_days == 0:
        recent = "今天"
    elif age_days == 1:
        recent = "昨天"
    else:
        recent = f"{age_days}天前"
    return {
        "covered": covered,
        "missing": missing,
        "coverage_pct": coverage * 100,
        "label": label,
        "color": color,
        "recent": recent,
    }


def _flt_render_hk_segment_tile(
    col,
    row: dict,
    *,
    key_prefix: str,
    selected_key: str,
) -> bool:
    row_key = str(row.get("key"))
    total_count = int(row.get("count", 0) or 0)
    current_selected = [str(item) for item in st.session_state.get(selected_key, [])]
    selected_now = row_key in set(current_selected)
    health = _flt_hk_segment_health(row)
    value = col.checkbox(
        f"{_safe_str(row.get('label'))}（{total_count}）",
        value=selected_now,
        key=f"{key_prefix}_seg_check_{row_key}",
    )
    col.markdown(
        f"<div style='margin:-0.42rem 0 0.95rem 2.05rem;"
        f"font-size:0.74rem;line-height:1.35;color:rgba(234,240,245,0.58);'>"
        f"<span style='color:{health['color']};font-weight:900'>{health['label']}</span>"
        f"<span style='opacity:.65'> / 覆盖 {health['coverage_pct']:.0f}%</span><br/>"
        f"<span>可筛 {health['covered']}/{total_count} · 缺 {health['missing']} · {health['recent']}</span>"
        f"</div>",
        unsafe_allow_html=True,
    )
    return bool(value)


def _display_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df
    out = df.copy()
    out = out.replace({None: "-", "None": "-", "nan": "-", "NaN": "-", "N/A": "-"})
    out = out.fillna("-")
    return out


def _flt_filter_snapshot_by_market(snapshot_df: pd.DataFrame, scope: str) -> pd.DataFrame:
    if snapshot_df is None or snapshot_df.empty or "market" not in snapshot_df.columns:
        return pd.DataFrame()
    market = str(scope or "").strip().upper()
    return snapshot_df[snapshot_df["market"].astype(str).str.upper() == market].copy().reset_index(drop=True)


def _flt_market_snapshot_summary(scope: str) -> dict:
    market_df = _flt_filter_snapshot_by_market(filter_load_snapshot(), scope)
    total = int(len(market_df))
    quality_counts = {"full": 0, "partial": 0, "missing": 0}
    if total > 0 and "data_quality" in market_df.columns:
        vc = market_df["data_quality"].value_counts(dropna=False).to_dict()
        quality_counts = {
            "full": int(vc.get("full", 0)),
            "partial": int(vc.get("partial", 0)),
            "missing": int(vc.get("missing", 0)),
        }
    covered = int(quality_counts["full"] + quality_counts["partial"])
    coverage_ratio = (covered / total) if total > 0 else 0.0
    return {"scope": str(scope or "").upper(), "total": total, "quality_counts": quality_counts, "coverage_ratio": coverage_ratio}


def _flt_format_governance_line(counts: dict, order: List[str]) -> str:
    parts = [f"{key} {int(counts.get(key, 0) or 0)}" for key in order]
    return " / ".join(parts)


def _flt_render_governance_cards(governance: dict) -> None:
    st.markdown(
        """
        <style>
        .qs-governance-row {
          display: grid;
          grid-template-columns: repeat(4, minmax(0, 1fr));
          gap: 1rem;
          margin-top: 0.25rem;
        }
        .qs-governance-card {
          border: 1px solid rgba(255,255,255,0.10);
          border-radius: 22px;
          padding: 1.15rem 1.25rem 1rem;
          background: linear-gradient(180deg, rgba(255,255,255,0.06), rgba(255,255,255,0.03));
          box-shadow: inset 0 1px 0 rgba(255,255,255,0.04);
        }
        .qs-governance-title {
          color: rgba(235, 226, 214, 0.78);
          font-size: 0.72rem;
          font-weight: 800;
          text-transform: uppercase;
          letter-spacing: 0.12em;
          margin-bottom: 0.8rem;
        }
        .qs-governance-item {
          display: flex;
          align-items: baseline;
          justify-content: space-between;
          gap: 0.8rem;
          padding: 0.14rem 0;
        }
        .qs-governance-key {
          color: rgba(222, 214, 202, 0.74);
          font-size: 0.98rem;
          font-weight: 700;
          letter-spacing: 0.01em;
        }
        .qs-governance-val {
          color: rgba(255, 249, 242, 0.96);
          font-family: var(--qs-display);
          font-size: 1.08rem;
          font-weight: 700;
          white-space: nowrap;
        }
        @media (max-width: 980px) {
          .qs-governance-row { grid-template-columns: repeat(2, minmax(0, 1fr)); }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    label_map = {
        "covered": "已覆盖",
        "missing": "缺失",
        "complete": "完整",
        "partial": "部分",
        "sparse": "稀疏",
        "fresh": "新鲜",
        "aging": "渐旧",
        "stale": "过期",
        "unknown": "未知",
        "ready": "可用",
        "usable": "可参考",
    }

    def _rows(title: str, counts: dict, order: List[str]) -> str:
        items = "".join(
            f"<div class='qs-governance-item'><span class='qs-governance-key'>{label_map.get(label, label)}</span><span class='qs-governance-val'>{int(counts.get(label, 0) or 0)}</span></div>"
            for label in order
        )
        return f"<div class='qs-governance-card'><div class='qs-governance-title'>{title}</div>{items}</div>"

    cards = "".join(
        [
            _rows("覆盖", governance.get("coverage", {}), ["covered", "missing"]),
            _rows("完整度", governance.get("completeness", {}), ["complete", "partial", "sparse"]),
            _rows("新鲜度", governance.get("freshness", {}), ["fresh", "aging", "stale", "unknown"]),
            _rows("状态", governance.get("status", {}), ["ready", "usable", "stale", "missing"]),
        ]
    )
    st.markdown(f"<div class='qs-governance-row'>{cards}</div>", unsafe_allow_html=True)


def _flt_all_market_snapshot_summary() -> dict:
    snapshot_df = filter_load_snapshot()
    total = int(len(snapshot_df)) if isinstance(snapshot_df, pd.DataFrame) else 0
    a_total = int(len(_flt_filter_snapshot_by_market(snapshot_df, "A")))
    hk_total = int(len(_flt_filter_snapshot_by_market(snapshot_df, "HK")))
    return {"total": total, "a_total": a_total, "hk_total": hk_total}


def _flt_format_ops_fallback_warning(label: str, stats: dict, *, weekly: bool = False) -> str:
    error_text = _safe_str(stats.get("error", "")).strip()
    prefix = f"{label}{'周更' if weekly else '本次'}未连通接口，已回退本地快照。"
    return f"{prefix} 原因：{error_text}" if error_text else prefix


def _flt_format_source_summary(stats: dict) -> str:
    text = _safe_str(stats.get("source_summary", "")).strip()
    return f"来源：{text}" if text else ""


def _flt_segment_choice_map() -> dict:
    return {key: label for key, label in filter_get_a_enrich_segments()}


def _flt_a_segment_status() -> List[dict]:
    return filter_get_a_enrich_segment_status(filter_load_snapshot())


def _flt_hk_segment_status() -> List[dict]:
    return filter_get_hk_enrich_segment_status(filter_load_snapshot())


def _flt_run_source_check(scope: str) -> None:
    label = {"A": "A股", "HK": "港股", "ALL": "全市场"}.get(scope, scope)
    with st.spinner(f"正在检测{label}数据源..."):
        result = filter_check_market_data_source_status(scope)
    store = st.session_state.setdefault("flt_ops_source_check_result", {})
    store[str(scope).upper()] = result


def _flt_render_source_check_result(scope: str) -> None:
    result_map = st.session_state.get("flt_ops_source_check_result", {}) or {}
    result = result_map.get(str(scope).upper())
    if not result:
        return
    if bool(result.get("all_ok")):
        st.success(f"数据源检测完成：{result.get('checked_at', '--')}，全部可用。")
    else:
        st.warning(f"数据源检测完成：{result.get('checked_at', '--')}，存在不可用源。")
    rows = []
    for item in result.get("sources", []):
        rows.append(
            {
                "数据源": _safe_str(item.get("source")),
                "状态": "可用" if bool(item.get("ok")) else "失败",
                "耗时(秒)": float(item.get("elapsed_sec", 0.0) or 0.0),
                "返回行数": int(item.get("rows", 0) or 0),
                "原因": _safe_str(item.get("detail")),
            }
        )
    if rows:
        st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)


def _flt_run_market_update(scope: str, *, max_stocks: int, enrich_n: int, enrich_segment: str, force_refresh: bool, rotate_enrich: bool, safe_mode: bool) -> None:
    label = {"A": "A股", "HK": "港股"}.get(scope, scope)
    with st.spinner(f"正在执行{label}更新..."):
        try:
            stats = filter_refresh_market_snapshot(
                max_stocks=int(max_stocks),
                enrich_top_n=int(enrich_n if scope == "A" else 0),
                force_refresh=bool(force_refresh),
                rotate_enrich=bool(rotate_enrich),
                market_scope=str(scope),
                enrich_segment=str(enrich_segment),
                weekly_mode=False,
                safe_mode=bool(safe_mode),
            )
            if bool(stats.get("fallback", False)):
                st.warning(_flt_format_ops_fallback_warning(label, stats, weekly=False))
            else:
                st.success(
                    f"{label}更新完成：{stats.get('row_count', 0)} 只，"
                    f"深补 {stats.get('enriched_count', 0)} 只 ｜ "
                    f"{_safe_str(stats.get('enrich_segment_label', ''))} / 共 {int(stats.get('segment_total', 0) or 0)} 只"
                    f"（区间 {int(stats.get('enrich_start', 0) or 0)} -> {int(stats.get('enrich_end', 0) or 0)}）"
                )
                st.caption(
                    f"{_flt_format_source_summary(stats)} ｜ "
                    f"缓存命中: {int(stats.get('cache_hit', 0) or 0)} ｜ "
                    f"重抓: {int(stats.get('cache_miss', 0) or 0)}"
                )
            st.session_state["flt_result"] = None
        except Exception as exc:
            st.error(f"{label}更新失败: {exc}")


def _flt_run_market_weekly(scope: str, *, enrich_n: int, enrich_segment: str) -> None:
    label = {"A": "A股", "HK": "港股"}.get(scope, scope)
    with st.spinner(f"正在执行{label}周更..."):
        try:
            stats = filter_refresh_market_snapshot(
                max_stocks=0,
                enrich_top_n=int(enrich_n if scope == "A" else 0),
                force_refresh=False,
                rotate_enrich=True,
                market_scope=str(scope),
                enrich_segment=str(enrich_segment),
                weekly_mode=True,
                safe_mode=True,
            )
            if bool(stats.get("skipped", False)):
                st.info(_safe_str(stats.get("reason", "周更间隔未到，本次跳过")))
            elif bool(stats.get("fallback", False)):
                st.warning(_flt_format_ops_fallback_warning(label, stats, weekly=True))
            else:
                st.success(
                    f"{label}周更完成：{stats.get('row_count', 0)} 只，"
                    f"深补 {stats.get('enriched_count', 0)} 只 ｜ "
                    f"{_safe_str(stats.get('enrich_segment_label', ''))} / 共 {int(stats.get('segment_total', 0) or 0)} 只"
                    f"（区间 {int(stats.get('enrich_start', 0) or 0)} -> {int(stats.get('enrich_end', 0) or 0)}）"
                )
                source_caption = _flt_format_source_summary(stats)
                if source_caption:
                    st.caption(source_caption)
            st.session_state["flt_result"] = None
        except Exception as exc:
            st.error(f"{label}周更失败: {exc}")


def _flt_run_a_segment_enrich(segment_key: str, segment_label: str, segment_count: int, *, force_refresh: bool, safe_mode: bool) -> None:
    if int(segment_count or 0) <= 0:
        st.info(f"{segment_label} 当前没有可深补股票。")
        return
    status_rows = _flt_a_segment_status()
    status_map = {str(row.get("key")): row for row in status_rows}
    current_row = status_map.get(str(segment_key), {})
    persisted_count = int(current_row.get("persisted_count", 0) or 0)
    pending_count = max(0, int(segment_count or 0) - persisted_count)
    if (not force_refresh) and pending_count <= 0:
        st.info(f"{segment_label} 已全部深补完成；当前没有待补股票。")
        return
    with st.spinner(f"正在深补 {segment_label} ..."):
        try:
            stats = filter_refresh_market_snapshot(
                max_stocks=0,
                enrich_top_n=int(pending_count if (not force_refresh and pending_count > 0) else segment_count),
                force_refresh=bool(force_refresh),
                rotate_enrich=False,
                market_scope="A",
                enrich_segment=str(segment_key),
                weekly_mode=False,
                safe_mode=bool(safe_mode),
                only_missing_enrich=True,
            )
            if bool(stats.get("fallback", False)):
                st.warning(_flt_format_ops_fallback_warning("A股", stats, weekly=False))
            else:
                mode_label = "补缺" if bool(stats.get("only_missing_enrich", False)) else "全量重补"
                base_total = int(stats.get("segment_pending", 0) or 0) if bool(stats.get("only_missing_enrich", False)) else int(stats.get('segment_total', 0) or 0)
                st.success(
                    f"{segment_label} {mode_label}完成：{int(stats.get('enriched_count', 0) or 0)} / {base_total}"
                )
                st.caption(
                    f"{_flt_format_source_summary(stats)} ｜ "
                    f"缓存命中: {int(stats.get('cache_hit', 0) or 0)} ｜ "
                    f"重抓: {int(stats.get('cache_miss', 0) or 0)}"
                )
            st.session_state["flt_result"] = None
        except Exception as exc:
            st.error(f"{segment_label} 深补失败: {exc}")


def _flt_run_hk_segment_enrich(segment_key: str, segment_label: str, segment_count: int, *, force_refresh: bool, safe_mode: bool) -> None:
    if int(segment_count or 0) <= 0:
        st.info(f"{segment_label} 当前没有可深补股票。")
        return
    status_rows = _flt_hk_segment_status()
    status_map = {str(row.get("key")): row for row in status_rows}
    current_row = status_map.get(str(segment_key), {})
    persisted_count = int(current_row.get("persisted_count", 0) or 0)
    pending_count = max(0, int(segment_count or 0) - persisted_count)
    if (not force_refresh) and pending_count <= 0:
        st.info(f"{segment_label} 已全部深补完成；当前没有待补股票。")
        return
    with st.spinner(f"正在深补 {segment_label} ..."):
        try:
            stats = filter_refresh_market_snapshot(
                max_stocks=0,
                enrich_top_n=int(pending_count if (not force_refresh and pending_count > 0) else segment_count),
                force_refresh=bool(force_refresh),
                rotate_enrich=False,
                market_scope="HK",
                enrich_segment=str(segment_key),
                weekly_mode=False,
                safe_mode=bool(safe_mode),
                only_missing_enrich=True,
            )
            if bool(stats.get("fallback", False)):
                st.warning(_flt_format_ops_fallback_warning("港股", stats, weekly=False))
            else:
                mode_label = "补缺" if bool(stats.get("only_missing_enrich", False)) else "全量重补"
                base_total = int(stats.get("segment_pending", 0) or 0) if bool(stats.get("only_missing_enrich", False)) else int(stats.get("segment_total", 0) or 0)
                st.success(f"{segment_label} {mode_label}完成：{int(stats.get('enriched_count', 0) or 0)} / {base_total}")
                st.caption(
                    f"{_flt_format_source_summary(stats)} ｜ "
                    f"缓存命中: {int(stats.get('cache_hit', 0) or 0)} ｜ "
                    f"重抓: {int(stats.get('cache_miss', 0) or 0)}"
                )
            st.session_state["flt_result"] = None
        except Exception as exc:
            st.error(f"{segment_label} 深补失败: {exc}")


def _render_filter_market_ops_tab(scope: str, *, key_prefix: str) -> None:
    label = {"A": "A股", "HK": "港股"}.get(scope, scope)
    summary = _flt_market_snapshot_summary(scope)
    weekly = filter_get_weekly_update_status(scope)
    weekly_state = "已到期可执行" if bool(weekly.get("due")) else f"未到期（剩余{float(weekly.get('remaining_hours', 0.0)):.1f}小时）"

    st.markdown("### 基础快照")
    st.caption("维护当前市场的基础行情底座，负责更新、周更和数据源连通性检测。")
    render_status_row(
        (
            ("样本数量", f"{summary['total']} 只"),
            ("上次周更", weekly.get("last") or "--"),
            ("周更状态", weekly_state),
        )
    )
    c1, c2, c3 = st.columns([1, 1, 1], vertical_alignment="bottom")
    max_stocks = c1.number_input(f"{label}更新股票数（0=全部）", min_value=0, max_value=12000, value=0, step=200, key=f"{key_prefix}_max_stocks")
    if scope == "A":
        enrich_n = 0
        enrich_segment = "sz_main"
    elif scope == "HK":
        enrich_n = 0
        enrich_segment = "financials"
    else:
        enrich_n = 0
        enrich_segment = "sz_main"
    safe_mode = c3.checkbox("安全模式（防封）", value=True, key=f"{key_prefix}_safe_mode")
    force_refresh = st.checkbox("忽略缓存强制重抓", value=False, key=f"{key_prefix}_force")
    st.caption(
        f"{label}周更上次: {weekly.get('last') or '--'} ｜ "
        f"下次到期: {weekly.get('next_due') or '立即可执行'} ｜ "
        f"当前状态: {weekly_state}"
    )
    b1, b2, b3 = st.columns(3)
    if b1.button(f"运行{label}更新", width="stretch", key=f"{key_prefix}_run_once"):
        _flt_run_market_update(scope, max_stocks=int(max_stocks), enrich_n=int(enrich_n), enrich_segment=str(enrich_segment), force_refresh=bool(force_refresh), rotate_enrich=False, safe_mode=bool(safe_mode))
    if b2.button(f"执行{label}周更（7天一次）", width="stretch", key=f"{key_prefix}_weekly"):
        _flt_run_market_weekly(scope, enrich_n=int(enrich_n), enrich_segment=str(enrich_segment))
    if b3.button(f"检测{label}数据源", width="stretch", key=f"{key_prefix}_source_check"):
        _flt_run_source_check(scope)
    _flt_render_source_check_result(scope)

    if scope in {"A", "HK"}:
        st.markdown("<div style='height:1.4rem'></div>", unsafe_allow_html=True)
        st.markdown("### 深补覆盖")
        st.caption(f"按数据库治理口径展示当前 {label} 深补资产的覆盖、字段完整度、时间新鲜度和最终可用状态。")
        governance = filter_get_enrichment_governance_summary(scope)
        _flt_render_governance_cards(governance)
        st.caption(
            f"覆盖：有无记录 ｜ 完整度：关键字段完备程度 ｜ 新鲜度：距上次深补的时间状态 ｜ "
            f"状态：综合后的最终等级 ｜ 覆盖率 {float(governance.get('coverage_ratio', 0.0) or 0.0) * 100:.1f}%"
        )

        st.markdown("<div style='height:1.2rem'></div>", unsafe_allow_html=True)
        st.markdown("### A股板块深补" if scope == "A" else "### 港股板块深补")
        if scope == "A":
            st.caption("点击下方板块按钮，直接深补该板块全部股票。")
        else:
            st.caption("勾选一个或多个港股板块，再点击“深补勾选板块”。默认只补缺，已深补股票不会重复抓。")
        segment_rows = _flt_a_segment_status() if scope == "A" else _flt_hk_segment_status()
        store_summary = filter_get_stock_enrichment_store_summary()
        completed_segments = sum(1 for row in segment_rows if _safe_str(row.get("status")) == "已完成")
        st.caption(
            f"持久化深补资产：{label} {int(store_summary.get('a_total', 0) if scope == 'A' else store_summary.get('hk_total', 0) or 0)} 条 ｜ "
            f"已完成板块 {completed_segments}/{len(segment_rows)} ｜ "
            f"最近深补 {_safe_str(store_summary.get('latest_enriched_at', '')) or '--'}"
        )
        if scope == "HK":
            selected_key = f"{key_prefix}_hk_selected_segments"
            if selected_key not in st.session_state:
                st.session_state[selected_key] = []
            row_map = {str(row["key"]): row for row in segment_rows}
            pending_keys = [
                str(row["key"])
                for row in segment_rows
                if int(row.get("persisted_count", 0) or 0) < int(row.get("count", 0) or 0)
            ]
            selected_segments = [str(item) for item in st.session_state.get(selected_key, []) if str(item) in row_map]
            st.session_state[selected_key] = selected_segments
            ctrl1, ctrl2, ctrl3 = st.columns([1.2, 1, 1], vertical_alignment="bottom")
            ctrl1.caption(f"已选择 {len(selected_segments)} 个板块")
            if ctrl2.button("全选未完成", width="stretch", key=f"{key_prefix}_hk_select_pending"):
                st.session_state[selected_key] = pending_keys
                for row in segment_rows:
                    st.session_state[f"{key_prefix}_seg_check_{row['key']}"] = str(row["key"]) in set(pending_keys)
                st.rerun()
            if ctrl3.button("清空选择", width="stretch", key=f"{key_prefix}_hk_clear_selected"):
                st.session_state[selected_key] = []
                for row in segment_rows:
                    st.session_state[f"{key_prefix}_seg_check_{row['key']}"] = False
                st.rerun()
            st.markdown("<div style='height:0.6rem'></div>", unsafe_allow_html=True)

        if scope == "HK":
            with st.form(f"{key_prefix}_hk_segment_select_form", border=False):
                for start in range(0, len(segment_rows), 4):
                    cols = st.columns(4)
                    for col, row in zip(cols, segment_rows[start : start + 4]):
                        _flt_render_hk_segment_tile(
                            col,
                            row,
                            key_prefix=key_prefix,
                            selected_key=f"{key_prefix}_hk_selected_segments",
                        )
                if st.form_submit_button("深补勾选板块", type="primary", width="stretch"):
                    chosen_now = [
                        str(row.get("key"))
                        for row in segment_rows
                        if bool(st.session_state.get(f"{key_prefix}_seg_check_{row.get('key')}", False))
                    ]
                    st.session_state[selected_key] = chosen_now
                    if not chosen_now:
                        st.warning("请先勾选至少一个港股板块。")
                        return
                    st.info(f"准备深补：{', '.join([_safe_str(row_map[k].get('label')) for k in chosen_now if k in row_map])}")
                    for segment_key in chosen_now:
                        row = row_map.get(str(segment_key))
                        if not row:
                            continue
                        _flt_run_hk_segment_enrich(
                            segment_key=str(row["key"]),
                            segment_label=str(row["label"]),
                            segment_count=int(row["count"]),
                            force_refresh=bool(force_refresh),
                            safe_mode=bool(safe_mode),
                        )
                    st.session_state["flt_result"] = None
        else:
            for start in range(0, len(segment_rows), 4):
                cols = st.columns(4)
                for col, row in zip(cols, segment_rows[start : start + 4]):
                    label_text = f"{row['label']}（{int(row['count'])}）"
                    if col.button(label_text, width="stretch", key=f"{key_prefix}_seg_{row['key']}"):
                        _flt_run_a_segment_enrich(
                            segment_key=str(row["key"]),
                            segment_label=str(row["label"]),
                            segment_count=int(row["count"]),
                            force_refresh=bool(force_refresh),
                            safe_mode=bool(safe_mode),
                        )
                    persisted = int(row.get("persisted_count", 0) or 0)
                    total_count = int(row.get("count", 0) or 0)
                    col.caption(f"{_safe_str(row.get('status'))} ｜ 已深补 {persisted}/{total_count}")
                    last_text = _safe_str(row.get("last_enriched_at", ""))
                    col.caption(f"上次深补：{last_text}" if last_text else "上次深补：未深补")


def _render_filter_ops_panel() -> None:
    ops_view = st.radio(
        "运维视图",
        options=["A股", "港股", "总览"],
        horizontal=True,
        label_visibility="collapsed",
        key="flt_ops_panel_view",
    )
    if ops_view == "A股":
        _render_filter_market_ops_tab("A", key_prefix="flt_ops_a")
    elif ops_view == "港股":
        _render_filter_market_ops_tab("HK", key_prefix="flt_ops_hk")
    else:
        governance = filter_get_enrichment_governance_summary("ALL")
        report = filter_get_snapshot_health_report(days=7, top_n=20)
        backup_status = filter_get_snapshot_backup_status()
        coverage = governance.get("coverage", {}) if isinstance(governance, dict) else {}
        completeness = governance.get("completeness", {}) if isinstance(governance, dict) else {}
        total = int(governance.get("total", 0) or 0)
        covered = int(coverage.get("covered", 0) or 0)
        missing = int(coverage.get("missing", 0) or 0)
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("总样本", total)
        m2.metric("已覆盖", covered)
        m3.metric("部分/稀疏", int(completeness.get("partial", 0) or 0) + int(completeness.get("sparse", 0) or 0))
        m4.metric("缺失", missing)
        coverage_ratio = float(governance.get("coverage_ratio", 0.0) or 0.0)
        st.progress(coverage_ratio, text=f"深补覆盖率 {coverage_ratio * 100:.1f}%")
        _flt_render_governance_cards(governance)

        action_col1, action_col2 = st.columns(2)
        if action_col1.button("生成体检报告（Excel）", width="stretch", key="flt_ops_generate_health_xlsx"):
            st.session_state["flt_ops_health_xlsx"] = filter_export_snapshot_health_excel(days=30, top_n=50)
            st.session_state["flt_ops_health_xlsx_name"] = f"filter_health_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        if st.session_state.get("flt_ops_health_xlsx"):
            action_col1.download_button(
                "下载体检报告（Excel）",
                data=st.session_state["flt_ops_health_xlsx"],
                file_name=st.session_state.get("flt_ops_health_xlsx_name", "filter_health_report.xlsx"),
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="flt_ops_download_health_xlsx",
            )
        if action_col2.button("从备份恢复快照", width="stretch", disabled=not bool(backup_status.get("exists")), key="flt_ops_restore_backup"):
            try:
                restored = filter_restore_snapshot_from_backup()
                st.success(
                    f"已从备份恢复快照：{int(restored.get('row_count', 0) or 0)} 行，"
                    f"恢复时间 {restored.get('restored_at', '--')}"
                )
                st.rerun()
            except Exception as exc:
                st.error(f"恢复失败: {exc}")
        if bool(backup_status.get("exists")):
            st.caption(
                f"当前备份：{int(backup_status.get('row_count', 0) or 0)} 行 ｜ "
                f"备份时间 {backup_status.get('backup_at', '--') or '--'}"
            )
        else:
            st.caption("当前没有可恢复的快照备份。")

        render_data_vault_panel(key_prefix="trading_filter_ops_vault")

        with st.expander("查看体检详情", expanded=False):
            st.dataframe(report.get("trend_df", pd.DataFrame()), width="stretch", hide_index=True)
            st.dataframe(report.get("runs_df", pd.DataFrame()), width="stretch", hide_index=True)


def _render_filter_page():
    st.caption(f"版本号: {FILTER_APP_VERSION} ｜ 侧边栏统一，筛选参数在主区顶部")

    cfg = st.session_state.get("flt_cfg", filter_default_filter_config())
    meta = filter_get_snapshot_meta()
    overall = _flt_all_market_snapshot_summary()
    render_section_intro(
        "数据运维台",
        "默认收起，避免首次进入页面加载过重；需要维护快照、深补、备份与数据体检时再手动打开。",
        kicker="Workflow",
        pills=("A+H更新", "周更", "体检", "导出"),
    )
    st.markdown(
        """
        <style>
        .st-key-flt_ops_toggle_in_filter_header button {
          font-size: 1.5rem !important;
          min-height: 3.2rem !important;
          padding: 0.55rem 1.6rem !important;
          white-space: nowrap !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    toggle_label = "打开数据运维台" if not bool(st.session_state.get("flt_show_ops_panel", False)) else "收起数据运维台"
    if st.button(toggle_label, width="content", key="flt_ops_toggle_in_filter_header"):
        st.session_state["flt_show_ops_panel"] = not bool(st.session_state.get("flt_show_ops_panel", False))
        st.rerun()
    render_status_row(
        (
            ("快照状态", meta.get("last_update", "尚未更新") if meta else "尚未更新"),
            ("总样本", f"{int(overall.get('total', 0) or 0)} 只"),
            ("A/H分布", f"A {int(overall.get('a_total', 0) or 0)} / HK {int(overall.get('hk_total', 0) or 0)}"),
        )
    )
    if bool(st.session_state.get("flt_show_ops_panel", False)):
        _render_filter_ops_panel()
        st.markdown("---")
    else:
        st.caption("数据运维台已收起，需要维护数据时点击上方按钮打开。")

    render_section_intro(
        "筛选矩阵",
        "筛选条件被拆成四层，从硬排除到五年后视镜，帮助你先做风险清洗，再做估值与长期验证。",
        kicker="Configuration",
        pills=("A 硬排除", "B 估值质量", "C 行业规模", "D 五年后视镜"),
    )
    st.markdown(
        """
        <style>
        [data-testid="stExpander"] details > summary {
          background: linear-gradient(180deg, rgba(142, 168, 107, 0.38), rgba(116, 146, 87, 0.34)) !important;
          border: 1px solid rgba(191, 217, 157, 0.30) !important;
          border-radius: 999px !important;
        }
        [data-testid="stExpander"] details > summary:hover {
          background: linear-gradient(180deg, rgba(156, 182, 119, 0.46), rgba(126, 156, 95, 0.42)) !important;
        }
        [data-testid="stExpander"] details > summary p,
        [data-testid="stExpander"] details > summary span,
        [data-testid="stExpander"] details > summary div,
        [data-testid="stExpander"] details > summary svg {
          color: rgba(250, 248, 241, 0.98) !important;
          fill: rgba(250, 248, 241, 0.98) !important;
          font-weight: 800 !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    mode = st.radio("筛选模式", options=["手动筛选", "AI辅助设定", "模板筛选"], horizontal=True, key="flt_mode_main")
    if mode == "AI辅助设定":
        render_section_intro(
            "AI 条件草拟",
            "先用自然语言生成一版初稿条件，再继续手动微调，适合从模糊目标快速进入筛选状态。",
            kicker="Assist",
            pills=("自然语言", "自动生成", "可继续微调"),
        )
        prompt = st.text_area("输入你的目标", value="高股息、低估值、低负债、防御型", height=80, key="flt_ai_prompt_main")
        if st.button("生成条件并应用", type="primary", key="flt_ai_apply_main"):
            cfg = filter_build_ai_quick_config(prompt, cfg)
            st.session_state["flt_cfg"] = cfg
            st.success("已根据描述生成条件，你可继续微调后执行筛选。")
            st.rerun()
    elif mode == "模板筛选":
        render_section_intro(
            "模板筛选",
            "直接加载现成模板后执行筛选，适合重复使用固定风格的条件集。",
            kicker="Template",
            pills=("读取模板", "保存模板", "快速复用"),
        )
        tpl_cols = st.columns([1.1, 0.65, 1.0, 0.65], vertical_alignment="bottom")
        all_tpl = filter_load_templates()
        tpl_names = sorted(all_tpl.keys())
        selected_tpl = tpl_cols[0].selectbox("模板", options=["(无)"] + tpl_names, key="flt_tpl_select_main")
        if tpl_cols[1].button("读取模板", width="stretch", key="flt_tpl_load_main"):
            if selected_tpl and selected_tpl != "(无)":
                st.session_state["flt_cfg"] = filter_get_template_config(selected_tpl)
                st.success(f"已加载模板: {selected_tpl}")
                st.rerun()
        save_tpl_name = tpl_cols[2].text_input("保存为模板名", value="", key="flt_tpl_save_main")
        if tpl_cols[3].button("保存模板", width="stretch", key="flt_tpl_save_btn_main"):
            try:
                filter_save_template(save_tpl_name, cfg)
                st.success("模板已保存")
            except Exception as exc:
                st.error(str(exc))
    with st.form("flt_condition_form", clear_on_submit=False):
        st.subheader("筛选条件（支持手动开关，像电商筛选一样）")

        with st.expander("A. 财务健康度与硬排除", expanded=False):
            c1, c2, c3 = st.columns(3)
            cfg["missing_policy"] = c1.selectbox("缺失数据处理", options=["ignore", "exclude"], index=0 if cfg.get("missing_policy") == "ignore" else 1)

            r = cfg["risk"]
            q = cfg["quality"]

            r["exclude_st"] = c1.checkbox("排除 ST/*ST", value=bool(r.get("exclude_st", True)))
            r["exclude_investigation"] = c1.checkbox("排除立案调查", value=bool(r.get("exclude_investigation", True)))
            r["exclude_penalty"] = c1.checkbox("排除重大处罚", value=bool(r.get("exclude_penalty", True)))

            r["exclude_fund_occupation"] = c2.checkbox("排除资金占用", value=bool(r.get("exclude_fund_occupation", True)))
            r["exclude_illegal_reduce"] = c2.checkbox("排除违规减持", value=bool(r.get("exclude_illegal_reduce", True)))
            r["require_standard_audit"] = c2.checkbox("审计意见必须标准无保留", value=bool(r.get("require_standard_audit", False)))

            q["ocf_3y_min_enabled"] = c3.checkbox("启用近3年经营现金流下限(亿)", value=bool(q.get("ocf_3y_min_enabled", False)))
            q["ocf_3y_min"] = c3.number_input("经营现金流下限(亿)", value=float(q.get("ocf_3y_min", 0.0)), step=1.0)
            q["asset_liability_max_enabled"] = c3.checkbox("启用资产负债率上限(%)", value=bool(q.get("asset_liability_max_enabled", False)))
            q["asset_liability_max"] = c3.number_input("资产负债率上限(%)", value=float(q.get("asset_liability_max", 80.0)), step=1.0)

        with st.expander("B. 估值与质量", expanded=False):
            c1, c2, c3 = st.columns(3)
            q = cfg["quality"]
            v = cfg["valuation"]

            q["roe_min_enabled"] = c1.checkbox("启用 ROE 下限(%)", value=bool(q.get("roe_min_enabled", False)))
            q["roe_min"] = c1.number_input("ROE 下限(%)", value=float(q.get("roe_min", 5.0)), step=0.5)
            q["gross_margin_min_enabled"] = c1.checkbox("启用毛利率下限(%)", value=bool(q.get("gross_margin_min_enabled", False)))
            q["gross_margin_min"] = c1.number_input("毛利率下限(%)", value=float(q.get("gross_margin_min", 20.0)), step=0.5)
            q["net_margin_min_enabled"] = c1.checkbox("启用净利率下限(%)", value=bool(q.get("net_margin_min_enabled", False)))
            q["net_margin_min"] = c1.number_input("净利率下限(%)", value=float(q.get("net_margin_min", 8.0)), step=0.5)

            q["receivable_ratio_max_enabled"] = c2.checkbox("启用应收代理指标上限", value=bool(q.get("receivable_ratio_max_enabled", False)))
            q["receivable_ratio_max"] = c2.number_input("应收代理指标上限", value=float(q.get("receivable_ratio_max", 50.0)), step=1.0)
            q["goodwill_ratio_max_enabled"] = c2.checkbox("启用商誉/净资产上限(%)", value=bool(q.get("goodwill_ratio_max_enabled", False)))
            q["goodwill_ratio_max"] = c2.number_input("商誉/净资产上限(%)", value=float(q.get("goodwill_ratio_max", 30.0)), step=1.0)
            q["interest_debt_asset_max_enabled"] = c2.checkbox("启用有息负债/总资产上限(%)", value=bool(q.get("interest_debt_asset_max_enabled", False)))
            q["interest_debt_asset_max"] = c2.number_input("有息负债/总资产上限(%)", value=float(q.get("interest_debt_asset_max", 20.0)), step=1.0)

            v["pe_ttm_min_enabled"] = c3.checkbox("启用 PE(TTM) 下限", value=bool(v.get("pe_ttm_min_enabled", False)))
            v["pe_ttm_min"] = c3.number_input("PE(TTM) 下限", value=float(v.get("pe_ttm_min", 0.0)), step=1.0)
            v["pe_ttm_max_enabled"] = c3.checkbox("启用 PE(TTM) 上限", value=bool(v.get("pe_ttm_max_enabled", False)))
            v["pe_ttm_max"] = c3.number_input("PE(TTM) 上限", value=float(v.get("pe_ttm_max", 25.0)), step=1.0)
            v["pb_max_enabled"] = c3.checkbox("启用 PB 上限", value=bool(v.get("pb_max_enabled", False)))
            v["pb_max"] = c3.number_input("PB 上限", value=float(v.get("pb_max", 3.0)), step=0.1)

        with st.expander("C. 行业、分红、流动性与规模", expanded=False):
            c1, c2, c3 = st.columns(3)
            r = cfg["risk"]
            v = cfg["valuation"]
            g = cfg["growth_liquidity"]

            scope_map = {"all": "全部市场", "A": "仅A股", "HK": "仅港股"}
            raw_scope = _safe_str(r.get("market_scope", "all")).upper()
            scope_value = "all" if raw_scope not in {"A", "HK"} else raw_scope
            r["market_scope"] = c1.selectbox(
                "筛选市场范围",
                options=["all", "A", "HK"],
                index=["all", "A", "HK"].index(scope_value),
                format_func=lambda x: scope_map.get(x, x),
            )
            r["industry_include_enabled"] = c1.checkbox("启用行业关键词包含", value=bool(r.get("industry_include_enabled", False)))
            r["industry_include_keywords"] = c1.text_input(
                "行业关键词（包含，逗号分隔）",
                value=str(r.get("industry_include_keywords", "")),
            )
            if str(r.get("industry_include_keywords", "")).strip():
                r["industry_include_enabled"] = True
            r["exclude_sunset_industry"] = c1.checkbox("排除夕阳行业", value=bool(r.get("exclude_sunset_industry", False)))
            r["sunset_industries"] = c1.text_area("夕阳行业关键词（逗号分隔）", value=str(r.get("sunset_industries", "")), height=120)
            r["pledge_ratio_max_enabled"] = c1.checkbox("启用质押率上限(%)", value=bool(r.get("pledge_ratio_max_enabled", False)))
            r["pledge_ratio_max"] = c1.number_input("质押率上限(%)", value=float(r.get("pledge_ratio_max", 80.0)), step=1.0)

            v["dividend_min_enabled"] = c2.checkbox("启用股息率下限(%)", value=bool(v.get("dividend_min_enabled", False)))
            v["dividend_min"] = c2.number_input("股息率下限(%)", value=float(v.get("dividend_min", 3.0)), step=0.1)
            v["dividend_max_enabled"] = c2.checkbox("启用股息率上限(%)", value=bool(v.get("dividend_max_enabled", False)))
            v["dividend_max"] = c2.number_input("股息率上限(%)", value=float(v.get("dividend_max", 12.0)), step=0.1)
            r["exclude_no_dividend_5y"] = c2.checkbox("排除近5年未分红", value=bool(r.get("exclude_no_dividend_5y", False)))

            g["market_cap_min_enabled"] = c3.checkbox("启用总市值下限(亿)", value=bool(g.get("market_cap_min_enabled", False)))
            g["market_cap_min"] = c3.number_input("总市值下限(亿)", value=float(g.get("market_cap_min", 100.0)), step=10.0)
            g["market_cap_max_enabled"] = c3.checkbox("启用总市值上限(亿)", value=bool(g.get("market_cap_max_enabled", False)))
            g["market_cap_max"] = c3.number_input("总市值上限(亿)", value=float(g.get("market_cap_max", 5000.0)), step=10.0)

            g["turnover_min_enabled"] = c3.checkbox("启用换手率下限(%)", value=bool(g.get("turnover_min_enabled", False)))
            g["turnover_min"] = c3.number_input("换手率下限(%)", value=float(g.get("turnover_min", 0.2)), step=0.1)
            g["turnover_max_enabled"] = c3.checkbox("启用换手率上限(%)", value=bool(g.get("turnover_max_enabled", False)))
            g["turnover_max"] = c3.number_input("换手率上限(%)", value=float(g.get("turnover_max", 15.0)), step=0.1)

            g["volume_ratio_min_enabled"] = c3.checkbox("启用量比下限", value=bool(g.get("volume_ratio_min_enabled", False)))
            g["volume_ratio_min"] = c3.number_input("量比下限", value=float(g.get("volume_ratio_min", 0.5)), step=0.1)
            g["volume_ratio_max_enabled"] = c3.checkbox("启用量比上限", value=bool(g.get("volume_ratio_max_enabled", False)))
            g["volume_ratio_max"] = c3.number_input("量比上限", value=float(g.get("volume_ratio_max", 3.0)), step=0.1)

        with st.expander("D. 五年后视镜（先看长期，再做当前筛选）", expanded=False):
            st.caption("建议先开这一层做长期体检，再叠加 A/B/C 做当下过滤。")
            d1, d2, d3 = st.columns(3)
            d5 = cfg.setdefault("rearview_5y", {})

            d5["revenue_cagr_5y_min_enabled"] = d1.checkbox(
                "启用营收5年CAGR下限(%)", value=bool(d5.get("revenue_cagr_5y_min_enabled", False))
            )
            d5["revenue_cagr_5y_min"] = d1.number_input(
                "营收5年CAGR下限(%)", value=float(d5.get("revenue_cagr_5y_min", 3.0)), step=0.5
            )
            d5["profit_cagr_5y_min_enabled"] = d1.checkbox(
                "启用净利5年CAGR下限(%)", value=bool(d5.get("profit_cagr_5y_min_enabled", False))
            )
            d5["profit_cagr_5y_min"] = d1.number_input(
                "净利5年CAGR下限(%)", value=float(d5.get("profit_cagr_5y_min", 3.0)), step=0.5
            )

            d5["roe_avg_5y_min_enabled"] = d2.checkbox(
                "启用ROE 5年均值下限(%)", value=bool(d5.get("roe_avg_5y_min_enabled", False))
            )
            d5["roe_avg_5y_min"] = d2.number_input(
                "ROE 5年均值下限(%)", value=float(d5.get("roe_avg_5y_min", 8.0)), step=0.5
            )
            d5["ocf_positive_years_5y_min_enabled"] = d2.checkbox(
                "启用经营现金流为正年数下限", value=bool(d5.get("ocf_positive_years_5y_min_enabled", False))
            )
            d5["ocf_positive_years_5y_min"] = int(
                d2.number_input(
                    "经营现金流为正年数下限(0-5)",
                    min_value=0,
                    max_value=5,
                    value=int(d5.get("ocf_positive_years_5y_min", 4)),
                    step=1,
                )
            )

            d5["debt_ratio_change_5y_max_enabled"] = d3.checkbox(
                "启用负债率5年变化上限(百分点)", value=bool(d5.get("debt_ratio_change_5y_max_enabled", False))
            )
            d5["debt_ratio_change_5y_max"] = d3.number_input(
                "负债率5年变化上限(百分点)", value=float(d5.get("debt_ratio_change_5y_max", 8.0)), step=0.5
            )
            d5["gross_margin_change_5y_min_enabled"] = d3.checkbox(
                "启用毛利率5年变化下限(百分点)", value=bool(d5.get("gross_margin_change_5y_min_enabled", False))
            )
            d5["gross_margin_change_5y_min"] = d3.number_input(
                "毛利率5年变化下限(百分点)", value=float(d5.get("gross_margin_change_5y_min", -6.0)), step=0.5
            )

        st.session_state["flt_cfg"] = cfg

        render_section_intro(
            "执行与结果",
            "执行区负责跑规则，结果区负责解释结果和导出表格，让批量筛选的收尾动作更集中。",
            kicker="Execution",
            pills=("执行筛选", "二段筛选", "结果导出"),
        )
        run_col1, run_col2, run_col3 = st.columns([1, 1.4, 2])
        run_now = run_col1.form_submit_button("执行筛选", type="primary", width="stretch")
        two_stage = run_col2.checkbox("二段筛选（先 A/B/C，再 D 五年后视镜）", value=True, key="flt_two_stage_main")
        run_col3.caption("注：部分标的5年数据可能缺失；可通过“缺失数据处理”决定是否直接排除。")

    if run_now:
        snap = filter_load_snapshot()
        if snap.empty:
            st.error("还没有市场快照。请先在上方点击“更新全市场数据”。")
        else:
            with st.spinner("正在执行筛选..."):
                if two_stage:
                    stage1_cfg = _build_stage1_config(cfg)
                    p1, r1, m1, s1 = filter_apply_filters(snap, stage1_cfg)

                    if _has_rearview_enabled(cfg):
                        stage2_cfg = _build_stage2_config(cfg)
                        p2, r2, m2, _s2 = filter_apply_filters(p1, stage2_cfg)
                        passed_df = p2
                        rejected_df = _concat_dedup(r1, r2)
                        missing_df = _concat_dedup(m1, m2)
                    else:
                        passed_df, rejected_df, missing_df = p1, r1, m1

                    stats = {
                        "total": int(len(snap)),
                        "passed": int(len(passed_df)),
                        "rejected": int(len(rejected_df)),
                        "missing": int(len(missing_df)),
                        "stage1_passed": int(s1.get("passed", len(p1))),
                        "stage2_passed": int(len(passed_df)),
                        "stage_mode": "two",
                    }
                else:
                    passed_df, rejected_df, missing_df, stats = filter_apply_filters(snap, cfg)
                    stats["stage_mode"] = "single"
                st.session_state["flt_result"] = {
                    "passed": passed_df,
                    "rejected": rejected_df,
                    "missing": missing_df,
                    "stats": stats,
                    "run_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }

    res = st.session_state.get("flt_result")
    if not res:
        st.info("请先更新市场快照，然后点击“执行筛选”。")
        return

    stats = res["stats"]
    render_status_row(
        (
            ("总样本", str(stats.get("total", 0))),
            ("通过池", str(stats.get("passed", 0))),
            ("排除池", str(stats.get("rejected", 0))),
            ("含缺失项", str(stats.get("missing", 0))),
        )
    )
    k1, k2, k3, k4 = st.columns(4)
    for col, label, val in [
        (k1, "总样本", stats.get("total", 0)),
        (k2, "通过池", stats.get("passed", 0)),
        (k3, "排除池", stats.get("rejected", 0)),
        (k4, "含缺失项", stats.get("missing", 0)),
    ]:
        col.markdown(f"<div class='kpi'><div class='label'>{label}</div><div class='value'>{val}</div></div>", unsafe_allow_html=True)

    if str(stats.get("stage_mode", "")) == "two":
        st.caption(
            f"二段筛选结果：第一段(A/B/C) 保留 {int(stats.get('stage1_passed', 0))} 只 ｜ "
            f"第二段(D) 后最终保留 {int(stats.get('stage2_passed', 0))} 只"
        )

    passed_df = res["passed"]
    rejected_df = res["rejected"]
    missing_df = res["missing"]
    st.caption(f"筛选时间: {res.get('run_at', '--')}")

    xlsx_bytes = filter_export_results_excel(passed_df, rejected_df, missing_df)
    st.download_button(
        "导出 Excel（通过池/排除池/缺失项）",
        data=xlsx_bytes,
        file_name=f"filter_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        width="stretch",
    )

    tab1, tab2, tab3 = st.tabs(["通过池", "排除池", "缺失项"])
    with tab1:
        t1, t2, t3 = st.columns([1.2, 1.2, 3.6], vertical_alignment="bottom")
        top_n = int(
            t1.number_input(
                "TopN（按总市值）",
                min_value=0,
                max_value=2000,
                value=50,
                step=10,
                key="flt_topn_mv_main",
            )
        )
        mv_desc = t2.checkbox("市值降序", value=True, key="flt_mv_desc_main")

        passed_view = passed_df.copy()
        mv_effective = False
        if "total_mv" in passed_view.columns:
            passed_view["_mv_sort"] = pd.to_numeric(passed_view["total_mv"], errors="coerce")
            mv_effective = bool(passed_view["_mv_sort"].notna().any())
            passed_view = passed_view.sort_values("_mv_sort", ascending=not mv_desc, na_position="last")
            passed_view = passed_view.drop(columns=["_mv_sort"], errors="ignore")
        if (not mv_effective) and ("market" in passed_view.columns):
            hk_only = passed_view["market"].astype(str).str.upper().eq("HK").all()
            if hk_only:
                st.warning("当前港股快照缺少总市值字段，市值排序暂不可用。先执行一次“更新全市场数据（运维台）”后再试。")
        if top_n > 0:
            passed_view = passed_view.head(top_n)

        t3.caption(f"展示数量: {len(passed_view)} / {len(passed_df)}")
        show1 = _display_df(passed_view)
        cols1 = [c for c in FILTER_DISPLAY_COLUMNS if c in show1.columns]
        st.dataframe(show1[cols1] if cols1 else show1, width="stretch", hide_index=True)
    with tab2:
        cols2 = [c for c in FILTER_DISPLAY_COLUMNS if c in rejected_df.columns]
        show2 = _display_df(rejected_df[cols2] if cols2 else rejected_df)
        st.dataframe(show2, width="stretch", hide_index=True)
    with tab3:
        cols3 = [c for c in FILTER_DISPLAY_COLUMNS if c in missing_df.columns]
        show3 = _display_df(missing_df[cols3] if cols3 else missing_df)
        st.dataframe(show3, width="stretch", hide_index=True)


def _render_backtest_page():
    render_section_intro(
        "回测系统",
        "这是 Quant 的第4个功能：通用港股多空对冲回测。支持策略配置、数据更新、回测执行与HTML报告产出。",
        kicker="Backtest",
        pills=("Universe管理", "策略配置", "一键回测", "交互报告"),
    )

    if "bt_console_output" not in st.session_state:
        st.session_state["bt_console_output"] = ""

    if not BACKTEST_RUNNER.exists():
        st.error(f"未找到回测入口: {BACKTEST_RUNNER}")
        return

    if "bt_preview_latest_open" not in st.session_state:
        st.session_state["bt_preview_latest_open"] = False

    BACKTEST_STRATEGY_DIR.mkdir(parents=True, exist_ok=True)
    BACKTEST_STRATEGY_TRASH_DIR.mkdir(parents=True, exist_ok=True)
    strategy_files = sorted(
        [p for p in BACKTEST_STRATEGY_DIR.glob("*.yaml") if p.is_file()],
        key=lambda p: (p.stat().st_mtime, p.name.lower()),
        reverse=True,
    )
    trash_files = sorted([p for p in BACKTEST_STRATEGY_TRASH_DIR.glob("*.yaml") if p.is_file()])
    if not strategy_files:
        st.info("策略库为空：可直接在下方输入框粘贴策略，然后保存到策略库。")

    st.markdown(
        """
        <style>
        [class*="st-key-bt_"] div.stButton > button {
          min-height: 2.2rem !important;
          padding: 0.25rem 0.75rem !important;
          font-size: 0.9rem !important;
          border-radius: 12px !important;
        }
        [class*="st-key-bt_load_strategy_"] div.stButton > button {
          min-height: 1.65rem !important;
          padding: 0.08rem 0.5rem !important;
          border-radius: 10px !important;
          font-size: 0.84rem !important;
        }
        [class*="st-key-bt_close_strategy_"] div.stButton > button,
        [class*="st-key-bt_lock_strategy_"] div.stButton > button {
          min-height: 1.3rem !important;
          min-width: 1.35rem !important;
          padding: 0rem 0.28rem !important;
          border-radius: 10px !important;
          font-size: 0.8rem !important;
        }
        [class*="st-key-bt_toggle_latest_preview"] div.stButton > button,
        [class*="st-key-bt_download_latest"] div.stDownloadButton > button {
          min-height: 2.0rem !important;
          border-radius: 10px !important;
          font-size: 0.98rem !important;
          font-weight: 650 !important;
          padding: 0.22rem 0.72rem !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    strategy_names = [p.name for p in strategy_files]
    if "bt_strategy_text" not in st.session_state:
        st.session_state["bt_strategy_text"] = ""
    if "bt_strategy_source" not in st.session_state:
        st.session_state["bt_strategy_source"] = "__inline__"

    selected_cfg = str(st.session_state.get("bt_selected_strategy", ""))
    if selected_cfg not in strategy_names:
        selected_cfg = ""
        st.session_state["bt_selected_strategy"] = ""

    strategy_source = str(st.session_state.get("bt_strategy_source", "") or "")
    if strategy_source and strategy_source not in {"__inline__", "__manual__"} and strategy_source not in strategy_names:
        st.session_state["bt_strategy_text"] = ""
        st.session_state["bt_strategy_source"] = "__inline__"

    def _run_cmd(cmd: list[str], label: str) -> bool:
        with st.spinner(f"{label} 执行中..."):
            try:
                proc = subprocess.run(
                    cmd,
                    cwd=str(BACKTEST_DIR),
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    timeout=1800,
                )
                output = (proc.stdout or "") + ("\n" + proc.stderr if proc.stderr else "")
                st.session_state["bt_console_output"] = output.strip()
                if proc.returncode == 0:
                    st.success(f"{label} 完成")
                    return True
                st.error(f"{label} 失败（exit={proc.returncode}）")
                return False
            except Exception as exc:
                st.error(f"{label} 异常: {exc}")
                return False

    def _extract_strategy_codes(strategy_text: str) -> list[str]:
        text = (strategy_text or "").strip()
        if not text:
            return []

        codes: list[str] = []
        try:
            import yaml  # type: ignore

            obj = yaml.safe_load(text) or {}
            if isinstance(obj, dict):
                for key in ("long_positions", "short_positions"):
                    items = obj.get(key, []) or []
                    if isinstance(items, list):
                        for item in items:
                            if isinstance(item, dict) and item.get("code"):
                                codes.append(str(item.get("code", "")).strip().upper())
        except Exception:
            # 回退到正则提取，容忍非严格 YAML 的中间态。
            codes = [m.strip().upper() for m in re.findall(r'^\s*code\s*:\s*["\']?([A-Za-z0-9.^_-]+)["\']?\s*$', text, flags=re.M)]

        seen: set[str] = set()
        out: list[str] = []
        for code in codes:
            if code and code not in seen:
                seen.add(code)
                out.append(code)
        return out

    def _autofill_universe_by_codes(codes: list[str]) -> tuple[list[str], str]:
        if not codes:
            return [], ""
        try:
            import yaml  # type: ignore
        except Exception as exc:
            return [], f"自动补全失败：缺少 PyYAML 依赖（{exc}）"

        try:
            if BACKTEST_STRATEGY_DIR.exists():
                # 保证目录存在时，universe 也可写。
                BACKTEST_DIR.mkdir(parents=True, exist_ok=True)
            universe_path = BACKTEST_DIR / "config" / "universe.yaml"
            universe_path.parent.mkdir(parents=True, exist_ok=True)
            if universe_path.exists():
                raw = yaml.safe_load(universe_path.read_text(encoding="utf-8")) or {}
            else:
                raw = {}
            if not isinstance(raw, dict):
                raw = {}

            sectors = raw.get("sectors")
            if not isinstance(sectors, dict):
                sectors = {}
                raw["sectors"] = sectors

            # 收集全局已有 code，避免重复。
            existing_codes: set[str] = set()
            for sec_obj in sectors.values():
                if not isinstance(sec_obj, dict):
                    continue
                groups = sec_obj.get("groups", {})
                if not isinstance(groups, dict):
                    continue
                for grp_obj in groups.values():
                    if not isinstance(grp_obj, dict):
                        continue
                    stocks = grp_obj.get("stocks", [])
                    if not isinstance(stocks, list):
                        continue
                    for st_obj in stocks:
                        if isinstance(st_obj, dict) and st_obj.get("code"):
                            existing_codes.add(str(st_obj.get("code")).strip().upper())

            auto_sector = sectors.get("auto_import")
            if not isinstance(auto_sector, dict):
                auto_sector = {
                    "name": "自动补全",
                    "description": "由策略校验按钮自动补齐的标的",
                    "sector_benchmark": "^HSI",
                    "groups": {},
                }
                sectors["auto_import"] = auto_sector

            groups = auto_sector.get("groups")
            if not isinstance(groups, dict):
                groups = {}
                auto_sector["groups"] = groups

            auto_group = groups.get("from_strategy")
            if not isinstance(auto_group, dict):
                auto_group = {
                    "name": "来自策略",
                    "stocks": [],
                }
                groups["from_strategy"] = auto_group

            stocks = auto_group.get("stocks")
            if not isinstance(stocks, list):
                stocks = []
                auto_group["stocks"] = stocks

            added_codes: list[str] = []
            for code in codes:
                c = str(code).strip().upper()
                if not c or c in existing_codes:
                    continue
                stocks.append(
                    {
                        "code": c,
                        "name": c,
                        "tags": ["auto-added", "from-strategy"],
                    }
                )
                existing_codes.add(c)
                added_codes.append(c)

            if added_codes:
                universe_path.write_text(
                    yaml.safe_dump(
                        raw,
                        allow_unicode=True,
                        sort_keys=False,
                        default_flow_style=False,
                    ),
                    encoding="utf-8",
                )
            return added_codes, ""
        except Exception as exc:
            return [], f"自动补全失败：{exc}"

    st.markdown("### 策略库管理")
    st.caption("点击策略名载入；右上角小 × 删除会进入回收站。")

    def _read_strategy_name_for_delete(src: Path) -> str:
        try:
            raw = src.read_text(encoding="utf-8", errors="replace")
        except Exception:
            return src.stem
        m = re.search(r'^\s*strategy_name\s*:\s*["\']?(.*?)["\']?\s*$', raw, flags=re.MULTILINE)
        if m:
            return (m.group(1) or "").strip() or src.stem
        return src.stem

    def _archive_paper_runs_for_strategy(src: Path) -> dict:
        trader = PaperTrader(base_dir=BACKTEST_DIR, logger=lambda *args, **kwargs: None)
        strategy_name = _read_strategy_name_for_delete(src)
        return trader.archive_runs_for_strategy(config_path=src, strategy_name=strategy_name)

    def _restore_paper_runs_for_strategy(dst: Path) -> dict:
        trader = PaperTrader(base_dir=BACKTEST_DIR, logger=lambda *args, **kwargs: None)
        strategy_name = _read_strategy_name_for_delete(dst)
        return trader.restore_runs_for_strategy(config_path=dst, strategy_name=strategy_name)

    def _move_strategy_to_trash(src: Path) -> Path:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name = f"{src.stem}__{ts}{src.suffix}"
        dst = BACKTEST_STRATEGY_TRASH_DIR / base_name
        idx = 1
        while dst.exists():
            dst = BACKTEST_STRATEGY_TRASH_DIR / f"{src.stem}__{ts}_{idx}{src.suffix}"
            idx += 1
        shutil.move(str(src), str(dst))
        return dst
    per_row = 3
    if not strategy_files:
        st.caption("暂无策略文件。可在下方填写后点击“保存到策略库”。")
    for start in range(0, len(strategy_files), per_row):
        chunk = strategy_files[start : start + per_row]
        cols = st.columns(per_row, vertical_alignment="top")
        for idx, p in enumerate(chunk):
            name = p.name
            with cols[idx]:
                b1, b2 = st.columns([8.8, 1.2], vertical_alignment="top")
                if b1.button(name, width="stretch", key=f"bt_load_strategy_{name}"):
                    try:
                        st.session_state["bt_selected_strategy"] = name
                        st.session_state["bt_strategy_text"] = p.read_text(encoding="utf-8")
                        st.session_state["bt_strategy_source"] = name
                        st.rerun()
                    except Exception as exc:
                        st.error(f"读取策略失败: {exc}")
                if b2.button("✕", width="stretch", key=f"bt_close_strategy_{name}"):
                    try:
                        archive_info = _archive_paper_runs_for_strategy(p)
                        _move_strategy_to_trash(p)
                        if st.session_state.get("bt_selected_strategy") == name:
                            st.session_state["bt_selected_strategy"] = ""
                            st.session_state["bt_strategy_text"] = ""
                            st.session_state["bt_strategy_source"] = "__inline__"
                        archived_count = int(archive_info.get("count", 0) or 0)
                        if archived_count > 0:
                            st.success(f"已移入回收站: {name} ｜ 同步归档模拟实盘 {archived_count} 个，资金占用已释放")
                        else:
                            st.success(f"已移入回收站: {name}")
                        st.rerun()
                    except Exception as exc:
                        st.error(f"删除失败: {exc}")

    st.markdown("#### 回收站")
    trash_files = sorted([p for p in BACKTEST_STRATEGY_TRASH_DIR.glob("*.yaml") if p.is_file()], reverse=True)
    if trash_files:
        t1, t2, t3 = st.columns([2.4, 1.0, 1.0], vertical_alignment="bottom")
        trash_name = t1.selectbox("回收站文件", options=[p.name for p in trash_files], key="bt_trash_selected")
        if t2.button("恢复", width="stretch", key="bt_trash_restore"):
            src = BACKTEST_STRATEGY_TRASH_DIR / trash_name
            raw_name = trash_name.split("__", 1)[0] + ".yaml"
            dst = BACKTEST_STRATEGY_DIR / raw_name
            if dst.exists():
                dst = BACKTEST_STRATEGY_DIR / f"{dst.stem}_restored_{datetime.now().strftime('%H%M%S')}{dst.suffix}"
            try:
                shutil.move(str(src), str(dst))
                restore_info = _restore_paper_runs_for_strategy(dst)
                restored_count = int(restore_info.get("count", 0) or 0)
                if restored_count > 0:
                    st.success(f"已恢复: {dst.name} ｜ 同步恢复模拟实盘 {restored_count} 个")
                else:
                    st.success(f"已恢复: {dst.name}")
                st.rerun()
            except Exception as exc:
                st.error(f"恢复失败: {exc}")
        if t3.button("清空回收站", width="stretch", key="bt_trash_clear"):
            err = None
            for fp in trash_files:
                try:
                    fp.unlink(missing_ok=True)
                except Exception as exc:
                    err = exc
            if err:
                st.error(f"清空失败: {err}")
            else:
                st.success("回收站已清空。")
                st.rerun()
    else:
        st.caption("回收站为空。")

    st.markdown("### 策略编写")
    bt_mode = st.radio("编写模式", options=["手动编写", "AI辅助写策略"], horizontal=True, key="bt_mode_main")
    if "bt_ai_prompt" not in st.session_state:
        st.session_state["bt_ai_prompt"] = ""
    if "bt_ai_draft_yaml" not in st.session_state:
        st.session_state["bt_ai_draft_yaml"] = ""
    if "bt_ai_draft_meta" not in st.session_state:
        st.session_state["bt_ai_draft_meta"] = {}

    if bt_mode == "AI辅助写策略":
        render_section_intro(
            "AI 策略草拟",
            "输入策略想法，AI 先生成可校验的港股多空 YAML 草案；你确认后再保存或直接回测。",
            kicker="DeepSeek",
            pills=("自然语言", "YAML草案", "校验后再保存"),
        )
        ai_template_options = ["(不使用模板)"] + strategy_names
        ai_cols = st.columns([1.2, 1.0], vertical_alignment="bottom")
        selected_template = ai_cols[0].selectbox("参考模板", options=ai_template_options, key="bt_ai_template")
        ai_save_name = ai_cols[1].text_input("AI草案保存文件名（可选）", value="", key="bt_ai_save_name")
        st.text_area(
            "策略想法",
            height=110,
            key="bt_ai_prompt",
            placeholder="例如：做多高股息央企和现金流稳健龙头，做空持续亏损的AI概念股，偏防守，月度调仓，总资金100万港币。",
        )
        ai_btn_cols = st.columns([1.0, 1.0, 1.2], vertical_alignment="bottom")
        if ai_btn_cols[0].button("生成AI策略草案", type="primary", width="stretch", key="bt_ai_generate"):
            try:
                draft = _bt_generate_ai_strategy_draft(
                    user_prompt=str(st.session_state.get("bt_ai_prompt", "") or ""),
                    template_name=str(selected_template or ""),
                )
                st.session_state["bt_ai_draft_yaml"] = draft["yaml_text"]
                st.session_state["bt_ai_draft_meta"] = draft
                if draft.get("valid"):
                    st.success(
                        f"草案生成完成：{draft.get('strategy_name') or '未命名'} ｜ "
                        f"{float(draft.get('elapsed', 0.0)):.1f}s ｜ 约 ${float(draft.get('cost', 0.0)):.4f}"
                    )
                else:
                    st.error(str(draft.get("error", "草案校验失败")))
            except Exception as exc:
                st.error(f"AI 生成失败: {exc}")
        if ai_btn_cols[1].button("采用草案到下方编辑区", width="stretch", key="bt_ai_apply_draft"):
            draft_yaml = str(st.session_state.get("bt_ai_draft_yaml", "") or "")
            if not draft_yaml.strip():
                st.error("当前没有 AI 草案。")
            else:
                st.session_state["bt_strategy_text"] = draft_yaml
                st.session_state["bt_strategy_source"] = "__manual__"
                st.success("已将 AI 草案写入下方策略编辑区。")
                st.rerun()
        if ai_btn_cols[2].button("保存并直接回测AI草案", width="stretch", key="bt_ai_save_run"):
            draft_yaml = str(st.session_state.get("bt_ai_draft_yaml", "") or "")
            ok, err = _bt_validate_strategy_yaml_text(draft_yaml)
            if not draft_yaml.strip():
                st.error("当前没有 AI 草案。")
            elif not ok:
                st.error(err or "AI 草案尚未通过校验。")
            else:
                raw = (ai_save_name or "").strip() or f"{(st.session_state.get('bt_ai_draft_meta', {}) or {}).get('strategy_name', 'ai_strategy')}.yaml"
                safe = re.sub(r'[\\/:*?"<>|\x00-\x1f]', '_', raw).strip().strip(".")
                if not safe:
                    safe = "ai_strategy.yaml"
                if not safe.lower().endswith((".yaml", ".yml")):
                    safe = f"{safe}.yaml"
                target = BACKTEST_STRATEGY_DIR / safe
                try:
                    target.write_text(draft_yaml, encoding="utf-8")
                    st.session_state["bt_selected_strategy"] = target.name
                    st.session_state["bt_strategy_text"] = draft_yaml
                    st.session_state["bt_strategy_source"] = target.name
                    _run_cmd(
                        [
                            sys.executable,
                            "run_backtest.py",
                            "--config",
                            f"config/strategies/{target.name}",
                            "--output",
                            "reports",
                            "--no-browser",
                        ],
                        f"运行AI策略({target.name})",
                    )
                except Exception as exc:
                    st.error(f"保存或回测失败: {exc}")

        draft_yaml = str(st.session_state.get("bt_ai_draft_yaml", "") or "")
        if draft_yaml.strip():
            draft_meta = st.session_state.get("bt_ai_draft_meta", {}) or {}
            render_status_row(
                (
                    ("草案名称", str(draft_meta.get("strategy_name", "未命名")) or "未命名"),
                    ("校验结果", "通过" if bool(draft_meta.get("valid")) else "失败"),
                    ("生成耗时", f"{float(draft_meta.get('elapsed', 0.0)):.1f}s"),
                )
            )
            st.text_area("AI 策略草案 YAML", value=draft_yaml, height=360, key="bt_ai_draft_preview")

    st.markdown("### 策略输入（直接粘贴即可）")
    st.caption("把完整 YAML 放进表单，只有点击保存或运行时才提交文本，避免边打字边触发页面重算。")
    with st.form("bt_strategy_text_form", clear_on_submit=False):
        st.text_area(
            "策略YAML",
            height=420,
            key="bt_strategy_text",
        )
        s1, s2, s3 = st.columns([1.2, 1.1, 1.1], vertical_alignment="bottom")
        save_name = s1.text_input("保存文件名（可选）", value="", key="bt_save_name")
        save_submitted = s2.form_submit_button("保存到策略库", width="stretch")
        run_pasted_submitted = s3.form_submit_button("粘贴策略并运行", type="primary", width="stretch")

    if save_submitted:
        text = (st.session_state.get("bt_strategy_text") or "").strip()
        if not text:
            st.error("策略文本为空，无法保存。")
        else:
            raw = (save_name or "").strip()
            safe = raw or "my_strategy.yaml"
            # 允许中文等Unicode文件名，仅替换文件系统常见非法字符。
            safe = re.sub(r'[\\/:*?"<>|\x00-\x1f]', '_', safe)
            safe = safe.strip().strip(".")
            if not safe:
                safe = "my_strategy.yaml"
            if not safe.lower().endswith((".yaml", ".yml")):
                safe = f"{safe}.yaml"
            target = BACKTEST_STRATEGY_DIR / safe
            try:
                target.write_text(text, encoding="utf-8")
                st.success(f"已保存策略: {target.name}")
                st.rerun()
            except Exception as exc:
                st.error(f"保存失败: {exc}")

    if run_pasted_submitted:
        strategy_text = (st.session_state.get("bt_strategy_text") or "").strip()
        if not strategy_text:
            st.error("请先粘贴策略 YAML。")
        else:
            pasted_path = BACKTEST_STRATEGY_DIR / "_pasted_strategy_from_ui.yaml"
            try:
                pasted_path.write_text(strategy_text, encoding="utf-8")
                _run_cmd(
                    [
                        sys.executable,
                        "run_backtest.py",
                        "--config",
                        f"config/strategies/{pasted_path.name}",
                        "--output",
                        "reports",
                        "--no-browser",
                    ],
                    "运行粘贴策略",
                )
            except Exception as exc:
                st.error(f"写入粘贴策略失败: {exc}")

    c1, c2, c3, c4 = st.columns(4)
    if c1.button("安装回测依赖", width="stretch", key="bt_install_deps"):
        _run_cmd([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"], "安装依赖")
    if c2.button("校验Universe", width="stretch", key="bt_validate_universe"):
        # 自动补齐当前策略中的缺失代码，减少手工维护 universe。
        strategy_text_for_fill = (st.session_state.get("bt_strategy_text") or "").strip()
        if not strategy_text_for_fill and selected_cfg:
            try:
                strategy_text_for_fill = (BACKTEST_STRATEGY_DIR / selected_cfg).read_text(encoding="utf-8")
            except Exception:
                strategy_text_for_fill = ""

        if strategy_text_for_fill:
            codes = _extract_strategy_codes(strategy_text_for_fill)
            if codes:
                added_codes, err = _autofill_universe_by_codes(codes)
                if err:
                    st.warning(err)
                elif added_codes:
                    preview = ", ".join(added_codes[:8])
                    suffix = " ..." if len(added_codes) > 8 else ""
                    st.success(f"已自动补齐 {len(added_codes)} 个代码到 universe（auto_import/from_strategy）：{preview}{suffix}")
        _run_cmd([sys.executable, "run_backtest.py", "--validate-universe"], "校验Universe")
    if c3.button("更新缓存数据", width="stretch", key="bt_update_data"):
        _run_cmd([sys.executable, "run_backtest.py", "--update-data-only", "--start", "2021-01-01", "--end", "today"], "更新缓存")
    if c4.button("运行选中文件", width="stretch", key="bt_run", disabled=not bool(selected_cfg)):
        if not selected_cfg:
            st.error("当前没有策略文件，请先在下方保存一个策略到策略库。")
        else:
            _run_cmd(
                [
                    sys.executable,
                    "run_backtest.py",
                    "--config",
                    f"config/strategies/{selected_cfg}",
                    "--output",
                    "reports",
                    "--no-browser",
                ],
                "运行回测",
            )

    console_output = st.session_state.get("bt_console_output", "")
    if console_output:
        st.caption("最近一次命令输出")
        st.code(console_output, language="bash")

    reports = sorted(BACKTEST_REPORT_DIR.glob("report_*.html"), key=lambda p: p.stat().st_mtime, reverse=True)
    if reports:
        latest = reports[0]
        st.success(f"最新报告: {latest.name}")
        preview_label = "打开页面内预览最新报告" if not st.session_state.get("bt_preview_latest_open", False) else "关闭页面内预览最新报告"
        pb1, pb2 = st.columns(2)
        with pb1:
            if st.button(preview_label, width="stretch", key="bt_toggle_latest_preview"):
                st.session_state["bt_preview_latest_open"] = not bool(st.session_state.get("bt_preview_latest_open", False))
                st.rerun()
        with pb2:
            st.download_button(
                "下载最新报告HTML",
                data=latest.read_bytes(),
                file_name=latest.name,
                mime="text/html",
                width="stretch",
                key="bt_download_latest",
            )
        if st.session_state.get("bt_preview_latest_open", False):
            try:
                html(latest.read_text(encoding="utf-8"), height=920, scrolling=True)
            except Exception as exc:
                st.warning(f"预览失败: {exc}")

        with st.expander("查看报告列表", expanded=False):
            rows = []
            for p in reports[:30]:
                stat = p.stat()
                rows.append(
                    {
                        "文件名": p.name,
                        "修改时间": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                        "大小(KB)": round(stat.st_size / 1024.0, 1),
                    }
                )
            st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)
    else:
        st.info("暂未找到回测报告，请先点击“运行回测”。")


def _render_paper_page():
    render_section_intro(
        "模拟实盘",
        "按策略逐日推进模拟持仓，记录交易与快照；入口即策略，先启动，后续逐日更新。",
        kicker="Paper",
        pills=("策略入口", "逐日更新", "看板输出"),
    )

    if "paper_console_output" not in st.session_state:
        st.session_state["paper_console_output"] = ""

    def _paper_daily_agent_status() -> dict:
        plist_path = Path.home() / "Library" / "LaunchAgents" / "com.quant.paper_daily.plist"
        out_log = PROJECT_ROOT / "data" / "logs" / "paper_daily.out.log"
        installed = plist_path.exists()
        last_auto = ""
        if out_log.exists():
            try:
                lines = [line.strip() for line in out_log.read_text(encoding="utf-8", errors="replace").splitlines() if line.strip()]
                for line in reversed(lines):
                    m = re.match(r"^\[PAPER-DAILY\]\s+(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+", line)
                    if m:
                        last_auto = m.group(1)
                        break
            except Exception:
                last_auto = ""

        next_run = ""
        if installed:
            now = datetime.now(ZoneInfo("Asia/Shanghai"))
            candidate = now.replace(hour=16, minute=20, second=0, microsecond=0)
            while True:
                if candidate.weekday() < 5 and candidate > now:
                    next_run = candidate.strftime("%Y-%m-%d %H:%M")
                    break
                candidate = (candidate + timedelta(days=1)).replace(hour=16, minute=20, second=0, microsecond=0)

        return {
            "installed": installed,
            "last_auto": last_auto or "--",
            "next_run": next_run or "--",
        }

    BACKTEST_PAPER_DIR.mkdir(parents=True, exist_ok=True)
    strategy_files = sorted(
        [p for p in BACKTEST_STRATEGY_DIR.glob("*.yaml") if p.is_file()],
        key=lambda p: (p.stat().st_mtime, p.name.lower()),
        reverse=True,
    )
    if not strategy_files:
        st.info("当前没有策略文件，请先在回测系统里保存策略。")
        return
    if not BACKTEST_PAPER_RUNNER.exists():
        st.error(f"未找到模拟实盘入口: {BACKTEST_PAPER_RUNNER}")
        return

    st.caption("规则：每个交易日 16:20 自动推进全部运行中模拟盘到最新交易日，并自动重建看板。")

    st.markdown(
        """
        <style>
        .paper-card-wrap {
          border: 1px solid rgba(255,255,255,0.08);
          border-radius: 14px;
          padding: 14px 14px 10px 14px;
          background: rgba(14, 25, 42, 0.38);
          min-height: 196px;
          display: flex;
          flex-direction: column;
          gap: 8px;
        }
        .paper-card-title {
          font-size: 1.12rem;
          font-weight: 700;
          line-height: 1.25;
          color: rgba(245, 248, 255, 0.96);
          min-height: 2.6rem;
          display: -webkit-box;
          -webkit-line-clamp: 2;
          -webkit-box-orient: vertical;
          overflow: hidden;
        }
        .paper-card-desc {
          color: rgba(210, 218, 232, 0.82);
          font-size: 0.95rem;
          line-height: 1.45;
          min-height: 2.8rem;
          max-height: 2.8rem;
          display: -webkit-box;
          -webkit-line-clamp: 2;
          -webkit-box-orient: vertical;
          overflow: hidden;
        }
        .paper-card-meta {
          color: rgba(186, 198, 220, 0.86);
          font-size: 0.92rem;
          min-height: 1.5rem;
          white-space: nowrap;
          overflow: hidden;
          text-overflow: ellipsis;
        }
        [class*="st-key-paper_action_"] div.stButton > button {
          min-height: 2.15rem !important;
          border-radius: 10px !important;
          font-size: 1.02rem !important;
          font-weight: 700 !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    def _paper_run_id_from_strategy(filename: str) -> str:
        stem = Path(filename).stem
        safe = re.sub(r'[\\/:*?"<>|\x00-\x1f]', "_", stem)
        safe = re.sub(r"\s+", "", safe).strip("._")
        return f"pt_{safe or 'strategy'}"

    def _run_paper_cmd(cmd: list[str], label: str) -> bool:
        with st.spinner(f"{label} 执行中..."):
            try:
                proc = subprocess.run(
                    cmd,
                    cwd=str(BACKTEST_DIR),
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    timeout=1800,
                )
                output = (proc.stdout or "") + ("\n" + proc.stderr if proc.stderr else "")
                st.session_state["paper_console_output"] = output.strip()
                if proc.returncode == 0:
                    st.success(f"{label} 完成")
                    return True
                st.error(f"{label} 失败（exit={proc.returncode}）")
                return False
            except Exception as exc:
                st.error(f"{label} 异常: {exc}")
                return False

    def _auto_sync_dashboard(strategy_paths: list[Path]) -> None:
        """Auto rebuild paper dashboard when strategy files changed."""
        if not BACKTEST_PAPER_RUNNER.exists():
            return
        need_sync = False
        if not BACKTEST_PAPER_DASHBOARD.exists():
            need_sync = True
        else:
            try:
                dash_mtime = BACKTEST_PAPER_DASHBOARD.stat().st_mtime
                watched = [
                    BACKTEST_PAPER_ACTIVE,
                    BACKTEST_PAPER_RUNNER,
                    BACKTEST_DIR / "src" / "paper_trader.py",
                    *strategy_paths,
                ]
                for p in watched:
                    if p.exists() and p.stat().st_mtime > dash_mtime:
                        need_sync = True
                        break
            except Exception:
                need_sync = True
        if not need_sync:
            return
        try:
            proc = subprocess.run(
                [sys.executable, "paper_trade.py", "dashboard"],
                cwd=str(BACKTEST_DIR),
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=1800,
            )
            output = (proc.stdout or "") + ("\n" + proc.stderr if proc.stderr else "")
            if proc.returncode != 0:
                st.session_state["paper_console_output"] = output.strip()
                st.warning("自动同步模拟看板失败，请手动点击“重建模拟看板”。")
        except Exception:
            st.warning("自动同步模拟看板异常，请手动点击“重建模拟看板”。")

    def _load_paper_active_map() -> dict:
        if not BACKTEST_PAPER_ACTIVE.exists():
            return {}
        try:
            obj = json.loads(BACKTEST_PAPER_ACTIVE.read_text(encoding="utf-8"))
            if not isinstance(obj, list):
                return {}
            out = {}
            for item in obj:
                if isinstance(item, dict):
                    rid = str(item.get("run_id", "")).strip()
                    if rid:
                        out[rid] = item
            return out
        except Exception:
            return {}

    def _strategy_meta(path: Path) -> tuple[str, str]:
        strategy_name = path.stem
        description = ""
        try:
            raw = path.read_text(encoding="utf-8", errors="replace")
            m1 = re.search(r'^\s*strategy_name\s*:\s*["\']?(.*?)["\']?\s*$', raw, flags=re.MULTILINE)
            if m1:
                strategy_name = (m1.group(1) or "").strip() or strategy_name
            m2 = re.search(r'^\s*description\s*:\s*["\']?(.*?)["\']?\s*$', raw, flags=re.MULTILINE)
            if m2:
                description = (m2.group(1) or "").strip()
        except Exception:
            pass
        return strategy_name, description

    def _paper_summary(run_id: str, row) -> str:
        if not row:
            return f"未创建（run_id={run_id}）"
        last_date = str(row.get("last_update_date", "")).strip() or "-"
        status = str(row.get("status", "active")).strip() or "active"
        run_dir = Path(str(row.get("run_dir", "")).strip())
        snap = run_dir / "snapshots.csv"
        if snap.exists():
            try:
                sdf = pd.read_csv(snap)
                if not sdf.empty:
                    last = sdf.iloc[-1]
                    eq = float(last.get("equity", float("nan")))
                    cr = float(last.get("cum_return", float("nan")))
                    eq_txt = "-" if not np.isfinite(eq) else f"{eq:,.0f} HKD"
                    cr_txt = "-" if not np.isfinite(cr) else f"{cr:.2%}"
                    return f"{status} | {last_date} | 权益 {eq_txt} | 收益 {cr_txt}"
            except Exception:
                pass
        return f"{status} | 最新 {last_date}"

    active_map = _load_paper_active_map()
    _auto_sync_dashboard(strategy_files)

    active_rows = []
    for rid, row in active_map.items():
        run_dir = Path(str(row.get("run_dir", "")).strip())
        eq = float("nan")
        cr = float("nan")
        snap = run_dir / "snapshots.csv"
        if snap.exists():
            try:
                sdf = pd.read_csv(snap)
                if not sdf.empty:
                    last = sdf.iloc[-1]
                    eq = float(last.get("equity", float("nan")))
                    cr = float(last.get("cum_return", float("nan")))
            except Exception:
                pass
        active_rows.append((rid, row, eq, cr))

    total_equity = float(sum(x[2] for x in active_rows if np.isfinite(x[2])))
    weighted_initial = float(sum((x[2] / (1.0 + x[3])) for x in active_rows if np.isfinite(x[2]) and np.isfinite(x[3]) and (1.0 + x[3]) != 0.0))
    total_cum = (total_equity / weighted_initial - 1.0) if weighted_initial > 0 else float("nan")

    if st.button("推进全部运行中模拟盘到最新交易日", width="stretch", key="paper_update_all"):
        _run_paper_cmd([sys.executable, "paper_trade.py", "update", "--all", "--as-of", "today"], "推进全部运行中模拟盘到最新交易日")
        st.rerun()

    render_status_row(
        (
            ("活跃策略", str(len(active_rows))),
            ("总权益(HKD)", "-" if not np.isfinite(total_equity) else f"{total_equity:,.2f}"),
            ("总收益", "-" if not np.isfinite(total_cum) else f"{total_cum:.2%}"),
        )
    )

    with st.expander("策略入口（单动作）", expanded=False):
        per_row = 3
        for start in range(0, len(strategy_files), per_row):
            chunk = strategy_files[start : start + per_row]
            cols = st.columns(per_row, vertical_alignment="top")
            for idx, p in enumerate(chunk):
                name = p.name
                run_id = _paper_run_id_from_strategy(name)
                row = active_map.get(run_id)
                sname, sdesc = _strategy_meta(p)
                with cols[idx]:
                    st.markdown(
                        f"""
                        <div class="paper-card-wrap">
                          <div class="paper-card-title">{sname}</div>
                          <div class="paper-card-desc">{sdesc or "无策略说明"}</div>
                          <div class="paper-card-meta">{_paper_summary(run_id, row)}</div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )
                    action_label = "更新到今日" if row else "启动模拟"
                    if st.button(action_label, width="stretch", key=f"paper_action_{name}"):
                        if row:
                            _run_paper_cmd(
                                [
                                    sys.executable,
                                    "paper_trade.py",
                                    "update",
                                    "--run-id",
                                    run_id,
                                    "--as-of",
                                    "today",
                                ],
                                f"模拟推进到今日({name})",
                            )
                        else:
                            _run_paper_cmd(
                                [
                                    sys.executable,
                                    "paper_trade.py",
                                    "start",
                                    "--config",
                                    f"config/strategies/{name}",
                                    "--run-id",
                                    run_id,
                                    "--as-of",
                                    "today",
                                ],
                                f"模拟启动({name})",
                            )
                        st.rerun()

    if BACKTEST_PAPER_DASHBOARD.exists():
        with st.expander("模拟盘管理中心", expanded=False):
            try:
                html(BACKTEST_PAPER_DASHBOARD.read_text(encoding="utf-8"), height=1500, scrolling=True)
            except Exception as exc:
                st.warning(f"看板预览失败: {exc}")
        st.download_button(
            "下载模拟看板HTML",
            data=BACKTEST_PAPER_DASHBOARD.read_bytes(),
            file_name=BACKTEST_PAPER_DASHBOARD.name,
            mime="text/html",
            width="stretch",
            key="paper_download_dashboard",
        )
    else:
        if st.button("生成模拟看板", width="stretch", key="paper_build_dashboard_init"):
            _run_paper_cmd([sys.executable, "paper_trade.py", "dashboard"], "生成模拟看板")
            st.rerun()

    console_output = st.session_state.get("paper_console_output", "")
    if console_output:
        st.caption("最近一次模拟命令输出")
        st.code(console_output, language="bash")


def _analysis_job_file(job_id: str) -> str:
    return os.path.join(ANALYSIS_JOB_DIR, f"{job_id}.json")


def _create_analysis_job(
    stock_code: str,
    stock_name: str,
    mode: str,
    quick_json: str,
    deep_json: str,
    quick_hash: str,
    deep_hash: str,
) -> str:
    os.makedirs(ANALYSIS_JOB_DIR, exist_ok=True)
    job_id = f"{stock_code}_{int(pytime.time() * 1000)}"
    job = {
        "job_id": job_id,
        "created_at": datetime.now().strftime("%m-%d %H:%M:%S"),
        "stock_code": str(stock_code),
        "stock_name": str(stock_name),
        "analysis_engine": "multi_agent_v1",
        "mode": str(mode),
        "status": "pending",
        "quick_json": quick_json,
        "deep_json": deep_json,
        "quick_hash": quick_hash,
        "deep_hash": deep_hash,
    }
    _save_json_file(_analysis_job_file(job_id), job)
    return job_id


def _upsert_live_analysis_job(
    stock_code: str,
    stock_name: str,
    quick_json: str,
    deep_json: str,
    quick_hash: str,
    deep_hash: str,
) -> str:
    os.makedirs(ANALYSIS_JOB_DIR, exist_ok=True)
    job_id = f"live_{stock_code}"
    path = _analysis_job_file(job_id)
    current = _load_json_file(path)
    now_text = datetime.now().strftime("%m-%d %H:%M:%S")

    if current and isinstance(current, dict):
        current["stock_name"] = str(stock_name)
        current["quick_json"] = quick_json
        current["deep_json"] = deep_json
        current["quick_hash"] = quick_hash
        current["deep_hash"] = deep_hash
        current["updated_at"] = now_text
        if str(current.get("analysis_engine", "")) != "multi_agent_v1":
            current["analysis_engine"] = "multi_agent_v1"
            current["mode"] = "idle"
            current["status"] = "pending"
            current.pop("final_text", None)
            current.pop("stats", None)
            current.pop("trigger_alert", None)
        if "created_at" not in current:
            current["created_at"] = now_text
        if "mode" not in current:
            current["mode"] = "idle"
        if "status" not in current:
            current["status"] = "pending"
        _save_json_file(path, current)
        return job_id

    job = {
        "job_id": job_id,
        "created_at": now_text,
        "updated_at": now_text,
        "stock_code": str(stock_code),
        "stock_name": str(stock_name),
        "analysis_engine": "multi_agent_v1",
        "mode": "idle",
        "status": "pending",
        "quick_json": quick_json,
        "deep_json": deep_json,
        "quick_hash": quick_hash,
        "deep_hash": deep_hash,
    }
    _save_json_file(path, job)
    return job_id


def _normalize_quick_result(quick_raw: str) -> dict:
    obj = _extract_json_object(quick_raw)
    risk = str(obj.get("risk_level", "medium")).lower()
    if risk not in {"low", "medium", "high"}:
        risk = "medium"
    conclusions = obj.get("conclusions", [])
    if not isinstance(conclusions, list):
        conclusions = []
    conclusions = [str(x) for x in conclusions][:3]
    if not conclusions:
        conclusions = ["快筛未返回标准结论，建议人工复核。"]

    need_full = obj.get("need_full_analysis", False)
    if isinstance(need_full, str):
        need_full = need_full.strip().lower() in {"1", "true", "yes", "y", "需要", "是"}
    else:
        need_full = bool(need_full)

    trigger_reasons = obj.get("trigger_reasons", [])
    if not isinstance(trigger_reasons, list):
        trigger_reasons = []
    trigger_reasons = [str(x) for x in trigger_reasons][:4]

    return {
        "risk_level": risk,
        "conclusions": conclusions,
        "need_full_analysis": need_full,
        "trigger_reasons": trigger_reasons,
        "raw": quick_raw,
    }


def _markdown_stream_chunks(text: str, chunk_size: int = 220):
    content = str(text or "")
    if not content:
        return
    for line in content.splitlines(keepends=True):
        if len(line) <= chunk_size:
            yield line
            continue
        for i in range(0, len(line), chunk_size):
            yield line[i : i + chunk_size]


def _render_markdown_stream(text: str) -> None:
    content = str(text or "").strip()
    if not content:
        st.info("暂无分析文本。")
        return
    chunks = _markdown_stream_chunks(content)
    stream_fn = getattr(st, "markdown_stream", None)
    if callable(stream_fn):
        stream_fn(chunks)
        return
    write_stream_fn = getattr(st, "write_stream", None)
    if callable(write_stream_fn):
        write_stream_fn(chunks)
        return
    st.markdown(str(text or ""))


def _render_final_report_block(job_id: str, job_obj: dict, key_suffix: str, height: int = 560, stream_output: bool = False) -> None:
    final_text = str(job_obj.get("final_text", "") or "")
    done_mark = str(job_obj.get("done_at", "") or "")
    text_key = f"job_text_{job_id}_{key_suffix}_{hashlib.md5((final_text + done_mark).encode('utf-8')).hexdigest()[:10]}"
    stats = job_obj.get("stats", {}) or {}
    analyzed_at = str(job_obj.get("done_at") or job_obj.get("updated_at") or job_obj.get("created_at") or "--")
    st.markdown(
        f"<div class='analysis-time-badge'>分析时间: {analyzed_at}</div>",
        unsafe_allow_html=True,
    )
    caption_bits = [
        f"总输入: {stats.get('deep_prompt_tokens', 0)}",
        f"总输出: {stats.get('deep_completion_tokens', 0)}",
    ]
    if int(stats.get("expert_prompt_tokens", 0) or 0) > 0 or int(stats.get("judge_prompt_tokens", 0) or 0) > 0:
        caption_bits.extend(
            [
                f"专家输入: {stats.get('expert_prompt_tokens', 0)}",
                f"专家输出: {stats.get('expert_completion_tokens', 0)}",
                f"法官输入: {stats.get('judge_prompt_tokens', 0)}",
                f"法官输出: {stats.get('judge_completion_tokens', 0)}",
            ]
        )
    caption_bits.append(f"预估总成本: {float(stats.get('total_cost', 0) or 0):.4f} 元")
    st.caption(" | ".join(caption_bits))
    report_text = _sanitize_deepseek_report(final_text)
    if report_text:
        if stream_output:
            _render_markdown_stream(report_text)
        else:
            st.markdown(report_text)
    else:
        st.info("暂无分析文本。")
    st.text_area("分析文本（可复制）", value=report_text, height=min(height, 280), key=text_key)
    final_b64 = base64.b64encode(final_text.encode("utf-8")).decode("ascii")
    html(
        f"""
        <div style="margin-top:0.2rem;">
          <button id="copy-job-doc-{job_id}-{key_suffix}"
            style="height:38px;padding:0 0.9rem;border-radius:8px;border:1px solid #a8c2e8;background:#dbeafe;color:#0f2a52;font-size:0.95rem;font-weight:700;cursor:pointer;">
            复制分析文档
          </button>
          <span id="copy-job-msg-{job_id}-{key_suffix}" style="margin-left:0.55rem;color:rgba(239,229,216,0.82);font-size:0.86rem;"></span>
        </div>
        <script>
          const b = document.getElementById("copy-job-doc-{job_id}-{key_suffix}");
          const m = document.getElementById("copy-job-msg-{job_id}-{key_suffix}");
          const t = decodeURIComponent(escape(window.atob("{final_b64}")));
          b.onclick = async function () {{
            try {{
              await navigator.clipboard.writeText(t);
              m.textContent = "已复制";
            }} catch (e) {{
              m.textContent = "复制失败";
            }}
          }};
        </script>
        """,
        height=64,
    )


def _execute_analysis_job(job_id: str, mode: str, ui_prefix: str = "", force_refresh: bool = False) -> dict:
    job = _load_json_file(_analysis_job_file(job_id))
    if not job:
        raise RuntimeError("分析任务不存在或已失效。")

    stock_code = str(job.get("stock_code", ""))
    stock_name = str(job.get("stock_name", ""))
    mode = "deep"

    job["mode"] = mode
    job["status"] = "running"
    job["started_at"] = datetime.now().strftime("%m-%d %H:%M:%S")
    job.pop("error", None)
    _save_json_file(_analysis_job_file(job_id), job)

    progress = st.progress(5, text=f"{ui_prefix}准备任务...")
    cache_store = _load_analysis_cache()
    cooldown_store = _load_json_file(ANALYSIS_COOLDOWN_PATH)
    now_ts = datetime.now().timestamp()

    deep_usage = {"prompt_tokens": 0, "completion_tokens": 0}
    total_cost = 0.0
    deep_report = ""
    deep_source = "未执行"

    deep_json = str(job.get("deep_json", "") or "")
    deep_hash = str(job.get("deep_hash", "") or "")

    try:
        if force_refresh:
            progress.progress(36, text=f"{ui_prefix}强制刷新多智能体分析...")
            deep_report, d_usage, d_cost, _ = _call_multi_agent_analysis(
                json_text=deep_json,
                stock_code=stock_code,
                stock_name=stock_name,
            )
            deep_usage = d_usage
            total_cost += float(d_cost or 0.0)
            deep_source = "手动刷新"
            cache_store[deep_hash] = {
                "engine": "multi_agent_v1",
                "stage": "deep",
                "result": deep_report,
                "usage": deep_usage,
                "saved_at": datetime.now().strftime("%m-%d %H:%M:%S"),
                "stock_code": stock_code,
                "stock_name": stock_name,
            }
            _save_analysis_cache(cache_store)
            cooldown_store[stock_code] = {
                "last_ts": now_ts,
                "last_time": datetime.now().strftime("%m-%d %H:%M:%S"),
                "last_deep_hash": deep_hash,
            }
            _save_json_file(ANALYSIS_COOLDOWN_PATH, cooldown_store)
        else:
            progress.progress(28, text=f"{ui_prefix}检查缓存...")
            d_cached = cache_store.get(deep_hash) if deep_hash else None
            if (
                d_cached
                and isinstance(d_cached, dict)
                and str(d_cached.get("engine", "")) == "multi_agent_v1"
            ):
                deep_report = str(d_cached.get("result", "") or "")
                deep_usage = d_cached.get("usage", {}) or deep_usage
                deep_source = "同快照复用"
            else:
                deep_cd = cooldown_store.get(stock_code, {})
                last_ts = float(deep_cd.get("last_ts", 0) or 0)
                in_cooldown = (now_ts - last_ts) < (DEEP_COOLDOWN_MINUTES * 60)
                cached_hash = str(deep_cd.get("last_deep_hash", ""))
                if in_cooldown and cached_hash:
                    cd_cached = cache_store.get(cached_hash)
                    if (
                        cd_cached
                        and isinstance(cd_cached, dict)
                        and str(cd_cached.get("engine", "")) == "multi_agent_v1"
                    ):
                        deep_report = str(cd_cached.get("result", "") or "")
                        deep_usage = cd_cached.get("usage", {}) or deep_usage
                        deep_source = f"冷却期复用({DEEP_COOLDOWN_MINUTES}分钟)"

                if not deep_report:
                    progress.progress(75, text=f"{ui_prefix}并行执行专家与法官审判...")
                    deep_report, d_usage, d_cost, _ = _call_multi_agent_analysis(
                        json_text=deep_json,
                        stock_code=stock_code,
                        stock_name=stock_name,
                    )
                    deep_usage = d_usage
                    total_cost += float(d_cost or 0.0)
                    cache_store[deep_hash] = {
                        "engine": "multi_agent_v1",
                        "stage": "deep",
                        "result": deep_report,
                        "usage": deep_usage,
                        "saved_at": datetime.now().strftime("%m-%d %H:%M:%S"),
                        "stock_code": stock_code,
                        "stock_name": stock_name,
                    }
                    _save_analysis_cache(cache_store)
                    deep_source = "实时调用"

                cooldown_store[stock_code] = {
                    "last_ts": now_ts,
                    "last_time": datetime.now().strftime("%m-%d %H:%M:%S"),
                    "last_deep_hash": deep_hash,
                }
                _save_json_file(ANALYSIS_COOLDOWN_PATH, cooldown_store)

        progress.progress(100, text=f"{ui_prefix}分析完成")
        final_text = _sanitize_deepseek_report(deep_report)

        stats = {
            "deep_prompt_tokens": int(deep_usage.get("prompt_tokens", 0) or 0),
            "deep_completion_tokens": int(deep_usage.get("completion_tokens", 0) or 0),
            "expert_prompt_tokens": int(deep_usage.get("expert_prompt_tokens", 0) or 0),
            "expert_completion_tokens": int(deep_usage.get("expert_completion_tokens", 0) or 0),
            "judge_prompt_tokens": int(deep_usage.get("judge_prompt_tokens", 0) or 0),
            "judge_completion_tokens": int(deep_usage.get("judge_completion_tokens", 0) or 0),
            "total_cost": float(total_cost),
            "deep_source": deep_source,
        }

        job["status"] = "done"
        job["done_at"] = datetime.now().strftime("%m-%d %H:%M:%S")
        job["final_text"] = final_text
        job["stats"] = stats
        _save_json_file(_analysis_job_file(job_id), job)
        progress.empty()
        return job
    except Exception as exc:
        job["status"] = "failed"
        job["failed_at"] = datetime.now().strftime("%m-%d %H:%M:%S")
        job["error"] = f"{type(exc).__name__}: {exc}"
        _save_json_file(_analysis_job_file(job_id), job)
        progress.empty()
        raise


def _render_analysis_window(job_id: str, embedded: bool = False, auto_mode: str = "") -> None:
    job = _load_json_file(_analysis_job_file(job_id))
    if not job:
        st.error("分析任务不存在或已失效。")
        return

    stock_code = str(job.get("stock_code", ""))
    stock_name = str(job.get("stock_name", ""))
    mode = str(job.get("mode", "idle"))

    if not embedded:
        safe_title = f"{stock_name}({stock_code}) - Quant".replace("\\", "\\\\").replace("'", "\\'")
        html(
            f"""
            <script>
              try {{
                document.title = '{safe_title}';
                if (window.parent && window.parent.document) {{
                  window.parent.document.title = '{safe_title}';
                }}
              }} catch (e) {{}}
            </script>
            """,
            height=0,
        )
        st.title(f"DeepSeek 分析窗口 · {stock_name} ({stock_code})")
        st.caption(f"任务ID: {job_id} | 上次模式: {mode} | 创建时间: {job.get('created_at', '--')}")
    else:
        st.subheader(f"多智能体分析文档 · {stock_name} ({stock_code})")
        st.caption(f"任务ID: {job_id} | 上次模式: {mode}")

    run_mode = ""
    force_refresh = False
    if embedded:
        run_mode = "deep" if auto_mode else ""
    else:
        btn_cols = st.columns(2)
        if btn_cols[0].button("多智能体分析", key=f"analysis_window_deep_{job_id}", width="stretch"):
            run_mode = "deep"
        if btn_cols[1].button("刷新", key=f"analysis_window_refresh_{job_id}", width="stretch"):
            run_mode = "deep"
            force_refresh = True

    if not run_mode:
        if job.get("status") == "done":
            if not embedded:
                st.success("上次分析已完成。可点击上方按钮重新分析。")
            _render_final_report_block(job_id, job, "saved", height=420 if embedded else 560)
        elif job.get("status") == "failed":
            st.error(f"上次分析失败: {job.get('error', '未知错误')}")
            if not embedded:
                st.info("请点击上方按钮重新执行。")
        else:
            st.info("点击“多智能体分析”开始执行。")
        return

    try:
        done_job = _execute_analysis_job(
            job_id=job_id,
            mode="deep",
            ui_prefix=f"{stock_name} ",
            force_refresh=force_refresh,
        )
        if not embedded:
            st.success("深析已完成。")
        else:
            st.success("分析完成。")
        _render_final_report_block(job_id, done_job, "new", height=420 if embedded else 560, stream_output=True)
    except Exception as exc:
        st.error(f"分析失败: {type(exc).__name__}: {exc}")


def _is_hk_code(code: str) -> bool:
    digits = "".join(ch for ch in str(code).strip() if ch.isdigit())
    return len(digits) == 5


def _is_market_open(code: str) -> bool:
    now = datetime.now(ZoneInfo("Asia/Shanghai"))
    if now.weekday() >= 5:
        return False

    t = now.time()
    if _is_hk_code(code):
        # 港股常规交易时段（简化口径）
        return (time(9, 30) <= t <= time(12, 0)) or (time(13, 0) <= t <= time(16, 0))

    # A股常规交易时段
    return (time(9, 30) <= t <= time(11, 30)) or (time(13, 0) <= t <= time(15, 0))


if st.session_state.get("active_page") == "fundamental":
    _render_fundamental_page()
    st.stop()
if st.session_state.get("active_page") == "filter":
    _render_filter_page()
    st.stop()
if st.session_state.get("active_page") == "portfolio":
    _render_portfolio_page(embedded=True)
    st.stop()
if st.session_state.get("active_page") == "backtest":
    _render_backtest_page()
    st.stop()
if st.session_state.get("active_page") == "paper":
    _render_paper_page()
    st.stop()


if "fast_selected_code" not in st.session_state:
    st.session_state["fast_selected_code"] = rows[0]["code"]
    st.session_state["fast_selected_name"] = rows[0]["name"]

selected_code_for_ctrl = st.session_state["fast_selected_code"]
market_open_for_ctrl = _is_market_open(selected_code_for_ctrl)

header_cols = st.columns([2.4, 0.8, 0.6, 0.9], vertical_alignment="bottom")
auto_refresh_on = header_cols[1].checkbox("自动刷新", value=False, key="fast_auto_refresh_on")
auto_refresh_sec = header_cols[2].selectbox(
    "刷新间隔(秒)",
    options=[15, 30, 60, 90, 120],
    index=2,
    key="fast_auto_refresh_sec",
)
if header_cols[3].button("立即刷新", width="stretch"):
    st.session_state["fast_force_refresh"] = True
    st.rerun()
render_section_intro(
    "标的工作台",
    "上方保留自动刷新和即时刷新，下方把股票池拆成持仓与观察两栏，减少切换负担，方便快速定位当前关注标的。",
    kicker="Deck",
    pills=("自动刷新", "持仓 / 观察", "单击切换标的"),
)
render_status_row(
    (
        ("当前标的", f"{st.session_state.get('fast_selected_name', '')} ({selected_code_for_ctrl})"),
        ("市场状态", "交易时段内" if market_open_for_ctrl else "非交易时段"),
        ("刷新策略", f"{auto_refresh_sec} 秒自动刷新" if auto_refresh_on else "手动刷新"),
        ("股票池结构", f"持仓 {_holding_count} / 观察 {_watch_count}"),
    )
)

group_map = get_stock_group_map()
holding_rows = [r for r in rows if group_map.get(str(r["code"]), "watch") == "holding"]
watch_rows = [r for r in rows if group_map.get(str(r["code"]), "watch") != "holding"]

all_pool_codes = [str(r.get("code", "")).strip() for r in rows if str(r.get("code", "")).strip()]
pool_quote_cache = st.session_state.get("fast_pool_quote_cache", {})
pool_quote_latency_ms = float(st.session_state.get("fast_pool_quote_latency_ms", 0.0) or 0.0)
force_refresh_requested = bool(st.session_state.get("fast_force_refresh", False))
should_refresh_pool_quote = bool((auto_refresh_on and market_open_for_ctrl) or force_refresh_requested)
if should_refresh_pool_quote:
    t0_pool = pytime.perf_counter()
    try:
        pool_quote_cache = fetch_realtime_quotes_batch(
            all_pool_codes,
            timeout_sec=0.8,
            fallback_to_provider=False,
        )
        st.session_state["fast_pool_quote_cache"] = pool_quote_cache
        pool_quote_latency_ms = (pytime.perf_counter() - t0_pool) * 1000.0
        st.session_state["fast_pool_quote_latency_ms"] = pool_quote_latency_ms
    except Exception:
        pool_quote_cache = st.session_state.get("fast_pool_quote_cache", {})


def _format_stock_button_label(name: str, code: str, quote_map: dict) -> str:
    quote = quote_map.get(str(code), {}) if isinstance(quote_map, dict) else {}
    px = quote.get("current_price") if isinstance(quote, dict) else None
    cp = quote.get("change_pct") if isinstance(quote, dict) else None
    if px is None:
        return f"{name}\n{code}"
    if cp is None:
        return f"{name} {float(px):.2f}\n{code}"
    return f"{name} {float(px):.2f} ({float(cp):+.2f}%)\n{code}"


def _stock_grid_cols(total: int) -> int:
    if total <= 1:
        return 1
    if total <= 4:
        return 2
    if total <= 9:
        return 3
    return 4


def _render_stock_group(stock_rows, group_key_prefix: str) -> None:
    if not stock_rows:
        st.caption("暂无标的")
        return

    grid_cols = _stock_grid_cols(len(stock_rows))
    for start in range(0, len(stock_rows), grid_cols):
        row_cols = st.columns(grid_cols)
        chunk = stock_rows[start : start + grid_cols]
        for idx, row in enumerate(chunk):
            col = row_cols[idx]
            with col:
                open_col, del_col = st.columns([5.2, 1], vertical_alignment="center")
                with open_col:
                    st.markdown('<div class="stock-open-wrap">', unsafe_allow_html=True)
                    if st.button(
                        f"{row['name']}\n{row['code']}",
                        key=f"open_fast_{group_key_prefix}_{row['code']}",
                        width="stretch",
                    ):
                        st.session_state["fast_selected_code"] = row["code"]
                        st.session_state["fast_selected_name"] = row["name"]
                    st.markdown("</div>", unsafe_allow_html=True)
                with del_col:
                    st.markdown('<div class="stock-del-inline-wrap">', unsafe_allow_html=True)
                    if st.button(
                        "🗑️",
                        key=f"mini_del_{group_key_prefix}_{row['code']}",
                        width="stretch",
                        type="tertiary",
                        help=f"删除 {row['name']}",
                    ):
                        remove_stock_from_pool(row["code"])
                        if st.session_state.get("fast_selected_code") == row["code"]:
                            st.session_state.pop("fast_selected_code", None)
                            st.session_state.pop("fast_selected_name", None)
                        st.rerun()
                    st.markdown("</div>", unsafe_allow_html=True)


holding_rows_needed = math.ceil(len(holding_rows) / max(_stock_grid_cols(len(holding_rows)), 1)) if holding_rows else 1
watch_rows_needed = math.ceil(len(watch_rows) / max(_stock_grid_cols(len(watch_rows)), 1)) if watch_rows else 1
divider_height = max(110, max(holding_rows_needed, watch_rows_needed) * 94 + 16)

group_cols = st.columns([1, 0.02, 1], vertical_alignment="top")
with group_cols[0]:
    st.markdown('<div class="group-title">持仓</div>', unsafe_allow_html=True)
    _render_stock_group(holding_rows, "holding")
with group_cols[1]:
    st.markdown(
        f'<div class="watch-split-divider" style="height:{divider_height}px;"></div>',
        unsafe_allow_html=True,
    )
with group_cols[2]:
    st.markdown('<div class="group-title">观察</div>', unsafe_allow_html=True)
    _render_stock_group(watch_rows, "watch")


def _empty_order_book(levels: int = 10) -> dict:
    return {
        "buy": [{"level": i + 1, "price": None, "volume_lot": None} for i in range(levels)],
        "sell": [{"level": i + 1, "price": None, "volume_lot": None} for i in range(levels)],
    }


def _empty_indicators() -> dict:
    return {
        "macd_hist": None,
        "rsi6": None,
        "rsi12": None,
        "rsi24": None,
        "ma5": None,
        "ma10": None,
        "ma20": None,
        "ma60": None,
        "boll_mid": None,
        "boll_upper": None,
        "boll_lower": None,
        "boll_pct_b": None,
        "boll_bandwidth": None,
    }


def _build_local_fast_panel(symbol: str, name: str, reason: str = "本地快速模式：未主动联网刷新") -> dict:
    normalized = str(symbol).strip()
    order_book_10 = _empty_order_book(10)
    return {
        "symbol": normalized,
        "quote": {
            "symbol": normalized,
            "name": name or normalized,
            "current_price": None,
            "prev_close": None,
            "open": None,
            "change_amount": None,
            "change_pct": None,
            "high": None,
            "low": None,
            "volume": None,
            "amount": None,
            "turnover_rate": None,
            "turnover_rate_estimated": False,
            "amplitude_pct": None,
            "float_market_value_yi": None,
            "total_market_value_yi": None,
            "volume_ratio": None,
            "order_diff": None,
            "vwap": None,
            "premium_pct": None,
            "quote_time": None,
            "is_trading_data": False,
            "pe_dynamic": None,
            "pe_ttm": None,
            "pb": None,
            "order_book_5": {"buy": [], "sell": []},
            "order_book_10": order_book_10,
            "error": None,
        },
        "indicators": _empty_indicators(),
        "intraday": pd.DataFrame(columns=["time", "price", "volume_lot", "amount"]),
        "order_book_5": {"buy": [], "sell": []},
        "order_book_10": order_book_10,
        "rsi_multi": {},
        "tf_indicators": {},
        "depth_note": reason,
        "error": reason,
        "local_only": True,
    }


def _render_fast_panel(selected_code: str, selected_name: str, panel=None):
    if panel is None:
        panel = fetch_fast_panel(selected_code)
    quote = panel["quote"]
    ind = panel["indicators"]
    intraday_df = panel["intraday"]
    order_book_5 = panel["order_book_5"]

    if panel.get("error") and not quote.get("current_price"):
        st.caption(str(panel["error"]))

    selected_slow = next((r for r in rows if str(r.get("code")) == str(selected_code)), {})
    live_val = {} if (panel.get("local_only") or panel.get("realtime_only")) else fetch_live_valuation_snapshot(selected_code)
    quote_pe_dynamic = quote.get("pe_dynamic")
    quote_pb = quote.get("pb")
    quote_pe_ttm = quote.get("pe_ttm")
    pe_dynamic_live = (
        quote_pe_dynamic
        if quote_pe_dynamic is not None
        else live_val.get("pe_dynamic")
        if isinstance(live_val, dict)
        else selected_slow.get("pe_dynamic")
    )
    pe_static_live = (
        (live_val.get("pe_static") if isinstance(live_val, dict) else None)
        if (isinstance(live_val, dict) and live_val.get("pe_static") is not None)
        else selected_slow.get("pe_static")
    )
    pe_rolling_live = (
        (live_val.get("pe_rolling") if isinstance(live_val, dict) else None)
        if (isinstance(live_val, dict) and live_val.get("pe_rolling") is not None)
        else (selected_slow.get("pe_rolling") if selected_slow.get("pe_rolling") is not None else quote_pe_ttm)
    )
    pb_live = (
        quote_pb
        if quote_pb is not None
        else live_val.get("pb")
        if isinstance(live_val, dict)
        else selected_slow.get("pb")
    )
    dy_live = (
        live_val.get("dividend_yield")
        if isinstance(live_val, dict) and live_val.get("dividend_yield") is not None
        else selected_slow.get("dividend_yield")
    )
    sell_lv_for_json = sorted(order_book_5.get("sell", []), key=lambda x: int(x.get("level", 0)))
    buy_lv_for_json = sorted(order_book_5.get("buy", []), key=lambda x: int(x.get("level", 0)))
    sell_total_for_json = sum(float(r.get("volume_lot") or 0) for r in sell_lv_for_json)
    buy_total_for_json = sum(float(r.get("volume_lot") or 0) for r in buy_lv_for_json)
    ofi_for_json = (buy_total_for_json / sell_total_for_json) if sell_total_for_json > 0 else None
    weibi_for_json = ((buy_total_for_json - sell_total_for_json) / (buy_total_for_json + sell_total_for_json) * 100) if (buy_total_for_json + sell_total_for_json) > 0 else None
    ask1_for_json = next((r.get("price") for r in sell_lv_for_json if int(r.get("level", 0)) == 1), None)
    bid1_for_json = next((r.get("price") for r in buy_lv_for_json if int(r.get("level", 0)) == 1), None)
    spread_for_json = (
        float(ask1_for_json) - float(bid1_for_json)
        if (ask1_for_json is not None and bid1_for_json is not None)
        else None
    )

    fast_compact_metrics = {
        "snapshot": {
            "current_price": quote.get("current_price"),
            "change_pct": quote.get("change_pct"),
            "change_amount": quote.get("change_amount"),
            "open": quote.get("open"),
            "prev_close": quote.get("prev_close"),
            "high": quote.get("high"),
            "low": quote.get("low"),
        },
        "trading": {
            "volume": quote.get("volume"),
            "amount": quote.get("amount"),
            "turnover_rate": quote.get("turnover_rate"),
            "amplitude_pct": quote.get("amplitude_pct"),
            "volume_ratio": quote.get("volume_ratio"),
            "vwap": quote.get("vwap"),
            "premium_pct": quote.get("premium_pct"),
        },
        "order_book_summary": {
            "buy_total_lot": buy_total_for_json,
            "sell_total_lot": sell_total_for_json,
            "imbalance_bid_ask": ofi_for_json,
            "weibi_pct": weibi_for_json,
            "spread": spread_for_json,
            "order_diff": quote.get("order_diff"),
        },
        "valuation": {
            "pe_dynamic": pe_dynamic_live,
            "pe_static": pe_static_live,
            "pe_rolling": pe_rolling_live,
            "pb": pb_live,
            "dividend_yield": dy_live,
            "total_market_value_yi": quote.get("total_market_value_yi"),
            "float_market_value_yi": quote.get("float_market_value_yi"),
        },
        "technical": {
            "macd_hist": ind.get("macd_hist"),
            "rsi6": ind.get("rsi6"),
            "rsi12": ind.get("rsi12"),
            "rsi24": ind.get("rsi24"),
            "rsi_multi": panel.get("rsi_multi", {}),
            "tf_indicators": panel.get("tf_indicators", {}),
            "ma5": ind.get("ma5"),
            "ma10": ind.get("ma10"),
            "ma20": ind.get("ma20"),
            "ma60": ind.get("ma60"),
            "boll_mid": ind.get("boll_mid"),
            "boll_upper": ind.get("boll_upper"),
            "boll_lower": ind.get("boll_lower"),
            "boll_pct_b": ind.get("boll_pct_b"),
            "boll_bandwidth": ind.get("boll_bandwidth"),
            "rsi_method": "Wilder(SMA, N,1)",
        },
    }

    price_now = quote.get("current_price")
    prev_close_for_pct = quote.get("prev_close")
    api_change_pct = quote.get("change_pct")
    calc_change_pct = None
    if (
        price_now is not None
        and prev_close_for_pct is not None
        and prev_close_for_pct > 0
    ):
        calc_change_pct = (price_now - prev_close_for_pct) / prev_close_for_pct * 100

    # 以现价/昨收重算为主，避免接口涨跌幅字段偶发异常导致颜色反向
    change_pct = calc_change_pct if calc_change_pct is not None else api_change_pct
    is_down = change_pct is not None and change_pct < 0
    price_class = "a-down" if is_down else "a-up"
    fast_compact_metrics["snapshot"]["display_change_pct"] = change_pct

    st.markdown('<div class="subsection-divider"></div>', unsafe_allow_html=True)
    head_left, head_right = st.columns([3.2, 1], vertical_alignment="center")
    copy_slot = None
    q_time = _format_display_time(quote.get("quote_time"))
    if price_now is not None:
        with head_left:
            st.markdown(
                f"""
                <div class="fast-head-title">{selected_name} ({selected_code})</div>
                <div class="fast-price-line">
                    <span class="price-num {price_class}">{price_now:.2f}</span>
                    <span class="chg-num {price_class}">{(change_pct or 0):+.2f}%</span>
                </div>
                """,
                unsafe_allow_html=True,
            )
            st.caption(f"更新时间: {q_time if q_time else 'N/A'}")
    else:
        with head_left:
            st.markdown(
                f"""
                <div class="fast-head-title">{selected_name} ({selected_code})</div>
                <div class="fast-price-line">
                    <span class="price-num">--</span>
                    <span class="chg-num">本地快照</span>
                </div>
                """,
                unsafe_allow_html=True,
            )
            st.caption("点击“立即刷新”获取实时盘口。")
    with head_right:
        copy_slot = st.container()

    def _fmt(v, nd=2):
        return "N/A" if v is None else f"{v:.{nd}f}"

    def _fmt_pct(v, nd=2):
        return "N/A" if v is None else f"{v:.{nd}f}%"

    def _fmt_signed(v, nd=2):
        return "N/A" if v is None else f"{v:+.{nd}f}"

    def _fmt_signed_pct(v, nd=2):
        return "N/A" if v is None else f"{v:+.{nd}f}%"

    def _fmt_lot(v):
        if v is None:
            return "N/A"
        return f"{int(v):,}"

    def _fmt_amount_yuan(v):
        if v is None:
            return "N/A"
        n = float(v)
        if abs(n) >= 1e8:
            return f"{n/1e8:.2f}亿"
        if abs(n) >= 1e4:
            return f"{n/1e4:.2f}万"
        return f"{n:.0f}"

    def _find_level(rows_data, level):
        for r in rows_data:
            if int(r.get("level", 0)) == int(level):
                return r
        return {}

    def _fmt_price_list(rows_data):
        vals = []
        for lv in range(1, 6):
            r = _find_level(rows_data, lv)
            vals.append(_fmt(r.get("price"), 2) if r else "N/A")
        return " / ".join(vals)

    def _fmt_vol_list(rows_data):
        vals = []
        for lv in range(1, 6):
            r = _find_level(rows_data, lv)
            vv = r.get("volume_lot") if r else None
            vals.append("--" if vv is None else str(int(float(vv))))
        return " / ".join(vals)

    def _rows_html(rows_data):
        return "".join(
            f'<div class="krow"><span class="k">{k}</span><span class="vv">{v}</span></div>'
            for k, v in rows_data
        )

    def _card_html(title, rows_data, desc=""):
        rows_html = _rows_html(rows_data)
        desc_html = f'<div class="d">{desc}</div>' if desc else ""
        return f'<div class="fast-card"><div class="t">{title}</div><div class="rows">{rows_html}</div>{desc_html}</div>'

    macd_val = ind.get("macd_hist")
    rsi_multi = panel.get("rsi_multi", {}) or {}
    rsi_tf_state = f"rsi_tf_key_{selected_code}"
    if st.session_state.get(rsi_tf_state) not in {"day", "week", "month", "intraday"}:
        st.session_state[rsi_tf_state] = "day"

    tf_cols = st.columns([0.42, 0.42, 0.42, 0.62, 2.12])
    tf_conf = [
        ("day", "日", "rsi-switch-day"),
        ("week", "周", "rsi-switch-week"),
        ("month", "月", "rsi-switch-month"),
        ("intraday", "分时", "rsi-switch-intra"),
    ]
    for idx, (tf_key, tf_label, tf_cls) in enumerate(tf_conf):
        is_active = st.session_state[rsi_tf_state] == tf_key
        with tf_cols[idx]:
            st.markdown(f'<div class="rsi-switch {tf_cls}">', unsafe_allow_html=True)
            if st.button(
                tf_label,
                key=f"rsi_tf_btn_{selected_code}_{tf_key}",
                width="stretch",
                type="primary" if is_active else "secondary",
            ):
                st.session_state[rsi_tf_state] = tf_key
                st.rerun()
            st.markdown("</div>", unsafe_allow_html=True)

    active_tf = st.session_state[rsi_tf_state]
    tf_indicators = panel.get("tf_indicators", {}) if isinstance(panel.get("tf_indicators", {}), dict) else {}
    active_ind = tf_indicators.get(active_tf, {}) if isinstance(tf_indicators, dict) else {}
    active_rsi = rsi_multi.get(active_tf, {}) if isinstance(rsi_multi, dict) else {}
    rsi_val = active_rsi.get("rsi6", active_ind.get("rsi6", ind.get("rsi6")))
    rsi12_val = active_rsi.get("rsi12", active_ind.get("rsi12", ind.get("rsi12")))
    rsi24_val = active_rsi.get("rsi24", active_ind.get("rsi24", ind.get("rsi24")))
    ma5_val = active_ind.get("ma5", ind.get("ma5"))
    ma10_val = active_ind.get("ma10", ind.get("ma10"))
    ma20_val = active_ind.get("ma20", ind.get("ma20"))
    ma60_val = active_ind.get("ma60", ind.get("ma60"))
    boll_mid_val = active_ind.get("boll_mid", ind.get("boll_mid"))
    boll_pct_b_fast = active_ind.get("boll_pct_b", ind.get("boll_pct_b"))
    boll_bw = active_ind.get("boll_bandwidth", ind.get("boll_bandwidth"))
    ref_val = quote.get("prev_close")
    boll_val = selected_slow.get("boll_index")
    pe_dynamic = pe_dynamic_live
    pe_static = pe_static_live
    pe_rolling = pe_rolling_live
    pb_val = pb_live
    dy_val = dy_live

    open_val = quote.get("open")
    high_val = quote.get("high")
    low_val = quote.get("low")
    change_amt = quote.get("change_amount")
    vwap_val = quote.get("vwap")
    premium_pct = quote.get("premium_pct")
    amplitude_pct = quote.get("amplitude_pct")
    turnover_rate = quote.get("turnover_rate")
    turnover_rate_estimated = bool(quote.get("turnover_rate_estimated", False))
    volume_ratio = quote.get("volume_ratio")
    total_mv = quote.get("total_market_value_yi")
    float_mv = quote.get("float_market_value_yi")
    order_diff = quote.get("order_diff")

    volume_shares = quote.get("volume")
    is_hk_code = str(selected_code).isdigit() and len(str(selected_code)) == 5
    if volume_shares is not None:
        volume_display = float(volume_shares) if is_hk_code else (float(volume_shares) / 100.0)
    else:
        volume_display = None
    volume_label = "成交量(股)" if is_hk_code else "成交量(手)"
    amount_label = "成交额(HKD)" if is_hk_code else "成交额(元)"
    turnover_label = "换手率(估算)" if turnover_rate_estimated else "换手率"
    volume_ratio_label = "量比(接口口径)" if is_hk_code else "量比"
    orderbook_unit = "档位量(接口口径)" if is_hk_code else "手"
    amount_yuan = quote.get("amount")

    macd_tf_val = active_ind.get("macd_hist", macd_val)
    macd_desc = "趋势偏强" if (macd_tf_val is not None and macd_tf_val > 0) else "趋势偏弱"
    rsi_desc = "超买区间" if (rsi_val is not None and rsi_val >= 70) else ("超卖区间" if (rsi_val is not None and rsi_val <= 30) else "强弱指标")
    tf_name_map = {"day": "日线", "week": "周线", "month": "月线", "intraday": "分时"}
    rsi_desc = f"{tf_name_map.get(active_tf, '日线')} · {rsi_desc}"
    tf_caption = tf_name_map.get(active_tf, "日线")

    sell_lv = sorted(order_book_5.get("sell", []), key=lambda x: int(x.get("level", 0)))
    buy_lv = sorted(order_book_5.get("buy", []), key=lambda x: int(x.get("level", 0)))
    sell_total = sum(float(r.get("volume_lot") or 0) for r in sell_lv)
    buy_total = sum(float(r.get("volume_lot") or 0) for r in buy_lv)
    ofi = (buy_total / sell_total) if sell_total > 0 else None
    weibi = ((buy_total - sell_total) / (buy_total + sell_total) * 100) if (buy_total + sell_total) > 0 else None
    ask1 = _find_level(sell_lv, 1).get("price")
    bid1 = _find_level(buy_lv, 1).get("price")
    spread = (float(ask1) - float(bid1)) if (ask1 is not None and bid1 is not None) else None

    cards = [
        (
            "实时快照",
            [
                ("现价", _fmt(price_now, 2)),
                ("涨跌幅", _fmt_signed_pct(change_pct, 2)),
                ("涨跌额", _fmt_signed(change_amt, 2)),
            ],
            "Now / Pct / Chg",
        ),
        (
            "日内区间",
            [
                ("今开", _fmt(open_val, 2)),
                ("昨收", _fmt(ref_val, 2)),
                ("最高", _fmt(high_val, 2)),
                ("最低", _fmt(low_val, 2)),
            ],
            "",
        ),
        (
            "成交活跃",
            [
                (volume_label, _fmt_lot(volume_display)),
                (amount_label, _fmt_amount_yuan(amount_yuan)),
                (volume_ratio_label, _fmt(volume_ratio, 2)),
                (turnover_label, _fmt_pct(turnover_rate, 2)),
            ],
            "",
        ),
        (
            "波动与均价",
            [
                ("VWAP", _fmt(vwap_val, 2)),
                ("偏离", _fmt_signed_pct(premium_pct, 2)),
                ("振幅", _fmt_pct(amplitude_pct, 2)),
            ],
            "",
        ),
        (
            "盘口结构",
            [
                ("买总量", _fmt_lot(buy_total)),
                ("卖总量", _fmt_lot(sell_total)),
                ("买卖比(B/A)", _fmt(ofi, 2)),
                ("委比", _fmt_signed_pct(weibi, 2)),
                ("买卖价差", _fmt(spread, 3)),
                ("委差", _fmt_signed(order_diff, 0)),
            ],
            "",
        ),
        (
            "PE 三口径",
            [
                ("PE(动)", _fmt(pe_dynamic, 2)),
                ("PE(静)", _fmt(pe_static, 2)),
                ("PE(滚)", _fmt(pe_rolling, 2)),
            ],
            "Eastmoney 口径",
        ),
        (
            "估值与规模",
            [
                ("PB", _fmt(pb_val, 2)),
                ("股息率", _fmt_pct(dy_val, 2)),
                ("总市值(亿)", _fmt(total_mv, 2)),
                ("流通市值(亿)", _fmt(float_mv, 2)),
            ],
            "",
        ),
        (
            "RSI 组合",
            [
                ("RSI(6)", _fmt(rsi_val, 2)),
                ("RSI(12)", _fmt(rsi12_val, 2)),
                ("RSI(24)", _fmt(rsi24_val, 2)),
            ],
            rsi_desc,
        ),
        (
            "均线组合",
            [
                ("MA5", _fmt(ma5_val, 2)),
                ("MA10", _fmt(ma10_val, 2)),
                ("MA20", _fmt(ma20_val, 2)),
                ("MA60", _fmt(ma60_val, 2)),
            ],
            f"{tf_caption}口径",
        ),
        (
            "MACD",
            [
                ("MACD柱", _fmt(macd_tf_val, 3)),
            ],
            f"{tf_caption} · {macd_desc}",
        ),
        (
            "BOLL",
            [
                ("BOLL %B", _fmt(boll_pct_b_fast if boll_pct_b_fast is not None else boll_val, 2)),
                ("BOLL带宽", _fmt_pct(boll_bw, 2)),
                ("BOLL中轨", _fmt(boll_mid_val, 2)),
            ],
            f"{tf_caption} · 布林带",
        ),
    ]

    cards_snapshot = {
        "timeframe_selected": active_tf,
        "timeframe_label": tf_caption,
        "snapshot": {
            "current_price": price_now,
            "change_pct_display": change_pct,
            "change_amount": change_amt,
            "open": open_val,
            "prev_close": ref_val,
            "high": high_val,
            "low": low_val,
            "quote_time": quote.get("quote_time"),
        },
        "trading": {
            "volume_lot": volume_display,
            "volume_shares": volume_shares,
            "volume_display_label": volume_label,
            "amount_display_label": amount_label,
            "amount_yuan": amount_yuan,
            "volume_ratio_label": volume_ratio_label,
            "volume_ratio": volume_ratio,
            "turnover_label": turnover_label,
            "turnover_rate": turnover_rate,
            "turnover_rate_estimated": turnover_rate_estimated,
            "vwap": vwap_val,
            "premium_pct": premium_pct,
            "amplitude_pct": amplitude_pct,
        },
        "order_book_summary": {
            "buy_total_lot": buy_total,
            "sell_total_lot": sell_total,
            "imbalance_bid_ask": ofi,
            "weibi_pct": weibi,
            "spread": spread,
            "order_diff": order_diff,
        },
        "valuation": {
            "pe_dynamic": pe_dynamic,
            "pe_static": pe_static,
            "pe_rolling": pe_rolling,
            "pb": pb_val,
            "dividend_yield": dy_val,
            "total_market_value_yi": total_mv,
            "float_market_value_yi": float_mv,
        },
        "technical_current_tf": {
            "macd_hist": macd_tf_val,
            "rsi6": rsi_val,
            "rsi12": rsi12_val,
            "rsi24": rsi24_val,
            "ma5": ma5_val,
            "ma10": ma10_val,
            "ma20": ma20_val,
            "ma60": ma60_val,
            "boll_pct_b": boll_pct_b_fast if boll_pct_b_fast is not None else boll_val,
            "boll_bandwidth": boll_bw,
            "boll_mid": boll_mid_val,
        },
        "cards": {title: {k: v for k, v in kv_rows} for title, kv_rows, _ in cards},
    }

    fast_compact_metrics["ui_state"] = {
        "selected_timeframe": active_tf,
        "selected_timeframe_label": tf_caption,
    }
    fast_compact_metrics["cards_snapshot"] = cards_snapshot
    render_status_row(
        (
            ("当前周期", tf_caption),
            ("更新时间", q_time if q_time else "N/A"),
            ("盘口失衡", _fmt(ofi, 2)),
            ("估值口径", f"PB {_fmt(pb_val, 2)} / 股息 {_fmt_pct(dy_val, 2)}"),
        )
    )

    run_analysis_now = False
    refresh_analysis_now = False
    live_job_id = None

    lightweight_panel = bool(panel.get("local_only") or panel.get("realtime_only"))
    if lightweight_panel:
        if copy_slot is not None:
            with copy_slot:
                st.caption("完整刷新后可复制 JSON / 生成分析。")
    else:
        export_payload = {
            "meta": {
                "generated_at": datetime.now().isoformat(timespec="seconds"),
                "app": "Quant",
                "analysis_user": (st.session_state.get("deepseek_user_input", "") or "").strip(),
            },
            "stock": {"code": selected_code, "name": selected_name},
            "slow_engine": selected_slow,
            "fast_engine": {
                "quote": quote,
                "indicators": ind,
                "rsi_multi": panel.get("rsi_multi", {}),
                "tf_indicators": panel.get("tf_indicators", {}),
                "order_book_5": order_book_5,
                "intraday": intraday_df,
                "depth_note": panel.get("depth_note"),
                "error": panel.get("error"),
                "compact_metrics": fast_compact_metrics,
                "cards_snapshot": cards_snapshot,
            },
        }
        export_json = json.dumps(_json_safe(export_payload), ensure_ascii=False, indent=2)
        analysis_payload = _build_analysis_payload(export_payload)
        analysis_json = json.dumps(analysis_payload, ensure_ascii=True, separators=(",", ":"))
        json_b64 = base64.b64encode(export_json.encode("utf-8")).decode("ascii")
        deep_json = analysis_json
        deep_hash = hashlib.sha256(f"multi_agent_v1:deep:{deep_json}".encode("utf-8")).hexdigest()
        live_job_id = _upsert_live_analysis_job(
            stock_code=selected_code,
            stock_name=selected_name,
            quick_json="",
            deep_json=deep_json,
            quick_hash="",
            deep_hash=deep_hash,
        )

    if copy_slot is not None and live_job_id is not None:
        with copy_slot:
            html(
                f"""
                <div style="margin:0.1rem 0 0.45rem 0;">
                  <button id="copy-json-btn-{selected_code}"
                    style="width:100%;height:44px;padding:0 0.95rem;border-radius:999px;border:1px solid rgba(255,255,255,0.14);background:linear-gradient(135deg,#f0d7b0 0%,#c99859 100%);color:#111827;font-size:1.02rem;font-weight:800;cursor:pointer;white-space:nowrap;box-shadow:0 12px 24px rgba(0,0,0,0.22);">
                    复制JSON
                  </button>
                  <div id="copy-json-msg-{selected_code}" style="margin-top:0.35rem;color:rgba(239,229,216,0.82);font-size:0.88rem;"></div>
                </div>
                <script>
                  const btn = document.getElementById("copy-json-btn-{selected_code}");
                  const msg = document.getElementById("copy-json-msg-{selected_code}");
                  const b64 = "{json_b64}";
                  const text = decodeURIComponent(escape(window.atob(b64)));
                  btn.onclick = async function () {{
                    try {{
                      await navigator.clipboard.writeText(text);
                      msg.textContent = "已复制";
                    }} catch (e) {{
                      msg.textContent = "复制失败，请重试";
                    }}
                  }};
                </script>
                """,
                height=96,
            )
            st.markdown('<div style="margin-top:0.22rem;"></div>', unsafe_allow_html=True)
            run_analysis_now = st.button("多智能体分析", key=f"run_inline_analysis_{selected_code}", width="stretch")

    render_section_intro(
        "快照矩阵",
        "把实时价格、成交、盘口、估值和技术指标压进一组可快速扫描的卡片，让盘中判断尽量停留在同一屏里。",
        kicker="Snapshot Matrix",
        pills=("实时报价", "盘口结构", "估值尺度", "技术状态"),
    )
    for i in range(0, len(cards), 4):
        cols = st.columns(4)
        for col, (title, kv_rows, desc) in zip(cols, cards[i : i + 4]):
            col.markdown(_card_html(title, kv_rows, desc), unsafe_allow_html=True)

    st.markdown('<div class="subsection-divider"></div>', unsafe_allow_html=True)
    st.markdown('<div class="fast-panels-gap"></div>', unsafe_allow_html=True)
    render_section_intro(
        "盘中结构",
        "左侧保留分时强弱，右侧保留五档盘口，用双栏视角把成交节奏和挂单深度放在一起读。",
        kicker="Intraday Structure",
        pills=("资金分时", "五档盘口", "同屏观察"),
    )
    left, right = st.columns([2, 1], vertical_alignment="top")
    with left:
        st.markdown('<div class="panel-title">资金分时</div>', unsafe_allow_html=True)
        if intraday_df.empty:
            st.info("暂无分时资金数据")
        else:
            chart_df = intraday_df.set_index("time")
            area_df = chart_df.reset_index()
            # A股配色: 涨红跌绿, 平盘中性灰
            area_color = "#ef4444" if (change_pct or 0) > 0 else ("#22c55e" if (change_pct or 0) < 0 else "#94a3b8")
            chart = (
                alt.Chart(area_df)
                .mark_area(color=area_color, opacity=0.9)
                .encode(
                    x=alt.X("time:T", title="time"),
                    y=alt.Y("volume_lot:Q", title="vol"),
                )
                .properties(height=330)
                .configure_view(strokeOpacity=0)
                .configure_axis(gridColor="#dbe4f0", labelColor="#4a5f7c", titleColor="#4a5f7c")
            )
            st.altair_chart(chart, width="stretch")

    with right:
        st.markdown(f'<div class="panel-title">实时盘口<span class="unit-sub">单位：{orderbook_unit}</span></div>', unsafe_allow_html=True)
        sell_df = pd.DataFrame(order_book_5.get("sell", []))
        buy_df = pd.DataFrame(order_book_5.get("buy", []))

        if sell_df.empty or buy_df.empty:
            st.info("暂无盘口数据")
        else:
            sell_df = sell_df.sort_values("level", ascending=False).copy()
            buy_df = buy_df.sort_values("level", ascending=True).copy()
            vol_max = max(
                1.0,
                max(pd.to_numeric(sell_df["volume_lot"], errors="coerce").fillna(0).max(), pd.to_numeric(buy_df["volume_lot"], errors="coerce").fillna(0).max()),
            )

            def _ob_rows(df: pd.DataFrame, side: str) -> str:
                rows_html = ""
                for _, r in df.iterrows():
                    lvl = int(r.get("level", 0))
                    price = r.get("price")
                    vol = r.get("volume_lot")
                    vol_num = float(vol) if vol is not None and pd.notna(vol) else 0.0
                    width = int((vol_num / vol_max) * 100)
                    width = max(width, 1 if vol_num > 0 else 0)
                    lab_class = "ob-sell" if side == "sell" else "ob-buy"
                    side_txt = "卖" if side == "sell" else "买"
                    bar_class = "sell" if side == "sell" else "buy"
                    p_txt = f"{float(price):.2f}" if price is not None and pd.notna(price) else "--"
                    v_txt = f"{int(vol_num)}" if vol_num > 0 else "--"
                    rows_html += (
                        f'<div class="ob-row">'
                        f'<div class="ob-lab {lab_class}">{side_txt}{lvl}</div>'
                        f'<div class="ob-price {lab_class}">{p_txt}</div>'
                        f'<div class="ob-bar-wrap"><div class="ob-bar {bar_class}" style="width:{width}%"></div></div>'
                        f'<div class="ob-vol">{v_txt}</div>'
                        f"</div>"
                    )
                return rows_html

            html_text = (
                '<div class="ob-block">'
                + _ob_rows(sell_df, "sell")
                + '<div class="ob-sep"></div>'
                + _ob_rows(buy_df, "buy")
                + "</div>"
            )
            st.markdown(html_text, unsafe_allow_html=True)

    st.caption(panel.get("depth_note", ""))
    st.markdown('<div class="subsection-divider"></div>', unsafe_allow_html=True)
    doc_cols = st.columns([5, 1], vertical_alignment="center")
    with doc_cols[0]:
        st.subheader(f"多智能体分析文档 · {selected_name} ({selected_code})")
    with doc_cols[1]:
        refresh_analysis_now = st.button(
            "刷新",
            key=f"refresh_inline_analysis_{selected_code}",
            width="stretch",
            disabled=live_job_id is None,
        )

    if live_job_id is None:
        st.caption("当前是轻量快照模式。需要完整分时/技术指标后，再生成多智能体分析文档。")
    else:
        try:
            if run_analysis_now or refresh_analysis_now:
                done_job = _execute_analysis_job(
                    job_id=live_job_id,
                    mode="deep",
                    ui_prefix=f"{selected_name} ",
                    force_refresh=bool(refresh_analysis_now),
                )
                _render_final_report_block(live_job_id, done_job, f"inline_new_{selected_code}", height=520, stream_output=True)
            else:
                live_job_obj = _load_json_file(_analysis_job_file(live_job_id))
                if isinstance(live_job_obj, dict) and live_job_obj.get("status") == "done":
                    _render_final_report_block(live_job_id, live_job_obj, f"inline_saved_{selected_code}", height=520)
                elif isinstance(live_job_obj, dict) and live_job_obj.get("status") == "failed":
                    st.error(f"上次分析失败: {live_job_obj.get('error', '未知错误')}")
                else:
                    st.caption("点击上方“多智能体分析”开始生成文档。")
        except Exception as exc:
            st.error(f"分析失败: {type(exc).__name__}: {exc}")


@st.cache_resource(show_spinner=False)
def _fast_panel_executor() -> concurrent.futures.ThreadPoolExecutor:
    return concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="fast-panel")


def _fast_panel_future_key(code: str) -> str:
    return f"fast_panel_future_{code}"


def _start_async_fast_refresh(code: str, *, mode: str = "realtime") -> concurrent.futures.Future:
    future_key = _fast_panel_future_key(code)
    existing = st.session_state.get(future_key)
    if isinstance(existing, concurrent.futures.Future) and not existing.done():
        return existing

    fetcher = fetch_realtime_panel if mode == "realtime" else fetch_fast_panel
    future = _fast_panel_executor().submit(fetcher, code)
    st.session_state[future_key] = future
    st.session_state[f"fast_panel_future_started_{code}"] = pytime.perf_counter()
    st.session_state[f"fast_panel_future_mode_{code}"] = mode
    return future


def _consume_async_fast_refresh(code: str) -> tuple:
    future_key = _fast_panel_future_key(code)
    future = st.session_state.get(future_key)
    if not isinstance(future, concurrent.futures.Future):
        return None, False, None, None
    mode = st.session_state.get(f"fast_panel_future_mode_{code}", "realtime")
    if not future.done():
        return None, True, None, mode

    st.session_state.pop(future_key, None)
    st.session_state.pop(f"fast_panel_future_started_{code}", None)
    st.session_state.pop(f"fast_panel_future_mode_{code}", None)
    try:
        return future.result(), False, None, mode
    except Exception as exc:
        return None, False, exc, mode


def _fast_refresh_pending(code: str) -> bool:
    future = st.session_state.get(_fast_panel_future_key(code))
    return isinstance(future, concurrent.futures.Future) and not future.done()


def _render_fast_panel_fragment():
    selected_code = st.session_state.get("fast_selected_code", rows[0]["code"])
    selected_name = st.session_state.get("fast_selected_name", rows[0]["name"])
    market_open = _is_market_open(selected_code)
    cache_key = f"fast_panel_cache_{selected_code}"
    force_refresh = bool(st.session_state.pop("fast_force_refresh", False))
    recently_added = st.session_state.pop("fast_recently_added_code", None) == selected_code
    auto_fetch_after_add = bool(st.session_state.pop("fast_auto_fetch_after_add", False))

    panel = None
    if force_refresh or auto_fetch_after_add or (auto_refresh_on and market_open):
        _start_async_fast_refresh(selected_code, mode="realtime")

    async_panel, async_pending, async_error, async_mode = _consume_async_fast_refresh(selected_code)
    if async_panel is not None:
        st.session_state[cache_key] = async_panel
        if async_mode == "realtime":
            st.session_state["fast_refresh_message"] = f"正在补充 {selected_name} ({selected_code}) 的资金分时和技术指标..."
            _start_async_fast_refresh(selected_code, mode="full")
        else:
            st.session_state["fast_refresh_message"] = ""
        st.rerun()
    elif async_pending:
        started = st.session_state.get(f"fast_panel_future_started_{selected_code}", pytime.perf_counter())
        elapsed = max(0.0, pytime.perf_counter() - float(started))
        default_msg = (
            f"正在补充 {selected_name} ({selected_code}) 的资金分时和技术指标..."
            if async_mode == "full"
            else f"正在获取 {selected_name} ({selected_code}) 的实时盘口..."
        )
        msg = st.session_state.get("fast_refresh_message") or default_msg
        st.info(f"{msg} 已等待 {elapsed:.1f} 秒，页面先显示已可用数据。")
        panel = st.session_state.get(cache_key)
        if panel is None:
            panel = _build_local_fast_panel(selected_code, selected_name, reason=msg)
            st.session_state[cache_key] = panel
    elif async_error is not None:
        label = "完整分时/技术指标" if async_mode == "full" else "实时盘口"
        st.warning(f"{label}获取失败: {async_error}")
        panel = st.session_state.get(cache_key)
        if panel is None:
            panel = _build_local_fast_panel(selected_code, selected_name, reason=f"{label}获取失败: {async_error}")
            st.session_state[cache_key] = panel
    else:
        panel = st.session_state.get(cache_key)
        if panel is None:
            reason = "刚加入股票，先显示本地快照；实时盘口请点“立即刷新”。" if recently_added else "本地快速模式：未主动联网刷新。"
            panel = _build_local_fast_panel(selected_code, selected_name, reason=reason)
            st.session_state[cache_key] = panel

    _render_fast_panel(selected_code, selected_name, panel=panel)

selected_code_for_poll = st.session_state.get("fast_selected_code", rows[0]["code"])
needs_fast_poll = (
    bool(st.session_state.get("fast_auto_fetch_after_add"))
    or bool(st.session_state.get("fast_force_refresh"))
    or _fast_refresh_pending(str(selected_code_for_poll))
)

if needs_fast_poll:
    @st.fragment(run_every="1s")
    def _pending_fast_panel_fragment():
        _render_fast_panel_fragment()

    _pending_fast_panel_fragment()
elif auto_refresh_on and market_open_for_ctrl:
    @st.fragment(run_every=f"{int(auto_refresh_sec)}s")
    def _auto_fast_panel_fragment():
        _render_fast_panel_fragment()

    _auto_fast_panel_fragment()
else:
    _render_fast_panel_fragment()
