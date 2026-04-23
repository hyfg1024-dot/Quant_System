from __future__ import annotations

import copy
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import List

import pandas as pd
import streamlit as st

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from filter_engine import (
    APP_VERSION,
    DISPLAY_COLUMNS,
    build_ai_quick_config,
    check_market_data_source_status,
    default_filter_config,
    export_snapshot_health_excel,
    export_results_excel,
    get_a_enrich_segments,
    get_a_enrich_segment_counts,
    get_a_enrich_segment_status,
    get_enrichment_governance_summary,
    get_snapshot_backup_status,
    get_snapshot_health_report,
    get_snapshot_meta,
    get_stock_enrichment_store_summary,
    get_weekly_update_status,
    get_template_config,
    load_snapshot,
    load_templates,
    refresh_market_snapshot,
    restore_snapshot_from_backup,
    save_template,
)
from shared.db_manager import init_db as init_duckdb
from shared.db_manager import run_filter_query, sync_snapshot_to_duckdb
from shared.ui_shell import render_app_shell, render_section_intro, render_status_row

st.set_page_config(page_title="大过滤器", page_icon="🧪", layout="wide")

DUCKDB_READY = True
DUCKDB_ERROR = ""
try:
    init_duckdb()
except Exception as exc:
    DUCKDB_READY = False
    DUCKDB_ERROR = str(exc)

st.markdown(
    """
<style>
.kpi {
  border: 1px solid rgba(255,255,255,0.10);
  border-radius: 12px;
  padding: 10px 14px;
  background: linear-gradient(180deg, rgba(255,255,255,0.09), rgba(255,255,255,0.03));
}
.kpi .label { color: rgba(232, 223, 210, 0.88); font-size: 0.95rem; font-weight: 700; }
.kpi .value { color: rgba(255, 248, 241, 0.98); font-size: 1.75rem; font-weight: 800; margin-top: 4px; }
</style>
""",
    unsafe_allow_html=True,
)

LOCAL_PREFS_PATH = "data/local_user_prefs.json"


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


if "flt_cfg" not in st.session_state:
    st.session_state["flt_cfg"] = default_filter_config()
if "flt_result" not in st.session_state:
    st.session_state["flt_result"] = None
if "deepseek_user_input" not in st.session_state:
    _prefs = _load_local_prefs()
    st.session_state["deepseek_user_input"] = _prefs.get("deepseek_user", "")
if "deepseek_api_key_input" not in st.session_state:
    _prefs = _load_local_prefs()
    st.session_state["deepseek_api_key_input"] = _prefs.get("deepseek_api_key", "")
if "show_ops_panel" not in st.session_state:
    st.session_state["show_ops_panel"] = False
if "ops_source_check_result" not in st.session_state:
    st.session_state["ops_source_check_result"] = {}


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


def _sync_snapshot_into_duckdb(snapshot_df: pd.DataFrame) -> dict:
    if snapshot_df is None or snapshot_df.empty:
        return {"stock_basic": 0, "daily_kline": 0, "daily_fundamental": 0}
    if not DUCKDB_READY:
        raise RuntimeError(f"DuckDB 不可用: {DUCKDB_ERROR}")
    return sync_snapshot_to_duckdb(snapshot_df)


def _filter_snapshot_by_market(snapshot_df: pd.DataFrame, scope: str) -> pd.DataFrame:
    if snapshot_df is None or snapshot_df.empty or "market" not in snapshot_df.columns:
        return pd.DataFrame()
    market = str(scope or "").strip().upper()
    return snapshot_df[snapshot_df["market"].astype(str).str.upper() == market].copy().reset_index(drop=True)


def _market_snapshot_summary(scope: str) -> dict:
    market_df = _filter_snapshot_by_market(load_snapshot(), scope)
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


def _format_governance_line(counts: dict, order: List[str]) -> str:
    parts = [f"{key} {int(counts.get(key, 0) or 0)}" for key in order]
    return " / ".join(parts)


def _render_governance_cards(governance: dict) -> None:
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


def _all_market_snapshot_summary() -> dict:
    snapshot_df = load_snapshot()
    total = int(len(snapshot_df)) if isinstance(snapshot_df, pd.DataFrame) else 0
    a_total = int(len(_filter_snapshot_by_market(snapshot_df, "A")))
    hk_total = int(len(_filter_snapshot_by_market(snapshot_df, "HK")))
    return {"total": total, "a_total": a_total, "hk_total": hk_total}


def _format_ops_fallback_warning(label: str, stats: dict, *, weekly: bool = False) -> str:
    error_text = _safe_str(stats.get("error", "")).strip()
    prefix = f"{label}{'周更' if weekly else '本次'}未连通接口，已回退本地快照。"
    return f"{prefix} 原因：{error_text}" if error_text else prefix


def _format_source_summary(stats: dict) -> str:
    text = _safe_str(stats.get("source_summary", "")).strip()
    return f"来源：{text}" if text else ""


def _segment_choice_map() -> dict:
    return {key: label for key, label in get_a_enrich_segments()}


def _a_segment_status() -> List[dict]:
    return get_a_enrich_segment_status(load_snapshot())


def _run_source_check(scope: str) -> None:
    label = {"A": "A股", "HK": "港股", "ALL": "全市场"}.get(scope, scope)
    with st.spinner(f"正在检测{label}数据源..."):
        result = check_market_data_source_status(scope)
    st.session_state["ops_source_check_result"][str(scope).upper()] = result


def _render_source_check_result(scope: str) -> None:
    result_map = st.session_state.get("ops_source_check_result", {}) or {}
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
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def _run_market_update(scope: str, *, max_stocks: int, enrich_n: int, enrich_segment: str, force_refresh: bool, rotate_enrich: bool, safe_mode: bool) -> None:
    label = {"A": "A股", "HK": "港股"}.get(scope, scope)
    with st.spinner(f"正在执行{label}更新..."):
        try:
            stats = refresh_market_snapshot(
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
                st.warning(_format_ops_fallback_warning(label, stats, weekly=False))
            else:
                st.success(
                    f"{label}更新完成：{stats.get('row_count', 0)} 只，"
                    f"深补 {stats.get('enriched_count', 0)} 只 ｜ "
                    f"{_safe_str(stats.get('enrich_segment_label', ''))} / 共 {int(stats.get('segment_total', 0) or 0)} 只"
                    f"（区间 {int(stats.get('enrich_start', 0) or 0)} -> {int(stats.get('enrich_end', 0) or 0)}）"
                )
                st.caption(
                    f"{_format_source_summary(stats)} ｜ "
                    f"缓存命中: {int(stats.get('cache_hit', 0) or 0)} ｜ "
                    f"重抓: {int(stats.get('cache_miss', 0) or 0)}"
                )
            try:
                sync_stats = _sync_snapshot_into_duckdb(load_snapshot())
                st.caption(
                    "DuckDB 同步: "
                    f"basic {int(sync_stats.get('stock_basic', 0))} / "
                    f"kline {int(sync_stats.get('daily_kline', 0))} / "
                    f"fund {int(sync_stats.get('daily_fundamental', 0))}"
                )
            except Exception as sync_exc:
                st.warning(f"DuckDB 同步失败: {sync_exc}")
        except Exception as exc:
            st.error(f"{label}更新失败: {exc}")


def _run_market_weekly(scope: str, *, enrich_n: int, enrich_segment: str) -> None:
    label = {"A": "A股", "HK": "港股"}.get(scope, scope)
    with st.spinner(f"正在执行{label}周更..."):
        try:
            stats = refresh_market_snapshot(
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
                st.warning(_format_ops_fallback_warning(label, stats, weekly=True))
            else:
                st.success(
                    f"{label}周更完成：{stats.get('row_count', 0)} 只，"
                    f"深补 {stats.get('enriched_count', 0)} 只 ｜ "
                    f"{_safe_str(stats.get('enrich_segment_label', ''))} / 共 {int(stats.get('segment_total', 0) or 0)} 只"
                    f"（区间 {int(stats.get('enrich_start', 0) or 0)} -> {int(stats.get('enrich_end', 0) or 0)}）"
                )
                source_caption = _format_source_summary(stats)
                if source_caption:
                    st.caption(source_caption)
            if not bool(stats.get("skipped", False)):
                try:
                    sync_stats = _sync_snapshot_into_duckdb(load_snapshot())
                    st.caption(
                        "DuckDB 同步: "
                        f"basic {int(sync_stats.get('stock_basic', 0))} / "
                        f"kline {int(sync_stats.get('daily_kline', 0))} / "
                        f"fund {int(sync_stats.get('daily_fundamental', 0))}"
                    )
                except Exception as sync_exc:
                    st.warning(f"DuckDB 同步失败: {sync_exc}")
        except Exception as exc:
            st.error(f"{label}周更失败: {exc}")


def _run_a_segment_enrich(segment_key: str, segment_label: str, segment_count: int, *, force_refresh: bool, safe_mode: bool) -> None:
    if int(segment_count or 0) <= 0:
        st.info(f"{segment_label} 当前没有可深补股票。")
        return
    status_rows = _a_segment_status()
    status_map = {str(row.get("key")): row for row in status_rows}
    current_row = status_map.get(str(segment_key), {})
    persisted_count = int(current_row.get("persisted_count", 0) or 0)
    pending_count = max(0, int(segment_count or 0) - persisted_count)
    if (not force_refresh) and pending_count <= 0:
        st.info(f"{segment_label} 已全部深补完成；当前没有待补股票。")
        return
    with st.spinner(f"正在深补 {segment_label} ..."):
        try:
            stats = refresh_market_snapshot(
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
                st.warning(_format_ops_fallback_warning("A股", stats, weekly=False))
            else:
                mode_label = "补缺" if bool(stats.get("only_missing_enrich", False)) else "全量重补"
                base_total = int(stats.get("segment_pending", 0) or 0) if bool(stats.get("only_missing_enrich", False)) else int(stats.get('segment_total', 0) or 0)
                st.success(
                    f"{segment_label} {mode_label}完成：{int(stats.get('enriched_count', 0) or 0)} / {base_total}"
                )
                st.caption(
                    f"{_format_source_summary(stats)} ｜ "
                    f"缓存命中: {int(stats.get('cache_hit', 0) or 0)} ｜ "
                    f"重抓: {int(stats.get('cache_miss', 0) or 0)}"
                )
        except Exception as exc:
            st.error(f"{segment_label} 深补失败: {exc}")


def _render_market_ops_tab(scope: str, *, key_prefix: str) -> None:
    label = {"A": "A股", "HK": "港股"}.get(scope, scope)
    summary = _market_snapshot_summary(scope)
    weekly = get_weekly_update_status(scope)
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
    c1, c2, c3 = st.columns([1, 1, 1])
    max_stocks = c1.number_input(f"{label}更新股票数（0=全部）", min_value=0, max_value=12000, value=0, step=200, key=f"{key_prefix}_max_stocks")
    if scope == "A":
        enrich_n = 0
        enrich_segment = "sz_main"
    else:
        enrich_n = 0
        enrich_segment = "sz_main"
        c2.markdown("**港股深度补充**")
        c2.caption("当前未接入港股逐股基本面深补，港股这里只更新行情快照。")
    safe_mode = c3.checkbox("安全模式（防封）", value=True, key=f"{key_prefix}_safe_mode")
    force_refresh = st.checkbox("忽略缓存强制重抓", value=False, key=f"{key_prefix}_force")
    st.caption(
        f"{label}周更上次: {weekly.get('last') or '--'} ｜ "
        f"下次到期: {weekly.get('next_due') or '立即可执行'} ｜ "
        f"当前状态: {weekly_state}"
    )
    b1, b2, b3 = st.columns(3)
    if b1.button(f"运行{label}更新", use_container_width=True, key=f"{key_prefix}_run_once"):
        _run_market_update(scope, max_stocks=int(max_stocks), enrich_n=int(enrich_n), enrich_segment=str(enrich_segment), force_refresh=bool(force_refresh), rotate_enrich=False, safe_mode=bool(safe_mode))
    if b2.button(f"执行{label}周更（7天一次）", use_container_width=True, key=f"{key_prefix}_weekly"):
        _run_market_weekly(scope, enrich_n=int(enrich_n), enrich_segment=str(enrich_segment))
    if b3.button(f"检测{label}数据源", use_container_width=True, key=f"{key_prefix}_source_check"):
        _run_source_check(scope)
    _render_source_check_result(scope)

    if scope == "A":
        st.markdown("<div style='height:1.4rem'></div>", unsafe_allow_html=True)
        st.markdown("### 深补覆盖")
        st.caption("按数据库治理口径展示当前 A股 深补资产的覆盖、有字段完整度、时间新鲜度和最终可用状态。")
        governance = get_enrichment_governance_summary("A")
        _render_governance_cards(governance)
        st.caption(
            f"覆盖：有无记录 ｜ 完整度：关键字段完备程度 ｜ 新鲜度：距上次深补的时间状态 ｜ "
            f"状态：综合后的最终等级 ｜ 覆盖率 {float(governance.get('coverage_ratio', 0.0) or 0.0) * 100:.1f}%"
        )

        st.markdown("<div style='height:1.2rem'></div>", unsafe_allow_html=True)
        st.markdown("### A股板块深补")
        st.caption("点击下方板块按钮，直接深补该板块全部股票。")
        segment_rows = _a_segment_status()
        store_summary = get_stock_enrichment_store_summary()
        completed_segments = sum(1 for row in segment_rows if _safe_str(row.get("status")) == "已完成")
        st.caption(
            f"持久化深补资产：A股 {int(store_summary.get('a_total', 0) or 0)} 条 ｜ "
            f"已完成板块 {completed_segments}/{len(segment_rows)} ｜ "
            f"最近深补 {_safe_str(store_summary.get('latest_enriched_at', '')) or '--'}"
        )
        for start in range(0, len(segment_rows), 4):
            cols = st.columns(4)
            for col, row in zip(cols, segment_rows[start : start + 4]):
                label_text = f"{row['label']}（{int(row['count'])}）"
                if col.button(label_text, use_container_width=True, key=f"{key_prefix}_seg_{row['key']}"):
                    _run_a_segment_enrich(
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


def _render_ops_panel() -> None:
    render_section_intro(
        "数据运维台",
        "把 A股 和 港股 的更新、周更和体检拆开管理，避免两边状态混在一起。",
        kicker="Ops",
        pills=("A股", "港股", "每周更新", "体检导出"),
    )
    tab_a, tab_hk, tab_all = st.tabs(["A股", "港股", "总览"])
    with tab_a:
        _render_market_ops_tab("A", key_prefix="ops_a")
    with tab_hk:
        _render_market_ops_tab("HK", key_prefix="ops_hk")
    with tab_all:
        report = get_snapshot_health_report(days=7, top_n=20)
        backup_status = get_snapshot_backup_status()
        qc = report.get("quality_counts", {})
        total = int(report.get("total", 0) or 0)
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("总样本", total)
        c2.metric("full", int(qc.get("full", 0) or 0))
        c3.metric("partial", int(qc.get("partial", 0) or 0))
        c4.metric("missing", int(qc.get("missing", 0) or 0))
        st.progress(float(report.get("coverage_ratio", 0.0) or 0.0), text=f"覆盖率 {(float(report.get('coverage_ratio', 0.0) or 0.0) * 100):.1f}%")

        diag_xlsx = export_snapshot_health_excel(days=30, top_n=50)
        action_col1, action_col2 = st.columns(2)
        action_col1.download_button(
            "导出体检报告（Excel）",
            data=diag_xlsx,
            file_name=f"filter_health_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        if action_col2.button("从备份恢复快照", use_container_width=True, disabled=not bool(backup_status.get("exists"))):
            try:
                restored = restore_snapshot_from_backup()
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

        with st.expander("查看体检详情", expanded=False):
            st.dataframe(report.get("trend_df", pd.DataFrame()), use_container_width=True, hide_index=True)
            st.dataframe(report.get("runs_df", pd.DataFrame()), use_container_width=True, hide_index=True)

meta = get_snapshot_meta()
overall = _all_market_snapshot_summary()
render_app_shell(
    "filter",
    version=APP_VERSION,
    badges=("全市场快照", "模板筛选", "Excel 导出"),
    metrics=(
        ("快照样本", f"{int(overall.get('total', 0) or 0)} 只"),
        ("A/H分布", f"A {int(overall.get('a_total', 0) or 0)} / HK {int(overall.get('hk_total', 0) or 0)}"),
        ("筛选流程", "配置 -> 执行 -> 导出"),
    ),
)
render_section_intro(
    "筛选作业流",
    "先更新快照，再配置模板和条件，最后执行筛选并导出结果。这一页现在按真实工作流排布，而不是把控件平铺满屏。",
    kicker="Workflow",
    pills=("更新快照", "保存模板", "执行筛选", "导出 Excel"),
)
render_status_row(
    (
        ("快照状态", meta.get("last_update", "尚未更新") if meta else "尚未更新"),
        ("总样本", f"{int(overall.get('total', 0) or 0)} 只"),
        ("A/H分布", f"A {int(overall.get('a_total', 0) or 0)} / HK {int(overall.get('hk_total', 0) or 0)}"),
    )
)
if not DUCKDB_READY:
    st.warning(f"DuckDB 初始化失败，SQL筛选不可用：{DUCKDB_ERROR}")

if st.session_state["show_ops_panel"]:
    _render_ops_panel()

# Sidebar: update
st.sidebar.markdown("---")
st.sidebar.subheader("数据更新")
max_stocks = st.sidebar.number_input("本次更新股票数（0=全部）", min_value=0, max_value=6000, value=2000, step=100)
enrich_n = st.sidebar.number_input("深度补充数量（调用基本面引擎）", min_value=0, max_value=2000, value=300, step=50)
rotate_enrich = st.sidebar.checkbox("深度补充采用轮转增量（推荐）", value=True)
force_refresh = st.sidebar.checkbox("忽略缓存强制重抓", value=False)

if st.sidebar.button("更新全市场数据", use_container_width=True):
    with st.spinner("正在更新市场数据，请稍候..."):
        try:
            stats = refresh_market_snapshot(
                max_stocks=int(max_stocks),
                enrich_top_n=int(enrich_n),
                force_refresh=bool(force_refresh),
                rotate_enrich=bool(rotate_enrich),
            )
            if bool(stats.get("fallback", False)):
                st.sidebar.warning(_format_ops_fallback_warning("全市场", stats, weekly=False))
            else:
                mode_label = "轮转" if str(stats.get("enrich_mode", "")) == "rotate" else "前排固定"
                start_pos = int(stats.get("enrich_start", 0) or 0)
                end_pos = int(stats.get("enrich_end", 0) or 0)
                extra = f"（{mode_label}区间 {start_pos} -> {end_pos}）" if int(stats.get("enriched_count", 0) or 0) > 0 else ""
                st.sidebar.success(f"更新完成：{stats['row_count']} 只，深度补充 {stats['enriched_count']} 只{extra}")
                source_caption = _format_source_summary(stats)
                if source_caption:
                    st.sidebar.caption(source_caption)
            try:
                sync_stats = _sync_snapshot_into_duckdb(load_snapshot())
                st.sidebar.caption(
                    "DuckDB 同步: "
                    f"basic {int(sync_stats.get('stock_basic', 0))} / "
                    f"kline {int(sync_stats.get('daily_kline', 0))} / "
                    f"fund {int(sync_stats.get('daily_fundamental', 0))}"
                )
            except Exception as sync_exc:
                st.sidebar.warning(f"DuckDB 同步失败: {sync_exc}")
            meta = get_snapshot_meta()
        except Exception as exc:
            st.sidebar.error(f"更新失败: {exc}")

if meta:
    st.sidebar.caption(
        f"最近更新: {meta.get('last_update', '--')}\n"
        f"样本数量: {meta.get('row_count', '--')}\n"
        f"深度补充: {meta.get('enriched_count', '--')}"
    )

# Sidebar: templates
st.sidebar.markdown("---")
st.sidebar.subheader("DeepSeek API")
user_input = st.sidebar.text_input(
    "用户名（用于区分不同使用者）",
    key="deepseek_user_input",
)
api_key_input = st.sidebar.text_input(
    "API Key（可留空，读取环境变量）",
    type="password",
    key="deepseek_api_key_input",
)
_current_prefs = {
    "deepseek_user": (user_input or "").strip(),
    "deepseek_api_key": (api_key_input or "").strip(),
}
_saved_prefs = _load_local_prefs()
if (
    _current_prefs.get("deepseek_user", "") != str(_saved_prefs.get("deepseek_user", ""))
    or _current_prefs.get("deepseek_api_key", "") != str(_saved_prefs.get("deepseek_api_key", ""))
):
    _save_local_prefs(_current_prefs["deepseek_user"], _current_prefs["deepseek_api_key"])
st.sidebar.caption("已本地保存，不会上传到 GitHub。")
if st.sidebar.button(
    "打开数据运维台" if not st.session_state["show_ops_panel"] else "收起数据运维台",
    use_container_width=True,
):
    st.session_state["show_ops_panel"] = not bool(st.session_state["show_ops_panel"])

st.sidebar.markdown("---")
st.sidebar.subheader("筛选模式")
mode = st.sidebar.radio("模式", options=["手动筛选", "AI辅助设定", "模板筛选"], index=0)

cfg = st.session_state["flt_cfg"]

if mode == "AI辅助设定":
    render_section_intro(
        "AI 条件草拟",
        "用一句目标描述生成一版初稿条件，然后你再手动微调，适合先粗筛再精修。",
        kicker="Assist",
        pills=("自然语言转条件", "生成后可继续修改"),
    )
    st.subheader("AI辅助设定（自然语言）")
    prompt = st.text_area("输入你的目标", value="高股息、低估值、低负债、防御型", height=90)
    if st.button("生成条件并应用", type="primary"):
        cfg = build_ai_quick_config(prompt, cfg)
        st.session_state["flt_cfg"] = cfg
        st.success("已根据描述生成条件，你可继续微调后执行筛选。")
elif mode == "模板筛选":
    render_section_intro(
        "模板筛选",
        "直接读取模板复用已有筛选口径，适合周频或重复执行的条件集。",
        kicker="Template",
        pills=("读取模板", "保存模板", "快速复用"),
    )
    tpl_cols = st.columns([1.1, 0.65, 1.0, 0.65], vertical_alignment="bottom")
    all_tpl = load_templates()
    tpl_names = sorted(all_tpl.keys())
    selected_tpl = tpl_cols[0].selectbox("模板", options=["(无)"] + tpl_names, key="flt_tpl_select_main")
    if tpl_cols[1].button("读取模板", use_container_width=True, key="flt_tpl_load_main"):
        if selected_tpl and selected_tpl != "(无)":
            st.session_state["flt_cfg"] = get_template_config(selected_tpl)
            st.success(f"已加载模板: {selected_tpl}")
            st.rerun()
    save_tpl_name = tpl_cols[2].text_input("保存为模板名", value="", key="flt_tpl_save_main")
    if tpl_cols[3].button("保存模板", use_container_width=True, key="flt_tpl_save_btn_main"):
        try:
            save_template(save_tpl_name, cfg)
            st.success("模板已保存")
        except Exception as exc:
            st.error(str(exc))

render_section_intro(
    "条件矩阵",
    "把筛选条件拆成四个层次，从硬排除到五年后视镜，帮助你先做风险清洗，再叠加估值、质量和长期验证。",
    kicker="Configuration",
    pills=("A 硬排除", "B 估值质量", "C 行业规模", "D 五年后视镜"),
)
st.subheader("筛选条件（支持手动开关，像电商筛选一样）")

with st.expander("A. 财务健康度与硬排除", expanded=True):
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

with st.expander("B. 估值与质量", expanded=True):
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

with st.expander("C. 行业、分红、流动性与规模", expanded=True):
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
    "执行区负责运行筛选逻辑，结果区集中展示样本分布、二段筛选说明和导出能力。",
    kicker="Execution",
    pills=("执行筛选", "二段筛选", "结果导出"),
)
run_col1, run_col2, run_col3 = st.columns([1, 1.4, 2])
run_now = run_col1.button("执行筛选", type="primary", use_container_width=True)
two_stage = run_col2.checkbox("二段筛选（先 A/B/C，再 D 五年后视镜）", value=True)
run_col3.caption("注：部分标的5年数据可能缺失；可通过“缺失数据处理”决定是否直接排除。")

if run_now:
    snap = load_snapshot()
    if snap.empty:
        st.error("还没有市场快照。请先在左侧点击“更新全市场数据”。")
    elif not DUCKDB_READY:
        st.error(f"DuckDB 不可用，无法执行 SQL 筛选：{DUCKDB_ERROR}")
    else:
        with st.spinner("正在执行筛选..."):
            try:
                _sync_snapshot_into_duckdb(snap)
            except Exception as sync_exc:
                st.error(f"快照同步至 DuckDB 失败：{sync_exc}")
                st.stop()

            if two_stage:
                stage1_cfg = _build_stage1_config(cfg)
                p1, r1, m1, s1 = run_filter_query(stage1_cfg, include_rearview=True)

                if _has_rearview_enabled(cfg):
                    stage2_cfg = _build_stage2_config(cfg)
                    candidate_df = p1[["market", "code"]].copy() if not p1.empty else pd.DataFrame(columns=["market", "code"])
                    p2, r2, m2, _s2 = run_filter_query(
                        stage2_cfg,
                        include_rearview=True,
                        candidate_codes=candidate_df,
                    )
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
                passed_df, rejected_df, missing_df, stats = run_filter_query(cfg, include_rearview=True)
                stats["stage_mode"] = "single"

            st.session_state["flt_result"] = {
                "passed": passed_df,
                "rejected": rejected_df,
                "missing": missing_df,
                "stats": stats,
                "run_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }

res = st.session_state.get("flt_result")

if res:
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

    xlsx_bytes = export_results_excel(passed_df, rejected_df, missing_df)
    st.download_button(
        "导出 Excel（通过池/排除池/缺失项）",
        data=xlsx_bytes,
        file_name=f"filter_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )

    tab1, tab2, tab3 = st.tabs(["通过池", "排除池", "缺失项"])
    with tab1:
        st.dataframe(passed_df[DISPLAY_COLUMNS], use_container_width=True, hide_index=True)
    with tab2:
        st.dataframe(rejected_df[DISPLAY_COLUMNS], use_container_width=True, hide_index=True)
    with tab3:
        st.dataframe(missing_df[DISPLAY_COLUMNS], use_container_width=True, hide_index=True)
else:
    st.info("请先更新市场快照，然后点击“执行筛选”。")
