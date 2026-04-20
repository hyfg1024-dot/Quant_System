from __future__ import annotations

from datetime import date

import altair as alt
import pandas as pd
import streamlit as st

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

APP_VERSION = "PORT-20260420-01"


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

        submitted = st.form_submit_button("保存持仓", use_container_width=True)

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
    if st.button("删除持仓", type="secondary", use_container_width=True):
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
        st.altair_chart(_build_weight_chart(overview_df), use_container_width=True)
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
            use_container_width=True,
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

        submitted = st.form_submit_button("计算建议仓位", use_container_width=True)

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
        use_container_width=True,
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
    _render_flows()
