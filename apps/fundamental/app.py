from __future__ import annotations

import json
from typing import Any, Dict, List

import streamlit as st

from fundamental_engine import (
    APP_VERSION,
    analyze_watchlist,
    build_overview_table,
    delete_watch_item,
    format_pct,
    load_watchlist,
    save_watchlist,
    upsert_watch_item,
)


st.set_page_config(page_title="基本面板块", page_icon="📊", layout="wide")

st.markdown(
    """
<style>
div[data-testid="stMetricValue"] { font-size: 1.7rem; }
.fnd-card {
  border: 1px solid rgba(80,120,180,.25);
  border-radius: 14px;
  padding: 14px 16px;
  min-height: 132px;
  background: rgba(240,245,255,0.55);
}
.fnd-card h4 {
  margin: 0 0 8px 0;
  font-size: 1.45rem;
}
.fnd-card .score {
  font-size: 1.9rem;
  font-weight: 800;
  margin: 4px 0 8px 0;
}
.fnd-card .desc {
  color: #5c6e89;
  font-size: 1.0rem;
}
</style>
""",
    unsafe_allow_html=True,
)


def _init_state() -> None:
    if "fnd_watchlist" not in st.session_state:
        st.session_state["fnd_watchlist"] = load_watchlist()
    if "fnd_rows" not in st.session_state:
        st.session_state["fnd_rows"] = analyze_watchlist(st.session_state["fnd_watchlist"], force_refresh=False)
    if "fnd_selected_code" not in st.session_state:
        st.session_state["fnd_selected_code"] = st.session_state["fnd_rows"][0]["code"] if st.session_state["fnd_rows"] else ""


def _refresh_rows(force_refresh: bool = False) -> None:
    st.session_state["fnd_rows"] = analyze_watchlist(st.session_state["fnd_watchlist"], force_refresh=force_refresh)
    if st.session_state["fnd_rows"] and not st.session_state["fnd_selected_code"]:
        st.session_state["fnd_selected_code"] = st.session_state["fnd_rows"][0]["code"]


def _selected_row(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    code = st.session_state.get("fnd_selected_code", "")
    for row in rows:
        if row.get("code") == code:
            return row
    return rows[0] if rows else {}


def _render_overview(rows: List[Dict[str, Any]]) -> None:
    st.subheader("股票列表")
    header = st.columns([1, 2, 1, 1, 1, 1], gap="small")
    header[0].markdown("**代码**")
    header[1].markdown("**名称**")
    header[2].markdown("**评分**")
    header[3].markdown("**类型**")
    header[4].markdown("**股息率**")
    header[5].markdown("**打开**")

    for row in rows:
        cols = st.columns([1, 2, 1, 1, 1, 1], gap="small")
        cols[0].write(row.get("code", ""))
        cols[1].write(row.get("name", ""))
        cols[2].write(row.get("total_score", "N/A"))
        cols[3].write(row.get("type", "观察"))
        cols[4].write(format_pct(row.get("dividend_yield")))
        if cols[5].button("打开", key=f"fnd_open_{row.get('code')}"):
            st.session_state["fnd_selected_code"] = row.get("code", "")
            st.rerun()


def _render_dimension_cards(row: Dict[str, Any]) -> None:
    dimensions = row.get("dimensions", [])
    if not dimensions:
        st.warning("暂无可用评分数据。")
        return
    st.subheader("八维评分")
    for i in range(0, len(dimensions), 4):
        cols = st.columns(4, gap="small")
        for j, card in enumerate(dimensions[i : i + 4]):
            with cols[j]:
                st.markdown(
                    f"""
<div class="fnd-card">
  <h4>{card.get("title", "")}</h4>
  <div class="score">{card.get("score", "N/A")} / {card.get("max_score", 5)}</div>
  <div class="desc">{card.get("comment", "")}</div>
</div>
""",
                    unsafe_allow_html=True,
                )


def _render_summary(row: Dict[str, Any]) -> None:
    st.subheader("总结性文本")
    st.info(row.get("summary_text", "暂无总结。"))
    json_payload = json.dumps(row, ensure_ascii=False, indent=2)
    c1, c2 = st.columns([1, 5], gap="small")
    if c1.button("复制JSON", key="fnd_copy_json"):
        st.toast("已生成当前分析 JSON，复制下面代码块即可。")
    c2.download_button(
        "下载JSON",
        data=json_payload.encode("utf-8"),
        file_name=f"fundamental_{row.get('code', 'stock')}.json",
        mime="application/json",
        use_container_width=False,
        key="fnd_download_json",
    )
    st.code(json_payload, language="json")


def _render_page() -> None:
    _init_state()

    st.title("基本面")
    st.caption(f"版本号: {APP_VERSION}")

    with st.sidebar:
        st.header("股票池管理")
        input_code = st.text_input("股票代码", placeholder="例如 600007")
        input_name = st.text_input("股票名称(可选)", placeholder="可留空")
        item_type = st.segmented_control("类型", options=["持仓", "观察"], default="观察")
        c1, c2 = st.columns(2, gap="small")
        if c1.button("加入", use_container_width=True):
            st.session_state["fnd_watchlist"] = upsert_watch_item(input_code, input_name, item_type or "观察")
            _refresh_rows(force_refresh=False)
            st.rerun()
        if c2.button("刷新全部", use_container_width=True):
            _refresh_rows(force_refresh=True)
            st.rerun()

        if st.session_state["fnd_watchlist"]:
            remove_code = st.selectbox(
                "删除股票",
                options=[x["code"] for x in st.session_state["fnd_watchlist"]],
                format_func=lambda c: next((f"{x['name']} ({x['code']})" for x in st.session_state["fnd_watchlist"] if x["code"] == c), c),
            )
            if st.button("删除", use_container_width=True):
                st.session_state["fnd_watchlist"] = delete_watch_item(remove_code)
                if st.session_state.get("fnd_selected_code") == remove_code:
                    st.session_state["fnd_selected_code"] = ""
                _refresh_rows(force_refresh=False)
                st.rerun()

    rows: List[Dict[str, Any]] = st.session_state["fnd_rows"]
    if not rows:
        st.warning("当前股票池为空，请先添加股票代码。")
        return

    _render_overview(rows)
    st.divider()

    row = _selected_row(rows)
    st.subheader(f"基本面评分板：{row.get('name', '')}（{row.get('code', '')}）")
    m1, m2, m3 = st.columns(3, gap="small")
    m1.metric("总分", row.get("total_score", "N/A"))
    m2.metric("结论", row.get("conclusion", "N/A"))
    m3.metric("覆盖率", format_pct((row.get("coverage_ratio") or 0) * 100))

    if row.get("data_warnings"):
        st.warning("；".join(row.get("data_warnings", [])))

    _render_dimension_cards(row)
    st.divider()
    _render_summary(row)

    with st.expander("原始表格预览"):
        st.dataframe(build_overview_table(rows), use_container_width=True, hide_index=True)


if __name__ == "__main__":
    _render_page()

