from __future__ import annotations

import pandas as pd
import streamlit as st

from shared.backup_manager import (
    create_backup,
    delete_backup,
    format_bytes,
    get_local_data_asset_status,
    list_backups,
    restore_backup,
    update_backup_note,
)

_STATUS_COLOR = {
    "正常": "#35c46a",
    "注意": "#e2b84d",
    "过旧": "#e8914b",
    "缺失": "#ef6461",
    "异常": "#ef6461",
}


def _status_badge(status: object) -> str:
    text = str(status or "未知")
    color = _STATUS_COLOR.get(text, "#8292a5")
    return f"<span style='color:{color};font-weight:900'>{text}</span>"


def _status_color(status: object) -> str:
    return _STATUS_COLOR.get(str(status or "未知"), "#8292a5")


def _vault_card(label: str, value: object, *, status: object | None = None) -> str:
    color = _status_color(status) if status is not None else "rgba(234,240,245,0.96)"
    value_text = str(value if value not in (None, "") else "--")
    return f"""
    <div style="border:1px solid rgba(255,255,255,0.12);border-radius:18px;
                background:linear-gradient(180deg,rgba(255,255,255,0.07),rgba(255,255,255,0.025));
                padding:18px 20px;min-height:112px;box-shadow:0 10px 24px rgba(0,0,0,0.16);">
      <div style="font-size:0.92rem;font-weight:800;color:rgba(234,240,245,0.68);">{label}</div>
      <div style="margin-top:12px;font-size:1.42rem;line-height:1.2;font-weight:900;
                  color:{color};white-space:normal;word-break:break-word;overflow-wrap:anywhere;">{value_text}</div>
    </div>
    """


def _metric_value(value: object, default: str = "--") -> str:
    if value is None or value == "":
        return default
    return str(value)


def render_data_vault_panel(*, key_prefix: str = "data_vault") -> None:
    st.markdown("#### 数据保险箱")
    st.caption(
        "保护本地数据资产：深补数据库、DuckDB、告警规则、筛选模板、回测策略和模拟盘状态。"
        "默认不备份本地 API Key。"
    )

    health = get_local_data_asset_status()
    backup = health.get("backup", {}) or {}
    assets = health.get("assets", []) or []
    issues = health.get("issues", []) or []

    st.markdown(
        f"当前数据资产状态：{_status_badge(health.get('overall_status'))}",
        unsafe_allow_html=True,
    )
    if issues:
        with st.expander("风险提示", expanded=True):
            for item in issues[:8]:
                st.warning(str(item))

    top1, top2, top3, top4 = st.columns(4)
    top1.markdown(_vault_card("备份数量", int(backup.get("count", 0) or 0)), unsafe_allow_html=True)
    top2.markdown(_vault_card("最近备份", str(backup.get("latest_at", "") or "--")), unsafe_allow_html=True)
    top3.markdown(_vault_card("备份占用", format_bytes(backup.get("total_size", 0))), unsafe_allow_html=True)
    top4.markdown(_vault_card("备份状态", backup.get("status", "未知"), status=backup.get("status")), unsafe_allow_html=True)

    sqlite_asset = next((a for a in assets if a.get("engine") == "SQLite"), {})
    duck_asset = next((a for a in assets if a.get("engine") == "DuckDB"), {})

    st.markdown("##### 数据库状态")
    db_rows = []
    for asset in assets:
        db_rows.append(
            {
                "资产": asset.get("label"),
                "引擎": asset.get("engine", ""),
                "状态": asset.get("status", ""),
                "大小": format_bytes(asset.get("size", 0)),
                "路径": asset.get("path", ""),
                "问题": "；".join(asset.get("issues", []) or []),
            }
        )
    st.dataframe(pd.DataFrame(db_rows), width="stretch", hide_index=True)

    st.markdown("##### 深补资产")
    e1, e2, e3, e4 = st.columns(4)
    e1.metric("快照样本", _metric_value(sqlite_asset.get("market_snapshot_rows")))
    e2.metric("深补总数", _metric_value(sqlite_asset.get("enrichment_rows")))
    e3.metric("A股深补", _metric_value(sqlite_asset.get("a_enrichment_rows")))
    e4.metric("港股深补", _metric_value(sqlite_asset.get("hk_enrichment_rows")))
    e5, e6, e7 = st.columns(3)
    e5.metric("港股分类", _metric_value(sqlite_asset.get("hk_classification_rows")))
    e6.metric("最近快照", _metric_value(sqlite_asset.get("latest_snapshot_at")))
    e7.metric("最近深补", _metric_value(sqlite_asset.get("latest_enriched_at")))

    if duck_asset:
        counts = duck_asset.get("table_counts", {}) or {}
        d1, d2, d3, d4 = st.columns(4)
        d1.metric("stock_basic", _metric_value(counts.get("stock_basic")))
        d2.metric("daily_kline", _metric_value(counts.get("daily_kline")))
        d3.metric("daily_fundamental", _metric_value(counts.get("daily_fundamental")))
        d4.metric("positions", _metric_value(counts.get("positions")))

    st.markdown("##### 备份操作")
    b1, b2 = st.columns([1, 2], vertical_alignment="bottom")
    backup_note = b2.text_input(
        "备份备注",
        value="manual_data_vault",
        placeholder="例如：A/H 深补完成后的手动恢复点",
        key=f"{key_prefix}_create_note",
    )
    if b1.button("立即创建完整备份", type="primary", width="stretch", key=f"{key_prefix}_create"):
        try:
            note_text = backup_note.strip() or "manual_data_vault"
            manifest = create_backup(reason=note_text, note=note_text, max_keep=30)
            st.success(f"已创建恢复点：{manifest.get('backup_id')}")
            st.rerun()
        except Exception as exc:
            st.error(f"备份失败：{exc}")
    b2.caption("规则：批量快照/深补写入前会自动创建数据库恢复点；手动完整备份会额外保存策略和模拟盘状态。")

    backups = list_backups(limit=20)
    if not backups:
        st.info("暂无备份。建议先创建一次完整备份。")
        return

    rows = []
    labels = []
    for item in backups:
        bid = str(item.get("backup_id", ""))
        labels.append(bid)
        rows.append(
            {
                "备份ID": bid,
                "创建时间": item.get("created_at", ""),
                "备注": item.get("note", "") or item.get("reason", ""),
                "资产数": item.get("asset_count", 0),
                "大小": format_bytes(item.get("total_size", 0)),
            }
        )
    st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)

    with st.expander("管理备份备注 / 删除", expanded=False):
        selected_manage = st.selectbox("选择备份", options=labels, key=f"{key_prefix}_manage_backup")
        selected_item = next((item for item in backups if str(item.get("backup_id", "")) == selected_manage), {})
        st.caption(
            "当前选择："
            f"{selected_manage} | {selected_item.get('created_at', '--')} | "
            f"{format_bytes(selected_item.get('total_size', 0))}"
        )
        manage_note = st.text_area(
            "备注",
            value=str(selected_item.get("note", "") or selected_item.get("reason", "")),
            height=90,
            key=f"{key_prefix}_manage_note_{selected_manage}",
        )
        c1, c2 = st.columns([1, 1], vertical_alignment="bottom")
        if c1.button("保存备注", width="stretch", key=f"{key_prefix}_save_note"):
            try:
                update_backup_note(selected_manage, manage_note)
                st.success(f"已更新备份备注：{selected_manage}")
                st.rerun()
            except Exception as exc:
                st.error(f"备注保存失败：{exc}")

        c2.warning("删除只移除这个恢复点，不影响当前数据库。")
        delete_confirm = st.text_input(
            "输入 DELETE 确认删除",
            value="",
            key=f"{key_prefix}_delete_confirm_{selected_manage}",
        )
        delete_ready = delete_confirm.strip().upper() == "DELETE"
        if st.button(
            "确认删除选中备份" if delete_ready else "先输入 DELETE 才能删除",
            width="stretch",
            disabled=not delete_ready,
            key=f"{key_prefix}_delete_backup",
        ):
            try:
                deleted = delete_backup(selected_manage)
                st.success(f"已删除备份 {selected_manage}，释放 {format_bytes(deleted.get('size', 0))}。")
                st.rerun()
            except Exception as exc:
                st.error(f"删除失败：{exc}")

    with st.expander("从备份恢复（危险操作）", expanded=False):
        selected = st.selectbox("选择备份", options=labels, key=f"{key_prefix}_selected_backup")
        st.warning("恢复会覆盖当前本地数据库和配置文件；执行前系统会自动再创建一个 before_restore 恢复点。")
        confirm = st.text_input("输入 RESTORE 确认恢复", value="", key=f"{key_prefix}_restore_confirm")
        if st.button(
            "恢复选中备份",
            width="stretch",
            disabled=(confirm.strip() != "RESTORE"),
            key=f"{key_prefix}_restore_btn",
        ):
            try:
                restored = restore_backup(selected, create_restore_point=True)
                count = len(restored.get("restored", []) or [])
                st.success(f"已恢复备份 {selected}，恢复资产 {count} 项。建议刷新页面；如数据库仍被占用，请重启应用。")
            except Exception as exc:
                st.error(f"恢复失败：{exc}")
