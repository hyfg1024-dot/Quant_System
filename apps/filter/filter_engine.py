from __future__ import annotations

import copy
import contextlib
import io
import json
import os
import random
import re
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import akshare as ak
import pandas as pd
import requests

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent.parent
FUNDAMENTAL_DIR = CURRENT_DIR.parent / "fundamental"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(FUNDAMENTAL_DIR) not in sys.path:
    sys.path.insert(0, str(FUNDAMENTAL_DIR))

from fundamental_engine import analyze_fundamental
try:
    from shared.backup_manager import create_backup as create_data_backup
except Exception:  # pragma: no cover - backup guard must never block data refresh
    create_data_backup = None  # type: ignore[assignment]

APP_VERSION = "FLT-20260423-07"
DATA_DIR = CURRENT_DIR / "data"
CACHE_DIR = DATA_DIR / "cache"
DB_PATH = DATA_DIR / "filter_market.db"
TEMPLATE_FILE = DATA_DIR / "filter_templates.json"
MANUAL_FLAGS_FILE = DATA_DIR / "manual_flags.json"

DEFAULT_SUNSET_INDUSTRIES = [
    "传统燃油汽车整车",
    "传统纸质媒体",
    "功能手机及相关",
    "胶卷及相关",
    "煤炭开采",
    "传统钢铁冶炼",
]

INDUSTRY_KEYWORD_ALIAS_MAP: Dict[str, List[str]] = {
    # 港股地产常见表达
    "房地产": ["地产", "物业", "reits", "reit"],
    "地产": ["房地产", "物业", "reits", "reit"],
    "物业": ["房地产", "地产", "reits", "reit"],
    "reits": ["reit", "房地产", "地产", "物业"],
    "reit": ["reits", "房地产", "地产", "物业"],
}

DEFAULT_FILTER_CONFIG: Dict[str, Any] = {
    "missing_policy": "ignore",  # ignore / exclude
    "risk": {
        "market_scope": "all",  # all / A / HK
        "industry_include_enabled": False,
        "industry_include_keywords": "",
        "exclude_st": True,
        "exclude_investigation": True,
        "exclude_penalty": True,
        "exclude_fund_occupation": True,
        "exclude_illegal_reduce": True,
        "require_standard_audit": False,
        "exclude_sunset_industry": False,
        "sunset_industries": "，".join(DEFAULT_SUNSET_INDUSTRIES),
        "pledge_ratio_max_enabled": False,
        "pledge_ratio_max": 80.0,
        "audit_change_max_enabled": False,
        "audit_change_max": 2,
        "exclude_no_dividend_5y": False,
    },
    "quality": {
        "ocf_3y_min_enabled": False,
        "ocf_3y_min": 0.0,
        "asset_liability_max_enabled": False,
        "asset_liability_max": 80.0,
        "interest_debt_asset_max_enabled": False,
        "interest_debt_asset_max": 20.0,
        "roe_min_enabled": False,
        "roe_min": 5.0,
        "gross_margin_min_enabled": False,
        "gross_margin_min": 20.0,
        "net_margin_min_enabled": False,
        "net_margin_min": 8.0,
        "receivable_ratio_max_enabled": False,
        "receivable_ratio_max": 50.0,
        "goodwill_ratio_max_enabled": False,
        "goodwill_ratio_max": 30.0,
    },
    "valuation": {
        "pe_ttm_min_enabled": False,
        "pe_ttm_min": 0.0,
        "pe_ttm_max_enabled": False,
        "pe_ttm_max": 25.0,
        "pb_max_enabled": False,
        "pb_max": 3.0,
        "ev_ebitda_max_enabled": False,
        "ev_ebitda_max": 18.0,
        "dividend_min_enabled": False,
        "dividend_min": 3.0,
        "dividend_max_enabled": False,
        "dividend_max": 12.0,
    },
    "growth_liquidity": {
        "revenue_growth_min_enabled": False,
        "revenue_growth_min": 0.0,
        "profit_growth_min_enabled": False,
        "profit_growth_min": 0.0,
        "market_cap_min_enabled": False,
        "market_cap_min": 100.0,  # 亿
        "market_cap_max_enabled": False,
        "market_cap_max": 5000.0,
        "turnover_min_enabled": False,
        "turnover_min": 0.2,
        "turnover_max_enabled": False,
        "turnover_max": 15.0,
        "volume_ratio_min_enabled": False,
        "volume_ratio_min": 0.5,
        "volume_ratio_max_enabled": False,
        "volume_ratio_max": 3.0,
        "amount_min_enabled": False,
        "amount_min": 100000000.0,  # 1亿
    },
    "rearview_5y": {
        "revenue_cagr_5y_min_enabled": False,
        "revenue_cagr_5y_min": 3.0,
        "profit_cagr_5y_min_enabled": False,
        "profit_cagr_5y_min": 3.0,
        "roe_avg_5y_min_enabled": False,
        "roe_avg_5y_min": 8.0,
        "ocf_positive_years_5y_min_enabled": False,
        "ocf_positive_years_5y_min": 4,
        "debt_ratio_change_5y_max_enabled": False,
        "debt_ratio_change_5y_max": 8.0,
        "gross_margin_change_5y_min_enabled": False,
        "gross_margin_change_5y_min": -6.0,
    },
}

DISPLAY_COLUMNS = [
    "market",
    "code",
    "name",
    "industry",
    "pe_ttm",
    "pb",
    "dividend_yield",
    "roe",
    "asset_liability_ratio",
    "turnover_ratio",
    "volume_ratio",
    "total_mv",
    "revenue_cagr_5y",
    "profit_cagr_5y",
    "roe_avg_5y",
    "ocf_positive_years_5y",
    "debt_ratio_change_5y",
    "gross_margin_change_5y",
    "data_quality",
    "exclude_reasons",
    "missing_fields",
]

ENRICHMENT_FIELD_COLUMNS: List[str] = [
    "pe_dynamic",
    "pe_static",
    "pe_ttm",
    "pb",
    "dividend_yield",
    "total_mv",
    "roe",
    "gross_margin",
    "net_margin",
    "asset_liability_ratio",
    "current_ratio",
    "operating_cashflow_3y",
    "receivable_revenue_ratio",
    "goodwill_equity_ratio",
    "interest_debt_asset_ratio",
    "ev_ebitda",
    "revenue_growth",
    "profit_growth",
    "revenue_cagr_5y",
    "profit_cagr_5y",
    "roe_avg_5y",
    "debt_ratio_avg_5y",
    "gross_margin_avg_5y",
    "debt_ratio_change_5y",
    "gross_margin_change_5y",
    "ocf_positive_years_5y",
    "total_score",
    "conclusion",
    "coverage_ratio",
    "audit_opinion",
    "enriched_at",
    "source_note",
]

ENRICHMENT_NUMERIC_COLUMNS: List[str] = [
    "pe_dynamic",
    "pe_static",
    "pe_ttm",
    "pb",
    "dividend_yield",
    "total_mv",
    "roe",
    "gross_margin",
    "net_margin",
    "asset_liability_ratio",
    "current_ratio",
    "operating_cashflow_3y",
    "receivable_revenue_ratio",
    "goodwill_equity_ratio",
    "interest_debt_asset_ratio",
    "ev_ebitda",
    "revenue_growth",
    "profit_growth",
    "revenue_cagr_5y",
    "profit_cagr_5y",
    "roe_avg_5y",
    "debt_ratio_avg_5y",
    "gross_margin_avg_5y",
    "debt_ratio_change_5y",
    "gross_margin_change_5y",
    "ocf_positive_years_5y",
    "total_score",
    "coverage_ratio",
]

ENRICHMENT_STORE_COLUMNS: List[str] = [
    "market",
    "code",
    "name",
    *ENRICHMENT_FIELD_COLUMNS,
    "app_version",
    "persisted_at",
]
ENRICHMENT_COMPLETENESS_FIELDS: List[str] = [
    "pe_ttm",
    "pb",
    "roe",
    "gross_margin",
    "net_margin",
    "asset_liability_ratio",
    "operating_cashflow_3y",
    "revenue_cagr_5y",
    "profit_cagr_5y",
    "total_score",
]

FRESH_WINDOW_DAYS = 7
AGING_WINDOW_DAYS = 30


A_ENRICH_SEGMENTS: List[Tuple[str, str]] = [
    ("sz_main", "000/001/003 深主板"),
    ("sme", "002 中小盘"),
    ("gem_300", "300 创业板"),
    ("gem_301", "301/302 创业板"),
    ("sh_main_600_601", "600/601 沪主板"),
    ("sh_main_603_605", "603/605 沪主板"),
    ("star", "688 科创板"),
    ("bse", "北交所"),
]

HK_ENRICH_SEGMENTS: List[Tuple[str, str]] = [
    ("energy", "能源"),
    ("materials", "原材料"),
    ("industrials", "工业"),
    ("consumer_discretionary", "可选消费"),
    ("consumer_staples", "必需消费"),
    ("healthcare", "医疗保健"),
    ("telecom", "电讯"),
    ("utilities", "公用事业"),
    ("financials", "金融"),
    ("properties_construction", "地产建筑"),
    ("information_technology", "资讯科技"),
    ("conglomerates", "综合企业"),
    ("gem", "GEM 创业板"),
]

HK_HSICS_KEYWORDS: Dict[str, List[str]] = {
    "energy": ["石油", "天然气", "煤炭", "油气", "能源", "新能源", "风电", "光伏", "太阳能", "电池", "核电"],
    "materials": ["钢铁", "有色", "黄金", "铜", "铝", "矿业", "矿产", "化工", "水泥", "建材", "纸业", "材料"],
    "industrials": ["工业", "机械", "设备", "工程", "物流", "运输", "航运", "航空", "铁路", "基建", "建筑设备", "制造"],
    "consumer_discretionary": ["汽车", "服装", "纺织", "鞋", "奢侈品", "零售", "餐饮", "酒店", "旅游", "娱乐", "博彩", "教育", "家电", "家居", "电商", "互联网零售"],
    "consumer_staples": ["食品", "饮料", "乳品", "农业", "农产品", "啤酒", "白酒", "包装食品", "家庭用品", "个人用品", "超市"],
    "healthcare": ["医疗", "医药", "生物", "制药", "医院", "器械", "保健"],
    "telecom": ["电讯", "通信", "通讯", "运营商", "宽带", "卫星"],
    "utilities": ["公用事业", "电力", "燃气", "水务", "环保", "废物处理"],
    "financials": ["银行", "保险", "证券", "金融", "资产管理", "信托", "租赁", "支付"],
    "properties_construction": ["地产", "房地产", "物业", "建筑", "基建地产", "REIT", "REITS"],
    "information_technology": ["软件", "半导体", "芯片", "计算机", "云", "互联网服务", "科技", "电子", "信息技术", "AI", "人工智能"],
    "conglomerates": ["综合企业", "控股", "多元化", "投资控股"],
}


def get_a_enrich_segments() -> List[Tuple[str, str]]:
    return list(A_ENRICH_SEGMENTS)


def get_a_enrich_segment_counts(snapshot_df: Optional[pd.DataFrame] = None) -> List[Dict[str, Any]]:
    if snapshot_df is None:
        snapshot_df = load_snapshot()
    if snapshot_df is None or snapshot_df.empty or "market" not in snapshot_df.columns:
        source_df = pd.DataFrame(columns=["market", "code"])
    else:
        source_df = snapshot_df.copy()
    source_df["market"] = source_df.get("market", pd.Series(dtype=object)).astype(str).str.upper()
    a_df = source_df[source_df["market"] == "A"].copy()
    codes = a_df.get("code", pd.Series(dtype=object)).astype(str).tolist()
    rows: List[Dict[str, Any]] = []
    for key, label in A_ENRICH_SEGMENTS:
        count = sum(1 for code in codes if _match_a_enrich_segment(code, key))
        rows.append({"key": key, "label": label, "count": int(count)})
    return rows


def get_a_enrich_segment_status(snapshot_df: Optional[pd.DataFrame] = None) -> List[Dict[str, Any]]:
    meta = get_snapshot_meta()
    rows = get_a_enrich_segment_counts(snapshot_df=snapshot_df)
    enrich_df = _load_stock_enrichment_latest()
    if enrich_df is None or enrich_df.empty:
        enrich_df = pd.DataFrame(columns=["market", "code", "enriched_at", "persisted_at"])
    enrich_df = enrich_df.copy()
    enrich_df["market"] = enrich_df.get("market", pd.Series(dtype=object)).astype(str).str.upper()
    enrich_df = enrich_df[enrich_df["market"] == "A"].copy()
    for row in rows:
        seg_key = _safe_str(row.get("key"))
        seg_df = enrich_df[enrich_df.get("code", pd.Series(dtype=object)).astype(str).map(lambda x: _match_a_enrich_segment(x, seg_key))].copy()
        persisted_count = int(len(seg_df))
        last_text = _safe_str(meta.get(f"last_enrich_segment_at_{seg_key}", ""))
        if not last_text and not seg_df.empty:
            last_candidates = pd.Series(dtype=object)
            if "enriched_at" in seg_df.columns:
                last_candidates = seg_df["enriched_at"].astype(str).str.strip()
                last_candidates = last_candidates[(last_candidates != "") & (last_candidates.str.lower() != "none")]
            if last_candidates.empty and "persisted_at" in seg_df.columns:
                last_candidates = seg_df["persisted_at"].astype(str).str.strip()
                last_candidates = last_candidates[(last_candidates != "") & (last_candidates.str.lower() != "none")]
            if not last_candidates.empty:
                last_text = _safe_str(last_candidates.max())
        row["persisted_count"] = persisted_count
        row["last_enriched_at"] = last_text
        total_count = int(row.get("count") or 0)
        if persisted_count <= 0:
            row["status"] = "未深补"
        elif total_count > 0 and persisted_count >= total_count:
            row["status"] = "已完成"
        else:
            row["status"] = "部分完成"
    return rows


def get_hk_enrich_segments() -> List[Tuple[str, str]]:
    return list(HK_ENRICH_SEGMENTS)


def _normalize_hk_enrich_segment(segment: Any) -> str:
    seg = _safe_str(segment)
    valid = {key for key, _ in HK_ENRICH_SEGMENTS}
    return seg if seg in valid else "financials"


def _get_hk_enrich_segment_label(segment: Any) -> str:
    seg = _normalize_hk_enrich_segment(segment)
    mapping = {key: label for key, label in HK_ENRICH_SEGMENTS}
    return mapping.get(seg, "金融")


def _normalize_hk_code(code: Any) -> str:
    text = re.sub(r"\D", "", _safe_str(code))
    return text[-5:].zfill(5) if text else ""


def _classify_hk_hsics(raw_industry: Any, name: Any = "") -> str:
    text = f"{_safe_str(raw_industry)} {_safe_str(name)}".upper()
    if not text.strip():
        return "综合企业"
    for key, keywords in HK_HSICS_KEYWORDS.items():
        for kw in keywords:
            if _safe_str(kw).upper() in text:
                return _get_hk_enrich_segment_label(key)
    return "综合企业"


def _is_hk_gem_code(code: Any) -> bool:
    return _normalize_hk_code(code).startswith("08")


def _normalize_hk_board(board: Any, code: Any = "") -> str:
    board_text = _safe_str(board)
    if "创业板" in board_text or "GEM" in board_text.upper() or _is_hk_gem_code(code):
        return "gem"
    return "main"


def _match_hk_enrich_segment_from_meta(code: Any, board: Any, hsics_sector: Any, segment: Any) -> bool:
    seg = _normalize_hk_enrich_segment(segment)
    board_norm = _normalize_hk_board(board, code)
    sector_text = _safe_str(hsics_sector)
    if seg == "gem":
        return board_norm == "gem"
    return board_norm == "main" and sector_text == _get_hk_enrich_segment_label(seg)


def _match_hk_enrich_segment(code: Any, segment: Any, classification_df: Optional[pd.DataFrame] = None) -> bool:
    code_norm = _normalize_hk_code(code)
    if not code_norm:
        return False
    seg = _normalize_hk_enrich_segment(segment)
    if seg == "gem":
        return _is_hk_gem_code(code_norm)
    if classification_df is None:
        classification_df = _load_hk_classification()
    if classification_df is None or classification_df.empty:
        return False
    work_df = classification_df.copy()
    work_df["code"] = work_df.get("code", pd.Series(dtype=object)).astype(str).map(_normalize_hk_code)
    matched = work_df[work_df["code"] == code_norm]
    if matched.empty:
        return False
    row = matched.iloc[0]
    return _match_hk_enrich_segment_from_meta(
        code_norm,
        row.get("board"),
        row.get("hsics_sector"),
        seg,
    )


def _load_hk_classification() -> pd.DataFrame:
    init_db()
    with _connect() as conn:
        try:
            df = pd.read_sql_query("SELECT * FROM hk_classification", conn)
        except Exception:
            return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    df["code"] = df.get("code", pd.Series(dtype=object)).astype(str).map(_normalize_hk_code)
    return df


def _upsert_hk_classification(rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    normalized: List[Dict[str, Any]] = []
    for row in rows:
        code = _normalize_hk_code(row.get("code"))
        if not code:
            continue
        normalized.append(
            {
                "code": code,
                "name": _safe_str(row.get("name")),
                "board": _normalize_hk_board(row.get("board"), code),
                "raw_industry": _safe_str(row.get("raw_industry")),
                "hsics_sector": _safe_str(row.get("hsics_sector")) or _classify_hk_hsics(row.get("raw_industry"), row.get("name")),
                "source_note": _safe_str(row.get("source_note")) or "东财公司资料",
                "updated_at": _safe_str(row.get("updated_at")) or now_text,
                "persisted_at": now_text,
            }
        )
    if not normalized:
        return 0
    cols = ["code", "name", "board", "raw_industry", "hsics_sector", "source_note", "updated_at", "persisted_at"]
    placeholders = ", ".join(["?"] * len(cols))
    updates = ", ".join([f"{col}=excluded.{col}" for col in cols if col != "code"])
    sql = (
        f"INSERT INTO hk_classification ({', '.join(cols)}) "
        f"VALUES ({placeholders}) "
        f"ON CONFLICT(code) DO UPDATE SET {updates}"
    )
    values = [tuple(row.get(col) for col in cols) for row in normalized]
    with _connect() as conn:
        conn.executemany(sql, values)
        conn.commit()
    return len(values)


def sync_hk_classification(
    snapshot_df: Optional[pd.DataFrame] = None,
    *,
    force_refresh: bool = False,
    safe_mode: bool = True,
    max_codes: int = 0,
) -> Dict[str, Any]:
    if snapshot_df is None:
        snapshot_df = load_snapshot()
    if snapshot_df is None or snapshot_df.empty:
        return {"requested": 0, "synced": 0, "skipped": 0, "errors": 0}
    work_df = snapshot_df.copy()
    work_df["market"] = work_df.get("market", pd.Series(dtype=object)).astype(str).str.upper()
    hk_df = work_df[work_df["market"] == "HK"].copy()
    if hk_df.empty:
        return {"requested": 0, "synced": 0, "skipped": 0, "errors": 0}
    hk_df["code"] = hk_df.get("code", pd.Series(dtype=object)).astype(str).map(_normalize_hk_code)
    hk_df["name"] = hk_df.get("name", pd.Series(dtype=object)).astype(str)
    existing = _load_hk_classification()
    existing_codes = set(existing["code"].tolist()) if not existing.empty else set()
    targets = hk_df[["code", "name"]].drop_duplicates().to_dict(orient="records")
    if not force_refresh:
        targets = [row for row in targets if row["code"] not in existing_codes]
    if max_codes and max_codes > 0:
        targets = targets[: int(max_codes)]
    synced = 0
    skipped = 0
    errors = 0
    rows_to_upsert: List[Dict[str, Any]] = []
    ses = _build_request_session(use_system_proxy=True)
    url = "https://datacenter.eastmoney.com/securities/api/data/v1/get"
    for item in targets:
        code = _normalize_hk_code(item.get("code"))
        name = _safe_str(item.get("name"))
        if not code:
            continue
        if _is_hk_gem_code(code):
            rows_to_upsert.append(
                {
                    "code": code,
                    "name": name,
                    "board": "gem",
                    "raw_industry": "",
                    "hsics_sector": "GEM 创业板",
                    "source_note": "代码段推断",
                }
            )
            synced += 1
            continue
        try:
            params = {
                "reportName": "RPT_HKF10_INFO_ORGPROFILE",
                "columns": "SECUCODE,SECURITY_CODE,ORG_NAME,BELONG_INDUSTRY",
                "quoteColumns": "",
                "filter": f'(SECUCODE="{code}.HK")',
                "pageNumber": "1",
                "pageSize": "50",
                "sortTypes": "",
                "sortColumns": "",
                "source": "F10",
                "client": "PC",
                "v": "04748497219912483",
            }
            resp = ses.get(url, params=params, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            resp.raise_for_status()
            data_json = resp.json()
            profile_df = pd.DataFrame(((data_json or {}).get("result") or {}).get("data") or [])
            raw_industry = ""
            board = "main"
            if profile_df is not None and not profile_df.empty:
                first_row = profile_df.iloc[0]
                raw_industry = _safe_str(first_row.get("BELONG_INDUSTRY"))
            rows_to_upsert.append(
                {
                    "code": code,
                    "name": name,
                    "board": board,
                    "raw_industry": raw_industry,
                    "hsics_sector": _classify_hk_hsics(raw_industry, name),
                    "source_note": "东财公司资料",
                }
            )
            synced += 1
        except Exception:
            errors += 1
        if safe_mode:
            time.sleep(random.uniform(0.08, 0.20))
    inserted = _upsert_hk_classification(rows_to_upsert)
    skipped = max(0, len(targets) - synced - errors)
    return {
        "requested": int(len(targets)),
        "synced": int(inserted),
        "skipped": int(skipped),
        "errors": int(errors),
    }


def get_hk_enrich_segment_counts(snapshot_df: Optional[pd.DataFrame] = None) -> List[Dict[str, Any]]:
    if snapshot_df is None:
        snapshot_df = load_snapshot()
    if snapshot_df is None or snapshot_df.empty or "market" not in snapshot_df.columns:
        hk_df = pd.DataFrame(columns=["code"])
    else:
        hk_df = snapshot_df.copy()
        hk_df["market"] = hk_df.get("market", pd.Series(dtype=object)).astype(str).str.upper()
        hk_df = hk_df[hk_df["market"] == "HK"].copy()
    hk_df["code"] = hk_df.get("code", pd.Series(dtype=object)).astype(str).map(_normalize_hk_code)
    classification_df = _load_hk_classification()
    if not classification_df.empty:
        classification_df = classification_df[classification_df["code"].isin(hk_df["code"].tolist())].copy()
    rows: List[Dict[str, Any]] = []
    for key, label in HK_ENRICH_SEGMENTS:
        if key == "gem":
            count = int(hk_df["code"].astype(str).map(_is_hk_gem_code).sum())
        else:
            if classification_df.empty:
                count = 0
            else:
                count = int(
                    classification_df.apply(
                        lambda r: _match_hk_enrich_segment_from_meta(
                            r.get("code"),
                            r.get("board"),
                            r.get("hsics_sector"),
                            key,
                        ),
                        axis=1,
                    ).sum()
                )
        rows.append({"key": key, "label": label, "count": count})
    return rows


def get_hk_enrich_segment_status(snapshot_df: Optional[pd.DataFrame] = None) -> List[Dict[str, Any]]:
    meta = get_snapshot_meta()
    rows = get_hk_enrich_segment_counts(snapshot_df=snapshot_df)
    enrich_df = _load_stock_enrichment_latest()
    class_df = _load_hk_classification()
    if enrich_df is None or enrich_df.empty:
        enrich_df = pd.DataFrame(columns=["market", "code", "enriched_at", "persisted_at"])
    enrich_df = enrich_df.copy()
    enrich_df["market"] = enrich_df.get("market", pd.Series(dtype=object)).astype(str).str.upper()
    enrich_df["code"] = enrich_df.get("code", pd.Series(dtype=object)).astype(str).map(_normalize_hk_code)
    enrich_df = enrich_df[enrich_df["market"] == "HK"].copy()
    for row in rows:
        seg_key = _safe_str(row.get("key"))
        seg_df = enrich_df[enrich_df.get("code", pd.Series(dtype=object)).astype(str).map(lambda x: _match_hk_enrich_segment(x, seg_key, class_df))].copy()
        persisted_count = int(len(seg_df))
        last_text = _safe_str(meta.get(f"last_enrich_segment_at_HK_{seg_key}", ""))
        if not last_text and not seg_df.empty:
            last_candidates = pd.Series(dtype=object)
            if "enriched_at" in seg_df.columns:
                last_candidates = seg_df["enriched_at"].astype(str).str.strip()
                last_candidates = last_candidates[(last_candidates != "") & (last_candidates.str.lower() != "none")]
            if last_candidates.empty and "persisted_at" in seg_df.columns:
                last_candidates = seg_df["persisted_at"].astype(str).str.strip()
                last_candidates = last_candidates[(last_candidates != "") & (last_candidates.str.lower() != "none")]
            if not last_candidates.empty:
                last_text = _safe_str(last_candidates.max())
        total_count = int(row.get("count") or 0)
        row["persisted_count"] = persisted_count
        row["last_enriched_at"] = last_text
        if persisted_count <= 0:
            row["status"] = "未深补"
        elif total_count > 0 and persisted_count >= total_count:
            row["status"] = "已完成"
        else:
            row["status"] = "部分完成"
    return rows


def get_stock_enrichment_store_summary() -> Dict[str, Any]:
    enrich_df = _load_stock_enrichment_latest()
    if enrich_df is None or enrich_df.empty:
        return {
            "total": 0,
            "a_total": 0,
            "hk_total": 0,
            "latest_enriched_at": "",
            "latest_persisted_at": "",
        }
    work_df = enrich_df.copy()
    work_df["market"] = work_df.get("market", pd.Series(dtype=object)).astype(str).str.upper()
    latest_enriched = ""
    if "enriched_at" in work_df.columns:
        enriched_series = work_df["enriched_at"].astype(str).str.strip()
        enriched_series = enriched_series[(enriched_series != "") & (enriched_series.str.lower() != "none")]
        if not enriched_series.empty:
            latest_enriched = _safe_str(enriched_series.max())
    latest_persisted = ""
    if "persisted_at" in work_df.columns:
        persisted_series = work_df["persisted_at"].astype(str).str.strip()
        persisted_series = persisted_series[(persisted_series != "") & (persisted_series.str.lower() != "none")]
        if not persisted_series.empty:
            latest_persisted = _safe_str(persisted_series.max())
    return {
        "total": int(len(work_df)),
        "a_total": int((work_df["market"] == "A").sum()),
        "hk_total": int((work_df["market"] == "HK").sum()),
        "latest_enriched_at": latest_enriched,
        "latest_persisted_at": latest_persisted,
    }


def _parse_datetime_value(v: Any) -> Optional[datetime]:
    text = _safe_str(v)
    if not text:
        return None
    for parser in (datetime.fromisoformat, lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S")):
        try:
            return parser(text)
        except Exception:
            continue
    return None


def _classify_enrichment_completeness(row: pd.Series) -> str:
    total_fields = len(ENRICHMENT_COMPLETENESS_FIELDS)
    if total_fields <= 0:
        return "sparse"
    filled = sum(1 for col in ENRICHMENT_COMPLETENESS_FIELDS if _to_float(row.get(col)) is not None)
    ratio = filled / total_fields
    if ratio >= 0.8:
        return "complete"
    if ratio >= 0.3:
        return "partial"
    return "sparse"


def _classify_enrichment_freshness(row: pd.Series) -> str:
    dt_val = _parse_datetime_value(row.get("enriched_at")) or _parse_datetime_value(row.get("persisted_at"))
    if dt_val is None:
        return "unknown"
    age_days = (datetime.now() - dt_val).total_seconds() / 86400.0
    if age_days <= FRESH_WINDOW_DAYS:
        return "fresh"
    if age_days <= AGING_WINDOW_DAYS:
        return "aging"
    return "stale"


def _classify_enrichment_status(*, covered: bool, completeness: str, freshness: str) -> str:
    if not covered:
        return "missing"
    if freshness in {"stale", "unknown"}:
        return "stale"
    if completeness == "complete" and freshness == "fresh":
        return "ready"
    return "usable"


def get_enrichment_governance_summary(
    scope: str = "all",
    snapshot_df: Optional[pd.DataFrame] = None,
) -> Dict[str, Any]:
    if snapshot_df is None:
        snapshot_df = load_snapshot()
    if snapshot_df is None or snapshot_df.empty:
        return {
            "scope": _safe_str(scope).upper() or "ALL",
            "total": 0,
            "coverage": {"covered": 0, "missing": 0},
            "completeness": {"complete": 0, "partial": 0, "sparse": 0},
            "freshness": {"fresh": 0, "aging": 0, "stale": 0, "unknown": 0},
            "status": {"ready": 0, "usable": 0, "stale": 0, "missing": 0},
            "coverage_ratio": 0.0,
            "latest_enriched_at": "",
            "latest_persisted_at": "",
        }

    scope_norm = _safe_str(scope).upper()
    work_df = snapshot_df.copy()
    if "market" in work_df.columns:
        work_df["market"] = work_df["market"].astype(str).str.upper()
        if scope_norm in {"A", "HK"}:
            work_df = work_df[work_df["market"] == scope_norm].copy()
    else:
        work_df["market"] = "A"
    key_df = work_df[["market", "code"]].copy() if "code" in work_df.columns else pd.DataFrame(columns=["market", "code"])
    key_df["code"] = key_df.get("code", pd.Series(dtype=object)).astype(str)
    total = int(len(key_df))

    enrich_df = _load_stock_enrichment_latest()
    if enrich_df is None or enrich_df.empty:
        enrich_df = pd.DataFrame(columns=["market", "code", *ENRICHMENT_COMPLETENESS_FIELDS, "enriched_at", "persisted_at"])
    enrich_df = enrich_df.copy()
    enrich_df["market"] = enrich_df.get("market", pd.Series(dtype=object)).astype(str).str.upper()
    enrich_df["code"] = enrich_df.get("code", pd.Series(dtype=object)).astype(str)
    if scope_norm in {"A", "HK"}:
        enrich_df = enrich_df[enrich_df["market"] == scope_norm].copy()
    enrich_df["__covered"] = 1
    sort_cols = [col for col in ["enriched_at", "persisted_at"] if col in enrich_df.columns]
    if sort_cols:
        enrich_df = enrich_df.sort_values(by=sort_cols, ascending=False, na_position="last")
    enrich_df = enrich_df.drop_duplicates(subset=["market", "code"], keep="first")

    merge_cols = ["market", "code"]
    attach_cols = [col for col in ["__covered", *ENRICHMENT_COMPLETENESS_FIELDS, "enriched_at", "persisted_at"] if col in enrich_df.columns]
    merged = key_df.merge(enrich_df[merge_cols + attach_cols], on=merge_cols, how="left")
    if merged.empty:
        return {
            "scope": scope_norm or "ALL",
            "total": total,
            "coverage": {"covered": 0, "missing": total},
            "completeness": {"complete": 0, "partial": 0, "sparse": 0},
            "freshness": {"fresh": 0, "aging": 0, "stale": 0, "unknown": 0},
            "status": {"ready": 0, "usable": 0, "stale": 0, "missing": total},
            "coverage_ratio": 0.0,
            "latest_enriched_at": "",
            "latest_persisted_at": "",
        }

    merged["coverage_flag"] = merged["__covered"].fillna(0).astype(int)
    merged["coverage_label"] = merged["coverage_flag"].map(lambda x: "covered" if int(x) == 1 else "missing")

    covered_df = merged[merged["coverage_flag"] == 1].copy()
    if covered_df.empty:
        completeness_counts = {"complete": 0, "partial": 0, "sparse": 0}
        freshness_counts = {"fresh": 0, "aging": 0, "stale": 0, "unknown": 0}
        latest_enriched = ""
        latest_persisted = ""
    else:
        covered_df["completeness_label"] = covered_df.apply(_classify_enrichment_completeness, axis=1)
        covered_df["freshness_label"] = covered_df.apply(_classify_enrichment_freshness, axis=1)
        comp_vc = covered_df["completeness_label"].value_counts(dropna=False).to_dict()
        completeness_counts = {
            "complete": int(comp_vc.get("complete", 0)),
            "partial": int(comp_vc.get("partial", 0)),
            "sparse": int(comp_vc.get("sparse", 0)),
        }
        fresh_vc = covered_df["freshness_label"].value_counts(dropna=False).to_dict()
        freshness_counts = {
            "fresh": int(fresh_vc.get("fresh", 0)),
            "aging": int(fresh_vc.get("aging", 0)),
            "stale": int(fresh_vc.get("stale", 0)),
            "unknown": int(fresh_vc.get("unknown", 0)),
        }
        enriched_series = covered_df["enriched_at"].astype(str).str.strip()
        enriched_series = enriched_series[(enriched_series != "") & (enriched_series.str.lower() != "none")]
        latest_enriched = _safe_str(enriched_series.max()) if not enriched_series.empty else ""
        persisted_series = covered_df["persisted_at"].astype(str).str.strip()
        persisted_series = persisted_series[(persisted_series != "") & (persisted_series.str.lower() != "none")]
        latest_persisted = _safe_str(persisted_series.max()) if not persisted_series.empty else ""

    merged["completeness_label"] = "sparse"
    merged["freshness_label"] = "unknown"
    if not covered_df.empty:
        merged.loc[covered_df.index, "completeness_label"] = covered_df["completeness_label"]
        merged.loc[covered_df.index, "freshness_label"] = covered_df["freshness_label"]
    merged["status_label"] = merged.apply(
        lambda row: _classify_enrichment_status(
            covered=bool(int(row.get("coverage_flag", 0) or 0)),
            completeness=_safe_str(row.get("completeness_label")) or "sparse",
            freshness=_safe_str(row.get("freshness_label")) or "unknown",
        ),
        axis=1,
    )
    cov_vc = merged["coverage_label"].value_counts(dropna=False).to_dict()
    status_vc = merged["status_label"].value_counts(dropna=False).to_dict()
    coverage_counts = {
        "covered": int(cov_vc.get("covered", 0)),
        "missing": int(cov_vc.get("missing", 0)),
    }
    status_counts = {
        "ready": int(status_vc.get("ready", 0)),
        "usable": int(status_vc.get("usable", 0)),
        "stale": int(status_vc.get("stale", 0)),
        "missing": int(status_vc.get("missing", 0)),
    }
    coverage_ratio = (coverage_counts["covered"] / total) if total > 0 else 0.0
    return {
        "scope": scope_norm or "ALL",
        "total": total,
        "coverage": coverage_counts,
        "completeness": completeness_counts,
        "freshness": freshness_counts,
        "status": status_counts,
        "coverage_ratio": coverage_ratio,
        "latest_enriched_at": latest_enriched,
        "latest_persisted_at": latest_persisted,
    }


def _normalize_enrich_segment(segment: Any) -> str:
    seg = _safe_str(segment).lower()
    valid = {key for key, _ in A_ENRICH_SEGMENTS}
    return seg if seg in valid else "sz_main"


def _get_enrich_segment_label(segment: Any) -> str:
    seg = _normalize_enrich_segment(segment)
    mapping = {key: label for key, label in A_ENRICH_SEGMENTS}
    return mapping.get(seg, "000/001/003 深主板")


def _match_a_enrich_segment(code: Any, segment: Any) -> bool:
    code_text = _safe_str(code)
    seg = _normalize_enrich_segment(segment)
    if not re.fullmatch(r"\d{6}", code_text):
        return False
    if seg == "sz_main":
        return code_text.startswith(("000", "001", "003"))
    if seg == "sme":
        return code_text.startswith("002")
    if seg == "gem_300":
        return code_text.startswith("300")
    if seg == "gem_301":
        return code_text.startswith(("301", "302"))
    if seg == "sh_main_600_601":
        return code_text.startswith(("600", "601"))
    if seg == "sh_main_603_605":
        return code_text.startswith(("603", "605"))
    if seg == "star":
        return code_text.startswith("688")
    if seg == "bse":
        return code_text.startswith(("4", "8", "92"))
    return False


def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _connect() -> sqlite3.Connection:
    ensure_dirs()
    return sqlite3.connect(DB_PATH)


def _to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        try:
            if pd.isna(v):
                return None
        except Exception:
            pass
        return float(v)
    text = str(v).strip().replace(",", "")
    if text in {"", "-", "--", "nan", "NaN", "None"}:
        return None
    try:
        return float(text)
    except Exception:
        return None


def _to_mv_100m(v: Any) -> Optional[float]:
    num = _to_float(v)
    if num is None:
        return None
    # 东方财富现货一般是“元”口径，这里统一转换到“亿”
    if abs(num) > 1_000_000:
        return num / 100_000_000
    return num


def _safe_str(v: Any) -> str:
    return str(v).strip() if v is not None else ""


_ENV_MISSING = object()
_PROXY_ENV_KEYS = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "NO_PROXY",
    "no_proxy",
)


def _get_explicit_http_proxy() -> str:
    for key in ("FILTER_HTTP_PROXY", "SURGE_HTTP_PROXY", "HTTP_PROXY", "http_proxy"):
        value = _safe_str(os.environ.get(key))
        if value:
            return value
    return "http://127.0.0.1:6152"


def _build_request_session(*, use_system_proxy: bool = False) -> requests.Session:
    ses = requests.Session()
    if use_system_proxy:
        proxy_url = _get_explicit_http_proxy()
        ses.trust_env = False
        ses.proxies.update({"http": proxy_url, "https": proxy_url})
    else:
        ses.trust_env = False
    return ses


@contextlib.contextmanager
def _temporary_proxy_env(proxy_url: Optional[str] = None, *, disable: bool = False):
    backup = {k: os.environ.get(k, _ENV_MISSING) for k in _PROXY_ENV_KEYS}
    try:
        for k in _PROXY_ENV_KEYS:
            os.environ.pop(k, None)
        if disable:
            os.environ["NO_PROXY"] = "*"
            os.environ["no_proxy"] = "*"
        elif proxy_url:
            os.environ["HTTP_PROXY"] = proxy_url
            os.environ["HTTPS_PROXY"] = proxy_url
            os.environ["ALL_PROXY"] = proxy_url
            os.environ["http_proxy"] = proxy_url
            os.environ["https_proxy"] = proxy_url
            os.environ["all_proxy"] = proxy_url
        yield
    finally:
        for k, v in backup.items():
            if v is _ENV_MISSING:
                os.environ.pop(k, None)
            else:
                os.environ[k] = str(v)


def _ak_call_with_proxy_fallback(func, *args, **kwargs):
    last_exc: Optional[Exception] = None

    explicit_proxy = _get_explicit_http_proxy()

    # 第一轮：显式走本机代理（适配 Surge）
    for _ in range(2):
        with _temporary_proxy_env(explicit_proxy):
            try:
                return func(*args, **kwargs)
            except Exception as exc:
                last_exc = exc
        time.sleep(0.8)

    # 第二轮：按当前环境直接请求
    for _ in range(2):
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            last_exc = exc
            time.sleep(0.8)

    # 第三轮：临时关闭代理环境变量后重试
    with _temporary_proxy_env(disable=True):
        for _ in range(2):
            try:
                return func(*args, **kwargs)
            except Exception as exc:
                last_exc = exc
            time.sleep(0.8)

    if last_exc is not None:
        raise last_exc
    raise RuntimeError("AKShare 请求失败")


def _pick_series(df: pd.DataFrame, names: List[str]) -> pd.Series:
    for n in names:
        if n in df.columns:
            return df[n]
    return pd.Series([None] * len(df), index=df.index)


def _clean_text_series(s: pd.Series) -> pd.Series:
    def _one(v: Any) -> str:
        if v is None:
            return ""
        try:
            if pd.isna(v):
                return ""
        except Exception:
            pass
        t = str(v).strip()
        return "" if t in {"None", "nan", "NaN"} else t

    return s.map(_one)


def _normalize_hk_company_code(v: Any) -> str:
    """
    港股公司口径：RMB 柜台(8xxxx)并入对应 HKD 柜台(0xxxx)，避免同一公司双柜台重复计数。
    """
    text = re.sub(r"\D+", "", _safe_str(v)).zfill(5)
    if len(text) != 5:
        return ""
    if text.startswith("8"):
        return "0" + text[1:]
    return text


def _snapshot_meta_set(key: str, value: str) -> None:
    with _connect() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO snapshot_meta(meta_key, meta_value) VALUES (?, ?)",
            (key, value),
        )
        conn.commit()


def get_snapshot_meta() -> Dict[str, str]:
    init_db()
    with _connect() as conn:
        rows = conn.execute("SELECT meta_key, meta_value FROM snapshot_meta").fetchall()
    return {str(k): str(v) for k, v in rows}


def get_weekly_update_status(market_scope: str = "AH") -> Dict[str, Any]:
    scope = _safe_str(market_scope).upper() or "AH"
    meta = get_snapshot_meta()
    key = f"last_weekly_update_{scope}"
    last_text = _safe_str(meta.get(key, ""))
    now = datetime.now()
    if not last_text:
        return {"scope": scope, "due": True, "last": "", "next_due": "", "remaining_hours": 0.0}
    last_dt: Optional[datetime] = None
    try:
        last_dt = datetime.fromisoformat(last_text)
    except Exception:
        try:
            last_dt = datetime.strptime(last_text, "%Y-%m-%d %H:%M:%S")
        except Exception:
            last_dt = None
    if last_dt is None:
        return {"scope": scope, "due": True, "last": last_text, "next_due": "", "remaining_hours": 0.0}
    next_due_dt = last_dt + timedelta(days=7)
    remaining_hours = max(0.0, (next_due_dt - now).total_seconds() / 3600.0)
    return {
        "scope": scope,
        "due": remaining_hours <= 0,
        "last": last_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "next_due": next_due_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "remaining_hours": remaining_hours,
    }


def init_db() -> None:
    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS market_snapshot (
                market TEXT,
                code TEXT PRIMARY KEY,
                name TEXT,
                industry TEXT,
                is_st INTEGER,
                close_price REAL,
                price_change_pct REAL,
                amount REAL,
                pe_dynamic REAL,
                pe_static REAL,
                pe_ttm REAL,
                pb REAL,
                dividend_yield REAL,
                total_mv REAL,
                float_mv REAL,
                turnover_ratio REAL,
                volume_ratio REAL,
                roe REAL,
                gross_margin REAL,
                net_margin REAL,
                asset_liability_ratio REAL,
                current_ratio REAL,
                operating_cashflow_3y REAL,
                receivable_revenue_ratio REAL,
                goodwill_equity_ratio REAL,
                interest_debt_asset_ratio REAL,
                ev_ebitda REAL,
                revenue_growth REAL,
                profit_growth REAL,
                revenue_cagr_5y REAL,
                profit_cagr_5y REAL,
                roe_avg_5y REAL,
                debt_ratio_avg_5y REAL,
                gross_margin_avg_5y REAL,
                debt_ratio_change_5y REAL,
                gross_margin_change_5y REAL,
                ocf_positive_years_5y REAL,
                investigation_flag INTEGER,
                penalty_flag INTEGER,
                fund_occupation_flag INTEGER,
                illegal_reduce_flag INTEGER,
                pledge_ratio REAL,
                no_dividend_5y_flag INTEGER,
                audit_change_count INTEGER,
                audit_opinion TEXT,
                sunset_industry_flag INTEGER,
                total_score REAL,
                conclusion TEXT,
                coverage_ratio REAL,
                data_quality TEXT,
                enriched_at TEXT,
                updated_at TEXT,
                source_note TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS snapshot_meta (
                meta_key TEXT PRIMARY KEY,
                meta_value TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS snapshot_runs (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_at TEXT,
                row_count INTEGER,
                enriched_count INTEGER,
                enrich_start INTEGER,
                enrich_end INTEGER,
                fallback INTEGER,
                error_brief TEXT,
                cache_hit INTEGER,
                cache_miss INTEGER,
                enrich_mode TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS market_snapshot_backup AS
            SELECT *, '' AS backup_at
            FROM market_snapshot
            WHERE 0
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS stock_enrichment_latest (
                market TEXT NOT NULL,
                code TEXT NOT NULL,
                name TEXT,
                pe_dynamic REAL,
                pe_static REAL,
                pe_ttm REAL,
                pb REAL,
                dividend_yield REAL,
                total_mv REAL,
                roe REAL,
                gross_margin REAL,
                net_margin REAL,
                asset_liability_ratio REAL,
                current_ratio REAL,
                operating_cashflow_3y REAL,
                receivable_revenue_ratio REAL,
                goodwill_equity_ratio REAL,
                interest_debt_asset_ratio REAL,
                ev_ebitda REAL,
                revenue_growth REAL,
                profit_growth REAL,
                revenue_cagr_5y REAL,
                profit_cagr_5y REAL,
                roe_avg_5y REAL,
                debt_ratio_avg_5y REAL,
                gross_margin_avg_5y REAL,
                debt_ratio_change_5y REAL,
                gross_margin_change_5y REAL,
                ocf_positive_years_5y REAL,
                total_score REAL,
                conclusion TEXT,
                coverage_ratio REAL,
                audit_opinion TEXT,
                enriched_at TEXT,
                source_note TEXT,
                app_version TEXT,
                persisted_at TEXT,
                PRIMARY KEY (market, code)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS hk_classification (
                code TEXT PRIMARY KEY,
                name TEXT,
                board TEXT,
                raw_industry TEXT,
                hsics_sector TEXT,
                source_note TEXT,
                updated_at TEXT,
                persisted_at TEXT
            )
            """
        )
        conn.commit()
    _migrate_snapshot_enrichment_to_latest()


def _has_meaningful_enrichment(row: pd.Series) -> bool:
    enriched_text = _safe_str(row.get("enriched_at", ""))
    if enriched_text:
        return True
    for col in [
        "total_score",
        "coverage_ratio",
        "revenue_cagr_5y",
        "profit_cagr_5y",
        "roe_avg_5y",
        "debt_ratio_avg_5y",
        "gross_margin_avg_5y",
        "operating_cashflow_3y",
    ]:
        if _to_float(row.get(col)) is not None:
            return True
    return False


def _normalize_enrichment_row(row: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "market": _safe_str(row.get("market")).upper(),
        "code": _safe_str(row.get("code")),
        "name": _safe_str(row.get("name")),
        "conclusion": _safe_str(row.get("conclusion")) or "观察",
        "audit_opinion": _safe_str(row.get("audit_opinion")) or "标准无保留意见",
        "enriched_at": _safe_str(row.get("enriched_at")),
        "source_note": _safe_str(row.get("source_note")),
        "app_version": _safe_str(row.get("app_version")) or APP_VERSION,
        "persisted_at": _safe_str(row.get("persisted_at")) or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    for col in ENRICHMENT_NUMERIC_COLUMNS:
        out[col] = _to_float(row.get(col))
    return out


def _upsert_stock_enrichment_latest(rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    normalized_rows = [_normalize_enrichment_row(row) for row in rows if _safe_str(row.get("market")) and _safe_str(row.get("code"))]
    if not normalized_rows:
        return 0
    placeholders = ", ".join(["?"] * len(ENRICHMENT_STORE_COLUMNS))
    updates = ", ".join(
        [
            f"{col}=excluded.{col}"
            for col in ENRICHMENT_STORE_COLUMNS
            if col not in {"market", "code"}
        ]
    )
    sql = (
        f"INSERT INTO stock_enrichment_latest ({', '.join(ENRICHMENT_STORE_COLUMNS)}) "
        f"VALUES ({placeholders}) "
        f"ON CONFLICT(market, code) DO UPDATE SET {updates}"
    )
    values = [tuple(row.get(col) for col in ENRICHMENT_STORE_COLUMNS) for row in normalized_rows]
    with _connect() as conn:
        conn.executemany(sql, values)
        conn.commit()
    return len(values)


def _migrate_snapshot_enrichment_to_latest() -> int:
    try:
        with _connect() as conn:
            snapshot_df = pd.read_sql_query("SELECT * FROM market_snapshot", conn)
    except Exception:
        return 0
    if snapshot_df is None or snapshot_df.empty:
        return 0
    rows: List[Dict[str, Any]] = []
    for _, row in snapshot_df.iterrows():
        if not _has_meaningful_enrichment(row):
            continue
        item = {col: row.get(col) for col in ["market", "code", "name", *ENRICHMENT_FIELD_COLUMNS]}
        item["app_version"] = APP_VERSION
        item["persisted_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rows.append(item)
    return _upsert_stock_enrichment_latest(rows)


def _load_stock_enrichment_latest() -> pd.DataFrame:
    init_db()
    with _connect() as conn:
        try:
            return pd.read_sql_query("SELECT * FROM stock_enrichment_latest", conn)
        except Exception:
            return pd.DataFrame()


def _apply_data_quality(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    key_cols = [
        "pe_ttm",
        "pb",
        "dividend_yield",
        "roe",
        "gross_margin",
        "net_margin",
        "asset_liability_ratio",
        "operating_cashflow_3y",
    ]

    def _quality(row: pd.Series) -> str:
        cnt = sum(1 for c in key_cols if _to_float(row.get(c)) is not None)
        if cnt >= 6:
            return "full"
        if cnt >= 3:
            return "partial"
        return "missing"

    out = df.copy()
    out["data_quality"] = out.apply(_quality, axis=1)
    return out


def _overlay_latest_enrichment(snapshot_df: pd.DataFrame) -> pd.DataFrame:
    if snapshot_df is None or snapshot_df.empty:
        return snapshot_df
    enrich_df = _load_stock_enrichment_latest()
    if enrich_df is None or enrich_df.empty:
        return _apply_data_quality(snapshot_df)
    merge_cols = ["market", "code"]
    enrich_cols = [col for col in ENRICHMENT_FIELD_COLUMNS if col in enrich_df.columns]
    merge_df = enrich_df[merge_cols + enrich_cols].copy()
    merge_df = merge_df.rename(columns={col: f"{col}__latest" for col in enrich_cols})
    out = snapshot_df.merge(merge_df, on=merge_cols, how="left")
    for col in enrich_cols:
        latest_col = f"{col}__latest"
        if latest_col not in out.columns:
            continue
        if col not in out.columns:
            out[col] = out[latest_col]
        else:
            out[col] = out[latest_col].where(out[latest_col].notna(), out[col])
        out = out.drop(columns=[latest_col])
    return _apply_data_quality(out)


def _build_enrichment_persist_row(row: pd.Series, *, source_note: str = "") -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "market": _safe_str(row.get("market")).upper(),
        "code": _safe_str(row.get("code")),
        "name": _safe_str(row.get("name")),
        "app_version": APP_VERSION,
        "persisted_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source_note": _safe_str(source_note) or _safe_str(row.get("source_note")),
    }
    for col in ENRICHMENT_FIELD_COLUMNS:
        payload[col] = row.get(col)
    return payload


def default_filter_config() -> Dict[str, Any]:
    return copy.deepcopy(DEFAULT_FILTER_CONFIG)


def load_templates() -> Dict[str, Dict[str, Any]]:
    ensure_dirs()
    if not TEMPLATE_FILE.exists():
        return {}
    try:
        obj = json.loads(TEMPLATE_FILE.read_text(encoding="utf-8"))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def save_template(name: str, config: Dict[str, Any]) -> None:
    key = _safe_str(name)
    if not key:
        raise ValueError("模板名不能为空")
    all_tpl = load_templates()
    all_tpl[key] = {
        "saved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "config": config,
    }
    TEMPLATE_FILE.write_text(json.dumps(all_tpl, ensure_ascii=False, indent=2), encoding="utf-8")


def get_template_config(name: str) -> Dict[str, Any]:
    all_tpl = load_templates()
    one = all_tpl.get(name, {})
    cfg = one.get("config") if isinstance(one, dict) else {}
    return cfg if isinstance(cfg, dict) else default_filter_config()


def _load_manual_flags() -> Dict[str, Dict[str, Any]]:
    ensure_dirs()
    if not MANUAL_FLAGS_FILE.exists():
        MANUAL_FLAGS_FILE.write_text("{}", encoding="utf-8")
        return {}
    try:
        obj = json.loads(MANUAL_FLAGS_FILE.read_text(encoding="utf-8"))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _cache_file(code: str) -> Path:
    return CACHE_DIR / f"enrich_{code}.json"


def _load_enrich_cache(code: str, ttl_days: int = 7) -> Optional[Dict[str, Any]]:
    p = _cache_file(code)
    if not p.exists():
        return None
    try:
        obj = json.loads(p.read_text(encoding="utf-8"))
        ts = datetime.fromisoformat(str(obj.get("cached_at")))
        if datetime.now() - ts <= timedelta(days=ttl_days):
            return obj
    except Exception:
        return None
    return None


def _save_enrich_cache(code: str, payload: Dict[str, Any]) -> None:
    p = _cache_file(code)
    data = dict(payload)
    data["cached_at"] = datetime.now().isoformat(timespec="seconds")
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _detect_sunset(industry: str, name: str, keywords: Optional[List[str]] = None) -> bool:
    words = keywords or DEFAULT_SUNSET_INDUSTRIES
    text = f"{_safe_str(industry)} {_safe_str(name)}"
    return any(w and w in text for w in words)


def _enrich_one(code: str, name: str, force_refresh: bool = False) -> Tuple[Dict[str, Any], str, Optional[str]]:
    if not force_refresh:
        cached = _load_enrich_cache(code)
        if cached:
            return cached, "cache", _safe_str(cached.get("cached_at")) or None

    result = analyze_fundamental(code=code, name=name, force_refresh=force_refresh, cache_ttl_hours=168)
    payload = {
        "pe_dynamic": _to_float(result.get("pe_dynamic")),
        "pe_static": _to_float(result.get("pe_static")),
        "pe_ttm": _to_float(result.get("pe_ttm")),
        "pb": _to_float(result.get("pb")),
        "dividend_yield": _to_float(result.get("dividend_yield")),
        "total_mv": _to_mv_100m(result.get("total_mv")),
        "roe": _to_float(result.get("roe")),
        "gross_margin": _to_float(result.get("gross_margin")),
        "net_margin": _to_float(result.get("net_margin")),
        "asset_liability_ratio": _to_float(result.get("debt_ratio")),
        "current_ratio": _to_float(result.get("current_ratio")),
        "operating_cashflow_3y": (
            (_to_float(result.get("ocf_sum_3y")) / 100000000) if _to_float(result.get("ocf_sum_3y")) is not None else None
        ),
        "receivable_revenue_ratio": _to_float(result.get("receivable_days")),
        "goodwill_equity_ratio": _to_float(result.get("goodwill_ratio_pct")),
        "interest_debt_asset_ratio": None,
        "ev_ebitda": None,
        "revenue_growth": _to_float(result.get("revenue_growth")),
        "profit_growth": _to_float(result.get("profit_growth")),
        "revenue_cagr_5y": _to_float(result.get("revenue_cagr_5y")),
        "profit_cagr_5y": _to_float(result.get("profit_cagr_5y")),
        "roe_avg_5y": _to_float(result.get("roe_avg_5y")),
        "debt_ratio_avg_5y": _to_float(result.get("debt_ratio_avg_5y")),
        "gross_margin_avg_5y": _to_float(result.get("gross_margin_avg_5y")),
        "debt_ratio_change_5y": _to_float(result.get("debt_ratio_change_5y")),
        "gross_margin_change_5y": _to_float(result.get("gross_margin_change_5y")),
        "ocf_positive_years_5y": _to_float(result.get("ocf_positive_years_5y")),
        "total_score": _to_float(result.get("total_score")),
        "conclusion": _safe_str(result.get("conclusion")) or "观察",
        "coverage_ratio": _to_float(result.get("coverage_ratio")),
        "audit_opinion": "标准无保留意见",
    }
    _save_enrich_cache(code, payload)
    return payload, "live", datetime.now().isoformat(timespec="seconds")


def _build_universe_from_spot(spot_df: pd.DataFrame, market: str, source_note: Optional[str] = None) -> pd.DataFrame:
    mkt = _safe_str(market).upper()
    if mkt == "HK":
        raw_code = _pick_series(spot_df, ["代码", "证券代码", "symbol", "Symbol"]).astype(str).str.extract(r"(\d+)")[0].fillna("")
        code = raw_code.map(_normalize_hk_company_code)
    else:
        raw_code = _pick_series(spot_df, ["代码", "symbol", "Symbol"]).astype(str).str.extract(r"(\d{6})")[0].fillna("")
        code = raw_code.str.zfill(6)
    name = _clean_text_series(_pick_series(spot_df, ["名称", "中文名称", "股票名称", "简称"]))
    industry = _clean_text_series(_pick_series(spot_df, ["所处行业", "所属行业", "行业", "industry"]))

    df = pd.DataFrame(
        {
            "market": mkt,
            "code": code,
            "name": name,
            "industry": industry,
            "is_st": name.str.contains("ST", na=False).astype(int),
            "close_price": _pick_series(spot_df, ["最新价", "最新", "收盘"]).map(_to_float),
            "price_change_pct": _pick_series(spot_df, ["涨跌幅", "涨跌幅(%)"]).map(_to_float),
            "amount": _pick_series(spot_df, ["成交额"]).map(_to_float),
            "pe_dynamic": _pick_series(spot_df, ["市盈率-动态", "市盈率动态", "市盈率"]).map(_to_float),
            "pb": _pick_series(spot_df, ["市净率"]).map(_to_float),
            "dividend_yield": _pick_series(spot_df, ["股息率", "股息率(%)"]).map(_to_float),
            "total_mv": _pick_series(spot_df, ["总市值"]).map(_to_mv_100m),
            "float_mv": _pick_series(spot_df, ["流通市值"]).map(_to_mv_100m),
            "turnover_ratio": _pick_series(spot_df, ["换手率"]).map(_to_float),
            "volume_ratio": _pick_series(spot_df, ["量比"]).map(_to_float),
        }
    )
    df = df[df["code"].astype(str).str.len() > 0].copy()
    if mkt == "HK":
        # 同公司双柜台去重，优先保留成交额/市值更完整的一条。
        df["_amt_sort"] = pd.to_numeric(df["amount"], errors="coerce")
        df["_mv_sort"] = pd.to_numeric(df["total_mv"], errors="coerce")
        df = (
            df.sort_values(by=["_amt_sort", "_mv_sort", "code"], ascending=[False, False, True], na_position="last")
            .drop_duplicates(subset=["code"], keep="first")
            .drop(columns=["_amt_sort", "_mv_sort"], errors="ignore")
            .reset_index(drop=True)
        )
    df["pe_ttm"] = df["pe_dynamic"]
    df["pe_static"] = None
    df["roe"] = None
    df["gross_margin"] = None
    df["net_margin"] = None
    df["asset_liability_ratio"] = None
    df["current_ratio"] = None
    df["operating_cashflow_3y"] = None
    df["receivable_revenue_ratio"] = None
    df["goodwill_equity_ratio"] = None
    df["interest_debt_asset_ratio"] = None
    df["ev_ebitda"] = None
    df["revenue_growth"] = None
    df["profit_growth"] = None
    df["revenue_cagr_5y"] = None
    df["profit_cagr_5y"] = None
    df["roe_avg_5y"] = None
    df["debt_ratio_avg_5y"] = None
    df["gross_margin_avg_5y"] = None
    df["debt_ratio_change_5y"] = None
    df["gross_margin_change_5y"] = None
    df["ocf_positive_years_5y"] = None
    df["total_score"] = None
    df["coverage_ratio"] = None
    df["conclusion"] = "观察"

    df["investigation_flag"] = 0
    df["penalty_flag"] = 0
    df["fund_occupation_flag"] = 0
    df["illegal_reduce_flag"] = 0
    df["pledge_ratio"] = None
    df["no_dividend_5y_flag"] = 0
    df["audit_change_count"] = 0
    df["audit_opinion"] = "标准无保留意见"

    df["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df["source_note"] = _safe_str(source_note) or f"ak.spot_{mkt.lower()} + fundamental_enrich"
    return df


def _retry_hk_fetch(fetcher, *, label: str, attempts: int = 3) -> pd.DataFrame:
    last_exc: Optional[Exception] = None
    for attempt in range(attempts):
        try:
            df = fetcher()
            if df is None or df.empty:
                raise RuntimeError(f"{label}结果为空")
            return df
        except Exception as exc:
            last_exc = exc
            if attempt < attempts - 1:
                time.sleep(0.8 + attempt * 0.4)
    if last_exc is not None:
        raise RuntimeError(f"{label}失败: {last_exc}") from last_exc
    raise RuntimeError(f"{label}失败")


def _fetch_hk_spot_ak() -> pd.DataFrame:
    # 港股优先走主板+GEM直连；若本机启用了 Surge 等系统代理，再补试一次走系统代理的直连请求；
    # 最后再回退到 AK 的东财/新浪口径。
    errors: List[str] = []

    try:
        return _retry_hk_fetch(
            lambda: _fetch_hk_spot_em_direct(use_system_proxy=False),
            label="港股东财直连",
            attempts=3,
        )
    except Exception as exc:
        errors.append(str(exc))

    try:
        return _retry_hk_fetch(
            lambda: _fetch_hk_spot_em_direct(use_system_proxy=True),
            label="港股东财直连(系统代理)",
            attempts=2,
        )
    except Exception as exc:
        errors.append(str(exc))

    try:
        return _retry_hk_fetch(
            lambda: _ak_call_with_proxy_fallback(ak.stock_hk_main_board_spot_em),
            label="港股AK主板行情",
            attempts=2,
        )
    except Exception as exc:
        errors.append(str(exc))

    try:
        return _retry_hk_fetch(
            lambda: _ak_call_with_proxy_fallback(ak.stock_hk_spot),
            label="港股AK新浪行情",
            attempts=2,
        )
    except Exception as exc:
        errors.append(str(exc))

    raise RuntimeError("；".join([e for e in errors if e]) or "港股行情抓取失败")


def _fetch_hk_spot_em_direct(use_system_proxy: bool = False) -> pd.DataFrame:
    """
    港股东方财富直连；默认禁用系统代理，必要时可显式允许走系统代理（适配 Surge 等环境）。
    优先获取市值等字段，避免 AKShare 精简字段导致大量空值。
    """
    hosts = [
        "https://72.push2.eastmoney.com/api/qt/clist/get",
        "https://81.push2.eastmoney.com/api/qt/clist/get",
        "https://push2.eastmoney.com/api/qt/clist/get",
    ]
    base_params = {
        "pn": "1",
        "pz": "200",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f12",
        # 主板 + GEM（上市公司口径），不混入其他证券类型
        "fs": "m:128 t:3,m:128 t:4",
        "fields": ",".join(
            [
                "f2",   # 最新价
                "f3",   # 涨跌幅
                "f6",   # 成交额
                "f8",   # 换手率
                "f9",   # 市盈率-动态
                "f10",  # 量比
                "f12",  # 代码
                "f14",  # 名称
                "f20",  # 总市值
                "f21",  # 流通市值
                "f23",  # 市净率
                "f100", # 行业
                "f127", # 行业(备用)
            ]
        ),
    }

    last_exc: Optional[Exception] = None
    for url in hosts:
        for _ in range(2):
            try:
                rows: List[Dict[str, Any]] = []
                seen_codes: set[str] = set()
                total_hint: Optional[int] = None
                ses = _build_request_session(use_system_proxy=use_system_proxy)

                for page in range(1, 120):
                    params = dict(base_params)
                    params["pn"] = str(page)
                    resp = ses.get(url, params=params, timeout=18, headers={"User-Agent": "Mozilla/5.0"})
                    resp.raise_for_status()
                    obj = resp.json()
                    data_obj = (obj or {}).get("data") or {}
                    if total_hint is None:
                        try:
                            total_hint = int(_to_float(data_obj.get("total")) or 0)
                        except Exception:
                            total_hint = None
                    diff = data_obj.get("diff") or []
                    if not diff:
                        break

                    page_new = 0
                    for it in diff:
                        raw = it or {}
                        code = str(raw.get("f12", "")).strip()
                        if (not code) or (code in seen_codes):
                            continue
                        seen_codes.add(code)
                        page_new += 1
                        industry = _safe_str(raw.get("f100")) or _safe_str(raw.get("f127"))
                        rows.append(
                            {
                                "代码": code,
                                "名称": str(raw.get("f14", "")).strip(),
                                "所处行业": industry,
                                "最新价": _to_float(raw.get("f2")),
                                "涨跌幅": _to_float(raw.get("f3")),
                                "成交额": _to_float(raw.get("f6")),
                                "市盈率-动态": _to_float(raw.get("f9")),
                                "市净率": _to_float(raw.get("f23")),
                                "股息率": None,
                                "总市值": _to_float(raw.get("f20")),
                                "流通市值": _to_float(raw.get("f21")),
                                "换手率": _to_float(raw.get("f8")),
                                "量比": _to_float(raw.get("f10")),
                            }
                        )
                    if page_new == 0:
                        break
                    if total_hint and len(rows) >= total_hint:
                        break

                out = pd.DataFrame(rows)
                if out.empty:
                    raise RuntimeError("港股直连结果为空")
                return out
            except Exception as exc:
                last_exc = exc
                time.sleep(0.8)

    if last_exc is not None:
        raise last_exc
    raise RuntimeError("港股直连失败")


def _build_base_universe(market_scope: str = "A") -> Tuple[pd.DataFrame, List[str]]:
    scope = _safe_str(market_scope).upper() or "A"
    use_a = scope in {"A", "AH", "ALL"}
    use_hk = scope in {"HK", "AH", "ALL"}
    frames: List[pd.DataFrame] = []
    errors: List[str] = []
    source_notes: List[str] = []

    if use_a:
        try:
            # 优先走直连端点；若本机处于 Surge 等系统代理环境，再补试一轮允许系统代理的请求。
            spot_a = _fetch_a_spot_em_direct(use_system_proxy=False)
            a_source = "A股东财直连"
        except Exception:
            try:
                spot_a = _fetch_a_spot_em_direct(use_system_proxy=True)
                a_source = "A股东财直连(系统代理)"
            except Exception:
                try:
                    spot_a = _ak_call_with_proxy_fallback(ak.stock_zh_a_spot_em)
                    a_source = "A股AK东财"
                except Exception:
                    spot_a = _ak_call_with_proxy_fallback(ak.stock_zh_a_spot)
                    a_source = "A股AK新浪"
        if spot_a is None or spot_a.empty:
            errors.append("A股快照为空")
        else:
            frames.append(_build_universe_from_spot(spot_a, market="A", source_note=a_source))
            source_notes.append(a_source)

    if use_hk:
        try:
            hk_source = ""
            try:
                spot_hk = _fetch_hk_spot_em_direct(use_system_proxy=False)
                hk_source = "港股东财直连"
            except Exception:
                try:
                    spot_hk = _fetch_hk_spot_em_direct(use_system_proxy=True)
                    hk_source = "港股东财直连(系统代理)"
                except Exception:
                    try:
                        spot_hk = _ak_call_with_proxy_fallback(ak.stock_hk_main_board_spot_em)
                        hk_source = "港股AK主板"
                    except Exception:
                        spot_hk = _ak_call_with_proxy_fallback(ak.stock_hk_spot)
                        hk_source = "港股AK新浪"
            if spot_hk is None or spot_hk.empty:
                errors.append("港股快照为空")
            else:
                frames.append(_build_universe_from_spot(spot_hk, market="HK", source_note=hk_source))
                source_notes.append(hk_source)
        except Exception as exc:
            errors.append(f"港股拉取失败: {exc}")

    if not frames:
        err = "；".join(errors) if errors else "未获取到市场快照"
        raise RuntimeError(err)

    out = pd.concat(frames, ignore_index=True)
    return out, source_notes


def _probe_data_source(label: str, fetcher) -> Dict[str, Any]:
    started = time.perf_counter()
    try:
        df = fetcher()
        elapsed = round(time.perf_counter() - started, 2)
        rows = int(len(df)) if df is not None else 0
        if df is None or df.empty:
            return {
                "source": label,
                "ok": False,
                "rows": rows,
                "elapsed_sec": elapsed,
                "detail": "结果为空",
            }
        return {
            "source": label,
            "ok": True,
            "rows": rows,
            "elapsed_sec": elapsed,
            "detail": "",
        }
    except Exception as exc:
        return {
            "source": label,
            "ok": False,
            "rows": 0,
            "elapsed_sec": round(time.perf_counter() - started, 2),
            "detail": str(exc),
        }


def check_market_data_source_status(market_scope: str = "ALL") -> Dict[str, Any]:
    scope = _safe_str(market_scope).upper() or "ALL"
    sources: List[Dict[str, Any]] = []

    if scope in {"A", "AH", "ALL"}:
        sources.append(_probe_data_source("A股东财直连", lambda: _fetch_a_spot_em_direct(use_system_proxy=False)))
        sources.append(_probe_data_source("A股东财直连(系统代理)", lambda: _fetch_a_spot_em_direct(use_system_proxy=True)))
        sources.append(
            _probe_data_source(
                "A股AK东财",
                lambda: _ak_call_with_proxy_fallback(ak.stock_zh_a_spot_em),
            )
        )
        sources.append(
            _probe_data_source(
                "A股AK新浪",
                lambda: _ak_call_with_proxy_fallback(ak.stock_zh_a_spot),
            )
        )

    if scope in {"HK", "AH", "ALL"}:
        sources.append(_probe_data_source("港股东财直连", lambda: _fetch_hk_spot_em_direct(use_system_proxy=False)))
        sources.append(_probe_data_source("港股东财直连(系统代理)", lambda: _fetch_hk_spot_em_direct(use_system_proxy=True)))
        sources.append(
            _probe_data_source(
                "港股AK主板",
                lambda: _ak_call_with_proxy_fallback(ak.stock_hk_main_board_spot_em),
            )
        )
        sources.append(
            _probe_data_source(
                "港股AK新浪",
                lambda: _ak_call_with_proxy_fallback(ak.stock_hk_spot),
            )
        )

    return {
        "scope": scope,
        "checked_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "all_ok": all(bool(item.get("ok")) for item in sources) if sources else False,
        "sources": sources,
    }


def _build_base_universe_legacy() -> pd.DataFrame:
    try:
        spot_df = _ak_call_with_proxy_fallback(ak.stock_zh_a_spot_em)
    except Exception:
        spot_df = _fetch_a_spot_em_direct()
    if spot_df is None or spot_df.empty:
        raise RuntimeError("未获取到全市场快照，请稍后重试")

    code = _pick_series(spot_df, ["代码"]).astype(str).str.strip().str.zfill(6)
    name = _pick_series(spot_df, ["名称"]).astype(str).str.strip()
    industry = _pick_series(spot_df, ["所处行业", "所属行业", "行业"]).astype(str).str.strip()

    df = pd.DataFrame(
        {
            "code": code,
            "name": name,
            "industry": industry,
            "is_st": name.str.contains("ST", na=False).astype(int),
            "close_price": _pick_series(spot_df, ["最新价", "最新", "收盘"]).map(_to_float),
            "price_change_pct": _pick_series(spot_df, ["涨跌幅"]).map(_to_float),
            "amount": _pick_series(spot_df, ["成交额"]).map(_to_float),
            "pe_dynamic": _pick_series(spot_df, ["市盈率-动态", "市盈率动态", "市盈率"]).map(_to_float),
            "pb": _pick_series(spot_df, ["市净率"]).map(_to_float),
            "dividend_yield": _pick_series(spot_df, ["股息率", "股息率(%)"]).map(_to_float),
            "total_mv": _pick_series(spot_df, ["总市值"]).map(_to_mv_100m),
            "float_mv": _pick_series(spot_df, ["流通市值"]).map(_to_mv_100m),
            "turnover_ratio": _pick_series(spot_df, ["换手率"]).map(_to_float),
            "volume_ratio": _pick_series(spot_df, ["量比"]).map(_to_float),
        }
    )
    df["pe_ttm"] = df["pe_dynamic"]
    df["pe_static"] = None
    df["roe"] = None
    df["gross_margin"] = None
    df["net_margin"] = None
    df["asset_liability_ratio"] = None
    df["current_ratio"] = None
    df["operating_cashflow_3y"] = None
    df["receivable_revenue_ratio"] = None
    df["goodwill_equity_ratio"] = None
    df["interest_debt_asset_ratio"] = None
    df["ev_ebitda"] = None
    df["revenue_growth"] = None
    df["profit_growth"] = None
    df["revenue_cagr_5y"] = None
    df["profit_cagr_5y"] = None
    df["roe_avg_5y"] = None
    df["debt_ratio_avg_5y"] = None
    df["gross_margin_avg_5y"] = None
    df["debt_ratio_change_5y"] = None
    df["gross_margin_change_5y"] = None
    df["ocf_positive_years_5y"] = None
    df["total_score"] = None
    df["coverage_ratio"] = None
    df["conclusion"] = "观察"

    df["investigation_flag"] = 0
    df["penalty_flag"] = 0
    df["fund_occupation_flag"] = 0
    df["illegal_reduce_flag"] = 0
    df["pledge_ratio"] = None
    df["no_dividend_5y_flag"] = 0
    df["audit_change_count"] = 0
    df["audit_opinion"] = "标准无保留意见"

    df["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df["source_note"] = "ak.stock_zh_a_spot_em + fundamental_enrich"

    return df


def _fetch_a_spot_em_direct(use_system_proxy: bool = False) -> pd.DataFrame:
    """
    东方财富直连兜底；默认禁用系统代理，必要时允许走系统代理（适配 Surge 等环境）。
    仅提供筛选所需核心字段。
    """
    hosts = [
        "https://82.push2.eastmoney.com/api/qt/clist/get",
        "https://push2.eastmoney.com/api/qt/clist/get",
        "https://71.push2.eastmoney.com/api/qt/clist/get",
    ]
    base_params = {
        "pn": "1",
        "pz": "200",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f12",
        "fs": "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048",
        "fields": ",".join(
            [
                "f2",   # 最新价
                "f3",   # 涨跌幅
                "f5",   # 成交量
                "f6",   # 成交额
                "f8",   # 换手率
                "f9",   # 市盈率-动态
                "f10",  # 量比
                "f12",  # 代码
                "f14",  # 名称
                "f20",  # 总市值
                "f21",  # 流通市值
                "f23",  # 市净率
                "f100", # 行业（可能为空）
            ]
        ),
    }

    last_exc: Optional[Exception] = None
    for url in hosts:
        for _ in range(2):
            try:
                rows: List[Dict[str, Any]] = []
                seen_codes: set[str] = set()
                total_hint: Optional[int] = None
                ses = _build_request_session(use_system_proxy=use_system_proxy)

                for page in range(1, 120):
                    params = dict(base_params)
                    params["pn"] = str(page)
                    resp = ses.get(url, params=params, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
                    resp.raise_for_status()
                    obj = resp.json()
                    data_obj = (obj or {}).get("data") or {}
                    if total_hint is None:
                        try:
                            total_hint = int(_to_float(data_obj.get("total")) or 0)
                        except Exception:
                            total_hint = None
                    diff = data_obj.get("diff") or []
                    if not diff:
                        break

                    page_new = 0
                    for it in diff:
                        raw = it or {}
                        code = str(raw.get("f12", "")).strip()
                        if (not code) or (code in seen_codes):
                            continue
                        seen_codes.add(code)
                        page_new += 1
                        rows.append(
                            {
                                "代码": code,
                                "名称": str(raw.get("f14", "")).strip(),
                                "所处行业": str(raw.get("f100", "")).strip(),
                                "最新价": _to_float(raw.get("f2")),
                                "涨跌幅": _to_float(raw.get("f3")),
                                "成交额": _to_float(raw.get("f6")),
                                "市盈率-动态": _to_float(raw.get("f9")),
                                "市净率": _to_float(raw.get("f23")),
                                "股息率": None,
                                "总市值": _to_float(raw.get("f20")),
                                "流通市值": _to_float(raw.get("f21")),
                                "换手率": _to_float(raw.get("f8")),
                                "量比": _to_float(raw.get("f10")),
                            }
                        )
                    if page_new == 0:
                        break
                    if total_hint and len(rows) >= total_hint:
                        break
                if not rows:
                    raise RuntimeError("东方财富直连返回空数据")
                return pd.DataFrame(rows)
            except Exception as exc:
                last_exc = exc
                time.sleep(0.8)

    if last_exc is not None:
        raise last_exc
    raise RuntimeError("东方财富直连失败")


def _normalize_dt_text(v: Any) -> str:
    text = _safe_str(v)
    if not text:
        return ""
    try:
        return datetime.fromisoformat(text).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return text


def _log_snapshot_run(
    row_count: int,
    enriched_count: int,
    enrich_start: int,
    enrich_end: int,
    fallback: bool,
    error_brief: str,
    cache_hit: int,
    cache_miss: int,
    enrich_mode: str,
) -> None:
    init_db()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO snapshot_runs(
                run_at, row_count, enriched_count, enrich_start, enrich_end,
                fallback, error_brief, cache_hit, cache_miss, enrich_mode
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                int(row_count),
                int(enriched_count),
                int(enrich_start),
                int(enrich_end),
                1 if fallback else 0,
                _safe_str(error_brief)[:360],
                int(cache_hit),
                int(cache_miss),
                _safe_str(enrich_mode) or "top",
            ),
        )
        conn.commit()


def _replace_table_with_df(conn: sqlite3.Connection, table_name: str, df: pd.DataFrame) -> None:
    """Replace a SQLite table without relying on pandas' fragile DROP TABLE path."""
    safe_name = _safe_str(table_name)
    if not safe_name.replace("_", "").isalnum():
        raise RuntimeError(f"非法表名: {safe_name}")
    conn.execute(f'DROP TABLE IF EXISTS "{safe_name}"')
    df.to_sql(safe_name, conn, if_exists="fail", index=False)


def _backup_current_snapshot(conn: sqlite3.Connection) -> Dict[str, Any]:
    try:
        current_df = pd.read_sql_query("SELECT * FROM market_snapshot", conn)
    except Exception:
        return {"backed_up": False, "row_count": 0, "backup_at": ""}
    if current_df is None or current_df.empty:
        return {"backed_up": False, "row_count": 0, "backup_at": ""}
    backup_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    backup_df = current_df.copy()
    backup_df["backup_at"] = backup_at
    _replace_table_with_df(conn, "market_snapshot_backup", backup_df)
    return {"backed_up": True, "row_count": int(len(backup_df)), "backup_at": backup_at}


def _merge_scope_snapshot(
    conn: sqlite3.Connection,
    scope: str,
    scope_df: pd.DataFrame,
) -> pd.DataFrame:
    scope_norm = _safe_str(scope).upper() or "A"
    if scope_df is None or scope_df.empty:
        return pd.DataFrame()
    merged_df = scope_df.copy()
    try:
        current_df = pd.read_sql_query("SELECT * FROM market_snapshot", conn)
    except Exception:
        current_df = pd.DataFrame()
    if current_df is None or current_df.empty or "market" not in current_df.columns:
        return merged_df.reset_index(drop=True)
    current_df = current_df.copy()
    current_df["market"] = current_df["market"].astype(str).str.upper()
    keep_df = current_df[current_df["market"] != scope_norm].copy()
    if keep_df.empty:
        return merged_df.reset_index(drop=True)
    all_cols = list(scope_df.columns)
    for col in all_cols:
        if col not in keep_df.columns:
            keep_df[col] = None
    keep_df = keep_df[all_cols]
    merged_df = pd.concat([keep_df, scope_df], ignore_index=True)
    return merged_df.reset_index(drop=True)


def _replace_market_snapshot_atomically(conn: sqlite3.Connection, save_df: pd.DataFrame) -> None:
    staging_table = "market_snapshot_staging"
    _replace_table_with_df(conn, staging_table, save_df)
    staged_count = conn.execute(f"SELECT COUNT(*) FROM {staging_table}").fetchone()[0]
    if int(staged_count or 0) <= 0:
        raise RuntimeError("staging 快照为空，已取消覆盖主表")

    col_list = ", ".join([f'"{col}"' for col in save_df.columns])
    try:
        conn.execute("BEGIN")
        conn.execute("DELETE FROM market_snapshot")
        conn.execute(f'INSERT INTO market_snapshot ({col_list}) SELECT {col_list} FROM {staging_table}')
        conn.execute(f"DROP TABLE IF EXISTS {staging_table}")
        conn.commit()
    except Exception:
        conn.rollback()
        conn.execute(f"DROP TABLE IF EXISTS {staging_table}")
        conn.commit()
        raise


def get_snapshot_backup_status() -> Dict[str, Any]:
    init_db()
    with _connect() as conn:
        try:
            backup_df = pd.read_sql_query("SELECT * FROM market_snapshot_backup", conn)
        except Exception:
            return {"exists": False, "row_count": 0, "backup_at": ""}
    if backup_df is None or backup_df.empty:
        return {"exists": False, "row_count": 0, "backup_at": ""}
    backup_at = _safe_str(backup_df.get("backup_at", pd.Series(dtype=str)).iloc[0] if "backup_at" in backup_df.columns else "")
    return {"exists": True, "row_count": int(len(backup_df)), "backup_at": backup_at}


def restore_snapshot_from_backup() -> Dict[str, Any]:
    init_db()
    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with _connect() as conn:
        try:
            backup_df = pd.read_sql_query("SELECT * FROM market_snapshot_backup", conn)
        except Exception as exc:
            raise RuntimeError(f"读取备份失败: {exc}") from exc
        if backup_df is None or backup_df.empty:
            raise RuntimeError("当前没有可恢复的快照备份")
        restore_df = backup_df.drop(columns=["backup_at"], errors="ignore").copy()
        if restore_df.empty:
            raise RuntimeError("备份表为空，无法恢复")
        _replace_market_snapshot_atomically(conn, restore_df)

    _snapshot_meta_set("last_update", now_text)
    _snapshot_meta_set("row_count", str(int(len(restore_df))))
    _snapshot_meta_set("last_refresh_fallback", "0")
    _snapshot_meta_set("last_refresh_error", "")
    _snapshot_meta_set("last_refresh_error_at", "")
    _snapshot_meta_set("last_restore_at", now_text)
    _snapshot_meta_set("last_restore_row_count", str(int(len(restore_df))))
    return {
        "restored": True,
        "row_count": int(len(restore_df)),
        "restored_at": now_text,
    }


def _classify_error_type(text: Any) -> str:
    t = _safe_str(text).lower()
    if not t:
        return "none"
    if "name resolution" in t or "failed to resolve" in t or "nodename nor servname" in t:
        return "dns"
    if "proxy" in t:
        return "proxy"
    if "timeout" in t:
        return "timeout"
    if "ssl" in t:
        return "ssl"
    if "connection" in t or "connect" in t:
        return "connection"
    if "rate" in t or "429" in t or "too many requests" in t:
        return "rate_limit"
    return "other"


def refresh_market_snapshot(
    max_stocks: int = 0,
    enrich_top_n: int = 300,
    force_refresh: bool = False,
    rotate_enrich: bool = True,
    market_scope: str = "A",
    enrich_segment: str = "sz_main",
    weekly_mode: bool = False,
    safe_mode: bool = True,
    only_missing_enrich: bool = False,
) -> Dict[str, Any]:
    init_db()
    scope = _safe_str(market_scope).upper() or "A"
    if scope == "HK":
        segment_key = _normalize_hk_enrich_segment(enrich_segment)
        segment_label = _get_hk_enrich_segment_label(segment_key)
    else:
        segment_key = _normalize_enrich_segment(enrich_segment)
        segment_label = _get_enrich_segment_label(segment_key)
    source_summary = ""
    meta0 = get_snapshot_meta()
    if weekly_mode:
        last_weekly = _safe_str(meta0.get(f"last_weekly_update_{scope}", ""))
        if last_weekly:
            try:
                last_dt = datetime.fromisoformat(last_weekly)
            except Exception:
                try:
                    last_dt = datetime.strptime(last_weekly, "%Y-%m-%d %H:%M:%S")
                except Exception:
                    last_dt = None
            if last_dt is not None:
                delta_days = (datetime.now() - last_dt).total_seconds() / 86400.0
                if delta_days < 7:
                    return {
                        "skipped": True,
                        "reason": f"周更间隔未满7天（已过 {delta_days:.1f} 天）",
                        "row_count": int(_to_float(meta0.get("row_count")) or 0),
                        "enriched_count": 0,
                        "market_scope": scope,
                        "enrich_segment": segment_key,
                        "enrich_segment_label": segment_label,
                        "segment_total": 0,
                        "segment_pending": 0,
                        "only_missing_enrich": bool(only_missing_enrich and not force_refresh),
                    }
    enrich_mode = "rotate" if rotate_enrich else "top"
    if only_missing_enrich:
        enrich_mode = f"{enrich_mode}_missing"

    data_backup_id = ""
    if create_data_backup is not None:
        try:
            backup_manifest = create_data_backup(
                reason=f"before_refresh_market_snapshot:{scope}:{segment_key}:{enrich_mode}",
                asset_keys=("filter_market_db", "filter_templates", "manual_flags"),
                max_keep=30,
            )
            data_backup_id = _safe_str(backup_manifest.get("backup_id"))
        except Exception:
            data_backup_id = ""

    local_snapshot_first = bool(scope in {"A", "HK"} and only_missing_enrich and not force_refresh)
    if local_snapshot_first:
        existing_snapshot = load_snapshot()
        if existing_snapshot is not None and not existing_snapshot.empty and "market" in existing_snapshot.columns:
            existing_snapshot = existing_snapshot[existing_snapshot["market"].astype(str).str.upper() == scope].copy()
        if existing_snapshot is not None and not existing_snapshot.empty:
            df = existing_snapshot.copy()
            source_summary = "本地快照"
        else:
            local_snapshot_first = False

    if not local_snapshot_first:
        try:
            df, source_notes = _build_base_universe(market_scope=scope)
            df = df.copy()
            source_summary = " / ".join([_safe_str(item) for item in source_notes if _safe_str(item)])
        except Exception as exc:
            # 网络/代理异常时兜底：如果本地已有快照，不让更新动作直接失败
            existed = load_snapshot()
            if existed is not None and not existed.empty:
                now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                _snapshot_meta_set("last_refresh_error", str(exc))
                _snapshot_meta_set("last_refresh_error_at", now_text)
                _snapshot_meta_set("last_refresh_fallback", "1")
                fallback_total = 0
                fallback_pending = 0
                if scope == "A":
                    for row in get_a_enrich_segment_status(existed):
                        if _safe_str(row.get("key")) == segment_key:
                            fallback_total = int(row.get("count", 0) or 0)
                            fallback_pending = max(0, fallback_total - int(row.get("persisted_count", 0) or 0))
                            break
                elif scope == "HK":
                    for row in get_hk_enrich_segment_status(existed):
                        if _safe_str(row.get("key")) == segment_key:
                            fallback_total = int(row.get("count", 0) or 0)
                            fallback_pending = max(0, fallback_total - int(row.get("persisted_count", 0) or 0))
                            break
                _log_snapshot_run(
                    row_count=int(len(existed)),
                    enriched_count=0,
                    enrich_start=0,
                    enrich_end=0,
                    fallback=True,
                    error_brief=str(exc),
                    cache_hit=0,
                    cache_miss=0,
                    enrich_mode=enrich_mode,
                )
                return {
                    "row_count": int(len(existed)),
                    "enriched_count": 0,
                    "updated_at": now_text,
                    "fallback": True,
                    "error": str(exc),
                    "enrich_mode": enrich_mode,
                    "enrich_start": 0,
                    "enrich_end": 0,
                    "enrich_segment": segment_key,
                    "enrich_segment_label": segment_label,
                    "segment_total": fallback_total,
                    "segment_pending": fallback_pending,
                    "only_missing_enrich": bool(only_missing_enrich and not force_refresh),
                    "source_summary": "本地快照",
                }
            raise RuntimeError(
                f"未能拉取市场快照（可能是代理/VPN导致连接被拒绝）：{exc}"
            ) from exc
    if "market" not in df.columns:
        df["market"] = "A"
    df["market"] = df["market"].astype(str).str.upper()
    df = df[
        ((df["market"] == "A") & df["code"].astype(str).str.fullmatch(r"\d{6}", na=False))
        | ((df["market"] == "HK") & df["code"].astype(str).str.fullmatch(r"\d{5}", na=False))
    ].copy()
    df = _overlay_latest_enrichment(df)
    df = df.sort_values(by=["total_mv", "code"], ascending=[False, True], na_position="last").reset_index(drop=True)

    if max_stocks and max_stocks > 0:
        df = df.head(int(max_stocks)).copy()

    hk_classification_df = pd.DataFrame()
    if scope == "HK" and int(enrich_top_n or 0) > 0:
        sync_hk_classification(df, force_refresh=False, safe_mode=safe_mode)
        hk_classification_df = _load_hk_classification()

    manual_flags = _load_manual_flags()

    enrich_n = max(0, min(int(enrich_top_n), len(df)))
    start_idx = 0
    target_indices: List[int] = []
    cache_hit = 0
    cache_miss = 0

    if scope == "A":
        eligible_indices = [
            int(i)
            for i, row in df.iterrows()
            if _safe_str(row.get("market")).upper() == "A" and _match_a_enrich_segment(row.get("code"), segment_key)
        ]
    elif scope == "HK":
        eligible_indices = [
            int(i)
            for i, row in df.iterrows()
            if _safe_str(row.get("market")).upper() == "HK" and _match_hk_enrich_segment(row.get("code"), segment_key, hk_classification_df)
        ]
    else:
        eligible_indices = []
    eligible_total = len(eligible_indices)
    pending_indices = eligible_indices
    if only_missing_enrich and not force_refresh:
        pending_indices = [idx for idx in eligible_indices if not _safe_str(df.at[idx, "enriched_at"])]
    pending_total = len(pending_indices)
    enrich_n = max(0, min(enrich_n, pending_total if (only_missing_enrich and not force_refresh) else eligible_total))
    cursor_key = f"enrich_cursor_index_{scope}_{segment_key}"

    active_indices = pending_indices if (only_missing_enrich and not force_refresh) else eligible_indices

    if enrich_n > 0 and active_indices:
        if rotate_enrich:
            meta = get_snapshot_meta()
            try:
                start_idx = int(meta.get(cursor_key, "0"))
            except Exception:
                start_idx = 0
            start_idx = start_idx % len(active_indices)
            target_indices = [active_indices[int((start_idx + step) % len(active_indices))] for step in range(enrich_n)]
        else:
            target_indices = active_indices[:enrich_n]

    persist_rows: List[Dict[str, Any]] = []
    consecutive_fail = 0
    for idx in target_indices:
        code = str(df.at[idx, "code"])
        name = str(df.at[idx, "name"])
        try:
            ext, source, cached_at = _enrich_one(code, name, force_refresh=force_refresh)
            for k, v in ext.items():
                if k in df.columns and v is not None:
                    df.at[idx, k] = v
            df.at[idx, "enriched_at"] = _normalize_dt_text(cached_at) or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if source == "cache":
                cache_hit += 1
            else:
                cache_miss += 1
            persist_rows.append(
                _build_enrichment_persist_row(
                    df.loc[idx],
                    source_note=f"deep_enrich:{source}",
                )
            )
            consecutive_fail = 0
        except Exception:
            consecutive_fail += 1

        # 防封节流：随机抖动 + 连续失败熔断
        if safe_mode:
            time.sleep(random.uniform(0.25, 0.65))
            if consecutive_fail >= 15:
                break
        if idx > 0 and idx % 40 == 0:
            time.sleep(0.2 if safe_mode else 0.05)

    for i in range(len(df)):
        code = str(df.at[i, "code"])
        flags = manual_flags.get(code, {}) if isinstance(manual_flags, dict) else {}
        df.at[i, "investigation_flag"] = int(bool(flags.get("investigation", False)))
        df.at[i, "penalty_flag"] = int(bool(flags.get("penalty", False)))
        df.at[i, "fund_occupation_flag"] = int(bool(flags.get("fund_occupation", False)))
        df.at[i, "illegal_reduce_flag"] = int(bool(flags.get("illegal_reduce", False)))
        df.at[i, "pledge_ratio"] = _to_float(flags.get("pledge_ratio"))
        df.at[i, "no_dividend_5y_flag"] = int(bool(flags.get("no_dividend_5y", False)))
        df.at[i, "audit_change_count"] = int(_to_float(flags.get("audit_change_count")) or 0)
        if _safe_str(flags.get("audit_opinion")):
            df.at[i, "audit_opinion"] = _safe_str(flags.get("audit_opinion"))

    df["sunset_industry_flag"] = df.apply(
        lambda r: int(_detect_sunset(str(r.get("industry", "")), str(r.get("name", "")))),
        axis=1,
    )

    # 数据质量分级
    key_cols = [
        "pe_ttm",
        "pb",
        "dividend_yield",
        "roe",
        "gross_margin",
        "net_margin",
        "asset_liability_ratio",
        "operating_cashflow_3y",
    ]

    def _quality(row: pd.Series) -> str:
        cnt = sum(1 for c in key_cols if _to_float(row.get(c)) is not None)
        if cnt >= 6:
            return "full"
        if cnt >= 3:
            return "partial"
        return "missing"

    df["data_quality"] = df.apply(_quality, axis=1)

    if persist_rows:
        _upsert_stock_enrichment_latest(persist_rows)
        df = _overlay_latest_enrichment(df)

    cols = [
        "market",
        "code",
        "name",
        "industry",
        "is_st",
        "close_price",
        "price_change_pct",
        "amount",
        "pe_dynamic",
        "pe_static",
        "pe_ttm",
        "pb",
        "dividend_yield",
        "total_mv",
        "float_mv",
        "turnover_ratio",
        "volume_ratio",
        "roe",
        "gross_margin",
        "net_margin",
        "asset_liability_ratio",
        "current_ratio",
        "operating_cashflow_3y",
        "receivable_revenue_ratio",
        "goodwill_equity_ratio",
        "interest_debt_asset_ratio",
        "ev_ebitda",
        "revenue_growth",
        "profit_growth",
        "revenue_cagr_5y",
        "profit_cagr_5y",
        "roe_avg_5y",
        "debt_ratio_avg_5y",
        "gross_margin_avg_5y",
        "debt_ratio_change_5y",
        "gross_margin_change_5y",
        "ocf_positive_years_5y",
        "investigation_flag",
        "penalty_flag",
        "fund_occupation_flag",
        "illegal_reduce_flag",
        "pledge_ratio",
        "no_dividend_5y_flag",
        "audit_change_count",
        "audit_opinion",
        "sunset_industry_flag",
        "total_score",
        "conclusion",
        "coverage_ratio",
        "data_quality",
        "enriched_at",
        "updated_at",
        "source_note",
    ]
    save_df = df[cols].copy()
    if save_df.empty:
        existed = load_snapshot()
        if existed is not None and not existed.empty:
            now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            empty_err = "更新结果为空，已阻止空快照覆盖旧数据"
            _snapshot_meta_set("last_refresh_error", empty_err)
            _snapshot_meta_set("last_refresh_error_at", now_text)
            _snapshot_meta_set("last_refresh_fallback", "1")
            fallback_total = 0
            fallback_pending = 0
            if scope == "A":
                for row in get_a_enrich_segment_status(existed):
                    if _safe_str(row.get("key")) == segment_key:
                        fallback_total = int(row.get("count", 0) or 0)
                        fallback_pending = max(0, fallback_total - int(row.get("persisted_count", 0) or 0))
                        break
            elif scope == "HK":
                for row in get_hk_enrich_segment_status(existed):
                    if _safe_str(row.get("key")) == segment_key:
                        fallback_total = int(row.get("count", 0) or 0)
                        fallback_pending = max(0, fallback_total - int(row.get("persisted_count", 0) or 0))
                        break
            _log_snapshot_run(
                row_count=int(len(existed)),
                enriched_count=0,
                enrich_start=0,
                enrich_end=0,
                fallback=True,
                error_brief=empty_err,
                cache_hit=cache_hit,
                cache_miss=cache_miss,
                enrich_mode=enrich_mode,
            )
            return {
                "row_count": int(len(existed)),
                "enriched_count": 0,
                "updated_at": now_text,
                "fallback": True,
                "error": empty_err,
                "enrich_mode": enrich_mode,
                "enrich_start": 0,
                "enrich_end": 0,
                "market_scope": scope,
                "enrich_segment": segment_key,
                "enrich_segment_label": segment_label,
                "segment_total": fallback_total,
                "segment_pending": fallback_pending,
                "only_missing_enrich": bool(only_missing_enrich and not force_refresh),
                "weekly_mode": bool(weekly_mode),
                "source_summary": "本地快照",
            }
        raise RuntimeError("更新结果为空，未写入 market_snapshot")

    backup_info = {"backed_up": False, "row_count": 0, "backup_at": ""}
    with _connect() as conn:
        backup_info = _backup_current_snapshot(conn)
        merged_df = _merge_scope_snapshot(conn, scope, save_df)
        if merged_df is None or merged_df.empty:
            raise RuntimeError(f"{scope} 市场合并写入结果为空，已取消覆盖主表")
        _replace_market_snapshot_atomically(conn, merged_df)

    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _snapshot_meta_set("last_update", now_text)
    _snapshot_meta_set("row_count", str(len(save_df)))
    _snapshot_meta_set("enriched_count", str(enrich_n))
    _snapshot_meta_set("app_version", APP_VERSION)
    _snapshot_meta_set("last_refresh_fallback", "0")
    _snapshot_meta_set("last_refresh_error", "")
    _snapshot_meta_set("last_refresh_error_at", "")
    _snapshot_meta_set("last_scope", scope)
    if scope == "A":
        _snapshot_meta_set("last_enrich_segment_A", segment_key)
        if enrich_n > 0:
            _snapshot_meta_set(f"last_enrich_segment_at_{segment_key}", now_text)
    elif scope == "HK":
        _snapshot_meta_set("last_enrich_segment_HK", segment_key)
        if enrich_n > 0:
            _snapshot_meta_set(f"last_enrich_segment_at_HK_{segment_key}", now_text)
    _snapshot_meta_set("last_backup_at", _safe_str(backup_info.get("backup_at", "")))
    _snapshot_meta_set("last_backup_row_count", str(int(backup_info.get("row_count", 0) or 0)))
    if rotate_enrich and enrich_n > 0 and active_indices:
        _snapshot_meta_set(cursor_key, str((start_idx + enrich_n) % len(active_indices)))
    if weekly_mode:
        _snapshot_meta_set(f"last_weekly_update_{scope}", now_text)

    enrich_start = int(start_idx + 1) if enrich_n > 0 else 0
    enrich_end = int(((start_idx + enrich_n - 1) % len(active_indices)) + 1) if enrich_n > 0 and active_indices else 0
    _log_snapshot_run(
        row_count=len(save_df),
        enriched_count=enrich_n,
        enrich_start=enrich_start,
        enrich_end=enrich_end,
        fallback=False,
        error_brief="",
        cache_hit=cache_hit,
        cache_miss=cache_miss,
        enrich_mode=enrich_mode,
    )

    return {
        "row_count": len(save_df),
        "enriched_count": enrich_n,
        "updated_at": now_text,
        "enrich_mode": enrich_mode,
        "enrich_start": enrich_start,
        "enrich_end": enrich_end,
        "cache_hit": cache_hit,
        "cache_miss": cache_miss,
        "market_scope": scope,
        "enrich_segment": segment_key,
        "enrich_segment_label": segment_label,
        "segment_total": eligible_total,
        "segment_pending": pending_total,
        "only_missing_enrich": bool(only_missing_enrich and not force_refresh),
        "weekly_mode": bool(weekly_mode),
        "source_summary": source_summary,
        "backup_at": _safe_str(backup_info.get("backup_at", "")),
        "backup_row_count": int(backup_info.get("row_count", 0) or 0),
        "data_backup_id": data_backup_id,
    }


def load_snapshot() -> pd.DataFrame:
    init_db()
    with _connect() as conn:
        try:
            df = pd.read_sql_query("SELECT * FROM market_snapshot ORDER BY total_mv DESC, code ASC", conn)
        except Exception:
            return pd.DataFrame()
    df = _overlay_latest_enrichment(df)
    if df is None or df.empty:
        return pd.DataFrame()
    return df.sort_values(by=["total_mv", "code"], ascending=[False, True], na_position="last").reset_index(drop=True)


def get_snapshot_health_report(days: int = 7, top_n: int = 20) -> Dict[str, Any]:
    init_db()
    meta = get_snapshot_meta()
    df = load_snapshot()

    total = int(len(df))
    quality_counts = {"full": 0, "partial": 0, "missing": 0}
    if total > 0 and "data_quality" in df.columns:
        vc = df["data_quality"].value_counts(dropna=False).to_dict()
        quality_counts = {
            "full": int(vc.get("full", 0)),
            "partial": int(vc.get("partial", 0)),
            "missing": int(vc.get("missing", 0)),
        }
    covered = int(quality_counts["full"] + quality_counts["partial"])
    coverage_ratio = (covered / total) if total > 0 else 0.0

    now = datetime.now()
    freshness = {
        "0_1d": 0,
        "1_3d": 0,
        "3_7d": 0,
        "7d_plus": 0,
        "never": 0,
    }

    enriched_series = df["enriched_at"] if ("enriched_at" in df.columns) else pd.Series([None] * total, index=df.index)
    parsed_dt: List[Optional[datetime]] = []
    for v in enriched_series:
        text = _safe_str(v)
        if not text:
            parsed_dt.append(None)
            freshness["never"] += 1
            continue
        dt_val: Optional[datetime] = None
        try:
            dt_val = datetime.fromisoformat(text)
        except Exception:
            try:
                dt_val = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
            except Exception:
                dt_val = None
        parsed_dt.append(dt_val)
        if dt_val is None:
            freshness["never"] += 1
            continue
        delta_days = (now - dt_val).total_seconds() / 86400.0
        if delta_days < 1:
            freshness["0_1d"] += 1
        elif delta_days < 3:
            freshness["1_3d"] += 1
        elif delta_days < 7:
            freshness["3_7d"] += 1
        else:
            freshness["7d_plus"] += 1

    key_cols = [
        "pe_ttm",
        "pb",
        "dividend_yield",
        "roe",
        "gross_margin",
        "net_margin",
        "asset_liability_ratio",
        "operating_cashflow_3y",
    ]
    available_key_cols = [c for c in key_cols if c in df.columns]
    if available_key_cols:
        missing_count_series = df[available_key_cols].isna().sum(axis=1)
    else:
        missing_count_series = pd.Series([0] * total, index=df.index)

    enrich_df = pd.DataFrame(
        {
            "code": df.get("code", pd.Series([], dtype=object)),
            "name": df.get("name", pd.Series([], dtype=object)),
            "enriched_at": [_normalize_dt_text(v.isoformat() if isinstance(v, datetime) else (v if v is not None else "")) for v in parsed_dt],
            "missing_fields_count": missing_count_series,
        }
    )

    oldest_df = enrich_df.copy()
    oldest_df["sort_dt"] = parsed_dt
    oldest_df = oldest_df[oldest_df["sort_dt"].notna()].sort_values(by=["sort_dt", "missing_fields_count"], ascending=[True, False]).head(int(top_n))
    oldest_df = oldest_df.drop(columns=["sort_dt"]).reset_index(drop=True)

    newest_df = enrich_df.copy()
    newest_df["sort_dt"] = parsed_dt
    newest_df = newest_df[newest_df["sort_dt"].notna()].sort_values(by=["sort_dt", "missing_fields_count"], ascending=[False, True]).head(int(top_n))
    newest_df = newest_df.drop(columns=["sort_dt"]).reset_index(drop=True)

    missing_rank = []
    for col in available_key_cols:
        missing_rank.append({"field": col, "missing_count": int(df[col].isna().sum())})
    missing_rank = sorted(missing_rank, key=lambda x: x["missing_count"], reverse=True)

    with _connect() as conn:
        trend_df = pd.read_sql_query(
            """
            SELECT substr(run_at, 1, 10) AS run_date,
                   SUM(enriched_count) AS enriched_total,
                   SUM(CASE WHEN fallback=1 THEN 1 ELSE 0 END) AS fallback_count,
                   COUNT(*) AS run_count
            FROM snapshot_runs
            WHERE run_at >= datetime('now', ?)
            GROUP BY substr(run_at, 1, 10)
            ORDER BY run_date ASC
            """,
            conn,
            params=(f"-{int(days)} days",),
        )
        latest_run_df = pd.read_sql_query(
            "SELECT * FROM snapshot_runs ORDER BY run_id DESC LIMIT 1",
            conn,
        )
        fail_df = pd.read_sql_query(
            """
            SELECT run_at, fallback, error_brief, enrich_mode
            FROM snapshot_runs
            WHERE fallback=1 OR length(trim(coalesce(error_brief, ''))) > 0
            ORDER BY run_id DESC
            LIMIT 5
            """,
            conn,
        )
        runs_df = pd.read_sql_query(
            """
            SELECT run_at, row_count, enriched_count, enrich_start, enrich_end,
                   fallback, cache_hit, cache_miss, enrich_mode, error_brief
            FROM snapshot_runs
            ORDER BY run_id DESC
            LIMIT 50
            """,
            conn,
        )

    latest_run = latest_run_df.iloc[0].to_dict() if not latest_run_df.empty else {}
    cache_hit = int(latest_run.get("cache_hit", 0) or 0)
    cache_miss = int(latest_run.get("cache_miss", 0) or 0)

    fail_type_df = pd.DataFrame(columns=["error_type", "count"])
    if isinstance(runs_df, pd.DataFrame) and (not runs_df.empty):
        tmp = runs_df.copy()
        tmp["error_type"] = tmp["error_brief"].map(_classify_error_type)
        tmp = tmp[tmp["error_type"] != "none"]
        if not tmp.empty:
            fail_type_df = (
                tmp.groupby("error_type", as_index=False)
                .size()
                .rename(columns={"size": "count"})
                .sort_values(by="count", ascending=False)
                .reset_index(drop=True)
            )

    last_scope = _safe_str(meta.get("last_scope", "A")) or "A"
    cursor_idx = int(_to_float(meta.get(f"enrich_cursor_index_{last_scope}", meta.get("enrich_cursor_index"))) or 0)
    cursor_pos = (cursor_idx + 1) if total > 0 else 0

    return {
        "meta": meta,
        "last_scope": last_scope,
        "total": total,
        "quality_counts": quality_counts,
        "covered": covered,
        "coverage_ratio": coverage_ratio,
        "freshness": freshness,
        "oldest_df": oldest_df,
        "newest_df": newest_df,
        "missing_rank": missing_rank,
        "trend_df": trend_df,
        "fail_df": fail_df,
        "fail_type_df": fail_type_df,
        "runs_df": runs_df,
        "latest_run": latest_run,
        "cache_hit": cache_hit,
        "cache_miss": cache_miss,
        "cursor_pos": cursor_pos,
    }


def export_snapshot_health_excel(days: int = 30, top_n: int = 50) -> bytes:
    report = get_snapshot_health_report(days=days, top_n=top_n)
    meta = report.get("meta", {}) if isinstance(report, dict) else {}
    total = int(report.get("total", 0) or 0)
    qc = report.get("quality_counts", {}) if isinstance(report, dict) else {}
    fresh = report.get("freshness", {}) if isinstance(report, dict) else {}

    summary_df = pd.DataFrame(
        [
            {"item": "last_update", "value": _safe_str(meta.get("last_update", ""))},
            {"item": "last_refresh_fallback", "value": _safe_str(meta.get("last_refresh_fallback", ""))},
            {"item": "last_refresh_error_at", "value": _safe_str(meta.get("last_refresh_error_at", ""))},
            {"item": "enrich_cursor_index", "value": _safe_str(meta.get("enrich_cursor_index", ""))},
            {"item": "row_count", "value": str(total)},
            {"item": "quality_full", "value": str(int(qc.get("full", 0) or 0))},
            {"item": "quality_partial", "value": str(int(qc.get("partial", 0) or 0))},
            {"item": "quality_missing", "value": str(int(qc.get("missing", 0) or 0))},
            {"item": "fresh_0_1d", "value": str(int(fresh.get("0_1d", 0) or 0))},
            {"item": "fresh_1_3d", "value": str(int(fresh.get("1_3d", 0) or 0))},
            {"item": "fresh_3_7d", "value": str(int(fresh.get("3_7d", 0) or 0))},
            {"item": "fresh_7d_plus", "value": str(int(fresh.get("7d_plus", 0) or 0))},
            {"item": "fresh_never", "value": str(int(fresh.get("never", 0) or 0))},
        ]
    )

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        summary_df.to_excel(writer, index=False, sheet_name="summary")
        pd.DataFrame(report.get("trend_df", pd.DataFrame())).to_excel(writer, index=False, sheet_name="trend_7d")
        pd.DataFrame(report.get("runs_df", pd.DataFrame())).to_excel(writer, index=False, sheet_name="runs_latest50")
        pd.DataFrame(report.get("fail_df", pd.DataFrame())).to_excel(writer, index=False, sheet_name="fails_latest5")
        pd.DataFrame(report.get("fail_type_df", pd.DataFrame())).to_excel(writer, index=False, sheet_name="fail_types")
        pd.DataFrame(report.get("missing_rank", [])).to_excel(writer, index=False, sheet_name="missing_rank")
        pd.DataFrame(report.get("oldest_df", pd.DataFrame())).to_excel(writer, index=False, sheet_name="oldest_enriched")
        pd.DataFrame(report.get("newest_df", pd.DataFrame())).to_excel(writer, index=False, sheet_name="newest_enriched")
    return output.getvalue()


def _num(v: Any) -> Optional[float]:
    return _to_float(v)


def _check_missing(enabled: bool, value: Optional[float], name: str, cfg: Dict[str, Any], reasons: List[str], missing: List[str]) -> bool:
    if not enabled:
        return False
    if value is not None:
        return False
    if str(cfg.get("missing_policy", "ignore")) == "exclude":
        reasons.append(f"{name}:缺失")
    else:
        missing.append(name)
    return True


def _check_min(enabled: bool, value: Optional[float], floor: float, name: str, cfg: Dict[str, Any], reasons: List[str], missing: List[str]) -> None:
    if not enabled:
        return
    if _check_missing(True, value, name, cfg, reasons, missing):
        return
    if value is not None and value < floor:
        reasons.append(f"{name}<{floor:g} (当前 {value:.2f})")


def _check_max(enabled: bool, value: Optional[float], ceil: float, name: str, cfg: Dict[str, Any], reasons: List[str], missing: List[str]) -> None:
    if not enabled:
        return
    if _check_missing(True, value, name, cfg, reasons, missing):
        return
    if value is not None and value > ceil:
        reasons.append(f"{name}>{ceil:g} (当前 {value:.2f})")


def _check_range(
    min_enabled: bool,
    max_enabled: bool,
    value: Optional[float],
    floor: float,
    ceil: float,
    name: str,
    cfg: Dict[str, Any],
    reasons: List[str],
    missing: List[str],
) -> None:
    if not min_enabled and not max_enabled:
        return
    if _check_missing(True, value, name, cfg, reasons, missing):
        return
    if value is None:
        return
    if min_enabled and value < floor:
        reasons.append(f"{name}<{floor:g} (当前 {value:.2f})")
    if max_enabled and value > ceil:
        reasons.append(f"{name}>{ceil:g} (当前 {value:.2f})")


def _split_keywords(text: str) -> List[str]:
    raw = str(text or "")
    parts = re.split(r"[,，;；\n]+", raw)
    return [p.strip() for p in parts if p.strip()]


def _expand_industry_keywords(kws: List[str]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()

    def _add(v: str) -> None:
        key = _safe_str(v).lower()
        if not key:
            return
        if key in seen:
            return
        seen.add(key)
        out.append(v)

    for one in kws:
        raw = _safe_str(one)
        _add(raw)
        for alias in INDUSTRY_KEYWORD_ALIAS_MAP.get(raw.lower(), []):
            _add(alias)
    return out


def apply_filters(df: pd.DataFrame, config: Dict[str, Any]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict[str, int]]:
    if df is None or df.empty:
        empty = pd.DataFrame(columns=DISPLAY_COLUMNS)
        return empty, empty, empty, {"total": 0, "passed": 0, "rejected": 0, "missing": 0}

    cfg = copy.deepcopy(config or default_filter_config())
    r = cfg.get("risk", {})
    q = cfg.get("quality", {})
    v = cfg.get("valuation", {})
    g = cfg.get("growth_liquidity", {})
    d5 = cfg.get("rearview_5y", {})

    out_rows: List[Dict[str, Any]] = []

    for _, row in df.iterrows():
        reasons: List[str] = []
        missing: List[str] = []

        scope = _safe_str(r.get("market_scope", "all")).upper()
        row_market = _safe_str(row.get("market")).upper() or "A"
        if scope in {"A", "HK"} and row_market != scope:
            reasons.append(f"市场不匹配({scope})")

        if bool(r.get("industry_include_enabled", False)):
            kws = _expand_industry_keywords(_split_keywords(r.get("industry_include_keywords", "")))
            if kws:
                search_text = f"{_safe_str(row.get('industry'))} {_safe_str(row.get('name'))}".lower()
                if not any(k.lower() in search_text for k in kws):
                    reasons.append("行业不匹配")

        is_st = int(_num(row.get("is_st")) or 0)
        if r.get("exclude_st", True) and is_st == 1:
            reasons.append("ST/*ST")

        if r.get("exclude_investigation", True) and int(_num(row.get("investigation_flag")) or 0) == 1:
            reasons.append("存在立案调查")
        if r.get("exclude_penalty", True) and int(_num(row.get("penalty_flag")) or 0) == 1:
            reasons.append("存在重大处罚")
        if r.get("exclude_fund_occupation", True) and int(_num(row.get("fund_occupation_flag")) or 0) == 1:
            reasons.append("存在资金占用")
        if r.get("exclude_illegal_reduce", True) and int(_num(row.get("illegal_reduce_flag")) or 0) == 1:
            reasons.append("存在违规减持")

        if r.get("require_standard_audit", False):
            audit = _safe_str(row.get("audit_opinion"))
            if not audit:
                _check_missing(True, None, "审计意见", cfg, reasons, missing)
            elif "标准无保留" not in audit:
                reasons.append(f"审计意见异常: {audit}")

        _check_max(
            bool(r.get("pledge_ratio_max_enabled", False)),
            _num(row.get("pledge_ratio")),
            float(r.get("pledge_ratio_max", 80.0)),
            "实控人质押率",
            cfg,
            reasons,
            missing,
        )

        _check_max(
            bool(r.get("audit_change_max_enabled", False)),
            _num(row.get("audit_change_count")),
            float(r.get("audit_change_max", 2)),
            "审计所更换次数(3年)",
            cfg,
            reasons,
            missing,
        )

        if r.get("exclude_no_dividend_5y", False) and int(_num(row.get("no_dividend_5y_flag")) or 0) == 1:
            reasons.append("近5年未分红")

        if r.get("exclude_sunset_industry", False):
            kws = _split_keywords(r.get("sunset_industries", "")) or DEFAULT_SUNSET_INDUSTRIES
            text = f"{_safe_str(row.get('industry'))} {_safe_str(row.get('name'))}"
            if any(k in text for k in kws):
                reasons.append("夕阳行业")

        _check_min(
            bool(q.get("ocf_3y_min_enabled", False)),
            _num(row.get("operating_cashflow_3y")),
            float(q.get("ocf_3y_min", 0.0)),
            "近3年经营现金流(亿)",
            cfg,
            reasons,
            missing,
        )
        _check_max(
            bool(q.get("asset_liability_max_enabled", False)),
            _num(row.get("asset_liability_ratio")),
            float(q.get("asset_liability_max", 80.0)),
            "资产负债率(%)",
            cfg,
            reasons,
            missing,
        )
        _check_max(
            bool(q.get("interest_debt_asset_max_enabled", False)),
            _num(row.get("interest_debt_asset_ratio")),
            float(q.get("interest_debt_asset_max", 20.0)),
            "有息负债/总资产(%)",
            cfg,
            reasons,
            missing,
        )
        _check_min(
            bool(q.get("roe_min_enabled", False)),
            _num(row.get("roe")),
            float(q.get("roe_min", 5.0)),
            "ROE(%)",
            cfg,
            reasons,
            missing,
        )
        _check_min(
            bool(q.get("gross_margin_min_enabled", False)),
            _num(row.get("gross_margin")),
            float(q.get("gross_margin_min", 20.0)),
            "毛利率(%)",
            cfg,
            reasons,
            missing,
        )
        _check_min(
            bool(q.get("net_margin_min_enabled", False)),
            _num(row.get("net_margin")),
            float(q.get("net_margin_min", 8.0)),
            "净利率(%)",
            cfg,
            reasons,
            missing,
        )
        _check_max(
            bool(q.get("receivable_ratio_max_enabled", False)),
            _num(row.get("receivable_revenue_ratio")),
            float(q.get("receivable_ratio_max", 50.0)),
            "应收代理指标",
            cfg,
            reasons,
            missing,
        )
        _check_max(
            bool(q.get("goodwill_ratio_max_enabled", False)),
            _num(row.get("goodwill_equity_ratio")),
            float(q.get("goodwill_ratio_max", 30.0)),
            "商誉/净资产(%)",
            cfg,
            reasons,
            missing,
        )

        _check_range(
            bool(v.get("pe_ttm_min_enabled", False)),
            bool(v.get("pe_ttm_max_enabled", False)),
            _num(row.get("pe_ttm")),
            float(v.get("pe_ttm_min", 0.0)),
            float(v.get("pe_ttm_max", 25.0)),
            "PE(TTM)",
            cfg,
            reasons,
            missing,
        )
        _check_max(
            bool(v.get("pb_max_enabled", False)),
            _num(row.get("pb")),
            float(v.get("pb_max", 3.0)),
            "PB",
            cfg,
            reasons,
            missing,
        )
        _check_max(
            bool(v.get("ev_ebitda_max_enabled", False)),
            _num(row.get("ev_ebitda")),
            float(v.get("ev_ebitda_max", 18.0)),
            "EV/EBITDA",
            cfg,
            reasons,
            missing,
        )
        _check_range(
            bool(v.get("dividend_min_enabled", False)),
            bool(v.get("dividend_max_enabled", False)),
            _num(row.get("dividend_yield")),
            float(v.get("dividend_min", 3.0)),
            float(v.get("dividend_max", 12.0)),
            "股息率(%)",
            cfg,
            reasons,
            missing,
        )

        _check_min(
            bool(g.get("revenue_growth_min_enabled", False)),
            _num(row.get("revenue_growth")),
            float(g.get("revenue_growth_min", 0.0)),
            "营收增速(%)",
            cfg,
            reasons,
            missing,
        )
        _check_min(
            bool(g.get("profit_growth_min_enabled", False)),
            _num(row.get("profit_growth")),
            float(g.get("profit_growth_min", 0.0)),
            "利润增速(%)",
            cfg,
            reasons,
            missing,
        )
        _check_range(
            bool(g.get("market_cap_min_enabled", False)),
            bool(g.get("market_cap_max_enabled", False)),
            _num(row.get("total_mv")),
            float(g.get("market_cap_min", 100.0)),
            float(g.get("market_cap_max", 5000.0)),
            "总市值(亿)",
            cfg,
            reasons,
            missing,
        )
        _check_range(
            bool(g.get("turnover_min_enabled", False)),
            bool(g.get("turnover_max_enabled", False)),
            _num(row.get("turnover_ratio")),
            float(g.get("turnover_min", 0.2)),
            float(g.get("turnover_max", 15.0)),
            "换手率(%)",
            cfg,
            reasons,
            missing,
        )
        _check_range(
            bool(g.get("volume_ratio_min_enabled", False)),
            bool(g.get("volume_ratio_max_enabled", False)),
            _num(row.get("volume_ratio")),
            float(g.get("volume_ratio_min", 0.5)),
            float(g.get("volume_ratio_max", 3.0)),
            "量比",
            cfg,
            reasons,
            missing,
        )
        _check_min(
            bool(g.get("amount_min_enabled", False)),
            _num(row.get("amount")),
            float(g.get("amount_min", 100000000.0)),
            "成交额(元)",
            cfg,
            reasons,
            missing,
        )

        _check_min(
            bool(d5.get("revenue_cagr_5y_min_enabled", False)),
            _num(row.get("revenue_cagr_5y")),
            float(d5.get("revenue_cagr_5y_min", 3.0)),
            "营收5年CAGR(%)",
            cfg,
            reasons,
            missing,
        )
        _check_min(
            bool(d5.get("profit_cagr_5y_min_enabled", False)),
            _num(row.get("profit_cagr_5y")),
            float(d5.get("profit_cagr_5y_min", 3.0)),
            "净利5年CAGR(%)",
            cfg,
            reasons,
            missing,
        )
        _check_min(
            bool(d5.get("roe_avg_5y_min_enabled", False)),
            _num(row.get("roe_avg_5y")),
            float(d5.get("roe_avg_5y_min", 8.0)),
            "ROE5年均值(%)",
            cfg,
            reasons,
            missing,
        )
        _check_min(
            bool(d5.get("ocf_positive_years_5y_min_enabled", False)),
            _num(row.get("ocf_positive_years_5y")),
            float(d5.get("ocf_positive_years_5y_min", 4)),
            "经营现金流为正年数(5年)",
            cfg,
            reasons,
            missing,
        )
        _check_max(
            bool(d5.get("debt_ratio_change_5y_max_enabled", False)),
            _num(row.get("debt_ratio_change_5y")),
            float(d5.get("debt_ratio_change_5y_max", 8.0)),
            "负债率5年变化(百分点)",
            cfg,
            reasons,
            missing,
        )
        _check_min(
            bool(d5.get("gross_margin_change_5y_min_enabled", False)),
            _num(row.get("gross_margin_change_5y")),
            float(d5.get("gross_margin_change_5y_min", -6.0)),
            "毛利率5年变化(百分点)",
            cfg,
            reasons,
            missing,
        )

        out = {
            "market": _safe_str(row.get("market")) or "A",
            "code": _safe_str(row.get("code")),
            "name": _safe_str(row.get("name")),
            "industry": _safe_str(row.get("industry")),
            "pe_ttm": _num(row.get("pe_ttm")),
            "pb": _num(row.get("pb")),
            "dividend_yield": _num(row.get("dividend_yield")),
            "roe": _num(row.get("roe")),
            "asset_liability_ratio": _num(row.get("asset_liability_ratio")),
            "turnover_ratio": _num(row.get("turnover_ratio")),
            "volume_ratio": _num(row.get("volume_ratio")),
            "total_mv": _num(row.get("total_mv")),
            "revenue_cagr_5y": _num(row.get("revenue_cagr_5y")),
            "profit_cagr_5y": _num(row.get("profit_cagr_5y")),
            "roe_avg_5y": _num(row.get("roe_avg_5y")),
            "ocf_positive_years_5y": _num(row.get("ocf_positive_years_5y")),
            "debt_ratio_change_5y": _num(row.get("debt_ratio_change_5y")),
            "gross_margin_change_5y": _num(row.get("gross_margin_change_5y")),
            "data_quality": _safe_str(row.get("data_quality")) or "partial",
            "exclude_reasons": "；".join(reasons),
            "missing_fields": "、".join(missing),
            "passed": 1 if len(reasons) == 0 else 0,
        }
        out_rows.append(out)

    res_df = pd.DataFrame(out_rows)
    passed_df = res_df[res_df["passed"] == 1].copy()
    rejected_df = res_df[res_df["passed"] == 0].copy()
    missing_df = res_df[res_df["missing_fields"].astype(str).str.len() > 0].copy()

    for d in (passed_df, rejected_df, missing_df):
        for c in DISPLAY_COLUMNS:
            if c not in d.columns:
                d[c] = None

    passed_df = passed_df[DISPLAY_COLUMNS].sort_values(by=["total_mv", "code"], ascending=[False, True], na_position="last")
    rejected_df = rejected_df[DISPLAY_COLUMNS].sort_values(by=["code"])
    missing_df = missing_df[DISPLAY_COLUMNS].sort_values(by=["code"])

    stats = {
        "total": int(len(res_df)),
        "passed": int(len(passed_df)),
        "rejected": int(len(rejected_df)),
        "missing": int(len(missing_df)),
    }
    return passed_df, rejected_df, missing_df, stats


def export_results_excel(passed_df: pd.DataFrame, rejected_df: pd.DataFrame, missing_df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        passed_df.to_excel(writer, index=False, sheet_name="通过池")
        rejected_df.to_excel(writer, index=False, sheet_name="排除池")
        missing_df.to_excel(writer, index=False, sheet_name="缺失项")
    return output.getvalue()


def build_ai_quick_config(prompt: str, base_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    cfg = copy.deepcopy(base_config or default_filter_config())
    text = _safe_str(prompt).lower()

    if any(k in text for k in ["高股息", "分红", "红利"]):
        cfg["valuation"]["dividend_min_enabled"] = True
        cfg["valuation"]["dividend_min"] = max(float(cfg["valuation"]["dividend_min"]), 4.0)

    if any(k in text for k in ["低估值", "便宜", "价值"]):
        cfg["valuation"]["pe_ttm_max_enabled"] = True
        cfg["valuation"]["pe_ttm_max"] = min(float(cfg["valuation"]["pe_ttm_max"]), 18.0)
        cfg["valuation"]["pb_max_enabled"] = True
        cfg["valuation"]["pb_max"] = min(float(cfg["valuation"]["pb_max"]), 2.2)

    if any(k in text for k in ["低负债", "稳健", "防御"]):
        cfg["quality"]["asset_liability_max_enabled"] = True
        cfg["quality"]["asset_liability_max"] = min(float(cfg["quality"]["asset_liability_max"]), 60.0)
        cfg["quality"]["roe_min_enabled"] = True
        cfg["quality"]["roe_min"] = max(float(cfg["quality"]["roe_min"]), 8.0)

    if any(k in text for k in ["现金流", "经营现金流"]):
        cfg["quality"]["ocf_3y_min_enabled"] = True
        cfg["quality"]["ocf_3y_min"] = max(float(cfg["quality"]["ocf_3y_min"]), 0.0)

    if any(k in text for k in ["大市值", "大盘", "龙头"]):
        cfg["growth_liquidity"]["market_cap_min_enabled"] = True
        cfg["growth_liquidity"]["market_cap_min"] = max(float(cfg["growth_liquidity"]["market_cap_min"]), 300.0)

    if any(k in text for k in ["五年", "长期", "后视镜", "复合增长"]):
        cfg["rearview_5y"]["revenue_cagr_5y_min_enabled"] = True
        cfg["rearview_5y"]["revenue_cagr_5y_min"] = max(float(cfg["rearview_5y"]["revenue_cagr_5y_min"]), 3.0)
        cfg["rearview_5y"]["profit_cagr_5y_min_enabled"] = True
        cfg["rearview_5y"]["profit_cagr_5y_min"] = max(float(cfg["rearview_5y"]["profit_cagr_5y_min"]), 3.0)
        cfg["rearview_5y"]["ocf_positive_years_5y_min_enabled"] = True
        cfg["rearview_5y"]["ocf_positive_years_5y_min"] = max(int(cfg["rearview_5y"]["ocf_positive_years_5y_min"]), 4)

    if any(k in text for k in ["排雷", "风险", "避雷"]):
        cfg["risk"]["exclude_st"] = True
        cfg["risk"]["exclude_investigation"] = True
        cfg["risk"]["exclude_penalty"] = True
        cfg["risk"]["exclude_fund_occupation"] = True
        cfg["risk"]["exclude_illegal_reduce"] = True

    return cfg
