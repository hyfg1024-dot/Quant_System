from __future__ import annotations

import copy
import io
import json
import os
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
FUNDAMENTAL_DIR = CURRENT_DIR.parent / "fundamental"
if str(FUNDAMENTAL_DIR) not in sys.path:
    sys.path.insert(0, str(FUNDAMENTAL_DIR))

from fundamental_engine import analyze_fundamental

APP_VERSION = "FLT-20260327-02"
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

DEFAULT_FILTER_CONFIG: Dict[str, Any] = {
    "missing_policy": "ignore",  # ignore / exclude
    "risk": {
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


def _ak_call_with_proxy_fallback(func, *args, **kwargs):
    last_exc: Optional[Exception] = None

    # 第一轮：按当前环境直接请求
    for _ in range(2):
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            last_exc = exc
            time.sleep(0.8)

    # 第二轮：临时关闭代理环境变量后重试
    backup = {k: os.environ.get(k, _ENV_MISSING) for k in _PROXY_ENV_KEYS}
    try:
        for k in _PROXY_ENV_KEYS:
            os.environ.pop(k, None)
        os.environ["NO_PROXY"] = "*"
        os.environ["no_proxy"] = "*"

        for _ in range(2):
            try:
                return func(*args, **kwargs)
            except Exception as exc:
                last_exc = exc
                time.sleep(0.8)
    finally:
        for k, v in backup.items():
            if v is _ENV_MISSING:
                os.environ.pop(k, None)
            else:
                os.environ[k] = str(v)

    if last_exc is not None:
        raise last_exc
    raise RuntimeError("AKShare 请求失败")


def _pick_series(df: pd.DataFrame, names: List[str]) -> pd.Series:
    for n in names:
        if n in df.columns:
            return df[n]
    return pd.Series([None] * len(df), index=df.index)


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


def init_db() -> None:
    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS market_snapshot (
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
        conn.commit()


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


def _enrich_one(code: str, name: str, force_refresh: bool = False) -> Dict[str, Any]:
    if not force_refresh:
        cached = _load_enrich_cache(code)
        if cached:
            return cached

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
    return payload


def _build_base_universe() -> pd.DataFrame:
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


def _fetch_a_spot_em_direct() -> pd.DataFrame:
    """
    东方财富直连兜底（禁用系统代理），避免 ProxyError 导致整次更新失败。
    仅提供筛选所需核心字段。
    """
    hosts = [
        "https://82.push2.eastmoney.com/api/qt/clist/get",
        "https://push2.eastmoney.com/api/qt/clist/get",
        "https://71.push2.eastmoney.com/api/qt/clist/get",
    ]
    params = {
        "pn": "1",
        "pz": "5000",
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
                ses = requests.Session()
                ses.trust_env = False
                resp = ses.get(url, params=params, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
                resp.raise_for_status()
                obj = resp.json()
                diff = (((obj or {}).get("data") or {}).get("diff") or [])
                if not diff:
                    raise RuntimeError("东方财富直连返回空数据")

                rows: List[Dict[str, Any]] = []
                for it in diff:
                    rows.append(
                        {
                            "代码": str((it or {}).get("f12", "")).strip(),
                            "名称": str((it or {}).get("f14", "")).strip(),
                            "所处行业": str((it or {}).get("f100", "")).strip(),
                            "最新价": _to_float((it or {}).get("f2")),
                            "涨跌幅": _to_float((it or {}).get("f3")),
                            "成交额": _to_float((it or {}).get("f6")),
                            "市盈率-动态": _to_float((it or {}).get("f9")),
                            "市净率": _to_float((it or {}).get("f23")),
                            "股息率": None,
                            "总市值": _to_float((it or {}).get("f20")),
                            "流通市值": _to_float((it or {}).get("f21")),
                            "换手率": _to_float((it or {}).get("f8")),
                            "量比": _to_float((it or {}).get("f10")),
                        }
                    )
                return pd.DataFrame(rows)
            except Exception as exc:
                last_exc = exc
                time.sleep(0.8)

    if last_exc is not None:
        raise last_exc
    raise RuntimeError("东方财富直连失败")


def refresh_market_snapshot(
    max_stocks: int = 0,
    enrich_top_n: int = 300,
    force_refresh: bool = False,
    rotate_enrich: bool = True,
) -> Dict[str, Any]:
    init_db()
    try:
        df = _build_base_universe().copy()
    except Exception as exc:
        # 网络/代理异常时兜底：如果本地已有快照，不让更新动作直接失败
        existed = load_snapshot()
        if existed is not None and not existed.empty:
            now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            _snapshot_meta_set("last_refresh_error", str(exc))
            _snapshot_meta_set("last_refresh_error_at", now_text)
            _snapshot_meta_set("last_refresh_fallback", "1")
            return {
                "row_count": int(len(existed)),
                "enriched_count": 0,
                "updated_at": now_text,
                "fallback": True,
                "error": str(exc),
                "enrich_mode": "rotate" if rotate_enrich else "top",
                "enrich_start": 0,
                "enrich_end": 0,
            }
        raise RuntimeError(
            f"未能拉取市场快照（可能是代理/VPN导致连接被拒绝）：{exc}"
        ) from exc
    df = df[df["code"].str.fullmatch(r"\d{6}", na=False)].copy()
    df = df.sort_values(by=["total_mv", "code"], ascending=[False, True], na_position="last").reset_index(drop=True)

    if max_stocks and max_stocks > 0:
        df = df.head(int(max_stocks)).copy()

    manual_flags = _load_manual_flags()

    enrich_n = max(0, min(int(enrich_top_n), len(df)))
    total_count = len(df)
    start_idx = 0
    target_indices: List[int] = []

    if enrich_n > 0 and total_count > 0:
        if rotate_enrich:
            meta = get_snapshot_meta()
            try:
                start_idx = int(meta.get("enrich_cursor_index", "0"))
            except Exception:
                start_idx = 0
            start_idx = start_idx % total_count
            target_indices = [int((start_idx + step) % total_count) for step in range(enrich_n)]
        else:
            target_indices = list(range(enrich_n))

    for idx in target_indices:
        code = str(df.at[idx, "code"])
        name = str(df.at[idx, "name"])
        try:
            ext = _enrich_one(code, name, force_refresh=force_refresh)
            for k, v in ext.items():
                if k in df.columns and v is not None:
                    df.at[idx, k] = v
        except Exception:
            pass

        if idx > 0 and idx % 40 == 0:
            time.sleep(0.05)

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

    cols = [
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
        "updated_at",
        "source_note",
    ]
    save_df = df[cols].copy()

    with _connect() as conn:
        save_df.to_sql("market_snapshot", conn, if_exists="replace", index=False)
        conn.commit()

    _snapshot_meta_set("last_update", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    _snapshot_meta_set("row_count", str(len(save_df)))
    _snapshot_meta_set("enriched_count", str(enrich_n))
    _snapshot_meta_set("app_version", APP_VERSION)
    _snapshot_meta_set("last_refresh_fallback", "0")
    _snapshot_meta_set("last_refresh_error", "")
    _snapshot_meta_set("last_refresh_error_at", "")
    if rotate_enrich and enrich_n > 0 and total_count > 0:
        _snapshot_meta_set("enrich_cursor_index", str((start_idx + enrich_n) % total_count))

    return {
        "row_count": len(save_df),
        "enriched_count": enrich_n,
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "enrich_mode": "rotate" if rotate_enrich else "top",
        "enrich_start": int(start_idx + 1) if enrich_n > 0 else 0,
        "enrich_end": int(((start_idx + enrich_n - 1) % total_count) + 1) if enrich_n > 0 and total_count > 0 else 0,
    }


def load_snapshot() -> pd.DataFrame:
    init_db()
    with _connect() as conn:
        try:
            df = pd.read_sql_query("SELECT * FROM market_snapshot ORDER BY total_mv DESC, code ASC", conn)
        except Exception:
            return pd.DataFrame()
    return df


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
