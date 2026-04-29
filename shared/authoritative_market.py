from __future__ import annotations

from datetime import datetime
import time
from typing import Any, Dict, Optional

import pandas as pd
import requests

try:  # pragma: no cover - 部分运行环境可能没有 akshare
    import akshare as ak
except Exception:  # pragma: no cover
    ak = None  # type: ignore[assignment]


EASTMONEY_QUOTE_URLS = (
    "https://2.push2.eastmoney.com/api/qt/stock/get",
    "https://push2.eastmoney.com/api/qt/stock/get",
    "https://push2his.eastmoney.com/api/qt/stock/get",
    "http://push2.eastmoney.com/api/qt/stock/get",
    "http://push2his.eastmoney.com/api/qt/stock/get",
)
EASTMONEY_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://quote.eastmoney.com/",
}
_A_SPOT_CACHE: Dict[str, Any] = {"ts": 0.0, "df": None}
_A_SPOT_TTL_SEC = 60.0


def to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            if pd.isna(value):
                return None
        except Exception:
            pass
        return float(value)

    text = str(value).strip().replace(",", "").replace("+", "")
    if text in {"", "-", "--", "None", "nan", "NaN", "False"}:
        return None
    negative = text.startswith("-")
    text = text.replace("亿元", "亿").replace("万元", "万")

    multiplier = 1.0
    if text.endswith("%"):
        text = text[:-1]
    if text.endswith("万亿"):
        multiplier = 1e12
        text = text[:-2]
    elif text.endswith("亿"):
        multiplier = 1e8
        text = text[:-1]
    elif text.endswith("万"):
        multiplier = 1e4
        text = text[:-1]

    try:
        num = float(text)
    except (TypeError, ValueError):
        return None
    if negative:
        num = -abs(num)
    return num * multiplier


def normalize_symbol(symbol: str) -> str:
    raw = str(symbol).strip().lower()
    digits = "".join(ch for ch in raw if ch.isdigit())
    if raw.startswith("hk") or raw.endswith(".hk"):
        return digits[-5:].zfill(5) if digits else ""
    if len(digits) == 5:
        return digits
    if len(digits) >= 6:
        return digits[-6:]
    return str(symbol).strip()


def is_hk_symbol(symbol: str) -> bool:
    s = normalize_symbol(symbol)
    return s.isdigit() and len(s) == 5


def eastmoney_secid(symbol: str) -> str:
    normalized = normalize_symbol(symbol)
    if is_hk_symbol(normalized):
        return f"116.{normalized}"
    if normalized.startswith(("5", "6", "9")):
        return f"1.{normalized}"
    return f"0.{normalized}"


def normalize_pe(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    v = float(value)
    if abs(v) < 1e-12 or abs(v) > 1e6:
        return None
    return v


def normalize_pb(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    v = float(value)
    if v <= 0 or v > 1e5:
        return None
    return v


def normalize_percent(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    v = float(value)
    if v < 0:
        return None
    if 0 < v < 0.2:
        v *= 100.0
    return v


def _request_eastmoney(symbol: str, fields: str, timeout: float = 4.0, fltt: Optional[str] = "2") -> Dict[str, Any]:
    params: Dict[str, Any] = {
        "invt": "2",
        "secid": eastmoney_secid(symbol),
        "fields": fields,
    }
    if fltt is not None:
        params["fltt"] = fltt

    last_error: Optional[Exception] = None
    for url in EASTMONEY_QUOTE_URLS:
        for trust_env in (False, True):
            try:
                session = requests.Session()
                session.trust_env = trust_env
                resp = session.get(url, params=params, headers=EASTMONEY_HEADERS, timeout=timeout)
                resp.raise_for_status()
                payload = resp.json() or {}
                data = payload.get("data") or {}
                if isinstance(data, dict) and data:
                    return data
            except Exception as exc:
                last_error = exc
                continue
    if last_error is not None:
        raise last_error
    raise RuntimeError("empty Eastmoney response")


def fetch_eastmoney_price(symbol: str, timeout: float = 3.0) -> Optional[float]:
    normalized = normalize_symbol(symbol)
    if not normalized:
        return None
    try:
        data = _request_eastmoney(normalized, "f43", timeout=timeout, fltt="2")
    except Exception:
        return None
    raw = to_float(data.get("f43"))
    if raw is None or raw <= 0:
        return None
    if raw < 1000:
        return raw
    scale = 1000.0 if is_hk_symbol(normalized) else 100.0
    return raw / scale


def fetch_eastmoney_valuation(symbol: str, timeout: float = 4.0) -> Dict[str, Optional[float]]:
    normalized = normalize_symbol(symbol)
    out: Dict[str, Optional[float]] = {
        "code": normalized,
        "current_price": None,
        "pe_dynamic": None,
        "pe_static": None,
        "pe_rolling": None,
        "pe_ttm": None,
        "pb": None,
        "dividend_yield": None,
    }
    if not normalized:
        return out

    try:
        data = _request_eastmoney(normalized, "f43,f57,f58,f126,f162,f163,f164,f167", timeout=timeout, fltt="2")
    except Exception:
        return out

    price = to_float(data.get("f43"))
    if price is not None and price > 0:
        # fltt=2 通常直接返回真实价格；若异常返回放大整数，再回退缩放。
        out["current_price"] = price if price < 1000 else price / (1000.0 if is_hk_symbol(normalized) else 100.0)
    out["pe_dynamic"] = normalize_pe(to_float(data.get("f162")))
    out["pe_static"] = normalize_pe(to_float(data.get("f163")))
    pe_rolling = normalize_pe(to_float(data.get("f164")))
    out["pe_rolling"] = pe_rolling
    out["pe_ttm"] = pe_rolling
    out["pb"] = normalize_pb(to_float(data.get("f167")))
    if not is_hk_symbol(normalized):
        out["dividend_yield"] = normalize_percent(to_float(data.get("f126")))
    return out


def _get_a_spot_em_cached() -> Optional[pd.DataFrame]:
    if ak is None:
        return None
    now = time.monotonic()
    cached_df = _A_SPOT_CACHE.get("df")
    if cached_df is not None and now - float(_A_SPOT_CACHE.get("ts") or 0.0) <= _A_SPOT_TTL_SEC:
        return cached_df
    try:
        df = ak.stock_zh_a_spot_em()
    except Exception:
        return None
    if df is None or df.empty:
        return None
    _A_SPOT_CACHE["df"] = df
    _A_SPOT_CACHE["ts"] = now
    return df


def fetch_eastmoney_a_spot_valuation(symbol: str) -> Dict[str, Optional[float]]:
    normalized = normalize_symbol(symbol)
    out: Dict[str, Optional[float]] = {
        "pe_dynamic": None,
        "pb": None,
        "dividend_yield": None,
    }
    if not normalized or len(normalized) != 6:
        return out

    df = _get_a_spot_em_cached()
    if df is None or df.empty or "代码" not in df.columns:
        return out
    try:
        tmp = df.copy()
        tmp["代码"] = tmp["代码"].astype(str).str.strip()
        row_df = tmp[tmp["代码"] == normalized]
        if row_df.empty:
            return out
        row = row_df.iloc[0]
        out["pe_dynamic"] = normalize_pe(
            to_float(row.get("市盈率-动态") or row.get("市盈率动态") or row.get("市盈率"))
        )
        out["pb"] = normalize_pb(to_float(row.get("市净率")))
        out["dividend_yield"] = normalize_percent(to_float(row.get("股息率") or row.get("股息率(%)")))
    except Exception:
        return {"pe_dynamic": None, "pb": None, "dividend_yield": None}
    return out


def fetch_a_dividend_yield_ttm(symbol: str, price: Optional[float] = None) -> Optional[float]:
    """A股股息率兜底口径：最新财年现金分红合计 / 当前价。

    交易页/基本面页优先使用东方财富现货快照里的“股息率”字段。只有现货快照
    拿不到时才走这里，避免因为只取最新一条分红记录而低估中国神华这类高分红股。
    """
    normalized = normalize_symbol(symbol)
    if not normalized or len(normalized) != 6 or ak is None:
        return None
    try:
        df = ak.stock_fhps_detail_em(symbol=normalized)
    except Exception:
        return None
    if df is None or df.empty or "报告期" not in df.columns:
        return None

    tmp = df.copy()
    tmp["报告期_dt"] = pd.to_datetime(tmp["报告期"], errors="coerce")
    tmp = tmp.dropna(subset=["报告期_dt"])
    if tmp.empty:
        return None

    latest_year = int(tmp["报告期_dt"].dt.year.max())
    chosen = tmp[tmp["报告期_dt"].dt.year == latest_year].copy()
    if chosen.empty:
        return None

    cash_col = "现金分红-现金分红比例"
    if cash_col in chosen.columns:
        cash_per_10 = pd.to_numeric(chosen[cash_col], errors="coerce").dropna()
        cash_per_10_sum = float(cash_per_10[cash_per_10 > 0].sum()) if not cash_per_10.empty else 0.0
        current_price = price if price is not None and price > 0 else fetch_eastmoney_price(normalized)
        if cash_per_10_sum > 0 and current_price is not None and current_price > 0:
            return round(cash_per_10_sum / 10.0 / current_price * 100.0, 6)

    yield_col = "现金分红-股息率"
    if yield_col in chosen.columns:
        yield_values = pd.to_numeric(chosen[yield_col], errors="coerce").dropna()
        vals = [normalize_percent(float(v)) for v in yield_values if float(v) > 0]
        vals = [float(v) for v in vals if v is not None]
        if vals:
            return round(sum(vals), 6)
    return None


def fetch_authoritative_valuation(
    symbol: str,
    include_dividend: bool = True,
    use_spot_fallback: bool = False,
) -> Dict[str, Any]:
    normalized = normalize_symbol(symbol)
    metrics = fetch_eastmoney_valuation(normalized)
    spot_metrics: Dict[str, Optional[float]] = {}
    field_sources: Dict[str, str] = {}
    for key in ("current_price", "pe_dynamic", "pe_static", "pe_rolling", "pe_ttm", "pb", "dividend_yield"):
        if metrics.get(key) is not None:
            field_sources[key] = "eastmoney_push2"

    needs_spot = (
        use_spot_fallback
        and normalized
        and not is_hk_symbol(normalized)
        and (
            metrics.get("pe_dynamic") is None
            or metrics.get("pb") is None
            or (include_dividend and metrics.get("dividend_yield") is None)
        )
    )
    if needs_spot:
        spot_metrics = fetch_eastmoney_a_spot_valuation(normalized)

    if metrics.get("pe_dynamic") is None:
        metrics["pe_dynamic"] = spot_metrics.get("pe_dynamic")
        if metrics["pe_dynamic"] is not None:
            field_sources["pe_dynamic"] = "eastmoney_spot"
    if metrics.get("pb") is None:
        metrics["pb"] = spot_metrics.get("pb")
        if metrics["pb"] is not None:
            field_sources["pb"] = "eastmoney_spot"

    if include_dividend and normalized and not is_hk_symbol(normalized):
        if metrics.get("dividend_yield") is None:
            metrics["dividend_yield"] = spot_metrics.get("dividend_yield")
        if metrics["dividend_yield"] is not None:
            field_sources.setdefault("dividend_yield", "eastmoney_spot")
        if metrics["dividend_yield"] is None:
            metrics["dividend_yield"] = fetch_a_dividend_yield_ttm(
                normalized,
                price=metrics.get("current_price"),
            )
            if metrics["dividend_yield"] is not None:
                field_sources["dividend_yield"] = "eastmoney_cash_ttm"
    else:
        metrics["dividend_yield"] = None

    authoritative_fields = [
        metrics.get("current_price"),
        metrics.get("pe_dynamic"),
        metrics.get("pe_static"),
        metrics.get("pe_rolling"),
        metrics.get("pb"),
        metrics.get("dividend_yield"),
    ]
    metrics["source"] = "eastmoney"
    metrics["source_status"] = "ok" if any(v is not None for v in authoritative_fields) else "unavailable"
    metrics["field_sources"] = field_sources
    metrics["checked_at"] = datetime.now().isoformat(timespec="seconds")
    return metrics
