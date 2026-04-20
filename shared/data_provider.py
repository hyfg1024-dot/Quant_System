from __future__ import annotations

import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence

import pandas as pd

try:  # pragma: no cover - 运行环境可能缺少 akshare
    import akshare as ak
except Exception:  # pragma: no cover
    ak = None  # type: ignore[assignment]


QuoteFetcher = Callable[[str], Dict[str, Any]]
IntradayFetcher = Callable[[str], pd.DataFrame]
KlineFetcher = Callable[[str, int], pd.DataFrame]


class DataProviderError(RuntimeError):
    """统一数据提供层异常。"""


class DataProviderUnavailableError(DataProviderError):
    """数据源不可用（如模块缺失、未登录）。"""


class DataProviderTimeoutError(DataProviderError):
    """数据源调用超时。"""


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if text in {"", "-", "--", "None", "nan", "NaN"}:
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _normalize_symbol(symbol: str) -> str:
    raw = str(symbol).strip().lower()
    digits = "".join(ch for ch in raw if ch.isdigit())
    if raw.startswith("hk"):
        return digits[-5:].zfill(5) if digits else ""
    if len(digits) == 5:
        return digits
    if len(digits) >= 6:
        return digits[-6:]
    return str(symbol).strip()


def _is_hk_symbol(symbol: str) -> bool:
    s = _normalize_symbol(symbol)
    return s.isdigit() and len(s) == 5


def _resolve_exchange(symbol: str) -> str:
    normalized = _normalize_symbol(symbol)
    if normalized.isdigit() and len(normalized) == 5:
        return "hk"
    if normalized.isdigit() and len(normalized) == 6:
        return "sh" if normalized.startswith(("5", "6", "9")) else "sz"
    raw = str(symbol).strip().lower()
    if raw.startswith("hk"):
        return "hk"
    return "sh" if raw.startswith("sh") else "sz"


def _empty_order_book(levels: int = 10) -> Dict[str, List[Dict[str, Optional[float]]]]:
    return {
        "buy": [{"level": i + 1, "price": None, "volume_lot": None} for i in range(levels)],
        "sell": [{"level": i + 1, "price": None, "volume_lot": None} for i in range(levels)],
    }


def _parse_datetime_value(value: Any) -> pd.Timestamp:
    if value is None:
        return pd.NaT
    if isinstance(value, pd.Timestamp):
        return value
    if isinstance(value, datetime):
        return pd.Timestamp(value)

    text = str(value).strip()
    if not text or text in {"None", "nan", "NaN"}:
        return pd.NaT

    if text.isdigit():
        if len(text) == 8:
            return pd.to_datetime(text, format="%Y%m%d", errors="coerce")
        if len(text) == 12:
            return pd.to_datetime(text, format="%Y%m%d%H%M", errors="coerce")
        if len(text) == 14:
            return pd.to_datetime(text, format="%Y%m%d%H%M%S", errors="coerce")
        if len(text) == 17:
            text = text + "000"
            return pd.to_datetime(text, format="%Y%m%d%H%M%S%f", errors="coerce")

        num = int(text)
        if num > 1_000_000_000_000:
            return pd.to_datetime(num, unit="ms", errors="coerce")
        if num > 1_000_000_000:
            return pd.to_datetime(num, unit="s", errors="coerce")

    return pd.to_datetime(text, errors="coerce")


def _pick_column(df: pd.DataFrame, candidates: Sequence[str]) -> Optional[str]:
    cols = {str(col).lower(): str(col) for col in df.columns}
    for candidate in candidates:
        if candidate in df.columns:
            return candidate
        match = cols.get(candidate.lower())
        if match is not None:
            return match
    return None


class BaseDataProvider(ABC):
    """行情数据提供层抽象基类。"""

    name: str = "base"

    @abstractmethod
    def get_realtime_quote(self, symbol: str) -> Dict[str, Any]:
        """获取实时快照。"""

    @abstractmethod
    def get_intraday_flow(self, symbol: str) -> pd.DataFrame:
        """获取分时成交流。"""

    def get_daily_kline(self, symbol: str, count: int = 320) -> pd.DataFrame:
        raise DataProviderError(f"{self.name} does not implement daily kline")


class QMTDataProvider(BaseDataProvider):
    """QMT / MiniQMT 行情数据源（L1/L2 + K线）。"""

    name = "qmt"

    def __init__(self, timeout_sec: float = 2.5) -> None:
        self.timeout_sec = timeout_sec
        self._xtdata = None
        self._import_error: Optional[Exception] = None
        try:
            import xtquant.xtdata as _xtdata  # type: ignore

            self._xtdata = _xtdata
        except Exception as exc:  # pragma: no cover - 无 QMT 环境常见
            self._import_error = exc

    @property
    def available(self) -> bool:
        return self._xtdata is not None

    def _ensure_ready(self) -> None:
        if not self.available:
            raise DataProviderUnavailableError(f"QMT 不可用: {self._import_error}")

    def _to_qmt_symbol(self, symbol: str) -> str:
        s = str(symbol).strip().upper()
        if "." in s and s.split(".")[-1] in {"SH", "SZ", "HK"}:
            return s
        normalized = _normalize_symbol(s)
        if len(normalized) == 5:
            return f"{normalized}.HK"
        if len(normalized) == 6:
            suffix = "SH" if normalized.startswith(("5", "6", "9")) else "SZ"
            return f"{normalized}.{suffix}"
        return s

    def _call_guarded(self, action: str, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        started = time.monotonic()
        try:
            result = func(*args, **kwargs)
        except Exception as exc:
            msg = str(exc).lower()
            if isinstance(exc, TimeoutError) or "timeout" in msg or "timed out" in msg:
                raise DataProviderTimeoutError(f"{action} 超时: {exc}") from exc
            if "not login" in msg or "disconnected" in msg or "connection" in msg:
                raise DataProviderUnavailableError(f"{action} 不可用: {exc}") from exc
            raise DataProviderError(f"{action} 失败: {exc}") from exc

        elapsed = time.monotonic() - started
        if self.timeout_sec > 0 and elapsed > self.timeout_sec:
            raise DataProviderTimeoutError(
                f"{action} 超时: {elapsed:.2f}s > {self.timeout_sec:.2f}s"
            )
        return result

    def _subscribe_quote(self, qmt_symbol: str, period: str) -> None:
        xtdata = self._xtdata
        assert xtdata is not None
        subscribe = getattr(xtdata, "subscribe_quote", None)
        if subscribe is None:
            return
        try:
            subscribe(qmt_symbol, period=period)
        except TypeError:
            try:
                subscribe(stock_code=qmt_symbol, period=period)
            except Exception:
                pass
        except Exception:
            pass

    def _pick_symbol_payload(self, payload: Any, qmt_symbol: str) -> Any:
        if payload is None:
            return None
        if isinstance(payload, dict):
            symbol_keys = [
                qmt_symbol,
                qmt_symbol.upper(),
                qmt_symbol.lower(),
                qmt_symbol.replace(".", ""),
                qmt_symbol.replace(".", "").upper(),
                qmt_symbol.replace(".", "").lower(),
            ]
            for key in symbol_keys:
                if key in payload:
                    return payload.get(key)
            if len(payload) == 1:
                return next(iter(payload.values()))
            return payload
        return payload

    def _extract_level_array(
        self,
        tick: Dict[str, Any],
        list_keys: Sequence[str],
        base_keys: Sequence[str],
    ) -> List[Optional[float]]:
        for key in list_keys:
            value = tick.get(key)
            if isinstance(value, (list, tuple, pd.Series)):
                return [_to_float(v) for v in list(value)[:10]]

        values: List[Optional[float]] = []
        for level in range(1, 11):
            raw_value: Any = None
            for base in base_keys:
                candidates = [
                    f"{base}{level}",
                    f"{base}_{level}",
                    f"{base.lower()}{level}",
                    f"{base.lower()}_{level}",
                    f"{base.upper()}{level}",
                ]
                for candidate in candidates:
                    if candidate in tick:
                        raw_value = tick.get(candidate)
                        break
                if raw_value is not None:
                    break
            values.append(_to_float(raw_value))
        return values

    def _extract_lob(self, tick: Dict[str, Any], is_hk: bool) -> Dict[str, Any]:
        bid_prices = self._extract_level_array(
            tick,
            list_keys=["bidPrice", "bid_price", "bidprice", "bidPrices"],
            base_keys=["bidPrice", "bid_price", "bidprice"],
        )
        ask_prices = self._extract_level_array(
            tick,
            list_keys=["askPrice", "ask_price", "askprice", "askPrices"],
            base_keys=["askPrice", "ask_price", "askprice"],
        )
        bid_vols = self._extract_level_array(
            tick,
            list_keys=["bidVol", "bid_volume", "bidvol", "bidVolumes"],
            base_keys=["bidVol", "bid_volume", "bidvol"],
        )
        ask_vols = self._extract_level_array(
            tick,
            list_keys=["askVol", "ask_volume", "askvol", "askVolumes"],
            base_keys=["askVol", "ask_volume", "askvol"],
        )

        bid_prices = (bid_prices + [None] * 10)[:10]
        ask_prices = (ask_prices + [None] * 10)[:10]
        bid_vols = (bid_vols + [None] * 10)[:10]
        ask_vols = (ask_vols + [None] * 10)[:10]

        buy_10: List[Dict[str, Optional[float]]] = []
        sell_10: List[Dict[str, Optional[float]]] = []
        for i in range(10):
            bv = bid_vols[i]
            av = ask_vols[i]
            if not is_hk:
                bv = (bv / 100.0) if bv is not None else None
                av = (av / 100.0) if av is not None else None
            buy_10.append({"level": i + 1, "price": bid_prices[i], "volume_lot": bv})
            sell_10.append({"level": i + 1, "price": ask_prices[i], "volume_lot": av})

        depth_points = sum(
            1
            for row in (buy_10 + sell_10)
            if row["price"] is not None and row["volume_lot"] is not None
        )
        quote_level = "L2" if depth_points >= 8 else "L1"
        return {
            "quote_level": quote_level,
            "order_book_5": {"buy": buy_10[:5], "sell": sell_10[:5]},
            "order_book_10": {"buy": buy_10, "sell": sell_10},
        }

    def _to_dataframe(self, obj: Any) -> pd.DataFrame:
        if isinstance(obj, pd.DataFrame):
            return obj.copy()
        if isinstance(obj, list):
            return pd.DataFrame(obj)
        if isinstance(obj, dict):
            try:
                return pd.DataFrame(obj)
            except ValueError:
                return pd.DataFrame([obj])
        raise DataProviderError("QMT 返回格式无法解析为 DataFrame")

    def _fetch_tick_snapshot(self, qmt_symbol: str) -> Dict[str, Any]:
        xtdata = self._xtdata
        assert xtdata is not None

        self._subscribe_quote(qmt_symbol, "tick")
        self._subscribe_quote(qmt_symbol, "l2quote")

        tick_data: Dict[str, Any] = {}
        get_full_tick = getattr(xtdata, "get_full_tick", None)
        if callable(get_full_tick):
            payload = self._call_guarded("QMT tick 快照", get_full_tick, [qmt_symbol]) or {}
            obj = self._pick_symbol_payload(payload, qmt_symbol)
            if isinstance(obj, dict):
                tick_data = obj

        if tick_data:
            return tick_data

        get_market_data_ex = getattr(xtdata, "get_market_data_ex", None)
        if callable(get_market_data_ex):
            payload = self._call_guarded(
                "QMT tick 回退快照",
                get_market_data_ex,
                field_list=["time", "lastPrice", "open", "high", "low", "volume", "amount"],
                stock_list=[qmt_symbol],
                period="tick",
                count=1,
            )
            obj = self._pick_symbol_payload(payload, qmt_symbol)
            if obj is not None:
                df = self._to_dataframe(obj)
                if not df.empty:
                    row = df.iloc[-1].to_dict()
                    tick_data = {str(k): v for k, v in row.items()}

        if not tick_data:
            raise DataProviderError("QMT tick 快照为空")
        return tick_data

    def _format_quote_time(self, raw_value: Any) -> Optional[str]:
        ts = _parse_datetime_value(raw_value)
        if pd.isna(ts):
            return None
        return ts.strftime("%Y-%m-%d %H:%M:%S")

    def _parse_market_data(
        self,
        payload: Any,
        qmt_symbol: str,
        period: str,
        is_hk: bool,
    ) -> pd.DataFrame:
        obj = self._pick_symbol_payload(payload, qmt_symbol)
        if obj is None:
            raise DataProviderError(f"QMT {period} 数据未命中标的")

        df = self._to_dataframe(obj)
        if df.empty:
            raise DataProviderError(f"QMT {period} 数据为空")

        time_col = _pick_column(df, ["time", "datetime", "trade_time", "date"])
        close_col = _pick_column(df, ["close", "lastPrice", "price", "last"])
        open_col = _pick_column(df, ["open"])
        high_col = _pick_column(df, ["high"])
        low_col = _pick_column(df, ["low"])
        volume_col = _pick_column(df, ["volume", "vol"])
        amount_col = _pick_column(df, ["amount", "turnover"])

        if close_col is None:
            raise DataProviderError(f"QMT {period} 缺少 close/price 字段")

        time_source = df[time_col] if time_col is not None else pd.Series(df.index)
        out = pd.DataFrame()
        out["time"] = time_source.apply(_parse_datetime_value)
        out["open"] = pd.to_numeric(df[open_col], errors="coerce") if open_col else pd.NA
        out["high"] = pd.to_numeric(df[high_col], errors="coerce") if high_col else pd.NA
        out["low"] = pd.to_numeric(df[low_col], errors="coerce") if low_col else pd.NA
        out["close"] = pd.to_numeric(df[close_col], errors="coerce")
        out["volume"] = pd.to_numeric(df[volume_col], errors="coerce") if volume_col else pd.NA
        out["amount"] = pd.to_numeric(df[amount_col], errors="coerce") if amount_col else pd.NA
        out = out.dropna(subset=["time", "close"]).reset_index(drop=True)

        if out.empty:
            raise DataProviderError(f"QMT {period} 解析后为空")

        if not is_hk and "volume" in out.columns:
            out["volume_lot"] = pd.to_numeric(out["volume"], errors="coerce") / 100.0
        else:
            out["volume_lot"] = pd.to_numeric(out["volume"], errors="coerce")

        return out

    def get_realtime_quote(self, symbol: str) -> Dict[str, Any]:
        self._ensure_ready()
        qmt_symbol = self._to_qmt_symbol(symbol)
        normalized = _normalize_symbol(symbol)
        is_hk = _is_hk_symbol(normalized)

        tick_data = self._fetch_tick_snapshot(qmt_symbol)
        current_price = _to_float(tick_data.get("lastPrice") or tick_data.get("last") or tick_data.get("price"))
        prev_close = _to_float(
            tick_data.get("lastClose")
            or tick_data.get("preClose")
            or tick_data.get("pre_close")
            or tick_data.get("prevClose")
        )
        open_price = _to_float(tick_data.get("open"))
        high_price = _to_float(tick_data.get("high"))
        low_price = _to_float(tick_data.get("low"))

        volume = _to_float(tick_data.get("volume") or tick_data.get("vol"))
        amount = _to_float(tick_data.get("amount") or tick_data.get("turnover"))
        vwap = _to_float(tick_data.get("avgPrice") or tick_data.get("vwap"))
        if vwap is None and amount is not None and volume is not None and volume > 0:
            vwap = amount / volume

        change_amount = _to_float(tick_data.get("change"))
        if change_amount is None and current_price is not None and prev_close is not None:
            change_amount = current_price - prev_close
        change_pct = _to_float(tick_data.get("changePercent"))
        if change_pct is None and change_amount is not None and prev_close not in {None, 0}:
            change_pct = change_amount / prev_close * 100.0

        premium_pct = None
        if current_price is not None and vwap is not None and vwap > 0:
            premium_pct = (current_price - vwap) / vwap * 100.0

        lob = self._extract_lob(tick_data, is_hk=is_hk)
        if not lob.get("order_book_10"):
            empty = _empty_order_book(10)
            lob = {
                "quote_level": "L1",
                "order_book_5": {"buy": empty["buy"][:5], "sell": empty["sell"][:5]},
                "order_book_10": empty,
            }

        return {
            "symbol": normalized or symbol,
            "name": str(tick_data.get("stockName") or tick_data.get("name") or (normalized or symbol)),
            "current_price": current_price,
            "prev_close": prev_close,
            "open": open_price,
            "change_amount": change_amount,
            "change_pct": change_pct,
            "high": high_price,
            "low": low_price,
            "volume": volume,
            "amount": amount,
            "turnover_rate": _to_float(tick_data.get("turnoverRate")),
            "turnover_rate_estimated": False,
            "amplitude_pct": _to_float(tick_data.get("amplitude")),
            "float_market_value_yi": _to_float(tick_data.get("floatMarketValue")),
            "total_market_value_yi": _to_float(tick_data.get("totalMarketValue")),
            "volume_ratio": _to_float(tick_data.get("volumeRatio")),
            "order_diff": _to_float(tick_data.get("entrustDiff")),
            "vwap": vwap,
            "premium_pct": premium_pct,
            "quote_time": self._format_quote_time(tick_data.get("time") or tick_data.get("updateTime")),
            "quote_level": lob.get("quote_level", "L1"),
            "is_trading_data": bool(volume and volume > 0),
            "pe_dynamic": _to_float(tick_data.get("pe") or tick_data.get("peDynamic")),
            "pe_ttm": _to_float(tick_data.get("peTtm") or tick_data.get("peTTM")),
            "pb": _to_float(tick_data.get("pb")),
            "order_book_5": lob["order_book_5"],
            "order_book_10": lob["order_book_10"],
            "error": None,
        }

    def get_intraday_flow(self, symbol: str) -> pd.DataFrame:
        self._ensure_ready()
        xtdata = self._xtdata
        assert xtdata is not None

        qmt_symbol = self._to_qmt_symbol(symbol)
        is_hk = _is_hk_symbol(symbol)
        self._subscribe_quote(qmt_symbol, "1m")

        get_market_data_ex = getattr(xtdata, "get_market_data_ex", None)
        if not callable(get_market_data_ex):
            raise DataProviderUnavailableError("QMT 缺少 get_market_data_ex 接口")

        payload = self._call_guarded(
            "QMT 1m 分时",
            get_market_data_ex,
            field_list=["time", "open", "high", "low", "close", "volume", "amount"],
            stock_list=[qmt_symbol],
            period="1m",
            count=-1,
        )
        out = self._parse_market_data(payload, qmt_symbol=qmt_symbol, period="1m", is_hk=is_hk)
        return out[["time", "close", "volume_lot", "amount"]].rename(columns={"close": "price"})

    def get_daily_kline(self, symbol: str, count: int = 320) -> pd.DataFrame:
        self._ensure_ready()
        xtdata = self._xtdata
        assert xtdata is not None

        qmt_symbol = self._to_qmt_symbol(symbol)
        is_hk = _is_hk_symbol(symbol)
        self._subscribe_quote(qmt_symbol, "1d")

        get_market_data_ex = getattr(xtdata, "get_market_data_ex", None)
        if not callable(get_market_data_ex):
            raise DataProviderUnavailableError("QMT 缺少 get_market_data_ex 接口")

        payload = self._call_guarded(
            "QMT 1d K线",
            get_market_data_ex,
            field_list=["time", "open", "high", "low", "close", "volume", "amount"],
            stock_list=[qmt_symbol],
            period="1d",
            count=max(1, int(count)),
        )
        out = self._parse_market_data(payload, qmt_symbol=qmt_symbol, period="1d", is_hk=is_hk)
        out = out.rename(columns={"time": "date"})
        out["date"] = pd.to_datetime(out["date"], errors="coerce")
        out = out.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)
        return out[["date", "open", "high", "low", "close", "volume", "amount"]]


class AkshareDataProvider(BaseDataProvider):
    """现有免费抓取通道（腾讯/akshare）封装。"""

    name = "akshare"

    def __init__(
        self,
        quote_fetcher: Optional[QuoteFetcher] = None,
        intraday_fetcher: Optional[IntradayFetcher] = None,
        kline_fetcher: Optional[KlineFetcher] = None,
    ) -> None:
        self._quote_fetcher = quote_fetcher
        self._intraday_fetcher = intraday_fetcher
        self._kline_fetcher = kline_fetcher

    def get_realtime_quote(self, symbol: str) -> Dict[str, Any]:
        if self._quote_fetcher is not None:
            return self._quote_fetcher(symbol)
        raise DataProviderError("AkshareDataProvider 未配置 quote_fetcher")

    def get_intraday_flow(self, symbol: str) -> pd.DataFrame:
        if self._intraday_fetcher is not None:
            return self._intraday_fetcher(symbol)

        if ak is None:
            raise DataProviderUnavailableError("akshare 不可用")
        normalized = _normalize_symbol(symbol)
        if not normalized or _is_hk_symbol(normalized):
            raise DataProviderError("akshare intraday fallback 暂不支持港股")
        try:
            df = ak.stock_intraday_em(symbol=normalized)
            if df is None or df.empty:
                raise DataProviderError("akshare intraday 返回空")

            out = pd.DataFrame()
            out["time"] = pd.to_datetime(df.get("时间"), errors="coerce")
            out["price"] = pd.to_numeric(df.get("成交价"), errors="coerce")
            out["volume_lot"] = pd.to_numeric(df.get("手数"), errors="coerce")
            out["amount"] = pd.to_numeric(df.get("成交额"), errors="coerce")
            out = out.dropna(subset=["time", "price"]).reset_index(drop=True)
            if out.empty:
                raise DataProviderError("akshare intraday 解析为空")
            return out[["time", "price", "volume_lot", "amount"]]
        except Exception as exc:
            raise DataProviderError(f"akshare intraday 获取失败: {exc}") from exc

    def get_daily_kline(self, symbol: str, count: int = 320) -> pd.DataFrame:
        if self._kline_fetcher is not None:
            return self._kline_fetcher(symbol, count)

        if ak is None:
            raise DataProviderUnavailableError("akshare 不可用")

        normalized = _normalize_symbol(symbol)
        if not normalized:
            raise DataProviderError("symbol 不能为空")

        try:
            if _is_hk_symbol(normalized):
                df = ak.stock_hk_daily(symbol=normalized.zfill(5), adjust="qfq")
                if df is None or df.empty:
                    raise DataProviderError("akshare hk daily 返回空")
                out = df.copy()
                out = out.reset_index().rename(columns={"index": "date"})
                out["date"] = pd.to_datetime(out["date"], errors="coerce")
                out["open"] = pd.to_numeric(out.get("open"), errors="coerce")
                out["high"] = pd.to_numeric(out.get("high"), errors="coerce")
                out["low"] = pd.to_numeric(out.get("low"), errors="coerce")
                out["close"] = pd.to_numeric(out.get("close"), errors="coerce")
                out["volume"] = pd.to_numeric(out.get("volume"), errors="coerce")
                out["amount"] = pd.to_numeric(out.get("amount"), errors="coerce")
                out = out.dropna(subset=["date", "close"]).sort_values("date")
                return out[["date", "open", "high", "low", "close", "volume", "amount"]].tail(count).reset_index(drop=True)

            df = ak.stock_zh_a_hist(symbol=normalized, period="daily", adjust="qfq")
            if df is None or df.empty:
                raise DataProviderError("akshare a-share daily 返回空")
            out = pd.DataFrame()
            out["date"] = pd.to_datetime(df.get("日期"), errors="coerce")
            out["open"] = pd.to_numeric(df.get("开盘"), errors="coerce")
            out["high"] = pd.to_numeric(df.get("最高"), errors="coerce")
            out["low"] = pd.to_numeric(df.get("最低"), errors="coerce")
            out["close"] = pd.to_numeric(df.get("收盘"), errors="coerce")
            out["volume"] = pd.to_numeric(df.get("成交量"), errors="coerce")
            out["amount"] = pd.to_numeric(df.get("成交额"), errors="coerce")
            out = out.dropna(subset=["date", "close"]).sort_values("date")
            return out[["date", "open", "high", "low", "close", "volume", "amount"]].tail(count).reset_index(drop=True)
        except Exception as exc:
            raise DataProviderError(f"akshare daily kline 获取失败: {exc}") from exc


class FallbackDataProvider(BaseDataProvider):
    """瀑布流降级代理：按顺序尝试各 provider。"""

    name = "fallback"

    def __init__(self, providers: List[BaseDataProvider]) -> None:
        self.providers = providers

    def _run(self, method_name: str, *args: Any, **kwargs: Any) -> Any:
        errors: List[str] = []
        for provider in self.providers:
            try:
                method = getattr(provider, method_name)
                return method(*args, **kwargs)
            except Exception as exc:
                errors.append(f"{provider.name}: {exc}")
        raise DataProviderError(" | ".join(errors) if errors else "no provider configured")

    def get_realtime_quote(self, symbol: str) -> Dict[str, Any]:
        return self._run("get_realtime_quote", symbol)

    def get_intraday_flow(self, symbol: str) -> pd.DataFrame:
        return self._run("get_intraday_flow", symbol)

    def get_daily_kline(self, symbol: str, count: int = 320) -> pd.DataFrame:
        return self._run("get_daily_kline", symbol, count=count)


def build_default_provider(
    quote_fetcher: Optional[QuoteFetcher] = None,
    intraday_fetcher: Optional[IntradayFetcher] = None,
    kline_fetcher: Optional[KlineFetcher] = None,
    prefer_qmt: bool = True,
    qmt_timeout_sec: float = 2.5,
) -> FallbackDataProvider:
    """构建默认数据代理：QMT 优先，失败回退 Akshare/现有抓取。"""
    providers: List[BaseDataProvider] = []
    if prefer_qmt:
        providers.append(QMTDataProvider(timeout_sec=qmt_timeout_sec))
    providers.append(
        AkshareDataProvider(
            quote_fetcher=quote_fetcher,
            intraday_fetcher=intraday_fetcher,
            kline_fetcher=kline_fetcher,
        )
    )
    return FallbackDataProvider(providers)

