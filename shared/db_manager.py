from __future__ import annotations

import argparse
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

try:  # pragma: no cover
    import duckdb
except Exception as exc:  # pragma: no cover
    duckdb = None  # type: ignore[assignment]
    _DUCKDB_IMPORT_ERROR: Optional[Exception] = exc
else:  # pragma: no cover
    _DUCKDB_IMPORT_ERROR = None

DISPLAY_COLUMNS: List[str] = [
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


INDUSTRY_KEYWORD_ALIAS_MAP: Dict[str, List[str]] = {
    "房地产": ["地产", "物业", "reits", "reit"],
    "地产": ["房地产", "物业", "reits", "reit"],
    "物业": ["房地产", "地产", "reits", "reit"],
    "reits": ["reit", "房地产", "地产", "物业"],
    "reit": ["reits", "房地产", "地产", "物业"],
}


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "quant_system.duckdb"


@dataclass
class Position:
    code: str
    avg_cost: float
    quantity: int
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    open_date: date | datetime | str = field(default_factory=date.today)
    market: str = "A"
    name: str = ""


def _ensure_duckdb() -> None:
    if duckdb is None:
        raise RuntimeError(f"duckdb 不可用: {_DUCKDB_IMPORT_ERROR}")


def _safe_str(v: Any) -> str:
    return str(v).strip() if v is not None else ""


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
    if text in {"", "-", "--", "None", "nan", "NaN"}:
        return None
    try:
        return float(text)
    except Exception:
        return None


def _to_int(v: Any) -> Optional[int]:
    f = _to_float(v)
    if f is None:
        return None
    try:
        return int(f)
    except Exception:
        return None


def _split_keywords(text: str) -> List[str]:
    parts = re.split(r"[,，;；\n]+", str(text or ""))
    return [p.strip() for p in parts if p.strip()]


def _expand_industry_keywords(kws: Sequence[str]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()

    def _add(one: str) -> None:
        key = _safe_str(one).lower()
        if not key or key in seen:
            return
        seen.add(key)
        out.append(_safe_str(one))

    for one in kws:
        raw = _safe_str(one)
        _add(raw)
        for alias in INDUSTRY_KEYWORD_ALIAS_MAP.get(raw.lower(), []):
            _add(alias)
    return out


def _normalize_market(value: Any) -> str:
    txt = _safe_str(value).upper()
    return "HK" if txt == "HK" else "A"


def _normalize_code(market: str, value: Any) -> str:
    digits = re.sub(r"\D+", "", _safe_str(value))
    if market == "HK":
        return digits[-5:].zfill(5) if digits else ""
    return digits[-6:].zfill(6) if digits else ""


def _normalize_trade_date(v: Any) -> date:
    if isinstance(v, date):
        return v
    ts = pd.to_datetime(v, errors="coerce")
    if pd.isna(ts):
        return date.today()
    return ts.date()


def _load_akshare():
    try:  # pragma: no cover
        import akshare as _ak  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(f"akshare 不可用: {exc}") from exc
    return _ak


def get_connection(db_path: Path | str = DEFAULT_DB_PATH, read_only: bool = False):
    _ensure_duckdb()
    p = Path(db_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    try:
        return duckdb.connect(str(p), read_only=read_only)
    except Exception as exc:
        # DuckDB 不允许同一数据库文件在同一进程里混用只读/读写连接。
        # Streamlit 页面会并发复用多个查询入口，这里自动降级到普通连接，
        # 避免 portfolio/filter 等页面因为 read_only 标志差异直接崩掉。
        msg = str(exc)
        if read_only and "same database file with a different configuration" in msg:
            return duckdb.connect(str(p), read_only=False)
        raise


def init_db(db_path: Path | str = DEFAULT_DB_PATH) -> None:
    with get_connection(db_path=db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS stock_basic (
                market VARCHAR NOT NULL,
                code VARCHAR NOT NULL,
                name VARCHAR,
                industry VARCHAR,
                exchange VARCHAR,
                sector VARCHAR,
                list_date DATE,
                updated_at TIMESTAMP,
                PRIMARY KEY (market, code)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_kline (
                trade_date DATE NOT NULL,
                market VARCHAR NOT NULL,
                code VARCHAR NOT NULL,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                amount DOUBLE,
                turnover_ratio DOUBLE,
                volume_ratio DOUBLE,
                updated_at TIMESTAMP,
                PRIMARY KEY (trade_date, market, code)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_fundamental (
                trade_date DATE NOT NULL,
                market VARCHAR NOT NULL,
                code VARCHAR NOT NULL,
                pe_ttm DOUBLE,
                pb DOUBLE,
                roe DOUBLE,
                dividend_yield DOUBLE,
                total_mv DOUBLE,
                asset_liability_ratio DOUBLE,
                gross_margin DOUBLE,
                net_margin DOUBLE,
                operating_cashflow_3y DOUBLE,
                receivable_revenue_ratio DOUBLE,
                goodwill_equity_ratio DOUBLE,
                interest_debt_asset_ratio DOUBLE,
                revenue_growth DOUBLE,
                profit_growth DOUBLE,
                revenue_cagr_5y DOUBLE,
                profit_cagr_5y DOUBLE,
                roe_avg_5y DOUBLE,
                ocf_positive_years_5y DOUBLE,
                debt_ratio_change_5y DOUBLE,
                gross_margin_change_5y DOUBLE,
                turnover_ratio DOUBLE,
                volume_ratio DOUBLE,
                amount DOUBLE,
                is_st INTEGER,
                investigation_flag INTEGER,
                penalty_flag INTEGER,
                fund_occupation_flag INTEGER,
                illegal_reduce_flag INTEGER,
                pledge_ratio DOUBLE,
                no_dividend_5y_flag INTEGER,
                audit_change_count INTEGER,
                audit_opinion VARCHAR,
                sunset_industry_flag INTEGER,
                data_quality VARCHAR,
                updated_at TIMESTAMP,
                PRIMARY KEY (trade_date, market, code)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS positions (
                market VARCHAR NOT NULL,
                code VARCHAR NOT NULL,
                name VARCHAR,
                avg_cost DOUBLE NOT NULL,
                quantity BIGINT NOT NULL,
                stop_loss DOUBLE,
                take_profit DOUBLE,
                open_date DATE NOT NULL,
                updated_at TIMESTAMP,
                PRIMARY KEY (market, code)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS position_flows (
                flow_time TIMESTAMP NOT NULL,
                market VARCHAR NOT NULL,
                code VARCHAR NOT NULL,
                name VARCHAR,
                action VARCHAR NOT NULL,
                quantity_delta BIGINT,
                quantity_after BIGINT,
                avg_cost DOUBLE,
                stop_loss DOUBLE,
                take_profit DOUBLE,
                note VARCHAR
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_stock_basic_market_code ON stock_basic(market, code)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_kline_market_code_date ON daily_kline(market, code, trade_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_fund_market_code_date ON daily_fundamental(market, code, trade_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_positions_market_code ON positions(market, code)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_position_flows_time ON position_flows(flow_time)")


def _q(name: str) -> str:
    return f'"{name}"'


def _merge_dataframe(
    conn,
    table: str,
    frame: pd.DataFrame,
    key_cols: Sequence[str],
) -> int:
    if frame is None or frame.empty:
        return 0
    df = frame.copy()
    cols = [str(c) for c in df.columns]
    tmp_name = f"_tmp_{table}_{int(time.time() * 1000)}"
    conn.register(tmp_name, df)
    try:
        on_clause = " AND ".join([f"t.{_q(k)} = s.{_q(k)}" for k in key_cols])
        non_keys = [c for c in cols if c not in key_cols]
        update_clause = ", ".join([f"{_q(c)} = s.{_q(c)}" for c in non_keys])
        insert_cols = ", ".join([_q(c) for c in cols])
        insert_vals = ", ".join([f"s.{_q(c)}" for c in cols])
        sql = f"""
            MERGE INTO {_q(table)} AS t
            USING {tmp_name} AS s
            ON {on_clause}
            WHEN MATCHED THEN UPDATE SET {update_clause}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
        conn.execute(sql)
    finally:
        conn.unregister(tmp_name)
    return int(len(df))


def upsert_stock_basic(frame: pd.DataFrame, db_path: Path | str = DEFAULT_DB_PATH) -> int:
    if frame is None or frame.empty:
        return 0
    df = frame.copy()
    now_ts = datetime.now()
    df["market"] = df.get("market", "A").map(_normalize_market)
    df["code"] = [
        _normalize_code(m, c) for m, c in zip(df["market"].tolist(), df.get("code", "").tolist())
    ]
    df["name"] = df.get("name", "").map(_safe_str)
    df["industry"] = df.get("industry", "").map(_safe_str)
    df["exchange"] = df.get("exchange", "").map(_safe_str)
    df["sector"] = df.get("sector", "").map(_safe_str)
    df["list_date"] = pd.to_datetime(df.get("list_date"), errors="coerce").dt.date
    df["updated_at"] = now_ts
    keep_cols = ["market", "code", "name", "industry", "exchange", "sector", "list_date", "updated_at"]
    df = df[keep_cols]
    df = df[df["code"].astype(str).str.len() > 0].drop_duplicates(subset=["market", "code"], keep="last")
    if df.empty:
        return 0
    init_db(db_path=db_path)
    with get_connection(db_path=db_path) as conn:
        return _merge_dataframe(conn, "stock_basic", df, key_cols=["market", "code"])


def upsert_daily_kline(frame: pd.DataFrame, db_path: Path | str = DEFAULT_DB_PATH) -> int:
    if frame is None or frame.empty:
        return 0
    df = frame.copy()
    now_ts = datetime.now()
    df["market"] = df.get("market", "A").map(_normalize_market)
    df["code"] = [
        _normalize_code(m, c) for m, c in zip(df["market"].tolist(), df.get("code", "").tolist())
    ]
    df["trade_date"] = df.get("trade_date", date.today()).map(_normalize_trade_date)
    for col in ["open", "high", "low", "close", "volume", "amount", "turnover_ratio", "volume_ratio"]:
        df[col] = df.get(col).map(_to_float)
    df["updated_at"] = now_ts
    keep_cols = [
        "trade_date",
        "market",
        "code",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "turnover_ratio",
        "volume_ratio",
        "updated_at",
    ]
    df = df[keep_cols]
    df = df[df["code"].astype(str).str.len() > 0].drop_duplicates(
        subset=["trade_date", "market", "code"], keep="last"
    )
    if df.empty:
        return 0
    init_db(db_path=db_path)
    with get_connection(db_path=db_path) as conn:
        return _merge_dataframe(conn, "daily_kline", df, key_cols=["trade_date", "market", "code"])


def upsert_daily_fundamental(frame: pd.DataFrame, db_path: Path | str = DEFAULT_DB_PATH) -> int:
    if frame is None or frame.empty:
        return 0
    df = frame.copy()
    now_ts = datetime.now()
    df["market"] = df.get("market", "A").map(_normalize_market)
    df["code"] = [
        _normalize_code(m, c) for m, c in zip(df["market"].tolist(), df.get("code", "").tolist())
    ]
    df["trade_date"] = df.get("trade_date", date.today()).map(_normalize_trade_date)
    float_cols = [
        "pe_ttm",
        "pb",
        "roe",
        "dividend_yield",
        "total_mv",
        "asset_liability_ratio",
        "gross_margin",
        "net_margin",
        "operating_cashflow_3y",
        "receivable_revenue_ratio",
        "goodwill_equity_ratio",
        "interest_debt_asset_ratio",
        "revenue_growth",
        "profit_growth",
        "revenue_cagr_5y",
        "profit_cagr_5y",
        "roe_avg_5y",
        "ocf_positive_years_5y",
        "debt_ratio_change_5y",
        "gross_margin_change_5y",
        "turnover_ratio",
        "volume_ratio",
        "amount",
        "pledge_ratio",
    ]
    int_cols = [
        "is_st",
        "investigation_flag",
        "penalty_flag",
        "fund_occupation_flag",
        "illegal_reduce_flag",
        "no_dividend_5y_flag",
        "audit_change_count",
        "sunset_industry_flag",
    ]
    for col in float_cols:
        df[col] = df.get(col).map(_to_float)
    for col in int_cols:
        df[col] = df.get(col).map(_to_int)
    df["audit_opinion"] = df.get("audit_opinion", "").map(_safe_str)
    df["data_quality"] = df.get("data_quality", "").map(_safe_str)
    df["updated_at"] = now_ts

    keep_cols = [
        "trade_date",
        "market",
        "code",
        "pe_ttm",
        "pb",
        "roe",
        "dividend_yield",
        "total_mv",
        "asset_liability_ratio",
        "gross_margin",
        "net_margin",
        "operating_cashflow_3y",
        "receivable_revenue_ratio",
        "goodwill_equity_ratio",
        "interest_debt_asset_ratio",
        "revenue_growth",
        "profit_growth",
        "revenue_cagr_5y",
        "profit_cagr_5y",
        "roe_avg_5y",
        "ocf_positive_years_5y",
        "debt_ratio_change_5y",
        "gross_margin_change_5y",
        "turnover_ratio",
        "volume_ratio",
        "amount",
        "is_st",
        "investigation_flag",
        "penalty_flag",
        "fund_occupation_flag",
        "illegal_reduce_flag",
        "pledge_ratio",
        "no_dividend_5y_flag",
        "audit_change_count",
        "audit_opinion",
        "sunset_industry_flag",
        "data_quality",
        "updated_at",
    ]
    df = df[keep_cols]
    df = df[df["code"].astype(str).str.len() > 0].drop_duplicates(
        subset=["trade_date", "market", "code"], keep="last"
    )
    if df.empty:
        return 0
    init_db(db_path=db_path)
    with get_connection(db_path=db_path) as conn:
        return _merge_dataframe(conn, "daily_fundamental", df, key_cols=["trade_date", "market", "code"])


def _normalize_position_input(position: Position | Dict[str, Any]) -> Dict[str, Any]:
    raw: Dict[str, Any]
    if isinstance(position, Position):
        raw = {
            "market": position.market,
            "code": position.code,
            "name": position.name,
            "avg_cost": position.avg_cost,
            "quantity": position.quantity,
            "stop_loss": position.stop_loss,
            "take_profit": position.take_profit,
            "open_date": position.open_date,
        }
    else:
        raw = dict(position or {})

    market = _normalize_market(raw.get("market", "A"))
    code = _normalize_code(market, raw.get("code", ""))
    if not code:
        raise ValueError("持仓代码不能为空")
    avg_cost = _to_float(raw.get("avg_cost"))
    quantity = _to_int(raw.get("quantity"))
    if avg_cost is None or avg_cost <= 0:
        raise ValueError("买入平均成本必须大于0")
    if quantity is None or quantity <= 0:
        raise ValueError("持仓数量必须大于0")
    stop_loss = _to_float(raw.get("stop_loss"))
    take_profit = _to_float(raw.get("take_profit"))
    if stop_loss is not None and stop_loss <= 0:
        stop_loss = None
    if take_profit is not None and take_profit <= 0:
        take_profit = None

    return {
        "market": market,
        "code": code,
        "name": _safe_str(raw.get("name", "")),
        "avg_cost": float(avg_cost),
        "quantity": int(quantity),
        "stop_loss": stop_loss,
        "take_profit": take_profit,
        "open_date": _normalize_trade_date(raw.get("open_date", date.today())),
    }


def _record_position_flow(
    conn,
    *,
    market: str,
    code: str,
    name: str,
    action: str,
    quantity_delta: int,
    quantity_after: int,
    avg_cost: Optional[float],
    stop_loss: Optional[float],
    take_profit: Optional[float],
    note: str = "",
) -> None:
    conn.execute(
        """
        INSERT INTO position_flows(
            flow_time, market, code, name, action,
            quantity_delta, quantity_after, avg_cost, stop_loss, take_profit, note
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            datetime.now(),
            market,
            code,
            _safe_str(name),
            _safe_str(action).upper(),
            int(quantity_delta),
            int(quantity_after),
            _to_float(avg_cost),
            _to_float(stop_loss),
            _to_float(take_profit),
            _safe_str(note),
        ),
    )


def upsert_position(
    position: Position | Dict[str, Any],
    db_path: Path | str = DEFAULT_DB_PATH,
    note: str = "",
) -> Dict[str, Any]:
    init_db(db_path=db_path)
    payload = _normalize_position_input(position)
    now_ts = datetime.now()

    frame = pd.DataFrame(
        [
            {
                **payload,
                "updated_at": now_ts,
            }
        ]
    )

    with get_connection(db_path=db_path) as conn:
        old = conn.execute(
            """
            SELECT name, avg_cost, quantity, stop_loss, take_profit
            FROM positions
            WHERE market = ? AND code = ?
            """,
            (payload["market"], payload["code"]),
        ).fetchone()
        _merge_dataframe(conn, "positions", frame, key_cols=["market", "code"])

        old_qty = int(old[2]) if old else 0
        action = "OPEN" if old is None else "ADJUST"
        _record_position_flow(
            conn,
            market=payload["market"],
            code=payload["code"],
            name=payload.get("name", ""),
            action=action,
            quantity_delta=int(payload["quantity"] - old_qty),
            quantity_after=int(payload["quantity"]),
            avg_cost=payload.get("avg_cost"),
            stop_loss=payload.get("stop_loss"),
            take_profit=payload.get("take_profit"),
            note=note,
        )
    return payload


def remove_position(
    code: str,
    market: str = "A",
    db_path: Path | str = DEFAULT_DB_PATH,
    note: str = "",
) -> bool:
    init_db(db_path=db_path)
    market_norm = _normalize_market(market)
    code_norm = _normalize_code(market_norm, code)
    if not code_norm:
        return False

    with get_connection(db_path=db_path) as conn:
        old = conn.execute(
            """
            SELECT name, avg_cost, quantity, stop_loss, take_profit
            FROM positions
            WHERE market = ? AND code = ?
            """,
            (market_norm, code_norm),
        ).fetchone()
        if old is None:
            return False

        conn.execute(
            "DELETE FROM positions WHERE market = ? AND code = ?",
            (market_norm, code_norm),
        )
        _record_position_flow(
            conn,
            market=market_norm,
            code=code_norm,
            name=str(old[0] or ""),
            action="CLOSE",
            quantity_delta=-int(old[2] or 0),
            quantity_after=0,
            avg_cost=_to_float(old[1]),
            stop_loss=_to_float(old[3]),
            take_profit=_to_float(old[4]),
            note=note,
        )
    return True


def list_positions(db_path: Path | str = DEFAULT_DB_PATH) -> pd.DataFrame:
    init_db(db_path=db_path)
    with get_connection(db_path=db_path, read_only=True) as conn:
        df = conn.execute(
            """
            SELECT
                market,
                code,
                COALESCE(name, '') AS name,
                avg_cost,
                quantity,
                stop_loss,
                take_profit,
                open_date,
                updated_at
            FROM positions
            ORDER BY market, code
            """
        ).fetchdf()
    return df if df is not None else pd.DataFrame()


def list_position_flows(limit: int = 120, db_path: Path | str = DEFAULT_DB_PATH) -> pd.DataFrame:
    init_db(db_path=db_path)
    with get_connection(db_path=db_path, read_only=True) as conn:
        df = conn.execute(
            """
            SELECT
                flow_time,
                market,
                code,
                COALESCE(name, '') AS name,
                action,
                quantity_delta,
                quantity_after,
                avg_cost,
                stop_loss,
                take_profit,
                note
            FROM position_flows
            ORDER BY flow_time DESC
            LIMIT ?
            """,
            (int(max(1, limit)),),
        ).fetchdf()
    return df if df is not None else pd.DataFrame()


def get_positions_overview(
    total_equity: float,
    db_path: Path | str = DEFAULT_DB_PATH,
) -> pd.DataFrame:
    init_db(db_path=db_path)
    eq = float(total_equity) if _to_float(total_equity) is not None else 0.0
    with get_connection(db_path=db_path, read_only=True) as conn:
        df = conn.execute(
            """
            WITH latest_k AS (
                SELECT
                    market,
                    code,
                    trade_date,
                    close,
                    ROW_NUMBER() OVER(PARTITION BY market, code ORDER BY trade_date DESC) AS rn
                FROM daily_kline
            )
            SELECT
                p.market,
                p.code,
                COALESCE(p.name, '') AS name,
                p.avg_cost,
                p.quantity,
                p.stop_loss,
                p.take_profit,
                p.open_date,
                k.trade_date AS latest_trade_date,
                k.close AS current_price
            FROM positions p
            LEFT JOIN latest_k k
                ON k.market = p.market AND k.code = p.code AND k.rn = 1
            ORDER BY p.market, p.code
            """
        ).fetchdf()

    if df is None or df.empty:
        return pd.DataFrame(
            columns=[
                "market",
                "code",
                "name",
                "avg_cost",
                "quantity",
                "stop_loss",
                "take_profit",
                "open_date",
                "latest_trade_date",
                "current_price",
                "market_value",
                "pnl_amount",
                "pnl_pct",
                "weight_in_equity_pct",
                "weight_in_position_pct",
                "risk_status",
            ]
        )

    df = df.copy()
    df["current_price"] = pd.to_numeric(df["current_price"], errors="coerce")
    df["avg_cost"] = pd.to_numeric(df["avg_cost"], errors="coerce")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)
    df["stop_loss"] = pd.to_numeric(df["stop_loss"], errors="coerce")
    df["take_profit"] = pd.to_numeric(df["take_profit"], errors="coerce")
    df["current_price"] = df["current_price"].fillna(df["avg_cost"])
    df["market_value"] = df["current_price"] * df["quantity"]
    df["pnl_amount"] = (df["current_price"] - df["avg_cost"]) * df["quantity"]
    df["pnl_pct"] = (df["current_price"] / df["avg_cost"] - 1.0) * 100.0

    total_mv = float(df["market_value"].sum())
    if eq > 0:
        df["weight_in_equity_pct"] = df["market_value"] / eq * 100.0
    else:
        df["weight_in_equity_pct"] = 0.0
    if total_mv > 0:
        df["weight_in_position_pct"] = df["market_value"] / total_mv * 100.0
    else:
        df["weight_in_position_pct"] = 0.0

    risk_status: List[str] = []
    for _, row in df.iterrows():
        px = _to_float(row.get("current_price"))
        sl = _to_float(row.get("stop_loss"))
        tp = _to_float(row.get("take_profit"))
        if px is None:
            risk_status.append("无最新价格")
        elif sl is not None and px <= sl:
            risk_status.append("触发止损")
        elif tp is not None and px >= tp:
            risk_status.append("触发止盈")
        else:
            risk_status.append("正常")
    df["risk_status"] = risk_status
    return df


def get_latest_close(
    code: str,
    market: str = "A",
    db_path: Path | str = DEFAULT_DB_PATH,
) -> Optional[float]:
    init_db(db_path=db_path)
    market_norm = _normalize_market(market)
    code_norm = _normalize_code(market_norm, code)
    if not code_norm:
        return None
    with get_connection(db_path=db_path, read_only=True) as conn:
        row = conn.execute(
            """
            SELECT close
            FROM daily_kline
            WHERE market = ? AND code = ?
            ORDER BY trade_date DESC
            LIMIT 1
            """,
            (market_norm, code_norm),
        ).fetchone()
    if row is None:
        return None
    return _to_float(row[0])


def get_atr20(
    code: str,
    market: str = "A",
    db_path: Path | str = DEFAULT_DB_PATH,
) -> Optional[float]:
    init_db(db_path=db_path)
    market_norm = _normalize_market(market)
    code_norm = _normalize_code(market_norm, code)
    if not code_norm:
        return None
    with get_connection(db_path=db_path, read_only=True) as conn:
        hist = conn.execute(
            """
            SELECT trade_date, high, low, close
            FROM daily_kline
            WHERE market = ? AND code = ?
            ORDER BY trade_date DESC
            LIMIT 60
            """,
            (market_norm, code_norm),
        ).fetchdf()
    if hist is None or hist.empty:
        return None
    df = hist.sort_values("trade_date", ascending=True).copy()
    df["high"] = pd.to_numeric(df["high"], errors="coerce")
    df["low"] = pd.to_numeric(df["low"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["high", "low", "close"])
    if len(df) < 21:
        return None
    prev_close = df["close"].shift(1)
    tr_1 = (df["high"] - df["low"]).abs()
    tr_2 = (df["high"] - prev_close).abs()
    tr_3 = (df["low"] - prev_close).abs()
    tr = pd.concat([tr_1, tr_2, tr_3], axis=1).max(axis=1)
    atr20 = tr.rolling(20).mean().iloc[-1]
    return _to_float(atr20)


def suggest_position_size(
    code: str,
    market: str,
    total_equity: float,
    entry_price: Optional[float] = None,
    stop_loss: Optional[float] = None,
    risk_pct: float = 1.0,
    lot_size: int = 100,
    db_path: Path | str = DEFAULT_DB_PATH,
) -> Dict[str, Any]:
    market_norm = _normalize_market(market)
    code_norm = _normalize_code(market_norm, code)
    if not code_norm:
        raise ValueError("代码无效")
    equity = _to_float(total_equity)
    if equity is None or equity <= 0:
        raise ValueError("总净值必须大于0")
    risk_pct_val = _to_float(risk_pct)
    if risk_pct_val is None or risk_pct_val <= 0:
        raise ValueError("风险比例必须大于0")

    atr20 = get_atr20(code=code_norm, market=market_norm, db_path=db_path)
    if atr20 is None or atr20 <= 0:
        raise RuntimeError("无法计算ATR20，请先更新该标的的历史K线")

    px = _to_float(entry_price)
    if px is None or px <= 0:
        px = get_latest_close(code=code_norm, market=market_norm, db_path=db_path)
    if px is None or px <= 0:
        raise RuntimeError("无法获取入场价，请手工输入 entry_price")

    stop = _to_float(stop_loss)
    atr_risk = float(atr20)
    sl_risk = abs(float(px) - float(stop)) if stop is not None and stop > 0 else 0.0
    risk_per_share = max(atr_risk, sl_risk)
    if risk_per_share <= 0:
        raise RuntimeError("风险距离计算失败")

    risk_amount = float(equity) * float(risk_pct_val) / 100.0
    raw_shares = int(risk_amount // risk_per_share)
    lot = max(1, int(lot_size))
    suggested_lots = raw_shares // lot
    suggested_shares = suggested_lots * lot
    capital_needed = float(suggested_shares) * float(px)

    return {
        "market": market_norm,
        "code": code_norm,
        "entry_price": float(px),
        "atr20": float(atr20),
        "stop_loss": stop,
        "risk_pct": float(risk_pct_val),
        "risk_amount": float(risk_amount),
        "risk_per_share": float(risk_per_share),
        "raw_shares": int(raw_shares),
        "lot_size": int(lot),
        "suggested_lots": int(suggested_lots),
        "suggested_shares": int(suggested_shares),
        "capital_needed": float(capital_needed),
    }


def sync_snapshot_to_duckdb(
    snapshot_df: pd.DataFrame,
    trade_date: Optional[date | datetime | str] = None,
    db_path: Path | str = DEFAULT_DB_PATH,
) -> Dict[str, int]:
    if snapshot_df is None or snapshot_df.empty:
        return {"stock_basic": 0, "daily_kline": 0, "daily_fundamental": 0}
    day = _normalize_trade_date(trade_date if trade_date is not None else date.today())
    src = snapshot_df.copy()
    src["market"] = src.get("market", "A").map(_normalize_market)
    src["code"] = [
        _normalize_code(m, c) for m, c in zip(src["market"].tolist(), src.get("code", "").tolist())
    ]
    src["trade_date"] = day

    stock_basic_df = pd.DataFrame(
        {
            "market": src["market"],
            "code": src["code"],
            "name": src.get("name", ""),
            "industry": src.get("industry", ""),
            "exchange": src.get("market", ""),
            "sector": src.get("industry", ""),
            "list_date": pd.NaT,
        }
    )

    kline_df = pd.DataFrame(
        {
            "trade_date": src["trade_date"],
            "market": src["market"],
            "code": src["code"],
            "open": src.get("close_price"),
            "high": src.get("close_price"),
            "low": src.get("close_price"),
            "close": src.get("close_price"),
            "volume": src.get("amount"),
            "amount": src.get("amount"),
            "turnover_ratio": src.get("turnover_ratio"),
            "volume_ratio": src.get("volume_ratio"),
        }
    )

    fundamental_df = pd.DataFrame(
        {
            "trade_date": src["trade_date"],
            "market": src["market"],
            "code": src["code"],
            "pe_ttm": src.get("pe_ttm"),
            "pb": src.get("pb"),
            "roe": src.get("roe"),
            "dividend_yield": src.get("dividend_yield"),
            "total_mv": src.get("total_mv"),
            "asset_liability_ratio": src.get("asset_liability_ratio"),
            "gross_margin": src.get("gross_margin"),
            "net_margin": src.get("net_margin"),
            "operating_cashflow_3y": src.get("operating_cashflow_3y"),
            "receivable_revenue_ratio": src.get("receivable_revenue_ratio"),
            "goodwill_equity_ratio": src.get("goodwill_equity_ratio"),
            "interest_debt_asset_ratio": src.get("interest_debt_asset_ratio"),
            "revenue_growth": src.get("revenue_growth"),
            "profit_growth": src.get("profit_growth"),
            "revenue_cagr_5y": src.get("revenue_cagr_5y"),
            "profit_cagr_5y": src.get("profit_cagr_5y"),
            "roe_avg_5y": src.get("roe_avg_5y"),
            "ocf_positive_years_5y": src.get("ocf_positive_years_5y"),
            "debt_ratio_change_5y": src.get("debt_ratio_change_5y"),
            "gross_margin_change_5y": src.get("gross_margin_change_5y"),
            "turnover_ratio": src.get("turnover_ratio"),
            "volume_ratio": src.get("volume_ratio"),
            "amount": src.get("amount"),
            "is_st": src.get("is_st"),
            "investigation_flag": src.get("investigation_flag"),
            "penalty_flag": src.get("penalty_flag"),
            "fund_occupation_flag": src.get("fund_occupation_flag"),
            "illegal_reduce_flag": src.get("illegal_reduce_flag"),
            "pledge_ratio": src.get("pledge_ratio"),
            "no_dividend_5y_flag": src.get("no_dividend_5y_flag"),
            "audit_change_count": src.get("audit_change_count"),
            "audit_opinion": src.get("audit_opinion"),
            "sunset_industry_flag": src.get("sunset_industry_flag"),
            "data_quality": src.get("data_quality"),
        }
    )

    inserted_basic = upsert_stock_basic(stock_basic_df, db_path=db_path)
    inserted_kline = upsert_daily_kline(kline_df, db_path=db_path)
    inserted_fund = upsert_daily_fundamental(fundamental_df, db_path=db_path)
    return {"stock_basic": inserted_basic, "daily_kline": inserted_kline, "daily_fundamental": inserted_fund}


def _fetch_a_universe(max_stocks: int = 0) -> pd.DataFrame:
    ak = _load_akshare()
    spot = ak.stock_zh_a_spot_em()
    if spot is None or spot.empty:
        return pd.DataFrame(columns=["market", "code", "name", "industry"])
    df = pd.DataFrame()
    df["market"] = "A"
    df["code"] = spot.get("代码").astype(str).str.strip().str.zfill(6)
    df["name"] = spot.get("名称").astype(str).str.strip()
    if "所处行业" in spot.columns:
        df["industry"] = spot.get("所处行业").astype(str).str.strip()
    elif "行业" in spot.columns:
        df["industry"] = spot.get("行业").astype(str).str.strip()
    else:
        df["industry"] = ""
    df = df[df["code"].str.len() == 6].drop_duplicates(subset=["code"], keep="first")
    if max_stocks and max_stocks > 0:
        df = df.head(int(max_stocks))
    return df.reset_index(drop=True)


def _fetch_hk_universe(max_stocks: int = 0) -> pd.DataFrame:
    ak = _load_akshare()
    spot = ak.stock_hk_spot_em()
    if spot is None or spot.empty:
        return pd.DataFrame(columns=["market", "code", "name", "industry"])
    code_col = "代码" if "代码" in spot.columns else "symbol"
    name_col = "名称" if "名称" in spot.columns else ("中文名称" if "中文名称" in spot.columns else "name")
    industry_col = "所处行业" if "所处行业" in spot.columns else ("行业" if "行业" in spot.columns else None)
    raw_code = spot.get(code_col).astype(str).str.extract(r"(\d+)")[0].fillna("")
    df = pd.DataFrame()
    df["market"] = "HK"
    df["code"] = raw_code.astype(str).str[-5:].str.zfill(5)
    df["name"] = spot.get(name_col).astype(str).str.strip()
    df["industry"] = spot.get(industry_col).astype(str).str.strip() if industry_col else ""
    df = df[df["code"].str.len() == 5].drop_duplicates(subset=["code"], keep="first")
    if max_stocks and max_stocks > 0:
        df = df.head(int(max_stocks))
    return df.reset_index(drop=True)


def _fetch_a_hist(code: str, lookback_days: int) -> pd.DataFrame:
    ak = _load_akshare()
    hist = ak.stock_zh_a_hist(symbol=str(code), period="daily", adjust="qfq")
    if hist is None or hist.empty:
        return pd.DataFrame()
    out = pd.DataFrame()
    out["trade_date"] = pd.to_datetime(hist.get("日期"), errors="coerce").dt.date
    out["open"] = pd.to_numeric(hist.get("开盘"), errors="coerce")
    out["high"] = pd.to_numeric(hist.get("最高"), errors="coerce")
    out["low"] = pd.to_numeric(hist.get("最低"), errors="coerce")
    out["close"] = pd.to_numeric(hist.get("收盘"), errors="coerce")
    out["volume"] = pd.to_numeric(hist.get("成交量"), errors="coerce")
    out["amount"] = pd.to_numeric(hist.get("成交额"), errors="coerce")
    out["turnover_ratio"] = pd.to_numeric(hist.get("换手率"), errors="coerce")
    out["volume_ratio"] = pd.NA
    out = out.dropna(subset=["trade_date", "close"]).tail(int(max(1, lookback_days)))
    return out


def _fetch_hk_hist(code: str, lookback_days: int) -> pd.DataFrame:
    ak = _load_akshare()
    hist = ak.stock_hk_daily(symbol=str(code).zfill(5), adjust="qfq")
    if hist is None or hist.empty:
        return pd.DataFrame()
    out = hist.copy().reset_index().rename(columns={"index": "trade_date"})
    out["trade_date"] = pd.to_datetime(out["trade_date"], errors="coerce").dt.date
    out["open"] = pd.to_numeric(out.get("open"), errors="coerce")
    out["high"] = pd.to_numeric(out.get("high"), errors="coerce")
    out["low"] = pd.to_numeric(out.get("low"), errors="coerce")
    out["close"] = pd.to_numeric(out.get("close"), errors="coerce")
    out["volume"] = pd.to_numeric(out.get("volume"), errors="coerce")
    out["amount"] = pd.to_numeric(out.get("amount"), errors="coerce")
    out["turnover_ratio"] = pd.NA
    out["volume_ratio"] = pd.NA
    out = out.dropna(subset=["trade_date", "close"]).tail(int(max(1, lookback_days)))
    return out[["trade_date", "open", "high", "low", "close", "volume", "amount", "turnover_ratio", "volume_ratio"]]


def batch_upsert_daily_close(
    market_scope: str = "AH",
    max_stocks: int = 0,
    lookback_days: int = 80,
    workers: int = 10,
    db_path: Path | str = DEFAULT_DB_PATH,
) -> Dict[str, Any]:
    """
    全市场 A/H 每日收盘批量落盘。
    默认抓取最近 `lookback_days` 个交易日，支持增量反复执行（MERGE Upsert）。
    """
    scope = _safe_str(market_scope).upper() or "AH"
    use_a = scope in {"A", "AH", "ALL"}
    use_hk = scope in {"HK", "AH", "ALL"}
    _load_akshare()

    init_db(db_path=db_path)
    universe_frames: List[pd.DataFrame] = []
    if use_a:
        universe_frames.append(_fetch_a_universe(max_stocks=max_stocks))
    if use_hk:
        universe_frames.append(_fetch_hk_universe(max_stocks=max_stocks))
    universe = pd.concat(universe_frames, ignore_index=True) if universe_frames else pd.DataFrame()
    if universe.empty:
        return {"universe": 0, "kline_rows": 0, "failed": 0}

    upsert_stock_basic(universe[["market", "code", "name", "industry"]], db_path=db_path)

    def _job(one: Dict[str, Any]) -> Tuple[str, str, pd.DataFrame, Optional[str]]:
        market = _normalize_market(one.get("market"))
        code = _normalize_code(market, one.get("code"))
        try:
            if market == "HK":
                hist = _fetch_hk_hist(code, lookback_days=lookback_days)
            else:
                hist = _fetch_a_hist(code, lookback_days=lookback_days)
            if hist is None or hist.empty:
                return market, code, pd.DataFrame(), "empty"
            hist = hist.copy()
            hist["market"] = market
            hist["code"] = code
            return market, code, hist, None
        except Exception as exc:
            return market, code, pd.DataFrame(), str(exc)

    records = universe[["market", "code"]].to_dict("records")
    failed = 0
    kline_rows = 0
    batch_frames: List[pd.DataFrame] = []
    batch_rows = 0

    with ThreadPoolExecutor(max_workers=max(1, int(workers))) as ex:
        futures = [ex.submit(_job, rec) for rec in records]
        for fut in as_completed(futures):
            _market, _code, one_df, err = fut.result()
            if err:
                failed += 1
                continue
            if one_df is None or one_df.empty:
                continue
            batch_frames.append(one_df)
            batch_rows += len(one_df)
            if batch_rows >= 120_000:
                all_df = pd.concat(batch_frames, ignore_index=True)
                kline_rows += upsert_daily_kline(all_df, db_path=db_path)
                batch_frames = []
                batch_rows = 0

    if batch_frames:
        all_df = pd.concat(batch_frames, ignore_index=True)
        kline_rows += upsert_daily_kline(all_df, db_path=db_path)

    return {
        "universe": int(len(universe)),
        "kline_rows": int(kline_rows),
        "failed": int(failed),
        "scope": scope,
        "lookback_days": int(lookback_days),
    }


def _build_base_universe_sql(candidate_codes: Optional[pd.DataFrame] = None) -> str:
    candidate_join = ""
    if candidate_codes is not None and not candidate_codes.empty:
        candidate_join = """
        INNER JOIN candidate_codes c
          ON c.market = b.market AND c.code = b.code
        """
    return f"""
        WITH kline_ranked AS (
            SELECT
                market,
                code,
                trade_date,
                close,
                volume,
                amount,
                turnover_ratio,
                volume_ratio,
                ROW_NUMBER() OVER (PARTITION BY market, code ORDER BY trade_date DESC) AS rn
            FROM daily_kline
        ),
        kline_feat AS (
            SELECT
                market,
                code,
                MAX(CASE WHEN rn = 1 THEN close END) AS close_price,
                MAX(CASE WHEN rn = 1 THEN volume END) AS latest_volume,
                MAX(CASE WHEN rn = 1 THEN amount END) AS latest_amount,
                MAX(CASE WHEN rn = 1 THEN turnover_ratio END) AS latest_turnover_ratio,
                MAX(CASE WHEN rn = 1 THEN volume_ratio END) AS latest_volume_ratio,
                AVG(CASE WHEN rn <= 20 THEN close END) AS ma20,
                AVG(CASE WHEN rn <= 20 THEN volume END) AS vol_ma20
            FROM kline_ranked
            GROUP BY market, code
        ),
        fund_ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY market, code ORDER BY trade_date DESC, updated_at DESC) AS rn
            FROM daily_fundamental
        ),
        latest_f AS (
            SELECT * FROM fund_ranked WHERE rn = 1
        ),
        universe AS (
            SELECT
                b.market,
                b.code,
                b.name,
                b.industry,
                f.pe_ttm,
                f.pb,
                f.dividend_yield,
                f.roe,
                f.asset_liability_ratio,
                COALESCE(k.latest_turnover_ratio, f.turnover_ratio) AS turnover_ratio,
                COALESCE(k.latest_volume_ratio, f.volume_ratio) AS volume_ratio,
                f.total_mv,
                f.revenue_cagr_5y,
                f.profit_cagr_5y,
                f.roe_avg_5y,
                f.ocf_positive_years_5y,
                f.debt_ratio_change_5y,
                f.gross_margin_change_5y,
                COALESCE(f.data_quality, 'partial') AS data_quality,
                f.operating_cashflow_3y,
                f.gross_margin,
                f.net_margin,
                f.receivable_revenue_ratio,
                f.goodwill_equity_ratio,
                f.interest_debt_asset_ratio,
                f.revenue_growth,
                f.profit_growth,
                COALESCE(k.latest_amount, f.amount) AS amount,
                k.latest_volume,
                k.close_price,
                k.ma20,
                k.vol_ma20,
                COALESCE(f.is_st, 0) AS is_st,
                COALESCE(f.investigation_flag, 0) AS investigation_flag,
                COALESCE(f.penalty_flag, 0) AS penalty_flag,
                COALESCE(f.fund_occupation_flag, 0) AS fund_occupation_flag,
                COALESCE(f.illegal_reduce_flag, 0) AS illegal_reduce_flag,
                f.pledge_ratio,
                COALESCE(f.no_dividend_5y_flag, 0) AS no_dividend_5y_flag,
                COALESCE(f.audit_change_count, 0) AS audit_change_count,
                COALESCE(f.audit_opinion, '') AS audit_opinion,
                COALESCE(f.sunset_industry_flag, 0) AS sunset_industry_flag
            FROM stock_basic b
            LEFT JOIN latest_f f
                ON f.market = b.market AND f.code = b.code
            LEFT JOIN kline_feat k
                ON k.market = b.market AND k.code = b.code
            {candidate_join}
        )
    """


def _add_numeric_range_clause(
    clauses: List[str],
    params: List[Any],
    missing_terms: List[str],
    missing_labels: List[str],
    column: str,
    min_enabled: bool,
    max_enabled: bool,
    min_value: float,
    max_value: float,
    missing_policy: str,
    label: str,
) -> None:
    if not min_enabled and not max_enabled:
        return
    missing_terms.append(f"{column} IS NULL")
    missing_labels.append(label)
    if missing_policy == "exclude":
        sub_parts = [f"{column} IS NOT NULL"]
        if min_enabled:
            sub_parts.append(f"{column} >= ?")
            params.append(float(min_value))
        if max_enabled:
            sub_parts.append(f"{column} <= ?")
            params.append(float(max_value))
        clauses.append("(" + " AND ".join(sub_parts) + ")")
        return

    if min_enabled and max_enabled:
        clauses.append(f"({column} IS NULL OR ({column} >= ? AND {column} <= ?))")
        params.extend([float(min_value), float(max_value)])
    elif min_enabled:
        clauses.append(f"({column} IS NULL OR {column} >= ?)")
        params.append(float(min_value))
    elif max_enabled:
        clauses.append(f"({column} IS NULL OR {column} <= ?)")
        params.append(float(max_value))


def _build_filter_predicates(
    cfg: Dict[str, Any],
    include_rearview: bool = True,
) -> Tuple[str, List[Any], str, str]:
    risk = cfg.get("risk", {}) if isinstance(cfg, dict) else {}
    quality = cfg.get("quality", {}) if isinstance(cfg, dict) else {}
    valuation = cfg.get("valuation", {}) if isinstance(cfg, dict) else {}
    growth = cfg.get("growth_liquidity", {}) if isinstance(cfg, dict) else {}
    rearview = cfg.get("rearview_5y", {}) if isinstance(cfg, dict) else {}
    missing_policy = _safe_str(cfg.get("missing_policy", "ignore")).lower()
    missing_policy = "exclude" if missing_policy == "exclude" else "ignore"

    clauses: List[str] = []
    params: List[Any] = []
    missing_terms: List[str] = []
    missing_labels: List[str] = []

    scope = _safe_str(risk.get("market_scope", "all")).upper()
    if scope in {"A", "HK"}:
        clauses.append("u.market = ?")
        params.append(scope)

    if bool(risk.get("industry_include_enabled", False)):
        kws = _expand_industry_keywords(_split_keywords(_safe_str(risk.get("industry_include_keywords", ""))))
        if kws:
            sub = []
            for kw in kws:
                like_value = f"%{kw.lower()}%"
                sub.append(
                    "(LOWER(COALESCE(u.industry, '')) LIKE ? OR LOWER(COALESCE(u.name, '')) LIKE ?)"
                )
                params.extend([like_value, like_value])
            clauses.append("(" + " OR ".join(sub) + ")")

    if bool(risk.get("exclude_st", True)):
        clauses.append("COALESCE(u.is_st, 0) = 0")
    if bool(risk.get("exclude_investigation", True)):
        clauses.append("COALESCE(u.investigation_flag, 0) = 0")
    if bool(risk.get("exclude_penalty", True)):
        clauses.append("COALESCE(u.penalty_flag, 0) = 0")
    if bool(risk.get("exclude_fund_occupation", True)):
        clauses.append("COALESCE(u.fund_occupation_flag, 0) = 0")
    if bool(risk.get("exclude_illegal_reduce", True)):
        clauses.append("COALESCE(u.illegal_reduce_flag, 0) = 0")

    if bool(risk.get("require_standard_audit", False)):
        missing_terms.append("(u.audit_opinion IS NULL OR TRIM(u.audit_opinion) = '')")
        missing_labels.append("审计意见")
        if missing_policy == "exclude":
            clauses.append("(u.audit_opinion IS NOT NULL AND u.audit_opinion LIKE '%标准无保留%')")
        else:
            clauses.append("(u.audit_opinion IS NULL OR u.audit_opinion LIKE '%标准无保留%')")

    _add_numeric_range_clause(
        clauses,
        params,
        missing_terms,
        missing_labels,
        "u.pledge_ratio",
        min_enabled=False,
        max_enabled=bool(risk.get("pledge_ratio_max_enabled", False)),
        min_value=0.0,
        max_value=float(risk.get("pledge_ratio_max", 80.0)),
        missing_policy=missing_policy,
        label="实控人质押率",
    )
    _add_numeric_range_clause(
        clauses,
        params,
        missing_terms,
        missing_labels,
        "u.audit_change_count",
        min_enabled=False,
        max_enabled=bool(risk.get("audit_change_max_enabled", False)),
        min_value=0.0,
        max_value=float(risk.get("audit_change_max", 2)),
        missing_policy=missing_policy,
        label="审计所更换次数(3年)",
    )

    if bool(risk.get("exclude_no_dividend_5y", False)):
        clauses.append("COALESCE(u.no_dividend_5y_flag, 0) = 0")

    if bool(risk.get("exclude_sunset_industry", False)):
        kws = _split_keywords(_safe_str(risk.get("sunset_industries", "")))
        if kws:
            sub = []
            for kw in kws:
                like_value = f"%{kw.lower()}%"
                sub.append("(LOWER(COALESCE(u.industry, '') || ' ' || COALESCE(u.name, '')) LIKE ?)")
                params.append(like_value)
            clauses.append("NOT (" + " OR ".join(sub) + ")")
        else:
            clauses.append("COALESCE(u.sunset_industry_flag, 0) = 0")

    _add_numeric_range_clause(
        clauses,
        params,
        missing_terms,
        missing_labels,
        "u.operating_cashflow_3y",
        min_enabled=bool(quality.get("ocf_3y_min_enabled", False)),
        max_enabled=False,
        min_value=float(quality.get("ocf_3y_min", 0.0)),
        max_value=0.0,
        missing_policy=missing_policy,
        label="近3年经营现金流(亿)",
    )
    _add_numeric_range_clause(
        clauses,
        params,
        missing_terms,
        missing_labels,
        "u.asset_liability_ratio",
        min_enabled=False,
        max_enabled=bool(quality.get("asset_liability_max_enabled", False)),
        min_value=0.0,
        max_value=float(quality.get("asset_liability_max", 80.0)),
        missing_policy=missing_policy,
        label="资产负债率(%)",
    )
    _add_numeric_range_clause(
        clauses,
        params,
        missing_terms,
        missing_labels,
        "u.interest_debt_asset_ratio",
        min_enabled=False,
        max_enabled=bool(quality.get("interest_debt_asset_max_enabled", False)),
        min_value=0.0,
        max_value=float(quality.get("interest_debt_asset_max", 20.0)),
        missing_policy=missing_policy,
        label="有息负债/总资产(%)",
    )
    _add_numeric_range_clause(
        clauses,
        params,
        missing_terms,
        missing_labels,
        "u.roe",
        min_enabled=bool(quality.get("roe_min_enabled", False)),
        max_enabled=False,
        min_value=float(quality.get("roe_min", 5.0)),
        max_value=0.0,
        missing_policy=missing_policy,
        label="ROE(%)",
    )
    _add_numeric_range_clause(
        clauses,
        params,
        missing_terms,
        missing_labels,
        "u.gross_margin",
        min_enabled=bool(quality.get("gross_margin_min_enabled", False)),
        max_enabled=False,
        min_value=float(quality.get("gross_margin_min", 20.0)),
        max_value=0.0,
        missing_policy=missing_policy,
        label="毛利率(%)",
    )
    _add_numeric_range_clause(
        clauses,
        params,
        missing_terms,
        missing_labels,
        "u.net_margin",
        min_enabled=bool(quality.get("net_margin_min_enabled", False)),
        max_enabled=False,
        min_value=float(quality.get("net_margin_min", 8.0)),
        max_value=0.0,
        missing_policy=missing_policy,
        label="净利率(%)",
    )
    _add_numeric_range_clause(
        clauses,
        params,
        missing_terms,
        missing_labels,
        "u.receivable_revenue_ratio",
        min_enabled=False,
        max_enabled=bool(quality.get("receivable_ratio_max_enabled", False)),
        min_value=0.0,
        max_value=float(quality.get("receivable_ratio_max", 50.0)),
        missing_policy=missing_policy,
        label="应收代理指标",
    )
    _add_numeric_range_clause(
        clauses,
        params,
        missing_terms,
        missing_labels,
        "u.goodwill_equity_ratio",
        min_enabled=False,
        max_enabled=bool(quality.get("goodwill_ratio_max_enabled", False)),
        min_value=0.0,
        max_value=float(quality.get("goodwill_ratio_max", 30.0)),
        missing_policy=missing_policy,
        label="商誉/净资产(%)",
    )

    _add_numeric_range_clause(
        clauses,
        params,
        missing_terms,
        missing_labels,
        "u.pe_ttm",
        min_enabled=bool(valuation.get("pe_ttm_min_enabled", False)),
        max_enabled=bool(valuation.get("pe_ttm_max_enabled", False)),
        min_value=float(valuation.get("pe_ttm_min", 0.0)),
        max_value=float(valuation.get("pe_ttm_max", 25.0)),
        missing_policy=missing_policy,
        label="PE(TTM)",
    )
    _add_numeric_range_clause(
        clauses,
        params,
        missing_terms,
        missing_labels,
        "u.pb",
        min_enabled=False,
        max_enabled=bool(valuation.get("pb_max_enabled", False)),
        min_value=0.0,
        max_value=float(valuation.get("pb_max", 3.0)),
        missing_policy=missing_policy,
        label="PB",
    )
    _add_numeric_range_clause(
        clauses,
        params,
        missing_terms,
        missing_labels,
        "u.dividend_yield",
        min_enabled=bool(valuation.get("dividend_min_enabled", False)),
        max_enabled=bool(valuation.get("dividend_max_enabled", False)),
        min_value=float(valuation.get("dividend_min", 3.0)),
        max_value=float(valuation.get("dividend_max", 12.0)),
        missing_policy=missing_policy,
        label="股息率(%)",
    )

    _add_numeric_range_clause(
        clauses,
        params,
        missing_terms,
        missing_labels,
        "u.revenue_growth",
        min_enabled=bool(growth.get("revenue_growth_min_enabled", False)),
        max_enabled=False,
        min_value=float(growth.get("revenue_growth_min", 0.0)),
        max_value=0.0,
        missing_policy=missing_policy,
        label="营收增速(%)",
    )
    _add_numeric_range_clause(
        clauses,
        params,
        missing_terms,
        missing_labels,
        "u.profit_growth",
        min_enabled=bool(growth.get("profit_growth_min_enabled", False)),
        max_enabled=False,
        min_value=float(growth.get("profit_growth_min", 0.0)),
        max_value=0.0,
        missing_policy=missing_policy,
        label="利润增速(%)",
    )
    _add_numeric_range_clause(
        clauses,
        params,
        missing_terms,
        missing_labels,
        "u.total_mv",
        min_enabled=bool(growth.get("market_cap_min_enabled", False)),
        max_enabled=bool(growth.get("market_cap_max_enabled", False)),
        min_value=float(growth.get("market_cap_min", 100.0)),
        max_value=float(growth.get("market_cap_max", 5000.0)),
        missing_policy=missing_policy,
        label="总市值(亿)",
    )
    _add_numeric_range_clause(
        clauses,
        params,
        missing_terms,
        missing_labels,
        "u.turnover_ratio",
        min_enabled=bool(growth.get("turnover_min_enabled", False)),
        max_enabled=bool(growth.get("turnover_max_enabled", False)),
        min_value=float(growth.get("turnover_min", 0.2)),
        max_value=float(growth.get("turnover_max", 15.0)),
        missing_policy=missing_policy,
        label="换手率(%)",
    )
    _add_numeric_range_clause(
        clauses,
        params,
        missing_terms,
        missing_labels,
        "u.volume_ratio",
        min_enabled=bool(growth.get("volume_ratio_min_enabled", False)),
        max_enabled=bool(growth.get("volume_ratio_max_enabled", False)),
        min_value=float(growth.get("volume_ratio_min", 0.5)),
        max_value=float(growth.get("volume_ratio_max", 3.0)),
        missing_policy=missing_policy,
        label="量比",
    )
    _add_numeric_range_clause(
        clauses,
        params,
        missing_terms,
        missing_labels,
        "u.amount",
        min_enabled=bool(growth.get("amount_min_enabled", False)),
        max_enabled=False,
        min_value=float(growth.get("amount_min", 100000000.0)),
        max_value=0.0,
        missing_policy=missing_policy,
        label="成交额(元)",
    )

    if bool(growth.get("close_above_ma20_enabled", False)):
        missing_terms.append("(u.close_price IS NULL OR u.ma20 IS NULL)")
        missing_labels.append("close/MA20")
        if missing_policy == "exclude":
            clauses.append("(u.close_price IS NOT NULL AND u.ma20 IS NOT NULL AND u.close_price > u.ma20)")
        else:
            clauses.append("(u.close_price IS NULL OR u.ma20 IS NULL OR u.close_price > u.ma20)")

    if bool(growth.get("volume_above_volma20_enabled", False)):
        missing_terms.append("(u.latest_volume IS NULL OR u.vol_ma20 IS NULL)")
        missing_labels.append("volume/volMA20")
        if missing_policy == "exclude":
            clauses.append("(u.latest_volume IS NOT NULL AND u.vol_ma20 IS NOT NULL AND u.latest_volume > u.vol_ma20)")
        else:
            clauses.append("(u.latest_volume IS NULL OR u.vol_ma20 IS NULL OR u.latest_volume > u.vol_ma20)")

    if include_rearview:
        _add_numeric_range_clause(
            clauses,
            params,
            missing_terms,
            missing_labels,
            "u.revenue_cagr_5y",
            min_enabled=bool(rearview.get("revenue_cagr_5y_min_enabled", False)),
            max_enabled=False,
            min_value=float(rearview.get("revenue_cagr_5y_min", 3.0)),
            max_value=0.0,
            missing_policy=missing_policy,
            label="营收5年CAGR(%)",
        )
        _add_numeric_range_clause(
            clauses,
            params,
            missing_terms,
            missing_labels,
            "u.profit_cagr_5y",
            min_enabled=bool(rearview.get("profit_cagr_5y_min_enabled", False)),
            max_enabled=False,
            min_value=float(rearview.get("profit_cagr_5y_min", 3.0)),
            max_value=0.0,
            missing_policy=missing_policy,
            label="净利5年CAGR(%)",
        )
        _add_numeric_range_clause(
            clauses,
            params,
            missing_terms,
            missing_labels,
            "u.roe_avg_5y",
            min_enabled=bool(rearview.get("roe_avg_5y_min_enabled", False)),
            max_enabled=False,
            min_value=float(rearview.get("roe_avg_5y_min", 8.0)),
            max_value=0.0,
            missing_policy=missing_policy,
            label="ROE5年均值(%)",
        )
        _add_numeric_range_clause(
            clauses,
            params,
            missing_terms,
            missing_labels,
            "u.ocf_positive_years_5y",
            min_enabled=bool(rearview.get("ocf_positive_years_5y_min_enabled", False)),
            max_enabled=False,
            min_value=float(rearview.get("ocf_positive_years_5y_min", 4)),
            max_value=0.0,
            missing_policy=missing_policy,
            label="经营现金流为正年数(5年)",
        )
        _add_numeric_range_clause(
            clauses,
            params,
            missing_terms,
            missing_labels,
            "u.debt_ratio_change_5y",
            min_enabled=False,
            max_enabled=bool(rearview.get("debt_ratio_change_5y_max_enabled", False)),
            min_value=0.0,
            max_value=float(rearview.get("debt_ratio_change_5y_max", 8.0)),
            missing_policy=missing_policy,
            label="负债率5年变化(百分点)",
        )
        _add_numeric_range_clause(
            clauses,
            params,
            missing_terms,
            missing_labels,
            "u.gross_margin_change_5y",
            min_enabled=bool(rearview.get("gross_margin_change_5y_min_enabled", False)),
            max_enabled=False,
            min_value=float(rearview.get("gross_margin_change_5y_min", -6.0)),
            max_value=0.0,
            missing_policy=missing_policy,
            label="毛利率5年变化(百分点)",
        )

    where_sql = " AND ".join(clauses) if clauses else "1=1"
    missing_sql = " OR ".join(missing_terms) if missing_terms else "1=0"
    missing_label_text = "、".join(dict.fromkeys(missing_labels))
    return where_sql, params, missing_sql, missing_label_text


def _read_filtered_df(conn, base_sql: str, where_sql: str, params: Sequence[Any], reject: bool = False, missing_text: str = "") -> pd.DataFrame:
    if reject:
        tag_reason = "SQL条件不满足"
        tag_missing = ""
    else:
        tag_reason = ""
        tag_missing = _safe_str(missing_text)
    sql = f"""
        {base_sql}
        SELECT
            u.market,
            u.code,
            u.name,
            u.industry,
            u.pe_ttm,
            u.pb,
            u.dividend_yield,
            u.roe,
            u.asset_liability_ratio,
            u.turnover_ratio,
            u.volume_ratio,
            u.total_mv,
            u.revenue_cagr_5y,
            u.profit_cagr_5y,
            u.roe_avg_5y,
            u.ocf_positive_years_5y,
            u.debt_ratio_change_5y,
            u.gross_margin_change_5y,
            u.data_quality,
            '{tag_reason}' AS exclude_reasons,
            '{tag_missing}' AS missing_fields
        FROM universe u
        WHERE {where_sql}
    """
    return conn.execute(sql, list(params)).fetchdf()


def run_filter_query(
    cfg: Dict[str, Any],
    include_rearview: bool = True,
    candidate_codes: Optional[pd.DataFrame] = None,
    db_path: Path | str = DEFAULT_DB_PATH,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict[str, int]]:
    init_db(db_path=db_path)
    where_sql, params, missing_sql, missing_text = _build_filter_predicates(cfg, include_rearview=include_rearview)
    base_sql = _build_base_universe_sql(candidate_codes=candidate_codes)

    with get_connection(db_path=db_path, read_only=True) as conn:
        if candidate_codes is not None and not candidate_codes.empty:
            candidate = candidate_codes[["market", "code"]].copy()
            candidate["market"] = candidate["market"].map(_normalize_market)
            candidate["code"] = [
                _normalize_code(m, c) for m, c in zip(candidate["market"].tolist(), candidate["code"].tolist())
            ]
            conn.register("candidate_codes", candidate.drop_duplicates(subset=["market", "code"]))
        try:
            total_sql = f"{base_sql} SELECT COUNT(*) AS cnt FROM universe"
            total = int(conn.execute(total_sql).fetchone()[0])
            passed_df = _read_filtered_df(conn, base_sql, where_sql, params=params, reject=False, missing_text="")
            rejected_df = _read_filtered_df(conn, base_sql, f"NOT ({where_sql})", params=params, reject=True)
            missing_df = _read_filtered_df(conn, base_sql, missing_sql, params=[], reject=False, missing_text=missing_text)
        finally:
            if candidate_codes is not None and not candidate_codes.empty:
                conn.unregister("candidate_codes")

    for frame in (passed_df, rejected_df, missing_df):
        if frame is None or frame.empty:
            continue
        for col in DISPLAY_COLUMNS:
            if col not in frame.columns:
                frame[col] = None
    passed_df = passed_df[DISPLAY_COLUMNS].sort_values(by=["total_mv", "code"], ascending=[False, True], na_position="last") if not passed_df.empty else pd.DataFrame(columns=DISPLAY_COLUMNS)
    rejected_df = rejected_df[DISPLAY_COLUMNS].sort_values(by=["code"], ascending=[True], na_position="last") if not rejected_df.empty else pd.DataFrame(columns=DISPLAY_COLUMNS)
    missing_df = missing_df[DISPLAY_COLUMNS].drop_duplicates(subset=["market", "code"], keep="first").sort_values(by=["code"], ascending=[True], na_position="last") if not missing_df.empty else pd.DataFrame(columns=DISPLAY_COLUMNS)

    stats = {
        "total": int(total),
        "passed": int(len(passed_df)),
        "rejected": int(len(rejected_df)),
        "missing": int(len(missing_df)),
    }
    return passed_df, rejected_df, missing_df, stats


def _build_cli() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="DuckDB 数据底座管理工具")
    sub = p.add_subparsers(dest="cmd")

    sub.add_parser("init", help="初始化 quant_system.duckdb 及核心表")

    up = sub.add_parser("upsert-daily-close", help="批量更新 A/H 每日收盘到 DuckDB")
    up.add_argument("--market", default="AH", help="A / HK / AH / ALL")
    up.add_argument("--max-stocks", type=int, default=0, help="每个市场最大股票数，0=全部")
    up.add_argument("--lookback-days", type=int, default=80, help="每只股票回补最近交易日数")
    up.add_argument("--workers", type=int, default=10, help="并发抓取线程数")

    return p


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = _build_cli()
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.cmd == "init":
        init_db()
        print(f"initialized: {DEFAULT_DB_PATH}")
        return 0

    if args.cmd == "upsert-daily-close":
        stats = batch_upsert_daily_close(
            market_scope=str(args.market),
            max_stocks=int(args.max_stocks),
            lookback_days=int(args.lookback_days),
            workers=int(args.workers),
        )
        print(stats)
        return 0

    parser.print_help()
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
