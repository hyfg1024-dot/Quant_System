from __future__ import annotations

import hashlib
import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BACKUP_ROOT = PROJECT_ROOT / "data" / "backups"

DEFAULT_ASSETS: list[dict[str, str]] = [
    {"key": "filter_market_db", "label": "大过滤器 SQLite 数据库", "path": "apps/filter/data/filter_market.db", "type": "file"},
    {"key": "quant_duckdb", "label": "DuckDB 本地数据底座", "path": "data/quant_system.duckdb", "type": "file"},
    {"key": "alert_rules", "label": "告警规则", "path": "config/alert_rules.yaml", "type": "file"},
    {"key": "filter_templates", "label": "筛选模板", "path": "apps/filter/data/filter_templates.json", "type": "file"},
    {"key": "manual_flags", "label": "手工风险标记", "path": "apps/filter/data/manual_flags.json", "type": "file"},
    {"key": "backtest_universe", "label": "回测股票池", "path": "apps/backtest/config/universe.yaml", "type": "file"},
    {"key": "backtest_strategies", "label": "本地回测策略", "path": "apps/backtest/config/strategies", "type": "dir"},
    {"key": "paper_trades", "label": "模拟盘状态与交易流水", "path": "apps/backtest/paper_trades", "type": "dir"},
]

EXCLUDE_NAMES = {".DS_Store", "dashboard.html"}
EXCLUDE_DIRS = {"__pycache__", "venv", ".venv", "cache", "_trash"}
EXCLUDE_SUFFIXES = {".pyc", ".pyo"}


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _backup_id() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _safe_reason(reason: str) -> str:
    text = str(reason or "manual").strip()
    return text[:120] if text else "manual"


def _safe_note(note: str) -> str:
    text = str(note or "").strip()
    return text[:300]


def _backup_dir_for_id(backup_id: str) -> tuple[str, Path]:
    bid = str(backup_id or "").strip()
    if not bid:
        raise ValueError("backup_id 不能为空")
    root = BACKUP_ROOT.resolve()
    backup_dir = (root / bid).resolve()
    if backup_dir.parent != root:
        raise ValueError("非法备份路径，拒绝操作")
    return bid, backup_dir


def _asset_map() -> dict[str, dict[str, str]]:
    return {item["key"]: item for item in DEFAULT_ASSETS}


def _selected_assets(asset_keys: Iterable[str] | None = None) -> list[dict[str, str]]:
    if asset_keys is None:
        return list(DEFAULT_ASSETS)
    amap = _asset_map()
    out = []
    for key in asset_keys:
        item = amap.get(str(key))
        if item:
            out.append(item)
    return out


def _file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _dir_size(path: Path) -> int:
    total = 0
    for fp in path.rglob("*"):
        if fp.is_file() and not _should_skip(fp):
            total += fp.stat().st_size
    return total


def _should_skip(path: Path) -> bool:
    if path.name in EXCLUDE_NAMES:
        return True
    if path.suffix in EXCLUDE_SUFFIXES:
        return True
    if any(part in EXCLUDE_DIRS for part in path.parts):
        return True
    return False


def _copy_dir(src: Path, dst: Path) -> tuple[int, int]:
    count = 0
    total_size = 0
    for fp in src.rglob("*"):
        if not fp.is_file() or _should_skip(fp):
            continue
        rel = fp.relative_to(src)
        target = dst / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(fp, target)
        count += 1
        total_size += fp.stat().st_size
    return count, total_size


def create_backup(
    reason: str = "manual",
    *,
    note: str = "",
    asset_keys: Iterable[str] | None = None,
    max_keep: int = 30,
) -> dict[str, Any]:
    """Create a local restore point under data/backups.

    Local credentials are intentionally not included in DEFAULT_ASSETS.
    """
    BACKUP_ROOT.mkdir(parents=True, exist_ok=True)
    bid = _backup_id()
    backup_dir = BACKUP_ROOT / bid
    while backup_dir.exists():
        bid = f"{_backup_id()}_{datetime.now().microsecond}"
        backup_dir = BACKUP_ROOT / bid
    payload_dir = backup_dir / "payload"
    payload_dir.mkdir(parents=True, exist_ok=True)

    manifest: dict[str, Any] = {
        "backup_id": bid,
        "created_at": _now_text(),
        "reason": _safe_reason(reason),
        "note": _safe_note(note),
        "project_root": str(PROJECT_ROOT),
        "assets": [],
        "schema_version": 1,
    }

    for asset in _selected_assets(asset_keys):
        rel_path = asset["path"]
        src = PROJECT_ROOT / rel_path
        entry: dict[str, Any] = {
            "key": asset["key"],
            "label": asset["label"],
            "path": rel_path,
            "type": asset["type"],
            "exists": src.exists(),
        }
        if not src.exists():
            manifest["assets"].append(entry)
            continue
        dst = payload_dir / rel_path
        if asset["type"] == "file":
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
            entry.update({"size": src.stat().st_size, "sha256": _file_sha256(src), "file_count": 1})
        else:
            dst.mkdir(parents=True, exist_ok=True)
            file_count, total_size = _copy_dir(src, dst)
            entry.update({"size": total_size, "file_count": file_count})
        manifest["assets"].append(entry)

    (backup_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    prune_backups(max_keep=max_keep)
    return manifest


def list_backups(limit: int | None = None) -> list[dict[str, Any]]:
    if not BACKUP_ROOT.exists():
        return []
    rows: list[dict[str, Any]] = []
    for manifest_path in BACKUP_ROOT.glob("*/manifest.json"):
        try:
            row = json.loads(manifest_path.read_text(encoding="utf-8"))
            row["backup_dir"] = str(manifest_path.parent)
            row["asset_count"] = sum(1 for item in row.get("assets", []) if item.get("exists"))
            row["total_size"] = sum(int(item.get("size", 0) or 0) for item in row.get("assets", []))
            row["note"] = _safe_note(row.get("note", "")) or _safe_reason(row.get("reason", ""))
            rows.append(row)
        except Exception:
            continue
    rows.sort(key=lambda x: str(x.get("backup_id", "")), reverse=True)
    return rows[:limit] if limit else rows


def get_backup_summary() -> dict[str, Any]:
    rows = list_backups()
    latest = rows[0] if rows else {}
    return {
        "count": len(rows),
        "latest_id": latest.get("backup_id", ""),
        "latest_at": latest.get("created_at", ""),
        "latest_reason": latest.get("reason", ""),
        "total_size": sum(int(row.get("total_size", 0) or 0) for row in rows),
    }


def restore_backup(
    backup_id: str,
    *,
    asset_keys: Iterable[str] | None = None,
    create_restore_point: bool = True,
) -> dict[str, Any]:
    bid, backup_dir = _backup_dir_for_id(backup_id)
    manifest_path = backup_dir / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"找不到备份: {bid}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    keys = set(str(k) for k in asset_keys) if asset_keys is not None else None
    if create_restore_point:
        create_backup(reason=f"before_restore:{bid}", max_keep=30)

    restored: list[dict[str, Any]] = []
    for asset in manifest.get("assets", []):
        if keys is not None and str(asset.get("key")) not in keys:
            continue
        if not asset.get("exists"):
            continue
        rel_path = str(asset.get("path", ""))
        if not rel_path:
            continue
        src = backup_dir / "payload" / rel_path
        dst = PROJECT_ROOT / rel_path
        if not src.exists():
            continue
        if asset.get("type") == "file":
            dst.parent.mkdir(parents=True, exist_ok=True)
            tmp = dst.with_suffix(dst.suffix + ".restore_tmp")
            shutil.copy2(src, tmp)
            tmp.replace(dst)
        else:
            for fp in src.rglob("*"):
                if not fp.is_file() or _should_skip(fp):
                    continue
                target = dst / fp.relative_to(src)
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(fp, target)
        restored.append({"key": asset.get("key"), "path": rel_path, "type": asset.get("type")})
    return {"backup_id": bid, "restored_at": _now_text(), "restored": restored}


def update_backup_note(backup_id: str, note: str) -> dict[str, Any]:
    bid, backup_dir = _backup_dir_for_id(backup_id)
    manifest_path = backup_dir / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"找不到备份: {bid}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["note"] = _safe_note(note)
    manifest["note_updated_at"] = _now_text()
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"backup_id": bid, "note": manifest["note"], "note_updated_at": manifest["note_updated_at"]}


def delete_backup(backup_id: str) -> dict[str, Any]:
    bid, backup_dir = _backup_dir_for_id(backup_id)
    if not backup_dir.exists():
        raise FileNotFoundError(f"找不到备份: {bid}")
    if not backup_dir.is_dir():
        raise ValueError("非法备份路径，拒绝删除")
    manifest_path = backup_dir / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"备份清单缺失，拒绝删除: {bid}")
    size = sum(int(item.get("size", 0) or 0) for item in json.loads(manifest_path.read_text(encoding="utf-8")).get("assets", []))
    shutil.rmtree(backup_dir)
    return {"backup_id": bid, "deleted_at": _now_text(), "size": size}


def prune_backups(max_keep: int = 30) -> int:
    rows = list_backups()
    if max_keep <= 0 or len(rows) <= max_keep:
        return 0
    removed = 0
    for row in rows[max_keep:]:
        bdir = Path(str(row.get("backup_dir", "")))
        if bdir.exists() and bdir.is_dir() and BACKUP_ROOT in bdir.parents:
            shutil.rmtree(bdir, ignore_errors=True)
            removed += 1
    return removed


def format_bytes(size: int | float | None) -> str:
    n = float(size or 0)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if n < 1024 or unit == "TB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{int(n)} B"
        n /= 1024
    return f"{n:.1f} TB"


def _parse_dt(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt_text, fmt in ((text[:19].replace("T", " "), "%Y-%m-%d %H:%M:%S"), (text[:10], "%Y-%m-%d")):
        try:
            return datetime.strptime(fmt_text, fmt)
        except Exception:
            continue
    return None


def _safe_sqlite_count(conn: Any, table: str) -> int | None:
    try:
        return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
    except Exception:
        return None


def _safe_sqlite_scalar(conn: Any, sql: str) -> Any:
    try:
        row = conn.execute(sql).fetchone()
        return row[0] if row else None
    except Exception:
        return None


def _sqlite_tables(conn: Any) -> set[str]:
    try:
        return {str(row[0]) for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    except Exception:
        return set()


def _duckdb_tables(conn: Any) -> set[str]:
    try:
        return {str(row[0]) for row in conn.execute("SHOW TABLES").fetchall()}
    except Exception:
        return set()


def _asset_status(label: str, path: Path) -> dict[str, Any]:
    exists = path.exists()
    return {
        "label": label,
        "path": str(path.relative_to(PROJECT_ROOT) if path.is_absolute() else path),
        "exists": exists,
        "size": path.stat().st_size if exists and path.is_file() else (_dir_size(path) if exists and path.is_dir() else 0),
        "status": "正常" if exists else "缺失",
        "issues": [] if exists else ["文件不存在"],
    }


def get_local_data_asset_status() -> dict[str, Any]:
    """Return a data-vault health report for local databases and backup freshness."""
    import sqlite3

    assets: list[dict[str, Any]] = []
    issues: list[str] = []

    filter_db = PROJECT_ROOT / "apps/filter/data/filter_market.db"
    filter_asset = _asset_status("大过滤器 SQLite", filter_db)
    filter_asset.update(
        {
            "engine": "SQLite",
            "market_snapshot_rows": None,
            "enrichment_rows": None,
            "a_enrichment_rows": None,
            "hk_enrichment_rows": None,
            "hk_classification_rows": None,
            "latest_snapshot_at": "",
            "latest_enriched_at": "",
            "tables": [],
        }
    )
    if filter_db.exists():
        try:
            conn = sqlite3.connect(str(filter_db))
            try:
                tables = _sqlite_tables(conn)
                filter_asset["tables"] = sorted(tables)
                required = {"market_snapshot", "stock_enrichment_latest", "snapshot_meta"}
                missing_tables = sorted(required - tables)
                if missing_tables:
                    filter_asset["issues"].append("缺表: " + ", ".join(missing_tables))
                if "market_snapshot" in tables:
                    filter_asset["market_snapshot_rows"] = _safe_sqlite_count(conn, "market_snapshot")
                    filter_asset["latest_snapshot_at"] = str(
                        _safe_sqlite_scalar(conn, "SELECT meta_value FROM snapshot_meta WHERE meta_key='last_update'") or ""
                    )
                if "stock_enrichment_latest" in tables:
                    filter_asset["enrichment_rows"] = _safe_sqlite_count(conn, "stock_enrichment_latest")
                    filter_asset["a_enrichment_rows"] = int(
                        _safe_sqlite_scalar(conn, "SELECT COUNT(*) FROM stock_enrichment_latest WHERE market='A'") or 0
                    )
                    filter_asset["hk_enrichment_rows"] = int(
                        _safe_sqlite_scalar(conn, "SELECT COUNT(*) FROM stock_enrichment_latest WHERE market='HK'") or 0
                    )
                    filter_asset["latest_enriched_at"] = str(
                        _safe_sqlite_scalar(conn, "SELECT MAX(enriched_at) FROM stock_enrichment_latest") or ""
                    )
                if "hk_classification" in tables:
                    filter_asset["hk_classification_rows"] = _safe_sqlite_count(conn, "hk_classification")
            finally:
                conn.close()
        except Exception as exc:
            filter_asset["status"] = "异常"
            filter_asset["issues"].append(str(exc))
    if filter_asset.get("market_snapshot_rows") == 0:
        filter_asset["issues"].append("快照为空")
    if filter_asset.get("enrichment_rows") == 0:
        filter_asset["issues"].append("深补为空")
    if filter_asset["issues"]:
        filter_asset["status"] = "异常" if any("缺失" in x or "缺表" in x or "为空" in x for x in filter_asset["issues"]) else "注意"
    assets.append(filter_asset)

    duck_path = PROJECT_ROOT / "data/quant_system.duckdb"
    duck_asset = _asset_status("DuckDB 本地底座", duck_path)
    duck_asset.update({"engine": "DuckDB", "tables": [], "table_counts": {}, "issues": duck_asset.get("issues", [])})
    if duck_path.exists():
        try:
            import duckdb

            conn = duckdb.connect(str(duck_path), read_only=True)
            try:
                tables = _duckdb_tables(conn)
                duck_asset["tables"] = sorted(tables)
                required = {"stock_basic", "daily_kline", "daily_fundamental"}
                missing_tables = sorted(required - tables)
                if missing_tables:
                    duck_asset["issues"].append("缺表: " + ", ".join(missing_tables))
                for table in sorted(required | ({"positions"} & tables)):
                    if table in tables:
                        try:
                            duck_asset["table_counts"][table] = int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                        except Exception:
                            duck_asset["table_counts"][table] = None
            finally:
                conn.close()
        except Exception as exc:
            duck_asset["status"] = "异常"
            duck_asset["issues"].append(str(exc))
    if duck_asset.get("issues"):
        duck_asset["status"] = "异常" if any("缺失" in x or "缺表" in x for x in duck_asset["issues"]) else "注意"
    assets.append(duck_asset)

    summary = get_backup_summary()
    latest_dt = _parse_dt(summary.get("latest_at"))
    backup_age_days = (datetime.now() - latest_dt).days if latest_dt else None
    backup_status = "正常"
    if not latest_dt:
        backup_status = "缺失"
        issues.append("尚无完整备份")
    elif backup_age_days is not None and backup_age_days > 7:
        backup_status = "过旧"
        issues.append(f"最近备份已超过 {backup_age_days} 天")

    for asset in assets:
        for issue in asset.get("issues", []):
            issues.append(f"{asset.get('label')}: {issue}")

    if any(asset.get("status") == "异常" for asset in assets) or backup_status == "缺失":
        overall = "异常"
    elif backup_status == "过旧" or any(asset.get("status") == "注意" for asset in assets):
        overall = "注意"
    else:
        overall = "正常"

    return {
        "overall_status": overall,
        "assets": assets,
        "backup": {**summary, "status": backup_status, "age_days": backup_age_days},
        "issues": issues,
    }
