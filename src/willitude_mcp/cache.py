"""Cache manager and helpers for market data.

Global cache layout (under $WILLITUDE_CACHE_DIR or ~/.willitude/willitude-data/):

tardis/
  {exchange}/
    {symbol}/
      {data_type}/
        raw/          # what tardis-dev download_datasets wrote (CSV.gz files)
        unified.parquet   # optional converted version (if enabled)

databento/
  {dataset}/
    {symbol}/
      {schema}/
        {date_range}.parquet

When using with a quant researcher's work folder, we "materialize" references
(symlinks by default) into the project (e.g. under data/raw/). A manifest
is written for provenance so the project feels self-contained and reproducible.
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl

from .config import get_config, get_databento_cache_dir, get_tardis_cache_dir

logger = logging.getLogger(__name__)


@dataclass
class CacheEntry:
    provider: str
    path: str
    size_bytes: int
    created_at: str
    meta: dict[str, Any]


class CacheManager:
    def __init__(self) -> None:
        self.root = get_config().cache_dir
        self.root.mkdir(parents=True, exist_ok=True)
        (self.root / "tardis").mkdir(exist_ok=True)
        (self.root / "databento").mkdir(exist_ok=True)

    def is_s3(self) -> bool:
        cfg = get_config()
        return bool(cfg.s3_cache_bucket)

    def s3_tardis_key(self, exchange: str, symbol: str, data_type: str, *, raw: bool = True) -> str:
        if not self.is_s3():
            return ""
        prefix = get_config().s3_cache_prefix
        base = f"{prefix}/tardis/{self._safe(exchange)}/{self._safe(symbol)}/{self._safe(data_type)}"
        return f"{base}/raw" if raw else base

    def s3_databento_key(self, dataset: str, symbol: str, schema: str, day: str | None = None) -> str:
        if not self.is_s3():
            return ""
        prefix = get_config().s3_cache_prefix
        base = f"{prefix}/databento/{self._safe(dataset)}/{self._safe(symbol)}/{self._safe(schema)}"
        if day:
            return f"{base}/{day}.parquet"
        return base

    def s3_tardis_bar_key(self, exchange: str, symbol: str, freq: str, day: str) -> str:
        if not self.is_s3():
            return ""
        prefix = get_config().s3_cache_prefix
        return f"{prefix}/tardis_bars/{self._safe(exchange)}/{self._safe(symbol)}/{self._safe(freq)}/{day}.parquet"

    def estimate_last_tardis_available(self) -> str:
        """Estimate based on Tardis publication rule: previous day's data published ~06:00 UTC."""
        now = datetime.now(UTC)
        cutoff = now.replace(hour=6, minute=0, second=0, microsecond=0)
        if now < cutoff:
            last = (now - timedelta(days=1)).date()
        else:
            last = now.date()
        return last.isoformat()

    def get_tardis_bar_symbols(self, exchange: str) -> list[str]:
        """List symbols with bars for the exchange, from local or S3."""
        if self.is_s3():
            prefix = f"{get_config().s3_cache_prefix}/tardis_bars/{self._safe(exchange)}/"
            try:
                import boto3
                cfg = get_config()
                s3 = boto3.client("s3", region_name=cfg.s3_region)
                symbols = set()
                paginator = s3.get_paginator('list_objects_v2')
                for page in paginator.paginate(Bucket=cfg.s3_cache_bucket, Prefix=prefix, Delimiter='/'):
                    for cp in page.get('CommonPrefixes', []):
                        sym = cp['Prefix'][len(prefix):].rstrip('/')
                        if sym:
                            symbols.add(sym)
                return sorted(symbols)
            except Exception:
                return []
        else:
            bars_root = get_tardis_cache_dir().parent / "tardis_bars" / self._safe(exchange)
            if not bars_root.exists():
                return []
            return sorted([p.name for p in bars_root.iterdir() if p.is_dir()])

    def get_tardis_symbols(self, exchange: str) -> list[str]:
        """List symbols with any raw data for the exchange, from local or S3."""
        if self.is_s3():
            prefix = f"{get_config().s3_cache_prefix}/tardis/{self._safe(exchange)}/"
            try:
                import boto3
                cfg = get_config()
                s3 = boto3.client("s3", region_name=cfg.s3_region)
                symbols = set()
                paginator = s3.get_paginator('list_objects_v2')
                for page in paginator.paginate(Bucket=cfg.s3_cache_bucket, Prefix=prefix, Delimiter='/'):
                    for cp in page.get('CommonPrefixes', []):
                        sym = cp['Prefix'][len(prefix):].rstrip('/')
                        if sym:
                            symbols.add(sym)
                return sorted(symbols)
            except Exception:
                return []
        else:
            root = get_tardis_cache_dir() / self._safe(exchange)
            if not root.exists():
                return []
            return sorted([p.name for p in root.iterdir() if p.is_dir()])

    def get_latest_tardis_raw_date(self, exchange: str, symbol: str, data_type: str) -> str | None:
        """Latest date from raw files for symbol/data_type, local or S3."""
        if self.is_s3():
            prefix = self.s3_tardis_key(exchange, symbol, data_type, raw=True) + "/"
            try:
                import boto3
                cfg = get_config()
                s3 = boto3.client("s3", region_name=cfg.s3_region)
                dates = set()
                paginator = s3.get_paginator('list_objects_v2')
                for page in paginator.paginate(Bucket=cfg.s3_cache_bucket, Prefix=prefix):
                    for obj in page.get('Contents', []):
                        m = self.DATE_RE.search(obj['Key'])
                        if m:
                            dates.add(m.group(1))
                return max(dates) if dates else None
            except Exception:
                return None
        else:
            base = self.tardis_dir(exchange, symbol, data_type, raw=True)
            dates = set()
            for f in base.rglob("*"):
                m = self.DATE_RE.search(f.name)
                if m:
                    dates.add(m.group(1))
            return max(dates) if dates else None

    def get_latest_tardis_bar_date(self, exchange: str, symbol: str, freq: str) -> str | None:
        """Latest cached bar date for symbol/freq, from local or S3 (no provider call)."""
        if self.is_s3():
            prefix = self.s3_tardis_bar_key(exchange, symbol, freq, "0000-00-00").rsplit("/", 1)[0] + "/"
            try:
                import boto3
                cfg = get_config()
                s3 = boto3.client("s3", region_name=cfg.s3_region)
                dates = set()
                paginator = s3.get_paginator('list_objects_v2')
                for page in paginator.paginate(Bucket=cfg.s3_cache_bucket, Prefix=prefix):
                    for obj in page.get('Contents', []):
                        m = self.DATE_RE.search(obj['Key'])
                        if m:
                            dates.add(m.group(1))
                return max(dates) if dates else None
            except Exception:
                return None
        else:
            d = self.tardis_bars_dir(exchange, symbol, freq)
            dates = sorted(f.stem for f in d.glob("*.parquet"))
            return dates[-1] if dates else None

    def upload_to_s3(self, local_path: Path, s3_key: str) -> None:
        if not self.is_s3() or not local_path.exists():
            return
        try:
            import boto3
        except ImportError:
            logger.warning("boto3 not installed; S3 upload skipped")
            return
        cfg = get_config()
        s3 = boto3.client("s3", region_name=cfg.s3_region)
        if local_path.is_file():
            s3.upload_file(str(local_path), cfg.s3_cache_bucket, s3_key)
        else:
            for f in local_path.rglob("*"):
                if f.is_file():
                    rel = f.relative_to(local_path)
                    key = f"{s3_key}/{rel}"
                    s3.upload_file(str(f), cfg.s3_cache_bucket, key)

    def download_from_s3(self, s3_key: str, local_path: Path) -> None:
        if not self.is_s3():
            return
        try:
            import boto3
        except ImportError:
            logger.warning("boto3 not installed; S3 download skipped")
            return
        cfg = get_config()
        s3 = boto3.client("s3", region_name=cfg.s3_region)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            s3.download_file(cfg.s3_cache_bucket, s3_key, str(local_path))
        except Exception:
            # not found or error, ignore for now
            pass

    def download_from_s3_prefix(self, s3_prefix: str, local_dir: Path) -> None:
        """Download all objects under s3_prefix into local_dir (preserves relative structure)."""
        if not self.is_s3() or not s3_prefix:
            return
        try:
            import boto3
        except ImportError:
            logger.warning("boto3 not installed; S3 prefix download skipped")
            return
        cfg = get_config()
        s3 = boto3.client("s3", region_name=cfg.s3_region)
        local_dir.mkdir(parents=True, exist_ok=True)
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=cfg.s3_cache_bucket, Prefix=s3_prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key == s3_prefix or key.endswith("/"):
                    continue
                rel = key[len(s3_prefix):].lstrip("/")
                if not rel:
                    continue
                local_file = local_dir / rel
                local_file.parent.mkdir(parents=True, exist_ok=True)
                try:
                    s3.download_file(cfg.s3_cache_bucket, key, str(local_file))
                except Exception as e:
                    logger.warning(f"Failed S3 download for {key}: {e}")

    def s3_object_exists(self, s3_key: str) -> bool:
        """Check if a specific S3 object exists (for skipping re-uploads on cache hits)."""
        if not self.is_s3():
            return False
        try:
            import boto3
            cfg = get_config()
            s3 = boto3.client("s3", region_name=cfg.s3_region)
            s3.head_object(Bucket=cfg.s3_cache_bucket, Key=s3_key)
            return True
        except Exception:
            return False

    def s3_prefix_exists(self, s3_prefix: str) -> bool:
        """Check if any objects exist under the prefix (for raw dir backfill decision)."""
        if not self.is_s3():
            return False
        try:
            import boto3
            cfg = get_config()
            s3 = boto3.client("s3", region_name=cfg.s3_region)
            resp = s3.list_objects_v2(Bucket=cfg.s3_cache_bucket, Prefix=s3_prefix, MaxKeys=1)
            return 'Contents' in resp and len(resp.get('Contents', [])) > 0
        except Exception:
            return False

    # ---------- Tardis ----------
    def tardis_dir(self, exchange: str, symbol: str, data_type: str, *, raw: bool = True) -> Path:
        base = get_tardis_cache_dir() / self._safe(exchange) / self._safe(symbol) / self._safe(data_type)
        sub = "raw" if raw else "."
        d = base / sub if raw else base
        d.mkdir(parents=True, exist_ok=True)
        return d

    def tardis_unified_parquet(self, exchange: str, symbol: str, data_type: str) -> Path:
        base = get_tardis_cache_dir() / self._safe(exchange) / self._safe(symbol) / self._safe(data_type)
        base.mkdir(parents=True, exist_ok=True)
        return base / "unified.parquet"

    # ---------- Databento ----------
    def databento_dir(self, dataset: str, symbol: str, schema: str) -> Path:
        d = get_databento_cache_dir() / self._safe(dataset) / self._safe(symbol) / self._safe(schema)
        d.mkdir(parents=True, exist_ok=True)
        return d

    def databento_parquet_path(self, dataset: str, symbol: str, schema: str, start: str, end: str) -> Path:
        """Legacy range-based path (kept for backward compat in listing)."""
        safe_start = start.replace(":", "").replace("T", "_")[:16]
        safe_end = end.replace(":", "").replace("T", "_")[:16]
        return self.databento_dir(dataset, symbol, schema) / f"{safe_start}_{safe_end}.parquet"

    def databento_daily_path(self, dataset: str, symbol: str, schema: str, day: str) -> Path:
        """Preferred: one clean Parquet per day."""
        return self.databento_dir(dataset, symbol, schema) / f"{day}.parquet"

    # ---------- General ----------
    def _safe(self, s: str) -> str:
        return "".join(c if c.isalnum() or c in "-_." else "_" for c in s)

    def record_download(self, entry: CacheEntry) -> None:
        """Append a small manifest entry (best-effort)."""
        manifest = self.root / "manifest.jsonl"
        try:
            with manifest.open("a", encoding="utf-8") as f:
                f.write(json.dumps(asdict(entry), ensure_ascii=False) + "\n")
        except Exception:
            logger.exception("Failed to write manifest entry")

    def list_cached(self, provider: str | None = None) -> list[dict[str, Any]]:
        """Return a summary of what is present on disk."""
        results: list[dict[str, Any]] = []
        if provider in (None, "tardis"):
            tardis_root = get_tardis_cache_dir()
            if tardis_root.exists():
                for exch in sorted(tardis_root.iterdir()):
                    if not exch.is_dir():
                        continue
                    for sym in sorted(exch.iterdir()):
                        if not sym.is_dir():
                            continue
                        for dtype in sorted(sym.iterdir()):
                            if not dtype.is_dir():
                                continue
                            files = list(dtype.rglob("*"))
                            total = sum(f.stat().st_size for f in files if f.is_file())
                            results.append(
                                {
                                    "provider": "tardis",
                                    "exchange": exch.name,
                                    "symbol": sym.name,
                                    "data_type": dtype.name,
                                    "files": len([f for f in files if f.is_file()]),
                                    "size_bytes": total,
                                    "path": str(dtype),
                                }
                            )
        if provider in (None, "databento"):
            db_root = get_databento_cache_dir()
            if db_root.exists():
                for ds in sorted(db_root.iterdir()):
                    if not ds.is_dir():
                        continue
                    for sym in sorted(ds.iterdir()):
                        if not sym.is_dir():
                            continue
                        for sch in sorted(sym.iterdir()):
                            if not sch.is_dir():
                                continue
                            files = [f for f in sch.iterdir() if f.is_file()]
                            total = sum(f.stat().st_size for f in files)
                            results.append(
                                {
                                    "provider": "databento",
                                    "dataset": ds.name,
                                    "symbol": sym.name,
                                    "schema": sch.name,
                                    "files": len(files),
                                    "size_bytes": total,
                                    "path": str(sch),
                                }
                            )
        if provider in (None, "tardis_bars"):
            bars_root = get_tardis_cache_dir().parent / "tardis_bars"
            if bars_root.exists():
                for exch in sorted(bars_root.iterdir()):
                    if not exch.is_dir():
                        continue
                    for sym in sorted(exch.iterdir()):
                        if not sym.is_dir():
                            continue
                        for fr in sorted(sym.iterdir()):
                            if not fr.is_dir():
                                continue
                            files = [f for f in fr.iterdir() if f.is_file()]
                            total = sum(f.stat().st_size for f in files)
                            results.append(
                                {
                                    "provider": "tardis_bars",
                                    "exchange": exch.name,
                                    "symbol": sym.name,
                                    "freq": fr.name,
                                    "files": len(files),
                                    "size_bytes": total,
                                    "path": str(fr),
                                }
                            )
        return results

    def get_size_summary(self) -> dict[str, Any]:
        total = 0
        by_provider: dict[str, int] = {}
        for p in ("tardis", "databento"):
            root = get_tardis_cache_dir() if p == "tardis" else get_databento_cache_dir()
            size = 0
            if root.exists():
                for f in root.rglob("*"):
                    if f.is_file():
                        size += f.stat().st_size
            by_provider[p] = size
            total += size
        return {
            "total_bytes": total,
            "total_gb": round(total / (1024**3), 3),
            "by_provider": {k: {"bytes": v, "gb": round(v / (1024**3), 3)} for k, v in by_provider.items()},
            "root": str(self.root),
        }

    def load_as_polars(self, path: str | Path, *, limit: int | None = None) -> pl.DataFrame:
        """Convenience loader. Supports .parquet and .csv(.gz)."""
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(p)
        if p.suffix == ".parquet" or p.name.endswith(".parquet"):
            df = pl.read_parquet(p)
        else:
            df = pl.read_csv(p, try_parse_dates=True)
        if limit:
            df = df.head(limit)
        return df

    # ---------------------- Smart date-aware caching helpers (for rolling campaigns) ----------------------

    DATE_RE = re.compile(r'(\d{4}-\d{2}-\d{2})')

    def _extract_dates(self, directory: Path) -> set[str]:
        """Scan a directory recursively for YYYY-MM-DD patterns in filenames."""
        dates: set[str] = set()
        if not directory.exists():
            return dates
        for f in directory.rglob("*"):
            if f.is_file():
                m = self.DATE_RE.search(f.name)
                if m:
                    dates.add(m.group(1))
        return dates

    def get_tardis_available_dates(self, exchange: str, symbol: str, data_type: str) -> set[str]:
        """Return set of 'YYYY-MM-DD' strings that have data for this Tardis symbol/type."""
        raw_dir = self.tardis_dir(exchange, symbol, data_type, raw=True)
        return self._extract_dates(raw_dir)

    def get_databento_available_dates(self, dataset: str, symbol: str, schema: str) -> set[str]:
        """Return set of 'YYYY-MM-DD' for which we have daily Parquet files."""
        # We will migrate to daily files; for backward compat also check old range files
        d = self.databento_dir(dataset, symbol, schema)
        dates: set[str] = set()
        if not d.exists():
            return dates
        for f in d.glob("*.parquet"):
            # New style: 2024-06-01.parquet
            m = self.DATE_RE.search(f.stem)
            if m:
                dates.add(m.group(1))
            else:
                # Old range style like 20240601_20240603 -> expand (approximate)
                parts = f.stem.split("_")
                if len(parts) >= 2:
                    try:
                        s = datetime.strptime(parts[0][:8], "%Y%m%d").date()
                        e = datetime.strptime(parts[1][:8], "%Y%m%d").date()
                        cur = s
                        while cur <= e:
                            dates.add(cur.isoformat())
                            cur += timedelta(days=1)
                    except Exception:
                        pass
        return dates

    # ---------- Tardis Bars (aggregated OHLCV + derived metrics) ----------
    def tardis_bars_dir(self, exchange: str, symbol: str, freq: str) -> Path:
        """Directory for bar parquet files (daily partitioned recommended)."""
        d = get_tardis_cache_dir().parent / "tardis_bars" / self._safe(exchange) / self._safe(symbol) / self._safe(freq)
        d.mkdir(parents=True, exist_ok=True)
        return d

    def tardis_bar_path(self, exchange: str, symbol: str, freq: str, day: str) -> Path:
        return self.tardis_bars_dir(exchange, symbol, freq) / f"{day}.parquet"

    def get_tardis_bar_available_dates(self, exchange: str, symbol: str, freq: str) -> set[str]:
        d = self.tardis_bars_dir(exchange, symbol, freq)
        dates: set[str] = set()
        for f in d.glob("*.parquet"):
            m = self.DATE_RE.search(f.stem)
            if m:
                dates.add(m.group(1))
        return dates

    def _date_range(self, from_date: str, to_date: str) -> list[str]:
        """Return inclusive list of 'YYYY-MM-DD' between two dates (inclusive)."""
        start = date.fromisoformat(from_date)
        end = date.fromisoformat(to_date)
        if start > end:
            return []
        days = []
        cur = start
        while cur <= end:
            days.append(cur.isoformat())
            cur += timedelta(days=1)
        return days

    def compute_missing_dates(self, from_date: str, to_date: str, available: set[str]) -> list[str]:
        """Return list of dates in the requested range that are not in available."""
        requested = self._date_range(from_date, to_date)
        return [d for d in requested if d not in available]

    def group_into_ranges(self, dates: list[str]) -> list[tuple[str, str]]:
        """Group a sorted list of dates into minimal contiguous [start, end] ranges."""
        if not dates:
            return []
        dates = sorted(dates)
        ranges = []
        start = dates[0]
        prev = date.fromisoformat(dates[0])
        for d_str in dates[1:]:
            d = date.fromisoformat(d_str)
            if (d - prev).days == 1:
                prev = d
            else:
                ranges.append((start, prev.isoformat()))
                start = d_str
                prev = d
        ranges.append((start, prev.isoformat()))
        return ranges

    # ---------------------- Project Materialize (UX for quant work folders) ----------------------

    def _safe_name(self, s: str) -> str:
        # Reuse the existing _safe implementation for consistency
        return self._safe(s)

    def materialize_tardis(
        self,
        exchange: str,
        symbol: str,
        data_type: str,
        target_project_dir: Path,
        target_subdir: str = "data/raw",
        use_symlinks: bool = True,
    ) -> dict[str, Any]:
        """Create symlink or copy of the best Tardis artifact into the project folder.

        Prefers unified.parquet when available, otherwise links the raw/ directory.
        Returns info including the project-side path.
        """
        source_raw = self.tardis_dir(exchange, symbol, data_type, raw=True)
        source_unified = self.tardis_unified_parquet(exchange, symbol, data_type)

        base_dest = (target_project_dir / target_subdir / "tardis" /
                     self._safe_name(exchange) / self._safe_name(symbol) / self._safe_name(data_type))
        base_dest.mkdir(parents=True, exist_ok=True)

        project_paths = []
        action = "symlink" if use_symlinks else "copy"

        if source_unified.exists():
            dest = base_dest / "unified.parquet"
            self._link_or_copy(source_unified, dest, use_symlinks)
            project_paths.append(str(dest.relative_to(target_project_dir)))
        elif source_raw.exists():
            dest = base_dest / "raw"
            self._link_or_copy_dir(source_raw, dest, use_symlinks)
            project_paths.append(str(dest.relative_to(target_project_dir)))

        info = {
            "provider": "tardis",
            "exchange": exchange,
            "symbol": symbol,
            "data_type": data_type,
            "action": action,
            "project_paths": project_paths,
            "source_cache": str(source_unified if source_unified.exists() else source_raw),
            "target_subdir": target_subdir,
        }
        return info

    def materialize_databento(
        self,
        dataset: str,
        symbol: str,
        schema: str,
        start: str,
        end: str,
        target_project_dir: Path,
        target_subdir: str = "data/raw",
        use_symlinks: bool = True,
    ) -> dict[str, Any]:
        """Materialize Databento Parquet(s) into the project (symlink or copy)."""
        # Find all matching parquet files for this selection (may be range-based files)
        source_dir = self.databento_dir(dataset, symbol, schema)
        if not source_dir.exists():
            return {
                "provider": "databento",
                "status": "no_cached_data",
                "dataset": dataset,
                "symbol": symbol,
                "schema": schema,
            }

        base_dest = (target_project_dir / target_subdir / "databento" /
                     self._safe_name(dataset) / self._safe_name(symbol) / self._safe_name(schema))
        base_dest.mkdir(parents=True, exist_ok=True)

        project_paths: list[str] = []
        action = "symlink" if use_symlinks else "copy"

        for src in sorted(source_dir.glob("*.parquet")):
            # Keep the date-range filename for clarity
            dest = base_dest / src.name
            self._link_or_copy(src, dest, use_symlinks)
            project_paths.append(str(dest.relative_to(target_project_dir)))

        return {
            "provider": "databento",
            "dataset": dataset,
            "symbol": symbol,
            "schema": schema,
            "start": start,
            "end": end,
            "action": action,
            "project_paths": project_paths,
            "source_cache_dir": str(source_dir),
            "target_subdir": target_subdir,
        }

    def _link_or_copy(self, src: Path, dest: Path, use_symlinks: bool) -> None:
        if dest.exists() or dest.is_symlink():
            dest.unlink()
        if use_symlinks:
            os.symlink(src, dest)
        else:
            shutil.copy2(src, dest)

    def _link_or_copy_dir(self, src_dir: Path, dest_dir: Path, use_symlinks: bool) -> None:
        if dest_dir.exists() or dest_dir.is_symlink():
            if dest_dir.is_symlink() or dest_dir.is_file():
                dest_dir.unlink()
            else:
                shutil.rmtree(dest_dir)
        if use_symlinks:
            os.symlink(src_dir, dest_dir)
        else:
            shutil.copytree(src_dir, dest_dir)

    def write_manifest(
        self,
        project_dir: Path,
        entries: list[dict[str, Any]],
        target_subdir: str = "data/raw",
    ) -> Path:
        """Append entries to a project-local manifest for reproducibility."""
        manifest_dir = project_dir / target_subdir
        manifest_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = manifest_dir / "_willitude_manifest.json"

        existing: list[dict] = []
        if manifest_path.exists():
            try:
                existing = json.loads(manifest_path.read_text(encoding="utf-8"))
                if not isinstance(existing, list):
                    existing = []
            except Exception:
                existing = []

        timestamp = datetime.now(UTC).isoformat()
        for e in entries:
            e = dict(e)  # copy
            e.setdefault("materialized_at", timestamp)
            existing.append(e)

        manifest_path.write_text(json.dumps(existing, indent=2, ensure_ascii=False), encoding="utf-8")
        logger.info("Wrote/updated Willitude manifest at %s (%d entries)", manifest_path, len(existing))
        return manifest_path

    def list_materialized_in_project(self, project_dir: Path, target_subdir: str = "data/raw") -> list[dict[str, Any]]:
        """Return what this project has materialized (from its manifest)."""
        manifest_path = project_dir / target_subdir / "_willitude_manifest.json"
        if not manifest_path.exists():
            return []
        try:
            data = json.loads(manifest_path.read_text(encoding="utf-8"))
            return data if isinstance(data, list) else []
        except Exception:
            return []
