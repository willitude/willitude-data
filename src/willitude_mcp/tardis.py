"""Tardis.dev data fetcher and cacher.

Uses the official `tardis-dev` package (download_datasets).
Tardis data is exchange-native tick data (trades, incremental_book_L2 / L3, quotes, etc.).
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from .cache import CacheEntry, CacheManager
from .config import get_config, get_tardis_cache_dir
from .ssm import get_tardis_key

logger = logging.getLogger(__name__)


def _import_tardis():
    try:
        from tardis_dev import download_datasets  # type: ignore
        return download_datasets
    except ImportError as exc:
        raise RuntimeError(
            "tardis-dev package is required. It should have been installed via uv."
        ) from exc


class TardisDataClient:
    def __init__(self, cache: CacheManager | None = None) -> None:
        self.cache = cache or CacheManager()
        self.download_datasets = _import_tardis()
        self.cfg = get_config()

    def ensure_cached(
        self,
        exchange: str,
        symbols: list[str],
        from_date: str,
        to_date: str,
        data_types: list[str] | None = None,
        *,
        force: bool = False,
    ) -> dict[str, Any]:
        """
        Download (or reuse) Tardis historical data for the requested range.

        Args:
            exchange: e.g. "binance", "bybit", "deribit", "okex"
            symbols: list of symbols, e.g. ["BTCUSDT", "ETHUSDT"] (use exchange native format)
            from_date, to_date: "YYYY-MM-DD" (inclusive)
            data_types: list like ["trades", "incremental_book_L2", "quotes"]. Default: ["trades"]
            force: if True, re-download even if files look present.

        Returns:
            Summary dict with paths and stats.
        """
        if data_types is None:
            data_types = ["trades"]

        key = get_tardis_key()
        results: list[dict[str, Any]] = []

        for symbol in symbols:
            for dtype in data_types:
                raw_dir = self.cache.tardis_dir(exchange, symbol, dtype, raw=True)
                unified = self.cache.tardis_unified_parquet(exchange, symbol, dtype)

                available = self.cache.get_tardis_available_dates(exchange, symbol, dtype)
                missing = self.cache.compute_missing_dates(from_date, to_date, available)

                if not missing and not force:
                    logger.info(
                        "Tardis smart cache hit: %s %s %s — all %s..%s days present",
                        exchange, symbol, dtype, from_date, to_date
                    )
                    results.append(
                        {
                            "exchange": exchange,
                            "symbol": symbol,
                            "data_type": dtype,
                            "status": "cached",
                            "raw_dir": str(raw_dir),
                            "files": len(list(raw_dir.glob("*.csv*")) + list(raw_dir.glob("*.gz"))),
                            "unified_parquet": str(unified) if unified.exists() else None,
                            "missing_days": 0,
                        }
                    )
                    continue

                # Compute minimal contiguous missing blocks to avoid re-downloading known days
                ranges = self.cache.group_into_ranges(missing) if missing else [(from_date, to_date)]

                logger.info(
                    "Tardis smart fetch: %s %s %s %s..%s — %d missing days in %d block(s)",
                    exchange, symbol, dtype, from_date, to_date, len(missing), len(ranges)
                )

                for rstart, rend in ranges:
                    try:
                        self.download_datasets(
                            exchange=exchange,
                            data_types=[dtype],
                            symbols=[symbol],
                            from_date=rstart,
                            to_date=rend,
                            api_key=key,
                            download_dir=str(raw_dir.parent),
                        )
                    except Exception as exc:
                        logger.warning("Tardis sub-range download issue for %s..%s: %s", rstart, rend, exc)
                        try:
                            self.download_datasets(
                                exchange=exchange,
                                data_types=[dtype],
                                symbols=[symbol],
                                from_date=rstart,
                                to_date=rend,
                                api_key=key,
                                download_dir=str(raw_dir),
                            )
                        except Exception as exc2:
                            logger.error("Tardis fallback also failed: %s", exc2)

                # Post-process: count + optional convert to single parquet for convenience
                files_after = list(raw_dir.glob("**/*")) if raw_dir.exists() else []
                csv_files = [
                    f for f in files_after if f.is_file() and (f.suffix in {".csv", ".gz"} or ".csv" in f.name)
                ]
                size = sum(f.stat().st_size for f in csv_files)

                entry = CacheEntry(
                    provider="tardis",
                    path=str(raw_dir),
                    size_bytes=size,
                    created_at=__import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
                    meta={
                        "exchange": exchange,
                        "symbol": symbol,
                        "data_type": dtype,
                        "from_date": from_date,
                        "to_date": to_date,
                    },
                )
                self.cache.record_download(entry)

                parquet_path = None
                if self.cfg.convert_tardis_to_parquet and csv_files:
                    parquet_path = self._convert_to_parquet(csv_files, unified)
                    if parquet_path:
                        logger.info("Converted to unified parquet: %s", parquet_path)

                results.append(
                    {
                        "exchange": exchange,
                        "symbol": symbol,
                        "data_type": dtype,
                        "status": "downloaded",
                        "raw_dir": str(raw_dir),
                        "files": len(csv_files),
                        "size_bytes": size,
                        "unified_parquet": str(parquet_path) if parquet_path else None,
                        "missing_days": len(missing) if "missing" in locals() else len(self.cache.compute_missing_dates(from_date, to_date, set())),
                    }
                )

        return {
            "provider": "tardis",
            "requested": {
                "exchange": exchange,
                "symbols": symbols,
                "from_date": from_date,
                "to_date": to_date,
                "data_types": data_types,
            },
            "results": results,
        }

    def _convert_to_parquet(self, csv_files: list[Path], target: Path) -> Path | None:
        """Concat multiple daily CSVs into one Polars parquet (best effort, keeps original columns)."""
        try:
            import polars as pl

            frames = []
            for f in sorted(csv_files):
                # Tardis CSVs usually have header; some are gz
                try:
                    df = pl.read_csv(f, try_parse_dates=True, infer_schema_length=1000)
                    frames.append(df)
                except Exception as e:
                    logger.warning("Skipping unreadable Tardis file %s: %s", f, e)
            if not frames:
                return None
            big = pl.concat(frames, how="diagonal_relaxed")
            # Sort by common timestamp columns if present
            for ts_col in ("timestamp", "local_timestamp", "time"):
                if ts_col in big.columns:
                    big = big.sort(ts_col)
                    break
            target.parent.mkdir(parents=True, exist_ok=True)
            big.write_parquet(target, compression="zstd")
            return target
        except Exception as exc:
            logger.exception("Failed to convert Tardis CSVs to parquet: %s", exc)
            return None

    def list_available_for(self, exchange: str, symbol: str) -> dict[str, Any]:
        """List what data types we have cached for a given exchange/symbol."""
        base = get_tardis_cache_dir() / self.cache._safe(exchange) / self.cache._safe(symbol)
        if not base.exists():
            return {"exchange": exchange, "symbol": symbol, "data_types": []}
        dtypes = []
        for p in base.iterdir():
            if p.is_dir():
                has_files = any(f.is_file() for f in p.rglob("*"))
                dtypes.append({"data_type": p.name, "has_files": has_files, "path": str(p)})
        return {"exchange": exchange, "symbol": symbol, "data_types": dtypes}
