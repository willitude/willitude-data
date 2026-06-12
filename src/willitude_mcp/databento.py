"""Databento historical data fetcher and cacher.

Official client: `databento`.
Stores primarily as efficient Parquet (easy for quant analysis).
Raw DBN support can be added later if needed for full fidelity replay.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd  # only for date splitting in smart fetch
import polars as pl

from .cache import CacheEntry, CacheManager
from .ssm import get_databento_key

logger = logging.getLogger(__name__)


def _import_databento():
    try:
        import databento as db  # type: ignore

        return db
    except ImportError as exc:
        raise RuntimeError("databento package is required.") from exc


class DatabentoDataClient:
    def __init__(self, cache: CacheManager | None = None) -> None:
        self.cache = cache or CacheManager()
        self.db = _import_databento()

    def ensure_cached(
        self,
        dataset: str,
        symbols: list[str],
        schema: str,
        start: str,
        end: str,
        *,
        stype_in: str | None = None,
        force: bool = False,
    ) -> dict[str, Any]:
        """
        Smart incremental fetch + daily partitioned storage.

        We store one clean {YYYY-MM-DD}.parquet per day.
        Only missing days (or forced) are downloaded.
        Overlapping or rolling requests (e.g. "last 30 days" repeatedly) become cheap.
        """
        key = get_databento_key()
        client = self.db.Historical(key=key)

        results = []

        for symbol in symbols:
            available = self.cache.get_databento_available_dates(dataset, symbol, schema)
            missing = self.cache.compute_missing_dates(start, end, available)

            if not missing and not force:
                logger.info("Databento cache hit (all days present): %s %s %s %s..%s", dataset, symbol, schema, start, end)
                results.append(
                    {
                        "dataset": dataset,
                        "symbol": symbol,
                        "schema": schema,
                        "status": "cached",
                        "missing_days": 0,
                        "path": str(self.cache.databento_dir(dataset, symbol, schema)),
                    }
                )
                continue

            # S3 read-through for daily databento shards
            if self.cache.is_s3():
                for day in missing[:]:  # copy
                    s3_key = self.cache.s3_databento_key(dataset, symbol, schema, day)
                    daily_path = self.cache.databento_daily_path(dataset, symbol, schema, day)
                    self.cache.download_from_s3(s3_key, daily_path)
                    if daily_path.exists():
                        missing.remove(day)  # now have it

            if not missing and not force:
                logger.info("Databento cache hit (restored from S3): %s %s %s %s..%s", dataset, symbol, schema, start, end)
                results.append(
                    {
                        "dataset": dataset,
                        "symbol": symbol,
                        "schema": schema,
                        "status": "restored_from_s3",
                        "missing_days": 0,
                        "path": str(self.cache.databento_dir(dataset, symbol, schema)),
                    }
                )
                continue

            if force:
                missing = self.cache._date_range(start, end)

            logger.info(
                "Databento smart fetch: %s %s %s %s..%s — %d missing days",
                dataset, symbol, schema, start, end, len(missing)
            )

            # Group missing days into minimal contiguous ranges for efficient SDK calls
            ranges = self.cache.group_into_ranges(missing)

            downloaded_paths = []
            total_size = 0

            for rstart, rend in ranges:
                try:
                    dbn = client.timeseries.get_range(
                        dataset=dataset,
                        symbols=[symbol],
                        schema=schema,
                        start=rstart,
                        end=rend,
                        stype_in=stype_in or "raw_symbol",
                    )

                    # Write daily files (even if the SDK gave us a block)
                    # Simplest robust way: convert to df and split by date
                    try:
                        df = dbn.to_df()
                        if df.empty:
                            continue
                        # Expect a timestamp column; Databento usually has 'ts_event' or similar
                        ts_col = None
                        for c in ("ts_event", "timestamp", "time", "ts"):
                            if c in df.columns:
                                ts_col = c
                                break
                        if ts_col:
                            df["_day"] = pd.to_datetime(df[ts_col]).dt.date
                            for day, day_df in df.groupby("_day"):
                                day_str = str(day)
                                daily_path = self.cache.databento_daily_path(dataset, symbol, schema, day_str)
                                daily_path.parent.mkdir(parents=True, exist_ok=True)
                                pl.from_pandas(day_df.drop(columns=["_day"], errors="ignore")).write_parquet(daily_path, compression="zstd")
                                downloaded_paths.append(str(daily_path))
                                if daily_path.exists():
                                    total_size += daily_path.stat().st_size
                        else:
                            # Fallback: write whole block as one daily-ish file (rare)
                            daily_path = self.cache.databento_daily_path(dataset, symbol, schema, rstart)
                            daily_path.parent.mkdir(parents=True, exist_ok=True)
                            pl.from_pandas(df).write_parquet(daily_path, compression="zstd")
                            downloaded_paths.append(str(daily_path))
                    except Exception:
                        # If to_df fails or no pandas, fall back to writing the whole block to the first day's name
                        daily_path = self.cache.databento_daily_path(dataset, symbol, schema, rstart)
                        daily_path.parent.mkdir(parents=True, exist_ok=True)
                        if hasattr(dbn, "to_parquet"):
                            dbn.to_parquet(str(daily_path))
                        else:
                            df = dbn.to_df()
                            pl.from_pandas(df).write_parquet(daily_path, compression="zstd")
                        downloaded_paths.append(str(daily_path))

                except Exception as exc:
                    logger.exception("Databento block fetch failed for %s %s %s %s..%s", dataset, symbol, schema, rstart, rend)
                    # continue with other blocks

            entry = CacheEntry(
                provider="databento",
                path=str(self.cache.databento_dir(dataset, symbol, schema)),
                size_bytes=total_size,
                created_at=__import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
                meta={
                    "dataset": dataset,
                    "symbol": symbol,
                    "schema": schema,
                    "requested_start": start,
                    "requested_end": end,
                    "fetched_ranges": ranges,
                },
            )
            self.cache.record_download(entry)

            results.append(
                {
                    "dataset": dataset,
                    "symbol": symbol,
                    "schema": schema,
                    "status": "downloaded",
                    "missing_days": len(missing),
                    "fetched_ranges": ranges,
                    "daily_files": len(downloaded_paths),
                    "path": str(self.cache.databento_dir(dataset, symbol, schema)),
                }
            )

        return {
            "provider": "databento",
            "requested": {
                "dataset": dataset,
                "symbols": symbols,
                "schema": schema,
                "start": start,
                "end": end,
            },
            "results": results,
        }

    def list_available(self) -> list[dict[str, Any]]:
        """List cached Databento datasets from disk."""
        return [e for e in self.cache.list_cached(provider="databento")]
