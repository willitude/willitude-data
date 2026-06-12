"""Tardis.dev data fetcher and cacher.

Uses the official `tardis-dev` package (download_datasets).
Tardis data is exchange-native tick data (trades, incremental_book_L2 / L3, quotes, etc.).
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import anyio
import polars as pl

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

    async def ensure_cached(
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
                    if self.cache.is_s3():
                        s3_prefix = self.cache.s3_tardis_key(exchange, symbol, dtype, raw=True)
                        self.cache.upload_to_s3(raw_dir, s3_prefix)
                    continue

                # S3 read-through: if S3 enabled, first try to pull existing shards for this symbol/type
                # This enables cross-machine sharing (notebook <-> canary) without re-download from provider.
                if self.cache.is_s3():
                    s3_prefix = self.cache.s3_tardis_key(exchange, symbol, dtype, raw=True)
                    self.cache.download_from_s3_prefix(s3_prefix, raw_dir)
                    # re-evaluate after pull
                    available = self.cache.get_tardis_available_dates(exchange, symbol, dtype)
                    missing = self.cache.compute_missing_dates(from_date, to_date, available)

                if not missing and not force:
                    logger.info(
                        "Tardis smart cache hit (restored from S3): %s %s %s — all %s..%s days present",
                        exchange, symbol, dtype, from_date, to_date
                    )
                    results.append(
                        {
                            "exchange": exchange,
                            "symbol": symbol,
                            "data_type": dtype,
                            "status": "restored_from_s3" if self.cache.is_s3() else "cached",
                            "raw_dir": str(raw_dir),
                            "files": len(list(raw_dir.glob("*.csv*")) + list(raw_dir.glob("*.gz"))),
                            "unified_parquet": str(unified) if unified.exists() else None,
                            "missing_days": 0,
                            "s3_synced": self.cache.is_s3(),
                        }
                    )
                    continue

                # (re)compute minimal contiguous missing blocks (after possible S3 pull)
                ranges = self.cache.group_into_ranges(missing) if missing else [(from_date, to_date)]

                logger.info(
                    "Tardis smart fetch: %s %s %s %s..%s — %d missing days in %d block(s)",
                    exchange, symbol, dtype, from_date, to_date, len(missing), len(ranges)
                )

                for rstart, rend in ranges:
                    # tardis-dev treats to_date as exclusive, while our API is inclusive.
                    # Add one day when calling the library.
                    effective_to = (date.fromisoformat(rend) + timedelta(days=1)).isoformat()

                    try:
                        await anyio.to_thread.run_sync(
                            lambda rs=rstart, re=effective_to: self.download_datasets(
                                exchange=exchange,
                                data_types=[dtype],
                                symbols=[symbol],
                                from_date=rs,
                                to_date=re,
                                api_key=key,
                                download_dir=str(raw_dir),
                                timeout=300,
                                concurrency=12,
                            )
                        )
                    except Exception as exc:
                        logger.warning("Tardis sub-range download issue for %s..%s: %s", rstart, rend, exc)
                        try:
                            await anyio.to_thread.run_sync(
                                lambda rs=rstart, re=effective_to: self.download_datasets(
                                    exchange=exchange,
                                    data_types=[dtype],
                                    symbols=[symbol],
                                    from_date=rs,
                                    to_date=re,
                                    api_key=key,
                                    download_dir=str(raw_dir),
                                    timeout=300,
                                    concurrency=12,
                                )
                            )
                        except Exception as exc2:
                            logger.error("Tardis fallback also failed: %s", exc2)

                # S3 write-through: after provider download (or if no download needed but we want to ensure local is in sync?),
                # upload the (updated) raw dir to S3 so other machines can read-through later.
                if self.cache.is_s3():
                    s3_prefix = self.cache.s3_tardis_key(exchange, symbol, dtype, raw=True)
                    self.cache.upload_to_s3(raw_dir, s3_prefix)
                    # Also record S3 key in manifest for traceability
                    try:
                        s3_entry = CacheEntry(
                            provider="tardis",
                            path=str(raw_dir),
                            size_bytes=0,
                            created_at=__import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
                            meta={
                                "exchange": exchange,
                                "symbol": symbol,
                                "data_type": dtype,
                                "s3_key": s3_prefix,
                                "s3_bucket": get_config().s3_cache_bucket,
                            },
                        )
                        self.cache.record_download(s3_entry)
                    except Exception:
                        pass

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

                post_available = self.cache.get_tardis_available_dates(exchange, symbol, dtype)
                still_missing = self.cache.compute_missing_dates(from_date, to_date, post_available)

                status = "downloaded"
                err = None
                if still_missing and len(missing) > 0:
                    status = "error"
                    err = f"Download completed but {len(still_missing)} requested days are still missing. Last attempted ranges: {ranges}"

                results.append(
                    {
                        "exchange": exchange,
                        "symbol": symbol,
                        "data_type": dtype,
                        "status": status,
                        "raw_dir": str(raw_dir),
                        "files": len(csv_files),
                        "size_bytes": size,
                        "unified_parquet": str(parquet_path) if parquet_path else None,
                        "missing_days": len(still_missing),
                        "error": err,
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

    # ---------- Bars (aggregated time series) ----------
    async def ensure_tardis_bars(
        self,
        exchange: str,
        symbols: list[str],
        from_date: str,
        to_date: str,
        freq: str = "1m",
        *,
        data_types: list[str] | None = None,
        keep_raw: bool = False,
        force: bool = False,
    ) -> dict[str, Any]:
        """
        High-level: ensure raw data for trades + derivative_ticker + liquidations,
        then build time-bar parquet files (OHLCV + vwap + taker flow + funding + OI + liquidations).

        This is the recommended entry point for quant research because raw tick data is too large
        to keep long-term. Bars are stored in a separate global cache (tardis_bars/) so the
        symlink-to-project model remains disk-efficient.

        freq: pandas offset alias (e.g. "1m", "5m", "1h")
        keep_raw=False: delete raw after successful bar creation for the day (recommended for large campaigns)
        """
        if data_types is None:
            data_types = ["trades", "derivative_ticker", "liquidations"]

        # 1. Ensure the raw inputs (smart incremental)
        raw_result = await self.ensure_cached(
            exchange=exchange,
            symbols=symbols,
            from_date=from_date,
            to_date=to_date,
            data_types=data_types,
            force=force,
        )

        results = []
        for symbol in symbols:
            available = self.cache.get_tardis_bar_available_dates(exchange, symbol, freq)
            missing = self.cache.compute_missing_dates(from_date, to_date, available)
            if not missing and not force:
                if self.cache.is_s3():
                    for day in self.cache._date_range(from_date, to_date):
                        bar_path = self.cache.tardis_bar_path(exchange, symbol, freq, day)
                        if bar_path.exists():
                            self.cache.upload_to_s3(bar_path, self.cache.s3_tardis_bar_key(exchange, symbol, freq, day))
                if not keep_raw:
                    for day in self.cache._date_range(from_date, to_date):
                        self._cleanup_raw_for_day(exchange, symbol, data_types, day)
                results.append({
                    "exchange": exchange,
                    "symbol": symbol,
                    "freq": freq,
                    "status": "cached",
                    "missing_days": 0,
                })
                continue

            ranges = self.cache.group_into_ranges(missing) if missing else [(from_date, to_date)]

            for rstart, rend in ranges:
                days = self.cache._date_range(rstart, rend)
                for day in days:
                    try:
                        bar_path = self.cache.tardis_bar_path(exchange, symbol, freq, day)
                        s3_key = self.cache.s3_tardis_bar_key(exchange, symbol, freq, day) if self.cache.is_s3() else None

                        # S3 read-through for bar
                        if s3_key:
                            self.cache.download_from_s3(s3_key, bar_path)

                        if bar_path.exists() and not force:
                            # already have (from S3 or previous)
                            if not keep_raw:
                                self._cleanup_raw_for_day(exchange, symbol, data_types, day)
                            results.append({
                                "exchange": exchange,
                                "symbol": symbol,
                                "freq": freq,
                                "day": day,
                                "status": "cached_from_s3" if s3_key else "cached",
                                "rows": 0,  # unknown without loading
                            })
                            continue

                        bar_df = self._build_bar_for_day(exchange, symbol, day, freq)
                        if bar_df is not None and len(bar_df) > 0:
                            bar_path.parent.mkdir(parents=True, exist_ok=True)
                            bar_df.write_parquet(bar_path, compression="zstd")

                            # S3 write-through for bar (bars are small, always keep in sync)
                            if s3_key:
                                self.cache.upload_to_s3(bar_path, s3_key)

                            # Record bar with S3 key in manifest for traceability
                            if s3_key:
                                try:
                                    bar_entry = CacheEntry(
                                        provider="tardis_bars",
                                        path=str(bar_path),
                                        size_bytes=bar_path.stat().st_size if bar_path.exists() else 0,
                                        created_at=__import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
                                        meta={
                                            "exchange": exchange,
                                            "symbol": symbol,
                                            "freq": freq,
                                            "day": day,
                                            "s3_key": s3_key,
                                            "s3_bucket": get_config().s3_cache_bucket,
                                        },
                                    )
                                    self.cache.record_download(bar_entry)
                                except Exception:
                                    pass

                            if not keep_raw:
                                # Clean raw for this day if requested
                                self._cleanup_raw_for_day(exchange, symbol, data_types, day)

                            results.append({
                                "exchange": exchange,
                                "symbol": symbol,
                                "freq": freq,
                                "day": day,
                                "status": "built",
                                "rows": len(bar_df),
                                "s3_key": s3_key or None,
                            })
                        else:
                            results.append({
                                "exchange": exchange,
                                "symbol": symbol,
                                "freq": freq,
                                "day": day,
                                "status": "error",
                                "error": "No bar rows produced (possible download failure or no trades that day)",
                            })
                    except Exception as exc:
                        logger.exception("Failed to build bar for %s %s %s %s", exchange, symbol, freq, day)
                        results.append({
                            "exchange": exchange,
                            "symbol": symbol,
                            "freq": freq,
                            "day": day,
                            "status": "error",
                            "error": str(exc),
                        })

        return {
            "provider": "tardis_bars",
            "requested": {
                "exchange": exchange,
                "symbols": symbols,
                "from_date": from_date,
                "to_date": to_date,
                "freq": freq,
                "keep_raw": keep_raw,
            },
            "results": results,
        }

    def _build_bar_for_day(self, exchange: str, symbol: str, day: str, freq: str) -> pl.DataFrame | None:
        """Build one day's bar dataframe from the three raw sources.
        Matches verified logic from willitude/scripts/build_panel.py process_symbol_day:
        - Proper us epoch parsing (done in _load)
        - Ticker fields keep nulls (consumer can ffill)
        - Liquidation notional = price * amount, split by side
        - Include predicted_funding_rate
        """
        # Load raw
        trades_dir = self.cache.tardis_dir(exchange, symbol, "trades", raw=True)
        ticker_dir = self.cache.tardis_dir(exchange, symbol, "derivative_ticker", raw=True)
        liq_dir = self.cache.tardis_dir(exchange, symbol, "liquidations", raw=True)

        trades = self._load_tardis_day_files(trades_dir, day)
        ticker = self._load_tardis_day_files(ticker_dir, day)
        liqs = self._load_tardis_day_files(liq_dir, day)

        if trades is None or len(trades) == 0:
            return None

        # Basic trade -> bar
        trades = trades.with_columns(
            pl.col("timestamp").dt.truncate(freq).alias("bar_time"),
            (pl.col("price") * pl.col("amount")).alias("notional"),
            (pl.col("side") == "buy").alias("is_buy"),   # keep as boolean
        )

        bars = (
            trades.group_by("bar_time")
            .agg(
                open=pl.col("price").first(),
                high=pl.col("price").max(),
                low=pl.col("price").min(),
                close=pl.col("price").last(),
                volume=pl.col("amount").sum(),
                vwap=(pl.col("notional").sum() / pl.col("amount").sum()),
                taker_buy_volume=pl.col("amount").filter(pl.col("is_buy")).sum(),
                taker_sell_volume=pl.col("amount").filter(~pl.col("is_buy")).sum(),
            )
            .sort("bar_time")
        )

        # Join derivative_ticker - last value per bar, keep nulls for missing ticks
        if ticker is not None and len(ticker) > 0:
            ticker = ticker.with_columns(pl.col("timestamp").dt.truncate(freq).alias("bar_time"))
            keep_cols = [c for c in ticker.columns if c in (
                "funding_rate", "predicted_funding_rate", "open_interest",
                "index_price", "mark_price"
            )]
            if keep_cols:
                ticker_agg = ticker.group_by("bar_time").agg([pl.col(c).last() for c in keep_cols])
                bars = bars.join(ticker_agg, on="bar_time", how="left")
                # Do NOT fill_null(0) here — nulls are semantically important for OI/funding

        # Join liquidations — notional = price * amount, split buy/sell
        if liqs is not None and len(liqs) > 0:
            liqs = liqs.with_columns(
                pl.col("timestamp").dt.truncate(freq).alias("bar_time"),
                (pl.col("price") * pl.col("amount")).alias("liq_notional"),
                (pl.col("side") == "buy").alias("liq_buy"),
            )
            liq_agg = (
                liqs.group_by("bar_time")
                .agg(
                    liquidation_buy_notional=pl.col("liq_notional").filter(pl.col("liq_buy")).sum(),
                    liquidation_sell_notional=pl.col("liq_notional").filter(~pl.col("liq_buy")).sum(),
                    liquidation_count=pl.len(),
                )
            )
            bars = bars.join(liq_agg, on="bar_time", how="left")

        # Only fill volume/taker fields that are naturally zero when no trades in the bar
        # Ticker and liq fields stay null if no data
        vol_cols = ["volume", "taker_buy_volume", "taker_sell_volume"]
        bars = bars.with_columns([pl.col(c).fill_null(0) for c in vol_cols if c in bars.columns])

        return bars

    def _load_tardis_day_files(self, base_dir: Path, day: str) -> pl.DataFrame | None:
        """Load all files for a given day from a raw tardis directory.
        Tardis CSVs use microsecond integer timestamps (epoch us), not strings.
        We explicitly parse with from_epoch(time_unit='us').
        """
        if not base_dir.exists():
            return None
        day_files = [f for f in base_dir.rglob("*") if day in f.name and f.suffix in (".csv", ".gz", ".parquet")]
        if not day_files:
            return None

        frames = []
        for f in sorted(day_files):
            try:
                if f.suffix == ".parquet" or "parquet" in f.name:
                    df = pl.read_parquet(f)
                else:
                    df = pl.read_csv(f)
                # Convert microsecond epoch integers to datetime
                for ts_col in ("timestamp", "local_timestamp"):
                    if ts_col in df.columns:
                        df = df.with_columns(
                            pl.from_epoch(pl.col(ts_col), time_unit="us").alias(ts_col)
                        )
                frames.append(df)
            except Exception as e:
                logger.warning("Failed to read %s: %s", f, e)
        if not frames:
            return None
        return pl.concat(frames, how="diagonal_relaxed").sort("timestamp")

    def _cleanup_raw_for_day(self, exchange: str, symbol: str, data_types: list[str], day: str):
        """Delete raw files for a specific day (used when keep_raw=False)."""
        for dtype in data_types:
            raw_dir = self.cache.tardis_dir(exchange, symbol, dtype, raw=True)
            for f in raw_dir.rglob("*"):
                if day in f.name:
                    try:
                        f.unlink()
                    except Exception:
                        pass
            # Remove empty day dirs if any
            try:
                for p in sorted(raw_dir.rglob("*"), reverse=True):
                    if p.is_dir() and not any(p.iterdir()):
                        p.rmdir()
            except Exception:
                pass
