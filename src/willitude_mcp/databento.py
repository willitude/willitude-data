"""Databento historical data fetcher and cacher.

Official client: `databento`.
Stores primarily as efficient Parquet (easy for quant analysis).
Raw DBN support can be added later if needed for full fidelity replay.
"""

from __future__ import annotations

import logging
from typing import Any

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
        Fetch Databento timeseries data and cache it as Parquet.

        Common datasets: "GLBX.MDP3" (CME), "XNAS.ITCH", "OPRA.PITCH", etc.
        Schemas: "trades", "mbo", "mbp-1", "mbp-10", "ohlcv-1s", "ohlcv-1m", "trades", ...
        symbols: e.g. ["ES.FUT", "NQ.FUT"] or specific contract like "ESH4"
        start/end: ISO like "2024-01-01T00:00:00" or "2024-01-01"
        """
        key = get_databento_key()
        client = self.db.Historical(key=key)

        results = []

        for symbol in symbols:
            target = self.cache.databento_parquet_path(dataset, symbol, schema, start, end)

            if target.exists() and not force:
                size = target.stat().st_size
                logger.info("Databento cache hit: %s %s %s -> %s", dataset, symbol, schema, target)
                results.append(
                    {
                        "dataset": dataset,
                        "symbol": symbol,
                        "schema": schema,
                        "status": "cached",
                        "path": str(target),
                        "size_bytes": size,
                    }
                )
                continue

            logger.info(
                "Requesting Databento %s %s %s %s..%s",
                dataset,
                symbol,
                schema,
                start,
                end,
            )

            try:
                # get_range returns a DBNStore
                dbn = client.timeseries.get_range(
                    dataset=dataset,
                    symbols=[symbol],
                    schema=schema,
                    start=start,
                    end=end,
                    stype_in=stype_in or "raw_symbol",  # common default
                )

                # Materialize to Parquet (zstd for good compression + speed)
                target.parent.mkdir(parents=True, exist_ok=True)
                # Preferred: convert via polars or use built-in
                try:
                    # DBNStore has to_parquet in recent versions
                    if hasattr(dbn, "to_parquet"):
                        dbn.to_parquet(str(target))  # type: ignore[attr-defined]
                    else:
                        # Fallback: to_df then polars
                        df = dbn.to_df()
                        pl.from_pandas(df).write_parquet(target, compression="zstd")
                except Exception:
                    # Another fallback path
                    df = dbn.to_df()
                    pl.from_pandas(df).write_parquet(target, compression="zstd")

                size = target.stat().st_size
                entry = CacheEntry(
                    provider="databento",
                    path=str(target),
                    size_bytes=size,
                    created_at=__import__("datetime").datetime.now(
                        __import__("datetime").timezone.utc
                    ).isoformat(),
                    meta={
                        "dataset": dataset,
                        "symbol": symbol,
                        "schema": schema,
                        "start": start,
                        "end": end,
                    },
                )
                self.cache.record_download(entry)

                results.append(
                    {
                        "dataset": dataset,
                        "symbol": symbol,
                        "schema": schema,
                        "status": "downloaded",
                        "path": str(target),
                        "size_bytes": size,
                    }
                )
            except Exception as exc:
                logger.exception("Databento fetch failed for %s %s %s", dataset, symbol, schema)
                results.append(
                    {
                        "dataset": dataset,
                        "symbol": symbol,
                        "schema": schema,
                        "status": "error",
                        "error": str(exc),
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
