"""
Willitude Data MCP Server

Primary UX for quant researchers:
- Global deduplicated cache for Tardis (crypto) + Databento (traditional markets) using AWS SSM keys.
- register_project() once per work folder.
- materialize_*_to_project(): main tool. Ensures globally + creates symlinks (or copies) + manifest inside the project.
- Your notebooks use clean relative paths under data/raw/.
- Low-level ensure_* still available if you only want to touch the global cache.

stdio transport (no ports). Works great from Cursor, Claude Desktop, Windsurf, etc.

Run with: AWS_PROFILE=YongseokMacProfile uv run willitude-mcp
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

from mcp.server.fastmcp import Context, FastMCP

from .cache import CacheManager
from .config import get_config
from .databento import DatabentoDataClient
from .ssm import get_key_provider
from .tardis import TardisDataClient

# Logging to stderr so it doesn't pollute stdio MCP transport
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("willitude_mcp")

mcp = FastMCP(
    name="WillitudeData",
    instructions=(
        "You are an AI agent connected to WillitudeData MCP (Tardis + Databento market data). "
        "FOLLOW THESE RULES STRICTLY: "
        "1. If the human mentions a research/project folder, call register_project with the absolute path FIRST. "
        "2. Prefer the high-level materialize_*_to_project tools. "
        "3. After success, always extract 'project_paths' from the JSON result and give the user clean relative loading code. "
        "4. Surface the manifest for reproducibility. "
        "Keys come only from SSM at runtime. See the quant_data_workflow prompt for full rules."
    ),
)


# Lazy singletons
_cache: CacheManager | None = None
_tardis: TardisDataClient | None = None
_databento: DatabentoDataClient | None = None

# Simple per-process project context (one researcher + one MCP session typically)
_current_project_root: str | None = None
_current_project_name: str | None = None


def _get_cache() -> CacheManager:
    global _cache
    if _cache is None:
        _cache = CacheManager()
    return _cache


def _get_tardis() -> TardisDataClient:
    global _tardis
    if _tardis is None:
        _tardis = TardisDataClient(cache=_get_cache())
    return _tardis


def _get_databento() -> DatabentoDataClient:
    global _databento
    if _databento is None:
        _databento = DatabentoDataClient(cache=_get_cache())
    return _databento


def _resolve_project_dir(explicit: str | None = None) -> Path | None:
    """Return the active project directory (explicit wins, then registered context)."""
    from pathlib import Path as _Path  # avoid shadowing at top level
    if explicit:
        return _Path(explicit).expanduser().resolve()
    if _current_project_root:
        return _Path(_current_project_root).expanduser().resolve()
    return None


@mcp.tool(
    name="get_cache_status",
    description="Show cache size/location + breakdown by provider. Call first to see what is already local.",
)
def get_cache_status() -> str:
    c = _get_cache()
    summary = c.get_size_summary()
    cached = c.list_cached()
    return json.dumps(
        {
            "cache_root": summary["root"],
            "total_gb": summary["total_gb"],
            "by_provider": summary["by_provider"],
            "num_cached_items": len(cached),
            "sample_items": cached[:8],  # don't overwhelm context
        },
        indent=2,
        ensure_ascii=False,
    )


@mcp.tool(
    name="list_cached_data",
    description="List detailed cached datasets. Filter with provider='tardis' or 'databento'.",
)
def list_cached_data(provider: str | None = None) -> str:
    items = _get_cache().list_cached(provider=provider)
    return json.dumps({"count": len(items), "items": items}, indent=2, ensure_ascii=False)


@mcp.tool(
    name="ensure_tardis_data",
    description=(
        "Ensure Tardis historical data (exchange/symbols/dates/data_types) is downloaded+cached. "
        "Reuses if present unless force=true. Returns paths to raw CSVs + unified.parquet (if enabled). "
        "Ex: exchange='binance', symbols=['BTCUSDT'], data_types=['trades','incremental_book_L2'], "
        "from_date='2024-01-01', to_date='2024-01-05'."
    ),
)
async def ensure_tardis_data(
    exchange: str,
    symbols: list[str],
    from_date: str,
    to_date: str,
    data_types: list[str] | None = None,
    force: bool = False,
    ctx: Context | None = None,
) -> str:
    if ctx:
        await ctx.info(
            f"Ensuring Tardis: {exchange} {symbols} {from_date}..{to_date} types={data_types or ['trades']}"
        )

    client = _get_tardis()
    result = client.ensure_cached(
        exchange=exchange,
        symbols=symbols,
        from_date=from_date,
        to_date=to_date,
        data_types=data_types,
        force=force,
    )
    return json.dumps(result, indent=2, ensure_ascii=False)


@mcp.tool(
    name="ensure_databento_data",
    description=(
        "Fetch + cache Databento data as Parquet. Common datasets: GLBX.MDP3 (futures), XNAS.ITCH etc. "
        "Ex: dataset='GLBX.MDP3', symbols=['ES.FUT'], schema='trades' or 'mbp-1', "
        "start/end as ISO. Returns ready-to-load local Parquet paths."
    ),
)
async def ensure_databento_data(
    dataset: str,
    symbols: list[str],
    schema: str,
    start: str,
    end: str,
    stype_in: str | None = None,
    force: bool = False,
    ctx: Context | None = None,
) -> str:
    if ctx:
        await ctx.info(f"Ensuring Databento: {dataset} {symbols} schema={schema} {start}..{end}")

    client = _get_databento()
    result = client.ensure_cached(
        dataset=dataset,
        symbols=symbols,
        schema=schema,
        start=start,
        end=end,
        stype_in=stype_in,
        force=force,
    )
    return json.dumps(result, indent=2, ensure_ascii=False)


@mcp.tool(
    name="get_data_paths",
    description=(
        "Return concrete local FS paths for a cached item. "
        "When a project is registered (or project_dir provided), also returns convenient relative paths. "
        "Call after ensure_ or materialize_*. Prefer unified.parquet for Tardis."
    ),
)
def get_data_paths(
    provider: str,
    exchange_or_dataset: str,
    symbol: str,
    data_type_or_schema: str,
    project_dir: str | None = None,
) -> str:
    c = _get_cache()
    proj = _resolve_project_dir(project_dir)

    if provider == "tardis":
        raw = c.tardis_dir(exchange_or_dataset, symbol, data_type_or_schema, raw=True)
        unified = c.tardis_unified_parquet(exchange_or_dataset, symbol, data_type_or_schema)
        result = {
            "raw_dir": str(raw),
            "unified_parquet": str(unified) if unified.exists() else None,
            "existing_files": [str(p) for p in raw.glob("*") if p.is_file()],
        }
    elif provider == "databento":
        d = c.databento_dir(exchange_or_dataset, symbol, data_type_or_schema)
        files = [str(p) for p in d.glob("*.parquet")]
        result = {"dir": str(d), "parquet_files": files}
    else:
        result = {"error": f"unknown provider {provider}"}

    if proj:
        result["project_dir"] = str(proj)
        # Best effort: pull any already-materialized paths for this item from the project's manifest
        try:
            manifest_items = c.list_materialized_in_project(proj)
            matching = [
                item for item in manifest_items
                if (provider == "tardis" and item.get("exchange") == exchange_or_dataset and item.get("symbol") == symbol and item.get("data_type") == data_type_or_schema)
                or (provider == "databento" and item.get("dataset") == exchange_or_dataset and item.get("symbol") == symbol and item.get("schema") == data_type_or_schema)
            ]
            if matching:
                result["materialized_in_project"] = matching[0].get("project_paths", [])
        except Exception:
            pass

        # Also try relative hints for cache paths (rarely useful but harmless)
        for k in list(result.keys()):
            val = result.get(k)
            if isinstance(val, str) and str(c.root) in val:
                try:
                    result[f"{k}_relative_to_project"] = str(Path(val).relative_to(proj))
                except Exception:
                    pass

    return json.dumps(result, indent=2)


@mcp.tool(
    name="load_cached_preview",
    description=(
        "Load a tiny preview (head N rows) of any cached Parquet/CSV right in the response. "
        "Use small limit (default 10) for cheap schema checks before heavy analysis."
    ),
)
def load_cached_preview(path: str, limit: int = 10) -> str:
    try:
        df = _get_cache().load_as_polars(path, limit=limit)
        # Return schema + small sample as JSON (avoid huge outputs)
        sample = df.head(min(limit, 20)).to_dicts() if len(df) > 0 else []
        return json.dumps(
            {
                "path": path,
                "rows_in_preview": len(sample),
                "schema": {name: str(dtype) for name, dtype in df.schema.items()},
                "preview": sample,
            },
            indent=2,
            default=str,
        )
    except Exception as exc:
        return json.dumps({"error": str(exc), "path": path})


@mcp.tool(
    name="get_tardis_key_info",
    description=(
        "Diagnostic: check if Tardis key readable from SSM (length only). "
        "Verifies AWS SSO / IAM permissions for the MCP process."
    ),
)
def get_tardis_key_info() -> str:
    try:
        kp = get_key_provider()
        val = kp.get_key("tardis")
        return json.dumps({"provider": "tardis", "status": "ok", "length": len(val)})
    except Exception as exc:
        return json.dumps({"provider": "tardis", "status": "error", "error": str(exc)})


@mcp.tool(
    name="get_databento_key_info",
    description="Diagnostic: check if Databento key is readable from SSM (length only).",
)
def get_databento_key_info() -> str:
    try:
        kp = get_key_provider()
        val = kp.get_key("databento")
        return json.dumps({"provider": "databento", "status": "ok", "length": len(val)})
    except Exception as exc:
        return json.dumps({"provider": "databento", "status": "error", "error": str(exc)})


# ---------------------- Project Context & Materialize (recommended UX for work folders) ----------------------

@mcp.tool(
    name="register_project",
    description=(
        "MANDATORY at the start of any conversation when the user is working inside a specific research/project folder. "
        "Registers the absolute path of the user's current work directory. "
        "After this, materialize_*_to_project will automatically create symlinks (default) or copies under data/raw/ inside that folder, "
        "and write a _willitude_manifest.json for reproducibility. "
        "get_data_paths will also become project-aware. "
        "ALWAYS call this first if the user mentions 'my project', 'this folder', 'the strategy repo', or gives a path. "
        "Example: register_project(project_root='/Users/quant/research/breakout-v3', name='breakout-v3'). "
        "Returns the registered root so you can confirm."
    ),
)
def register_project(project_root: str, name: str | None = None) -> str:
    global _current_project_root, _current_project_name
    from pathlib import Path as _P
    root = _P(project_root).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    _current_project_root = str(root)
    _current_project_name = name or root.name
    return json.dumps({
        "status": "registered",
        "project_root": _current_project_root,
        "name": _current_project_name,
        "note": "Future materialize calls will default to this project. Use explicit project_dir to override."
    }, indent=2)


@mcp.tool(
    name="materialize_tardis_to_project",
    description=(
        "PRIMARY RECOMMENDED TOOL for bringing Tardis data into the user's research project. "
        "It does TWO things in one call: (1) ensures the requested data exists in the global deduplicated cache using SSM keys, "
        "(2) creates symlinks (use_symlinks=true by default) or real copies under the registered project directory (data/raw/tardis/...). "
        "Also appends a detailed entry to data/raw/_willitude_manifest.json (original query, timestamps, source paths). "
        "RULE: Prefer this over ensure_tardis_data whenever the user is working in a project folder. "
        "After success, parse the 'project_paths' array from the JSON result — these are relative paths you should put into the user's code (pl.scan_parquet etc.). "
        "Parameters like exchange, symbols, from_date etc. are the same as ensure_tardis_data. "
        "Use small date ranges first for testing. Returns structured result with ensure_summary + materialized + manifest path + usage_hint."
    ),
)
async def materialize_tardis_to_project(
    exchange: str,
    symbols: list[str],
    from_date: str,
    to_date: str,
    data_types: list[str] | None = None,
    target_subdir: str = "data/raw",
    use_symlinks: bool = True,
    force: bool = False,
    project_dir: str | None = None,
    ctx: Context | None = None,
) -> str:
    if ctx:
        await ctx.info(f"Materializing Tardis {exchange} {symbols} into project (symlinks={use_symlinks})...")

    proj = _resolve_project_dir(project_dir)
    if not proj:
        return json.dumps({
            "error": "No project registered and no project_dir provided. "
                    "Call register_project first or pass project_dir explicitly."
        })

    client = _get_tardis()
    ensure_result = client.ensure_cached(
        exchange=exchange,
        symbols=symbols,
        from_date=from_date,
        to_date=to_date,
        data_types=data_types,
        force=force,
    )

    c = _get_cache()
    materialized: list[dict] = []

    # For each item that was ensured, materialize the best artifact
    for res in ensure_result.get("results", []):
        if res.get("status") == "error":
            continue
        ex = res["exchange"]
        sym = res["symbol"]
        dt = res["data_type"]
        info = c.materialize_tardis(
            exchange=ex,
            symbol=sym,
            data_type=dt,
            target_project_dir=proj,
            target_subdir=target_subdir,
            use_symlinks=use_symlinks,
        )
        materialized.append(info)

    # Write/append manifest
    manifest_path = c.write_manifest(proj, materialized, target_subdir=target_subdir)

    return json.dumps({
        "provider": "tardis",
        "project_dir": str(proj),
        "target_subdir": target_subdir,
        "use_symlinks": use_symlinks,
        "ensure_summary": ensure_result,
        "materialized": materialized,
        "manifest": str(manifest_path),
        "usage_hint": (
            f"Use relative paths under {target_subdir}/tardis/... "
            "(e.g. pl.scan_parquet('data/raw/tardis/...'))"
        )
    }, indent=2, default=str)


@mcp.tool(
    name="materialize_databento_to_project",
    description=(
        "PRIMARY RECOMMENDED TOOL for Databento data. "
        "Combines ensure + materialize: populates global cache if needed, then creates symlinks (default) or copies inside the registered project under data/raw/databento/..., "
        "plus writes provenance to _willitude_manifest.json. "
        "RULE: Use this (not ensure_databento_data) when the user has a project folder. "
        "After calling, take the 'project_paths' from the result (they are relative to the project) and insert clean loading code for the user. "
        "Same selection parameters as ensure_databento_data (dataset, symbols, schema, start, end...). "
        "Returns JSON with materialized info + manifest location + usage_hint."
    ),
)
async def materialize_databento_to_project(
    dataset: str,
    symbols: list[str],
    schema: str,
    start: str,
    end: str,
    stype_in: str | None = None,
    target_subdir: str = "data/raw",
    use_symlinks: bool = True,
    force: bool = False,
    project_dir: str | None = None,
    ctx: Context | None = None,
) -> str:
    if ctx:
        await ctx.info(f"Materializing Databento {dataset} {symbols} {schema} into project...")

    proj = _resolve_project_dir(project_dir)
    if not proj:
        return json.dumps({"error": "No active project. Call register_project(...) or pass project_dir."})

    client = _get_databento()
    ensure_result = client.ensure_cached(
        dataset=dataset,
        symbols=symbols,
        schema=schema,
        start=start,
        end=end,
        stype_in=stype_in,
        force=force,
    )

    c = _get_cache()
    materialized: list[dict] = []

    for res in ensure_result.get("results", []):
        if res.get("status") == "error":
            continue
        info = c.materialize_databento(
            dataset=res["dataset"],
            symbol=res["symbol"],
            schema=res["schema"],
            start=start,
            end=end,
            target_project_dir=proj,
            target_subdir=target_subdir,
            use_symlinks=use_symlinks,
        )
        materialized.append(info)

    manifest_path = c.write_manifest(proj, materialized, target_subdir=target_subdir)

    return json.dumps({
        "provider": "databento",
        "project_dir": str(proj),
        "target_subdir": target_subdir,
        "use_symlinks": use_symlinks,
        "ensure_summary": ensure_result,
        "materialized": materialized,
        "manifest": str(manifest_path),
        "usage_hint": f"Load with relative paths: pl.scan_parquet('{target_subdir}/databento/.../*.parquet')"
    }, indent=2, default=str)


@mcp.tool(
    name="list_data_in_project",
    description="List what data has been materialized into a project (from its _willitude_manifest.json).",
)
def list_data_in_project(project_dir: str | None = None) -> str:
    proj = _resolve_project_dir(project_dir)
    if not proj:
        return json.dumps({"error": "No project context. Pass project_dir or call register_project first."})
    items = _get_cache().list_materialized_in_project(proj)
    return json.dumps({
        "project_dir": str(proj),
        "count": len(items),
        "items": items
    }, indent=2)


@mcp.tool(
    name="create_data_manifest",
    description="Rewrite the _willitude_manifest.json for the project (provenance). Useful after manual file ops.",
)
def create_data_manifest(project_dir: str | None = None) -> str:
    proj = _resolve_project_dir(project_dir)
    if not proj:
        return json.dumps({"error": "No project."})
    c = _get_cache()
    # Re-scan what is actually under the subdir and write a fresh manifest based on current materialized state
    items = c.list_materialized_in_project(proj)  # existing is fine, or we could enhance scanning
    manifest = c.write_manifest(proj, items)
    return json.dumps({"manifest": str(manifest), "entries": len(items)})


@mcp.resource("cache://status")
def cache_status_resource() -> str:
    """MCP Resource exposing current cache summary as text."""
    summary = _get_cache().get_size_summary()
    return (
        f"Willitude cache at {summary['root']}\n"
        f"Total: {summary['total_gb']} GB\n"
        f"{json.dumps(summary['by_provider'], indent=2)}"
    )


@mcp.resource("cache://list")
def cache_list_resource() -> str:
    items = _get_cache().list_cached()
    return json.dumps(items, indent=2)


@mcp.prompt()
def quant_data_workflow() -> str:
    """Strict rules and workflow that the AI agent MUST follow when using WillitudeData MCP for a quant researcher."""

    return """
You are using the WillitudeData MCP server (Tardis crypto + Databento traditional market data).

=== CORE RULES (NEVER VIOLATE) ===

1. **Project Awareness is Mandatory**
   - If the user mentions any specific folder, "my project", "this research", "the strategy folder", "breakout-v3", or provides a path → you MUST call register_project FIRST with the absolute path.
   - Only after register_project succeeds should you call materialize tools.
   - If no project context and user wants data "for this work", ask for the project path and register it.

2. **Preferred Tool = materialize_*_to_project (not the low-level ensure)**
   - When the user wants data for analysis/backtesting/research inside a folder → ALWAYS prefer `materialize_tardis_to_project` or `materialize_databento_to_project`.
   - These tools do "ensure in global cache + bring into project as symlinks + write manifest" atomically.
   - Only fall back to ensure_*_data if the user explicitly says "just populate the global cache, don't touch my project" or "I don't want it in the folder yet".

3. **After materialize success**
   - Look at the returned JSON.
   - Extract the "project_paths" list — these are clean relative paths (e.g. "data/raw/tardis/binance/BTCUSDT/unified.parquet").
   - Immediately propose or insert loading code using those relative paths with polars.scan_parquet / read_parquet.
   - Mention the manifest location so the user has provenance.

4. **Efficiency & Safety**
   - Start with small date ranges (1-3 days) when the user is exploring.
   - Use use_symlinks=true (the default) unless the user wants a self-contained copy (use_symlinks=false).
   - Report disk impact or ask before very large requests.

5. **Reproducibility**
   - The _willitude_manifest.json written in the project's data/raw/ is the source of truth for "what data was used for this experiment".
   - After materializing important datasets, call list_data_in_project() or read the manifest to confirm.

=== RECOMMENDED STEP-BY-STEP FLOW ===

When user says something like "get me Binance BTC L2 for Jan-Feb in my alpha project":

A. Call register_project with the correct absolute path (if not already registered this session).
B. Call get_cache_status() or list_data_in_project() to see what already exists.
C. Call the appropriate materialize_*_to_project with precise parameters.
D. On success:
   - Show the user the relative paths from "project_paths".
   - Offer ready-to-paste Polars (or Pandas/DuckDB) loading code.
   - Optionally call load_cached_preview on one of the new paths for a quick sanity check.
E. If user later asks "what data did we use for this backtest?", call list_data_in_project() and surface the manifest.

=== OTHER USEFUL TOOLS ===

- get_data_paths(...) → use when you need the original global cache location or to double-check.
- load_cached_preview(path, limit=8) → fast schema + sample without leaving the chat.
- list_data_in_project() / create_data_manifest() → for auditing.
- get_*_key_info() → only when you suspect auth/SSM problems.

=== DATA KNOWLEDGE ===

- Tardis: exchange-native symbols (BTCUSDT, BTC-PERPETUAL on Deribit, etc.). Good data_types: trades, incremental_book_L2, quotes, etc.
- Databento: datasets like GLBX.MDP3, symbols often "ES.FUT" or specific contracts, schemas: trades, mbp-1, mbo, ohlcv-*, etc.
- Always prefer the "unified.parquet" (Tardis) or the .parquet files (Databento) for analysis speed.

Return structured JSON from tools and act on "project_paths", "manifest", and "usage_hint" fields.

This pattern gives the researcher maximum efficiency (global dedup cache) + clean project folders + full reproducibility.
"""


def run_server() -> None:
    """Entry point for the `willitude-mcp` CLI and python -m willitude_mcp."""
    cfg = get_config()
    logger.info("Starting WillitudeData MCP server")
    logger.info("Cache root: %s", cfg.cache_dir)
    logger.info(
        "AWS profile: %s | region: %s",
        cfg.aws_profile or "(default chain)",
        cfg.aws_region,
    )

    # Optional early credential check (non-fatal)
    try:
        kp = get_key_provider()
        # Touch both to surface problems early
        _ = kp.get_key("tardis")
        _ = kp.get_key("databento")
        logger.info("SSM keys for Tardis and Databento are accessible.")
    except Exception as exc:
        logger.warning("Could not fetch one or both SSM keys at startup (will retry on first use): %s", exc)

    # Default transport is stdio — perfect for Claude Desktop, Cursor, Windsurf, etc.
    mcp.run()


if __name__ == "__main__":
    run_server()
