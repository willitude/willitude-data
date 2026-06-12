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
import os
import shutil
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


def get_user_context(ctx: Context) -> dict:
    """Extract identity from API Gateway injected headers (symmetric with other willitude MCPs).
    For HTTP transport, uses ctx.request_context.request.headers (Starlette, case-insensitive).
    For local (stdio), falls back to unknown.
    """
    if ctx is None or not hasattr(ctx, "request_context"):
        return {"role": "unknown", "user_id": None, "team": None}
    req_ctx = ctx.request_context
    request = getattr(req_ctx, "request", None)
    if request is not None:
        headers = getattr(request, "headers", {}) or {}
    else:
        headers = getattr(req_ctx, "headers", {}) or {}
    # Headers object or dict; get works case-insensitively for Starlette Headers
    def get_h(k: str):
        if hasattr(headers, "get"):
            v = headers.get(k)
            if v is None:
                v = headers.get(k.lower())
            return v
        return headers.get(k) if isinstance(headers, dict) else None
    transport = "http" if request is not None else "stdio"
    return {
        "user_id": get_h("x-user-id"),
        "role": get_h("x-user-role") or "unknown",
        "team": get_h("x-team"),
        "transport": transport,
    }


def require_role(user: dict, allowed: set[str]):
    role = user.get("role", "unknown")
    transport = user.get("transport", "stdio")
    if transport == "stdio":
        return  # local stdio: bypass (fail-open only for local)
    # HTTP/remote: fail-closed. Unknown or missing role means auth failed -> deny.
    if role == "unknown" or role not in allowed:
        raise PermissionError(f"Role '{role}' not authorized for HTTP transport. Allowed: {allowed}")


mcp = FastMCP(
    name="WillitudeData",
    instructions=(
        "You are an AI agent connected to WillitudeData MCP (Tardis + Databento market data). "
        "FOLLOW THESE RULES STRICTLY: "
        "1. If the human mentions a research/project folder, call register_project with the absolute path FIRST. "
        "2. Prefer the high-level materialize_*_to_project tools. "
        "3. After success, always extract 'project_paths' from the JSON result and give the user clean relative loading code. "
        "4. Surface the manifest for reproducibility. "
        "Keys come only from SSM at runtime. See the quant_data_workflow prompt for full rules.\n\n"
        "**Authentication (remote/SSE use)**: Same as willitude-knowledge and willitude-trace. "
        "Protected by API Gateway + Cognito JWT + Lambda Authorizer. "
        "Use Authorization: Bearer <JWT> (from the shared willitude Cognito pool, using role-specific user like agent-quant-researcher). "
        "Authorizer injects X-User-Role etc. Tools may require specific roles (e.g. to materialize expensive data). "
        "For local: use AWS_PROFILE for SSM + mock headers or direct."
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
    description="List detailed cached datasets. Filter with provider='tardis', 'databento' or 'tardis_bars'. Empty/zero-size entries (failed downloads) are filtered out by default to avoid ghost entries.",
)
def list_cached_data(provider: str | None = None, include_empty: bool = False) -> str:
    items = _get_cache().list_cached(provider=provider)
    if not include_empty:
        items = [it for it in items if it.get("files", 0) > 0 or it.get("size_bytes", 0) > 0]
    return json.dumps({"count": len(items), "items": items}, indent=2, ensure_ascii=False)


@mcp.tool(
    name="ensure_tardis_data",
    description=(
        "Ensure Tardis historical data (exchange/symbols/dates/data_types) is downloaded+cached. "
        "Reuses if present unless force=true. Returns paths to raw CSVs + unified.parquet (if enabled). "
        "Ex: exchange='binance-futures', symbols=['BTCUSDT'], data_types=['trades','incremental_book_L2'], "
        "from_date='2024-01-01', to_date='2024-01-05'. "
        "NOTE: For perps use 'binance-futures' (USDT-M) or 'binance-delivery' (COIN-M), not spot 'binance'."
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
    user = get_user_context(ctx) if ctx is not None else {"role": "unknown"}
    require_role(user, {"quant-researcher", "data-engineer", "admin"})

    if ctx:
        await ctx.info(
            f"Ensuring Tardis: {exchange} {symbols} {from_date}..{to_date} types={data_types or ['trades']}"
        )

    client = _get_tardis()
    result = await client.ensure_cached(
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
    user = get_user_context(ctx) if ctx is not None else {"role": "unknown"}
    require_role(user, {"quant-researcher", "data-engineer", "admin"})

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
        "SMART INCREMENTAL: only missing days are downloaded (daily-aware cache). "
        "Does (1) smart ensure in global cache (2) symlinks/copies under project/data/raw/tardis/... + manifest. "
        "RULE: Use this for any rolling/ongoing campaign. After success use the 'project_paths' for relative loads. "
        "Same params as ensure_tardis_data. "
        "NOTE: For perpetuals use exchange='binance-futures' (USDT-M) or 'binance-delivery' (COIN-M), not 'binance' (spot)."
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
    user = get_user_context(ctx) if ctx is not None else {"role": "unknown"}
    require_role(user, {"quant-researcher", "data-engineer", "admin"})

    if ctx:
        await ctx.info(f"Materializing Tardis {exchange} {symbols} into project (symlinks={use_symlinks})...")

    proj = _resolve_project_dir(project_dir)
    if not proj:
        return json.dumps({
            "error": "No project registered and no project_dir provided. "
                    "Call register_project first or pass project_dir explicitly."
        })

    client = _get_tardis()
    ensure_result = await client.ensure_cached(
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
        "SMART INCREMENTAL: stores daily Parquets, only fetches missing days for rolling windows. "
        "Combines smart ensure + materialize (symlinks under data/raw/databento/ + manifest). "
        "After success, use 'project_paths' (relative) for loading code. Prefer over raw ensure_*."
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
    user = get_user_context(ctx) if ctx is not None else {"role": "unknown"}
    require_role(user, {"quant-researcher", "data-engineer", "admin"})

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
    name="materialize_tardis_bars_to_project",
    description=(
        "Materialize already-built Tardis bars into the current project as symlinks (or copies). "
        "Use after ensure_tardis_bars. This is how you get clean relative paths like data/raw/tardis_bars/... "
        "while the heavy bar data lives in the global cache."
    ),
)
async def materialize_tardis_bars_to_project(
    exchange: str,
    symbols: list[str],
    freq: str = "1m",
    from_date: str | None = None,
    to_date: str | None = None,
    target_subdir: str = "data/raw",
    use_symlinks: bool = True,
    project_dir: str | None = None,
    ctx: Context | None = None,
) -> str:
    if ctx:
        await ctx.info(f"Materializing Tardis bars {exchange} {symbols} {freq} into project...")

    user = get_user_context(ctx) if ctx is not None else {"role": "unknown"}
    require_role(user, {"quant-researcher", "data-engineer", "admin"})

    proj = _resolve_project_dir(project_dir)
    if not proj:
        return json.dumps({"error": "No active project. Call register_project first."})

    c = _get_cache()
    materialized = []
    for symbol in symbols:
        bar_dir = c.tardis_bars_dir(exchange, symbol, freq)
        if not bar_dir.exists():
            continue
        dest_dir = proj / target_subdir / "tardis_bars" / c._safe_name(exchange) / c._safe_name(symbol) / c._safe_name(freq)
        dest_dir.mkdir(parents=True, exist_ok=True)

        for src in sorted(bar_dir.glob("*.parquet")):
            if from_date and src.stem < from_date:
                continue
            if to_date and src.stem > to_date:
                continue
            dest = dest_dir / src.name
            if use_symlinks:
                if dest.exists() or dest.is_symlink():
                    dest.unlink()
                os.symlink(src, dest)
            else:
                shutil.copy2(src, dest)
            materialized.append(str(dest.relative_to(proj)))

    return json.dumps({
        "provider": "tardis_bars",
        "project_dir": str(proj),
        "materialized_count": len(materialized),
        "example_paths": materialized[:3],
    }, indent=2)


@mcp.tool(
    name="ensure_tardis_bars",
    description=(
        "PRIMARY TOOL for quant research. Downloads the necessary raw (trades + derivative_ticker + liquidations), "
        "builds time-bar parquet (OHLCV + vwap + taker flow + funding + OI + liquidations) at the requested freq, "
        "and stores the bars in the global cache (tardis_bars/). "
        "This solves the disk problem: raw is tens of GB, 1m bars for same coverage is hundreds of MB. "
        "keep_raw=False (default) deletes the raw after bar creation. "
        "Supports the same smart missing-day resume as raw. "
        "NOTE: Minutes with zero trades have no row (group_by). Consumer should reindex to full freq grid if needed for continuous time series. "
        "Example: ensure_tardis_bars(exchange='binance-futures', symbols=['BTCUSDT'], from_date='2024-01-01', to_date='2025-06-12', freq='1m')"
    ),
)
async def ensure_tardis_bars(
    exchange: str,
    symbols: list[str],
    from_date: str,
    to_date: str,
    freq: str = "1m",
    keep_raw: bool = False,
    force: bool = False,
    ctx: Context | None = None,
) -> str:
    if ctx:
        await ctx.info(f"Building Tardis bars {exchange} {symbols} {freq} {from_date}..{to_date} keep_raw={keep_raw}")

    user = get_user_context(ctx) if ctx is not None else {"role": "unknown"}
    require_role(user, {"quant-researcher", "data-engineer", "admin"})

    client = _get_tardis()
    result = await client.ensure_tardis_bars(
        exchange=exchange,
        symbols=symbols,
        from_date=from_date,
        to_date=to_date,
        freq=freq,
        keep_raw=keep_raw,
        force=force,
    )
    return json.dumps(result, indent=2, default=str)


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

2. **Preferred Tool for research = ensure_tardis_bars + materialize_tardis_bars_to_project**
   - Raw ticks are huge (tens of GB). For actual quant work (backtests, panels, models) ALWAYS use the bar layer first.
   - Call ensure_tardis_bars(..., freq="1m", keep_raw=False) → this builds compact OHLCV+derived bars in global cache and optionally discards raw.
   - Then materialize_tardis_bars_to_project(...) to get symlinks in the project with clean relative paths.
   - Fall back to raw materialize only when user explicitly needs tick-level data.

3. **After materialize success**
   - Look at the returned JSON.
   - Extract the "project_paths" list — these are clean relative paths (e.g. "data/raw/tardis/binance/BTCUSDT/unified.parquet").
   - Immediately propose or insert loading code using those relative paths with polars.scan_parquet / read_parquet.
   - Mention the manifest location so the user has provenance.

4. **Efficiency & Safety**
   - Start with small date ranges (1-3 days) when the user is exploring.
   - Use use_symlinks=true (the default) unless the user wants a self-contained copy (use_symlinks=false).
   - Report disk impact or ask before very large requests.

5. **Reproducibility & Rolling Campaigns**
   - The cache is now date-partitioned and smart: repeated calls for rolling windows (e.g. "last 90 days" every day) only download the new days.
   - _willitude_manifest.json + list_data_in_project() give full provenance.
   - Always use materialize_*_to_project for campaigns so the project has clean daily references.

=== RECOMMENDED STEP-BY-STEP FLOW ===

When user says something like "get me Binance BTC L2 for Jan-Feb in my alpha project":

A. Call register_project with the correct absolute path (if not already registered this session).
B. Call get_cache_status() or list_data_in_project() to see what already exists.
C. For real research: Call ensure_tardis_bars(...) (with keep_raw=False) then materialize_tardis_bars_to_project(...).
   For raw ticks (rare): use the raw materialize tools.
D. On success:
   - Show the user the relative paths from "project_paths" (or from the bars materialize result).
   - Offer ready-to-paste Polars loading code.
   - Optionally call load_cached_preview on one of the new paths.
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

    transport = os.getenv("TRANSPORT", "stdio")
    if transport == "streamable-http":
        # For remote deploy behind API Gateway (auth at gateway).
        # Path matches ALB route /data/mcp ; security settings disable DNS rebinding (as in willitude-knowledge/trace).
        mcp.run(transport="streamable-http", path="/data/mcp")
    else:
        # Default transport is stdio — perfect for Claude Desktop, Cursor, Windsurf, etc.
        mcp.run()


if __name__ == "__main__":
    run_server()
