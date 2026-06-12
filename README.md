# willitude-data

**Willitude Data MCP Server** — Quant-grade cached access to Tardis.dev (crypto tick data) and Databento (equities, futures, options) using keys stored in AWS SSM Parameter Store.

Designed for quant researchers who want to ask an AI coding/research agent (Claude, Cursor, Windsurf, Cline, etc.) to "get me the last 3 months of Binance BTCUSDT L2 deltas + Databento ES trades" and have the data appear locally as efficient Parquet/CSV files, ready for Polars, Pandas, or DuckDB analysis.

- Runs locally (your laptop) using AWS SSO
- Can also run on AWS (EC2, ECS, SageMaker, etc.) using IAM roles — same code
- Never hard-codes or logs the API keys
- Keys live only in SSM + short-lived in-process memory

## Prerequisites

- Python ≥ 3.12
- `uv` (recommended) or pip + venv
- AWS CLI configured with SSO for the profile that can read the SSM parameters (typically `YongseokMacProfile`)
- Access to the following SSM parameters in `ap-northeast-1`:
  - `/willitude/tardis/api-key`
  - `/willitude/databento/api-key`

## Quick Start (Local)

```bash
# 1. Clone / cd into this repo
cd willitude-data

# 2. Login to AWS SSO (do this once per session or when token expires)
aws sso login --profile YongseokMacProfile

# 3. Sync dependencies (creates .venv)
uv sync

# 4. (Optional) Verify keys are readable
AWS_PROFILE=YongseokMacProfile uv run python -c '
from willitude_mcp.ssm import get_tardis_key, get_databento_key
print("Tardis key len:", len(get_tardis_key()))
print("Databento key len:", len(get_databento_key()))
'

# 5. Run the MCP server directly (stdio)
AWS_PROFILE=YongseokMacProfile uv run willitude-mcp
# or
uv run python -m willitude_mcp
```

## Using from an MCP Client (Claude Desktop example)

Edit your Claude Desktop config (usually `~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "willitude-data": {
      "command": "uv",
      "args": [
        "--directory",
        "/Users/yongseok/willitude-data",
        "run",
        "willitude-mcp"
      ],
      "env": {
        "AWS_PROFILE": "YongseokMacProfile"
      }
    }
  }
}
```

After saving, restart Claude Desktop. The tools should appear.

Other clients (Cursor, Windsurf, etc.) have similar JSON config for MCP servers.

## Recommended Workflow (Global Cache + Project Materialize)

This is the intended UX for a quant researcher who lives in their own research folders.

1. **Register your current work folder** (do this once when you open a project):
   `register_project("/Users/you/research/my-alpha-2026", name="alpha-2026")`

2. **Bring data into the project** (the main action):
   - `materialize_tardis_to_project(exchange="binance", symbols=["BTCUSDT"], from_date=..., to_date=..., data_types=["trades", "incremental_book_L2"], use_symlinks=true)`
   - `materialize_databento_to_project(...)`

   These two calls:
   - Make sure the data exists in the global efficient cache (using your AWS SSM keys).
   - Create **symlinks** (default) or real copies under `data/raw/tardis/...` and `data/raw/databento/...` inside **your** project.
   - Write `data/raw/_willitude_manifest.json` (exact request, timestamps, source cache paths).

3. In your notebooks use beautiful relative paths:
   ```python
   import polars as pl
   df = pl.scan_parquet("data/raw/tardis/binance/BTCUSDT/unified.parquet")
   ```

4. Extras: `list_data_in_project()`, `get_data_paths(..., project_dir=...)`, `load_cached_preview(...)`, `create_data_manifest()`.

This gives you deduplication + speed of a central cache, while every research repo feels self-contained and git-friendly.

**Smart caching for rolling campaigns**: Both providers now use date-partitioned storage + missing-day calculation. Repeatedly calling `materialize_*_to_project(..., from_date=..., to_date="today")` only downloads the new days. Overlapping requests share daily shards. No more full re-downloads for ongoing work.

## For AI Agents (Cursor, Claude Desktop, Windsurf, Cline, etc.)

This server is specifically designed to be **agent-friendly**.

**Strongly recommended agent behavior (also encoded in the `quant_data_workflow` prompt):**

- As soon as the user mentions a project folder or "my research", **immediately** call `register_project` with the absolute path.
- Prefer the high-level `materialize_*_to_project` tools. They are the "one call does the right thing" tools.
- After any materialize call succeeds, **parse the JSON**, take the `project_paths` array, and generate ready-to-paste code using relative paths.
- Use `list_data_in_project()` when the user asks about provenance or "what data are we using for this experiment?"
- The embedded prompt `quant_data_workflow` contains strict rules the agent should follow. Many clients will automatically load it.

**Why this is good for agents:**
- High-level tools reduce the number of steps the agent has to plan.
- Returns are rich JSON with explicit "project_paths", "manifest", and "usage_hint".
- Project registration makes the agent context-aware for the rest of the conversation.
- Manifest provides built-in audit trail.

**Pro tip for users:** In Cursor/Claude settings, make sure the MCP server is started with `AWS_PROFILE=YongseokMacProfile` (or equivalent). The agent will then be able to handle the entire "get data for my current strategy" flow with almost no manual intervention from you.

## Available MCP Tools

**Project-oriented (recommended):**
- `register_project(project_root, name?)`
- `materialize_tardis_to_project(...)` / `materialize_databento_to_project(...)`
- `list_data_in_project(project_dir?)`
- `create_data_manifest(project_dir?)`

**Lower-level / cache focused:**
- `get_cache_status`, `list_cached_data`
- `ensure_tardis_data(...)`, `ensure_databento_data(...)`
- `get_data_paths(...)` (now returns relative hints when project is active)
- `load_cached_preview(path, limit)`
- `get_tardis_key_info`, `get_databento_key_info`

Resources: `cache://status`, `cache://list`

There is also an embedded prompt `quant_data_workflow` that teaches the LLM the above flow.

## Cache Layout (Global)

Default location: `~/.willitude/willitude-data/` (or `$WILLITUDE_CACHE_DIR`)

```
~/.willitude/willitude-data/
├── tardis/
│   └── binance/
│       └── BTCUSDT/
│           └── incremental_book_L2/
│               ├── raw/               # original .csv.gz from Tardis
│               └── unified.parquet    # (optional) single convenient file
└── databento/
    └── GLBX.MDP3/
        └── ES.FUT/
            └── trades/
                └── 20240601_20240603.parquet
```

Inside a project after `materialize_*_to_project`, you will see symlinks:

```
your-project/
└── data/
    └── raw/
        ├── tardis/
        │   └── binance/BTCUSDT/trades/unified.parquet   (symlink)
        ├── databento/...
        └── _willitude_manifest.json
```

You can safely delete subdirectories to free space. The server will re-download on next `ensure_*` call.

## Environment Variables

| Variable                        | Purpose                                      | Example                              |
|--------------------------------|----------------------------------------------|--------------------------------------|
| `WILLITUDE_CACHE_DIR`          | Override cache root (local working copy)     | `/Volumes/data/willitude-cache`     |
| `WILLITUDE_S3_CACHE_BUCKET`    | Enable S3 global cache (e.g. willitude-data-cache in ap-northeast-1). Enables read-through/write-through for cross-machine sharing (notebook + canary). | `willitude-data-cache` |
| `WILLITUDE_S3_CACHE_PREFIX`    | Prefix under the bucket (default willitude-data) | `willitude-data` |
| `AWS_PROFILE`                  | AWS profile (SSO)                            | `YongseokMacProfile`                 |
| `AWS_REGION` / `WILLITUDE_AWS_REGION` | Region for SSM and S3 client             | `ap-northeast-1` (Tokyo recommended) |
| `WILLITUDE_CONVERT_TARDIS_PARQUET` | `0` to disable auto parquet conversion | `1` (default)                        |

When S3 is enabled, `ensure_*` will check S3 first (download to local working copy if present), fetch from provider only for missing, then upload to S3. Bars are always synced to S3. Manifest records S3 keys.

## Running on AWS

1. Give the EC2 / ECS task role (or Lambda execution role) `ssm:GetParameter` on the two `/willitude/*` ARNs.
2. Same container/image or EC2 just runs `uv run willitude-mcp` (or the Docker image below).
3. Point cache dir at a large EBS volume or EFS mount if you want shared across instances.
4. For heavy users, consider a small always-on ECS service exposing the MCP server over SSE (advanced).

## Dockerfile (for AWS or containerized use)

See `Dockerfile` in the repo root. Example build & run:

```bash
docker build -t willitude-mcp .
docker run --rm -e AWS_PROFILE=YongseokMacProfile -v ~/.aws:/root/.aws -v $HOME/.willitude:/root/.willitude willitude-mcp
```

(When using IAM roles on AWS, you usually don't mount ~/.aws.)

## Development

```bash
uv sync
uv run ruff check .
uv run pytest
uv run python -m willitude_mcp   # run server
```

## Data Notes (for researchers)

**Tardis.dev**
- Exchange-native format.
- Common data_types: `trades`, `quotes`, `incremental_book_L2`, `incremental_book_L3`, `book_snapshot_5`, etc.
- Symbol format is exchange-specific (usually no separator, e.g. `BTCUSDT`, `BTC-PERPETUAL` on Deribit).
- The server also produces `unified.parquet` (zstd) for fast columnar access.

**Databento**
- Normalized, high-quality.
- Popular datasets: `GLBX.MDP3` (CME futures), `XNAS.ITCH` (Nasdaq), `OPRA.PITCH`, etc.
- Schemas give different normalization levels (`trades`, `mbp-1`, `mbo` for full depth, `ohlcv-*` etc.).
- Use `stype_in="continuous"` or parent symbols when you want continuous contracts.

## License / Internal

Internal tool for Willitude quant research. Do not share the SSM keys or cached data outside approved environments.

---

## Final MCP Specification (as of 2026-06-12)

**Server name:** `WillitudeData`

**Transport:** stdio (default). SSE possible in future via the MCP SDK.

### Tools

**Project context (use these for normal quant work in a folder):**

| Tool | Parameters (key ones) | Description |
|------|-----------------------|-------------|
| `register_project` | `project_root: str`, `name?: str` | Register the researcher's current work folder. All subsequent materialize calls default to it. |
| `materialize_tardis_to_project` | `exchange, symbols, from_date, to_date, data_types?, target_subdir="data/raw", use_symlinks=true, force=false, project_dir?` | Ensure in global cache + create symlinks/copies + manifest inside the project. |
| `materialize_databento_to_project` | `dataset, symbols, schema, start, end, stype_in?, target_subdir="data/raw", use_symlinks=true, force=false, project_dir?` | Same as above for Databento. |
| `list_data_in_project` | `project_dir?` | What has been materialized in this project (from the manifest). |
| `create_data_manifest` | `project_dir?` | (Re)write the `_willitude_manifest.json`. |

**Cache / low-level tools:**

- `get_cache_status()`
- `list_cached_data(provider?)`
- `ensure_tardis_data(...)` / `ensure_databento_data(...)` (populate global cache only)
- `get_data_paths(provider, exchange_or_dataset, symbol, data_type_or_schema, project_dir?)` — now includes relative hints when project active.
- `load_cached_preview(path, limit=10)`
- `get_tardis_key_info()` / `get_databento_key_info()`

### Resources

- `cache://status` — human readable cache size summary
- `cache://list` — JSON of all cached items on disk

### Prompt

- `quant_data_workflow` — detailed instructions for the LLM on the recommended "register → materialize → use relative paths + manifest" flow.

### Environment variables that affect behavior

- `WILLITUDE_CACHE_DIR`
- `AWS_PROFILE` (or rely on default credential chain / IAM role)
- `WILLITUDE_CONVERT_TARDIS_PARQUET`

### Key Design Decisions (final)

- Single global deduplicated cache by default (disk + API cost efficient).
- Explicit "materialize into my project" step gives the researcher ownership and clean relative paths without sacrificing the central cache.
- Symlinks are the default for materialize (huge space savings).
- Every materialized dataset gets a `_willitude_manifest.json` entry with original request parameters + timestamps.
- The LLM is expected to call `register_project` early and prefer the `materialize_*_to_project` tools when the human is working inside a specific research folder.
- Low-level `ensure_*` tools remain for power users who only want to touch the shared cache.

This spec is what the quant researcher (and their AI agent) should rely on.

## Credits

Built with:
- Model Context Protocol (MCP) + FastMCP
- tardis-dev + databento official clients
- Polars + PyArrow for fast data handling
- boto3 + AWS SSM for secret management

Happy researching!
