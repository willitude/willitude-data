"""
Willitude Data MCP Server package.

Public re-exports for convenience:
    from willitude_mcp.server import mcp, run_server
    from willitude_mcp.config import get_config
"""

from .server import mcp, run_server  # noqa: F401

__all__ = ["mcp", "run_server"]
