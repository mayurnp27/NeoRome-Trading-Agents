"""Minimal stdio MCP server exposing one fixed BTCUSD fast stochastic tool."""
from __future__ import annotations

from typing import Any

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

from neorome.tradingview_fast_source import fetch_fast_stochastic


mcp = FastMCP("TradingView BTCUSD Fast Paper Feed")


@mcp.tool(
    annotations=ToolAnnotations(
        title="BTCUSD Fast Stochastic",
        readOnlyHint=True,
        destructiveHint=False,
        openWorldHint=True,
    )
)
async def fast_stochastic(
    symbol: str = "BTCUSD",
    exchange: str = "COINBASE",
    timeframe: str = "5m",
) -> dict[str, Any]:
    """Return closed TradingView COINBASE:BTCUSD 5m bars and Stoch(5,3,3)."""
    if symbol != "BTCUSD" or exchange != "COINBASE" or timeframe != "5m":
        raise ValueError("fast_stochastic is fixed to BTCUSD on COINBASE at 5m")
    return await fetch_fast_stochastic()


if __name__ == "__main__":
    mcp.run(transport="stdio")
