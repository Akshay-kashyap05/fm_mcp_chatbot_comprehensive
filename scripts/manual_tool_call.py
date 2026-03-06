#!/usr/bin/env python3
"""Manual one-shot MCP tool caller.

Use when you want to reproduce request/response with explicit args.

Example:
  python manual_tool_call.py fm_basic_analytics_item '{"fm_client_name":"ceat-nagpur","fleet_name":"CEAT-Nagpur-North-Plant","timezone":"Asia/Kolkata","status":["succeeded","failed","cancelled"],"start_time":"2026-01-22 00:00:00","end_time":"2026-01-22 23:59:59","item":"total_trips"}'
"""

from __future__ import annotations

import asyncio
import json
import sys

from mcp.client.stdio import stdio_client
from mcp import ClientSession, StdioServerParameters


async def main() -> None:
    if len(sys.argv) < 3:
        print("Usage: python manual_tool_call.py <tool_name> '<json_args>'")
        raise SystemExit(2)

    tool_name = sys.argv[1]
    args = json.loads(sys.argv[2])

    params = StdioServerParameters(command=sys.executable, args=["mcp_server.py"])

    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool(tool_name, args)
            # result is usually a list of content blocks; keep it readable
            print(result)


if __name__ == "__main__":
    asyncio.run(main())
