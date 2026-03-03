#!/usr/bin/env python3
"""Standard MCP client for Sanjaya Analytics.

This client uses standard MCP tools, resources, and prompts.
No custom response format handling - just plain text responses.
"""

from __future__ import annotations

import asyncio
import os
import sys

from mcp.client.stdio import stdio_client
from mcp import ClientSession, StdioServerParameters


async def main() -> None:
    server_cmd = os.environ.get("MCP_SERVER_CMD", "python")
    server_args = os.environ.get("MCP_SERVER_ARGS", "mcp_server.py").split()

    params = StdioServerParameters(command=server_cmd, args=server_args)

    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            init_result = await session.initialize()
            
            # Display server information
            sys.stdout.write("=" * 60 + "\n")
            sys.stdout.write("Sanjaya Analytics MCP Server\n")
            sys.stdout.write("=" * 60 + "\n")
            
            if init_result and hasattr(init_result, 'serverInfo'):
                server_info = init_result.serverInfo
                if server_info and hasattr(server_info, 'name'):
                    sys.stdout.write(f"Server: {server_info.name}\n")
            
            if init_result and hasattr(init_result, 'instructions') and init_result.instructions:
                sys.stdout.write("\n📖 Server Instructions:\n")
                sys.stdout.write("-" * 60 + "\n")
                instructions = init_result.instructions
                for line in instructions.split('\n'):
                    if line.strip():
                        sys.stdout.write(f"  {line}\n")
                    else:
                        sys.stdout.write("\n")
                sys.stdout.write("-" * 60 + "\n")
            
            # List available tools
            tools = await session.list_tools()
            tool_names = [t.name for t in tools.tools]
            sys.stdout.write(f"\n🔧 Available Tools: {', '.join(tool_names)}\n")
            
            # List available resources
            resources = await session.list_resources()
            resource_uris = [str(r.uri) for r in resources.resources]
            if resource_uris:
                sys.stdout.write(f"\n📚 Available Resources: {', '.join(resource_uris)}\n")
            
            # List available prompts
            prompts = await session.list_prompts()
            prompt_names = [p.name for p in prompts.prompts]
            if prompt_names:
                sys.stdout.write(f"\n💬 Available Prompts: {', '.join(prompt_names)}\n")
            
            sys.stdout.write("\n💡 Quick Start:\n")
            sys.stdout.write("  • Always include 'client' and 'fleet' in your queries\n")
            sys.stdout.write("  • Example: 'total trips today for client ceat-nagpur and fleet CEAT-Nagpur-North-Plant'\n")
            sys.stdout.write("  • Use 'resolve_client_name' and 'resolve_fleet_name' to find names\n")
            sys.stdout.write("  • Type '/help' for help, '/quit' to exit\n")
            sys.stdout.write("\n" + "=" * 60 + "\n")
            sys.stdout.write("\nType your question. Commands: /help, /quit\n\n")
            sys.stdout.flush()

            while True:
                try:
                    text = input("> ").strip()
                except (EOFError, KeyboardInterrupt):
                    break

                if not text:
                    continue
                if text in {"/quit", "/exit"}:
                    break
                if text == "/help":
                    resp = await session.call_tool("sanjaya_chat", {"text": "help"})
                    if resp.content:
                        sys.stdout.write(resp.content[0].text)
                        sys.stdout.write("\n\n")
                    continue

                # Use sanjaya_chat for natural language queries
                resp = await session.call_tool("sanjaya_chat", {"text": text})

                if resp.isError:
                    sys.stdout.write(f"Error: {resp.content[0].text if resp.content else 'Unknown error'}\n\n")
                    continue

                # Display the response (plain text)
                if resp.content:
                    sys.stdout.write("\n📊 Result:\n")
                    sys.stdout.write("=" * 60 + "\n")
                    sys.stdout.write(resp.content[0].text)
                    sys.stdout.write("\n" + "=" * 60 + "\n\n")
                else:
                    sys.stdout.write("No response received.\n\n")


if __name__ == "__main__":
    asyncio.run(main())
