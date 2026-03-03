# Backup of Legacy MCP Server Implementation

This folder contains the original implementation before refactoring to standard MCP patterns.

## Files

- **mcp_server_legacy.py**: Original implementation with large `sanjaya_chat` tool (conversational approach)
- **mcp_server_standard.py**: Intermediate standard version (if present)

## What Changed

The original implementation was refactored to follow standard MCP patterns:

### Before (Legacy)
- Single large `sanjaya_chat` tool handling all logic
- Custom response format with dictionaries
- Client-side prompting for missing parameters
- No Resources or Prompts

### After (Standard)
- Focused, composable tools (`get_analytics_summary`, `get_metric`, etc.)
- Resources for exposing data (`sanjaya://clients`, etc.)
- Prompts for common query patterns
- Standard MCP content types (strings)
- Better composability and discoverability

## Migration Notes

The new `mcp_server.py` maintains backward compatibility through a simplified `sanjaya_chat` tool that calls the focused tools internally.
