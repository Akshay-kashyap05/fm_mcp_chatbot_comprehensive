# Quick Start Guide

## Prerequisites

1. **Activate virtual environment:**
   ```bash
   source venv/bin/activate
   ```

2. **Ensure `.env` file exists** with your credentials:
   ```bash
   # Copy example if needed
   cp .env.example .env
   # Edit .env with your SANJAYA_USERNAME and SANJAYA_PASSWORD
   ```

## Running the Server

### 🚀 Quick Test (Chat Client)

**Terminal 1 - Server:**
```bash
source venv/bin/activate
python mcp_server.py
```

**Terminal 2 - Client:**
```bash
source venv/bin/activate
python chat_client.py
```

Then type queries like:
```
> total trips today for client ceat-nagpur and fleet CEAT-Nagpur-North-Plant
> /help
```

**Important:** Always include `client` and `fleet` in your queries. 

### 🔧 Development/Testing (MCP Inspector)

Interactive web UI for testing:
```bash
source venv/bin/activate
mcp dev mcp_server.py
```

Opens browser with tool browser, resource explorer, and interactive testing.

### 🎯 Production (Claude Desktop)

One-time setup:
```bash
source venv/bin/activate
mcp install mcp_server.py
```

Then restart Claude Desktop. The server will be available automatically.

## Common Commands

```bash
# Activate environment
source venv/bin/activate

# Run server only
python mcp_server.py

# Run with chat client
python chat_client.py

# Development mode (web UI)
mcp dev mcp_server.py

# Install to Claude Desktop
mcp install mcp_server.py

# Check available tools
python -c "from mcp_server import mcp; print([t.name for t in mcp._tools.values()])"
```

## Troubleshooting

**Server won't start:**
- Check `.env` file exists and has credentials
- Ensure virtual environment is activated
- Check Python version: `python --version` (needs 3.8+)

**Client can't connect:**
- Make sure server is running first
- Check server logs for errors
- Verify MCP packages installed: `pip list | grep mcp`

**Authentication errors:**
- Verify `SANJAYA_USERNAME` and `SANJAYA_PASSWORD` in `.env`
- Check credentials are correct
- Server auto-refreshes tokens, but initial login must succeed

**Enable detailed API logging:**
```bash
export SANJAYA_HTTP_TRACE=1
python mcp_server.py
```

This will show:
- Which API endpoint was called
- Request parameters
- Full response JSON
- Response status codes

