# Sanjaya Analytics MCP Server + Replaceable Chat Client (Local LLM via Ollama)

**Quick Start:** See [QUICKSTART.md](QUICKSTART.md) for immediate run instructions.

This package is built so **the MCP client is replaceable**.

- The **MCP Server** owns: auth, time-range parsing, API calls, response shaping, and optional local-LLM NLU.
- The **Client** owns: UX only (a simple chat loop). Replace it with Claude Desktop / another MCP client anytime.

## What you can ask

These are all supported (examples):

### Analytics summaries
- `Give me today’s analytics summary`
- `Show analytics for yesterday`
- `Give me last week summary`
- `Analytics summary for 10th Jan 2026`
- `Summary for previous month`

### Single metrics / items
- `total trips today`
- `total distance yesterday`
- `utilization of tug-104 today`
- `uptime of tug-104`  ✅ will resolve `tug-104` to a matching full name like `tug-104-ceat-nagpur-11` when present in the response
- `battery of tug-32 today`
- `mode of tug-93`

### Time ranges (flexible)
The server understands:
- Absolute dates: `10th Jan 2026`, `10-01-26`, `2026/01/10`, `Jan 10, 2026` …
- Relative: `today`, `yesterday`, `day before yesterday`, `last hour`, `3 days back`
- Periods: `this week`, `previous week`, `previous month`, `this quarter`, `last quarter`, `in 2025`, `in 2026`

## Why you were seeing those MCP errors

1) **"No module named mcp"** — the venv didn’t have the MCP Python package installed.
2) **"Failed to parse JSONRPC message"** — the server was printing non-JSON-RPC lines (like `{"type":"ready"}`) to **stdout**. MCP stdio requires **only JSON-RPC** on stdout; logging must go to **stderr**.

This implementation fixes that by:
- Logging only to **stderr**.
- Running the server using `FastMCP` (clean stdio transport).

## Setup

### 1) Create a venv + install dependencies

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2) Configure environment

Copy `.env.example` → `.env` and edit values:

```bash
cp .env.example .env
```

Then load it (bash):

```bash
set -a
source .env
set +a
```

### 3) (Optional but recommended) Install Ollama (local LLM)

**Linux:**
```bash
curl -fsSL https://ollama.com/install.sh | sh
```

**macOS / Windows:** install from the Ollama website and start the app.

Pull a small model (good enough for intent extraction):

```bash
ollama pull qwen2.5:3b-instruct
```

> If Ollama is not running, the server still works using a deterministic parser.

## Run

### Option A — Development/Testing with Chat Client

For development and testing, run the server with the included chat client:

**Terminal 1 - Start the server:**
```bash
source venv/bin/activate
python mcp_server.py
```

**Terminal 2 - Start the chat client:**
```bash
source venv/bin/activate
python chat_client.py
```

Then type your queries in the chat client. Example:
```
> total trips today for client ceat-nagpur and fleet CEAT-Nagpur-North-Plant
```

### Option B — Use MCP Inspector (Development/Testing)

For interactive testing with a web UI:

```bash
source venv/bin/activate
mcp dev mcp_server.py
```

This opens a web interface where you can:
- Browse available tools
- Browse resources
- Test prompts
- Call tools interactively

### Option C — Use Claude Desktop (Production)

For daily use with Claude Desktop:

1. **Install to Claude Desktop:**
   ```bash
   source venv/bin/activate
   mcp install mcp_server.py
   ```

2. **Restart Claude Desktop**

3. The server will be available in Claude Desktop automatically

See [CLIENT_OPTIONS.md](CLIENT_OPTIONS.md) for detailed setup instructions.

### Option D — Direct Server Execution

To run the server directly (for debugging or custom clients):

```bash
source venv/bin/activate
python mcp_server.py
```

The server communicates via stdio (standard input/output), so it's ready to accept MCP protocol messages.

## Tools exposed by the MCP server

- `fm_login()`
- `fm_basic_analytics(fm_client_name, start_time, end_time, timezone, fleet_name, status, sherpa_name=None)`
- `fm_basic_analytics_item(..., item=...)`
- `sanjaya_chat(query)`  ← best entry point for chat UI

## Debugging

Set one env var to turn on detailed API logging:

```bash
export SANJAYA_HTTP_TRACE=1
```

The server logs (stderr) will include:
- Full request URL with query parameters
- Request method and headers (tokens redacted)
- Request body (if POST/PUT)
- Response status code
- Response headers
- Full response JSON (pretty-printed) or response text

Example output:
```
================================================================================
API REQUEST:
  Method: GET
  URL: https://sanjaya.atimotors.com/api/v1/master_fm/analytics/basic_analytics/ceat-nagpur?start_time=...
  Query Params: {
    "start_time": "2026-01-26 00:00:00",
    "end_time": "2026-01-26 18:56:17",
    ...
  }
  Status: 200
  Response JSON:
  {
    "total_trips": 0,
    "total_distance_km": 0,
    ...
  }
================================================================================
```

## Notes

- Auth token is automatically refreshed if missing/expired.
- `sherpa_name` is **never** sent as an empty string (that can trigger 422/401 in some backends).
- `status` query is always sent as repeated query params (httpx handles lists).
