# Sanjaya Analytics MCP Server - Architecture Document

## Table of Contents

1. [Overview](#overview)
2. [MCP Protocol Basics](#mcp-protocol-basics)
3. [System Architecture](#system-architecture)
4. [Component Details](#component-details)
5. [Data Flow](#data-flow)
6. [Interface with MCP Clients](#interface-with-mcp-clients)
7. [Key Design Decisions](#key-design-decisions)
8. [Extension Points](#extension-points)

---

## Overview

The Sanjaya Analytics MCP Server is a **standard MCP (Model Context Protocol) server** that provides natural language access to fleet management analytics data. It follows MCP best practices with focused tools, resources for data exposure, and prompts for common query patterns.

### Key Characteristics

- **Protocol**: MCP (Model Context Protocol) over stdio
- **Transport**: Standard input/output (stdio) for JSON-RPC messages
- **Architecture**: Layered architecture with clear separation of concerns
- **NLU**: Optional local LLM (Ollama) with heuristic fallback
- **Caching**: TTL-based caching for API responses
- **Client Agnostic**: Works with any standard MCP client (Claude Desktop, custom clients, etc.)

---

## MCP Protocol Basics

### What is MCP?

The **Model Context Protocol (MCP)** is a standardized protocol that enables AI applications to securely access external data sources and tools. It uses JSON-RPC 2.0 over stdio for communication.

### MCP Communication Model

```
┌─────────────┐                    ┌─────────────┐
│ MCP Client  │  JSON-RPC over    │ MCP Server  │
│ (Claude,    │◄─────────────────►│ (This       │
│  Custom)    │     stdio         │  Server)    │
└─────────────┘                    └─────────────┘
```

### MCP Server Capabilities

An MCP server exposes three main types of capabilities:

1. **Tools**: Functions that can be called by the client
2. **Resources**: Data that can be read by the client
3. **Prompts**: Reusable templates for common interactions

### Message Flow

```
Client Request:
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "tools/call",
  "params": {
    "name": "sanjaya_chat",
    "arguments": {"query": "total trips today"}
  }
}

Server Response:
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "content": [
      {"type": "text", "text": "total_trips: 101"}
    ]
  }
}
```

---

## System Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    MCP Client Layer                          │
│  (Claude Desktop, Custom Client, MCP Inspector, etc.)       │
└───────────────────────┬─────────────────────────────────────┘
                         │ JSON-RPC over stdio
                         │
┌───────────────────────▼─────────────────────────────────────┐
│                  MCP Server Layer                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │    Tools     │  │  Resources  │  │   Prompts    │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
│                                                              │
│  • sanjaya_chat()                                           │
│  • get_metric()                                             │
│  • get_analytics_summary()                                  │
│  • resolve_client_name()                                    │
│  • resolve_fleet_name()                                     │
└───────────────────────┬─────────────────────────────────────┘
                         │
┌───────────────────────▼─────────────────────────────────────┐
│              Natural Language Understanding (NLU)           │
│  ┌────────────────────────────────────────────────────┐    │
│  │  Ollama (Optional)  │  Heuristic Parser (Fallback) │    │
│  └────────────────────────────────────────────────────┘    │
│                                                              │
│  • Intent extraction                                         │
│  • Parameter extraction (client, fleet, time, sherpa)       │
│  • Query validation                                          │
└───────────────────────┬─────────────────────────────────────┘
                         │
┌───────────────────────▼─────────────────────────────────────┐
│                  Business Logic Layer                        │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │ Time Parser  │  │   Formatter  │  │   Cache      │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
│                                                              │
│  • Time range parsing                                       │
│  • Response formatting                                       │
│  • TTL-based caching                                         │
└───────────────────────┬─────────────────────────────────────┘
                         │
┌───────────────────────▼─────────────────────────────────────┐
│                  API Client Layer                            │
│  ┌────────────────────────────────────────────────────┐    │
│  │            SanjayaAPI Client                        │    │
│  └────────────────────────────────────────────────────┘    │
│                                                              │
│  • JWT authentication                                        │
│  • HTTP request handling                                     │
│  • Error handling                                            │
│  • Token refresh                                             │
└───────────────────────┬─────────────────────────────────────┘
                         │ HTTPS
                         │
┌───────────────────────▼─────────────────────────────────────┐
│              Sanjaya Backend API                             │
│         (https://sanjaya.atimotors.com)                      │
└──────────────────────────────────────────────────────────────┘
```

### Component Layers

1. **MCP Server Layer** (`mcp_server.py`)
   - Exposes tools, resources, and prompts
   - Handles MCP protocol communication
   - Routes requests to appropriate handlers

2. **NLU Layer** (`src/nlu.py`)
   - Parses natural language queries
   - Extracts intent and parameters
   - Validates extracted parameters

3. **Business Logic Layer**
   - `src/time_parse.py`: Time range parsing
   - `src/formatting.py`: Response formatting
   - `src/cache.py`: TTL-based caching

4. **API Client Layer** (`src/sanjaya_client.py`)
   - HTTP client for Sanjaya API
   - Authentication management
   - Request/response handling

---

## Component Details

### 1. MCP Server (`mcp_server.py`)

**Purpose**: Main entry point that implements the MCP server interface.

**Key Components**:

#### Tools

- **`sanjaya_chat(query: str)`**
  - Primary entry point for natural language queries
  - Uses NLU to parse query
  - Routes to appropriate handler (`get_metric` or `get_analytics_summary`)
  - Returns plain text response

- **`get_metric(metric, client_name, fleet_name, ...)`**
  - Fetches a specific metric value
  - Handles sherpa resolution (all sherpas if not specified)
  - Calls appropriate API (`basic_analytics` or `route_analytics`)
  - Formats response using `extract_item_value`

- **`get_analytics_summary(client_name, fleet_name, ...)`**
  - Fetches comprehensive analytics summary
  - Calls `basic_analytics` API
  - Formats response using `summarize_basic_analytics`

- **`resolve_client_name(partial_name: str)`**
  - Resolves partial client names to full names
  - Uses cached client list
  - Returns matching clients

- **`resolve_fleet_name(client_name, partial_fleet_name)`**
  - Resolves partial fleet names to full names
  - Fetches client details to get fleet list
  - Returns matching fleets

#### Resources

- **`sanjaya://clients`**
  - Lists all available clients
  - Uses cached client list
  - Returns formatted text

- **`sanjaya://clients/{client_id}/fleets`**
  - Lists fleets for a specific client
  - Uses cached client details
  - Returns formatted text

- **`sanjaya://fleets/{fleet_id}/sherpas`**
  - Lists sherpas for a specific fleet
  - Uses cached sherpa list
  - Returns formatted text

#### Prompts

- **`analytics_summary_query`**
  - Template for analytics summary queries
  - Provides example queries

- **`metric_query`**
  - Template for metric queries
  - Provides example queries

### 2. Natural Language Understanding (`src/nlu.py`)

**Purpose**: Parse natural language queries into structured parameters.

**Architecture**:

```
User Query
    │
    ├─► Ollama (if enabled)
    │   └─► JSON extraction
    │       └─► ParsedQuery object
    │
    └─► Heuristic Parser (fallback)
        └─► Regex-based extraction
            └─► ParsedQuery object
```

**Key Functions**:

- **`parse_query(text, defaults)`**
  - Main entry point
  - Tries Ollama first (if enabled)
  - Falls back to heuristic parser
  - Applies post-processing validation

- **`_heuristic_parse(text)`**
  - Rule-based parser
  - Uses regex patterns
  - Extracts: intent, item, client, fleet, sherpa, time_phrase

- **`_ollama_json(prompt, model)`**
  - Calls Ollama API
  - Returns structured JSON
  - Handles errors gracefully

**Post-Processing**:

1. **Metric Fixes**: Detects and fixes misclassified metrics (e.g., "takt time" → `takt_time`)
2. **Client/Fleet Swap Detection**: Detects and fixes swapped client/fleet names
3. **Sherpa Validation**: Clears hallucinated sherpa names that don't appear in query
4. **Timezone Normalization**: Ensures valid timezone values

**ParsedQuery Structure**:

```python
@dataclass
class ParsedQuery:
    intent: str                    # "basic_analytics" | "basic_analytics_item" | "help"
    item: Optional[str]           # Metric name (e.g., "total_trips")
    sherpa_hint: Optional[str]    # Partial sherpa name (e.g., "tug-104")
    fm_client_name: Optional[str] # Client name
    fleet_name: Optional[str]      # Fleet name
    timezone: Optional[str]       # IANA timezone
    time_phrase: Optional[str]     # Natural time phrase (e.g., "last 7 days")
```

### 3. Time Parser (`src/time_parse.py`)

**Purpose**: Convert natural language time phrases into datetime ranges.

**Supported Formats**:

- **Absolute dates**: `10th Jan 2026`, `10-01-26`, `2026/01/10`
- **Relative**: `today`, `yesterday`, `last 7 days`, `last hour`
- **Periods**: `this week`, `previous month`, `this quarter`, `in 2025`

**Key Function**:

- **`parse_time_range(text, time_zone, now)`**
  - Parses time phrase
  - Returns `TimeRange` object with `start` and `end` datetimes
  - Handles timezone conversion
  - Uses `dateparser` library for complex dates

**TimeRange Structure**:

```python
@dataclass(frozen=True)
class TimeRange:
    start: datetime  # Start of time range
    end: datetime    # End of time range
    
    def to_strings(self) -> dict[str, str]:
        # Returns API-compatible format
        return {
            "start_time": self.start.strftime("%Y-%m-%d %H:%M:%S"),
            "end_time": self.end.strftime("%Y-%m-%d %H:%M:%S"),
        }
```

### 4. API Client (`src/sanjaya_client.py`)

**Purpose**: HTTP client for Sanjaya backend API.

**Key Features**:

- **JWT Authentication**: Automatic token management
- **Token Refresh**: Refreshes expired tokens automatically
- **Error Handling**: Handles HTTP errors gracefully
- **Debug Logging**: Optional detailed HTTP logging

**Key Methods**:

- **`ensure_token()`**: Ensures valid JWT token
- **`get_clients()`**: Fetches list of clients
- **`get_client_by_id(client_id)`**: Fetches client details
- **`get_sherpas_by_client_id(client_id)`**: Fetches sherpas for client
- **`basic_analytics(...)`**: Calls basic analytics API
- **`route_analytics(...)`**: Calls route analytics API

**Authentication Flow**:

```
1. Check if token exists and is valid
2. If not, call /api/v1/master_fm/user/login
3. Extract JWT token from response
4. Store token for future requests
5. Include token in Authorization header
```

### 5. Caching (`src/cache.py`)

**Purpose**: TTL-based caching to reduce API calls.

**Implementation**:

- **TTLCache**: Thread-safe cache with TTL
- **Cache Keys**: Structured keys (e.g., `"all_clients"`, `"client_21"`)
- **TTL Values**:
  - Clients: 600 seconds (10 minutes)
  - Client details: 600 seconds (10 minutes)
  - Sherpas: 300 seconds (5 minutes)

**Usage**:

```python
# Get or set cached value
clients = await client_cache.get_or_set("all_clients", client.get_clients)
```

### 6. Formatting (`src/formatting.py`)

**Purpose**: Format API responses for user consumption.

**Key Functions**:

- **`summarize_basic_analytics(payload)`**
  - Formats comprehensive analytics summary
  - Handles multiple metrics
  - Returns formatted text

- **`extract_item_value(payload, item, sherpa_hint)`**
  - Extracts specific metric value
  - Handles sherpa-specific extraction
  - Handles per-sherpa queries (returns all sherpas)
  - Returns value and optional note

**Extraction Logic**:

1. Check sherpa-specific arrays (`sherpa_wise_distance`, `sherpa_wise_trips`)
2. Check top-level keys
3. Handle fuzzy matching for sherpa names
4. Return formatted value with notes

---

## Data Flow

### Example: "total trips today for client ceat-nagpur fleet CEAT-Nagpur-North-Plant"

```
1. MCP Client sends request:
   ┌─────────────────────────────────────────────┐
   │ tools/call                                  │
   │ name: "sanjaya_chat"                        │
   │ arguments: {                                │
   │   "query": "total trips today for client    │
   │             ceat-nagpur fleet               │
   │             CEAT-Nagpur-North-Plant"        │
   │ }                                           │
   └─────────────────────────────────────────────┘
                    │
                    ▼
2. MCP Server receives request:
   ┌─────────────────────────────────────────────┐
   │ sanjaya_chat(query)                         │
   └─────────────────────────────────────────────┘
                    │
                    ▼
3. NLU Parsing:
   ┌─────────────────────────────────────────────┐
   │ parse_query(query)                          │
   │   ├─► Ollama (if enabled)                  │
   │   │   └─► Returns JSON:                    │
   │   │       {                                 │
   │   │         "intent": "basic_analytics_item"│
   │   │         "item": "total_trips"          │
   │   │         "fm_client_name": "ceat-nagpur"│
   │   │         "fleet_name": "CEAT-Nagpur-..." │
   │   │         "time_phrase": "today"          │
   │   │       }                                 │
   │   └─► Post-processing:                     │
   │       • Validate sherpa_hint                │
   │       • Fix swapped client/fleet            │
   │       • Normalize timezone                  │
   └─────────────────────────────────────────────┘
                    │
                    ▼
4. Business Logic:
   ┌─────────────────────────────────────────────┐
   │ get_metric(                                 │
   │   metric="total_trips",                     │
   │   client_name="ceat-nagpur",               │
   │   fleet_name="CEAT-Nagpur-North-Plant",     │
   │   time_range="today"                        │
   │ )                                           │
   │                                             │
   │ 1. Parse time range:                       │
   │    parse_time_range("today")               │
   │    └─► TimeRange(start=2026-01-26 00:00:00,│
   │                   end=2026-01-26 22:30:00)  │
   │                                             │
   │ 2. Resolve sherpas (if needed):            │
   │    • Get client_id from cache              │
   │    • Fetch sherpas for client               │
   │    • Filter by fleet_name                  │
   │    • Get list of sherpa names              │
   └─────────────────────────────────────────────┘
                    │
                    ▼
5. API Call:
   ┌─────────────────────────────────────────────┐
   │ client.basic_analytics(                     │
   │   fm_client_name="ceat-nagpur",            │
   │   start_time="2026-01-26 00:00:00",        │
   │   end_time="2026-01-26 22:30:00",          │
   │   fleet_name="CEAT-Nagpur-North-Plant",     │
   │   sherpa_name=["tug-51-...", "tug-93-..."] │
   │ )                                           │
   │                                             │
   │ HTTP GET:                                   │
   │ /api/v1/master_fm/analytics/               │
   │   basic_analytics/ceat-nagpur?             │
   │   start_time=2026-01-26+00:00:00&           │
   │   end_time=2026-01-26+22:30:00&            │
   │   fleet_name=CEAT-Nagpur-North-Plant&      │
   │   sherpa_name=tug-51-...&                  │
   │   sherpa_name=tug-93-...                   │
   └─────────────────────────────────────────────┘
                    │
                    ▼
6. Response Processing:
   ┌─────────────────────────────────────────────┐
   │ extract_item_value(                        │
   │   payload={                                │
   │     "total_trips": 101,                    │
   │     "sherpa_wise_trips": [...]             │
   │   },                                       │
   │   item="total_trips",                      │
   │   sherpa_hint=None                         │
   │ )                                          │
   │                                            │
   │ Returns: (101, "")                         │
   └─────────────────────────────────────────────┘
                    │
                    ▼
7. Format Response:
   ┌─────────────────────────────────────────────┐
   │ "total_trips: 101"                         │
   └─────────────────────────────────────────────┘
                    │
                    ▼
8. MCP Server sends response:
   ┌─────────────────────────────────────────────┐
   │ {                                           │
   │   "jsonrpc": "2.0",                        │
   │   "id": 1,                                 │
   │   "result": {                              │
   │     "content": [                          │
   │       {                                    │
   │         "type": "text",                   │
   │         "text": "total_trips: 101"        │
   │       }                                    │
   │     ]                                     │
   │   }                                        │
   │ }                                          │
   └─────────────────────────────────────────────┘
```

---

## Interface with MCP Clients

### Standard MCP Client Interface

The server implements the standard MCP protocol, making it compatible with any MCP client:

- **Claude Desktop**: Production use
- **MCP Inspector**: Development/testing
- **Custom Clients**: Any client implementing MCP protocol

### Communication Protocol

**Transport**: stdio (standard input/output)

**Protocol**: JSON-RPC 2.0

**Message Format**:

```json
{
  "jsonrpc": "2.0",
  "id": <unique_id>,
  "method": "<method_name>",
  "params": { ... }
}
```

### Client Initialization

```
1. Client starts server process
2. Client sends initialize request
3. Server responds with capabilities
4. Client sends initialized notification
5. Ready for requests
```

### Available Capabilities

**Tools**:
- `sanjaya_chat`
- `get_metric`
- `get_analytics_summary`
- `resolve_client_name`
- `resolve_fleet_name`

**Resources**:
- `sanjaya://clients`
- `sanjaya://clients/{client_id}/fleets`
- `sanjaya://fleets/{fleet_id}/sherpas`

**Prompts**:
- `analytics_summary_query`
- `metric_query`

### Example Client Usage

```python
# Initialize session
async with ClientSession(read, write) as session:
    await session.initialize()
    
    # Call tool
    result = await session.call_tool(
        "sanjaya_chat",
        {"query": "total trips today"}
    )
    
    # Read resource
    resource = await session.read_resource(
        "sanjaya://clients"
    )
    
    # Get prompt
    prompt = await session.get_prompt(
        "analytics_summary_query",
        {}
    )
```

### Error Handling

The server returns standard JSON-RPC errors:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "error": {
    "code": -32603,
    "message": "Internal error",
    "data": "Error details..."
  }
}
```

---

## Key Design Decisions

### 1. Standard MCP Patterns

**Decision**: Follow standard MCP patterns (tools, resources, prompts)

**Rationale**:
- Compatibility with any MCP client
- Clear separation of concerns
- Reusable components

### 2. NLU with Fallback

**Decision**: Use Ollama (optional) with heuristic fallback

**Rationale**:
- Better intent extraction with LLM
- Works without LLM (heuristic parser)
- Graceful degradation

### 3. TTL-Based Caching

**Decision**: Cache API responses with TTL

**Rationale**:
- Reduce API calls
- Improve performance
- Balance freshness vs. performance

### 4. Sherpa Resolution

**Decision**: Automatically fetch all sherpas if not specified

**Rationale**:
- Matches user expectations
- Reduces need for explicit sherpa specification
- Handles "per sherpa" queries correctly

### 5. Plain Text Responses

**Decision**: Return plain text (not structured JSON)

**Rationale**:
- Simpler for LLM clients to consume
- Standard MCP content type
- Easy to format for display

### 6. Client/Fleet Name Normalization

**Decision**: Normalize case by looking up from API

**Rationale**:
- API is case-sensitive
- User input may have wrong case
- Ensures correct API calls

### 7. Validation of Extracted Parameters

**Decision**: Post-process NLU output to validate parameters

**Rationale**:
- Prevents hallucinated values (e.g., sherpa names)
- Fixes common parsing errors (swapped client/fleet)
- Improves reliability

---

## Extension Points

### Adding New Metrics

1. Add metric to `ALLOWED_ITEMS` in `src/nlu.py`
2. Add extraction logic in `src/formatting.py` (`extract_item_value`)
3. Update NLU patterns if needed

### Adding New Time Phrases

1. Add pattern to `src/time_parse.py` (`parse_time_range`)
2. Update NLU patterns if needed

### Adding New API Endpoints

1. Add method to `src/sanjaya_client.py`
2. Add tool to `mcp_server.py` if needed
3. Update caching if applicable

### Custom Client Integration

The server is designed to work with any MCP client. To integrate:

1. Implement MCP client protocol
2. Connect via stdio
3. Call tools, read resources, get prompts
4. Handle responses

---

## Security Considerations

### Authentication

- JWT tokens stored in memory only
- Tokens automatically refreshed
- No tokens in client configuration

### Input Validation

- NLU output validated before use
- Parameter normalization prevents injection
- Time range validation prevents errors

### Logging

- Sensitive data (tokens) redacted in logs
- HTTP trace optional (disabled by default)
- Logs go to stderr (not stdout)

---

## Performance Considerations

### Caching Strategy

- Clients: 10 minutes TTL
- Client details: 10 minutes TTL
- Sherpas: 5 minutes TTL

### API Call Optimization

- Batch sherpa queries (list of sherpa names)
- Cache frequently accessed data
- Parallel requests where possible

### NLU Performance

- Ollama calls are async (non-blocking)
- Heuristic parser is fast (regex-based)
- Fallback ensures no blocking

---

## Troubleshooting

### Common Issues

1. **"No module named mcp"**
   - Install dependencies: `pip install -r requirements.txt`

2. **"Failed to parse JSON-RPC message"**
   - Ensure logging goes to stderr (not stdout)
   - Check server is using FastMCP

3. **"Client not found"**
   - Check client name spelling
   - Use `resolve_client_name` tool
   - Check cache TTL (may need refresh)

4. **"Sherpa name not found"**
   - Check sherpa name spelling
   - Use fuzzy matching (partial names work)
   - Check fleet has sherpas

5. **"Invalid time range"**
   - Check time phrase format
   - Check timezone is valid IANA timezone
   - Check date parsing logic

---

## Conclusion

The Sanjaya Analytics MCP Server provides a clean, standard interface to fleet management analytics. Its layered architecture ensures maintainability, while standard MCP patterns ensure compatibility with any MCP client. The optional NLU with heuristic fallback provides flexibility, while caching and validation ensure reliability and performance.

