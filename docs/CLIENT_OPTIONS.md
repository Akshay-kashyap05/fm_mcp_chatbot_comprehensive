# Standard MCP Client Options

Since the Sanjaya Analytics MCP server follows standard MCP patterns, you can use any standard MCP client. Here are your options:

## 1. Claude Desktop (Recommended)

Claude Desktop is Anthropic's official MCP client. It provides a rich UI and automatically discovers MCP servers.

### Setup

**Important: Security & Configuration**

The server handles all authentication and API configuration internally. The client (Claude Desktop) should **never** know about:
- Base URL
- Username/Password
- API credentials

All sensitive configuration stays in the server's `.env` file, which is loaded automatically.

**Option A: Automatic Installation (Recommended)**

The MCP CLI can automatically configure Claude Desktop for you:

```bash
# Activate your virtual environment
source venv/bin/activate

# Install the server to Claude Desktop
# The server will automatically load .env file from the project directory
mcp install mcp_server.py

# Or with a custom name
mcp install mcp_server.py --name "Sanjaya Analytics"
```

That's it! The `mcp install` command automatically:
- Finds Claude Desktop's config file
- Adds your server configuration
- Sets up the correct paths and working directory
- The server loads `.env` file automatically (no credentials in client config!)

**⚠️ Security Note:** Never pass authentication credentials via `-v` flags or in the client config. Keep them in the `.env` file only.

**Option B: Manual Configuration**

If you prefer manual setup or `mcp install` doesn't work:

1. **Install Claude Desktop** (if not already installed)
   - Download from: https://claude.ai/download

2. **Edit Claude Desktop's MCP configuration file:**
   
   **macOS:**
   ```
   ~/Library/Application Support/Claude/claude_desktop_config.json
   ```
   
   **Windows:**
   ```
   %APPDATA%\Claude\claude_desktop_config.json
   ```
   
   **Linux:**
   ```
   ~/.config/Claude/claude_desktop_config.json
   ```

3. **Add Server Configuration**

   Add this to the `mcpServers` section:

   ```json
   {
     "mcpServers": {
       "sanjaya-analytics": {
         "command": "python",
         "args": ["/absolute/path/to/mcp_server.py"],
         "cwd": "/absolute/path/to/project/directory"
       }
     }
   }
   ```

   **Important:** 
   - Do NOT include an `env` section with credentials
   - The server automatically loads `.env` file from the `cwd` directory
   - All authentication stays in the server's `.env` file, not in the client config

4. **Restart Claude Desktop**

   The server will be automatically discovered and available in Claude Desktop.

## 2. ChatGPT with MCP Support

If you have access to ChatGPT with MCP support, you can configure it similarly.

## 3. MCP Inspector (Development/Testing)

The MCP Inspector is a web-based tool for testing MCP servers.

### Installation

```bash
npm install -g @modelcontextprotocol/inspector
```

### Usage

```bash
mcp-inspector python mcp_server.py
```

This opens a web UI where you can:
- Browse available tools
- Browse resources
- Test prompts
- Call tools interactively

## 4. Custom Python Client (Current)

Your current `chat_client.py` is already a standard MCP client using the official MCP Python SDK. It works perfectly, but you can replace it with any of the above options.

## 5. Build Your Own Client

Since the server follows standard MCP patterns, you can build a custom client using:

- **Python**: `mcp` package (what you're currently using)
- **TypeScript/JavaScript**: `@modelcontextprotocol/sdk`
- **Any language**: As long as it implements the MCP protocol over stdio

## Benefits of Using Standard Clients

1. **Rich UI**: Claude Desktop provides a polished interface
2. **Automatic Discovery**: No manual connection code needed
3. **Better UX**: Built-in features like conversation history, formatting, etc.
4. **No Maintenance**: Standard clients are maintained by their creators
5. **Multi-Server**: Can connect to multiple MCP servers simultaneously

## Current Client vs Standard Clients

| Feature | Current `chat_client.py` | Claude Desktop | MCP Inspector |
|---------|-------------------------|----------------|--------------|
| Simple text interface | ✅ | ❌ | ❌ |
| Rich UI | ❌ | ✅ | ✅ |
| Tool discovery | ✅ | ✅ | ✅ |
| Resource browsing | ✅ | ✅ | ✅ |
| Prompt templates | ✅ | ✅ | ✅ |
| Conversation history | ❌ | ✅ | ❌ |
| Multi-server support | ❌ | ✅ | ✅ |
| Development/testing | ✅ | ❌ | ✅ |

## Recommendation

- **For daily use**: Use **Claude Desktop** - best UX and features
- **For development/testing**: Use **MCP Inspector** - great for debugging
- **For simple scripts**: Keep your current `chat_client.py` - it's perfectly fine

All options work with your refactored server since it follows standard MCP patterns!

