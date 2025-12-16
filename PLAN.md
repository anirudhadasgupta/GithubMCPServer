# GitHub Search MCP Tool - Conceptual Plan

## Overview

Build a simple yet effective GitHub Search MCP (Model Context Protocol) server compatible with OpenAI's ChatGPT and the Responses API. The server will enable repository cloning, fast code search, and file operations for repositories owned by `anirudhadasgupta`.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    ChatGPT / OpenAI API                      │
└──────────────────────────┬──────────────────────────────────┘
                           │ HTTP/SSE Transport
                           ▼
┌─────────────────────────────────────────────────────────────┐
│               GitHub MCP Server (FastAPI + FastMCP)          │
├─────────────────────────────────────────────────────────────┤
│  Endpoints:                                                  │
│  - POST /mcp (MCP protocol messages)                        │
│  - GET /health (health check)                               │
│  - GET /capabilities (server capabilities)                  │
├─────────────────────────────────────────────────────────────┤
│  Tools (5 max, readOnlyHint=true):                          │
│  1. clone_repository - Clone repos from anirudhadasgupta    │
│  2. search_code - Fast grep/ripgrep search                  │
│  3. get_tree - Display repository tree structure            │
│  4. read_file - Read and download file contents             │
│  5. get_outline - Show code outline/structure               │
├─────────────────────────────────────────────────────────────┤
│  Resources:                                                  │
│  - repo://{owner}/{repo} - Repository resources             │
│  - file://{path} - File resources                           │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                  Local File System                           │
│  /tmp/repos/{owner}/{repo} - Cloned repositories            │
└─────────────────────────────────────────────────────────────┘
```

## Technical Specifications

### Transport Protocol
- **Streamable HTTP** (primary) - single POST endpoint for JSON-RPC messages
- Compatible with OpenAI's Responses API and ChatGPT Connectors

### Authentication
- **No Auth** - Server-side GitHub PAT token via environment variable
- Server handles GitHub API authentication internally

### Tool Annotations (MCP 2025-06-18 spec)
All tools marked with:
```json
{
  "annotations": {
    "readOnlyHint": true,
    "destructiveHint": false,
    "idempotentHint": true,
    "openWorldHint": true
  }
}
```

## Tools Design (5 Tools)

### 1. `clone_repository`
**Purpose**: Clone a GitHub repository from anirudhadasgupta
**Input Schema**:
```json
{
  "type": "object",
  "properties": {
    "repo_name": {
      "type": "string",
      "description": "Repository name to clone"
    }
  },
  "required": ["repo_name"]
}
```

### 2. `search_code`
**Purpose**: Fast code search using ripgrep
**Input Schema**:
```json
{
  "type": "object",
  "properties": {
    "repo_name": {"type": "string"},
    "pattern": {"type": "string"},
    "file_pattern": {"type": "string", "description": "Optional glob pattern"},
    "case_sensitive": {"type": "boolean", "default": false}
  },
  "required": ["repo_name", "pattern"]
}
```

### 3. `get_tree`
**Purpose**: Display repository directory tree
**Input Schema**:
```json
{
  "type": "object",
  "properties": {
    "repo_name": {"type": "string"},
    "path": {"type": "string", "default": "."},
    "max_depth": {"type": "integer", "default": 3}
  },
  "required": ["repo_name"]
}
```

### 4. `read_file`
**Purpose**: Read file contents (downloadable for ChatGPT)
**Input Schema**:
```json
{
  "type": "object",
  "properties": {
    "repo_name": {"type": "string"},
    "file_path": {"type": "string"},
    "start_line": {"type": "integer"},
    "end_line": {"type": "integer"}
  },
  "required": ["repo_name", "file_path"]
}
```

### 5. `get_outline`
**Purpose**: Show code structure/outline (functions, classes, etc.)
**Input Schema**:
```json
{
  "type": "object",
  "properties": {
    "repo_name": {"type": "string"},
    "file_path": {"type": "string"}
  },
  "required": ["repo_name", "file_path"]
}
```

## Endpoints

### `/health`
```json
{
  "status": "healthy",
  "version": "1.0.0",
  "timestamp": "2025-12-16T..."
}
```

### `/capabilities`
```json
{
  "tools": ["clone_repository", "search_code", "get_tree", "read_file", "get_outline"],
  "resources": true,
  "transport": "streamable-http",
  "authentication": "none"
}
```

## Persistence Strategy

To persist after ChatGPT's URI rotation:
1. Use stable tool names and schemas
2. Implement idempotent operations
3. Cache cloned repositories in `/tmp/repos/`
4. Return consistent resource URIs

## Security Considerations

1. **Whitelist only**: Only clone from `anirudhadasgupta` username
2. **Path traversal prevention**: Validate all file paths
3. **Rate limiting**: Implement basic rate limiting
4. **No secrets in responses**: GitHub PAT is server-side only

## Implementation Checklist

- [ ] Project structure and dependencies
- [ ] FastAPI server with MCP integration
- [ ] Tool 1: clone_repository
- [ ] Tool 2: search_code (ripgrep)
- [ ] Tool 3: get_tree
- [ ] Tool 4: read_file
- [ ] Tool 5: get_outline
- [ ] /health endpoint
- [ ] /capabilities endpoint
- [ ] Error handling
- [ ] Environment configuration
- [ ] Documentation

## Dependencies

```
fastapi>=0.104.0
uvicorn>=0.24.0
mcp>=1.0.0
httpx>=0.25.0
python-dotenv>=1.0.0
```

## File Structure

```
GithubMCPServer/
├── main.py              # FastAPI server entry point
├── mcp_server.py        # MCP server implementation
├── tools/
│   ├── __init__.py
│   ├── clone.py         # clone_repository tool
│   ├── search.py        # search_code tool
│   ├── tree.py          # get_tree tool
│   ├── file.py          # read_file tool
│   └── outline.py       # get_outline tool
├── utils/
│   ├── __init__.py
│   └── github.py        # GitHub API utilities
├── requirements.txt
├── .env.example
├── Dockerfile
└── README.md
```
