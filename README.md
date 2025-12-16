# GitHub Search MCP Server

A Model Context Protocol (MCP) server for searching and exploring GitHub repositories. Compatible with OpenAI's ChatGPT and Responses API.

## Features

- **5 MCP Tools** with `readOnlyHint` annotations:
  1. `clone_repository` - Clone repositories from a whitelisted GitHub user
  2. `search_code` - Fast grep-based code search with regex support
  3. `get_tree` - Display repository directory structure
  4. `read_file` - Read file contents with line range support
  5. `get_outline` - Extract code structure (classes, functions, methods)

- **No Authentication Required** - Server handles GitHub PAT internally
- **Streamable HTTP Transport** - Compatible with OpenAI's MCP integration
- **MCP Protocol 2025-06-18** - Latest specification compliance

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
cp .env.example .env
# Edit .env with your GitHub PAT token
```

### 3. Run the Server

```bash
python main.py
```

The server will start on `http://localhost:8000`.

## Docker

```bash
# Build
docker build -t github-mcp-server .

# Run
docker run -p 8000:8000 -e GITHUB_PAT=your_token github-mcp-server
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Server info and available endpoints |
| `/mcp` | POST | MCP protocol endpoint (JSON-RPC) |
| `/health` | GET | Health check |
| `/capabilities` | GET | Server capabilities |

## MCP Tools

### clone_repository

Clone a GitHub repository from the whitelisted user.

```json
{
  "name": "clone_repository",
  "arguments": {
    "repo_name": "my-repo"
  }
}
```

### search_code

Search for code patterns using grep.

```json
{
  "name": "search_code",
  "arguments": {
    "repo_name": "my-repo",
    "pattern": "def main",
    "file_pattern": "*.py",
    "case_sensitive": false,
    "max_results": 50
  }
}
```

### get_tree

Display repository directory tree.

```json
{
  "name": "get_tree",
  "arguments": {
    "repo_name": "my-repo",
    "path": "src",
    "max_depth": 3,
    "show_hidden": false
  }
}
```

### read_file

Read file contents with optional line ranges.

```json
{
  "name": "read_file",
  "arguments": {
    "repo_name": "my-repo",
    "file_path": "src/main.py",
    "start_line": 1,
    "end_line": 50
  }
}
```

### get_outline

Get code outline showing classes and functions.

```json
{
  "name": "get_outline",
  "arguments": {
    "repo_name": "my-repo",
    "file_path": "src/main.py"
  }
}
```

## Connecting to ChatGPT

1. Deploy the server to a public URL (e.g., Railway, Render, or AWS)
2. In ChatGPT, go to Settings > Connectors > Add Connector
3. Enter your server URL (e.g., `https://your-server.com/mcp`)
4. Select "No authentication required"

## Security

- **Whitelisted User**: Only repositories from the configured `ALLOWED_USERNAME` can be cloned
- **Path Traversal Prevention**: All file paths are validated
- **No Secrets in Responses**: GitHub PAT is server-side only

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `GITHUB_PAT` | GitHub Personal Access Token | (empty) |
| `HOST` | Server host | `0.0.0.0` |
| `PORT` | Server port | `8000` |
| `REPO_STORAGE_PATH` | Path for cloned repos | `/tmp/repos` |
| `ALLOWED_USERNAME` | Whitelisted GitHub username | `anirudhadasgupta` |

## License

MIT
