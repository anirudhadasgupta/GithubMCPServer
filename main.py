"""
GitHub Search MCP Server

A Model Context Protocol (MCP) server for searching and exploring GitHub repositories.
Compatible with OpenAI's ChatGPT and Responses API.

Author: anirudhadasgupta
"""

import os
import re
import ast
import json
import shutil
import asyncio
import subprocess
import base64
import zipfile
import io
from pathlib import Path
from datetime import datetime
from typing import Optional, Any

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
GITHUB_PAT = os.getenv("GITHUB_PAT", "")
REPO_STORAGE_PATH = Path(os.getenv("REPO_STORAGE_PATH", "/tmp/repos"))
ALLOWED_USERNAME = os.getenv("ALLOWED_USERNAME", "anirudhadasgupta")
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8000"))

# Ensure storage path exists
REPO_STORAGE_PATH.mkdir(parents=True, exist_ok=True)

# FastAPI app
app = FastAPI(
    title="GitHub Search MCP Server",
    description="MCP server for searching and exploring GitHub repositories",
    version="1.0.0"
)

# CORS middleware for broad compatibility
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================================
# Tool Definitions with MCP 2025-06-18 Specification
# ============================================================================

TOOLS = [
    {
        "name": "clone_repository",
        "title": "Clone Repository",
        "description": f"Clone a GitHub repository from user '{ALLOWED_USERNAME}'. The repository will be available for searching and browsing.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "repo_name": {
                    "type": "string",
                    "description": "Name of the repository to clone (without owner prefix)"
                }
            },
            "required": ["repo_name"]
        },
        "annotations": {
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True
        }
    },
    {
        "name": "search_code",
        "title": "Search Code",
        "description": "Search for code patterns in a cloned repository using fast grep/ripgrep-style search. Returns matching lines with context.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "repo_name": {
                    "type": "string",
                    "description": "Name of the repository to search in"
                },
                "pattern": {
                    "type": "string",
                    "description": "Search pattern (supports regex)"
                },
                "file_pattern": {
                    "type": "string",
                    "description": "Optional glob pattern to filter files (e.g., '*.py', '*.js')"
                },
                "case_sensitive": {
                    "type": "boolean",
                    "description": "Whether search is case-sensitive",
                    "default": False
                },
                "max_results": {
                    "type": "integer",
                    "description": "Maximum number of results to return",
                    "default": 50
                }
            },
            "required": ["repo_name", "pattern"]
        },
        "annotations": {
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False
        }
    },
    {
        "name": "get_tree",
        "title": "Get Repository Tree",
        "description": "Display the directory structure of a cloned repository as a tree view.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "repo_name": {
                    "type": "string",
                    "description": "Name of the repository"
                },
                "path": {
                    "type": "string",
                    "description": "Subdirectory path to start from",
                    "default": "."
                },
                "max_depth": {
                    "type": "integer",
                    "description": "Maximum depth of tree traversal",
                    "default": 3
                },
                "show_hidden": {
                    "type": "boolean",
                    "description": "Whether to show hidden files/directories",
                    "default": False
                }
            },
            "required": ["repo_name"]
        },
        "annotations": {
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False
        }
    },
    {
        "name": "read_file",
        "title": "Read File",
        "description": "Read the contents of a file from a cloned repository. Supports partial reads with line ranges.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "repo_name": {
                    "type": "string",
                    "description": "Name of the repository"
                },
                "file_path": {
                    "type": "string",
                    "description": "Path to the file within the repository"
                },
                "start_line": {
                    "type": "integer",
                    "description": "Starting line number (1-indexed)",
                    "default": 1
                },
                "end_line": {
                    "type": "integer",
                    "description": "Ending line number (inclusive, 0 for entire file)",
                    "default": 0
                }
            },
            "required": ["repo_name", "file_path"]
        },
        "annotations": {
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False
        }
    },
    {
        "name": "get_outline",
        "title": "Get Code Outline",
        "description": "Get the structural outline of a code file showing classes, functions, and methods with their line numbers.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "repo_name": {
                    "type": "string",
                    "description": "Name of the repository"
                },
                "file_path": {
                    "type": "string",
                    "description": "Path to the code file within the repository"
                }
            },
            "required": ["repo_name", "file_path"]
        },
        "annotations": {
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False
        }
    },
    {
        "name": "archive_repository",
        "title": "Archive Repository",
        "description": "Archive a cloned repository as a ZIP file and return it as base64-encoded content. This allows downloading the entire repository for local analysis. The .git directory is excluded to reduce size.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "repo_name": {
                    "type": "string",
                    "description": "Name of the repository to archive"
                },
                "include_hidden": {
                    "type": "boolean",
                    "description": "Whether to include hidden files (starting with .) except .git",
                    "default": False
                },
                "path": {
                    "type": "string",
                    "description": "Optional subdirectory path to archive (defaults to entire repo)",
                    "default": ""
                }
            },
            "required": ["repo_name"]
        },
        "annotations": {
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False
        }
    }
]

# Resources definition
RESOURCES = [
    {
        "uri": f"repo://{ALLOWED_USERNAME}/{{repo_name}}",
        "name": "Repository",
        "description": f"GitHub repositories from {ALLOWED_USERNAME}",
        "mimeType": "application/x-directory"
    }
]

# ============================================================================
# Tool Implementation Functions
# ============================================================================

def validate_repo_name(repo_name: str) -> bool:
    """Validate repository name to prevent path traversal"""
    return bool(re.match(r'^[\w\-\.]+$', repo_name))


def get_repo_path(repo_name: str) -> Path:
    """Get the local path for a repository"""
    return REPO_STORAGE_PATH / ALLOWED_USERNAME / repo_name


def validate_file_path(repo_path: Path, file_path: str) -> Optional[Path]:
    """Validate file path to prevent path traversal attacks"""
    try:
        full_path = (repo_path / file_path).resolve()
        resolved_repo = repo_path.resolve()

        # Reject if trying to access the repo directory itself (not a file)
        if full_path == resolved_repo:
            return None

        # Check that the file is inside the repo directory
        # The repo path must be a parent of the full path
        if resolved_repo not in full_path.parents:
            return None

        # Additional safety check: path string must start with repo path
        if not str(full_path).startswith(str(resolved_repo)):
            return None

        return full_path
    except (ValueError, RuntimeError):
        return None


async def clone_repository_impl(repo_name: str) -> dict:
    """Clone a repository from the allowed username"""
    if not validate_repo_name(repo_name):
        return {"error": "Invalid repository name", "success": False}

    repo_path = get_repo_path(repo_name)

    # Check if already cloned
    if repo_path.exists() and (repo_path / ".git").exists():
        # Pull latest changes
        try:
            result = subprocess.run(
                ["git", "-C", str(repo_path), "pull", "--ff-only"],
                capture_output=True,
                text=True,
                timeout=60
            )
            return {
                "success": True,
                "message": f"Repository '{repo_name}' already cloned. Updated with latest changes.",
                "path": str(repo_path),
                "status": "updated"
            }
        except subprocess.TimeoutExpired:
            return {
                "success": True,
                "message": f"Repository '{repo_name}' exists (update timed out)",
                "path": str(repo_path),
                "status": "existing"
            }

    # Clone the repository
    repo_path.parent.mkdir(parents=True, exist_ok=True)

    if GITHUB_PAT:
        clone_url = f"https://{GITHUB_PAT}@github.com/{ALLOWED_USERNAME}/{repo_name}.git"
    else:
        clone_url = f"https://github.com/{ALLOWED_USERNAME}/{repo_name}.git"

    try:
        result = subprocess.run(
            ["git", "clone", "--depth", "1", clone_url, str(repo_path)],
            capture_output=True,
            text=True,
            timeout=120
        )

        if result.returncode != 0:
            return {
                "success": False,
                "error": f"Failed to clone repository: {result.stderr}",
                "status": "failed"
            }

        return {
            "success": True,
            "message": f"Successfully cloned '{ALLOWED_USERNAME}/{repo_name}'",
            "path": str(repo_path),
            "status": "cloned"
        }
    except subprocess.TimeoutExpired:
        return {
            "success": False,
            "error": "Clone operation timed out",
            "status": "timeout"
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "status": "error"
        }


async def search_code_impl(
    repo_name: str,
    pattern: str,
    file_pattern: Optional[str] = None,
    case_sensitive: bool = False,
    max_results: int = 50
) -> dict:
    """Search for code patterns using grep"""
    if not validate_repo_name(repo_name):
        return {"error": "Invalid repository name", "matches": []}

    repo_path = get_repo_path(repo_name)
    if not repo_path.exists():
        return {"error": f"Repository '{repo_name}' not cloned. Use clone_repository first.", "matches": []}

    matches = []

    try:
        # Build grep command
        grep_args = ["grep", "-rn"]
        if not case_sensitive:
            grep_args.append("-i")
        grep_args.append("--")
        grep_args.append(pattern)

        if file_pattern:
            grep_args.extend(["--include", file_pattern])

        grep_args.append(str(repo_path))

        result = subprocess.run(
            grep_args,
            capture_output=True,
            text=True,
            timeout=30,
            cwd=str(repo_path)
        )

        for line in result.stdout.split('\n')[:max_results]:
            if not line.strip():
                continue

            # Parse grep output: filename:line_number:content
            parts = line.split(':', 2)
            if len(parts) >= 3:
                file_path = parts[0].replace(str(repo_path) + '/', '')
                try:
                    line_num = int(parts[1])
                    content = parts[2]
                    matches.append({
                        "file": file_path,
                        "line": line_num,
                        "content": content.strip()[:500]  # Limit content length
                    })
                except (ValueError, IndexError):
                    continue

        return {
            "success": True,
            "pattern": pattern,
            "matches": matches,
            "total_matches": len(matches),
            "truncated": len(result.stdout.split('\n')) > max_results
        }
    except subprocess.TimeoutExpired:
        return {"error": "Search timed out", "matches": matches}
    except Exception as e:
        return {"error": str(e), "matches": []}


async def get_tree_impl(
    repo_name: str,
    path: str = ".",
    max_depth: int = 3,
    show_hidden: bool = False
) -> dict:
    """Get directory tree structure"""
    if not validate_repo_name(repo_name):
        return {"error": "Invalid repository name", "tree": ""}

    repo_path = get_repo_path(repo_name)
    if not repo_path.exists():
        return {"error": f"Repository '{repo_name}' not cloned. Use clone_repository first.", "tree": ""}

    target_path = validate_file_path(repo_path, path)
    if target_path is None:
        target_path = repo_path

    if not target_path.exists():
        return {"error": f"Path '{path}' does not exist", "tree": ""}

    def build_tree(current_path: Path, prefix: str = "", depth: int = 0) -> list:
        if depth > max_depth:
            return []

        lines = []
        try:
            items = sorted(current_path.iterdir(), key=lambda x: (not x.is_dir(), x.name.lower()))
        except PermissionError:
            return []

        # Filter hidden files if needed
        if not show_hidden:
            items = [i for i in items if not i.name.startswith('.')]

        for i, item in enumerate(items):
            is_last = i == len(items) - 1
            connector = "└── " if is_last else "├── "

            if item.is_dir():
                lines.append(f"{prefix}{connector}{item.name}/")
                extension = "    " if is_last else "│   "
                lines.extend(build_tree(item, prefix + extension, depth + 1))
            else:
                size = item.stat().st_size
                size_str = f" ({size:,} bytes)" if size < 1024 * 1024 else f" ({size / 1024 / 1024:.1f} MB)"
                lines.append(f"{prefix}{connector}{item.name}{size_str}")

        return lines

    tree_lines = [f"{target_path.name}/"]
    tree_lines.extend(build_tree(target_path))

    return {
        "success": True,
        "repo_name": repo_name,
        "path": path,
        "tree": "\n".join(tree_lines)
    }


async def read_file_impl(
    repo_name: str,
    file_path: str,
    start_line: int = 1,
    end_line: int = 0
) -> dict:
    """Read file contents"""
    if not validate_repo_name(repo_name):
        return {"error": "Invalid repository name", "content": ""}

    repo_path = get_repo_path(repo_name)
    if not repo_path.exists():
        return {"error": f"Repository '{repo_name}' not cloned. Use clone_repository first.", "content": ""}

    full_path = validate_file_path(repo_path, file_path)
    if full_path is None:
        return {"error": "Invalid file path", "content": ""}

    if not full_path.exists():
        return {"error": f"File '{file_path}' not found", "content": ""}

    if not full_path.is_file():
        return {"error": f"'{file_path}' is not a file", "content": ""}

    try:
        # Check file size
        file_size = full_path.stat().st_size
        max_size = 1024 * 1024  # 1MB limit

        if file_size > max_size:
            return {
                "error": f"File too large ({file_size / 1024 / 1024:.1f} MB). Maximum size is 1MB.",
                "content": ""
            }

        with open(full_path, 'r', encoding='utf-8', errors='replace') as f:
            lines = f.readlines()

        total_lines = len(lines)

        # Handle line ranges
        start_idx = max(0, start_line - 1)
        end_idx = total_lines if end_line <= 0 else min(end_line, total_lines)

        selected_lines = lines[start_idx:end_idx]

        # Add line numbers
        numbered_content = []
        for i, line in enumerate(selected_lines, start=start_idx + 1):
            numbered_content.append(f"{i:4d} | {line.rstrip()}")

        return {
            "success": True,
            "repo_name": repo_name,
            "file_path": file_path,
            "content": "\n".join(numbered_content),
            "total_lines": total_lines,
            "start_line": start_idx + 1,
            "end_line": end_idx,
            "file_size": file_size
        }
    except UnicodeDecodeError:
        return {"error": "Cannot read binary file", "content": ""}
    except Exception as e:
        return {"error": str(e), "content": ""}


async def get_outline_impl(repo_name: str, file_path: str) -> dict:
    """Get code outline for a file"""
    if not validate_repo_name(repo_name):
        return {"error": "Invalid repository name", "outline": []}

    repo_path = get_repo_path(repo_name)
    if not repo_path.exists():
        return {"error": f"Repository '{repo_name}' not cloned. Use clone_repository first.", "outline": []}

    full_path = validate_file_path(repo_path, file_path)
    if full_path is None:
        return {"error": "Invalid file path", "outline": []}

    if not full_path.exists():
        return {"error": f"File '{file_path}' not found", "outline": []}

    outline = []

    try:
        with open(full_path, 'r', encoding='utf-8', errors='replace') as f:
            content = f.read()

        ext = full_path.suffix.lower()

        # Python files - use AST
        if ext == '.py':
            try:
                tree = ast.parse(content)
                for node in ast.walk(tree):
                    if isinstance(node, ast.ClassDef):
                        outline.append({
                            "type": "class",
                            "name": node.name,
                            "line": node.lineno,
                            "end_line": getattr(node, 'end_lineno', node.lineno)
                        })
                        for item in node.body:
                            if isinstance(item, ast.FunctionDef):
                                outline.append({
                                    "type": "method",
                                    "name": f"{node.name}.{item.name}",
                                    "line": item.lineno,
                                    "end_line": getattr(item, 'end_lineno', item.lineno)
                                })
                    elif isinstance(node, ast.FunctionDef) and not any(
                        isinstance(parent, ast.ClassDef)
                        for parent in ast.walk(tree)
                        if hasattr(parent, 'body') and node in getattr(parent, 'body', [])
                    ):
                        outline.append({
                            "type": "function",
                            "name": node.name,
                            "line": node.lineno,
                            "end_line": getattr(node, 'end_lineno', node.lineno)
                        })
            except SyntaxError:
                pass

        # JavaScript/TypeScript - regex based
        elif ext in ['.js', '.ts', '.jsx', '.tsx']:
            # Classes
            for match in re.finditer(r'^(?:export\s+)?class\s+(\w+)', content, re.MULTILINE):
                line_num = content[:match.start()].count('\n') + 1
                outline.append({
                    "type": "class",
                    "name": match.group(1),
                    "line": line_num
                })

            # Functions
            for match in re.finditer(
                r'^(?:export\s+)?(?:async\s+)?function\s+(\w+)|'
                r'^(?:export\s+)?const\s+(\w+)\s*=\s*(?:async\s+)?\(|'
                r'^(?:export\s+)?const\s+(\w+)\s*=\s*(?:async\s+)?(?:\([^)]*\)|[^=])\s*=>',
                content, re.MULTILINE
            ):
                name = match.group(1) or match.group(2) or match.group(3)
                line_num = content[:match.start()].count('\n') + 1
                outline.append({
                    "type": "function",
                    "name": name,
                    "line": line_num
                })

        # Generic fallback - look for common patterns
        else:
            # Look for function-like patterns
            for match in re.finditer(
                r'^(?:def|func|function|fn|pub fn|async fn)\s+(\w+)',
                content, re.MULTILINE
            ):
                line_num = content[:match.start()].count('\n') + 1
                outline.append({
                    "type": "function",
                    "name": match.group(1),
                    "line": line_num
                })

            # Look for class-like patterns
            for match in re.finditer(
                r'^(?:class|struct|type|interface)\s+(\w+)',
                content, re.MULTILINE
            ):
                line_num = content[:match.start()].count('\n') + 1
                outline.append({
                    "type": "class",
                    "name": match.group(1),
                    "line": line_num
                })

        # Sort by line number
        outline.sort(key=lambda x: x['line'])

        return {
            "success": True,
            "repo_name": repo_name,
            "file_path": file_path,
            "file_type": ext,
            "outline": outline,
            "total_items": len(outline)
        }
    except Exception as e:
        return {"error": str(e), "outline": []}


async def archive_repository_impl(
    repo_name: str,
    include_hidden: bool = False,
    path: str = ""
) -> dict:
    """
    Archive a repository as a ZIP file and return as base64-encoded content.

    This tool creates an in-memory ZIP archive of the repository (or a subdirectory)
    and returns it as base64-encoded data. The .git directory is always excluded.

    The response includes:
    - zip_base64: Base64-encoded ZIP file content
    - filename: Suggested filename for the archive
    - size_bytes: Size of the ZIP file in bytes
    - file_count: Number of files included in the archive

    ChatGPT can use this to download and unpack the repository in its container.
    """
    if not validate_repo_name(repo_name):
        return {"error": "Invalid repository name", "success": False}

    repo_path = get_repo_path(repo_name)
    if not repo_path.exists():
        return {
            "error": f"Repository '{repo_name}' not cloned. Use clone_repository first.",
            "success": False
        }

    # Determine the target path to archive
    if path:
        target_path = validate_file_path(repo_path, path)
        if target_path is None:
            return {"error": "Invalid path specified", "success": False}
        if not target_path.exists():
            return {"error": f"Path '{path}' does not exist", "success": False}
        if not target_path.is_dir():
            return {"error": f"Path '{path}' is not a directory", "success": False}
        archive_name = f"{repo_name}_{target_path.name}"
    else:
        target_path = repo_path
        archive_name = repo_name

    try:
        # Create in-memory ZIP file
        zip_buffer = io.BytesIO()
        file_count = 0
        max_archive_size = 50 * 1024 * 1024  # 50MB limit

        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for file_path in target_path.rglob('*'):
                # Skip .git directory
                if '.git' in file_path.parts:
                    continue

                # Skip hidden files if not included
                if not include_hidden:
                    # Check if any part of the path (except the target) starts with .
                    relative_parts = file_path.relative_to(target_path).parts
                    if any(part.startswith('.') for part in relative_parts):
                        continue

                # Only add files, not directories
                if file_path.is_file():
                    # Calculate relative path for the archive
                    arcname = str(file_path.relative_to(target_path))

                    # Check file size before adding
                    try:
                        file_size = file_path.stat().st_size
                        if file_size > 10 * 1024 * 1024:  # Skip files larger than 10MB
                            continue

                        zip_file.write(file_path, arcname)
                        file_count += 1

                        # Check if we're exceeding the archive size limit
                        if zip_buffer.tell() > max_archive_size:
                            return {
                                "error": f"Archive would exceed {max_archive_size // (1024*1024)}MB limit. Try archiving a subdirectory using the 'path' parameter.",
                                "success": False
                            }
                    except (PermissionError, OSError):
                        continue

        # Get the ZIP content
        zip_content = zip_buffer.getvalue()
        zip_size = len(zip_content)

        # Encode as base64
        zip_base64 = base64.b64encode(zip_content).decode('utf-8')

        return {
            "success": True,
            "repo_name": repo_name,
            "path": path if path else "/",
            "filename": f"{archive_name}.zip",
            "zip_base64": zip_base64,
            "size_bytes": zip_size,
            "file_count": file_count,
            "include_hidden": include_hidden,
            "instructions": "Decode the base64 content and save as a ZIP file. Extract using: unzip <filename>.zip"
        }

    except Exception as e:
        return {"error": str(e), "success": False}


# ============================================================================
# MCP Protocol Handler
# ============================================================================

async def handle_mcp_request(request_data: dict) -> dict:
    """Handle MCP JSON-RPC requests"""
    method = request_data.get("method", "")
    params = request_data.get("params", {})
    request_id = request_data.get("id")

    result = None
    error = None

    try:
        # Initialize
        if method == "initialize":
            result = {
                "protocolVersion": "2025-06-18",
                "serverInfo": {
                    "name": "github-search-mcp",
                    "version": "1.0.0"
                },
                "capabilities": {
                    "tools": {"listChanged": False},
                    "resources": {"listChanged": False}
                }
            }

        # List tools
        elif method == "tools/list":
            result = {"tools": TOOLS}

        # Call tool
        elif method == "tools/call":
            tool_name = params.get("name")
            tool_args = params.get("arguments", {})

            if tool_name == "clone_repository":
                tool_result = await clone_repository_impl(**tool_args)
            elif tool_name == "search_code":
                tool_result = await search_code_impl(**tool_args)
            elif tool_name == "get_tree":
                tool_result = await get_tree_impl(**tool_args)
            elif tool_name == "read_file":
                tool_result = await read_file_impl(**tool_args)
            elif tool_name == "get_outline":
                tool_result = await get_outline_impl(**tool_args)
            elif tool_name == "archive_repository":
                tool_result = await archive_repository_impl(**tool_args)
            else:
                error = {
                    "code": -32601,
                    "message": f"Unknown tool: {tool_name}"
                }
                tool_result = None

            if tool_result is not None:
                is_error = "error" in tool_result and not tool_result.get("success", True)
                result = {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(tool_result, indent=2)
                        }
                    ],
                    "isError": is_error
                }

        # List resources
        elif method == "resources/list":
            result = {"resources": RESOURCES}

        # Ping
        elif method == "ping":
            result = {}

        # Notifications (no response needed)
        elif method.startswith("notifications/"):
            return None

        else:
            error = {
                "code": -32601,
                "message": f"Method not found: {method}"
            }

    except Exception as e:
        error = {
            "code": -32603,
            "message": str(e)
        }

    # Build response
    response = {"jsonrpc": "2.0"}

    if request_id is not None:
        response["id"] = request_id

    if error:
        response["error"] = error
    else:
        response["result"] = result

    return response


# ============================================================================
# FastAPI Endpoints
# ============================================================================

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "version": "1.0.0",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "service": "github-search-mcp"
    }


@app.get("/capabilities")
async def capabilities():
    """Server capabilities endpoint"""
    return {
        "name": "github-search-mcp",
        "version": "1.0.0",
        "description": f"MCP server for searching GitHub repositories from {ALLOWED_USERNAME}",
        "tools": [t["name"] for t in TOOLS],
        "tool_count": len(TOOLS),
        "resources": True,
        "transport": ["streamable-http", "http"],
        "authentication": "none",
        "mcp_protocol_version": "2025-06-18",
        "annotations": {
            "readOnlyHint": True,
            "destructiveHint": False
        }
    }


@app.post("/mcp")
async def mcp_endpoint(request: Request):
    """Main MCP protocol endpoint (Streamable HTTP transport)"""
    try:
        body = await request.json()

        # Handle batch requests
        if isinstance(body, list):
            responses = []
            for req in body:
                resp = await handle_mcp_request(req)
                if resp is not None:
                    responses.append(resp)
            return JSONResponse(content=responses)

        # Handle single request
        response = await handle_mcp_request(body)
        if response is None:
            return Response(status_code=204)

        return JSONResponse(content=response)

    except json.JSONDecodeError:
        return JSONResponse(
            status_code=400,
            content={
                "jsonrpc": "2.0",
                "error": {
                    "code": -32700,
                    "message": "Parse error"
                },
                "id": None
            }
        )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "jsonrpc": "2.0",
                "error": {
                    "code": -32603,
                    "message": str(e)
                },
                "id": None
            }
        )


@app.get("/")
async def root():
    """Root endpoint with server info"""
    return {
        "name": "GitHub Search MCP Server",
        "version": "1.0.0",
        "endpoints": {
            "mcp": "/mcp",
            "health": "/health",
            "capabilities": "/capabilities"
        },
        "documentation": "https://modelcontextprotocol.io/specification/2025-06-18"
    }


# ============================================================================
# Entry Point
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=HOST, port=PORT)
