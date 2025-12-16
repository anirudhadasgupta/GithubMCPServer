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
import zipfile
import io
import uuid
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional, Any

from fastapi import FastAPI, Request, Response, Query
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG")
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("mcp-server")

# Configuration
GITHUB_PAT = os.getenv("GITHUB_PAT", "")
REPO_STORAGE_PATH = Path(os.getenv("REPO_STORAGE_PATH", "/tmp/repos"))
ALLOWED_USERNAME = os.getenv("ALLOWED_USERNAME", "anirudhadasgupta")
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8000"))
BASE_URL = os.getenv("BASE_URL", f"http://{HOST}:{PORT}")

logger.info(f"Starting MCP server with BASE_URL={BASE_URL}, HOST={HOST}, PORT={PORT}")

# Response size limit (ChatGPT has ~100KB limit for action responses)
MAX_RESPONSE_SIZE = int(os.getenv("MAX_RESPONSE_SIZE", "50000"))  # 50KB default
logger.info(f"MAX_RESPONSE_SIZE={MAX_RESPONSE_SIZE}")

# Session management for SSE connections
# Maps session_id -> asyncio.Queue for sending responses
sse_sessions: dict[str, asyncio.Queue] = {}

# Session management for streamable HTTP transport
# Maps session_id -> {created_at, last_used, protocol_version}
http_sessions: dict[str, dict] = {}
SESSION_TIMEOUT_SECONDS = 3600  # 1 hour session timeout

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
        "description": "Get a download link for a cloned repository as a ZIP file. Returns a URL that can be used to download the entire repository (or a subdirectory) for local analysis. The .git directory is excluded to reduce size. ChatGPT can use this URL to download and extract the repository.",
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
    logger.info(f"[TOOL:clone_repository] Starting clone for repo_name={repo_name}")

    if not validate_repo_name(repo_name):
        logger.warning(f"[TOOL:clone_repository] Invalid repo name: {repo_name}")
        return {"error": "Invalid repository name", "success": False}

    repo_path = get_repo_path(repo_name)
    logger.debug(f"[TOOL:clone_repository] repo_path={repo_path}")

    # Check if already cloned
    if repo_path.exists() and (repo_path / ".git").exists():
        logger.info(f"[TOOL:clone_repository] Repo already exists, pulling latest")
        # Pull latest changes
        try:
            result = subprocess.run(
                ["git", "-C", str(repo_path), "pull", "--ff-only"],
                capture_output=True,
                text=True,
                timeout=60
            )
            logger.info(f"[TOOL:clone_repository] Pull completed, returncode={result.returncode}")
            return {
                "success": True,
                "message": f"Repository '{repo_name}' already cloned. Updated with latest changes.",
                "path": str(repo_path),
                "status": "updated"
            }
        except subprocess.TimeoutExpired:
            logger.warning(f"[TOOL:clone_repository] Pull timed out")
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
        logger.debug(f"[TOOL:clone_repository] Using PAT for authentication")
    else:
        clone_url = f"https://github.com/{ALLOWED_USERNAME}/{repo_name}.git"
        logger.debug(f"[TOOL:clone_repository] No PAT, using public clone")

    try:
        logger.info(f"[TOOL:clone_repository] Starting git clone")
        result = subprocess.run(
            ["git", "clone", "--depth", "1", clone_url, str(repo_path)],
            capture_output=True,
            text=True,
            timeout=120
        )

        if result.returncode != 0:
            logger.error(f"[TOOL:clone_repository] Clone failed: {result.stderr}")
            return {
                "success": False,
                "error": f"Failed to clone repository: {result.stderr}",
                "status": "failed"
            }

        logger.info(f"[TOOL:clone_repository] Clone successful")
        return {
            "success": True,
            "message": f"Successfully cloned '{ALLOWED_USERNAME}/{repo_name}'",
            "path": str(repo_path),
            "status": "cloned"
        }
    except subprocess.TimeoutExpired:
        logger.error(f"[TOOL:clone_repository] Clone timed out after 120s")
        return {
            "success": False,
            "error": "Clone operation timed out",
            "status": "timeout"
        }
    except Exception as e:
        logger.error(f"[TOOL:clone_repository] Exception: {e}", exc_info=True)
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
    max_results: int = 20  # Reduced from 50 to prevent large responses
) -> dict:
    """Search for code patterns using grep (Memory Safe Version)"""
    logger.info(f"[TOOL:search_code] Starting search: repo={repo_name}, pattern={pattern[:50]}, file_pattern={file_pattern}, max_results={max_results}")

    if not validate_repo_name(repo_name):
        logger.warning(f"[TOOL:search_code] Invalid repo name: {repo_name}")
        return {"error": "Invalid repository name", "matches": []}

    repo_path = get_repo_path(repo_name)
    if not repo_path.exists():
        logger.warning(f"[TOOL:search_code] Repo not found: {repo_path}")
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
        logger.debug(f"[TOOL:search_code] grep command: {' '.join(grep_args[:5])}...")

        # Use Popen to stream output line-by-line (memory safe)
        process = subprocess.Popen(
            grep_args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=str(repo_path)
        )
        logger.debug(f"[TOOL:search_code] Popen started, pid={process.pid}")

        match_count = 0

        # Iterate over stdout as it is generated
        try:
            for line in process.stdout:
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
                            "content": content.strip()[:200]  # Limit content length
                        })
                        match_count += 1

                        # Stop reading if we have enough results
                        if match_count >= max_results:
                            logger.info(f"[TOOL:search_code] Reached max_results={max_results}, terminating")
                            process.terminate()
                            break

                    except (ValueError, IndexError):
                        continue
        finally:
            # Clean up
            if process.stdout:
                process.stdout.close()
            if process.stderr:
                process.stderr.close()
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=5)

        logger.info(f"[TOOL:search_code] Complete: {len(matches)} matches found")
        return {
            "success": True,
            "pattern": pattern,
            "matches": matches,
            "total_matches": len(matches),
            "truncated": match_count >= max_results
        }
    except subprocess.TimeoutExpired:
        logger.error(f"[TOOL:search_code] Search timed out")
        return {"error": "Search timed out", "matches": matches}
    except Exception as e:
        logger.error(f"[TOOL:search_code] Exception: {e}", exc_info=True)
        return {"error": str(e), "matches": []}


async def get_tree_impl(
    repo_name: str,
    path: str = ".",
    max_depth: int = 3,
    show_hidden: bool = False
) -> dict:
    """Get directory tree structure"""
    logger.info(f"[TOOL:get_tree] Starting: repo={repo_name}, path={path}, max_depth={max_depth}")

    if not validate_repo_name(repo_name):
        logger.warning(f"[TOOL:get_tree] Invalid repo name: {repo_name}")
        return {"error": "Invalid repository name", "tree": ""}

    repo_path = get_repo_path(repo_name)
    if not repo_path.exists():
        logger.warning(f"[TOOL:get_tree] Repo not found: {repo_path}")
        return {"error": f"Repository '{repo_name}' not cloned. Use clone_repository first.", "tree": ""}

    target_path = validate_file_path(repo_path, path)
    if target_path is None:
        logger.debug(f"[TOOL:get_tree] Invalid path, using repo root")
        target_path = repo_path

    if not target_path.exists():
        logger.warning(f"[TOOL:get_tree] Path does not exist: {path}")
        return {"error": f"Path '{path}' does not exist", "tree": ""}

    # Limit output to prevent large responses
    MAX_TREE_LINES = 200
    line_count = [0]  # Use list to allow modification in nested function

    def build_tree(current_path: Path, prefix: str = "", depth: int = 0) -> list:
        if depth > max_depth or line_count[0] >= MAX_TREE_LINES:
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
            if line_count[0] >= MAX_TREE_LINES:
                break

            is_last = i == len(items) - 1
            # Use ASCII characters for better compatibility
            connector = "+-- " if is_last else "|-- "

            if item.is_dir():
                lines.append(f"{prefix}{connector}{item.name}/")
                line_count[0] += 1
                extension = "    " if is_last else "|   "
                lines.extend(build_tree(item, prefix + extension, depth + 1))
            else:
                size = item.stat().st_size
                size_str = f" ({size:,} bytes)" if size < 1024 * 1024 else f" ({size / 1024 / 1024:.1f} MB)"
                lines.append(f"{prefix}{connector}{item.name}{size_str}")
                line_count[0] += 1

        return lines

    tree_lines = [f"{target_path.name}/"]
    line_count[0] = 1
    tree_lines.extend(build_tree(target_path))

    truncated = line_count[0] >= MAX_TREE_LINES
    logger.info(f"[TOOL:get_tree] Complete: {len(tree_lines)} lines in tree, truncated={truncated}")

    result = {
        "success": True,
        "repo_name": repo_name,
        "path": path,
        "tree": "\n".join(tree_lines)
    }
    if truncated:
        result["truncated"] = True
        result["note"] = f"Output limited to {MAX_TREE_LINES} lines. Use path parameter to explore subdirectories."

    return result


async def read_file_impl(
    repo_name: str,
    file_path: str,
    start_line: int = 1,
    end_line: int = 0
) -> dict:
    """Read file contents"""
    logger.info(f"[TOOL:read_file] Starting: repo={repo_name}, file={file_path}, lines={start_line}-{end_line}")

    if not validate_repo_name(repo_name):
        logger.warning(f"[TOOL:read_file] Invalid repo name: {repo_name}")
        return {"error": "Invalid repository name", "content": ""}

    repo_path = get_repo_path(repo_name)
    if not repo_path.exists():
        logger.warning(f"[TOOL:read_file] Repo not found: {repo_path}")
        return {"error": f"Repository '{repo_name}' not cloned. Use clone_repository first.", "content": ""}

    full_path = validate_file_path(repo_path, file_path)
    if full_path is None:
        logger.warning(f"[TOOL:read_file] Invalid file path: {file_path}")
        return {"error": "Invalid file path", "content": ""}

    if not full_path.exists():
        logger.warning(f"[TOOL:read_file] File not found: {full_path}")
        return {"error": f"File '{file_path}' not found", "content": ""}

    if not full_path.is_file():
        logger.warning(f"[TOOL:read_file] Not a file: {full_path}")
        return {"error": f"'{file_path}' is not a file", "content": ""}

    try:
        # Check file size
        file_size = full_path.stat().st_size
        max_size = 1024 * 1024  # 1MB limit
        logger.debug(f"[TOOL:read_file] File size: {file_size} bytes")

        if file_size > max_size:
            logger.warning(f"[TOOL:read_file] File too large: {file_size} bytes")
            return {
                "error": f"File too large ({file_size / 1024 / 1024:.1f} MB). Maximum size is 1MB.",
                "content": ""
            }

        with open(full_path, 'r', encoding='utf-8', errors='replace') as f:
            lines = f.readlines()

        total_lines = len(lines)
        logger.debug(f"[TOOL:read_file] Total lines: {total_lines}")

        # Handle line ranges - limit to 200 lines max to prevent large responses
        MAX_LINES = 200
        start_idx = max(0, start_line - 1)
        if end_line <= 0:
            end_idx = min(start_idx + MAX_LINES, total_lines)
        else:
            end_idx = min(end_line, start_idx + MAX_LINES, total_lines)

        selected_lines = lines[start_idx:end_idx]
        was_truncated = (end_line <= 0 and total_lines > end_idx) or (end_line > 0 and end_line > end_idx)

        # Add line numbers
        numbered_content = []
        for i, line in enumerate(selected_lines, start=start_idx + 1):
            numbered_content.append(f"{i:4d} | {line.rstrip()}")

        logger.info(f"[TOOL:read_file] Complete: {len(selected_lines)} lines returned, truncated={was_truncated}")
        result = {
            "success": True,
            "repo_name": repo_name,
            "file_path": file_path,
            "content": "\n".join(numbered_content),
            "total_lines": total_lines,
            "start_line": start_idx + 1,
            "end_line": end_idx,
            "file_size": file_size
        }
        if was_truncated:
            result["truncated"] = True
            result["note"] = f"Output limited to {MAX_LINES} lines. Use start_line/end_line to read other sections."
        return result
    except UnicodeDecodeError:
        logger.error(f"[TOOL:read_file] Binary file: {file_path}")
        return {"error": "Cannot read binary file", "content": ""}
    except Exception as e:
        logger.error(f"[TOOL:read_file] Exception: {e}", exc_info=True)
        return {"error": str(e), "content": ""}


async def get_outline_impl(repo_name: str, file_path: str) -> dict:
    """Get code outline for a file"""
    logger.info(f"[TOOL:get_outline] Starting: repo={repo_name}, file={file_path}")

    if not validate_repo_name(repo_name):
        logger.warning(f"[TOOL:get_outline] Invalid repo name: {repo_name}")
        return {"error": "Invalid repository name", "outline": []}

    repo_path = get_repo_path(repo_name)
    if not repo_path.exists():
        logger.warning(f"[TOOL:get_outline] Repo not found: {repo_path}")
        return {"error": f"Repository '{repo_name}' not cloned. Use clone_repository first.", "outline": []}

    full_path = validate_file_path(repo_path, file_path)
    if full_path is None:
        logger.warning(f"[TOOL:get_outline] Invalid file path: {file_path}")
        return {"error": "Invalid file path", "outline": []}

    if not full_path.exists():
        logger.warning(f"[TOOL:get_outline] File not found: {full_path}")
        return {"error": f"File '{file_path}' not found", "outline": []}

    outline = []
    logger.debug(f"[TOOL:get_outline] Processing file: {full_path}")

    try:
        with open(full_path, 'r', encoding='utf-8', errors='replace') as f:
            content = f.read()

        ext = full_path.suffix.lower()
        logger.debug(f"[TOOL:get_outline] File extension: {ext}, content length: {len(content)}")

        # Python files - use AST
        if ext == '.py':
            logger.debug(f"[TOOL:get_outline] Parsing Python file with AST")
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
            except SyntaxError as e:
                logger.warning(f"[TOOL:get_outline] Python syntax error: {e}")

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

        logger.info(f"[TOOL:get_outline] Complete: {len(outline)} items found in {ext} file")
        return {
            "success": True,
            "repo_name": repo_name,
            "file_path": file_path,
            "file_type": ext,
            "outline": outline,
            "total_items": len(outline)
        }
    except Exception as e:
        logger.error(f"[TOOL:get_outline] Exception: {e}", exc_info=True)
        return {"error": str(e), "outline": []}


async def archive_repository_impl(
    repo_name: str,
    include_hidden: bool = False,
    path: str = "",
    base_url: str = ""
) -> dict:
    """
    Generate a download URL for a repository archive as a ZIP file.

    This tool validates the repository exists and returns a download URL.
    The actual ZIP file is generated when the download URL is accessed.
    The .git directory is always excluded from the archive.

    The response includes:
    - download_url: URL to download the ZIP file
    - filename: Suggested filename for the archive
    - file_count: Estimated number of files in the archive

    ChatGPT can use the download_url to fetch and extract the repository.
    """
    logger.info(f"[TOOL:archive_repository] Starting: repo={repo_name}, path={path}, include_hidden={include_hidden}")
    logger.debug(f"[TOOL:archive_repository] base_url parameter: {base_url}")

    if not validate_repo_name(repo_name):
        logger.warning(f"[TOOL:archive_repository] Invalid repo name: {repo_name}")
        return {"error": "Invalid repository name", "success": False}

    repo_path = get_repo_path(repo_name)
    if not repo_path.exists():
        logger.warning(f"[TOOL:archive_repository] Repo not found: {repo_path}")
        return {
            "error": f"Repository '{repo_name}' not cloned. Use clone_repository first.",
            "success": False
        }

    # Determine the target path to archive
    if path:
        logger.debug(f"[TOOL:archive_repository] Validating subpath: {path}")
        target_path = validate_file_path(repo_path, path)
        if target_path is None:
            logger.warning(f"[TOOL:archive_repository] Invalid path: {path}")
            return {"error": "Invalid path specified", "success": False}
        if not target_path.exists():
            logger.warning(f"[TOOL:archive_repository] Path does not exist: {path}")
            return {"error": f"Path '{path}' does not exist", "success": False}
        if not target_path.is_dir():
            logger.warning(f"[TOOL:archive_repository] Path is not a directory: {path}")
            return {"error": f"Path '{path}' is not a directory", "success": False}
        archive_name = f"{repo_name}_{target_path.name}"
    else:
        target_path = repo_path
        archive_name = repo_name

    logger.debug(f"[TOOL:archive_repository] target_path={target_path}, archive_name={archive_name}")

    # Count files to give an estimate
    logger.debug(f"[TOOL:archive_repository] Counting files in {target_path}")
    file_count = 0
    for file_path in target_path.rglob('*'):
        # Skip .git directory
        if '.git' in file_path.parts:
            continue
        # Skip hidden files if not included
        if not include_hidden:
            relative_parts = file_path.relative_to(target_path).parts
            if any(part.startswith('.') for part in relative_parts):
                continue
        if file_path.is_file():
            file_count += 1

    logger.debug(f"[TOOL:archive_repository] Found {file_count} files to archive")

    # Build download URL with query parameters
    # Use provided base_url (from request) or fall back to configured BASE_URL
    effective_base_url = base_url or BASE_URL
    logger.debug(f"[TOOL:archive_repository] Using effective_base_url: {effective_base_url}")
    download_url = f"{effective_base_url}/download/{repo_name}"
    query_params = []
    if include_hidden:
        query_params.append("include_hidden=true")
    if path:
        query_params.append(f"path={path}")
    if query_params:
        download_url += "?" + "&".join(query_params)

    logger.info(f"[TOOL:archive_repository] Complete: download_url={download_url}, file_count={file_count}")
    return {
        "success": True,
        "repo_name": repo_name,
        "path": path if path else "/",
        "filename": f"{archive_name}.zip",
        "download_url": download_url,
        "file_count": file_count,
        "include_hidden": include_hidden,
        "instructions": f"Download the ZIP file from the URL above. ChatGPT can use: curl -o {archive_name}.zip '{download_url}' && unzip {archive_name}.zip"
    }


# ============================================================================
# MCP Protocol Handler
# ============================================================================

def truncate_response(response: dict, max_size: int = MAX_RESPONSE_SIZE) -> dict:
    """Truncate response if it exceeds max size to prevent connection issues."""
    response_str = json.dumps(response)
    original_size = len(response_str)

    if original_size <= max_size:
        return response

    logger.warning(f"[TRUNCATE] Response too large: {original_size} bytes, truncating to {max_size}")

    # Try to truncate the content inside the response
    if "result" in response and "content" in response["result"]:
        content = response["result"]["content"]
        if isinstance(content, list):
            for item in content:
                if item.get("type") == "text" and "text" in item:
                    text = item["text"]
                    # Calculate how much we need to trim
                    overhead = original_size - len(text)
                    max_text_size = max_size - overhead - 200  # Leave room for truncation message

                    if len(text) > max_text_size:
                        truncated_text = text[:max_text_size]
                        item["text"] = truncated_text + f"\n\n... [TRUNCATED: Response was {original_size} bytes, limit is {max_size} bytes. Use more specific queries or smaller file ranges.]"
                        logger.info(f"[TRUNCATE] Truncated text from {len(text)} to {len(item['text'])} chars")

    return response


async def handle_mcp_request(request_data: dict, base_url: str = "") -> dict:
    """Handle MCP JSON-RPC requests"""
    method = request_data.get("method", "")
    params = request_data.get("params", {})
    request_id = request_data.get("id")

    logger.info(f"[MCP] Handling method={method}, id={request_id}")
    logger.debug(f"[MCP] Params: {params}")

    result = None
    error = None

    try:
        # Initialize
        if method == "initialize":
            # Echo back the client's protocol version for compatibility
            client_protocol = params.get("protocolVersion", "2025-06-18")
            logger.info(f"[MCP] Client protocol version: {client_protocol}")
            result = {
                "protocolVersion": client_protocol,
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
            result = {"tools": TOOLS, "nextCursor": None}

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
                tool_result = await archive_repository_impl(**tool_args, base_url=base_url)
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
            result = {"resources": RESOURCES, "nextCursor": None}

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

    # Truncate if response is too large
    response = truncate_response(response)

    response_size = len(json.dumps(response))
    logger.info(f"[MCP] Response size: {response_size} bytes")

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


@app.get("/download/{repo_name}")
async def download_repository(
    repo_name: str,
    include_hidden: bool = Query(default=False, description="Include hidden files"),
    path: str = Query(default="", description="Subdirectory path to archive")
):
    """
    Download a repository as a ZIP file.

    This endpoint generates and streams a ZIP archive of the specified repository.
    The .git directory is always excluded.
    """
    if not validate_repo_name(repo_name):
        return JSONResponse(
            status_code=400,
            content={"error": "Invalid repository name"}
        )

    repo_path = get_repo_path(repo_name)
    if not repo_path.exists():
        return JSONResponse(
            status_code=404,
            content={"error": f"Repository '{repo_name}' not found. Clone it first using the clone_repository tool."}
        )

    # Determine the target path to archive
    if path:
        target_path = validate_file_path(repo_path, path)
        if target_path is None:
            return JSONResponse(
                status_code=400,
                content={"error": "Invalid path specified"}
            )
        if not target_path.exists():
            return JSONResponse(
                status_code=404,
                content={"error": f"Path '{path}' does not exist"}
            )
        if not target_path.is_dir():
            return JSONResponse(
                status_code=400,
                content={"error": f"Path '{path}' is not a directory"}
            )
        archive_name = f"{repo_name}_{target_path.name}"
    else:
        target_path = repo_path
        archive_name = repo_name

    def generate_zip():
        """Generator that yields ZIP file chunks"""
        zip_buffer = io.BytesIO()
        max_archive_size = 50 * 1024 * 1024  # 50MB limit

        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for file_path in target_path.rglob('*'):
                # Skip .git directory
                if '.git' in file_path.parts:
                    continue

                # Skip hidden files if not included
                if not include_hidden:
                    relative_parts = file_path.relative_to(target_path).parts
                    if any(part.startswith('.') for part in relative_parts):
                        continue

                # Only add files, not directories
                if file_path.is_file():
                    arcname = str(file_path.relative_to(target_path))

                    try:
                        file_size = file_path.stat().st_size
                        if file_size > 10 * 1024 * 1024:  # Skip files larger than 10MB
                            continue

                        zip_file.write(file_path, arcname)

                        # Check size limit
                        if zip_buffer.tell() > max_archive_size:
                            break
                    except (PermissionError, OSError):
                        continue

        zip_buffer.seek(0)
        yield zip_buffer.read()

    return StreamingResponse(
        generate_zip(),
        media_type="application/zip",
        headers={
            "Content-Disposition": f"attachment; filename={archive_name}.zip"
        }
    )


def get_base_url_from_request(request: Request) -> str:
    """Extract base URL from request headers for constructing download links."""
    scheme = request.headers.get("x-forwarded-proto") or request.url.scheme
    host = request.headers.get("x-forwarded-host") or request.headers.get("host")

    if not host or "0.0.0.0" in host:
        if BASE_URL and "0.0.0.0" not in BASE_URL:
            return BASE_URL.rstrip("/")
        else:
            return f"{scheme}://{request.url.netloc}"
    else:
        return f"{scheme}://{host}"


@app.get("/sse")
async def sse_stream(request: Request):
    """
    SSE streaming endpoint for MCP protocol.

    1. Creates a session and sends the endpoint URL for POSTing messages
    2. Streams responses and heartbeat pings every 15 seconds
    """
    session_id = str(uuid.uuid4())
    message_queue: asyncio.Queue = asyncio.Queue()
    sse_sessions[session_id] = message_queue

    base_url = get_base_url_from_request(request)
    client_host = request.client.host if request.client else "unknown"

    logger.info(f"[SSE] New connection from {client_host}, session_id={session_id}")
    logger.debug(f"[SSE] Request headers: {dict(request.headers)}")
    logger.info(f"[SSE] Active sessions: {len(sse_sessions)}")

    async def event_generator():
        heartbeat_count = 0
        message_count = 0
        try:
            # Send the endpoint URL as the first event
            endpoint_url = f"{base_url}/messages?session_id={session_id}"
            logger.info(f"[SSE:{session_id[:8]}] Sending endpoint URL: {endpoint_url}")
            yield f"event: endpoint\ndata: {endpoint_url}\n\n"

            while True:
                # Check if client disconnected
                if await request.is_disconnected():
                    logger.warning(f"[SSE:{session_id[:8]}] Client disconnected after {heartbeat_count} heartbeats, {message_count} messages")
                    break

                # Check for messages with timeout for heartbeat
                try:
                    message = await asyncio.wait_for(message_queue.get(), timeout=15.0)
                    message_count += 1
                    logger.info(f"[SSE:{session_id[:8]}] Sending message #{message_count}: {json.dumps(message)[:200]}...")
                    yield f"event: message\ndata: {json.dumps(message)}\n\n"
                except asyncio.TimeoutError:
                    # Send heartbeat ping
                    heartbeat_count += 1
                    logger.debug(f"[SSE:{session_id[:8]}] Heartbeat #{heartbeat_count}")
                    yield f"event: ping\ndata: {json.dumps({'type': 'ping', 'timestamp': datetime.utcnow().isoformat() + 'Z'})}\n\n"

        except asyncio.CancelledError:
            logger.warning(f"[SSE:{session_id[:8]}] Connection cancelled")
        except Exception as e:
            logger.error(f"[SSE:{session_id[:8]}] Error: {e}")
        finally:
            # Cleanup session
            sse_sessions.pop(session_id, None)
            logger.info(f"[SSE:{session_id[:8]}] Session closed. Active sessions: {len(sse_sessions)}")

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )


@app.post("/messages")
async def mcp_messages(request: Request, session_id: str = Query(...)):
    """
    Receive MCP messages and push responses to the SSE stream.
    """
    logger.info(f"[MSG:{session_id[:8]}] Received POST /messages")
    logger.debug(f"[MSG:{session_id[:8]}] Request headers: {dict(request.headers)}")

    if session_id not in sse_sessions:
        logger.error(f"[MSG:{session_id[:8]}] Session not found! Active sessions: {list(sse_sessions.keys())}")
        return JSONResponse(
            status_code=404,
            content={"error": "Session not found. Connect to /sse first."}
        )

    message_queue = sse_sessions[session_id]
    base_url = get_base_url_from_request(request)

    try:
        body = await request.json()
        logger.info(f"[MSG:{session_id[:8]}] Request body: {json.dumps(body)[:500]}...")

        # Handle batch requests
        if isinstance(body, list):
            logger.info(f"[MSG:{session_id[:8]}] Processing batch of {len(body)} requests")
            for req in body:
                response = await handle_mcp_request(req, base_url=base_url)
                if response is not None:
                    await message_queue.put(response)
        else:
            response = await handle_mcp_request(body, base_url=base_url)
            if response is not None:
                logger.info(f"[MSG:{session_id[:8]}] Queued response: {json.dumps(response)[:200]}...")
                await message_queue.put(response)

        return Response(status_code=202)  # Accepted

    except json.JSONDecodeError as e:
        logger.error(f"[MSG:{session_id[:8]}] JSON parse error: {e}")
        error_response = {
            "jsonrpc": "2.0",
            "error": {"code": -32700, "message": "Parse error"},
            "id": None
        }
        await message_queue.put(error_response)
        return Response(status_code=202)
    except Exception as e:
        logger.error(f"[MSG:{session_id[:8]}] Error: {e}", exc_info=True)
        error_response = {
            "jsonrpc": "2.0",
            "error": {"code": -32603, "message": str(e)},
            "id": None
        }
        await message_queue.put(error_response)
        return Response(status_code=202)


def cleanup_expired_sessions():
    """Remove expired HTTP sessions"""
    now = datetime.utcnow()
    expired = []
    for sid, session in http_sessions.items():
        age = (now - session["created_at"]).total_seconds()
        if age > SESSION_TIMEOUT_SECONDS:
            expired.append(sid)
    for sid in expired:
        del http_sessions[sid]
        logger.info(f"[SESSION] Expired session removed: {sid[:8]}")
    if expired:
        logger.info(f"[SESSION] Cleaned up {len(expired)} expired sessions. Active: {len(http_sessions)}")


@app.post("/sse")
async def mcp_endpoint(request: Request):
    """
    Direct MCP protocol endpoint (Streamable HTTP transport).
    For clients that don't use SSE, this provides direct request/response.
    Supports Mcp-Session-Id header for session management.
    """
    client_host = request.client.host if request.client else "unknown"
    logger.info(f"[POST /sse] Request from {client_host}")
    logger.debug(f"[POST /sse] Request headers: {dict(request.headers)}")

    # Periodic cleanup of expired sessions
    cleanup_expired_sessions()

    # Get session ID from request header
    session_id = request.headers.get("mcp-session-id")
    is_new_session = False
    protocol_version = request.headers.get("mcp-protocol-version", "2025-06-18")

    try:
        body = await request.json()
        base_url = get_base_url_from_request(request)
        method = body.get("method", "") if isinstance(body, dict) else ""
        logger.info(f"[POST /sse] method={method}, session={session_id[:8] if session_id else 'none'}")
        logger.debug(f"[POST /sse] Body: {json.dumps(body)[:500]}...")

        # Handle initialize - create new session
        if method == "initialize":
            session_id = str(uuid.uuid4())
            is_new_session = True
            # Get protocol version from request body
            if isinstance(body, dict) and "params" in body:
                protocol_version = body["params"].get("protocolVersion", protocol_version)
            # Store session
            http_sessions[session_id] = {
                "created_at": datetime.utcnow(),
                "last_used": datetime.utcnow(),
                "protocol_version": protocol_version,
                "client_host": client_host
            }
            logger.info(f"[POST /sse] New session created: {session_id[:8]}, protocol={protocol_version}, active_sessions={len(http_sessions)}")

        # Validate existing session
        elif session_id:
            if session_id in http_sessions:
                http_sessions[session_id]["last_used"] = datetime.utcnow()
                logger.debug(f"[POST /sse] Valid session: {session_id[:8]}")
            else:
                # Session not found - this might be why tools "drop"
                # Be lenient: accept the request but log a warning
                logger.warning(f"[POST /sse] Unknown session ID: {session_id[:8]} - accepting anyway")
        else:
            # No session ID provided for non-initialize request
            logger.warning(f"[POST /sse] No session ID for method={method}")

        # Handle batch requests
        if isinstance(body, list):
            logger.info(f"[POST /sse] Processing batch of {len(body)} requests")
            responses = []
            for req in body:
                resp = await handle_mcp_request(req, base_url=base_url)
                if resp is not None:
                    responses.append(resp)
            response = JSONResponse(content=responses)
        else:
            # Handle single request
            mcp_response = await handle_mcp_request(body, base_url=base_url)
            if mcp_response is None:
                logger.info("[POST /sse] No response (204)")
                resp = Response(status_code=204)
                if session_id:
                    resp.headers["Mcp-Session-Id"] = session_id
                return resp

            logger.info(f"[POST /sse] Response: {json.dumps(mcp_response)[:200]}...")
            response = JSONResponse(content=mcp_response)

        # Include session ID in response headers
        if session_id:
            response.headers["Mcp-Session-Id"] = session_id
            if is_new_session:
                logger.info(f"[POST /sse] Returning new Mcp-Session-Id: {session_id[:8]}")

        return response

    except json.JSONDecodeError as e:
        logger.error(f"[POST /sse] JSON parse error: {e}")
        error_response = JSONResponse(
            status_code=400,
            content={
                "jsonrpc": "2.0",
                "error": {"code": -32700, "message": "Parse error"},
                "id": None
            }
        )
        if session_id:
            error_response.headers["Mcp-Session-Id"] = session_id
        return error_response
    except Exception as e:
        logger.error(f"[POST /sse] Error: {e}", exc_info=True)
        error_response = JSONResponse(
            status_code=500,
            content={
                "jsonrpc": "2.0",
                "error": {"code": -32603, "message": str(e)},
                "id": None
            }
        )
        if session_id:
            error_response.headers["Mcp-Session-Id"] = session_id
        return error_response


@app.get("/")
async def root():
    """Root endpoint with server info"""
    return {
        "name": "GitHub Search MCP Server",
        "version": "1.0.0",
        "endpoints": {
            "sse": "/sse (GET for SSE stream, POST for direct requests)",
            "messages": "/messages?session_id=<id> (POST MCP messages for SSE sessions)",
            "health": "/health",
            "capabilities": "/capabilities",
            "download": "/download/{repo_name}"
        },
        "documentation": "https://modelcontextprotocol.io/specification/2025-06-18"
    }


# ============================================================================
# Entry Point
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=HOST, port=PORT)
