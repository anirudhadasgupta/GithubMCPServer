"""
GitHub Search MCP Server - Fixed for ChatGPT URI Rotation

CHANGES FROM ORIGINAL:
1. Removed session dependency for tool execution (stateless design)
2. POST /sse accepts all requests without session validation
3. Removed 404 responses that trigger tool eviction
4. Added structured error responses with retry semantics
5. Stable URLs without session parameters
6. GET /sse is now optional (kept for backwards compatibility)
7. Added retry-after and retryable hints in errors

Author: anirudhadasgupta (fixes by Claude)
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

# SSE sessions (optional, for backwards compatibility only)
# IMPORTANT: These are NOT required for tool operation
sse_sessions: dict[str, asyncio.Queue] = {}

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

# Server metadata for tool discovery
SERVER_INFO = {
    "name": "github-search-mcp",
    "version": "1.0.0",
    "author": "anirudhadasgupta",
    "description": "MCP server for exploring GitHub repositories. Clone repos, search code, browse files, and analyze structure.",
    "capabilities": ["clone", "search", "browse", "read", "outline"],
    "workflow": [
        "1. First call clone_repository with the repo name",
        "2. Then use other tools to explore the cloned repo",
        "3. All paths are relative to repo root (e.g., 'src/App.tsx')"
    ],
    "limits": {
        "max_file_lines": 200,
        "max_tree_lines": 200,
        "max_search_results": 20
    }
}

TOOLS = [
    {
        "name": "clone_repository",
        "title": "Clone Repository",
        "description": "Clone a GitHub repository to make it available for exploration. MUST be called first before using other tools.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "repo_name": {
                    "type": "string",
                    "description": "Repository name without owner prefix (e.g., 'my-project')"
                }
            },
            "required": ["repo_name"]
        },
        "annotations": {
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True
        },
        "usage": {
            "prerequisite": None,
            "instructions": [
                "Call with repository name only",
                "Wait for success confirmation",
                "Then use other tools to explore"
            ],
            "example": {
                "call": "clone_repository",
                "arguments": {"repo_name": "CLARIOERP_WMS"}
            },
            "returns": ["status", "message", "path"]
        }
    },
    {
        "name": "search_code",
        "title": "Search Code",
        "description": "Search for code patterns in a cloned repository using grep. Returns matching lines with file paths and line numbers.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "repo_name": {
                    "type": "string",
                    "description": "Repository name (must be cloned first)"
                },
                "pattern": {
                    "type": "string",
                    "description": "Search pattern (literal text or regex)"
                },
                "file_pattern": {
                    "type": "string",
                    "description": "Glob filter (e.g., '*.py', '*.ts', 'src/*.js')"
                },
                "case_sensitive": {
                    "type": "boolean",
                    "description": "Case-sensitive search",
                    "default": False
                },
                "max_results": {
                    "type": "integer",
                    "description": "Maximum results",
                    "default": 20
                }
            },
            "required": ["repo_name", "pattern"]
        },
        "annotations": {
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False
        },
        "usage": {
            "prerequisite": "clone_repository",
            "instructions": [
                "Provide repo_name exactly as used in clone",
                "Use pattern for search term",
                "Optionally filter by file type"
            ],
            "example": {
                "call": "search_code",
                "arguments": {"repo_name": "CLARIOERP_WMS", "pattern": "async function", "file_pattern": "*.ts"}
            },
            "returns": ["matches[].file", "matches[].line", "matches[].content"]
        }
    },
    {
        "name": "get_tree",
        "title": "Get Repository Tree",
        "description": "Display directory structure of a cloned repository as a tree view. Limited to 200 lines.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "repo_name": {
                    "type": "string",
                    "description": "Repository name (must be cloned first)"
                },
                "path": {
                    "type": "string",
                    "description": "Subdirectory path (e.g., 'src/components')",
                    "default": "."
                },
                "max_depth": {
                    "type": "integer",
                    "description": "Tree depth limit",
                    "default": 3
                },
                "show_hidden": {
                    "type": "boolean",
                    "description": "Include hidden files",
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
        },
        "usage": {
            "prerequisite": "clone_repository",
            "instructions": [
                "Provide repo_name exactly as used in clone",
                "Use path to focus on subdirectory",
                "Path is relative to repo root (not including repo name)"
            ],
            "example": {
                "call": "get_tree",
                "arguments": {"repo_name": "CLARIOERP_WMS", "path": "src", "max_depth": 2}
            },
            "returns": ["tree", "truncated"]
        }
    },
    {
        "name": "read_file",
        "title": "Read File",
        "description": "Read contents of a file from a cloned repository. Limited to 200 lines per call.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "repo_name": {
                    "type": "string",
                    "description": "Repository name (must be cloned first)"
                },
                "file_path": {
                    "type": "string",
                    "description": "File path relative to repo root (e.g., 'src/App.tsx')"
                },
                "start_line": {
                    "type": "integer",
                    "description": "Starting line (1-indexed)",
                    "default": 1
                },
                "end_line": {
                    "type": "integer",
                    "description": "Ending line (0 = auto-limit)",
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
        },
        "usage": {
            "prerequisite": "clone_repository",
            "instructions": [
                "Provide repo_name exactly as used in clone",
                "file_path is relative to repo root",
                "Do NOT include repo name in file_path",
                "Use line ranges for large files"
            ],
            "example": {
                "call": "read_file",
                "arguments": {"repo_name": "CLARIOERP_WMS", "file_path": "src/App.tsx", "start_line": 1, "end_line": 50}
            },
            "returns": ["content", "total_lines", "file_size"]
        }
    },
    {
        "name": "get_outline",
        "title": "Get Code Outline",
        "description": "Get structural outline of a code file showing classes, functions, and methods with line numbers.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "repo_name": {
                    "type": "string",
                    "description": "Repository name (must be cloned first)"
                },
                "file_path": {
                    "type": "string",
                    "description": "File path relative to repo root"
                }
            },
            "required": ["repo_name", "file_path"]
        },
        "annotations": {
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False
        },
        "usage": {
            "prerequisite": "clone_repository",
            "instructions": [
                "Provide repo_name exactly as used in clone",
                "file_path is relative to repo root",
                "Best for .py, .js, .ts, .jsx, .tsx files"
            ],
            "example": {
                "call": "get_outline",
                "arguments": {"repo_name": "CLARIOERP_WMS", "file_path": "src/services/api.ts"}
            },
            "returns": ["outline[].type", "outline[].name", "outline[].line"]
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

        if full_path == resolved_repo:
            return None

        if resolved_repo not in full_path.parents:
            return None

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
        return {"error": "Invalid repository name", "success": False, "retryable": False}

    repo_path = get_repo_path(repo_name)
    logger.debug(f"[TOOL:clone_repository] repo_path={repo_path}")

    # Check if already cloned
    if repo_path.exists() and (repo_path / ".git").exists():
        logger.info(f"[TOOL:clone_repository] Repo already exists, pulling latest")
        try:
            result = subprocess.run(
                ["git", "-C", str(repo_path), "pull", "--ff-only"],
                capture_output=True,
                text=True,
                timeout=60
            )
            return {
                "status": "updated",
                "message": f"Repository '{repo_name}' updated with latest changes",
                "path": str(repo_path),
                "success": True
            }
        except subprocess.TimeoutExpired:
            return {
                "status": "exists",
                "message": f"Repository '{repo_name}' exists (pull timed out)",
                "path": str(repo_path),
                "success": True
            }
        except Exception as e:
            logger.error(f"[TOOL:clone_repository] Pull error: {e}")
            return {
                "status": "exists",
                "message": f"Repository '{repo_name}' exists",
                "path": str(repo_path),
                "success": True
            }

    # Clone the repository
    repo_path.parent.mkdir(parents=True, exist_ok=True)
    clone_url = f"https://github.com/{ALLOWED_USERNAME}/{repo_name}.git"

    if GITHUB_PAT:
        clone_url = f"https://{GITHUB_PAT}@github.com/{ALLOWED_USERNAME}/{repo_name}.git"

    try:
        logger.info(f"[TOOL:clone_repository] Cloning from GitHub")
        result = subprocess.run(
            ["git", "clone", "--depth", "1", clone_url, str(repo_path)],
            capture_output=True,
            text=True,
            timeout=120
        )

        if result.returncode != 0:
            logger.error(f"[TOOL:clone_repository] Clone failed: {result.stderr}")
            return {
                "error": f"Clone failed: {result.stderr}",
                "success": False,
                "retryable": True,
                "retry_after": 5
            }

        return {
            "status": "cloned",
            "message": f"Successfully cloned '{ALLOWED_USERNAME}/{repo_name}'",
            "path": str(repo_path),
            "success": True
        }

    except subprocess.TimeoutExpired:
        logger.error(f"[TOOL:clone_repository] Clone timeout")
        return {
            "error": "Clone operation timed out",
            "success": False,
            "retryable": True,
            "retry_after": 5
        }
    except Exception as e:
        logger.error(f"[TOOL:clone_repository] Error: {e}")
        return {
            "error": str(e),
            "success": False,
            "retryable": True
        }


async def search_code_impl(
    repo_name: str,
    pattern: str,
    file_pattern: str = None,
    case_sensitive: bool = False,
    max_results: int = 20
) -> dict:
    """Search for code patterns in a repository"""
    logger.info(f"[TOOL:search_code] repo={repo_name}, pattern={pattern}")

    if not validate_repo_name(repo_name):
        return {"error": "Invalid repository name", "matches": [], "success": False, "retryable": False}

    repo_path = get_repo_path(repo_name)
    if not repo_path.exists():
        return {
            "error": f"Repository not found. Call clone_repository first.",
            "matches": [],
            "success": False,
            "retryable": False
        }

    # Fix common mistake: strip repo name from pattern if included
    if pattern.startswith(repo_name + "/"):
        pattern = pattern[len(repo_name) + 1:]

    cmd = ["grep", "-r", "-n", "--include", file_pattern or "*"]
    if not case_sensitive:
        cmd.append("-i")
    cmd.append("--")
    cmd.append(pattern)
    cmd.append(str(repo_path))

    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        matches = []
        try:
            for line in process.stdout:
                if len(matches) >= max_results:
                    process.terminate()
                    break

                if ":" in line:
                    parts = line.split(":", 2)
                    if len(parts) >= 3:
                        file_path = parts[0].replace(str(repo_path) + "/", "")
                        line_num = parts[1]
                        content = parts[2].strip()[:200]
                        matches.append({
                            "file": file_path,
                            "line": int(line_num) if line_num.isdigit() else 0,
                            "content": content
                        })
        finally:
            if process.stdout:
                process.stdout.close()
            if process.stderr:
                process.stderr.close()
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=5)

        return {
            "matches": matches,
            "total_matches": len(matches),
            "pattern": pattern,
            "truncated": len(matches) >= max_results,
            "success": True
        }

    except subprocess.TimeoutExpired:
        return {
            "error": "Search timed out",
            "matches": [],
            "success": False,
            "retryable": True,
            "retry_after": 5
        }
    except Exception as e:
        return {
            "error": str(e),
            "matches": [],
            "success": False,
            "retryable": True
        }


async def get_tree_impl(
    repo_name: str,
    path: str = ".",
    max_depth: int = 3,
    show_hidden: bool = False
) -> dict:
    """Get directory tree of a repository"""
    logger.info(f"[TOOL:get_tree] repo={repo_name}, path={path}")

    if not validate_repo_name(repo_name):
        return {"error": "Invalid repository name", "tree": "", "success": False, "retryable": False}

    repo_path = get_repo_path(repo_name)
    if not repo_path.exists():
        return {
            "error": f"Repository not found. Call clone_repository first.",
            "tree": "",
            "success": False,
            "retryable": False
        }

    # Fix common mistake: strip repo name from path if included
    if path.startswith(repo_name + "/"):
        path = path[len(repo_name) + 1:]
    elif path == repo_name:
        path = "."

    target_path = validate_file_path(repo_path, path)
    if target_path is None:
        target_path = repo_path

    if not target_path.exists():
        return {"error": f"Path '{path}' does not exist", "tree": "", "success": False, "retryable": False}

    MAX_TREE_LINES = 200
    line_count = [0]

    def build_tree(dir_path: Path, prefix: str = "", depth: int = 0) -> list:
        if depth > max_depth or line_count[0] >= MAX_TREE_LINES:
            return []

        lines = []
        try:
            entries = sorted(dir_path.iterdir(), key=lambda x: (not x.is_dir(), x.name.lower()))
            entries = [e for e in entries if show_hidden or not e.name.startswith(".")]
            entries = [e for e in entries if e.name not in ["node_modules", "__pycache__", ".git", "venv", ".venv"]]

            for i, entry in enumerate(entries):
                if line_count[0] >= MAX_TREE_LINES:
                    break

                is_last = i == len(entries) - 1
                connector = "+-- " if is_last else "|-- "
                size_info = ""
                if entry.is_file():
                    size = entry.stat().st_size
                    size_info = f" ({size:,} bytes)" if size < 1024 * 1024 else f" ({size // 1024 // 1024:.1f} MB)"

                lines.append(f"{prefix}{connector}{entry.name}{size_info}")
                line_count[0] += 1

                if entry.is_dir():
                    extension = "    " if is_last else "|   "
                    lines.extend(build_tree(entry, prefix + extension, depth + 1))

        except PermissionError:
            lines.append(f"{prefix}[Permission denied]")

        return lines

    tree_lines = [f"{repo_name}/"]
    line_count[0] = 1
    tree_lines.extend(build_tree(target_path))

    truncated = line_count[0] >= MAX_TREE_LINES

    result = {
        "tree": "\n".join(tree_lines),
        "repo_name": repo_name,
        "path": path,
        "truncated": truncated,
        "success": True
    }
    if truncated:
        result["note"] = f"Output limited to {MAX_TREE_LINES} lines. Use path parameter to explore subdirectories."

    return result


async def read_file_impl(
    repo_name: str,
    file_path: str,
    start_line: int = 1,
    end_line: int = 0
) -> dict:
    """Read file contents from a repository"""
    logger.info(f"[TOOL:read_file] repo={repo_name}, file={file_path}")

    if not validate_repo_name(repo_name):
        return {"error": "Invalid repository name", "content": "", "success": False, "retryable": False}

    repo_path = get_repo_path(repo_name)
    if not repo_path.exists():
        return {
            "error": f"Repository not found. Call clone_repository first.",
            "content": "",
            "success": False,
            "retryable": False
        }

    # Fix common mistake: strip repo name from file_path if included
    if file_path.startswith(repo_name + "/"):
        file_path = file_path[len(repo_name) + 1:]

    full_path = validate_file_path(repo_path, file_path)
    if full_path is None:
        return {"error": "Invalid file path", "content": "", "success": False, "retryable": False}

    if not full_path.exists():
        return {"error": f"File not found: {file_path}", "content": "", "success": False, "retryable": False}

    if not full_path.is_file():
        return {"error": f"Not a file: {file_path}", "content": "", "success": False, "retryable": False}

    try:
        file_size = full_path.stat().st_size
        if file_size > 1024 * 1024:  # 1MB limit
            return {
                "error": f"File too large ({file_size // 1024 // 1024:.1f} MB). Maximum is 1MB.",
                "content": "",
                "success": False,
                "retryable": False
            }

        with open(full_path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()

        total_lines = len(lines)
        MAX_LINES = 200

        start_idx = max(0, start_line - 1)
        if end_line <= 0:
            end_idx = min(start_idx + MAX_LINES, total_lines)
        else:
            end_idx = min(end_line, start_idx + MAX_LINES, total_lines)

        selected_lines = lines[start_idx:end_idx]
        was_truncated = (end_line <= 0 and total_lines > end_idx) or (end_line > 0 and end_line > end_idx)

        numbered_lines = [
            f"{start_idx + i + 1:>4} | {line.rstrip()}"
            for i, line in enumerate(selected_lines)
        ]

        result = {
            "content": "\n".join(numbered_lines),
            "file_path": file_path,
            "repo_name": repo_name,
            "start_line": start_idx + 1,
            "end_line": end_idx,
            "total_lines": total_lines,
            "file_size": file_size,
            "success": True
        }
        if was_truncated:
            result["truncated"] = True
            result["note"] = f"Output limited to {MAX_LINES} lines. Use start_line/end_line to read other sections."

        return result

    except UnicodeDecodeError:
        return {"error": "Cannot read binary file", "content": "", "success": False, "retryable": False}
    except Exception as e:
        return {
            "error": str(e),
            "content": "",
            "success": False,
            "retryable": True
        }


async def get_outline_impl(repo_name: str, file_path: str) -> dict:
    """Get code outline for a file"""
    logger.info(f"[TOOL:get_outline] repo={repo_name}, file={file_path}")

    if not validate_repo_name(repo_name):
        return {"error": "Invalid repository name", "outline": [], "success": False, "retryable": False}

    repo_path = get_repo_path(repo_name)
    if not repo_path.exists():
        return {
            "error": f"Repository not found. Call clone_repository first.",
            "outline": [],
            "success": False,
            "retryable": False
        }

    # Fix common mistake: strip repo name from file_path if included
    if file_path.startswith(repo_name + "/"):
        file_path = file_path[len(repo_name) + 1:]

    full_path = validate_file_path(repo_path, file_path)
    if full_path is None:
        return {"error": "Invalid file path", "outline": [], "success": False, "retryable": False}

    if not full_path.exists():
        return {"error": f"File not found: {file_path}", "outline": [], "success": False, "retryable": False}

    try:
        with open(full_path, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()

        outline = []
        ext = full_path.suffix.lower()

        if ext == ".py":
            try:
                tree = ast.parse(content)
                for node in ast.walk(tree):
                    if isinstance(node, ast.ClassDef):
                        outline.append({
                            "type": "class",
                            "name": node.name,
                            "line": node.lineno
                        })
                        for item in node.body:
                            if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                                outline.append({
                                    "type": "method",
                                    "name": f"{node.name}.{item.name}",
                                    "line": item.lineno
                                })
                    elif isinstance(node, ast.FunctionDef) and not any(
                        isinstance(parent, ast.ClassDef)
                        for parent in ast.walk(tree)
                        if hasattr(parent, 'body') and node in getattr(parent, 'body', [])
                    ):
                        outline.append({
                            "type": "function",
                            "name": node.name,
                            "line": node.lineno
                        })
                    elif isinstance(node, ast.AsyncFunctionDef) and not any(
                        isinstance(parent, ast.ClassDef)
                        for parent in ast.walk(tree)
                        if hasattr(parent, 'body') and node in getattr(parent, 'body', [])
                    ):
                        outline.append({
                            "type": "async_function",
                            "name": node.name,
                            "line": node.lineno
                        })
            except SyntaxError:
                pass

        elif ext in [".js", ".ts", ".jsx", ".tsx"]:
            patterns = [
                (r'(?:export\s+)?(?:async\s+)?function\s+(\w+)', "function"),
                (r'(?:export\s+)?class\s+(\w+)', "class"),
                (r'(?:export\s+)?const\s+(\w+)\s*=\s*(?:async\s+)?\(', "arrow_function"),
                (r'(\w+)\s*:\s*(?:async\s+)?function', "method"),
            ]
            for line_num, line in enumerate(content.split("\n"), 1):
                for pattern, item_type in patterns:
                    match = re.search(pattern, line)
                    if match:
                        outline.append({
                            "type": item_type,
                            "name": match.group(1),
                            "line": line_num
                        })

        # Generic fallback
        else:
            for match in re.finditer(r'^(?:def|func|function|fn|pub fn|async fn)\s+(\w+)', content, re.MULTILINE):
                line_num = content[:match.start()].count('\n') + 1
                outline.append({"type": "function", "name": match.group(1), "line": line_num})

            for match in re.finditer(r'^(?:class|struct|type|interface)\s+(\w+)', content, re.MULTILINE):
                line_num = content[:match.start()].count('\n') + 1
                outline.append({"type": "class", "name": match.group(1), "line": line_num})

        outline.sort(key=lambda x: x["line"])

        return {
            "outline": outline,
            "file_path": file_path,
            "repo_name": repo_name,
            "file_type": ext,
            "total_items": len(outline),
            "success": True
        }

    except Exception as e:
        return {
            "error": str(e),
            "outline": [],
            "success": False,
            "retryable": True
        }


# ============================================================================
# Response Formatting
# ============================================================================

def format_result_as_markdown(tool_name: str, result: dict) -> str:
    """Format tool result as markdown for better readability"""
    if "error" in result:
        lines = [
            f"## Error\n",
            f"**Tool:** `{tool_name}`",
            f"**Error:** {result['error']}"
        ]
        if result.get("retryable"):
            lines.append("\n*This operation can be retried.*")
        lines.append("\n**Suggestion:** Make sure the repository is cloned first using `clone_repository`.")
        return "\n".join(lines)

    if tool_name == "clone_repository":
        status = "SUCCESS" if result.get("success") else "FAILED"
        return f"""## Clone Repository - {status}

**Status:** {result.get('status', 'unknown')}
**Message:** {result.get('message', 'No message')}
**Path:** `{result.get('path', 'N/A')}`

The repository is now available for exploration with other tools."""

    elif tool_name == "search_code":
        matches = result.get("matches", [])
        total = result.get("total_matches", len(matches))
        truncated = result.get("truncated", False)

        if not matches:
            return f"""## Search Results

**Pattern:** `{result.get('pattern', '')}`
**Matches:** 0

No matches found."""

        lines = [f"""## Search Results

**Pattern:** `{result.get('pattern', '')}`
**Matches:** {total}{' (truncated)' if truncated else ''}

### Matches:
"""]
        for m in matches[:20]:
            lines.append(f"- **{m['file']}** (line {m['line']}): `{m['content'][:100]}`")

        return "\n".join(lines)

    elif tool_name == "get_tree":
        tree = result.get("tree", "")
        truncated = result.get("truncated", False)

        return f"""## Directory Tree

**Repository:** `{result.get('repo_name', '')}`
**Path:** `{result.get('path', '.')}`
{f"**Note:** {result.get('note', '')}" if truncated else ""}

```
{tree}
```"""

    elif tool_name == "read_file":
        content = result.get("content", "")
        truncated = result.get("truncated", False)

        return f"""## File Contents

**Repository:** `{result.get('repo_name', '')}`
**File:** `{result.get('file_path', '')}`
**Lines:** {result.get('start_line', 1)}-{result.get('end_line', '?')} of {result.get('total_lines', '?')}
**Size:** {result.get('file_size', 0):,} bytes
{f"**Note:** {result.get('note', '')}" if truncated else ""}

```
{content}
```"""

    elif tool_name == "get_outline":
        outline = result.get("outline", [])

        if not outline:
            return f"""## Code Outline

**File:** `{result.get('file_path', '')}`
**Type:** `{result.get('file_type', '')}`

No classes or functions found."""

        lines = [f"""## Code Outline

**File:** `{result.get('file_path', '')}`
**Type:** `{result.get('file_type', '')}`
**Items:** {len(outline)}

### Structure:
"""]
        for item in outline:
            type_label = item['type'].upper()
            lines.append(f"- [{type_label}] **{item['name']}** - line {item['line']}")

        return "\n".join(lines)

    # Default: return as JSON
    return f"""## Result

```json
{json.dumps(result, indent=2)}
```"""


def truncate_response(response: dict, max_size: int = MAX_RESPONSE_SIZE) -> dict:
    """Truncate response if it exceeds max size"""
    response_str = json.dumps(response)
    original_size = len(response_str)

    if original_size <= max_size:
        return response

    logger.warning(f"[TRUNCATE] Response too large: {original_size} bytes")

    if "result" in response and "content" in response["result"]:
        content = response["result"]["content"]
        if isinstance(content, list):
            for item in content:
                if item.get("type") == "text" and "text" in item:
                    text = item["text"]
                    overhead = original_size - len(text)
                    max_text_size = max_size - overhead - 200

                    if len(text) > max_text_size:
                        item["text"] = text[:max_text_size] + f"\n\n... [TRUNCATED: {original_size} bytes]"

    return response


# ============================================================================
# MCP Request Handler
# ============================================================================

async def handle_mcp_request(request_data: dict, base_url: str = "") -> dict:
    """Handle MCP JSON-RPC requests"""
    method = request_data.get("method", "")
    params = request_data.get("params", {})
    request_id = request_data.get("id")

    logger.info(f"[MCP] method={method}, id={request_id}")

    result = None
    error = None

    try:
        if method == "initialize":
            client_protocol = params.get("protocolVersion", "2025-06-18")
            logger.info(f"[MCP] Client protocol: {client_protocol}")
            result = {
                "protocolVersion": client_protocol,
                "serverInfo": {
                    "name": SERVER_INFO["name"],
                    "version": SERVER_INFO["version"],
                    "description": SERVER_INFO["description"],
                    "workflow": SERVER_INFO["workflow"],
                    "limits": SERVER_INFO["limits"]
                },
                "capabilities": {
                    "tools": {"listChanged": False},
                    "resources": {"listChanged": False}
                }
            }

        elif method == "tools/list":
            result = {"tools": TOOLS, "nextCursor": None}

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
            else:
                error = {"code": -32601, "message": f"Unknown tool: {tool_name}"}
                tool_result = None

            if tool_result is not None:
                is_error = "error" in tool_result and not tool_result.get("success", True)
                formatted_text = format_result_as_markdown(tool_name, tool_result)
                result = {
                    "content": [{"type": "text", "text": formatted_text}],
                    "isError": is_error
                }

        elif method == "resources/list":
            result = {"resources": RESOURCES, "nextCursor": None}

        elif method == "ping":
            result = {}

        elif method.startswith("notifications/"):
            return None

        else:
            error = {"code": -32601, "message": f"Method not found: {method}"}

    except Exception as e:
        logger.error(f"[MCP] Error: {e}", exc_info=True)
        error = {
            "code": -32603,
            "message": str(e),
            "data": {"retryable": True}
        }

    response = {"jsonrpc": "2.0"}

    if request_id is not None:
        response["id"] = request_id

    if error:
        response["error"] = error
    else:
        response["result"] = result

    response = truncate_response(response)
    logger.info(f"[MCP] Response size: {len(json.dumps(response))} bytes")

    return response


# ============================================================================
# Helper Functions
# ============================================================================

def get_base_url_from_request(request: Request) -> str:
    """Get the base URL from request headers"""
    scheme = request.headers.get("x-forwarded-proto", "https")
    host = request.headers.get("x-forwarded-host") or request.headers.get("host")

    if not host or "0.0.0.0" in host:
        if BASE_URL and "0.0.0.0" not in BASE_URL:
            return BASE_URL.rstrip("/")
        else:
            return f"{scheme}://{request.url.netloc}"
    else:
        return f"{scheme}://{host}"


# ============================================================================
# FastAPI Endpoints
# ============================================================================

@app.get("/health")
async def health_check():
    """Health check endpoint - MUST respond quickly"""
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
        "transport": ["streamable-http"],
        "authentication": "none",
        "mcp_protocol_version": "2025-06-18",
        "stateless": True,
        "annotations": {
            "readOnlyHint": True,
            "destructiveHint": False
        }
    }


@app.get("/sse")
async def sse_stream(request: Request):
    """
    SSE streaming endpoint (OPTIONAL - for backwards compatibility).

    NOTE: ChatGPT does NOT use this endpoint well. The POST /sse endpoint
    is preferred for stateless operation.
    """
    session_id = str(uuid.uuid4())
    message_queue: asyncio.Queue = asyncio.Queue()
    sse_sessions[session_id] = message_queue

    base_url = get_base_url_from_request(request)
    logger.info(f"[SSE] New connection, session_id={session_id}")

    async def event_generator():
        try:
            endpoint_url = f"{base_url}/messages?session_id={session_id}"
            yield f"event: endpoint\ndata: {endpoint_url}\n\n"

            while True:
                if await request.is_disconnected():
                    break

                try:
                    message = await asyncio.wait_for(message_queue.get(), timeout=15.0)
                    yield f"event: message\ndata: {json.dumps(message)}\n\n"
                except asyncio.TimeoutError:
                    yield f"event: ping\ndata: {json.dumps({'type': 'ping'})}\n\n"

        except asyncio.CancelledError:
            pass
        finally:
            sse_sessions.pop(session_id, None)

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
async def mcp_messages(request: Request, session_id: str = Query(None)):
    """
    Receive MCP messages for SSE sessions.

    IMPORTANT FIX: Now accepts requests even without valid session.
    This prevents tool eviction when sessions are lost.
    """
    logger.info(f"[MSG] POST /messages, session={session_id[:8] if session_id else 'none'}")

    base_url = get_base_url_from_request(request)

    try:
        body = await request.json()

        if isinstance(body, list):
            responses = []
            for req in body:
                resp = await handle_mcp_request(req, base_url=base_url)
                if resp is not None:
                    responses.append(resp)

            if session_id and session_id in sse_sessions:
                for resp in responses:
                    await sse_sessions[session_id].put(resp)
                return Response(status_code=202)
            else:
                return JSONResponse(content=responses)
        else:
            response = await handle_mcp_request(body, base_url=base_url)

            if session_id and session_id in sse_sessions:
                if response is not None:
                    await sse_sessions[session_id].put(response)
                return Response(status_code=202)
            else:
                if response is None:
                    return Response(status_code=204)
                return JSONResponse(content=response)

    except json.JSONDecodeError as e:
        logger.error(f"[MSG] JSON parse error: {e}")
        return JSONResponse(
            status_code=200,  # Return 200 to avoid tool eviction
            content={
                "jsonrpc": "2.0",
                "error": {
                    "code": -32700,
                    "message": "Parse error",
                    "data": {"retryable": True}
                },
                "id": None
            }
        )
    except Exception as e:
        logger.error(f"[MSG] Error: {e}", exc_info=True)
        return JSONResponse(
            status_code=200,  # Return 200 to avoid tool eviction
            content={
                "jsonrpc": "2.0",
                "error": {
                    "code": -32603,
                    "message": str(e),
                    "data": {"retryable": True, "retry_after": 1}
                },
                "id": None
            }
        )


@app.post("/sse")
async def mcp_endpoint(request: Request):
    """
    Direct MCP protocol endpoint (Streamable HTTP transport).

    THIS IS THE PRIMARY ENDPOINT FOR CHATGPT.

    Key design principles:
    1. STATELESS: No session validation required
    2. IDEMPOTENT: Same request always produces same response
    3. TOLERANT: Never returns 404 or session errors
    4. EXPLICIT: Always returns structured JSON, never silence
    """
    client_host = request.client.host if request.client else "unknown"
    logger.info(f"[POST /sse] Request from {client_host}")

    session_id = request.headers.get("mcp-session-id")
    protocol_version = request.headers.get("mcp-protocol-version", "2025-06-18")

    try:
        body = await request.json()
        base_url = get_base_url_from_request(request)
        method = body.get("method", "") if isinstance(body, dict) else ""
        logger.info(f"[POST /sse] method={method}")

        if method == "initialize":
            session_id = str(uuid.uuid4())
            if isinstance(body, dict) and "params" in body:
                protocol_version = body["params"].get("protocolVersion", protocol_version)
            logger.info(f"[POST /sse] New session: {session_id[:8]}")

        if isinstance(body, list):
            logger.info(f"[POST /sse] Processing batch of {len(body)} requests")
            responses = []
            for req in body:
                resp = await handle_mcp_request(req, base_url=base_url)
                if resp is not None:
                    responses.append(resp)
            response = JSONResponse(content=responses)
        else:
            mcp_response = await handle_mcp_request(body, base_url=base_url)
            if mcp_response is None:
                resp = Response(status_code=204)
                if session_id:
                    resp.headers["Mcp-Session-Id"] = session_id
                return resp

            response = JSONResponse(content=mcp_response)

        if session_id:
            response.headers["Mcp-Session-Id"] = session_id

        return response

    except json.JSONDecodeError as e:
        logger.error(f"[POST /sse] JSON parse error: {e}")
        return JSONResponse(
            status_code=200,  # Return 200 to avoid tool eviction
            content={
                "jsonrpc": "2.0",
                "error": {
                    "code": -32700,
                    "message": "Parse error",
                    "data": {"retryable": True}
                },
                "id": None
            }
        )
    except Exception as e:
        logger.error(f"[POST /sse] Error: {e}", exc_info=True)
        # CRITICAL: Return 200 with error in body, not 500
        # This prevents ChatGPT from marking the tool as unhealthy
        return JSONResponse(
            status_code=200,
            content={
                "jsonrpc": "2.0",
                "error": {
                    "code": -32603,
                    "message": str(e),
                    "data": {
                        "retryable": True,
                        "retry_after": 1
                    }
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
        "transport": "streamable-http",
        "stateless": True,
        "endpoints": {
            "mcp": "/sse (POST for MCP requests - PRIMARY)",
            "sse_legacy": "/sse (GET for SSE stream - OPTIONAL)",
            "messages_legacy": "/messages?session_id=<id> (POST - OPTIONAL)",
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
