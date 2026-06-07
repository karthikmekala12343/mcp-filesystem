"""Simple CLI for MCP filesystem resume operations."""
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from src.agent.matching_agent import MultiMCPAgent


async def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="MCP filesystem CLI")
    parser.add_argument("command", choices=["list", "read", "save"], help="Command to run")
    parser.add_argument("filename", nargs="?", help="Resume filename for read/save")
    parser.add_argument("content", nargs="?", help="Content for save command")
    parser.add_argument("--host", default="127.0.0.1", help="MCP server host")
    parser.add_argument("--port", type=int, default=8765, help="MCP server port")
    args = parser.parse_args(argv)

    server_name = f"{args.host}:{args.port}"
    agent = MultiMCPAgent()
    await agent.add_server(args.host, args.port, name=server_name)

    try:
        if args.command == "list":
            resumes = await agent.list_resumes(server_name)
            for resume in resumes:
                print(resume)
            return 0

        if args.command == "read":
            if not args.filename:
                print("error: filename is required for read", file=sys.stderr)
                return 1
            content = await agent.read_resume(server_name, args.filename)
            print(content)
            return 0

        if args.command == "save":
            if not args.filename or args.content is None:
                print("error: filename and content are required for save", file=sys.stderr)
                return 1
            result = await agent.save_resume(server_name, args.filename, args.content)
            print(result)
            return 0

        print(f"Unknown command: {args.command}", file=sys.stderr)
        return 1
    finally:
        await agent.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
