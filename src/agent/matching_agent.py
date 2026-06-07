"""Matching agent that uses MCP clients to access filesystem MCP and other MCP services.

This agent demonstrates replacing local filesystem tools with MCP RPC calls
and shows multi-MCP integration.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class MCPClient:
    def __init__(self, host: str = "127.0.0.1", port: int = 8765, name: str | None = None):
        self.host = host
        self.port = port
        self.name = name or f"{host}:{port}"
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        self._id = 0
        self._recv_task: Optional[asyncio.Task] = None
        self._pending: Dict[int, asyncio.Future] = {}
        self._notification_handlers: List[Callable[[str, Dict[str, Any]], None]] = []

    async def connect(self) -> None:
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
        self._recv_task = asyncio.create_task(self._recv_loop())
        logger.info("Connected to MCP %s", self.name)

    async def _recv_loop(self) -> None:
        assert self.reader is not None
        while True:
            line = await self.reader.readline()
            if not line:
                break
            try:
                msg = json.loads(line.decode())
            except Exception:
                continue

            # Notification
            if "method" in msg and "id" not in msg:
                method = msg["method"]
                params = msg.get("params", {})
                for h in self._notification_handlers:
                    try:
                        h(method, params)
                    except Exception:
                        logger.exception("Notification handler error")
                continue

            # Response
            req_id = msg.get("id")
            if req_id is None:
                continue
            fut = self._pending.pop(req_id, None)
            if fut is None:
                continue
            if "error" in msg:
                fut.set_exception(RuntimeError(msg["error"]))
            else:
                fut.set_result(msg.get("result"))

    async def call(self, method: str, params: Dict[str, Any] | None = None, timeout: float = 5.0) -> Any:
        if self.writer is None:
            raise RuntimeError("Not connected")
        self._id += 1
        req = {"jsonrpc": "2.0", "id": self._id, "method": method, "params": params or {}}
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending[self._id] = fut
        self.writer.write((json.dumps(req) + "\n").encode())
        await self.writer.drain()
        return await asyncio.wait_for(fut, timeout=timeout)

    async def notify(self, method: str, params: Dict[str, Any] | None = None) -> None:
        if self.writer is None:
            raise RuntimeError("Not connected")
        req = {"jsonrpc": "2.0", "method": method, "params": params or {}}
        self.writer.write((json.dumps(req) + "\n").encode())
        await self.writer.drain()

    def add_notification_handler(self, handler: Callable[[str, Dict[str, Any]], None]) -> None:
        self._notification_handlers.append(handler)

    async def close(self) -> None:
        if self.writer:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception:
                pass
        if self._recv_task:
            self._recv_task.cancel()


class MultiMCPAgent:
    def __init__(self):
        self.clients: Dict[str, MCPClient] = {}
        self._notification_queue: asyncio.Queue[Tuple[str, str, Dict[str, Any]]] = asyncio.Queue()

    async def add_server(self, host: str, port: int, name: str | None = None) -> MCPClient:
        client = MCPClient(host=host, port=port, name=name)
        # notification handler routes into queue
        client.add_notification_handler(lambda m, p, n=name or f"{host}:{port}": asyncio.create_task(self._enqueue_notification(n, m, p)))
        await client.connect()
        key = client.name
        self.clients[key] = client
        return client

    async def _enqueue_notification(self, source: str, method: str, params: Dict[str, Any]) -> None:
        await self._notification_queue.put((source, method, params))

    async def call(self, server_name: str, method: str, params: Dict[str, Any] | None = None, timeout: float = 5.0) -> Any:
        print(f"Calling {method} on {server_name} with params {params}")
        client = self.clients.get(server_name)
        if client is None:
            raise RuntimeError("Unknown server")
        return await client.call(method, params, timeout=timeout)

    async def notify(self, server_name: str, method: str, params: Dict[str, Any] | None = None) -> None:
        client = self.clients.get(server_name)
        if client is None:
            raise RuntimeError("Unknown server")
        await client.notify(method, params)

    async def list_resumes(self, server_name: str) -> List[str]:
        return await self.call(server_name, "list_files", {"path": "resumes"})

    async def read_resume(self, server_name: str, filename: str) -> str:
        return await self.call(server_name, "read_file", {"path": f"resumes/{filename}"})

    async def save_resume(self, server_name: str, filename: str, content: str) -> bool:
        return await self.call(server_name, "write_file", {"path": f"resumes/{filename}", "content": content})

    async def watch_resumes(self, server_name: str) -> bool:
        return await self.call(server_name, "watch_directory", {"path": "resumes"})

    async def unwatch_resumes(self, server_name: str) -> bool:
        return await self.call(server_name, "unwatch_directory", {"path": "resumes"})

    async def run_notification_loop(self) -> None:
        while True:
            source, method, params = await self._notification_queue.get()
            logger.info("Notification from %s: %s %s", source, method, params)

    async def close(self) -> None:
        for c in list(self.clients.values()):
            await c.close()


async def use_case_list_and_read_resumes(agent: MultiMCPAgent, server_name: str) -> None:
    logger.info("Use case 1: list and batch read resumes")
    files = await agent.list_resumes(server_name)
    logger.info("Resumes in directory: %s", json.dumps(files, indent=2))
    if not files:
        logger.info("No resumes found, creating a sample resume.")
        await agent.save_resume(server_name, "sample_resume.txt", "Name: Sample\nSkills: Python, MCP")
        files = await agent.list_resumes(server_name)
    batch = await agent.call(server_name, "batch_process", {"paths": [f"resumes/{name}" for name in files], "op": "read"})
    logger.info("Batch read resume contents: %s", {k: (v[:40] + '...' if isinstance(v, str) and len(v) > 40 else v) for k, v in batch.items()})


async def use_case_save_resume(agent: MultiMCPAgent, server_name: str) -> None:
    logger.info("Use case 2: save a new resume")
    filename = f"resumes/agent_resume_{int(time.time())}.txt"
    success = await agent.save_resume(server_name, filename.split("/", 1)[1], "Name: Agent\nRole: MCP client")
    logger.info("Saved resume %s: %s", filename, success)
    exists = await agent.call(server_name, "exists", {"path": filename})
    logger.info("Resume exists after save: %s", exists)


import random  # Place this import at the top of your script

async def use_case_watch_new_resumes(agent: MultiMCPAgent, server_name: str) -> None:
    logger.info("Use case 3: watch for new resumes")
    await agent.watch_resumes(server_name)
    await asyncio.sleep(0.25)
    
    filename = f"resumes/new_resume_{int(time.time())}.txt"
    
    # Pool of names for variance during testing
    names = [
        "Karthik Mekala", "Elena Vance", "Marcus Sterling", 
        "Aria Lin", "David Choi", "Nadia Petrov", "Kiran Patel"
    ]
    selected_name = random.choice(names)
    
    # Dynamically inject the name and tech stack details
    resume_content = (
        f"Name: {selected_name}\n"
        "Status: New\n"
        "Tech Stack: Python, TypeScript, Asyncio, FastAPI, Docker, PostgreSQL, AWS"
    )
    
    await agent.save_resume(server_name, filename.split("/", 1)[1], resume_content)
    logger.info("Created new resume for %s to trigger watch: %s", selected_name, filename)
    await asyncio.sleep(2.0)



async def agent_workflow(
    host: str = "127.0.0.1",
    port: int = 8765,
    use_config: bool = True,
    action: str = "list",
    filename: str | None = None,
    content: str | None = None,
) -> None:
    base = Path(__file__).resolve().parents[3]
    config_path = base / "config.json"
    cfg = {"host": host, "port": port}
    if use_config and config_path.exists():
        cfg.update(json.loads(config_path.read_text(encoding="utf-8")))

    agent = MultiMCPAgent()
    server_name = f"{cfg.get('host')}:{cfg.get('port')}"
    await agent.add_server(cfg.get("host"), int(cfg.get("port")), name=server_name)

    # start notification consumer
    consumer = asyncio.create_task(agent.run_notification_loop())

    try:
        # Resource discovery
        resources = await agent.call(server_name, "list_resources")
        logger.info("Resources: %s", json.dumps(resources, indent=2))

        if action == "list":
            await use_case_list_and_read_resumes(agent, server_name)
        elif action == "read":
            if not filename:
                raise ValueError("--filename is required for action=read")
            resume_path = f"resumes/{filename}"
            exists = await agent.call(server_name, "exists", {"path": resume_path})
            if not exists:
                logger.warning("Resume %s not found.", filename)
                # await agent.save_resume(server_name, filename, "Name: Sample\nSkills: Python, MCP")
            else:
                content = await agent.read_resume(server_name, filename)
                logger.info("Read resume %s:\n%s", filename, content)
        elif action == "create":
            if not filename or content is None:
                raise ValueError("--filename and --content are required for action=create")
            success = await agent.save_resume(server_name, filename, content)
            logger.info("Created resume %s: %s", filename, success)
        else:
            raise ValueError(f"Unsupported action: {action}")

        logger.info("Demo workflow completed")

    finally:
        consumer.cancel()
        await agent.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run MCP agent workflow")
    parser.add_argument("--host", default="127.0.0.1", help="MCP server host")
    parser.add_argument("--port", type=int, default=8765, help="MCP server port")
    parser.add_argument(
        "--action",
        default="list",
        choices=["list", "read", "create"],
        help="Workflow action to execute",
    )
    parser.add_argument("--filename", help="Resume filename for read/create")
    parser.add_argument("--content", help="Resume content for create")
    parser.add_argument(
        "--no-config",
        dest="use_config",
        action="store_false",
        help="Disable loading host/port from config.json",
    )
    args = parser.parse_args()

    asyncio.run(
        agent_workflow(
            host=args.host,
            port=args.port,
            use_config=args.use_config,
            action=args.action,
            filename=args.filename,
            content=args.content,
        )
    )
