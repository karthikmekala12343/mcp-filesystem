"""JSON-RPC 2.0 MCP Filesystem Server.

Supports a newline-delimited JSON transport. Implements JSON-RPC 2.0
request/response semantics and notifications. Provides filesystem
resources and MCP-specific capabilities such as `watch_directory`
and `batch_process`.
"""
from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set


JSON = Dict[str, Any]


class JSONRPCError(Exception):
    def __init__(self, code: int, message: str, data: Any | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.data = data


@dataclass
class ClientContext:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    addr: Any
    watched_paths: Set[str]


class MCPServer:
    def __init__(self, host: str = "127.0.0.1", port: int = 8765, root: Path | str = "./data", poll_interval: float = 1.0):
        self.host = host
        self.port = port
        self.root = Path(root).resolve()
        self.server: Optional[asyncio.AbstractServer] = None
        self.clients: List[ClientContext] = []
        self._watch_index: Dict[str, Dict[str, float]] = {}  # path -> name -> mtime
        self.poll_interval = poll_interval
        self._watch_task: Optional[asyncio.Task] = None

    # --- JSON-RPC helpers -------------------------------------------------
    def _make_response(self, request_id: Any, result: Any = None, error: Any = None) -> JSON:
        resp: JSON = {"jsonrpc": "2.0", "id": request_id}
        if error is not None:
            resp["error"] = error
        else:
            resp["result"] = result
        return resp

    async def _send(self, writer: asyncio.StreamWriter, payload: JSON) -> None:
        writer.write((json.dumps(payload) + "\n").encode())
        await writer.drain()

    # --- Connection handling ----------------------------------------------
    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        addr = writer.get_extra_info("peername")
        ctx = ClientContext(reader=reader, writer=writer, addr=addr, watched_paths=set())
        self.clients.append(ctx)
        print(f"Client connected: {addr}")

        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                try:
                    req = json.loads(line.decode())
                except Exception:
                    # JSON parse error -> send JSON-RPC error (Parse error -32700)
                    err = {"code": -32700, "message": "Parse error"}
                    await self._send(writer, self._make_response(None, error=err))
                    continue

                # Notification (no id) or request
                request_id = req.get("id")
                method = req.get("method")
                params = req.get("params") or {}

                print(f"Request from {addr}: method={method!r}, id={request_id!r}, params={params!r}")

                if not method:
                    err = {"code": -32600, "message": "Invalid Request"}
                    await self._send(writer, self._make_response(request_id, error=err))
                    continue

                # Dispatch
                handler_name = f"rpc_{method}"
                if not hasattr(self, handler_name):
                    err = {"code": -32601, "message": "Method not found"}
                    await self._send(writer, self._make_response(request_id, error=err))
                    continue

                handler = getattr(self, handler_name)
                try:
                    # Allow handlers to be sync or async
                    if asyncio.iscoroutinefunction(handler):
                        result = await handler(ctx, **params)
                    else:
                        result = handler(ctx, **params)
                    if request_id is not None:
                        await self._send(writer, self._make_response(request_id, result=result))
                except JSONRPCError as jerr:
                    err = {"code": jerr.code, "message": jerr.message, "data": jerr.data}
                    await self._send(writer, self._make_response(request_id, error=err))
                except Exception as exc:
                    err = {"code": -32000, "message": str(exc)}
                    await self._send(writer, self._make_response(request_id, error=err))

        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            self.clients.remove(ctx)
            print(f"Client disconnected: {addr}")

    # --- Path utilities --------------------------------------------------
    def _resolve_path(self, path: str) -> Path:
        target = (self.root / path).resolve()
        try:
            target.relative_to(self.root)
        except Exception:
            raise JSONRPCError(-32602, "Path escapes configured root")
        return target

    # --- RPC methods -----------------------------------------------------
    def rpc_list_files(self, ctx: ClientContext, path: str = "") -> List[str]:
        base = self._resolve_path(path)
        if not base.exists():
            return []
        if base.is_file():
            return [base.name]
        return [p.name for p in sorted(base.iterdir())]

    def rpc_read_file(self, ctx: ClientContext, path: str) -> str:
        target = self._resolve_path(path)
        if not target.exists() or not target.is_file():
            raise JSONRPCError(-32602, "File not found")
        return target.read_text(encoding="utf-8")

    def rpc_write_file(self, ctx: ClientContext, path: str, content: str) -> bool:
        target = self._resolve_path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
        return True

    def rpc_exists(self, ctx: ClientContext, path: str) -> bool:
        target = self._resolve_path(path)
        return target.exists()

    def rpc_stat(self, ctx: ClientContext, path: str) -> Dict[str, Any]:
        target = self._resolve_path(path)
        if not target.exists():
            raise JSONRPCError(-32602, "Path not found")
        st = target.stat()
        return {"size": st.st_size, "mtime": st.st_mtime, "is_file": target.is_file(), "is_dir": target.is_dir()}

    async def rpc_batch_process(self, ctx: ClientContext, paths: List[str], op: str = "read") -> Dict[str, Any]:
        """Process multiple files concurrently. Supported ops: 'read', 'stat'."""
        results: Dict[str, Any] = {}

        async def _read(p: str) -> None:
            try:
                results[p] = self.rpc_read_file(ctx, p)
            except Exception as e:
                results[p] = {"error": str(e)}

        async def _stat(p: str) -> None:
            try:
                results[p] = self.rpc_stat(ctx, p)
            except Exception as e:
                results[p] = {"error": str(e)}

        tasks = []
        for p in paths:
            if op == "read":
                tasks.append(asyncio.create_task(_read(p)))
            elif op == "stat":
                tasks.append(asyncio.create_task(_stat(p)))
            else:
                results[p] = {"error": "unsupported op"}

        if tasks:
            await asyncio.gather(*tasks)
        return results

    def rpc_list_resources(self, ctx: ClientContext) -> Dict[str, Any]:
        """Return discovery metadata describing available RPC methods and capabilities."""
        methods = [name[4:] for name in dir(self) if name.startswith("rpc_")]
        return {"service": "mcp-filesystem", "root": str(self.root), "methods": sorted(methods)}

    def rpc_get_config(self, ctx: ClientContext) -> Dict[str, Any]:
        return {"host": self.host, "port": self.port, "root": str(self.root), "poll_interval": self.poll_interval}

    def rpc_reload_config(self, ctx: ClientContext) -> bool:
        base = Path(__file__).resolve().parents[2]
        config_path = base / "config.json"
        if not config_path.exists():
            raise JSONRPCError(-32001, "Config not found")
        cfg = json.loads(config_path.read_text(encoding="utf-8"))
        self.host = cfg.get("host", self.host)
        self.port = int(cfg.get("port", self.port))
        self.root = (base / cfg.get("root", "./data")).resolve()
        self.poll_interval = float(cfg.get("poll_interval", self.poll_interval))
        return True

    # --- Watch directory (notification) support --------------------------
    async def rpc_watch_directory(self, ctx: ClientContext, path: str = "", recursive: bool = False) -> bool:
        """Register this client to receive 'file_added' notifications for `path`.

        Notifications are sent as JSON-RPC notifications with method 'file_added'.
        """
        target = self._resolve_path(path)
        watched = str(target)
        ctx.watched_paths.add(watched)
        # Initialize watch index
        names = {}
        if target.exists() and target.is_dir():
            for p in target.iterdir():
                names[p.name] = p.stat().st_mtime
        self._watch_index[watched] = names
        if self._watch_task is None or self._watch_task.done():
            self._watch_task = asyncio.create_task(self._watcher_loop())
        return True

    async def rpc_unwatch_directory(self, ctx: ClientContext, path: str = "") -> bool:
        target = self._resolve_path(path)
        watched = str(target)
        ctx.watched_paths.discard(watched)
        # remove index if no clients watching
        still_watched = any(watched in c.watched_paths for c in self.clients)
        if not still_watched:
            self._watch_index.pop(watched, None)
        return True

    async def _watcher_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self.poll_interval)
                # For each watched path, check for new files
                for watched_path, known in list(self._watch_index.items()):
                    try:
                        p = Path(watched_path)
                        if not p.exists() or not p.is_dir():
                            continue
                        current = {f.name: f.stat().st_mtime for f in p.iterdir()}
                        # New files: keys in current not in known
                        added = [name for name in current.keys() if name not in known]
                        if added:
                            # update known
                            known.update({name: current[name] for name in added})
                            # notify all clients that watch this path
                            for c in list(self.clients):
                                if watched_path in c.watched_paths:
                                    for name in added:
                                        notif = {"jsonrpc": "2.0", "method": "file_added", "params": {"path": watched_path, "name": name}}
                                        try:
                                            await self._send(c.writer, notif)
                                        except Exception:
                                            # ignore send errors; client cleanup happens elsewhere
                                            pass
                    except Exception:
                        continue
        except asyncio.CancelledError:
            return

    # --- Server lifecycle -----------------------------------------------
    async def start(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        self.server = await asyncio.start_server(self.handle_client, self.host, self.port)
        addr = self.server.sockets[0].getsockname()
        print(f"MCP filesystem server listening on {addr}, root={self.root}")
        async with self.server:
            await self.server.serve_forever()


if __name__ == "__main__":
    base = Path(__file__).resolve().parents[2]
    config_path = base / "config.json"
    cfg = {"host": "127.0.0.1", "port": 8765, "root": "./data", "poll_interval": 1.0}
    if config_path.exists():
        cfg.update(json.loads(config_path.read_text(encoding="utf-8")))

    server = MCPServer(host=cfg.get("host"), port=int(cfg.get("port")), root=(base / cfg.get("root")), poll_interval=float(cfg.get("poll_interval", 1.0)))
    try:
        asyncio.run(server.start())
    except KeyboardInterrupt:
        print("Server stopped by user")
