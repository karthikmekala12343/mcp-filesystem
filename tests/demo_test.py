import asyncio
import shutil
import tempfile
from pathlib import Path

from src.server.filesystem_mcp_server import MCPServer
from src.agent.matching_agent import demo_workflow


async def run_demo():
    tmp = Path(tempfile.mkdtemp())
    try:
        # create a small data root for the server
        data_root = tmp / "data"
        data_root.mkdir()

        server = MCPServer(host="127.0.0.1", port=0, root=data_root, poll_interval=0.5)
        server_task = asyncio.create_task(server.start())
        while server.server is None:
            await asyncio.sleep(0.05)

        actual_port = server.server.sockets[0].getsockname()[1]
        await demo_workflow(host="127.0.0.1", port=actual_port, use_config=False)

        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass

    finally:
        shutil.rmtree(tmp)


if __name__ == "__main__":
    asyncio.run(run_demo())
