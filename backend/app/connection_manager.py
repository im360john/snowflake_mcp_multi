import asyncio
import multiprocessing
from typing import Dict, Optional
import uvicorn
from contextlib import asynccontextmanager
import signal
import os
from .config import settings

class ConnectionManager:
    def __init__(self):
        self.active_servers: Dict[str, multiprocessing.Process] = {}
        self.port_allocator = PortAllocator(
            start_port=settings.port_range_start,
            end_port=settings.port_range_end
        )
    
    async def start_connection(self, connection_id: str, connection_config: dict) -> int:
        """Start a new MCP server for a connection"""
        if connection_id in self.active_servers:
            await self.stop_connection(connection_id)
        
        port = self.port_allocator.allocate()
        
        # Start MCP server in a separate process
        process = multiprocessing.Process(
            target=run_mcp_server,
            args=(connection_config, port, connection_id)
        )
        process.start()
        
        self.active_servers[connection_id] = process
        
        # Wait for server to be ready
        await self._wait_for_server(port)
        
        return port
    
    async def stop_connection(self, connection_id: str):
        """Stop an MCP server"""
        if connection_id in self.active_servers:
            process = self.active_servers[connection_id]
            process.terminate()
            process.join(timeout=5)
            if process.is_alive():
                process.kill()
            del self.active_servers[connection_id]
    
    async def _wait_for_server(self, port: int, timeout: int = 10):
        """Wait for server to be ready"""
        import aiohttp
        start_time = asyncio.get_event_loop().time()
        
        while asyncio.get_event_loop().time() - start_time < timeout:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(f"http://localhost:{port}/health") as resp:
                        if resp.status == 200:
                            return
            except:
                await asyncio.sleep(0.5)
        
        raise TimeoutError(f"Server on port {port} didn't start in time")

class PortAllocator:
    def __init__(self, start_port: int = 8100, end_port: int = 8200):
        self.current_port = start_port
        self.end_port = end_port
        self.allocated_ports = set()
    
    def allocate(self) -> int:
        while self.current_port in self.allocated_ports:
            self.current_port += 1
            if self.current_port > self.end_port:
                raise ValueError("No available ports in range")
        
        port = self.current_port
        self.allocated_ports.add(port)
        self.current_port += 1
        
        return port
    
    def release(self, port: int):
        self.allocated_ports.discard(port)

def run_mcp_server(connection_config: dict, port: int, connection_id: str):
    """Run MCP server in a separate process"""
    from mcp_servers.snowflake_server import create_snowflake_mcp_server
    
    # Set up signal handling for graceful shutdown
    signal.signal(signal.SIGTERM, lambda *args: os._exit(0))
    
    # Create and run the MCP server
    app = create_snowflake_mcp_server(connection_config, connection_id)
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        log_level="info",
        access_log=False
    )
