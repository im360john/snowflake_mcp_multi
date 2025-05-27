from fastmcp import FastMCP
from starlette.applications import Starlette
from starlette.routing import Route
from starlette.responses import PlainTextResponse
from mcp.server.sse import SseServerTransport
import snowflake.connector
from typing import Optional
import os
import json

def create_snowflake_mcp_server(config: dict, connection_id: str) -> Starlette:
    """Create a Snowflake MCP server with SSE transport"""
    
    # Create FastMCP instance
    mcp = FastMCP(
        name=f"Snowflake-{config['name']}",
        instructions=f"""
        This server provides access to Snowflake database: {config['database']}.
        Schema: {config['schema']}
        Warehouse: {config['warehouse']}
        """
    )
    
    # Connection pool for Snowflake
    conn_pool = SnowflakeConnectionPool(config)
    
    # Define tools
    @mcp.tool()
    async def read_query(query: str) -> dict:
        """Execute a SELECT query on Snowflake"""
        if not query.strip().upper().startswith('SELECT'):
            return {"error": "Only SELECT queries are allowed"}
        
        try:
            with conn_pool.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                
                # Get column names
                columns = [desc[0] for desc in cursor.description]
                
                # Fetch results
                results = cursor.fetchall()
                
                # Convert to list of dicts
                data = [dict(zip(columns, row)) for row in results]
                
                return {
                    "success": True,
                    "data": data,
                    "row_count": len(data)
                }
        except Exception as e:
            return {"error": str(e)}
    
    @mcp.tool()
    async def list_tables(database: Optional[str] = None, schema: Optional[str] = None) -> dict:
        """List tables in the specified database and schema"""
        try:
            db = database or config['database']
            sch = schema or config['schema']
            
            query = f"SHOW TABLES IN {db}.{sch}"
            
            with conn_pool.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                
                tables = cursor.fetchall()
                
                return {
                    "success": True,
                    "tables": [{"name": t[1], "database": t[2], "schema": t[3]} for t in tables]
                }
        except Exception as e:
            return {"error": str(e)}
    
    @mcp.tool()
    async def describe_table(table_name: str) -> dict:
        """Get column information for a table"""
        try:
            # Parse table name (could be fully qualified)
            parts = table_name.split('.')
            if len(parts) == 3:
                db, schema, table = parts
            elif len(parts) == 2:
                db = config['database']
                schema, table = parts
            else:
                db = config['database']
                schema = config['schema']
                table = table_name
            
            query = f"DESCRIBE TABLE {db}.{schema}.{table}"
            
            with conn_pool.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                
                columns = cursor.fetchall()
                
                return {
                    "success": True,
                    "columns": [
                        {
                            "name": col[0],
                            "type": col[1],
                            "nullable": col[2] == 'Y',
                            "default": col[3],
                            "comment": col[8] if len(col) > 8 else None
                        }
                        for col in columns
                    ]
                }
        except Exception as e:
            return {"error": str(e)}
    
    @mcp.tool()
    async def list_databases() -> dict:
        """List all databases"""
        try:
            with conn_pool.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SHOW DATABASES")
                
                databases = cursor.fetchall()
                
                return {
                    "success": True,
                    "databases": [{"name": db[1]} for db in databases]
                }
        except Exception as e:
            return {"error": str(e)}
    
    @mcp.tool()
    async def list_schemas(database: Optional[str] = None) -> dict:
        """List all schemas in a database"""
        try:
            db = database or config['database']
            
            with conn_pool.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f"SHOW SCHEMAS IN DATABASE {db}")
                
                schemas = cursor.fetchall()
                
                return {
                    "success": True,
                    "schemas": [{"name": s[1], "database": db} for s in schemas]
                }
        except Exception as e:
            return {"error": str(e)}
    
    # Create SSE app
    transport = SseServerTransport("/sse")
    
    async def handle_sse(request):
        async with transport.connect_sse(request) as streams:
            await mcp.run(
                transport_streams=streams,
                raise_exceptions=True
            )
    
    async def handle_messages(request):
        body = await request.body()
        return await transport.handle_post_message(request)
    
    async def health_check(request):
        return PlainTextResponse("OK")
    
    # Create Starlette app
    app = Starlette(
        routes=[
            Route("/sse", endpoint=handle_sse),
            Route("/messages", endpoint=handle_messages, methods=["POST"]),
            Route("/health", endpoint=health_check),
        ]
    )
    
    return app

class SnowflakeConnectionPool:
    """Simple connection pool for Snowflake"""
    def __init__(self, config: dict):
        self.config = config
        self._connection = None
    
    def get_connection(self):
        if not self._connection or self._connection.is_closed():
            self._connection = snowflake.connector.connect(
                user=self.config['user'],
                password=self.config['password'],
                account=self.config['account'],
                warehouse=self.config['warehouse'],
                database=self.config['database'],
                schema=self.config['schema'],
                role=self.config['role']
            )
        return self._connection
    
    def __enter__(self):
        return self.get_connection()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Keep connection open for reuse
        pass
