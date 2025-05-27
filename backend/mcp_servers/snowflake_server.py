from starlette.applications import Starlette
from starlette.routing import Route
from starlette.responses import PlainTextResponse, JSONResponse
from sse_starlette import EventSourceResponse
import snowflake.connector
from typing import Optional, Dict, Any
import json
import asyncio
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

def create_snowflake_mcp_server(config: dict, connection_id: str) -> Starlette:
    """Create a Snowflake MCP server with SSE transport"""
    
    # Connection pool for Snowflake
    conn_pool = SnowflakeConnectionPool(config)
    
    # Tool definitions
    tools = [
        {
            "name": "read_query",
            "description": "Execute a SELECT query on Snowflake",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "The SELECT SQL query to execute"}
                },
                "required": ["query"]
            }
        },
        {
            "name": "list_tables",
            "description": "List tables in the specified database and schema",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "database": {"type": "string", "description": "Database name"},
                    "schema": {"type": "string", "description": "Schema name"}
                }
            }
        },
        {
            "name": "describe_table",
            "description": "Get column information for a table",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Table name (can be fully qualified)"}
                },
                "required": ["table_name"]
            }
        },
        {
            "name": "list_databases",
            "description": "List all databases",
            "inputSchema": {
                "type": "object",
                "properties": {}
            }
        },
        {
            "name": "list_schemas",
            "description": "List all schemas in a database",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "database": {"type": "string", "description": "Database name"}
                }
            }
        }
    ]
    
    # Tool implementations
    async def read_query(query: str) -> Dict[str, Any]:
        """Execute a SELECT query on Snowflake"""
        if not query.strip().upper().startswith('SELECT'):
            return {"error": "Only SELECT queries are allowed"}
        
        try:
            conn = conn_pool.get_connection()
            cursor = conn.cursor()
            cursor.execute(query)
            
            # Get column names
            columns = [desc[0] for desc in cursor.description]
            
            # Fetch results
            results = cursor.fetchall()
            
            # Convert to list of dicts
            data = [dict(zip(columns, [str(v) if isinstance(v, (datetime, bytes)) else v for v in row])) for row in results]
            
            return {
                "success": True,
                "data": data,
                "row_count": len(data)
            }
        except Exception as e:
            logger.error(f"Query error: {e}")
            return {"error": str(e)}
        finally:
            cursor.close()
    
    async def list_tables(database: Optional[str] = None, schema: Optional[str] = None) -> Dict[str, Any]:
        """List tables in the specified database and schema"""
        try:
            db = database or config['database']
            sch = schema or config['schema']
            
            query = f"SHOW TABLES IN {db}.{sch}"
            
            conn = conn_pool.get_connection()
            cursor = conn.cursor()
            cursor.execute(query)
            
            tables = cursor.fetchall()
            
            return {
                "success": True,
                "tables": [{"name": t[1], "database": t[0], "schema": t[2]} for t in tables]
            }
        except Exception as e:
            logger.error(f"List tables error: {e}")
            return {"error": str(e)}
        finally:
            cursor.close()
    
    async def describe_table(table_name: str) -> Dict[str, Any]:
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
            
            conn = conn_pool.get_connection()
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
            logger.error(f"Describe table error: {e}")
            return {"error": str(e)}
        finally:
            cursor.close()
    
    async def list_databases() -> Dict[str, Any]:
        """List all databases"""
        try:
            conn = conn_pool.get_connection()
            cursor = conn.cursor()
            cursor.execute("SHOW DATABASES")
            
            databases = cursor.fetchall()
            
            return {
                "success": True,
                "databases": [{"name": db[1]} for db in databases]
            }
        except Exception as e:
            logger.error(f"List databases error: {e}")
            return {"error": str(e)}
        finally:
            cursor.close()
    
    async def list_schemas(database: Optional[str] = None) -> Dict[str, Any]:
        """List all schemas in a database"""
        try:
            db = database or config['database']
            
            conn = conn_pool.get_connection()
            cursor = conn.cursor()
            cursor.execute(f"SHOW SCHEMAS IN DATABASE {db}")
            
            schemas = cursor.fetchall()
            
            return {
                "success": True,
                "schemas": [{"name": s[1], "database": db} for s in schemas]
            }
        except Exception as e:
            logger.error(f"List schemas error: {e}")
            return {"error": str(e)}
        finally:
            cursor.close()
    
    # Map tool names to functions
    tool_functions = {
        "read_query": read_query,
        "list_tables": list_tables,
        "describe_table": describe_table,
        "list_databases": list_databases,
        "list_schemas": list_schemas
    }
    
    # SSE endpoint handler
    async def handle_sse(request):
        """Handle SSE connections"""
        async def event_generator():
            # Send initial connection event
            yield {
                "event": "open",
                "data": json.dumps({
                    "protocol": "mcp",
                    "version": "1.0.0",
                    "capabilities": {
                        "tools": True
                    }
                })
            }
            
            # Keep connection alive
            try:
                while True:
                    await asyncio.sleep(30)
                    yield {
                        "event": "ping",
                        "data": json.dumps({"type": "ping"})
                    }
            except asyncio.CancelledError:
                pass
        
        return EventSourceResponse(event_generator())
    
    # Message handler
    async def handle_messages(request):
        """Handle MCP protocol messages"""
        try:
            body = await request.json()
            method = body.get("method")
            params = body.get("params", {})
            
            # Handle different MCP methods
            if method == "initialize":
                return JSONResponse({
                    "protocolVersion": "1.0.0",
                    "capabilities": {
                        "tools": {
                            "listChanged": False
                        }
                    }
                })
            
            elif method == "tools/list":
                return JSONResponse({
                    "tools": tools
                })
            
            elif method == "tools/call":
                tool_name = params.get("name")
                arguments = params.get("arguments", {})
                
                if tool_name not in tool_functions:
                    return JSONResponse(
                        {"error": f"Unknown tool: {tool_name}"},
                        status_code=400
                    )
                
                # Execute the tool
                result = await tool_functions[tool_name](**arguments)
                
                return JSONResponse({
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(result)
                        }
                    ]
                })
            
            else:
                return JSONResponse(
                    {"error": f"Unknown method: {method}"},
                    status_code=400
                )
            
        except Exception as e:
            logger.error(f"Error handling message: {e}")
            return JSONResponse(
                {"error": str(e)},
                status_code=500
            )
    
    async def health_check(request):
        """Health check endpoint"""
        try:
            # Test database connection
            conn = conn_pool.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            return PlainTextResponse("OK")
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return PlainTextResponse(f"ERROR: {str(e)}", status_code=500)
    
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
        """Get or create a Snowflake connection"""
        try:
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
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            raise
