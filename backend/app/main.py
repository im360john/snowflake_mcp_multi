from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.responses import StreamingResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from contextlib import asynccontextmanager
from typing import List, Dict, Any, Optional, AsyncGenerator
import os
import logging
import asyncio
import json
import snowflake.connector
from datetime import datetime
import uuid
import io

from .database import SessionLocal, engine
from .models import Base, SnowflakeConnection, ConnectionCreate, ConnectionResponse

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
API_BASE_URL = os.getenv("API_BASE_URL", "https://snowflake-mcp-backend.onrender.com")

# Store Snowflake connections
snowflake_connections: Dict[str, Any] = {}

# Store active SSE connections
active_sse_connections: Dict[str, Dict[str, Any]] = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting up...")
    # Create tables
    logger.info("Creating database tables...")
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables created successfully")
    
    yield
    
    # Shutdown - close all Snowflake connections
    logger.info("Shutting down...")
    for conn_id in list(snowflake_connections.keys()):
        try:
            snowflake_connections[conn_id].close()
        except:
            pass

app = FastAPI(title="Snowflake MCP Manager", lifespan=lifespan)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_snowflake_connection(config: dict) -> snowflake.connector.SnowflakeConnection:
    """Get or create a Snowflake connection"""
    conn_id = config.get('id', 'default')
    
    if conn_id not in snowflake_connections or snowflake_connections[conn_id].is_closed():
        logger.info(f"Creating new Snowflake connection for {conn_id}")
        snowflake_connections[conn_id] = snowflake.connector.connect(
            user=config['user'],
            password=config['password'],
            account=config['account'],
            warehouse=config['warehouse'],
            database=config['database'],
            schema=config['schema'],
            role=config['role']
        )
    
    return snowflake_connections[conn_id]

# MCP Tool implementations
async def read_query(query: str, config: dict) -> Dict[str, Any]:
    """Execute a SELECT query on Snowflake"""
    if not query.strip().upper().startswith('SELECT'):
        return {"error": "Only SELECT queries are allowed"}
    
    try:
        conn = get_snowflake_connection(config)
        cursor = conn.cursor()
        
        # Log the query being executed
        logger.info(f"Executing query: {query}")
        
        # Ensure we're using the correct database and schema context
        cursor.execute(f"USE DATABASE {config['database']}")
        cursor.execute(f"USE SCHEMA {config['schema']}")
        
        # Execute the actual query
        cursor.execute(query)
        
        # Get column names
        columns = [desc[0] for desc in cursor.description]
        
        # Fetch results
        results = cursor.fetchall()
        
        # Convert to list of dicts
        data = [dict(zip(columns, [str(v) if isinstance(v, (datetime, bytes)) else v for v in row])) for row in results]
        
        cursor.close()
        
        return {
            "success": True,
            "data": data,
            "row_count": len(data),
            "query": query,
            "database": config['database'],
            "schema": config['schema']
        }
    except Exception as e:
        logger.error(f"Query error: {e}")
        error_msg = str(e)
        
        # Provide helpful error message if it's a schema issue
        if "does not exist or not authorized" in error_msg:
            return {
                "error": error_msg,
                "hint": f"Make sure the table exists in {config['database']}.{config['schema']}. You may need to use the fully qualified name: {config['database']}.{config['schema']}.table_name"
            }
        
        return {"error": error_msg}

async def list_tables(database: Optional[str], schema: Optional[str], config: dict) -> Dict[str, Any]:
    """List tables in the specified database and schema"""
    try:
        db = database or config['database']
        sch = schema or config['schema']
        
        query = f"SHOW TABLES IN {db}.{sch}"
        
        conn = get_snowflake_connection(config)
        cursor = conn.cursor()
        cursor.execute(query)
        
        tables = cursor.fetchall()
        cursor.close()
        
        # Return a simple list of table names for better compatibility
        table_list = []
        for t in tables:
            table_list.append({
                "name": t[1],
                "database": t[0],
                "schema": t[2] if len(t) > 2 else sch,
                "type": t[3] if len(t) > 3 else "TABLE"
            })
        
        return {
            "success": True,
            "tables": table_list,
            "count": len(table_list),
            "database": db,
            "schema": sch
        }
    except Exception as e:
        logger.error(f"List tables error: {e}")
        return {"success": False, "error": str(e)}

async def describe_table(table_name: str, config: dict) -> Dict[str, Any]:
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
        
        conn = get_snowflake_connection(config)
        cursor = conn.cursor()
        cursor.execute(query)
        
        columns = cursor.fetchall()
        cursor.close()
        
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

async def list_databases(config: dict) -> Dict[str, Any]:
    """List all databases"""
    try:
        conn = get_snowflake_connection(config)
        cursor = conn.cursor()
        cursor.execute("SHOW DATABASES")
        
        databases = cursor.fetchall()
        cursor.close()
        
        return {
            "success": True,
            "databases": [{"name": db[1]} for db in databases]
        }
    except Exception as e:
        logger.error(f"List databases error: {e}")
        return {"error": str(e)}

async def list_schemas(database: Optional[str], config: dict) -> Dict[str, Any]:
    """List all schemas in a database"""
    try:
        db = database or config['database']
        
        conn = get_snowflake_connection(config)
        cursor = conn.cursor()
        cursor.execute(f"SHOW SCHEMAS IN DATABASE {db}")
        
        schemas = cursor.fetchall()
        cursor.close()
        
        return {
            "success": True,
            "schemas": [{"name": s[1], "database": db} for s in schemas]
        }
    except Exception as e:
        logger.error(f"List schemas error: {e}")
        return {"error": str(e)}

# Tool definitions
TOOLS = [
    {
        "name": "read_query",
        "description": "Execute a SELECT query on Snowflake database. When querying tables, use just the table name (e.g., 'SELECT * FROM ticketline_sales') unless you need to query from a different schema. The current schema context is automatically set.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string", 
                    "description": "The SELECT SQL query to execute. Use simple table names like 'ticketline_sales' not schema names."
                }
            },
            "required": ["query"]
        }
    },
    {
        "name": "list_tables",
        "description": "List all tables available in the Snowflake database and schema. Call this first to see what tables are available to query.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "database": {
                    "type": "string", 
                    "description": "Database name (optional, uses configured default)"
                },
                "schema": {
                    "type": "string", 
                    "description": "Schema name (optional, uses configured default)"
                }
            },
            "required": []
        }
    },
    {
        "name": "describe_table",
        "description": "Get column information for a specific table. Use just the table name unless querying from a different schema.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string", 
                    "description": "Table name (e.g., 'ticketline_sales'). Can include schema if needed (e.g., 'other_schema.table_name')"
                }
            },
            "required": ["table_name"]
        }
    },
    {
        "name": "list_databases",
        "description": "List all available databases in Snowflake",
        "inputSchema": {
            "type": "object",
            "properties": {},
            "required": []
        }
    },
    {
        "name": "list_schemas",
        "description": "List all schemas in a specific database",
        "inputSchema": {
            "type": "object",
            "properties": {
                "database": {
                    "type": "string", 
                    "description": "Database name (optional, uses configured default)"
                }
            },
            "required": []
        }
    }
]

@app.get("/")
async def root():
    return {"message": "Snowflake MCP Multi-Connection Server API", "status": "healthy"}

@app.get("/health")
async def health_check():
    try:
        # Test database connection
        db = SessionLocal()
        db.execute("SELECT 1")
        db.close()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {"status": "unhealthy", "error": str(e)}

@app.post("/connections", response_model=ConnectionResponse)
async def create_connection(
    connection: ConnectionCreate,
    db: Session = Depends(get_db)
):
    """Create a new Snowflake connection"""
    logger.info(f"Creating connection: {connection.name}")
    
    # Create database entry
    db_connection = SnowflakeConnection(**connection.dict())
    db.add(db_connection)
    db.commit()
    db.refresh(db_connection)
    
    # Test the connection
    try:
        config = connection.dict()
        config['id'] = db_connection.id
        conn = get_snowflake_connection(config)
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_VERSION()")
        cursor.fetchone()
        cursor.close()
        
        db_connection.active = True
        db.commit()
        
        logger.info(f"Connection {connection.name} created successfully")
        
        return ConnectionResponse(
            id=db_connection.id,
            name=db_connection.name,
            account=db_connection.account,
            sse_endpoint=f"{API_BASE_URL}/mcp/{db_connection.id}/sse",
            active=db_connection.active,
            created_at=db_connection.created_at
        )
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {e}")
        db.delete(db_connection)
        db.commit()
        raise HTTPException(status_code=400, detail=f"Failed to connect to Snowflake: {str(e)}")

@app.get("/connections", response_model=List[ConnectionResponse])
async def list_connections(db: Session = Depends(get_db)):
    """List all connections"""
    logger.info("Listing connections")
    connections = db.query(SnowflakeConnection).all()
    
    return [
        ConnectionResponse(
            id=conn.id,
            name=conn.name,
            account=conn.account,
            sse_endpoint=f"{API_BASE_URL}/mcp/{conn.id}/sse" if conn.active else "",
            active=conn.active,
            created_at=conn.created_at
        )
        for conn in connections
    ]

@app.delete("/connections/{connection_id}")
async def delete_connection(
    connection_id: str,
    db: Session = Depends(get_db)
):
    """Delete a connection"""
    logger.info(f"Deleting connection: {connection_id}")
    
    conn = db.query(SnowflakeConnection).filter(
        SnowflakeConnection.id == connection_id
    ).first()
    
    if not conn:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    # Close Snowflake connection if exists
    if connection_id in snowflake_connections:
        try:
            snowflake_connections[connection_id].close()
            del snowflake_connections[connection_id]
        except:
            pass
    
    # Delete from database
    db.delete(conn)
    db.commit()
    
    logger.info(f"Connection {connection_id} deleted successfully")
    return {"message": "Connection deleted"}

# Store message queues for each SSE session
message_queues: Dict[str, asyncio.Queue] = {}

# MCP SSE Endpoint - Optimized for LibreChat SSE transport
@app.get("/mcp/{connection_id}/sse")
async def mcp_sse(connection_id: str, request: Request, db: Session = Depends(get_db)):
    """SSE endpoint for MCP protocol - optimized for LibreChat SSE transport"""
    logger.info(f"SSE endpoint called for connection: {connection_id}")
    logger.info(f"User-Agent: {request.headers.get('user-agent', 'Unknown')}")
    
    # Get connection from database
    conn = db.query(SnowflakeConnection).filter(
        SnowflakeConnection.id == connection_id
    ).first()
    
    if not conn or not conn.active:
        raise HTTPException(status_code=404, detail="Connection not found or not active")
    
    # Generate unique session ID for this SSE connection
    session_id = str(uuid.uuid4())
    
    # Create a message queue for this session
    message_queue = asyncio.Queue()
    message_queues[connection_id] = message_queue
    
    # Get the full URL for the messages endpoint
    base_url = str(request.url).replace('/sse', '')
    messages_url = f"{base_url}/messages"
    
    async def event_stream() -> AsyncGenerator[bytes, None]:
        """Generate SSE events for LibreChat SSE transport"""
        # Track this connection
        active_sse_connections[session_id] = {
            "connection_id": connection_id,
            "started_at": datetime.now(),
            "last_heartbeat": datetime.now(),
            "message_queue": message_queue
        }
        
        try:
            logger.info(f"Starting SSE stream for connection: {connection_id}, session: {session_id}")
            
            # Send the endpoint URL as required by MCP SSE transport
            yield f"event: endpoint\ndata: {messages_url}\n\n".encode('utf-8')
            logger.info(f"Sent endpoint URL: {messages_url}")
            
            # Start tasks for heartbeat and message handling
            async def heartbeat_sender():
                """Send periodic heartbeats"""
                counter = 0
                while session_id in active_sse_connections:
                    await asyncio.sleep(30)
                    counter += 1
                    # LibreChat doesn't need frequent heartbeats
                    logger.debug(f"Heartbeat {counter} for session {session_id}")
            
            heartbeat_task = asyncio.create_task(heartbeat_sender())
            
            # Main loop to send queued messages
            while True:
                try:
                    # Wait for messages with a timeout
                    message = await asyncio.wait_for(message_queue.get(), timeout=1.0)
                    
                    # Send the message through SSE
                    message_str = json.dumps(message)
                    yield f"event: message\ndata: {message_str}\n\n".encode('utf-8')
                    logger.debug(f"Sent message through SSE: {message_str[:100]}...")
                    
                except asyncio.TimeoutError:
                    # No message, check if still connected
                    if await request.is_disconnected():
                        logger.info(f"Client disconnected for session: {session_id}")
                        break
                    # Send keep-alive comment
                    yield b": keepalive\n\n"
                    
        except asyncio.CancelledError:
            logger.info(f"SSE connection cancelled for session: {session_id}")
            raise
        except Exception as e:
            logger.error(f"Error in SSE stream for session {session_id}: {e}", exc_info=True)
            raise
        finally:
            # Cancel heartbeat task
            if 'heartbeat_task' in locals():
                heartbeat_task.cancel()
            # Clean up
            message_queues.pop(connection_id, None)
            active_sse_connections.pop(session_id, None)
            logger.info(f"SSE connection closed for session: {session_id}")
    
    # Return streaming response
    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        }
    )

# Simple SSE test endpoint
@app.get("/test-sse")
async def test_sse():
    """Simple SSE test endpoint for debugging"""
    async def generate():
        yield b"retry: 1000\n\n"
        yield b"data: SSE connection established\n\n"
        yield b": This is a comment to test keep-alive\n\n"
        
        for i in range(5):
            yield f"data: Count {i}\n\n".encode('utf-8')
            await asyncio.sleep(1)
        
        yield b"data: Test complete\n\n"
    
    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        }
    )

# Add endpoint to check active SSE connections
@app.get("/mcp/sse-status")
async def sse_status():
    """Check status of active SSE connections"""
    return {
        "active_connections": len(active_sse_connections),
        "connections": [
            {
                "session_id": session_id,
                "connection_id": info["connection_id"],
                "started_at": info["started_at"].isoformat(),
                "last_heartbeat": info["last_heartbeat"].isoformat(),
                "uptime_seconds": (datetime.now() - info["started_at"]).total_seconds()
            }
            for session_id, info in active_sse_connections.items()
        ]
    }

@app.post("/mcp/{connection_id}/messages")
async def mcp_messages(
    connection_id: str, 
    request: Request,
    db: Session = Depends(get_db)
):
    """Handle MCP protocol messages - sends responses through SSE stream"""
    # Get connection from database
    conn = db.query(SnowflakeConnection).filter(
        SnowflakeConnection.id == connection_id
    ).first()
    
    if not conn or not conn.active:
        raise HTTPException(status_code=404, detail="Connection not found or not active")
    
    # Parse request body
    body = await request.json()
    method = body.get("method")
    params = body.get("params", {})
    request_id = body.get("id")
    
    logger.info(f"Processing MCP message for {connection_id}: {method}")
    logger.debug(f"Request body: {body}")
    
    # Get the message queue for this connection
    message_queue = message_queues.get(connection_id)
    if not message_queue:
        logger.error(f"No message queue found for connection {connection_id}")
        return Response(status_code=503)  # Service Unavailable
    
    # Build config for Snowflake connection
    config = {
        'id': conn.id,
        'user': conn.user,
        'password': conn.password,
        'account': conn.account,
        'warehouse': conn.warehouse,
        'database': conn.database,
        'schema': conn.schema,
        'role': conn.role
    }
    
    try:
        # Handle different MCP methods
        if method == "initialize":
            result = {
                "protocolVersion": "2025-03-26",  # Match LibreChat's version
                "capabilities": {
                    "tools": {}
                },
                "serverInfo": {
                    "name": f"snowflake-{conn.name}",
                    "version": "1.0.0"
                }
            }
        
        elif method == "tools/list":
            # Return tools in the correct format
            result = {
                "tools": TOOLS
            }
            logger.info(f"Returning {len(TOOLS)} tools")
        
        elif method == "tools/call":
            tool_name = params.get("name")
            arguments = params.get("arguments", {})
            
            logger.info(f"Calling tool: {tool_name} with arguments: {arguments}")
            
            # Execute the appropriate tool
            if tool_name == "read_query":
                tool_result = await read_query(arguments.get("query"), config)
            elif tool_name == "list_tables":
                tool_result = await list_tables(
                    arguments.get("database"),
                    arguments.get("schema"),
                    config
                )
            elif tool_name == "describe_table":
                tool_result = await describe_table(arguments.get("table_name"), config)
            elif tool_name == "list_databases":
                tool_result = await list_databases(config)
            elif tool_name == "list_schemas":
                tool_result = await list_schemas(arguments.get("database"), config)
            else:
                raise ValueError(f"Unknown tool: {tool_name}")
            
            # Format the result properly
            result = {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(tool_result, indent=2)
                    }
                ]
            }
        
        elif method == "notifications/cancelled":
            # Handle notification cancellation - just acknowledge it
            logger.info(f"Received cancellation notification for request: {params.get('requestId')}")
            # For notifications, we don't send a response
            return Response(status_code=202)  # Accepted
        
        elif method == "ping":
            # Handle ping requests - LibreChat expects empty result
            result = {}
        
        else:
            # Send error through SSE for unknown methods
            if request_id is not None:
                error_response = {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {
                        "code": -32601,
                        "message": f"Method not found: {method}"
                    }
                }
                await message_queue.put(error_response)
            return Response(status_code=202)
        
        # Send successful response through SSE stream
        if request_id is not None:
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": result
            }
            
            # Put the response in the queue to be sent through SSE
            await message_queue.put(response)
            logger.debug(f"Queued response for SSE: {response}")
        
        # Return 202 Accepted to indicate the request was received
        return Response(status_code=202)
    
    except Exception as e:
        logger.error(f"Error processing MCP message: {e}", exc_info=True)
        if request_id is not None:
            error_response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -32603,
                    "message": str(e)
                }
            }
            await message_queue.put(error_response)
        return Response(status_code=202)
