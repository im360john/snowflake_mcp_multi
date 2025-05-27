from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from contextlib import asynccontextmanager
from typing import List, Dict, Any, Optional
import os
import logging
import asyncio
import json
import snowflake.connector
from datetime import datetime
from sse_starlette import EventSourceResponse

from .database import SessionLocal, engine
from .models import Base, SnowflakeConnection, ConnectionCreate, ConnectionResponse
from .connection_manager import ConnectionManager

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
API_BASE_URL = os.getenv("API_BASE_URL", "https://snowflake-mcp-backend.onrender.com")

# Global connection manager
connection_manager = ConnectionManager()

# Store Snowflake connections
snowflake_connections: Dict[str, Any] = {}

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
            "row_count": len(data)
        }
    except Exception as e:
        logger.error(f"Query error: {e}")
        return {"error": str(e)}

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
        
        return {
            "success": True,
            "tables": [{"name": t[1], "database": t[0], "schema": t[2]} for t in tables]
        }
    except Exception as e:
        logger.error(f"List tables error: {e}")
        return {"error": str(e)}

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

# MCP SSE Endpoint - CRITICAL FIX HERE
@app.get("/mcp/{connection_id}/sse")
async def mcp_sse(connection_id: str, db: Session = Depends(get_db)):
    """SSE endpoint for MCP protocol"""
    # Get connection from database
    conn = db.query(SnowflakeConnection).filter(
        SnowflakeConnection.id == connection_id
    ).first()
    
    if not conn or not conn.active:
        raise HTTPException(status_code=404, detail="Connection not found or not active")
    
    logger.info(f"Establishing SSE connection for {connection_id}")
    
    # Create event generator
    async def event_generator():
        # CRITICAL: Send initial endpoint event
        # This tells the client where to POST messages
        yield {
            "event": "endpoint",
            "data": f"/mcp/{connection_id}/messages"
        }
        
        # Send a ready message
        yield {
            "event": "message", 
            "data": json.dumps({
                "jsonrpc": "2.0",
                "method": "notification",
                "params": {
                    "method": "server.ready",
                    "params": {}
                }
            })
        }
        
        # Keep connection alive with periodic pings
        try:
            while True:
                await asyncio.sleep(30)
                # Send ping as a comment (SSE format)
                yield {
                    "event": "ping",
                    "data": ""
                }
        except asyncio.CancelledError:
            logger.info(f"SSE connection closed for {connection_id}")
            pass
    
    return EventSourceResponse(event_generator())

@app.post("/mcp/{connection_id}/messages")
async def mcp_messages(
    connection_id: str, 
    request: Request,
    db: Session = Depends(get_db)
):
    """Handle MCP protocol messages"""
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
                "protocolVersion": "1.0.0",
                "capabilities": {
                    "tools": {
                        "listChanged": False
                    }
                }
            }
        
        elif method == "tools/list":
            result = {
                "tools": TOOLS
            }
        
        elif method == "tools/call":
            tool_name = params.get("name")
            arguments = params.get("arguments", {})
            
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
            
            result = {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(tool_result)
                    }
                ]
            }
        
        else:
            raise ValueError(f"Unknown method: {method}")
        
        # Return JSON-RPC response
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": result
        }
    
    except Exception as e:
        logger.error(f"Error processing MCP message: {e}")
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {
                "code": -32603,
                "message": str(e)
            }
        }
