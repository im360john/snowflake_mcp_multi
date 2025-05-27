from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from contextlib import asynccontextmanager
from typing import List
import os
import logging
import asyncio
import json
import httpx
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

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting up...")
    # Create tables
    logger.info("Creating database tables...")
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables created successfully")
    
    yield
    
    # Shutdown - stop all MCP servers
    logger.info("Shutting down...")
    for conn_id in list(connection_manager.active_servers.keys()):
        await connection_manager.stop_connection(conn_id)

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
    
    # Start MCP server
    try:
        port = await connection_manager.start_connection(
            db_connection.id,
            connection.dict()
        )
        
        # Update connection with port
        db_connection.port = port
        db_connection.active = True
        db.commit()
        
        logger.info(f"Connection {connection.name} created successfully on port {port}")
        
        return ConnectionResponse(
            id=db_connection.id,
            name=db_connection.name,
            account=db_connection.account,
            sse_endpoint=f"{API_BASE_URL}/mcp/{db_connection.id}/sse",
            active=db_connection.active,
            created_at=db_connection.created_at
        )
    except Exception as e:
        logger.error(f"Failed to start MCP server: {e}")
        db.delete(db_connection)
        db.commit()
        raise HTTPException(status_code=500, detail=str(e))

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
    
    # Stop MCP server
    await connection_manager.stop_connection(connection_id)
    
    # Delete from database
    db.delete(conn)
    db.commit()
    
    logger.info(f"Connection {connection_id} deleted successfully")
    return {"message": "Connection deleted"}

@app.post("/connections/{connection_id}/start")
async def start_connection(
    connection_id: str,
    db: Session = Depends(get_db)
):
    """Start a connection's MCP server"""
    logger.info(f"Starting connection: {connection_id}")
    
    conn = db.query(SnowflakeConnection).filter(
        SnowflakeConnection.id == connection_id
    ).first()
    
    if not conn:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    if conn.active:
        return {"message": "Connection already active"}
    
    try:
        port = await connection_manager.start_connection(
            conn.id,
            {
                "name": conn.name,
                "account": conn.account,
                "user": conn.user,
                "password": conn.password,
                "warehouse": conn.warehouse,
                "database": conn.database,
                "schema": conn.schema,
                "role": conn.role
            }
        )
        
        conn.port = port
        conn.active = True
        db.commit()
        
        logger.info(f"Connection {connection_id} started on port {port}")
        
        return {
            "message": "Connection started",
            "sse_endpoint": f"{API_BASE_URL}/mcp/{connection_id}/sse"
        }
    except Exception as e:
        logger.error(f"Failed to start connection: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/connections/{connection_id}/stop")
async def stop_connection(
    connection_id: str,
    db: Session = Depends(get_db)
):
    """Stop a connection's MCP server"""
    logger.info(f"Stopping connection: {connection_id}")
    
    conn = db.query(SnowflakeConnection).filter(
        SnowflakeConnection.id == connection_id
    ).first()
    
    if not conn:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    await connection_manager.stop_connection(connection_id)
    
    conn.active = False
    db.commit()
    
    logger.info(f"Connection {connection_id} stopped successfully")
    return {"message": "Connection stopped"}

# MCP Proxy Endpoints
@app.get("/mcp/{connection_id}/sse")
async def proxy_sse(connection_id: str, db: Session = Depends(get_db)):
    """Proxy SSE connection to the appropriate MCP server"""
    # Get connection from database
    conn = db.query(SnowflakeConnection).filter(
        SnowflakeConnection.id == connection_id
    ).first()
    
    if not conn or not conn.active:
        raise HTTPException(status_code=404, detail="Connection not found or not active")
    
    logger.info(f"Establishing SSE connection for {connection_id}")
    
    # Create event generator
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
            logger.info(f"SSE connection closed for {connection_id}")
            pass
    
    return EventSourceResponse(event_generator())

@app.post("/mcp/{connection_id}/messages")
async def proxy_messages(
    connection_id: str, 
    request: dict,
    db: Session = Depends(get_db)
):
    """Proxy messages to the appropriate MCP server"""
    # Get connection from database
    conn = db.query(SnowflakeConnection).filter(
        SnowflakeConnection.id == connection_id
    ).first()
    
    if not conn or not conn.active:
        raise HTTPException(status_code=404, detail="Connection not found or not active")
    
    logger.info(f"Proxying message to {connection_id}: {request.get('method')}")
    
    # Forward to the internal MCP server
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                f"http://localhost:{conn.port}/messages",
                json=request,
                timeout=30.0
            )
            return response.json()
        except httpx.ConnectError:
            logger.error(f"Failed to connect to internal MCP server on port {conn.port}")
            raise HTTPException(
                status_code=503, 
                detail="Internal MCP server not reachable. Try restarting the connection."
            )
        except Exception as e:
            logger.error(f"Error proxying message: {e}")
            raise HTTPException(status_code=500, detail=str(e))
