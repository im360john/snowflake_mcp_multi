from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from contextlib import asynccontextmanager
from typing import List
import os
import logging

from .database import SessionLocal, engine
from .models import Base, SnowflakeConnection, ConnectionCreate, ConnectionResponse
from .connection_manager import ConnectionManager

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
            sse_endpoint=f"http://localhost:{port}/sse",
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
            sse_endpoint=f"http://localhost:{conn.port}/sse" if conn.port else "",
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
            "sse_endpoint": f"http://localhost:{port}/sse"
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
