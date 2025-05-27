from sqlalchemy import Column, String, Integer, Boolean, DateTime, JSON
from pydantic import BaseModel, Field
from typing import Optional, Dict
from datetime import datetime
import uuid

from .database import Base

class SnowflakeConnection(Base):
    __tablename__ = "connections"
    
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name = Column(String, nullable=False)
    account = Column(String, nullable=False)
    user = Column(String, nullable=False)
    password = Column(String, nullable=False)  # Should be encrypted in production
    warehouse = Column(String, nullable=False)
    database = Column(String, nullable=False)
    schema = Column(String, nullable=False)
    role = Column(String, nullable=False)
    port = Column(Integer, unique=True, nullable=True)  # Unique port for SSE endpoint
    active = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    config = Column(JSON, default={})

class ConnectionCreate(BaseModel):
    name: str
    account: str
    user: str
    password: str
    warehouse: str
    database: str
    schema: str
    role: str
    config: Optional[Dict] = Field(default_factory=dict)

class ConnectionResponse(BaseModel):
    id: str
    name: str
    account: str
    sse_endpoint: str
    active: bool
    created_at: datetime
    
    class Config:
        from_attributes = True
