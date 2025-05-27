import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    database_url: str = "postgresql://postgres:postgres@localhost:5432/snowflake_mcp"
    secret_key: str = "dev-secret-key"
    port_range_start: int = 8100
    port_range_end: int = 8200
    
    class Config:
        env_file = ".env"

settings = Settings()
