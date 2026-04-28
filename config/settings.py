"""
Centralized Configuration Management

Engineering Standard: Use Pydantic Settings for type-safe configuration
with validation, environment variable loading, and sensible defaults.
"""

import os
from typing import Optional
from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class WMATASettings(BaseSettings):
    """WMATA API configuration."""
    
    api_key: str = Field(..., description="WMATA API key")
    timeout: int = Field(default=30, ge=1, le=120)
    max_retries: int = Field(default=3, ge=0, le=10)
    base_url: str = Field(
        default="https://api.wmata.com/StationPrediction.svc/json/GetPrediction"
    )
    
    model_config = SettingsConfigDict(
        env_prefix="WMATA_",
        extra="ignore"
    )


class DatabaseSettings(BaseSettings):
    """PostgreSQL database configuration."""
    
    host: str = Field(default="localhost")
    port: int = Field(default=5432, ge=1, le=65535)
    name: str = Field(default="wmata_etl", alias="db")
    user: str = Field(default="postgres")
    password: str = Field(default="")
    
    # Alternative: full connection string
    url: Optional[str] = Field(default=None, alias="database_url")
    
    model_config = SettingsConfigDict(
        env_prefix="POSTGRES_",
        extra="ignore"
    )
    
    @property
    def connection_string(self) -> str:
        """Generate SQLAlchemy connection string."""
        if self.url:
            return self.url
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"


class PipelineSettings(BaseSettings):
    """Pipeline execution configuration."""
    
    poll_interval_minutes: int = Field(default=5, ge=1, le=60)
    batch_size: int = Field(default=1000, ge=1, le=10000)
    log_level: str = Field(default="INFO")
    environment: str = Field(default="development")
    
    model_config = SettingsConfigDict(extra="ignore")
    
    @field_validator("environment")
    @classmethod
    def validate_environment(cls, v: str) -> str:
        allowed = {"development", "staging", "production"}
        if v.lower() not in allowed:
            raise ValueError(f"environment must be one of {allowed}")
        return v.lower()
    
    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if v.upper() not in allowed:
            raise ValueError(f"log_level must be one of {allowed}")
        return v.upper()


class Settings(BaseSettings):
    """
    Main settings class that aggregates all configuration.
    
    Usage:
        from config.settings import get_settings
        settings = get_settings()
        print(settings.wmata.api_key)
        print(settings.database.connection_string)
    """
    
    wmata: WMATASettings = Field(default_factory=WMATASettings)
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    pipeline: PipelineSettings = Field(default_factory=PipelineSettings)
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )


@lru_cache()
def get_settings() -> Settings:
    """
    Get cached settings instance.
    
    Uses LRU cache to avoid re-reading .env file on every call.
    Call get_settings.cache_clear() to reload settings.
    """
    return Settings()


# Convenience function for quick access
def get_database_url() -> str:
    """Get database connection string."""
    return get_settings().database.connection_string


def get_wmata_api_key() -> str:
    """Get WMATA API key."""
    return get_settings().wmata.api_key


def is_production() -> bool:
    """Check if running in production environment."""
    return get_settings().pipeline.environment == "production"
