"""Configuration management for the embedding service."""

from typing import Literal, Optional
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Service configuration
    service_name: str = "vectorsync-embedding-service"
    service_version: str = "1.0.0"
    host: str = "0.0.0.0"
    port: int = 8000
    log_level: str = "INFO"

    # Provider configuration
    default_provider: Literal["self_hosted", "managed"] = "self_hosted"
    default_model: str = "all-MiniLM-L6-v2"

    # Self-hosted provider settings
    self_hosted_device: str = "cpu"  # cpu, cuda, mps
    self_hosted_max_workers: int = 4

    # Managed provider settings
    managed_api_url: Optional[str] = None
    managed_api_key: Optional[str] = None
    managed_max_concurrent: int = 10
    managed_timeout: float = 30.0
    managed_max_retries: int = 3

    # Batching configuration
    max_batch_size: int = 64
    enable_token_batching: bool = False

    # Performance settings
    enable_request_logging: bool = True
    enable_metrics: bool = True

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


# Global settings instance
settings = Settings()
