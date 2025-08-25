"""
Configuration objects for CosmosDB isolation utilities.

This module provides dataclasses for grouping related parameters,
reducing method argument counts and improving code readability.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class DatabaseConfig:
    """Database connection configuration."""
    endpoint: str
    key: str
    database: str
    allow_insecure: bool = False


@dataclass
class UploadConfig:
    """Upload command configuration."""
    input_file: str
    batch_size: int = 100
    upsert: bool = False
    dry_run: bool = False
    force: bool = False
    create_containers: bool = False
    containers: Optional[str] = None


@dataclass
class DumpConfig:
    """Dump command configuration."""
    output_dir: str
    containers: Optional[str] = None
    batch_size: int = 100
    pretty: bool = False
    list_containers: bool = False


@dataclass
class DeleteConfig:
    """Delete command configuration."""
    force: bool = False
    list_only: bool = False


@dataclass
class StatusConfig:
    """Status command configuration."""
    containers: Optional[str] = None
    detailed: bool = False


@dataclass
class ConnectionConfig:
    """Connection test configuration."""
    create_database: bool = False
    force: bool = False
