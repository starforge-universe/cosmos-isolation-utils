"""
Base class for subcommand executors.

This module provides a common superclass for all subcommand implementations,
ensuring consistent initialization patterns and client management.
"""

from typing import Optional

from .config import DatabaseConfig
from .cosmos_client import CosmosDBClient
from .logging_utils import log_info


class BaseSubcommandExecutor:
    """
    Base class for all subcommand executors.

    This class provides common functionality for subcommand implementations:
    - Database configuration storage
    - Client initialization pattern
    - Common interface structure
    """

    def __init__(self, db_config: DatabaseConfig):
        """
        Initialize the base executor with database configuration.

        Args:
            db_config: Database connection configuration
        """
        self._db_config = db_config
        self._client: Optional[CosmosDBClient] = None

    @property
    def db_config(self) -> DatabaseConfig:
        """Get the database configuration."""
        return self._db_config

    @property
    def client(self) -> Optional[CosmosDBClient]:
        """Get the CosmosDB client (may be None if not initialized)."""
        return self._client

    def _initialize_client(self) -> None:
        """
        Initialize the CosmosDB client.

        This method creates a new CosmosDBClient instance using the stored
        database configuration. It should be called before any operations
        that require the client.
        """
        self._client = CosmosDBClient(self._db_config)

    def _display_connection_info(self) -> None:
        """
        Display connection configuration information.

        This method shows the endpoint and allow_insecure settings,
        but does not display the database name for security reasons.
        """
        log_info(f"Endpoint: {self._db_config.endpoint}")
        log_info(f"Allow insecure: {self._db_config.allow_insecure}")
