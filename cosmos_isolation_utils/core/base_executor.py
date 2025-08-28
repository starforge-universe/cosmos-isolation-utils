"""
Base class for subcommand executors.

This module provides a common superclass for all subcommand implementations,
ensuring consistent initialization patterns and client management.
"""

from typing import Optional
from azure.cosmos import PartitionKey

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

    def create_container(self, container_name: str, partition_key_paths: list[str]) -> None:
        """
        Create a container in the current database.
        
        This method provides a common interface for all subclasses to create
        containers, ensuring consistent behavior across the application.
        
        Args:
            container_name: Name of the container to create
            partition_key_paths: List of strings representing partition key paths, 
                                or a single string for a single path
        """
        if self._client is None:
            raise RuntimeError("Client not initialized. Call _initialize_client() first.")
        
        # Convert string to list if needed
        if isinstance(partition_key_paths, str):
            partition_key_paths = [partition_key_paths]
        elif not isinstance(partition_key_paths, list):
            raise ValueError(f"Invalid partition key paths format: {partition_key_paths}")
        
        pk = self._create_partition_key(partition_key_paths)
        self._client.create_container(container_name, pk)

    def list_containers(self) -> list[str]:
        """
        List all containers in the current database.
        
        This method provides a common interface for all subclasses to list
        containers, ensuring consistent behavior across the application.
        
        Returns:
            List of container names as strings
            
        Raises:
            RuntimeError: If client is not initialized
        """
        if self._client is None:
            raise RuntimeError("Client not initialized. Call _initialize_client() first.")
        
        return self._client.list_containers()

    def get_container_properties(self, container_name: str) -> dict[str, any]:
        """
        Get container properties including partition key information.
        
        This method provides a common interface for all subclasses to get
        container properties, ensuring consistent behavior across the application.
        
        Args:
            container_name: Name of the container to get properties for
            
        Returns:
            Dictionary containing container properties
            
        Raises:
            RuntimeError: If client is not initialized
        """
        if self._client is None:
            raise RuntimeError("Client not initialized. Call _initialize_client() first.")
        
        return self._client.get_container_properties(container_name)

    def create_database_if_not_exists(self, database_name: str):
        """
        Create a database if it doesn't exist.
        
        This method provides a common interface for all subclasses to create
        databases, ensuring consistent behavior across the application.
        
        Args:
            database_name: Name of the database to create
            
        Returns:
            Database object from the Azure SDK
            
        Raises:
            RuntimeError: If client is not initialized
        """
        if self._client is None:
            raise RuntimeError("Client not initialized. Call _initialize_client() first.")
        
        return self._client.create_database_if_not_exists(database_name)

    def process_items_batch(self, container_name: str, items: list[dict[str, any]], 
                           batch_size: int = 100, upsert: bool = False) -> list[dict[str, any]]:
        """
        Process multiple items in batches with create or upsert operation.
        
        This method provides a common interface for all subclasses to process
        items in batches, supporting both create and upsert operations.
        
        Args:
            container_name: Name of the container to process items in
            items: List of items to process
            batch_size: Size of batches for processing (default: 100)
            upsert: If True, use upsert operation; if False, use create operation
            
        Returns:
            List of processed items
            
        Raises:
            RuntimeError: If client is not initialized
        """
        if self._client is None:
            raise RuntimeError("Client not initialized. Call _initialize_client() first.")
        
        if upsert:
            return self._client.upsert_items_batch(container_name, items, batch_size)
        else:
            return self._client.create_items_batch(container_name, items, batch_size)

    def get_all_items(self, container_name: str) -> list[dict[str, any]]:
        """
        Get all items from a container with progress tracking.
        
        This method provides a common interface for all subclasses to retrieve
        all items from a container, ensuring consistent behavior across the application.
        
        Args:
            container_name: Name of the container to get items from
            
        Returns:
            List of items from the container
            
        Raises:
            RuntimeError: If client is not initialized
        """
        if self._client is None:
            raise RuntimeError("Client not initialized. Call _initialize_client() first.")
        
        return self._client.get_all_items(container_name)

    def list_databases(self) -> list[str]:
        """
        List all databases in the CosmosDB account.
        
        This method provides a common interface for all subclasses to list
        databases, ensuring consistent behavior across the application.
        
        Returns:
            List of database names as strings
            
        Raises:
            RuntimeError: If client is not initialized
        """
        if self._client is None:
            raise RuntimeError("Client not initialized. Call _initialize_client() first.")
        
        return self._client.list_databases()

    def get_database_info(self, database_name: str) -> dict[str, any]:
        """
        Get database properties and container list for a specific database.
        
        This method provides a common interface for all subclasses to get
        database information, ensuring consistent behavior across the application.
        
        Args:
            database_name: Name of the database to get info for
            
        Returns:
            Dictionary containing database properties and container list
            
        Raises:
            RuntimeError: If client is not initialized
        """
        if self._client is None:
            raise RuntimeError("Client not initialized. Call _initialize_client() first.")
        
        return self._client.get_database_info(database_name)

    def delete_database(self, database_name: str) -> None:
        """
        Delete a database.
        
        This method provides a common interface for all subclasses to delete
        databases, ensuring consistent behavior across the application.
        
        Args:
            database_name: Name of the database to delete
            
        Raises:
            RuntimeError: If client is not initialized
        """
        if self._client is None:
            raise RuntimeError("Client not initialized. Call _initialize_client() first.")
        
        self._client.delete_database(database_name)

    def _create_partition_key(self, paths: list[str]) -> PartitionKey:
        """
        Create a PartitionKey object from paths.
        
        Args:
            paths: list of strings representing partition key paths
            
        Returns:
            PartitionKey object for use with container creation
        """
        if len(paths) == 1:
            return PartitionKey(path=paths[0])
        return PartitionKey(paths=paths)
