"""
Base class for subcommand executors.

This module provides a common superclass for all subcommand implementations,
ensuring consistent initialization patterns and client management.
"""

from typing import Dict, List, Any, Optional
from azure.cosmos.database import DatabaseProxy
from rich.prompt import Confirm
import urllib3
from azure.cosmos import PartitionKey, CosmosClient
from azure.cosmos.exceptions import CosmosHttpResponseError
from rich.progress import Progress, SpinnerColumn, TextColumn

from .config import DatabaseConfig
from .logging_utils import (
    log_checkmark, log_info, log_error, log_step, log_warning, log_success, console
)


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
        self._client: Optional[CosmosClient] = None
        self._database: Optional[DatabaseProxy] = None
        
        # Automatically display connection info and initialize client
        self._display_connection_info()
        log_step(1, "Initializing client...")
        self._initialize_client()

    @property
    def db_config(self) -> DatabaseConfig:
        """Get the database configuration."""
        return self._db_config

    def _initialize_client(self) -> None:
        """
        Initialize the CosmosDB client.

        This method creates a new CosmosClient instance using the stored
        database configuration. It should be called before any operations
        that require the client.
        """
        # Control HTTPS verification warnings
        if self._db_config.allow_insecure:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        self._client = CosmosClient(self._db_config.endpoint, self._db_config.key)
        self._database = self._client.get_database_client(self._db_config.database)

    def _display_connection_info(self) -> None:
        """
        Display connection configuration information.

        This method shows the endpoint and allow_insecure settings,
        but does not display the database name for security reasons.
        """
        log_info(f"Endpoint: {self._db_config.endpoint}")
        log_info(f"Allow insecure: {self._db_config.allow_insecure}")

    def _filter_internal_attributes(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Filter out CosmosDB internal attributes from an item."""
        internal_attributes = {'_rid', '_self', '_etag', '_attachments', '_ts'}
        return {k: v for k, v in item.items() if k not in internal_attributes}

    def _filter_items_batch(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filter out CosmosDB internal attributes from a batch of items."""
        return [self._filter_internal_attributes(item) for item in items]

    def get_container_client(self, container_name: str):
        """Get a container client for the specified container."""
        return self._database.get_container_client(container_name)

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
        # Convert string to list if needed
        if isinstance(partition_key_paths, str):
            partition_key_paths = [partition_key_paths]
        elif not isinstance(partition_key_paths, list):
            raise ValueError(f"Invalid partition key paths format: {partition_key_paths}")
        
        pk = self._create_partition_key(partition_key_paths)
        self._database.create_container(id=container_name, partition_key=pk)

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
        containers = list(self._database.list_containers())
        return [container['id'] for container in containers]

    def get_container_properties(self, container_name: str) -> dict[str, any]:
        """
        Get container properties including partition key information.
        
        This method provides a common interface for all subclasses to get
        container properties, ensuring consistent behavior across the application.
        
        Args:
            container_name: Name of the container to get properties for
            
        Returns:
            Dictionary containing container properties
            
        """
        container = self.get_container_client(container_name)
        return container.read()

    def create_database(self, force: bool = False) -> None:
        """Create the database if it doesn't exist."""
        if not force:
            if not Confirm.ask(f"Do you want to create database '{self.db_config.database}'?"):
                log_warning("Database creation cancelled.")
                raise Exception("Database creation cancelled by user")

        try:
            log_info(f"Creating database '{self.db_config.database}'...")
            self._client.create_database_if_not_exists(self.db_config.database)
            log_checkmark(f"Database '{self.db_config.database}' created successfully")
        except Exception as e:
            log_error(f"Error creating database '{self.db_config.database}': {e}")
            raise

    def create_database_if_not_exists(self, database_name: str):
        """
        Create a database if it doesn't exist.
        
        This method provides a common interface for all subclasses to create
        databases, ensuring consistent behavior across the application.
        
        Args:
            database_name: Name of the database to create
            
        Returns:
            Database object from the Azure SDK
            
        """
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
            
        """
        container = self.get_container_client(container_name)
        processed_items = []

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            operation = "Upserting" if upsert else "Creating"
            task = progress.add_task(f"{operation} items in {container_name}...", total=len(items))

            for i in range(0, len(items), batch_size):
                batch = items[i:i + batch_size]

                for item in batch:
                    try:
                        if upsert:
                            processed_item = container.upsert_item(item)
                        else:
                            processed_item = container.create_item(item)
                        processed_items.append(processed_item)
                        progress.advance(task)
                    except CosmosHttpResponseError as e:
                        log_error(f"Error {operation.lower()} item: {e}")
                        # Continue with other items
                        progress.advance(task)

        return processed_items

    def get_all_items(self, container_name: str) -> list[dict[str, any]]:
        """
        Get all items from a container with progress tracking.
        
        This method provides a common interface for all subclasses to retrieve
        all items from a container, ensuring consistent behavior across the application.
        
        Args:
            container_name: Name of the container to get items from
            
        Returns:
            List of items from the container
            
        """
        container = self.get_container_client(container_name)

        # First, get the total count
        count_query = "SELECT VALUE COUNT(1) FROM c"
        count_result = list(container.query_items(query=count_query, enable_cross_partition_query=True))
        total_count = count_result[0] if count_result else 0

        if total_count == 0:
            log_warning(f"No items found in container '{container_name}'")
            return []

        log_success(f"Found {total_count} items in container '{container_name}'")

        # Get all items with progress tracking
        all_items = []
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task(f"Fetching items from {container_name}...", total=total_count)

            query = "SELECT * FROM c"
            items = container.query_items(query=query, enable_cross_partition_query=True)

            for item in items:
                filtered_item = self._filter_internal_attributes(item)
                all_items.append(filtered_item)
                progress.advance(task)

        return all_items

    def list_databases(self) -> list[str]:
        """
        List all databases in the CosmosDB account.
        
        This method provides a common interface for all subclasses to list
        databases, ensuring consistent behavior across the application.
        
        Returns:
            List of database names as strings
            
        """
        databases = list(self._client.list_databases())
        return [db['id'] for db in databases]

    def get_database_info(self, database_name: str) -> dict[str, any]:
        """
        Get database properties and container list for a specific database.
        
        This method provides a common interface for all subclasses to get
        database information, ensuring consistent behavior across the application.
        
        Args:
            database_name: Name of the database to get info for
            
        Returns:
            Dictionary containing database properties and container list
            
        """
        database = self._client.get_database_client(database_name)
        properties = database.read()
        containers = list(database.list_containers())

        return {
            "properties": properties,
            "containers": containers,
            "container_count": len(containers)
        }

    def delete_database(self, database_name: str) -> None:
        """
        Delete a database.
        
        This method provides a common interface for all subclasses to delete
        databases, ensuring consistent behavior across the application.
        
        Args:
            database_name: Name of the database to delete
            
        """
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
