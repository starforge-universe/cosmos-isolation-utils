"""
CosmosDB client wrapper for common database operations.

This module provides a high-level interface for interacting with Azure CosmosDB,
including operations like querying, creating, and managing containers and items.
"""

from typing import List, Dict, Any, Optional, Iterator
import urllib3
from azure.cosmos import CosmosClient
from azure.cosmos.exceptions import CosmosHttpResponseError
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

console = Console()


class CosmosDBClient:
    """Wrapper class for CosmosDB operations."""

    def __init__(self, endpoint: str, key: str, database: str, allow_insecure: bool = False):
        self.endpoint = endpoint
        self.key = key
        self.database_name = database

        console.print("[cyan]Initializing CosmosDB client...[/cyan]")
        console.print(f"  Endpoint: {endpoint}")
        console.print(f"  Database: {database}")
        console.print(f"  Allow insecure: {allow_insecure}")

        # Control HTTPS verification warnings
        if allow_insecure:
            console.print("[cyan]  Suppressing HTTPS verification warnings...[/cyan]")
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        console.print("[cyan]  Creating CosmosClient...[/cyan]")
        self.client = CosmosClient(endpoint, key)
        console.print("[green]  ✓ CosmosClient created[/green]")

        console.print("[cyan]  Getting database client...[/cyan]")
        self.database = self.client.get_database_client(database)
        console.print("[green]  ✓ Database client obtained[/green]")

        console.print("[green]✓ CosmosDB client initialization completed[/green]")

    def _filter_internal_attributes(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Filter out CosmosDB internal attributes from an item."""
        internal_attributes = {'_rid', '_self', '_etag', '_attachments', '_ts'}
        return {k: v for k, v in item.items() if k not in internal_attributes}

    def _filter_items_batch(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filter out CosmosDB internal attributes from a batch of items."""
        return [self._filter_internal_attributes(item) for item in items]

    def get_container_client(self, container_name: str):
        """Get a container client for the specified container."""
        try:
            return self.database.get_container_client(container_name)
        except CosmosHttpResponseError as e:
            console.print(f"[red]Error accessing container '{container_name}': {e}[/red]")
            raise

    def create_database_if_not_exists(self, database_name: str):
        """Create database if it doesn't exist."""
        try:
            return self.client.create_database_if_not_exists(database_name)
        except Exception as e:
            console.print(f"[red]Error creating database '{database_name}': {e}[/red]")
            raise

    def get_container_properties(self, container_name: str) -> Dict[str, Any]:
        """Get container properties including partition key information."""
        try:
            container = self.get_container_client(container_name)
            return container.read()
        except CosmosHttpResponseError as e:
            console.print(f"[red]Error reading container properties for '{container_name}': {e}[/red]")
            raise

    def list_containers(self) -> List[str]:
        """List all containers in the database."""
        containers = list(self.database.list_containers())
        return [container['id'] for container in containers]

    def query_items(self, container_name: str, query: str = "SELECT * FROM c",
                   parameters: Optional[List[Dict[str, Any]]] = None) -> Iterator[Dict[str, Any]]:
        """Query items from a container."""
        container = self.get_container_client(container_name)

        try:
            if parameters:
                items = container.query_items(query=query, parameters=parameters, enable_cross_partition_query=True)
            else:
                items = container.query_items(query=query, enable_cross_partition_query=True)

            for item in items:
                yield self._filter_internal_attributes(item)

        except CosmosHttpResponseError as e:
            console.print(f"[red]Error querying container '{container_name}': {e}[/red]")
            raise

    def get_all_items(self, container_name: str) -> List[Dict[str, Any]]:
        """Get all items from a container with progress tracking."""
        container = self.get_container_client(container_name)

        try:
            # First, get the total count
            count_query = "SELECT VALUE COUNT(1) FROM c"
            count_result = list(container.query_items(
                query=count_query, enable_cross_partition_query=True
            ))
            total_count = count_result[0] if count_result else 0

            if total_count == 0:
                console.print(f"[yellow]No items found in container '{container_name}'[/yellow]")
                return []

            console.print(f"[green]Found {total_count} items in container '{container_name}'[/green]")

            # Get all items with progress tracking
            all_items = []
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console
            ) as progress:
                task = progress.add_task(
                    f"Fetching items from {container_name}...", 
                    total=total_count
                )

                query = "SELECT * FROM c"
                items = container.query_items(
                    query=query, enable_cross_partition_query=True
                )

                for item in items:
                    filtered_item = self._filter_internal_attributes(item)
                    all_items.append(filtered_item)
                    progress.advance(task)

            return all_items

        except CosmosHttpResponseError as e:
            console.print(
                f"[red]Error getting items from container "
                f"'{container_name}': {e}[/red]"
            )
            raise

    def create_item(self, container_name: str, item: Dict[str, Any]) -> Dict[str, Any]:
        """Create a single item in a container."""
        container = self.get_container_client(container_name)

        try:
            return container.create_item(item)
        except CosmosHttpResponseError as e:
            console.print(
                f"[red]Error creating item in container "
                f"'{container_name}': {e}[/red]"
            )
            raise

    def upsert_item(self, container_name: str, item: Dict[str, Any]) -> Dict[str, Any]:
        """Upsert a single item in a container."""
        container = self.get_container_client(container_name)

        try:
            return container.upsert_item(item)
        except CosmosHttpResponseError as e:
            console.print(
                f"[red]Error upserting item in container "
                f"'{container_name}': {e}[/red]"
            )
            raise

    def create_items_batch(self, container_name: str, items: List[Dict[str, Any]],
                          batch_size: int = 100) -> List[Dict[str, Any]]:
        """Create multiple items in batches with progress tracking."""
        container = self.get_container_client(container_name)
        created_items = []

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task(
                f"Creating items in {container_name}...", total=len(items)
            )

            for i in range(0, len(items), batch_size):
                batch = items[i:i + batch_size]

                for item in batch:
                    try:
                        created_item = container.create_item(item)
                        created_items.append(created_item)
                        progress.advance(task)
                    except CosmosHttpResponseError as e:
                        console.print(f"[red]Error creating item: {e}[/red]")
                        # Continue with other items
                        progress.advance(task)

        return created_items

    def upsert_items_batch(self, container_name: str, items: List[Dict[str, Any]],
                           batch_size: int = 100) -> List[Dict[str, Any]]:
        """Upsert multiple items in batches with progress tracking."""
        container = self.get_container_client(container_name)
        upserted_items = []

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task(
                f"Upserting items in {container_name}...", total=len(items)
            )

            for i in range(0, len(items), batch_size):
                batch = items[i:i + batch_size]

                for item in batch:
                    try:
                        upserted_item = container.upsert_item(item)
                        upserted_items.append(upserted_item)
                        progress.advance(task)
                    except CosmosHttpResponseError as e:
                        console.print(f"[red]Error upserting item: {e}[/red]")
                        # Continue with other items
                        progress.advance(task)

        return upserted_items

    def get_container_stats(self, container_name: str) -> Dict[str, Any]:
        """Get container statistics including item count and partition key info."""
        try:
            container = self.get_container_client(container_name)
            properties = container.read()

            # Get item count
            count_query = "SELECT VALUE COUNT(1) FROM c"
            count_result = list(container.query_items(
                query=count_query, enable_cross_partition_query=True
            ))
            item_count = count_result[0] if count_result else 0

            stats = {
                "name": container_name,
                "item_count": item_count,
                "partition_key": properties.get('partitionKey'),
                "last_modified": properties.get('lastModified'),
                "etag": properties.get('_etag')
            }

            return stats

        except CosmosHttpResponseError as e:
            console.print(
                f"[red]Error getting stats for container '{container_name}': {e}[/red]"
            )
            raise

    def get_all_containers_stats(self) -> List[Dict[str, Any]]:
        """Get statistics for all containers in the database."""
        try:
            containers = self.list_containers()
            stats = []

            for container_name in containers:
                try:
                    container_stats = self.get_container_stats(container_name)
                    stats.append(container_stats)
                except Exception as e:
                    console.print(
                        f"[yellow]Warning: Could not get stats for container "
                        f"'{container_name}': {e}[/yellow]"
                    )
                    # Add basic info even if stats fail
                    stats.append({
                        "name": container_name,
                        "item_count": "Unknown",
                        "partition_key": "Unknown",
                        "last_modified": "Unknown",
                        "etag": "Unknown"
                    })

            return stats

        except Exception as e:
            console.print(f"[red]Error getting container statistics: {e}[/red]")
            raise
