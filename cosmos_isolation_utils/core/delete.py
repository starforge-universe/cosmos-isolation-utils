"""
Core database deletion functionality for CosmosDB.
"""

from rich.table import Table
from rich.prompt import Confirm
from azure.cosmos import CosmosClient
from azure.cosmos.exceptions import CosmosHttpResponseError
import urllib3

from .config import DatabaseConfig, DeleteConfig
from .logging_utils import (
    log_info, log_success, log_warning, log_error,
    log_panel, log_checkmark, log_warning_icon, console
)


class DatabaseDeleter:  # pylint: disable=too-few-public-methods
    """Class for deleting CosmosDB databases."""

    def __init__(self, db_config: DatabaseConfig):
        """Initialize the database deleter with database configuration."""
        self.db_config = db_config
        self.client = None

    def _initialize_client(self) -> None:
        """Initialize the CosmosDB client."""
        log_info("Initializing CosmosDB client...")
        log_info(f"  Endpoint: {self.db_config.endpoint}")
        log_info(f"  Allow insecure: {self.db_config.allow_insecure}")

        # Control HTTPS verification warnings
        if self.db_config.allow_insecure:
            log_info("  Suppressing HTTPS verification warnings...")
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        log_info("  Creating CosmosClient...")
        self.client = CosmosClient(self.db_config.endpoint, self.db_config.key)
        log_checkmark("  CosmosClient created")
        log_checkmark("Database deleter initialization completed")

    def _list_databases(self) -> list:
        """List all databases in the CosmosDB account."""
        try:
            databases = list(self.client.list_databases())
            return [db['id'] for db in databases]
        except Exception as e:
            log_error(f"Error listing databases: {e}")
            raise

    def _get_database_info(self, database_name: str) -> dict:
        """Get information about a specific database."""
        try:
            database = self.client.get_database_client(database_name)
            properties = database.read()

            # Get container count
            containers = list(database.list_containers())
            container_count = len(containers)

            return {
                "name": database_name,
                "id": properties.get('id'),
                "rid": properties.get('_rid'),
                "ts": properties.get('_ts'),
                "etag": properties.get('_etag'),
                "container_count": container_count,
                "containers": [container['id'] for container in containers]
            }
        except CosmosHttpResponseError as e:
            if e.status_code == 404:
                log_warning(f"Database '{database_name}' not found")
                return None
            log_error(f"Error getting database info for '{database_name}': {e}")
            raise
        except Exception as e:
            log_error(f"Error getting database info for '{database_name}': {e}")
            raise

    def _delete_single_database(self, database_name: str, force: bool = False) -> bool:
        """Delete a single database with optional confirmation."""
        try:
            # Get database info first
            db_info = self._get_database_info(database_name)
            if not db_info:
                log_error(f"Database '{database_name}' does not exist")
                return False

            # Show database information
            log_panel("[bold red]Database to be deleted:[/bold red]", style="red")
            log_info(f"  Name: {db_info['name']}")
            log_info(f"  ID: {db_info['id']}")
            log_info(f"  Containers: {db_info['container_count']}")
            if db_info['containers']:
                log_info(
                    f"  Container names: {', '.join(db_info['containers'])}"
                )
            log_info(f"  Created: {db_info['ts']}")

            # Safety confirmation
            if not force:
                log_warning_icon("WARNING: This action cannot be undone!")
                log_error(
                    f"All data in database '{database_name}' will be "
                    f"permanently deleted."
                )

                if not Confirm.ask(
                    f"Are you sure you want to delete database '{database_name}'?"
                ):
                    log_warning("Database deletion cancelled")
                    return False

                # Double confirmation for databases with containers
                if db_info['container_count'] > 0:
                    log_warning_icon(
                        f"This database contains "
                        f"{db_info['container_count']} containers with data!"
                    )
                    if not Confirm.ask(
                        "Are you absolutely sure? This will delete ALL data permanently!"
                    ):
                        log_warning("Database deletion cancelled")
                        return False

            # Proceed with deletion
            log_info(f"Deleting database '{database_name}'...")
            self.client.delete_database(database_name)

            log_success(f"✓ Database '{database_name}' deleted successfully")
            return True

        except CosmosHttpResponseError as e:
            if e.status_code == 404:
                log_warning(f"Database '{database_name}' not found")
                return False
            log_error(f"Error deleting database '{database_name}': {e}")
            raise
        except Exception as e:
            log_error(f"Error deleting database '{database_name}': {e}")
            raise

    def _display_databases_table(self, databases: list) -> None:
        """Display databases in a formatted table."""
        if not databases:
            log_warning("No databases found in the CosmosDB account")
            return

        table = Table(title="Available Databases")
        table.add_column("Database Name", style="cyan")
        table.add_column("Index", style="green")

        for i, db_name in enumerate(databases, 1):
            table.add_row(db_name, str(i))

        console.print(table)

    def _handle_list_only_mode(self) -> None:
        """Handle list-only mode to display available databases."""
        log_panel("[bold blue]Listing all databases[/bold blue]", style="blue")
        databases = self._list_databases()
        self._display_databases_table(databases)

    def _handle_default_mode(self) -> None:
        """Handle default mode when no specific action is specified."""
        log_warning("Note: Database deletion requires specifying the database name.")
        log_warning("Use the list-databases option to see available databases.")

        # List databases by default
        log_panel("[bold blue]Available Databases[/bold blue]", style="blue")
        databases = self._list_databases()
        self._display_databases_table(databases)

    def delete_database(self, delete_config: DeleteConfig) -> None:
        """Main method to handle database deletion operations."""
        # Initialize client
        self._initialize_client()

        if delete_config.list_only:
            self._handle_list_only_mode()
        else:
            self._handle_default_mode()
