"""
Core database deletion functionality for CosmosDB.
"""

from rich.table import Table
from rich.prompt import Confirm
from azure.cosmos.exceptions import CosmosHttpResponseError

from .config import DatabaseConfig, DeleteConfig
from .logging_utils import (
    log_info, log_success, log_warning, log_error,
    log_panel, log_warning_icon, console
)
from .base_executor import BaseSubcommandExecutor



class DatabaseDeleter(BaseSubcommandExecutor):  # pylint: disable=too-few-public-methods
    """Class for deleting CosmosDB databases."""

    def __init__(self, db_config: DatabaseConfig):  # pylint: disable=useless-parent-delegation
        """Initialize the database deleter with database configuration."""
        super().__init__(db_config)

    def _list_databases(self) -> list:
        """List all databases in the CosmosDB account."""
        try:
            databases = self.list_databases()
            return databases
        except Exception as e:
            log_error(f"Error listing databases: {e}")
            raise

    def _get_database_info(self, database_name: str) -> dict:
        """Get information about a specific database."""
        try:
            db_info = self.get_database_info(database_name)
            properties = db_info["properties"]
            containers = db_info["containers"]

            return {
                "name": database_name,
                "id": properties.get('id'),
                "rid": properties.get('_rid'),
                "ts": properties.get('_ts'),
                "etag": properties.get('_etag'),
                "container_count": db_info["container_count"],
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
                log_info(f"  Container names: {', '.join(db_info['containers'])}")
            log_info(f"  Created: {db_info['ts']}")

            # Safety confirmation
            if not force:
                log_warning_icon("WARNING: This action cannot be undone!")
                log_error(f"All data in database '{database_name}' will be permanently deleted.")

                if not Confirm.ask(f"Are you sure you want to delete database '{database_name}'?"):
                    log_warning("Database deletion cancelled")
                    return False

                # Double confirmation for databases with containers
                if db_info['container_count'] > 0:
                    log_warning_icon(f"This database contains {db_info['container_count']} containers with data!")
                    if not Confirm.ask("Are you absolutely sure? This will delete ALL data permanently!"):
                        log_warning("Database deletion cancelled")
                        return False

            # Proceed with deletion
            log_info(f"Deleting database '{database_name}'...")
            self.delete_database(database_name)

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

    def _handle_list_only_mode(self) -> None:
        """Handle list-only mode to display available databases."""
        log_panel("[bold blue]Listing all databases[/bold blue]", style="blue")
        databases = self._list_databases()

        if not databases:
            log_warning("No databases found in the CosmosDB account")
            return

        table = Table(title="Available Databases")
        table.add_column("Database Name", style="cyan")
        table.add_column("Index", style="green")

        for i, db_name in enumerate(databases, 1):
            table.add_row(db_name, str(i))

        console.print(table)

    def _handle_default_mode(self, delete_config: DeleteConfig) -> None:
        """Handle default mode - delete the specified database."""
        database_name = self.db_config.database

        if not database_name:
            log_error("Error: No database name specified for deletion.")
            log_warning("Please specify a database name using the --database parameter.")
            return

        log_panel("[bold red]Database Deletion Mode[/bold red]", style="red")
        log_info(f"Target database: {database_name}")

        # Attempt to delete the specified database
        success = self._delete_single_database(database_name, delete_config.force)

        if success:
            log_success(f"✓ Database '{database_name}' deleted successfully")
        else:
            log_warning(f"Database '{database_name}' was not deleted")

    def delete_database(self, delete_config: DeleteConfig) -> None:
        """Main method to handle database deletion operations."""
        # Display connection info
        self._display_connection_info()

        # Initialize client
        self._initialize_client()

        if delete_config.list_only:
            self._handle_list_only_mode()
        else:
            self._handle_default_mode(delete_config)

    def execute(self, delete_config: DeleteConfig) -> None:
        """Execute the delete database operation."""
        self.delete_database(delete_config)
