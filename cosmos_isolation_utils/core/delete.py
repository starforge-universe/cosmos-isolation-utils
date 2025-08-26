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


class DatabaseDeleter:
    """Class for deleting CosmosDB databases."""

    def __init__(self, db_config: DatabaseConfig):
        log_info("Initializing CosmosDB client...")
        log_info(f"  Endpoint: {db_config.endpoint}")
        log_info(f"  Allow insecure: {db_config.allow_insecure}")

        # Control HTTPS verification warnings
        if db_config.allow_insecure:
            log_info("  Suppressing HTTPS verification warnings...")
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        log_info("  Creating CosmosClient...")
        self.client = CosmosClient(db_config.endpoint, db_config.key)
        log_checkmark("  CosmosClient created")
        log_checkmark("Database deleter initialization completed")

    def list_databases(self) -> list:
        """List all databases in the CosmosDB account."""
        try:
            databases = list(self.client.list_databases())
            return [db['id'] for db in databases]
        except Exception as e:
            log_error(f"Error listing databases: {e}")
            raise

    def get_database_info(self, database_name: str) -> dict:
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

    def delete_database(self, database_name: str, force: bool = False) -> bool:  # pylint: disable=unused-argument
        """Delete a database with optional confirmation."""
        try:
            # Get database info first
            db_info = self.get_database_info(database_name)
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


def delete_database(db_config: DatabaseConfig, delete_config: DeleteConfig):
    """Delete CosmosDB databases with safety confirmations."""
    try:
        # Initialize database deleter
        deleter = DatabaseDeleter(db_config)

        if delete_config.list_only:
            log_panel("[bold blue]Listing all databases[/bold blue]", style="blue")
            databases = deleter.list_databases()

            if not databases:
                log_warning("No databases found in the CosmosDB account")
                return

            table = Table(title="Available Databases")
            table.add_column("Database Name", style="cyan")
            table.add_column("Index", style="green")

            for i, db_name in enumerate(databases, 1):
                table.add_row(db_name, str(i))

            console.print(table)
            return

        # For the unified CLI, we need to get the database name from the context
        # This function is called from the delete_db subcommand which doesn't have a database parameter
        # So we'll just list databases for now
        log_warning("Note: Database deletion requires specifying the database name.")
        log_warning("Use the list-databases option to see available databases.")

        # List databases by default
        log_panel("[bold blue]Available Databases[/bold blue]", style="blue")
        databases = deleter.list_databases()

        if not databases:
            log_warning("No databases found in the CosmosDB account")
            return

        table = Table(title="Available Databases")
        table.add_column("Database Name", style="cyan")
        table.add_column("Index", style="green")

        for i, db_name in enumerate(databases, 1):
            table.add_row(db_name, str(i))

        console.print(table)

    except Exception as e:
        log_error(f"Fatal error: {e}")
        raise
