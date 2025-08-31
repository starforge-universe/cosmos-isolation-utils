"""
Core connection testing functionality for CosmosDB.
"""

from azure.cosmos.exceptions import CosmosHttpResponseError

from .config import DatabaseConfig, ConnectionConfig
from .logging_utils import (
    log_info, log_success, log_error, log_warning, log_step, log_with_color
)
from .base_executor import BaseSubcommandExecutor


class ConnectionTester(BaseSubcommandExecutor):  # pylint: disable=too-few-public-methods
    """Tester class for CosmosDB connection and database operations."""

    def __init__(self, db_config: DatabaseConfig):  # pylint: disable=useless-parent-delegation
        """Initialize the connection tester with database configuration."""
        super().__init__(db_config)

    def _test_database_access(self, connection_config: ConnectionConfig) -> list:
        """Test database access and return list of containers."""
        log_info("  Attempting to list containers...")

        try:
            containers = self.list_containers()
            log_success(f"✓ Successfully connected to database: {self.db_config.database}")
            return containers
        except CosmosHttpResponseError as e:
            if "Owner resource does not exist" in str(e) or "NotFound" in str(e):
                return self._handle_database_not_found(e, connection_config)

            log_error(f"Error accessing database: {e}")
            raise

    def _handle_database_not_found(self, error: CosmosHttpResponseError, connection_config: ConnectionConfig) -> list:
        """Handle the case when the database doesn't exist."""
        log_warning(f"Database '{self.db_config.database}' does not exist or is not accessible.")

        if connection_config.create_database:
            self.create_database(connection_config.force)
            return self._test_database_access(connection_config)

        log_error(f"Database '{self.db_config.database}' does not exist. Use --create-database flag to create it.")
        raise Exception(f"Database '{self.db_config.database}' does not exist") from error

    def _display_containers(self, containers: list) -> None:
        """Display the list of available containers."""
        if containers:
            log_with_color(f"\nAvailable containers ({len(containers)}):", "bold cyan")
            for i, container in enumerate(containers, 1):
                log_info(f"  {i}. {container}")
        else:
            log_warning("No containers found in the database.")

    def test_connection(self, connection_config: ConnectionConfig) -> None:
        """Main method to test CosmosDB connection and list containers."""
        # Test database access
        log_step(2, "Testing database access...")
        containers = self._test_database_access(connection_config)

        # Display containers
        log_step(3, "Listing containers...")
        self._display_containers(containers)
