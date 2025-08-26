"""
Core connection testing functionality for CosmosDB.
"""

from azure.cosmos.exceptions import CosmosHttpResponseError
from rich.prompt import Confirm

from .cosmos_client import CosmosDBClient
from .config import DatabaseConfig, ConnectionConfig
from .logging_utils import (
    log_info, log_success, log_error, log_warning, log_step, log_checkmark, log_with_color
)


class ConnectionTester:  # pylint: disable=too-few-public-methods
    """Tester class for CosmosDB connection and database operations."""

    def __init__(self, db_config: DatabaseConfig, connection_config: ConnectionConfig):
        """Initialize the connection tester with database and connection configuration."""
        self.db_config = db_config
        self.connection_config = connection_config
        self.client = None

    def _display_connection_info(self) -> None:
        """Display connection configuration information."""
        log_info(f"Endpoint: {self.db_config.endpoint}")
        log_info(f"Database: {self.db_config.database}")
        log_info(f"Allow insecure: {self.db_config.allow_insecure}")
        log_info(f"Create database if missing: {self.connection_config.create_database}")

    def _initialize_client(self) -> None:
        """Initialize the CosmosDB client."""
        log_step(1, "Initializing CosmosDB client...")
        try:
            self.client = CosmosDBClient(self.db_config)
            log_checkmark("CosmosDB client initialized")
        except Exception as e:
            log_error(f"Error initializing client: {e}")
            raise

    def _test_database_access(self) -> list:
        """Test database access and return list of containers."""
        log_step(2, "Testing database access...")
        log_info("  Attempting to list containers...")

        try:
            containers = self.client.list_containers()
            log_success(f"✓ Successfully connected to database: {self.db_config.database}")
            return containers
        except CosmosHttpResponseError as e:
            if "Owner resource does not exist" in str(e) or "NotFound" in str(e):
                return self._handle_database_not_found(e)

            log_error(f"Error accessing database: {e}")
            raise

    def _handle_database_not_found(self, error: CosmosHttpResponseError) -> list:
        """Handle the case when the database doesn't exist."""
        log_warning(
            f"Database '{self.db_config.database}' does not exist or "
            f"is not accessible."
        )

        if self.connection_config.create_database:
            return self._create_database_and_test()

        log_error(
            f"Database '{self.db_config.database}' does not exist. "
            f"Use --create-database flag to create it."
        )
        raise Exception(f"Database '{self.db_config.database}' does not exist") from error

    def _create_database_and_test(self) -> list:
        """Create the database and test access."""
        if not self.connection_config.force:
            if not Confirm.ask(f"Do you want to create database '{self.db_config.database}'?"):
                log_warning("Database creation cancelled.")
                return []  # User cancelled, return empty list

        try:
            log_info(f"Creating database '{self.db_config.database}'...")
            self.client.client.create_database_if_not_exists(self.db_config.database)
            log_checkmark(f"Database '{self.db_config.database}' created successfully")

            # Test access to the newly created database
            log_info("Testing database access after creation...")
            containers = self.client.list_containers()
            log_success(
                f"✓ Successfully connected to newly created "
                f"database: {self.db_config.database}"
            )
            return containers

        except Exception as e:
            log_error(f"Error creating database '{self.db_config.database}': {e}")
            raise

    def _display_containers(self, containers: list) -> None:
        """Display the list of available containers."""
        log_step(3, "Listing containers...")
        if containers:
            log_with_color(f"\nAvailable containers ({len(containers)}):", "bold cyan")
            for i, container in enumerate(containers, 1):
                log_info(f"  {i}. {container}")
        else:
            log_warning("No containers found in the database.")

    def test_connection(self) -> None:
        """Main method to test CosmosDB connection and list containers."""
        # Display connection information
        self._display_connection_info()

        # Initialize client
        self._initialize_client()

        # Test database access
        containers = self._test_database_access()

        # Display containers
        self._display_containers(containers)

        log_success("Connection test completed successfully!")
