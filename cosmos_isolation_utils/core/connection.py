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

    def __init__(self, db_config: DatabaseConfig):
        """Initialize the connection tester with database configuration."""
        self.db_config = db_config
        self.client = None

    def _display_connection_info(self, connection_config: ConnectionConfig) -> None:
        """Display connection configuration information."""
        log_info(f"Endpoint: {self.db_config.endpoint}")
        log_info(f"Database: {self.db_config.database}")
        log_info(f"Allow insecure: {self.db_config.allow_insecure}")
        log_info(f"Create database if missing: {connection_config.create_database}")

    def _initialize_client(self) -> None:
        """Initialize the CosmosDB client."""
        log_step(1, "Initializing CosmosDB client...")
        try:
            self.client = CosmosDBClient(self.db_config)
            log_checkmark("CosmosDB client initialized")
        except Exception as e:
            log_error(f"Error initializing client: {e}")
            raise

    def _test_database_access(self, connection_config: ConnectionConfig) -> list:
        """Test database access and return list of containers."""
        log_step(2, "Testing database access...")
        log_info("  Attempting to list containers...")

        try:
            containers = self.client.list_containers()
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
            self._create_database(connection_config)
            return self._test_database_access_after_creation()

        log_error(f"Database '{self.db_config.database}' does not exist. Use --create-database flag to create it.")
        raise Exception(f"Database '{self.db_config.database}' does not exist") from error

    def _create_database(self, connection_config: ConnectionConfig) -> None:
        """Create the database if it doesn't exist."""
        if not connection_config.force:
            if not Confirm.ask(f"Do you want to create database '{self.db_config.database}'?"):
                log_warning("Database creation cancelled.")
                raise Exception("Database creation cancelled by user")

        try:
            log_info(f"Creating database '{self.db_config.database}'...")
            self.client.create_database_if_not_exists(self.db_config.database)
            log_checkmark(f"Database '{self.db_config.database}' created successfully")
        except Exception as e:
            log_error(f"Error creating database '{self.db_config.database}': {e}")
            raise

    def _test_database_access_after_creation(self) -> list:
        """Test access to the newly created database."""
        try:
            log_info("Testing database access after creation...")
            containers = self.client.list_containers()
            log_success(f"✓ Successfully connected to newly created database: {self.db_config.database}")
            return containers
        except Exception as e:
            log_error(f"Error testing database access after creation: {e}")
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

    def test_connection(self, connection_config: ConnectionConfig) -> None:
        """Main method to test CosmosDB connection and list containers."""
        # Display connection information
        self._display_connection_info(connection_config)

        # Initialize client
        self._initialize_client()

        # Test database access
        containers = self._test_database_access(connection_config)

        # Display containers
        self._display_containers(containers)
