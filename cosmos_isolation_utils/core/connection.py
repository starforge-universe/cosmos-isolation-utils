"""
Core connection testing functionality for CosmosDB.
"""

from azure.cosmos.exceptions import CosmosHttpResponseError
from rich.prompt import Confirm

from .cosmos_client import CosmosDBClient
from .config import DatabaseConfig, ConnectionConfig
from .logging_utils import (
    log_info, log_success, log_error, log_warning, log_step, log_checkmark
)


def test_connection(db_config: DatabaseConfig, connection_config: ConnectionConfig):
    """Test CosmosDB connection and list containers."""
    log_info(f"Endpoint: {db_config.endpoint}")
    log_info(f"Database: {db_config.database}")
    log_info(f"Allow insecure: {db_config.allow_insecure}")
    log_info(f"Create database if missing: {connection_config.create_database}")

    log_step(1, "Initializing CosmosDB client...")
    # Test connection using our custom client
    try:
        client = CosmosDBClient(db_config)
        log_checkmark("CosmosDB client initialized")
    except Exception as e:
        log_error(f"Error initializing client: {e}")
        raise

    log_step(2, "Testing database access...")
    # Test if we can access the database by listing containers
    log_info("  Attempting to list containers...")

    try:
        containers = client.list_containers()
        log_success(f"✓ Successfully connected to database: {db_config.database}")

        log_step(3, "Listing containers...")
        if containers:
            log_info(f"\n[bold]Available containers ({len(containers)}):[/bold]")
            for i, container in enumerate(containers, 1):
                log_info(f"  {i}. {container}")
        else:
            log_warning("No containers found in the database.")

        log_success("Connection test completed successfully!")

    except CosmosHttpResponseError as e:
        if "Owner resource does not exist" in str(e) or "NotFound" in str(e):
            log_warning(
                f"Database '{db_config.database}' does not exist or "
                f"is not accessible."
            )

            if connection_config.create_database:
                if not connection_config.force:
                    if not Confirm.ask(f"Do you want to create database '{db_config.database}'?"):
                        log_warning("Database creation cancelled.")
                        return  # User cancelled, exit cleanly

                try:
                    log_info(f"Creating database '{db_config.database}'...")
                    client.client.create_database_if_not_exists(db_config.database)
                    log_checkmark(f"Database '{db_config.database}' created successfully")

                    # Try listing containers again
                    log_info("Testing database access after creation...")
                    containers = client.list_containers()
                    log_success(
                        f"✓ Successfully connected to newly created "
                        f"database: {db_config.database}"
                    )

                    if containers:
                        log_info(f"\n[bold]Available containers ({len(containers)}):[/bold]")
                        for i, container in enumerate(containers, 1):
                            log_info(f"  {i}. {container}")
                    else:
                        log_warning("No containers found in the new database.")

                    log_success("Connection test completed successfully!")

                except Exception as e2:
                    log_error(f"Error creating database '{db_config.database}': {e2}")
                    raise
            else:
                log_error(
                    f"Database '{db_config.database}' does not exist. "
                    f"Use --create-database flag to create it."
                )
                raise Exception(f"Database '{db_config.database}' does not exist") from e
        else:
            log_error(f"Error accessing database: {e}")
            raise
