#!/usr/bin/env python3
"""
Main entry point for the cosmos-isolation-utils CLI tool.

This module provides a unified command-line interface for all CosmosDB isolation utilities.
"""

import os
import sys
import click

from cosmos_isolation_utils.core import (
    ConnectionTester,
    ContainerDumper,
    ContainerUploader,
    DatabaseDeleter,
    DatabaseConfig,
    UploadConfig,
    DumpConfig,
    DeleteConfig,
    StatusConfig,
    ConnectionConfig
)
from cosmos_isolation_utils.core.status import ContainerStatusAnalyzer
from cosmos_isolation_utils.core.logging_utils import log_bold, log_error


@click.group()
@click.version_option(version="0.1.0", prog_name="cosmos-isolation-utils")
@click.pass_context
def main(ctx):
    """
    CosmosDB Isolation Utilities - A unified CLI for managing CosmosDB databases.
    
    This tool provides various utilities for testing, monitoring, and managing
    CosmosDB databases in isolation environments.
    
    Connection parameters can be specified via command line options or environment variables:
    - COSMOS_ENDPOINT: CosmosDB endpoint URL
    - COSMOS_KEY: CosmosDB primary key
    - COSMOS_DATABASE: CosmosDB database name
    """
    # Initialize context
    ctx.ensure_object(dict)


def _create_database_config(endpoint: str, key: str, database: str, allow_insecure: bool) -> DatabaseConfig:
    """
    Create a DatabaseConfig instance from command line parameters or environment variables.
    
    Args:
        endpoint: Command line endpoint parameter (or None)
        key: Command line key parameter (or None)
        database: Command line database parameter (or None)
        allow_insecure: Command line allow_insecure parameter
        
    Returns:
        DatabaseConfig instance with validated connection parameters
        
    Raises:
        SystemExit: If any required connection parameters are missing
    """
    # Get values from command line or environment variables
    final_endpoint = endpoint or os.environ.get('COSMOS_ENDPOINT')
    final_key = key or os.environ.get('COSMOS_KEY')
    final_database = database or os.environ.get('COSMOS_DATABASE')

    # Validate that all required parameters are available
    missing_params = []
    if not final_endpoint:
        missing_params.append('endpoint (--endpoint/-e or COSMOS_ENDPOINT)')
    if not final_key:
        missing_params.append('key (--key/-k or COSMOS_KEY)')
    if not final_database:
        missing_params.append('database (--database/-d or COSMOS_DATABASE)')

    if missing_params:
        log_error("Missing required connection parameters:")
        for param in missing_params:
            log_error(f"  - {param}")
        log_error("\nPlease specify these parameters via command line options or environment variables.")
        sys.exit(1)

    return DatabaseConfig(
        endpoint=final_endpoint,
        key=final_key,
        database=final_database,
        allow_insecure=allow_insecure
    )


@main.command()
@click.option('--endpoint', '-e',
              help='CosmosDB endpoint URL (or set COSMOS_ENDPOINT env var)')
@click.option('--key', '-k',
              help='CosmosDB primary key (or set COSMOS_KEY env var)')
@click.option('--database', '-d',
              help='CosmosDB database name (or set COSMOS_DATABASE env var)')
@click.option('--allow-insecure', '-a', is_flag=True,
              help='Allow insecure HTTPS requests (suppress warnings)')
@click.option('--create-database', is_flag=True,
              help='Create database if it does not exist')
@click.option('--force', '-f', is_flag=True,
              help='Skip confirmation prompts')
def test(endpoint: str, key: str, database: str, allow_insecure: bool, create_database: bool, force: bool):  # pylint: disable=too-many-arguments,too-many-positional-arguments
    """Test CosmosDB connection and list containers."""
    log_bold("Testing CosmosDB Connection", color="blue")
    try:
        # Create configuration objects
        db_config = _create_database_config(endpoint, key, database, allow_insecure)
        connection_config = ConnectionConfig(
            create_database=create_database,
            force=force
        )

        # Create connection tester instance and run test
        tester = ConnectionTester(db_config)
        tester.test_connection(connection_config)
    except Exception as e:
        log_error(f"Connection test failed: {e}")
        sys.exit(1)


@main.command()
@click.option('--endpoint', '-e',
              help='CosmosDB endpoint URL (or set COSMOS_ENDPOINT env var)')
@click.option('--key', '-k',
              help='CosmosDB primary key (or set COSMOS_KEY env var)')
@click.option('--database', '-d',
              help='CosmosDB database name (or set COSMOS_DATABASE env var)')
@click.option('--allow-insecure', '-a', is_flag=True,
              help='Allow insecure HTTPS requests (suppress warnings)')
@click.option('--detailed', is_flag=True,
              help='Show detailed information for each container')
def status(endpoint: str, key: str, database: str, allow_insecure: bool, detailed: bool):  # pylint: disable=too-many-arguments,too-many-positional-arguments
    """Show the status and statistics of all containers in a CosmosDB database."""
    try:
        # Create configuration objects
        db_config = _create_database_config(endpoint, key, database, allow_insecure)
        status_config = StatusConfig(
            detailed=detailed
        )

        # Create analyzer instance and run analysis
        analyzer = ContainerStatusAnalyzer(db_config)
        analyzer.analyze(status_config)
    except Exception as e:
        log_error(f"Failed to get container status: {e}")
        sys.exit(1)


@main.command()
@click.option('--endpoint', '-e',
              help='CosmosDB endpoint URL (or set COSMOS_ENDPOINT env var)')
@click.option('--key', '-k',
              help='CosmosDB primary key (or set COSMOS_KEY env var)')
@click.option('--database', '-d',
              help='CosmosDB database name (or set COSMOS_DATABASE env var)')
@click.option('--allow-insecure', '-a', is_flag=True,
              help='Allow insecure HTTPS requests (suppress warnings)')
@click.option('--containers', '-c',
              help='Comma-separated list of container names to dump (or "all" for all containers)')
@click.option('--output', '-o', required=True,
              help='Output JSON file path')
@click.option('--batch-size', '-b', default=100,
              help='Batch size for processing (default: 100)')
@click.option('--pretty', '-p', is_flag=True,
              help='Pretty print JSON output')
def dump(endpoint: str, key: str, database: str, allow_insecure: bool,  # pylint: disable=too-many-arguments,too-many-positional-arguments
         containers: str, output: str, batch_size: int, pretty: bool):
    """Dump all entries from multiple CosmosDB containers to a single JSON file."""
    try:
        # Create configuration objects
        db_config = _create_database_config(endpoint, key, database, allow_insecure)
        dump_config = DumpConfig(
            output_dir=output,
            containers=containers,
            batch_size=batch_size,
            pretty=pretty
        )

        # Create container dumper instance and run dump
        dumper = ContainerDumper(db_config)
        dumper.dump_containers(dump_config)
    except Exception as e:
        log_error(f"Failed to dump containers: {e}")
        sys.exit(1)


@main.command()
@click.option('--endpoint', '-e',
              help='CosmosDB endpoint URL (or set COSMOS_ENDPOINT env var)')
@click.option('--key', '-k',
              help='CosmosDB primary key (or set COSMOS_KEY env var)')
@click.option('--database', '-d',
              help='CosmosDB database name (or set COSMOS_DATABASE env var)')
@click.option('--allow-insecure', '-a', is_flag=True,
              help='Allow insecure HTTPS requests (suppress warnings)')
@click.option('--input', '-i', required=True,
              help='Input JSON file path')
@click.option('--batch-size', '-b', default=100,
              help='Batch size for processing (default: 100)')
@click.option('--upsert', '-u', is_flag=True,
              help='Use upsert instead of create (overwrites existing items)')
@click.option('--dry-run', '-r', is_flag=True,
              help='Show what would be uploaded without actually uploading')
@click.option('--force', '-f', is_flag=True,
              help='Skip confirmation prompts')
@click.option('--create-containers', is_flag=True,
              help='Automatically create containers if they do not exist')
@click.option('--containers', '-c',
              help='Comma-separated list of specific containers to upload')
def upload(endpoint: str, key: str, database: str, allow_insecure: bool,  # pylint: disable=too-many-arguments,too-many-positional-arguments
           input_file: str, batch_size: int, upsert: bool, dry_run: bool,
           force: bool, create_containers: bool, containers: str):
    """Upload entries from a multi-container JSON file to CosmosDB containers."""
    try:
        # Create configuration objects
        db_config = _create_database_config(endpoint, key, database, allow_insecure)
        upload_config = UploadConfig(
            input_file=input_file,
            batch_size=batch_size,
            upsert=upsert,
            dry_run=dry_run,
            force=force,
            create_containers=create_containers,
            containers=containers
        )

        # Create container uploader instance and run upload
        uploader = ContainerUploader(db_config)
        uploader.upload_entries(upload_config)
    except Exception as e:
        log_error(f"Failed to upload entries: {e}")
        sys.exit(1)


@main.command()
@click.option('--endpoint', '-e',
              help='CosmosDB endpoint URL (or set COSMOS_ENDPOINT env var)')
@click.option('--key', '-k',
              help='CosmosDB primary key (or set COSMOS_KEY env var)')
@click.option('--database', '-d',
              help='CosmosDB database name (or set COSMOS_DATABASE env var)')
@click.option('--allow-insecure', '-a', is_flag=True,
              help='Allow insecure HTTPS requests (suppress warnings)')
@click.option('--list-databases', '-l', is_flag=True,
              help='List all existing databases')
@click.option('--force', '-f', is_flag=True,
              help='Skip confirmation prompts for deletion')
def delete_db(endpoint: str, key: str, database: str, allow_insecure: bool, list_databases: bool, force: bool):  # pylint: disable=too-many-arguments,too-many-positional-arguments
    """Delete CosmosDB databases with safety confirmations."""
    try:
        # Create configuration objects
        db_config = _create_database_config(endpoint, key, database, allow_insecure)
        delete_config = DeleteConfig(
            force=force,
            list_only=list_databases
        )

        # Create database deleter instance and run delete operation
        deleter = DatabaseDeleter(db_config)
        deleter.delete_database(delete_config)
    except Exception as e:
        log_error(f"Failed to delete database: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
