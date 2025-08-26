#!/usr/bin/env python3
"""
Main entry point for the cosmos-isolation-utils CLI tool.

This module provides a unified command-line interface for all CosmosDB isolation utilities.
"""

import sys
import click

from .core import (
    test_connection,
    dump_containers,
    upload_entries,
    delete_database,
    DatabaseConfig,
    UploadConfig,
    DumpConfig,
    DeleteConfig,
    StatusConfig,
    ConnectionConfig
)
from .core.status import ContainerStatusAnalyzer
from .core.logging_utils import log_bold, log_error


@click.group()
@click.version_option(version="0.1.0", prog_name="cosmos-isolation-utils")
@click.option('--endpoint', '-e', required=True,
              help='CosmosDB endpoint URL')
@click.option('--key', '-k', required=True,
              help='CosmosDB primary key')
@click.option('--database', '-d', required=True,
              help='CosmosDB database name')
@click.option('--allow-insecure', '-a', is_flag=True,
              help='Allow insecure HTTPS requests (suppress warnings)')
@click.pass_context
def main(ctx, endpoint: str, key: str, database: str, allow_insecure: bool):
    """
    CosmosDB Isolation Utilities - A unified CLI for managing CosmosDB databases.
    
    This tool provides various utilities for testing, monitoring, and managing
    CosmosDB databases in isolation environments.
    """
    # Store common parameters in context
    ctx.ensure_object(dict)
    ctx.obj['endpoint'] = endpoint
    ctx.obj['key'] = key
    ctx.obj['database'] = database
    ctx.obj['allow_insecure'] = allow_insecure


@main.command()
@click.option('--create-database', is_flag=True,
              help='Create database if it does not exist')
@click.option('--force', '-f', is_flag=True,
              help='Skip confirmation prompts')
@click.pass_context
def test(ctx, create_database: bool, force: bool):
    """Test CosmosDB connection and list containers."""
    log_bold("Testing CosmosDB Connection", color="blue")
    try:
        # Create configuration objects
        db_config = DatabaseConfig(
            endpoint=ctx.obj['endpoint'],
            key=ctx.obj['key'],
            database=ctx.obj['database'],
            allow_insecure=ctx.obj['allow_insecure']
        )
        connection_config = ConnectionConfig(
            create_database=create_database,
            force=force
        )

        test_connection(db_config, connection_config)
    except Exception as e:
        log_error(f"Connection test failed: {e}")
        sys.exit(1)


@main.command()
@click.option('--detailed', is_flag=True,
              help='Show detailed information for each container')
@click.pass_context
def status(ctx, detailed: bool):
    """Show the status and statistics of all containers in a CosmosDB database."""
    try:
        # Create configuration objects
        db_config = DatabaseConfig(
            endpoint=ctx.obj['endpoint'],
            key=ctx.obj['key'],
            database=ctx.obj['database'],
            allow_insecure=ctx.obj['allow_insecure']
        )
        status_config = StatusConfig(
            detailed=detailed
        )

        # Create analyzer instance and run analysis
        analyzer = ContainerStatusAnalyzer(db_config, status_config)
        analyzer.analyze()
    except Exception as e:
        log_error(f"Failed to get container status: {e}")
        sys.exit(1)


@main.command()
@click.option('--containers', '-c',
              help='Comma-separated list of container names to dump (or "all" for all containers)')
@click.option('--output', '-o', required=True,
              help='Output JSON file path')
@click.option('--batch-size', '-b', default=100,
              help='Batch size for processing (default: 100)')
@click.option('--pretty', '-p', is_flag=True,
              help='Pretty print JSON output')
@click.option('--list-containers', '-l', is_flag=True,
              help='List all available containers')
@click.pass_context
def dump(ctx, containers: str, output: str, batch_size: int,  # pylint: disable=too-many-arguments,too-many-positional-arguments
         pretty: bool, list_containers: bool):
    """Dump all entries from multiple CosmosDB containers to a single JSON file."""
    try:
        # Create configuration objects
        db_config = DatabaseConfig(
            endpoint=ctx.obj['endpoint'],
            key=ctx.obj['key'],
            database=ctx.obj['database'],
            allow_insecure=ctx.obj['allow_insecure']
        )
        dump_config = DumpConfig(
            output_dir=output,
            containers=containers,
            batch_size=batch_size,
            pretty=pretty,
            list_containers=list_containers
        )

        dump_containers(db_config, dump_config)
    except Exception as e:
        log_error(f"Failed to dump containers: {e}")
        sys.exit(1)


@main.command()
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
@click.pass_context
def upload(ctx, input_file: str, batch_size: int, upsert: bool, dry_run: bool,  # pylint: disable=too-many-arguments,too-many-positional-arguments
           force: bool, create_containers: bool, containers: str):
    """Upload entries from a multi-container JSON file to CosmosDB containers."""
    try:
        # Create configuration objects
        db_config = DatabaseConfig(
            endpoint=ctx.obj['endpoint'],
            key=ctx.obj['key'],
            database=ctx.obj['database'],
            allow_insecure=ctx.obj['allow_insecure']
        )
        upload_config = UploadConfig(
            input_file=input_file,
            batch_size=batch_size,
            upsert=upsert,
            dry_run=dry_run,
            force=force,
            create_containers=create_containers,
            containers=containers
        )

        upload_entries(db_config, upload_config)
    except Exception as e:
        log_error(f"Failed to upload entries: {e}")
        sys.exit(1)


@main.command()
@click.option('--list-databases', '-l', is_flag=True,
              help='List all existing databases')
@click.option('--force', '-f', is_flag=True,
              help='Skip confirmation prompts for deletion')
@click.pass_context
def delete_db(ctx, list_databases: bool, force: bool):
    """Delete CosmosDB databases with safety confirmations."""
    try:
        # Create configuration objects
        db_config = DatabaseConfig(
            endpoint=ctx.obj['endpoint'],
            key=ctx.obj['key'],
            database="",  # Not used for delete operations
            allow_insecure=ctx.obj['allow_insecure']
        )
        delete_config = DeleteConfig(
            force=force,
            list_only=list_databases
        )

        delete_database(db_config, delete_config)
    except Exception as e:
        log_error(f"Failed to delete database: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
