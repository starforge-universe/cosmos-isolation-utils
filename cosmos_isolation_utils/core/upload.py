"""
Core container upload functionality for CosmosDB.
"""

import json
from pathlib import Path
from azure.cosmos import PartitionKey
from azure.cosmos.exceptions import CosmosHttpResponseError
from rich.table import Table
from rich.prompt import Confirm

from .cosmos_client import CosmosDBClient
from .logging_utils import (
    log_info, log_success, log_warning, log_error, log_panel,
    log_checkmark, log_upload_summary, log_results_summary, console
)
from .config import DatabaseConfig, UploadConfig


def _calculate_total_items(containers_to_process):
    """Calculate total items across all containers."""
    return sum(c['total_items'] for c in containers_to_process)


def _format_container_list(containers):
    """Format a list of container names for display."""
    return ', '.join(containers)


def _create_partition_key(paths):
    """Create a PartitionKey object from paths."""
    if isinstance(paths, str):
        paths = [paths]
    elif not isinstance(paths, list):
        raise ValueError(f"Invalid partition key paths format: {paths}")

    if len(paths) == 1:
        return PartitionKey(path=paths[0])
    return PartitionKey(paths=paths)


def upload_entries(db_config: DatabaseConfig, upload_config: UploadConfig):
    """Upload entries from a multi-container JSON file to CosmosDB containers."""

    try:
        # Check if input file exists
        input_path = Path(upload_config.input_file)
        if not input_path.exists():
            log_error(f"Error: Input file '{upload_config.input_file}' not found!")
            raise Exception(f"Input file '{upload_config.input_file}' not found")

        # Load JSON data first to check structure
        try:
            with open(input_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            log_error(f"Error: Invalid JSON file: {e}")
            raise Exception(f"Invalid JSON file: {e}")

        # Check if this is a multi-container dump file
        if 'containers' in data and isinstance(data['containers'], list):
            log_info(f"Found multi-container dump file with {len(data['containers'])} containers")
            log_info(f"Database: {data.get('database', 'N/A')}")
            log_info(f"Total items: {data.get('total_items', 0)}")

            # Determine which containers to process
            if upload_config.containers:
                # User specified specific containers
                target_containers = [c.strip() for c in upload_config.containers.split(',')]
                available_containers = [c['name'] for c in data['containers']]
                missing_containers = [c for c in target_containers if c not in available_containers]

                if missing_containers:
                    log_error(f"Error: Specified containers not found in JSON: {', '.join(missing_containers)}")
                    log_info(f"Available containers in JSON: {', '.join(available_containers)}")
                    raise Exception(f"Specified containers not found in JSON: {', '.join(missing_containers)}")

                containers_to_process = [c for c in data['containers'] if c['name'] in target_containers]
                log_info(f"Processing specified containers: {', '.join(target_containers)}")
            else:
                # Process all containers in the JSON
                containers_to_process = data['containers']
                log_info("Processing all containers from JSON")
        else:
            # Legacy single-container format support
            log_warning("Detected legacy single-container format, converting to multi-container format")
            if 'container' in data and 'items' in data:
                containers_to_process = [{
                    'name': data['container'],
                    'total_items': len(data['items']),
                    'partition_key': data.get('partition_key'),
                    'items': data['items']
                }]
                log_info(f"Converted legacy format: 1 container with {len(data['items'])} items")
            else:
                raise Exception("Invalid JSON structure. Expected 'containers' array or legacy format.")

        # Initialize CosmosDB client
        try:
            client = CosmosDBClient(db_config)
        except Exception as e:
            log_error(f"Error initializing CosmosDB client: {e}")
            log_warning("Please check your connection parameters and ensure the database exists.")
            raise

        # Check database existence and create if needed
        try:
            available_containers = client.list_containers()
        except CosmosHttpResponseError as e:
            if "Owner resource does not exist" in str(e) or "NotFound" in str(e):
                log_warning(f"Database '{db_config.database}' does not exist or is not accessible.")
                if not upload_config.force and not upload_config.dry_run:
                    if Confirm.ask(f"Do you want to create database '{db_config.database}' first?"):
                        try:
                            client.client.create_database_if_not_exists(db_config.database)
                            log_checkmark(f"Database '{db_config.database}' created successfully")
                            available_containers = []
                        except Exception as e2:
                            log_error(f"Error creating database '{db_config.database}': {e2}")
                            raise
                    else:
                        log_warning("Database creation cancelled. Cannot proceed without database.")
                        raise Exception("Database creation cancelled")
                else:
                    # Force mode - try to create database
                    try:
                        client.client.create_database_if_not_exists(db_config.database)
                        log_checkmark(f"Database '{db_config.database}' created successfully")
                        available_containers = []
                    except Exception as e2:
                        log_error(f"Error creating database '{db_config.database}': {e2}")
                        raise
            else:
                log_error(f"Error listing containers: {e}")
                raise

        # Display upload summary
        total_items = _calculate_total_items(containers_to_process)
        mode = 'Upsert' if upload_config.upsert else 'Create'
        log_upload_summary(
            database=db_config.database,
            container_count=len(containers_to_process),
            total_items=total_items,
            batch_size=upload_config.batch_size,
            mode=mode,
            dry_run=upload_config.dry_run,
            create_containers=upload_config.create_containers
        )

        # Show container details table
        table = Table(title="Container Details")
        table.add_column("Container", style="cyan")
        table.add_column("Items", style="green")
        table.add_column("Partition Key", style="yellow")
        table.add_column("Status", style="blue")

        for container_data in containers_to_process:
            partition_key_info = "N/A"
            if container_data.get("partition_key") and "paths" in container_data["partition_key"]:
                partition_key_info = str(container_data["partition_key"]["paths"])

            status = "Will create" if container_data["name"] not in available_containers else "Exists"
            table.add_row(
                container_data["name"],
                str(container_data["total_items"]),
                partition_key_info,
                status
            )

        console.print(table)

        # Confirmation prompt (unless force flag is used)
        if not upload_config.force and not upload_config.dry_run:
            if not Confirm.ask("Do you want to proceed with the upload?"):
                log_warning("Upload cancelled.")
                return  # User cancelled, exit cleanly

        if upload_config.dry_run:
            total_items = _calculate_total_items(containers_to_process)
            log_success(
                f"Dry run completed. Would upload {total_items} items to {len(containers_to_process)} containers"
            )
            return

        # Process each container
        total_uploaded = 0
        successful_containers = []
        failed_containers = []

        for container_data in containers_to_process:
            container_name = container_data["name"]
            items = container_data.get("items", [])
            partition_key = container_data.get("partition_key")

            log_panel(f"Processing container: {container_name}", style="blue")

            # Check if container exists
            container_exists = container_name in available_containers

            if not container_exists:
                if upload_config.create_containers:
                    log_warning(f"Container '{container_name}' not found. Creating new container...")

                    if not upload_config.force:
                        if not Confirm.ask(f"Do you want to create container '{container_name}'?"):
                            log_warning(f"Skipping container '{container_name}'")
                            failed_containers.append(container_name)
                            continue

                    try:
                        # Create the container with partition key if available
                        if partition_key and 'paths' in partition_key:
                            log_info(f"Creating container with partition key: {partition_key['paths']}")
                            try:
                                pk = _create_partition_key(partition_key['paths'])

                                client.database.create_container(id=container_name, partition_key=pk)
                                log_checkmark(f"Successfully created container '{container_name}' with partition key")
                                container_exists = True
                            except Exception as pk_error:
                                log_warning(f"Warning: Failed to create container with partition key: {pk_error}")
                                log_warning("Attempting to create container with simple partition key...")

                                try:
                                    simple_pk = PartitionKey(path="pk")
                                    client.database.create_container(id=container_name, partition_key=simple_pk)
                                    log_checkmark(
                                        f"Successfully created container '{container_name}'"
                                        " with simple partition key 'pk'"
                                    )
                                    log_warning(
                                        "Note: Container created with partition key 'pk'. "
                                        "You may need to add this field to your documents."
                                    )
                                    container_exists = True
                                except Exception as simple_error:
                                    log_error(f"Error: Cannot create container '{container_name}': {simple_error}")
                                    failed_containers.append(container_name)
                                    continue
                        else:
                            log_info("Creating container with default partition key")
                            try:
                                client.database.create_container(
                                    id=container_name,
                                    partition_key=PartitionKey(path="/id")
                                )
                                log_checkmark(f"Successfully created container '{container_name}'")
                                container_exists = True
                            except Exception as e:
                                log_error(f"Error creating container '{container_name}': {e}")
                                failed_containers.append(container_name)
                                continue
                    except Exception as e:
                        log_error(f"Error creating container '{container_name}': {e}")
                        failed_containers.append(container_name)
                        continue
                else:
                    log_error(f"Error: Container '{container_name}' not found!")
                    log_warning("Use --create-containers flag to automatically create missing containers.")
                    failed_containers.append(container_name)
                    continue

            # Upload items to the container
            if items:
                log_info(f"Uploading {len(items)} items to container '{container_name}'...")

                try:
                    if upload_config.upsert:
                        uploaded_items = client.upsert_items_batch(container_name, items, upload_config.batch_size)
                    else:
                        uploaded_items = client.create_items_batch(container_name, items, upload_config.batch_size)

                    log_checkmark(f"Successfully uploaded {len(uploaded_items)} items to container '{container_name}'")
                    total_uploaded += len(uploaded_items)
                    successful_containers.append(container_name)

                except Exception as e:
                    log_error(f"Error uploading items to container '{container_name}': {e}")
                    failed_containers.append(container_name)
                    continue
            else:
                log_warning(f"No items to upload for container '{container_name}'")
                successful_containers.append(container_name)

        # Display final results
        log_results_summary(
            total_uploaded=total_uploaded,
            successful_count=len(successful_containers),
            failed_count=len(failed_containers)
        )

        if successful_containers:
            successful_list = _format_container_list(successful_containers)
            log_success(f"Successfully processed containers: {successful_list}")

        if failed_containers:
            failed_list = _format_container_list(failed_containers)
            log_error(f"Failed containers: {failed_list}")
            log_warning("You may need to check the errors above and manually create these containers.")

        # Check results
        if not successful_containers:
            log_error("Error: No containers were successfully processed!")
            raise Exception("No containers were successfully processed")
        elif failed_containers:
            log_warning(f"Upload completed with warnings. {len(failed_containers)} containers failed.")
        else:
            log_success("All containers processed successfully!")

    except Exception as e:
        log_error(f"Error: {e}")
        log_warning("Please check your CosmosDB connection parameters and container names.")
        raise
