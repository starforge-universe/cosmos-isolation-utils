"""
Core container dumping functionality for CosmosDB.
"""

import json
from pathlib import Path
from rich.table import Table

from .cosmos_client import CosmosDBClient
from .config import DatabaseConfig, DumpConfig
from .logging_utils import (
    log_info, log_success, log_error, log_warning, log_panel, console
)


def dump_containers(db_config: DatabaseConfig, dump_config: DumpConfig):
    """Dump all entries from multiple CosmosDB containers to a single JSON file."""

    try:
        # Initialize CosmosDB client
        client = CosmosDBClient(db_config)

        if dump_config.list_containers:
            containers_list = client.list_containers()
            table = Table(title="Available Containers")
            table.add_column("Container Name", style="cyan")
            table.add_column("Index", style="green")

            for i, container_name in enumerate(containers_list, 1):
                table.add_row(container_name, str(i))

            console.print(table)
            return

        # Determine which containers to dump
        if not dump_config.containers:
            log_error("Error: Please specify containers to dump using --containers option")
            log_info("Use --containers all to dump all containers")
            log_info(
                "Or use --containers 'container1,container2,container3' to dump "
                "specific containers"
            )
            raise Exception("No containers specified")

        available_containers = client.list_containers()

        if dump_config.containers.lower() == 'all':
            containers_to_dump = available_containers
            log_info(f"Dumping all {len(containers_to_dump)} containers")
        else:
            containers_to_dump = [c.strip() for c in dump_config.containers.split(',')]
            # Validate that all specified containers exist
            missing_containers = [
                c for c in containers_to_dump if c not in available_containers
            ]
            if missing_containers:
                log_error(f"Error: Containers not found: {', '.join(missing_containers)}")
                log_info(f"Available containers: {', '.join(available_containers)}")
                raise Exception(f"Containers not found: {', '.join(missing_containers)}")

        # Prepare output data structure for multiple containers
        output_data = {
            "database": db_config.database,
            "exported_at": (
                str(Path(dump_config.output_dir).stat().st_mtime)
                if Path(dump_config.output_dir).exists()
                else "N/A"
            ),
            "total_containers": len(containers_to_dump),
            "total_items": 0,
            "containers": []
        }

        # Process each container
        failed_containers = []
        for container_name in containers_to_dump:
            log_panel(f"[bold blue]Processing container: {container_name}[/bold blue]", style="blue")

            # Get container properties to extract partition key
            log_info(f"Extracting partition key information for {container_name}...")
            try:
                container_properties = client.get_container_properties(container_name)

                # Extract only partition key information
                partition_key = None
                if 'partitionKey' in container_properties:
                    partition_key = container_properties['partitionKey']
                    log_success(f"✓ Found partition key: {partition_key}")
                else:
                    log_warning(f"No partition key found for container '{container_name}'")

                # Get all items from the container
                items = client.get_all_items(container_name)

                if not items:
                    log_warning(f"No items found in container '{container_name}'")
                    # Still add container info even if empty
                    container_data = {
                        "name": container_name,
                        "total_items": 0,
                        "partition_key": partition_key,
                        "items": []
                    }
                    output_data["containers"].append(container_data)
                    continue

                # Prepare container data
                container_data = {
                    "name": container_name,
                    "total_items": len(items),
                    "partition_key": partition_key,
                    "items": items
                }

                output_data["containers"].append(container_data)
                output_data["total_items"] += len(items)

                log_success(
                    f"✓ Successfully processed container '{container_name}' with {len(items)} items"
                )

                # Show sample of exported data
                if items:
                    sample_item = items[0]
                    log_info(f"[bold]Sample item structure for {container_name}:[/bold]")
                    log_info(f"Keys: {list(sample_item.keys())}")
                    if 'id' in sample_item:
                        log_info(f"ID: {sample_item['id']}")
                    if 'type' in sample_item:
                        log_info(f"Type: {sample_item['type']}")
                    if partition_key and 'paths' in partition_key:
                        partition_paths = partition_key['paths']
                        log_info(f"Partition paths: {partition_paths}")

            except Exception as e:
                log_error(f"Error processing container '{container_name}': {e}")
                log_warning(
                    f"Skipping container '{container_name}' and continuing with others..."
                )
                failed_containers.append(container_name)
                continue

        # Check if we have any successful containers
        if not output_data["containers"]:
            log_error("Error: No containers were successfully processed!")
            raise Exception("No containers were successfully processed")

        # Ensure output directory exists
        output_path = Path(dump_config.output_dir)
        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            log_error(f"Error creating output directory: {e}")
            raise

        # Write to JSON file
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                if dump_config.pretty:
                    json.dump(output_data, f, indent=2, ensure_ascii=False, default=str)
                else:
                    json.dump(output_data, f, ensure_ascii=False, default=str)
        except Exception as e:
            log_error(f"Error writing to output file: {e}")
            raise

        # Display final summary
        log_panel("[bold green]Export Summary[/bold green]", style="green")
        log_success(
            f"Successfully exported {output_data['total_items']} items from "
            f"{len(output_data['containers'])} containers to {dump_config.output_dir}"
        )

        if failed_containers:
            failed_names = ', '.join(failed_containers)
            log_warning(
                f"Warning: {len(failed_containers)} containers failed to process: {failed_names}"
            )

        # Show container summary table
        table = Table(title="Container Export Summary")
        table.add_column("Container", style="cyan")
        table.add_column("Items", style="green")
        table.add_column("Partition Key", style="yellow")

        for container_data in output_data["containers"]:
            partition_key_info = "N/A"
            if container_data["partition_key"] and "paths" in container_data["partition_key"]:
                partition_key_info = str(container_data["partition_key"]["paths"])

            table.add_row(
                container_data["name"],
                str(container_data["total_items"]),
                partition_key_info
            )

        console.print(table)

        # Show warning if some containers failed
        if failed_containers:
            log_warning(
                f"Export completed with warnings. {len(failed_containers)} containers failed."
            )

    except Exception as e:
        log_error(f"Error: {e}")
        log_info("Please check your CosmosDB connection parameters and container names.")
        raise
