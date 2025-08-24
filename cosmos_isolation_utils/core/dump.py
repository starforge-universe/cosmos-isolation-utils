"""
Core container dumping functionality for CosmosDB.
"""

import json
import sys
from pathlib import Path
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from .cosmos_client import CosmosDBClient

console = Console()


def dump_containers(endpoint: str, key: str, database: str, allow_insecure: bool,
                   containers: str, output: str, batch_size: int, pretty: bool, list_containers: bool):
    """Dump all entries from multiple CosmosDB containers to a single JSON file."""

    try:
        # Initialize CosmosDB client
        client = CosmosDBClient(endpoint, key, database, allow_insecure)

        if list_containers:
            containers_list = client.list_containers()
            table = Table(title="Available Containers")
            table.add_column("Container Name", style="cyan")
            table.add_column("Index", style="green")

            for i, container_name in enumerate(containers_list, 1):
                table.add_row(container_name, str(i))

            console.print(table)
            return

        # Determine which containers to dump
        if not containers:
            console.print(
                "[red]Error: Please specify containers to dump using --containers option[/red]"
            )
            console.print("Use --containers all to dump all containers")
            console.print(
                "Or use --containers 'container1,container2,container3' to dump specific containers"
            )
            raise Exception("No containers specified")

        available_containers = client.list_containers()

        if containers.lower() == 'all':
            containers_to_dump = available_containers
            console.print(f"[cyan]Dumping all {len(containers_to_dump)} containers[/cyan]")
        else:
            containers_to_dump = [c.strip() for c in containers.split(',')]
            # Validate that all specified containers exist
            missing_containers = [
                c for c in containers_to_dump if c not in available_containers
            ]
            if missing_containers:
                console.print(
                    f"[red]Error: Containers not found: {', '.join(missing_containers)}[/red]"
                )
                console.print(f"Available containers: {', '.join(available_containers)}")
                raise Exception(f"Containers not found: {', '.join(missing_containers)}")

        # Prepare output data structure for multiple containers
        output_data = {
            "database": database,
            "exported_at": str(Path(output).stat().st_mtime) if Path(output).exists() else "N/A",
            "total_containers": len(containers_to_dump),
            "total_items": 0,
            "containers": []
        }

        # Process each container
        failed_containers = []
        for container_name in containers_to_dump:
            console.print(Panel(f"[bold blue]Processing container: {container_name}[/bold blue]"))

            # Get container properties to extract partition key
            console.print(f"[cyan]Extracting partition key information for {container_name}...[/cyan]")
            try:
                container_properties = client.get_container_properties(container_name)

                # Extract only partition key information
                partition_key = None
                if 'partitionKey' in container_properties:
                    partition_key = container_properties['partitionKey']
                    console.print(f"[green]✓ Found partition key: {partition_key}[/green]")
                else:
                    console.print(
                        f"[yellow]No partition key found for container '{container_name}'[/yellow]"
                    )

                # Get all items from the container
                items = client.get_all_items(container_name)

                if not items:
                    console.print(f"[yellow]No items found in container '{container_name}'[/yellow]")
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

                console.print(
                    f"[green]✓ Successfully processed container '{container_name}' with {len(items)} items[/green]"
                )

                # Show sample of exported data
                if items:
                    sample_item = items[0]
                    console.print(f"[bold]Sample item structure for {container_name}:[/bold]")
                    console.print(f"Keys: {list(sample_item.keys())}")
                    if 'id' in sample_item:
                        console.print(f"ID: {sample_item['id']}")
                    if 'type' in sample_item:
                        console.print(f"Type: {sample_item['type']}")
                    if partition_key and 'paths' in partition_key:
                        partition_paths = partition_key['paths']
                        console.print(f"Partition paths: {partition_paths}")

            except Exception as e:
                console.print(f"[red]Error processing container '{container_name}': {e}[/red]")
                console.print(
                    f"[yellow]Skipping container '{container_name}' and continuing with others...[/yellow]"
                )
                failed_containers.append(container_name)
                continue

        # Check if we have any successful containers
        if not output_data["containers"]:
            console.print("[red]Error: No containers were successfully processed![/red]")
            raise Exception("No containers were successfully processed")

        # Ensure output directory exists
        output_path = Path(output)
        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            console.print(f"[red]Error creating output directory: {e}[/red]")
            raise

        # Write to JSON file
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                if pretty:
                    json.dump(output_data, f, indent=2, ensure_ascii=False, default=str)
                else:
                    json.dump(output_data, f, ensure_ascii=False, default=str)
        except Exception as e:
            console.print(f"[red]Error writing to output file: {e}[/red]")
            raise

        # Display final summary
        console.print(Panel("[bold green]Export Summary[/bold green]"))
        console.print(
            f"[green]Successfully exported {output_data['total_items']} items from "
            f"{len(output_data['containers'])} containers to {output}[/green]"
        )

        if failed_containers:
            failed_names = ', '.join(failed_containers)
            console.print(
                f"[yellow]Warning: {len(failed_containers)} containers failed to process: {failed_names}[/yellow]"
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
            console.print(
                f"\n[yellow]Export completed with warnings. {len(failed_containers)} containers failed.[/yellow]"
            )

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        console.print("Please check your CosmosDB connection parameters and container names.")
        raise
