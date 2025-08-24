"""
Core container upload functionality for CosmosDB.
"""

import json
import sys
from pathlib import Path
from azure.cosmos import PartitionKey
from azure.cosmos.exceptions import CosmosHttpResponseError
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Confirm

from .cosmos_client import CosmosDBClient

console = Console()


def upload_entries(endpoint: str, key: str, database: str, allow_insecure: bool,
                  input: str, batch_size: int, upsert: bool, dry_run: bool, 
                  force: bool, create_containers: bool, containers: str):
    """Upload entries from a multi-container JSON file to CosmosDB containers."""

    try:
        # Check if input file exists
        input_path = Path(input)
        if not input_path.exists():
            console.print(f"[red]Error: Input file '{input}' not found![/red]")
            raise Exception(f"Input file '{input}' not found")

        # Load JSON data first to check structure
        try:
            with open(input_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            console.print(f"[red]Error: Invalid JSON file: {e}[/red]")
            raise Exception(f"Invalid JSON file: {e}")

        # Check if this is a multi-container dump file
        if 'containers' in data and isinstance(data['containers'], list):
            console.print(f"[cyan]Found multi-container dump file with {len(data['containers'])} containers[/cyan]")
            console.print(f"[cyan]Database: {data.get('database', 'N/A')}[/cyan]")
            console.print(f"[cyan]Total items: {data.get('total_items', 0)}[/cyan]")

            # Determine which containers to process
            if containers:
                # User specified specific containers
                target_containers = [c.strip() for c in containers.split(',')]
                available_containers = [c['name'] for c in data['containers']]
                missing_containers = [c for c in target_containers if c not in available_containers]

                if missing_containers:
                    console.print(f"[red]Error: Specified containers not found in JSON: {', '.join(missing_containers)}[/red]")
                    console.print(f"Available containers in JSON: {', '.join(available_containers)}")
                    raise Exception(f"Specified containers not found in JSON: {', '.join(missing_containers)}")

                containers_to_process = [c for c in data['containers'] if c['name'] in target_containers]
                console.print(f"[cyan]Processing specified containers: {', '.join(target_containers)}[/cyan]")
            else:
                # Process all containers in the JSON
                containers_to_process = data['containers']
                console.print(f"[cyan]Processing all containers from JSON[/cyan]")
        else:
            # Legacy single-container format support
            console.print("[yellow]Detected legacy single-container format, converting to multi-container format[/yellow]")
            if 'container' in data and 'items' in data:
                containers_to_process = [{
                    'name': data['container'],
                    'total_items': len(data['items']),
                    'partition_key': data.get('partition_key'),
                    'items': data['items']
                }]
                console.print(f"[cyan]Converted legacy format: 1 container with {len(data['items'])} items[/cyan]")
            else:
                console.print(f"[red]Error: Invalid JSON structure. Expected 'containers' array or legacy format.[/red]")
                raise Exception("Invalid JSON structure. Expected 'containers' array or legacy format.")

        # Initialize CosmosDB client
        try:
            client = CosmosDBClient(endpoint, key, database, allow_insecure)
        except Exception as e:
            console.print(f"[red]Error initializing CosmosDB client: {e}[/red]")
            console.print("Please check your connection parameters and ensure the database exists.")
            raise

        # Check database existence and create if needed
        try:
            available_containers = client.list_containers()
        except CosmosHttpResponseError as e:
            if "Owner resource does not exist" in str(e) or "NotFound" in str(e):
                console.print(f"[yellow]Database '{database}' does not exist or is not accessible.[/yellow]")
                if not force and not dry_run:
                    if Confirm.ask(f"Do you want to create database '{database}' first?"):
                        try:
                            client.client.create_database_if_not_exists(database)
                            console.print(f"[green]✓ Database '{database}' created successfully[/green]")
                            available_containers = []
                        except Exception as e2:
                            console.print(f"[red]Error creating database '{database}': {e2}[/red]")
                            raise
                    else:
                        console.print("[yellow]Database creation cancelled. Cannot proceed without database.[/yellow]")
                        raise Exception("Database creation cancelled")
                else:
                    # Force mode - try to create database
                    try:
                        client.client.create_database_if_not_exists(database)
                        console.print(f"[green]✓ Database '{database}' created successfully[/green]")
                        available_containers = []
                    except Exception as e2:
                        console.print(f"[red]Error creating database '{database}': {e2}[/red]")
                        raise
            else:
                console.print(f"[red]Error listing containers: {e}[/red]")
                raise

        # Display upload summary
        console.print(Panel(f"[bold blue]Upload Summary[/bold blue]"))
        console.print(f"Database: {database}")
        console.print(f"Containers to process: {len(containers_to_process)}")
        console.print(f"Total items to upload: {sum(c['total_items'] for c in containers_to_process)}")
        console.print(f"Batch size: {batch_size}")
        console.print(f"Mode: {'Upsert' if upsert else 'Create'}")
        console.print(f"Dry run: {'Yes' if dry_run else 'No'}")
        console.print(f"Create containers: {'Yes' if create_containers else 'No'}")

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
        if not force and not dry_run:
            if not Confirm.ask("Do you want to proceed with the upload?"):
                console.print("[yellow]Upload cancelled.[/yellow]")
                return  # User cancelled, exit cleanly

        if dry_run:
            console.print(f"\n[green]Dry run completed. Would upload {sum(c['total_items'] for c in containers_to_process)} items to {len(containers_to_process)} containers[/green]")
            return

        # Process each container
        total_uploaded = 0
        successful_containers = []
        failed_containers = []

        for container_data in containers_to_process:
            container_name = container_data["name"]
            items = container_data.get("items", [])
            partition_key = container_data.get("partition_key")

            console.print(Panel(f"[bold blue]Processing container: {container_name}[/bold blue]"))

            # Check if container exists
            container_exists = container_name in available_containers

            if not container_exists:
                if create_containers:
                    console.print(f"[yellow]Container '{container_name}' not found. Creating new container...[/yellow]")

                    if not force:
                        if not Confirm.ask(f"Do you want to create container '{container_name}'?"):
                            console.print(f"[yellow]Skipping container '{container_name}'[/yellow]")
                            failed_containers.append(container_name)
                            continue

                    try:
                        # Create the container with partition key if available
                        if partition_key and 'paths' in partition_key:
                            console.print(f"[cyan]Creating container with partition key: {partition_key['paths']}[/cyan]")
                            paths = partition_key['paths']
                            if isinstance(paths, str):
                                paths = [paths]
                            elif not isinstance(paths, list):
                                console.print(f"[red]Invalid partition key paths format: {paths}[/red]")
                                failed_containers.append(container_name)
                                continue

                            try:
                                if len(paths) == 1:
                                    pk = PartitionKey(path=paths[0])
                                else:
                                    pk = PartitionKey(paths=paths)

                                client.database.create_container(id=container_name, partition_key=pk)
                                console.print(f"[green]✓ Successfully created container '{container_name}' with partition key[/green]")
                                container_exists = True
                            except Exception as pk_error:
                                console.print(f"[yellow]Warning: Failed to create container with partition key: {pk_error}[/yellow]")
                                console.print("[yellow]Attempting to create container with simple partition key...[/yellow]")

                                try:
                                    simple_pk = PartitionKey(path="pk")
                                    client.database.create_container(id=container_name, partition_key=simple_pk)
                                    console.print(f"[green]✓ Successfully created container '{container_name}' with simple partition key 'pk'[/green]")
                                    console.print("[yellow]Note: Container created with partition key 'pk'. You may need to add this field to your documents.[/yellow]")
                                    container_exists = True
                                except Exception as simple_error:
                                    console.print(f"[red]Error: Cannot create container '{container_name}': {simple_error}[/red]")
                                    failed_containers.append(container_name)
                                    continue
                        else:
                            console.print("[cyan]Creating container with default partition key[/cyan]")
                            try:
                                client.database.create_container(id=container_name, partition_key=PartitionKey(path="/id"))
                                console.print(f"[green]✓ Successfully created container '{container_name}'[/green]")
                                container_exists = True
                            except Exception as e:
                                console.print(f"[red]Error creating container '{container_name}': {e}[/red]")
                                failed_containers.append(container_name)
                                continue
                    except Exception as e:
                        console.print(f"[red]Error creating container '{container_name}': {e}[/red]")
                        failed_containers.append(container_name)
                        continue
                else:
                    console.print(f"[red]Error: Container '{container_name}' not found![/red]")
                    console.print(f"Use --create-containers flag to automatically create missing containers.")
                    failed_containers.append(container_name)
                    continue

            # Upload items to the container
            if items:
                console.print(f"[cyan]Uploading {len(items)} items to container '{container_name}'...[/cyan]")

                try:
                    if upsert:
                        uploaded_items = client.upsert_items_batch(container_name, items, batch_size)
                    else:
                        uploaded_items = client.create_items_batch(container_name, items, batch_size)

                    console.print(f"[green]✓ Successfully uploaded {len(uploaded_items)} items to container '{container_name}'[/green]")
                    total_uploaded += len(uploaded_items)
                    successful_containers.append(container_name)

                except Exception as e:
                    console.print(f"[red]Error uploading items to container '{container_name}': {e}[/red]")
                    failed_containers.append(container_name)
                    continue
            else:
                console.print(f"[yellow]No items to upload for container '{container_name}'[/yellow]")
                successful_containers.append(container_name)

        # Display final results
        console.print(Panel(f"[bold green]Upload Results[/bold green]"))
        console.print(f"[green]Total items uploaded: {total_uploaded}[/green]")
        console.print(f"[green]Successful containers: {len(successful_containers)}[/green]")
        console.print(f"[red]Failed containers: {len(failed_containers)}[/red]")

        if successful_containers:
            console.print(f"\n[green]Successfully processed containers: {', '.join(successful_containers)}[/green]")

        if failed_containers:
            console.print(f"\n[red]Failed containers: {', '.join(failed_containers)}[/red]")
            console.print("[yellow]You may need to check the errors above and manually create these containers.[/yellow]")

        # Check results
        if not successful_containers:
            console.print("[red]Error: No containers were successfully processed![/red]")
            raise Exception("No containers were successfully processed")
        elif failed_containers:
            console.print(f"\n[yellow]Upload completed with warnings. {len(failed_containers)} containers failed.[/yellow]")
        else:
            console.print("\n[green]All containers processed successfully![/green]")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        console.print("Please check your CosmosDB connection parameters and container names.")
        raise
