"""
Core container status functionality for CosmosDB.
"""

import sys
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn

from .cosmos_client import CosmosDBClient

console = Console()


def get_container_status(endpoint: str, key: str, database: str, allow_insecure: bool, detailed: bool):
    """Show the status and statistics of all containers in a CosmosDB database."""

    try:
        # Initialize CosmosDB client
        client = CosmosDBClient(endpoint, key, database, allow_insecure)

        console.print(Panel(f"[bold blue]Database: {database}[/bold blue]"))

        # Get container statistics
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Gathering container statistics...", total=None)
            container_stats = client.get_all_containers_stats()
            progress.update(task, completed=True)

        if not container_stats:
            console.print("[yellow]No containers found in the database.[/yellow]")
            return

        # Display summary
        total_items = sum(
            stat['item_count'] for stat in container_stats
            if isinstance(stat['item_count'], int)
        )
        console.print(
            f"[cyan]Found {len(container_stats)} containers with {total_items} total items[/cyan]"
        )

        # Create main table
        table = Table(title="Container Status")
        table.add_column("Container Name", style="cyan")
        table.add_column("Items", style="green")
        table.add_column("Partition Key", style="yellow")
        table.add_column("Last Modified", style="blue")

        for stat in container_stats:
            item_count = stat['item_count']
            if isinstance(item_count, int):
                item_count_str = f"{item_count:,}"
            else:
                item_count_str = str(item_count)

            partition_key = stat['partition_key']
            if partition_key and 'paths' in partition_key:
                partition_key_str = str(partition_key['paths'])
            else:
                partition_key_str = "N/A"

            last_modified = stat['last_modified']
            if last_modified:
                last_modified_str = str(last_modified)
            else:
                last_modified_str = "N/A"

            table.add_row(
                stat['name'],
                item_count_str,
                partition_key_str,
                last_modified_str
            )

        console.print(table)

        # Show detailed information if requested
        if detailed:
            console.print("\n" + "="*80)
            console.print("[bold]Detailed Container Information[/bold]")

            for stat in container_stats:
                console.print(Panel(f"[bold blue]{stat['name']}[/bold blue]"))
                console.print(f"  Items: {stat['item_count']}")
                console.print(f"  Partition Key: {stat['partition_key']}")
                console.print(f"  Last Modified: {stat['last_modified']}")
                console.print(f"  ETag: {stat['etag']}")
                console.print()

        # Show recommendations
        console.print("\n" + "="*80)
        console.print("[bold]Recommendations[/bold]")

        # Check for containers with no items
        empty_containers = [
            stat for stat in container_stats if stat['item_count'] == 0
        ]
        if empty_containers:
            empty_names = ', '.join(stat['name'] for stat in empty_containers)
            console.print(
                f"[yellow]• {len(empty_containers)} containers are empty: {empty_names}[/yellow]"
            )

        # Check for containers without partition keys
        no_partition_key = [
            stat for stat in container_stats if not stat['partition_key']
        ]
        if no_partition_key:
            no_pk_names = ', '.join(stat['name'] for stat in no_partition_key)
            console.print(
                f"[yellow]• {len(no_partition_key)} containers have no partition key: {no_pk_names}[/yellow]"
            )

        # Show dump commands
        console.print("\n[bold]Dump Commands:[/bold]")
        dump_all_cmd = (
            f"cosmos-isolation-utils -e {endpoint} -k {key} -d {database} "
            f"dump -c all -o all_containers.json"
        )
        console.print(f"[cyan]• Dump all containers:[/cyan] {dump_all_cmd}")

        dump_specific_cmd = (
            f"cosmos-isolation-utils -e {endpoint} -k {key} -d {database} "
            f"dump -c 'container1,container2' -o selected_containers.json"
        )
        console.print(f"[cyan]• Dump specific containers:[/cyan] {dump_specific_cmd}")

        # Show upload commands
        console.print("\n[bold]Upload Commands:[/bold]")
        upload_all_cmd = (
            f"cosmos-isolation-utils -e {endpoint} -k {key} -d {database} "
            f"upload -i all_containers.json --create-containers"
        )
        console.print(f"[cyan]• Upload all containers:[/cyan] {upload_all_cmd}")

        upload_specific_cmd = (
            f"cosmos-isolation-utils -e {endpoint} -k {key} -d {database} "
            f"upload -i all_containers.json -c 'container1,container2' --create-containers"
        )
        console.print(f"[cyan]• Upload specific containers:[/cyan] {upload_specific_cmd}")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        console.print("Please check your CosmosDB connection parameters.")
        raise
