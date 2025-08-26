"""
Core container status functionality for CosmosDB.
"""

from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn

from .cosmos_client import CosmosDBClient
from .config import DatabaseConfig, StatusConfig
from .logging_utils import (
    log_info, log_warning, log_error, log_panel, console
)


def get_container_status(db_config: DatabaseConfig, status_config: StatusConfig):
    """Show the status and statistics of all containers in a CosmosDB database."""

    try:
        # Initialize CosmosDB client
        client = CosmosDBClient(db_config)

        log_panel(f"[bold blue]Database: {db_config.database}[/bold blue]", style="blue")

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
            log_warning("No containers found in the database.")
            return

        # Display summary
        total_items = sum(
            stat['item_count'] for stat in container_stats
            if isinstance(stat['item_count'], int)
        )
        log_info(f"Found {len(container_stats)} containers with {total_items} total items")

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
        if status_config.detailed:
            log_info("\n" + "="*80)
            log_info("[bold]Detailed Container Information[/bold]")

            for stat in container_stats:
                log_panel(f"[bold blue]{stat['name']}[/bold blue]", style="blue")
                log_info(f"  Items: {stat['item_count']}")
                log_info(f"  Partition Key: {stat['partition_key']}")
                log_info(f"  Last Modified: {stat['last_modified']}")
                log_info(f"  ETag: {stat['etag']}")
                log_info("")

        # Show recommendations
        log_info("\n" + "="*80)
        log_info("[bold]Recommendations[/bold]")

        # Check for containers with no items
        empty_containers = [
            stat for stat in container_stats if stat['item_count'] == 0
        ]
        if empty_containers:
            empty_names = ', '.join(stat['name'] for stat in empty_containers)
            log_warning(
                f"• {len(empty_containers)} containers are empty: "
                f"{empty_names}"
            )

        # Check for containers without partition keys
        no_partition_key = [
            stat for stat in container_stats if not stat['partition_key']
        ]
        if no_partition_key:
            no_pk_names = ', '.join(stat['name'] for stat in no_partition_key)
            log_warning(
                f"• {len(no_partition_key)} containers have no partition key: {no_pk_names}"
            )

        # Show dump commands
        log_info("\n[bold]Dump Commands:[/bold]")
        dump_all_cmd = (
            f"cosmos-isolation-utils -e {db_config.endpoint} -k {db_config.key} -d {db_config.database} "
            f"dump -c all -o all_containers.json"
        )
        log_info(f"[cyan]• Dump all containers:[/cyan] {dump_all_cmd}")

        dump_specific_cmd = (
            f"cosmos-isolation-utils -e {db_config.endpoint} -k {db_config.key} -d {db_config.database} "
            f"dump -c 'container1,container2' -o selected_containers.json"
        )
        log_info(f"[cyan]• Dump specific containers:[/cyan] {dump_specific_cmd}")

        # Show upload commands
        log_info("\n[bold]Upload Commands:[/bold]")
        upload_all_cmd = (
            f"cosmos-isolation-utils -e {db_config.endpoint} -k {db_config.key} -d {db_config.database} "
            f"upload -i all_containers.json --create-containers"
        )
        log_info(f"[cyan]• Upload all containers:[/cyan] {upload_all_cmd}")

        upload_specific_cmd = (
            f"cosmos-isolation-utils -e {db_config.endpoint} -k {db_config.key} -d {db_config.database} "
            f"upload -i all_containers.json -c 'container1,container2' --create-containers"
        )
        log_info(f"[cyan]• Upload specific containers:[/cyan] {upload_specific_cmd}")

    except Exception as e:
        log_error(f"Error: {e}")
        log_info("Please check your CosmosDB connection parameters.")
        raise
