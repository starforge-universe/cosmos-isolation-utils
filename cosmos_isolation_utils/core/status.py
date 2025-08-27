"""
Core container status functionality for CosmosDB.
"""

from typing import List, Dict, Any
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn

from .config import DatabaseConfig, StatusConfig
from .logging_utils import (
    log_error, log_info, log_warning, log_panel, console, log_with_color
)
from .base_executor import BaseSubcommandExecutor


class ContainerStatusAnalyzer(BaseSubcommandExecutor):  # pylint: disable=too-few-public-methods
    """Analyzer class for container status and statistics."""

    def __init__(self, db_config: DatabaseConfig):
        """Initialize the analyzer with database configuration."""
        super().__init__(db_config)
        self.container_stats = []

    def _gather_container_statistics(self) -> None:
        """Gather container statistics with progress tracking."""
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Gathering container statistics...", total=None)

            try:
                containers = self.client.list_containers()
                stats = []

                for container_name in containers:
                    try:
                        container_stats = self.client.get_container_properties(container_name)
                        stats.append(container_stats)
                    except Exception as e:
                        log_warning(f"Warning: Could not get stats for container '{container_name}': {e}")
                        # Add basic info even if stats fail
                        stats.append({
                            "name": container_name,
                            "item_count": "Unknown",
                            "partition_key": "Unknown",
                            "last_modified": "Unknown",
                            "etag": "Unknown"
                        })

                self.container_stats = stats
                progress.update(task, completed=True)

            except Exception as e:
                log_error(f"Error getting container statistics: {e}")
                raise

    def _display_database_header(self) -> None:
        """Display the database header panel."""
        log_panel(f"Database: {self.db_config.database}", style="bold blue")

    def _check_empty_database(self) -> bool:
        """Check if the database has no containers."""
        if not self.container_stats:
            log_warning("No containers found in the database.")
            return True
        return False

    def _calculate_total_items(self) -> int:
        """Calculate total items across all containers."""
        return sum(
            stat['item_count'] for stat in self.container_stats
            if isinstance(stat['item_count'], int)
        )

    def _display_summary(self, total_items: int) -> None:
        """Display the summary information."""
        log_info(f"Found {len(self.container_stats)} containers with {total_items} total items")

    def _format_item_count(self, item_count: Any) -> str:
        """Format item count for display."""
        if isinstance(item_count, int):
            return f"{item_count:,}"
        return str(item_count)

    def _format_partition_key(self, partition_key: Any) -> str:
        """Format partition key for display."""
        if partition_key and 'paths' in partition_key:
            return str(partition_key['paths'])
        return "N/A"

    def _format_last_modified(self, last_modified: Any) -> str:
        """Format last modified date for display."""
        if last_modified:
            return str(last_modified)
        return "N/A"

    def _create_status_table(self) -> Table:
        """Create and populate the container status table."""
        table = Table(title="Container Status")
        table.add_column("Container Name", style="cyan")
        table.add_column("Items", style="green")
        table.add_column("Partition Key", style="yellow")
        table.add_column("Last Modified", style="blue")

        for stat in self.container_stats:
            table.add_row(
                stat['name'],
                self._format_item_count(stat['item_count']),
                self._format_partition_key(stat['partition_key']),
                self._format_last_modified(stat['last_modified'])
            )

        return table

    def _display_detailed_information(self, status_config: StatusConfig) -> None:
        """Display detailed container information if requested."""
        if not status_config.detailed:
            return

        log_info("\n" + "="*80)
        log_with_color("Detailed Container Information", "bold cyan")

        for stat in self.container_stats:
            log_panel(f"{stat['name']}", style="bold blue")
            log_info(f"  Items: {stat['item_count']}")
            log_info(f"  Partition Key: {stat['partition_key']}")
            log_info(f"  Last Modified: {stat['last_modified']}")
            log_info(f"  ETag: {stat['etag']}")
            log_info("")

    def _find_empty_containers(self) -> List[Dict[str, Any]]:
        """Find containers with no items."""
        return [
            stat for stat in self.container_stats if stat['item_count'] == 0
        ]

    def _find_containers_without_partition_keys(self) -> List[Dict[str, Any]]:
        """Find containers without partition keys."""
        return [
            stat for stat in self.container_stats if not stat['partition_key']
        ]

    def _display_recommendations(self) -> None:
        """Display recommendations based on container analysis."""
        log_info("\n" + "="*80)
        log_with_color("Recommendations", "bold cyan")

        # Check for empty containers
        empty_containers = self._find_empty_containers()
        if empty_containers:
            empty_names = ', '.join(stat['name'] for stat in empty_containers)
            log_warning(
                f"• {len(empty_containers)} containers are empty: "
                f"{empty_names}"
            )

        # Check for containers without partition keys
        no_partition_key = self._find_containers_without_partition_keys()
        if no_partition_key:
            no_pk_names = ', '.join(stat['name'] for stat in no_partition_key)
            log_warning(
                f"• {len(no_partition_key)} containers have no partition key: "
                f"{no_pk_names}"
            )

    def _build_command_string(self, command: str) -> str:
        """Build a command string with the current database configuration."""
        base_cmd = (
            f"cosmos-isolation-utils -e {self.db_config.endpoint} "
            f"-k {self.db_config.key} -d {self.db_config.database}"
        )
        return f"{base_cmd} {command}"

    def _display_dump_commands(self) -> None:
        """Display dump command examples."""
        log_with_color("Dump Commands:", "bold cyan")

        dump_all_cmd = self._build_command_string("dump -c all -o all_containers.json")
        log_with_color(f"• Dump all containers: {dump_all_cmd}", "cyan")

        dump_specific_cmd = self._build_command_string(
            "dump -c 'container1,container2' -o selected_containers.json"
        )
        log_with_color(f"• Dump specific containers: {dump_specific_cmd}", "cyan")

    def _display_upload_commands(self) -> None:
        """Display upload command examples."""
        log_with_color("Upload Commands:", "bold cyan")

        upload_all_cmd = self._build_command_string("upload -i all_containers.json --create-containers")
        log_with_color(f"• Upload all containers: {upload_all_cmd}", "cyan")

        upload_specific_cmd = self._build_command_string(
            "upload -i all_containers.json -c 'container1,container2' --create-containers"
        )
        log_with_color(f"• Upload specific containers: {upload_specific_cmd}", "cyan")

    def analyze(self, status_config: StatusConfig) -> None:
        """Main method to analyze and display container status."""
        # Display connection info
        self._display_connection_info()

        # Initialize client
        self._initialize_client()

        # Display database header and gather data
        self._display_database_header()
        self._gather_container_statistics()

        # Check for empty database
        if self._check_empty_database():
            return

        # Display summary and table
        total_items = self._calculate_total_items()
        self._display_summary(total_items)

        status_table = self._create_status_table()
        console.print(status_table)

        # Display detailed information if requested
        self._display_detailed_information(status_config)

        # Display recommendations and commands
        self._display_recommendations()
        self._display_dump_commands()
        self._display_upload_commands()
