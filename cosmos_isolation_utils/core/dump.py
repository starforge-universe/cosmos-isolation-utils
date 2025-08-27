"""
Core container dumping functionality for CosmosDB.
"""

import json
from pathlib import Path
from rich.table import Table

from .config import DatabaseConfig, DumpConfig
from .logging_utils import (
    log_info, log_success, log_error, log_warning, log_panel, console, log_with_color
)
from .base_executor import BaseSubcommandExecutor


class ContainerDumper(BaseSubcommandExecutor):  # pylint: disable=too-few-public-methods
    """Dumper class for CosmosDB container operations."""

    def __init__(self, db_config: DatabaseConfig):
        """Initialize the container dumper with database configuration."""
        super().__init__(db_config)
        self.output_data = None

    def _validate_containers(self, dump_config: DumpConfig) -> list:
        """Validate and determine which containers to dump."""
        if not dump_config.containers:
            log_error("Error: Please specify containers to dump using --containers option")
            log_info("Use --containers all to dump all containers")
            log_info(
                "Or use --containers 'container1,container2,container3' to dump "
                "specific containers"
            )
            raise Exception("No containers specified")

        available_containers = self.client.list_containers()

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

        return containers_to_dump

    def _prepare_output_structure(self, containers_to_dump: list, dump_config: DumpConfig) -> None:
        """Prepare the output data structure for multiple containers."""
        self.output_data = {
            "database": self.db_config.database,
            "exported_at": (
                str(Path(dump_config.output_dir).stat().st_mtime)
                if Path(dump_config.output_dir).exists()
                else "N/A"
            ),
            "total_containers": len(containers_to_dump),
            "total_items": 0,
            "containers": []
        }

    def _process_container(self, container_name: str) -> bool:
        """Process a single container and return success status."""
        log_panel(f"[bold blue]Processing container: {container_name}[/bold blue]", style="blue")

        try:
            # Get container properties to extract partition key
            log_info(f"Extracting partition key information for {container_name}...")
            container_properties = self.client.get_container_properties(container_name)

            # Extract only partition key information
            partition_key = None
            if 'partitionKey' in container_properties:
                partition_key = container_properties['partitionKey']
                log_success(f"✓ Found partition key: {partition_key}")
            else:
                log_warning(f"No partition key found for container '{container_name}'")

            # Get all items from the container
            items = self.client.get_all_items(container_name)

            if not items:
                log_warning(f"No items found in container '{container_name}'")
                # Still add container info even if empty
                container_data = {
                    "name": container_name,
                    "total_items": 0,
                    "partition_key": partition_key,
                    "items": []
                }
                self.output_data["containers"].append(container_data)
                return True

            # Prepare container data
            container_data = {
                "name": container_name,
                "total_items": len(items),
                "partition_key": partition_key,
                "items": items
            }

            self.output_data["containers"].append(container_data)
            self.output_data["total_items"] += len(items)

            log_success(
                f"✓ Successfully processed container '{container_name}' with {len(items)} items"
            )

            # Show sample of exported data
            if items:
                sample_item = items[0]
                log_with_color(f"Sample item structure for {container_name}:", "bold cyan")
                log_info(f"Keys: {list(sample_item.keys())}")
                if 'id' in sample_item:
                    log_info(f"ID: {sample_item['id']}")
                if 'type' in sample_item:
                    log_info(f"Type: {sample_item['type']}")
                if partition_key and 'paths' in partition_key:
                    partition_paths = partition_key['paths']
                    log_info(f"Partition paths: {partition_paths}")

            return True

        except Exception as e:
            log_error(f"Error processing container '{container_name}': {e}")
            log_warning(
                f"Skipping container '{container_name}' and continuing with others..."
            )
            return False

    def _process_all_containers(self, containers_to_dump: list) -> list:
        """Process all containers and return list of failed containers."""
        failed_containers = []

        for container_name in containers_to_dump:
            if not self._process_container(container_name):
                failed_containers.append(container_name)

        return failed_containers

    def _validate_processing_results(self) -> None:
        """Validate that at least some containers were processed successfully."""
        if not self.output_data["containers"]:
            log_error("Error: No containers were successfully processed!")
            raise Exception("No containers were successfully processed")

    def _ensure_output_directory(self, dump_config: DumpConfig) -> None:
        """Ensure the output directory exists."""
        output_path = Path(dump_config.output_dir)
        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            log_error(f"Error creating output directory: {e}")
            raise

    def _write_output_file(self, dump_config: DumpConfig) -> None:
        """Write the output data to JSON file."""
        try:
            with open(dump_config.output_dir, 'w', encoding='utf-8') as f:
                if dump_config.pretty:
                    json.dump(self.output_data, f, indent=2, ensure_ascii=False, default=str)
                else:
                    json.dump(self.output_data, f, ensure_ascii=False, default=str)
        except Exception as e:
            log_error(f"Error writing to output file: {e}")
            raise

    def _display_export_summary(self, failed_containers: list, dump_config: DumpConfig) -> None:
        """Display the final export summary."""
        log_panel("[bold green]Export Summary[/bold green]", style="green")
        log_success(
            f"Successfully exported {self.output_data['total_items']} items from "
            f"{len(self.output_data['containers'])} containers to {dump_config.output_dir}"
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

        for container_data in self.output_data["containers"]:
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

    def dump_containers(self, dump_config: DumpConfig) -> None:
        """Main method to dump containers to JSON file."""
        # Display connection info
        self._display_connection_info()

        # Initialize client
        self._initialize_client()

        # Validate and determine containers to dump
        containers_to_dump = self._validate_containers(dump_config)

        # Prepare output structure
        self._prepare_output_structure(containers_to_dump, dump_config)

        # Process all containers
        failed_containers = self._process_all_containers(containers_to_dump)

        # Validate processing results
        self._validate_processing_results()

        # Ensure output directory exists
        self._ensure_output_directory(dump_config)

        # Write output file
        self._write_output_file(dump_config)

        # Display summary
        self._display_export_summary(failed_containers, dump_config)
