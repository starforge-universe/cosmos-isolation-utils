"""
Core container upload functionality for CosmosDB.
"""

import json
from pathlib import Path
from azure.cosmos.exceptions import CosmosHttpResponseError
from rich.table import Table
from rich.prompt import Confirm

from .logging_utils import (
    log_info, log_success, log_warning, log_error, log_panel,
    log_checkmark, log_upload_summary, log_results_summary, console
)
from .config import DatabaseConfig, UploadConfig
from .base_executor import BaseSubcommandExecutor


class ContainerUploader(BaseSubcommandExecutor):  # pylint: disable=too-few-public-methods
    """Uploader class for CosmosDB container operations."""

    def __init__(self, db_config: DatabaseConfig):
        """Initialize the container uploader with database configuration."""
        super().__init__(db_config)
        self.data = None
        self.containers_to_process = []
        self.available_containers = []

    def _calculate_total_items(self, containers_to_process):
        """Calculate total items across all containers."""
        return sum(c['total_items'] for c in containers_to_process)

    def _format_container_list(self, containers):
        """Format a list of container names for display."""
        return ', '.join(containers)

    def _validate_input_file(self, upload_config: UploadConfig) -> None:
        """Validate that the input file exists and is readable."""
        input_path = Path(upload_config.input_file)
        if not input_path.exists():
            log_error(f"Error: Input file '{upload_config.input_file}' not found!")
            raise Exception(f"Input file '{upload_config.input_file}' not found")

    def _load_json_data(self, upload_config: UploadConfig) -> None:
        """Load and parse the JSON data from the input file."""
        try:
            with open(upload_config.input_file, 'r', encoding='utf-8') as f:
                self.data = json.load(f)
        except json.JSONDecodeError as e:
            log_error(f"Error: Invalid JSON file: {e}")
            raise Exception(f"Invalid JSON file: {e}") from e

    def _parse_container_data(self, upload_config: UploadConfig) -> None:
        """Parse container data from JSON and determine which containers to process."""
        # Check if this is a multi-container dump file
        if 'containers' in self.data and isinstance(self.data['containers'], list):
            log_info(f"Found multi-container dump file with {len(self.data['containers'])} containers")
            log_info(f"Database: {self.data.get('database', 'N/A')}")
            log_info(f"Total items: {self.data.get('total_items', 0)}")

            # Determine which containers to process
            if upload_config.containers:
                # User specified specific containers
                target_containers = [c.strip() for c in upload_config.containers.split(',')]
                available_containers = [c['name'] for c in self.data['containers']]
                missing_containers = [c for c in target_containers if c not in available_containers]

                if missing_containers:
                    log_error(f"Error: Specified containers not found in JSON: {', '.join(missing_containers)}")
                    log_info(f"Available containers in JSON: {', '.join(available_containers)}")
                    raise Exception(f"Specified containers not found in JSON: {', '.join(missing_containers)}")

                self.containers_to_process = [c for c in self.data['containers'] if c['name'] in target_containers]
                log_info(f"Processing specified containers: {', '.join(target_containers)}")
            else:
                # Process all containers in the JSON
                self.containers_to_process = self.data['containers']
                log_info("Processing all containers from JSON")
        else:
            # Legacy single-container format support
            log_warning("Detected legacy single-container format, converting to multi-container format")
            if 'container' in self.data and 'items' in self.data:
                self.containers_to_process = [{
                    'name': self.data['container'],
                    'total_items': len(self.data['items']),
                    'partition_key': self.data.get('partition_key'),
                    'items': self.data['items']
                }]
                log_info(f"Converted legacy format: 1 container with {len(self.data['items'])} items")
            else:
                raise Exception("Invalid JSON structure. Expected 'containers' array or legacy format.")

    def _check_database_existence(self, upload_config: UploadConfig) -> None:
        """Check if database exists and create if needed."""
        try:
            self.available_containers = self.list_containers()
        except CosmosHttpResponseError as e:
            if "Owner resource does not exist" in str(e) or "NotFound" in str(e):
                self._handle_database_not_found(upload_config)
            else:
                log_error(f"Error listing containers: {e}")
                raise

    def _handle_database_not_found(self, upload_config: UploadConfig) -> None:
        """Handle the case when the database doesn't exist."""
        log_warning(f"Database '{self.db_config.database}' does not exist or is not accessible.")
        if not upload_config.force and not upload_config.dry_run:
            if Confirm.ask(f"Do you want to create database '{self.db_config.database}' first?"):
                self._create_database()
            else:
                log_warning("Database creation cancelled. Cannot proceed without database.")
                raise Exception("Database creation cancelled")
        else:
            # Force mode - try to create database
            self._create_database()

    def _create_database(self) -> None:
        """Create the database."""
        try:
            self.create_database_if_not_exists(self.db_config.database)
            log_checkmark(f"Database '{self.db_config.database}' created successfully")
            self.available_containers = []
        except Exception as e:
            log_error(f"Error creating database '{self.db_config.database}': {e}")
            raise

    def _display_upload_summary(self, upload_config: UploadConfig) -> None:
        """Display the upload summary and container details."""
        total_items = self._calculate_total_items(self.containers_to_process)
        mode = 'Upsert' if upload_config.upsert else 'Create'
        log_upload_summary(
            database=self.db_config.database,
            container_count=len(self.containers_to_process),
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

        for container_data in self.containers_to_process:
            partition_key_info = "N/A"
            if container_data.get("partition_key") and "paths" in container_data["partition_key"]:
                partition_key_info = str(container_data["partition_key"]["paths"])

            status = "Will create" if container_data["name"] not in self.available_containers else "Exists"
            table.add_row(
                container_data["name"],
                str(container_data["total_items"]),
                partition_key_info,
                status
            )

        console.print(table)

    def _handle_dry_run(self) -> None:
        """Handle dry run mode."""
        total_items = self._calculate_total_items(self.containers_to_process)
        log_success(
            f"Dry run completed. Would upload {total_items} items to {len(self.containers_to_process)} containers"
        )

    def _create_container_if_needed(self, container_name: str, partition_key, upload_config: UploadConfig) -> bool:  # pylint: disable=too-many-return-statements
        """Create a container if it doesn't exist and creation is requested."""
        if container_name in self.available_containers:
            return True

        if not upload_config.create_containers:
            log_error(f"Error: Container '{container_name}' not found!")
            log_warning("Use --create-containers flag to automatically create missing containers.")
            return False

        log_warning(f"Container '{container_name}' not found. Creating new container...")

        if not upload_config.force:
            if not Confirm.ask(f"Do you want to create container '{container_name}'?"):
                log_warning(f"Skipping container '{container_name}'")
                return False

        try:
            # Create the container with partition key if available
            if partition_key and 'paths' in partition_key:
                log_info(f"Creating container with partition key: {partition_key['paths']}")
                try:
                    self.create_container(container_name, partition_key['paths'])
                    log_checkmark(f"Successfully created container '{container_name}' with partition key")
                    return True
                except Exception as pk_error:
                    log_warning(f"Warning: Failed to create container with partition key: {pk_error}")
                    log_warning("Attempting to create container with simple partition key...")

                    try:
                        self.create_container(container_name, ["pk"])
                        log_checkmark(
                            f"Successfully created container '{container_name}' with simple partition key 'pk'"
                        )
                        log_warning(
                            "Note: Container created with partition key 'pk'. "
                            "You may need to add this field to your documents."
                        )
                        return True
                    except Exception as simple_error:
                        log_error(f"Error: Cannot create container '{container_name}': {simple_error}")
                        return False
            else:
                log_info("Creating container with default partition key")
                try:
                    self.create_container(container_name, ["/id"])
                    log_checkmark(f"Successfully created container '{container_name}'")
                    return True
                except Exception as e:
                    log_error(f"Error creating container '{container_name}': {e}")
                    return False
        except Exception as e:
            log_error(f"Error creating container '{container_name}': {e}")
            return False

    def _upload_container_items(self, container_data: dict, upload_config: UploadConfig) -> tuple:
        """Upload items to a specific container and return success status and count."""
        container_name = container_data["name"]
        items = container_data.get("items", [])
        partition_key = container_data.get("partition_key")

        log_panel(f"Processing container: {container_name}", style="blue")

        # Check if container exists or create if needed
        if not self._create_container_if_needed(container_name, partition_key, upload_config):
            return False, 0

        # Upload items to the container
        if items:
            log_info(f"Uploading {len(items)} items to container '{container_name}'...")

            try:
                uploaded_items = self.process_items_batch(container_name, items, upload_config.batch_size, upload_config.upsert)

                log_checkmark(f"Successfully uploaded {len(uploaded_items)} items to container '{container_name}'")
                return True, len(uploaded_items)

            except Exception as e:
                log_error(f"Error uploading items to container '{container_name}': {e}")
                return False, 0
        else:
            log_warning(f"No items to upload for container '{container_name}'")
            return True, 0

    def _process_all_containers(self, upload_config: UploadConfig) -> tuple:
        """Process all containers and return results."""
        total_uploaded = 0
        successful_containers = []
        failed_containers = []

        for container_data in self.containers_to_process:
            success, count = self._upload_container_items(container_data, upload_config)
            if success:
                successful_containers.append(container_data["name"])
                total_uploaded += count
            else:
                failed_containers.append(container_data["name"])

        return total_uploaded, successful_containers, failed_containers

    def _display_results(self, total_uploaded: int, successful_containers: list, failed_containers: list) -> None:
        """Display the final upload results."""
        log_results_summary(
            total_uploaded=total_uploaded,
            successful_count=len(successful_containers),
            failed_count=len(failed_containers)
        )

        if successful_containers:
            successful_list = self._format_container_list(successful_containers)
            log_success(f"Successfully processed containers: {successful_list}")

        if failed_containers:
            failed_list = self._format_container_list(failed_containers)
            log_error(f"Failed containers: {failed_list}")
            log_warning("You may need to check the errors above and manually create these containers.")

        # Check results
        if not successful_containers:
            log_error("Error: No containers were successfully processed!")
            raise Exception("No containers were successfully processed")
        if failed_containers:
            log_warning(f"Upload completed with warnings. {len(failed_containers)} containers failed.")
        else:
            log_success("All containers processed successfully!")

    def upload_entries(self, upload_config: UploadConfig) -> None:
        """Main method to upload entries to CosmosDB containers."""
        # Validate input file
        self._validate_input_file(upload_config)

        # Load JSON data
        self._load_json_data(upload_config)

        # Parse container data
        self._parse_container_data(upload_config)

        # Check database existence
        self._check_database_existence(upload_config)

        # Display upload summary
        self._display_upload_summary(upload_config)

        # Confirm upload
        if not upload_config.force and not upload_config.dry_run:
            if not Confirm.ask("Do you want to proceed with the upload?"):
                log_warning("Upload cancelled.")
                return

        # Handle dry run
        if upload_config.dry_run:
            self._handle_dry_run()
            return

        # Process all containers
        total_uploaded, successful_containers, failed_containers = self._process_all_containers(upload_config)

        # Display results
        self._display_results(total_uploaded, successful_containers, failed_containers)
