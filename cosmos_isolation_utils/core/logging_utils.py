"""
Logging utilities for consistent console output formatting.
"""

from rich.console import Console
from rich.panel import Panel

console = Console()


def log_with_color(text: str, color: str):
    """Log message with specified color."""
    console.print(f"[{color}]{text}[/{color}]")


def log_info(text: str):
    """Log informational message with cyan styling."""
    log_with_color(text, "cyan")


def log_success(text: str):
    """Log success message with green styling."""
    log_with_color(text, "green")


def log_warning(text: str):
    """Log warning message with yellow styling."""
    log_with_color(text, "yellow")


def log_error(text: str):
    """Log error message with red styling."""
    log_with_color(text, "red")


def log_bold(text: str, color: str = "blue"):
    """Log bold message with specified color."""
    log_with_color(text, f"bold {color}")


def log_panel(text: str, title: str = "", style: str = "blue"):
    """Log message in a panel with optional title."""
    console.print(Panel(text, title=title, border_style=style))


def log_step(step_number: int, text: str):
    """Log a step in a process."""
    log_with_color(f"\nStep {step_number}: {text}", "cyan")


def log_checkmark(text: str):
    """Log success message with checkmark."""
    log_with_color(f"✓ {text}", "green")


def log_cross(text: str):
    """Log error message with cross."""
    log_with_color(f"✗ {text}", "red")


def log_warning_icon(text: str):
    """Log warning message with warning icon."""
    log_with_color(f"⚠️  {text}", "yellow")


def log_database_info(endpoint: str, database: str, allow_insecure: bool):
    """Log database connection information."""
    console.print(f"Endpoint: {endpoint}")
    console.print(f"Database: {database}")
    console.print(f"Allow insecure: {allow_insecure}")


def log_container_info(container_name: str, item_count: int):
    """Log container information."""
    log_with_color(f"Container: {container_name} ({item_count} items)", "cyan")


def log_upload_summary(database: str, container_count: int, total_items: int,  # pylint: disable=too-many-arguments,too-many-positional-arguments
                      batch_size: int, mode: str, dry_run: bool, create_containers: bool):
    """Log upload summary information."""
    log_panel("[bold blue]Upload Summary[/bold blue]")
    console.print(f"Database: {database}")
    console.print(f"Containers to process: {container_count}")
    console.print(f"Total items to upload: {total_items}")
    console.print(f"Batch size: {batch_size}")
    console.print(f"Mode: {mode}")
    console.print(f"Dry run: {'Yes' if dry_run else 'No'}")
    console.print(f"Create containers: {'Yes' if create_containers else 'No'}")


def log_results_summary(total_uploaded: int, successful_count: int, failed_count: int):
    """Log upload results summary."""
    log_panel("[bold green]Upload Results[/bold green]")
    log_with_color(f"Total items uploaded: {total_uploaded}", "green")
    log_with_color(f"Successful containers: {successful_count}", "green")
    log_with_color(f"Failed containers: {failed_count}", "red")
