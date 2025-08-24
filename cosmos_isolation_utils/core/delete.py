"""
Core database deletion functionality for CosmosDB.
"""

import sys
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Confirm
from azure.cosmos import CosmosClient
from azure.cosmos.exceptions import CosmosHttpResponseError
import urllib3

console = Console()


class DatabaseDeleter:
    """Class for deleting CosmosDB databases."""

    def __init__(self, endpoint: str, key: str, allow_insecure: bool = False):
        self.endpoint = endpoint
        self.key = key

        console.print("[cyan]Initializing CosmosDB client...[/cyan]")
        console.print(f"  Endpoint: {endpoint}")
        console.print(f"  Allow insecure: {allow_insecure}")

        # Control HTTPS verification warnings
        if allow_insecure:
            console.print("[cyan]  Suppressing HTTPS verification warnings...[/cyan]")
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        console.print("[cyan]  Creating CosmosClient...[/cyan]")
        self.client = CosmosClient(endpoint, key)
        console.print("[green]  ✓ CosmosClient created[/green]")
        console.print("[green]✓ Database deleter initialization completed[/green]")

    def list_databases(self) -> list:
        """List all databases in the CosmosDB account."""
        try:
            databases = list(self.client.list_databases())
            return [db['id'] for db in databases]
        except Exception as e:
            console.print(f"[red]Error listing databases: {e}[/red]")
            raise

    def get_database_info(self, database_name: str) -> dict:
        """Get information about a specific database."""
        try:
            database = self.client.get_database_client(database_name)
            properties = database.read()

            # Get container count
            containers = list(database.list_containers())
            container_count = len(containers)

            return {
                "name": database_name,
                "id": properties.get('id'),
                "rid": properties.get('_rid'),
                "ts": properties.get('_ts'),
                "etag": properties.get('_etag'),
                "container_count": container_count,
                "containers": [container['id'] for container in containers]
            }
        except CosmosHttpResponseError as e:
            if e.status_code == 404:
                console.print(f"[yellow]Database '{database_name}' not found[/yellow]")
                return None
            console.print(f"[red]Error getting database info for '{database_name}': {e}[/red]")
            raise
        except Exception as e:
            console.print(f"[red]Error getting database info for '{database_name}': {e}[/red]")
            raise

    def delete_database(self, database_name: str, force: bool = False) -> bool:
        """Delete a database with optional confirmation."""
        try:
            # Get database info first
            db_info = self.get_database_info(database_name)
            if not db_info:
                console.print(f"[red]Database '{database_name}' does not exist[/red]")
                return False

            # Show database information
            console.print(Panel("[bold red]Database to be deleted:[/bold red]"))
            console.print(f"  Name: {db_info['name']}")
            console.print(f"  ID: {db_info['id']}")
            console.print(f"  Containers: {db_info['container_count']}")
            if db_info['containers']:
                console.print(
                    f"  Container names: {', '.join(db_info['containers'])}"
                )
            console.print(f"  Created: {db_info['ts']}")

            # Safety confirmation
            if not force:
                console.print("\n[bold red]⚠️  WARNING: This action cannot be undone![/bold red]")
                console.print(
                    f"[red]All data in database '{database_name}' will be permanently deleted.[/red]"
                )

                if not Confirm.ask(
                    f"Are you sure you want to delete database '{database_name}'?"
                ):
                    console.print("[yellow]Database deletion cancelled[/yellow]")
                    return False

                # Double confirmation for databases with containers
                if db_info['container_count'] > 0:
                    console.print(
                        f"[bold red]⚠️  This database contains "
                        f"{db_info['container_count']} containers with data![/bold red]"
                    )
                    if not Confirm.ask(
                        "Are you absolutely sure? This will delete ALL data permanently!"
                    ):
                        console.print("[yellow]Database deletion cancelled[/yellow]")
                        return False

            # Proceed with deletion
            console.print(f"[cyan]Deleting database '{database_name}'...[/cyan]")
            self.client.delete_database(database_name)

            console.print(f"[green]✓ Database '{database_name}' deleted successfully[/green]")
            return True

        except CosmosHttpResponseError as e:
            if e.status_code == 404:
                console.print(f"[yellow]Database '{database_name}' not found[/yellow]")
                return False
            console.print(f"[red]Error deleting database '{database_name}': {e}[/red]")
            raise
        except Exception as e:
            console.print(f"[red]Error deleting database '{database_name}': {e}[/red]")
            raise


def delete_database(endpoint: str, key: str, allow_insecure: bool,
                   list_databases: bool, force: bool):
    """Delete CosmosDB databases with safety confirmations."""
    
    try:
        # Initialize database deleter
        deleter = DatabaseDeleter(endpoint, key, allow_insecure)

        if list_databases:
            console.print(Panel("[bold blue]Listing all databases[/bold blue]"))
            databases = deleter.list_databases()

            if not databases:
                console.print("[yellow]No databases found in the CosmosDB account[/yellow]")
                return

            table = Table(title="Available Databases")
            table.add_column("Database Name", style="cyan")
            table.add_column("Index", style="green")

            for i, db_name in enumerate(databases, 1):
                table.add_row(db_name, str(i))

            console.print(table)
            return

        # For the unified CLI, we need to get the database name from the context
        # This function is called from the delete_db subcommand which doesn't have a database parameter
        # So we'll just list databases for now
        console.print("[yellow]Note: Database deletion requires specifying the database name.[/yellow]")
        console.print("[yellow]Use the list-databases option to see available databases.[/yellow]")
        
        # List databases by default
        console.print(Panel("[bold blue]Available Databases[/bold blue]"))
        databases = deleter.list_databases()

        if not databases:
            console.print("[yellow]No databases found in the CosmosDB account[/yellow]")
            return

        table = Table(title="Available Databases")
        table.add_column("Database Name", style="cyan")
        table.add_column("Index", style="green")

        for i, db_name in enumerate(databases, 1):
            table.add_row(db_name, str(i))

        console.print(table)

    except Exception as e:
        console.print(f"[red]Fatal error: {e}[/red]")
        raise
