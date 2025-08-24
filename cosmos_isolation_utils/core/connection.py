"""
Core connection testing functionality for CosmosDB.
"""

import sys
from azure.cosmos.exceptions import CosmosHttpResponseError
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm

from .cosmos_client import CosmosDBClient

console = Console()


def test_connection(endpoint: str, key: str, database: str, allow_insecure: bool,
                   create_database: bool, force: bool):
    """Test CosmosDB connection and list containers."""
    
    console.print(f"Endpoint: {endpoint}")
    console.print(f"Database: {database}")
    console.print(f"Allow insecure: {allow_insecure}")
    console.print(f"Create database if missing: {create_database}")

    console.print("\n[cyan]Step 1: Initializing CosmosDB client...[/cyan]")
    # Test connection using our custom client
    try:
        client = CosmosDBClient(endpoint, key, database, allow_insecure)
        console.print("[green]✓ CosmosDB client initialized[/green]")
    except Exception as e:
        console.print(f"[red]Error initializing client: {e}[/red]")
        raise

    console.print("\n[cyan]Step 2: Testing database access...[/cyan]")
    # Test if we can access the database by listing containers
    console.print("[cyan]  Attempting to list containers...[/cyan]")

    try:
        containers = client.list_containers()
        console.print(f"[green]✓ Successfully connected to database: {database}[/green]")

        console.print("\n[cyan]Step 3: Listing containers...[/cyan]")
        if containers:
            console.print(f"\n[bold]Available containers ({len(containers)}):[/bold]")
            for i, container in enumerate(containers, 1):
                console.print(f"  {i}. {container}")
        else:
            console.print("\n[yellow]No containers found in the database.[/yellow]")

        console.print("\n[green]Connection test completed successfully![/green]")

    except CosmosHttpResponseError as e:
        if "Owner resource does not exist" in str(e) or "NotFound" in str(e):
            console.print(
                f"[yellow]Database '{database}' does not exist or is not accessible.[/yellow]"
            )

            if create_database:
                if not force:
                    if not Confirm.ask(f"Do you want to create database '{database}'?"):
                        console.print("[yellow]Database creation cancelled.[/yellow]")
                        return  # User cancelled, exit cleanly

                try:
                    console.print(f"[cyan]Creating database '{database}'...[/cyan]")
                    client.client.create_database_if_not_exists(database)
                    console.print(f"[green]✓ Database '{database}' created successfully[/green]")

                    # Try listing containers again
                    console.print("\n[cyan]Testing database access after creation...[/cyan]")
                    containers = client.list_containers()
                    console.print(
                        f"[green]✓ Successfully connected to newly created database: {database}[/green]"
                    )

                    if containers:
                        console.print(f"\n[bold]Available containers ({len(containers)}):[/bold]")
                        for i, container in enumerate(containers, 1):
                            console.print(f"  {i}. {container}")
                    else:
                        console.print("\n[yellow]No containers found in the new database.[/yellow]")

                    console.print("\n[green]Connection test completed successfully![/green]")

                except Exception as e2:
                    console.print(f"[red]Error creating database '{database}': {e2}[/red]")
                    raise
            else:
                console.print(
                    f"[red]Database '{database}' does not exist. "
                    f"Use --create-database flag to create it.[/red]"
                )
                raise Exception(f"Database '{database}' does not exist")
        else:
            console.print(f"[red]Error accessing database: {e}[/red]")
            raise
