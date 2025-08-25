"""
Core connection testing functionality for CosmosDB.
"""

from azure.cosmos.exceptions import CosmosHttpResponseError
from rich.console import Console
from rich.prompt import Confirm

from .cosmos_client import CosmosDBClient
from .config import DatabaseConfig, ConnectionConfig

console = Console()


def test_connection(db_config: DatabaseConfig, connection_config: ConnectionConfig):
    """Test CosmosDB connection and list containers."""
    console.print(f"Endpoint: {db_config.endpoint}")
    console.print(f"Database: {db_config.database}")
    console.print(f"Allow insecure: {db_config.allow_insecure}")
    console.print(f"Create database if missing: {connection_config.create_database}")

    console.print("\n[cyan]Step 1: Initializing CosmosDB client...[/cyan]")
    # Test connection using our custom client
    try:
        client = CosmosDBClient(db_config)
        console.print("[green]✓ CosmosDB client initialized[/green]")
    except Exception as e:
        console.print(f"[red]Error initializing client: {e}[/red]")
        raise

    console.print("\n[cyan]Step 2: Testing database access...[/cyan]")
    # Test if we can access the database by listing containers
    console.print("[cyan]  Attempting to list containers...[/cyan]")

    try:
        containers = client.list_containers()
        console.print(f"[green]✓ Successfully connected to database: {db_config.database}[/green]")

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
                f"[yellow]Database '{db_config.database}' does not exist or "
                f"is not accessible.[/yellow]"
            )

            if connection_config.create_database:
                if not connection_config.force:
                    if not Confirm.ask(f"Do you want to create database '{db_config.database}'?"):
                        console.print("[yellow]Database creation cancelled.[/yellow]")
                        return  # User cancelled, exit cleanly

                try:
                    console.print(f"[cyan]Creating database '{db_config.database}'...[/cyan]")
                    client.client.create_database_if_not_exists(db_config.database)
                    console.print(f"[green]✓ Database '{db_config.database}' created successfully[/green]")

                    # Try listing containers again
                    console.print("\n[cyan]Testing database access after creation...[/cyan]")
                    containers = client.list_containers()
                    console.print(
                        f"[green]✓ Successfully connected to newly created "
                        f"database: {db_config.database}[/green]"
                    )

                    if containers:
                        console.print(f"\n[bold]Available containers ({len(containers)}):[/bold]")
                        for i, container in enumerate(containers, 1):
                            console.print(f"  {i}. {container}")
                    else:
                        console.print("\n[yellow]No containers found in the new database.[/yellow]")

                    console.print("\n[green]Connection test completed successfully![/green]")

                except Exception as e2:
                    console.print(f"[red]Error creating database '{db_config.database}': {e2}[/red]")
                    raise
            else:
                console.print(
                    f"[red]Database '{db_config.database}' does not exist. "
                    f"Use --create-database flag to create it.[/red]"
                )
                raise Exception(f"Database '{db_config.database}' does not exist") from e
        else:
            console.print(f"[red]Error accessing database: {e}[/red]")
            raise
