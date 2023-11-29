"""sqlcli CLI."""

from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.console import Console
from rich.table import Table

import sqlcli.settings
from sqlcli.api import get_columns, get_schema, get_tables, open_catalog, scan_database
from sqlcli.core.database import Database

app = typer.Typer()
query_app = typer.Typer()
catalog_app = typer.Typer()

app.add_typer(query_app, name="query")
app.add_typer(catalog_app, name="catalog")

__version__ = "0.1.0"


def version_callback(value: bool):
    """Callback function for --version option. Prints the version of the application and exits.

    Args:
        value (bool): If True, the version of the application is printed.
    """
    if value:
        print(f"Awesome CLI Version: {__version__}")
        raise typer.Exit


# pylint: disable=too-many-arguments
@app.callback(invoke_without_command=True)
def cli(
    version: Annotated[
        Optional[bool],
        typer.Option("--version", callback=version_callback, is_eager=True),
    ] = None,
) -> None:
    """Main CLI function. Creates the application directory if it doesn't exist.

    Args:
        config_path (Path): Path to the configuration directory.
        version (Optional[bool]): If True, the version of the application is printed.
    """
    app_dir = typer.get_app_dir("sqlcli")
    app_dir_path = Path(app_dir)
    app_dir_path.mkdir(parents=True, exist_ok=True)

    sqlcli.settings.APP_DIR = app_dir_path


@query_app.command("run")
def run_query(
    url: Annotated[str, typer.Argument(help="database url")],
    sql: Annotated[Optional[str], typer.Option(help="execute a SQL command")] = None,
    file: Annotated[
        Optional[str], typer.Option(help="execute commands from a file and exit.")
    ] = None,
    password: Annotated[bool, typer.Option(help="Force password prompt.")] = True,
):
    """Executes a SQL query or a file of SQL commands.

    Args:
        url (str): The database URL.
        sql (str): The SQL command to execute.
        file (str): The file of SQL commands to execute.
        password (bool): If True, forces a password prompt.
    """
    with Database(url) as db:
        if sql:
            collection = db.execute(sql)
            table = Table(show_header=True, header_style="bold magenta")
            first = True

            for record in collection:
                if first:
                    for key in record.keys():  # noqa: SIM118
                        table.add_column(key)
                    first = False
                table.add_row(*record.values_str())

            console = Console()
            console.print(table)


@catalog_app.command("scan")
def scan_catalog(
    url: Annotated[str, typer.Argument(help="database url")],
    password: Annotated[bool, typer.Option(help="Force password prompt.")] = True,
):
    """Scans the catalog for tables and views.

    Args:
        url (str): The database URL.
        password (bool): If True, forces a password prompt.
    """
    catalog = open_catalog(sqlcli.settings.APP_DIR)
    scan_database(catalog, url)


@catalog_app.command("list-schema")
def list_schema(
    url: Annotated[str, typer.Argument(help="database url")],
    password: Annotated[bool, typer.Option(help="Force password prompt.")] = True,
):
    """List the schemas in the catalog.

    Args:
        url (str): The database URL.
        password (bool): If True, forces a password prompt.
    """
    catalog = open_catalog(sqlcli.settings.APP_DIR)
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Schema")

    for schema in get_schema(catalog, url):
        table.add_row(schema.name)

    console = Console()
    console.print(table)


@catalog_app.command("list-tables")
def list_table(
    uri: Annotated[str, typer.Argument(help="database url")],
    schema: Annotated[Optional[str], typer.Option(help="schema name")] = None,
    password: Annotated[bool, typer.Option(help="Force password prompt.")] = True,
):
    """List the tables in the catalog.

    Args:
        uri (str): The database URL.
        schema (str): The schema name.
        password (bool): If True, forces a password prompt.
    """
    catalog = open_catalog(sqlcli.settings.APP_DIR)

    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Schema")
    table.add_column("Table")

    for tbl in get_tables(db_uri=uri, schema_name=schema, catalog=catalog):
        table.add_row(tbl.schema.name, tbl.name)

    console = Console()
    console.print(table)


@catalog_app.command("list-columns")
def list_columns(
    uri: Annotated[str, typer.Argument(help="database url")],
    schema: Annotated[Optional[str], typer.Option(help="schema name")] = None,
    table: Annotated[Optional[str], typer.Option(help="table name")] = None,
    password: Annotated[bool, typer.Option(help="Force password prompt.")] = True,
):
    """List the columns in the catalog.

    Args:
        uri (str): The database URL.
        schema (str): The schema name.
        table (str): The table name.
        password (bool): If True, forces a password prompt.
    """
    catalog = open_catalog(sqlcli.settings.APP_DIR)

    tui_table = Table(show_header=True, header_style="bold magenta")
    tui_table.add_column("Schema")
    tui_table.add_column("Table")
    tui_table.add_column("Column")
    tui_table.add_column("Data Type")
    tui_table.add_column("Sort Order")

    for column in get_columns(db_uri=uri, schema_name=schema, table_name=table, catalog=catalog):
        tui_table.add_row(
            column.table.schema.name,
            column.table.name,
            column.name,
            column.data_type,
            str(column.sort_order),
        )

    console = Console()
    console.print(tui_table)
