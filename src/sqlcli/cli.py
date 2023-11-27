"""sqlcli CLI."""

from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.console import Console
from rich.table import Table

import sqlcli.settings
from sqlcli.core.database import Database

app = typer.Typer()

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


@app.command()
def query(
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
