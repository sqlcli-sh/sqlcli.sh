"""sqlcli CLI."""

from pathlib import Path
from typing import Annotated, Optional
import typer

from rich.console import Console
from rich.table import Table
from sqlcli.core.database import Database

app = typer.Typer()

__version__ = "0.1.0"

def version_callback(value: bool):
    if value:
        print(f"Awesome CLI Version: {__version__}")
        raise typer.Exit()


# pylint: disable=too-many-arguments
@app.callback(invoke_without_command=True)
def cli(
        config_path: Path = typer.Option(
            typer.get_app_dir("tokern"), help="Path to config directory"
        ),
        version: Optional[bool] = typer.Option(
            None, "--version", callback=version_callback, is_eager=True
        ),
    ) -> None:
    app_dir_path = Path(config_path)
    app_dir_path.mkdir(parents=True, exist_ok=True)


@app.command()
def query(
        url: Annotated[str, typer.Argument(help="database url")],
        sql: Annotated[str, typer.Option(help="execute a SQL command")] = None,
        file: Annotated[str, typer.Option(help="execute commands from a file and exit.")] = None,
        password: Annotated[bool, typer.Option(help="Force password prompt.")] = True
):
    with Database(url) as db:
        if sql:
            collection = db.execute(sql)
            table = Table(show_header=True, header_style="bold magenta")
            first = collection[0]
            for key in first.keys():
                table.add_column(key)

            for record in collection:
                table.add_row(*record.values())

            console = Console()
            console.print(table)
