"""Test sqlcli CLI."""

from typer.testing import CliRunner

from sqlcli.cli import app

runner = CliRunner()


def test_query_command(load_countries_of_the_world_data) -> None:
    """Test the query command with a SQL command."""
    url = f"sqlite:///{load_countries_of_the_world_data}"
    sql = "SELECT count(*) FROM countries_of_the_world"
    result = runner.invoke(app, ["query", "run", url, "--sql", sql])
    assert result.stdout != ""
    assert result.exit_code == 0


def test_list_schema_command(load_countries_of_the_world_data) -> None:
    """Test the query command with a SQL command."""
    url = f"sqlite:///{load_countries_of_the_world_data}"
    result = runner.invoke(app, ["catalog", "list-schema", url])
    assert result.stdout != ""
    assert result.exit_code == 0


def test_list_tables_command(load_countries_of_the_world_data) -> None:
    """Test the query command with a SQL command."""
    url = f"sqlite:///{load_countries_of_the_world_data}"
    result = runner.invoke(app, ["catalog", "list-tables", url])
    assert result.stdout != ""
    assert result.exit_code == 0


def test_list_columns_command(load_countries_of_the_world_data) -> None:
    """Test the query command with a SQL command."""
    url = f"sqlite:///{load_countries_of_the_world_data}"
    result = runner.invoke(app, ["catalog", "list-columns", url])
    assert result.stdout != ""
    assert result.exit_code == 0
