"""Test sqlcli."""

import sqlcli


def test_import() -> None:
    """Test that the package can be imported."""
    assert isinstance(sqlcli.__name__, str)
