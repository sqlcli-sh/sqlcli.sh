import os
from contextlib import contextmanager
from typing import Generator, Optional

from sqlalchemy import create_engine, exc, inspect
from sqlalchemy.engine.interfaces import ReflectedColumn

from sqlcli.core.connection import Connection, SQLAlchemyConnection
from sqlcli.core.records import RecordCollection


class Database:
    """A Database. Encapsulates a url and an SQLAlchemy engine with a pool of connections."""

    def __init__(self, db_url: Optional[str] = None, **kwargs):
        """Initializes the Database object.

        Args:
            db_url (str): The database URL. If not provided, it will fallback to $DATABASE_URL.
            **kwargs: Additional keyword arguments for the SQLAlchemy engine.

        Raises:
            ValueError: If no db_url was provided and $DATABASE_URL is not set.
        """
        # If no db_url was provided, fallback to $DATABASE_URL.
        self.db_url = db_url or os.environ.get("DATABASE_URL")

        if not self.db_url:
            raise ValueError("You must provide a db_url.")

        # Create an engine.
        self._engine = create_engine(self.db_url, **kwargs)
        self.open = True

    def close(self):
        """Closes the Database."""
        self._engine.dispose()
        self.open = False

    def __enter__(self):
        """Context manager entry point."""
        return self

    def __exit__(self, exc, val, traceback):
        """Context manager exit point."""
        self.close()

    def __repr__(self):
        """Return a string representation of the Database object."""
        return f"<Database open={self.open}>"

    def get_connection(self) -> Connection:
        """Get a connection to this Database. Connections are retrieved from a pool."""
        if not self.open:
            raise exc.ResourceClosedError("Database closed.")

        return SQLAlchemyConnection(self._engine.connect())

    def execute(self, query: str, fetchall: bool = False, **params) -> RecordCollection:
        """Executes the given SQL query against the Database.

        Parameters can, optionally, be provided. Returns a RecordCollection, which can be
        iterated over to get result rows as dictionaries.
        """
        with self.get_connection() as conn:
            return conn.execute(query, fetchall, **params)

    def bulk_query(self, query, *multiparams):
        """Bulk insert or update."""
        with self.get_connection() as conn:
            conn.bulk_query(query, *multiparams)

    def query_file(self, path, fetchall=False, **params):
        """Like Database.query, but takes a filename to load a query from."""
        with self.get_connection() as conn:
            return conn.query_file(path, fetchall, **params)

    def bulk_query_file(self, path, *multiparams):
        """Like Database.bulk_query, but takes a filename to load a query from."""
        with self.get_connection() as conn:
            conn.bulk_query_file(path, *multiparams)

    def schemata(self) -> Generator[str, None, None]:
        """Returns a list of schemata for the connected database."""
        # Setup SQLAlchemy for Database inspection.
        yield from inspect(self._engine).get_schema_names()

    def tables(self, schema: str) -> Generator[str, None, None]:
        """Returns a list of tables for the connected database."""
        # Setup SQLAlchemy for Database inspection.
        yield from inspect(self._engine).get_table_names(schema)

    def columns(self, schema: str, table: str) -> Generator[ReflectedColumn, None, None]:
        """Returns a list of columns for the connected database."""
        # Setup SQLAlchemy for Database inspection.
        yield from inspect(self._engine).get_columns(table, schema)

    @contextmanager
    def transaction(self):
        """A context manager for executing a transaction on this Database."""
        conn = self.get_connection()
        tx = conn.transaction()
        try:
            yield conn
            tx.commit()
        except Exception:
            tx.rollback()
        finally:
            conn.close()
