import os
from abc import ABC, abstractmethod
from typing import Any

from sqlalchemy import text

from sqlcli.core.records import Record, RecordCollection


class Connection(ABC):
    """Abstract class for a database connection.

    This class provides an interface for a database connection. It defines
    the basic methods that a database connection should have.
    """

    @abstractmethod
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the Connection object.

        Args:
            *args (Any): Additional arguments.
            **kwargs (Any): Additional keyword arguments.
        """

    @abstractmethod
    def __enter__(self):
        """Enter the context of the database connection."""

    @abstractmethod
    def __exit__(self, exc, val, traceback):
        """Exit the context of the database connection."""

    @abstractmethod
    def execute(self, query: str, fetchall: bool = False, **params: str) -> Any:
        """Execute a query on the database.

        Args:
            query (str): The SQL query to execute.
            fetchall (bool): If True, fetch all results after executing the query.
            **params (str): Parameters to pass to the query.

        Returns:
            A cursor object.
        """

    @abstractmethod
    def get_catalog(self) -> Any:
        """Get the catalog of the database.

        Returns:
            A catalog object.
        """


class SQLAlchemyConnection(Connection):
    """A Database connection that uses SQLAlchemy.

    This class provides a concrete implementation of the Connection interface
    using SQLAlchemy. It provides methods to execute queries, handle transactions,
    and interact with the database in other ways.
    """

    def __init__(self, connection) -> None:
        """Initialize the SQLAlchemyConnection object.

        Args:
            connection: The SQLAlchemy connection object.
        """
        self._conn = connection
        self.open = not connection.closed

    def close(self):
        """Close the database connection."""
        self._conn.close()
        self.open = False

    def __enter__(self):
        """Enter the context of the database connection."""
        return self

    def __exit__(self, exc, val, traceback):
        """Exit the context of the database connection."""
        self.close()

    def __repr__(self):
        """Return a string representation of the SQLAlchemyConnection object."""
        return f"<Connection open={self.open}>"

    def execute(self, query, fetchall=False, **params: str) -> RecordCollection:
        """Execute a query on the database.

        Args:
            query (str): The SQL query to execute.
            fetchall (bool): If True, fetch all results after executing the query.
            **params (str): Parameters to pass to the query.

        Returns:
            A RecordCollection object.
        """
        # Execute the given query.
        cursor = self._conn.execute(text(query), **params)  # TODO: PARAMS GO HERE

        # Row-by-row Record generator.
        row_gen = (Record(cursor.keys(), row) for row in cursor)

        # Convert psycopg2 results to RecordCollection.
        results = RecordCollection(row_gen)

        # Fetch all results if desired.
        if fetchall:
            results.all()

        return results

    def get_catalog(self) -> Any:
        """Get the catalog of the database.

        Returns:
            A catalog object.
        """

    def bulk_query(self, query, *multiparams):
        """Execute a bulk insert or update query on the database.

        Args:
            query (str): The SQL query to execute.
            *multiparams: Parameters to pass to the query.
        """
        self._conn.execute(text(query), *multiparams)

    def query_file(self, path, fetchall=False, **params):
        """Execute a query on the database from a file.

        Args:
            path (str): The path to the file containing the SQL query.
            fetchall (bool): If True, fetch all results after executing the query.
            **params: Parameters to pass to the query.
        """
        # If path doesn't exists
        if not os.path.exists(path):
            raise OSError(f"File '{path}' not found!")

        # If it's a directory
        if os.path.isdir(path):
            raise OSError(f"'{path}' is a directory!")

        # Read the given .sql file into memory.
        with open(path) as f:
            query = f.read()

        # Defer processing to self.query method.
        return self.query(query=query, fetchall=fetchall, **params)

    def bulk_query_file(self, path, *multiparams):
        """Execute a bulk insert or update query on the database from a file.

        Args:
            path (str): The path to the file containing the SQL query.
            *multiparams: Parameters to pass to the query.
        """
        # If path doesn't exists
        if not os.path.exists(path):
            raise OSError(f"File '{path}'' not found!")

        # If it's a directory
        if os.path.isdir(path):
            raise OSError(f"'{path}' is a directory!")

        # Read the given .sql file into memory.
        with open(path) as f:
            query = f.read()

        self._conn.execute(text(query), *multiparams)

    def transaction(self):
        """Start a transaction on the database.

        Returns:
            A transaction object.
        """
        return self._conn.begin()
