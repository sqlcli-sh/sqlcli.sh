from abc import ABC, abstractmethod
import os
from typing import Any

from sqlalchemy import text

from .records import Record, RecordCollection

class Connection(ABC):
    """
    Abstract class for a database connection.
    """

    @abstractmethod
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """
        Initialize the Connection object.

        Args:
            *args (Any): Additional arguments.
            **kwargs (Any): Additional keyword arguments.
        """
        pass

    @abstractmethod
    def execute(self, query: str, fetchall: bool=False, **params: str) -> Any:
        """
        Execute a query on the database.

        Args:
            query (str): The SQL query to execute.

        Returns:
            A cursor object.
        """
        pass

    @abstractmethod
    def get_catalog(self) -> Any:
        """
        Get the catalog of the database.

        Returns:
            A catalog object.
        """
        pass

class SQLAlchemyConnection(Connection):
    """A Database connection that uses SQLAlchemy."""

    def __init__(self, connection) -> None:
        self._conn = connection
        self.open = not connection.closed

    def close(self):
        self._conn.close()
        self.open = False

    def __enter__(self):
        return self

    def __exit__(self, exc, val, traceback):
        self.close()

    def __repr__(self):
        return '<Connection open={}>'.format(self.open)

    def execute(self, query, fetchall=False, **params: str) -> RecordCollection:
        """Executes the given SQL query against the connected Database.
        Parameters can, optionally, be provided. Returns a RecordCollection,
        which can be iterated over to get result rows as dictionaries.
        """

        # Execute the given query.
        cursor = self._conn.execute(text(query), **params) # TODO: PARAMS GO HERE

        # Row-by-row Record generator.
        row_gen = (Record(cursor.keys(), row) for row in cursor)

        # Convert psycopg2 results to RecordCollection.
        results = RecordCollection(row_gen)

        # Fetch all results if desired.
        if fetchall:
            results.all()

        return results

    def get_catalog(self) -> Any:
        pass

    def bulk_query(self, query, *multiparams):
        """Bulk insert or update."""

        self._conn.execute(text(query), *multiparams)

    def query_file(self, path, fetchall=False, **params):
        """Like Connection.query, but takes a filename to load a query from."""

        # If path doesn't exists
        if not os.path.exists(path):
            raise IOError("File '{}' not found!".format(path))

        # If it's a directory
        if os.path.isdir(path):
            raise IOError("'{}' is a directory!".format(path))

        # Read the given .sql file into memory.
        with open(path) as f:
            query = f.read()

        # Defer processing to self.query method.
        return self.query(query=query, fetchall=fetchall, **params)

    def bulk_query_file(self, path, *multiparams):
        """Like Connection.bulk_query, but takes a filename to load a query
        from.
        """

         # If path doesn't exists
        if not os.path.exists(path):
            raise IOError("File '{}'' not found!".format(path))

        # If it's a directory
        if os.path.isdir(path):
            raise IOError("'{}' is a directory!".format(path))

        # Read the given .sql file into memory.
        with open(path) as f:
            query = f.read()

        self._conn.execute(text(query), *multiparams)

    def transaction(self):
        """Returns a transaction object. Call ``commit`` or ``rollback``
        on the returned object as appropriate."""

        return self._conn.begin()