import logging
from contextlib import contextmanager
from typing import Generator, List, Optional

from sqlalchemy import (
    TIMESTAMP,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
    create_engine,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    Session,
    mapped_column,
    relationship,
    scoped_session,
    sessionmaker,
)
from sqlalchemy_mixins.serialize import SerializeMixin
from sqlalchemy_mixins.timestamp import TimestampsMixin

logger = logging.getLogger("sqlcli.core.Catalog")


class BaseModel(DeclarativeBase, SerializeMixin, TimestampsMixin):
    """Base model class that includes helper methods for serialization and timestamps."""


class CatSource(BaseModel):
    """Catalog Source class that represents a source in the catalog."""

    __tablename__ = "sources"

    id: Mapped[int] = mapped_column(primary_key=True)  # noqa: A003
    name: Mapped[str] = mapped_column(String, unique=True)
    uri: Mapped[str] = mapped_column(String)

    schemata: Mapped[List["CatSchema"]] = relationship(back_populates="source")
    default_schema: Mapped["DefaultSchema"] = relationship(
        back_populates="source",
        cascade="all, delete-orphan",
        uselist=False,
    )

    @property
    def fqdn(self):
        """Returns the fully qualified domain name of the CatSource object.

        Returns:
            str: The fully qualified domain name of the CatSource object.
        """
        return self.name

    def __repr__(self):
        return f"<Source: {self.name}>"

    def __eq__(self, other):
        return self.fqdn == other.fqdn

    def __hash__(self):
        return hash(self.fqdn)


class CatSchema(BaseModel):
    """Catalog Schema class that represents a schema in the catalog."""

    __tablename__ = "schemata"

    id: Mapped[int] = mapped_column(primary_key=True)  # noqa: A003
    name: Mapped[str] = mapped_column(String, unique=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id"))
    source: Mapped["CatSource"] = relationship(back_populates="schemata", lazy="joined")
    tables: Mapped[List["CatTable"]] = relationship(back_populates="schema")

    __table_args__ = (UniqueConstraint("source_id", "name", name="unique_schema_name"),)

    @property
    def fqdn(self):
        """Returns the fully qualified domain name of the CatSchema object.

        Returns:
            Tuple[str, str]: The fully qualified domain name of the CatSchema object, consisting of the source name and the schema name.
        """
        return self.source.name, self.name

    def __repr__(self):
        return f"<Database: {self.source.name}, Schema: {self.name}>"

    def __eq__(self, other):
        return self.fqdn == other.fqdn

    def __hash__(self):
        return hash(self.fqdn)


class DefaultSchema(BaseModel):
    """Default Schema class that represents the default schema in the catalog."""

    __tablename__ = "default_schema"

    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id"), primary_key=True)
    schema_id: Mapped[int] = mapped_column(Integer, ForeignKey("schemata.id"))
    schema: Mapped["CatSchema"] = relationship()
    source: Mapped["CatSource"] = relationship(back_populates="default_schema")


class CatTable(BaseModel):
    """Catalog Table class that represents a table in the catalog."""

    __tablename__ = "tables"

    id: Mapped[int] = mapped_column(primary_key=True)  # noqa: A003
    name: Mapped[str] = mapped_column(String, unique=True)
    schema_id: Mapped[int] = mapped_column(ForeignKey("schemata.id"))
    schema: Mapped["CatSchema"] = relationship(back_populates="tables", lazy="joined")
    columns: Mapped[List["CatColumn"]] = relationship(
        back_populates="table",
        order_by="CatColumn.sort_order",
    )  # type: ignore

    __table_args__ = (UniqueConstraint("schema_id", "name", name="unique_table_name"),)

    @property
    def fqdn(self):
        """Returns the fully qualified domain name of the CatTable object."""
        return self.schema.source.name, self.schema.name, self.name

    def __repr__(self):
        return "<Source: {}, Schema: {}, Table: {}>".format(
            self.schema.source.name, self.schema.name, self.name
        )

    def __eq__(self, other):
        return self.fqdn == other.fqdn

    def __hash__(self):
        return hash(self.fqdn)


class CatColumn(BaseModel):
    """Catalog Column class that represents a column in the catalog."""

    __tablename__ = "columns"

    id: Mapped[int] = mapped_column(primary_key=True)  # noqa: A003
    name: Mapped[str] = mapped_column(String, unique=True)
    data_type: Mapped[str]
    sort_order: Mapped[int]
    table_id: Mapped[int] = mapped_column(ForeignKey("tables.id"))
    table: Mapped["CatTable"] = relationship(back_populates="columns", lazy="joined")

    __table_args__ = (UniqueConstraint("table_id", "name", name="unique_column_name"),)

    @property
    def fqdn(self):
        """Returns the fully qualified domain name of the CatColumn object."""
        return (
            self.table.schema.source.name,
            self.table.schema.name,
            self.table.name,
            self.name,
        )

    def __repr__(self):
        return "<Source: {}, Schema: {}, Table: {}, Column: {}>".format(
            self.table.schema.source.name,
            self.table.schema.name,
            self.table.name,
            self.name,
        )

    def __eq__(self, other):
        return self.fqdn == other.fqdn

    def __lt__(self, other) -> bool:
        for s, o in zip(
            [
                self.table.schema.source.name,
                self.table.schema.name,
                self.table.name,
                self.sort_order,
            ],
            [
                other.table.schema.source.name,
                other.table.schema.name,
                other.table.name,
                other.sort_order,
            ],
        ):
            if s < o:
                return True
            if s > o:
                return False

        return False

    def __hash__(self):
        return hash(self.fqdn)


class Catalog:
    """Catalog class for managing database connections and sessions."""

    def __init__(self, uri: str, **kwargs):
        """Initializes a Catalog object with a URI and additional keyword arguments.

        Args:
            uri (str): The URI of the catalog.
            **kwargs: Additional keyword arguments.
        """
        self.uri = uri
        self._engine: object = create_engine(uri, **kwargs)
        self._scoped_session: Optional[scoped_session[Session]] = None
        self._connection_args = kwargs
        self._current_session: Optional[scoped_session[Session]] = None
        self._session_context_depth = 0
        self._commit_context_depth = 0

    def get_scoped_session(self) -> scoped_session:
        """Returns a scoped session for the catalog.

        Returns:
            scoped_session: The scoped session for the catalog.
        """
        if self._scoped_session is None:
            self._scoped_session = scoped_session(sessionmaker(bind=self.engine))

        return self._scoped_session

    @property  # type: ignore
    @contextmanager
    def managed_session(self) -> Generator[scoped_session, None, None]:
        """Context manager for managing sessions within the catalog.

        Yields:
            scoped_session: The managed session.
        """
        try:
            if self._current_session is None:
                self._current_session = self.get_scoped_session()
                logger.debug("Started new managed session: %s", self._current_session)
            else:
                logger.debug("Reusing session: %s", self._current_session)
            self._session_context_depth += 1
            logger.debug("Nesting level for session context: %d", self._session_context_depth)
            yield self._current_session
        except Exception:
            if self._current_session is not None:
                self._current_session.rollback()
            logger.debug("Rolled back session: %s", self._current_session)
            raise
        else:
            self._current_session.commit()
            logger.debug("Committed session: %s", self._current_session)
        finally:
            self._session_context_depth -= 1
            logger.debug("Un-nesting level for session context: %d", self._session_context_depth)
            assert self._session_context_depth >= 0

            if self._session_context_depth == 0:
                assert self._current_session is not None
                logger.debug("Removed managed session: %s", self._current_session)
                self._current_session.remove()
                self._current_session = None

    @property  # type: ignore
    @contextmanager
    def commit_context(self):
        """Context manager for managing commit context within the catalog.

        Yields:
            scoped_session: The current session.
        """
        try:
            self._commit_context_depth += 1
            logger.debug("Nesting level for commit context: %d", self._commit_context_depth)
            yield self._current_session
        except Exception:
            self._current_session.rollback()
            logger.debug(
                "Rolled back transaction due to exception. Session: %s",
                self._current_session,
            )
            raise
        else:
            self._current_session.commit()
            logger.debug("Committed transaction. Session: %s", self._current_session)
        finally:
            self._commit_context_depth -= 1
            logger.debug("Un-nesting level for commit context: %d", self._commit_context_depth)
            assert self._commit_context_depth >= 0

    def close(self):
        """Disposes the engine if it exists."""
        if self._engine is not None:
            self._engine.dispose()

    @property
    def engine(self):
        """Returns the engine for the catalog."""
        return self._engine

    def add_source(self, name: str, uri: str) -> CatSource:
        """Adds a source to the catalog."""
        assert self._current_session is not None
        created = CatSource(name=name, uri=uri)
        self._current_session.add(created)
        self._current_session.flush()
        return created

    def add_schema(self, schema_name: str, source: CatSource) -> CatSchema:
        """Adds a schema to the catalog."""
        assert self._current_session is not None
        created = CatSchema(name=schema_name, source=source)
        self._current_session.add(created)
        self._current_session.flush()
        return created

    def add_table(self, table_name: str, schema: CatSchema) -> CatTable:
        """Adds a table to the catalog."""
        assert self._current_session is not None

        created = CatTable(name=table_name, schema=schema)
        self._current_session.add(created)
        self._current_session.flush()
        return created

    def add_column(
        self, column_name: str, data_type: str, sort_order: int, table: CatTable
    ) -> CatColumn:
        """Adds a column to the catalog.

        Args:
            column_name (str): The name of the column.
            data_type (str): The data type of the column.
            sort_order (int): The sort order of the column.
            table (CatTable): The table to which the column belongs.

        Returns:
            CatColumn: The added column.
        """
        assert self._current_session is not None
        created = CatColumn(
            name=column_name, data_type=data_type, sort_order=sort_order, table=table
        )
        self._current_session.add(created)
        self._current_session.flush()
        return created

    def get_source(self, source_name: str) -> CatSource:
        """Gets a source from the catalog based on the provided name.

        Args:
            source_name (str): The name of the source.

        Returns:
            CatSource: The matching source.
        """
        assert self._current_session is not None
        return self._current_session.query(CatSource).filter(CatSource.name == source_name).one()

    def get_schema(self, source_name: str, schema_name: str) -> CatSchema:
        """Gets a schema from the catalog based on the provided source and schema names.

        Args:
            source_name (str): The name of the source.
            schema_name (str): The name of the schema.

        Returns:
            CatSchema: The matching schema.
        """
        assert self._current_session is not None
        return (
            self._current_session.query(CatSchema)
            .join(CatSchema.source)
            .filter(CatSource.name == source_name)
            .filter(CatSchema.name == schema_name)
            .one()
        )

    def get_table(self, source_name: str, schema_name: str, table_name: str) -> CatTable:
        """Gets a table from the catalog based on the provided source, schema, and table names.

        Args:
            source_name (str): The name of the source.
            schema_name (str): The name of the schema.
            table_name (str): The name of the table.

        Returns:
            CatTable: The matching table.
        """
        assert self._current_session is not None
        return (
            self._current_session.query(CatTable)
            .join(CatTable.schema)
            .join(CatSchema.source)
            .filter(CatSource.name == source_name)
            .filter(CatSchema.name == schema_name)
            .filter(CatTable.name == table_name)
            .one()
        )

    def get_columns_for_table(
        self,
        table: CatTable,
        column_names: Optional[List[str]] = None,
        newer_than: Optional[TIMESTAMP] = None,
    ) -> List[CatColumn]:
        """Gets columns for a specific table in the catalog based on the provided criteria.

        Args:
            table (CatTable): The table for which to retrieve columns.
            column_names (Optional[List[str]], optional): The list of column names to filter. Defaults to None.
            newer_than (TIMESTAMP, optional): The timestamp for filtering newer columns. Defaults to None.

        Returns:
            List[CatColumn]: A list of matching columns.
        """
        assert self._current_session is not None
        stmt = (
            self._current_session.query(CatColumn)
            .join(CatColumn.table)
            .join(CatTable.schema)
            .join(CatSchema.source)
            .filter(CatSource.name == table.schema.source.name)
            .filter(CatSchema.name == table.schema.name)
            .filter(CatTable.name == table.name)
        )

        if column_names is not None:
            stmt = stmt.filter(CatColumn.name.in_(column_names))
        if newer_than is not None:
            stmt = stmt.filter(CatColumn.updated_at > newer_than)
        stmt = stmt.order_by(CatColumn.sort_order)
        return stmt.all()

    def get_column(
        self, source_name: str, schema_name: str, table_name: str, column_name: str
    ) -> CatColumn:
        """Gets a specific column in the catalog based on the provided criteria.

        Args:
            source_name (str): The name of the source.
            schema_name (str): The name of the schema.
            table_name (str): The name of the table.
            column_name (str): The name of the column.

        Returns:
            CatColumn: The matching column.
        """
        assert self._current_session is not None
        return (
            self._current_session.query(CatColumn)
            .join(CatColumn.table)
            .join(CatTable.schema)
            .join(CatSchema.source)
            .filter(CatSource.name == source_name)
            .filter(CatSchema.name == schema_name)
            .filter(CatTable.name == table_name)
            .filter(CatColumn.name == column_name)
            .one()
        )

    def get_source_by_id(self, source_id: int) -> CatSource:
        """Gets a source in the catalog based on the provided source_id.

        Args:
            source_id (int): The id of the source.

        Returns:
            CatSource: The matching source.
        """
        assert self._current_session is not None
        return self._current_session.query(CatSource).filter(CatSource.id == source_id).one()

    def get_schema_by_id(self, schema_id: int) -> CatSchema:
        """Gets a schema in the catalog based on the provided schema_id.

        Args:
            schema_id (int): The id of the schema.

        Returns:
            CatSchema: The matching schema.
        """
        assert self._current_session is not None
        return self._current_session.query(CatSchema).filter(CatSchema.id == schema_id).one()

    def get_table_by_id(self, table_id: int) -> CatTable:
        """Gets a table in the catalog based on the provided table_id.

        Args:
            table_id (int): The id of the table.

        Returns:
            CatTable: The matching table.
        """
        assert self._current_session is not None
        return self._current_session.query(CatTable).filter(CatTable.id == table_id).one()

    def get_column_by_id(self, column_id: int) -> CatColumn:
        """Gets a column in the catalog based on the provided column_id.

        Args:
            column_id (int): The id of the column.

        Returns:
            CatColumn: The matching column.
        """
        assert self._current_session is not None
        return self._current_session.query(CatColumn).filter(CatColumn.id == column_id).one()

    def get_source_by_uri(self, uri: str) -> CatSource:
        """Gets a source from the catalog based on the provided name.

        Args:
            uri (str): The URI of the database.

        Returns:
            CatSource: The matching source.
        """
        assert self._current_session is not None
        return self._current_session.query(CatSource).filter(CatSource.uri == uri).one()

    def get_sources(self) -> List[CatSource]:
        """Gets all sources in the catalog.

        Returns:
            List[CatSource]: A list of all sources in the catalog.
        """
        assert self._current_session is not None
        return self._current_session.query(CatSource).all()

    def get_schemas(self, source_name: str) -> List[CatSchema]:
        """Gets all schemas in the catalog for the provided source.

        Args:
            source_name (str): The name of the source.

        Returns:
            List[CatSchema]: A list of all schemas in the catalog for the provided source.
        """
        assert self._current_session is not None
        return (
            self._current_session.query(CatSchema)
            .join(CatSchema.source)
            .filter(CatSource.name == source_name)
            .all()
        )

    def get_tables(self, source_name: str, schema_name: Optional[str]) -> List[CatTable]:
        """Gets all tables in the catalog for the provided source and schema.

        Args:
            source_name (str): The name of the source.
            schema_name (str): The name of the schema.

        Returns:
            List[CatTable]: A list of all tables in the catalog for the provided source and schema.
        """
        assert self._current_session is not None
        stmt = (
            self._current_session.query(CatTable)
            .join(CatTable.schema)
            .join(CatSchema.source)
            .filter(CatSource.name == source_name)
        )

        if schema_name is not None:
            stmt = stmt.filter(CatSchema.name == schema_name)

        return stmt.all()

    def get_columns(
        self, source_name: str, schema_name: Optional[str], table_name: Optional[str]
    ) -> List[CatColumn]:
        """Gets all columns in the catalog for the provided source, schema, and table.

        Args:
            source_name (str): The name of the source.
            schema_name (str): The name of the schema.
            table_name (str): The name of the schema.


        Returns:
            List[CatColumn]: A list of all columns in the catalog for the provided source, schema and table.
        """
        assert self._current_session is not None
        stmt = (
            self._current_session.query(CatColumn)
            .join(CatColumn.table)
            .join(CatTable.schema)
            .join(CatSchema.source)
            .filter(CatSource.name == source_name)
        )

        if table_name is not None:
            stmt = stmt.filter(CatTable.name == table_name)
        if schema_name is not None:
            stmt = stmt.filter(CatSchema.name == schema_name)

        stmt = stmt.order_by(CatSchema.name, CatTable.name, CatColumn.sort_order)
        return stmt.all()

    def search_sources(self, source_like: str) -> List[CatSource]:
        """Searches for sources in the catalog based on the provided source_like pattern.

        Args:
            source_like (str): The pattern to match source names.

        Returns:
            List[CatSource]: A list of matching sources.
        """
        assert self._current_session is not None
        return self._current_session.query(CatSource).filter(CatSource.name.like(source_like)).all()

    def search_schema(self, schema_like: str, source_like: Optional[str] = None) -> List[CatSchema]:
        """Searches for schemas in the catalog based on the provided schema_like pattern.

        Args:
            schema_like (str): The pattern to match schema names.
            source_like (Optional[str], optional): The pattern to match source names. Defaults to None.

        Returns:
            List[CatSchema]: A list of matching schemas.
        """
        assert self._current_session is not None
        stmt = self._current_session.query(CatSchema)
        if source_like is not None:
            stmt = stmt.join(CatSchema.source).filter(CatSource.name.like(source_like))
        stmt = stmt.filter(CatSchema.name.like(schema_like))
        logger.debug(str(stmt))
        return stmt.all()

    def search_tables(
        self,
        table_like: str,
        schema_like: Optional[str] = None,
        source_like: Optional[str] = None,
    ) -> List[CatTable]:
        """Searches for tables in the catalog based on the provided criteria.

        Args:
            table_like (str): The pattern to match table names.
            schema_like (Optional[str], optional): The pattern to match schema names. Defaults to None.
            source_like (Optional[str], optional): The pattern to match source names. Defaults to None.

        Returns:
            List[CatTable]: A list of matching tables.
        """
        assert self._current_session is not None
        stmt = self._current_session.query(CatTable)
        if source_like is not None or schema_like is not None:
            stmt = stmt.join(CatTable.schema)
        if source_like is not None:
            stmt = stmt.join(CatSchema.source).filter(CatSource.name.like(source_like))
        if schema_like is not None:
            stmt = stmt.filter(CatSchema.name.like(schema_like))

        stmt = stmt.filter(CatTable.name.like(table_like))
        logger.debug(str(stmt))
        return stmt.all()

    def search_table(
        self,
        table_like: str,
        schema_like: Optional[str] = None,
        source_like: Optional[str] = None,
    ) -> CatTable:
        """Searches for a specific table in the catalog based on the provided criteria.

        Args:
            table_like (str): The pattern to match table names.
            schema_like (Optional[str], optional): The pattern to match schema names. Defaults to None.
            source_like (Optional[str], optional): The pattern to match source names. Defaults to None.

        Returns:
            CatTable: The matching table.
        """
        tables = self.search_tables(
            table_like=table_like, schema_like=schema_like, source_like=source_like
        )
        if len(tables) == 0:
            raise RuntimeError(f"'{table_like}' table not found")
        if len(tables) > 1:
            raise RuntimeError("Ambiguous table name. Multiple matches found")

        return tables[0]

    def search_column(
        self,
        column_like: str,
        table_like: Optional[str] = None,
        schema_like: Optional[str] = None,
        source_like: Optional[str] = None,
    ) -> List[CatColumn]:
        """Searches for columns in the catalog based on the provided criteria.

        Args:
            column_like (str): The pattern to match column names.
            table_like (Optional[str], optional): The pattern to match table names. Defaults to None.
            schema_like (Optional[str], optional): The pattern to match schema names. Defaults to None.
            source_like (Optional[str], optional): The pattern to match source names. Defaults to None.

        Returns:
            List[CatColumn]: A list of matching columns.
        """
        assert self._current_session is not None
        stmt = self._current_session.query(CatColumn)
        if source_like is not None or schema_like is not None or table_like is not None:
            stmt = stmt.join(CatColumn.table)
        if source_like is not None or schema_like is not None:
            stmt = stmt.join(CatTable.schema)
        if source_like is not None:
            stmt = stmt.join(CatSchema.source).filter(CatSource.name.like(source_like))
        if schema_like is not None:
            stmt = stmt.filter(CatSchema.name.like(schema_like))
        if table_like is not None:
            stmt = stmt.filter(CatTable.name.like(table_like))

        stmt = stmt.filter(CatColumn.name.like(column_like))
        return stmt.all()

    def update_source(self, source: CatSource, default_schema: CatSchema) -> DefaultSchema:
        """Updates the source with the provided default schema.

        Args:
            source (CatSource): The source to be updated.
            default_schema (CatSchema): The default schema to be associated with the source.

        Returns:
            DefaultSchema: The updated default schema.
        """
        assert self._current_session is not None
        created = DefaultSchema(source=source, schema=default_schema)
        self._current_session.add(created)
        self._current_session.flush()
        return created
