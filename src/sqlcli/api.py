import logging
from pathlib import Path
from typing import Generator, Optional

from alembic import command
from names_generator import generate_name
from sqlalchemy.orm.exc import NoResultFound

from sqlcli.core.catalog import Catalog, CatColumn, CatSchema, CatTable
from sqlcli.core.database import Database
from sqlcli.core.migrations import get_alembic_config

LOGGER = logging.getLogger(__name__)


def open_catalog(
    app_dir: Path,
    uri: Optional[str] = None,
) -> Catalog:
    """Open the catalog from the given uri."""
    if not uri:
        uri = f"sqlite:///{app_dir}/catalog.db"

    LOGGER.debug("Open Catalog from uri %s", uri)
    catalog = Catalog(uri)

    config = get_alembic_config(catalog.engine)
    command.upgrade(config, "heads")
    LOGGER.info("Initialized the database")

    return catalog


def scan_database(catalog: Catalog, db_url: str) -> None:
    """Scan the database and update the catalog."""
    LOGGER.debug("Scan database %s", db_url)
    with catalog.managed_session:
        source_scanned: bool = True
        try:
            catalog.get_source_by_uri(db_url)
            LOGGER.debug("Database %s already scanned", db_url)
        except NoResultFound:
            LOGGER.debug("Database %s not scanned yet", db_url)
            source_scanned = False

        if not source_scanned:
            cat_source = catalog.add_source(name=generate_name(), uri=db_url)
            with Database(db_url) as db:
                for schema in db.schemata():
                    cat_schema: CatSchema = catalog.add_schema(schema, cat_source)
                    for table in db.tables(schema):
                        cat_table: CatTable = catalog.add_table(table, cat_schema)
                        sort_order: int = 0
                        for column in db.columns(schema, table):
                            catalog.add_column(
                                column_name=column["name"],
                                data_type=str(column["type"]),
                                sort_order=sort_order,
                                table=cat_table,
                            )
                            sort_order += 1


def get_schema(catalog: Catalog, db_uri: str) -> Generator[CatSchema, None, None]:
    """Get the schema from the catalog."""
    with catalog.managed_session:
        scan_database(catalog, db_url=db_uri)
        cat_source = catalog.get_source_by_uri(db_uri)

        yield from catalog.get_schemas(source_name=cat_source.name)


def get_tables(
    catalog: Catalog, db_uri: str, schema_name: Optional[str]
) -> Generator[CatTable, None, None]:
    """Get the tables from the catalog."""
    with catalog.managed_session:
        scan_database(catalog, db_url=db_uri)
        cat_source = catalog.get_source_by_uri(db_uri)

        yield from catalog.get_tables(source_name=cat_source.name, schema_name=schema_name)


def get_columns(
    catalog: Catalog, db_uri: str, schema_name: Optional[str], table_name: Optional[str]
) -> Generator[CatColumn, None, None]:
    """Get the tables from the catalog."""
    with catalog.managed_session:
        scan_database(catalog, db_url=db_uri)
        cat_source = catalog.get_source_by_uri(db_uri)

        yield from catalog.get_columns(
            source_name=cat_source.name, schema_name=schema_name, table_name=table_name
        )
