import datetime
import logging
from contextlib import closing
from typing import Generator

import pytest
from sqlalchemy.orm.exc import NoResultFound

from sqlcli.api import open_catalog
from sqlcli.core.catalog import (
    Catalog,
    CatColumn,
    CatSchema,
    CatSource,
    CatTable,
)

logger = logging.getLogger("sqlcli.test_catalog")


class File:
    def __init__(self, name: str, path: str, catalog: Catalog):
        self.name = name
        self._path = path
        self._catalog = catalog

    @property
    def path(self):
        return self._path

    def scan(self):
        import json

        with open(self.path) as file:
            content = json.load(file)

        with self._catalog.managed_session:
            try:
                source = self._catalog.get_source(content["name"])
            except NoResultFound:
                source = self._catalog.add_source(name=content["name"], uri=content["uri"])
            for s in content["schemata"]:
                try:
                    schema = self._catalog.get_schema(
                        source_name=source.name, schema_name=s["name"]
                    )
                except NoResultFound:
                    schema = self._catalog.add_schema(s["name"], source=source)

                for t in s["tables"]:
                    try:
                        table = self._catalog.get_table(
                            source_name=source.name,
                            schema_name=schema.name,
                            table_name=t["name"],
                        )
                    except NoResultFound:
                        table = self._catalog.add_table(t["name"], schema)

                    index = 0
                    for c in t["columns"]:
                        try:
                            self._catalog.get_column(
                                source_name=source.name,
                                schema_name=schema.name,
                                table_name=table.name,
                                column_name=c["name"],
                            )
                        except NoResultFound:
                            self._catalog.add_column(
                                column_name=c["name"],
                                data_type=c["data_type"],
                                sort_order=index,
                                table=table,
                            )
                        index += 1


@pytest.fixture(scope="module")
def save_catalog(open_catalog_connection):
    catalog = open_catalog_connection
    scanner = File("test", "tests/catalog.json", catalog)
    scanner.scan()
    yield catalog
    logging.debug("Deleting catalog loaded from file.")
    with catalog.managed_session as session:
        [session.delete(db) for db in session.query(CatSource).all()]
        [session.delete(schema) for schema in session.query(CatSchema).all()]
        [session.delete(table) for table in session.query(CatTable).all()]
        [session.delete(col) for col in session.query(CatColumn).all()]
        session.commit()


def test_catalog_tables(open_catalog_connection):
    catalog = open_catalog_connection
    with catalog.managed_session as session:
        session.query(CatSource).all()
        session.query(CatSchema).all()
        session.query(CatTable).all()
        session.query(CatColumn).all()


def test_read_catalog(save_catalog):  # noqa: PLR0915
    catalog = save_catalog

    with catalog.managed_session as session:
        dbs = session.query(CatSource).all()
        assert len(dbs) == 1
        db = dbs[0]
        assert db.name == "test"
        assert db.created_at is not None
        assert db.updated_at is not None

        assert len(db.schemata) == 1
        schema = db.schemata[0]
        assert schema.created_at is not None
        assert schema.updated_at is not None

        assert schema.name == "default"
        assert len(schema.tables) == 8

        tables = session.query(CatTable).filter(CatTable.name == "normalized_pagecounts").all()
        assert len(tables) == 1
        table = tables[0]
        assert table is not None
        assert table.name == "normalized_pagecounts"
        assert table.created_at is not None
        assert table.updated_at is not None
        assert len(table.columns) == 5

        page_id_column = table.columns[0]
        assert page_id_column.name == "page_id"
        assert page_id_column.data_type == "BIGINT"
        assert page_id_column.sort_order == 0
        assert page_id_column.created_at is not None
        assert page_id_column.updated_at is not None

        page_title_column = table.columns[1]
        assert page_title_column.name == "page_title"
        assert page_title_column.data_type == "STRING"
        assert page_title_column.sort_order == 1
        assert page_title_column.created_at is not None
        assert page_title_column.updated_at is not None

        page_url_column = table.columns[2]
        assert page_url_column.name == "page_url"
        assert page_url_column.data_type == "STRING"
        assert page_url_column.sort_order == 2
        assert page_url_column.created_at is not None
        assert page_url_column.updated_at is not None

        views_column = table.columns[3]
        assert views_column.name == "views"
        assert views_column.data_type == "BIGINT"
        assert views_column.sort_order == 3
        assert views_column.created_at is not None
        assert views_column.updated_at is not None

        bytes_sent_column = table.columns[4]
        assert bytes_sent_column.name == "bytes_sent"
        assert bytes_sent_column.data_type == "BIGINT"
        assert bytes_sent_column.sort_order == 4
        assert bytes_sent_column.created_at is not None
        assert bytes_sent_column.updated_at is not None


@pytest.fixture(scope="module")
def managed_session(save_catalog) -> Generator[Catalog, None, None]:
    catalog = save_catalog
    with catalog.managed_session:
        yield catalog


def test_get_source(managed_session):
    catalog = managed_session
    source = catalog.get_source("test")
    assert source.fqdn == "test"


def test_get_source_by_id(managed_session):
    catalog = managed_session
    source = catalog.get_source("test")

    source_by_id = catalog.get_source_by_id(source.id)

    assert source_by_id.fqdn == "test"


def test_get_schema(managed_session):
    catalog = managed_session
    schema = catalog.get_schema("test", "default")
    assert schema.fqdn == ("test", "default")


def test_get_schema_by_id(managed_session):
    catalog = managed_session
    schema = catalog.get_schema("test", "default")

    schema_by_id = catalog.get_schema_by_id(schema.id)
    assert schema_by_id.fqdn == ("test", "default")


def test_get_table(managed_session):
    catalog = managed_session
    table = catalog.get_table("test", "default", "page")
    assert table.fqdn == ("test", "default", "page")


def test_get_table_by_id(managed_session):
    catalog = managed_session
    table = catalog.get_table("test", "default", "page")

    table_by_id = catalog.get_table_by_id(table.id)

    assert table_by_id.fqdn == ("test", "default", "page")


def test_get_table_columns(managed_session):
    catalog = managed_session
    table = catalog.get_table("test", "default", "page")
    columns = catalog.get_columns_for_table(table)
    assert len(columns) == 3


def test_get_table_columns_with_timestamp(managed_session):
    catalog = managed_session
    table = catalog.get_table("test", "default", "page")
    columns = catalog.get_columns_for_table(table)

    for c in columns:
        print(c.updated_at.timestamp())

    updated_at = columns[0].updated_at
    before = updated_at - datetime.timedelta(minutes=1)
    after = updated_at + datetime.timedelta(minutes=1)

    columns = catalog.get_columns_for_table(table=table, newer_than=before)
    assert len(columns) == 3

    columns = catalog.get_columns_for_table(table=table, newer_than=after)
    assert len(columns) == 0


def test_get_column_in(managed_session):
    catalog = managed_session
    table = catalog.get_table("test", "default", "page")
    columns = catalog.get_columns_for_table(table=table, column_names=["page_id", "page_latest"])
    assert len(columns) == 2

    columns = catalog.get_columns_for_table(table=table, column_names=["page_id"])
    assert len(columns) == 1


def test_get_column(managed_session):
    catalog = managed_session
    column = catalog.get_column("test", "default", "page", "page_title")
    assert column.fqdn == ("test", "default", "page", "page_title")


def test_get_column_by_id(managed_session):
    catalog = managed_session
    column = catalog.get_column("test", "default", "page", "page_title")

    column_by_id = catalog.get_column_by_id(column.id)
    assert column_by_id.fqdn == ("test", "default", "page", "page_title")


def test_get_schemas(managed_session):
    catalog = managed_session
    schemas = catalog.get_schemas("test")
    assert len(schemas) == 1


def test_get_tables(managed_session):
    catalog = managed_session
    tables = catalog.get_tables("test", "default")
    assert len(tables) == 8


def test_get_columns(managed_session):
    catalog = managed_session
    columns = catalog.get_columns("test", "default", "page")
    assert len(columns) == 3


def test_get_all_columns(managed_session):
    catalog = managed_session
    columns = catalog.get_columns(source_name="test", schema_name=None, table_name=None)
    assert len(columns) == 32


def test_search_source(managed_session):
    catalog = managed_session
    databases = catalog.search_sources("t%")
    assert len(databases) == 1


def test_search_schema(managed_session):
    catalog = managed_session
    schemata = catalog.search_schema(source_like="test", schema_like="def%")
    assert len(schemata) == 1

    name_only = catalog.search_schema(schema_like="def%")
    assert len(name_only) == 1


def test_search_tables(managed_session):
    catalog = managed_session
    tables = catalog.search_tables(source_like="test", schema_like="default", table_like="page%")
    assert len(tables) == 5

    name_only = catalog.search_tables(table_like="page%")
    assert len(name_only) == 5


def test_search_table(managed_session):
    catalog = managed_session
    table = catalog.search_table(source_like="test", schema_like="default", table_like="pagecount%")
    assert table is not None

    name_only = catalog.search_table(table_like="pagecount%")
    assert name_only is not None


def test_search_column(managed_session):
    catalog = managed_session
    columns = catalog.search_column(
        source_like="test",
        schema_like="default",
        table_like="pagecounts",
        column_like="views",
    )
    assert len(columns) == 1

    name_only = catalog.search_column(column_like="view%")
    assert len(name_only) == 3


def test_update_default_schema(managed_session):
    catalog = managed_session
    source = catalog.get_source("test")
    schema = catalog.get_schema("test", "default")

    inserted_default_schema = catalog.update_source(source=source, default_schema=schema)

    default_schema = source.default_schema

    assert default_schema.source_id == inserted_default_schema.source_id
    assert default_schema.schema_id == inserted_default_schema.schema_id
    assert default_schema.source_id == source.id
    assert default_schema.schema_id == schema.id
    assert default_schema.schema == schema
    assert default_schema.source == source
    assert default_schema.updated_at >= default_schema.created_at


@pytest.mark.parametrize(
    ("name", "uri"),
    [
        ("pg", "postgres://user:pass@host:port/dbnametest"),
        ("mysql", "mysql://user:pass@host:port/dbname"),
        ("bigquery", "bigquery://user:pass@project_id"),
        ("glue", "glue://user:pass@region"),
        ("snowflake", "snowflake://user:pass@account/dbname"),
        ("athena", "athena://user:pass@region"),
        ("oracle", "oracle://user:pass@host:port/dbname"),
        ("sqlite", "sqlite:///path/to/file"),
    ],
)
def test_add_sources(open_catalog_connection, name, uri):
    catalog = open_catalog_connection

    with catalog.managed_session:
        catalog.add_source(name=name, uri=uri)

        source = catalog.get_source(source_name=name)

        assert source.name == name
        assert source.uri == uri


def test_default_catalog(tmpdir):
    with closing(open_catalog(app_dir=tmpdir)) as catalog:
        assert catalog.uri == f"sqlite:///{tmpdir}/catalog.db"
        default_catalog = tmpdir / "catalog.db"
        assert default_catalog.exists()
