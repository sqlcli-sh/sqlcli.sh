import csv
import logging
from shutil import rmtree
from typing import Dict

import pytest
from sqlalchemy import create_engine, text

sqlite_catalog_conf = """
catalog:
  path: {path}
"""


@pytest.fixture(scope="module")
def temp_sqlite_path(tmpdir_factory):
    """Generate temporary file path for SQLite database."""
    temp_dir = tmpdir_factory.mktemp("sqlite_test")
    sqlite_path = temp_dir.join("sqldb")

    yield sqlite_path

    rmtree(temp_dir)
    logging.info(f"Deleted {temp_dir!s}")


@pytest.fixture(scope="module")
def load_countries_of_the_world_data(temp_sqlite_path):
    """Load the countries of the world data into the SQLite database."""
    # Get  Data
    keys = [
        "Country",
        "Region",
        "Population",
        "Area_sq_mi",
        "Pop_Density_per_sq_mi",
        "Coastline_coast_area_ratio",
        "Net_migration",
        "Infant_mortality_per_1000_births",
        "GDP_per_capita",
        "Literacy_percent",
        "Phones_per_1000",
        "Arable_percent",
        "Crops_percent",
        "Other_percent",
        "Climate",
        "Birthrate",
        "Deathrate",
        "Agriculture",
        "Industry",
        "Service",
    ]

    with open("tests/data/countries_insert.sql") as file:
        insert = text(file.read())

    with open("tests/data/countries.csv") as csv_file:
        reader = csv.reader(csv_file)

        engine = create_engine(f"sqlite:///{temp_sqlite_path}")
        with engine.connect() as conn:
            with open("tests/data/countries.sql") as file:
                query = text(file.read())
                conn.execute(query)

            reader.__next__()
            for row in reader:
                params: Dict[str, str] = dict(zip(keys, row))
                conn.execute(insert, parameters=params)
            conn.commit()

    return temp_sqlite_path
