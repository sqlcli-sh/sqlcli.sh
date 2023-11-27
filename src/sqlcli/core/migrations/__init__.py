import os

from alembic.config import Config

import sqlcli.core


def get_alembic_config(engine) -> Config:
    """Set migration_path and connection in alembic config."""
    library_dir = os.path.dirname(os.path.abspath(sqlcli.core.__file__))
    migration_path = os.path.join(library_dir, "migrations")
    config = Config(os.path.join(library_dir, "alembic.ini"))
    config.set_main_option("script_location", migration_path.replace("%", "%%"))
    config.attributes["connection"] = engine

    return config
