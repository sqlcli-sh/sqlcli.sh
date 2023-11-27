import logging
from pathlib import Path
from typing import Optional

from alembic import command

from sqlcli.core.catalog import Catalog
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
    print(config)
    command.upgrade(config, "heads")
    LOGGER.info("Initialized the database")

    return catalog
