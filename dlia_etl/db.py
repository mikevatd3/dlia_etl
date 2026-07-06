"""SQLAlchemy engine factory."""
import os
from sqlalchemy import create_engine, Engine
from urllib.parse import quote


def get_engine(db_name: str = "ipds") -> Engine:
    """Build a SQLAlchemy connection URL for the given database."""
    user = os.environ["DLIA_DB_USER"]
    password = quote(os.environ.get("DLIA_DB_PASSWORD", ""), safe="")
    host = os.environ.get("DLIA_DB_HOST", "edw")
    port = os.environ.get("DLIA_DB_PORT", "5432")

    if password:
        return create_engine(
            f"postgresql+psycopg://{user}:{password}@{host}:{port}/{db_name}"
        )
    return create_engine(f"postgresql+psycopg://{user}@{host}:{port}/{db_name}")

