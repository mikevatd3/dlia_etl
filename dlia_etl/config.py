"""Environment-based configuration loaded from .env file."""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

VAULT_PATH = Path(os.environ.get("VAULT_PATH", "/mnt/v"))
PROPRIETARY_PATH = Path(os.environ.get("PROPRIETARY_PATH", "/mnt/s/1_PROPRIETARY/PROPRIETARY"))
DUA_PATH = Path(os.environ.get("DUA_PATH", "/mnt/q"))
SQL_DIR = Path(__file__).parent / "sql"
SOURCE_DIR = Path(__file__).parent / "sources"
FIELD_REFERENCE_DIR = Path(__file__).parent / "field_references"
OUT_SCHEMA = "dlia"  # Detroit Land Information Archive

