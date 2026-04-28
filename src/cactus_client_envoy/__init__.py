import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

# Load .env from the package repo root (src/../.env). envoy's model layer reads DATABASE_URL at import time
load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")

__version__ = "0.0.2"

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
