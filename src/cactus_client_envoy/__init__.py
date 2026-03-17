import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

# Load .env from the repo root. envoy's model layer reads DATABASE_URL at import time
load_dotenv(Path("/home/ubuntu/code/cactus-client-envoy/.env"))

__version__ = "0.0.1"

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
