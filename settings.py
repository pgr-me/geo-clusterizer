# standard library imports
import os
from pathlib import Path
# third-party imports
from dotenv import load_dotenv


load_dotenv(override=True)

CRS = os.getenv("CRS")

DATA_DIR = Path(os.getenv("DATA_DIR"), "data")
RAW_DIR = DATA_DIR / "raw"
INTERIM_DIR = DATA_DIR / "interim"
PROCESSED_DIR = DATA_DIR / "processed"
MODELS_DIR = Path(os.getenv("MODELS_DIR", "models"))
DIRS = [DATA_DIR, RAW_DIR, INTERIM_DIR, PROCESSED_DIR, MODELS_DIR]

