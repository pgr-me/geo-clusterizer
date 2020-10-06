# standard library imports
import os
from pathlib import Path


CRS = "EPSG:4269"
ACS_YEAR = 2018
ACS_SPAN = 5  # 1 or 5
if ACS_SPAN not in [1, 5]:
    raise ValueError(
        "ACS_SPAN must be either 1 or 5 for 1-year or 5-year ACS datasets, respectively"
    )
ROOT_DIR = Path(".").absolute()
DATA_DIR = ROOT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
RAW_ACS_DATA_DIR = RAW_DIR / f"{ACS_YEAR}_{ACS_SPAN}_year_data"
RAW_SHAPEFILES_DIR = RAW_DIR / f"{ACS_YEAR}_tiger"
INTERIM_DIR = DATA_DIR / "interim"
INTERIM_ACS_DST = INTERIM_DIR / 'acs.pkl'
PROCESSED_DIR = DATA_DIR / "processed"
MODELS_DIR = ROOT_DIR / "models"
LOG_PATH = ROOT_DIR / "log.log"
LOOKUPS_SRC = ROOT_DIR / '2018_5y_lookup.txt'  # specify which tables you want by modifying this file
DIRS = [DATA_DIR, RAW_DIR, RAW_ACS_DATA_DIR, RAW_SHAPEFILES_DIR, INTERIM_DIR, PROCESSED_DIR, MODELS_DIR]

RANDOM_STATE = 777

# corex model constants
N_HIDDEN = 20  # maximum number of corex components
N_SAMPLES = 40000  # number of samples to draw for each trial
CE_CUTOFF = 0.01  # cutoff used to select number of corex components
N_TRIALS = 5  # number of model training trials

# gaussian mixture components
MAX_COMPONENTS_LIST = list(range(2, 20+1))