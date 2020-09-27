# standard library imports
import datetime
import getpass
import os
from pathlib import Path
import time
# third-party imports
from doit.tools import config_changed, create_folder, run_once
from loguru import logger
import pandas as pd
# local imports
from settings import DIRS, RAW_DIR, INTERIM_DIR, PROCESSED_DIR, MODELS_DIR, LOG_PATH, ROOT_DIR, ACS_SPAN, ACS_YEAR, RAW_ACS_DATA_DIR
from src.acs import ACS

logger.add(LOG_PATH)

@logger.catch
def task_makedirs():
    """Make directories if they don't exist"""
    for dir_ in DIRS:
        yield dict(name=dir_, actions=[(create_folder, [dir_])], uptodate=[run_once])


@logger.catch
def task_download_acs():
    """Download American Community Survey (ACS) data.
    This underlying code is from
    https://gist.githubusercontent.com/erikbern/89c5f44bd1354854a8954fa2df04453d/raw/efd7b7c31d781a5cae9849be60ab86967bf7d2ed/american_community_survey_example.py
    Author of that underlying code is Erik Bernhardsson | erikbern | https://gist.github.com/erikbern
    """
    acs = ACS(ACS_YEAR, ACS_SPAN, RAW_ACS_DATA_DIR, overwrite=False)
    return dict(actions=[acs.get_acs_metadata, acs.get_acs_data], task_dep=["makedirs"], verbosity=2, clean=True)


@logger.catch
def task_parse_acs():
    """Parse downloaded ACS data.
    This underlying code is from
    https://gist.githubusercontent.com/erikbern/89c5f44bd1354854a8954fa2df04453d/raw/efd7b7c31d781a5cae9849be60ab86967bf7d2ed/american_community_survey_example.py
    Author of that underlying code is Erik Bernhardsson | erikbern | https://gist.github.com/erikbern
    """
    cmd = f"python etl_acs.py"
    return dict(actions=[cmd], task_dep=["makedirs"], verbosity=2, clean=True)


def task_etl_tiger():
    """Download TIGER Census tract shapefiles"""


def task_etl_he():
    """Pull data from """
    pass
