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
from settings import DIRS, RAW_DIR, INTERIM_DIR, PROCESSED_DIR, MODELS_DIR


def task_make_dirs():
    """Make directories if they don't exist"""
    for dir_ in DIRS:
        yield dict(name=dir_,
                   actions=[(create_folder, [dir_])],
                   uptodate=[run_once])


def task_etl_acs():
    """ETL American Community Survey data.
    This underlying code is from
    https://gist.githubusercontent.com/erikbern/89c5f44bd1354854a8954fa2df04453d/raw/efd7b7c31d781a5cae9849be60ab86967bf7d2ed/american_community_survey_example.py
    Author of that underlying code is Erik Bernhardsson | erikbern | https://gist.github.com/erikbern
    """
    pass


def task_etl_tiger():
    """Download TIGER Census tract shapefiles"""


def task_etl_he():
    """Pull data from """
    pass

