# standard library imports
import datetime
import getpass
import os
from pathlib import Path
from string import Template
import time

# third-party imports
from doit.tools import config_changed, create_folder, run_once
from loguru import logger
import pandas as pd
import requests

# local imports
from settings import (
    DIRS,
    RAW_DIR,
    INTERIM_DIR,
    PROCESSED_DIR,
    MODELS_DIR,
    LOG_PATH,
    ROOT_DIR,
    ACS_SPAN,
    ACS_YEAR,
    RAW_ACS_DATA_DIR,
    RAW_SHAPEFILES_DIR,
)
from src.acs import ACS

logger.add(LOG_PATH)


@logger.catch
def task_makedirs():
    """Make directories if they don't exist"""
    for dir_ in DIRS:
        yield dict(name=dir_, actions=[(create_folder, [dir_])], uptodate=[run_once])


@logger.catch
def task_get_tiger_files():
    """Download and save TIGER shapefiles"""

    def get_zips(url, dst):
        r = requests.get(url)
        with open(dst, "wb") as f:
            f.write(r.content)
        return True

    fips = """1, 2, 4, 5, 6, 8, 9, 10, 11, 12, 13, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 44, 45, 46, 47, 48, 49, 50, 51, 53, 54, 55, 56, 60, 66, 69, 72, 78"""
    fips = [x.strip().zfill(2) for x in fips.split(",") if len(x) > 0]
    url_template = Template(
        "https://www2.census.gov/geo/tiger/TIGER${year}/TRACT/tl_${year}_${fip}_tract.zip"
    )
    for fip in fips:
        url = url_template.substitute(year=ACS_YEAR, fip=fip)
        fn = url.split("/")[-1]
        dst = RAW_SHAPEFILES_DIR / fn
        yield dict(
            name=dst.stem,
            actions=[(get_zips, [url, dst])],
            uptodate=[True],
            targets=[dst],
            clean=True,
        )


@logger.catch
def task_download_acs():
    """Download American Community Survey (ACS) data.
    This underlying code is from
    https://gist.githubusercontent.com/erikbern/89c5f44bd1354854a8954fa2df04453d/raw/efd7b7c31d781a5cae9849be60ab86967bf7d2ed/american_community_survey_example.py
    Author of that underlying code is Erik Bernhardsson | erikbern | https://gist.github.com/erikbern
    """
    acs = ACS(ACS_YEAR, ACS_SPAN, RAW_ACS_DATA_DIR, overwrite=False)
    return dict(
        actions=[acs.get_acs_metadata, acs.get_acs_data],
        task_dep=["makedirs"],
        verbosity=2,
        clean=True,
    )


@logger.catch
def task_parse_acs():
    """Parse downloaded ACS data.
    This underlying code is from
    https://gist.githubusercontent.com/erikbern/89c5f44bd1354854a8954fa2df04453d/raw/efd7b7c31d781a5cae9849be60ab86967bf7d2ed/american_community_survey_example.py
    Author of that underlying code is Erik Bernhardsson | erikbern | https://gist.github.com/erikbern
    """
    cmd = f"python etl_acs.py"
    return dict(actions=[cmd], task_dep=["makedirs"], verbosity=2, clean=True)

